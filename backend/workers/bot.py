import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import signal
import shutil
import sqlite3
import subprocess
import sys
from collections import deque
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.runtime.bootstrap import ensure_runtime_schema  # noqa: E402
from backend.runtime.logging import bind_context, configure_logging, log_event, reset_context  # noqa: E402
from backend.runtime.constants import (  # noqa: E402
    AUDIO_STATUS_DISABLED,
    AUDIO_STATUS_FAILED,
    AUDIO_STATUS_PENDING,
    AUDIO_STATUS_READY,
    AUDIO_STATUS_RECORDING,
    POSTPROCESS_STATUS_PENDING,
    POSTPROCESS_STATUS_QUEUED,
)
from backend.runtime.paths import (  # noqa: E402
    db_path as runtime_db_path,
    get_meeting_audio_dir,
    get_meeting_audio_chunks_dir,
    get_meeting_master_audio_path,
    get_meeting_pcm_audio_path,
)
from backend.runtime.participant_names import (  # noqa: E402
    is_roster_heading_name,
    normalize_participant_name,
)
from backend.runtime.teams_links import (  # noqa: E402
    TEAMS_JOIN_WITH_ID_PAGE_URL,
    parse_join_with_id_target,
)

configure_logging()
logger = logging.getLogger("notera.worker.bot")
DEBUG_ARTIFACTS_ENABLED = os.getenv("NOTERA_DEBUG_ARTIFACTS", "1").strip().lower() in {"1", "true", "yes", "on"}
CHAT_NOTICE_MESSAGE = (
    "Konuşmaları kayıt altına almaya başladım. "
    "Beni toplantıdan çıkarmak için chat'e sadece 'bot ok' yazabilirsiniz."
)
CHAT_LANGUAGE_HELP_MESSAGE = (
    "Caption dilini değiştirmek isterseniz Teams içinde şu yolu izleyin: "
    "More -> Language and speech -> Show live captions. "
    "Ardından caption alanındaki ayar simgesinden Language settings veya Caption settings bölümünü açıp Language alanını değiştirin."
)
CHAT_AUDIO_FAILURE_MESSAGE = "Ses kaydı başlatılamadı."
CHAT_EXIT_COMMAND = "bot ok"
CHAT_EXIT_ACK_MESSAGE = "Çıkış komutu algılandı. Toplantıdan çıkılıyor."
REMOTE_AUDIO_ATTRIBUTE = "data-notera-remote-audio"
CAPTION_RECENT_SCAN_LIMIT = 24
CAPTION_SLOT_DEDUPE_WINDOW_SECONDS = 20.0
CAPTION_MEETING_DEDUPE_WINDOW_SECONDS = 6.0
CAPTION_MEMORY_WINDOW_SECONDS = 180.0
CAPTION_REPLAY_MIN_AGE_SECONDS = 12.0
CAPTION_REPLAY_BURST_MIN_MATCHES = 4
CAPTION_REPLAY_VISIBLE_WINDOW = 8
CAPTION_LOG_FALLBACK_LINE_WINDOW = 18
PARTICIPANT_REGISTRY_CONFIG = {
    "version": 5,
    "panel_selectors": [
        "[data-tid='roster-panel']",
        "[data-tid*='roster-panel']",
        "[data-tid='roster-panel-content']",
        "[data-tid*='participants-panel']",
        "[data-tid*='people-panel']",
        "[data-tid*='right-side-panel']",
        "[aria-label*='Participants'][role='dialog']",
        "[aria-label*='People'][role='dialog']",
        "[title*='Participants'][role='dialog']",
        "[title*='People'][role='dialog']",
    ],
    "panel_button_selectors": [
        "#roster-button",
        "button[aria-label*='Participants']",
        "button[aria-label*='People']",
        "button[title*='Participants']",
        "button[title*='People']",
        "button:has-text('Participants')",
        "button:has-text('People')",
        "[role='button'][aria-label*='Show Participants']",
        "[role='button'][aria-label*='Show participants']",
        "[role='button'][title*='Participants']",
        "[data-tid='roster-button']",
        "[data-tid*='participants']",
        "[data-tid*='roster']",
    ],
    "row_selectors": [
        "[role='listitem']",
        "[role='row']",
        "[role='treeitem']",
        "[role='option']",
        "[role='menuitem']",
        "[data-tid*='participant']",
        "[data-tid*='roster-item']",
        "[data-tid*='people-picker']",
        "[data-tid*='persona']",
        "[data-tid*='member']",
        ".roster-item",
        "[class*='participant']",
        "[class*='roster']",
        "[class*='persona']",
    ],
    "tile_selectors": [
        "[data-tid*='video-tile']",
        "[data-tid*='calling-participant']",
        "[data-tid*='grid-tile']",
        ".video-tile",
        "[class*='videoTile']",
    ],
    "video_surface_selectors": [
        "[data-tid='only-videos-wrapper']",
        "[data-tid*='calling-pagination']",
        "[data-tid*='video-gallery']",
        "[data-tid*='gallery']",
        "[data-stream-type='Video']",
        "[role='menu'][data-acc-id]",
    ],
    "name_selectors": [
        "[data-tid*='display-name']",
        "[data-tid*='participant-name']",
        "[data-tid*='persona-name']",
        "[data-tid*='roster-name']",
        "[data-tid*='item-display-name']",
        "[id*='itemDisplayName']",
        "[class*='displayName']",
        "[class*='participant-name']",
        "[class*='persona']",
        "[class*='name']",
    ],
    "speaking_selectors": [
        "[data-tid='voice-level-stream-outline']",
        "[data-tid*='voice-level']",
        "[data-tid*='speaking']",
        "[aria-label*='speaking']",
        "[title*='speaking']",
        "[class*='speaking']",
        "[class*='active-speaker']",
        "[class*='talking']",
        ".vdi-frame-occlusion",
    ],
    "voice_signal_selectors": [
        "[data-tid='voice-level-stream-outline']",
    ],
    "signal_container_selectors": [
        "[role='menuitem']",
        "[role='listitem']",
        "[role='row']",
        "[role='treeitem']",
        "[role='option']",
        "[data-tid*='participant']",
        "[data-tid*='roster-item']",
        "[data-tid*='video-tile']",
        "[data-tid*='videoTile']",
        "[data-tid*='calling-participant']",
        ".participant-tile",
        ".video-tile",
        ".roster-item",
    ],
    "occlusion_class_names": [
        "vdi-frame-occlusion",
    ],
    "identity_attributes": [
        "data-participant-id",
        "data-user-id",
        "data-object-id",
        "data-person-id",
        "data-id",
        "data-key",
        "data-item-key",
    ],
    "participant_keywords": [
        "participant",
        "participants",
        "people",
        "roster",
        "attendee",
        "member",
        "guest",
        "show participants",
        "katılımcı",
        "katilimci",
        "kişi",
        "kisi",
    ],
}


def get_db_path():
    return str(runtime_db_path())


def normalize_caption_text(text):
    return " ".join((text or "").strip().split())


def parse_bot_dt(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def caption_tokens(text):
    return re.findall(r"[\wçğıöşüÇĞİÖŞÜ'-]+", normalize_caption_text(text).casefold())


def fuzzy_common_prefix_token_count(left_tokens, right_tokens):
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if not caption_token_match(left_token, right_token):
            break
        count += 1
    return count


def caption_token_match(left_token, right_token):
    if left_token == right_token:
        return True

    shorter_length = min(len(left_token), len(right_token))
    if shorter_length >= 2 and (left_token.startswith(right_token) or right_token.startswith(left_token)):
        return True

    if shorter_length >= 4 and SequenceMatcher(None, left_token, right_token).ratio() >= 0.82:
        return True

    return False


def caption_metrics(text):
    normalized = normalize_caption_text(text)
    return {
        "text_length": len(normalized),
        "token_count": len(caption_tokens(normalized)),
    }


def make_caption_fingerprint(speaker, text):
    normalized_speaker = normalize_caption_text(speaker).casefold()
    normalized_text = normalize_caption_text(text).casefold()
    return f"{normalized_speaker}|{normalized_text}"


def make_caption_equivalence_fingerprint(speaker, text):
    normalized_speaker = normalize_caption_text(speaker).casefold()
    normalized_text = normalize_caption_text(text).casefold().rstrip(".,!?;:…")
    return f"{normalized_speaker}|{normalized_text}"


def _compatible_speakers_for_merge(existing_speaker, new_speaker):
    existing_value = normalize_caption_text(existing_speaker).casefold()
    new_value = normalize_caption_text(new_speaker).casefold()
    if not existing_value or existing_value == "unknown":
        return True
    if not new_value or new_value == "unknown":
        return True
    return existing_value == new_value


def _texts_should_merge(existing_text, new_text):
    old_text = normalize_caption_text(existing_text)
    new_value = normalize_caption_text(new_text)
    if not old_text or not new_value:
        return False

    old_fold = old_text.casefold()
    new_fold = new_value.casefold()
    if old_fold == new_fold:
        return True

    punctuation = ".,!?;:…"
    if new_fold.rstrip(punctuation) == old_fold.rstrip(punctuation):
        return True

    old_tokens = caption_tokens(old_text)
    new_tokens = caption_tokens(new_value)
    if old_tokens and new_tokens:
        shared_prefix = fuzzy_common_prefix_token_count(old_tokens, new_tokens)
        shorter_length = min(len(old_tokens), len(new_tokens))
        if shared_prefix >= min(3, shorter_length) and shared_prefix / max(shorter_length, 1) >= 0.6:
            return True

    ratio = SequenceMatcher(None, old_fold, new_fold).ratio()
    return ratio >= 0.82 and abs(len(new_fold) - len(old_fold)) <= 96


def _caption_revision_score(text):
    normalized = normalize_caption_text(text)
    if not normalized:
        return -1
    return len(caption_tokens(normalized)) * 100 + len(normalized)


def _choose_preferred_caption_text(existing_text, new_text):
    existing_normalized = normalize_caption_text(existing_text)
    new_normalized = normalize_caption_text(new_text)
    if not existing_normalized:
        return new_normalized
    if not new_normalized:
        return existing_normalized
    if _caption_revision_score(new_normalized) > _caption_revision_score(existing_normalized):
        return new_normalized
    return existing_normalized


def _caption_duplicate_reason(cursor, meeting_id, speaker, text, observed_at, slot_index, revision_no):
    current_dt = observed_at or datetime.utcnow()
    current_exact = make_caption_fingerprint(speaker, text)
    current_equivalent = make_caption_equivalence_fingerprint(speaker, text)
    recent_rows = cursor.execute(
        """
        SELECT speaker_name, text, observed_at, slot_index, revision_no
        FROM teamscaptionevent
        WHERE meeting_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (meeting_id, CAPTION_RECENT_SCAN_LIMIT),
    ).fetchall()

    for previous_speaker, previous_text, previous_observed_at, previous_slot_index, previous_revision_no in recent_rows:
        previous_dt = parse_bot_dt(previous_observed_at)
        if previous_dt is None:
            continue
        delta_seconds = abs((current_dt - previous_dt).total_seconds())
        if delta_seconds > CAPTION_SLOT_DEDUPE_WINDOW_SECONDS:
            break

        previous_exact = make_caption_fingerprint(previous_speaker, previous_text)
        previous_equivalent = make_caption_equivalence_fingerprint(previous_speaker, previous_text)
        previous_revision = int(previous_revision_no or 0)

        if (
            slot_index is not None
            and previous_slot_index is not None
            and int(previous_slot_index) == int(slot_index)
            and previous_exact == current_exact
        ):
            return "duplicate_slot_exact"

        if (
            delta_seconds <= CAPTION_MEETING_DEDUPE_WINDOW_SECONDS
            and previous_exact == current_exact
        ):
            return "duplicate_recent_exact"

        if (
            delta_seconds <= CAPTION_MEETING_DEDUPE_WINDOW_SECONDS
            and previous_equivalent == current_equivalent
            and revision_no <= previous_revision
        ):
            return "duplicate_recent"

    return None


def _remember_recent_caption_event(memory, caption, observed_at):
    if not caption:
        return
    memory.append(
        {
            "fingerprint": make_caption_equivalence_fingerprint(caption.get("speaker"), caption.get("text")),
            "speaker": caption.get("speaker"),
            "text": caption.get("text"),
            "observed_at": observed_at or datetime.utcnow(),
        }
    )


def _prune_recent_caption_memory(memory, current_dt):
    while memory:
        observed_at = memory[0].get("observed_at")
        if observed_at is None:
            memory.popleft()
            continue
        if abs((current_dt - observed_at).total_seconds()) <= CAPTION_MEMORY_WINDOW_SECONDS:
            break
        memory.popleft()


def _find_recent_caption_memory_match(memory, speaker, text, current_dt):
    fingerprint = make_caption_equivalence_fingerprint(speaker, text)
    for entry in reversed(memory):
        observed_at = entry.get("observed_at")
        if observed_at is None:
            continue
        age_seconds = abs((current_dt - observed_at).total_seconds())
        if age_seconds > CAPTION_MEMORY_WINDOW_SECONDS:
            break
        if age_seconds < CAPTION_REPLAY_MIN_AGE_SECONDS:
            continue
        if entry.get("fingerprint") == fingerprint:
            return entry
    return None


def _caption_match_score(existing_caption, speaker, text, slot_hint):
    existing_speaker = existing_caption.get("speaker")
    existing_text = existing_caption.get("text")
    if not _compatible_speakers_for_merge(existing_speaker, speaker):
        return None
    if not _texts_should_merge(existing_text, text):
        return None

    score = 0
    if slot_hint and existing_caption.get("slot_hint") == slot_hint:
        score += 80
    existing_exact = make_caption_fingerprint(existing_speaker, existing_text)
    current_exact = make_caption_fingerprint(speaker, text)
    if existing_exact == current_exact:
        score += 40
    score += min(_caption_revision_score(text), _caption_revision_score(existing_text)) // 100
    return score


def _find_pending_caption_key(pending_captions, speaker, text, slot_hint):
    best_key = None
    best_score = None
    for key, caption in pending_captions.items():
        score = _caption_match_score(caption, speaker, text, slot_hint)
        if score is None:
            continue
        if best_score is None or score > best_score:
            best_key = key
            best_score = score
    return best_key


async def get_first_visible_locator(page, selectors, timeout=1000):
    """Return the first visible locator matching any selector."""
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=timeout):
                    continue
                return candidate, selector
        except Exception as e:
            logger.debug("Failed searching selector %s: %s", selector, e)
    return None, None


async def click_first_visible_selector(
    page,
    selectors,
    click_label,
    wait_after=1,
    dump_filename=None,
):
    """Click the first visible element matching the provided selectors."""
    for selector in selectors:
        candidate, _ = await get_first_visible_locator(page, [selector], timeout=1000)
        if candidate is None:
            continue
        try:
            await candidate.click(force=True, timeout=5000)
            logger.info(f"Clicked {click_label} with selector: {selector}")
            if wait_after:
                await asyncio.sleep(wait_after)
            if dump_filename:
                await dump_dom(page, dump_filename)
            return True
        except Exception as e:
            logger.debug(f"Failed clicking {click_label} with selector {selector}: {e}")
    return False


async def open_more_menu(page):
    """Open the Teams in-meeting More menu."""
    more_button_selectors = [
        "button#callingButtons-showMoreBtn",
        "button[aria-label='More']",
        "button[title='More options']",
        "button[aria-label='More options']",
        "button:has-text('More')",
    ]

    if await click_first_visible_selector(
        page,
        more_button_selectors,
        "More button",
        wait_after=1,
        dump_filename="more_menu_dom.html",
    ):
        return True

    logger.warning("Could not open the Teams More menu in the bot session.")
    return False


async def install_participant_registry_hook(page):
    try:
        return await page.evaluate(
            """(config) => {
              const version = Number(config?.version || 1);
              if (window.__noteraParticipantRegistry?.version === version) {
                window.__noteraParticipantRegistry.config = config;
                window.__noteraParticipantRegistry.ensureObserver();
                return true;
              }

              const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
              const lower = (value) => normalize(value).toLowerCase();
              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
              };
              const listHasMatch = (value, values) => values.some((candidate) => lower(value).includes(lower(candidate)));
              const hasParticipantHint = (value) => listHasMatch(value, config.participant_keywords || []);
              const participantHeaderVisible = (root) => {
                const header = root?.querySelector?.('[data-tid="right-side-panel-header-title"], [role="heading"], h1, h2, h3');
                const headerText = normalize(header?.innerText || header?.getAttribute?.('aria-label') || header?.getAttribute?.('title') || '');
                return hasParticipantHint(headerText);
              };
              const isLikelyDisplayName = (value) => {
                const candidate = normalize(value);
                if (!candidate || candidate.length > 120) return false;
                if (/^(participants?|people|meeting chat|chat|search|call controls|more options)$/i.test(candidate)) return false;
                if (/^(organizer|presenter|attendee|guest|speaker|microphone|muted)$/i.test(candidate)) return false;
                if (/^(?:in|not in) this meeting(?:\s*\(\d+\))?$/i.test(candidate)) return false;
                if (/^(?:bu toplantıda|bu toplantida)(?:\s*\(\d+\))?$/i.test(candidate)) return false;
                if (/^(?:bu toplantıda değil|bu toplantida degil)(?:\s*\(\d+\))?$/i.test(candidate)) return false;
                if (/^\d+\s+(?:people|participants?)$/i.test(candidate)) return false;
                return true;
              };
              const queryAllSafe = (root, selectors) => {
                const results = [];
                for (const selector of selectors || []) {
                  try {
                    results.push(...Array.from(root.querySelectorAll(selector)));
                  } catch (_err) {
                    continue;
                  }
                }
                return results;
              };
              const pushUnique = (values, node) => {
                if (!node || values.includes(node) || !isVisible(node)) return;
                values.push(node);
              };
              const extractAttribute = (row, names) => {
                for (const name of names || []) {
                  const direct = row.getAttribute?.(name);
                  if (direct) return direct;
                }
                for (const child of Array.from(row.querySelectorAll('*')).slice(0, 12)) {
                  for (const name of names || []) {
                    const value = child.getAttribute?.(name);
                    if (value) return value;
                  }
                }
                return '';
              };
              const extractAttributeWithSource = (row, names) => {
                for (const name of names || []) {
                  const direct = row.getAttribute?.(name);
                  if (direct) return { value: direct, source: name };
                }
                for (const child of Array.from(row.querySelectorAll('*')).slice(0, 12)) {
                  for (const name of names || []) {
                    const value = child.getAttribute?.(name);
                    if (value) return { value, source: name };
                  }
                }
                return { value: '', source: '' };
              };
              const extractDisplayName = (row, lines) => {
                for (const selector of config.name_selectors || []) {
                  try {
                    const node = row.querySelector(selector);
                    const value = normalize(node?.innerText || node?.getAttribute?.('aria-label') || node?.getAttribute?.('title') || '');
                    if (isLikelyDisplayName(value)) return value;
                  } catch (_err) {
                    continue;
                  }
                }
                return lines.find((line) => isLikelyDisplayName(line)) || '';
              };
              const matchesAnySelector = (node, selectors) => {
                for (const selector of selectors || []) {
                  try {
                    if (node?.matches?.(selector)) return true;
                  } catch (_err) {
                    continue;
                  }
                }
                return false;
              };
              const closestMatchingAncestor = (node, selectors) => {
                let current = node instanceof Element ? node : node?.parentElement;
                while (current) {
                  if (matchesAnySelector(current, selectors)) {
                    return current;
                  }
                  current = current.parentElement;
                }
                return null;
              };
              const detectSpeakingFromVoiceSignal = (row) => {
                const voiceOutline = queryAllSafe(row, config.voice_signal_selectors || []).find((node) => node instanceof HTMLElement);
                if (!voiceOutline) {
                  return { isSpeaking: false, hasSignal: false };
                }
                let current = voiceOutline;
                while (current) {
                  for (const className of config.occlusion_class_names || []) {
                    if (current.classList?.contains?.(className)) {
                      return { isSpeaking: true, hasSignal: true };
                    }
                  }
                  current = current.parentElement;
                }
                return { isSpeaking: false, hasSignal: true };
              };
              const detectSpeaking = (row, text, sourceKind) => {
                const voiceDetection = detectSpeakingFromVoiceSignal(row);
                if (voiceDetection.hasSignal) {
                  return voiceDetection.isSpeaking;
                }
                if (sourceKind !== 'participant_panel') {
                  return false;
                }
                return /(currently speaking|is speaking|konusuyor|konuşuyor|speaking|talking)/.test(lower(text));
              };
              const hasVoiceSignal = (row) => {
                return detectSpeakingFromVoiceSignal(row).hasSignal;
              };
              const buildEntry = (row, sourceKind) => {
                if (!isVisible(row)) return null;
                const text = normalize(row.innerText || row.getAttribute('aria-label') || row.getAttribute('title') || '');
                if (!text || text.length > 400) return null;
                const lines = text.split('\\n').map((line) => normalize(line)).filter(Boolean);
                const displayName = extractDisplayName(row, lines);
                if (!isLikelyDisplayName(displayName)) return null;
                const stableKeyMatch = extractAttributeWithSource(row, ['data-key', 'data-item-key']);
                const platformIdentityMatch = extractAttributeWithSource(row, config.identity_attributes);
                const stableKey =
                  stableKeyMatch.value
                  || row.id
                  || '';
                const platformIdentity =
                  platformIdentityMatch.value
                  || row.dataset?.participantId
                  || row.dataset?.personId
                  || '';
                const lowered = lower(text);
                const roleLine = lines.find((line) => /(organizer|presenter|attendee|sunucu|duzenleyen)/i.test(line)) || '';
                return {
                  display_name: displayName,
                  stable_key: stableKey,
                  stable_key_source: stableKeyMatch.source || (row.id ? 'id' : ''),
                  platform_identity: platformIdentity,
                  source_kind: sourceKind,
                  role: roleLine || null,
                  join_state: /(left|not in this meeting|ayrildi|ayrıldı)/.test(lowered) ? 'left' : 'present',
                  is_speaking: detectSpeaking(row, text, sourceKind),
                  is_muted: /(muted|mic off|microphone off|mikrofon kapali|mikrofon kapalı|sesi kapali|sesi kapalı)/.test(lowered),
                  is_bot: /transcription bot|notera bot| bot$/i.test(displayName),
                  dom_key: [
                    sourceKind,
                    stableKey,
                    row.getAttribute('data-tid') || '',
                    row.getAttribute('role') || '',
                    row.getAttribute('aria-label') || '',
                    row.className || '',
                  ].filter(Boolean).join('|'),
                };
              };
              const findPanelRoots = () => {
                const roots = [];
                for (const selector of config.panel_selectors || []) {
                  try {
                    const node = document.querySelector(selector);
                    if (node?.getAttribute?.('data-tid') === 'calling-right-side-panel' && !participantHeaderVisible(node)) {
                      continue;
                    }
                    pushUnique(roots, node);
                  } catch (_err) {
                    continue;
                  }
                }
                const genericContainers = Array.from(document.querySelectorAll('[role="dialog"], aside, [role="complementary"], section, div'));
                for (const node of genericContainers) {
                  if (!isVisible(node)) continue;
                  const ownText = lower([
                    node.getAttribute('data-tid'),
                    node.getAttribute('aria-label'),
                    node.getAttribute('title'),
                  ].filter(Boolean).join(' '));
                  const header = node.querySelector('[data-tid="right-side-panel-header-title"], [role="heading"], h1, h2, h3');
                  const headingText = lower(header?.innerText || header?.getAttribute?.('aria-label') || header?.getAttribute?.('title') || '');
                  if (hasParticipantHint(`${ownText} ${headingText}`)) {
                    pushUnique(roots, node);
                  }
                }
                return roots;
              };
              const findVideoRoots = () => {
                const roots = [];
                for (const selector of config.video_surface_selectors || []) {
                  try {
                    for (const node of Array.from(document.querySelectorAll(selector))) {
                      pushUnique(roots, node);
                    }
                  } catch (_err) {
                    continue;
                  }
                }
                return roots;
              };
              const collectVoiceSignalEntries = (results, seen) => {
                const voiceSignals = queryAllSafe(document, config.voice_signal_selectors || []);
                for (const signalNode of voiceSignals) {
                  const container = closestMatchingAncestor(signalNode, config.signal_container_selectors || config.row_selectors || []);
                  if (!container || !isVisible(container) || !hasVoiceSignal(container)) {
                    continue;
                  }
                  const entry = buildEntry(container, 'voice_signal');
                  if (!entry) continue;
                  const dedupeKey = `${entry.stable_key}|${entry.platform_identity}|${entry.display_name}|voice_signal`;
                  if (seen.has(dedupeKey)) continue;
                  seen.add(dedupeKey);
                  results.push(entry);
                }
              };
              const collectFromRoots = (roots, sourceKind, results, seen) => {
                for (const root of roots) {
                  const rows = queryAllSafe(root, config.row_selectors);
                  for (const row of rows) {
                    const entry = buildEntry(row, sourceKind);
                    if (!entry) continue;
                    const dedupeKey = `${entry.stable_key}|${entry.platform_identity}|${entry.display_name}|${sourceKind}`;
                    if (seen.has(dedupeKey)) continue;
                    seen.add(dedupeKey);
                    results.push(entry);
                  }
                }
              };
              const collectVideoTileFallback = (results, seen) => {
                const tiles = queryAllSafe(document, config.tile_selectors);
                for (const tile of tiles) {
                  const entry = buildEntry(tile, 'video_tile');
                  if (!entry) continue;
                  const dedupeKey = `${entry.stable_key}|${entry.platform_identity}|${entry.display_name}|video_tile`;
                  if (seen.has(dedupeKey)) continue;
                  seen.add(dedupeKey);
                  results.push(entry);
                }
              };
              const collectSnapshot = () => {
                const results = [];
                const seen = new Set();
                const roots = findPanelRoots();
                collectFromRoots(roots, 'participant_panel', results, seen);
                collectVoiceSignalEntries(results, seen);
                if (!roots.length) {
                  collectFromRoots(findVideoRoots(), 'video_surface', results, seen);
                }
                if (!results.length) {
                  collectVideoTileFallback(results, seen);
                }
                return results;
              };

              const state = {
                version,
                config,
                observer: null,
                lastSnapshot: [],
                lastMutationAtMs: null,
                refresh() {
                  this.lastSnapshot = collectSnapshot();
                  this.lastMutationAtMs = Date.now();
                  return this.lastSnapshot;
                },
                ensureObserver() {
                  if (this.observer || !document.body) return;
                  this.observer = new MutationObserver(() => {
                    this.refresh();
                  });
                  this.observer.observe(document.body, {
                    subtree: true,
                    childList: true,
                    attributes: true,
                    attributeFilter: ['data-tid', 'aria-label', 'title', 'class', 'style'],
                  });
                },
                disconnect() {
                  if (this.observer) {
                    this.observer.disconnect();
                    this.observer = null;
                  }
                },
              };

              state.refresh();
              state.ensureObserver();
              window.__noteraParticipantRegistry = state;
              window.__noteraGetParticipantSnapshot = () => {
                state.ensureObserver();
                return {
                  items: state.refresh(),
                  changed_at_ms: state.lastMutationAtMs,
                };
              };
              return true;
            }""",
            PARTICIPANT_REGISTRY_CONFIG,
        )
    except Exception as e:
        logger.debug("Could not install participant registry hook: %s", e)
        return False


async def participant_panel_visible(page):
    selectors = PARTICIPANT_REGISTRY_CONFIG["panel_selectors"]
    for selector in selectors:
        try:
            if not await page.locator(selector).first.is_visible(timeout=250):
                continue
            if selector == "[data-tid*='right-side-panel']":
                header_text = await page.evaluate(
                    """() => {
                      const root = document.querySelector('[data-tid="calling-right-side-panel"]');
                      const header = root?.querySelector?.('[data-tid="right-side-panel-header-title"], [role="heading"], h1, h2, h3');
                      return (header?.innerText || header?.getAttribute?.('aria-label') || header?.getAttribute?.('title') || '').trim();
                    }"""
                )
                if not re.search(r"participant|people|roster|katılımc|kisi|kişi", (header_text or ""), re.IGNORECASE):
                    continue
                return True
        except Exception:
            continue
    try:
        return await page.evaluate(
            """() => {
              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
              };
              const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              const hasParticipantHint = (value) => /participant|people|roster|attendee|member|guest|katılımc|kisi|kişi/.test(value);
              const candidates = Array.from(document.querySelectorAll('[role="dialog"], aside, [role="complementary"], section, div'));
              for (const node of candidates) {
                if (!isVisible(node)) continue;
                const ownText = normalize([
                  node.getAttribute('data-tid'),
                  node.getAttribute('aria-label'),
                  node.getAttribute('title'),
                ].filter(Boolean).join(' '));
                const headingText = normalize(
                  (node.querySelector('[data-tid="right-side-panel-header-title"], h1, h2, h3, [role="heading"]')?.innerText || '')
                );
                if (!hasParticipantHint(`${ownText} ${headingText}`)) continue;
                const hasRows = node.querySelector(
                  '[role="listitem"], [role="row"], [role="treeitem"], [role="option"], [data-tid*="participant"], [data-tid*="persona"], [data-tid*="roster"], [class*="participant"], [class*="roster"], [class*="persona"]'
                );
                if (hasRows || headingText) {
                  return true;
                }
              }
              return false;
            }"""
        )
    except Exception:
        return False


async def open_participant_panel(page):
    if await participant_panel_visible(page):
        return True
    selectors = PARTICIPANT_REGISTRY_CONFIG["panel_button_selectors"]
    if await click_first_visible_selector(page, selectors, "participant panel", wait_after=1):
        return await participant_panel_visible(page)
    try:
        clicked = await page.evaluate(
            """() => {
              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
              };
              const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              const scoreFor = (value) => {
                let score = 0;
                if (/participants?/.test(value)) score += 6;
                if (/people/.test(value)) score += 5;
                if (/roster/.test(value)) score += 4;
                if (/member|attendee|guest/.test(value)) score += 3;
                if (/chat and people/.test(value)) score += 2;
                if (/katılımc|kisi|kişi/.test(value)) score += 4;
                if (/chat/.test(value) && !/people|participant/.test(value)) score -= 3;
                if (/more/.test(value)) score -= 2;
                return score;
              };
              let best = null;
              let bestScore = 0;
              for (const node of Array.from(document.querySelectorAll('button, [role="button"], [role="tab"]'))) {
                if (!isVisible(node)) continue;
                const text = normalize([
                  node.innerText,
                  node.getAttribute('aria-label'),
                  node.getAttribute('title'),
                  node.getAttribute('data-tid'),
                  node.id,
                ].filter(Boolean).join(' '));
                const score = scoreFor(text);
                if (score > bestScore) {
                  best = node;
                  bestScore = score;
                }
              }
              if (!best || bestScore <= 0) return false;
              best.click();
              return true;
            }"""
        )
        if clicked:
            logger.info("Opened participant panel via generic button scan.")
            await asyncio.sleep(1)
            return await participant_panel_visible(page)
    except Exception as e:
        logger.debug("Generic participant panel opener failed: %s", e)
    return False


async def collect_participant_registry_snapshot(page, observed_at):
    await install_participant_registry_hook(page)
    snapshot = await page.evaluate(
        "() => window.__noteraGetParticipantSnapshot ? window.__noteraGetParticipantSnapshot() : ({items: []})"
    )
    items = (snapshot or {}).get("items") or []
    normalized_items = []
    for item in items or []:
        display_name, role_value = split_participant_display_name(
            item.get("display_name") or "",
            item.get("role"),
        )
        if not display_name or is_roster_heading_name(display_name):
            continue
        stable_key = normalize_caption_text(item.get("stable_key"))
        platform_identity = normalize_caption_text(item.get("platform_identity"))
        if is_generic_participant_identity(stable_key):
            stable_key = ""
        if is_unstable_stable_key(stable_key):
            stable_key = ""
        if is_generic_participant_identity(platform_identity):
            platform_identity = ""
        normalized_items.append(
            {
                "display_name": display_name,
                "stable_key": stable_key,
                "stable_key_source": normalize_caption_text(item.get("stable_key_source")),
                "platform_identity": platform_identity,
                "source_kind": normalize_caption_text(item.get("source_kind")) or "participant_panel",
                "role": role_value,
                "join_state": normalize_caption_text(item.get("join_state")) or "present",
                "is_speaking": bool(item.get("is_speaking")),
                "is_muted": bool(item.get("is_muted")),
                "dom_key": normalize_caption_text(item.get("dom_key")),
                "is_bot": bool(item.get("is_bot")) or is_bot_participant_name(display_name),
                "observed_at": observed_at,
            }
        )
    deduped_items = {}
    for item in normalized_items:
        dedupe_key = (
            item.get("platform_identity")
            or item.get("stable_key")
            or item.get("display_name", "").casefold()
        )
        current = deduped_items.get(dedupe_key)
        if current is None:
            deduped_items[dedupe_key] = item
            continue
        preferred, other = (
            (item, current)
            if participant_snapshot_item_score(item) > participant_snapshot_item_score(current)
            else (current, item)
        )
        deduped_items[dedupe_key] = merge_participant_snapshot_items(preferred, other)

    final_items = list(deduped_items.values())
    by_display_name = {}
    for item in final_items:
        display_key = item.get("display_name", "").casefold()
        current = by_display_name.get(display_key)
        if current is None:
            by_display_name[display_key] = item
            continue
        preferred, other = (
            (item, current)
            if participant_snapshot_item_score(item) > participant_snapshot_item_score(current)
            else (current, item)
        )
        by_display_name[display_key] = merge_participant_snapshot_items(preferred, other)

    return list(by_display_name.values())


async def collect_participant_debug_state(page):
    try:
        return await page.evaluate(
            """(config) => {
              const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
              const lower = (value) => normalize(value).toLowerCase();
              const isVisible = (node) => {
                if (!node) return false;
                const style = window.getComputedStyle(node);
                const rect = node.getBoundingClientRect();
                return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
              };
              const summarize = (node) => ({
                tag: node.tagName,
                data_tid: node.getAttribute('data-tid') || '',
                role: node.getAttribute('role') || '',
                aria_label: node.getAttribute('aria-label') || '',
                title: node.getAttribute('title') || '',
                class_name: String(node.className || '').slice(0, 200),
                text: normalize(node.innerText || '').slice(0, 300),
              });
              const visibleButtons = Array.from(document.querySelectorAll('button, [role="button"], [role="tab"]'))
                .filter((node) => isVisible(node))
                .map((node) => summarize(node))
                .filter((item) => /participant|people|roster|katılımc|kisi|kişi/.test(lower([item.aria_label, item.title, item.text, item.data_tid].join(' '))))
                .slice(0, 20);
              const visiblePanels = Array.from(document.querySelectorAll('[role="dialog"], aside, [role="complementary"], section, div'))
                .filter((node) => isVisible(node))
                .map((node) => summarize(node))
                .filter((item) => /participant|people|roster|katılımc|kisi|kişi/.test(lower([item.aria_label, item.title, item.text, item.data_tid].join(' '))))
                .slice(0, 20);
              let rowCount = 0;
              for (const selector of config?.row_selectors || []) {
                try {
                  rowCount += document.querySelectorAll(selector).length;
                } catch (_err) {
                  continue;
                }
              }
              let tileCount = 0;
              for (const selector of config?.tile_selectors || []) {
                try {
                  tileCount += document.querySelectorAll(selector).length;
                } catch (_err) {
                  continue;
                }
              }
              let videoSurfaceCount = 0;
              for (const selector of config?.video_surface_selectors || []) {
                try {
                  videoSurfaceCount += document.querySelectorAll(selector).length;
                } catch (_err) {
                  continue;
                }
              }
              let voiceSignalCount = 0;
              for (const selector of config?.voice_signal_selectors || []) {
                try {
                  voiceSignalCount += document.querySelectorAll(selector).length;
                } catch (_err) {
                  continue;
                }
              }
              return {
                observer_installed: Boolean(window.__noteraParticipantRegistry?.observer),
                snapshot_count: (window.__noteraParticipantRegistry?.lastSnapshot || []).length,
                visible_buttons: visibleButtons,
                visible_panels: visiblePanels,
                row_selector_match_count: rowCount,
                tile_selector_match_count: tileCount,
                video_surface_match_count: videoSurfaceCount,
                voice_signal_match_count: voiceSignalCount,
              };
            }""",
            PARTICIPANT_REGISTRY_CONFIG,
        )
    except Exception as e:
        return {"error": str(e)}


def sync_speaker_activity(meeting_id, participant_items, active_speaker_state, current_offset_ms):
    seen_keys = set()
    for item in participant_items:
        participant_key = extract_participant_key(item)
        display_name = item.get("display_name") or "Unknown"
        normalized_name = normalize_participant_name(display_name).casefold()
        participant_id = upsert_meeting_participant(
            meeting_id,
            participant_key,
            display_name,
            platform_identity=item.get("platform_identity") or None,
            role=item.get("role") or None,
            is_bot=bool(item.get("is_bot")),
            join_state=item.get("join_state") or "present",
        )
        if participant_id <= 0:
            continue
        seen_keys.add(participant_key)
        if item.get("dom_key"):
            register_identity_evidence(
                meeting_id,
                participant_id,
                "dom_key",
                item["dom_key"],
                confidence=0.95,
                payload={"participant_key": participant_key},
            )
        if item.get("platform_identity"):
            register_identity_evidence(
                meeting_id,
                participant_id,
                "platform_identity",
                item["platform_identity"],
                confidence=0.98,
                payload={"participant_key": participant_key},
            )

        active_entry = active_speaker_state.get(participant_key)
        if active_entry and active_entry.get("normalized_name") not in {"", normalized_name}:
            append_speaker_activity_interval(
                meeting_id,
                active_entry["participant_id"],
                active_entry["start_offset_ms"],
                current_offset_ms,
                source="roster_speaking_indicator",
                confidence=0.75,
                metadata={
                    "participant_key": participant_key,
                    "closed_reason": "identity_shift",
                    "previous_display_name": active_entry.get("display_name"),
                    "current_display_name": display_name,
                },
            )
            del active_speaker_state[participant_key]
            active_entry = None
        if item.get("is_speaking"):
            if active_entry is None:
                active_speaker_state[participant_key] = {
                    "participant_id": participant_id,
                    "start_offset_ms": current_offset_ms,
                    "last_seen_offset_ms": current_offset_ms,
                    "display_name": display_name,
                    "normalized_name": normalized_name,
                }
            else:
                active_entry["participant_id"] = participant_id
                active_entry["last_seen_offset_ms"] = current_offset_ms
                active_entry["display_name"] = display_name
                active_entry["normalized_name"] = normalized_name
        elif active_entry is not None:
            append_speaker_activity_interval(
                meeting_id,
                active_entry["participant_id"],
                active_entry["start_offset_ms"],
                current_offset_ms,
                source="roster_speaking_indicator",
                confidence=0.91,
                metadata={"participant_key": participant_key},
            )
            del active_speaker_state[participant_key]

    stale_keys = []
    for participant_key, entry in active_speaker_state.items():
        if participant_key in seen_keys:
            continue
        if current_offset_ms - entry.get("last_seen_offset_ms", current_offset_ms) < 3000:
            continue
        append_speaker_activity_interval(
            meeting_id,
            entry["participant_id"],
            entry["start_offset_ms"],
            current_offset_ms,
            source="roster_speaking_indicator",
            confidence=0.75,
            metadata={"participant_key": participant_key, "closed_reason": "participant_missing"},
        )
        stale_keys.append(participant_key)
    for participant_key in stale_keys:
        active_speaker_state.pop(participant_key, None)


def flush_speaker_activity(meeting_id, active_speaker_state, current_offset_ms):
    for participant_key, entry in list(active_speaker_state.items()):
        append_speaker_activity_interval(
            meeting_id,
            entry["participant_id"],
            entry["start_offset_ms"],
            current_offset_ms,
            source="roster_speaking_indicator",
            confidence=0.7,
            metadata={"participant_key": participant_key, "closed_reason": "meeting_end"},
        )
        active_speaker_state.pop(participant_key, None)


async def open_language_and_speech_menu(page):
    """Open the Language and speech submenu when Teams exposes captions there."""
    language_and_speech_selectors = [
        "[role='menuitem']:has-text('Language and speech')",
        "button:has-text('Language and speech')",
        "[role='button']:has-text('Language and speech')",
        "[aria-label='Language and speech']",
        "[title='Language and speech']",
    ]

    return await click_first_visible_selector(
        page,
        language_and_speech_selectors,
        "Language and speech entry",
        wait_after=1,
        dump_filename="language_and_speech_menu_dom.html",
    )


async def select_computer_audio(page):
    """Prefer Teams computer audio mode so remote meeting audio is available."""
    selectors = [
        "button:has-text('Computer audio')",
        "[role='button']:has-text('Computer audio')",
        "[role='radio']:has-text('Computer audio')",
        "[data-tid='prejoin-join-audio-option-computer-audio']",
        "[data-tid='prejoin-audio-option-computer-audio']",
        "button:has-text('Audio')",
    ]
    if await click_first_visible_selector(
        page,
        selectors,
        "Computer audio",
        wait_after=1,
        dump_filename="computer_audio_dom.html",
    ):
        return True

    try:
        return await page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll('button, [role="button"], [role="radio"]'));
            const target = candidates.find((element) => {
                const text = (element.innerText || element.getAttribute('aria-label') || '').trim();
                return /computer audio/i.test(text);
            });
            if (!target) return false;
            target.click();
            return true;
        }""")
    except Exception as e:
        logger.debug("Could not select computer audio via page evaluation: %s", e)
        return False


async def launch_teams_browser(playwright):
    launch_options = {
        "headless": True,
        "args": ["--autoplay-policy=no-user-gesture-required"],
    }
    try:
        browser = await playwright.chromium.launch(
            channel="msedge",
            **launch_options,
        )
        logger.info("Launched Teams bot with Playwright msedge channel.")
        return browser
    except Exception as e:
        logger.warning("msedge channel launch failed, falling back to bundled chromium: %s", e)
        return await playwright.chromium.launch(**launch_options)


async def open_meeting_entry(page, meeting_url):
    meeting_by_id = parse_join_with_id_target(meeting_url)
    if not meeting_by_id:
        log_event(
            logger,
            logging.INFO,
            "meeting.navigation.started",
            "Navigating to Teams meeting URL",
            navigation_mode="direct_link",
        )
        await page.goto(meeting_url)
        return

    meeting_id, passcode = meeting_by_id
    logger.info("Navigating to Teams join-by-ID page for meeting %s", meeting_id)
    await page.goto(TEAMS_JOIN_WITH_ID_PAGE_URL)

    meeting_id_input = None
    for locator in [
        page.get_by_label(re.compile("meeting id", re.IGNORECASE)).first,
        page.get_by_placeholder(re.compile("meeting id", re.IGNORECASE)).first,
        page.locator("input[name*='meeting']").first,
    ]:
        try:
            await locator.wait_for(state="visible", timeout=5000)
            meeting_id_input = locator
            break
        except Exception:
            continue
    if meeting_id_input is None:
        raise RuntimeError("Teams join-by-ID formunda meeting ID alani bulunamadi.")
    await meeting_id_input.fill(meeting_id)

    passcode_input = None
    for locator in [
        page.get_by_label(re.compile("passcode", re.IGNORECASE)).first,
        page.get_by_placeholder(re.compile("passcode", re.IGNORECASE)).first,
        page.locator("input[name*='passcode']").first,
        page.locator("input[type='password']").first,
    ]:
        try:
            await locator.wait_for(state="visible", timeout=5000)
            passcode_input = locator
            break
        except Exception:
            continue
    if passcode_input is None:
        raise RuntimeError("Teams join-by-ID formunda passcode alani bulunamadi.")
    await passcode_input.fill(passcode)

    join_button = page.get_by_role("button", name=re.compile("join a meeting", re.IGNORECASE)).first
    await join_button.wait_for(state="visible", timeout=10000)
    await join_button.click(timeout=10000)
    await page.wait_for_load_state("networkidle")


async def has_caption_surface(page):
    """Returns True if Teams is currently rendering live captions in this page."""
    try:
        return await page.evaluate("""() => {
            const selectors = [
                '[data-tid="closed-caption-renderer-wrapper"]',
                '[data-tid="closed-caption-text"]',
                '[role="log"]',
            ];
            return selectors.some((selector) => document.querySelector(selector));
        }""")
    except Exception as e:
        logger.debug(f"Caption surface check failed: {e}")
        return False

def is_stop_requested(meeting_id):
    conn = sqlite3.connect(get_db_path())
    try:
        row = conn.execute(
            "SELECT stop_requested FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        return bool(row[0]) if row is not None and row[0] is not None else False
    except Exception as e:
        logger.debug("Could not resolve stop flag from database for meeting %s: %s", meeting_id, e)
        return False
    finally:
        conn.close()


def is_audio_recording_enabled(meeting_id):
    conn = sqlite3.connect(get_db_path())
    try:
        row = conn.execute(
            "SELECT audio_recording_enabled FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        if row is None:
            return True
        return bool(row[0])
    except Exception as e:
        logger.warning("Could not resolve audio recording flag for meeting %s: %s", meeting_id, e)
        return True
    finally:
        conn.close()


def update_meeting_fields(meeting_id, **fields):
    if not fields:
        return

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        columns = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [meeting_id]
        cursor.execute(
            f"UPDATE meeting SET {columns} WHERE id = ?",
            values,
        )
        conn.commit()
    except Exception as e:
        logger.error("Failed updating meeting %s fields %s: %s", meeting_id, list(fields.keys()), e)
    finally:
        conn.close()


def update_audio_status(meeting_id, status, error=None):
    update_meeting_fields(
        meeting_id,
        audio_status=status,
        audio_error=error,
    )


def register_audio_asset(
    meeting_id,
    master_audio_path,
    fmt,
    status,
    duration_ms=None,
    pcm_audio_path=None,
    postprocess_version=None,
):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        existing = cursor.execute(
            "SELECT id FROM meetingaudioasset WHERE meeting_id = ? ORDER BY id DESC LIMIT 1",
            (meeting_id,),
        ).fetchone()
        if existing:
            cursor.execute(
                """
                UPDATE meetingaudioasset
                SET master_audio_path = ?, pcm_audio_path = ?, format = ?, duration_ms = ?, status = ?, postprocess_version = ?
                WHERE id = ?
                """,
                (
                    master_audio_path,
                    pcm_audio_path,
                    fmt,
                    duration_ms,
                    status,
                    postprocess_version,
                    existing[0],
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO meetingaudioasset (
                    meeting_id,
                    master_audio_path,
                    pcm_audio_path,
                    format,
                    duration_ms,
                    status,
                    postprocess_version,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    meeting_id,
                    master_audio_path,
                    pcm_audio_path,
                    fmt,
                    duration_ms,
                    status,
                    postprocess_version,
                ),
            )
        conn.commit()
    except Exception as e:
        logger.error("Failed registering audio asset for meeting %s: %s", meeting_id, e)
    finally:
        conn.close()


def is_bot_participant_name(value):
    normalized = normalize_participant_name(value).casefold()
    return normalized in {"transcription bot", "notera bot", "bot"} or normalized.endswith(" bot")


def is_generic_participant_identity(value):
    normalized = normalize_caption_text(value).casefold()
    if not normalized:
        return False
    generic_fragments = (
        "participant-avatar",
        "avatar-image-container",
        "avatar",
        "image-container",
        "voice-level-stream-outline",
        "ai-interpreter-outline",
        "outline",
    )
    return any(fragment in normalized for fragment in generic_fragments)


def is_unstable_stable_key(value):
    normalized = normalize_caption_text(value).casefold()
    if not normalized:
        return False
    if normalized.isdigit():
        return True
    if re.fullmatch(r"r\d+", normalized):
        return True
    return bool(
        re.fullmatch(
            r"(?:row|item|persona|participant|member|entry|tile|video)[-_:\s]?\d+",
            normalized,
        )
    )


def participant_identity_conflicts(existing_normalized_name, existing_platform_identity, new_normalized_name, new_platform_identity):
    existing_name = normalize_participant_name(existing_normalized_name).casefold()
    new_name = normalize_participant_name(new_normalized_name).casefold()
    if not existing_name or not new_name or existing_name == new_name:
        return False
    existing_platform = normalize_caption_text(existing_platform_identity)
    new_platform = normalize_caption_text(new_platform_identity)
    if existing_platform and new_platform and existing_platform == new_platform:
        return False
    return True


def disambiguated_participant_key(participant_key, normalized_name):
    digest = hashlib.sha1((normalized_name or "unknown").encode("utf-8")).hexdigest()[:10]
    return f"{participant_key}|name:{digest}"[:255]


def split_participant_display_name(display_name, role_hint=None):
    cleaned_name = normalize_participant_name(display_name)
    cleaned_role = normalize_caption_text(role_hint)
    if not cleaned_name:
        return "", cleaned_role or None

    suffix_match = re.match(
        r"^(?P<name>.+?)\s*\((?P<role>guest|organizer|presenter|attendee)\)$",
        cleaned_name,
        re.IGNORECASE,
    )
    if suffix_match:
        return (
            normalize_participant_name(suffix_match.group("name")),
            normalize_caption_text(suffix_match.group("role")),
        )

    inline_match = re.match(
        r"^(?P<name>.+?)\s+(?P<role>Organizer|Guest|Presenter|Attendee)$",
        cleaned_name,
        re.IGNORECASE,
    )
    if inline_match:
        return (
            normalize_participant_name(inline_match.group("name")),
            normalize_caption_text(inline_match.group("role")),
        )

    return cleaned_name, cleaned_role or None


def participant_snapshot_source_rank(source_kind):
    normalized = normalize_caption_text(source_kind).casefold()
    if normalized == "voice_signal":
        return 5
    if normalized == "participant_panel":
        return 4
    if normalized == "video_surface":
        return 3
    if normalized == "video_tile":
        return 2
    return 1


def participant_snapshot_item_score(item):
    return (
        int(bool(item.get("platform_identity"))) * 40
        + int(bool(item.get("stable_key"))) * 20
        + int(bool(item.get("role"))) * 10
        + int(bool(item.get("is_speaking"))) * 12
        + participant_snapshot_source_rank(item.get("source_kind"))
    )


def merge_participant_snapshot_items(preferred, other):
    merged = dict(preferred)
    for field in ("stable_key", "stable_key_source", "platform_identity", "role", "dom_key"):
        if not merged.get(field) and other.get(field):
            merged[field] = other[field]
    merged["join_state"] = "left" if merged.get("join_state") == "left" and other.get("join_state") == "left" else "present"
    merged["is_speaking"] = bool(preferred.get("is_speaking")) or bool(other.get("is_speaking"))
    merged["is_muted"] = bool(preferred.get("is_muted")) or bool(other.get("is_muted"))
    merged["is_bot"] = bool(preferred.get("is_bot")) or bool(other.get("is_bot"))
    if bool(other.get("is_speaking")) and not bool(preferred.get("is_speaking")):
        merged["source_kind"] = other.get("source_kind") or merged.get("source_kind")
    elif participant_snapshot_source_rank(other.get("source_kind")) > participant_snapshot_source_rank(merged.get("source_kind")):
        merged["source_kind"] = other.get("source_kind") or merged.get("source_kind")
    return merged


def extract_participant_key(item):
    stable_key = normalize_caption_text(item.get("stable_key"))
    platform_identity = normalize_caption_text(item.get("platform_identity"))
    if is_generic_participant_identity(stable_key):
        stable_key = ""
    if is_unstable_stable_key(stable_key):
        stable_key = ""
    if is_generic_participant_identity(platform_identity):
        platform_identity = ""
    display_name = normalize_participant_name(item.get("display_name") or item.get("name") or "Unknown")
    if platform_identity:
        return f"teams-platform:{platform_identity}"
    if stable_key:
        return f"teams-roster:{stable_key}"
    bucket = int(datetime.utcnow().timestamp() // 900)
    return f"teams-name:{display_name.casefold()}:{bucket}"


def participant_key_rank(participant_key):
    normalized = normalize_caption_text(participant_key).casefold()
    if normalized.startswith("teams-platform:"):
        return 3
    if normalized.startswith("teams-roster:"):
        return 2
    if normalized.startswith("teams-name:"):
        return 1
    return 0


def upsert_meeting_participant(
    meeting_id,
    participant_key,
    display_name,
    platform_identity=None,
    role=None,
    is_bot=False,
    join_state="present",
):
    cleaned_display_name = normalize_participant_name(display_name) or "Unknown"
    normalized_name = cleaned_display_name.casefold()
    now_iso = datetime.utcnow().isoformat()
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        existing = cursor.execute(
            """
            SELECT id, participant_key, normalized_name, platform_identity
            FROM meetingparticipant
            WHERE meeting_id = ? AND participant_key = ?
            LIMIT 1
            """,
            (meeting_id, participant_key),
        ).fetchone()
        if existing and participant_identity_conflicts(existing[2], existing[3], normalized_name, platform_identity):
            participant_key = disambiguated_participant_key(participant_key, normalized_name)
            existing = cursor.execute(
                """
                SELECT id, participant_key, normalized_name, platform_identity
                FROM meetingparticipant
                WHERE meeting_id = ? AND participant_key = ?
                LIMIT 1
                """,
                (meeting_id, participant_key),
            ).fetchone()
        if existing is None and platform_identity:
            platform_matches = cursor.execute(
                """
                SELECT id, participant_key, normalized_name, platform_identity
                FROM meetingparticipant
                WHERE meeting_id = ?
                  AND platform_identity = ?
                  AND is_bot = ?
                  AND join_state != 'merged'
                  AND merged_into_participant_id IS NULL
                ORDER BY id
                """,
                (meeting_id, platform_identity, 1 if is_bot else 0),
            ).fetchall()
            if len(platform_matches) == 1:
                existing = platform_matches[0]
        if existing is None:
            name_matches = cursor.execute(
                """
                SELECT id, participant_key, normalized_name, platform_identity
                FROM meetingparticipant
                WHERE meeting_id = ?
                  AND normalized_name = ?
                  AND is_bot = ?
                  AND join_state != 'merged'
                  AND merged_into_participant_id IS NULL
                ORDER BY id
                """,
                (meeting_id, normalized_name, 1 if is_bot else 0),
            ).fetchall()
            if len(name_matches) == 1:
                existing = name_matches[0]
        if existing:
            resolved_participant_key = existing[1]
            if participant_key_rank(participant_key) > participant_key_rank(existing[1]):
                resolved_participant_key = participant_key
            cursor.execute(
                """
                UPDATE meetingparticipant
                SET participant_key = ?,
                    platform_identity = COALESCE(?, platform_identity),
                    display_name = ?,
                    normalized_name = ?,
                    role = COALESCE(?, role),
                    is_bot = COALESCE(?, is_bot),
                    join_state = ?,
                    last_seen_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    resolved_participant_key,
                    platform_identity,
                    cleaned_display_name,
                    normalized_name,
                    role,
                    1 if is_bot else 0,
                    join_state,
                    now_iso,
                    now_iso,
                    existing[0],
                ),
            )
            conn.commit()
            return int(existing[0])

        cursor.execute(
            """
            INSERT INTO meetingparticipant (
                meeting_id,
                participant_key,
                platform_identity,
                display_name,
                normalized_name,
                role,
                is_bot,
                join_state,
                first_seen_at,
                last_seen_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                participant_key,
                platform_identity,
                cleaned_display_name,
                normalized_name,
                role,
                1 if is_bot else 0,
                join_state,
                now_iso,
                now_iso,
                now_iso,
                now_iso,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    except Exception as e:
        logger.error("Failed upserting meeting participant for meeting %s: %s", meeting_id, e)
        return 0
    finally:
        conn.close()


def register_identity_evidence(
    meeting_id,
    participant_id,
    evidence_type,
    evidence_value,
    confidence=0.0,
    audio_source_id=None,
    payload=None,
):
    if participant_id <= 0 or not evidence_value:
        return
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO identityevidence (
                meeting_id,
                participant_id,
                audio_source_id,
                evidence_type,
                evidence_value,
                confidence,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                participant_id,
                audio_source_id,
                evidence_type,
                evidence_value,
                confidence,
                datetime.utcnow().isoformat(),
                json.dumps(payload, ensure_ascii=False) if payload is not None else None,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.debug("Failed recording identity evidence for meeting %s: %s", meeting_id, e)
    finally:
        conn.close()


def register_audio_source(
    meeting_id,
    source_key,
    source_kind,
    track_id=None,
    stream_id=None,
    file_path=None,
    fmt=None,
    status="pending",
):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now_iso = datetime.utcnow().isoformat()
    try:
        existing = cursor.execute(
            """
            SELECT id
            FROM audiosource
            WHERE meeting_id = ? AND source_key = ?
            LIMIT 1
            """,
            (meeting_id, source_key),
        ).fetchone()
        if existing:
            cursor.execute(
                """
                UPDATE audiosource
                SET track_id = COALESCE(?, track_id),
                    stream_id = COALESCE(?, stream_id),
                    file_path = COALESCE(?, file_path),
                    format = COALESCE(?, format),
                    status = COALESCE(?, status),
                    last_seen_at = ?,
                    sample_rate_hz = COALESCE(sample_rate_hz, 16000),
                    channel_count = COALESCE(channel_count, 1)
                WHERE id = ?
                """,
                (
                    track_id,
                    stream_id,
                    file_path,
                    fmt,
                    status,
                    now_iso,
                    existing[0],
                ),
            )
            conn.commit()
            return int(existing[0])
        cursor.execute(
            """
            INSERT INTO audiosource (
                meeting_id,
                source_key,
                source_kind,
                track_id,
                stream_id,
                file_path,
                format,
                sample_rate_hz,
                channel_count,
                first_seen_at,
                last_seen_at,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                source_key,
                source_kind,
                track_id,
                stream_id,
                file_path,
                fmt,
                16000 if fmt == "wav" else None,
                1 if fmt == "wav" else None,
                now_iso,
                now_iso,
                status,
                now_iso,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    except Exception as e:
        logger.error("Failed registering audio source for meeting %s: %s", meeting_id, e)
        return 0
    finally:
        conn.close()


def write_participant_snapshot_debug(meeting_id, observed_at, participant_items, debug_state=None):
    if not DEBUG_ARTIFACTS_ENABLED:
        return
    try:
        payload = {
            "meeting_id": meeting_id,
            "observed_at": observed_at.isoformat() if observed_at else None,
            "participant_count": len(participant_items or []),
            "items": participant_items or [],
            "debug_state": debug_state or {},
        }
        path = get_bot_debug_path(f"participant_snapshot_meeting_{meeting_id}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.debug("Could not write participant snapshot debug for meeting %s: %s", meeting_id, e)


def finalize_audio_sources(meeting_id, status="ready"):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now_iso = datetime.utcnow().isoformat()
    try:
        cursor.execute(
            """
            UPDATE audiosource
            SET status = ?,
                last_seen_at = COALESCE(last_seen_at, ?)
            WHERE meeting_id = ?
            """,
            (status, now_iso, meeting_id),
        )
        conn.commit()
    except Exception as e:
        logger.error("Failed finalizing audio sources for meeting %s: %s", meeting_id, e)
    finally:
        conn.close()


def append_speaker_activity_interval(
    meeting_id,
    participant_id,
    start_offset_ms,
    end_offset_ms,
    source="roster_speaking_indicator",
    confidence=0.0,
    metadata=None,
):
    if participant_id <= 0 or end_offset_ms <= start_offset_ms:
        return
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO speakeractivityevent (
                meeting_id,
                participant_id,
                start_offset_ms,
                end_offset_ms,
                source,
                confidence,
                metadata_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                participant_id,
                int(start_offset_ms),
                int(end_offset_ms),
                source,
                confidence,
                json.dumps(metadata, ensure_ascii=False) if metadata is not None else None,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    except Exception as e:
        logger.error("Failed saving speaker activity interval for meeting %s: %s", meeting_id, e)
    finally:
        conn.close()


def save_caption_event(meeting_id, speaker, text, sequence_no, observed_at, slot_index, revision_no):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    observed_value = (observed_at or datetime.utcnow()).isoformat()
    try:
        duplicate_reason = _caption_duplicate_reason(
            cursor,
            meeting_id,
            speaker,
            text,
            observed_at,
            slot_index,
            revision_no,
        )
        if duplicate_reason is not None:
            log_event(
                logger,
                logging.INFO,
                "caption.duplicate_skipped",
                "Duplicate caption skipped",
                duplicate_reason=duplicate_reason,
                slot_index=slot_index,
                revision_no=revision_no,
                **caption_metrics(text),
            )
            return "skipped"

        cursor.execute(
            """
            INSERT INTO teamscaptionevent (
                meeting_id,
                sequence_no,
                speaker_name,
                text,
                observed_at,
                slot_index,
                revision_no
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                sequence_no,
                speaker,
                text,
                observed_value,
                slot_index,
                revision_no,
            ),
        )
        conn.commit()
        logger.info(
            "Caption event saved for meeting %s sequence=%s slot=%s revision=%s",
            meeting_id,
            sequence_no,
            slot_index,
            revision_no,
        )
        return "inserted"
    except Exception as e:
        logger.error("Failed to save caption event: %s", e)
        return "failed"
    finally:
        conn.close()


def probe_audio_duration_ms(audio_path):
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nk=1:nw=1",
                str(audio_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return None
        seconds = float((result.stdout or "").strip())
        if seconds <= 0:
            return None
        return int(seconds * 1000)
    except Exception:
        return None


def trigger_postprocess_worker(meeting_id):
    if os.getenv("NOTERA_DISABLE_INTERNAL_POSTPROCESS_TRIGGER") == "1":
        logger.info(
            "Skipping internal post-process trigger for meeting %s because external supervisor is enabled.",
            meeting_id,
        )
        return
    worker_path = REPO_ROOT / "backend" / "workers" / "postprocess_worker.py"
    try:
        update_meeting_fields(
            meeting_id,
            postprocess_status=POSTPROCESS_STATUS_QUEUED,
            postprocess_error=None,
            postprocess_progress_pct=None,
            postprocess_progress_note=None,
        )
        subprocess.Popen(
            [sys.executable, "-u", str(worker_path), str(meeting_id)],
            start_new_session=True,
        )
        logger.info("Started post-process worker for meeting %s", meeting_id)
    except Exception as e:
        logger.error("Failed starting post-process worker for meeting %s: %s", meeting_id, e)
        update_meeting_fields(
            meeting_id,
            postprocess_status="failed",
            postprocess_error=str(e),
            postprocess_progress_pct=None,
            postprocess_progress_note=None,
        )


class MeetingAudioChunkWriter:
    def __init__(self, meeting_id):
        self.meeting_id = int(meeting_id)
        self.chunk_index = 0
        self.chunk_paths = []
        self.mime_type = ""
        self.format = "webm"
        self.accept_writes = True
        self.chunk_dir = get_meeting_audio_chunks_dir(self.meeting_id)
        self.aggregate_path = get_meeting_audio_dir(self.meeting_id) / "recording.part"
        for stale_chunk in self.chunk_dir.glob("chunk_*"):
            stale_chunk.unlink(missing_ok=True)
        self.aggregate_path.unlink(missing_ok=True)

    def save_chunk(self, payload):
        if not self.accept_writes:
            return False

        base64_data = payload.get("base64") or ""
        mime_type = (payload.get("mimeType") or "").lower()
        if not base64_data:
            return False

        raw_bytes = base64.b64decode(base64_data)
        if not raw_bytes:
            return False

        self.chunk_index += 1
        self.mime_type = mime_type or self.mime_type
        self.format = self._format_from_mime(self.mime_type)
        chunk_path = self.chunk_dir / f"chunk_{self.chunk_index:05d}.{self.format}"
        chunk_path.write_bytes(raw_bytes)
        self.chunk_paths.append(chunk_path)
        with self.aggregate_path.open("ab") as aggregate_file:
            aggregate_file.write(raw_bytes)
        logger.info(
            "Saved audio chunk for meeting %s: part #%s (%s bytes)",
            self.meeting_id,
            self.chunk_index,
            len(raw_bytes),
        )
        return True

    def finalize(self):
        master_path = get_meeting_master_audio_path(self.meeting_id, self.format)
        pcm_path = get_meeting_pcm_audio_path(self.meeting_id)
        finalized = False
        used_aggregate_stream = False
        if self._should_prefer_aggregate_stream() and self.aggregate_path.exists() and self.aggregate_path.stat().st_size > 0:
            finalized = self._finalize_from_aggregate_stream(master_path)
            if not finalized:
                shutil.copy2(self.aggregate_path, master_path)
                finalized = True
            used_aggregate_stream = finalized
        elif self.chunk_paths:
            finalized = self._finalize_from_chunk_concat(master_path)
        elif self.aggregate_path.exists() and self.aggregate_path.stat().st_size > 0:
            shutil.copy2(self.aggregate_path, master_path)
            finalized = True

        if not finalized:
            raise RuntimeError("no audio chunks were captured")

        pcm_result = self._build_pcm_copy(master_path, pcm_path)
        if pcm_result.returncode != 0 and used_aggregate_stream and self.aggregate_path.exists():
            logger.warning(
                "Could not decode remuxed aggregate audio stream for meeting %s (ffmpeg_return_code=%s). Retrying from aggregate stream.",
                self.meeting_id,
                pcm_result.returncode,
            )
            pcm_result = self._build_pcm_copy(self.aggregate_path, pcm_path)

        if pcm_result.returncode != 0 and self.chunk_paths and not used_aggregate_stream:
            if self._finalize_from_chunk_concat(master_path):
                pcm_result = self._build_pcm_copy(master_path, pcm_path)

        if pcm_result.returncode != 0:
            logger.warning(
                "Could not create PCM audio copy for meeting %s (ffmpeg_return_code=%s)",
                self.meeting_id,
                pcm_result.returncode,
            )
        duration_ms = probe_audio_duration_ms(pcm_path) if pcm_path.exists() else None
        if duration_ms is None:
            duration_ms = probe_audio_duration_ms(master_path)
        log_event(
            logger,
            logging.INFO,
            "audio.finalized",
            "Meeting audio finalized",
            duration_ms=duration_ms,
            has_pcm_copy=pcm_path.exists(),
            format=self.format,
        )
        if master_path.exists() and (pcm_path.exists() or duration_ms is not None):
            self._cleanup_temporary_audio_parts()
        return master_path, pcm_path if pcm_path.exists() else None, self.format, duration_ms

    def stop_accepting_writes(self):
        self.accept_writes = False

    def _should_prefer_aggregate_stream(self):
        return self.format in {"webm", "ogg"}

    def _finalize_from_chunk_concat(self, master_path):
        if not self.chunk_paths:
            return False
        concat_manifest_path = self.chunk_dir / "concat_inputs.txt"
        concat_manifest_path.write_text(
            "".join(f"file '{self._escape_concat_path(path)}'\n" for path in self.chunk_paths),
            encoding="utf-8",
        )
        concat_copy_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_manifest_path),
                "-c",
                "copy",
                str(master_path),
            ],
            capture_output=True,
            text=True,
        )
        if concat_copy_result.returncode != 0:
            logger.warning(
                "Could not concat audio chunks with stream copy for meeting %s: %s",
                self.meeting_id,
                concat_copy_result.stderr.strip() or "ffmpeg concat copy failed",
            )
            concat_transcode_result = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    str(concat_manifest_path),
                    *self._master_transcode_args(),
                    str(master_path),
                ],
                capture_output=True,
                text=True,
            )
            if concat_transcode_result.returncode != 0:
                logger.warning(
                    "Could not concat audio chunks with transcode for meeting %s: %s",
                    self.meeting_id,
                    concat_transcode_result.stderr.strip() or "audio chunk concat failed",
                )
                return False
        return True

    def _finalize_from_aggregate_stream(self, master_path):
        remux_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(self.aggregate_path),
                "-c",
                "copy",
                str(master_path),
            ],
            capture_output=True,
            text=True,
        )
        if remux_result.returncode == 0:
            return True
        logger.warning(
            "Could not remux aggregate audio stream for meeting %s: %s",
            self.meeting_id,
            remux_result.stderr.strip() or "aggregate audio remux failed",
        )
        transcode_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(self.aggregate_path),
                *self._master_transcode_args(),
                str(master_path),
            ],
            capture_output=True,
            text=True,
        )
        if transcode_result.returncode != 0:
            logger.warning(
                "Could not transcode aggregate audio stream for meeting %s: %s",
                self.meeting_id,
                transcode_result.stderr.strip() or "aggregate audio transcode failed",
            )
            return False
        return True

    @staticmethod
    def _build_pcm_copy(master_path, pcm_path):
        return subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(master_path),
                "-ac",
                "1",
                "-ar",
                "16000",
                str(pcm_path),
            ],
            capture_output=True,
            text=True,
        )

    def _cleanup_temporary_audio_parts(self):
        if DEBUG_ARTIFACTS_ENABLED:
            return
        for stale_chunk in self.chunk_dir.glob("chunk_*"):
            stale_chunk.unlink(missing_ok=True)
        (self.chunk_dir / "concat_inputs.txt").unlink(missing_ok=True)
        self.aggregate_path.unlink(missing_ok=True)

    def _master_transcode_args(self):
        if self.format == "wav":
            return ["-vn", "-c:a", "pcm_s16le"]
        if self.format == "m4a":
            return ["-vn", "-c:a", "aac", "-b:a", "128k"]
        if self.format == "ogg":
            return ["-vn", "-c:a", "libopus"]
        return ["-vn", "-c:a", "libopus"]

    @staticmethod
    def _escape_concat_path(path):
        return str(path).replace("'", "'\\''")

    @staticmethod
    def _format_from_mime(mime_type):
        if "wav" in mime_type:
            return "wav"
        if "mp4" in mime_type or "m4a" in mime_type:
            return "m4a"
        if "ogg" in mime_type:
            return "ogg"
        return "webm"


async def try_enable_live_captions(page):
    """
    Opens Teams' More menu and enables live captions in the bot's own session.
    This is required for headless runs because captions are rendered per client.
    """
    if await has_caption_surface(page):
        return True

    logger.info("No live caption DOM detected yet. Trying to enable captions in the bot session...")

    if not await open_more_menu(page):
        return False

    if await open_language_and_speech_menu(page):
        logger.info("Opened Language and speech submenu before enabling live captions.")
    else:
        logger.info("Language and speech submenu was not visible. Falling back to direct captions entry.")

    caption_selectors = [
        "[role='menuitem']:has-text('Show live captions')",
        "button:has-text('Show live captions')",
        "[title='Show live captions']",
        "#closed-captions-button",
        "[role='menuitem'][id='closed-captions-button']",
        "[role='menuitem'][aria-label='Captions']",
    ]

    for selector in caption_selectors:
        try:
            item = page.locator(selector)
            count = await item.count()
            for idx in range(count):
                candidate = item.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue

                title = await candidate.get_attribute("title") or ""
                if "Hide live captions" in title:
                    logger.info("Live captions are already enabled in the bot session.")
                    await asyncio.sleep(1)
                    return await has_caption_surface(page)

                await candidate.click(force=True, timeout=5000)
                logger.info(f"Clicked live captions entry with selector: {selector}")
                await asyncio.sleep(2)
                await dump_dom(page, "live_captions_enabled_dom.html")
                return await has_caption_surface(page)
        except Exception as e:
            logger.debug(f"Failed clicking captions menu item with selector {selector}: {e}")

    logger.warning("Could not find a visible Captions menu item in the Teams More menu.")
    await dump_dom(page, "live_captions_entry_not_found_dom.html")
    return False


async def is_chat_compose_visible(page):
    compose_selectors = [
        "div[data-tid='ckeditor'][contenteditable='true']",
        "[data-tid='chat-pane-compose-message-footer']",
        "button[data-tid='newMessageCommands-send']",
    ]

    for selector in compose_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if await candidate.is_visible(timeout=500):
                    return True
        except Exception as e:
            logger.debug(f"Chat compose visibility check failed for {selector}: {e}")

    return False


async def ensure_chat_panel_open(page):
    if await is_chat_compose_visible(page):
        return True

    chat_button_selectors = [
        "button#chat-button",
        "button[aria-label='Chat']",
        "button:has-text('Chat')",
    ]

    for selector in chat_button_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info(f"Clicked chat button with selector: {selector}")
                await asyncio.sleep(2)
                if await is_chat_compose_visible(page):
                    return True
        except Exception as e:
            logger.debug(f"Failed opening chat panel with selector {selector}: {e}")

    logger.warning("Could not open the Teams chat panel.")
    return False


async def send_chat_message(page, message):
    """Send a one-time chat message to the Teams meeting chat."""
    if not await ensure_chat_panel_open(page):
        return False

    editor_selectors = [
        "div[data-tid='ckeditor'][contenteditable='true']",
        "[role='textbox'][contenteditable='true'][data-tid='ckeditor']",
    ]
    editor = None

    for selector in editor_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if await candidate.is_visible(timeout=1000):
                    editor = candidate
                    break
            if editor is not None:
                break
        except Exception as e:
            logger.debug(f"Failed locating chat editor with selector {selector}: {e}")

    if editor is None:
        logger.warning("Could not find a visible Teams chat editor.")
        return False

    try:
        await editor.click(timeout=5000)
        await editor.fill(message)
    except Exception as e:
        logger.warning(f"Could not fill Teams chat editor directly: {e}")
        try:
            await editor.click(timeout=5000)
            await page.keyboard.press("Meta+A")
            await page.keyboard.type(message)
        except Exception as keyboard_error:
            logger.error(f"Could not type the Teams chat announcement: {keyboard_error}")
            return False

    send_selectors = [
        "button[data-tid='newMessageCommands-send']",
        "button[name='send']",
        "button[title*='Send']",
    ]

    for selector in send_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info("Message sent to Teams chat: %s", message)
                return True
        except Exception as e:
            logger.debug(f"Failed clicking send button with selector {selector}: {e}")

    try:
        await editor.press("Meta+Enter")
        logger.info("Message sent to Teams chat via keyboard shortcut: %s", message)
        return True
    except Exception as e:
        logger.error(f"Could not send the Teams chat announcement: {e}")
        return False


async def send_chat_notice(page):
    """Send the initial bot notice to the Teams meeting chat."""
    return await send_chat_message(page, CHAT_NOTICE_MESSAGE)


async def send_chat_language_help_notice(page):
    """Send manual Teams caption language instructions to the meeting chat."""
    return await send_chat_message(page, CHAT_LANGUAGE_HELP_MESSAGE)


def normalize_chat_command_text(text):
    normalized = (
        (text or "")
        .casefold()
    )
    return " ".join(normalized.split())


def is_exit_command_message(text):
    return normalize_chat_command_text(text) == CHAT_EXIT_COMMAND


async def get_chat_messages(page):
    """Read currently visible Teams meeting chat messages."""
    if not await ensure_chat_panel_open(page):
        return []

    try:
        return await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('[data-tid="chat-pane-message"]'))
                .map((messageNode) => {
                    const item = messageNode.closest('[data-tid="chat-pane-item"]') || messageNode.parentElement;
                    const authorNode = item?.querySelector('[data-tid="message-author-name"]');
                    const contentNode = messageNode.querySelector('[id^="content-"]');
                    const mid = messageNode.getAttribute('data-mid') || messageNode.id || '';
                    const author = authorNode?.innerText?.trim() || '';
                    const text =
                        contentNode?.innerText?.trim() ||
                        contentNode?.getAttribute('aria-label')?.trim() ||
                        messageNode.getAttribute('aria-label')?.trim() ||
                        '';
                    return { mid, author, text };
                })
                .filter((message) => message.mid && message.text);
        }""")
    except Exception as e:
        logger.debug(f"Could not read Teams chat messages: {e}")
        return []


async def detect_exit_command(page, known_message_ids):
    """Detect a new chat command that instructs the bot to leave."""
    messages = await get_chat_messages(page)
    if not messages:
        return False, known_message_ids

    updated_ids = set(known_message_ids)
    for message in messages:
        mid = message.get("mid", "")
        author = (message.get("author") or "").strip()
        text = (message.get("text") or "").strip()

        if not mid:
            continue
        if mid in updated_ids:
            continue

        updated_ids.add(mid)

        if not text or author == "Transcription Bot":
            continue

        if is_exit_command_message(text):
            logger.info(
                "Chat exit command detected from '%s': %s",
                author or "Unknown",
                text,
            )
            return True, updated_ids

    return False, updated_ids


async def leave_meeting_via_ui(page):
    """Try to leave the Teams meeting before closing the browser."""
    leave_selectors = [
        "button#hangup-button",
        "[data-tid='call-end-button']",
        "button[title*='Leave']",
        "button[aria-label*='Leave']",
        "button[title*='Ayrıl']",
        "button[aria-label*='Ayrıl']",
        "button:has-text('Leave')",
        "button:has-text('Ayrıl')",
        "button:has-text('Hang up')",
    ]

    for selector in leave_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                candidate = locator.nth(idx)
                if not await candidate.is_visible(timeout=1000):
                    continue
                await candidate.click(force=True, timeout=5000)
                logger.info(f"Clicked leave button with selector: {selector}")
                await asyncio.sleep(1)
                return True
        except Exception as e:
            logger.debug(f"Failed clicking leave button with selector {selector}: {e}")

    try:
        clicked = await page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll('button, [role="button"]'));
            const target = candidates.find((element) => {
                const text = (
                    element.innerText ||
                    element.getAttribute('aria-label') ||
                    element.getAttribute('title') ||
                    ''
                ).trim();
                return /leave|hang up|ayrıl/i.test(text);
            });
            if (!target) return false;
            target.click();
            return true;
        }""")
        if clicked:
            logger.info("Clicked leave button via DOM text fallback.")
            await asyncio.sleep(1)
            return True
    except Exception as e:
        logger.debug("Failed leave button DOM fallback: %s", e)

    logger.warning("Could not find a visible Leave button before shutdown.")
    return False


def get_live_meeting_screenshot_path(meeting_id):
    """Return the per-user, per-meeting asset path for live screenshots."""
    db_path = get_db_path()
    screenshot_root = os.getenv("NOTERA_LIVE_PREVIEW_ROOT")
    if screenshot_root:
        screenshot_dir = os.path.abspath(screenshot_root)
    else:
        screenshot_dir = os.path.abspath(os.path.join(REPO_ROOT, "data", "live_previews"))
    os.makedirs(screenshot_dir, exist_ok=True)

    user_id = "unknown"
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT user_id FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        if row and row[0] is not None:
            user_id = str(row[0])
    except Exception as e:
        logger.debug(f"Could not resolve meeting owner for screenshot path: {e}")
    finally:
        conn.close()

    return os.path.join(screenshot_dir, f"user_{user_id}_meeting_{meeting_id}.png")


def delete_live_meeting_screenshot(screenshot_path):
    """Delete a meeting-scoped screenshot file if it exists."""
    if not screenshot_path:
        return
    try:
        os.remove(screenshot_path)
        logger.info("Deleted live meeting screenshot asset.")
    except FileNotFoundError:
        return
    except Exception as e:
        logger.debug("Could not delete live meeting screenshot asset: %s", e)


async def take_periodic_screenshot(page, stop_event, screenshot_path):
    """Takes a screenshot every 10 seconds until stop_event is set."""
    while not stop_event.is_set():
        try:
            await page.screenshot(path=screenshot_path)
            logger.info("Periodic live preview screenshot updated.")
        except Exception as e:
            logger.error(f"Failed to take periodic screenshot: {e}")
        await asyncio.sleep(10)

def get_bot_debug_path(filename):
    """Return an absolute path inside the bot workspace for debug artifacts."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


def build_timestamped_debug_filename(filename):
    """Create a timestamped sibling filename for archived debug artifacts."""
    stem, ext = os.path.splitext(filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return f"{stem}_{timestamp}{ext}"


async def collect_debug_summary(page):
    """Collect a compact, structured summary of the current Teams UI state."""
    try:
        return await page.evaluate(
            """() => {
                const normalize = (value) =>
                    (value || "").replace(/\\s+/g, " ").trim();
                const isVisible = (el) => {
                    if (!el) {
                        return false;
                    }
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return (
                        style.display !== "none" &&
                        style.visibility !== "hidden" &&
                        rect.width > 0 &&
                        rect.height > 0
                    );
                };
                const collectTexts = (selector, limit = 30) => {
                    const values = [];
                    for (const el of Array.from(document.querySelectorAll(selector))) {
                        if (!isVisible(el)) {
                            continue;
                        }
                        const value = normalize(
                            el.innerText ||
                            el.getAttribute("aria-label") ||
                            el.getAttribute("title") ||
                            ""
                        );
                        if (!value || values.includes(value)) {
                            continue;
                        }
                        values.push(value);
                        if (values.length >= limit) {
                            break;
                        }
                    }
                    return values;
                };

                const bodyText = normalize(document.body?.innerText || "");
                const activeElement = document.activeElement;
                const activeText = normalize(
                    activeElement?.innerText ||
                    activeElement?.getAttribute?.("aria-label") ||
                    activeElement?.getAttribute?.("title") ||
                    activeElement?.id ||
                    activeElement?.tagName ||
                    ""
                );

                return {
                    url: window.location.href,
                    title: document.title,
                    active_element: activeText,
                    phrases: {
                        language_and_speech: bodyText.includes("Language and speech"),
                        show_live_captions: bodyText.includes("Show live captions"),
                        language_settings: bodyText.includes("Language settings"),
                        spoken_language_in_meeting: bodyText.includes("Spoken language in this meeting"),
                        caption_language: bodyText.includes("Caption language"),
                        translate_hint: bodyText.includes("Translate your transcript and captions to this language."),
                        confirmation_dialog: bodyText.includes("Is this the language that everyone is speaking?"),
                        update_button: bodyText.includes("Update"),
                    },
                    visible: {
                        headings: collectTexts("h1, h2, h3, [data-tid='right-side-panel-header-title']", 20),
                        menuitems: collectTexts("[role='menuitem']", 40),
                        buttons: collectTexts("button", 50),
                        options: collectTexts("[role='option'], [role='menuitemradio']", 40),
                        comboboxes: collectTexts("[role='combobox']", 20),
                    },
                    data_tids: Array.from(
                        new Set(
                            Array.from(document.querySelectorAll("[data-tid]"))
                                .map((el) => el.getAttribute("data-tid"))
                                .filter(Boolean)
                        )
                    ).slice(0, 150),
                    body_text_excerpt: bodyText.slice(0, 4000),
                };
            }"""
        )
    except Exception as e:
        return {"summary_error": str(e)}


async def dump_dom(page, filename="debug_dom.html"):
    """Dump HTML plus screenshot and UI summary for debugging."""
    if not DEBUG_ARTIFACTS_ENABLED:
        return False
    try:
        content = await page.content()
        screenshot_bytes = await page.screenshot(type="png")
        summary = await collect_debug_summary(page)
        summary["captured_at"] = datetime.now().isoformat()

        archive_filename = build_timestamped_debug_filename(filename)
        targets = [filename, archive_filename]

        written_paths = []
        for target in targets:
            html_path = get_bot_debug_path(target)
            stem, _ = os.path.splitext(html_path)
            png_path = f"{stem}.png"
            json_path = f"{stem}.json"

            with open(html_path, "w", encoding="utf-8") as f:
                f.write(content)
            with open(png_path, "wb") as f:
                f.write(screenshot_bytes)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)

            written_paths.append((html_path, png_path, json_path))

        latest_html, latest_png, latest_json = written_paths[0]
        logger.info("Debug artifacts written for bot session inspection.")
        return True
    except Exception as e:
        logger.error(f"Failed to dump debug artifacts: {e}")
        return False


async def install_teams_audio_hook(context):
    audio_attr = json.dumps(REMOTE_AUDIO_ATTRIBUTE)
    await context.add_init_script(
        f"""
        (() => {{
          if (window.__noteraTeamsAudioHookInstalled) return;
          const audioAttr = {audio_attr};
          const remoteAudioEntries = new Map();
          let hiddenContainer = null;

          const ensureContainer = () => {{
            if (hiddenContainer && document.body?.contains(hiddenContainer)) {{
              return hiddenContainer;
            }}
            hiddenContainer = document.createElement('div');
            hiddenContainer.setAttribute('data-notera-remote-audio-container', 'true');
            hiddenContainer.style.cssText = 'position:fixed;left:-9999px;top:-9999px;width:1px;height:1px;opacity:0;pointer-events:none;';
            (document.body || document.documentElement).appendChild(hiddenContainer);
            return hiddenContainer;
          }};

          const connectToRecorderIfReady = (entry) => {{
            const controller = window.__noteraRecorderController;
            if (controller && typeof controller.connectEntry === 'function') {{
              controller.connectEntry(entry);
            }}
          }};

          const attachAudioTrack = (track) => {{
            try {{
              if (!track || track.kind !== 'audio') return;
              const trackId = track.id || `audio-${{Date.now()}}-${{Math.random().toString(16).slice(2)}}`;
              if (remoteAudioEntries.has(trackId)) return;

              const stream = new MediaStream([track]);
              const audioEl = document.createElement('audio');
              audioEl.autoplay = true;
              audioEl.playsInline = true;
              audioEl.controls = false;
              audioEl.muted = true;
              audioEl.srcObject = stream;
              audioEl.setAttribute(audioAttr, 'true');
              audioEl.dataset.noteraRemoteTrackId = trackId;
              audioEl.style.cssText = 'position:absolute;width:1px;height:1px;opacity:0;pointer-events:none;';
              ensureContainer().appendChild(audioEl);
              const entry = {{ trackId, track, stream, streamId: stream.id || '', audioEl }};
              remoteAudioEntries.set(trackId, entry);
              connectToRecorderIfReady(entry);
              if (typeof window.__noteraRegisterAudioSource === 'function') {{
                Promise.resolve(window.__noteraRegisterAudioSource({{
                  source_key: `webrtc:track:${{trackId}}`,
                  source_kind: 'webrtc_remote_track',
                  track_id: trackId,
                  stream_id: entry.streamId || '',
                  format: 'webm',
                  status: 'recording',
                }})).catch((error) => {{
                  console.warn('[Notera] Failed registering audio source', error);
                }});
              }}

              const cleanup = () => {{
                remoteAudioEntries.delete(trackId);
                const controller = window.__noteraRecorderController;
                if (controller?.sources?.has(trackId)) {{
                  const sourceBundle = controller.sources.get(trackId);
                  try {{
                    sourceBundle?.sourceNode?.disconnect();
                  }} catch (error) {{
                    console.warn('[Notera] Failed disconnecting source node', error);
                  }}
                  try {{
                    sourceBundle?.gainNode?.disconnect();
                  }} catch (error) {{
                    console.warn('[Notera] Failed disconnecting gain node', error);
                  }}
                  controller.sources.delete(trackId);
                  controller.sourceCount = controller.sources.size;
                }}
                audioEl.remove();
              }};
              track.addEventListener('ended', cleanup, {{ once: true }});
            }} catch (error) {{
              console.warn('[Notera] Failed attaching Teams audio track', error);
            }}
          }};

          const NativePC = window.RTCPeerConnection || window.webkitRTCPeerConnection;
          if (!NativePC) return;

          const patchInstance = (pc) => {{
            pc.addEventListener('track', (event) => {{
              try {{
                if (event.track && event.track.kind === 'audio') {{
                  attachAudioTrack(event.track);
                }}
                if (Array.isArray(event.streams)) {{
                  for (const stream of event.streams) {{
                    for (const streamTrack of stream.getAudioTracks()) {{
                      attachAudioTrack(streamTrack);
                    }}
                  }}
                }}
              }} catch (error) {{
                console.warn('[Notera] Failed processing Teams ontrack event', error);
              }}
            }});
          }};

          class PatchedRTCPeerConnection extends NativePC {{
            constructor(...args) {{
              super(...args);
              patchInstance(this);
            }}
          }}

          PatchedRTCPeerConnection.prototype = NativePC.prototype;
          Object.setPrototypeOf(PatchedRTCPeerConnection, NativePC);
          window.RTCPeerConnection = PatchedRTCPeerConnection;
          if (window.webkitRTCPeerConnection) {{
            window.webkitRTCPeerConnection = PatchedRTCPeerConnection;
          }}

          window.__noteraRemoteAudioInfo = () => {{
            return {{
              count: remoteAudioEntries.size,
              activeRecorderSources: window.__noteraRecorderController?.sourceCount || 0,
            }};
          }};

          window.__noteraStartAudioRecorder = async () => {{
            const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
            if (!AudioContextCtor) {{
              return {{ ok: false, error: 'AudioContext unavailable' }};
            }}
            if (!window.MediaRecorder) {{
              return {{ ok: false, error: 'MediaRecorder unavailable' }};
            }}

            const existingController = window.__noteraRecorderController;
            if (existingController?.recorder?.state === 'recording') {{
              return {{
                ok: true,
                mimeType: existingController.mimeType || '',
                sourceCount: existingController.sourceCount || 0,
              }};
            }}

            const audioContext = new AudioContextCtor();
            try {{
              if (audioContext.state === 'suspended') {{
                await audioContext.resume();
              }}
            }} catch (error) {{
              console.warn('[Notera] Failed resuming AudioContext', error);
            }}

            const destination = audioContext.createMediaStreamDestination();
            const controller = {{
              audioContext,
              destination,
              recorder: null,
              mimeType: '',
              sourceCount: 0,
              sources: new Map(),
              pendingUploads: new Set(),
              connectEntry: async (entry) => {{
                if (!entry?.trackId || !entry.stream || controller.sources.has(entry.trackId)) return;
                try {{
                  const sourceNode = audioContext.createMediaStreamSource(entry.stream);
                  const gainNode = audioContext.createGain();
                  gainNode.gain.value = 1.0;
                  sourceNode.connect(gainNode);
                  gainNode.connect(destination);
                  controller.sources.set(entry.trackId, {{ sourceNode, gainNode }});
                  controller.sourceCount = controller.sources.size;
                }} catch (error) {{
                  console.warn('[Notera] Failed connecting remote audio stream', error);
                }}
              }},
            }};

            window.__noteraRecorderController = controller;

            const audioEntries = Array.from(remoteAudioEntries.values());
            for (const entry of audioEntries) {{
              if (typeof window.__noteraRegisterAudioSource === 'function') {{
                try {{
                  await window.__noteraRegisterAudioSource({{
                    source_key: `webrtc:track:${{entry.trackId}}`,
                    source_kind: 'webrtc_remote_track',
                    track_id: entry.trackId,
                    stream_id: entry.streamId || '',
                    format: 'webm',
                    status: 'recording',
                  }});
                }} catch (error) {{
                  console.warn('[Notera] Failed backfilling audio source registration', error);
                }}
              }}
              await controller.connectEntry(entry);
            }}

            if (!controller.sourceCount) {{
              return {{ ok: false, error: 'No remote audio tracks available' }};
            }}

            const mimeCandidates = ['audio/webm;codecs=opus', 'audio/webm', 'audio/mp4'];
            const mimeType = mimeCandidates.find((candidate) => MediaRecorder.isTypeSupported(candidate)) || '';
            let recorder;
            try {{
              recorder = mimeType
                ? new MediaRecorder(destination.stream, {{ mimeType }})
                : new MediaRecorder(destination.stream);
            }} catch (error) {{
              return {{ ok: false, error: String(error) }};
            }}
            controller.recorder = recorder;
            controller.mimeType = recorder.mimeType || mimeType || '';

            recorder.ondataavailable = async (event) => {{
              let uploadPromise = null;
              try {{
                if (!event.data || !event.data.size || typeof window.__noteraSaveAudioChunk !== 'function') return;
                uploadPromise = (async () => {{
                  const arrayBuffer = await event.data.arrayBuffer();
                  const bytes = new Uint8Array(arrayBuffer);
                  let binary = '';
                  const chunkSize = 0x8000;
                  for (let offset = 0; offset < bytes.length; offset += chunkSize) {{
                    const slice = bytes.subarray(offset, offset + chunkSize);
                    binary += String.fromCharCode(...slice);
                  }}
                  await window.__noteraSaveAudioChunk({{
                    base64: btoa(binary),
                    mimeType: event.data.type || controller.mimeType,
                  }});
                }})();
                controller.pendingUploads.add(uploadPromise);
                await uploadPromise;
              }} catch (error) {{
                console.warn('[Notera] Failed persisting recorder chunk', error);
              }} finally {{
                if (uploadPromise) {{
                  controller.pendingUploads.delete(uploadPromise);
                }}
              }}
            }};

            recorder.start(5000);
            return {{
              ok: true,
              mimeType: controller.mimeType,
              sourceCount: controller.sourceCount,
            }};
          }};

          window.__noteraPrepareAudioRecorderShutdown = async () => {{
            return {{ ok: true }};
          }};

          window.__noteraStopAudioRecorder = async () => {{
            const controller = window.__noteraRecorderController;
            if (!controller?.recorder) {{
              return {{ ok: false, error: 'Recorder not initialized' }};
            }}
            if (controller.recorder.state === 'inactive') {{
              return {{ ok: true }};
            }}
            await new Promise((resolve) => {{
              controller.recorder.addEventListener('stop', resolve, {{ once: true }});
              controller.recorder.stop();
            }});
            if (controller.pendingUploads.size) {{
              await Promise.allSettled(Array.from(controller.pendingUploads));
            }}
            return {{ ok: true }};
          }};

          window.__noteraTeamsAudioHookInstalled = true;
        }})();
        """
    )


async def get_remote_audio_info(page):
    try:
        return await page.evaluate(
            "() => window.__noteraRemoteAudioInfo ? window.__noteraRemoteAudioInfo() : ({count: 0, activeRecorderSources: 0})"
        )
    except Exception as e:
        logger.debug("Could not read remote audio info: %s", e)
        return {"count": 0, "activeRecorderSources": 0}


async def start_browser_audio_capture(page, chunk_writer, meeting_id):
    try:
        await page.expose_function("__noteraSaveAudioChunk", chunk_writer.save_chunk)
    except Exception as e:
        if "__noteraSaveAudioChunk" not in str(e):
            raise
    try:
        await page.expose_function(
            "__noteraRegisterAudioSource",
            lambda payload: register_audio_source(
                meeting_id,
                (payload or {}).get("source_key") or f"webrtc:track:{datetime.utcnow().timestamp()}",
                (payload or {}).get("source_kind") or "webrtc_remote_track",
                track_id=(payload or {}).get("track_id"),
                stream_id=(payload or {}).get("stream_id"),
                fmt=(payload or {}).get("format"),
                status=(payload or {}).get("status") or "recording",
            ),
        )
    except Exception as e:
        if "__noteraRegisterAudioSource" not in str(e):
            raise

    deadline = asyncio.get_running_loop().time() + 30
    last_error = "Toplantı ses akışı bulunamadı."
    last_info = {"count": 0, "activeRecorderSources": 0}

    while asyncio.get_running_loop().time() < deadline:
        last_info = await get_remote_audio_info(page)
        if last_info.get("count", 0) > 0:
            result = await page.evaluate(
                "() => window.__noteraStartAudioRecorder ? window.__noteraStartAudioRecorder() : ({ok: false, error: 'Audio recorder bridge unavailable'})"
            )
            if result.get("ok"):
                return True, result, None
            last_error = result.get("error") or last_error
        await asyncio.sleep(1)

    return False, last_info, last_error


async def stop_browser_audio_capture(page):
    try:
        await page.evaluate(
            "() => window.__noteraPrepareAudioRecorderShutdown ? window.__noteraPrepareAudioRecorderShutdown() : ({ok: true})"
        )
    except Exception:
        pass

    try:
        return await page.evaluate(
            "() => window.__noteraStopAudioRecorder ? window.__noteraStopAudioRecorder() : ({ok: false, error: 'Audio recorder bridge unavailable'})"
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}


def update_meeting_status(meeting_id, status, clear_bot_pid=False):
    """Updates the status of a meeting in the database."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        joined_at = datetime.utcnow().isoformat() if status == "active" else None
        ended_at = datetime.utcnow().isoformat() if status == "completed" else None
        if clear_bot_pid:
            cursor.execute(
                """
                UPDATE meeting
                SET status = ?,
                    bot_pid = NULL,
                    joined_at = COALESCE(joined_at, ?),
                    ended_at = COALESCE(?, ended_at)
                WHERE id = ?
                """,
                (status, joined_at, ended_at, meeting_id),
            )
        else:
            cursor.execute(
                """
                UPDATE meeting
                SET status = ?,
                    joined_at = COALESCE(joined_at, ?),
                    ended_at = COALESCE(?, ended_at)
                WHERE id = ?
                """,
                (status, joined_at, ended_at, meeting_id),
            )
        conn.commit()
        logger.info(f"Meeting {meeting_id} status updated to {status}")
    except Exception as e:
        logger.error(f"Failed to update status: {e}")
    finally:
        conn.close()

async def run_bot(meeting_url, meeting_id):
    meeting_id = int(meeting_id)
    run_id_value = os.getenv("NOTERA_WORKER_RUN_ID")
    run_id = int(run_id_value) if run_id_value and run_id_value.isdigit() else run_id_value
    context_token = bind_context(meeting_id=meeting_id, worker_type="bot", run_id=run_id)
    ensure_runtime_schema(get_db_path())
    log_event(logger, logging.INFO, "worker.started", "Bot worker started")
    shutdown_requested = False
    leave_attempted = False
    meeting_screenshot_path = get_live_meeting_screenshot_path(meeting_id)
    audio_recording_enabled = is_audio_recording_enabled(meeting_id)
    audio_capture_started = False
    audio_capture_started_monotonic = None
    audio_capture_error = None
    audio_failure_notified = False
    chunk_writer = MeetingAudioChunkWriter(meeting_id) if audio_recording_enabled else None
    participant_panel_opened = False
    active_speaker_state = {}
    meeting_started_monotonic = None

    if audio_recording_enabled:
        update_audio_status(meeting_id, AUDIO_STATUS_PENDING, None)
        update_meeting_fields(
            meeting_id,
            postprocess_status=POSTPROCESS_STATUS_PENDING,
            postprocess_error=None,
        )
    else:
        update_audio_status(meeting_id, AUDIO_STATUS_DISABLED, None)

    def request_shutdown(signum, _frame):
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.info(
            "Received signal %s. Preparing to leave meeting %s.",
            signum,
            meeting_id,
        )

    previous_handlers = {}
    for current_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            previous_handlers[current_signal] = signal.getsignal(current_signal)
            signal.signal(current_signal, request_shutdown)
        except Exception as e:
            logger.debug(f"Could not register signal handler for {current_signal}: {e}")

    async with async_playwright() as p:
        try:
            logger.info("Launching browser...")
            browser = await launch_teams_browser(p)
            context = await browser.new_context(
                permissions=["microphone"],
                bypass_csp=True,
            )
            await install_teams_audio_hook(context)
            page = await context.new_page()

            stop_screenshots = asyncio.Event()
            screenshot_task = asyncio.create_task(
                take_periodic_screenshot(page, stop_screenshots, meeting_screenshot_path)
            )

            await open_meeting_entry(page, meeting_url)

            async def handle_popups(allow_without_audio=False):
                try:
                    logger.info("Checking for audio/video permission popups...")
                    permission_btns = [
                        "button:has-text('Continue on this browser')",
                        "button:has-text('Allow')",
                        "button:has-text('Dismiss')",
                        "button:has-text('Got it')",
                    ]
                    if allow_without_audio:
                        permission_btns = [
                            "button:has-text('Continue without audio or video')",
                            "button[aria-label*='Continue without']",
                            "button[data-tid*='continue-without']",
                        ] + permission_btns

                    for selector in permission_btns:
                        try:
                            btn = page.locator(selector)
                            count = await btn.count()
                            if count <= 0:
                                continue
                            for index in range(count):
                                target = btn.nth(index)
                                if await target.is_visible(timeout=2000):
                                    logger.info("Clicking permission/blocker button: %s", selector)
                                    await target.click(force=True, timeout=10000)
                                    await asyncio.sleep(2)
                                    return True
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug("Permission popup check failure: %s", e)
                return False

            await handle_popups(allow_without_audio=not audio_recording_enabled)

            logger.info("Waiting for join name input (up to 60s)...")
            name_input_selectors = [
                "input[placeholder='Type your name']",
                "input[data-tid='prejoin-display-name-input']",
                "input[name='displayName']",
                "input.input-field",
            ]

            name_input = None
            try:
                await page.wait_for_selector("input", timeout=60000)
            except Exception as e:
                logger.error("Page did not load any input within 60s: %s", e)
                screenshot_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"timeout_input_{datetime.now().strftime('%H%M%S')}.png",
                )
                await page.screenshot(path=screenshot_path)
                return

            for selector in name_input_selectors:
                try:
                    name_input = page.locator(selector)
                    if await name_input.is_visible(timeout=2000):
                        logger.info("Found name input with selector: %s", selector)
                        break
                except Exception:
                    continue

            if not name_input or not await name_input.is_visible(timeout=500):
                logger.warning("Preferred selectors failed. Using first visible input.")
                name_input = page.locator("input").first

            await name_input.fill("Transcription Bot")
            logger.info("Name filled.")

            await handle_popups(allow_without_audio=not audio_recording_enabled)
            if audio_recording_enabled:
                if await select_computer_audio(page):
                    logger.info("Computer audio selected successfully.")
                else:
                    logger.warning("Computer audio selection did not succeed; audio capture may fail.")
                    await handle_popups(allow_without_audio=True)
            else:
                await handle_popups(allow_without_audio=True)

            logger.info("Waiting for 'Join now' button (up to 45s)...")
            join_buttons = [
                "button:has-text('Join now')",
                "button[data-tid='prejoin-join-button']",
                "button.primary-button",
                "button[type='button']:has-text('Join')",
            ]

            joined = False
            for _ in range(45):
                await handle_popups(allow_without_audio=not audio_recording_enabled)
                for btn_selector in join_buttons:
                    try:
                        btn = page.locator(btn_selector)
                        if await btn.is_visible(timeout=500):
                            await btn.click(timeout=2000)
                            joined = True
                            logger.info("Clicked join button with selector: %s", btn_selector)
                            break
                    except Exception:
                        continue
                if joined:
                    break
                await asyncio.sleep(1)

            if not joined:
                logger.error("Could not find a Join button after 45s. Taking screenshot.")
                screenshot_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    f"timeout_join_{datetime.now().strftime('%H%M%S')}.png",
                )
                await page.screenshot(path=screenshot_path)
                return

            logger.info("Joined successfully! Monitoring participant registry and audio sources...")
            update_meeting_status(meeting_id, "active")
            meeting_started_monotonic = asyncio.get_running_loop().time()

            await asyncio.sleep(15)
            await page.screenshot(path=meeting_screenshot_path)
            logger.info("Post-join screenshot taken. Stopping periodic shots.")
            stop_screenshots.set()

            await dump_dom(page, "initial_join_dom.html")
            await install_participant_registry_hook(page)
            participant_panel_opened = await open_participant_panel(page)
            if participant_panel_opened:
                logger.info("Participant panel opened successfully.")
            else:
                logger.warning("Participant panel could not be opened immediately after join.")

            async def restore_participant_panel(reason):
                nonlocal participant_panel_opened
                participant_panel_opened = await open_participant_panel(page)
                if participant_panel_opened:
                    logger.info("Participant panel restored after %s.", reason)
                else:
                    logger.warning("Participant panel could not be restored after %s.", reason)
                return participant_panel_opened

            if audio_recording_enabled and chunk_writer is not None:
                started, audio_result, audio_error = await start_browser_audio_capture(page, chunk_writer, meeting_id)
                if started:
                    audio_capture_started = True
                    audio_capture_started_monotonic = asyncio.get_running_loop().time()
                    capture_started_at = datetime.utcnow().isoformat()
                    update_audio_status(meeting_id, AUDIO_STATUS_RECORDING, None)
                    update_meeting_fields(meeting_id, audio_capture_started_at=capture_started_at)
                    register_audio_asset(
                        meeting_id,
                        str(get_meeting_master_audio_path(meeting_id, chunk_writer.format)),
                        chunk_writer.format,
                        AUDIO_STATUS_RECORDING,
                    )
                    register_audio_source(
                        meeting_id,
                        "meeting:master",
                        "meeting_mixed_master",
                        file_path=str(get_meeting_master_audio_path(meeting_id, chunk_writer.format)),
                        fmt=chunk_writer.format,
                        status=AUDIO_STATUS_RECORDING,
                    )
                    logger.info("Browser audio capture started: %s", audio_result)
                else:
                    audio_capture_error = audio_error or "Ses kaydı başlatılamadı."
                    logger.warning("Browser audio capture could not start: %s", audio_capture_error)
                    update_audio_status(meeting_id, AUDIO_STATUS_FAILED, audio_capture_error)
                    audio_failure_notified = await send_chat_message(page, CHAT_AUDIO_FAILURE_MESSAGE)
                    await restore_participant_panel("audio failure chat message")

            logger.info("Starting participant registry monitoring...")
            poll_count = 0
            chat_notice_sent = False
            chat_notice_attempts = 0
            known_chat_message_ids = set()

            if chat_notice_attempts < 3:
                chat_notice_attempts += 1
                chat_notice_sent = await send_chat_notice(page)
                await restore_participant_panel("initial chat notice")

            for message in await get_chat_messages(page):
                mid = message.get("mid")
                if mid:
                    known_chat_message_ids.add(mid)
            await restore_participant_panel("initial chat sync")

            while True:
                try:
                    if shutdown_requested or is_stop_requested(meeting_id):
                        logger.info("Stop requested for meeting %s. Closing bot session.", meeting_id)
                        if not leave_attempted:
                            leave_attempted = True
                            await leave_meeting_via_ui(page)
                        break

                    poll_count += 1
                    activity_origin_monotonic = (
                        audio_capture_started_monotonic
                        or meeting_started_monotonic
                        or asyncio.get_running_loop().time()
                    )
                    current_offset_ms = int(
                        max(0.0, asyncio.get_running_loop().time() - activity_origin_monotonic)
                        * 1000
                    )

                    if poll_count % 120 == 0:
                        await dump_dom(page, "monitoring_debug_dom.html")

                    if not chat_notice_sent and chat_notice_attempts < 3 and poll_count >= 20 and poll_count % 60 == 20:
                        chat_notice_attempts += 1
                        chat_notice_sent = await send_chat_notice(page)
                        await restore_participant_panel("chat notice retry")

                    if audio_recording_enabled and not audio_capture_started and not audio_failure_notified and poll_count % 20 == 0:
                        audio_failure_notified = await send_chat_message(page, CHAT_AUDIO_FAILURE_MESSAGE)
                        await restore_participant_panel("audio failure notice")

                    if poll_count % 4 == 0:
                        exit_requested, known_chat_message_ids = await detect_exit_command(page, known_chat_message_ids)
                        if exit_requested:
                            logger.info(
                                "Leaving meeting %s because a user sent the '%s' chat command.",
                                meeting_id,
                                CHAT_EXIT_COMMAND,
                            )
                            await send_chat_message(page, CHAT_EXIT_ACK_MESSAGE)
                            if not leave_attempted:
                                leave_attempted = True
                                await leave_meeting_via_ui(page)
                            break
                        await restore_participant_panel("exit command check")

                    if not participant_panel_opened or not await participant_panel_visible(page):
                        if poll_count % 8 == 1:
                            participant_panel_opened = await open_participant_panel(page)
                            if participant_panel_opened:
                                logger.info("Participant panel reopened.")

                    if poll_count == 1 or poll_count % 4 == 0:
                        observed_at = datetime.utcnow()
                        participant_items = await collect_participant_registry_snapshot(page, observed_at)
                        if not participant_items:
                            restored = await restore_participant_panel("empty participant snapshot")
                            if restored:
                                await asyncio.sleep(0.4)
                                participant_items = await collect_participant_registry_snapshot(page, observed_at)
                        if DEBUG_ARTIFACTS_ENABLED and (poll_count == 1 or poll_count % 20 == 0 or not participant_items):
                            debug_state = await collect_participant_debug_state(page)
                            write_participant_snapshot_debug(meeting_id, observed_at, participant_items, debug_state)
                        if participant_items:
                            if poll_count == 1 or poll_count % 40 == 0:
                                logger.info(
                                    "Participant registry snapshot collected for meeting %s: %s participants",
                                    meeting_id,
                                    len(participant_items),
                                )
                            sync_speaker_activity(
                                meeting_id,
                                participant_items,
                                active_speaker_state,
                                current_offset_ms,
                            )
                        elif poll_count % 40 == 0:
                            logger.warning("Participant registry snapshot is empty for meeting %s.", meeting_id)
                            await dump_dom(page, "participant_registry_empty_dom.html")

                except Exception as e:
                    logger.error("Error during participant registry polling: %s", e, exc_info=True)
                    try:
                        await page.evaluate("1+1")
                    except Exception:
                        logger.error("Page disconnected! Meeting may have ended.")
                        try:
                            await page.screenshot(
                                path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "exit_screenshot.png")
                            )
                        except Exception:
                            pass
                        break

                await asyncio.sleep(0.5)

            logger.info("Participant registry monitoring loop ended.")
        except Exception as e:
            logger.error("An error occurred during bot execution: %s", e, exc_info=True)
        finally:
            activity_origin_monotonic = audio_capture_started_monotonic or meeting_started_monotonic
            if activity_origin_monotonic is not None:
                current_offset_ms = int(
                    max(0.0, asyncio.get_running_loop().time() - activity_origin_monotonic) * 1000
                )
                flush_speaker_activity(meeting_id, active_speaker_state, current_offset_ms)
            if "stop_screenshots" in locals():
                stop_screenshots.set()
                await screenshot_task
            if "page" in locals() and not page.is_closed() and audio_capture_started:
                stop_result = await stop_browser_audio_capture(page)
                if not stop_result.get("ok"):
                    logger.warning("Browser audio recorder stop failed: %s", stop_result.get("error"))
                elif chunk_writer is not None:
                    chunk_writer.stop_accepting_writes()
                await asyncio.sleep(0.25)
            if (
                "page" in locals()
                and not page.is_closed()
                and (shutdown_requested or is_stop_requested(meeting_id))
                and not leave_attempted
            ):
                try:
                    leave_attempted = True
                    await leave_meeting_via_ui(page)
                except Exception as e:
                    logger.debug("Failed final leave attempt: %s", e)
            if "browser" in locals():
                logger.info("Closing browser.")
                await browser.close()

            master_audio_path = None
            if audio_capture_started and chunk_writer is not None:
                try:
                    master_audio_path, pcm_audio_path, audio_format, duration_ms = chunk_writer.finalize()
                    register_audio_asset(
                        meeting_id,
                        str(master_audio_path),
                        audio_format,
                        AUDIO_STATUS_READY,
                        duration_ms=duration_ms,
                        pcm_audio_path=str(pcm_audio_path) if pcm_audio_path else None,
                        postprocess_version="whisperx_global_v1",
                    )
                    register_audio_source(
                        meeting_id,
                        "meeting:master",
                        "meeting_mixed_master",
                        file_path=str(master_audio_path),
                        fmt=audio_format,
                        status=AUDIO_STATUS_READY,
                    )
                    finalize_audio_sources(meeting_id, status=AUDIO_STATUS_READY)
                    update_audio_status(meeting_id, AUDIO_STATUS_READY, None)
                except Exception as e:
                    audio_capture_error = str(e)
                    logger.error("Failed finalizing meeting audio for %s: %s", meeting_id, e)
                    finalize_audio_sources(meeting_id, status=AUDIO_STATUS_FAILED)
                    update_audio_status(meeting_id, AUDIO_STATUS_FAILED, audio_capture_error)
            elif audio_recording_enabled and not audio_capture_started:
                finalize_audio_sources(meeting_id, status=AUDIO_STATUS_FAILED)
                update_audio_status(
                    meeting_id,
                    AUDIO_STATUS_FAILED,
                    audio_capture_error or "Ses kaydı başlatılamadı.",
                )

            update_meeting_status(meeting_id, "completed", clear_bot_pid=True)
            update_meeting_fields(meeting_id, ended_at=datetime.utcnow().isoformat())
            trigger_postprocess_worker(meeting_id)
            delete_live_meeting_screenshot(meeting_screenshot_path)
            for current_signal, previous_handler in previous_handlers.items():
                try:
                    signal.signal(current_signal, previous_handler)
                except Exception:
                    pass
            log_event(logger, logging.INFO, "worker.completed", "Bot worker finished")
            reset_context(context_token)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        asyncio.run(run_bot(sys.argv[1], sys.argv[2]))
    else:
        print("Usage: python -m backend.workers.bot <url> <id>")
