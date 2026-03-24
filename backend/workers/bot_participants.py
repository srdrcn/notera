from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.runtime.participant_names import is_roster_heading_name, normalize_participant_name
from backend.workers.bot_store import (
    append_speaker_activity_interval,
    register_identity_evidence,
    upsert_meeting_participant,
)


logger = logging.getLogger("notera.worker.bot")
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


def normalize_caption_text(text: str | None) -> str:
    return " ".join((text or "").strip().split())


def is_bot_participant_name(value: str | None) -> bool:
    normalized = normalize_participant_name(value).casefold()
    return normalized in {"transcription bot", "notera bot", "bot"} or normalized.endswith(" bot")


def is_generic_participant_identity(value: str | None) -> bool:
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


def is_unstable_stable_key(value: str | None) -> bool:
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


def participant_identity_conflicts(
    existing_normalized_name: str | None,
    existing_platform_identity: str | None,
    new_normalized_name: str | None,
    new_platform_identity: str | None,
) -> bool:
    existing_name = normalize_participant_name(existing_normalized_name).casefold()
    new_name = normalize_participant_name(new_normalized_name).casefold()
    if not existing_name or not new_name or existing_name == new_name:
        return False
    existing_platform = normalize_caption_text(existing_platform_identity)
    new_platform = normalize_caption_text(new_platform_identity)
    if existing_platform and new_platform and existing_platform == new_platform:
        return False
    return True


def disambiguated_participant_key(participant_key: str, normalized_name: str) -> str:
    digest = hashlib.sha1((normalized_name or "unknown").encode("utf-8")).hexdigest()[:10]
    return f"{participant_key}|name:{digest}"[:255]


def split_participant_display_name(display_name: str | None, role_hint: str | None = None) -> tuple[str, str | None]:
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


def participant_snapshot_source_rank(source_kind: str | None) -> int:
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


def participant_snapshot_item_score(item: dict[str, Any]) -> int:
    return (
        int(bool(item.get("platform_identity"))) * 40
        + int(bool(item.get("stable_key"))) * 20
        + int(bool(item.get("role"))) * 10
        + int(bool(item.get("is_speaking"))) * 12
        + participant_snapshot_source_rank(item.get("source_kind"))
    )


def merge_participant_snapshot_items(preferred: dict[str, Any], other: dict[str, Any]) -> dict[str, Any]:
    merged = dict(preferred)
    for field in ("stable_key", "stable_key_source", "platform_identity", "role", "dom_key"):
        if not merged.get(field) and other.get(field):
            merged[field] = other[field]
    merged["join_state"] = "left" if merged.get("join_state") == "left" and other.get("join_state") == "left" else "present"
    merged["is_speaking"] = bool(preferred.get("is_speaking")) or bool(other.get("is_speaking"))
    merged["voice_signal_present"] = bool(preferred.get("voice_signal_present")) or bool(other.get("voice_signal_present"))
    merged["is_muted"] = bool(preferred.get("is_muted")) or bool(other.get("is_muted"))
    merged["is_bot"] = bool(preferred.get("is_bot")) or bool(other.get("is_bot"))
    if bool(other.get("is_speaking")) and not bool(preferred.get("is_speaking")):
        merged["source_kind"] = other.get("source_kind") or merged.get("source_kind")
    elif participant_snapshot_source_rank(other.get("source_kind")) > participant_snapshot_source_rank(merged.get("source_kind")):
        merged["source_kind"] = other.get("source_kind") or merged.get("source_kind")
    return merged


def extract_participant_key(item: dict[str, Any]) -> str:
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


def participant_key_rank(participant_key: str | None) -> int:
    normalized = normalize_caption_text(participant_key).casefold()
    if normalized.startswith("teams-platform:"):
        return 3
    if normalized.startswith("teams-roster:"):
        return 2
    if normalized.startswith("teams-name:"):
        return 1
    return 0


def normalize_participant_snapshot_items(
    raw_items: list[dict[str, Any]],
    observed_at: datetime,
) -> list[dict[str, Any]]:
    normalized_items: list[dict[str, Any]] = []
    for item in raw_items or []:
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
                "voice_signal_present": bool(item.get("voice_signal_present")),
                "is_muted": bool(item.get("is_muted")),
                "dom_key": normalize_caption_text(item.get("dom_key")),
                "is_bot": bool(item.get("is_bot")) or is_bot_participant_name(display_name),
                "observed_at": observed_at,
            }
        )
    return normalized_items


def deduplicate_participant_snapshot_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped_items: dict[str, dict[str, Any]] = {}
    for item in items:
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

    by_display_name: dict[str, dict[str, Any]] = {}
    for item in deduped_items.values():
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


async def install_participant_registry_hook(page) -> bool:
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
                if (/^(?:in|not in) this meeting(?:\\s*\\(\\d+\\))?$/i.test(candidate)) return false;
                if (/^(?:bu toplantıda|bu toplantida)(?:\\s*\\(\\d+\\))?$/i.test(candidate)) return false;
                if (/^(?:bu toplantıda değil|bu toplantida degil)(?:\\s*\\(\\d+\\))?$/i.test(candidate)) return false;
                if (/^\\d+\\s+(?:people|participants?)$/i.test(candidate)) return false;
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
                const voiceSignalPresent = hasVoiceSignal(row);
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
                  voice_signal_present: voiceSignalPresent,
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
    except Exception as exc:
        logger.debug("Could not install participant registry hook: %s", exc)
        return False


async def collect_participant_registry_snapshot(page, observed_at: datetime) -> list[dict[str, Any]]:
    await install_participant_registry_hook(page)
    snapshot = await page.evaluate(
        "() => window.__noteraGetParticipantSnapshot ? window.__noteraGetParticipantSnapshot() : ({items: []})"
    )
    items = (snapshot or {}).get("items") or []
    normalized_items = normalize_participant_snapshot_items(items, observed_at)
    return deduplicate_participant_snapshot_items(normalized_items)


async def collect_participant_debug_state(page) -> dict[str, Any]:
    try:
        return await page.evaluate(
            """(config) => {
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
                row_selector_match_count: rowCount,
                tile_selector_match_count: tileCount,
                video_surface_match_count: videoSurfaceCount,
                voice_signal_match_count: voiceSignalCount,
              };
            }""",
            PARTICIPANT_REGISTRY_CONFIG,
        )
    except Exception as exc:
        return {"error": str(exc)}


def write_participant_snapshot_debug(
    meeting_id: int,
    observed_at: datetime,
    participant_items: list[dict[str, Any]],
    debug_state: dict[str, Any] | None = None,
) -> None:
    return None


def sync_speaker_activity(
    meeting_id: int,
    participant_items: list[dict[str, Any]],
    active_speaker_state: dict[str, dict[str, Any]],
    current_offset_ms: int,
) -> None:
    speaking_items = [item for item in participant_items if item.get("is_speaking")]
    simultaneous_claim_count = len(speaking_items)
    conflicted_claim = simultaneous_claim_count > 1
    seen_keys: set[str] = set()
    for item in participant_items:
        participant_key = extract_participant_key(item)
        display_name = item.get("display_name") or "Unknown"
        normalized_name = normalize_participant_name(display_name).casefold()
        participant_id = upsert_meeting_participant(
            meeting_id,
            participant_key,
            display_name,
            normalize_participant_name=normalize_participant_name,
            participant_identity_conflicts=participant_identity_conflicts,
            participant_key_rank=participant_key_rank,
            disambiguated_participant_key=disambiguated_participant_key,
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
                    "voice_signal_present": bool(active_entry.get("voice_signal_present")),
                    "simultaneous_claim_count": int(active_entry.get("simultaneous_claim_count") or 0),
                    "conflicted_claim": bool(active_entry.get("conflicted_claim")),
                    "source_kind": active_entry.get("source_kind"),
                    "closed_reason": "identity_shift",
                    "previous_display_name": active_entry.get("display_name"),
                    "current_display_name": display_name,
                },
            )
            del active_speaker_state[participant_key]
            active_entry = None

        if item.get("is_speaking"):
            interval_confidence = 0.58 if conflicted_claim else 0.91
            if active_entry is None:
                active_speaker_state[participant_key] = {
                    "participant_id": participant_id,
                    "start_offset_ms": current_offset_ms,
                    "last_seen_offset_ms": current_offset_ms,
                    "display_name": display_name,
                    "normalized_name": normalized_name,
                    "confidence": interval_confidence,
                    "voice_signal_present": bool(item.get("voice_signal_present")),
                    "simultaneous_claim_count": simultaneous_claim_count,
                    "conflicted_claim": conflicted_claim,
                    "source_kind": item.get("source_kind"),
                }
            else:
                active_entry["participant_id"] = participant_id
                active_entry["last_seen_offset_ms"] = current_offset_ms
                active_entry["display_name"] = display_name
                active_entry["normalized_name"] = normalized_name
                active_entry["confidence"] = min(
                    float(active_entry.get("confidence") or interval_confidence),
                    interval_confidence,
                )
                active_entry["voice_signal_present"] = bool(active_entry.get("voice_signal_present")) or bool(
                    item.get("voice_signal_present")
                )
                active_entry["simultaneous_claim_count"] = max(
                    int(active_entry.get("simultaneous_claim_count") or 0),
                    simultaneous_claim_count,
                )
                active_entry["conflicted_claim"] = bool(active_entry.get("conflicted_claim")) or conflicted_claim
                if item.get("source_kind"):
                    active_entry["source_kind"] = item.get("source_kind")
        elif active_entry is not None:
            append_speaker_activity_interval(
                meeting_id,
                active_entry["participant_id"],
                active_entry["start_offset_ms"],
                current_offset_ms,
                source="roster_speaking_indicator",
                confidence=float(active_entry.get("confidence") or 0.91),
                metadata={
                    "participant_key": participant_key,
                    "voice_signal_present": bool(active_entry.get("voice_signal_present")),
                    "simultaneous_claim_count": int(active_entry.get("simultaneous_claim_count") or 0),
                    "conflicted_claim": bool(active_entry.get("conflicted_claim")),
                    "source_kind": active_entry.get("source_kind"),
                },
            )
            del active_speaker_state[participant_key]

    stale_keys: list[str] = []
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
            metadata={
                "participant_key": participant_key,
                "voice_signal_present": bool(entry.get("voice_signal_present")),
                "simultaneous_claim_count": int(entry.get("simultaneous_claim_count") or 0),
                "conflicted_claim": bool(entry.get("conflicted_claim")),
                "source_kind": entry.get("source_kind"),
                "closed_reason": "participant_missing",
            },
        )
        stale_keys.append(participant_key)
    for participant_key in stale_keys:
        active_speaker_state.pop(participant_key, None)


def flush_speaker_activity(
    meeting_id: int,
    active_speaker_state: dict[str, dict[str, Any]],
    current_offset_ms: int,
) -> None:
    for participant_key, entry in list(active_speaker_state.items()):
        append_speaker_activity_interval(
            meeting_id,
            entry["participant_id"],
            entry["start_offset_ms"],
            current_offset_ms,
            source="roster_speaking_indicator",
            confidence=0.7,
            metadata={
                "participant_key": participant_key,
                "voice_signal_present": bool(entry.get("voice_signal_present")),
                "simultaneous_claim_count": int(entry.get("simultaneous_claim_count") or 0),
                "conflicted_claim": bool(entry.get("conflicted_claim")),
                "source_kind": entry.get("source_kind"),
                "closed_reason": "meeting_end",
            },
        )
        active_speaker_state.pop(participant_key, None)
