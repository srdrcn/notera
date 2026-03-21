import asyncio
import base64
import json
import logging
import os
import re
import signal
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.runtime.bootstrap import ensure_runtime_schema  # noqa: E402
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
from backend.runtime.teams_links import (  # noqa: E402
    TEAMS_JOIN_WITH_ID_PAGE_URL,
    parse_join_with_id_target,
)

# Setup logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger("TeamsBot")
DEBUG_ARTIFACTS_ENABLED = False
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


def make_caption_fingerprint(speaker, text):
    normalized_speaker = normalize_caption_text(speaker).casefold()
    normalized_text = normalize_caption_text(text).casefold()
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
        logger.info("Navigating to meeting URL: %s", meeting_url)
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


def save_caption_event(meeting_id, speaker, text, sequence_no, observed_at, slot_index, revision_no):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    observed_value = (observed_at or datetime.utcnow()).isoformat()
    try:
        recent = cursor.execute(
            """
            SELECT speaker_name, text, observed_at
            FROM teamscaptionevent
            WHERE meeting_id = ? AND COALESCE(slot_index, -1) = COALESCE(?, -1)
            ORDER BY id DESC
            LIMIT 1
            """,
            (meeting_id, slot_index),
        ).fetchone()
        if recent:
            previous_speaker, previous_text, previous_observed_at = recent
            previous_dt = parse_bot_dt(previous_observed_at)
            current_dt = observed_at or datetime.utcnow()
            if (
                make_caption_fingerprint(previous_speaker, previous_text)
                == make_caption_fingerprint(speaker, text)
                and previous_dt
                and abs((current_dt - previous_dt).total_seconds()) <= 20
            ):
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
        self.aggregate_path = get_meeting_audio_dir(self.meeting_id) / "recording.part"
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
        with self.aggregate_path.open("ab") as aggregate_file:
            aggregate_file.write(raw_bytes)
        if DEBUG_ARTIFACTS_ENABLED:
            chunk_dir = get_meeting_audio_chunks_dir(self.meeting_id)
            chunk_path = chunk_dir / f"chunk_{self.chunk_index:05d}.{self.format}"
            chunk_path.write_bytes(raw_bytes)
            self.chunk_paths.append(chunk_path)
        logger.info(
            "Saved audio chunk for meeting %s: part #%s (%s bytes)",
            self.meeting_id,
            self.chunk_index,
            len(raw_bytes),
        )
        return True

    def finalize(self):
        if not self.aggregate_path.exists() or self.aggregate_path.stat().st_size <= 0:
            raise RuntimeError("no audio chunks were captured")

        master_path = get_meeting_master_audio_path(self.meeting_id, self.format)
        shutil.copy2(self.aggregate_path, master_path)
        pcm_path = get_meeting_pcm_audio_path(self.meeting_id)
        pcm_result = subprocess.run(
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
        if pcm_result.returncode != 0:
            logger.warning(
                "Could not create PCM audio copy for meeting %s: %s",
                self.meeting_id,
                pcm_result.stderr.strip() or "ffmpeg conversion failed",
            )
        logger.info("Finalized master audio for meeting %s at %s", self.meeting_id, master_path)
        duration_ms = probe_audio_duration_ms(master_path)
        return master_path, pcm_path if pcm_path.exists() else None, self.format, duration_ms

    def stop_accepting_writes(self):
        self.accept_writes = False

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
        "button[title='Leave']",
        "button[aria-label='Leave']",
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
                await asyncio.sleep(2)
                return True
        except Exception as e:
            logger.debug(f"Failed clicking leave button with selector {selector}: {e}")

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
        logger.info("Deleted live meeting screenshot: %s", screenshot_path)
    except FileNotFoundError:
        return
    except Exception as e:
        logger.debug(f"Could not delete live meeting screenshot {screenshot_path}: {e}")


async def take_periodic_screenshot(page, stop_event, screenshot_path):
    """Takes a screenshot every 10 seconds until stop_event is set."""
    while not stop_event.is_set():
        try:
            await page.screenshot(path=screenshot_path)
            logger.info(f"Periodic screenshot updated: {screenshot_path}")
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
        logger.info(
            "Debug artifacts written: html=%s screenshot=%s summary=%s",
            latest_html,
            latest_png,
            latest_json,
        )
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
              const entry = {{ trackId, track, stream, audioEl }};
              remoteAudioEntries.set(trackId, entry);
              connectToRecorderIfReady(entry);

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


async def start_browser_audio_capture(page, chunk_writer):
    try:
        await page.expose_function("__noteraSaveAudioChunk", chunk_writer.save_chunk)
    except Exception as e:
        if "__noteraSaveAudioChunk" not in str(e):
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
    ensure_runtime_schema(get_db_path())
    logger.info("Bot starting for meeting ID: %s", meeting_id)
    shutdown_requested = False
    leave_attempted = False
    meeting_screenshot_path = get_live_meeting_screenshot_path(meeting_id)
    audio_recording_enabled = is_audio_recording_enabled(meeting_id)
    audio_capture_started = False
    audio_capture_error = None
    audio_failure_notified = False
    chunk_writer = MeetingAudioChunkWriter(meeting_id) if audio_recording_enabled else None
    caption_event_sequence = 0

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

            logger.info("Joined successfully! Monitoring for captions...")
            update_meeting_status(meeting_id, "active")

            await asyncio.sleep(15)
            await page.screenshot(path=meeting_screenshot_path)
            logger.info("Post-join screenshot taken. Stopping periodic shots.")
            stop_screenshots.set()

            logger.info("Enabling live captions in the bot session.")
            await dump_dom(page, "initial_join_dom.html")
            initial_caption_enabled = await try_enable_live_captions(page)
            if initial_caption_enabled:
                logger.info("Live captions enabled immediately after join.")
            else:
                logger.warning("Initial live caption enable attempt did not produce a caption DOM.")

            if audio_recording_enabled and chunk_writer is not None:
                started, audio_result, audio_error = await start_browser_audio_capture(page, chunk_writer)
                if started:
                    audio_capture_started = True
                    update_audio_status(meeting_id, AUDIO_STATUS_RECORDING, None)
                    register_audio_asset(
                        meeting_id,
                        str(get_meeting_master_audio_path(meeting_id, chunk_writer.format)),
                        chunk_writer.format,
                        AUDIO_STATUS_RECORDING,
                    )
                    logger.info("Browser audio capture started: %s", audio_result)
                else:
                    audio_capture_error = audio_error or "Ses kaydı başlatılamadı."
                    logger.warning("Browser audio capture could not start: %s", audio_capture_error)
                    update_audio_status(meeting_id, AUDIO_STATUS_FAILED, audio_capture_error)
                    audio_failure_notified = await send_chat_message(page, CHAT_AUDIO_FAILURE_MESSAGE)

            logger.info("Starting caption monitoring...")
            first_transcript_preview_captured = False
            pending_captions = {}
            poll_count = 0
            caption_discovery_done = False
            caption_enable_attempts = 0
            chat_notice_sent = False
            chat_language_help_sent = False
            chat_notice_attempts = 0
            chat_language_help_attempts = 0
            known_chat_message_ids = set()

            if chat_notice_attempts < 3:
                chat_notice_attempts += 1
                chat_notice_sent = await send_chat_notice(page)
            if chat_language_help_attempts < 3:
                chat_language_help_attempts += 1
                chat_language_help_sent = await send_chat_language_help_notice(page)

            for message in await get_chat_messages(page):
                mid = message.get("mid")
                if mid:
                    known_chat_message_ids.add(mid)

            async def persist_caption_event(caption):
                nonlocal caption_event_sequence, first_transcript_preview_captured

                if not caption:
                    return False

                event_dt = caption.get("last_updated_at") or caption.get("first_seen") or datetime.utcnow()
                action = save_caption_event(
                    meeting_id,
                    caption["speaker"],
                    caption["text"],
                    caption_event_sequence + 1,
                    event_dt,
                    caption.get("slot_index"),
                    caption.get("revision_no", 0),
                )
                if action != "inserted":
                    return False

                caption_event_sequence += 1
                logger.info("CAPTION EVENT - [%s]: %s", caption["speaker"], caption["text"])
                if not first_transcript_preview_captured:
                    try:
                        await page.screenshot(path=meeting_screenshot_path)
                        first_transcript_preview_captured = True
                        logger.info("Captured live meeting preview after first caption event.")
                    except Exception as e:
                        logger.warning("Failed to capture first transcript preview: %s", e)
                return True

            while True:
                try:
                    if shutdown_requested or is_stop_requested(meeting_id):
                        logger.info("Stop requested for meeting %s. Closing bot session.", meeting_id)
                        if not leave_attempted:
                            leave_attempted = True
                            await leave_meeting_via_ui(page)
                        break

                    poll_count += 1
                    now = datetime.now().timestamp()

                    if poll_count % 120 == 0:
                        await dump_dom(page, "monitoring_debug_dom.html")

                    if not caption_discovery_done and poll_count % 60 == 1:
                        logger.info("Running caption element discovery scan...")
                        discovery = await page.evaluate("""() => {
                            const results = {};
                            const patterns = [
                                { name: 'data-tid caption', sel: '[data-tid*="caption"]' },
                                { name: 'class caption', sel: '[class*="caption"]' },
                                { name: 'role log', sel: '[role="log"]' },
                                { name: 'data-tid closed-captions', sel: '[data-tid*="closed-captions"]' },
                            ];
                            for (const p of patterns) {
                                const els = document.querySelectorAll(p.sel);
                                if (els.length > 0) {
                                    results[p.name] = els.length + ' elements, first: ' +
                                        els[0].tagName + ' | text: ' + (els[0].innerText || '').substring(0, 100) +
                                        ' | html: ' + els[0].outerHTML.substring(0, 200);
                                }
                            }
                            return results;
                        }""")
                        if discovery:
                            for pattern, info in discovery.items():
                                logger.info("DISCOVERY [%s]: %s", pattern, info)
                        else:
                            logger.info("Discovery scan: No caption-related elements found yet.")
                        if poll_count > 600:
                            caption_discovery_done = True

                    caption_items = await page.evaluate("""() => {
                        const results = [];
                        const wrapper = document.querySelector('[data-tid="closed-caption-renderer-wrapper"]');

                        if (wrapper) {
                            const captionTexts = wrapper.querySelectorAll('[data-tid="closed-caption-text"]');
                            for (const textEl of captionTexts) {
                                const text = textEl.innerText?.trim();
                                if (!text) continue;

                                let speaker = 'Unknown';
                                let node = textEl.parentElement;
                                for (let i = 0; i < 8 && node && node !== wrapper; i++) {
                                    const fullText = node.innerText?.trim() || '';
                                    if (fullText.length > text.length) {
                                        const lines = fullText.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                                        const textIdx = lines.findIndex(l => l === text || text.startsWith(l) || l.startsWith(text));
                                        if (textIdx > 0) {
                                            const possibleSpeaker = lines[textIdx - 1];
                                            if (possibleSpeaker && possibleSpeaker.length < 50 && possibleSpeaker !== text) {
                                                speaker = possibleSpeaker;
                                                break;
                                            }
                                        }
                                    }
                                    node = node.parentElement;
                                }

                                results.push({ speaker, text, idx: results.length });
                            }
                            return results;
                        }

                        const logs = document.querySelectorAll('[role="log"]');
                        for (const log of logs) {
                            const lines = (log.innerText || '')
                                .split('\\n')
                                .map(line => line.trim())
                                .filter(Boolean);
                            if (lines.length < 2) continue;

                            for (let i = 0; i < lines.length - 1; i += 2) {
                                const speaker = lines[i];
                                const text = lines[i + 1];
                                if (!speaker || !text) continue;
                                results.push({ speaker, text, idx: results.length });
                            }
                        }

                        return results;
                    }""")

                    if not caption_items and caption_enable_attempts < 3 and poll_count >= 10 and poll_count % 60 == 10:
                        caption_enable_attempts += 1
                        enabled = await try_enable_live_captions(page)
                        if enabled:
                            logger.info("Live captions enabled in the bot session.")
                        else:
                            logger.warning("Live captions still not visible after enable attempt.")

                    if not chat_notice_sent and chat_notice_attempts < 3 and poll_count >= 20 and poll_count % 60 == 20:
                        chat_notice_attempts += 1
                        chat_notice_sent = await send_chat_notice(page)

                    if (
                        not chat_language_help_sent
                        and chat_language_help_attempts < 3
                        and poll_count >= 24
                        and poll_count % 60 == 24
                    ):
                        chat_language_help_attempts += 1
                        chat_language_help_sent = await send_chat_language_help_notice(page)

                    if audio_recording_enabled and not audio_capture_started and not audio_failure_notified and poll_count % 20 == 0:
                        audio_failure_notified = await send_chat_message(page, CHAT_AUDIO_FAILURE_MESSAGE)

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

                    seen_caption_slots = set()
                    if caption_items:
                        for item in caption_items:
                            text = normalize_caption_text(item.get("text", ""))
                            speaker = normalize_caption_text(item.get("speaker", "Unknown")) or "Unknown"

                            if not text or len(text) < 2:
                                continue
                            if text in ("Captions are turned on.", "Captions are turned off."):
                                continue

                            slot_key = item.get("idx")
                            if slot_key is None:
                                slot_key = make_caption_fingerprint(speaker, text)
                            seen_caption_slots.add(slot_key)
                            current_dt = datetime.utcnow()
                            previous = pending_captions.get(slot_key)
                            if previous is None:
                                pending_captions[slot_key] = {
                                    "speaker": speaker,
                                    "text": text,
                                    "first_seen": current_dt,
                                    "last_updated_at": current_dt,
                                    "last_seen": now,
                                    "missing_polls": 0,
                                    "slot_index": slot_key if isinstance(slot_key, int) else None,
                                    "revision_no": 0,
                                }
                                await persist_caption_event(pending_captions[slot_key])
                                continue

                            same_caption_stream = (
                                _compatible_speakers_for_merge(previous.get("speaker"), speaker)
                                and _texts_should_merge(previous.get("text"), text)
                            )
                            if same_caption_stream:
                                if make_caption_fingerprint(previous.get("speaker"), previous.get("text")) != make_caption_fingerprint(speaker, text):
                                    previous["revision_no"] = previous.get("revision_no", 0) + 1
                                    previous["speaker"] = speaker
                                    previous["text"] = text
                                    previous["last_updated_at"] = current_dt
                                    previous["last_seen"] = now
                                    previous["missing_polls"] = 0
                                    await persist_caption_event(previous)
                                    continue
                                previous["speaker"] = speaker
                                previous["text"] = text
                                previous["last_updated_at"] = current_dt
                                previous["last_seen"] = now
                                previous["missing_polls"] = 0
                                continue

                            pending_captions[slot_key] = {
                                "speaker": speaker,
                                "text": text,
                                "first_seen": current_dt,
                                "last_updated_at": current_dt,
                                "last_seen": now,
                                "missing_polls": 0,
                                "slot_index": slot_key if isinstance(slot_key, int) else None,
                                "revision_no": 0,
                            }
                            await persist_caption_event(pending_captions[slot_key])

                    keys_to_remove = []
                    for key, caption in pending_captions.items():
                        if key not in seen_caption_slots:
                            caption["missing_polls"] = caption.get("missing_polls", 0) + 1
                        else:
                            caption["missing_polls"] = 0

                        if caption.get("missing_polls", 0) >= 3:
                            keys_to_remove.append(key)
                            continue

                    for key in keys_to_remove:
                        del pending_captions[key]

                except Exception as e:
                    logger.error("Error during caption polling: %s", e, exc_info=True)
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

            logger.info("Caption monitoring loop ended.")
        except Exception as e:
            logger.error("An error occurred during bot execution: %s", e, exc_info=True)
        finally:
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
                    update_audio_status(meeting_id, AUDIO_STATUS_READY, None)
                except Exception as e:
                    audio_capture_error = str(e)
                    logger.error("Failed finalizing meeting audio for %s: %s", meeting_id, e)
                    update_audio_status(meeting_id, AUDIO_STATUS_FAILED, audio_capture_error)
            elif audio_recording_enabled and not audio_capture_started:
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

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        asyncio.run(run_bot(sys.argv[1], sys.argv[2]))
    else:
        print("Usage: python -m backend.workers.bot <url> <id>")
