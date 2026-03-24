from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import sqlite3
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

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
)
from backend.runtime.logging import bind_context, configure_logging, log_event, reset_context  # noqa: E402
from backend.workers.bot_audio import (  # noqa: E402
    MeetingAudioChunkWriter,
    probe_audio_duration_ms,
    install_teams_audio_hook,
    start_browser_audio_capture,
    stop_browser_audio_capture,
)
from backend.workers.bot_participants import (  # noqa: E402
    collect_participant_registry_snapshot,
    flush_speaker_activity,
    install_participant_registry_hook,
    normalize_caption_text,
    sync_speaker_activity,
)
from backend.workers.bot_store import (  # noqa: E402
    finalize_audio_sources,
    get_db_path,
    is_audio_recording_enabled,
    is_stop_requested,
    register_audio_asset,
    register_audio_source,
    trigger_postprocess_worker,
    update_audio_status,
    update_meeting_fields,
    update_meeting_status,
)
from backend.workers.bot_ui import (  # noqa: E402
    CHAT_AUDIO_FAILURE_MESSAGE,
    CHAT_EXIT_ACK_MESSAGE,
    CHAT_EXIT_COMMAND,
    complete_prejoin_join,
    delete_live_meeting_screenshot,
    detect_exit_command,
    get_chat_messages,
    get_live_meeting_screenshot_path,
    launch_teams_browser,
    leave_meeting_via_ui,
    open_meeting_entry,
    open_participant_panel,
    participant_panel_visible,
    send_chat_message,
    send_chat_notice,
    take_periodic_screenshot,
)


configure_logging()
logger = logging.getLogger("notera.worker.bot")
CAPTION_RECENT_SCAN_LIMIT = 24
CAPTION_SLOT_DEDUPE_WINDOW_SECONDS = 20.0
CAPTION_MEETING_DEDUPE_WINDOW_SECONDS = 6.0
CAPTION_MEMORY_WINDOW_SECONDS = 180.0
CAPTION_REPLAY_MIN_AGE_SECONDS = 12.0
CAPTION_REPLAY_BURST_MIN_MATCHES = 4
CAPTION_REPLAY_VISIBLE_WINDOW = 8
CAPTION_LOG_FALLBACK_LINE_WINDOW = 18


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


def caption_token_match(left_token, right_token):
    if left_token == right_token:
        return True

    shorter_length = min(len(left_token), len(right_token))
    if shorter_length >= 2 and (left_token.startswith(right_token) or right_token.startswith(left_token)):
        return True

    if shorter_length >= 4 and SequenceMatcher(None, left_token, right_token).ratio() >= 0.82:
        return True

    return False


def fuzzy_common_prefix_token_count(left_tokens, right_tokens):
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if not caption_token_match(left_token, right_token):
            break
        count += 1
    return count


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

        if delta_seconds <= CAPTION_MEETING_DEDUPE_WINDOW_SECONDS and previous_exact == current_exact:
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


def save_caption_event(meeting_id, speaker, text, sequence_no, observed_at, slot_index, revision_no):
    conn = sqlite3.connect(get_db_path())
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
    except Exception as exc:
        logger.error("Failed to save caption event: %s", exc)
        return "failed"
    finally:
        conn.close()


@dataclass
class BotRunState:
    meeting_id: int
    meeting_screenshot_path: str
    audio_recording_enabled: bool
    chunk_writer: MeetingAudioChunkWriter | None
    shutdown_requested: bool = False
    leave_attempted: bool = False
    audio_capture_started: bool = False
    audio_capture_started_monotonic: float | None = None
    audio_capture_error: str | None = None
    audio_failure_notified: bool = False
    participant_panel_opened: bool = False
    meeting_started_monotonic: float | None = None
    active_speaker_state: dict[str, dict[str, Any]] = field(default_factory=dict)


def _build_run_state(meeting_id: int) -> BotRunState:
    audio_recording_enabled = is_audio_recording_enabled(meeting_id)
    return BotRunState(
        meeting_id=meeting_id,
        meeting_screenshot_path=get_live_meeting_screenshot_path(meeting_id),
        audio_recording_enabled=audio_recording_enabled,
        chunk_writer=MeetingAudioChunkWriter(meeting_id) if audio_recording_enabled else None,
    )


def _initialize_audio_state(state: BotRunState) -> None:
    if state.audio_recording_enabled:
        update_audio_status(state.meeting_id, AUDIO_STATUS_PENDING, None)
        update_meeting_fields(
            state.meeting_id,
            postprocess_status=POSTPROCESS_STATUS_PENDING,
            postprocess_error=None,
        )
        return
    update_audio_status(state.meeting_id, AUDIO_STATUS_DISABLED, None)


def _current_offset_ms(state: BotRunState) -> int:
    activity_origin_monotonic = (
        state.audio_capture_started_monotonic
        or state.meeting_started_monotonic
        or asyncio.get_running_loop().time()
    )
    return int(max(0.0, asyncio.get_running_loop().time() - activity_origin_monotonic) * 1000)


def _install_signal_handlers(state: BotRunState) -> dict[int, Any]:
    def request_shutdown(signum, _frame):
        state.shutdown_requested = True
        logger.info("Received signal %s. Preparing to leave meeting %s.", signum, state.meeting_id)

    previous_handlers: dict[int, Any] = {}
    for current_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            previous_handlers[current_signal] = signal.getsignal(current_signal)
            signal.signal(current_signal, request_shutdown)
        except Exception as exc:
            logger.debug("Could not register signal handler for %s: %s", current_signal, exc)
    return previous_handlers


def _restore_signal_handlers(previous_handlers: dict[int, Any]) -> None:
    for current_signal, previous_handler in previous_handlers.items():
        try:
            signal.signal(current_signal, previous_handler)
        except Exception:
            continue


async def _restore_participant_panel(page, state: BotRunState, reason: str) -> bool:
    state.participant_panel_opened = await open_participant_panel(page)
    if state.participant_panel_opened:
        logger.info("Participant panel restored after %s.", reason)
    else:
        logger.warning("Participant panel could not be restored after %s.", reason)
    return state.participant_panel_opened


async def _join_meeting_session(page, meeting_url: str, state: BotRunState) -> bool:
    await open_meeting_entry(page, meeting_url)
    joined = await complete_prejoin_join(page, state.audio_recording_enabled)
    if not joined:
        return False

    logger.info("Joined successfully! Monitoring participant registry and audio sources...")
    update_meeting_status(state.meeting_id, "active")
    state.meeting_started_monotonic = asyncio.get_running_loop().time()
    await asyncio.sleep(15)
    await page.screenshot(path=state.meeting_screenshot_path)
    logger.info("Post-join screenshot taken. Stopping periodic shots.")
    return True


async def _prepare_participant_monitoring(page, state: BotRunState) -> None:
    await install_participant_registry_hook(page)
    state.participant_panel_opened = await open_participant_panel(page)
    if state.participant_panel_opened:
        logger.info("Participant panel opened successfully.")
    else:
        logger.warning("Participant panel could not be opened immediately after join.")


async def _start_audio_capture_if_enabled(page, state: BotRunState) -> None:
    if not state.audio_recording_enabled or state.chunk_writer is None:
        return

    started, audio_result, audio_error = await start_browser_audio_capture(page, state.chunk_writer, state.meeting_id)
    if started:
        state.audio_capture_started = True
        state.audio_capture_started_monotonic = asyncio.get_running_loop().time()
        capture_started_at = datetime.utcnow().isoformat()
        update_audio_status(state.meeting_id, AUDIO_STATUS_RECORDING, None)
        update_meeting_fields(state.meeting_id, audio_capture_started_at=capture_started_at)
        register_audio_asset(
            state.meeting_id,
            str(state.chunk_writer.chunk_dir.parent / f"master.{state.chunk_writer.format}"),
            state.chunk_writer.format,
            AUDIO_STATUS_RECORDING,
        )
        register_audio_source(
            state.meeting_id,
            "meeting:master",
            "meeting_mixed_master",
            file_path=str(state.chunk_writer.chunk_dir.parent / f"master.{state.chunk_writer.format}"),
            fmt=state.chunk_writer.format,
            status=AUDIO_STATUS_RECORDING,
        )
        logger.info("Browser audio capture started: %s", audio_result)
        return

    state.audio_capture_error = audio_error or "Ses kaydı başlatılamadı."
    logger.warning("Browser audio capture could not start: %s", state.audio_capture_error)
    update_audio_status(state.meeting_id, AUDIO_STATUS_FAILED, state.audio_capture_error)
    state.audio_failure_notified = await send_chat_message(page, CHAT_AUDIO_FAILURE_MESSAGE)
    await _restore_participant_panel(page, state, "audio failure chat message")


async def _bootstrap_chat_state(page, state: BotRunState) -> tuple[bool, int, set[str]]:
    chat_notice_sent = False
    chat_notice_attempts = 0
    known_chat_message_ids: set[str] = set()

    if chat_notice_attempts < 3:
        chat_notice_attempts += 1
        chat_notice_sent = await send_chat_notice(page)
        await _restore_participant_panel(page, state, "initial chat notice")

    for message in await get_chat_messages(page):
        mid = message.get("mid")
        if mid:
            known_chat_message_ids.add(mid)
    await _restore_participant_panel(page, state, "initial chat sync")
    return chat_notice_sent, chat_notice_attempts, known_chat_message_ids


async def _monitor_meeting(page, state: BotRunState) -> None:
    logger.info("Starting participant registry monitoring...")
    poll_count = 0
    chat_notice_sent, chat_notice_attempts, known_chat_message_ids = await _bootstrap_chat_state(page, state)

    while True:
        try:
            if state.shutdown_requested or is_stop_requested(state.meeting_id):
                logger.info("Stop requested for meeting %s. Closing bot session.", state.meeting_id)
                if not state.leave_attempted:
                    state.leave_attempted = True
                    await leave_meeting_via_ui(page)
                break

            poll_count += 1
            current_offset_ms = _current_offset_ms(state)

            if not chat_notice_sent and chat_notice_attempts < 3 and poll_count >= 20 and poll_count % 60 == 20:
                chat_notice_attempts += 1
                chat_notice_sent = await send_chat_notice(page)
                await _restore_participant_panel(page, state, "chat notice retry")

            if (
                state.audio_recording_enabled
                and not state.audio_capture_started
                and not state.audio_failure_notified
                and poll_count % 20 == 0
            ):
                state.audio_failure_notified = await send_chat_message(page, CHAT_AUDIO_FAILURE_MESSAGE)
                await _restore_participant_panel(page, state, "audio failure notice")

            if poll_count % 4 == 0:
                exit_requested, known_chat_message_ids = await detect_exit_command(page, known_chat_message_ids)
                if exit_requested:
                    logger.info(
                        "Leaving meeting %s because a user sent the '%s' chat command.",
                        state.meeting_id,
                        CHAT_EXIT_COMMAND,
                    )
                    await send_chat_message(page, CHAT_EXIT_ACK_MESSAGE)
                    if not state.leave_attempted:
                        state.leave_attempted = True
                        await leave_meeting_via_ui(page)
                    break
                await _restore_participant_panel(page, state, "exit command check")

            if not state.participant_panel_opened or not await participant_panel_visible(page):
                if poll_count % 8 == 1:
                    state.participant_panel_opened = await open_participant_panel(page)
                    if state.participant_panel_opened:
                        logger.info("Participant panel reopened.")

            if poll_count == 1 or poll_count % 4 == 0:
                observed_at = datetime.utcnow()
                participant_items = await collect_participant_registry_snapshot(page, observed_at)
                if not participant_items:
                    restored = await _restore_participant_panel(page, state, "empty participant snapshot")
                    if restored:
                        await asyncio.sleep(0.4)
                        participant_items = await collect_participant_registry_snapshot(page, observed_at)
                if participant_items:
                    if poll_count == 1 or poll_count % 40 == 0:
                        logger.info(
                            "Participant registry snapshot collected for meeting %s: %s participants",
                            state.meeting_id,
                            len(participant_items),
                        )
                    sync_speaker_activity(
                        state.meeting_id,
                        participant_items,
                        state.active_speaker_state,
                        current_offset_ms,
                    )
                    if poll_count == 1 or poll_count % 40 == 0:
                        active_claim_count = sum(1 for item in participant_items if item.get("is_speaking"))
                        logger.info(
                            "Participant telemetry meeting=%s participant_count=%s active_claim_count=%s observer_attached=%s",
                            state.meeting_id,
                            len(participant_items),
                            active_claim_count,
                            True,
                        )
                elif poll_count % 40 == 0:
                    logger.warning("Participant registry snapshot is empty for meeting %s.", state.meeting_id)

        except Exception as exc:
            logger.error("Error during participant registry polling: %s", exc, exc_info=True)
            try:
                await page.evaluate("1+1")
            except Exception:
                logger.error("Page disconnected! Meeting may have ended.")
                break

        await asyncio.sleep(0.5)

    logger.info("Participant registry monitoring loop ended.")


async def _finalize_audio_capture(state: BotRunState) -> None:
    if state.audio_capture_started and state.chunk_writer is not None:
        try:
            master_audio_path, pcm_audio_path, audio_format, duration_ms = state.chunk_writer.finalize()
            register_audio_asset(
                state.meeting_id,
                str(master_audio_path),
                audio_format,
                AUDIO_STATUS_READY,
                duration_ms=duration_ms,
                pcm_audio_path=str(pcm_audio_path) if pcm_audio_path else None,
                postprocess_version="whisperx_global_v1",
            )
            register_audio_source(
                state.meeting_id,
                "meeting:master",
                "meeting_mixed_master",
                file_path=str(master_audio_path),
                fmt=audio_format,
                status=AUDIO_STATUS_READY,
            )
            finalize_audio_sources(state.meeting_id, status=AUDIO_STATUS_READY)
            update_audio_status(state.meeting_id, AUDIO_STATUS_READY, None)
        except Exception as exc:
            state.audio_capture_error = str(exc)
            logger.error("Failed finalizing meeting audio for %s: %s", state.meeting_id, exc)
            finalize_audio_sources(state.meeting_id, status=AUDIO_STATUS_FAILED)
            update_audio_status(state.meeting_id, AUDIO_STATUS_FAILED, state.audio_capture_error)
    elif state.audio_recording_enabled and not state.audio_capture_started:
        finalize_audio_sources(state.meeting_id, status=AUDIO_STATUS_FAILED)
        update_audio_status(
            state.meeting_id,
            AUDIO_STATUS_FAILED,
            state.audio_capture_error or "Ses kaydı başlatılamadı.",
        )


async def run_bot(meeting_url, meeting_id):
    meeting_id = int(meeting_id)
    run_id_value = os.getenv("NOTERA_WORKER_RUN_ID")
    run_id = int(run_id_value) if run_id_value and run_id_value.isdigit() else run_id_value
    context_token = bind_context(meeting_id=meeting_id, worker_type="bot", run_id=run_id)
    ensure_runtime_schema(get_db_path())
    log_event(logger, logging.INFO, "worker.started", "Bot worker started")

    state = _build_run_state(meeting_id)
    _initialize_audio_state(state)
    previous_handlers = _install_signal_handlers(state)

    browser = None
    page = None
    stop_screenshots = None
    screenshot_task = None

    async with async_playwright() as playwright:
        try:
            logger.info("Launching browser...")
            browser = await launch_teams_browser(playwright)
            context = await browser.new_context(
                permissions=["microphone"],
                bypass_csp=True,
            )
            await install_teams_audio_hook(context)
            page = await context.new_page()

            stop_screenshots = asyncio.Event()
            screenshot_task = asyncio.create_task(
                take_periodic_screenshot(page, stop_screenshots, state.meeting_screenshot_path)
            )

            joined = await _join_meeting_session(page, meeting_url, state)
            if not joined:
                return

            stop_screenshots.set()
            await _prepare_participant_monitoring(page, state)
            await _start_audio_capture_if_enabled(page, state)
            await _monitor_meeting(page, state)
        except Exception as exc:
            logger.error("An error occurred during bot execution: %s", exc, exc_info=True)
        finally:
            activity_origin_monotonic = state.audio_capture_started_monotonic or state.meeting_started_monotonic
            if activity_origin_monotonic is not None:
                current_offset_ms = int(max(0.0, asyncio.get_running_loop().time() - activity_origin_monotonic) * 1000)
                flush_speaker_activity(state.meeting_id, state.active_speaker_state, current_offset_ms)

            if stop_screenshots is not None:
                stop_screenshots.set()
            if screenshot_task is not None:
                await screenshot_task

            if page is not None and not page.is_closed() and state.audio_capture_started:
                stop_result = await stop_browser_audio_capture(page)
                if not stop_result.get("ok"):
                    logger.warning("Browser audio recorder stop failed: %s", stop_result.get("error"))
                elif state.chunk_writer is not None:
                    state.chunk_writer.stop_accepting_writes()
                await asyncio.sleep(0.25)

            if page is not None and not page.is_closed() and (state.shutdown_requested or is_stop_requested(state.meeting_id)) and not state.leave_attempted:
                try:
                    state.leave_attempted = True
                    await leave_meeting_via_ui(page)
                except Exception as exc:
                    logger.debug("Failed final leave attempt: %s", exc)

            if browser is not None:
                logger.info("Closing browser.")
                await browser.close()

            await _finalize_audio_capture(state)
            update_meeting_status(state.meeting_id, "completed", clear_bot_pid=True)
            update_meeting_fields(state.meeting_id, ended_at=datetime.utcnow().isoformat())
            trigger_postprocess_worker(state.meeting_id)
            delete_live_meeting_screenshot(state.meeting_screenshot_path)
            _restore_signal_handlers(previous_handlers)
            log_event(logger, logging.INFO, "worker.completed", "Bot worker finished")
            reset_context(context_token)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        asyncio.run(run_bot(sys.argv[1], sys.argv[2]))
    else:
        print("Usage: python -m backend.workers.bot <url> <id>")
