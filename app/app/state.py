import asyncio
import hashlib
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from difflib import SequenceMatcher
from typing import List, Optional

import reflex as rx
from pydantic import BaseModel
from sqlmodel import select

from .meeting_runtime import (
    AUDIO_STATUS_DISABLED,
    AUDIO_STATUS_FAILED,
    AUDIO_STATUS_PENDING,
    AUDIO_STATUS_READY,
    AUDIO_STATUS_RECORDING,
    POSTPROCESS_STATUS_ALIGNING,
    POSTPROCESS_STATUS_CANONICALIZING,
    POSTPROCESS_STATUS_COMPLETED,
    POSTPROCESS_STATUS_FAILED,
    POSTPROCESS_STATUS_PENDING,
    POSTPROCESS_STATUS_QUEUED,
    POSTPROCESS_STATUS_REBUILDING,
    POSTPROCESS_STATUS_REVIEW_READY,
    POSTPROCESS_STATUS_RUNNING,
    POSTPROCESS_STATUS_TRANSCRIBING,
    REVIEW_STATUS_ACCEPTED,
    REVIEW_STATUS_PENDING,
    REVIEW_STATUS_REJECTED,
    TRANSCRIPT_STATUS_ACCEPTED,
    TRANSCRIPT_STATUS_PENDING_REVIEW,
    TRANSCRIPT_STATUS_REJECTED,
    cleanup_meeting_artifacts,
    ensure_runtime_schema,
    get_db_path,
    get_meeting_pcm_audio_path,
    get_public_meeting_audio_src,
    get_review_clip_src,
    sync_public_meeting_audio,
)
from .models import Meeting, MeetingAudioAsset, TeamsCaptionEvent, Transcript, TranscriptReviewItem, User

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("State")

def _format_relative_time(value: Optional[datetime]) -> str:
    """Return a compact human-readable relative time label."""
    if not value:
        return "Henüz aktivite yok"

    if value.tzinfo is not None:
        value = value.replace(tzinfo=None)

    seconds = max(int((datetime.utcnow() - value).total_seconds()), 0)
    if seconds < 60:
        return "Az önce"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} dk önce"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} sa önce"
    if seconds < 604800:
        days = seconds // 86400
        return f"{days} gün önce"
    return value.strftime("%d.%m.%Y")


def _meeting_status_label(status: str) -> str:
    return {
        "pending": "Hazır",
        "joining": "Bağlanıyor",
        "active": "Canlı",
        "completed": "Tamamlandı",
    }.get(status, "Beklemede")


def _audio_status_label(status: str) -> str:
    return {
        AUDIO_STATUS_DISABLED: "Ses kapalı",
        AUDIO_STATUS_PENDING: "Ses hazırlanıyor",
        AUDIO_STATUS_RECORDING: "Ses kaydediliyor",
        AUDIO_STATUS_READY: "Ses hazır",
        AUDIO_STATUS_FAILED: "Ses kaydı alınamadı",
    }.get(status, "Ses durumu bilinmiyor")


def _postprocess_status_label(status: str) -> str:
    return {
        POSTPROCESS_STATUS_PENDING: "Doğrulama bekliyor",
        POSTPROCESS_STATUS_QUEUED: "Doğrulama sırada",
        POSTPROCESS_STATUS_RUNNING: "WhisperX işleniyor",
        POSTPROCESS_STATUS_TRANSCRIBING: "WhisperX transcript çıkarıyor",
        POSTPROCESS_STATUS_CANONICALIZING: "Teams transcript temizleniyor",
        POSTPROCESS_STATUS_ALIGNING: "Transcriptler hizalanıyor",
        POSTPROCESS_STATUS_REBUILDING: "Final transcript hazırlanıyor",
        POSTPROCESS_STATUS_REVIEW_READY: "Review kararları hazır",
        POSTPROCESS_STATUS_COMPLETED: "Doğrulama tamamlandı",
        POSTPROCESS_STATUS_FAILED: "Doğrulama başarısız",
    }.get(status, "Doğrulama durumu bilinmiyor")


def _normalize_transcript_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _transcript_tokens(value: str | None) -> list[str]:
    return re.findall(r"[\wçğıöşüÇĞİÖŞÜ'-]+", _normalize_transcript_text(value).casefold())


def _common_prefix_token_count(left_tokens: list[str], right_tokens: list[str]) -> int:
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if left_token != right_token:
            break
        count += 1
    return count


def _transcript_token_match(left_token: str, right_token: str) -> bool:
    if left_token == right_token:
        return True
    shorter_length = min(len(left_token), len(right_token))
    if shorter_length >= 2 and (left_token.startswith(right_token) or right_token.startswith(left_token)):
        return True
    if shorter_length >= 4 and SequenceMatcher(None, left_token, right_token).ratio() >= 0.82:
        return True
    return False


def _fuzzy_common_prefix_token_count(left_tokens: list[str], right_tokens: list[str]) -> int:
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if not _transcript_token_match(left_token, right_token):
            break
        count += 1
    return count


def _transcript_base_text(transcript: Transcript) -> str:
    return _normalize_transcript_text(getattr(transcript, "teams_text", None) or transcript.text)


def _transcript_revision_score(text: str | None) -> int:
    normalized = _normalize_transcript_text(text)
    if not normalized:
        return -9999
    score = len(normalized)
    if normalized.endswith((".", "!", "?", "…")):
        score += 24
    score += normalized.count(".") * 4
    score += normalized.count("?") * 4
    score += normalized.count("!") * 4
    tokens = _transcript_tokens(normalized)
    if tokens and len(tokens[-1]) <= 2:
        score -= 8
    return score


def _choose_preferred_transcript_text(existing_text: str | None, new_text: str | None) -> str:
    old_text = _normalize_transcript_text(existing_text)
    new_value = _normalize_transcript_text(new_text)
    if not old_text:
        return new_value
    if not new_value:
        return old_text

    old_tokens = _transcript_tokens(old_text)
    new_tokens = _transcript_tokens(new_value)
    shared_prefix = _common_prefix_token_count(old_tokens, new_tokens)
    shorter_length = min(len(old_tokens), len(new_tokens))

    if shorter_length and shared_prefix >= min(4, shorter_length) and shared_prefix / max(shorter_length, 1) >= 0.75:
        if len(new_tokens) > len(old_tokens):
            return new_value
        if len(new_tokens) < len(old_tokens):
            if _transcript_revision_score(new_value) >= _transcript_revision_score(old_text):
                return new_value
            return old_text

    if _transcript_revision_score(new_value) > _transcript_revision_score(old_text):
        return new_value
    return old_text


def _compatible_transcript_speakers(existing_speaker: str | None, new_speaker: str | None) -> bool:
    existing_value = _normalize_transcript_text(existing_speaker).casefold()
    new_value = _normalize_transcript_text(new_speaker).casefold()
    if not existing_value or existing_value == "unknown":
        return True
    if not new_value or new_value == "unknown":
        return True
    return existing_value == new_value


def _transcript_texts_should_merge(existing_text: str | None, new_text: str | None) -> bool:
    old_text = _normalize_transcript_text(existing_text)
    new_value = _normalize_transcript_text(new_text)
    if not old_text or not new_value:
        return False

    old_fold = old_text.casefold()
    new_fold = new_value.casefold()
    if old_fold == new_fold:
        return True

    punctuation = ".,!?;:…"
    if new_fold.rstrip(punctuation) == old_fold.rstrip(punctuation):
        return True

    old_tokens = _transcript_tokens(old_text)
    new_tokens = _transcript_tokens(new_value)
    if old_tokens and new_tokens:
        shared_prefix = _fuzzy_common_prefix_token_count(old_tokens, new_tokens)
        shorter_length = min(len(old_tokens), len(new_tokens))
        if shared_prefix >= min(3, shorter_length) and shared_prefix / max(shorter_length, 1) >= 0.6:
            return True

    return (
        SequenceMatcher(None, old_fold, new_fold).ratio() >= 0.82
        and abs(len(new_fold) - len(old_fold)) <= 96
    )


def _transcript_sort_time(transcript: Transcript) -> datetime | None:
    return transcript.caption_finalized_at or transcript.timestamp or transcript.caption_started_at


def _transcript_merge_time(transcript: Transcript) -> datetime | None:
    return transcript.caption_started_at or transcript.timestamp or transcript.caption_finalized_at


def _collapse_rotating_duplicate_transcripts(
    transcripts: list[Transcript],
    rotation_window_seconds: int = 20,
) -> list[Transcript]:
    filtered: list[Transcript] = []
    last_index_by_key: dict[tuple[str, str], int] = {}

    for raw_index, transcript in enumerate(transcripts):
        text_key = _normalize_transcript_text(transcript.text)
        speaker_key = _normalize_transcript_text(transcript.speaker).casefold()
        content_key = (speaker_key, text_key.casefold())
        current_time = _transcript_sort_time(transcript)

        previous_index = last_index_by_key.get(content_key)
        should_skip = False
        if previous_index is not None and current_time is not None:
            previous_transcript = transcripts[previous_index]
            previous_time = _transcript_sort_time(previous_transcript)
            if previous_time is not None:
                delta_seconds = (current_time - previous_time).total_seconds()
                distinct_between = {
                    (
                        _normalize_transcript_text(item.speaker).casefold(),
                        _normalize_transcript_text(item.text).casefold(),
                    )
                    for item in transcripts[previous_index + 1 : raw_index]
                    if _normalize_transcript_text(item.text)
                }
                distinct_between.discard(content_key)
                if 0 <= delta_seconds <= rotation_window_seconds and len(distinct_between) >= 1:
                    should_skip = True

        last_index_by_key[content_key] = raw_index
        if not should_skip:
            filtered.append(transcript)

    collapsed: list[Transcript] = []
    for transcript in filtered:
        current_text = _transcript_base_text(transcript)
        if not collapsed:
            collapsed.append(transcript)
            continue

        previous_transcript = collapsed[-1]
        previous_time = _transcript_merge_time(previous_transcript)
        current_time = _transcript_merge_time(transcript)
        if previous_time is not None and current_time is not None:
            delta_seconds = (current_time - previous_time).total_seconds()
            if (
                0 <= delta_seconds <= 8
                and _compatible_transcript_speakers(previous_transcript.speaker, transcript.speaker)
                and _transcript_texts_should_merge(
                    _transcript_base_text(previous_transcript),
                    current_text,
                )
            ):
                collapsed[-1] = transcript
                continue

        collapsed.append(transcript)

    return collapsed


def _collapse_live_caption_events(events: list[TeamsCaptionEvent]) -> list["TranscriptEntry"]:
    collapsed: list[dict] = []

    for event in events:
        text = _normalize_transcript_text(event.text)
        if len(text) < 2:
            continue
        speaker = _normalize_transcript_text(event.speaker_name) or "Unknown"
        observed_at = event.observed_at or datetime.utcnow()

        if collapsed:
            previous = collapsed[-1]
            delta_seconds = (
                (observed_at - previous["observed_at"]).total_seconds()
                if previous["observed_at"] and observed_at
                else 999
            )
            if (
                0 <= delta_seconds <= 8
                and _compatible_transcript_speakers(previous["speaker"], speaker)
                and _transcript_texts_should_merge(previous["text"], text)
            ):
                previous["speaker"] = speaker if speaker.casefold() != "unknown" else previous["speaker"]
                previous["text"] = _choose_preferred_transcript_text(previous["text"], text)
                previous["observed_at"] = observed_at
                previous["id"] = event.id
                continue

        collapsed.append(
            {
                "id": event.id,
                "speaker": speaker,
                "text": text,
                "observed_at": observed_at,
            }
        )

    return [
        TranscriptEntry(
            id=item["id"],
            speaker=item["speaker"],
            text=item["text"],
            timestamp=item["observed_at"].strftime("%H:%M:%S") if item["observed_at"] else "",
            initials="",
            color="",
            resolution_status="original",
            auto_corrected=False,
        )
        for item in collapsed
    ]


def _is_process_running(pid: Optional[int]) -> bool:
    """Check whether a process id still exists."""
    if not pid:
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _bot_stop_flag_path(meeting_id: int) -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "bot" / f"stop_{meeting_id}.flag"


def _meeting_preview_dir() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    preview_dir = repo_root / "app" / "assets" / "live_meeting_frames"
    preview_dir.mkdir(parents=True, exist_ok=True)
    return preview_dir


def _meeting_preview_name(user_id: int, meeting_id: int) -> str:
    return f"user_{user_id}_meeting_{meeting_id}.png"


def _meeting_preview_path(user_id: int, meeting_id: int) -> Path:
    return _meeting_preview_dir() / _meeting_preview_name(user_id, meeting_id)


def _meeting_preview_src(user_id: int, meeting_id: int, version: int) -> str:
    return f"/live_meeting_frames/{_meeting_preview_name(user_id, meeting_id)}?v={version}"


def _remove_meeting_preview(user_id: int, meeting_id: int):
    preview_path = _meeting_preview_path(user_id, meeting_id)
    preview_path.unlink(missing_ok=True)


def _ensure_runtime_schema():
    ensure_runtime_schema(get_db_path())


def _terminate_bot_process(pid: Optional[int], timeout_seconds: float = 6.0) -> bool:
    """Terminate a bot process gracefully, then force kill if needed."""
    if not pid or not _is_process_running(pid):
        return True

    try:
        process_group_id = os.getpgid(pid)
    except ProcessLookupError:
        return True
    except Exception:
        process_group_id = None

    attempts = (
        (signal.SIGTERM, timeout_seconds / 2),
        (signal.SIGKILL, timeout_seconds / 2),
    )

    for current_signal, wait_seconds in attempts:
        delivered = False

        if process_group_id is not None:
            try:
                os.killpg(process_group_id, current_signal)
                delivered = True
                logger.info(
                    "Sent %s to bot process group pgid=%s",
                    current_signal.name,
                    process_group_id,
                )
            except ProcessLookupError:
                return True
            except Exception as exc:
                logger.warning(
                    "Failed sending %s to process group pgid=%s: %s",
                    current_signal.name,
                    process_group_id,
                    exc,
                )

        if not delivered:
            try:
                os.kill(pid, current_signal)
                delivered = True
                logger.info("Sent %s to bot pid=%s", current_signal.name, pid)
            except ProcessLookupError:
                return True
            except Exception as exc:
                logger.warning(
                    "Failed sending %s to pid=%s: %s",
                    current_signal.name,
                    pid,
                    exc,
                )

        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            if not _is_process_running(pid):
                return True
            time.sleep(0.25)

    return not _is_process_running(pid)


def _wait_for_process_exit(pid: Optional[int], timeout_seconds: float) -> bool:
    """Wait briefly for a process to stop on its own."""
    if not pid:
        return True

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _is_process_running(pid):
            return True
        time.sleep(0.25)

    return not _is_process_running(pid)


def _stop_meeting_operation(meeting_id: int):
    """Stop a running meeting bot and finalize the session state."""
    with rx.session() as session:
        meeting = session.exec(
            select(Meeting).where(Meeting.id == meeting_id)
        ).first()
        if not meeting:
            return False, rx.toast.error("Toplantı bulunamadı.", position="top-right")

        stop_flag_path = _bot_stop_flag_path(meeting.id)
        stop_flag_path.write_text(
            datetime.utcnow().isoformat(),
            encoding="utf-8",
        )

        process_stopped = _wait_for_process_exit(meeting.bot_pid, timeout_seconds=8.0)
        if meeting.bot_pid and not process_stopped:
            process_stopped = _terminate_bot_process(meeting.bot_pid)

        meeting.status = "completed"
        _remove_meeting_preview(meeting.user_id, meeting.id)
        if process_stopped:
            meeting.bot_pid = None
            toast_event = rx.toast.warning(
                "Bot toplantıdan çıkarıldı. Oturum tamamlandı.",
                position="top-right",
            )
        else:
            logger.error(
                "Bot process for meeting %s did not stop cleanly (pid=%s).",
                meeting.id,
                meeting.bot_pid,
            )
            toast_event = rx.toast.warning(
                "Durdurma sinyali gönderildi. Bot ayrıldığında oturum tamamlanmış olacak.",
                position="top-right",
            )

        session.add(meeting)
        session.commit()
        return True, toast_event


def _delete_meeting_operation(meeting_id: int):
    with rx.session() as session:
        meeting = session.exec(
            select(Meeting).where(Meeting.id == meeting_id)
        ).first()
        if not meeting:
            return False

        transcript_ids = [
            transcript.id
            for transcript in session.exec(
                select(Transcript).where(Transcript.meeting_id == meeting_id)
            ).all()
        ]
        if transcript_ids:
            review_items = session.exec(
                select(TranscriptReviewItem).where(
                    TranscriptReviewItem.transcript_id.in_(transcript_ids)
                )
            ).all()
            for review_item in review_items:
                session.delete(review_item)

        audio_assets = session.exec(
            select(MeetingAudioAsset).where(MeetingAudioAsset.meeting_id == meeting_id)
        ).all()
        for audio_asset in audio_assets:
            session.delete(audio_asset)

        caption_events = session.exec(
            select(TeamsCaptionEvent).where(TeamsCaptionEvent.meeting_id == meeting_id)
        ).all()
        for caption_event in caption_events:
            session.delete(caption_event)

        transcripts = session.exec(
            select(Transcript).where(Transcript.meeting_id == meeting_id)
        ).all()
        for transcript in transcripts:
            session.delete(transcript)

        _remove_meeting_preview(meeting.user_id, meeting.id)
        cleanup_meeting_artifacts(meeting_id)
        session.delete(meeting)
        session.commit()
        return True

class State(rx.State):
    """The app state."""
    user: Optional[User] = None
    email: str = ""
    error_message: str = ""
    is_logged_in: bool = False

    def _set_authenticated_user(self, user_id: int, email: str):
        """Store a session-independent snapshot of the authenticated user."""
        self.user = User(id=user_id, email=email)
        self.is_logged_in = True
        self.error_message = ""
    
    def cleanup(self):
        """Reconcile stale meetings without touching active bot processes."""
        _ensure_runtime_schema()
        with rx.session() as session:
            meetings = session.exec(select(Meeting)).all()

            for meeting in meetings:
                if meeting.status in {"joining", "active"}:
                    if _is_process_running(meeting.bot_pid):
                        logger.info(
                            "Cleanup: keeping meeting %s as %s (pid=%s alive)",
                            meeting.id,
                            meeting.status,
                            meeting.bot_pid,
                        )
                        continue

                    logger.warning(
                        "Cleanup: marking stale meeting %s from %s to completed (pid=%s missing)",
                        meeting.id,
                        meeting.status,
                        meeting.bot_pid,
                    )
                    meeting.status = "completed"
                    meeting.bot_pid = None
                    _remove_meeting_preview(meeting.user_id, meeting.id)
                    session.add(meeting)
                    continue
    
                if meeting.status == "completed" and meeting.bot_pid and not _is_process_running(meeting.bot_pid):
                    meeting.bot_pid = None
                    _remove_meeting_preview(meeting.user_id, meeting.id)
                    session.add(meeting)

            session.commit()

    def check_login(self):
        logger.info(f"Checking login... is_logged_in: {self.is_logged_in}, user: {self.user.email if self.user else 'None'}")
        if not self.is_logged_in:
            return rx.redirect("/")

    def set_email(self, email: str):
        self.email = email

    def login(self):
        """Log in a user."""
        if not self.email:
            self.error_message = "E-posta gereklidir."
            return

        with rx.session() as session:
            user = session.exec(
                select(User).where(User.email == self.email)
            ).first()
            
            if user:
                self._set_authenticated_user(user.id, user.email)
                return rx.redirect("/dashboard")
            else:
                self.error_message = "Bu e-posta adresiyle kayıtlı kullanıcı bulunamadı."

    def register(self):
        """Register a new user."""
        if not self.email:
            self.error_message = "E-posta gereklidir."
            return

        with rx.session() as session:
            existing_user = session.exec(
                select(User).where(User.email == self.email)
            ).first()
            if existing_user:
                self.error_message = "Bu e-posta adresi zaten kullanımda."
                return

            new_user = User(
                email=self.email,
            )
            session.add(new_user)
            session.commit()
            session.refresh(new_user)
            self._set_authenticated_user(new_user.id, new_user.email)
            return rx.redirect("/dashboard")

    def logout(self):
        """Log out the user."""
        self.user = None
        self.is_logged_in = False
        return rx.redirect("/")

    @rx.var
    def logged_in_email(self) -> str:
        return self.user.email if self.user else ""

class IndexState(State):
    """State specific to the index (login) page."""
    def on_load(self):
        self.cleanup()
        if self.is_logged_in:
            return rx.redirect("/dashboard")

class DashboardState(State):
    """Dashboard state."""
    meetings: List[Meeting] = []
    new_meeting_title: str = ""
    new_meeting_link: str = ""
    new_meeting_audio_recording_enabled: bool = True
    transcript_entry_count: int = 0
    meetings_with_transcripts: int = 0
    latest_activity_label: str = "Henüz aktivite yok"
    latest_activity_title: str = "Yeni toplantı ekleyin"
    busy_meeting_id: int = 0
    busy_action: str = ""
    live_updates_enabled: bool = False

    def on_load(self):
        """Dashboard specific load logic."""
        self.cleanup()
        return self.check_login() or self.load_meetings()

    def page_mount(self):
        """Initialize dashboard data and start live updates."""
        return [DashboardState.on_load, DashboardState.poll_meetings]

    def stop_live_updates(self):
        """Stop dashboard polling when leaving the page."""
        self.live_updates_enabled = False

    def set_new_meeting_title(self, title: str):
        self.new_meeting_title = title

    def set_new_meeting_link(self, link: str):
        self.new_meeting_link = link

    def set_new_meeting_audio_recording_enabled(self, value: bool):
        self.new_meeting_audio_recording_enabled = bool(value)

    @rx.event(background=True)
    async def poll_meetings(self):
        """Keep meeting statuses in sync without a full page refresh."""
        async with self:
            if self.live_updates_enabled:
                return
            self.live_updates_enabled = True

        try:
            while True:
                async with self:
                    if not self.live_updates_enabled:
                        break

                    if not self.is_logged_in:
                        self.live_updates_enabled = False
                        break

                    self.load_meetings()

                await asyncio.sleep(1)
        finally:
            async with self:
                self.live_updates_enabled = False

    def load_meetings(self):
        if not self.is_logged_in:
            return
        with rx.session() as session:
            self.meetings = session.exec(
                select(Meeting)
                .where(Meeting.user_id == self.user.id)
                .order_by(Meeting.created_at.desc())
            ).all()

            meeting_ids = [meeting.id for meeting in self.meetings]
            transcript_entries = []
            if meeting_ids:
                transcript_entries = session.exec(
                    select(Transcript).where(Transcript.meeting_id.in_(meeting_ids))
                ).all()

            self.transcript_entry_count = len(transcript_entries)
            self.meetings_with_transcripts = len(
                {entry.meeting_id for entry in transcript_entries}
            )

            if self.meetings:
                latest_meeting = self.meetings[0]
                self.latest_activity_title = latest_meeting.title
                self.latest_activity_label = _format_relative_time(
                    latest_meeting.created_at
                )
            else:
                self.latest_activity_title = "Yeni toplantı ekleyin"
                self.latest_activity_label = "Henüz aktivite yok"

    @rx.var
    def total_meetings(self) -> int:
        return len(self.meetings)

    @rx.var
    def live_meeting_count(self) -> int:
        return sum(
            1 for meeting in self.meetings if meeting.status in {"joining", "active"}
        )

    @rx.var
    def standby_meeting_count(self) -> int:
        return sum(1 for meeting in self.meetings if meeting.status == "pending")

    @rx.var
    def archived_meeting_count(self) -> int:
        return self.meetings_with_transcripts

    @rx.var
    def operations_summary(self) -> str:
        if self.live_meeting_count:
            return f"{self.live_meeting_count} canlı operasyon izleniyor"
        if self.total_meetings:
            return "Yeni toplantılar eklendiğinde bot otomatik olarak katılır"
        return "İlk toplantınızı oluşturarak operasyon merkezini aktif edin"

    @rx.var
    def readiness_label(self) -> str:
        if self.total_meetings == 0:
            return "Kurulum bekleniyor"
        if self.live_meeting_count:
            return "Canlı izleme açık"
        return "Yeni toplantıya hazır"

    def _start_bot_for_meeting(self, session, meeting: Meeting):
        if meeting.status == "completed":
            return False, rx.toast.warning(
                "Bu toplantı oturumu tamamlandı. Aynı oturum yeniden başlatılamaz.",
                position="top-right",
            )

        if meeting.status in {"joining", "active"} and _is_process_running(meeting.bot_pid):
            return False, rx.toast.warning(
                "Bu toplantı için bot zaten çalışıyor.",
                position="top-right",
            )

        meeting.status = "joining"
        session.add(meeting)
        session.commit()

        stop_flag_path = _bot_stop_flag_path(meeting.id)
        if stop_flag_path.exists():
            stop_flag_path.unlink(missing_ok=True)

        bot_path = os.path.join(os.getcwd(), "..", "bot", "bot.py")
        python_executable = sys.executable
        bot_log_path = os.path.join(os.path.dirname(bot_path), "bot.log")

        try:
            with open(bot_log_path, "a", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    [
                        python_executable,
                        bot_path,
                        meeting.teams_link,
                        str(meeting.id),
                    ],
                    stdout=log_file,
                    stderr=log_file,
                    start_new_session=True,
                )
        except Exception as exc:
            logger.exception("Bot process could not be started for meeting %s", meeting.id)
            meeting.status = "pending"
            meeting.bot_pid = None
            session.add(meeting)
            session.commit()
            return False, rx.toast.error(
                f"Bot başlatılamadı: {exc}",
                position="top-right",
            )

        meeting.bot_pid = process.pid
        session.add(meeting)
        session.commit()

        logger.info(
            "Bot process started for meeting %s with pid=%s",
            meeting.id,
            process.pid,
        )
        return True, rx.toast.info(
            "Bot toplantıya katılmak için başlatıldı.",
            position="top-right",
        )

    def add_meeting(self):
        if not self.is_logged_in:
            return

        toast_event = None
        should_reload = False

        with rx.session() as session:
            new_meeting = Meeting(
                user_id=self.user.id,
                title=self.new_meeting_title,
                teams_link=self.new_meeting_link,
                audio_recording_enabled=self.new_meeting_audio_recording_enabled,
                audio_status=(
                    AUDIO_STATUS_PENDING
                    if self.new_meeting_audio_recording_enabled
                    else AUDIO_STATUS_DISABLED
                ),
                postprocess_status=POSTPROCESS_STATUS_PENDING,
            )
            session.add(new_meeting)
            session.commit()
            session.refresh(new_meeting)

            started, toast_event = self._start_bot_for_meeting(session, new_meeting)
            should_reload = True

            if started:
                self.new_meeting_title = ""
                self.new_meeting_link = ""
                self.new_meeting_audio_recording_enabled = True
            else:
                session.delete(new_meeting)
                session.commit()

        if should_reload:
            self.load_meetings()

        if toast_event is not None:
            yield toast_event

    def _begin_meeting_action(self, meeting_id: int, action: str):
        self.busy_meeting_id = meeting_id
        self.busy_action = action

    def _finish_meeting_action(self):
        self.busy_meeting_id = 0
        self.busy_action = ""

    def join_meeting(self, meeting_id: int):
        """Trigger the bot to join the meeting."""
        self._begin_meeting_action(meeting_id, "join")
        yield

        toast_event = None
        should_reload = False

        try:
            with rx.session() as session:
                meeting = session.exec(
                    select(Meeting).where(Meeting.id == meeting_id)
                ).first()
                if not meeting:
                    toast_event = rx.toast.error("Toplantı bulunamadı.", position="top-right")
                else:
                    _, toast_event = self._start_bot_for_meeting(session, meeting)
                    should_reload = True
        finally:
            if should_reload:
                self.load_meetings()
            self._finish_meeting_action()

        if toast_event is not None:
            yield toast_event

    def view_transcripts(self, meeting_id: int):
        """Navigate to transcript page."""
        return rx.redirect(f"/transcripts/{meeting_id}")

    def leave_meeting(self, meeting_id: int):
        """Stop the bot process and update meeting status."""
        self._begin_meeting_action(meeting_id, "leave")
        yield

        toast_event = None
        should_reload = False

        try:
            should_reload, toast_event = _stop_meeting_operation(meeting_id)
        finally:
            if should_reload:
                self.load_meetings()
            self._finish_meeting_action()

        if toast_event is not None:
            yield toast_event

    def delete_meeting(self, meeting_id: int):
        """Delete a meeting and its transcripts."""
        if _delete_meeting_operation(meeting_id):
            self.load_meetings()


class TranscriptEntry(BaseModel):
    """A single transcript entry for the UI."""
    id: int
    speaker: str
    text: str
    timestamp: str
    initials: str
    color: str
    resolution_status: str
    auto_corrected: bool


class ReviewItemEntry(BaseModel):
    """A review suggestion entry for the UI."""
    id: int
    transcript_id: int
    speaker: str
    timestamp: str
    granularity: str
    current_text: str
    suggested_text: str
    confidence_label: str
    audio_clip_src: str
    has_audio_clip: bool

class TranscriptPageState(State):
    """State for the dedicated transcript page."""
    meeting_title: str = ""
    current_meeting_id: int = 0
    transcripts: List[TranscriptEntry] = []
    review_items: List[ReviewItemEntry] = []
    meeting_status: str = "pending"
    audio_status: str = AUDIO_STATUS_PENDING
    audio_error: str = ""
    postprocess_status: str = POSTPROCESS_STATUS_PENDING
    postprocess_error: str = ""
    master_audio_src: str = ""
    master_audio_label: str = "Henüz ses kaydı yok"
    bot_preview_src: str = ""
    bot_preview_label: str = "Henüz canlı görüntü yok"
    live_updates_enabled: bool = False
    is_stopping_meeting: bool = False
    
    def _get_color_scheme(self, meeting_id: int, name: str):
        colors = ["tomato", "red", "ruby", "crimson", "blue", "cyan", "teal", "jade", "green", "grass", "orange", "amber"]
        seed = f"{meeting_id}:{name}".encode("utf-8")
        idx = hashlib.sha256(seed).digest()[0] % len(colors)
        return colors[idx]

    def _get_initials(self, name: str):
        parts = [p for p in name.split() if p]
        if not parts: return "?"
        if len(parts) == 1: return parts[0][:2].upper()
        return (parts[0][0] + parts[-1][0]).upper()

    @rx.var
    def speaker_count(self) -> int:
        return len({entry.speaker for entry in self.transcripts})

    @rx.var
    def has_transcripts(self) -> bool:
        return bool(self.transcripts)

    @rx.var
    def has_bot_preview(self) -> bool:
        return bool(self.bot_preview_src)

    @rx.var
    def meeting_status_label(self) -> str:
        return _meeting_status_label(self.meeting_status)

    @rx.var
    def can_stop_meeting(self) -> bool:
        return self.meeting_status in {"joining", "active"}

    @rx.var
    def audio_status_label(self) -> str:
        return _audio_status_label(self.audio_status)

    @rx.var
    def postprocess_status_label(self) -> str:
        if self.audio_status == AUDIO_STATUS_DISABLED:
            return "Audio kapalı"
        if self.audio_status == AUDIO_STATUS_FAILED:
            return "Teams-only transcript"
        return _postprocess_status_label(self.postprocess_status)

    @rx.var
    def has_review_items(self) -> bool:
        return bool(self.review_items)

    @rx.var
    def pending_review_count(self) -> int:
        return len(self.review_items)

    @rx.var
    def has_master_audio(self) -> bool:
        return bool(self.master_audio_src)

    @rx.var
    def has_audio_warning(self) -> bool:
        return self.audio_status == AUDIO_STATUS_FAILED

    @rx.var
    def audio_status_detail(self) -> str:
        if self.has_master_audio and self.master_audio_label:
            return self.master_audio_label
        if self.audio_status == AUDIO_STATUS_FAILED and self.audio_error:
            return self.audio_error
        return self.audio_status_label

    @rx.var
    def postprocess_status_detail(self) -> str:
        if self.audio_status == AUDIO_STATUS_DISABLED:
            return "Ses kaydı kapalı olduğu için final transcript yalnızca Teams caption temizliği ile üretildi."
        if self.audio_status == AUDIO_STATUS_FAILED:
            return "Ses alınamadığı için final transcript yalnızca Teams caption temizliği ile üretildi."
        if self.postprocess_error:
            return self.postprocess_error
        return self.postprocess_status_label

    def _apply_transcript_snapshot(
        self,
        meeting: Meeting,
        db_caption_events: list[TeamsCaptionEvent],
        db_transcripts: list[Transcript],
        db_review_items: list[TranscriptReviewItem],
        audio_asset: MeetingAudioAsset | None,
    ):
        visible_transcripts = _collapse_rotating_duplicate_transcripts(db_transcripts)
        live_entries = _collapse_live_caption_events(db_caption_events)

        self.meeting_title = meeting.title
        self.meeting_status = meeting.status
        self.audio_status = meeting.audio_status
        self.audio_error = meeting.audio_error or ""
        self.postprocess_status = meeting.postprocess_status
        self.postprocess_error = meeting.postprocess_error or ""
        self.master_audio_src = ""
        self.master_audio_label = "Henüz ses kaydı yok"
        final_entries = [
            TranscriptEntry(
                id=t.id,
                speaker=t.speaker,
                text=t.text,
                timestamp=(
                    (t.caption_finalized_at or t.timestamp).strftime("%H:%M:%S")
                    if (t.caption_finalized_at or t.timestamp)
                    else ""
                ),
                initials=self._get_initials(t.speaker),
                color=self._get_color_scheme(meeting.id, t.speaker),
                resolution_status=t.resolution_status,
                auto_corrected=t.auto_corrected,
            )
            for t in visible_transcripts
        ]
        if meeting.status in {"joining", "active"} or not final_entries:
            self.transcripts = [
                TranscriptEntry(
                    id=entry.id,
                    speaker=entry.speaker,
                    text=entry.text,
                    timestamp=entry.timestamp,
                    initials=self._get_initials(entry.speaker),
                    color=self._get_color_scheme(meeting.id, entry.speaker),
                    resolution_status=entry.resolution_status,
                    auto_corrected=entry.auto_corrected,
                )
                for entry in live_entries
            ]
        else:
            self.transcripts = final_entries

        transcript_map = {transcript.id: transcript for transcript in db_transcripts}
        self.review_items = [
            ReviewItemEntry(
                id=review_item.id,
                transcript_id=review_item.transcript_id,
                speaker=transcript_map[review_item.transcript_id].speaker,
                timestamp=(
                    (transcript_map[review_item.transcript_id].caption_finalized_at or transcript_map[review_item.transcript_id].timestamp).strftime("%H:%M:%S")
                    if (transcript_map[review_item.transcript_id].caption_finalized_at or transcript_map[review_item.transcript_id].timestamp)
                    else ""
                ),
                granularity=review_item.granularity,
                current_text=review_item.current_text,
                suggested_text=review_item.suggested_text,
                confidence_label=f"%{int(round(review_item.confidence * 100))}",
                audio_clip_src=(
                    get_review_clip_src(review_item.audio_clip_path)
                    if review_item.audio_clip_path
                    else ""
                ),
                has_audio_clip=bool(review_item.audio_clip_path),
            )
            for review_item in db_review_items
            if review_item.transcript_id in transcript_map
        ]

        if audio_asset and audio_asset.master_audio_path:
            try:
                preferred_audio_path = Path(audio_asset.master_audio_path)
                pcm_audio_path = get_meeting_pcm_audio_path(meeting.id)
                if pcm_audio_path.exists():
                    preferred_audio_path = pcm_audio_path

                public_audio_path = sync_public_meeting_audio(
                    meeting.id,
                    preferred_audio_path,
                )
                version = int(public_audio_path.stat().st_mtime)
                self.master_audio_src = get_public_meeting_audio_src(public_audio_path.name, version)
                self.master_audio_label = (
                    "WAV oynatma kopyası"
                    if preferred_audio_path.suffix.lower() == ".wav"
                    else f"{audio_asset.format.upper()} master kayıt"
                )
            except FileNotFoundError:
                self.master_audio_label = "Kayıt dosyası bulunamadı"
            except Exception as exc:
                logger.warning(
                    "Could not sync public master audio for meeting %s: %s",
                    meeting.id,
                    exc,
                )
                self.master_audio_label = "Kayıt hazırlanamadı"

        preview_path = _meeting_preview_path(meeting.user_id, meeting.id)
        if preview_path.exists() and meeting.status in {"joining", "active"}:
            version = int(preview_path.stat().st_mtime)
            self.bot_preview_src = _meeting_preview_src(meeting.user_id, meeting.id, version)
            self.bot_preview_label = _format_relative_time(
                datetime.utcfromtimestamp(preview_path.stat().st_mtime)
            )
        else:
            self.bot_preview_src = ""
            if meeting.status in {"joining", "active"}:
                self.bot_preview_label = "Bot henüz canlı kare yüklemedi"
            else:
                self.bot_preview_label = "Canlı görüntü sadece toplantı sırasında görünür"

    def _refresh_current_meeting_transcripts(self) -> bool:
        if not self.current_meeting_id:
            return False

        _ensure_runtime_schema()
        with rx.session() as session:
            meeting = session.exec(
                select(Meeting).where(Meeting.id == self.current_meeting_id)
            ).first()

            if not meeting:
                logger.error(
                    "Meeting with ID %s not found while refreshing transcripts.",
                    self.current_meeting_id,
                )
                return False

            db_caption_events = session.exec(
                select(TeamsCaptionEvent).where(
                    TeamsCaptionEvent.meeting_id == self.current_meeting_id
                ).order_by(TeamsCaptionEvent.sequence_no, TeamsCaptionEvent.id)
            ).all()
            db_transcripts = session.exec(
                select(Transcript).where(
                    Transcript.meeting_id == self.current_meeting_id
                ).order_by(Transcript.sequence_no, Transcript.timestamp, Transcript.id)
            ).all()
            transcript_ids = [transcript.id for transcript in db_transcripts]
            db_review_items = []
            if transcript_ids:
                db_review_items = session.exec(
                    select(TranscriptReviewItem).where(
                        TranscriptReviewItem.transcript_id.in_(transcript_ids),
                        TranscriptReviewItem.status == REVIEW_STATUS_PENDING,
                    ).order_by(TranscriptReviewItem.id)
                ).all()
            audio_asset = session.exec(
                select(MeetingAudioAsset).where(
                    MeetingAudioAsset.meeting_id == self.current_meeting_id
                ).order_by(MeetingAudioAsset.id.desc())
            ).first()

            self._apply_transcript_snapshot(
                meeting,
                db_caption_events,
                db_transcripts,
                db_review_items,
                audio_asset,
            )
            return True

    def page_mount(self):
        """Initialize transcript page and start live updates."""
        return [TranscriptPageState.load_transcripts, TranscriptPageState.poll_transcripts]

    def stop_live_updates(self):
        """Stop transcript polling when leaving the page."""
        self.live_updates_enabled = False

    def leave_current_meeting(self):
        """Stop the current meeting from the transcript page."""
        if not self.current_meeting_id:
            yield rx.toast.error("Toplantı bulunamadı.", position="top-right")
            return

        self.is_stopping_meeting = True
        yield

        toast_event = None
        should_refresh = False

        try:
            should_refresh, toast_event = _stop_meeting_operation(self.current_meeting_id)
        finally:
            if should_refresh:
                self._refresh_current_meeting_transcripts()
            self.is_stopping_meeting = False

        if toast_event is not None:
            yield toast_event

    @rx.event(background=True)
    async def poll_transcripts(self):
        """Keep transcript view updated without a page refresh."""
        async with self:
            if self.live_updates_enabled:
                return
            self.live_updates_enabled = True

        try:
            while True:
                async with self:
                    if not self.live_updates_enabled:
                        break

                    if not self.is_logged_in:
                        self.live_updates_enabled = False
                        break

                    if self.current_meeting_id:
                        self._refresh_current_meeting_transcripts()

                await asyncio.sleep(1)
        finally:
            async with self:
                self.live_updates_enabled = False

    def load_transcripts(self):
        """Load transcripts for the meeting from URL param."""
        self.cleanup()

        router_url = self.router.url
        url_path = router_url.path or ""
        query_params = dict(router_url.query_parameters or {})
        logger.info(
            "Loading transcripts. is_logged_in: %s, path: %s, query: %s",
            self.is_logged_in,
            url_path,
            query_params,
        )

        login_redirect = self.check_login()
        if login_redirect:
            logger.warning("Redirecting to login: Not logged in")
            return login_redirect

        meeting_id_str = query_params.get("meeting_id")
        if not meeting_id_str:
            path_parts = [part for part in url_path.split("/") if part]
            if len(path_parts) >= 2 and path_parts[-2] == "transcripts":
                meeting_id_str = path_parts[-1]

        if not meeting_id_str or meeting_id_str == "[meeting_id]":
            # Reflex occasionally shows the route template string before hydration
            logger.info(f"Param not hydrated yet (value: {meeting_id_str}), waiting...")
            return

        try:
            target_id = int(meeting_id_str)
            if target_id == 0:
                logger.warning("Meeting ID is 0, skipping lookup.")
                return
            self.current_meeting_id = target_id
            logger.info(f"Target meeting ID set to: {self.current_meeting_id}")
        except (ValueError, TypeError):
            logger.error(f"Invalid meeting ID format: {meeting_id_str}")
            return rx.redirect("/dashboard")

        if not self._refresh_current_meeting_transcripts():
            return rx.redirect("/dashboard")

    def apply_review_item(self, review_item_id: int):
        with rx.session() as session:
            review_item = session.exec(
                select(TranscriptReviewItem).where(TranscriptReviewItem.id == review_item_id)
            ).first()
            if not review_item:
                return rx.toast.error("Düzeltme önerisi bulunamadı.", position="top-right")

            transcript = session.exec(
                select(Transcript).where(Transcript.id == review_item.transcript_id)
            ).first()
            if not transcript:
                return rx.toast.error("Transcript satırı bulunamadı.", position="top-right")

            transcript.text = review_item.suggested_text
            transcript.resolution_status = TRANSCRIPT_STATUS_ACCEPTED
            transcript.auto_corrected = False
            review_item.status = REVIEW_STATUS_ACCEPTED
            session.add(transcript)
            session.add(review_item)
            session.commit()

        self._refresh_current_meeting_transcripts()
        return rx.toast.success("Önerilen metin uygulandı.", position="top-right")

    def keep_review_item(self, review_item_id: int):
        with rx.session() as session:
            review_item = session.exec(
                select(TranscriptReviewItem).where(TranscriptReviewItem.id == review_item_id)
            ).first()
            if not review_item:
                return rx.toast.error("Düzeltme önerisi bulunamadı.", position="top-right")

            transcript = session.exec(
                select(Transcript).where(Transcript.id == review_item.transcript_id)
            ).first()
            if not transcript:
                return rx.toast.error("Transcript satırı bulunamadı.", position="top-right")

            transcript.text = transcript.teams_text or transcript.text
            transcript.resolution_status = TRANSCRIPT_STATUS_REJECTED
            transcript.auto_corrected = False
            review_item.status = REVIEW_STATUS_REJECTED
            session.add(transcript)
            session.add(review_item)
            session.commit()

        self._refresh_current_meeting_transcripts()
        return rx.toast.info("Mevcut caption korundu.", position="top-right")
    
    def download_txt(self):
        """Download transcripts as a TXT file."""
        lines = []
        for t in self.transcripts:
            lines.append(f"[{t.timestamp}] {t.speaker}: {t.text}")
        content = "\n".join(lines)
        filename = f"transcript_{self.current_meeting_id}.txt"
        return rx.download(data=content, filename=filename)
    
    def download_csv(self):
        """Download transcripts as a CSV file."""
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Zaman", "Konuşmacı", "Metin"])
        for t in self.transcripts:
            writer.writerow([t.timestamp, t.speaker, t.text])
        content = output.getvalue()
        filename = f"transcript_{self.current_meeting_id}.csv"
        return rx.download(data=content, filename=filename)
