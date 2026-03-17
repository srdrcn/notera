import asyncio
import hashlib
import logging
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from typing import List, Optional

import reflex as rx
from pydantic import BaseModel
from sqlmodel import select

from .models import Meeting, Transcript, User

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


def _app_db_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "app" / "reflex.db"


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


def _ensure_meeting_schema():
    """Remove legacy meeting columns from existing local SQLite databases."""
    db_path = _app_db_path()
    if not db_path.exists():
        return

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        columns = {
            row[1]
            for row in cursor.execute("PRAGMA table_info(meeting)").fetchall()
        }
        if "caption_language" in columns:
            cursor.execute("ALTER TABLE meeting DROP COLUMN caption_language")
            conn.commit()
            logger.info("Removed legacy meeting.caption_language column from SQLite database.")
    except Exception:
        logger.exception("Failed ensuring meeting schema compatibility.")
    finally:
        conn.close()


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
        _ensure_meeting_schema()
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
        _ensure_meeting_schema()

        toast_event = None
        should_reload = False

        with rx.session() as session:
            new_meeting = Meeting(
                user_id=self.user.id,
                title=self.new_meeting_title,
                teams_link=self.new_meeting_link,
            )
            session.add(new_meeting)
            session.commit()
            session.refresh(new_meeting)

            started, toast_event = self._start_bot_for_meeting(session, new_meeting)
            should_reload = True

            if started:
                self.new_meeting_title = ""
                self.new_meeting_link = ""
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
        with rx.session() as session:
            meeting = session.exec(
                select(Meeting).where(Meeting.id == meeting_id)
            ).first()
            if meeting:
                _remove_meeting_preview(meeting.user_id, meeting.id)
                transcripts = session.exec(
                    select(Transcript).where(Transcript.meeting_id == meeting_id)
                ).all()
                for transcript in transcripts:
                    session.delete(transcript)
                
                session.delete(meeting)
                session.commit()
                self.load_meetings()


class TranscriptEntry(BaseModel):
    """A single transcript entry for the UI."""
    speaker: str
    text: str
    timestamp: str
    initials: str
    color: str

class TranscriptPageState(State):
    """State for the dedicated transcript page."""
    meeting_title: str = ""
    current_meeting_id: int = 0
    transcripts: List[TranscriptEntry] = []
    meeting_status: str = "pending"
    bot_preview_src: str = ""
    bot_preview_label: str = "Henüz canlı görüntü yok"
    live_updates_enabled: bool = False
    is_stopping_meeting: bool = False
    
    def _get_color_scheme(self, meeting_id: int, name: str):
        colors = ["tomato", "red", "ruby", "crimson", "pink", "plum", "purple", "violet", "iris", "indigo", "blue", "cyan", "teal", "jade", "green", "grass", "orange", "amber"]
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

    def _apply_transcript_snapshot(self, meeting: Meeting, db_transcripts: list[Transcript]):
        self.meeting_title = meeting.title
        self.meeting_status = meeting.status
        self.transcripts = [
            TranscriptEntry(
                speaker=t.speaker,
                text=t.text,
                timestamp=t.timestamp.strftime("%H:%M:%S") if t.timestamp else "",
                initials=self._get_initials(t.speaker),
                color=self._get_color_scheme(meeting.id, t.speaker),
            )
            for t in db_transcripts
        ]

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

            db_transcripts = session.exec(
                select(Transcript).where(
                    Transcript.meeting_id == self.current_meeting_id
                ).order_by(Transcript.timestamp)
            ).all()

            self._apply_transcript_snapshot(meeting, db_transcripts)
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
        
        # Log current state for debugging
        logger.info(f"Loading transcripts. is_logged_in: {self.is_logged_in}, params: {self.router.page.params}")
        
        login_redirect = self.check_login()
        if login_redirect:
            logger.warning("Redirecting to login: Not logged in")
            return login_redirect
        
        meeting_id_str = self.router.page.params.get("meeting_id")
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
