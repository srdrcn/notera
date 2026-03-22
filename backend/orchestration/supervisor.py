from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime

from sqlalchemy import select

from backend.config import get_settings
from backend.db.session import db_session
from backend.models import Meeting, WorkerRun
from backend.runtime.constants import (
    POSTPROCESS_STATUS_FAILED,
    POSTPROCESS_STATUS_PENDING,
    POSTPROCESS_STATUS_QUEUED,
)


logger = logging.getLogger("notera.supervisor")
settings = get_settings()
RECOVERABLE_POSTPROCESS_STATUSES = {
    POSTPROCESS_STATUS_PENDING,
    POSTPROCESS_STATUS_QUEUED,
}


def _is_process_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _relay_stream(stream, meeting_id: int, worker_type: str, run_id: int, level: int) -> None:
    try:
        for line in iter(stream.readline, ""):
            text = line.rstrip()
            if text:
                logger.log(level, "[meeting=%s worker=%s run=%s] %s", meeting_id, worker_type, run_id, text)
    finally:
        stream.close()


class MeetingSupervisor:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def _worker_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "NOTERA_DB_PATH": str(settings.db_path),
                "NOTERA_MEETING_AUDIO_ROOT": str(settings.meeting_audio_root),
                "NOTERA_LIVE_PREVIEW_ROOT": str(settings.live_preview_root),
                "NOTERA_REVIEW_CLIP_ROOT": str(settings.review_clip_root),
                "NOTERA_RUNTIME_CACHE_ROOT": str(settings.runtime_cache_root),
                "NOTERA_DISABLE_INTERNAL_POSTPROCESS_TRIGGER": "1",
                "PYTHONPATH": str(settings.repo_root),
            }
        )
        return env

    def reconcile(self) -> None:
        with db_session() as db:
            active_runs = list(
                db.scalars(
                    select(WorkerRun).where(WorkerRun.status.in_(["starting", "running"]))
                )
            )
            for run in active_runs:
                if _is_process_running(run.pid):
                    continue
                run.status = "failed"
                run.ended_at = datetime.utcnow()
                run.exit_code = run.exit_code if run.exit_code is not None else -1
                db.add(run)

                meeting = db.get(Meeting, run.meeting_id)
                if meeting is None:
                    continue
                if run.worker_type == "bot":
                    meeting.bot_pid = None
                    meeting.active_bot_run_id = None
                    if meeting.status in {"joining", "active"}:
                        meeting.status = "completed"
                        meeting.ended_at = meeting.ended_at or datetime.utcnow()
                else:
                    meeting.active_postprocess_run_id = None
                    if meeting.postprocess_status not in {"completed", "review_ready"}:
                        meeting.postprocess_status = POSTPROCESS_STATUS_FAILED
                        meeting.postprocess_error = "Postprocess worker beklenmeden sonlandı."
                db.add(meeting)

    def _mark_run_started(self, meeting_id: int, worker_type: str, pid: int) -> int:
        with db_session() as db:
            run = WorkerRun(
                meeting_id=meeting_id,
                worker_type=worker_type,
                status="running",
                pid=pid,
                started_at=datetime.utcnow(),
            )
            db.add(run)
            db.flush()
            meeting = db.get(Meeting, meeting_id)
            if meeting is not None:
                if worker_type == "bot":
                    meeting.active_bot_run_id = run.id
                    meeting.bot_pid = pid
                    meeting.stop_requested = False
                else:
                    meeting.active_postprocess_run_id = run.id
                db.add(meeting)
            return run.id

    def _watch_process(self, process: subprocess.Popen[str], meeting_id: int, worker_type: str, run_id: int) -> None:
        exit_code = process.wait()
        should_recover_postprocess = False
        with db_session() as db:
            run = db.get(WorkerRun, run_id)
            meeting = db.get(Meeting, meeting_id)
            if run is not None:
                run.exit_code = exit_code
                run.ended_at = datetime.utcnow()
                run.status = "completed" if exit_code == 0 else "failed"
                db.add(run)
            if meeting is not None:
                if worker_type == "bot":
                    if meeting.active_bot_run_id == run_id:
                        meeting.active_bot_run_id = None
                    meeting.bot_pid = None
                    should_recover_postprocess = (
                        meeting.status == "completed"
                        and meeting.postprocess_status in RECOVERABLE_POSTPROCESS_STATUSES
                        and not meeting.active_postprocess_run_id
                    )
                else:
                    if meeting.active_postprocess_run_id == run_id:
                        meeting.active_postprocess_run_id = None
                db.add(meeting)

        if worker_type == "bot" and (exit_code == 0 or should_recover_postprocess):
            try:
                self.ensure_postprocess(meeting_id)
            except Exception:
                logger.exception("Failed starting postprocess for meeting %s after bot exit", meeting_id)

    def _spawn(self, meeting_id: int, worker_type: str, command: list[str]) -> int:
        process = subprocess.Popen(
            command,
            cwd=str(settings.repo_root),
            env=self._worker_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        run_id = self._mark_run_started(meeting_id, worker_type, process.pid)
        threading.Thread(
            target=_relay_stream,
            args=(process.stdout, meeting_id, worker_type, run_id, logging.INFO),
            daemon=True,
        ).start()
        threading.Thread(
            target=_relay_stream,
            args=(process.stderr, meeting_id, worker_type, run_id, logging.ERROR),
            daemon=True,
        ).start()
        threading.Thread(
            target=self._watch_process,
            args=(process, meeting_id, worker_type, run_id),
            daemon=True,
        ).start()
        return run_id

    def start_bot(self, meeting: Meeting) -> int:
        with self._lock:
            if meeting.status == "completed":
                raise ValueError("Bu toplantı oturumu tamamlandı. Aynı oturum yeniden başlatılamaz.")
            if _is_process_running(meeting.bot_pid):
                raise ValueError("Bu toplantı için bot zaten çalışıyor.")
            with db_session() as db:
                current = db.get(Meeting, meeting.id)
                if current is None:
                    raise ValueError("Toplantı bulunamadı.")
                current.status = "joining"
                current.stop_requested = False
                current.postprocess_status = current.postprocess_status or POSTPROCESS_STATUS_PENDING
                current.updated_at = datetime.utcnow()
                db.add(current)
            return self._spawn(
                meeting.id,
                "bot",
                [
                    settings.bot_python_bin,
                    "-u",
                    "-m",
                    settings.bot_entrypoint,
                    meeting.teams_link,
                    str(meeting.id),
                ],
            )

    def start_postprocess(self, meeting_id: int) -> int:
        with self._lock:
            with db_session() as db:
                meeting = db.get(Meeting, meeting_id)
                if meeting is None:
                    raise ValueError("Toplantı bulunamadı.")
                if meeting.active_postprocess_run_id:
                    run = db.get(WorkerRun, meeting.active_postprocess_run_id)
                    if run and _is_process_running(run.pid):
                        return run.id
                meeting.postprocess_status = meeting.postprocess_status or POSTPROCESS_STATUS_PENDING
                meeting.postprocess_error = None
                meeting.updated_at = datetime.utcnow()
                db.add(meeting)
            return self._spawn(
                meeting_id,
                "postprocess",
                [
                    settings.bot_python_bin,
                    "-u",
                    "-m",
                    settings.postprocess_entrypoint,
                    str(meeting_id),
                ],
            )

    def ensure_postprocess(self, meeting_id: int) -> int | None:
        with self._lock:
            with db_session() as db:
                meeting = db.get(Meeting, meeting_id)
                if meeting is None:
                    raise ValueError("Toplantı bulunamadı.")
                if meeting.status != "completed":
                    return None
                if meeting.postprocess_status not in RECOVERABLE_POSTPROCESS_STATUSES:
                    return None
                if meeting.active_postprocess_run_id:
                    run = db.get(WorkerRun, meeting.active_postprocess_run_id)
                    if run and _is_process_running(run.pid):
                        return run.id
                    meeting.active_postprocess_run_id = None
                meeting.postprocess_error = None
                meeting.updated_at = datetime.utcnow()
                db.add(meeting)
            return self._spawn(
                meeting_id,
                "postprocess",
                [
                    settings.bot_python_bin,
                    "-u",
                    "-m",
                    settings.postprocess_entrypoint,
                    str(meeting_id),
                ],
            )

    def stop_bot(self, meeting: Meeting, timeout_seconds: float = 8.0) -> bool:
        with db_session() as db:
            current = db.get(Meeting, meeting.id)
            if current is None:
                raise ValueError("Toplantı bulunamadı.")
            current.stop_requested = True
            current.updated_at = datetime.utcnow()
            db.add(current)
            pid = current.bot_pid

        if not pid or not _is_process_running(pid):
            return True

        graceful_wait_seconds = min(2.5, max(timeout_seconds * 0.35, 1.0))
        graceful_deadline = time.monotonic() + graceful_wait_seconds
        while time.monotonic() < graceful_deadline:
            if not _is_process_running(pid):
                return True
            time.sleep(0.25)

        try:
            process_group_id = os.getpgid(pid)
        except Exception:
            process_group_id = None

        remaining_timeout = max(timeout_seconds - graceful_wait_seconds, 0.5)
        for current_signal, wait_seconds in (
            (signal.SIGTERM, remaining_timeout / 2),
            (signal.SIGKILL, remaining_timeout / 2),
        ):
            delivered = False
            if process_group_id is not None:
                try:
                    os.killpg(process_group_id, current_signal)
                    delivered = True
                except Exception:
                    delivered = False
            if not delivered:
                try:
                    os.kill(pid, current_signal)
                except Exception:
                    pass
            deadline = time.monotonic() + wait_seconds
            while time.monotonic() < deadline:
                if not _is_process_running(pid):
                    return True
                time.sleep(0.25)

        return not _is_process_running(pid)


supervisor = MeetingSupervisor()
