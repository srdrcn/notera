from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from backend.runtime.constants import POSTPROCESS_STATUS_QUEUED
from backend.runtime.logging import log_event
from backend.runtime.paths import db_path as runtime_db_path


logger = logging.getLogger("notera.worker.bot")
REPO_ROOT = Path(__file__).resolve().parents[2]


def get_db_path() -> str:
    return str(runtime_db_path())


def is_stop_requested(meeting_id: int) -> bool:
    conn = sqlite3.connect(get_db_path())
    try:
        row = conn.execute(
            "SELECT stop_requested FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        return bool(row[0]) if row is not None and row[0] is not None else False
    except Exception as exc:
        logger.debug("Could not resolve stop flag from database for meeting %s: %s", meeting_id, exc)
        return False
    finally:
        conn.close()


def is_audio_recording_enabled(meeting_id: int) -> bool:
    conn = sqlite3.connect(get_db_path())
    try:
        row = conn.execute(
            "SELECT audio_recording_enabled FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        if row is None:
            return True
        return bool(row[0])
    except Exception as exc:
        logger.warning("Could not resolve audio recording flag for meeting %s: %s", meeting_id, exc)
        return True
    finally:
        conn.close()


def update_meeting_fields(meeting_id: int, **fields: object) -> None:
    if not fields:
        return

    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    try:
        columns = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [meeting_id]
        cursor.execute(
            f"UPDATE meeting SET {columns} WHERE id = ?",
            values,
        )
        conn.commit()
    except Exception as exc:
        logger.error("Failed updating meeting %s fields %s: %s", meeting_id, list(fields.keys()), exc)
    finally:
        conn.close()


def update_audio_status(meeting_id: int, status: str, error: str | None = None) -> None:
    update_meeting_fields(
        meeting_id,
        audio_status=status,
        audio_error=error,
    )


def update_meeting_status(meeting_id: int, status: str, clear_bot_pid: bool = False) -> None:
    conn = sqlite3.connect(get_db_path())
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
        logger.info("Meeting %s status updated to %s", meeting_id, status)
    except Exception as exc:
        logger.error("Failed to update status: %s", exc)
    finally:
        conn.close()


def register_audio_asset(
    meeting_id: int,
    master_audio_path: str,
    fmt: str,
    status: str,
    duration_ms: int | None = None,
    pcm_audio_path: str | None = None,
    postprocess_version: str | None = None,
) -> None:
    conn = sqlite3.connect(get_db_path())
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
    except Exception as exc:
        logger.error("Failed registering audio asset for meeting %s: %s", meeting_id, exc)
    finally:
        conn.close()


def register_identity_evidence(
    meeting_id: int,
    participant_id: int,
    evidence_type: str,
    evidence_value: str,
    confidence: float = 0.0,
    audio_source_id: int | None = None,
    payload: dict[str, object] | None = None,
) -> None:
    if participant_id <= 0 or not evidence_value:
        return

    conn = sqlite3.connect(get_db_path())
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
    except Exception as exc:
        logger.debug("Failed recording identity evidence for meeting %s: %s", meeting_id, exc)
    finally:
        conn.close()


def register_audio_source(
    meeting_id: int,
    source_key: str,
    source_kind: str,
    track_id: str | None = None,
    stream_id: str | None = None,
    file_path: str | None = None,
    fmt: str | None = None,
    status: str = "pending",
) -> int:
    conn = sqlite3.connect(get_db_path())
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
    except Exception as exc:
        logger.error("Failed registering audio source for meeting %s: %s", meeting_id, exc)
        return 0
    finally:
        conn.close()


def finalize_audio_sources(meeting_id: int, status: str = "ready") -> None:
    conn = sqlite3.connect(get_db_path())
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
    except Exception as exc:
        logger.error("Failed finalizing audio sources for meeting %s: %s", meeting_id, exc)
    finally:
        conn.close()


def append_speaker_activity_interval(
    meeting_id: int,
    participant_id: int,
    start_offset_ms: int,
    end_offset_ms: int,
    source: str = "roster_speaking_indicator",
    confidence: float = 0.0,
    metadata: dict[str, object] | None = None,
) -> None:
    if participant_id <= 0 or end_offset_ms <= start_offset_ms:
        return

    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    try:
        normalized_metadata = metadata or {}
        raw_signal_kind = str(normalized_metadata.get("signal_kind") or normalized_metadata.get("source_kind") or "").strip().lower()
        signal_kind_map = {
            "voice_signal": "teams_ui_outline",
            "participant_panel": "teams_ui_polling",
            "video_surface": "teams_dom_mutation",
            "video_tile": "teams_dom_mutation",
        }
        signal_kind = signal_kind_map.get(raw_signal_kind, "fallback")
        source_session_id = normalized_metadata.get("source_session_id")
        source_session_id = str(source_session_id).strip() if source_session_id else None
        observed_at = datetime.utcnow().isoformat()
        cursor.execute(
            """
            INSERT INTO speakeractivityevent (
                meeting_id,
                participant_id,
                start_offset_ms,
                end_offset_ms,
                source,
                event_type,
                signal_kind,
                event_confidence,
                ui_observed_at,
                relative_offset_ms,
                source_session_id,
                confidence,
                metadata_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_id,
                participant_id,
                int(start_offset_ms),
                int(end_offset_ms),
                source,
                "active",
                signal_kind,
                confidence,
                observed_at,
                int(end_offset_ms),
                source_session_id,
                confidence,
                json.dumps(normalized_metadata, ensure_ascii=False) if metadata is not None else None,
                observed_at,
            ),
        )
        conn.commit()
    except Exception as exc:
        logger.error("Failed saving speaker activity interval for meeting %s: %s", meeting_id, exc)
    finally:
        conn.close()


def upsert_meeting_participant(
    meeting_id: int,
    participant_key: str,
    display_name: str,
    normalize_participant_name,
    participant_identity_conflicts,
    participant_key_rank,
    disambiguated_participant_key,
    platform_identity: str | None = None,
    role: str | None = None,
    is_bot: bool = False,
    join_state: str = "present",
) -> int:
    cleaned_display_name = normalize_participant_name(display_name) or "Unknown"
    normalized_name = cleaned_display_name.casefold()
    now_iso = datetime.utcnow().isoformat()
    conn = sqlite3.connect(get_db_path())
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
    except Exception as exc:
        logger.error("Failed upserting meeting participant for meeting %s: %s", meeting_id, exc)
        return 0
    finally:
        conn.close()


def resolve_meeting_owner_user_id(meeting_id: int) -> str:
    conn = sqlite3.connect(get_db_path())
    try:
        row = conn.execute(
            "SELECT user_id FROM meeting WHERE id = ?",
            (meeting_id,),
        ).fetchone()
        if row and row[0] is not None:
            return str(row[0])
        return "unknown"
    except Exception as exc:
        logger.debug("Could not resolve meeting owner for screenshot path: %s", exc)
        return "unknown"
    finally:
        conn.close()


def trigger_postprocess_worker(meeting_id: int) -> None:
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
    except Exception as exc:
        logger.error("Failed starting post-process worker for meeting %s: %s", meeting_id, exc)
        update_meeting_fields(
            meeting_id,
            postprocess_status="failed",
            postprocess_error=str(exc),
            postprocess_progress_pct=None,
            postprocess_progress_note=None,
        )
