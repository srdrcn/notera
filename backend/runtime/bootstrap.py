from __future__ import annotations

import sqlite3
from pathlib import Path

from sqlalchemy import create_engine, text

from backend.db.session import engine
from backend.models import Base
from backend.runtime.constants import (
    AUDIO_STATUS_PENDING,
    POSTPROCESS_STATUS_PENDING,
    REVIEW_STATUS_PENDING,
    TRANSCRIPT_STATUS_ORIGINAL,
)
from backend.runtime.paths import db_path


def _connect(target: Path) -> sqlite3.Connection:
    target.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(target)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def _column_names(cursor: sqlite3.Cursor, table: str) -> set[str]:
    return {row[1] for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()}


def _column_default(cursor: sqlite3.Cursor, table: str, column: str) -> str | None:
    for row in cursor.execute(f"PRAGMA table_info({table})").fetchall():
        if row[1] == column:
            return row[4]
    return None


def _ensure_column(cursor: sqlite3.Cursor, table: str, name: str, ddl: str) -> None:
    if name not in _column_names(cursor, table):
        cursor.execute(ddl)


def _rebuild_table(cursor: sqlite3.Cursor, table: str, create_sql: str, columns: list[str]) -> None:
    backup_table = f"{table}__backup"
    quoted_columns = ", ".join(columns)
    cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
    cursor.execute(f"ALTER TABLE {table} RENAME TO {backup_table}")
    cursor.execute(create_sql)
    cursor.execute(
        f"""
        INSERT INTO {table} ({quoted_columns})
        SELECT {quoted_columns}
        FROM {backup_table}
        """
    )
    cursor.execute(f"DROP TABLE {backup_table}")


def _ensure_sqlalchemy_tables(target: Path) -> None:
    default_target = db_path().resolve()
    if target.resolve() == default_target:
        Base.metadata.create_all(engine)
        with engine.begin() as db:
            db.execute(text("SELECT 1"))
        return

    transient_engine = create_engine(
        f"sqlite:///{target}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    try:
        Base.metadata.create_all(transient_engine)
        with transient_engine.begin() as db:
            db.execute(text("SELECT 1"))
    finally:
        transient_engine.dispose()


def ensure_runtime_schema(target_db: Path | None = None) -> Path:
    target = Path(target_db or db_path())
    _ensure_sqlalchemy_tables(target)

    conn = _connect(target)
    try:
        cursor = conn.cursor()
        _ensure_column(cursor, "user", "created_at", "ALTER TABLE user ADD COLUMN created_at DATETIME")

        meeting_alters = [
            ("audio_recording_enabled", "ALTER TABLE meeting ADD COLUMN audio_recording_enabled INTEGER NOT NULL DEFAULT 1"),
            ("audio_status", "ALTER TABLE meeting ADD COLUMN audio_status TEXT NOT NULL DEFAULT 'pending'"),
            ("audio_error", "ALTER TABLE meeting ADD COLUMN audio_error TEXT"),
            ("postprocess_status", "ALTER TABLE meeting ADD COLUMN postprocess_status TEXT NOT NULL DEFAULT 'pending'"),
            ("postprocess_error", "ALTER TABLE meeting ADD COLUMN postprocess_error TEXT"),
            ("postprocess_progress_pct", "ALTER TABLE meeting ADD COLUMN postprocess_progress_pct INTEGER"),
            ("postprocess_progress_note", "ALTER TABLE meeting ADD COLUMN postprocess_progress_note TEXT"),
            ("joined_at", "ALTER TABLE meeting ADD COLUMN joined_at DATETIME"),
            ("ended_at", "ALTER TABLE meeting ADD COLUMN ended_at DATETIME"),
            ("stop_requested", "ALTER TABLE meeting ADD COLUMN stop_requested INTEGER NOT NULL DEFAULT 0"),
            ("active_bot_run_id", "ALTER TABLE meeting ADD COLUMN active_bot_run_id INTEGER"),
            ("active_postprocess_run_id", "ALTER TABLE meeting ADD COLUMN active_postprocess_run_id INTEGER"),
            ("updated_at", "ALTER TABLE meeting ADD COLUMN updated_at DATETIME"),
        ]
        for column_name, statement in meeting_alters:
            _ensure_column(cursor, "meeting", column_name, statement)

        transcript_alters = [
            ("sequence_no", "ALTER TABLE transcript ADD COLUMN sequence_no INTEGER"),
            ("teams_text", "ALTER TABLE transcript ADD COLUMN teams_text TEXT"),
            ("start_sec", "ALTER TABLE transcript ADD COLUMN start_sec REAL"),
            ("end_sec", "ALTER TABLE transcript ADD COLUMN end_sec REAL"),
            ("caption_started_at", "ALTER TABLE transcript ADD COLUMN caption_started_at DATETIME"),
            ("caption_finalized_at", "ALTER TABLE transcript ADD COLUMN caption_finalized_at DATETIME"),
            ("resolution_status", "ALTER TABLE transcript ADD COLUMN resolution_status TEXT NOT NULL DEFAULT 'original'"),
            ("auto_corrected", "ALTER TABLE transcript ADD COLUMN auto_corrected INTEGER NOT NULL DEFAULT 0"),
            ("updated_at", "ALTER TABLE transcript ADD COLUMN updated_at DATETIME"),
        ]
        for column_name, statement in transcript_alters:
            _ensure_column(cursor, "transcript", column_name, statement)

        _ensure_column(cursor, "meetingaudioasset", "pcm_audio_path", "ALTER TABLE meetingaudioasset ADD COLUMN pcm_audio_path TEXT")
        _ensure_column(
            cursor,
            "meetingaudioasset",
            "postprocess_version",
            "ALTER TABLE meetingaudioasset ADD COLUMN postprocess_version TEXT",
        )
        _ensure_column(
            cursor,
            "transcriptreviewitem",
            "updated_at",
            "ALTER TABLE transcriptreviewitem ADD COLUMN updated_at DATETIME",
        )

        review_item_created_at_default = _column_default(cursor, "transcriptreviewitem", "created_at")
        if not review_item_created_at_default or "CURRENT_TIMESTAMP" not in str(review_item_created_at_default).upper():
            _rebuild_table(
                cursor,
                "transcriptreviewitem",
                """
                CREATE TABLE transcriptreviewitem (
                    id INTEGER NOT NULL PRIMARY KEY,
                    transcript_id INTEGER NOT NULL,
                    granularity TEXT NOT NULL,
                    current_text TEXT NOT NULL,
                    suggested_text TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0,
                    audio_clip_path TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    clip_start_ms INTEGER NOT NULL DEFAULT 0,
                    clip_end_ms INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME,
                    FOREIGN KEY(transcript_id) REFERENCES transcript (id)
                )
                """,
                [
                    "id",
                    "transcript_id",
                    "granularity",
                    "current_text",
                    "suggested_text",
                    "confidence",
                    "audio_clip_path",
                    "status",
                    "clip_start_ms",
                    "clip_end_ms",
                    "created_at",
                    "updated_at",
                ],
            )

        cursor.execute("UPDATE user SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)")
        cursor.execute(
            """
            UPDATE meeting
            SET audio_recording_enabled = COALESCE(audio_recording_enabled, 1),
                audio_status = COALESCE(audio_status, ?),
                postprocess_status = COALESCE(postprocess_status, ?),
                stop_requested = COALESCE(stop_requested, 0),
                updated_at = COALESCE(updated_at, created_at)
            """,
            (AUDIO_STATUS_PENDING, POSTPROCESS_STATUS_PENDING),
        )
        cursor.execute(
            """
            UPDATE transcript
            SET sequence_no = COALESCE(sequence_no, id),
                teams_text = COALESCE(NULLIF(teams_text, ''), text),
                caption_started_at = COALESCE(caption_started_at, timestamp),
                caption_finalized_at = COALESCE(caption_finalized_at, timestamp),
                resolution_status = COALESCE(NULLIF(resolution_status, ''), ?),
                auto_corrected = COALESCE(auto_corrected, 0),
                updated_at = COALESCE(updated_at, timestamp)
            """,
            (TRANSCRIPT_STATUS_ORIGINAL,),
        )
        cursor.execute(
            """
            UPDATE transcriptreviewitem
            SET status = COALESCE(NULLIF(status, ''), ?),
                updated_at = COALESCE(updated_at, created_at)
            """,
            (REVIEW_STATUS_PENDING,),
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_meeting_user_created ON meeting(user_id, created_at)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS ix_transcript_meeting_seq ON transcript(meeting_id, sequence_no, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS ix_review_transcript_status ON transcriptreviewitem(transcript_id, status)"
        )
        conn.commit()
    finally:
        conn.close()

    return target


def ensure_schema() -> Path:
    return ensure_runtime_schema()
