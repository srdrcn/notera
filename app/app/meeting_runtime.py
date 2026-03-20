import os
import shutil
import sqlite3
from pathlib import Path


AUDIO_STATUS_DISABLED = "disabled"
AUDIO_STATUS_PENDING = "pending"
AUDIO_STATUS_RECORDING = "recording"
AUDIO_STATUS_READY = "ready"
AUDIO_STATUS_FAILED = "failed"

POSTPROCESS_STATUS_PENDING = "pending"
POSTPROCESS_STATUS_QUEUED = "queued"
POSTPROCESS_STATUS_TRANSCRIBING = "transcribing"
POSTPROCESS_STATUS_CANONICALIZING = "canonicalizing"
POSTPROCESS_STATUS_ALIGNING = "aligning"
POSTPROCESS_STATUS_REBUILDING = "rebuilding"
POSTPROCESS_STATUS_REVIEW_READY = "review_ready"
POSTPROCESS_STATUS_RUNNING = "running"
POSTPROCESS_STATUS_COMPLETED = "completed"
POSTPROCESS_STATUS_FAILED = "failed"

TRANSCRIPT_STATUS_ORIGINAL = "original"
TRANSCRIPT_STATUS_PENDING_REVIEW = "pending_review"
TRANSCRIPT_STATUS_AUTO_APPLIED = "auto_applied"
TRANSCRIPT_STATUS_ACCEPTED = "accepted"
TRANSCRIPT_STATUS_REJECTED = "rejected"

REVIEW_STATUS_PENDING = "pending"
REVIEW_STATUS_ACCEPTED = "accepted"
REVIEW_STATUS_REJECTED = "rejected"
REVIEW_STATUS_AUTO_APPLIED = "auto_applied"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_db_path() -> Path:
    return repo_root() / "app" / "reflex.db"


def meeting_audio_root() -> Path:
    path = repo_root() / "bot" / "meeting_audio"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_meeting_audio_dir(meeting_id: int) -> Path:
    path = meeting_audio_root() / f"meeting_{meeting_id}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_meeting_audio_chunks_dir(meeting_id: int) -> Path:
    path = get_meeting_audio_dir(meeting_id) / "chunks"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_meeting_master_audio_path(meeting_id: int, ext: str = "webm") -> Path:
    sanitized_ext = ext.lstrip(".") or "webm"
    return get_meeting_audio_dir(meeting_id) / f"master.{sanitized_ext}"


def get_meeting_pcm_audio_path(meeting_id: int) -> Path:
    return get_meeting_audio_dir(meeting_id) / "master_16k_mono.wav"


def get_meeting_artifact_path(meeting_id: int, filename: str) -> Path:
    return get_meeting_audio_dir(meeting_id) / filename


def get_whisperx_result_path(meeting_id: int) -> Path:
    return get_meeting_artifact_path(meeting_id, "whisperx_result.json")


def get_teams_canonical_path(meeting_id: int) -> Path:
    return get_meeting_artifact_path(meeting_id, "teams_canonical.json")


def get_alignment_map_path(meeting_id: int) -> Path:
    return get_meeting_artifact_path(meeting_id, "alignment_map.json")


def review_audio_asset_dir() -> Path:
    path = repo_root() / "app" / "assets" / "review_audio_clips"
    path.mkdir(parents=True, exist_ok=True)
    return path


def public_meeting_audio_dir() -> Path:
    path = repo_root() / "app" / "assets" / "meeting_audio"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_public_meeting_audio_filename(meeting_id: int, ext: str) -> str:
    sanitized_ext = ext.lstrip(".") or "webm"
    return f"meeting_{meeting_id}.{sanitized_ext}"


def get_public_meeting_audio_path(meeting_id: int, ext: str) -> Path:
    return public_meeting_audio_dir() / get_public_meeting_audio_filename(meeting_id, ext)


def get_public_meeting_audio_src(filename: str, version: int | None = None) -> str:
    suffix = f"?v={version}" if version is not None else ""
    return f"/meeting_audio/{filename}{suffix}"


def sync_public_meeting_audio(meeting_id: int, source_path: str | Path) -> Path:
    src_path = Path(source_path)
    if not src_path.exists():
        raise FileNotFoundError(src_path)
    target_path = get_public_meeting_audio_path(meeting_id, src_path.suffix)
    shutil.copy2(src_path, target_path)
    return target_path


def get_review_clip_filename(meeting_id: int, transcript_id: int, review_item_id: int) -> str:
    return f"meeting_{meeting_id}_transcript_{transcript_id}_review_{review_item_id}.wav"


def get_review_clip_path(meeting_id: int, transcript_id: int, review_item_id: int) -> Path:
    return review_audio_asset_dir() / get_review_clip_filename(
        meeting_id, transcript_id, review_item_id
    )


def get_review_clip_src(filename: str) -> str:
    return f"/review_audio_clips/{filename}"


def remove_meeting_audio_artifacts(meeting_id: int):
    shutil.rmtree(meeting_audio_root() / f"meeting_{meeting_id}", ignore_errors=True)


def remove_review_clip_by_path(path_value: str | None):
    if not path_value:
        return
    try:
        path = Path(path_value)
        if not path.is_absolute():
            path = review_audio_asset_dir() / path.name
        path.unlink(missing_ok=True)
    except Exception:
        return


def remove_review_clips_for_meeting(meeting_id: int):
    pattern = f"meeting_{meeting_id}_"
    for path in review_audio_asset_dir().glob(f"{pattern}*"):
        path.unlink(missing_ok=True)


def remove_public_meeting_audio_for_meeting(meeting_id: int):
    for path in public_meeting_audio_dir().glob(f"meeting_{meeting_id}.*"):
        path.unlink(missing_ok=True)


def cleanup_meeting_artifacts(meeting_id: int):
    remove_meeting_audio_artifacts(meeting_id)
    remove_review_clips_for_meeting(meeting_id)
    remove_public_meeting_audio_for_meeting(meeting_id)


def _get_column_names(cursor: sqlite3.Cursor, table: str) -> set[str]:
    return {
        row[1]
        for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()
    }


def ensure_runtime_schema(db_path: Path | None = None):
    target_db = Path(db_path or get_db_path())
    if not target_db.exists():
        return

    conn = sqlite3.connect(target_db)
    try:
        cursor = conn.cursor()

        meeting_columns = _get_column_names(cursor, "meeting")
        meeting_alters = [
            ("audio_recording_enabled", "ALTER TABLE meeting ADD COLUMN audio_recording_enabled INTEGER NOT NULL DEFAULT 1"),
            ("audio_status", f"ALTER TABLE meeting ADD COLUMN audio_status TEXT NOT NULL DEFAULT '{AUDIO_STATUS_PENDING}'"),
            ("audio_error", "ALTER TABLE meeting ADD COLUMN audio_error TEXT"),
            ("postprocess_status", f"ALTER TABLE meeting ADD COLUMN postprocess_status TEXT NOT NULL DEFAULT '{POSTPROCESS_STATUS_PENDING}'"),
            ("postprocess_error", "ALTER TABLE meeting ADD COLUMN postprocess_error TEXT"),
            ("joined_at", "ALTER TABLE meeting ADD COLUMN joined_at DATETIME"),
            ("ended_at", "ALTER TABLE meeting ADD COLUMN ended_at DATETIME"),
        ]
        for column_name, statement in meeting_alters:
            if column_name not in meeting_columns:
                cursor.execute(statement)

        transcript_columns = _get_column_names(cursor, "transcript")
        transcript_alters = [
            ("sequence_no", "ALTER TABLE transcript ADD COLUMN sequence_no INTEGER"),
            ("teams_text", "ALTER TABLE transcript ADD COLUMN teams_text TEXT"),
            ("start_sec", "ALTER TABLE transcript ADD COLUMN start_sec REAL"),
            ("end_sec", "ALTER TABLE transcript ADD COLUMN end_sec REAL"),
            ("caption_started_at", "ALTER TABLE transcript ADD COLUMN caption_started_at DATETIME"),
            ("caption_finalized_at", "ALTER TABLE transcript ADD COLUMN caption_finalized_at DATETIME"),
            ("resolution_status", f"ALTER TABLE transcript ADD COLUMN resolution_status TEXT NOT NULL DEFAULT '{TRANSCRIPT_STATUS_ORIGINAL}'"),
            ("auto_corrected", "ALTER TABLE transcript ADD COLUMN auto_corrected INTEGER NOT NULL DEFAULT 0"),
        ]
        for column_name, statement in transcript_alters:
            if column_name not in transcript_columns:
                cursor.execute(statement)

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS teamscaptionevent (
                id INTEGER NOT NULL PRIMARY KEY,
                meeting_id INTEGER NOT NULL,
                sequence_no INTEGER NOT NULL,
                speaker_name TEXT NOT NULL,
                text TEXT NOT NULL,
                observed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                slot_index INTEGER,
                revision_no INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(meeting_id) REFERENCES meeting (id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS meetingaudioasset (
                id INTEGER NOT NULL PRIMARY KEY,
                meeting_id INTEGER NOT NULL,
                master_audio_path TEXT NOT NULL,
                pcm_audio_path TEXT,
                format TEXT NOT NULL,
                duration_ms INTEGER,
                status TEXT NOT NULL,
                postprocess_version TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(meeting_id) REFERENCES meeting (id)
            )
            """
        )
        meeting_audio_columns = _get_column_names(cursor, "meetingaudioasset")
        if "pcm_audio_path" not in meeting_audio_columns:
            cursor.execute("ALTER TABLE meetingaudioasset ADD COLUMN pcm_audio_path TEXT")
        if "postprocess_version" not in meeting_audio_columns:
            cursor.execute("ALTER TABLE meetingaudioasset ADD COLUMN postprocess_version TEXT")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transcriptreviewitem (
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
                FOREIGN KEY(transcript_id) REFERENCES transcript (id)
            )
            """
        )

        cursor.execute(
            """
            UPDATE meeting
            SET audio_recording_enabled = COALESCE(audio_recording_enabled, 1),
                audio_status = COALESCE(audio_status, ?),
                postprocess_status = COALESCE(postprocess_status, ?)
            """,
            (AUDIO_STATUS_PENDING, POSTPROCESS_STATUS_PENDING),
        )
        cursor.execute(
            """
            UPDATE transcript
            SET sequence_no = COALESCE(sequence_no, id),
                teams_text = COALESCE(NULLIF(teams_text, ''), text),
                start_sec = COALESCE(start_sec, NULL),
                end_sec = COALESCE(end_sec, NULL),
                caption_started_at = COALESCE(caption_started_at, timestamp),
                caption_finalized_at = COALESCE(caption_finalized_at, timestamp),
                resolution_status = COALESCE(NULLIF(resolution_status, ''), ?),
                auto_corrected = COALESCE(auto_corrected, 0)
            """,
            (TRANSCRIPT_STATUS_ORIGINAL,),
        )

        conn.commit()
    finally:
        conn.close()
