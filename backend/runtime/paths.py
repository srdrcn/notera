from __future__ import annotations

import shutil
from pathlib import Path

from backend.config import get_settings


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def db_path() -> Path:
    settings = get_settings()
    _ensure_dir(settings.db_path.parent)
    return settings.db_path


def meeting_audio_root() -> Path:
    return _ensure_dir(get_settings().meeting_audio_root)


def get_meeting_audio_dir(meeting_id: int) -> Path:
    return _ensure_dir(meeting_audio_root() / f"meeting_{meeting_id}")


def get_meeting_audio_chunks_dir(meeting_id: int) -> Path:
    return _ensure_dir(get_meeting_audio_dir(meeting_id) / "chunks")


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


def live_preview_root() -> Path:
    return _ensure_dir(get_settings().live_preview_root)


def preview_filename(user_id: int, meeting_id: int) -> str:
    return f"user_{user_id}_meeting_{meeting_id}.png"


def preview_path(user_id: int, meeting_id: int) -> Path:
    return live_preview_root() / preview_filename(user_id, meeting_id)


def review_clip_root() -> Path:
    return _ensure_dir(get_settings().review_clip_root)


def get_review_clip_filename(meeting_id: int, transcript_id: int, review_item_id: int) -> str:
    return f"meeting_{meeting_id}_transcript_{transcript_id}_review_{review_item_id}.wav"


def get_review_clip_path(meeting_id: int, transcript_id: int, review_item_id: int) -> Path:
    return review_clip_root() / get_review_clip_filename(meeting_id, transcript_id, review_item_id)


def remove_review_clips_for_meeting(meeting_id: int) -> None:
    for path in review_clip_root().glob(f"meeting_{meeting_id}_*"):
        path.unlink(missing_ok=True)


def runtime_cache_root() -> Path:
    return _ensure_dir(get_settings().runtime_cache_root)


def runtime_cache_dir(name: str) -> Path:
    return _ensure_dir(runtime_cache_root() / name)


def cleanup_meeting_artifacts(meeting_id: int) -> None:
    shutil.rmtree(meeting_audio_root() / f"meeting_{meeting_id}", ignore_errors=True)
    remove_review_clips_for_meeting(meeting_id)
