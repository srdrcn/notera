from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from backend.models import Meeting
from backend.repositories.meetings import delete_meeting_related_rows, get_owned_meeting, list_meetings_for_user
from backend.runtime.paths import cleanup_meeting_artifacts


def meeting_summary(meeting: Meeting) -> dict:
    return {
        "id": meeting.id,
        "title": meeting.title,
        "status": meeting.status,
        "audio_status": meeting.audio_status,
        "postprocess_status": meeting.postprocess_status,
        "postprocess_progress_pct": meeting.postprocess_progress_pct,
        "postprocess_progress_note": meeting.postprocess_progress_note,
        "created_at": meeting.created_at,
        "joined_at": meeting.joined_at,
        "ended_at": meeting.ended_at,
        "can_join": meeting.status not in {"joining", "active", "completed"},
        "can_stop": meeting.status in {"joining", "active"},
        "can_view_transcripts": True,
    }


def create_meeting(
    db: Session,
    user_id: int,
    title: str,
    teams_link: str,
    audio_recording_enabled: bool,
) -> Meeting:
    meeting = Meeting(
        user_id=user_id,
        title=title,
        teams_link=teams_link,
        audio_recording_enabled=audio_recording_enabled,
        audio_status="pending" if audio_recording_enabled else "disabled",
        postprocess_status="pending",
        updated_at=datetime.utcnow(),
    )
    db.add(meeting)
    db.flush()
    return meeting


def list_meeting_summaries(db: Session, user_id: int) -> list[dict]:
    return [meeting_summary(meeting) for meeting in list_meetings_for_user(db, user_id)]


def delete_meeting(db: Session, user_id: int, meeting_id: int) -> None:
    meeting = get_owned_meeting(db, user_id, meeting_id)
    if meeting is None:
        raise ValueError("Toplantı bulunamadı.")
    if meeting.status in {"joining", "active"}:
        raise ValueError("Canlı toplantı silinmeden önce durdurulmalıdır.")
    delete_meeting_related_rows(db, meeting_id)
    cleanup_meeting_artifacts(meeting_id)
    db.delete(meeting)
