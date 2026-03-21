from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.app.api.deps import owned_user
from backend.app.db.session import get_db
from backend.app.orchestration.supervisor import supervisor
from backend.app.repositories.meetings import (
    caption_events_for_meeting,
    get_owned_meeting,
    latest_audio_asset,
    review_items_for_transcripts,
    transcripts_for_meeting,
)
from backend.app.schemas.meeting import CreateMeetingRequest, MeetingSummaryOut
from backend.app.schemas.transcript import MeetingSnapshotOut
from backend.app.services.meetings import create_meeting, delete_meeting, list_meeting_summaries, meeting_summary
from backend.app.services.transcript_logic import build_snapshot


router = APIRouter(prefix="/api/meetings", tags=["meetings"])


@router.get("", response_model=list[MeetingSummaryOut])
def list_meetings(user=Depends(owned_user), db: Session = Depends(get_db)):
    return list_meeting_summaries(db, user.id)


@router.post("", response_model=MeetingSummaryOut)
def create_new_meeting(
    payload: CreateMeetingRequest,
    user=Depends(owned_user),
    db: Session = Depends(get_db),
):
    meeting = create_meeting(db, user.id, payload.title, payload.teams_link, payload.audio_recording_enabled)
    db.commit()
    db.refresh(meeting)
    return meeting_summary(meeting)


@router.get("/{meeting_id}", response_model=MeetingSummaryOut)
def get_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    return meeting_summary(meeting)


@router.post("/{meeting_id}/join", response_model=MeetingSummaryOut)
def join_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    try:
        supervisor.start_bot(meeting)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    db.expire_all()
    meeting = get_owned_meeting(db, user.id, meeting_id)
    return meeting_summary(meeting)


@router.post("/{meeting_id}/stop", response_model=MeetingSummaryOut)
def stop_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    try:
        supervisor.stop_bot(meeting)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    db.expire_all()
    meeting = get_owned_meeting(db, user.id, meeting_id)
    return meeting_summary(meeting)


@router.delete("/{meeting_id}")
def remove_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    try:
        delete_meeting(db, user.id, meeting_id)
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True}


@router.get("/{meeting_id}/snapshot", response_model=MeetingSnapshotOut)
def meeting_snapshot(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    caption_events = caption_events_for_meeting(db, meeting_id)
    transcripts = transcripts_for_meeting(db, meeting_id)
    transcript_ids = [row.id for row in transcripts]
    review_items = review_items_for_transcripts(db, transcript_ids, pending_only=True)
    audio_asset = latest_audio_asset(db, meeting_id)
    return build_snapshot(meeting, caption_events, transcripts, review_items, audio_asset)
