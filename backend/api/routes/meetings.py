from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.api.deps import owned_user
from backend.db.session import get_db
from backend.orchestration.supervisor import supervisor
from backend.runtime.logging import bind_context, log_event
from backend.repositories.meetings import (
    caption_events_for_meeting,
    get_owned_meeting,
    latest_audio_asset,
    review_items_for_transcripts,
    transcripts_for_meeting,
)
from backend.schemas.meeting import CreateMeetingRequest, MeetingSummaryOut
from backend.schemas.transcript import MeetingSnapshotOut
from backend.services.meetings import create_meeting, delete_meeting, list_meeting_summaries, meeting_summary
from backend.services.transcript_logic import build_snapshot


router = APIRouter(prefix="/api/meetings", tags=["meetings"])
logger = logging.getLogger("notera.routes.meetings")


@router.get("", response_model=list[MeetingSummaryOut])
def list_meetings(user=Depends(owned_user), db: Session = Depends(get_db)):
    meetings = list_meeting_summaries(db, user.id)
    log_event(
        logger,
        logging.DEBUG,
        "meeting.list.loaded",
        "Meeting list loaded",
        user_id=user.id,
        meeting_count=len(meetings),
    )
    return meetings


@router.post("", response_model=MeetingSummaryOut)
def create_new_meeting(
    payload: CreateMeetingRequest,
    user=Depends(owned_user),
    db: Session = Depends(get_db),
):
    meeting = create_meeting(db, user.id, payload.title, payload.teams_link, payload.audio_recording_enabled)
    db.commit()
    db.refresh(meeting)
    bind_context(meeting_id=meeting.id)
    log_event(
        logger,
        logging.INFO,
        "meeting.created",
        "Meeting created",
        meeting_id=meeting.id,
        user_id=user.id,
        audio_recording_enabled=meeting.audio_recording_enabled,
    )
    return meeting_summary(meeting)


@router.get("/{meeting_id}", response_model=MeetingSummaryOut)
def get_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    return meeting_summary(meeting)


@router.post("/{meeting_id}/join", response_model=MeetingSummaryOut)
def join_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    try:
        run_id = supervisor.start_bot(meeting)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "meeting.join.requested",
        "Meeting join requested",
        meeting_id=meeting.id,
        user_id=user.id,
        run_id=run_id,
        worker_type="bot",
    )
    db.expire_all()
    meeting = get_owned_meeting(db, user.id, meeting_id)
    return meeting_summary(meeting)


@router.post("/{meeting_id}/stop", response_model=MeetingSummaryOut)
def stop_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    try:
        stopped = supervisor.stop_bot(meeting)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "meeting.stop.requested",
        "Meeting stop requested",
        meeting_id=meeting.id,
        user_id=user.id,
        bot_stopped=stopped,
    )
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
    bind_context(meeting_id=meeting_id)
    log_event(
        logger,
        logging.INFO,
        "meeting.deleted",
        "Meeting deleted",
        meeting_id=meeting_id,
        user_id=user.id,
    )
    return {"ok": True}


@router.get("/{meeting_id}/snapshot", response_model=MeetingSnapshotOut)
def meeting_snapshot(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    previous_run_id = meeting.active_postprocess_run_id
    previous_status = meeting.postprocess_status
    recovered_run_id = supervisor.ensure_postprocess(meeting_id)
    if (
        recovered_run_id is not None
        and previous_run_id != recovered_run_id
        and previous_status in {"pending", "queued"}
    ):
        log_event(
            logger,
            logging.INFO,
            "meeting.postprocess.recovered",
            "Meeting postprocess recovered from snapshot request",
            meeting_id=meeting.id,
            user_id=user.id,
            run_id=recovered_run_id,
            worker_type="postprocess",
        )
    db.expire_all()
    meeting = get_owned_meeting(db, user.id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    caption_events = caption_events_for_meeting(db, meeting_id)
    transcripts = transcripts_for_meeting(db, meeting_id)
    transcript_ids = [row.id for row in transcripts]
    review_items = review_items_for_transcripts(db, transcript_ids, pending_only=True)
    audio_asset = latest_audio_asset(db, meeting_id)
    return build_snapshot(meeting, caption_events, transcripts, review_items, audio_asset)
