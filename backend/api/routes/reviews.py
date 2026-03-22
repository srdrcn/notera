from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.api.deps import owned_user
from backend.db.session import get_db
from backend.models import Meeting, TranscriptSegment
from backend.runtime.logging import bind_context, log_event
from backend.schemas.transcript import (
    ParticipantMergeRequest,
    ParticipantSplitRequest,
    SegmentParticipantUpdateRequest,
)
from backend.services.reviews import merge_participants, reassign_segment_participant, split_participant


router = APIRouter(tags=["reviews"])
logger = logging.getLogger("notera.routes.reviews")


def _owned_meeting(db: Session, user_id: int, meeting_id: int) -> Meeting:
    meeting = db.get(Meeting, meeting_id)
    if meeting is None or meeting.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    return meeting


@router.patch("/api/transcript-segments/{segment_id}/participant")
def update_segment_participant(
    segment_id: int,
    payload: SegmentParticipantUpdateRequest,
    user=Depends(owned_user),
    db: Session = Depends(get_db),
):
    segment = db.get(TranscriptSegment, segment_id)
    if segment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transcript segment bulunamadı.")
    meeting = _owned_meeting(db, user.id, segment.meeting_id)
    try:
        reassign_segment_participant(db, segment_id, payload.participant_id)
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "segment.participant.updated",
        "Transcript segment participant updated",
        meeting_id=meeting.id,
        segment_id=segment_id,
        participant_id=payload.participant_id,
        user_id=user.id,
    )
    return {"ok": True}


@router.post("/api/meetings/{meeting_id}/participants/merge")
def merge_meeting_participants(
    meeting_id: int,
    payload: ParticipantMergeRequest,
    user=Depends(owned_user),
    db: Session = Depends(get_db),
):
    meeting = _owned_meeting(db, user.id, meeting_id)
    try:
        moved_count = merge_participants(
            db,
            meeting_id,
            payload.source_participant_id,
            payload.target_participant_id,
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "participant.merged",
        "Meeting participants merged",
        meeting_id=meeting.id,
        source_participant_id=payload.source_participant_id,
        target_participant_id=payload.target_participant_id,
        moved_count=moved_count,
        user_id=user.id,
    )
    return {"ok": True, "moved_count": moved_count}


@router.post("/api/meetings/{meeting_id}/participants/split")
def split_meeting_participant(
    meeting_id: int,
    payload: ParticipantSplitRequest,
    user=Depends(owned_user),
    db: Session = Depends(get_db),
):
    meeting = _owned_meeting(db, user.id, meeting_id)
    try:
        participant = split_participant(
            db,
            meeting_id,
            payload.participant_id,
            payload.segment_ids,
            payload.display_name,
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "participant.split",
        "Meeting participant split",
        meeting_id=meeting.id,
        source_participant_id=payload.participant_id,
        new_participant_id=participant.id,
        moved_segment_count=len(payload.segment_ids),
        user_id=user.id,
    )
    return {"ok": True, "participant_id": participant.id}
