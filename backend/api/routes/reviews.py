from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.api.deps import owned_user
from backend.db.session import get_db
from backend.models import Meeting, Transcript, TranscriptReviewItem
from backend.runtime.logging import bind_context, log_event
from backend.services.reviews import (
    apply_all_reviews,
    apply_review,
    duplicate_merge_candidate_count,
    keep_review,
    merge_duplicate_transcripts,
)


router = APIRouter(tags=["reviews"])
logger = logging.getLogger("notera.routes.reviews")


def _owned_review(db: Session, user_id: int, review_id: int) -> tuple[TranscriptReviewItem, Meeting]:
    review = db.scalar(select(TranscriptReviewItem).where(TranscriptReviewItem.id == review_id))
    if review is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Düzeltme önerisi bulunamadı.")
    transcript = db.get(Transcript, review.transcript_id)
    meeting = db.get(Meeting, transcript.meeting_id) if transcript else None
    if transcript is None or meeting is None or meeting.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Düzeltme önerisi bulunamadı.")
    bind_context(meeting_id=meeting.id)
    return review, meeting


@router.post("/api/reviews/{review_id}/apply")
def review_apply(review_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    review, meeting = _owned_review(db, user.id, review_id)
    try:
        apply_review(db, review_id)
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "review.applied",
        "Review applied",
        review_id=review.id,
        meeting_id=meeting.id,
        user_id=user.id,
    )
    return {"ok": True}


@router.post("/api/reviews/{review_id}/keep")
def review_keep(review_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    review, meeting = _owned_review(db, user.id, review_id)
    try:
        keep_review(db, review_id)
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    log_event(
        logger,
        logging.INFO,
        "review.kept",
        "Review kept",
        review_id=review.id,
        meeting_id=meeting.id,
        user_id=user.id,
    )
    return {"ok": True}


@router.post("/api/meetings/{meeting_id}/reviews/apply-all")
def apply_all_for_meeting(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = db.get(Meeting, meeting_id)
    if meeting is None or meeting.user_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    count = apply_all_reviews(db, meeting_id)
    db.commit()
    log_event(
        logger,
        logging.INFO,
        "review.apply_all.completed",
        "All pending reviews applied",
        meeting_id=meeting.id,
        user_id=user.id,
        applied_count=count,
    )
    return {"ok": True, "applied_count": count}


@router.post("/api/meetings/{meeting_id}/transcripts/merge-duplicates")
def merge_duplicates(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = db.get(Meeting, meeting_id)
    if meeting is None or meeting.user_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    if duplicate_merge_candidate_count(db, meeting_id) <= 0:
        return {"ok": True, "merged_count": 0}
    merged_count = merge_duplicate_transcripts(db, meeting_id)
    db.commit()
    log_event(
        logger,
        logging.INFO,
        "transcript.duplicates.merged",
        "Duplicate transcripts merged",
        meeting_id=meeting.id,
        user_id=user.id,
        merged_count=merged_count,
    )
    return {"ok": True, "merged_count": merged_count}
