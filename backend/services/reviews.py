from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Meeting, Transcript, TranscriptReviewItem
from backend.runtime.constants import (
    REVIEW_STATUS_ACCEPTED,
    REVIEW_STATUS_PENDING,
    REVIEW_STATUS_REJECTED,
    TRANSCRIPT_STATUS_ACCEPTED,
    TRANSCRIPT_STATUS_REJECTED,
)
from backend.services.transcript_logic import (
    _collect_duplicate_transcript_merge_candidates,
    merge_duplicate_transcripts_after_reviews,
)


def apply_review(db: Session, review_id: int) -> TranscriptReviewItem:
    review = db.scalar(select(TranscriptReviewItem).where(TranscriptReviewItem.id == review_id))
    if review is None:
        raise ValueError("Düzeltme önerisi bulunamadı.")
    transcript = db.get(Transcript, review.transcript_id)
    if transcript is None:
        raise ValueError("Transcript satırı bulunamadı.")
    transcript.text = review.suggested_text
    transcript.resolution_status = TRANSCRIPT_STATUS_ACCEPTED
    transcript.auto_corrected = False
    transcript.updated_at = datetime.utcnow()
    review.status = REVIEW_STATUS_ACCEPTED
    review.updated_at = datetime.utcnow()
    db.add(transcript)
    db.add(review)
    return review


def keep_review(db: Session, review_id: int) -> TranscriptReviewItem:
    review = db.scalar(select(TranscriptReviewItem).where(TranscriptReviewItem.id == review_id))
    if review is None:
        raise ValueError("Düzeltme önerisi bulunamadı.")
    transcript = db.get(Transcript, review.transcript_id)
    if transcript is None:
        raise ValueError("Transcript satırı bulunamadı.")
    transcript.text = transcript.teams_text or transcript.text
    transcript.resolution_status = TRANSCRIPT_STATUS_REJECTED
    transcript.auto_corrected = False
    transcript.updated_at = datetime.utcnow()
    review.status = REVIEW_STATUS_REJECTED
    review.updated_at = datetime.utcnow()
    db.add(transcript)
    db.add(review)
    return review


def apply_all_reviews(db: Session, meeting_id: int) -> int:
    review_items = list(
        db.scalars(
            select(TranscriptReviewItem)
            .join(Transcript, Transcript.id == TranscriptReviewItem.transcript_id)
            .where(
                Transcript.meeting_id == meeting_id,
                TranscriptReviewItem.status == REVIEW_STATUS_PENDING,
            )
            .order_by(TranscriptReviewItem.id)
        )
    )
    if not review_items:
        return 0
    transcript_ids = [item.transcript_id for item in review_items]
    transcript_map = {
        transcript.id: transcript
        for transcript in db.scalars(select(Transcript).where(Transcript.id.in_(transcript_ids)))
    }
    applied_count = 0
    for review in review_items:
        transcript = transcript_map.get(review.transcript_id)
        if transcript is None:
            continue
        transcript.text = review.suggested_text
        transcript.resolution_status = TRANSCRIPT_STATUS_ACCEPTED
        transcript.auto_corrected = False
        transcript.updated_at = datetime.utcnow()
        review.status = REVIEW_STATUS_ACCEPTED
        review.updated_at = datetime.utcnow()
        db.add(transcript)
        db.add(review)
        applied_count += 1
    return applied_count


def duplicate_merge_candidate_count(db: Session, meeting_id: int) -> int:
    transcripts = list(
        db.scalars(
            select(Transcript)
            .where(Transcript.meeting_id == meeting_id)
            .order_by(Transcript.sequence_no, Transcript.timestamp, Transcript.id)
        )
    )
    count, _ = _collect_duplicate_transcript_merge_candidates(transcripts)
    return count


def merge_duplicate_transcripts(db: Session, meeting_id: int) -> int:
    meeting = db.get(Meeting, meeting_id)
    if meeting is None:
        return 0
    review_items = list(
        db.scalars(
            select(TranscriptReviewItem)
            .join(Transcript, Transcript.id == TranscriptReviewItem.transcript_id)
            .where(Transcript.meeting_id == meeting_id)
            .order_by(TranscriptReviewItem.id)
        )
    )
    transcripts = list(
        db.scalars(
            select(Transcript)
            .where(Transcript.meeting_id == meeting_id)
            .order_by(Transcript.sequence_no, Transcript.timestamp, Transcript.id)
        )
    )
    merged_count = merge_duplicate_transcripts_after_reviews(meeting, transcripts, review_items)
    if merged_count <= 0:
        return 0

    current_ids = {transcript.id for transcript in transcripts}
    existing_rows = list(
        db.scalars(select(Transcript).where(Transcript.meeting_id == meeting_id))
    )
    for transcript in existing_rows:
        if transcript.id not in current_ids:
            db.delete(transcript)
    for transcript in transcripts:
        db.add(transcript)
    for review_item in review_items:
        db.add(review_item)
    return merged_count
