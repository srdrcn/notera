from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import MeetingParticipant, Transcript, TranscriptReviewItem, TranscriptSegment
from backend.runtime.constants import REVIEW_STATUS_ACCEPTED, REVIEW_STATUS_PENDING, TRANSCRIPT_STATUS_PENDING_REVIEW


def _normalized_name(value: str) -> str:
    return " ".join((value or "").strip().split()).casefold()


def pending_speaker_review_count(db: Session, meeting_id: int) -> int:
    return sum(
        1
        for _ in db.scalars(
            select(TranscriptSegment.id).where(
                TranscriptSegment.meeting_id == meeting_id,
                TranscriptSegment.needs_speaker_review.is_(True),
            )
        )
    )


def reassign_segment_participant(
    db: Session,
    segment_id: int,
    participant_id: int | None,
) -> TranscriptSegment:
    segment = db.get(TranscriptSegment, segment_id)
    if segment is None:
        raise ValueError("Transcript segment bulunamadı.")

    participant = None
    if participant_id is not None:
        participant = db.get(MeetingParticipant, participant_id)
        if participant is None or participant.meeting_id != segment.meeting_id:
            raise ValueError("Konusmaci bulunamadi.")

    segment.participant_id = participant_id
    segment.assignment_method = "manual_reassignment"
    segment.assignment_confidence = 1.0 if participant_id is not None else 0.0
    segment.needs_speaker_review = participant_id is None
    segment.resolution_status = "accepted" if participant_id is not None else TRANSCRIPT_STATUS_PENDING_REVIEW
    segment.updated_at = datetime.utcnow()
    db.add(segment)

    review_items = list(
        db.scalars(
            select(TranscriptReviewItem).where(
                TranscriptReviewItem.transcript_segment_id == segment.id,
                TranscriptReviewItem.status == REVIEW_STATUS_PENDING,
            )
        )
    )
    for item in review_items:
        item.current_participant_id = participant_id
        item.suggested_participant_id = participant_id
        item.suggested_text = participant.display_name if participant else "Unknown"
        item.status = REVIEW_STATUS_ACCEPTED if participant_id is not None else REVIEW_STATUS_PENDING
        item.updated_at = datetime.utcnow()
        db.add(item)
        if item.transcript_id:
            transcript = db.get(Transcript, item.transcript_id)
            if transcript is not None:
                transcript.speaker = participant.display_name if participant else "Unknown"
                transcript.resolution_status = segment.resolution_status
                transcript.updated_at = datetime.utcnow()
                db.add(transcript)
    return segment


def merge_participants(
    db: Session,
    meeting_id: int,
    source_participant_id: int,
    target_participant_id: int,
) -> int:
    if source_participant_id == target_participant_id:
        return 0
    source = db.get(MeetingParticipant, source_participant_id)
    target = db.get(MeetingParticipant, target_participant_id)
    if source is None or target is None or source.meeting_id != meeting_id or target.meeting_id != meeting_id:
        raise ValueError("Konusmaci bulunamadi.")

    moved_count = 0
    segments = list(
        db.scalars(
            select(TranscriptSegment).where(
                TranscriptSegment.meeting_id == meeting_id,
                TranscriptSegment.participant_id == source_participant_id,
            )
        )
    )
    for segment in segments:
        segment.participant_id = target_participant_id
        segment.assignment_method = "manual_merge"
        segment.assignment_confidence = 1.0
        segment.needs_speaker_review = False
        segment.resolution_status = "accepted"
        segment.updated_at = datetime.utcnow()
        db.add(segment)
        moved_count += 1

    review_items = list(
        db.scalars(
            select(TranscriptReviewItem).where(
                (TranscriptReviewItem.current_participant_id == source_participant_id)
                | (TranscriptReviewItem.suggested_participant_id == source_participant_id)
            )
        )
    )
    for item in review_items:
        if item.current_participant_id == source_participant_id:
            item.current_participant_id = target_participant_id
        if item.suggested_participant_id == source_participant_id:
            item.suggested_participant_id = target_participant_id
        item.updated_at = datetime.utcnow()
        db.add(item)

    source.merged_into_participant_id = target_participant_id
    source.join_state = "merged"
    source.updated_at = datetime.utcnow()
    db.add(source)
    return moved_count


def split_participant(
    db: Session,
    meeting_id: int,
    participant_id: int,
    segment_ids: list[int],
    display_name: str,
) -> MeetingParticipant:
    source = db.get(MeetingParticipant, participant_id)
    if source is None or source.meeting_id != meeting_id:
        raise ValueError("Konusmaci bulunamadi.")
    if not segment_ids:
        raise ValueError("Bolunecek segment secilmedi.")
    normalized_name = _normalized_name(display_name)
    if not normalized_name:
        raise ValueError("Yeni konusmaci adi bos olamaz.")

    created_at = datetime.utcnow()
    new_participant = MeetingParticipant(
        meeting_id=meeting_id,
        participant_key=f"manual-split:{participant_id}:{int(created_at.timestamp() * 1000)}",
        platform_identity=None,
        display_name=" ".join(display_name.strip().split()),
        normalized_name=normalized_name,
        role=source.role,
        is_bot=False,
        join_state="present",
        merged_into_participant_id=None,
        first_seen_at=created_at,
        last_seen_at=created_at,
        created_at=created_at,
        updated_at=created_at,
    )
    db.add(new_participant)
    db.flush()

    segments = list(
        db.scalars(
            select(TranscriptSegment).where(
                TranscriptSegment.meeting_id == meeting_id,
                TranscriptSegment.id.in_(segment_ids),
            )
        )
    )
    for segment in segments:
        segment.participant_id = new_participant.id
        segment.assignment_method = "manual_split"
        segment.assignment_confidence = 1.0
        segment.needs_speaker_review = False
        segment.resolution_status = "accepted"
        segment.updated_at = datetime.utcnow()
        db.add(segment)

    return new_participant
