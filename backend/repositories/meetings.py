from __future__ import annotations

from sqlalchemy import delete, desc, select
from sqlalchemy.orm import Session

from backend.models import (
    AudioSource,
    AudioSourceBinding,
    IdentityEvidence,
    Meeting,
    MeetingAudioAsset,
    MeetingParticipant,
    ParticipantAudioAsset,
    SpeakerActivityEvent,
    TeamsCaptionEvent,
    Transcript,
    TranscriptReviewItem,
    TranscriptSegment,
    WorkerRun,
)


def list_meetings_for_user(db: Session, user_id: int) -> list[Meeting]:
    return list(
        db.scalars(
            select(Meeting).where(Meeting.user_id == user_id).order_by(desc(Meeting.created_at))
        )
    )


def get_owned_meeting(db: Session, user_id: int, meeting_id: int) -> Meeting | None:
    return db.scalar(
        select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == user_id)
    )
def latest_audio_asset(db: Session, meeting_id: int) -> MeetingAudioAsset | None:
    return db.scalar(
        select(MeetingAudioAsset)
        .where(MeetingAudioAsset.meeting_id == meeting_id)
        .order_by(MeetingAudioAsset.id.desc())
    )


def latest_mixed_audio_source(db: Session, meeting_id: int) -> AudioSource | None:
    return db.scalar(
        select(AudioSource)
        .where(AudioSource.meeting_id == meeting_id, AudioSource.source_kind == "meeting_mixed_master")
        .order_by(AudioSource.id.desc())
    )


def participants_for_meeting(db: Session, meeting_id: int) -> list[MeetingParticipant]:
    return list(
        db.scalars(
            select(MeetingParticipant)
            .where(MeetingParticipant.meeting_id == meeting_id)
            .order_by(MeetingParticipant.display_name, MeetingParticipant.id)
        )
    )


def segments_for_meeting(db: Session, meeting_id: int) -> list[TranscriptSegment]:
    return list(
        db.scalars(
            select(TranscriptSegment)
            .where(TranscriptSegment.meeting_id == meeting_id)
            .order_by(TranscriptSegment.sequence_no, TranscriptSegment.start_offset_ms, TranscriptSegment.id)
        )
    )


def review_items_for_segments(
    db: Session,
    segment_ids: list[int],
    pending_only: bool = True,
) -> list[TranscriptReviewItem]:
    if not segment_ids:
        return []
    stmt = select(TranscriptReviewItem).where(
        TranscriptReviewItem.transcript_segment_id.in_(segment_ids)
    )
    if pending_only:
        stmt = stmt.where(TranscriptReviewItem.status == "pending")
    stmt = stmt.order_by(TranscriptReviewItem.id)
    return list(db.scalars(stmt))


def participant_audio_assets_for_meeting(db: Session, meeting_id: int) -> list[ParticipantAudioAsset]:
    return list(
        db.scalars(
            select(ParticipantAudioAsset)
            .where(ParticipantAudioAsset.meeting_id == meeting_id)
            .order_by(ParticipantAudioAsset.participant_id, ParticipantAudioAsset.start_offset_ms, ParticipantAudioAsset.id)
        )
    )


def transcripts_for_meeting(db: Session, meeting_id: int) -> list[Transcript]:
    return list(
        db.scalars(
            select(Transcript)
            .where(Transcript.meeting_id == meeting_id)
            .order_by(Transcript.sequence_no, Transcript.timestamp, Transcript.id)
        )
    )


def review_items_for_transcripts(
    db: Session,
    transcript_ids: list[int],
    pending_only: bool = True,
) -> list[TranscriptReviewItem]:
    if not transcript_ids:
        return []
    stmt = select(TranscriptReviewItem).where(
        TranscriptReviewItem.transcript_id.in_(transcript_ids)
    )
    if pending_only:
        stmt = stmt.where(TranscriptReviewItem.status == "pending")
    stmt = stmt.order_by(TranscriptReviewItem.id)
    return list(db.scalars(stmt))
def delete_meeting_related_rows(db: Session, meeting_id: int) -> None:
    transcript_ids = [row.id for row in transcripts_for_meeting(db, meeting_id)]
    segment_ids = [row.id for row in segments_for_meeting(db, meeting_id)]
    if transcript_ids:
        db.execute(
            delete(TranscriptReviewItem).where(
                TranscriptReviewItem.transcript_id.in_(transcript_ids)
            )
        )
    if segment_ids:
        db.execute(
            delete(TranscriptReviewItem).where(
                TranscriptReviewItem.transcript_segment_id.in_(segment_ids)
            )
        )
    db.execute(delete(TranscriptSegment).where(TranscriptSegment.meeting_id == meeting_id))
    db.execute(delete(ParticipantAudioAsset).where(ParticipantAudioAsset.meeting_id == meeting_id))
    db.execute(delete(AudioSourceBinding).where(AudioSourceBinding.meeting_id == meeting_id))
    db.execute(delete(IdentityEvidence).where(IdentityEvidence.meeting_id == meeting_id))
    db.execute(delete(SpeakerActivityEvent).where(SpeakerActivityEvent.meeting_id == meeting_id))
    db.execute(delete(AudioSource).where(AudioSource.meeting_id == meeting_id))
    db.execute(delete(MeetingParticipant).where(MeetingParticipant.meeting_id == meeting_id))
    db.execute(delete(MeetingAudioAsset).where(MeetingAudioAsset.meeting_id == meeting_id))
    db.execute(delete(TeamsCaptionEvent).where(TeamsCaptionEvent.meeting_id == meeting_id))
    db.execute(delete(Transcript).where(Transcript.meeting_id == meeting_id))
    db.execute(delete(WorkerRun).where(WorkerRun.meeting_id == meeting_id))
