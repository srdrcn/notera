from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.models.base import Base


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )


class AuthSession(Base):
    __tablename__ = "auth_session"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), index=True, nullable=False)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    user_agent: Mapped[str | None] = mapped_column(Text)
    ip_address: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime)


class WorkerRun(Base):
    __tablename__ = "worker_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    worker_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    pid: Mapped[int | None] = mapped_column(Integer)
    exit_code: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    stop_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    ended_at: Mapped[datetime | None] = mapped_column(DateTime)


class Meeting(Base):
    __tablename__ = "meeting"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), index=True, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    teams_link: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    bot_pid: Mapped[int | None] = mapped_column(Integer)
    audio_recording_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    audio_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    audio_error: Mapped[str | None] = mapped_column(Text)
    postprocess_status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    postprocess_error: Mapped[str | None] = mapped_column(Text)
    postprocess_progress_pct: Mapped[int | None] = mapped_column(Integer)
    postprocess_progress_note: Mapped[str | None] = mapped_column(Text)
    stop_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    active_bot_run_id: Mapped[int | None] = mapped_column(Integer)
    active_postprocess_run_id: Mapped[int | None] = mapped_column(Integer)
    joined_at: Mapped[datetime | None] = mapped_column(DateTime)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime)
    audio_capture_started_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.current_timestamp(),
        onupdate=datetime.utcnow,
    )

    user: Mapped[User | None] = relationship(lazy="joined")
    worker_runs: Mapped[list[WorkerRun]] = relationship(lazy="selectin")


class TeamsCaptionEvent(Base):
    __tablename__ = "teamscaptionevent"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    speaker_name: Mapped[str] = mapped_column(Text, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    slot_index: Mapped[int | None] = mapped_column(Integer)
    revision_no: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class MeetingAudioAsset(Base):
    __tablename__ = "meetingaudioasset"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    master_audio_path: Mapped[str] = mapped_column(Text, nullable=False)
    pcm_audio_path: Mapped[str | None] = mapped_column(Text)
    format: Mapped[str] = mapped_column(String(32), nullable=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    postprocess_version: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )


class MeetingParticipant(Base):
    __tablename__ = "meetingparticipant"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    participant_key: Mapped[str] = mapped_column(String(255), nullable=False)
    platform_identity: Mapped[str | None] = mapped_column(String(255), index=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    role: Mapped[str | None] = mapped_column(String(64))
    is_bot: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    join_state: Mapped[str] = mapped_column(String(32), default="present", nullable=False)
    merged_into_participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"))
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.current_timestamp(),
        onupdate=datetime.utcnow,
    )


class SpeakerActivityEvent(Base):
    __tablename__ = "speakeractivityevent"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    participant_id: Mapped[int] = mapped_column(ForeignKey("meetingparticipant.id"), index=True, nullable=False)
    start_offset_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    end_offset_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    signal_kind: Mapped[str | None] = mapped_column(String(64))
    event_confidence: Mapped[float | None] = mapped_column(Float)
    ui_observed_at: Mapped[datetime | None] = mapped_column(DateTime)
    relative_offset_ms: Mapped[int | None] = mapped_column(Integer)
    source_session_id: Mapped[str | None] = mapped_column(String(128))
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    metadata_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )


class AudioSource(Base):
    __tablename__ = "audiosource"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    source_key: Mapped[str] = mapped_column(String(255), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    track_id: Mapped[str | None] = mapped_column(String(255))
    stream_id: Mapped[str | None] = mapped_column(String(255))
    file_path: Mapped[str | None] = mapped_column(Text)
    format: Mapped[str | None] = mapped_column(String(32))
    sample_rate_hz: Mapped[int | None] = mapped_column(Integer)
    channel_count: Mapped[int | None] = mapped_column(Integer)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )


class AudioSourceBinding(Base):
    __tablename__ = "audiosourcebinding"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    audio_source_id: Mapped[int] = mapped_column(ForeignKey("audiosource.id"), index=True, nullable=False)
    participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"), index=True)
    valid_from_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    valid_to_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    binding_status: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    binding_method: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        server_default=func.current_timestamp(),
        onupdate=datetime.utcnow,
    )


class IdentityEvidence(Base):
    __tablename__ = "identityevidence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"), index=True)
    audio_source_id: Mapped[int | None] = mapped_column(ForeignKey("audiosource.id"), index=True)
    evidence_type: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_value: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    payload_json: Mapped[str | None] = mapped_column(Text)


class ParticipantAudioAsset(Base):
    __tablename__ = "participantaudioasset"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"), index=True)
    audio_source_id: Mapped[int | None] = mapped_column(ForeignKey("audiosource.id"), index=True)
    asset_type: Mapped[str] = mapped_column(String(64), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str | None] = mapped_column(String(32))
    sample_rate_hz: Mapped[int | None] = mapped_column(Integer)
    channel_count: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    start_offset_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    end_offset_ms: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    derivation_method: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )


class TranscriptSegment(Base):
    __tablename__ = "transcriptsegment"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"), index=True)
    participant_audio_asset_id: Mapped[int | None] = mapped_column(ForeignKey("participantaudioasset.id"), index=True)
    audio_source_id: Mapped[int | None] = mapped_column(ForeignKey("audiosource.id"), index=True)
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str | None] = mapped_column(String(16))
    start_offset_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    end_offset_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    asr_confidence: Mapped[float | None] = mapped_column(Float)
    assignment_method: Mapped[str] = mapped_column(String(64), nullable=False)
    assignment_confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    speaker_resolution_status: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    overlap_group_id: Mapped[str | None] = mapped_column(String(64), index=True)
    needs_speaker_review: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    resolution_status: Mapped[str] = mapped_column(String(32), default="original", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime)


class Transcript(Base):
    __tablename__ = "transcript"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    meeting_id: Mapped[int] = mapped_column(ForeignKey("meeting.id"), index=True, nullable=False)
    sequence_no: Mapped[int | None] = mapped_column(Integer)
    speaker: Mapped[str] = mapped_column(Text, nullable=False)
    teams_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    start_sec: Mapped[float | None] = mapped_column(Float)
    end_sec: Mapped[float | None] = mapped_column(Float)
    caption_started_at: Mapped[datetime | None] = mapped_column(DateTime)
    caption_finalized_at: Mapped[datetime | None] = mapped_column(DateTime)
    resolution_status: Mapped[str] = mapped_column(String(32), default="original", nullable=False)
    auto_corrected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime)


class TranscriptReviewItem(Base):
    __tablename__ = "transcriptreviewitem"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transcript_id: Mapped[int] = mapped_column(ForeignKey("transcript.id"), index=True, nullable=False)
    transcript_segment_id: Mapped[int | None] = mapped_column(ForeignKey("transcriptsegment.id"), index=True)
    review_type: Mapped[str] = mapped_column(String(32), default="text", nullable=False)
    granularity: Mapped[str] = mapped_column(String(32), nullable=False)
    current_text: Mapped[str] = mapped_column(Text, nullable=False)
    suggested_text: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    current_participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"), index=True)
    suggested_participant_id: Mapped[int | None] = mapped_column(ForeignKey("meetingparticipant.id"), index=True)
    audio_clip_path: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    clip_start_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    clip_end_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime)
