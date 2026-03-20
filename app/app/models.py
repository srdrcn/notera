import reflex as rx
from sqlmodel import Field, SQLModel, Relationship
from datetime import datetime
from typing import List, Optional


class User(rx.Model, table=True):
    """User profile."""
    email: str = Field(unique=True, index=True, nullable=False)
    meetings: List["Meeting"] = Relationship(back_populates="user")


class Meeting(rx.Model, table=True):
    """Meeting details."""
    user_id: int = Field(foreign_key="user.id")
    title: str
    teams_link: str
    status: str = "pending"  # pending, joining, active, completed
    bot_pid: Optional[int] = None
    audio_recording_enabled: bool = True
    audio_status: str = "pending"
    audio_error: Optional[str] = None
    postprocess_status: str = "pending"
    postprocess_error: Optional[str] = None
    joined_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    user: Optional[User] = Relationship(back_populates="meetings")
    caption_events: List["TeamsCaptionEvent"] = Relationship(back_populates="meeting")
    transcripts: List["Transcript"] = Relationship(back_populates="meeting")
    audio_assets: List["MeetingAudioAsset"] = Relationship(back_populates="meeting")


class TeamsCaptionEvent(rx.Model, table=True):
    """Raw live caption events captured from Teams during the meeting."""
    meeting_id: int = Field(foreign_key="meeting.id")
    sequence_no: int
    speaker_name: str
    text: str
    observed_at: datetime = Field(default_factory=datetime.utcnow)
    slot_index: Optional[int] = None
    revision_no: int = 0

    meeting: Optional[Meeting] = Relationship(back_populates="caption_events")


class Transcript(rx.Model, table=True):
    """Meeting transcripts."""
    meeting_id: int = Field(foreign_key="meeting.id")
    sequence_no: Optional[int] = None
    speaker: str
    teams_text: str = ""
    text: str
    start_sec: Optional[float] = None
    end_sec: Optional[float] = None
    caption_started_at: Optional[datetime] = None
    caption_finalized_at: Optional[datetime] = None
    resolution_status: str = "original"
    auto_corrected: bool = False
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    meeting: Optional[Meeting] = Relationship(back_populates="transcripts")
    review_items: List["TranscriptReviewItem"] = Relationship(back_populates="transcript")


class MeetingAudioAsset(rx.Model, table=True):
    """Persisted audio artifact for a meeting."""
    meeting_id: int = Field(foreign_key="meeting.id")
    master_audio_path: str
    pcm_audio_path: Optional[str] = None
    format: str
    duration_ms: Optional[int] = None
    status: str = "pending"
    postprocess_version: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    meeting: Optional[Meeting] = Relationship(back_populates="audio_assets")


class TranscriptReviewItem(rx.Model, table=True):
    """Suggested transcript correction requiring review or audit."""
    transcript_id: int = Field(foreign_key="transcript.id")
    granularity: str
    current_text: str
    suggested_text: str
    confidence: float = 0.0
    audio_clip_path: Optional[str] = None
    status: str = "pending"
    clip_start_ms: int = 0
    clip_end_ms: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)

    transcript: Optional[Transcript] = Relationship(back_populates="review_items")
