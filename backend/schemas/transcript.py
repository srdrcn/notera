from __future__ import annotations

from pydantic import BaseModel, Field


class ReviewOut(BaseModel):
    id: int
    review_type: str
    confidence_label: str
    current_text: str
    suggested_text: str
    current_participant_id: int | None
    suggested_participant_id: int | None
    audio_clip_url: str | None
    has_audio_clip: bool


class ParticipantOut(BaseModel):
    id: int
    display_name: str
    binding_state: str
    segment_count: int
    has_audio_asset: bool
    is_bot: bool
    join_state: str


class SegmentEntryOut(BaseModel):
    id: int
    participant_id: int | None
    speaker: str
    text: str
    raw_text: str
    timestamp: str
    start_sec: float | None
    end_sec: float | None
    initials: str
    color: str
    assignment_method: str
    assignment_confidence: float
    needs_speaker_review: bool
    overlap_group_id: str | None
    resolution_status: str
    review: ReviewOut | None


class SnapshotMeetingOut(BaseModel):
    id: int
    title: str
    status: str


class SnapshotSummaryOut(BaseModel):
    speaker_count: int
    segment_count: int
    pending_speaker_review_count: int


class SnapshotAudioOut(BaseModel):
    status: str
    error: str | None
    has_audio: bool
    audio_url: str | None
    label: str


class SnapshotPostprocessOut(BaseModel):
    status: str
    error: str | None
    progress_pct: int | None
    progress_note: str | None


class SnapshotPreviewOut(BaseModel):
    has_preview: bool
    image_url: str | None
    label: str


class SnapshotActionsOut(BaseModel):
    pending_review_count: int
    can_stop_meeting: bool
    can_manage_speakers: bool


class MeetingSnapshotOut(BaseModel):
    meeting: SnapshotMeetingOut
    summary: SnapshotSummaryOut
    audio: SnapshotAudioOut
    postprocess: SnapshotPostprocessOut
    preview: SnapshotPreviewOut
    participants: list[ParticipantOut]
    segments: list[SegmentEntryOut]
    actions: SnapshotActionsOut


class SegmentParticipantUpdateRequest(BaseModel):
    participant_id: int | None = None


class ParticipantMergeRequest(BaseModel):
    source_participant_id: int
    target_participant_id: int


class ParticipantSplitRequest(BaseModel):
    participant_id: int
    segment_ids: list[int] = Field(default_factory=list)
    display_name: str
