from __future__ import annotations

from pydantic import BaseModel


class ReviewOut(BaseModel):
    id: int
    granularity: str
    confidence_label: str
    current_text: str
    suggested_text: str
    audio_clip_url: str | None
    has_audio_clip: bool


class TranscriptEntryOut(BaseModel):
    id: int
    speaker: str
    text: str
    teams_text: str
    timestamp: str
    start_sec: float | None
    end_sec: float | None
    initials: str
    color: str
    resolution_status: str
    auto_corrected: bool
    has_pending_review: bool
    has_duplicate_merge_candidate: bool
    review: ReviewOut | None


class SnapshotMeetingOut(BaseModel):
    id: int
    title: str
    status: str


class SnapshotSummaryOut(BaseModel):
    speaker_count: int
    transcript_count: int


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
    duplicate_merge_candidate_count: int
    can_apply_all_reviews: bool
    can_merge_duplicate_transcripts: bool
    can_stop_meeting: bool


class MeetingSnapshotOut(BaseModel):
    meeting: SnapshotMeetingOut
    summary: SnapshotSummaryOut
    audio: SnapshotAudioOut
    postprocess: SnapshotPostprocessOut
    preview: SnapshotPreviewOut
    transcripts: list[TranscriptEntryOut]
    actions: SnapshotActionsOut
