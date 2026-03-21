from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator

from backend.runtime.teams_links import normalize_teams_join_target


class CreateMeetingRequest(BaseModel):
    title: str
    teams_link: str
    audio_recording_enabled: bool = True

    @field_validator("teams_link")
    @classmethod
    def validate_teams_link(cls, value: str) -> str:
        return normalize_teams_join_target(value)


class MeetingSummaryOut(BaseModel):
    id: int
    title: str
    status: str
    audio_status: str
    postprocess_status: str
    postprocess_progress_pct: int | None
    postprocess_progress_note: str | None
    created_at: datetime | None
    joined_at: datetime | None
    ended_at: datetime | None
    can_join: bool
    can_stop: bool
    can_view_transcripts: bool
