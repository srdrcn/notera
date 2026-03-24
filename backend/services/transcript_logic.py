from __future__ import annotations

import hashlib
from pathlib import Path

from backend.models import (
    Meeting,
    MeetingAudioAsset,
    MeetingParticipant,
    ParticipantAudioAsset,
    TranscriptReviewItem,
    TranscriptSegment,
)
from backend.schemas.transcript import (
    MeetingSnapshotOut,
    ParticipantOut,
    ReviewOut,
    SegmentEntryOut,
    SnapshotActionsOut,
    SnapshotAudioOut,
    SnapshotMeetingOut,
    SnapshotPostprocessOut,
    SnapshotPreviewOut,
    SnapshotSummaryOut,
)
from backend.runtime.paths import get_meeting_pcm_audio_path, preview_path
from backend.runtime.participant_names import is_roster_heading_name, normalize_participant_name


def speaker_initials(name: str) -> str:
    parts = [p for p in (name or "").split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def speaker_color(meeting_id: int, name: str) -> str:
    colors = ["tomato", "red", "ruby", "crimson", "blue", "cyan", "teal", "jade", "green", "grass", "orange", "amber"]
    seed = f"{meeting_id}:{name}".encode("utf-8")
    return colors[hashlib.sha256(seed).digest()[0] % len(colors)]


def review_audio_url(review_item: TranscriptReviewItem | None) -> str | None:
    if not review_item or not review_item.audio_clip_path:
        return None
    return f"/api/media/reviews/{review_item.id}/clip"


def _build_audio_payload(meeting: Meeting, audio_asset: MeetingAudioAsset | None) -> SnapshotAudioOut:
    if audio_asset is None or not audio_asset.master_audio_path:
        return SnapshotAudioOut(
            status=meeting.audio_status,
            error=meeting.audio_error,
            has_audio=False,
            audio_url=None,
            label="Henüz ses kaydı yok",
        )
    preferred_path = Path(audio_asset.master_audio_path)
    pcm_audio = get_meeting_pcm_audio_path(meeting.id)
    if pcm_audio.exists():
        preferred_path = pcm_audio
    return SnapshotAudioOut(
        status=meeting.audio_status,
        error=meeting.audio_error,
        has_audio=preferred_path.exists(),
        audio_url=f"/api/media/meetings/{meeting.id}/audio" if preferred_path.exists() else None,
        label="Toplantı kaydı",
    )


def _build_preview_payload(meeting: Meeting) -> SnapshotPreviewOut:
    if meeting.user_id is None:
        return SnapshotPreviewOut(has_preview=False, image_url=None, label="")
    path = preview_path(meeting.user_id, meeting.id)
    if path.exists() and meeting.status in {"joining", "active"}:
        version = int(path.stat().st_mtime)
        return SnapshotPreviewOut(
            has_preview=True,
            image_url=f"/api/media/meetings/{meeting.id}/preview?v={version}",
            label="Son canlı kare",
        )
    label = "Canlı önizleme toplantı sırasında görünür." if meeting.status in {"joining", "active"} else ""
    return SnapshotPreviewOut(has_preview=False, image_url=None, label=label)


def participant_binding_state(
    participant: MeetingParticipant,
    segments: list[TranscriptSegment],
) -> str:
    return participant_binding_state_for_ids({participant.id}, segments)


def participant_binding_state_for_ids(
    participant_ids: set[int],
    segments: list[TranscriptSegment],
) -> str:
    participant_segments = [segment for segment in segments if segment.participant_id in participant_ids]
    if not participant_segments:
        return "unknown"
    if any(segment.needs_speaker_review for segment in participant_segments):
        return "provisional"
    return "confirmed"


def participant_binding_rank(binding_state: str) -> int:
    return {
        "confirmed": 2,
        "provisional": 1,
        "unknown": 0,
    }.get(binding_state, 0)


def participant_key_rank(participant_key: str | None) -> int:
    normalized = (participant_key or "").strip().lower()
    if normalized.startswith("teams-platform:"):
        return 3
    if normalized.startswith("teams-roster:"):
        return 2
    if normalized.startswith("teams-name:"):
        return 1
    return 0


def should_collapse_duplicate_participants(participants: list[MeetingParticipant]) -> bool:
    return len(participants) > 1 and any((participant.participant_key or "").startswith("teams-name:") for participant in participants)


def build_snapshot(
    meeting: Meeting,
    participants: list[MeetingParticipant],
    segments: list[TranscriptSegment],
    review_items: list[TranscriptReviewItem],
    audio_asset: MeetingAudioAsset | None,
    participant_audio_assets: list[ParticipantAudioAsset],
) -> MeetingSnapshotOut:
    participant_map = {participant.id: participant for participant in participants}
    review_map = {item.transcript_segment_id: item for item in review_items if item.transcript_segment_id}
    asset_participant_ids = {asset.participant_id for asset in participant_audio_assets if asset.participant_id is not None}

    segment_rows = []
    for segment in segments:
        participant = participant_map.get(segment.participant_id) if segment.participant_id is not None else None
        speaker = (
            participant.display_name
            if participant is not None and not is_roster_heading_name(participant.display_name)
            else "Unknown"
        )
        review_item = review_map.get(segment.id)
        end_ms = segment.end_offset_ms if segment.end_offset_ms is not None else segment.start_offset_ms
        total_seconds = int(round((end_ms or 0) / 1000))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        timestamp = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        segment_rows.append(
            SegmentEntryOut(
                id=segment.id,
                participant_id=segment.participant_id,
                speaker=speaker,
                text=segment.text,
                raw_text=segment.raw_text,
                timestamp=timestamp,
                start_sec=(segment.start_offset_ms / 1000.0) if segment.start_offset_ms is not None else None,
                end_sec=(segment.end_offset_ms / 1000.0) if segment.end_offset_ms is not None else None,
                initials=speaker_initials(speaker),
                color=speaker_color(meeting.id, speaker),
                assignment_method=segment.assignment_method,
                assignment_confidence=segment.assignment_confidence,
                speaker_resolution_status=segment.speaker_resolution_status,
                needs_speaker_review=segment.needs_speaker_review,
                overlap_group_id=segment.overlap_group_id,
                resolution_status=segment.resolution_status,
                review=(
                    ReviewOut(
                        id=review_item.id,
                        review_type=review_item.review_type,
                        confidence_label=f"%{int(round(review_item.confidence * 100))}",
                        current_text=review_item.current_text,
                        suggested_text=review_item.suggested_text,
                        current_participant_id=review_item.current_participant_id,
                        suggested_participant_id=review_item.suggested_participant_id,
                        audio_clip_url=review_audio_url(review_item),
                        has_audio_clip=bool(review_item.audio_clip_path),
                    )
                    if review_item
                    else None
                ),
            )
        )

    segment_count_by_participant_id = {
        participant.id: sum(1 for segment in segments if segment.participant_id == participant.id)
        for participant in participants
    }
    binding_state_by_participant_id = {
        participant.id: participant_binding_state(participant, segments)
        for participant in participants
    }
    visible_participants = [
        participant
        for participant in participants
        if not participant.is_bot
        and participant.join_state != "merged"
        and not is_roster_heading_name(participant.display_name)
    ]
    participants_by_name: dict[str, list[MeetingParticipant]] = {}
    for participant in visible_participants:
        name_key = normalize_participant_name(participant.display_name).casefold()
        participants_by_name.setdefault(name_key, []).append(participant)

    participant_rows = []
    emitted_name_keys: set[str] = set()
    for participant in visible_participants:
        name_key = normalize_participant_name(participant.display_name).casefold()
        grouped_participants = participants_by_name[name_key]
        if should_collapse_duplicate_participants(grouped_participants):
            if name_key in emitted_name_keys:
                continue
            emitted_name_keys.add(name_key)
            participant_ids = {item.id for item in grouped_participants}
            representative = max(
                grouped_participants,
                key=lambda item: (
                    int(item.id in asset_participant_ids),
                    segment_count_by_participant_id.get(item.id, 0),
                    participant_binding_rank(binding_state_by_participant_id.get(item.id, "unknown")),
                    participant_key_rank(item.participant_key),
                    item.id,
                ),
            )
            participant_rows.append(
                ParticipantOut(
                    id=representative.id,
                    display_name=representative.display_name,
                    binding_state=participant_binding_state_for_ids(participant_ids, segments),
                    segment_count=sum(1 for segment in segments if segment.participant_id in participant_ids),
                    has_audio_asset=any(participant_id in asset_participant_ids for participant_id in participant_ids),
                    is_bot=representative.is_bot,
                    join_state=representative.join_state,
                )
            )
            continue

        participant_rows.append(
            ParticipantOut(
                id=participant.id,
                display_name=participant.display_name,
                binding_state=binding_state_by_participant_id.get(participant.id, "unknown"),
                segment_count=segment_count_by_participant_id.get(participant.id, 0),
                has_audio_asset=participant.id in asset_participant_ids,
                is_bot=participant.is_bot,
                join_state=participant.join_state,
            )
        )
    pending_review_count = sum(1 for segment in segments if segment.needs_speaker_review)
    return MeetingSnapshotOut(
        meeting=SnapshotMeetingOut(id=meeting.id, title=meeting.title, status=meeting.status),
        summary=SnapshotSummaryOut(
            speaker_count=len({row.speaker for row in segment_rows}),
            segment_count=len(segment_rows),
            pending_speaker_review_count=pending_review_count,
        ),
        audio=_build_audio_payload(meeting, audio_asset),
        postprocess=SnapshotPostprocessOut(
            status=meeting.postprocess_status,
            error=meeting.postprocess_error,
            progress_pct=meeting.postprocess_progress_pct,
            progress_note=meeting.postprocess_progress_note,
        ),
        preview=_build_preview_payload(meeting),
        participants=participant_rows,
        segments=segment_rows,
        actions=SnapshotActionsOut(
            pending_review_count=pending_review_count,
            can_stop_meeting=meeting.status in {"joining", "active"},
            can_manage_speakers=bool(segment_rows),
        ),
    )
