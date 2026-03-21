from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

from backend.app.models import Meeting, MeetingAudioAsset, TeamsCaptionEvent, Transcript, TranscriptReviewItem
from backend.app.runtime.constants import (
    AUDIO_STATUS_DISABLED,
    AUDIO_STATUS_FAILED,
    POSTPROCESS_STATUS_ALIGNING,
    POSTPROCESS_STATUS_COMPLETED,
    POSTPROCESS_STATUS_REVIEW_READY,
    POSTPROCESS_STATUS_TRANSCRIBING,
)
from backend.app.runtime.paths import get_meeting_pcm_audio_path, preview_path
from backend.app.schemas.transcript import (
    MeetingSnapshotOut,
    ReviewOut,
    SnapshotActionsOut,
    SnapshotAudioOut,
    SnapshotMeetingOut,
    SnapshotPostprocessOut,
    SnapshotPreviewOut,
    SnapshotSummaryOut,
    TranscriptEntryOut,
)


def _normalize_transcript_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _tokenize_transcript_text(value: str | None) -> list[dict]:
    tokens = []
    for match in re.finditer(r"[\wçğıöşüÇĞİÖŞÜ'-]+", _normalize_transcript_text(value)):
        token_text = match.group(0)
        tokens.append(
            {
                "text": token_text,
                "folded": token_text.casefold(),
                "norm": token_text.casefold(),
                "start": match.start(),
                "end": match.end(),
            }
        )
    return tokens


def _transcript_tokens(value: str | None) -> list[str]:
    return [token["folded"] for token in _tokenize_transcript_text(value)]


def _transcript_token_match(left_token: str, right_token: str) -> bool:
    if left_token == right_token:
        return True
    shorter_length = min(len(left_token), len(right_token))
    if shorter_length >= 2 and (
        left_token.startswith(right_token) or right_token.startswith(left_token)
    ):
        return True
    if shorter_length >= 4 and SequenceMatcher(None, left_token, right_token).ratio() >= 0.82:
        return True
    return False


def _fuzzy_common_prefix_token_count(left_tokens: list[str], right_tokens: list[str]) -> int:
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if not _transcript_token_match(left_token, right_token):
            break
        count += 1
    return count


def _transcript_base_text(transcript: Transcript) -> str:
    return _normalize_transcript_text(transcript.teams_text or transcript.text)


def _transcript_revision_score(text: str | None) -> int:
    normalized = _normalize_transcript_text(text)
    if not normalized:
        return -1
    return len(_transcript_tokens(normalized)) * 100 + len(normalized)


def _choose_preferred_transcript_text(existing_text: str | None, new_text: str | None) -> str:
    existing_normalized = _normalize_transcript_text(existing_text)
    new_normalized = _normalize_transcript_text(new_text)
    if not existing_normalized:
        return new_normalized
    if not new_normalized:
        return existing_normalized
    existing_score = _transcript_revision_score(existing_normalized)
    new_score = _transcript_revision_score(new_normalized)
    if new_score > existing_score:
        return new_normalized
    return existing_normalized


def _compatible_transcript_speakers(existing_speaker: str | None, new_speaker: str | None) -> bool:
    existing_value = _normalize_transcript_text(existing_speaker).casefold()
    new_value = _normalize_transcript_text(new_speaker).casefold()
    if not existing_value or existing_value == "unknown":
        return True
    if not new_value or new_value == "unknown":
        return True
    return existing_value == new_value


def _transcript_texts_should_merge(existing_text: str | None, new_text: str | None) -> bool:
    old_text = _normalize_transcript_text(existing_text)
    new_value = _normalize_transcript_text(new_text)
    if not old_text or not new_value:
        return False
    old_fold = old_text.casefold()
    new_fold = new_value.casefold()
    if old_fold == new_fold:
        return True
    punctuation = ".,!?;:…"
    if new_fold.rstrip(punctuation) == old_fold.rstrip(punctuation):
        return True
    old_tokens = _transcript_tokens(old_text)
    new_tokens = _transcript_tokens(new_value)
    if old_tokens and new_tokens:
        shared_prefix = _fuzzy_common_prefix_token_count(old_tokens, new_tokens)
        shorter_length = min(len(old_tokens), len(new_tokens))
        if shared_prefix >= min(3, shorter_length) and shared_prefix / max(shorter_length, 1) >= 0.6:
            return True
    ratio = SequenceMatcher(None, old_fold, new_fold).ratio()
    return ratio >= 0.82 and abs(len(new_fold) - len(old_fold)) <= 96


def _transcript_sort_time(transcript: Transcript) -> datetime | None:
    return transcript.caption_started_at or transcript.caption_finalized_at or transcript.timestamp


def _find_transcript_suffix_prefix_merge(existing_text: str | None, new_text: str | None):
    existing_tokens = _tokenize_transcript_text(existing_text)
    new_tokens = _tokenize_transcript_text(new_text)
    min_tokens = 4
    if len(existing_tokens) < min_tokens or len(new_tokens) < min_tokens:
        return None

    normalized_existing = _normalize_transcript_text(existing_text)
    normalized_new = _normalize_transcript_text(new_text)
    max_overlap = min(len(existing_tokens), len(new_tokens))
    for overlap_size in range(max_overlap, min_tokens - 1, -1):
        existing_suffix = existing_tokens[-overlap_size:]
        new_prefix = new_tokens[:overlap_size]
        match_count = 0
        exact_count = 0
        for existing_token, new_token in zip(existing_suffix, new_prefix):
            if not _transcript_token_match(existing_token["norm"], new_token["norm"]):
                break
            match_count += 1
            if existing_token["norm"] == new_token["norm"]:
                exact_count += 1
        if match_count != overlap_size or exact_count < max(1, overlap_size - 1):
            continue

        suffix_remainder = normalized_new[new_prefix[-1]["end"] :].strip()
        if (
            suffix_remainder
            and normalized_existing.endswith((".", "!", "?", "…"))
            and suffix_remainder[0] in ".,!?;:…"
        ):
            suffix_remainder = suffix_remainder.lstrip(".,!?;:…").strip()
        if not suffix_remainder:
            return normalized_existing
        return f"{normalized_existing} {suffix_remainder}".strip()
    return None


def _merge_transcript_text_pair(existing_text: str | None, new_text: str | None) -> str | None:
    existing_normalized = _normalize_transcript_text(existing_text)
    new_normalized = _normalize_transcript_text(new_text)
    if not existing_normalized:
        return new_normalized or None
    if not new_normalized:
        return existing_normalized
    overlap_merge = _find_transcript_suffix_prefix_merge(existing_normalized, new_normalized)
    if overlap_merge:
        return overlap_merge

    if _transcript_texts_should_merge(existing_normalized, new_normalized):
        return _choose_preferred_transcript_text(existing_normalized, new_normalized)
    return None


TRANSCRIPT_DUPLICATE_MERGE_WINDOW_SECONDS = 12
TRANSCRIPT_DUPLICATE_OVERLAP_WINDOW_SECONDS = 15


def _merge_transcript_pair_if_candidate(
    existing_text: str | None,
    new_text: str | None,
    delta_seconds: float,
) -> str | None:
    if delta_seconds < 0:
        return None

    overlap_merge = _find_transcript_suffix_prefix_merge(existing_text, new_text)
    if overlap_merge:
        if delta_seconds <= TRANSCRIPT_DUPLICATE_OVERLAP_WINDOW_SECONDS:
            return overlap_merge
        return None

    if delta_seconds > TRANSCRIPT_DUPLICATE_MERGE_WINDOW_SECONDS:
        return None

    return _merge_transcript_text_pair(existing_text, new_text)


def _merge_datetime_min(*values: datetime | None) -> datetime | None:
    filtered = [value for value in values if value is not None]
    return min(filtered) if filtered else None


def _merge_datetime_max(*values: datetime | None) -> datetime | None:
    filtered = [value for value in values if value is not None]
    return max(filtered) if filtered else None


def _merge_float_min(*values: float | None) -> float | None:
    filtered = [value for value in values if value is not None]
    return min(filtered) if filtered else None


def _merge_float_max(*values: float | None) -> float | None:
    filtered = [value for value in values if value is not None]
    return max(filtered) if filtered else None


def _merge_resolution_status(left_status: str, right_status: str) -> str:
    reviewed_statuses = {"accepted", "rejected"}
    if left_status in reviewed_statuses or right_status in reviewed_statuses:
        return "accepted"
    if left_status == "auto_applied" or right_status == "auto_applied":
        return "auto_applied"
    return "original"


def _collect_duplicate_transcript_merge_candidates(transcripts: list[Transcript]) -> tuple[int, set[int]]:
    candidates: set[int] = set()
    ordered = sorted(
        transcripts,
        key=lambda item: (
            _transcript_sort_time(item) or datetime.min,
            item.sequence_no if item.sequence_no is not None else item.id,
            item.id,
        ),
    )
    for previous, current in zip(ordered, ordered[1:]):
        if not _compatible_transcript_speakers(previous.speaker, current.speaker):
            continue
        previous_time = previous.caption_finalized_at or previous.timestamp
        current_time = current.caption_finalized_at or current.timestamp
        delta_seconds = (
            (current_time - previous_time).total_seconds()
            if previous_time is not None and current_time is not None
            else 999.0
        )
        if _merge_transcript_pair_if_candidate(previous.text, current.text, delta_seconds):
            candidates.add(previous.id)
            candidates.add(current.id)
    return max(0, len(candidates) // 2), candidates


def merge_duplicate_transcripts_after_reviews(
    meeting: Meeting,
    transcripts: list[Transcript],
    review_items: list[TranscriptReviewItem],
) -> int:
    if meeting.postprocess_status not in {"review_ready", "completed"}:
        return 0
    if any(review_item.status == "pending" for review_item in review_items):
        return 0
    if len(transcripts) < 2:
        return 0

    review_items_by_transcript_id: dict[int, list[TranscriptReviewItem]] = {}
    for review_item in review_items:
        review_items_by_transcript_id.setdefault(review_item.transcript_id, []).append(review_item)

    merged_count = 0
    index = 1
    while index < len(transcripts):
        previous = transcripts[index - 1]
        current = transcripts[index]
        previous_time = previous.caption_finalized_at or previous.timestamp
        current_time = current.caption_finalized_at or current.timestamp
        delta_seconds = (
            (current_time - previous_time).total_seconds()
            if previous_time is not None and current_time is not None
            else 999.0
        )
        if _compatible_transcript_speakers(previous.speaker, current.speaker):
            merged_text = _merge_transcript_pair_if_candidate(previous.text, current.text, delta_seconds)
            if merged_text:
                merged_teams_text = _merge_transcript_text_pair(previous.teams_text, current.teams_text)
                if not merged_teams_text:
                    merged_teams_text = _choose_preferred_transcript_text(
                        previous.teams_text or previous.text,
                        current.teams_text or current.text,
                    )

                previous.speaker = (
                    current.speaker
                    if _normalize_transcript_text(current.speaker).casefold() != "unknown"
                    else previous.speaker
                )
                previous.text = merged_text
                previous.teams_text = merged_teams_text
                previous.sequence_no = min(
                    value for value in [previous.sequence_no, current.sequence_no] if value is not None
                ) if previous.sequence_no is not None or current.sequence_no is not None else None
                previous.start_sec = _merge_float_min(previous.start_sec, current.start_sec)
                previous.end_sec = _merge_float_max(previous.end_sec, current.end_sec)
                previous.caption_started_at = _merge_datetime_min(
                    previous.caption_started_at,
                    current.caption_started_at,
                    previous.timestamp,
                    current.timestamp,
                )
                previous.caption_finalized_at = _merge_datetime_max(
                    previous.caption_finalized_at,
                    current.caption_finalized_at,
                    previous.timestamp,
                    current.timestamp,
                )
                previous.timestamp = _merge_datetime_min(previous.timestamp, current.timestamp) or previous.timestamp
                previous.resolution_status = _merge_resolution_status(
                    previous.resolution_status,
                    current.resolution_status,
                )
                previous.auto_corrected = previous.resolution_status == "auto_applied"

                current_review_items = review_items_by_transcript_id.pop(current.id, [])
                if current_review_items:
                    previous_review_items = review_items_by_transcript_id.setdefault(previous.id, [])
                    for review_item in current_review_items:
                        review_item.transcript_id = previous.id
                        previous_review_items.append(review_item)

                transcripts.pop(index)
                merged_count += 1
                continue
        index += 1

    for sequence_no, transcript in enumerate(transcripts, start=1):
        transcript.sequence_no = sequence_no

    return merged_count


@dataclass
class LiveTranscriptRow:
    id: int
    speaker: str
    text: str
    timestamp: str
    resolution_status: str = "original"
    auto_corrected: bool = False


def _collapse_live_caption_events(events: list[TeamsCaptionEvent]) -> list[LiveTranscriptRow]:
    rows: list[LiveTranscriptRow] = []
    for event in events:
        normalized_text = _normalize_transcript_text(event.text)
        if not normalized_text:
            continue
        timestamp = event.observed_at.strftime("%H:%M:%S") if event.observed_at else ""
        row = LiveTranscriptRow(
            id=event.id,
            speaker=event.speaker_name,
            text=normalized_text,
            timestamp=timestamp,
        )
        if rows and _compatible_transcript_speakers(rows[-1].speaker, row.speaker):
            if _transcript_texts_should_merge(rows[-1].text, row.text):
                rows[-1].text = _merge_transcript_text_pair(rows[-1].text, row.text) or rows[-1].text
                rows[-1].timestamp = row.timestamp or rows[-1].timestamp
                rows[-1].id = row.id
                continue
        rows.append(row)
    return rows


def speaker_initials(name: str) -> str:
    parts = [p for p in name.split() if p]
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
    label = "WAV oynatma kopyası" if preferred_path.suffix.lower() == ".wav" else f"{audio_asset.format.upper()} master kayıt"
    return SnapshotAudioOut(
        status=meeting.audio_status,
        error=meeting.audio_error,
        has_audio=True,
        audio_url=f"/api/media/meetings/{meeting.id}/audio",
        label=label,
    )


def _build_preview_payload(meeting: Meeting) -> SnapshotPreviewOut:
    if meeting.user_id is None:
        return SnapshotPreviewOut(has_preview=False, image_url=None, label="Canlı görüntü sadece toplantı sırasında görünür")
    path = preview_path(meeting.user_id, meeting.id)
    if path.exists() and meeting.status in {"joining", "active"}:
        version = int(path.stat().st_mtime)
        return SnapshotPreviewOut(
            has_preview=True,
            image_url=f"/api/media/meetings/{meeting.id}/preview?v={version}",
            label="Son canlı kare",
        )
    label = "Bot henüz canlı kare yüklemedi" if meeting.status in {"joining", "active"} else "Canlı görüntü sadece toplantı sırasında görünür"
    return SnapshotPreviewOut(has_preview=False, image_url=None, label=label)


def build_snapshot(
    meeting: Meeting,
    caption_events: list[TeamsCaptionEvent],
    transcripts: list[Transcript],
    review_items: list[TranscriptReviewItem],
    audio_asset: MeetingAudioAsset | None,
) -> MeetingSnapshotOut:
    review_map = {item.transcript_id: item for item in review_items}
    duplicate_count = 0
    duplicate_ids: set[int] = set()

    visible_transcripts = sorted(
        transcripts,
        key=lambda item: (
            _transcript_sort_time(item) or datetime.min,
            item.sequence_no if item.sequence_no is not None else item.id,
            item.id,
        ),
    )
    live_rows = _collapse_live_caption_events(caption_events)
    if meeting.postprocess_status in {POSTPROCESS_STATUS_REVIEW_READY, POSTPROCESS_STATUS_COMPLETED} and not review_items:
        duplicate_count, duplicate_ids = _collect_duplicate_transcript_merge_candidates(visible_transcripts)

    if meeting.status in {"joining", "active"} or not visible_transcripts:
        transcript_rows = [
            TranscriptEntryOut(
                id=row.id,
                speaker=row.speaker,
                text=row.text,
                teams_text=row.text,
                timestamp=row.timestamp,
                initials=speaker_initials(row.speaker),
                color=speaker_color(meeting.id, row.speaker),
                resolution_status=row.resolution_status,
                auto_corrected=row.auto_corrected,
                has_pending_review=False,
                has_duplicate_merge_candidate=False,
                review=None,
            )
            for row in live_rows
        ]
    else:
        transcript_rows = []
        for transcript in visible_transcripts:
            review_item = review_map.get(transcript.id)
            timestamp_value = transcript.caption_finalized_at or transcript.timestamp
            transcript_rows.append(
                TranscriptEntryOut(
                    id=transcript.id,
                    speaker=transcript.speaker,
                    text=transcript.text,
                    teams_text=transcript.teams_text or transcript.text,
                    timestamp=timestamp_value.strftime("%H:%M:%S") if timestamp_value else "",
                    initials=speaker_initials(transcript.speaker),
                    color=speaker_color(meeting.id, transcript.speaker),
                    resolution_status=transcript.resolution_status,
                    auto_corrected=transcript.auto_corrected,
                    has_pending_review=review_item is not None,
                    has_duplicate_merge_candidate=transcript.id in duplicate_ids,
                    review=(
                        ReviewOut(
                            id=review_item.id,
                            granularity=review_item.granularity,
                            confidence_label=f"%{int(round(review_item.confidence * 100))}",
                            current_text=review_item.current_text,
                            suggested_text=review_item.suggested_text,
                            audio_clip_url=review_audio_url(review_item),
                            has_audio_clip=bool(review_item.audio_clip_path),
                        )
                        if review_item
                        else None
                    ),
                )
            )

    pending_review_count = sum(1 for row in transcript_rows if row.has_pending_review)
    return MeetingSnapshotOut(
        meeting=SnapshotMeetingOut(id=meeting.id, title=meeting.title, status=meeting.status),
        summary=SnapshotSummaryOut(
            speaker_count=len({row.speaker for row in transcript_rows}),
            transcript_count=len(transcript_rows),
        ),
        audio=_build_audio_payload(meeting, audio_asset),
        postprocess=SnapshotPostprocessOut(
            status=meeting.postprocess_status,
            error=meeting.postprocess_error,
            progress_pct=meeting.postprocess_progress_pct,
            progress_note=meeting.postprocess_progress_note,
        ),
        preview=_build_preview_payload(meeting),
        transcripts=transcript_rows,
        actions=SnapshotActionsOut(
            pending_review_count=pending_review_count,
            duplicate_merge_candidate_count=duplicate_count,
            can_apply_all_reviews=pending_review_count > 0,
            can_merge_duplicate_transcripts=(
                meeting.postprocess_status in {POSTPROCESS_STATUS_REVIEW_READY, POSTPROCESS_STATUS_COMPLETED}
                and pending_review_count == 0
                and duplicate_count > 0
            ),
            can_stop_meeting=meeting.status in {"joining", "active"},
        ),
    )
