import importlib.util
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.app.meeting_runtime import (  # noqa: E402
    AUDIO_STATUS_READY,
    POSTPROCESS_STATUS_ALIGNING,
    POSTPROCESS_STATUS_CANONICALIZING,
    POSTPROCESS_STATUS_COMPLETED,
    POSTPROCESS_STATUS_FAILED,
    POSTPROCESS_STATUS_QUEUED,
    POSTPROCESS_STATUS_REBUILDING,
    POSTPROCESS_STATUS_REVIEW_READY,
    POSTPROCESS_STATUS_TRANSCRIBING,
    REVIEW_STATUS_PENDING,
    TRANSCRIPT_STATUS_AUTO_APPLIED,
    TRANSCRIPT_STATUS_ORIGINAL,
    TRANSCRIPT_STATUS_PENDING_REVIEW,
    ensure_runtime_schema,
    get_alignment_map_path,
    get_db_path,
    get_meeting_pcm_audio_path,
    get_review_clip_filename,
    get_review_clip_path,
    get_teams_canonical_path,
    get_whisperx_result_path,
    remove_review_clips_for_meeting,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("PostprocessWorker")

ASR_MODEL_DEFAULT = "large-v3"
TURKISH_ALIGNMENT_MODEL = "cahya/wav2vec2-base-turkish"
POSTPROCESS_VERSION = "whisperx_global_v1"
TOKEN_RE = re.compile(r"[\wçğıöşüÇĞİÖŞÜ@.\-/']+")
DEFAULT_SPOKEN_LANGUAGE = os.getenv("WHISPERX_LANGUAGE", "tr").strip().lower() or "tr"
FORCE_SPOKEN_LANGUAGE = os.getenv("WHISPERX_FORCE_LANGUAGE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}


def runtime_cache_dir(name: str) -> Path:
    path = REPO_ROOT / "bot" / "runtime_cache" / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def configure_runtime_environment():
    os.environ.setdefault("MPLCONFIGDIR", str(runtime_cache_dir("matplotlib")))
    os.environ.setdefault("HF_HOME", str(runtime_cache_dir("huggingface")))
    os.environ.setdefault("XDG_CACHE_HOME", str(runtime_cache_dir("xdg")))


def huggingface_model_cached(repo_id: str) -> bool:
    model_dir = runtime_cache_dir("huggingface") / "hub" / f"models--{repo_id.replace('/', '--')}"
    snapshots_dir = model_dir / "snapshots"
    return snapshots_dir.exists() and any(snapshots_dir.iterdir())


def dependency_error_message(require_whisperx: bool) -> str | None:
    if require_whisperx and importlib.util.find_spec("whisperx") is None:
        return (
            "WhisperX kurulu değil. "
            "`conda activate teams-bot && python -m pip install -r app/requirements.txt` "
            "veya `python -m pip install whisperx` çalıştırın."
        )
    if require_whisperx and shutil.which("ffmpeg") is None:
        return "ffmpeg bulunamadı. Sistem PATH içinde ffmpeg kurulu olmalı."
    return None


def db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def update_meeting_postprocess_status(meeting_id: int, status: str, error: str | None = None):
    conn = db_connection()
    try:
        conn.execute(
            """
            UPDATE meeting
            SET postprocess_status = ?, postprocess_error = ?
            WHERE id = ?
            """,
            (status, error, meeting_id),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_meeting(meeting_id: int) -> sqlite3.Row | None:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, status, audio_status, joined_at, ended_at
            FROM meeting
            WHERE id = ?
            """,
            (meeting_id,),
        ).fetchone()
    finally:
        conn.close()


def fetch_meeting_audio_asset(meeting_id: int) -> sqlite3.Row | None:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, meeting_id, master_audio_path, pcm_audio_path, format, duration_ms, status, postprocess_version
            FROM meetingaudioasset
            WHERE meeting_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (meeting_id,),
        ).fetchone()
    finally:
        conn.close()


def update_audio_asset_paths(
    asset_id: int,
    pcm_audio_path: str | None = None,
    postprocess_version: str | None = None,
):
    conn = db_connection()
    try:
        conn.execute(
            """
            UPDATE meetingaudioasset
            SET pcm_audio_path = COALESCE(?, pcm_audio_path),
                postprocess_version = COALESCE(?, postprocess_version)
            WHERE id = ?
            """,
            (pcm_audio_path, postprocess_version, asset_id),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_caption_events(meeting_id: int) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, meeting_id, sequence_no, speaker_name, text, observed_at, slot_index, revision_no
            FROM teamscaptionevent
            WHERE meeting_id = ?
            ORDER BY COALESCE(sequence_no, id), observed_at, id
            """,
            (meeting_id,),
        ).fetchall()
    finally:
        conn.close()


def fetch_legacy_transcripts(meeting_id: int) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, meeting_id, sequence_no, speaker, teams_text, text, timestamp,
                   caption_started_at, caption_finalized_at
            FROM transcript
            WHERE meeting_id = ?
            ORDER BY COALESCE(sequence_no, id), timestamp, id
            """,
            (meeting_id,),
        ).fetchall()
    finally:
        conn.close()


def clear_previous_outputs(meeting_id: int):
    conn = db_connection()
    try:
        transcript_ids = [
            row[0]
            for row in conn.execute(
                "SELECT id FROM transcript WHERE meeting_id = ?",
                (meeting_id,),
            ).fetchall()
        ]
        if transcript_ids:
            placeholders = ",".join("?" for _ in transcript_ids)
            conn.execute(
                f"DELETE FROM transcriptreviewitem WHERE transcript_id IN ({placeholders})",
                transcript_ids,
            )
        conn.execute("DELETE FROM transcript WHERE meeting_id = ?", (meeting_id,))
        conn.commit()
    finally:
        conn.close()
    remove_review_clips_for_meeting(meeting_id)


def convert_audio_to_pcm(master_audio_path: Path, meeting_id: int) -> Path:
    output_path = get_meeting_pcm_audio_path(meeting_id)
    command = [
        "ffmpeg",
        "-y",
        "-i",
        str(master_audio_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        str(output_path),
    ]
    logger.info("Converting %s to %s", master_audio_path, output_path)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ffmpeg conversion failed")
    return output_path


def persist_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def load_whisperx_result(audio_path: Path) -> dict:
    configure_runtime_environment()
    try:
        import whisperx
    except Exception as exc:
        raise RuntimeError(
            "WhisperX import edilemedi. "
            "teams-bot env içinde `python -m pip install -r app/requirements.txt` çalıştırın. "
            f"Ayrıntı: {exc}"
        ) from exc

    model_name = os.getenv("WHISPERX_MODEL_PATH") or os.getenv("WHISPERX_MODEL", ASR_MODEL_DEFAULT)
    compute_type = os.getenv("WHISPERX_COMPUTE_TYPE", "int8")
    batch_size = int(os.getenv("WHISPERX_BATCH_SIZE", "8"))
    requested_language = DEFAULT_SPOKEN_LANGUAGE if FORCE_SPOKEN_LANGUAGE else None
    whisper_local_files_only = False
    if not os.getenv("WHISPERX_MODEL_PATH") and model_name == ASR_MODEL_DEFAULT:
        whisper_local_files_only = huggingface_model_cached("Systran/faster-whisper-large-v3")

    logger.info(
        "Loading WhisperX model=%s compute_type=%s language=%s force_language=%s cache_only=%s",
        model_name,
        compute_type,
        requested_language or "auto",
        FORCE_SPOKEN_LANGUAGE,
        whisper_local_files_only,
    )
    try:
        model = whisperx.load_model(
            model_name,
            device="cpu",
            compute_type=compute_type,
            language=requested_language,
            local_files_only=whisper_local_files_only,
        )
    except Exception as exc:
        error_text = str(exc)
        if (
            "LocalEntryNotFoundError" in exc.__class__.__name__
            or "snapshot folder" in error_text
            or "trying to locate the files on the Hub" in error_text
            or "huggingface.co" in error_text
            or "NameResolutionError" in error_text
        ):
            raise RuntimeError(
                "WhisperX modeli yerel cache'te bulunamadı. "
                "Ilk calistirmada internet baglantisi gerekir. "
                "Alternatif olarak modeli once indirip `WHISPERX_MODEL_PATH` ile local path verin."
            ) from exc
        raise

    result = model.transcribe(
        str(audio_path),
        batch_size=batch_size,
        language=requested_language,
    )
    language_code = (result.get("language") or requested_language or "").lower()
    if requested_language:
        result["language"] = language_code
    if not language_code:
        return result

    align_model_name = TURKISH_ALIGNMENT_MODEL if language_code.startswith("tr") else None
    align_cache_only = bool(align_model_name and huggingface_model_cached(align_model_name))
    try:
        align_model, metadata = whisperx.load_align_model(
            language_code=language_code,
            device="cpu",
            model_name=align_model_name,
            model_cache_only=align_cache_only,
        )
        aligned_result = whisperx.align(
            result["segments"],
            align_model,
            metadata,
            str(audio_path),
            device="cpu",
        )
        aligned_result["language"] = language_code
        aligned_result["_alignment_model"] = align_model_name or metadata.get("language")
        result = aligned_result
    except Exception as exc:
        logger.warning("WhisperX alignment failed, continuing with coarse segments: %s", exc)
    return result


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def tokenize_text(value: str | None) -> list[dict]:
    normalized = normalize_text(value)
    tokens = []
    for match in TOKEN_RE.finditer(normalized):
        token_text = match.group(0)
        normalized_token = token_text.casefold().strip(".,!?;:…\"'`()[]{}")
        if not normalized_token:
            continue
        tokens.append(
            {
                "text": token_text,
                "norm": normalized_token,
                "start": match.start(),
                "end": match.end(),
            }
        )
    return tokens


def token_match(left_token: str, right_token: str) -> bool:
    if left_token == right_token:
        return True
    shorter_length = min(len(left_token), len(right_token))
    if shorter_length >= 2 and (left_token.startswith(right_token) or right_token.startswith(left_token)):
        return True
    if shorter_length >= 4 and SequenceMatcher(None, left_token, right_token).ratio() >= 0.82:
        return True
    return False


def caption_tokens(value: str | None) -> list[str]:
    return [token["norm"] for token in tokenize_text(value)]


def text_ends_cleanly(text: str) -> bool:
    return normalize_text(text).endswith((".", "!", "?", "…"))


def revision_text_score(text: str) -> int:
    normalized = normalize_text(text)
    if not normalized:
        return -9999
    score = len(normalized)
    if text_ends_cleanly(normalized):
        score += 24
    score += normalized.count(".") * 4
    score += normalized.count("?") * 4
    score += normalized.count("!") * 4
    tokens = caption_tokens(normalized)
    if tokens and len(tokens[-1]) <= 2:
        score -= 8
    return score


def compatible_speakers(existing_speaker: str | None, new_speaker: str | None) -> bool:
    existing_value = normalize_text(existing_speaker).casefold()
    new_value = normalize_text(new_speaker).casefold()
    if not existing_value or existing_value == "unknown":
        return True
    if not new_value or new_value == "unknown":
        return True
    return existing_value == new_value


def common_prefix_token_count(left_tokens: list[str], right_tokens: list[str]) -> int:
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if left_token != right_token:
            break
        count += 1
    return count


def fuzzy_common_prefix_token_count(left_tokens: list[str], right_tokens: list[str]) -> int:
    count = 0
    for left_token, right_token in zip(left_tokens, right_tokens):
        if not token_match(left_token, right_token):
            break
        count += 1
    return count


def token_sequence_match_ratio(left_text: str | None, right_text: str | None) -> float:
    left_tokens = caption_tokens(left_text)
    right_tokens = caption_tokens(right_text)
    if not left_tokens or not right_tokens:
        return 0.0
    matcher = SequenceMatcher(None, left_tokens, right_tokens, autojunk=False)
    matched_tokens = sum(block.size for block in matcher.get_matching_blocks())
    shorter_length = min(len(left_tokens), len(right_tokens))
    if shorter_length <= 0:
        return 0.0
    return matched_tokens / shorter_length


def same_slot_rephrase(existing_text: str | None, new_text: str | None) -> bool:
    left_tokens = caption_tokens(existing_text)
    right_tokens = caption_tokens(new_text)
    shorter_length = min(len(left_tokens), len(right_tokens))
    if shorter_length < 6:
        return False
    overlap_ratio = token_sequence_match_ratio(existing_text, new_text)
    if overlap_ratio >= 0.78:
        return True
    shared_prefix = fuzzy_common_prefix_token_count(left_tokens, right_tokens)
    return shared_prefix >= 5 and overlap_ratio >= 0.62


def choose_preferred_caption_text(existing_text: str | None, new_text: str | None) -> str:
    normalized_existing = normalize_text(existing_text)
    normalized_new = normalize_text(new_text)
    if not normalized_existing:
        return normalized_new
    if not normalized_new:
        return normalized_existing

    existing_tokens = caption_tokens(normalized_existing)
    new_tokens = caption_tokens(normalized_new)
    shared_prefix = common_prefix_token_count(existing_tokens, new_tokens)
    shorter_length = min(len(existing_tokens), len(new_tokens))

    if shorter_length and shared_prefix >= min(4, shorter_length) and shared_prefix / max(shorter_length, 1) >= 0.75:
        if len(new_tokens) > len(existing_tokens):
            return normalized_new
        if len(new_tokens) < len(existing_tokens):
            if revision_text_score(normalized_new) >= revision_text_score(normalized_existing):
                return normalized_new
            return normalized_existing

    if revision_text_score(normalized_new) > revision_text_score(normalized_existing):
        return normalized_new
    return normalized_existing


def texts_should_merge(existing_text: str | None, new_text: str | None) -> bool:
    old_text = normalize_text(existing_text)
    new_value = normalize_text(new_text)
    if not old_text or not new_value:
        return False
    old_fold = old_text.casefold()
    new_fold = new_value.casefold()
    if old_fold == new_fold:
        return True
    punctuation = ".,!?;:…"
    if new_fold.rstrip(punctuation) == old_fold.rstrip(punctuation):
        return True
    old_tokens = caption_tokens(old_text)
    new_tokens = caption_tokens(new_value)
    if old_tokens and new_tokens:
        shared_prefix = fuzzy_common_prefix_token_count(old_tokens, new_tokens)
        shorter_length = min(len(old_tokens), len(new_tokens))
        if shared_prefix >= min(3, shorter_length) and shared_prefix / max(shorter_length, 1) >= 0.6:
            return True
    ratio = SequenceMatcher(None, old_fold, new_fold).ratio()
    return ratio >= 0.82 and abs(len(new_fold) - len(old_fold)) <= 96


def find_suffix_prefix_overlap(existing_text: str | None, new_text: str | None, min_tokens: int = 4):
    existing_tokens = tokenize_text(existing_text)
    new_tokens = tokenize_text(new_text)
    if len(existing_tokens) < min_tokens or len(new_tokens) < min_tokens:
        return None

    normalized_existing = normalize_text(existing_text)
    normalized_new = normalize_text(new_text)
    max_overlap = min(len(existing_tokens), len(new_tokens))
    for overlap_size in range(max_overlap, min_tokens - 1, -1):
        existing_suffix = existing_tokens[-overlap_size:]
        new_prefix = new_tokens[:overlap_size]
        match_count = 0
        exact_count = 0
        for existing_token, new_token in zip(existing_suffix, new_prefix):
            if not token_match(existing_token["norm"], new_token["norm"]):
                break
            match_count += 1
            if existing_token["norm"] == new_token["norm"]:
                exact_count += 1
        if match_count != overlap_size or exact_count < max(1, overlap_size - 1):
            continue
        existing_prefix = normalized_existing[: existing_suffix[0]["start"]].strip()
        if not existing_prefix:
            continue
        return {"prefix_text": existing_prefix, "overlap_tokens": overlap_size}
    return None


def sequence_ratio(current_text: str, suggested_text: str) -> float:
    return SequenceMatcher(None, normalize_text(current_text).casefold(), normalize_text(suggested_text).casefold()).ratio()


def texts_equivalent_for_review(current_text: str, suggested_text: str) -> bool:
    if normalize_text(current_text).casefold() == normalize_text(suggested_text).casefold():
        return True
    return caption_tokens(current_text) == caption_tokens(suggested_text)


def token_edit_distance(left: list[str], right: list[str]) -> int:
    if not left:
        return len(right)
    if not right:
        return len(left)
    matrix = [[0] * (len(right) + 1) for _ in range(len(left) + 1)]
    for i in range(len(left) + 1):
        matrix[i][0] = i
    for j in range(len(right) + 1):
        matrix[0][j] = j
    for i, left_token in enumerate(left, start=1):
        for j, right_token in enumerate(right, start=1):
            cost = 0 if left_token == right_token else 1
            matrix[i][j] = min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost,
            )
    return matrix[-1][-1]


def sensitive_token_change(current_text: str, suggested_text: str) -> bool:
    email_or_url = re.compile(r"(@|https?://|www\.)", re.IGNORECASE)
    digits = re.compile(r"\d")
    if digits.search(current_text) or digits.search(suggested_text):
        if normalize_text(current_text) != normalize_text(suggested_text):
            return True
    if email_or_url.search(current_text) or email_or_url.search(suggested_text):
        if normalize_text(current_text) != normalize_text(suggested_text):
            return True
    name_token = re.compile(r"\b[A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü'-]{1,}\b")
    current_names = set(name_token.findall(current_text)[1:])
    suggested_names = set(name_token.findall(suggested_text)[1:])
    return current_names != suggested_names


def build_events_from_legacy_transcripts(rows: list[sqlite3.Row]) -> list[dict]:
    events: list[dict] = []
    for index, row in enumerate(rows, start=1):
        events.append(
            {
                "sequence_no": row["sequence_no"] or index,
                "speaker_name": row["speaker"],
                "text": row["teams_text"] or row["text"] or "",
                "observed_at": row["caption_finalized_at"] or row["timestamp"],
                "slot_index": None,
                "revision_no": 0,
            }
        )
    return events


def canonicalize_caption_events(rows: list[sqlite3.Row] | list[dict]) -> list[dict]:
    canonical: list[dict] = []
    slot_anchor_index: dict[int, int] = {}

    for raw_row in rows:
        row = dict(raw_row)
        speaker = normalize_text(row.get("speaker_name") or row.get("speaker") or "Unknown") or "Unknown"
        text = normalize_text(row.get("text") or row.get("teams_text") or "")
        if len(text) < 2:
            continue
        if text in {"Captions are turned on.", "Captions are turned off."}:
            continue

        observed_at = parse_dt(row.get("observed_at") or row.get("caption_finalized_at") or row.get("timestamp"))
        slot_index = row.get("slot_index")
        revision_no = int(row.get("revision_no") or 0)
        if observed_at is None:
            observed_at = datetime.utcnow()

        candidate_index = slot_anchor_index.get(slot_index) if slot_index is not None else None
        candidate = canonical[candidate_index] if candidate_index is not None and 0 <= candidate_index < len(canonical) else (canonical[-1] if canonical else None)

        if candidate:
            candidate_time = candidate.get("finalized_at") or candidate.get("started_at")
            delta_seconds = abs((observed_at - candidate_time).total_seconds()) if candidate_time else 999.0
            if (
                delta_seconds <= 20
                and compatible_speakers(candidate.get("speaker"), speaker)
                and normalize_text(candidate.get("text")).casefold() == text.casefold()
            ):
                candidate["finalized_at"] = observed_at
                continue

            overlap = find_suffix_prefix_overlap(candidate.get("text"), text)
            if delta_seconds <= 8 and overlap and compatible_speakers(candidate.get("speaker"), speaker):
                candidate["text"] = overlap["prefix_text"]

            if (
                slot_index is not None
                and slot_index == candidate.get("slot_index")
                and delta_seconds <= 4
                and compatible_speakers(candidate.get("speaker"), speaker)
                and same_slot_rephrase(candidate.get("text"), text)
            ):
                candidate["speaker"] = speaker if normalize_text(speaker).casefold() != "unknown" else candidate["speaker"]
                candidate["text"] = choose_preferred_caption_text(candidate.get("text"), text)
                candidate["finalized_at"] = observed_at
                candidate["revision_no"] = max(int(candidate.get("revision_no") or 0), revision_no)
                continue

            if (
                delta_seconds <= 8
                and compatible_speakers(candidate.get("speaker"), speaker)
                and texts_should_merge(candidate.get("text"), text)
            ):
                candidate["speaker"] = speaker if normalize_text(speaker).casefold() != "unknown" else candidate["speaker"]
                candidate["text"] = choose_preferred_caption_text(candidate.get("text"), text)
                candidate["finalized_at"] = observed_at
                candidate["revision_no"] = max(int(candidate.get("revision_no") or 0), revision_no)
                continue

        canonical.append(
            {
                "speaker": speaker,
                "text": text,
                "started_at": observed_at,
                "finalized_at": observed_at,
                "sequence_no": row.get("sequence_no") or len(canonical) + 1,
                "slot_index": slot_index,
                "revision_no": revision_no,
            }
        )
        if slot_index is not None:
            slot_anchor_index[slot_index] = len(canonical) - 1

    cleaned: list[dict] = []
    for entry in canonical:
        if not cleaned:
            cleaned.append(entry)
            continue
        previous = cleaned[-1]
        previous_time = previous.get("finalized_at") or previous.get("started_at")
        current_time = entry.get("started_at") or entry.get("finalized_at")
        delta_seconds = abs((current_time - previous_time).total_seconds()) if previous_time and current_time else 999.0

        overlap = find_suffix_prefix_overlap(previous.get("text"), entry.get("text"))
        if delta_seconds <= 8 and overlap and compatible_speakers(previous.get("speaker"), entry.get("speaker")):
            previous["text"] = overlap["prefix_text"]

        if (
            previous.get("slot_index") is not None
            and previous.get("slot_index") == entry.get("slot_index")
            and delta_seconds <= 4
            and compatible_speakers(previous.get("speaker"), entry.get("speaker"))
            and same_slot_rephrase(previous.get("text"), entry.get("text"))
        ):
            previous["speaker"] = (
                entry["speaker"]
                if normalize_text(entry["speaker"]).casefold() != "unknown"
                else previous["speaker"]
            )
            previous["text"] = choose_preferred_caption_text(previous.get("text"), entry.get("text"))
            previous["finalized_at"] = entry.get("finalized_at") or previous.get("finalized_at")
            previous["revision_no"] = max(int(previous.get("revision_no") or 0), int(entry.get("revision_no") or 0))
            continue

        if (
            delta_seconds <= 8
            and compatible_speakers(previous.get("speaker"), entry.get("speaker"))
            and texts_should_merge(previous.get("text"), entry.get("text"))
        ):
            previous["speaker"] = (
                entry["speaker"]
                if normalize_text(entry["speaker"]).casefold() != "unknown"
                else previous["speaker"]
            )
            previous["text"] = choose_preferred_caption_text(previous.get("text"), entry.get("text"))
            previous["finalized_at"] = entry.get("finalized_at") or previous.get("finalized_at")
            previous["revision_no"] = max(int(previous.get("revision_no") or 0), int(entry.get("revision_no") or 0))
            continue

        cleaned.append(entry)

    final_rows = []
    for entry in cleaned:
        if len(caption_tokens(entry["text"])) < 2 and not text_ends_cleanly(entry["text"]):
            continue
        final_rows.append(entry)
    return final_rows


def build_teams_tokens(canonical_rows: list[dict]) -> list[dict]:
    tokens: list[dict] = []
    for utterance_index, row in enumerate(canonical_rows):
        row_tokens = tokenize_text(row["text"])
        row["token_start_idx"] = len(tokens)
        for token_index, token in enumerate(row_tokens):
            token_data = dict(token)
            token_data["utterance_index"] = utterance_index
            token_data["token_index"] = token_index
            tokens.append(token_data)
        row["token_end_idx"] = len(tokens)
        row["token_count"] = row["token_end_idx"] - row["token_start_idx"]
    return tokens


def build_whisper_tokens(result: dict) -> tuple[list[dict], list[dict]]:
    tokens: list[dict] = []
    segments: list[dict] = []
    for segment_index, segment in enumerate(result.get("segments", [])):
        segment_start_idx = len(tokens)
        words = segment.get("words") or []
        for word_index, word in enumerate(words):
            start = word.get("start")
            end = word.get("end")
            score = word.get("score")
            word_text = normalize_text(word.get("word") or "")
            if start is None or end is None or not word_text:
                continue
            for token in tokenize_text(word_text):
                token_data = dict(token)
                token_data.update(
                    {
                        "start_sec": float(start),
                        "end_sec": float(end),
                        "score": float(score) if score is not None else None,
                        "segment_index": segment_index,
                        "word_index": word_index,
                    }
                )
                tokens.append(token_data)
        segment_end_idx = len(tokens)
        if segment_end_idx <= segment_start_idx:
            continue
        segments.append(
            {
                "segment_index": segment_index,
                "text": normalize_text(segment.get("text") or ""),
                "start_sec": float(segment.get("start") or 0.0),
                "end_sec": float(segment.get("end") or 0.0),
                "token_start_idx": segment_start_idx,
                "token_end_idx": segment_end_idx,
            }
        )
    return tokens, segments


def local_align_gap(teams_tokens: list[dict], whisper_tokens: list[dict]) -> list[tuple[int, int]]:
    if not teams_tokens or not whisper_tokens:
        return []
    if len(teams_tokens) * len(whisper_tokens) > 50000:
        return []

    n = len(teams_tokens)
    m = len(whisper_tokens)
    dp = [[0.0] * (m + 1) for _ in range(n + 1)]
    action = [[None] * (m + 1) for _ in range(n + 1)]

    for i in range(1, n + 1):
        dp[i][0] = float(i)
        action[i][0] = "del"
    for j in range(1, m + 1):
        dp[0][j] = float(j)
        action[0][j] = "ins"

    for i in range(1, n + 1):
        left_norm = teams_tokens[i - 1]["norm"]
        for j in range(1, m + 1):
            right_norm = whisper_tokens[j - 1]["norm"]
            subst_cost = 0.0 if left_norm == right_norm else (0.25 if token_match(left_norm, right_norm) else 1.0)
            choices = (
                (dp[i - 1][j] + 1.0, "del"),
                (dp[i][j - 1] + 1.0, "ins"),
                (dp[i - 1][j - 1] + subst_cost, "diag"),
            )
            best_cost, best_action = min(choices, key=lambda item: item[0])
            dp[i][j] = best_cost
            action[i][j] = best_action

    pairs: list[tuple[int, int]] = []
    i = n
    j = m
    while i > 0 or j > 0:
        current_action = action[i][j]
        if current_action == "diag":
            left_norm = teams_tokens[i - 1]["norm"]
            right_norm = whisper_tokens[j - 1]["norm"]
            if token_match(left_norm, right_norm):
                pairs.append((teams_tokens[i - 1]["global_index"], whisper_tokens[j - 1]["global_index"]))
            i -= 1
            j -= 1
        elif current_action == "del":
            i -= 1
        else:
            j -= 1
    pairs.reverse()
    return pairs


def align_token_streams(teams_tokens: list[dict], whisper_tokens: list[dict]) -> tuple[dict[int, int], dict[int, int], list[dict]]:
    teams_norms = [token["norm"] for token in teams_tokens]
    whisper_norms = [token["norm"] for token in whisper_tokens]

    matcher = SequenceMatcher(None, teams_norms, whisper_norms, autojunk=False)
    blocks = [block for block in matcher.get_matching_blocks() if block.size > 0]

    teams_to_whisper: dict[int, int] = {}
    whisper_to_teams: dict[int, int] = {}
    anchors: list[dict] = []

    previous_team = 0
    previous_whisper = 0
    for block in blocks:
        gap_pairs = local_align_gap(
            teams_tokens[previous_team:block.a],
            whisper_tokens[previous_whisper:block.b],
        )
        for team_index, whisper_index in gap_pairs:
            teams_to_whisper[team_index] = whisper_index
            whisper_to_teams[whisper_index] = team_index

        if block.size >= 2:
            anchors.append(
                {
                    "teams_start": block.a,
                    "whisper_start": block.b,
                    "size": block.size,
                }
            )
        for offset in range(block.size):
            team_index = teams_tokens[block.a + offset]["global_index"]
            whisper_index = whisper_tokens[block.b + offset]["global_index"]
            teams_to_whisper[team_index] = whisper_index
            whisper_to_teams[whisper_index] = team_index
        previous_team = block.a + block.size
        previous_whisper = block.b + block.size

    tail_pairs = local_align_gap(
        teams_tokens[previous_team:],
        whisper_tokens[previous_whisper:],
    )
    for team_index, whisper_index in tail_pairs:
        teams_to_whisper[team_index] = whisper_index
        whisper_to_teams[whisper_index] = team_index

    return teams_to_whisper, whisper_to_teams, anchors


def join_team_tokens(teams_tokens: list[dict], token_indices: list[int]) -> str:
    return " ".join(teams_tokens[index]["text"] for index in token_indices).strip()


def join_whisper_tokens(whisper_tokens: list[dict], token_indices: list[int]) -> str:
    return " ".join(whisper_tokens[index]["text"] for index in token_indices).strip()


def build_final_rows(
    canonical_rows: list[dict],
    teams_tokens: list[dict],
    whisper_tokens: list[dict],
    whisper_segments: list[dict],
    teams_to_whisper: dict[int, int],
    whisper_to_teams: dict[int, int],
) -> tuple[list[dict], list[dict], list[dict]]:
    rows: list[dict] = []
    alignment_debug: list[dict] = []
    whisper_segments_by_index = {
        int(segment["segment_index"]): segment
        for segment in whisper_segments
        if segment.get("segment_index") is not None
    }
    for utterance_index, utterance in enumerate(canonical_rows):
        team_token_indices = list(
            range(
                int(utterance.get("token_start_idx") or 0),
                int(utterance.get("token_end_idx") or 0),
            )
        )
        mapped_team_token_indices = [
            team_index for team_index in team_token_indices if team_index in teams_to_whisper
        ]
        mapped_whisper_token_indices = sorted(
            {
                teams_to_whisper[team_index]
                for team_index in mapped_team_token_indices
            }
        )

        whisper_token_indices: list[int] = []
        if mapped_whisper_token_indices:
            whisper_token_indices = list(
                range(mapped_whisper_token_indices[0], mapped_whisper_token_indices[-1] + 1)
            )

        coverage = len(mapped_team_token_indices) / max(len(team_token_indices), 1)

        start_sec = None
        end_sec = None
        avg_score = None
        whisper_text = ""
        whisper_segment_count = 0
        if whisper_token_indices:
            whisper_segment_indices = sorted(
                {
                    whisper_tokens[index].get("segment_index")
                    for index in whisper_token_indices
                    if whisper_tokens[index].get("segment_index") is not None
                }
            )
            whisper_segment_count = len(whisper_segment_indices)
            if whisper_segment_indices:
                segment_rows = [
                    whisper_segments_by_index[index]
                    for index in whisper_segment_indices
                    if index in whisper_segments_by_index
                ]
                if segment_rows:
                    start_sec = float(segment_rows[0].get("start_sec") or 0.0)
                    end_sec = float(segment_rows[-1].get("end_sec") or 0.0)
                    whisper_text = " ".join(
                        normalize_text(segment.get("text") or "")
                        for segment in segment_rows
                        if normalize_text(segment.get("text") or "")
                    ).strip()
            if not whisper_text:
                start_sec = whisper_tokens[whisper_token_indices[0]].get("start_sec")
                end_sec = whisper_tokens[whisper_token_indices[-1]].get("end_sec")
                whisper_text = join_whisper_tokens(whisper_tokens, whisper_token_indices)
            scores = [
                whisper_tokens[index]["score"]
                for index in whisper_token_indices
                if whisper_tokens[index].get("score") is not None
            ]
            avg_score = sum(scores) / len(scores) if scores else 0.8

        teams_text = utterance["text"]
        rows.append(
            {
                "speaker": utterance["speaker"],
                "teams_text": teams_text,
                "whisper_text": whisper_text,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "caption_started_at": utterance.get("started_at"),
                "caption_finalized_at": utterance.get("finalized_at"),
                "coverage": coverage,
                "avg_confidence": avg_score,
                "whisper_segment_count": whisper_segment_count,
            }
        )
        alignment_debug.append(
            {
                "utterance_index": utterance_index,
                "speaker": utterance["speaker"],
                "teams_text": teams_text,
                "whisper_text": whisper_text,
                "coverage": coverage,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "whisper_segment_count": whisper_segment_count,
            }
        )

    rows.sort(
        key=lambda row: (
            row["caption_started_at"] or row["caption_finalized_at"] or datetime.utcnow(),
            row["start_sec"] if row["start_sec"] is not None else float("inf"),
        )
    )
    return rows, alignment_debug, canonical_rows


def create_review_item(
    conn: sqlite3.Connection,
    transcript_id: int,
    granularity: str,
    current_text: str,
    suggested_text: str,
    confidence: float,
    clip_start_ms: int,
    clip_end_ms: int,
) -> int:
    created_at = datetime.utcnow().isoformat()
    cursor = conn.execute(
        """
        INSERT INTO transcriptreviewitem (
            transcript_id,
            granularity,
            current_text,
            suggested_text,
            confidence,
            status,
            clip_start_ms,
            clip_end_ms,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            transcript_id,
            granularity,
            current_text,
            suggested_text,
            confidence,
            REVIEW_STATUS_PENDING,
            clip_start_ms,
            clip_end_ms,
            created_at,
        ),
    )
    return int(cursor.lastrowid)


def update_review_item_clip_path(conn: sqlite3.Connection, review_item_id: int, clip_filename: str | None):
    conn.execute(
        "UPDATE transcriptreviewitem SET audio_clip_path = ? WHERE id = ?",
        (clip_filename, review_item_id),
    )


def create_audio_clip(
    source_audio_path: Path,
    meeting_id: int,
    transcript_id: int,
    review_item_id: int,
    clip_start_sec: float,
    clip_end_sec: float,
) -> str | None:
    output_path = get_review_clip_path(meeting_id, transcript_id, review_item_id)
    duration = max(clip_end_sec - clip_start_sec, 1.0)
    command = [
        "ffmpeg",
        "-y",
        "-ss",
        f"{max(clip_start_sec, 0.0):.3f}",
        "-i",
        str(source_audio_path),
        "-t",
        f"{duration:.3f}",
        "-ac",
        "1",
        "-ar",
        "16000",
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning("Failed to create review clip: %s", result.stderr.strip())
        return None
    return get_review_clip_filename(meeting_id, transcript_id, review_item_id)


def persist_final_outputs(
    meeting_id: int,
    rows: list[dict],
    source_audio_path: Path | None,
) -> int:
    conn = db_connection()
    pending_reviews = 0
    try:
        for sequence_no, row in enumerate(rows, start=1):
            teams_text = normalize_text(row.get("teams_text") or "")
            whisper_text = normalize_text(row.get("whisper_text") or "")
            coverage = float(row.get("coverage") or 0.0)
            avg_confidence = row.get("avg_confidence")
            avg_confidence = float(avg_confidence) if avg_confidence is not None else 0.0
            whisper_segment_count = int(row.get("whisper_segment_count") or 0)

            final_text = teams_text
            resolution_status = TRANSCRIPT_STATUS_ORIGINAL
            auto_corrected = 0
            should_create_review = False

            if whisper_text and not texts_equivalent_for_review(teams_text, whisper_text):
                similarity = sequence_ratio(teams_text, whisper_text)
                token_distance = token_edit_distance(caption_tokens(teams_text), caption_tokens(whisper_text))
                sensitive_change = sensitive_token_change(teams_text, whisper_text)

                if (
                    coverage >= 0.90
                    and similarity >= 0.92
                    and avg_confidence >= 0.75
                    and not sensitive_change
                ):
                    final_text = whisper_text
                    resolution_status = TRANSCRIPT_STATUS_AUTO_APPLIED
                    auto_corrected = 1
                elif coverage >= 0.60:
                    resolution_status = TRANSCRIPT_STATUS_PENDING_REVIEW
                    should_create_review = True
                    granularity = (
                        "word"
                        if token_distance <= 3
                        and whisper_segment_count <= 1
                        and avg_confidence >= 0.72
                        else "sentence"
                    )
                else:
                    granularity = "sentence"
            else:
                granularity = "sentence"

            cursor = conn.execute(
                """
                INSERT INTO transcript (
                    meeting_id,
                    sequence_no,
                    speaker,
                    teams_text,
                    text,
                    start_sec,
                    end_sec,
                    caption_started_at,
                    caption_finalized_at,
                    resolution_status,
                    auto_corrected,
                    timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    meeting_id,
                    sequence_no,
                    row["speaker"],
                    teams_text,
                    final_text,
                    row.get("start_sec"),
                    row.get("end_sec"),
                    row.get("caption_started_at").isoformat() if row.get("caption_started_at") else None,
                    row.get("caption_finalized_at").isoformat() if row.get("caption_finalized_at") else None,
                    resolution_status,
                    auto_corrected,
                    (row.get("caption_finalized_at") or row.get("caption_started_at") or datetime.utcnow()).isoformat(),
                ),
            )
            transcript_id = int(cursor.lastrowid)

            if should_create_review:
                clip_start_sec = None
                clip_end_sec = None
                if row.get("start_sec") is not None and row.get("end_sec") is not None and coverage >= 0.60:
                    clip_start_sec = max(float(row["start_sec"]) - 1.0, 0.0)
                    clip_end_sec = max(float(row["end_sec"]) + 1.0, clip_start_sec + 1.0)
                review_item_id = create_review_item(
                    conn,
                    transcript_id,
                    granularity,
                    teams_text,
                    whisper_text,
                    min(
                        1.0,
                        min(
                            max(coverage, similarity),
                            avg_confidence if avg_confidence > 0 else 1.0,
                        ),
                    ),
                    int((clip_start_sec or 0.0) * 1000),
                    int((clip_end_sec or 0.0) * 1000),
                )
                if source_audio_path is not None and clip_start_sec is not None and clip_end_sec is not None:
                    clip_filename = create_audio_clip(
                        source_audio_path,
                        meeting_id,
                        transcript_id,
                        review_item_id,
                        clip_start_sec,
                        clip_end_sec,
                    )
                    if clip_filename:
                        update_review_item_clip_path(conn, review_item_id, clip_filename)
                pending_reviews += 1

        conn.commit()
    finally:
        conn.close()
    return pending_reviews


def process_meeting(meeting_id: int):
    meeting = fetch_meeting(meeting_id)
    if not meeting:
        raise RuntimeError("meeting not found")

    asset = fetch_meeting_audio_asset(meeting_id)
    audio_ready = bool(
        asset
        and asset["status"] == AUDIO_STATUS_READY
        and asset["master_audio_path"]
        and Path(asset["master_audio_path"]).exists()
    )
    dependency_error = dependency_error_message(require_whisperx=audio_ready)
    if dependency_error:
        raise RuntimeError(dependency_error)

    update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_CANONICALIZING, None)
    caption_events = fetch_caption_events(meeting_id)
    if not caption_events:
        caption_events = build_events_from_legacy_transcripts(fetch_legacy_transcripts(meeting_id))
    canonical_rows = canonicalize_caption_events(caption_events)
    persist_json(get_teams_canonical_path(meeting_id), canonical_rows)

    whisper_result = None
    whisper_tokens: list[dict] = []
    whisper_segments: list[dict] = []
    source_audio_path: Path | None = None

    if audio_ready and asset is not None:
        update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_TRANSCRIBING, None)
        master_audio_path = Path(asset["master_audio_path"])
        pcm_audio_path = Path(asset["pcm_audio_path"]) if asset["pcm_audio_path"] else None
        if not pcm_audio_path or not pcm_audio_path.exists():
            pcm_audio_path = convert_audio_to_pcm(master_audio_path, meeting_id)
        update_audio_asset_paths(asset["id"], str(pcm_audio_path), POSTPROCESS_VERSION)
        source_audio_path = pcm_audio_path
        whisper_result = load_whisperx_result(pcm_audio_path)
        persist_json(get_whisperx_result_path(meeting_id), whisper_result)
        whisper_tokens, whisper_segments = build_whisper_tokens(whisper_result)

    update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_ALIGNING, None)
    teams_tokens = build_teams_tokens(canonical_rows)
    for index, token in enumerate(teams_tokens):
        token["global_index"] = index
    for index, token in enumerate(whisper_tokens):
        token["global_index"] = index

    if whisper_tokens:
        teams_to_whisper, whisper_to_teams, anchors = align_token_streams(teams_tokens, whisper_tokens)
    else:
        teams_to_whisper, whisper_to_teams, anchors = {}, {}, []

    final_rows, alignment_debug, canonical_debug = build_final_rows(
        canonical_rows,
        teams_tokens,
        whisper_tokens,
        whisper_segments,
        teams_to_whisper,
        whisper_to_teams,
    )
    alignment_payload = {
        "anchors": anchors,
        "teams_token_count": len(teams_tokens),
        "whisper_token_count": len(whisper_tokens),
        "mapped_team_tokens": len(teams_to_whisper),
        "mapped_whisper_tokens": len(whisper_to_teams),
        "rows": alignment_debug,
        "canonical_rows": canonical_debug,
    }
    persist_json(get_alignment_map_path(meeting_id), alignment_payload)

    update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_REBUILDING, None)
    clear_previous_outputs(meeting_id)
    pending_reviews = persist_final_outputs(meeting_id, final_rows, source_audio_path)
    if pending_reviews:
        update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_REVIEW_READY, None)
    update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_COMPLETED, None)


def main():
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python bot/postprocess_worker.py <meeting_id>")

    meeting_id = int(sys.argv[1])
    ensure_runtime_schema(get_db_path())
    update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_QUEUED, None)
    try:
        process_meeting(meeting_id)
        logger.info("Post-process completed for meeting %s", meeting_id)
    except Exception as exc:
        logger.exception("Post-process failed for meeting %s", meeting_id)
        update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_FAILED, str(exc))
        raise


if __name__ == "__main__":
    main()
