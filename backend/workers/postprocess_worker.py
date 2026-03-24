import contextlib
import importlib.util
import inspect
import json
import logging
import os
import re
import shutil
import sqlite3
import statistics
import subprocess
import sys
import wave
from collections import defaultdict
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.runtime.bootstrap import ensure_runtime_schema  # noqa: E402
from backend.runtime.logging import bind_context, configure_logging, log_event, reset_context  # noqa: E402
from backend.runtime.constants import (  # noqa: E402
    ASSIGNMENT_STATUS_CONFIRMED,
    ASSIGNMENT_STATUS_PROVISIONAL,
    ASSIGNMENT_STATUS_UNKNOWN,
    AUDIO_STATUS_READY,
    POSTPROCESS_STATUS_ASSEMBLING_SEGMENTS,
    POSTPROCESS_STATUS_BINDING_SOURCES,
    POSTPROCESS_STATUS_ALIGNING,
    POSTPROCESS_STATUS_CANONICALIZING,
    POSTPROCESS_STATUS_COMPLETED,
    POSTPROCESS_STATUS_FAILED,
    POSTPROCESS_STATUS_MATERIALIZING_AUDIO,
    POSTPROCESS_STATUS_QUEUED,
    POSTPROCESS_STATUS_REBUILDING,
    POSTPROCESS_STATUS_REVIEW_READY,
    POSTPROCESS_STATUS_TRANSCRIBING_PARTICIPANTS,
    POSTPROCESS_STATUS_TRANSCRIBING,
    REVIEW_STATUS_PENDING,
    TRANSCRIPT_STATUS_AUTO_APPLIED,
    TRANSCRIPT_STATUS_ORIGINAL,
    TRANSCRIPT_STATUS_PENDING_REVIEW,
)
from backend.runtime.participant_names import is_roster_heading_name, normalize_participant_name  # noqa: E402
from backend.runtime.paths import (  # noqa: E402
    get_alignment_map_path,
    get_meeting_artifact_path,
    db_path as get_db_path,
    get_participant_audio_asset_path,
    get_meeting_pcm_audio_path,
    get_review_clip_filename,
    get_review_clip_path,
    get_segment_review_clip_filename,
    get_segment_review_clip_path,
    get_teams_canonical_path,
    get_whisperx_result_path,
    remove_review_clips_for_meeting,
    runtime_cache_dir,
)

configure_logging()
logger = logging.getLogger("notera.worker.postprocess")

ASR_MODEL_DEFAULT = "large-v3"
TURKISH_ALIGNMENT_MODEL = "cahya/wav2vec2-base-turkish"
POSTPROCESS_VERSION = "whisperx_global_v1"
TOKEN_RE = re.compile(r"[\wçğıöşüÇĞİÖŞÜ@.\-/']+")
PROGRESS_RE = re.compile(r"Progress:\s*([0-9]+(?:\.[0-9]+)?)%")
DEFAULT_SPOKEN_LANGUAGE = os.getenv("WHISPERX_LANGUAGE", "tr").strip().lower() or "tr"
FORCE_SPOKEN_LANGUAGE = os.getenv("WHISPERX_FORCE_LANGUAGE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}
LATE_REPLAY_MATCH_WINDOW_SECONDS = 24.0
LATE_REPLAY_BURST_WINDOW_SECONDS = 3.0
LATE_REPLAY_SLOT_RESET_MIN_SEEN = 8
LATE_REPLAY_LOOKBACK_ROWS = 160
FINAL_ROW_DEFENSIVE_DUPLICATE_WINDOW_SECONDS = 180.0
FINAL_ROW_DEFENSIVE_LOOKBACK_ROWS = 96
PYANNOTE_DIARIZATION_MODEL_DEFAULT = "pyannote/speaker-diarization-3.1"
PYANNOTE_EMBEDDING_MODEL_DEFAULT = "pyannote/embedding"
MIN_DIARIZED_TURN_MS = 800
DIARIZATION_MERGE_GAP_MS = 500
MIN_SEED_TURN_MS = 1500
MIN_SEED_SPEECH_RATIO = 0.60
MIN_SEED_PRIOR = 0.55
MIN_SEED_MARGIN = 0.15
RELAXED_SEED_TURN_MS = 1000
RELAXED_SEED_SPEECH_RATIO = 0.50
RELAXED_SEED_PRIOR = 0.35
RELAXED_SEED_MARGIN = 0.05
CLUSTER_BOOTSTRAP_PRIOR = 0.33
CLUSTER_BOOTSTRAP_MARGIN = 0.08
MAX_PROFILE_SEED_TURNS = 20
PROFILE_OUTLIER_TOLERANCE = 0.12
CLUSTER_CONFIRM_SCORE = 0.68
CLUSTER_CONFIRM_MARGIN = 0.12
CLUSTER_CONFIRM_DURATION_MS = 4000
CLUSTER_CONFIRM_TURN_COUNT = 2
CLUSTER_REMAP_MARGIN = 0.18
CLUSTER_REMAP_TURN_COUNT = 2
MIN_UNRESOLVED_ASSIGNMENT_SCORE = 0.33
MIN_UNRESOLVED_DOM_SIGNAL = 0.28
MAX_ASSIGNMENT_PASSES = 4
SEGMENT_SINGLE_TURN_DOMINANCE_RATIO = 0.70
SEGMENT_SPLIT_MIN_OVERLAP_RATIO = 0.15
SEGMENT_SPLIT_MIN_OVERLAP_MS = 350
CLUSTER_PROTOTYPE_TURN_MS = 1000
CLUSTER_PROTOTYPE_SPEECH_RATIO = 0.55
CLUSTER_IMPURE_PURITY = 0.80
CLUSTER_IMPURE_DOM_MARGIN = 0.10
CLUSTER_IMPURE_COVERAGE_MS = 8000
CLUSTER_SPLIT_CHILD_SIMILARITY = 0.82
CLUSTER_SPLIT_MIN_COVERAGE_MS = 2500
CLUSTER_SPLIT_MIN_TURNS = 2
HYBRID_SEED_MIN_DURATION_MS = 1500
HYBRID_SEED_MIN_SPEECH_RATIO = 0.60
HYBRID_SEED_MIN_AUDIO_IDENTITY_SCORE = 0.68
HYBRID_SEED_MIN_UI_ACTIVITY_SCORE = 0.78
HYBRID_SEED_MIN_AUDIO_MARGIN = 0.15
HYBRID_SEED_MIN_UI_MARGIN = 0.18
HYBRID_CONFIRMED_FINAL_SCORE_FLOOR = 0.72
HYBRID_PROVISIONAL_FINAL_SCORE_FLOOR = 0.58
HYBRID_CONFIRMED_AUDIO_SCORE_FLOOR = 0.62
HYBRID_CONFIRMED_UI_SCORE_FLOOR = 0.68
HYBRID_CONFIRMED_AUDIO_MARGIN_FLOOR = 0.14
HYBRID_CONFIRMED_UI_MARGIN_FLOOR = 0.16
HYBRID_PROVISIONAL_AUDIO_MARGIN_FLOOR = 0.12
HYBRID_PROVISIONAL_UI_MARGIN_FLOOR = 0.14
HYBRID_STRONG_PROVISIONAL_REFRESH_FLOOR = 0.66
HYBRID_NO_SIGNAL_PRIMARY_FLOOR = 0.20
HYBRID_STRONG_CONFLICT_PRIMARY_FLOOR = 0.60
HYBRID_STRONG_CONFLICT_MARGIN_FLOOR = 0.10
HYBRID_TEMPORAL_NEUTRAL_BASE = 0.15
HYBRID_TEMPORAL_NEIGHBOR_GAP_MS = 1200
HYBRID_CAPTION_FUZZY_UNIQUE_FLOOR = 0.60
HYBRID_CAPTION_FUZZY_SEPARATION_MARGIN = 0.10
HYBRID_UI_MERGE_GAP_MS = 700
HYBRID_UI_MIN_PULSE_MS = 300
HYBRID_UI_CONFIDENCE_CAP = 0.95
HYBRID_UI_SIGNAL_RELIABILITY = {
    "teams_ui_outline": 1.00,
    "teams_dom_mutation": 0.90,
    "teams_ui_polling": 0.75,
    "fallback": 0.60,
}
HYBRID_ASSIGNMENT_METHOD_CONFIRMED = "hybrid_ui_audio_confirmed"
HYBRID_ASSIGNMENT_METHOD_UI_PROVISIONAL = "hybrid_ui_led_provisional"
HYBRID_ASSIGNMENT_METHOD_AUDIO_PROVISIONAL = "hybrid_audio_led_provisional"
HYBRID_ASSIGNMENT_METHOD_CONFLICTED = "hybrid_conflicted_review"
HYBRID_ASSIGNMENT_METHOD_UNKNOWN_NO_SIGNAL = "hybrid_unknown_no_signal"
HYBRID_ASSIGNMENT_METHOD_UNKNOWN_LOW_EVIDENCE = "hybrid_unknown_low_evidence"
HYBRID_SPEAKER_STATUS_CONFIRMED = "confirmed"
HYBRID_SPEAKER_STATUS_UI_PROVISIONAL = "provisional_ui_led"
HYBRID_SPEAKER_STATUS_AUDIO_PROVISIONAL = "provisional_audio_led"
HYBRID_SPEAKER_STATUS_CONFLICTED = "conflicted"
HYBRID_SPEAKER_STATUS_UNKNOWN = "unknown"
HYBRID_CAPTION_MATCH_TYPES = {"exact", "fuzzy", "ambiguous", "none"}
HYBRID_REASON_CODE_UI_AUDIO_CONFLICT = "ui_audio_conflict"
HYBRID_REASON_CODE_UI_CAPTION_CONFLICT = "ui_caption_conflict"
HYBRID_REASON_CODE_CAPTION_AUDIO_CONFLICT = "caption_audio_conflict"
HYBRID_REASON_CODE_MULTI_ACTIVE = "multi_active_claim"
HYBRID_REASON_CODE_NO_SIGNAL = "no_signal"
HYBRID_REASON_CODE_LOW_EVIDENCE = "low_evidence"
HYBRID_REASON_CODE_WEAK_AUDIO_MARGIN = "weak_audio_margin"
HYBRID_REASON_CODE_WEAK_UI_MARGIN = "weak_ui_margin"
HYBRID_REASON_CODE_TEMPORAL_CONFLICT = "temporal_conflict"
HYBRID_REASON_CODE_CAPTION_AMBIGUOUS = "caption_ambiguous"
HYBRID_REVIEW_TYPE_MULTI_ACTIVE = "speaker_multi_active_claim"
HYBRID_REVIEW_TYPE_UI_AUDIO_CONFLICT = "speaker_conflict_ui_audio"
HYBRID_REVIEW_TYPE_UI_CAPTION_CONFLICT = "speaker_conflict_ui_caption"
HYBRID_REVIEW_TYPE_CAPTION_AUDIO_CONFLICT = "speaker_conflict_caption_audio"
HYBRID_REVIEW_TYPE_LOW_EVIDENCE = "speaker_low_evidence"

def configure_runtime_environment():
    os.environ.setdefault("MPLCONFIGDIR", str(runtime_cache_dir("matplotlib")))
    os.environ.setdefault("HF_HOME", str(runtime_cache_dir("huggingface")))
    os.environ.setdefault("XDG_CACHE_HOME", str(runtime_cache_dir("xdg")))


def configure_torch_checkpoint_compatibility():
    try:
        import torch
        from omegaconf.base import ContainerMetadata
        from omegaconf.dictconfig import DictConfig
        from omegaconf.listconfig import ListConfig
        from omegaconf.nodes import (
            AnyNode,
            BooleanNode,
            BytesNode,
            EnumNode,
            FloatNode,
            IntegerNode,
            PathNode,
            StringNode,
            ValueNode,
        )
    except Exception:
        return

    # Older pyannote checkpoints can still rely on objects that PyTorch 2.6+
    # no longer accepts by default when `weights_only=True`.
    try:
        torch.serialization.add_safe_globals(
            [
                ContainerMetadata,
                DictConfig,
                ListConfig,
                AnyNode,
                BooleanNode,
                BytesNode,
                EnumNode,
                FloatNode,
                IntegerNode,
                PathNode,
                StringNode,
                ValueNode,
            ]
        )
    except Exception:
        pass

    if getattr(torch.load, "_notera_trusted_checkpoint_patch", False):
        return

    original_torch_load = torch.load

    def trusted_torch_load(*args, **kwargs):
        # WhisperX / pyannote checkpoints used by this app are treated as
        # trusted runtime assets; force legacy load semantics to keep
        # PyTorch 2.6+ compatibility with older OmegaConf-based checkpoints.
        kwargs["weights_only"] = False
        return original_torch_load(*args, **kwargs)

    trusted_torch_load._notera_trusted_checkpoint_patch = True  # type: ignore[attr-defined]
    torch.load = trusted_torch_load


def huggingface_model_cached(repo_id: str) -> bool:
    model_dir = runtime_cache_dir("huggingface") / "hub" / f"models--{repo_id.replace('/', '--')}"
    snapshots_dir = model_dir / "snapshots"
    return snapshots_dir.exists() and any(snapshots_dir.iterdir())


def huggingface_model_snapshot_path(repo_id: str) -> Path | None:
    model_dir = runtime_cache_dir("huggingface") / "hub" / f"models--{repo_id.replace('/', '--')}"
    snapshots_dir = model_dir / "snapshots"
    if not snapshots_dir.exists():
        return None
    snapshots = sorted(path for path in snapshots_dir.iterdir() if path.is_dir())
    if not snapshots:
        return None
    return snapshots[-1]


def module_spec_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except ModuleNotFoundError:
        return False


def dependency_error_message(require_whisperx: bool, require_pyannote: bool = False) -> str | None:
    if require_whisperx and importlib.util.find_spec("whisperx") is None:
        return (
            "WhisperX kurulu değil. "
            "`conda activate teams-bot && python -m pip install -r backend/requirements.txt` "
            "veya `python -m pip install whisperx` çalıştırın."
        )
    if require_pyannote and not module_spec_available("pyannote.audio"):
        return (
            "pyannote.audio kurulu değil. "
            "`conda activate teams-bot && python -m pip install -r backend/requirements.txt` çalıştırın."
        )
    if require_whisperx and shutil.which("ffmpeg") is None:
        return "ffmpeg bulunamadı. Sistem PATH içinde ffmpeg kurulu olmalı."
    return None


def db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def update_meeting_postprocess_status(
    meeting_id: int,
    status: str,
    error: str | None = None,
    progress_pct: int | None = None,
    progress_note: str | None = None,
):
    conn = db_connection()
    try:
        conn.execute(
            """
            UPDATE meeting
            SET postprocess_status = ?,
                postprocess_error = ?,
                postprocess_progress_pct = ?,
                postprocess_progress_note = ?
            WHERE id = ?
            """,
            (status, error, progress_pct, progress_note, meeting_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_meeting_postprocess_progress(
    meeting_id: int,
    progress_pct: int | None,
    progress_note: str | None = None,
):
    conn = db_connection()
    try:
        conn.execute(
            """
            UPDATE meeting
            SET postprocess_progress_pct = ?,
                postprocess_progress_note = COALESCE(?, postprocess_progress_note)
            WHERE id = ?
            """,
            (progress_pct, progress_note, meeting_id),
        )
        conn.commit()
    finally:
        conn.close()


@contextlib.contextmanager
def capture_progress_prints(meeting_id: int, stage_note: str, *modules):
    original_print = print
    module_states: list[tuple[object, bool, object | None]] = []
    last_pct: int | None = None

    def tracked_print(*args, **kwargs):
        nonlocal last_pct
        sep = kwargs.get("sep", " ")
        text = sep.join(str(arg) for arg in args)
        match = PROGRESS_RE.search(text)
        if match:
            progress_pct = max(0, min(int(round(float(match.group(1)))), 100))
            if progress_pct != last_pct:
                last_pct = progress_pct
                update_meeting_postprocess_progress(
                    meeting_id,
                    progress_pct,
                    stage_note,
                )
        return original_print(*args, **kwargs)

    for module in modules:
        had_print = "print" in vars(module)
        previous_print = vars(module).get("print")
        module_states.append((module, had_print, previous_print))
        setattr(module, "print", tracked_print)
    try:
        yield
    finally:
        for module, had_print, previous_print in module_states:
            if had_print:
                setattr(module, "print", previous_print)
            else:
                delattr(module, "print")


def fetch_meeting(meeting_id: int) -> sqlite3.Row | None:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, status, audio_status, joined_at, ended_at, audio_capture_started_at
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


def fetch_audio_sources(meeting_id: int) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, meeting_id, source_key, source_kind, track_id, stream_id, file_path, format,
                   sample_rate_hz, channel_count, first_seen_at, last_seen_at, status
            FROM audiosource
            WHERE meeting_id = ?
            ORDER BY id
            """,
            (meeting_id,),
        ).fetchall()
    finally:
        conn.close()


def upsert_mixed_audio_source(
    meeting_id: int,
    file_path: str,
    fmt: str | None,
    status: str = AUDIO_STATUS_READY,
) -> int:
    conn = db_connection()
    try:
        existing = conn.execute(
            """
            SELECT id
            FROM audiosource
            WHERE meeting_id = ? AND source_key = 'meeting:master'
            LIMIT 1
            """,
            (meeting_id,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE audiosource
                SET file_path = ?, format = ?, status = ?, last_seen_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (file_path, fmt, status, existing["id"]),
            )
            conn.commit()
            return int(existing["id"])
        cursor = conn.execute(
            """
            INSERT INTO audiosource (
                meeting_id,
                source_key,
                source_kind,
                file_path,
                format,
                status,
                first_seen_at,
                last_seen_at,
                created_at
            ) VALUES (?, 'meeting:master', 'meeting_mixed_master', ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (meeting_id, file_path, fmt, status),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def fetch_meeting_participants(meeting_id: int) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, meeting_id, participant_key, platform_identity, display_name, normalized_name,
                   role, is_bot, join_state, merged_into_participant_id, first_seen_at, last_seen_at
            FROM meetingparticipant
            WHERE meeting_id = ?
            ORDER BY display_name, id
            """,
            (meeting_id,),
        ).fetchall()
        return [row for row in rows if not is_roster_heading_name(row["display_name"])]
    finally:
        conn.close()


def fetch_speaker_activity_events(meeting_id: int) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, meeting_id, participant_id, start_offset_ms, end_offset_ms, source,
                   event_type, signal_kind, event_confidence, ui_observed_at, relative_offset_ms,
                   source_session_id, confidence, metadata_json
            FROM speakeractivityevent
            WHERE meeting_id = ?
            ORDER BY start_offset_ms, end_offset_ms, id
            """,
            (meeting_id,),
        ).fetchall()
    finally:
        conn.close()


def fetch_participant_audio_assets(meeting_id: int) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        return conn.execute(
            """
            SELECT id, meeting_id, participant_id, audio_source_id, asset_type, file_path, format,
                   sample_rate_hz, channel_count, duration_ms, start_offset_ms, end_offset_ms, status,
                   derivation_method, confidence
            FROM participantaudioasset
            WHERE meeting_id = ?
            ORDER BY participant_id, start_offset_ms, id
            """,
            (meeting_id,),
        ).fetchall()
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
        segment_ids = [
            row[0]
            for row in conn.execute(
                "SELECT id FROM transcriptsegment WHERE meeting_id = ?",
                (meeting_id,),
            ).fetchall()
        ]
        if transcript_ids:
            placeholders = ",".join("?" for _ in transcript_ids)
            conn.execute(
                f"DELETE FROM transcriptreviewitem WHERE transcript_id IN ({placeholders})",
                transcript_ids,
            )
        if segment_ids:
            placeholders = ",".join("?" for _ in segment_ids)
            conn.execute(
                f"DELETE FROM transcriptreviewitem WHERE transcript_segment_id IN ({placeholders})",
                segment_ids,
            )
        conn.execute("DELETE FROM transcriptsegment WHERE meeting_id = ?", (meeting_id,))
        conn.execute("DELETE FROM participantaudioasset WHERE meeting_id = ?", (meeting_id,))
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
    logger.info("Converting meeting audio to PCM format.")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ffmpeg conversion failed")
    return output_path


def persist_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def load_whisperx_result(audio_path: Path, meeting_id: int) -> dict:
    configure_runtime_environment()
    configure_torch_checkpoint_compatibility()
    try:
        import whisperx
        import whisperx.alignment as whisperx_alignment
        import whisperx.asr as whisperx_asr
    except Exception as exc:
        raise RuntimeError(
            "WhisperX import edilemedi. "
            "teams-bot env içinde `python -m pip install -r backend/requirements.txt` çalıştırın. "
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

    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_TRANSCRIBING,
        None,
        0,
        "Transcript çıkarılıyor",
    )
    with capture_progress_prints(
        meeting_id,
        "Transcript çıkarılıyor",
        whisperx_asr,
    ):
        result = model.transcribe(
            str(audio_path),
            batch_size=batch_size,
            language=requested_language,
            print_progress=True,
            combined_progress=True,
        )
    language_code = (result.get("language") or requested_language or "").lower()
    if requested_language:
        result["language"] = language_code
    if not language_code:
        return result

    align_model_name = TURKISH_ALIGNMENT_MODEL if language_code.startswith("tr") else None
    align_snapshot_path = (
        huggingface_model_snapshot_path(align_model_name)
        if align_model_name
        else None
    )
    align_model_source = str(align_snapshot_path) if align_snapshot_path else align_model_name
    align_cache_only = bool(align_snapshot_path)
    try:
        update_meeting_postprocess_status(
            meeting_id,
            POSTPROCESS_STATUS_ALIGNING,
            None,
            None,
            "Hizalama modeli yükleniyor",
        )
        align_kwargs = {
            "language_code": language_code,
            "device": "cpu",
            "model_name": align_model_source,
        }
        try:
            signature = inspect.signature(whisperx.load_align_model)
            if "model_cache_only" in signature.parameters:
                align_kwargs["model_cache_only"] = align_cache_only
            if "model_dir" in signature.parameters:
                align_kwargs["model_dir"] = str(runtime_cache_dir("huggingface"))
        except (TypeError, ValueError):
            pass

        try:
            align_model, metadata = whisperx.load_align_model(**align_kwargs)
        except TypeError as exc:
            if "model_cache_only" not in str(exc):
                raise
            align_kwargs.pop("model_cache_only", None)
            align_model, metadata = whisperx.load_align_model(**align_kwargs)
        update_meeting_postprocess_status(
            meeting_id,
            POSTPROCESS_STATUS_ALIGNING,
            None,
            50,
            "Transcriptler hizalanıyor",
        )
        with capture_progress_prints(
            meeting_id,
            "Transcriptler hizalanıyor",
            whisperx_alignment,
        ):
            aligned_result = whisperx.align(
                result["segments"],
                align_model,
                metadata,
                str(audio_path),
                device="cpu",
                print_progress=True,
                combined_progress=True,
            )
        aligned_result["language"] = language_code
        aligned_result["_alignment_model"] = align_model_name or metadata.get("language")
        if align_snapshot_path:
            aligned_result["_alignment_model_source"] = str(align_snapshot_path)
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


def equivalent_text_fingerprint(value: str | None) -> str:
    return normalize_text(value).casefold().rstrip(".,!?;:…")


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


def is_late_replay_match(
    canonical_rows: list[dict],
    speaker: str,
    text: str,
    observed_at: datetime,
) -> bool:
    current_fingerprint = equivalent_text_fingerprint(text)
    if not current_fingerprint:
        return False

    for previous in reversed(canonical_rows[-LATE_REPLAY_LOOKBACK_ROWS:]):
        if not compatible_speakers(previous.get("speaker"), speaker):
            continue
        if equivalent_text_fingerprint(previous.get("text")) != current_fingerprint:
            continue
        previous_time = previous.get("finalized_at") or previous.get("started_at")
        if previous_time is None:
            continue
        if abs((observed_at - previous_time).total_seconds()) >= LATE_REPLAY_MATCH_WINDOW_SECONDS:
            return True
    return False


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


def looks_garbled_caption(text: str) -> bool:
    normalized = normalize_text(text)
    tokens = caption_tokens(normalized)
    token_count = len(tokens)
    if token_count < 6:
        return False

    punctuation_count = sum(normalized.count(mark) for mark in ".!?…")
    unique_ratio = len(set(tokens)) / max(token_count, 1)
    signals = 0

    if not text_ends_cleanly(normalized):
        signals += 1
    if punctuation_count == 0 and token_count >= 8:
        signals += 1
    if len(normalized) >= 80 and punctuation_count == 0:
        signals += 1
    if token_count >= 8 and unique_ratio <= 0.78:
        signals += 1

    return signals >= 2


def should_force_low_confidence_review(
    teams_text: str,
    whisper_text: str,
    coverage: float,
    avg_confidence: float,
) -> bool:
    if not whisper_text or texts_equivalent_for_review(teams_text, whisper_text):
        return False
    if coverage >= 0.60 or coverage < 0.10:
        return False

    teams_tokens = caption_tokens(teams_text)
    whisper_tokens = caption_tokens(whisper_text)
    if len(teams_tokens) < 6 or len(whisper_tokens) < 3:
        return False
    if not looks_garbled_caption(teams_text):
        return False

    signals = 0
    if sequence_ratio(teams_text, whisper_text) <= 0.45:
        signals += 1
    if len(teams_tokens) >= len(whisper_tokens) * 2:
        signals += 1
    if revision_text_score(whisper_text) >= revision_text_score(teams_text) + 12:
        signals += 1
    if text_ends_cleanly(whisper_text):
        signals += 1

    return signals >= 2


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
    max_seen_slot_index = -1
    replay_burst_until: datetime | None = None

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

        slot_reset_detected = (
            slot_index is not None
            and max_seen_slot_index >= LATE_REPLAY_SLOT_RESET_MIN_SEEN
            and int(slot_index) <= max(2, max_seen_slot_index // 4)
            and int(slot_index) + 6 < max_seen_slot_index
        )
        late_replay_match = is_late_replay_match(canonical, speaker, text, observed_at)
        if late_replay_match and replay_burst_until is not None and observed_at <= replay_burst_until:
            logger.debug(
                "Skipping late caption replay inside burst slot=%s text_length=%s",
                slot_index,
                len(text),
            )
            continue
        if late_replay_match and slot_reset_detected:
            replay_burst_until = observed_at + timedelta(seconds=LATE_REPLAY_BURST_WINDOW_SECONDS)
            logger.info(
                "Skipping late caption replay after slot reset slot=%s text_length=%s",
                slot_index,
                len(text),
            )
            continue

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
            max_seen_slot_index = max(max_seen_slot_index, int(slot_index))
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


def final_row_base_text(row: dict) -> str:
    return normalize_text(row.get("teams_text") or row.get("whisper_text") or "")


def final_row_score(row: dict) -> int:
    score = revision_text_score(final_row_base_text(row))
    score += int(round(float(row.get("coverage") or 0.0) * 100))
    score += int(round(float(row.get("avg_confidence") or 0.0) * 100))
    if row.get("start_sec") is not None and row.get("end_sec") is not None:
        score += 8
    return score


def defensively_filter_final_rows(rows: list[dict]) -> list[dict]:
    kept: list[dict] = []
    for row in rows:
        fingerprint = equivalent_text_fingerprint(final_row_base_text(row))
        if not fingerprint:
            kept.append(row)
            continue

        duplicate_index = None
        current_time = row.get("caption_started_at") or row.get("caption_finalized_at")
        for index in range(len(kept) - 1, max(-1, len(kept) - FINAL_ROW_DEFENSIVE_LOOKBACK_ROWS - 1), -1):
            previous = kept[index]
            if not compatible_speakers(previous.get("speaker"), row.get("speaker")):
                continue
            if equivalent_text_fingerprint(final_row_base_text(previous)) != fingerprint:
                continue
            previous_time = previous.get("caption_started_at") or previous.get("caption_finalized_at")
            delta_seconds = (
                abs((current_time - previous_time).total_seconds())
                if previous_time is not None and current_time is not None
                else 999.0
            )
            if delta_seconds <= FINAL_ROW_DEFENSIVE_DUPLICATE_WINDOW_SECONDS:
                duplicate_index = index
                break

        if duplicate_index is None:
            kept.append(row)
            continue

        if final_row_score(row) > final_row_score(kept[duplicate_index]):
            kept[duplicate_index] = row

    return kept


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
        logger.warning("Failed to create review clip (ffmpeg_return_code=%s)", result.returncode)
        return None
    return get_review_clip_filename(meeting_id, transcript_id, review_item_id)


def create_segment_audio_clip(
    source_audio_path: Path,
    meeting_id: int,
    segment_id: int,
    review_item_id: int,
    clip_start_sec: float,
    clip_end_sec: float,
) -> str | None:
    output_path = get_segment_review_clip_path(meeting_id, segment_id, review_item_id)
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
        logger.warning("Failed to create segment review clip (ffmpeg_return_code=%s)", result.returncode)
        return None
    return get_segment_review_clip_filename(meeting_id, segment_id, review_item_id)


def insert_participant_audio_asset(
    conn: sqlite3.Connection,
    meeting_id: int,
    participant_id: int | None,
    audio_source_id: int | None,
    asset_type: str,
    file_path: str,
    fmt: str | None,
    start_offset_ms: int,
    end_offset_ms: int | None,
    derivation_method: str,
    confidence: float,
    duration_ms: int | None = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO participantaudioasset (
            meeting_id,
            participant_id,
            audio_source_id,
            asset_type,
            file_path,
            format,
            sample_rate_hz,
            channel_count,
            duration_ms,
            start_offset_ms,
            end_offset_ms,
            status,
            derivation_method,
            confidence,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            meeting_id,
            participant_id,
            audio_source_id,
            asset_type,
            file_path,
            fmt,
            16000 if fmt == "wav" else None,
            1 if fmt == "wav" else None,
            duration_ms,
            start_offset_ms,
            end_offset_ms,
            AUDIO_STATUS_READY,
            derivation_method,
            confidence,
            datetime.utcnow().isoformat(),
        ),
    )
    return int(cursor.lastrowid)


def build_participant_lookup(rows: list[sqlite3.Row]) -> dict[int, dict]:
    lookup: dict[int, dict] = {}
    for row in rows:
        lookup[int(row["id"])] = dict(row)
    return lookup


def _event_row_value(row: sqlite3.Row | dict, key: str, default=None):
    if isinstance(row, sqlite3.Row):
        return row[key] if key in row.keys() else default
    return row.get(key, default)


def ui_signal_reliability(signal_kind: str | None) -> float:
    return float(HYBRID_UI_SIGNAL_RELIABILITY.get(normalize_text(signal_kind).casefold(), HYBRID_UI_SIGNAL_RELIABILITY["fallback"]))


def normalize_speaker_activity_events(activity_rows: list[sqlite3.Row]) -> list[dict]:
    grouped_events: dict[int, list[dict]] = defaultdict(list)
    for row in activity_rows:
        participant_id = _event_row_value(row, "participant_id")
        if participant_id is None:
            continue
        event_type = normalize_text(_event_row_value(row, "event_type", "active")).casefold() or "active"
        if event_type != "active":
            continue
        start_offset_ms = int(_event_row_value(row, "start_offset_ms", 0) or 0)
        end_offset_ms = int(_event_row_value(row, "end_offset_ms", 0) or 0)
        if end_offset_ms <= start_offset_ms:
            continue
        metadata = parse_metadata_json(_event_row_value(row, "metadata_json"))
        source_value = normalize_text(_event_row_value(row, "source")).casefold()
        inferred_signal_kind = metadata.get("signal_kind") or metadata.get("source_kind")
        if not inferred_signal_kind and source_value == "roster_speaking_indicator":
            inferred_signal_kind = "teams_ui_outline"
        signal_kind = normalize_text(_event_row_value(row, "signal_kind") or inferred_signal_kind or "fallback")
        base_confidence = float(
            _event_row_value(row, "event_confidence", _event_row_value(row, "confidence", 0.0)) or 0.0
        )
        grouped_events[int(participant_id)].append(
            {
                "participant_id": int(participant_id),
                "start_offset_ms": start_offset_ms,
                "end_offset_ms": end_offset_ms,
                "signal_kind": signal_kind or "fallback",
                "base_confidence": clamp_score(base_confidence),
                "effective_confidence": clamp_score(base_confidence * ui_signal_reliability(signal_kind)),
                "conflicted_claim": bool(metadata.get("conflicted_claim")),
            }
        )

    merged_windows: list[dict] = []
    for participant_id, events in grouped_events.items():
        current: dict | None = None
        for event in sorted(events, key=lambda item: (item["start_offset_ms"], item["end_offset_ms"])):
            if current is None:
                current = {
                    "participant_id": participant_id,
                    "start_offset_ms": event["start_offset_ms"],
                    "end_offset_ms": event["end_offset_ms"],
                    "support_count": 1,
                    "signal_kinds": {event["signal_kind"]},
                    "confidence_sum": float(event["effective_confidence"]),
                    "base_confidence_sum": float(event["base_confidence"]),
                    "conflicted_claim": bool(event["conflicted_claim"]),
                }
                continue
            if event["start_offset_ms"] <= int(current["end_offset_ms"]) + HYBRID_UI_MERGE_GAP_MS:
                current["end_offset_ms"] = max(int(current["end_offset_ms"]), event["end_offset_ms"])
                current["support_count"] = int(current["support_count"]) + 1
                current["signal_kinds"].add(event["signal_kind"])
                current["confidence_sum"] = float(current["confidence_sum"]) + float(event["effective_confidence"])
                current["base_confidence_sum"] = float(current["base_confidence_sum"]) + float(event["base_confidence"])
                current["conflicted_claim"] = bool(current["conflicted_claim"]) or bool(event["conflicted_claim"])
                continue
            duration_ms = int(current["end_offset_ms"]) - int(current["start_offset_ms"])
            if duration_ms >= HYBRID_UI_MIN_PULSE_MS:
                merged_windows.append(current)
            current = {
                "participant_id": participant_id,
                "start_offset_ms": event["start_offset_ms"],
                "end_offset_ms": event["end_offset_ms"],
                "support_count": 1,
                "signal_kinds": {event["signal_kind"]},
                "confidence_sum": float(event["effective_confidence"]),
                "base_confidence_sum": float(event["base_confidence"]),
                "conflicted_claim": bool(event["conflicted_claim"]),
            }
        if current is not None:
            duration_ms = int(current["end_offset_ms"]) - int(current["start_offset_ms"])
            if duration_ms >= HYBRID_UI_MIN_PULSE_MS:
                merged_windows.append(current)

    normalized_windows: list[dict] = []
    for window in merged_windows:
        support_count = max(int(window["support_count"]), 1)
        signal_kinds = sorted(window["signal_kinds"])
        boost = 0.0
        if len(signal_kinds) >= 2:
            boost += 0.08
        if len(signal_kinds) >= 3:
            boost += 0.04
        confidence = clamp_score((float(window["confidence_sum"]) / support_count) + boost)
        normalized_windows.append(
            {
                "participant_id": int(window["participant_id"]),
                "start_offset_ms": int(window["start_offset_ms"]),
                "end_offset_ms": int(window["end_offset_ms"]),
                "confidence": min(HYBRID_UI_CONFIDENCE_CAP, confidence),
                "support_count": support_count,
                "signal_kinds": signal_kinds,
                "conflicted_claim": bool(window["conflicted_claim"]),
            }
        )

    for index, window in enumerate(normalized_windows):
        for other in normalized_windows[index + 1 :]:
            if window["participant_id"] == other["participant_id"]:
                continue
            overlap_ms = interval_overlap_ms(
                window["start_offset_ms"],
                window["end_offset_ms"],
                other["start_offset_ms"],
                other["end_offset_ms"],
            )
            if overlap_ms < 250:
                continue
            window["conflicted_claim"] = True
            other["conflicted_claim"] = True
            window["confidence"] = clamp_score(float(window["confidence"]) - 0.20)
            other["confidence"] = clamp_score(float(other["confidence"]) - 0.20)

    return sorted(
        normalized_windows,
        key=lambda item: (item["start_offset_ms"], item["end_offset_ms"], item["participant_id"]),
    )


def build_activity_windows(activity_rows: list[sqlite3.Row]) -> list[dict]:
    return normalize_speaker_activity_events(activity_rows)


def trim_audio_window(source_audio_path: Path, output_path: Path, start_offset_ms: int, end_offset_ms: int) -> bool:
    source_duration_ms = probe_wav_duration_ms(source_audio_path)
    if source_duration_ms is not None:
        if start_offset_ms >= source_duration_ms:
            return False
        end_offset_ms = min(end_offset_ms, source_duration_ms)
    if end_offset_ms <= start_offset_ms:
        return False
    duration_ms = max(end_offset_ms - start_offset_ms, 1000)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "ffmpeg",
        "-y",
        "-ss",
        f"{max(start_offset_ms, 0) / 1000:.3f}",
        "-i",
        str(source_audio_path),
        "-t",
        f"{duration_ms / 1000:.3f}",
        "-ac",
        "1",
        "-ar",
        "16000",
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 or not output_path.exists():
        return False
    trimmed_duration_ms = probe_wav_duration_ms(output_path)
    return trimmed_duration_ms is not None and trimmed_duration_ms > 0


def probe_wav_duration_ms(audio_path: Path) -> int | None:
    try:
        with wave.open(str(audio_path), "rb") as handle:
            frame_count = handle.getnframes()
            frame_rate = handle.getframerate()
    except (FileNotFoundError, EOFError, wave.Error):
        return None
    if frame_rate <= 0:
        return None
    return int(round((frame_count / frame_rate) * 1000))


def materialize_participant_audio_assets(
    meeting_id: int,
    source_audio_path: Path,
    audio_source_id: int | None,
    activity_rows: list[sqlite3.Row],
) -> list[sqlite3.Row]:
    conn = db_connection()
    try:
        windows = build_activity_windows(activity_rows)
        for window in windows:
            output_path = get_participant_audio_asset_path(
                meeting_id,
                window["participant_id"],
                window["start_offset_ms"],
                window["end_offset_ms"],
            )
            if not trim_audio_window(
                source_audio_path,
                output_path,
                window["start_offset_ms"],
                window["end_offset_ms"],
            ):
                continue
            insert_participant_audio_asset(
                conn,
                meeting_id,
                window["participant_id"],
                audio_source_id,
                "activity_interval_clip",
                str(output_path),
                "wav",
                window["start_offset_ms"],
                window["end_offset_ms"],
                "speaker_activity_trim",
                max(0.0, min(1.0, window["confidence"])),
                duration_ms=window["end_offset_ms"] - window["start_offset_ms"],
            )
        conn.commit()
    finally:
        conn.close()
    return fetch_participant_audio_assets(meeting_id)


def build_segments_from_whisper_result(
    result: dict,
    participant_id: int | None,
    participant_audio_asset_id: int | None,
    audio_source_id: int | None,
    base_offset_ms: int,
    assignment_method: str,
    assignment_confidence: float,
    needs_speaker_review: bool,
    source_audio_path: Path,
) -> list[dict]:
    segments: list[dict] = []
    for segment_index, segment in enumerate(result.get("segments", []), start=1):
        text = normalize_text(segment.get("text") or "")
        if not text:
            continue
        start_sec = float(segment.get("start") or 0.0)
        end_sec = float(segment.get("end") or start_sec)
        if end_sec <= start_sec:
            end_sec = start_sec + 0.5
        words: list[dict] = []
        for word_index, word in enumerate(segment.get("words") or []):
            word_text = normalize_text(word.get("word") or "")
            if not word_text:
                continue
            word_start = word.get("start")
            word_end = word.get("end")
            if word_start is None or word_end is None:
                continue
            word_start_ms = max(0, int(round(base_offset_ms + (float(word_start) * 1000))))
            word_end_ms = max(word_start_ms + 1, int(round(base_offset_ms + (float(word_end) * 1000))))
            words.append(
                {
                    "word_index": word_index,
                    "text": word_text,
                    "start_offset_ms": word_start_ms,
                    "end_offset_ms": word_end_ms,
                }
            )
        segments.append(
            {
                "segment_id": f"seg-{segment_index}-0",
                "participant_id": participant_id,
                "participant_audio_asset_id": participant_audio_asset_id,
                "audio_source_id": audio_source_id,
                "raw_text": text,
                "text": text,
                "language": DEFAULT_SPOKEN_LANGUAGE,
                "start_offset_ms": max(0, int(round(base_offset_ms + (start_sec * 1000)))),
                "end_offset_ms": max(0, int(round(base_offset_ms + (end_sec * 1000)))),
                "asr_confidence": None,
                "assignment_method": assignment_method,
                "assignment_confidence": assignment_confidence,
                "speaker_resolution_status": HYBRID_SPEAKER_STATUS_UNKNOWN,
                "needs_speaker_review": needs_speaker_review,
                "resolution_status": (
                    TRANSCRIPT_STATUS_PENDING_REVIEW if needs_speaker_review else TRANSCRIPT_STATUS_ORIGINAL
                ),
                "source_audio_path": source_audio_path,
                "overlap_group_id": None,
                "source_segment_index": segment_index,
                "words": words,
                "cluster_lineage": None,
            }
        )
    return segments


def clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def interval_overlap_ms(start_a: int, end_a: int, start_b: int, end_b: int) -> int:
    return max(0, min(end_a, end_b) - max(start_a, start_b))


def normalize_score_map(score_map: dict[int, float]) -> dict[int, float]:
    total = sum(max(0.0, float(score)) for score in score_map.values())
    if total <= 0:
        return {participant_id: 0.0 for participant_id in score_map}
    return {
        participant_id: clamp_score(max(0.0, float(score)) / total)
        for participant_id, score in score_map.items()
    }


def flatten_numeric_sequence(value) -> list[float]:
    if value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)]
    if hasattr(value, "detach"):
        value = value.detach().cpu().tolist()
    elif hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        flattened: list[float] = []
        for item in value:
            flattened.extend(flatten_numeric_sequence(item))
        return flattened
    return []


def normalize_embedding_vector(raw_embedding) -> list[float] | None:
    values = flatten_numeric_sequence(raw_embedding)
    if not values:
        return None
    norm = sum(value * value for value in values) ** 0.5
    if norm <= 0:
        return None
    return [value / norm for value in values]


def average_embeddings(embeddings: list[list[float]]) -> list[float] | None:
    usable = [embedding for embedding in embeddings if embedding]
    if not usable:
        return None
    width = len(usable[0])
    if width <= 0:
        return None
    totals = [0.0] * width
    for embedding in usable:
        if len(embedding) != width:
            continue
        for index, value in enumerate(embedding):
            totals[index] += value
    count = len(usable)
    if count <= 0:
        return None
    averaged = [value / count for value in totals]
    return normalize_embedding_vector(averaged)


def cosine_similarity_vectors(left: list[float] | None, right: list[float] | None) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return clamp_score((sum(a * b for a, b in zip(left, right)) + 1.0) / 2.0)


def score_margin(score_map: dict[int, float]) -> float:
    ranked = sorted(score_map.values(), reverse=True)
    best = ranked[0] if ranked else 0.0
    second = ranked[1] if len(ranked) > 1 else 0.0
    return float(best - second)


def build_cluster_prototypes(turns: list[dict]) -> dict[str, dict]:
    grouped_turns: dict[str, list[dict]] = defaultdict(list)
    for turn in turns:
        grouped_turns[turn["speaker_label"]].append(turn)

    prototypes: dict[str, dict] = {}
    for label, label_turns in grouped_turns.items():
        prototype_turns = [
            turn
            for turn in label_turns
            if turn.get("embedding")
            and int(turn.get("duration_ms") or 0) >= CLUSTER_PROTOTYPE_TURN_MS
            and float(turn.get("speech_ratio") or 0.0) >= CLUSTER_PROTOTYPE_SPEECH_RATIO
            and not turn.get("overlap_flag")
        ]
        if not prototype_turns:
            prototype_turns = [turn for turn in label_turns if turn.get("embedding")]

        centroid = average_embeddings([turn.get("embedding") for turn in prototype_turns])
        similarities = [
            cosine_similarity_vectors(turn.get("embedding"), centroid)
            for turn in prototype_turns
            if turn.get("embedding")
        ]
        coverage_ms = sum(int(turn.get("duration_ms") or 0) for turn in label_turns)
        purity = (sum(similarities) / len(similarities)) if similarities else 0.0
        prototypes[label] = {
            "speaker_label": label,
            "centroid": centroid,
            "purity": purity,
            "coverage_ms": coverage_ms,
            "turn_count": len(label_turns),
            "prototype_turn_ids": [turn["turn_id"] for turn in prototype_turns],
            "base_label": label.split("#", 1)[0],
            "split_applied": "#" in label,
            "impure": False,
        }
    return prototypes


def split_cluster_by_embeddings(label_turns: list[dict]) -> dict | None:
    candidates = [turn for turn in label_turns if turn.get("embedding")]
    if len(candidates) < CLUSTER_SPLIT_MIN_TURNS * 2:
        return None

    seed_pair: tuple[dict, dict] | None = None
    lowest_similarity = 1.0
    for index, left_turn in enumerate(candidates):
        for right_turn in candidates[index + 1 :]:
            similarity = cosine_similarity_vectors(left_turn.get("embedding"), right_turn.get("embedding"))
            if similarity < lowest_similarity:
                lowest_similarity = similarity
                seed_pair = (left_turn, right_turn)
    if seed_pair is None:
        return None

    center_a = seed_pair[0].get("embedding")
    center_b = seed_pair[1].get("embedding")
    assignments: dict[str, int] = {}
    for _ in range(6):
        new_assignments: dict[str, int] = {}
        for turn in candidates:
            similarity_a = cosine_similarity_vectors(turn.get("embedding"), center_a)
            similarity_b = cosine_similarity_vectors(turn.get("embedding"), center_b)
            new_assignments[turn["turn_id"]] = 0 if similarity_a >= similarity_b else 1
        if new_assignments == assignments:
            break
        assignments = new_assignments
        bucket_a = [turn.get("embedding") for turn in candidates if assignments.get(turn["turn_id"]) == 0]
        bucket_b = [turn.get("embedding") for turn in candidates if assignments.get(turn["turn_id"]) == 1]
        if len(bucket_a) < CLUSTER_SPLIT_MIN_TURNS or len(bucket_b) < CLUSTER_SPLIT_MIN_TURNS:
            return None
        center_a = average_embeddings(bucket_a)
        center_b = average_embeddings(bucket_b)

    child_turns = {
        0: [turn for turn in candidates if assignments.get(turn["turn_id"]) == 0],
        1: [turn for turn in candidates if assignments.get(turn["turn_id"]) == 1],
    }
    if any(len(turns) < CLUSTER_SPLIT_MIN_TURNS for turns in child_turns.values()):
        return None

    child_coverages = {
        child_index: sum(int(turn.get("duration_ms") or 0) for turn in turns)
        for child_index, turns in child_turns.items()
    }
    if any(coverage < CLUSTER_SPLIT_MIN_COVERAGE_MS for coverage in child_coverages.values()):
        return None

    child_centroids = {
        0: average_embeddings([turn.get("embedding") for turn in child_turns[0]]),
        1: average_embeddings([turn.get("embedding") for turn in child_turns[1]]),
    }
    if cosine_similarity_vectors(child_centroids[0], child_centroids[1]) >= CLUSTER_SPLIT_CHILD_SIMILARITY:
        return None

    return {
        "assignments": assignments,
        "child_centroids": child_centroids,
        "child_coverages": child_coverages,
    }


def refine_impure_clusters(turns: list[dict]) -> tuple[list[dict], dict[str, dict], dict[str, dict]]:
    base_prototypes = build_cluster_prototypes(turns)
    turns_by_label: dict[str, list[dict]] = defaultdict(list)
    for turn in turns:
        turns_by_label[turn["speaker_label"]].append(turn)

    refined_turns: list[dict] = []
    cluster_split_lineage: dict[str, dict] = {}
    for label, label_turns in turns_by_label.items():
        prototype = base_prototypes.get(label, {})
        impure = (
            float(prototype.get("purity") or 0.0) < CLUSTER_IMPURE_PURITY
            and int(prototype.get("coverage_ms") or 0) > CLUSTER_IMPURE_COVERAGE_MS
        )
        split_payload = split_cluster_by_embeddings(label_turns) if impure else None
        if split_payload is not None:
            for turn in label_turns:
                child_index = split_payload["assignments"].get(turn["turn_id"], 0)
                child_label = f"{label}#{'ab'[child_index]}"
                refined_turns.append(
                    {
                        **turn,
                        "base_speaker_label": label,
                        "speaker_label": child_label,
                        "cluster_lineage": child_label,
                    }
                )
                cluster_split_lineage[child_label] = {
                    "parent_label": label,
                    "split_applied": True,
                    "impure_parent": True,
                }
            continue

        for turn in label_turns:
            refined_turns.append(
                {
                    **turn,
                    "base_speaker_label": label,
                    "cluster_lineage": label,
                }
            )
        cluster_split_lineage[label] = {
            "parent_label": label,
            "split_applied": False,
            "impure_parent": impure,
        }

    refined_prototypes = build_cluster_prototypes(refined_turns)
    for label, prototype in refined_prototypes.items():
        lineage = cluster_split_lineage.get(label, {})
        prototype["base_label"] = lineage.get("parent_label", prototype.get("base_label", label))
        prototype["split_applied"] = bool(lineage.get("split_applied"))
        prototype["impure"] = bool(lineage.get("impure_parent")) and not bool(lineage.get("split_applied"))
    return refined_turns, refined_prototypes, cluster_split_lineage


def parse_metadata_json(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def resolve_pyannote_model_reference(path_env_name: str, name_env_name: str, default_reference: str) -> str:
    explicit_path = (os.getenv(path_env_name) or "").strip()
    if explicit_path:
        return explicit_path
    explicit_name = (os.getenv(name_env_name) or "").strip()
    if explicit_name:
        return explicit_name
    return default_reference


def pyannote_auth_token() -> str | None:
    for env_name in ("PYANNOTE_AUTH_TOKEN", "HUGGINGFACE_HUB_TOKEN", "HF_TOKEN"):
        value = (os.getenv(env_name) or "").strip()
        if value:
            return value
    return None


def load_pyannote_diarization_pipeline():
    configure_runtime_environment()
    configure_torch_checkpoint_compatibility()
    try:
        import torch
        from pyannote.audio import Pipeline
    except Exception as exc:
        raise RuntimeError(
            "Pyannote diarization import edilemedi. "
            "`conda activate teams-bot && python -m pip install -r backend/requirements.txt` çalıştırın. "
            f"Ayrıntı: {exc}"
        ) from exc

    model_reference = resolve_pyannote_model_reference(
        "PYANNOTE_DIARIZATION_MODEL_PATH",
        "PYANNOTE_DIARIZATION_MODEL",
        PYANNOTE_DIARIZATION_MODEL_DEFAULT,
    )
    kwargs = {"cache_dir": str(runtime_cache_dir("pyannote"))}
    token = pyannote_auth_token()
    if token:
        kwargs["use_auth_token"] = token
    try:
        pipeline = Pipeline.from_pretrained(model_reference, **kwargs)
    except Exception as exc:
        raise RuntimeError(
            "Pyannote diarization modeli yüklenemedi. "
            "Local model kullanmak için `PYANNOTE_DIARIZATION_MODEL_PATH`, "
            "remote model için `PYANNOTE_AUTH_TOKEN`/`HF_TOKEN` sağlayın. "
            f"Ayrıntı: {exc}"
        ) from exc
    if pipeline is None:
        raise RuntimeError(
            "Pyannote diarization modeli indirilemedi. "
            f"`{model_reference}` için Hugging Face erişimi ve model koşullarının kabul edilmiş olması gerekiyor."
        )
    try:
        pipeline.to(torch.device("cpu"))
    except Exception:
        pass
    return pipeline


def load_pyannote_embedding_inference():
    configure_runtime_environment()
    configure_torch_checkpoint_compatibility()
    try:
        import torch
        from pyannote.audio import Audio, Inference, Model
    except Exception as exc:
        raise RuntimeError(
            "Pyannote embedding import edilemedi. "
            "`conda activate teams-bot && python -m pip install -r backend/requirements.txt` çalıştırın. "
            f"Ayrıntı: {exc}"
        ) from exc

    model_reference = resolve_pyannote_model_reference(
        "PYANNOTE_EMBEDDING_MODEL_PATH",
        "PYANNOTE_EMBEDDING_MODEL",
        PYANNOTE_EMBEDDING_MODEL_DEFAULT,
    )
    kwargs = {"cache_dir": str(runtime_cache_dir("pyannote"))}
    token = pyannote_auth_token()
    if token:
        kwargs["use_auth_token"] = token
    try:
        model = Model.from_pretrained(model_reference, **kwargs)
    except Exception as exc:
        raise RuntimeError(
            "Pyannote embedding modeli yüklenemedi. "
            "Local model kullanmak için `PYANNOTE_EMBEDDING_MODEL_PATH`, "
            "remote model için `PYANNOTE_AUTH_TOKEN`/`HF_TOKEN` sağlayın. "
            f"Ayrıntı: {exc}"
        ) from exc
    if model is None:
        raise RuntimeError(
            "Pyannote embedding modeli indirilemedi. "
            f"`{model_reference}` için Hugging Face erişimi ve model koşullarının kabul edilmiş olması gerekiyor."
        )
    try:
        inference = Inference(model, window="whole", device=torch.device("cpu"))
    except Exception as exc:
        raise RuntimeError(f"Pyannote embedding inference başlatılamadı: {exc}") from exc
    return inference, Audio()


def merge_diarized_turns(raw_turns: list[dict]) -> list[dict]:
    ordered = sorted(raw_turns, key=lambda item: (item["start_offset_ms"], item["end_offset_ms"], item["speaker_label"]))
    merged: list[dict] = []
    current: dict | None = None
    for turn in ordered:
        if current is None:
            current = dict(turn)
            continue
        same_label = turn["speaker_label"] == current["speaker_label"]
        small_gap = turn["start_offset_ms"] <= current["end_offset_ms"] + DIARIZATION_MERGE_GAP_MS
        if same_label and small_gap:
            current["end_offset_ms"] = max(current["end_offset_ms"], turn["end_offset_ms"])
            continue
        if current["end_offset_ms"] - current["start_offset_ms"] >= MIN_DIARIZED_TURN_MS:
            merged.append(current)
        current = dict(turn)
    if current and current["end_offset_ms"] - current["start_offset_ms"] >= MIN_DIARIZED_TURN_MS:
        merged.append(current)

    for turn in merged:
        turn["duration_ms"] = turn["end_offset_ms"] - turn["start_offset_ms"]
        turn["overlap_flag"] = any(
            other["speaker_label"] != turn["speaker_label"]
            and interval_overlap_ms(
                turn["start_offset_ms"],
                turn["end_offset_ms"],
                other["start_offset_ms"],
                other["end_offset_ms"],
            ) > 0
            for other in merged
        )
    for index, turn in enumerate(merged, start=1):
        turn["turn_id"] = f"turn-{index:04d}"
    return merged


def compute_waveform_speech_ratio(waveform, sample_rate: int) -> float:
    if waveform is None or sample_rate <= 0:
        return 0.0
    try:
        import torch
    except Exception:
        return 1.0
    samples = waveform.float()
    if samples.ndim > 1:
        samples = samples.mean(dim=0)
    samples = samples.flatten()
    frame_size = max(int(sample_rate * 0.03), 1)
    usable_frames = samples.numel() // frame_size
    if usable_frames <= 0:
        return 1.0
    frames = samples[: usable_frames * frame_size].reshape(usable_frames, frame_size)
    rms = torch.sqrt(torch.mean(frames * frames, dim=1))
    if rms.numel() == 0:
        return 0.0
    median_rms = float(torch.median(rms).item())
    threshold = max(0.01, median_rms * 0.75)
    speech_frames = int(torch.count_nonzero(rms >= threshold).item())
    return clamp_score(speech_frames / int(rms.numel()))


def diarize_speaker_turns(source_audio_path: Path) -> list[dict]:
    pipeline = load_pyannote_diarization_pipeline()
    try:
        diarization = pipeline(str(source_audio_path))
    except Exception as exc:
        raise RuntimeError(f"Pyannote diarization çalıştırılamadı: {exc}") from exc

    raw_turns: list[dict] = []
    for segment, _track, speaker_label in diarization.itertracks(yield_label=True):
        start_offset_ms = max(0, int(round(float(segment.start) * 1000)))
        end_offset_ms = max(0, int(round(float(segment.end) * 1000)))
        if end_offset_ms <= start_offset_ms:
            continue
        raw_turns.append(
            {
                "speaker_label": normalize_text(str(speaker_label)) or str(speaker_label),
                "start_offset_ms": start_offset_ms,
                "end_offset_ms": end_offset_ms,
            }
        )
    return merge_diarized_turns(raw_turns)


def attach_embeddings_to_turns(turns: list[dict], source_audio_path: Path) -> list[dict]:
    if not turns:
        return []
    inference, audio_helper = load_pyannote_embedding_inference()
    try:
        from pyannote.core import Segment
    except Exception as exc:
        raise RuntimeError(f"pyannote Segment import edilemedi: {exc}") from exc

    enriched: list[dict] = []
    for turn in turns:
        segment = Segment(turn["start_offset_ms"] / 1000.0, turn["end_offset_ms"] / 1000.0)
        waveform, sample_rate = audio_helper.crop(str(source_audio_path), segment, mode="pad")
        embedding = normalize_embedding_vector(
            inference({"waveform": waveform, "sample_rate": sample_rate})
        )
        enriched.append(
            {
                **turn,
                "embedding": embedding,
                "speech_ratio": compute_waveform_speech_ratio(waveform, sample_rate),
            }
        )
    return enriched


def attach_dom_priors_to_turns(turns: list[dict], activity_rows: list[sqlite3.Row]) -> tuple[list[dict], dict[int, float]]:
    activity_windows = normalize_speaker_activity_events(activity_rows)
    global_totals: dict[int, float] = defaultdict(float)
    enriched_turns: list[dict] = []
    for turn in turns:
        duration_ms = max(int(turn["duration_ms"]), 1)
        ui_scores_by_participant: dict[int, float] = defaultdict(float)
        simultaneous_claim_count = 0
        signal_density_by_participant: dict[int, float] = defaultdict(float)
        overlapping_participants: set[int] = set()
        turn_conflicted = False
        for window in activity_windows:
            participant_id = int(window["participant_id"])
            overlap_ms = interval_overlap_ms(
                turn["start_offset_ms"],
                turn["end_offset_ms"],
                int(window["start_offset_ms"] or 0),
                int(window["end_offset_ms"] or 0),
            )
            if overlap_ms <= 0:
                continue
            overlapping_participants.add(participant_id)
            overlap_ratio = overlap_ms / duration_ms
            window_confidence = clamp_score(float(window.get("confidence") or 0.0))
            support_count = max(int(window.get("support_count") or 1), 1)
            density_bonus = min(0.10, (support_count - 1) * 0.03)
            event_score = clamp_score((overlap_ratio * window_confidence) + density_bonus)
            ui_scores_by_participant[participant_id] += event_score
            signal_density_by_participant[participant_id] += support_count
            global_totals[participant_id] += overlap_ms * window_confidence
            simultaneous_claim_count = max(simultaneous_claim_count, len(overlapping_participants))
            turn_conflicted = turn_conflicted or bool(window.get("conflicted_claim"))

        claimant_count = len(overlapping_participants)
        simultaneous_claim_count = max(simultaneous_claim_count, claimant_count)
        normalized_priors = {participant_id: clamp_score(score) for participant_id, score in ui_scores_by_participant.items()}
        enriched_turns.append(
            {
                **turn,
                "ui_local_evidence_by_participant": dict(normalized_priors),
                "local_dom_prior_by_participant": dict(normalized_priors),
                "conflicted_claim": turn_conflicted,
                "simultaneous_claim_count": simultaneous_claim_count,
                "ui_signal_density_by_participant": {
                    participant_id: float(value)
                    for participant_id, value in signal_density_by_participant.items()
                },
            }
        )

    return enriched_turns, normalize_score_map(global_totals)


def build_cluster_dom_priors(turns: list[dict]) -> dict[str, dict[int, float]]:
    cluster_support: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for turn in turns:
        duration_ms = max(int(turn.get("duration_ms") or 0), 1)
        for participant_id, score in turn.get("local_dom_prior_by_participant", {}).items():
            cluster_support[turn["speaker_label"]][int(participant_id)] += float(score) * duration_ms
    return {
        label: normalize_score_map(dict(support))
        for label, support in cluster_support.items()
    }


def select_clean_seed_turns(turns: list[dict]) -> dict[int, list[dict]]:
    seeds_by_participant: dict[int, list[dict]] = defaultdict(list)
    for turn in turns:
        if turn["duration_ms"] < MIN_SEED_TURN_MS:
            continue
        if float(turn.get("speech_ratio") or 0.0) < MIN_SEED_SPEECH_RATIO:
            continue
        if turn.get("overlap_flag"):
            continue
        if not turn.get("embedding"):
            continue
        ranked = sorted(
            turn.get("local_dom_prior_by_participant", {}).items(),
            key=lambda item: (item[1], -item[0]),
            reverse=True,
        )
        if not ranked:
            continue
        best_participant_id, best_prior = ranked[0]
        second_prior = ranked[1][1] if len(ranked) > 1 else 0.0
        if best_prior < MIN_SEED_PRIOR or (best_prior - second_prior) < MIN_SEED_MARGIN:
            continue
        seeds_by_participant[int(best_participant_id)].append(
            {
                **turn,
                "seed_prior_score": float(best_prior),
                "seed_margin": float(best_prior - second_prior),
            }
        )
    return seeds_by_participant


def select_cluster_bootstrap_seed_turns(
    turns: list[dict],
    cluster_dom_priors: dict[str, dict[int, float]],
    participant_ids: list[int],
    existing_profiles: dict[int, dict] | None = None,
) -> dict[int, list[dict]]:
    existing_profiles = existing_profiles or {}
    missing_participants = {int(participant_id) for participant_id in participant_ids if int(participant_id) not in existing_profiles}
    if not missing_participants:
        return {}

    cluster_owners: dict[str, tuple[int, float, float]] = {}
    for label, priors in cluster_dom_priors.items():
        ranked = sorted(priors.items(), key=lambda item: (item[1], -item[0]), reverse=True)
        if not ranked:
            continue
        best_participant_id, best_prior = ranked[0]
        second_prior = ranked[1][1] if len(ranked) > 1 else 0.0
        if int(best_participant_id) not in missing_participants:
            continue
        if best_prior < CLUSTER_BOOTSTRAP_PRIOR or (best_prior - second_prior) < CLUSTER_BOOTSTRAP_MARGIN:
            continue
        cluster_owners[label] = (int(best_participant_id), float(best_prior), float(best_prior - second_prior))

    seeds_by_participant: dict[int, list[dict]] = defaultdict(list)
    for turn in turns:
        owner = cluster_owners.get(turn["speaker_label"])
        if owner is None:
            continue
        participant_id, cluster_prior, cluster_margin = owner
        if turn["duration_ms"] < RELAXED_SEED_TURN_MS:
            continue
        if float(turn.get("speech_ratio") or 0.0) < RELAXED_SEED_SPEECH_RATIO:
            continue
        if turn.get("overlap_flag"):
            continue
        if not turn.get("embedding"):
            continue
        ranked = sorted(
            turn.get("local_dom_prior_by_participant", {}).items(),
            key=lambda item: (item[1], -item[0]),
            reverse=True,
        )
        if not ranked:
            continue
        best_participant_id, best_prior = ranked[0]
        second_prior = ranked[1][1] if len(ranked) > 1 else 0.0
        if int(best_participant_id) != participant_id:
            continue
        if best_prior < RELAXED_SEED_PRIOR or (best_prior - second_prior) < RELAXED_SEED_MARGIN:
            continue
        seeds_by_participant[participant_id].append(
            {
                **turn,
                "seed_prior_score": float((best_prior * 0.6) + (cluster_prior * 0.4)),
                "seed_margin": float(max(best_prior - second_prior, cluster_margin)),
                "cluster_bootstrap_prior": cluster_prior,
            }
        )
    return seeds_by_participant


def select_local_bootstrap_seed_turns(
    turns: list[dict],
    participant_ids: list[int],
    existing_profiles: dict[int, dict] | None = None,
) -> dict[int, list[dict]]:
    existing_profiles = existing_profiles or {}
    missing_participants = {int(participant_id) for participant_id in participant_ids if int(participant_id) not in existing_profiles}
    if not missing_participants:
        return {}

    seeds_by_participant: dict[int, list[dict]] = defaultdict(list)
    for turn in turns:
        if turn["duration_ms"] < RELAXED_SEED_TURN_MS:
            continue
        if float(turn.get("speech_ratio") or 0.0) < RELAXED_SEED_SPEECH_RATIO:
            continue
        if turn.get("overlap_flag"):
            continue
        if not turn.get("embedding"):
            continue
        ranked = sorted(
            turn.get("local_dom_prior_by_participant", {}).items(),
            key=lambda item: (item[1], -item[0]),
            reverse=True,
        )
        if not ranked:
            continue
        best_participant_id, best_prior = ranked[0]
        second_prior = ranked[1][1] if len(ranked) > 1 else 0.0
        best_participant_id = int(best_participant_id)
        if best_participant_id not in missing_participants:
            continue
        if best_prior < RELAXED_SEED_PRIOR or (best_prior - second_prior) < CLUSTER_BOOTSTRAP_MARGIN:
            continue
        seeds_by_participant[best_participant_id].append(
            {
                **turn,
                "seed_prior_score": float(best_prior),
                "seed_margin": float(best_prior - second_prior),
            }
        )
    return seeds_by_participant


def build_profile_from_turns(turns: list[dict], source_kind: str) -> dict | None:
    candidates = [turn for turn in turns if turn.get("embedding")]
    if not candidates:
        return None
    ordered = sorted(
        candidates,
        key=lambda item: (
            float(item.get("seed_prior_score") or item.get("best_score") or 0.0),
            int(item.get("duration_ms") or 0),
        ),
        reverse=True,
    )[:MAX_PROFILE_SEED_TURNS]
    centroid = average_embeddings([turn["embedding"] for turn in ordered])
    if centroid is None:
        return None
    similarities = [cosine_similarity_vectors(turn["embedding"], centroid) for turn in ordered]
    median_similarity = statistics.median(similarities) if similarities else 0.0
    filtered_turns = [
        turn
        for turn, similarity in zip(ordered, similarities)
        if similarity >= median_similarity - PROFILE_OUTLIER_TOLERANCE
    ]
    centroid = average_embeddings([turn["embedding"] for turn in filtered_turns]) or centroid
    quality_scores = [cosine_similarity_vectors(turn["embedding"], centroid) for turn in filtered_turns]
    return {
        "participant_id": int(filtered_turns[0]["participant_id"] if "participant_id" in filtered_turns[0] else 0),
        "source_kind": source_kind,
        "seed_turn_ids": [turn["turn_id"] for turn in filtered_turns],
        "seed_count": len(filtered_turns),
        "coverage_ms": sum(int(turn["duration_ms"]) for turn in filtered_turns),
        "quality": (sum(quality_scores) / len(quality_scores)) if quality_scores else 0.0,
        "centroid": centroid,
    }


def build_participant_profiles(turns: list[dict], participant_ids: list[int] | None = None) -> dict[int, dict]:
    profiles: dict[int, dict] = {}
    for participant_id, seed_turns in select_clean_seed_turns(turns).items():
        profile = build_profile_from_turns(
            [{**turn, "participant_id": participant_id} for turn in seed_turns],
            source_kind="clean_seed",
        )
        if profile is not None:
            profiles[int(participant_id)] = profile
    if participant_ids:
        cluster_dom_priors = build_cluster_dom_priors(turns)
        for participant_id, seed_turns in select_cluster_bootstrap_seed_turns(
            turns,
            cluster_dom_priors,
            participant_ids,
            profiles,
        ).items():
            profile = build_profile_from_turns(
                [{**turn, "participant_id": participant_id} for turn in seed_turns],
                source_kind="cluster_bootstrap",
            )
            if profile is not None:
                profiles[int(participant_id)] = profile
        for participant_id, seed_turns in select_local_bootstrap_seed_turns(
            turns,
            participant_ids,
            profiles,
        ).items():
            profile = build_profile_from_turns(
                [{**turn, "participant_id": participant_id} for turn in seed_turns],
                source_kind="local_dom_bootstrap",
            )
            if profile is not None:
                profiles[int(participant_id)] = profile
    return profiles


def expand_missing_participant_profiles(
    participant_ids: list[int],
    turn_results: list[dict],
    participant_profiles: dict[int, dict],
    cluster_dom_priors: dict[str, dict[int, float]],
) -> dict[int, dict]:
    expanded = dict(participant_profiles)
    for participant_id, seed_turns in select_cluster_bootstrap_seed_turns(
        turn_results,
        cluster_dom_priors,
        participant_ids,
        expanded,
    ).items():
        profile = build_profile_from_turns(
            [{**turn, "participant_id": participant_id} for turn in seed_turns],
            source_kind="cluster_bootstrap",
        )
        if profile is not None:
            expanded[int(participant_id)] = profile
    for participant_id, seed_turns in select_local_bootstrap_seed_turns(
        turn_results,
        participant_ids,
        expanded,
    ).items():
        profile = build_profile_from_turns(
            [{**turn, "participant_id": participant_id} for turn in seed_turns],
            source_kind="local_dom_bootstrap",
        )
        if profile is not None:
            expanded[int(participant_id)] = profile

    for participant_id in participant_ids:
        if participant_id in expanded:
            continue
        candidates = [
            turn
            for turn in turn_results
            if turn.get("participant_id") == participant_id
            and turn.get("embedding")
            and turn["duration_ms"] >= MIN_SEED_TURN_MS
            and not turn.get("overlap_flag")
            and float(turn.get("best_score") or 0.0) >= 0.72
            and float(turn.get("score_margin") or 0.0) >= 0.15
        ]
        profile = build_profile_from_turns(candidates, source_kind="iterative_bootstrap")
        if profile is not None:
            profile["participant_id"] = participant_id
            expanded[participant_id] = profile
    return expanded


def score_turn_candidates(
    turn: dict,
    participant_ids: list[int],
    participant_profiles: dict[int, dict],
    cluster_prototypes: dict[str, dict],
    cluster_dom_prior_by_participant: dict[int, float],
    global_dom_priors: dict[int, float],
) -> dict[int, dict]:
    scores: dict[int, dict] = {}
    local_dom_scores = turn.get("local_dom_prior_by_participant", {})
    cluster_prototype = cluster_prototypes.get(turn["speaker_label"], {})
    cluster_is_impure = bool(cluster_prototype.get("impure"))
    for participant_id in participant_ids:
        profile = participant_profiles.get(participant_id)
        cluster_profile_similarity = cosine_similarity_vectors(
            cluster_prototype.get("centroid"),
            profile.get("centroid") if profile else None,
        )
        turn_profile_similarity = cosine_similarity_vectors(
            turn.get("embedding"),
            profile.get("centroid") if profile else None,
        )
        cluster_level_prior = clamp_score(cluster_dom_prior_by_participant.get(participant_id, 0.0))
        local_dom_prior = clamp_score(local_dom_scores.get(participant_id, 0.0))
        if turn.get("conflicted_claim"):
            local_dom_prior = min(local_dom_prior, 0.35)
        dom_score = clamp_score((0.75 * cluster_level_prior) + (0.25 * local_dom_prior))
        if profile:
            audio_similarity = turn_profile_similarity if cluster_is_impure else cluster_profile_similarity
        else:
            audio_similarity = 0.0
        if profile and audio_similarity > 0:
            if cluster_is_impure:
                final_score = (
                    (0.60 * audio_similarity)
                    + (0.15 * cluster_level_prior)
                    + (0.25 * local_dom_prior)
                )
            else:
                final_score = (
                    (0.60 * audio_similarity)
                    + (0.25 * cluster_level_prior)
                    + (0.15 * local_dom_prior)
                )
        elif cluster_is_impure:
            final_score = (
                (0.40 * cluster_level_prior)
                + (0.60 * local_dom_prior)
            )
        else:
            final_score = (0.75 * cluster_level_prior) + (0.25 * local_dom_prior)
        if turn.get("overlap_flag"):
            final_score -= 0.10
        if final_score <= 0 and global_dom_priors:
            final_score = 0.10 * clamp_score(global_dom_priors.get(participant_id, 0.0))
        scores[participant_id] = {
            "cluster_profile_similarity": cluster_profile_similarity,
            "turn_profile_similarity": turn_profile_similarity,
            "audio_similarity": audio_similarity,
            "cluster_level_prior": cluster_level_prior,
            "local_dom_prior": local_dom_prior,
            "dom_score": dom_score,
            "final_score": clamp_score(final_score),
            "used_profile": bool(profile and audio_similarity > 0),
            "profile_source_kind": (profile or {}).get("source_kind"),
        }
    return scores


def pick_best_participant_from_scores(scores_by_participant: dict[int, dict]) -> tuple[int | None, float, float]:
    if not scores_by_participant:
        return None, 0.0, 0.0
    ranked = sorted(
        scores_by_participant.items(),
        key=lambda item: (item[1]["final_score"], -item[0]),
        reverse=True,
    )
    best_participant_id, best_payload = ranked[0]
    second_score = ranked[1][1]["final_score"] if len(ranked) > 1 else 0.0
    best_final_score = float(best_payload["final_score"])
    best_dom_signal = max(
        float(best_payload.get("dom_score") or 0.0),
        float(best_payload.get("cluster_level_prior") or 0.0),
        float(best_payload.get("local_dom_prior") or 0.0),
    )
    if not bool(best_payload.get("used_profile")):
        if best_final_score < MIN_UNRESOLVED_ASSIGNMENT_SCORE or best_dom_signal < MIN_UNRESOLVED_DOM_SIGNAL:
            return None, best_final_score, float(second_score)
    return int(best_participant_id), float(best_payload["final_score"]), float(second_score)


def resolve_turn_assignment(
    turn: dict,
    scores_by_participant: dict[int, dict],
    cluster_prototypes: dict[str, dict],
) -> tuple[int | None, float, float, list[str]]:
    participant_id, best_score, second_score = pick_best_participant_from_scores(scores_by_participant)
    ranked = sorted(
        scores_by_participant.items(),
        key=lambda item: (item[1]["final_score"], -item[0]),
        reverse=True,
    )
    best_payload = ranked[0][1] if ranked else {}
    cluster_prototype = cluster_prototypes.get(turn["speaker_label"], {})
    best_dom_signal = max(
        float(best_payload.get("dom_score") or 0.0),
        float(best_payload.get("cluster_level_prior") or 0.0),
        float(best_payload.get("local_dom_prior") or 0.0),
    )

    unresolved_reasons: list[str] = []
    if best_score < MIN_UNRESOLVED_ASSIGNMENT_SCORE:
        unresolved_reasons.append("weak_best_score")
    if not bool(best_payload.get("used_profile")) and best_dom_signal < MIN_UNRESOLVED_DOM_SIGNAL:
        unresolved_reasons.append("weak_dom_without_profile")
    if bool(turn.get("conflicted_claim")) and (best_score - second_score) < 0.08:
        unresolved_reasons.append("conflicted_low_margin")
    if bool(cluster_prototype.get("impure")) and best_score < CLUSTER_CONFIRM_SCORE:
        unresolved_reasons.append("impure_cluster_unconfirmed")

    if participant_id is None or unresolved_reasons:
        return None, float(best_score), float(second_score), unresolved_reasons or ["insufficient_evidence"]
    return participant_id, float(best_score), float(second_score), []


def collect_segment_turn_matches(segment: dict, assigned_turns: list[dict]) -> dict:
    segment_duration_ms = max(segment["end_offset_ms"] - segment["start_offset_ms"], 1)
    matches: list[dict] = []
    nearest_match: dict | None = None
    nearest_distance_ms: int | None = None
    for turn in assigned_turns:
        overlap_ms = interval_overlap_ms(
            segment["start_offset_ms"],
            segment["end_offset_ms"],
            turn["start_offset_ms"],
            turn["end_offset_ms"],
        )
        distance_ms = 0
        if overlap_ms <= 0:
            distance_ms = min(
                abs(segment["start_offset_ms"] - turn["end_offset_ms"]),
                abs(segment["end_offset_ms"] - turn["start_offset_ms"]),
            )
            if nearest_distance_ms is None or distance_ms < nearest_distance_ms:
                nearest_distance_ms = distance_ms
                nearest_match = {
                    "turn": turn,
                    "overlap_ms": 0,
                    "overlap_ratio": 0.0,
                    "distance_ms": distance_ms,
                }
            continue
        matches.append(
            {
                "turn": turn,
                "overlap_ms": overlap_ms,
                "overlap_ratio": overlap_ms / segment_duration_ms,
                "distance_ms": distance_ms,
            }
        )

    if not matches and nearest_match is not None:
        matches = [nearest_match]
    matches = sorted(
        matches,
        key=lambda item: (
            item["turn"]["start_offset_ms"],
            item["turn"]["end_offset_ms"],
            item["turn"]["turn_id"],
        ),
    )
    significant_matches = [
        item
        for item in matches
        if item["overlap_ratio"] >= SEGMENT_SPLIT_MIN_OVERLAP_RATIO
        or item["overlap_ms"] >= SEGMENT_SPLIT_MIN_OVERLAP_MS
    ]
    if not significant_matches and matches:
        significant_matches = [max(matches, key=lambda item: (item["overlap_ms"], -item["distance_ms"]))]
    dominant_match = max(matches, key=lambda item: (item["overlap_ratio"], item["overlap_ms"]), default=None)
    return {
        "matches": matches,
        "significant_matches": significant_matches,
        "dominant_match": dominant_match,
    }


def choose_best_turn_match(window_start_ms: int, window_end_ms: int, matches: list[dict]) -> tuple[dict, bool]:
    best_match: dict | None = None
    best_overlap_ms = -1
    best_distance_ms: int | None = None
    window_duration_ms = max(window_end_ms - window_start_ms, 1)
    for match in matches:
        turn = match["turn"]
        overlap_ms = interval_overlap_ms(
            window_start_ms,
            window_end_ms,
            turn["start_offset_ms"],
            turn["end_offset_ms"],
        )
        if overlap_ms > best_overlap_ms:
            best_overlap_ms = overlap_ms
            best_match = match
            best_distance_ms = 0
            continue
        if overlap_ms <= 0:
            distance_ms = min(
                abs(window_start_ms - turn["end_offset_ms"]),
                abs(window_end_ms - turn["start_offset_ms"]),
            )
            if best_distance_ms is None or distance_ms < best_distance_ms:
                best_match = match
                best_distance_ms = distance_ms
    if best_match is None:
        raise RuntimeError("Segment penceresi için speaker turn bulunamadı.")
    return best_match, (best_overlap_ms / window_duration_ms) < 0.35


def build_fragment_text(words: list[dict]) -> str:
    return normalize_text(" ".join(word["text"] for word in words))


def split_segment_by_words(segment: dict, matches: list[dict]) -> list[dict]:
    words = [word for word in segment.get("words") or [] if normalize_text(word.get("text"))]
    if not words:
        return []

    fragments: list[dict] = []
    current: dict | None = None
    for word in words:
        best_match, low_alignment = choose_best_turn_match(
            int(word["start_offset_ms"]),
            int(word["end_offset_ms"]),
            matches,
        )
        turn_id = best_match["turn"]["turn_id"]
        if current is None or current["turn"]["turn_id"] != turn_id:
            current = {
                "turn": best_match["turn"],
                "words": [word],
                "low_alignment": low_alignment,
                "unresolved_reasons": [],
                "split_source": "word_timestamps",
            }
            fragments.append(current)
            continue
        current["words"].append(word)
        current["low_alignment"] = current["low_alignment"] or low_alignment

    normalized_fragments: list[dict] = []
    for index, fragment in enumerate(fragments, start=1):
        text = build_fragment_text(fragment["words"])
        if not text:
            continue
        normalized_fragments.append(
            {
                "fragment_index": index,
                "turn": fragment["turn"],
                "text": text,
                "raw_text": text,
                "start_offset_ms": int(fragment["words"][0]["start_offset_ms"]),
                "end_offset_ms": int(fragment["words"][-1]["end_offset_ms"]),
                "split_source": fragment["split_source"],
                "low_alignment": fragment["low_alignment"],
                "needs_review": len(fragments) > 1,
                "unresolved_reasons": fragment["unresolved_reasons"],
            }
        )
    return normalized_fragments


def allocate_turn_token_counts(token_count: int, matches: list[dict]) -> list[int]:
    if token_count <= 0:
        return [0 for _ in matches]
    if len(matches) == 1:
        return [token_count]
    base_counts = [1] * min(token_count, len(matches))
    if len(base_counts) < len(matches):
        base_counts.extend([0] * (len(matches) - len(base_counts)))
    remaining = token_count - sum(base_counts)
    if remaining <= 0:
        return base_counts

    weights = [max(int(match["overlap_ms"]), SEGMENT_SPLIT_MIN_OVERLAP_MS) for match in matches]
    total_weight = sum(weights) or len(matches)
    fractional: list[tuple[float, int]] = []
    for index, weight in enumerate(weights):
        exact = remaining * (weight / total_weight)
        assigned = int(exact)
        base_counts[index] += assigned
        fractional.append((exact - assigned, index))
    leftover = token_count - sum(base_counts)
    for _fraction, index in sorted(fractional, reverse=True):
        if leftover <= 0:
            break
        base_counts[index] += 1
        leftover -= 1
    return base_counts


def split_segment_without_words(segment: dict, matches: list[dict]) -> list[dict]:
    tokens = normalize_text(segment.get("text") or "").split()
    if not tokens:
        return []
    token_counts = allocate_turn_token_counts(len(tokens), matches)
    segment_duration_ms = max(segment["end_offset_ms"] - segment["start_offset_ms"], 1)
    fragments: list[dict] = []
    token_cursor = 0
    for index, (match, token_count) in enumerate(zip(matches, token_counts), start=1):
        if token_count <= 0:
            continue
        fragment_tokens = tokens[token_cursor : token_cursor + token_count]
        token_cursor += token_count
        if not fragment_tokens:
            continue
        time_start = segment["start_offset_ms"] + int(round(segment_duration_ms * ((token_cursor - token_count) / len(tokens))))
        time_end = segment["start_offset_ms"] + int(round(segment_duration_ms * (token_cursor / len(tokens))))
        fragments.append(
            {
                "fragment_index": index,
                "turn": match["turn"],
                "text": normalize_text(" ".join(fragment_tokens)),
                "raw_text": normalize_text(" ".join(fragment_tokens)),
                "start_offset_ms": max(segment["start_offset_ms"], time_start),
                "end_offset_ms": min(segment["end_offset_ms"], max(time_start + 1, time_end)),
                "split_source": "time_fallback",
                "low_alignment": True,
                "needs_review": True,
                "unresolved_reasons": ["no_word_timestamps_split"],
            }
        )
    return fragments


def best_score_details(score_map: dict[int, float]) -> tuple[int | None, float, float, float]:
    if not score_map:
        return None, 0.0, 0.0, 0.0
    ranked = sorted(score_map.items(), key=lambda item: (item[1], -item[0]), reverse=True)
    best_participant_id, best_score = ranked[0]
    second_score = float(ranked[1][1]) if len(ranked) > 1 else 0.0
    return int(best_participant_id), float(best_score), float(best_score - second_score), second_score


def build_caption_timeline(canonical_captions: list[dict], meeting_row: sqlite3.Row | None) -> list[dict]:
    if not canonical_captions:
        return []
    anchor_dt = None
    if meeting_row is not None:
        anchor_dt = parse_dt(meeting_row["audio_capture_started_at"] if "audio_capture_started_at" in meeting_row.keys() else None)
        if anchor_dt is None:
            anchor_dt = parse_dt(meeting_row["joined_at"] if "joined_at" in meeting_row.keys() else None)
    if anchor_dt is None:
        return []

    entries: list[dict] = []
    for row in canonical_captions:
        started_at = row.get("started_at")
        finalized_at = row.get("finalized_at")
        if isinstance(started_at, str):
            started_at = parse_dt(started_at)
        if isinstance(finalized_at, str):
            finalized_at = parse_dt(finalized_at)
        if started_at is None and finalized_at is None:
            continue
        start_dt = started_at or finalized_at
        end_dt = finalized_at or started_at
        if start_dt is None or end_dt is None:
            continue
        start_offset_ms = max(0, int(round((start_dt - anchor_dt).total_seconds() * 1000)))
        end_offset_ms = max(start_offset_ms + 1, int(round((end_dt - anchor_dt).total_seconds() * 1000)))
        entries.append(
            {
                "sequence_no": int(row.get("sequence_no") or len(entries) + 1),
                "speaker": normalize_text(row.get("speaker") or row.get("speaker_name") or "Unknown") or "Unknown",
                "text": normalize_text(row.get("text") or ""),
                "start_offset_ms": start_offset_ms,
                "end_offset_ms": end_offset_ms,
            }
        )
    return entries


def select_caption_event_for_segment(segment: dict, caption_entries: list[dict]) -> dict | None:
    best_entry: dict | None = None
    best_key = None
    for entry in caption_entries:
        overlap_ms = interval_overlap_ms(
            segment["start_offset_ms"],
            segment["end_offset_ms"],
            entry["start_offset_ms"],
            entry["end_offset_ms"],
        )
        distance_ms = 0
        if overlap_ms <= 0:
            distance_ms = min(
                abs(segment["start_offset_ms"] - entry["end_offset_ms"]),
                abs(segment["end_offset_ms"] - entry["start_offset_ms"]),
            )
        text_score = token_sequence_match_ratio(segment.get("text"), entry.get("text"))
        if overlap_ms <= 0 and distance_ms > 2500 and text_score < 0.45:
            continue
        ranking_key = (
            1 if overlap_ms > 0 else 0,
            text_score,
            overlap_ms,
            -distance_ms,
            -int(entry.get("sequence_no") or 0),
        )
        if best_key is None or ranking_key > best_key:
            best_key = ranking_key
            best_entry = entry
    return best_entry


def compute_caption_name_scores(
    caption_speaker: str | None,
    participant_rows: list[sqlite3.Row],
) -> tuple[dict[int, float], int | None, str]:
    scores: dict[int, float] = {}
    visible_rows = [
        row
        for row in participant_rows
        if not bool(row["is_bot"]) and not is_roster_heading_name(row["display_name"])
    ]
    for row in visible_rows:
        scores[int(row["id"])] = 0.0

    normalized_caption = normalize_participant_name(caption_speaker)
    if not normalized_caption or normalized_caption.casefold() == "unknown":
        return scores, None, "none"

    exact_matches: list[int] = []
    fuzzy_ranked: list[tuple[float, int]] = []
    for row in visible_rows:
        participant_id = int(row["id"])
        display_name = normalize_participant_name(row["display_name"])
        if not display_name:
            continue
        if display_name.casefold() == normalized_caption.casefold():
            exact_matches.append(participant_id)
            scores[participant_id] = 1.0
            continue
        ratio = sequence_ratio(display_name, normalized_caption)
        token_ratio = token_sequence_match_ratio(display_name, normalized_caption)
        partial_match = (
            len(display_name) >= 4
            and len(normalized_caption) >= 4
            and (
                display_name.casefold() in normalized_caption.casefold()
                or normalized_caption.casefold() in display_name.casefold()
            )
        )
        fuzzy_score = max(ratio, token_ratio, 0.60 if partial_match else 0.0)
        if fuzzy_score > 0.0:
            scores[participant_id] = clamp_score(fuzzy_score)
            fuzzy_ranked.append((clamp_score(fuzzy_score), participant_id))

    if len(exact_matches) == 1:
        return scores, exact_matches[0], "exact"
    if len(exact_matches) > 1:
        for participant_id in exact_matches:
            scores[participant_id] = min(scores[participant_id], 0.35) or 0.35
        return scores, None, "ambiguous"

    fuzzy_ranked.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    if not fuzzy_ranked:
        return scores, None, "ambiguous"

    best_score, best_participant_id = fuzzy_ranked[0]
    second_score = fuzzy_ranked[1][0] if len(fuzzy_ranked) > 1 else 0.0
    if (
        best_score >= HYBRID_CAPTION_FUZZY_UNIQUE_FLOOR
        and (best_score - second_score) >= HYBRID_CAPTION_FUZZY_SEPARATION_MARGIN
    ):
        return scores, int(best_participant_id), "fuzzy"
    return scores, None, "ambiguous"


def compute_ui_activity_score(
    start_offset_ms: int,
    end_offset_ms: int,
    participant_id: int,
    activity_windows: list[dict],
) -> float:
    duration_ms = max(end_offset_ms - start_offset_ms, 1)
    total_score = 0.0
    proximity_score = 0.0
    for window in activity_windows:
        if int(window["participant_id"]) != int(participant_id):
            continue
        overlap_ms = interval_overlap_ms(
            start_offset_ms,
            end_offset_ms,
            int(window["start_offset_ms"]),
            int(window["end_offset_ms"]),
        )
        confidence = clamp_score(float(window.get("confidence") or 0.0))
        support_bonus = min(0.08, max(int(window.get("support_count") or 1) - 1, 0) * 0.02)
        if overlap_ms > 0:
            overlap_ratio = overlap_ms / duration_ms
            recency_bonus = 0.08 if abs(int(window["start_offset_ms"]) - start_offset_ms) <= HYBRID_UI_MERGE_GAP_MS else 0.0
            score = (overlap_ratio * confidence) + support_bonus + recency_bonus
            if window.get("conflicted_claim"):
                score *= 0.8
            total_score += score
            continue
        gap_ms = min(
            abs(start_offset_ms - int(window["end_offset_ms"])),
            abs(end_offset_ms - int(window["start_offset_ms"])),
        )
        if gap_ms <= HYBRID_UI_MERGE_GAP_MS:
            proximity_score = max(
                proximity_score,
                0.12 * confidence * (1.0 - (gap_ms / HYBRID_UI_MERGE_GAP_MS)),
            )
    return clamp_score(total_score + proximity_score)


def score_turn_local_evidence(
    turn: dict,
    participant_ids: list[int],
    participant_profiles: dict[int, dict],
    cluster_prototypes: dict[str, dict],
    ui_local_evidence_by_participant: dict[int, float],
) -> dict[int, dict]:
    cluster_prototype = cluster_prototypes.get(turn["speaker_label"], {})
    cluster_purity = clamp_score(float(cluster_prototype.get("purity") or 0.0))
    scores: dict[int, dict] = {}
    for participant_id in participant_ids:
        profile = participant_profiles.get(int(participant_id))
        cluster_profile_similarity = cosine_similarity_vectors(
            cluster_prototype.get("centroid"),
            profile.get("centroid") if profile else None,
        )
        turn_profile_similarity = cosine_similarity_vectors(
            turn.get("embedding"),
            profile.get("centroid") if profile else None,
        )
        if profile:
            if bool(cluster_prototype.get("impure")):
                audio_identity_score = (0.70 * turn_profile_similarity) + (0.30 * cluster_profile_similarity)
            else:
                audio_identity_score = (0.55 * cluster_profile_similarity) + (0.45 * turn_profile_similarity)
            audio_identity_score *= 0.65 + (0.35 * max(cluster_purity, float(turn.get("speech_ratio") or 0.0)))
        else:
            audio_identity_score = 0.0
        if turn.get("overlap_flag"):
            audio_identity_score *= 0.9
        scores[int(participant_id)] = {
            "audio_identity_score": clamp_score(audio_identity_score),
            "ui_activity_score": clamp_score(ui_local_evidence_by_participant.get(int(participant_id), 0.0)),
            "turn_profile_similarity": clamp_score(turn_profile_similarity),
            "cluster_profile_similarity": clamp_score(cluster_profile_similarity),
            "prototype_purity": cluster_purity,
        }
    return scores


def summarize_turn_local_evidence(turn: dict, candidate_scores: dict[int, dict]) -> dict:
    audio_map = {
        participant_id: float(payload.get("audio_identity_score") or 0.0)
        for participant_id, payload in candidate_scores.items()
    }
    ui_map = {
        participant_id: float(payload.get("ui_activity_score") or 0.0)
        for participant_id, payload in candidate_scores.items()
    }
    best_audio_participant_id, _best_audio_score, audio_margin, _audio_second = best_score_details(audio_map)
    best_ui_participant_id, _best_ui_score, ui_margin, _ui_second = best_score_details(ui_map)
    summary_scores: dict[str, dict] = {}
    for participant_id in sorted(candidate_scores):
        payload = candidate_scores[participant_id]
        summary_scores[str(participant_id)] = {
            "audio_identity_score": float(payload.get("audio_identity_score") or 0.0),
            "ui_activity_score": float(payload.get("ui_activity_score") or 0.0),
            "audio_margin": float(audio_margin),
            "ui_margin": float(ui_margin),
        }
    return {
        "turn_id": turn["turn_id"],
        "speaker_label": turn["speaker_label"],
        "start_offset_ms": int(turn["start_offset_ms"]),
        "end_offset_ms": int(turn["end_offset_ms"]),
        "best_audio_participant_id": best_audio_participant_id,
        "best_ui_participant_id": best_ui_participant_id,
        "candidate_scores": summary_scores,
        "audio_margin": float(audio_margin),
        "ui_margin": float(ui_margin),
    }


def build_initial_hybrid_profiles(turns: list[dict], participant_ids: list[int]) -> dict[int, dict]:
    seed_turns_by_participant: dict[int, list[dict]] = defaultdict(list)
    for turn in turns:
        if not turn.get("embedding"):
            continue
        if int(turn.get("duration_ms") or 0) < HYBRID_SEED_MIN_DURATION_MS:
            continue
        if float(turn.get("speech_ratio") or 0.0) < HYBRID_SEED_MIN_SPEECH_RATIO:
            continue
        if turn.get("overlap_flag") or turn.get("conflicted_claim"):
            continue
        ui_map = turn.get("ui_local_evidence_by_participant") or {}
        best_ui_participant_id, best_ui_score, ui_margin, _ = best_score_details(
            {int(participant_id): float(ui_map.get(int(participant_id), 0.0)) for participant_id in participant_ids}
        )
        if best_ui_participant_id is None:
            continue
        if best_ui_score < HYBRID_SEED_MIN_UI_ACTIVITY_SCORE or ui_margin < HYBRID_SEED_MIN_UI_MARGIN:
            continue
        seed_turns_by_participant[int(best_ui_participant_id)].append(
            {
                **turn,
                "participant_id": int(best_ui_participant_id),
                "seed_prior_score": float(best_ui_score),
                "best_score": float(best_ui_score),
            }
        )

    profiles: dict[int, dict] = {}
    for participant_id in participant_ids:
        profile = build_profile_from_turns(seed_turns_by_participant.get(int(participant_id), []), source_kind="ui_seed")
        if profile is None:
            continue
        profile["participant_id"] = int(participant_id)
        profiles[int(participant_id)] = profile

    if len(profiles) < len(participant_ids):
        bootstrap_profiles = build_participant_profiles(turns, participant_ids)
        for participant_id, profile in bootstrap_profiles.items():
            if profile is None:
                continue
            profile["participant_id"] = int(participant_id)
            profiles.setdefault(int(participant_id), profile)
    return profiles


def refresh_hybrid_participant_profiles(
    turns_by_id: dict[str, dict],
    segment_results: list[dict],
    participant_profiles: dict[int, dict],
) -> dict[int, dict]:
    refreshed = dict(participant_profiles)
    seed_turns_by_participant: dict[int, list[dict]] = defaultdict(list)
    for segment in segment_results:
        participant_id = segment.get("participant_id")
        if participant_id is None:
            continue
        if segment.get("speaker_resolution_status") == HYBRID_SPEAKER_STATUS_CONFIRMED:
            accepted = True
        else:
            accepted = (
                segment.get("speaker_resolution_status") in {
                    HYBRID_SPEAKER_STATUS_UI_PROVISIONAL,
                    HYBRID_SPEAKER_STATUS_AUDIO_PROVISIONAL,
                }
                and float(segment.get("assignment_confidence") or 0.0) >= HYBRID_STRONG_PROVISIONAL_REFRESH_FLOOR
            )
        if not accepted or bool(segment.get("multi_active_claim")):
            continue
        for turn_id in segment.get("source_turn_ids") or []:
            turn = turns_by_id.get(turn_id)
            if turn is None or not turn.get("embedding") or turn.get("overlap_flag"):
                continue
            candidate_payload = (turn.get("candidate_scores") or {}).get(int(participant_id), {})
            dominant_channel = "ui" if float(candidate_payload.get("ui_activity_score") or 0.0) >= float(candidate_payload.get("audio_identity_score") or 0.0) else "audio"
            dominant_margin = float(turn.get("ui_margin") or 0.0) if dominant_channel == "ui" else float(turn.get("audio_margin") or 0.0)
            dominant_score = float(candidate_payload.get("ui_activity_score") or 0.0) if dominant_channel == "ui" else float(candidate_payload.get("audio_identity_score") or 0.0)
            if dominant_channel == "ui":
                if dominant_score < HYBRID_SEED_MIN_UI_ACTIVITY_SCORE or dominant_margin < HYBRID_SEED_MIN_UI_MARGIN:
                    continue
            else:
                if dominant_score < HYBRID_SEED_MIN_AUDIO_IDENTITY_SCORE or dominant_margin < HYBRID_SEED_MIN_AUDIO_MARGIN:
                    continue
            seed_turns_by_participant[int(participant_id)].append(
                {
                    **turn,
                    "participant_id": int(participant_id),
                    "seed_prior_score": float(segment.get("assignment_confidence") or 0.0),
                    "best_score": float(segment.get("assignment_confidence") or 0.0),
                }
            )

    for participant_id, seed_turns in seed_turns_by_participant.items():
        profile = build_profile_from_turns(seed_turns, source_kind="segment_refresh")
        if profile is None:
            continue
        profile["participant_id"] = int(participant_id)
        existing = refreshed.get(int(participant_id))
        if existing is None or (
            float(profile.get("quality") or 0.0),
            int(profile.get("coverage_ms") or 0),
        ) >= (
            float(existing.get("quality") or 0.0),
            int(existing.get("coverage_ms") or 0),
        ):
            refreshed[int(participant_id)] = profile
    return refreshed


def score_turns_locally(
    turns: list[dict],
    participant_ids: list[int],
    participant_profiles: dict[int, dict],
    cluster_prototypes: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    scored_turns: list[dict] = []
    turn_local_summary: list[dict] = []
    for turn in turns:
        candidate_scores = score_turn_local_evidence(
            turn,
            participant_ids,
            participant_profiles,
            cluster_prototypes,
            turn.get("ui_local_evidence_by_participant") or {},
        )
        summary = summarize_turn_local_evidence(turn, candidate_scores)
        scored_turns.append(
            {
                **turn,
                "candidate_scores": candidate_scores,
                "best_audio_participant_id": summary["best_audio_participant_id"],
                "best_ui_participant_id": summary["best_ui_participant_id"],
                "audio_margin": summary["audio_margin"],
                "ui_margin": summary["ui_margin"],
            }
        )
        turn_local_summary.append(summary)
    return scored_turns, turn_local_summary


def collect_segment_candidates(
    segments: list[dict],
    assigned_turns: list[dict],
    participant_ids: list[int],
    activity_windows: list[dict],
) -> list[dict]:
    if not assigned_turns:
        raise RuntimeError("Speaker diarization herhangi bir turn üretmediği için segmentler eşlenemedi.")

    segment_candidates: list[dict] = []
    for segment in segments:
        match_payload = collect_segment_turn_matches(segment, assigned_turns)
        matches = match_payload["matches"]
        significant_matches = match_payload["significant_matches"]
        dominant_match = match_payload["dominant_match"]
        if not matches or dominant_match is None:
            continue

        split_required = len(significant_matches) > 1 and float(dominant_match.get("overlap_ratio") or 0.0) < SEGMENT_SINGLE_TURN_DOMINANCE_RATIO
        if split_required:
            fragments = split_segment_by_words(segment, significant_matches)
            if not fragments:
                fragments = split_segment_without_words(segment, significant_matches)
        else:
            fragment_text = normalize_text(segment.get("text") or "")
            fragments = [
                {
                    "fragment_index": 1,
                    "turn": dominant_match["turn"],
                    "text": fragment_text,
                    "raw_text": fragment_text,
                    "start_offset_ms": segment["start_offset_ms"],
                    "end_offset_ms": segment["end_offset_ms"],
                    "split_source": "single_turn",
                    "low_alignment": float(dominant_match.get("overlap_ratio") or 0.0) < 0.35,
                    "needs_review": False,
                    "unresolved_reasons": [],
                }
            ]

        for fragment in fragments:
            if fragment["end_offset_ms"] <= fragment["start_offset_ms"]:
                continue
            relevant_matches = [
                match
                for match in (significant_matches or matches)
                if interval_overlap_ms(
                    fragment["start_offset_ms"],
                    fragment["end_offset_ms"],
                    match["turn"]["start_offset_ms"],
                    match["turn"]["end_offset_ms"],
                ) > 0
            ]
            if not relevant_matches:
                relevant_matches = [dominant_match]

            audio_scores_by_participant: dict[int, float] = {}
            ui_scores_by_participant: dict[int, float] = {}
            match_weights: dict[str, float] = {}
            for match in relevant_matches:
                overlap_ms = interval_overlap_ms(
                    fragment["start_offset_ms"],
                    fragment["end_offset_ms"],
                    match["turn"]["start_offset_ms"],
                    match["turn"]["end_offset_ms"],
                )
                match_weights[match["turn"]["turn_id"]] = float(max(overlap_ms, 1))

            total_weight = sum(match_weights.values()) or 1.0
            for participant_id in participant_ids:
                weighted_audio = 0.0
                for match in relevant_matches:
                    turn = match["turn"]
                    weight = match_weights.get(turn["turn_id"], 1.0)
                    weighted_audio += weight * float(
                        (turn.get("candidate_scores") or {}).get(int(participant_id), {}).get("audio_identity_score") or 0.0
                    )
                audio_scores_by_participant[int(participant_id)] = clamp_score(weighted_audio / total_weight)
                ui_scores_by_participant[int(participant_id)] = compute_ui_activity_score(
                    int(fragment["start_offset_ms"]),
                    int(fragment["end_offset_ms"]),
                    int(participant_id),
                    activity_windows,
                )

            best_audio_participant_id, _best_audio_score, audio_margin, _ = best_score_details(audio_scores_by_participant)
            best_ui_participant_id, _best_ui_score, ui_margin, _ = best_score_details(ui_scores_by_participant)
            active_ui_claimants = [
                participant_id
                for participant_id, score in ui_scores_by_participant.items()
                if score >= 0.25
            ]
            multi_active_claim = len(active_ui_claimants) > 1 or any(
                window.get("conflicted_claim")
                and interval_overlap_ms(
                    int(fragment["start_offset_ms"]),
                    int(fragment["end_offset_ms"]),
                    int(window["start_offset_ms"]),
                    int(window["end_offset_ms"]),
                ) > 0
                for window in activity_windows
            )

            candidate_participants: dict[int, dict] = {}
            for participant_id in participant_ids:
                candidate_participants[int(participant_id)] = {
                    "audio_identity_score": float(audio_scores_by_participant.get(int(participant_id), 0.0)),
                    "ui_activity_score": float(ui_scores_by_participant.get(int(participant_id), 0.0)),
                    "caption_name_score": 0.0,
                    "temporal_continuity_score": HYBRID_TEMPORAL_NEUTRAL_BASE,
                    "evidence_agreement_score": 0.0,
                    "final_score": 0.0,
                }

            segment_candidates.append(
                {
                    **segment,
                    "segment_id": f"seg-{int(segment.get('source_segment_index') or 0)}-{int(fragment['fragment_index'])}",
                    "fragment_index": int(fragment["fragment_index"]),
                    "text": fragment["text"],
                    "raw_text": fragment["raw_text"],
                    "start_offset_ms": int(fragment["start_offset_ms"]),
                    "end_offset_ms": int(fragment["end_offset_ms"]),
                    "matched_turn_ids": [match["turn"]["turn_id"] for match in relevant_matches],
                    "source_turn_ids": [match["turn"]["turn_id"] for match in relevant_matches],
                    "candidate_participants": candidate_participants,
                    "best_audio_participant_id": best_audio_participant_id,
                    "best_ui_participant_id": best_ui_participant_id,
                    "best_caption_participant_id": None,
                    "caption_match_type": "none",
                    "multi_active_claim": multi_active_claim,
                    "audio_margin": float(audio_margin),
                    "ui_margin": float(ui_margin),
                    "reason_codes": [HYBRID_REASON_CODE_MULTI_ACTIVE] if multi_active_claim else [],
                }
            )
    return segment_candidates


def attach_segment_caption_scores(
    segment_candidates: list[dict],
    caption_entries: list[dict],
    participant_rows: list[sqlite3.Row],
) -> list[dict]:
    for segment in segment_candidates:
        caption_entry = select_caption_event_for_segment(segment, caption_entries)
        if caption_entry is None:
            segment["caption_match_type"] = "none"
            segment["best_caption_participant_id"] = None
            continue
        caption_scores, best_caption_participant_id, caption_match_type = compute_caption_name_scores(
            caption_entry.get("speaker"),
            participant_rows,
        )
        segment["best_caption_participant_id"] = best_caption_participant_id
        segment["caption_match_type"] = caption_match_type if caption_match_type in HYBRID_CAPTION_MATCH_TYPES else "none"
        if caption_match_type == "ambiguous" and HYBRID_REASON_CODE_CAPTION_AMBIGUOUS not in segment["reason_codes"]:
            segment["reason_codes"].append(HYBRID_REASON_CODE_CAPTION_AMBIGUOUS)
        for participant_id, candidate_payload in segment["candidate_participants"].items():
            candidate_payload["caption_name_score"] = float(caption_scores.get(int(participant_id), 0.0))
    return segment_candidates


def attach_segment_temporal_scores(
    segment_candidates: list[dict],
    reference_assignments: list[dict] | None,
) -> list[dict]:
    reference_assignments = reference_assignments or []
    index_by_segment_id = {
        assignment["segment_id"]: assignment
        for assignment in reference_assignments
    }
    for index, segment in enumerate(segment_candidates):
        prev_assignment = index_by_segment_id.get(segment_candidates[index - 1]["segment_id"]) if index > 0 else None
        next_assignment = index_by_segment_id.get(segment_candidates[index + 1]["segment_id"]) if index + 1 < len(segment_candidates) else None
        conflicting_candidate_ids: set[int] = set()
        for participant_id, candidate_payload in segment["candidate_participants"].items():
            continuity_score = HYBRID_TEMPORAL_NEUTRAL_BASE
            for neighbor, gap_ms in (
                (
                    prev_assignment,
                    segment["start_offset_ms"] - segment_candidates[index - 1]["end_offset_ms"],
                )
                if index > 0
                else (None, 999999),
                (
                    next_assignment,
                    segment_candidates[index + 1]["start_offset_ms"] - segment["end_offset_ms"],
                )
                if index + 1 < len(segment_candidates)
                else (None, 999999),
            ):
                if not neighbor or gap_ms > HYBRID_TEMPORAL_NEIGHBOR_GAP_MS:
                    continue
                neighbor_participant_id = neighbor.get("chosen_participant_id")
                neighbor_status = neighbor.get("speaker_resolution_status")
                if neighbor_participant_id is None:
                    continue
                if int(neighbor_participant_id) == int(participant_id):
                    if neighbor_status == HYBRID_SPEAKER_STATUS_CONFIRMED:
                        continuity_score = max(continuity_score, 0.80)
                    elif neighbor_status in {HYBRID_SPEAKER_STATUS_UI_PROVISIONAL, HYBRID_SPEAKER_STATUS_AUDIO_PROVISIONAL}:
                        continuity_score = max(continuity_score, 0.60)
                elif neighbor_status == HYBRID_SPEAKER_STATUS_CONFIRMED:
                    continuity_score = 0.0
                    conflicting_candidate_ids.add(int(participant_id))
            candidate_payload["temporal_continuity_score"] = float(clamp_score(continuity_score))
        segment["temporal_conflicting_candidate_ids"] = sorted(conflicting_candidate_ids)
    return segment_candidates


def compute_evidence_agreement_score(segment: dict, participant_id: int) -> float:
    score = 0.15
    best_audio_participant_id = segment.get("best_audio_participant_id")
    best_ui_participant_id = segment.get("best_ui_participant_id")
    best_caption_participant_id = segment.get("best_caption_participant_id")
    caption_match_type = segment.get("caption_match_type")
    if participant_id == best_audio_participant_id and participant_id == best_ui_participant_id and participant_id is not None:
        score += 0.55
    elif participant_id in {best_audio_participant_id, best_ui_participant_id}:
        score += 0.25
    if participant_id == best_caption_participant_id:
        if caption_match_type == "exact":
            score += 0.15
        elif caption_match_type == "fuzzy":
            score += 0.10
    if (
        best_audio_participant_id is not None
        and best_ui_participant_id is not None
        and best_audio_participant_id != best_ui_participant_id
        and participant_id in {best_audio_participant_id, best_ui_participant_id}
    ):
        score -= 0.20
    if segment.get("multi_active_claim") and participant_id == best_ui_participant_id:
        score -= 0.10
    return clamp_score(score)


def build_segment_evidence_graph(
    segments: list[dict],
    assigned_turns: list[dict],
    activity_windows: list[dict],
    canonical_captions: list[dict],
    participant_rows: list[sqlite3.Row],
    participant_ids: list[int],
    reference_assignments: list[dict] | None = None,
) -> list[dict]:
    segment_candidates = collect_segment_candidates(
        segments,
        assigned_turns,
        participant_ids,
        activity_windows,
    )
    segment_candidates = attach_segment_caption_scores(segment_candidates, canonical_captions, participant_rows)
    segment_candidates = attach_segment_temporal_scores(segment_candidates, reference_assignments)
    for segment in segment_candidates:
        for participant_id, candidate_payload in segment["candidate_participants"].items():
            evidence_agreement_score = compute_evidence_agreement_score(segment, int(participant_id))
            candidate_payload["evidence_agreement_score"] = float(evidence_agreement_score)
            candidate_payload["final_score"] = float(
                clamp_score(
                    (0.40 * float(candidate_payload.get("audio_identity_score") or 0.0))
                    + (0.35 * float(candidate_payload.get("ui_activity_score") or 0.0))
                    + (0.15 * float(candidate_payload.get("caption_name_score") or 0.0))
                    + (0.10 * float(candidate_payload.get("temporal_continuity_score") or 0.0))
                )
            )
    return segment_candidates


def build_reason_codes_for_segment(segment: dict) -> list[str]:
    reason_codes = list(segment.get("reason_codes") or [])
    best_audio_participant_id = segment.get("best_audio_participant_id")
    best_ui_participant_id = segment.get("best_ui_participant_id")
    best_caption_participant_id = segment.get("best_caption_participant_id")
    candidate_participants = segment.get("candidate_participants") or {}
    audio_scores = {
        participant_id: float(payload.get("audio_identity_score") or 0.0)
        for participant_id, payload in candidate_participants.items()
    }
    ui_scores = {
        participant_id: float(payload.get("ui_activity_score") or 0.0)
        for participant_id, payload in candidate_participants.items()
    }
    _best_audio, best_audio_score, audio_margin, _ = best_score_details(audio_scores)
    _best_ui, best_ui_score, ui_margin, _ = best_score_details(ui_scores)

    strong_audio_conflict = (
        best_audio_participant_id is not None
        and best_ui_participant_id is not None
        and best_audio_participant_id != best_ui_participant_id
        and best_audio_score >= HYBRID_STRONG_CONFLICT_PRIMARY_FLOOR
        and best_ui_score >= HYBRID_STRONG_CONFLICT_PRIMARY_FLOOR
        and audio_margin >= HYBRID_STRONG_CONFLICT_MARGIN_FLOOR
        and ui_margin >= HYBRID_STRONG_CONFLICT_MARGIN_FLOOR
    )
    if strong_audio_conflict and HYBRID_REASON_CODE_UI_AUDIO_CONFLICT not in reason_codes:
        reason_codes.append(HYBRID_REASON_CODE_UI_AUDIO_CONFLICT)
    if (
        best_caption_participant_id is not None
        and best_ui_participant_id is not None
        and best_caption_participant_id != best_ui_participant_id
        and HYBRID_REASON_CODE_UI_CAPTION_CONFLICT not in reason_codes
    ):
        reason_codes.append(HYBRID_REASON_CODE_UI_CAPTION_CONFLICT)
    if (
        best_caption_participant_id is not None
        and best_audio_participant_id is not None
        and best_caption_participant_id != best_audio_participant_id
        and HYBRID_REASON_CODE_CAPTION_AUDIO_CONFLICT not in reason_codes
    ):
        reason_codes.append(HYBRID_REASON_CODE_CAPTION_AUDIO_CONFLICT)
    return reason_codes


def dominant_review_type(reason_codes: list[str]) -> str | None:
    if HYBRID_REASON_CODE_MULTI_ACTIVE in reason_codes:
        return HYBRID_REVIEW_TYPE_MULTI_ACTIVE
    if HYBRID_REASON_CODE_UI_AUDIO_CONFLICT in reason_codes:
        return HYBRID_REVIEW_TYPE_UI_AUDIO_CONFLICT
    if HYBRID_REASON_CODE_UI_CAPTION_CONFLICT in reason_codes:
        return HYBRID_REVIEW_TYPE_UI_CAPTION_CONFLICT
    if HYBRID_REASON_CODE_CAPTION_AUDIO_CONFLICT in reason_codes:
        return HYBRID_REVIEW_TYPE_CAPTION_AUDIO_CONFLICT
    if HYBRID_REASON_CODE_LOW_EVIDENCE in reason_codes or HYBRID_REASON_CODE_NO_SIGNAL in reason_codes:
        return HYBRID_REVIEW_TYPE_LOW_EVIDENCE
    return None


def resolve_segment_assignment(segment_evidence: dict) -> dict:
    candidate_participants = segment_evidence.get("candidate_participants") or {}
    ranked_candidates = sorted(
        candidate_participants.items(),
        key=lambda item: (float(item[1].get("final_score") or 0.0), -int(item[0])),
        reverse=True,
    )
    chosen_participant_id = int(ranked_candidates[0][0]) if ranked_candidates else None
    chosen_payload = ranked_candidates[0][1] if ranked_candidates else {}
    final_score = float(chosen_payload.get("final_score") or 0.0)
    audio_scores = {
        participant_id: float(payload.get("audio_identity_score") or 0.0)
        for participant_id, payload in candidate_participants.items()
    }
    ui_scores = {
        participant_id: float(payload.get("ui_activity_score") or 0.0)
        for participant_id, payload in candidate_participants.items()
    }
    best_audio_participant_id = segment_evidence.get("best_audio_participant_id")
    best_ui_participant_id = segment_evidence.get("best_ui_participant_id")
    best_audio_participant_id, best_audio_score, audio_margin, _ = best_score_details(audio_scores)
    best_ui_participant_id, best_ui_score, ui_margin, _ = best_score_details(ui_scores)

    primary_signal_available = any(
        max(float(payload.get("audio_identity_score") or 0.0), float(payload.get("ui_activity_score") or 0.0)) >= HYBRID_NO_SIGNAL_PRIMARY_FLOOR
        for payload in candidate_participants.values()
    )
    reason_codes = build_reason_codes_for_segment(segment_evidence)
    if not primary_signal_available and HYBRID_REASON_CODE_NO_SIGNAL not in reason_codes:
        reason_codes.append(HYBRID_REASON_CODE_NO_SIGNAL)

    strong_conflict = HYBRID_REASON_CODE_UI_AUDIO_CONFLICT in reason_codes
    if chosen_participant_id is not None and chosen_participant_id in candidate_participants:
        selected_audio_score = float(candidate_participants[chosen_participant_id].get("audio_identity_score") or 0.0)
        selected_ui_score = float(candidate_participants[chosen_participant_id].get("ui_activity_score") or 0.0)
        evidence_agreement_score = float(candidate_participants[chosen_participant_id].get("evidence_agreement_score") or 0.0)
    else:
        selected_audio_score = 0.0
        selected_ui_score = 0.0
        evidence_agreement_score = 0.0

    primary_channel = "ui" if selected_ui_score >= selected_audio_score else "audio"
    selected_margin = ui_margin if primary_channel == "ui" else audio_margin
    primary_conflict_from_other_channel = (
        (primary_channel == "ui" and best_audio_participant_id is not None and best_audio_participant_id != chosen_participant_id and best_audio_score >= HYBRID_STRONG_CONFLICT_PRIMARY_FLOOR and audio_margin >= HYBRID_STRONG_CONFLICT_MARGIN_FLOOR)
        or (primary_channel == "audio" and best_ui_participant_id is not None and best_ui_participant_id != chosen_participant_id and best_ui_score >= HYBRID_STRONG_CONFLICT_PRIMARY_FLOOR and ui_margin >= HYBRID_STRONG_CONFLICT_MARGIN_FLOOR)
    )

    speaker_resolution_status = HYBRID_SPEAKER_STATUS_UNKNOWN
    assignment_method = HYBRID_ASSIGNMENT_METHOD_UNKNOWN_LOW_EVIDENCE if primary_signal_available else HYBRID_ASSIGNMENT_METHOD_UNKNOWN_NO_SIGNAL
    needs_speaker_review = True
    participant_id: int | None = None

    confirmed = False
    if chosen_participant_id is not None and final_score >= HYBRID_CONFIRMED_FINAL_SCORE_FLOOR:
        if chosen_participant_id in {best_audio_participant_id, best_ui_participant_id} and not primary_conflict_from_other_channel:
            if primary_channel == "audio":
                confirmed = (
                    selected_audio_score >= HYBRID_CONFIRMED_AUDIO_SCORE_FLOOR
                    and selected_margin >= HYBRID_CONFIRMED_AUDIO_MARGIN_FLOOR
                    and evidence_agreement_score >= 0.45
                )
                if not confirmed and selected_margin < HYBRID_CONFIRMED_AUDIO_MARGIN_FLOOR and HYBRID_REASON_CODE_WEAK_AUDIO_MARGIN not in reason_codes:
                    reason_codes.append(HYBRID_REASON_CODE_WEAK_AUDIO_MARGIN)
            else:
                confirmed = (
                    selected_ui_score >= HYBRID_CONFIRMED_UI_SCORE_FLOOR
                    and selected_margin >= HYBRID_CONFIRMED_UI_MARGIN_FLOOR
                    and evidence_agreement_score >= 0.45
                )
                if not confirmed and selected_margin < HYBRID_CONFIRMED_UI_MARGIN_FLOOR and HYBRID_REASON_CODE_WEAK_UI_MARGIN not in reason_codes:
                    reason_codes.append(HYBRID_REASON_CODE_WEAK_UI_MARGIN)

    if confirmed and not strong_conflict:
        speaker_resolution_status = HYBRID_SPEAKER_STATUS_CONFIRMED
        assignment_method = HYBRID_ASSIGNMENT_METHOD_CONFIRMED
        needs_speaker_review = False
        participant_id = chosen_participant_id
    elif strong_conflict:
        speaker_resolution_status = HYBRID_SPEAKER_STATUS_CONFLICTED
        assignment_method = HYBRID_ASSIGNMENT_METHOD_CONFLICTED
        needs_speaker_review = True
        participant_id = None
    elif chosen_participant_id is not None and final_score >= HYBRID_PROVISIONAL_FINAL_SCORE_FLOOR:
        if (
            chosen_participant_id == best_ui_participant_id
            and ui_margin >= HYBRID_PROVISIONAL_UI_MARGIN_FLOOR
            and not primary_conflict_from_other_channel
        ):
            speaker_resolution_status = HYBRID_SPEAKER_STATUS_UI_PROVISIONAL
            assignment_method = HYBRID_ASSIGNMENT_METHOD_UI_PROVISIONAL
            participant_id = chosen_participant_id
            needs_speaker_review = bool(
                segment_evidence.get("multi_active_claim")
                or HYBRID_REASON_CODE_UI_CAPTION_CONFLICT in reason_codes
                or evidence_agreement_score < 0.40
            )
        elif (
            chosen_participant_id == best_audio_participant_id
            and audio_margin >= HYBRID_PROVISIONAL_AUDIO_MARGIN_FLOOR
            and not primary_conflict_from_other_channel
        ):
            speaker_resolution_status = HYBRID_SPEAKER_STATUS_AUDIO_PROVISIONAL
            assignment_method = HYBRID_ASSIGNMENT_METHOD_AUDIO_PROVISIONAL
            participant_id = chosen_participant_id
            needs_speaker_review = bool(
                segment_evidence.get("multi_active_claim")
                or HYBRID_REASON_CODE_CAPTION_AUDIO_CONFLICT in reason_codes
                or evidence_agreement_score < 0.40
            )
        else:
            if HYBRID_REASON_CODE_LOW_EVIDENCE not in reason_codes:
                reason_codes.append(HYBRID_REASON_CODE_LOW_EVIDENCE)
    else:
        if primary_signal_available and HYBRID_REASON_CODE_LOW_EVIDENCE not in reason_codes:
            reason_codes.append(HYBRID_REASON_CODE_LOW_EVIDENCE)

    if chosen_participant_id is not None and chosen_participant_id in set(segment_evidence.get("temporal_conflicting_candidate_ids") or []):
        if HYBRID_REASON_CODE_TEMPORAL_CONFLICT not in reason_codes:
            reason_codes.append(HYBRID_REASON_CODE_TEMPORAL_CONFLICT)

    if speaker_resolution_status == HYBRID_SPEAKER_STATUS_UNKNOWN and not primary_signal_available:
        assignment_method = HYBRID_ASSIGNMENT_METHOD_UNKNOWN_NO_SIGNAL
        participant_id = None
    elif speaker_resolution_status == HYBRID_SPEAKER_STATUS_UNKNOWN:
        assignment_method = HYBRID_ASSIGNMENT_METHOD_UNKNOWN_LOW_EVIDENCE
        participant_id = None

    if any(
        reason_code in reason_codes
        for reason_code in (
            HYBRID_REASON_CODE_MULTI_ACTIVE,
            HYBRID_REASON_CODE_UI_CAPTION_CONFLICT,
            HYBRID_REASON_CODE_CAPTION_AUDIO_CONFLICT,
            HYBRID_REASON_CODE_TEMPORAL_CONFLICT,
        )
    ):
        needs_speaker_review = True

    review_type = dominant_review_type(reason_codes)
    if needs_speaker_review and review_type is None:
        review_type = HYBRID_REVIEW_TYPE_LOW_EVIDENCE

    return {
        "segment_id": segment_evidence["segment_id"],
        "chosen_participant_id": participant_id,
        "participant_id": participant_id,
        "speaker_resolution_status": speaker_resolution_status,
        "assignment_method": assignment_method,
        "assignment_confidence": final_score,
        "needs_speaker_review": needs_speaker_review,
        "review_type": review_type,
        "reason_codes": reason_codes,
        "final_score": final_score,
        "best_audio_participant_id": best_audio_participant_id,
        "best_ui_participant_id": best_ui_participant_id,
        "best_caption_participant_id": segment_evidence.get("best_caption_participant_id"),
        "multi_active_claim": bool(segment_evidence.get("multi_active_claim")),
    }


def materialize_hybrid_segments(
    evidence_graph: list[dict],
    source_audio_path: Path,
) -> tuple[list[dict], list[dict], list[dict]]:
    final_segments: list[dict] = []
    segment_candidate_summary: list[dict] = []
    final_decision_summary: list[dict] = []
    for segment in evidence_graph:
        resolution = resolve_segment_assignment(segment)
        candidate_ids_sorted = [
            participant_id
            for participant_id, _payload in sorted(
                segment["candidate_participants"].items(),
                key=lambda item: (float(item[1].get("final_score") or 0.0), -int(item[0])),
                reverse=True,
            )
        ]
        cluster_lineage = None
        if segment.get("source_turn_ids"):
            cluster_lineage = segment["source_turn_ids"][0]
        final_segments.append(
            {
                **segment,
                "participant_id": resolution["participant_id"],
                "participant_audio_asset_id": None,
                "assignment_method": resolution["assignment_method"],
                "assignment_confidence": resolution["assignment_confidence"],
                "speaker_resolution_status": resolution["speaker_resolution_status"],
                "needs_speaker_review": resolution["needs_speaker_review"],
                "resolution_status": (
                    TRANSCRIPT_STATUS_PENDING_REVIEW if resolution["needs_speaker_review"] else TRANSCRIPT_STATUS_ORIGINAL
                ),
                "source_audio_path": source_audio_path,
                "cluster_lineage": cluster_lineage,
                "review_type": resolution["review_type"],
                "reason_codes": resolution["reason_codes"],
                "multi_active_claim": resolution["multi_active_claim"],
            }
        )
        segment_candidate_summary.append(
            {
                "segment_id": segment["segment_id"],
                "start_offset_ms": segment["start_offset_ms"],
                "end_offset_ms": segment["end_offset_ms"],
                "candidate_participant_ids": candidate_ids_sorted,
                "candidate_scores": {
                    str(participant_id): {
                        "audio_identity_score": float(payload.get("audio_identity_score") or 0.0),
                        "ui_activity_score": float(payload.get("ui_activity_score") or 0.0),
                        "caption_name_score": float(payload.get("caption_name_score") or 0.0),
                        "temporal_continuity_score": float(payload.get("temporal_continuity_score") or 0.0),
                        "evidence_agreement_score": float(payload.get("evidence_agreement_score") or 0.0),
                        "final_score": float(payload.get("final_score") or 0.0),
                    }
                    for participant_id, payload in sorted(
                        segment["candidate_participants"].items(),
                        key=lambda item: int(item[0]),
                    )
                },
                "best_audio_participant_id": segment.get("best_audio_participant_id"),
                "best_ui_participant_id": segment.get("best_ui_participant_id"),
                "best_caption_participant_id": segment.get("best_caption_participant_id"),
                "caption_match_type": segment.get("caption_match_type") if segment.get("caption_match_type") in HYBRID_CAPTION_MATCH_TYPES else "none",
                "chosen_participant_id": resolution["chosen_participant_id"],
                "speaker_resolution_status": resolution["speaker_resolution_status"],
                "assignment_method": resolution["assignment_method"],
                "reason_codes": resolution["reason_codes"],
            }
        )
        final_decision_summary.append(
            {
                "segment_id": segment["segment_id"],
                "chosen_participant_id": resolution["chosen_participant_id"],
                "speaker_resolution_status": resolution["speaker_resolution_status"],
                "assignment_method": resolution["assignment_method"],
                "needs_speaker_review": resolution["needs_speaker_review"],
                "review_type": resolution["review_type"],
                "reason_codes": resolution["reason_codes"],
            }
        )
    return final_segments, segment_candidate_summary, final_decision_summary


def build_audio_primary_segments(
    meeting_id: int,
    source_audio_path: Path,
    audio_source_id: int | None,
    participant_rows: list[sqlite3.Row],
    activity_rows: list[sqlite3.Row],
    meeting_row: sqlite3.Row | None = None,
    canonical_captions: list[dict] | None = None,
) -> tuple[list[dict], dict]:
    whisper_result = load_whisperx_result(source_audio_path, meeting_id)
    master_segments = build_segments_from_whisper_result(
        whisper_result,
        participant_id=None,
        participant_audio_asset_id=None,
        audio_source_id=audio_source_id,
        base_offset_ms=0,
        assignment_method=HYBRID_ASSIGNMENT_METHOD_UNKNOWN_LOW_EVIDENCE,
        assignment_confidence=0.0,
        needs_speaker_review=True,
        source_audio_path=source_audio_path,
    )
    if not master_segments:
        return [], {
            "transcript_segment_count": 0,
            "diarized_turn_count": 0,
            "profile_count": 0,
            "pass_count": 0,
        }

    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_MATERIALIZING_AUDIO,
        None,
        None,
        "Speaker diarization çalışıyor",
    )
    diarized_turns = diarize_speaker_turns(source_audio_path)
    if not diarized_turns:
        raise RuntimeError("Pyannote diarization hiçbir speaker turn üretmedi.")

    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_MATERIALIZING_AUDIO,
        None,
        None,
        "Speaker embeddingleri çıkarılıyor",
    )
    diarized_turns = attach_embeddings_to_turns(diarized_turns, source_audio_path)
    diarized_turns, global_dom_priors = attach_dom_priors_to_turns(diarized_turns, activity_rows)
    diarized_turns, cluster_prototypes, cluster_split_lineage = refine_impure_clusters(diarized_turns)

    participant_ids = [
        int(row["id"])
        for row in participant_rows
        if not bool(row["is_bot"]) and not is_roster_heading_name(row["display_name"])
    ]
    if not participant_ids:
        raise RuntimeError("Toplantıda eşlenecek görünür participant bulunamadı.")

    activity_windows = normalize_speaker_activity_events(activity_rows)
    caption_entries = build_caption_timeline(canonical_captions or [], meeting_row)
    participant_profiles = build_initial_hybrid_profiles(diarized_turns, participant_ids)
    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_ASSEMBLING_SEGMENTS,
        None,
        None,
        "Konuşmacılar hibrit kanıtlarla eşleştiriliyor",
    )
    pass1_turns, pass1_turn_local_summary = score_turns_locally(
        diarized_turns,
        participant_ids,
        participant_profiles,
        cluster_prototypes,
    )
    pass1_graph = build_segment_evidence_graph(
        master_segments,
        pass1_turns,
        activity_windows,
        caption_entries,
        participant_rows,
        participant_ids,
    )
    provisional_segments, pass1_segment_candidate_summary, pass1_final_decision_summary = materialize_hybrid_segments(
        pass1_graph,
        source_audio_path,
    )
    turns_by_id = {turn["turn_id"]: turn for turn in pass1_turns}
    participant_profiles = refresh_hybrid_participant_profiles(
        turns_by_id,
        provisional_segments,
        participant_profiles,
    )
    pass2_turns, turn_local_summary = score_turns_locally(
        diarized_turns,
        participant_ids,
        participant_profiles,
        cluster_prototypes,
    )
    pass2_graph = build_segment_evidence_graph(
        master_segments,
        pass2_turns,
        activity_windows,
        caption_entries,
        participant_rows,
        participant_ids,
        reference_assignments=pass1_final_decision_summary,
    )
    final_segments, segment_candidate_summary, final_decision_summary = materialize_hybrid_segments(
        pass2_graph,
        source_audio_path,
    )
    reason_counts: dict[str, int] = defaultdict(int)
    for decision in final_decision_summary:
        for reason_code in decision.get("reason_codes") or []:
            reason_counts[str(reason_code)] += 1

    def sanitize_turn(turn: dict) -> dict:
        return {
            "turn_id": turn["turn_id"],
            "speaker_label": turn["speaker_label"],
            "start_offset_ms": turn["start_offset_ms"],
            "end_offset_ms": turn["end_offset_ms"],
            "duration_ms": turn.get("duration_ms"),
            "speech_ratio": turn.get("speech_ratio"),
            "overlap_flag": bool(turn.get("overlap_flag")),
            "conflicted_claim": bool(turn.get("conflicted_claim")),
            "best_audio_participant_id": turn.get("best_audio_participant_id"),
            "best_ui_participant_id": turn.get("best_ui_participant_id"),
            "audio_margin": float(turn.get("audio_margin") or 0.0),
            "ui_margin": float(turn.get("ui_margin") or 0.0),
        }

    def sanitize_cluster_prototype(prototype: dict) -> dict:
        return {
            "speaker_label": prototype.get("speaker_label"),
            "purity": prototype.get("purity"),
            "coverage_ms": prototype.get("coverage_ms"),
            "turn_count": prototype.get("turn_count"),
            "prototype_turn_ids": prototype.get("prototype_turn_ids"),
            "base_label": prototype.get("base_label"),
            "split_applied": prototype.get("split_applied"),
            "impure": prototype.get("impure"),
        }

    return final_segments, {
        "transcript_segment_count": len(master_segments),
        "diarized_turn_count": len(diarized_turns),
        "profile_count": len(participant_profiles),
        "pass_count": 2,
        "diarized_turns": [sanitize_turn(turn) for turn in diarized_turns],
        "cluster_prototypes": {
            label: sanitize_cluster_prototype(payload)
            for label, payload in cluster_prototypes.items()
        },
        "cluster_split_lineage": cluster_split_lineage,
        "normalized_activity_windows": activity_windows,
        "turn_local_summary": turn_local_summary,
        "pass1_turn_local_summary": pass1_turn_local_summary,
        "pass1_segment_candidate_summary": pass1_segment_candidate_summary,
        "segment_candidate_summary": segment_candidate_summary,
        "final_decision_summary": final_decision_summary,
        "reason_counts": dict(reason_counts),
        "global_dom_priors": global_dom_priors,
        "assigned_turns": [sanitize_turn(turn) for turn in pass2_turns],
    }


def transcribe_participant_assets(
    meeting_id: int,
    asset_rows: list[sqlite3.Row],
) -> list[dict]:
    segments: list[dict] = []
    for asset in asset_rows:
        file_path = Path(asset["file_path"])
        if not file_path.exists():
            continue
        if (probe_wav_duration_ms(file_path) or 0) <= 0:
            continue
        result = load_whisperx_result(file_path, meeting_id)
        segments.extend(
            build_segments_from_whisper_result(
                result,
                participant_id=asset["participant_id"],
                participant_audio_asset_id=asset["id"],
                audio_source_id=asset["audio_source_id"],
                base_offset_ms=int(asset["start_offset_ms"] or 0),
                assignment_method="activity_bound_asset",
                assignment_confidence=max(0.0, min(1.0, float(asset["confidence"] or 0.0))),
                needs_speaker_review=float(asset["confidence"] or 0.0) < 0.85,
                source_audio_path=file_path,
            )
        )
    return segments


def pick_mixed_segment_assignment(segment_start_ms: int, segment_end_ms: int, activity_rows: list[sqlite3.Row]) -> tuple[int | None, str, float, bool]:
    duration_ms = max(segment_end_ms - segment_start_ms, 1)
    overlap_by_participant: dict[int, int] = {}
    for row in activity_rows:
        participant_id = row["participant_id"]
        if participant_id is None:
            continue
        overlap_start = max(segment_start_ms, int(row["start_offset_ms"] or 0))
        overlap_end = min(segment_end_ms, int(row["end_offset_ms"] or 0))
        if overlap_end <= overlap_start:
            continue
        overlap_by_participant[int(participant_id)] = overlap_by_participant.get(int(participant_id), 0) + (
            overlap_end - overlap_start
        )

    if not overlap_by_participant:
        return None, "mixed_overlap_ambiguous", 0.0, True

    ranked = sorted(overlap_by_participant.items(), key=lambda item: item[1], reverse=True)
    best_participant_id, best_overlap = ranked[0]
    best_ratio = best_overlap / duration_ms
    second_ratio = ranked[1][1] / duration_ms if len(ranked) > 1 else 0.0
    if best_ratio >= 0.65 and (best_ratio - second_ratio) >= 0.20:
        return best_participant_id, "mixed_overlap", best_ratio, False
    if best_ratio > 0:
        return best_participant_id, "mixed_overlap_provisional", best_ratio, True
    return None, "mixed_overlap_ambiguous", 0.0, True


def transcribe_mixed_fallback(
    meeting_id: int,
    source_audio_path: Path,
    audio_source_id: int | None,
    activity_rows: list[sqlite3.Row],
) -> list[dict]:
    result = load_whisperx_result(source_audio_path, meeting_id)
    segments: list[dict] = []
    for item in build_segments_from_whisper_result(
        result,
        participant_id=None,
        participant_audio_asset_id=None,
        audio_source_id=audio_source_id,
        base_offset_ms=0,
        assignment_method="mixed_fallback",
        assignment_confidence=0.0,
        needs_speaker_review=True,
        source_audio_path=source_audio_path,
    ):
        participant_id, assignment_method, assignment_confidence, needs_review = pick_mixed_segment_assignment(
            item["start_offset_ms"],
            item["end_offset_ms"],
            activity_rows,
        )
        item["participant_id"] = participant_id
        item["assignment_method"] = assignment_method
        item["assignment_confidence"] = assignment_confidence
        if participant_id is None:
            item["speaker_resolution_status"] = HYBRID_SPEAKER_STATUS_UNKNOWN
        elif needs_review:
            item["speaker_resolution_status"] = HYBRID_SPEAKER_STATUS_UI_PROVISIONAL
        else:
            item["speaker_resolution_status"] = HYBRID_SPEAKER_STATUS_CONFIRMED
        item["needs_speaker_review"] = needs_review
        item["resolution_status"] = (
            TRANSCRIPT_STATUS_PENDING_REVIEW if needs_review else TRANSCRIPT_STATUS_ORIGINAL
        )
        segments.append(item)
    return segments


def annotate_overlap_groups(segments: list[dict]) -> None:
    ordered = sorted(segments, key=lambda item: (item["start_offset_ms"], item["end_offset_ms"]))
    active: list[dict] = []
    group_index = 0
    for segment in ordered:
        active = [item for item in active if item["end_offset_ms"] > segment["start_offset_ms"]]
        overlaps = [item for item in active if item["participant_id"] != segment["participant_id"]]
        if overlaps:
            group_index += 1
            group_id = f"overlap-{group_index}"
            segment["overlap_group_id"] = group_id
            for overlap in overlaps:
                overlap["overlap_group_id"] = overlap.get("overlap_group_id") or group_id
        active.append(segment)


def build_transcript_segments(participant_segments: list[dict], mixed_segments: list[dict]) -> list[dict]:
    segments = participant_segments if participant_segments else mixed_segments
    deduped: list[dict] = []
    for segment in sorted(segments, key=lambda item: (item["start_offset_ms"], item["end_offset_ms"], item["text"])):
        if not deduped:
            deduped.append(segment)
            continue
        previous = deduped[-1]
        if (
            previous["participant_id"] == segment["participant_id"]
            and previous["text"].casefold() == segment["text"].casefold()
            and abs(previous["start_offset_ms"] - segment["start_offset_ms"]) <= 800
            and previous.get("assignment_method") == segment.get("assignment_method")
            and previous.get("cluster_lineage") == segment.get("cluster_lineage")
        ):
            previous["end_offset_ms"] = max(previous["end_offset_ms"], segment["end_offset_ms"])
            previous["needs_speaker_review"] = previous["needs_speaker_review"] or segment["needs_speaker_review"]
            previous["assignment_confidence"] = max(previous["assignment_confidence"], segment["assignment_confidence"])
            previous["reason_codes"] = sorted(
                set(previous.get("reason_codes") or []).union(segment.get("reason_codes") or [])
            )
            previous["review_type"] = previous.get("review_type") or segment.get("review_type")
            continue
        deduped.append(segment)
    annotate_overlap_groups(deduped)
    for index, segment in enumerate(deduped, start=1):
        segment["sequence_no"] = index
    return deduped


def create_segment_review_item(
    conn: sqlite3.Connection,
    transcript_id: int,
    transcript_segment_id: int,
    review_type: str,
    current_text: str,
    suggested_text: str,
    confidence: float,
    current_participant_id: int | None,
    suggested_participant_id: int | None,
    clip_start_ms: int,
    clip_end_ms: int,
) -> int:
    created_at = datetime.utcnow().isoformat()
    cursor = conn.execute(
        """
        INSERT INTO transcriptreviewitem (
            transcript_id,
            transcript_segment_id,
            review_type,
            granularity,
            current_text,
            suggested_text,
            confidence,
            current_participant_id,
            suggested_participant_id,
            status,
            clip_start_ms,
            clip_end_ms,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            transcript_id,
            transcript_segment_id,
            review_type,
            "speaker",
            current_text,
            suggested_text,
            confidence,
            current_participant_id,
            suggested_participant_id,
            REVIEW_STATUS_PENDING,
            clip_start_ms,
            clip_end_ms,
            created_at,
        ),
    )
    return int(cursor.lastrowid)


def persist_transcript_segments(
    meeting_id: int,
    segments: list[dict],
    participant_lookup: dict[int, dict],
) -> int:
    conn = db_connection()
    pending_reviews = 0
    try:
        for segment in segments:
            participant = participant_lookup.get(segment["participant_id"]) if segment.get("participant_id") else None
            speaker = normalize_text(participant.get("display_name") if participant else "Unknown") or "Unknown"
            timestamp = datetime.utcnow().isoformat()
            transcript_cursor = conn.execute(
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
                    segment["sequence_no"],
                    speaker,
                    segment["raw_text"],
                    segment["text"],
                    segment["start_offset_ms"] / 1000.0,
                    segment["end_offset_ms"] / 1000.0,
                    None,
                    None,
                    segment["resolution_status"],
                    0,
                    timestamp,
                ),
            )
            transcript_id = int(transcript_cursor.lastrowid)
            cursor = conn.execute(
                """
                INSERT INTO transcriptsegment (
                    meeting_id,
                    participant_id,
                    participant_audio_asset_id,
                    audio_source_id,
                    sequence_no,
                    raw_text,
                    text,
                    language,
                    start_offset_ms,
                    end_offset_ms,
                    asr_confidence,
                    assignment_method,
                    assignment_confidence,
                    speaker_resolution_status,
                    overlap_group_id,
                    needs_speaker_review,
                    resolution_status,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    meeting_id,
                    segment.get("participant_id"),
                    segment.get("participant_audio_asset_id"),
                    segment.get("audio_source_id"),
                    segment["sequence_no"],
                    segment["raw_text"],
                    segment["text"],
                    segment.get("language"),
                    segment["start_offset_ms"],
                    segment["end_offset_ms"],
                    segment.get("asr_confidence"),
                    segment["assignment_method"],
                    segment["assignment_confidence"],
                    segment.get("speaker_resolution_status", HYBRID_SPEAKER_STATUS_UNKNOWN),
                    segment.get("overlap_group_id"),
                    1 if segment["needs_speaker_review"] else 0,
                    segment["resolution_status"],
                    timestamp,
                    timestamp,
                ),
            )
            transcript_segment_id = int(cursor.lastrowid)
            if not segment["needs_speaker_review"]:
                continue
            clip_start_sec = max((segment["start_offset_ms"] / 1000.0) - 1.0, 0.0)
            clip_end_sec = max((segment["end_offset_ms"] / 1000.0) + 1.0, clip_start_sec + 1.0)
            review_item_id = create_segment_review_item(
                conn,
                transcript_id,
                transcript_segment_id,
                segment.get("review_type") or HYBRID_REVIEW_TYPE_LOW_EVIDENCE,
                segment["text"],
                speaker,
                max(0.0, min(1.0, segment["assignment_confidence"])),
                segment.get("participant_id"),
                segment.get("participant_id"),
                int(clip_start_sec * 1000),
                int(clip_end_sec * 1000),
            )
            source_audio_path = segment.get("source_audio_path")
            if isinstance(source_audio_path, Path) and source_audio_path.exists():
                clip_filename = create_segment_audio_clip(
                    source_audio_path,
                    meeting_id,
                    transcript_segment_id,
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


def fetch_participant_context(meeting_id: int) -> tuple[list[sqlite3.Row], list[sqlite3.Row], list[sqlite3.Row]]:
    return (
        fetch_meeting_participants(meeting_id),
        fetch_speaker_activity_events(meeting_id),
        fetch_audio_sources(meeting_id),
    )


def compute_source_vad(source_audio_path: Path, activity_rows: list[sqlite3.Row]) -> list[dict]:
    # v1 uses observed speaker activity as the initial speech mask until per-source VAD lands.
    if not source_audio_path.exists():
        return []
    return build_activity_windows(activity_rows)


def bind_audio_sources(
    meeting_id: int,
    audio_source_rows: list[sqlite3.Row],
    activity_rows: list[sqlite3.Row],
) -> None:
    conn = db_connection()
    try:
        conn.execute("DELETE FROM audiosourcebinding WHERE meeting_id = ?", (meeting_id,))
        activity_windows = build_activity_windows(activity_rows)
        for source_row in audio_source_rows:
            if source_row["source_kind"] != "meeting_mixed_master":
                continue
            for window in activity_windows:
                confidence = max(0.0, min(1.0, float(window["confidence"] or 0.0)))
                binding_status = (
                    ASSIGNMENT_STATUS_CONFIRMED if confidence >= 0.85
                    else ASSIGNMENT_STATUS_PROVISIONAL if confidence >= 0.65
                    else ASSIGNMENT_STATUS_UNKNOWN
                )
                conn.execute(
                    """
                    INSERT INTO audiosourcebinding (
                        meeting_id,
                        audio_source_id,
                        participant_id,
                        valid_from_ms,
                        valid_to_ms,
                        binding_status,
                        binding_method,
                        confidence,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        meeting_id,
                        source_row["id"],
                        window["participant_id"],
                        window["start_offset_ms"],
                        window["end_offset_ms"],
                        binding_status,
                        "activity_overlap",
                        confidence,
                        datetime.utcnow().isoformat(),
                        datetime.utcnow().isoformat(),
                    ),
                )
        conn.commit()
    finally:
        conn.close()


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
                elif should_force_low_confidence_review(teams_text, whisper_text, coverage, avg_confidence):
                    resolution_status = TRANSCRIPT_STATUS_PENDING_REVIEW
                    should_create_review = True
                    granularity = "sentence"
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
                if row.get("start_sec") is not None and row.get("end_sec") is not None:
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
    if not audio_ready:
        raise RuntimeError("Audio-primary speaker assignment için hazır mixed master audio bulunamadı.")

    dependency_error = dependency_error_message(require_whisperx=True, require_pyannote=True)
    if dependency_error:
        raise RuntimeError(dependency_error)

    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_BINDING_SOURCES,
        None,
        None,
        "Participant registry ve speaker activity baglaniyor",
    )
    participant_rows, activity_rows, audio_source_rows = fetch_participant_context(meeting_id)
    caption_rows = fetch_caption_events(meeting_id)
    legacy_transcript_rows = fetch_legacy_transcripts(meeting_id)
    canonical_caption_rows = canonicalize_caption_events(caption_rows) if caption_rows else []
    legacy_canonical_rows = (
        canonicalize_caption_events(build_events_from_legacy_transcripts(legacy_transcript_rows))
        if legacy_transcript_rows
        else []
    )
    persist_json(
        get_teams_canonical_path(meeting_id),
        {
            "participants": [dict(row) for row in participant_rows],
            "speaker_activity": [dict(row) for row in activity_rows],
            "audio_sources": [dict(row) for row in audio_source_rows],
            "captions": canonical_caption_rows,
            "legacy_captions": legacy_canonical_rows,
        },
    )

    source_audio_path: Path | None = None
    mixed_source_id: int | None = None
    if asset is not None:
        master_audio_path = Path(asset["master_audio_path"])
        pcm_audio_path = Path(asset["pcm_audio_path"]) if asset["pcm_audio_path"] else None
        if not pcm_audio_path or not pcm_audio_path.exists():
            pcm_audio_path = convert_audio_to_pcm(master_audio_path, meeting_id)
        update_audio_asset_paths(asset["id"], str(pcm_audio_path), POSTPROCESS_VERSION)
        source_audio_path = pcm_audio_path
        mixed_source_id = upsert_mixed_audio_source(meeting_id, str(pcm_audio_path), "wav")
        participant_rows, activity_rows, audio_source_rows = fetch_participant_context(meeting_id)
        if source_audio_path.exists():
            bind_audio_sources(meeting_id, audio_source_rows, activity_rows)
            persist_json(get_alignment_map_path(meeting_id), compute_source_vad(source_audio_path, activity_rows))

    if source_audio_path is None or not source_audio_path.exists():
        raise RuntimeError("PCM meeting audio hazırlanamadı.")

    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_TRANSCRIBING,
        None,
        None,
        "Mixed master audio transcript ve diarization hazırlanıyor",
    )
    participant_lookup = build_participant_lookup(fetch_meeting_participants(meeting_id))
    participant_audio_assets: list[sqlite3.Row] = []
    final_segments, assignment_debug = build_audio_primary_segments(
        meeting_id,
        source_audio_path,
        mixed_source_id,
        participant_rows,
        activity_rows,
        meeting_row=meeting,
        canonical_captions=canonical_caption_rows,
    )
    final_segments = build_transcript_segments(final_segments, [])
    clear_previous_outputs(meeting_id)
    get_meeting_artifact_path(meeting_id, "speaker_profiles.json").unlink(missing_ok=True)
    persist_json(
        get_whisperx_result_path(meeting_id),
        {
            "participant_asset_count": len(participant_audio_assets),
            "participant_segment_count": 0,
            "mixed_segment_count": assignment_debug.get("transcript_segment_count", len(final_segments)),
            "diarized_turn_count": assignment_debug.get("diarized_turn_count", 0),
            "profile_count": assignment_debug.get("profile_count", 0),
            "assignment_pass_count": assignment_debug.get("pass_count", 0),
            "final_segment_count": len(final_segments),
        },
    )
    persist_json(
        get_meeting_artifact_path(meeting_id, "speaker_diarization.json"),
        {
            "turns": assignment_debug.get("diarized_turns", []),
            "assigned_turns": assignment_debug.get("assigned_turns", []),
            "cluster_split_lineage": assignment_debug.get("cluster_split_lineage", {}),
            "cluster_prototypes": assignment_debug.get("cluster_prototypes", {}),
        },
    )
    persist_json(
        get_meeting_artifact_path(meeting_id, "speaker_assignment_debug.json"),
        {
            "normalized_activity_windows": assignment_debug.get("normalized_activity_windows", []),
            "turn_local_summary": assignment_debug.get("turn_local_summary", []),
            "segment_candidate_summary": assignment_debug.get("segment_candidate_summary", []),
            "final_decision_summary": assignment_debug.get("final_decision_summary", []),
            "reason_counts": assignment_debug.get("reason_counts", {}),
            "pass1_turn_local_summary": assignment_debug.get("pass1_turn_local_summary", []),
            "pass1_segment_candidate_summary": assignment_debug.get("pass1_segment_candidate_summary", []),
        },
    )

    update_meeting_postprocess_status(
        meeting_id,
        POSTPROCESS_STATUS_ASSEMBLING_SEGMENTS,
        None,
        None,
        "Transcript segmentleri yaziliyor",
    )
    pending_reviews = persist_transcript_segments(meeting_id, final_segments, participant_lookup)
    if pending_reviews:
        update_meeting_postprocess_status(
            meeting_id,
            POSTPROCESS_STATUS_REVIEW_READY,
            None,
            None,
            None,
        )
    else:
        update_meeting_postprocess_status(
            meeting_id,
            POSTPROCESS_STATUS_COMPLETED,
            None,
            None,
            None,
        )


def main():
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m backend.workers.postprocess_worker <meeting_id>")

    meeting_id = int(sys.argv[1])
    run_id_value = os.getenv("NOTERA_WORKER_RUN_ID")
    run_id = int(run_id_value) if run_id_value and run_id_value.isdigit() else run_id_value
    context_token = bind_context(meeting_id=meeting_id, worker_type="postprocess", run_id=run_id)
    ensure_runtime_schema(get_db_path())
    update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_QUEUED, None, None, None)
    try:
        log_event(logger, logging.INFO, "worker.started", "Postprocess worker started")
        process_meeting(meeting_id)
        log_event(logger, logging.INFO, "worker.completed", "Postprocess worker completed")
    except Exception as exc:
        log_event(
            logger,
            logging.ERROR,
            "worker.failed",
            "Postprocess worker failed",
            error_name=type(exc).__name__,
            error_message=str(exc),
            exc_info=exc,
        )
        update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_FAILED, str(exc), None, None)
        raise
    finally:
        reset_context(context_token)


if __name__ == "__main__":
    main()
