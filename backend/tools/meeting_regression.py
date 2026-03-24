from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from backend.runtime.bootstrap import ensure_runtime_schema
from backend.runtime.participant_names import is_roster_heading_name, normalize_participant_name
from backend.runtime.paths import db_path as runtime_db_path, meeting_audio_root


def _connect() -> sqlite3.Connection:
    ensure_runtime_schema(runtime_db_path())
    conn = sqlite3.connect(runtime_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _default_meeting_ids() -> list[int]:
    ids: list[int] = []
    for path in sorted(meeting_audio_root().glob("meeting_*")):
        if not path.is_dir():
            continue
        try:
            ids.append(int(path.name.split("_", 1)[1]))
        except (IndexError, ValueError):
            continue
    return ids


def _rerun_postprocess(meeting_id: int) -> None:
    from backend.workers.postprocess_worker import process_meeting, update_meeting_postprocess_status
    from backend.runtime.constants import POSTPROCESS_STATUS_FAILED, POSTPROCESS_STATUS_QUEUED

    try:
        update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_QUEUED, None, None, None)
        process_meeting(meeting_id)
    except Exception as exc:
        update_meeting_postprocess_status(meeting_id, POSTPROCESS_STATUS_FAILED, str(exc), None, None)
        raise


def _meeting_exists(conn: sqlite3.Connection, meeting_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM meeting WHERE id = ? LIMIT 1", (meeting_id,)).fetchone()
    return row is not None


def _meeting_summary(conn: sqlite3.Connection, meeting_id: int) -> dict:
    meeting = conn.execute(
        """
        SELECT id, title, status, audio_status, postprocess_status, postprocess_error
        FROM meeting
        WHERE id = ?
        """,
        (meeting_id,),
    ).fetchone()
    if meeting is None:
        raise ValueError(f"Meeting {meeting_id} bulunamadı.")

    participant_rows = conn.execute(
        """
        SELECT id, display_name, participant_key, is_bot, join_state, merged_into_participant_id
        FROM meetingparticipant
        WHERE meeting_id = ?
        ORDER BY id
        """,
        (meeting_id,),
    ).fetchall()
    segments = conn.execute(
        """
        SELECT participant_id, assignment_method, needs_speaker_review
        FROM transcriptsegment
        WHERE meeting_id = ?
        ORDER BY sequence_no, id
        """,
        (meeting_id,),
    ).fetchall()
    activity_rows = conn.execute(
        """
        SELECT participant_id, COUNT(*) AS event_count
        FROM speakeractivityevent
        WHERE meeting_id = ?
        GROUP BY participant_id
        """,
        (meeting_id,),
    ).fetchall()

    visible_names: list[str] = []
    duplicate_names: list[str] = []
    roster_heading_names: list[str] = []
    participant_map: dict[int, str] = {}
    for row in participant_rows:
        participant_map[int(row["id"])] = row["display_name"]
        if row["is_bot"] or row["join_state"] == "merged" or row["merged_into_participant_id"] is not None:
            continue
        if is_roster_heading_name(row["display_name"]):
            roster_heading_names.append(row["display_name"])
            continue
        visible_names.append(normalize_participant_name(row["display_name"]).casefold())

    duplicate_names = sorted(name for name, count in Counter(visible_names).items() if count > 1)
    segment_counts = Counter()
    assignment_methods = Counter()
    pending_review_count = 0
    unknown_segment_count = 0
    for row in segments:
        assignment_methods[row["assignment_method"]] += 1
        if row["needs_speaker_review"]:
            pending_review_count += 1
        participant_id = row["participant_id"]
        if participant_id is None:
            unknown_segment_count += 1
            continue
        segment_counts[participant_map.get(int(participant_id), f"id:{participant_id}")] += 1

    activity_counts = defaultdict(int)
    for row in activity_rows:
        activity_counts[participant_map.get(int(row["participant_id"]), f"id:{row['participant_id']}")] = int(row["event_count"])

    return {
        "meeting_id": meeting_id,
        "title": meeting["title"],
        "status": meeting["status"],
        "audio_status": meeting["audio_status"],
        "postprocess_status": meeting["postprocess_status"],
        "postprocess_error": meeting["postprocess_error"],
        "participant_count": len(participant_rows),
        "visible_duplicate_names": duplicate_names,
        "roster_heading_names": roster_heading_names,
        "segment_count": len(segments),
        "unknown_segment_count": unknown_segment_count,
        "pending_review_count": pending_review_count,
        "segment_counts_by_speaker": dict(sorted(segment_counts.items())),
        "speaker_activity_counts": dict(sorted(activity_counts.items())),
        "assignment_methods": dict(sorted(assignment_methods.items())),
    }


def _status_line(summary: dict) -> str:
    findings: list[str] = []
    if summary["visible_duplicate_names"]:
        findings.append(f"duplicate={','.join(summary['visible_duplicate_names'])}")
    if summary["roster_heading_names"]:
        findings.append("roster_heading_visible")
    if summary["unknown_segment_count"]:
        findings.append(f"unknown_segments={summary['unknown_segment_count']}")
    if summary["pending_review_count"]:
        findings.append(f"pending_review={summary['pending_review_count']}")
    health = "WARN" if findings else "OK"
    suffix = f" [{' | '.join(findings)}]" if findings else ""
    return (
        f"{health} meeting={summary['meeting_id']} "
        f"audio={summary['audio_status']} postprocess={summary['postprocess_status']} "
        f"segments={summary['segment_count']} speakers={json.dumps(summary['segment_counts_by_speaker'], ensure_ascii=False)}"
        f"{suffix}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recorded meeting replay/regression runner. Can optionally rerun postprocess and print assignment summaries."
    )
    parser.add_argument("meeting_ids", nargs="*", type=int, help="Meeting IDs to inspect. Default: all recorded meetings under data/meeting_audio.")
    parser.add_argument("--rerun", action="store_true", help="Rerun postprocess before collecting the summary.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of human-readable lines.")
    args = parser.parse_args()

    meeting_ids = args.meeting_ids or _default_meeting_ids()
    if not meeting_ids:
        raise SystemExit("Hiç recorded meeting bulunamadı.")

    conn = _connect()
    try:
        summaries: list[dict] = []
        for meeting_id in meeting_ids:
            if not _meeting_exists(conn, meeting_id):
                raise SystemExit(f"Meeting {meeting_id} veritabanında yok.")
            if args.rerun:
                _rerun_postprocess(meeting_id)
            summaries.append(_meeting_summary(conn, meeting_id))
    finally:
        conn.close()

    if args.json:
        print(json.dumps(summaries, ensure_ascii=False, indent=2))
        return

    for summary in summaries:
        print(_status_line(summary))
        print(f"  activity={json.dumps(summary['speaker_activity_counts'], ensure_ascii=False)}")
        print(f"  methods={json.dumps(summary['assignment_methods'], ensure_ascii=False)}")


if __name__ == "__main__":
    main()
