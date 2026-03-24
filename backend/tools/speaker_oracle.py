from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from backend.runtime.bootstrap import ensure_runtime_schema
from backend.runtime.participant_names import normalize_participant_name
from backend.runtime.paths import db_path as runtime_db_path


def _connect() -> sqlite3.Connection:
    ensure_runtime_schema(runtime_db_path())
    conn = sqlite3.connect(runtime_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().split()).casefold()


def load_oracle(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Oracle file must contain a JSON object.")
    if not isinstance(payload.get("entries"), list):
        raise ValueError("Oracle file must contain an 'entries' array.")
    return payload


def fetch_segments(conn: sqlite3.Connection, meeting_id: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT ts.id, ts.meeting_id, ts.sequence_no, ts.start_offset_ms, ts.end_offset_ms,
               ts.text, ts.assignment_method, ts.assignment_confidence, ts.needs_speaker_review,
               coalesce(mp.display_name, 'Unknown') AS speaker
        FROM transcriptsegment ts
        LEFT JOIN meetingparticipant mp ON mp.id = ts.participant_id
        WHERE ts.meeting_id = ?
        ORDER BY ts.start_offset_ms, ts.end_offset_ms, ts.id
        """,
        (meeting_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def match_oracle_entry(segments: list[dict], entry: dict) -> dict | None:
    expected_start_ms = int(entry.get("start_offset_ms") or 0)
    tolerance_ms = int(entry.get("time_tolerance_ms") or 3000)
    text_contains = normalize_text(entry.get("text_contains") or "")
    candidates: list[tuple[int, int, int, dict]] = []
    for segment in segments:
        text = normalize_text(segment.get("text"))
        if text_contains and text_contains not in text:
            continue
        distance_ms = abs(int(segment["start_offset_ms"]) - expected_start_ms)
        if distance_ms > tolerance_ms:
            continue
        exact_match_bonus = 0 if text == text_contains else 1
        candidates.append(
            (
                exact_match_bonus,
                distance_ms,
                abs(int(segment["end_offset_ms"]) - int(segment["start_offset_ms"])),
                segment,
            )
        )
    if not candidates:
        return None
    return min(candidates, key=lambda item: (item[0], item[1], item[2]))[3]


def evaluate_entry(segment: dict | None, entry: dict) -> dict:
    expected_speakers = [normalize_participant_name(value) for value in entry.get("expected_speakers", [])]
    if segment is None:
        return {
            "id": entry.get("id"),
            "status": "missing",
            "expected_speakers": expected_speakers,
            "actual_speaker": None,
            "segment": None,
            "notes": entry.get("notes"),
        }
    actual_speaker = normalize_participant_name(segment.get("speaker"))
    status = "pass" if actual_speaker in expected_speakers else "fail"
    return {
        "id": entry.get("id"),
        "status": status,
        "expected_speakers": expected_speakers,
        "actual_speaker": actual_speaker,
        "segment": segment,
        "notes": entry.get("notes"),
    }


def evaluate_oracle(segments: list[dict], oracle_payload: dict) -> dict:
    results = []
    for entry in oracle_payload.get("entries", []):
        matched = match_oracle_entry(segments, entry)
        results.append(evaluate_entry(matched, entry))

    summary = {
        "meeting_id": oracle_payload.get("meeting_id"),
        "description": oracle_payload.get("description"),
        "total": len(results),
        "passed": sum(1 for item in results if item["status"] == "pass"),
        "failed": sum(1 for item in results if item["status"] == "fail"),
        "missing": sum(1 for item in results if item["status"] == "missing"),
        "results": results,
    }
    return summary


def print_summary(summary: dict) -> None:
    print(
        f"meeting={summary['meeting_id']} total={summary['total']} "
        f"passed={summary['passed']} failed={summary['failed']} missing={summary['missing']}"
    )
    for result in summary["results"]:
        segment = result["segment"] or {}
        speaker = result["actual_speaker"] or "MISSING"
        text = segment.get("text") or ""
        start_ms = segment.get("start_offset_ms")
        assignment_method = segment.get("assignment_method")
        confidence = segment.get("assignment_confidence")
        print(
            f"[{result['status'].upper()}] {result['id']} expected={result['expected_speakers']} "
            f"actual={speaker} start={start_ms} method={assignment_method} conf={confidence} text={text}"
        )


def default_oracle_path(meeting_id: int) -> Path:
    return Path(__file__).resolve().parent / "oracles" / f"meeting_{meeting_id}.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate speaker assignment against a small meeting oracle.")
    parser.add_argument("meeting_id", nargs="?", type=int, help="Meeting ID. Defaults to oracle file meeting_id.")
    parser.add_argument("--oracle", type=Path, help="Path to oracle JSON. Defaults to backend/tools/oracles/meeting_<id>.json")
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    parser.add_argument("--fail-on-mismatch", action="store_true", help="Exit non-zero when any fail/missing exists.")
    args = parser.parse_args()

    meeting_id = args.meeting_id
    oracle_path = args.oracle
    if meeting_id is None and oracle_path is None:
        raise SystemExit("Provide either meeting_id or --oracle.")
    if oracle_path is None:
        oracle_path = default_oracle_path(meeting_id)
    oracle_payload = load_oracle(oracle_path)
    if meeting_id is None:
        meeting_id = int(oracle_payload["meeting_id"])

    conn = _connect()
    try:
        segments = fetch_segments(conn, int(meeting_id))
    finally:
        conn.close()

    summary = evaluate_oracle(segments, oracle_payload)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print_summary(summary)

    if args.fail_on_mismatch and (summary["failed"] or summary["missing"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
