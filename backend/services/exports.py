from __future__ import annotations

import csv
import io

from backend.schemas.transcript import MeetingSnapshotOut


def export_txt(snapshot: MeetingSnapshotOut) -> str:
    return "\n".join(f"[{row.timestamp}] {row.speaker}: {row.text}" for row in snapshot.transcripts)


def export_csv(snapshot: MeetingSnapshotOut) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Zaman", "Konuşmacı", "Metin"])
    for row in snapshot.transcripts:
        writer.writerow([row.timestamp, row.speaker, row.text])
    return output.getvalue()
