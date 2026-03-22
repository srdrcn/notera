from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy.orm import Session

from backend.api.deps import owned_user
from backend.db.session import get_db
from backend.runtime.logging import bind_context, log_event
from backend.repositories.meetings import (
    caption_events_for_meeting,
    get_owned_meeting,
    latest_audio_asset,
    review_items_for_transcripts,
    transcripts_for_meeting,
)
from backend.services.exports import export_csv, export_txt
from backend.services.transcript_logic import build_snapshot


router = APIRouter(prefix="/api/meetings", tags=["exports"])
logger = logging.getLogger("notera.routes.exports")


def _snapshot_for_export(db: Session, user_id: int, meeting_id: int):
    meeting = get_owned_meeting(db, user_id, meeting_id)
    if meeting is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    bind_context(meeting_id=meeting.id)
    caption_events = caption_events_for_meeting(db, meeting_id)
    transcripts = transcripts_for_meeting(db, meeting_id)
    review_items = review_items_for_transcripts(db, [row.id for row in transcripts], pending_only=True)
    audio_asset = latest_audio_asset(db, meeting_id)
    return build_snapshot(meeting, caption_events, transcripts, review_items, audio_asset)


@router.get("/{meeting_id}/export.txt")
def download_txt(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    snapshot = _snapshot_for_export(db, user.id, meeting_id)
    headers = {"Content-Disposition": f'attachment; filename="transcript_{meeting_id}.txt"'}
    log_event(
        logger,
        logging.INFO,
        "export.txt.downloaded",
        "TXT transcript export generated",
        meeting_id=meeting_id,
        user_id=user.id,
    )
    return PlainTextResponse(export_txt(snapshot), headers=headers)


@router.get("/{meeting_id}/export.csv")
def download_csv(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    snapshot = _snapshot_for_export(db, user.id, meeting_id)
    headers = {"Content-Disposition": f'attachment; filename="transcript_{meeting_id}.csv"'}
    log_event(
        logger,
        logging.INFO,
        "export.csv.downloaded",
        "CSV transcript export generated",
        meeting_id=meeting_id,
        user_id=user.id,
    )
    return Response(export_csv(snapshot), media_type="text/csv", headers=headers)
