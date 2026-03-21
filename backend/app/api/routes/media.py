from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.api.deps import owned_user
from backend.app.db.session import get_db
from backend.app.models import Meeting, MeetingAudioAsset, Transcript, TranscriptReviewItem
from backend.app.runtime.paths import get_meeting_pcm_audio_path, preview_path, review_clip_root


router = APIRouter(prefix="/api/media", tags=["media"])


@router.get("/meetings/{meeting_id}/audio")
def meeting_audio(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = db.get(Meeting, meeting_id)
    if meeting is None or meeting.user_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    asset = db.scalar(
        select(MeetingAudioAsset)
        .where(MeetingAudioAsset.meeting_id == meeting_id)
        .order_by(MeetingAudioAsset.id.desc())
    )
    if asset is None or not asset.master_audio_path:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ses kaydı bulunamadı.")
    file_path = Path(asset.master_audio_path)
    pcm_path = get_meeting_pcm_audio_path(meeting_id)
    if pcm_path.exists():
        file_path = pcm_path
    if not file_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ses dosyası bulunamadı.")
    return FileResponse(file_path)


@router.get("/meetings/{meeting_id}/preview")
def meeting_preview(meeting_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    meeting = db.get(Meeting, meeting_id)
    if meeting is None or meeting.user_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Toplantı bulunamadı.")
    image_path = preview_path(user.id, meeting_id)
    if not image_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Önizleme bulunamadı.")
    return FileResponse(image_path, headers={"Cache-Control": "no-store"})


@router.get("/reviews/{review_id}/clip")
def review_clip(review_id: int, user=Depends(owned_user), db: Session = Depends(get_db)):
    review = db.get(TranscriptReviewItem, review_id)
    if review is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Review bulunamadı.")
    transcript = db.get(Transcript, review.transcript_id)
    meeting = db.get(Meeting, transcript.meeting_id) if transcript else None
    if transcript is None or meeting is None or meeting.user_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Review bulunamadı.")
    if not review.audio_clip_path:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Audio clip bulunamadı.")
    clip_path = Path(review.audio_clip_path)
    if not clip_path.is_absolute():
        clip_path = review_clip_root() / clip_path.name
    if not clip_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Audio clip bulunamadı.")
    return FileResponse(clip_path)
