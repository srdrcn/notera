# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /srv/notera

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libgomp1 tini \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /tmp/backend-requirements.txt

RUN python -m pip install --upgrade pip \
    && python -m pip install -r /tmp/backend-requirements.txt \
    && python -m playwright install --with-deps chromium

COPY backend /srv/notera/backend
COPY bot /srv/notera/bot

RUN mkdir -p \
    /data \
    /data/live_previews \
    /data/review_clips \
    /data/meeting_audio \
    /data/runtime_cache

ENV NOTERA_DB_PATH=/data/notera.db \
    NOTERA_MEETING_AUDIO_ROOT=/data/meeting_audio \
    NOTERA_LIVE_PREVIEW_ROOT=/data/live_previews \
    NOTERA_REVIEW_CLIP_ROOT=/data/review_clips \
    NOTERA_RUNTIME_CACHE_ROOT=/data/runtime_cache

EXPOSE 8000

ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "backend.app"]
