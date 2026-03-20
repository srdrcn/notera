FROM python:3.11-slim

ARG PRELOAD_WHISPERX_MODELS=1
ARG WHISPERX_MODEL_REPO=Systran/faster-whisper-large-v3
ARG WHISPERX_ALIGN_MODEL_REPO=cahya/wav2vec2-base-turkish
ARG WHISPERX_VAD_MODEL_REPO=pyannote/segmentation
ARG HF_TOKEN=""

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FRONTEND_PORT=3000 \
    BACKEND_PORT=8000 \
    API_URL=http://localhost:8000 \
    MPLCONFIGDIR=/srv/notera/bot/runtime_cache/matplotlib \
    HF_HOME=/srv/notera/bot/runtime_cache/huggingface \
    XDG_CACHE_HOME=/srv/notera/bot/runtime_cache/xdg

WORKDIR /srv/notera

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg libgomp1 tini \
    && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip \
    && python -m pip install -r /tmp/requirements.txt \
    && python -m playwright install --with-deps chromium

RUN if [ "$PRELOAD_WHISPERX_MODELS" = "1" ]; then \
        PRELOAD_WHISPERX_MODELS="$PRELOAD_WHISPERX_MODELS" \
        WHISPERX_MODEL_REPO="$WHISPERX_MODEL_REPO" \
        WHISPERX_ALIGN_MODEL_REPO="$WHISPERX_ALIGN_MODEL_REPO" \
        WHISPERX_VAD_MODEL_REPO="$WHISPERX_VAD_MODEL_REPO" \
        HF_TOKEN="$HF_TOKEN" \
        python - <<'PY'; \
import os
from huggingface_hub import snapshot_download

repos = [
    os.environ["WHISPERX_MODEL_REPO"],
    os.environ["WHISPERX_ALIGN_MODEL_REPO"],
    os.environ["WHISPERX_VAD_MODEL_REPO"],
]
token = os.environ.get("HF_TOKEN") or None
cache_dir = os.environ["HF_HOME"]

for repo_id in repos:
    print(f"Preloading Hugging Face model cache: {repo_id}", flush=True)
    snapshot_download(
        repo_id=repo_id,
        cache_dir=cache_dir,
        token=token,
    )
PY \
    ; fi

COPY . /srv/notera

RUN mkdir -p \
    /srv/notera/app/assets/live_meeting_frames \
    /srv/notera/app/assets/meeting_audio \
    /srv/notera/app/assets/review_audio_clips \
    /srv/notera/bot/meeting_audio \
    /srv/notera/bot/runtime_cache/matplotlib \
    /srv/notera/bot/runtime_cache/huggingface \
    /srv/notera/bot/runtime_cache/xdg

WORKDIR /srv/notera/app

EXPOSE 3000 8000

ENTRYPOINT ["tini", "--"]
CMD ["sh", "-c", "reflex run --env prod --backend-host 0.0.0.0 --frontend-port ${FRONTEND_PORT} --backend-port ${BACKEND_PORT}"]
