FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FRONTEND_PORT=3000 \
    BACKEND_PORT=8000 \
    API_URL=http://localhost:8000

WORKDIR /srv/notera

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip \
    && python -m pip install -r /tmp/requirements.txt \
    && python -m playwright install --with-deps chromium

COPY . /srv/notera

RUN mkdir -p /srv/notera/app/assets/live_meeting_frames

WORKDIR /srv/notera/app

EXPOSE 3000 8000

CMD ["sh", "-c", "reflex run --env prod --backend-host 0.0.0.0 --frontend-port ${FRONTEND_PORT} --backend-port ${BACKEND_PORT}"]
