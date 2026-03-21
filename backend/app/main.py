from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.api.routes.auth import router as auth_router
from backend.app.api.routes.exports import router as exports_router
from backend.app.api.routes.media import router as media_router
from backend.app.api.routes.meetings import router as meetings_router
from backend.app.api.routes.reviews import router as reviews_router
from backend.app.config import get_settings
from backend.app.orchestration.supervisor import supervisor
from backend.app.runtime.bootstrap import ensure_schema


settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_schema()
    supervisor.reconcile()
    yield


app = FastAPI(title="Notera API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)
app.include_router(meetings_router)
app.include_router(reviews_router)
app.include_router(media_router)
app.include_router(exports_router)


@app.get("/health")
def health():
    return {"ok": True}
