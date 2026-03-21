from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes.auth import router as auth_router
from backend.api.routes.exports import router as exports_router
from backend.api.routes.media import router as media_router
from backend.api.routes.meetings import router as meetings_router
from backend.api.routes.reviews import router as reviews_router
from backend.config import get_settings
from backend.orchestration.supervisor import supervisor
from backend.runtime.bootstrap import ensure_schema


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


def _format_validation_error(exc: RequestValidationError) -> str:
    errors = exc.errors()
    if not errors:
        return "İstek doğrulanamadı."

    first_error = errors[0]
    error_type = first_error.get("type", "")
    location = first_error.get("loc", ())

    if error_type == "missing":
        if "email" in location:
            return "E-posta adresi gerekli."
        return "Gerekli alan eksik."

    if "email" in location:
        return "Geçerli bir e-posta adresi girin."

    message = first_error.get("msg")
    if isinstance(message, str) and message:
        return message
    return "İstek doğrulanamadı."


@app.exception_handler(RequestValidationError)
async def handle_validation_error(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": _format_validation_error(exc)},
    )


@app.get("/health")
def health():
    return {"ok": True}
