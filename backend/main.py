from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from backend.api.routes.auth import router as auth_router
from backend.api.routes.exports import router as exports_router
from backend.api.routes.media import router as media_router
from backend.api.routes.meetings import router as meetings_router
from backend.api.routes.reviews import router as reviews_router
from backend.config import get_settings
from backend.orchestration.supervisor import supervisor
from backend.runtime.bootstrap import ensure_schema
from backend.runtime.logging import bind_context, configure_logging, log_event, reset_context


settings = get_settings()
configure_logging()
logger = logging.getLogger("notera.api")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_schema()
    supervisor.reconcile()
    log_event(logger, logging.INFO, "app.started", "Backend application started")
    yield
    log_event(logger, logging.INFO, "app.stopped", "Backend application stopped")


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


def _request_log_level(status_code: int) -> int:
    if status_code >= 500:
        return logging.ERROR
    if status_code >= 400:
        return logging.WARNING
    return logging.INFO


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid4().hex
    request.state.request_id = request_id
    started_at = perf_counter()
    token = bind_context(request_id=request_id)
    try:
        log_event(
            logger,
            logging.DEBUG,
            "http.request.started",
            "HTTP request started",
            method=request.method,
            path=request.url.path,
        )
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        log_event(
            logger,
            _request_log_level(response.status_code),
            "http.request.completed",
            "HTTP request completed" if response.status_code < 400 else "HTTP request failed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        return response
    finally:
        reset_context(token)


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
async def handle_validation_error(request: Request, exc: RequestValidationError):
    log_event(
        logger,
        logging.WARNING,
        "http.request.validation_failed",
        "Request validation failed",
        path=request.url.path,
        method=request.method,
        status_code=status.HTTP_400_BAD_REQUEST,
        error_name=type(exc).__name__,
        error_message=_format_validation_error(exc),
        validation_error_count=len(exc.errors()),
    )
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": _format_validation_error(exc)},
    )


@app.exception_handler(Exception)
async def handle_unexpected_error(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        raise exc

    log_event(
        logger,
        logging.ERROR,
        "http.request.exception",
        "Unhandled request exception",
        path=request.url.path,
        method=request.method,
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_name=type(exc).__name__,
        error_message=str(exc),
        exc_info=exc,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


@app.get("/health")
def health():
    return {"ok": True}
