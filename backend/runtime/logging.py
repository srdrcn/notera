from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.config import get_settings


LOG_CONTEXT: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "notera_log_context",
    default={},
)
DEFAULT_EVENT_NAME = "log.recorded"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sanitize_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, BaseException):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_value(item) for item in value]
    return str(value)


def get_log_context() -> dict[str, Any]:
    return dict(LOG_CONTEXT.get())


def bind_context(**fields: Any) -> contextvars.Token[dict[str, Any]]:
    next_context = get_log_context()
    for key, value in fields.items():
        if value is None:
            next_context.pop(key, None)
            continue
        next_context[key] = _sanitize_value(value)
    return LOG_CONTEXT.set(next_context)


def reset_context(token: contextvars.Token[dict[str, Any]]) -> None:
    LOG_CONTEXT.reset(token)


def clear_context() -> None:
    LOG_CONTEXT.set({})


class JsonLogFormatter(logging.Formatter):
    def __init__(self, app_name: str = "backend") -> None:
        super().__init__()
        self.app_name = app_name

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": _timestamp(),
            "level": record.levelname.lower(),
            "app": getattr(record, "app_name", self.app_name),
            "event": getattr(record, "event_name", DEFAULT_EVENT_NAME),
            "message": record.getMessage(),
        }

        payload.update(get_log_context())

        extra_fields = getattr(record, "log_fields", None)
        if isinstance(extra_fields, dict):
            for key, value in extra_fields.items():
                sanitized = _sanitize_value(value)
                if sanitized is None:
                    continue
                payload[key] = sanitized

        if record.exc_info:
            error_type, error, _traceback = record.exc_info
            if error_type is not None:
                payload.setdefault("error_name", error_type.__name__)
            if error is not None:
                payload.setdefault("error_message", str(error))
            payload["traceback"] = self.formatException(record.exc_info)

        return json.dumps(
            {key: value for key, value in payload.items() if value is not None},
            ensure_ascii=False,
        )


def normalize_log_level(value: str | int | None) -> int:
    if isinstance(value, int):
        return value
    candidate = str(value or "INFO").strip().upper()
    return getattr(logging, candidate, logging.INFO)


def configure_logging(force: bool = True) -> None:
    settings = get_settings()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter(app_name="backend"))

    root_logger = logging.getLogger()
    if force:
        root_logger.handlers.clear()
    root_logger.setLevel(normalize_log_level(settings.log_level))
    root_logger.addHandler(handler)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        named_logger = logging.getLogger(logger_name)
        named_logger.handlers.clear()
        named_logger.propagate = True
        named_logger.setLevel(normalize_log_level(settings.log_level))

    for logger_name in ("httpx", "httpcore"):
        named_logger = logging.getLogger(logger_name)
        named_logger.handlers.clear()
        named_logger.propagate = True
        named_logger.setLevel(logging.WARNING)

    logging.captureWarnings(True)


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str,
    *,
    exc_info: Any = None,
    **fields: Any,
) -> None:
    logger.log(
        level,
        message,
        extra={"event_name": event, "log_fields": fields},
        exc_info=exc_info,
    )
