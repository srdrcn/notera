from __future__ import annotations

import logging

from fastapi import APIRouter, Body, Depends, HTTPException, Request, Response, status
from sqlalchemy.orm import Session

from backend.api.deps import current_user
from backend.config import get_settings
from backend.db.session import get_db
from backend.runtime.logging import bind_context, log_event
from backend.schemas.auth import EmailAuthRequest, SessionOut, UserOut, normalize_email_input
from backend.services.auth import login_or_register, logout_cookie


router = APIRouter(prefix="/api/auth", tags=["auth"])
settings = get_settings()
logger = logging.getLogger("notera.routes.auth")


def _write_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=settings.session_cookie_name,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=settings.session_ttl_hours * 3600,
        path="/",
    )


@router.post("/register", response_model=SessionOut)
def register(
    request: Request,
    response: Response,
    payload: EmailAuthRequest | None = Body(default=None),
    db: Session = Depends(get_db),
):
    try:
        email = normalize_email_input(payload.email if payload else None)
        result = login_or_register(
            db,
            email,
            register=True,
            user_agent=request.headers.get("user-agent"),
            ip_address=request.client.host if request.client else None,
        )
        db.commit()
    except ValueError as exc:
        log_event(
            logger,
            logging.WARNING,
            "auth.register.rejected",
            "Registration request rejected",
            error_name=type(exc).__name__,
            error_message=str(exc),
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    bind_context(user_id=result.user.id)
    log_event(
        logger,
        logging.INFO,
        "auth.register.succeeded",
        "User registered",
        user_id=result.user.id,
    )
    _write_session_cookie(response, result.token)
    return SessionOut(user=UserOut(id=result.user.id, email=result.user.email))


@router.post("/login", response_model=SessionOut)
def login(
    request: Request,
    response: Response,
    payload: EmailAuthRequest | None = Body(default=None),
    db: Session = Depends(get_db),
):
    try:
        email = normalize_email_input(payload.email if payload else None)
        result = login_or_register(
            db,
            email,
            register=False,
            user_agent=request.headers.get("user-agent"),
            ip_address=request.client.host if request.client else None,
        )
        db.commit()
    except ValueError as exc:
        log_event(
            logger,
            logging.WARNING,
            "auth.login.rejected",
            "Login request rejected",
            error_name=type(exc).__name__,
            error_message=str(exc),
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    bind_context(user_id=result.user.id)
    log_event(
        logger,
        logging.INFO,
        "auth.login.succeeded",
        "User logged in",
        user_id=result.user.id,
    )
    _write_session_cookie(response, result.token)
    return SessionOut(user=UserOut(id=result.user.id, email=result.user.email))


@router.post("/logout")
def logout(
    response: Response,
    request: Request,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    bind_context(user_id=user.id)
    logout_cookie(db, request.cookies.get(settings.session_cookie_name))
    db.commit()
    response.delete_cookie(settings.session_cookie_name, path="/")
    log_event(
        logger,
        logging.INFO,
        "auth.logout.succeeded",
        "User logged out",
        user_id=user.id,
    )
    return {"ok": True}


@router.get("/me", response_model=SessionOut)
def me(user=Depends(current_user)):
    log_event(
        logger,
        logging.DEBUG,
        "auth.session.loaded",
        "Session loaded",
        user_id=user.id,
    )
    return SessionOut(user=UserOut(id=user.id, email=user.email))
