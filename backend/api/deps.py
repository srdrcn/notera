from __future__ import annotations

from fastapi import Cookie, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.db.session import get_db
from backend.models import User
from backend.runtime.logging import bind_context
from backend.services.auth import authenticate_cookie


settings = get_settings()


def current_user(
    request: Request,
    db: Session = Depends(get_db),
    session_cookie: str | None = Cookie(default=None, alias=settings.session_cookie_name),
) -> User:
    user = authenticate_cookie(db, session_cookie)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    bind_context(user_id=user.id)
    return user


def owned_user(user: User = Depends(current_user)) -> User:
    return user
