from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.models import User
from backend.repositories.auth import (
    create_session,
    create_user,
    get_active_session_by_hash,
    get_user_by_email,
    revoke_session,
)


settings = get_settings()


def _hash_token(token: str) -> str:
    return hashlib.sha256(f"{settings.session_secret}:{token}".encode("utf-8")).hexdigest()


@dataclass
class AuthResult:
    token: str
    user: User


def login_or_register(
    db: Session,
    email: str,
    register: bool,
    user_agent: str | None,
    ip_address: str | None,
) -> AuthResult:
    existing = get_user_by_email(db, email)
    if register:
        if existing is not None:
            raise ValueError("Bu e-posta adresi zaten kullanımda.")
        user = create_user(db, email)
    else:
        if existing is None:
            raise ValueError("Bu e-posta adresiyle kayıtlı kullanıcı bulunamadı.")
        user = existing

    raw_token = secrets.token_urlsafe(48)
    create_session(
        db,
        user_id=user.id,
        token_hash=_hash_token(raw_token),
        expires_at=datetime.utcnow() + timedelta(hours=settings.session_ttl_hours),
        user_agent=user_agent,
        ip_address=ip_address,
    )
    return AuthResult(token=raw_token, user=user)


def authenticate_cookie(db: Session, raw_token: str | None) -> User | None:
    if not raw_token:
        return None
    session_row = get_active_session_by_hash(db, _hash_token(raw_token))
    if session_row is None:
        return None
    session_row.last_seen_at = datetime.utcnow()
    db.add(session_row)
    return db.get(User, session_row.user_id)


def logout_cookie(db: Session, raw_token: str | None) -> None:
    if not raw_token:
        return
    session_row = get_active_session_by_hash(db, _hash_token(raw_token))
    if session_row is None:
        return
    revoke_session(db, session_row)
