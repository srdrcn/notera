from __future__ import annotations

from datetime import datetime

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from backend.app.models import AuthSession, User


def get_user_by_email(db: Session, email: str) -> User | None:
    return db.scalar(select(User).where(User.email == email))


def create_user(db: Session, email: str) -> User:
    user = User(email=email)
    db.add(user)
    db.flush()
    return user


def create_session(
    db: Session,
    user_id: int,
    token_hash: str,
    expires_at: datetime,
    user_agent: str | None,
    ip_address: str | None,
) -> AuthSession:
    session_row = AuthSession(
        user_id=user_id,
        token_hash=token_hash,
        expires_at=expires_at,
        user_agent=user_agent,
        ip_address=ip_address,
        last_seen_at=datetime.utcnow(),
    )
    db.add(session_row)
    db.flush()
    return session_row


def get_active_session_by_hash(db: Session, token_hash: str) -> AuthSession | None:
    stmt: Select[tuple[AuthSession]] = select(AuthSession).where(
        AuthSession.token_hash == token_hash,
        AuthSession.revoked_at.is_(None),
    )
    session_row = db.scalar(stmt)
    if session_row is None:
        return None
    if session_row.expires_at <= datetime.utcnow():
        return None
    return session_row


def revoke_session(db: Session, session_row: AuthSession) -> None:
    session_row.revoked_at = datetime.utcnow()
    db.add(session_row)
