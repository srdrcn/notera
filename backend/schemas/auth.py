from __future__ import annotations

from pydantic import BaseModel, EmailStr, TypeAdapter, ValidationError


_EMAIL_ADAPTER = TypeAdapter(EmailStr)


class EmailAuthRequest(BaseModel):
    email: str | None = None


def normalize_email_input(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if not normalized:
        raise ValueError("E-posta adresi gerekli.")
    try:
        return _EMAIL_ADAPTER.validate_python(normalized)
    except ValidationError as exc:
        raise ValueError("Geçerli bir e-posta adresi girin.") from exc


class UserOut(BaseModel):
    id: int
    email: str


class SessionOut(BaseModel):
    user: UserOut
