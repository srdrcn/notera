from __future__ import annotations

from pydantic import BaseModel, EmailStr


class EmailAuthRequest(BaseModel):
    email: EmailStr


class UserOut(BaseModel):
    id: int
    email: str


class SessionOut(BaseModel):
    user: UserOut
