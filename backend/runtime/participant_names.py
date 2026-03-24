from __future__ import annotations

import re


_ROSTER_HEADING_PATTERNS = (
    re.compile(r"^(?:in|not in) this meeting(?:\s*\(\d+\))?$", re.IGNORECASE),
    re.compile(r"^(?:bu toplantıda|bu toplantida)(?:\s*\(\d+\))?$", re.IGNORECASE),
    re.compile(r"^(?:bu toplantıda değil|bu toplantida degil)(?:\s*\(\d+\))?$", re.IGNORECASE),
    re.compile(r"^(?:participants?|people|katılımcılar|katilimcilar|kişiler|kisiler)(?:\s*\(\d+\))?$", re.IGNORECASE),
    re.compile(r"^\d+\s+(?:participants?|people|katılımcı(?:lar)?|katilimci(?:lar)?)$", re.IGNORECASE),
)


def normalize_participant_name(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def is_roster_heading_name(value: str | None) -> bool:
    normalized = normalize_participant_name(value)
    if not normalized:
        return False
    return any(pattern.fullmatch(normalized) for pattern in _ROSTER_HEADING_PATTERNS)
