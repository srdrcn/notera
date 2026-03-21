from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urlparse


TEAMS_JOIN_WITH_ID_SCHEME = "teams"
TEAMS_JOIN_WITH_ID_NETLOC = "join-with-id"
TEAMS_JOIN_WITH_ID_PAGE_URL = "https://www.microsoft.com/en-us/microsoft-teams/join-a-meeting"

_URL_PATTERN = re.compile(r"https?://[^\s<>\"]+")


def _clean_url_candidate(candidate: str) -> str:
    return candidate.strip().strip("<>").rstrip(").,;!?")


def _is_supported_teams_url(candidate: str) -> bool:
    parsed = urlparse(candidate)
    hostname = (parsed.hostname or "").lower()
    path = parsed.path.lower()

    if hostname == "teams.live.com" or hostname.endswith(".teams.live.com"):
        return True
    if hostname == "teams.microsoft.com" or hostname.endswith(".teams.microsoft.com"):
        return True
    if hostname.endswith("microsoft.com") and "/microsoft-teams/join-a-meeting" in path:
        return True
    return False


def _extract_teams_url(raw_value: str) -> str | None:
    for match in _URL_PATTERN.findall(raw_value):
        candidate = _clean_url_candidate(match)
        if _is_supported_teams_url(candidate):
            return candidate
    return None


def build_join_with_id_target(meeting_id: str, passcode: str) -> str:
    meeting_id_value = re.sub(r"\s+", "", str(meeting_id))
    passcode_value = str(passcode).strip().rstrip(".,;:!?")
    query = urlencode({"meetingId": meeting_id_value, "passcode": passcode_value})
    return f"{TEAMS_JOIN_WITH_ID_SCHEME}://{TEAMS_JOIN_WITH_ID_NETLOC}?{query}"


def parse_join_with_id_target(value: str) -> tuple[str, str] | None:
    parsed = urlparse((value or "").strip())
    if parsed.scheme != TEAMS_JOIN_WITH_ID_SCHEME or parsed.netloc != TEAMS_JOIN_WITH_ID_NETLOC:
        return None

    query = parse_qs(parsed.query)
    meeting_id = re.sub(r"\s+", "", query.get("meetingId", [""])[0])
    passcode = query.get("passcode", [""])[0].strip()
    if not meeting_id or not passcode:
        return None
    return meeting_id, passcode


def normalize_teams_join_target(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError("Teams toplantı linki boş olamaz.")

    teams_url = _extract_teams_url(value)
    if teams_url:
        return teams_url

    raise ValueError("Geçerli bir Teams toplantı linki girin.")
