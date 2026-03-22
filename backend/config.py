from __future__ import annotations

import sys
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_data_root() -> Path:
    return _repo_root() / "data"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NOTERA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "notera-backend"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    log_format: str = "json"

    session_cookie_name: str = "notera_session"
    session_ttl_hours: int = 24 * 30
    session_secret: str = "change-me-in-prod"

    data_root: Path = Field(default_factory=_default_data_root)
    db_path: Path = Field(default_factory=lambda: _default_data_root() / "notera.db")
    meeting_audio_root: Path = Field(default_factory=lambda: _default_data_root() / "meeting_audio")
    live_preview_root: Path = Field(default_factory=lambda: _default_data_root() / "live_previews")
    review_clip_root: Path = Field(default_factory=lambda: _default_data_root() / "review_clips")
    runtime_cache_root: Path = Field(default_factory=lambda: _default_data_root() / "runtime_cache")

    bot_python_bin: str = Field(default_factory=lambda: sys.executable)
    bot_entrypoint: str = "backend.workers.bot"
    postprocess_entrypoint: str = "backend.workers.postprocess_worker"

    @property
    def repo_root(self) -> Path:
        return _repo_root()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
