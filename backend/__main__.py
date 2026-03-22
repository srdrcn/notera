from __future__ import annotations

import uvicorn

from backend.config import get_settings
from backend.runtime.logging import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging()
    uvicorn.run(
        "backend.main:app",
        host=settings.api_host,
        port=settings.api_port,
        access_log=False,
        log_config=None,
        log_level=settings.log_level.lower(),
        reload=False,
        workers=1,
    )


if __name__ == "__main__":
    main()
