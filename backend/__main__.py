from __future__ import annotations

from backend.config import get_settings
import uvicorn


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "backend.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        workers=1,
    )


if __name__ == "__main__":
    main()
