import os

import reflex as rx

config = rx.Config(
    app_name="app",
    api_url=os.getenv("REFLEX_API_URL", os.getenv("API_URL", "http://localhost:8000")),
    backend_host="0.0.0.0",
    plugins=[
        rx.plugins.SitemapPlugin(),
        rx.plugins.TailwindV4Plugin(),
    ]
)
