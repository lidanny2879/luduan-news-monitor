import os
from pathlib import Path
from dotenv import load_dotenv

_BASE_DIR = Path(__file__).resolve().parent
load_dotenv(_BASE_DIR / ".env", override=False)  # optional; env vars take priority


class Settings:
    _base_dir = os.path.dirname(os.path.abspath(__file__))
    _default_db = f"sqlite:///{os.path.join(_base_dir, 'data', 'news_monitor.db')}"
    DATABASE_URL: str = os.getenv("DATABASE_URL", _default_db)

    MEDIACLOUD_API_KEY: str = os.getenv("MEDIACLOUD_API_KEY", "")
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    SEARCH_QUERIES: list[str] = [
        q.strip()
        for q in os.getenv(
            "SEARCH_QUERIES",
            "international relations,global economy,technology,climate change,geopolitics,AI,trade war",
        ).split(",")
    ]
    GOOGLE_NEWS_QUERIES: list[str] = [
        q.strip()
        for q in os.getenv(
            "GOOGLE_NEWS_QUERIES",
            "world news,global politics,breaking news,international trade",
        ).split(",")
    ]

    MEDIACLOUD_INTERVAL: int = int(os.getenv("MEDIACLOUD_INTERVAL", "360"))
    GOOGLE_NEWS_INTERVAL: int = int(os.getenv("GOOGLE_NEWS_INTERVAL", "120"))
    GDELT_INTERVAL: int = int(os.getenv("GDELT_INTERVAL", "60"))
    ANALYSIS_INTERVAL: int = int(os.getenv("ANALYSIS_INTERVAL", "180"))

    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))


settings = Settings()
