import logging
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
import feedparser

from .base import RawArticle, clean_html, dedup_articles

logger = logging.getLogger(__name__)

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"

LANG_REGIONS = [
    ("en", "US", "US:en"),
    ("zh-CN", "CN", "CN:zh-Hans"),
    ("ja", "JP", "JP:ja"),
    ("ko", "KR", "KR:ko"),
    ("fr", "FR", "FR:fr"),
    ("de", "DE", "DE:de"),
    ("es", "ES", "ES:es"),
    ("ru", "RU", "RU:ru"),
    ("ar", "SA", "SA:ar"),
]


class GoogleNewsFetcher:
    def __init__(self, queries: list[str] | None = None, languages: list[str] | None = None):
        self.queries = queries or [
            "world news",
            "global politics",
            "international trade",
            "technology",
        ]
        # Default: English + Chinese + Japanese
        self.lang_regions = [
            lr for lr in LANG_REGIONS if languages is None or lr[0] in languages
        ][:3]

    async def fetch(self, hours: int = 24, max_per_query: int = 100) -> list[RawArticle]:
        all_articles = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            for query in self.queries:
                for hl, gl, ceid in self.lang_regions:
                    try:
                        articles = await self._fetch_rss(client, query, hl, gl, ceid)
                        all_articles.extend(articles[:max_per_query])
                        logger.info(
                            f"GoogleNews [{query}/{hl}]: fetched {len(articles)} articles"
                        )
                    except Exception as e:
                        logger.error(f"GoogleNews [{query}/{hl}] error: {e}")
        return dedup_articles(all_articles)

    async def _fetch_rss(
        self, client: httpx.AsyncClient, query: str, hl: str, gl: str, ceid: str
    ) -> list[RawArticle]:
        url = f"{GOOGLE_NEWS_RSS}?q={quote(query)}&hl={hl}&gl={gl}&ceid={ceid}"
        resp = await client.get(url)
        resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        articles = []
        for entry in feed.entries:
            pub_date = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    pass

            source_name = ""
            if hasattr(entry, "source") and hasattr(entry.source, "title"):
                source_name = entry.source.title

            articles.append(
                RawArticle(
                    title=clean_html(entry.get("title", "")),
                    url=entry.get("link", ""),
                    source_name=source_name,
                    country=gl,
                    language=hl.split("-")[0],
                    published_at=pub_date,
                    snippet=clean_html(entry.get("summary", "")),
                )
            )
        return articles
