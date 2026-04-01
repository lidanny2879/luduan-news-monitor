import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx

from .base import RawArticle, clean_html, dedup_articles

logger = logging.getLogger(__name__)

# Media Cloud v4 API (search.mediacloud.org)
MEDIACLOUD_API_BASE = "https://search.mediacloud.org/api"


class MediaCloudFetcher:
    def __init__(self, api_key: str, queries: list[str] | None = None):
        self.api_key = api_key
        self.queries = queries or [
            "international relations",
            "global economy",
            "climate change",
            "technology",
            "geopolitics",
        ]

    async def fetch(self, hours: int = 24, max_per_query: int = 100) -> list[RawArticle]:
        if not self.api_key:
            logger.warning("MediaCloud API key not configured, skipping")
            return []

        all_articles = []
        headers = {
            "Authorization": f"Token {self.api_key}",
            "Accept": "application/json",
            "User-Agent": "luduan-news-monitor/1.0",
        }
        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            for i, query in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(3)  # rate limit
                try:
                    articles = await self._fetch_stories(client, query, hours, max_per_query)
                    all_articles.extend(articles)
                    logger.info(f"MediaCloud [{query}]: fetched {len(articles)} articles")
                except Exception as e:
                    logger.error(f"MediaCloud [{query}] error: {e}")
        return dedup_articles(all_articles)

    async def _fetch_stories(
        self, client: httpx.AsyncClient, query: str, hours: int, limit: int
    ) -> list[RawArticle]:
        """Fetch from Media Cloud v4 search/story-list endpoint."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=hours)

        params = {
            "q": query,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": now.strftime("%Y-%m-%d"),
            "page_size": str(min(limit, 100)),
        }

        resp = await client.get(f"{MEDIACLOUD_API_BASE}/search/story-list", params=params)
        resp.raise_for_status()
        data = resp.json()

        articles = []
        stories = data.get("stories", []) if isinstance(data, dict) else data

        for story in stories:
            pub_date = None
            for date_field in ("indexed_date", "publish_date"):
                raw = story.get(date_field)
                if not raw:
                    continue
                try:
                    raw = raw.replace("Z", "+00:00")
                    pub_date = datetime.fromisoformat(raw)
                    if pub_date.tzinfo is None:
                        pub_date = pub_date.replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue

            articles.append(
                RawArticle(
                    title=clean_html(story.get("title", "")),
                    url=story.get("url", ""),
                    source_name=story.get("media_name", story.get("source", "")),
                    country=story.get("media_country", story.get("country", "")),
                    language=story.get("language", "en"),
                    published_at=pub_date,
                    snippet=clean_html(story.get("snippet", story.get("title", ""))),
                )
            )
        return articles
