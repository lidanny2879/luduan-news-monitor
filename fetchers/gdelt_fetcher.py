import asyncio
import logging
from datetime import datetime, timezone

import httpx

from .base import RawArticle, clean_html, dedup_articles

logger = logging.getLogger(__name__)

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

# FIPS country code -> ISO 2-letter
FIPS_TO_ISO = {
    "US": "US", "UK": "GB", "CH": "CN", "JA": "JP", "GM": "DE",
    "FR": "FR", "RS": "RU", "IN": "IN", "KS": "KR", "BR": "BR",
    "AS": "AU", "CA": "CA", "IT": "IT", "SP": "ES", "MX": "MX",
    "IS": "IL", "TW": "TW", "SA": "SA", "TU": "TR", "ID": "ID",
    "PK": "PK", "EG": "EG", "NI": "NG", "SF": "ZA", "TH": "TH",
    "VM": "VN", "PL": "PL", "UP": "UA", "SW": "SE", "NO": "NO",
}


class GDELTFetcher:
    def __init__(self, queries: list[str] | None = None):
        self.queries = queries or [
            "international relations",
            "global economy",
            "geopolitics",
            "climate change",
            "technology AI",
        ]

    async def fetch(self, hours: int = 24, max_records: int = 250) -> list[RawArticle]:
        all_articles = []
        async with httpx.AsyncClient(timeout=30) as client:
            for i, query in enumerate(self.queries):
                if i > 0:
                    await asyncio.sleep(2)  # rate limit: max ~30 req/min
                try:
                    articles = await self._fetch_query(client, query, hours, max_records)
                    all_articles.extend(articles)
                    logger.info(f"GDELT [{query}]: fetched {len(articles)} articles")
                except Exception as e:
                    logger.error(f"GDELT [{query}] error: {e}")
        return dedup_articles(all_articles)

    async def _fetch_query(
        self, client: httpx.AsyncClient, query: str, hours: int, max_records: int
    ) -> list[RawArticle]:
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": str(max_records),
            "timespan": f"{hours}h" if hours <= 720 else "720h",
            "format": "json",
            "sort": "datedesc",
        }
        resp = await client.get(GDELT_API, params=params)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or text[0] != '{':
            logger.debug(f"GDELT empty/invalid response for query: {query}")
            return []
        data = resp.json()

        articles = []
        for item in data.get("articles", []):
            pub_date = None
            if item.get("seendate"):
                try:
                    pub_date = datetime.strptime(item["seendate"], "%Y%m%dT%H%M%SZ").replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    pass

            country_fips = (item.get("sourcecountry") or "")[:2].upper()
            country_iso = FIPS_TO_ISO.get(country_fips, country_fips)

            tone = None
            if item.get("tone"):
                try:
                    tone = float(str(item["tone"]).split(",")[0])
                except (ValueError, IndexError):
                    pass

            articles.append(
                RawArticle(
                    title=clean_html(item.get("title", "")),
                    url=item.get("url", ""),
                    source_name=item.get("domain", ""),
                    country=country_iso,
                    language=item.get("language", "English")[:2].lower(),
                    published_at=pub_date,
                    snippet=clean_html(item.get("title", "")),
                    sentiment_hint=tone,
                )
            )
        return articles
