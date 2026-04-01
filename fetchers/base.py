import re
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from html import unescape


@dataclass
class RawArticle:
    title: str
    url: str
    source_name: str = ""
    country: str = ""
    language: str = "en"
    published_at: datetime | None = None
    snippet: str = ""
    sentiment_hint: float | None = None  # some sources provide tone


def clean_html(text: str) -> str:
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1000]


def make_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def dedup_articles(articles: list[RawArticle]) -> list[RawArticle]:
    seen = set()
    result = []
    for a in articles:
        h = make_url_hash(a.url)
        if h not in seen:
            seen.add(h)
            result.append(a)
    return result
