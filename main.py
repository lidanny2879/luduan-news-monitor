import json
import logging
import asyncio
from datetime import datetime, timezone, timedelta, date
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, desc
from sqlalchemy.orm import Session
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import settings
from models import Article, FetchLog, TopicCluster, TrendSnapshot, init_db, get_session
from fetchers import GDELTFetcher, GoogleNewsFetcher, MediaCloudFetcher
from analyzer import (
    compute_sentiment,
    compute_trends,
    compute_topics,
    extract_keywords,
    run_full_analysis,
)
from fetchers.base import make_url_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("news-monitor")

scheduler = AsyncIOScheduler()
SessionLocal = None


# --- Fetch Jobs ---

async def _save_articles(articles, source_name: str, search_keywords: list[str] | None = None):
    """Save fetched articles to database. If search_keywords provided, tag articles."""
    session = SessionLocal()
    started = datetime.now(timezone.utc)
    log = FetchLog(source=source_name, started_at=started, status="running")
    session.add(log)
    session.commit()

    kw_tag = ",".join(search_keywords) if search_keywords else None
    new_count = 0
    updated_count = 0
    try:
        for raw in articles:
            url_hash = make_url_hash(raw.url)
            existing = session.query(Article).filter(Article.url_hash == url_hash).first()
            if existing:
                # Update search_keywords on existing article if we have new keywords
                if kw_tag and existing.search_keywords:
                    existing_kws = set(existing.search_keywords.split(","))
                    new_kws = set(kw_tag.split(","))
                    merged = existing_kws | new_kws
                    if merged != existing_kws:
                        existing.search_keywords = ",".join(merged)
                        updated_count += 1
                elif kw_tag and not existing.search_keywords:
                    existing.search_keywords = kw_tag
                    updated_count += 1
                continue

            meta = {}
            if raw.sentiment_hint is not None:
                meta["tone_hint"] = raw.sentiment_hint

            article = Article(
                url_hash=url_hash,
                title=raw.title,
                url=raw.url,
                source=source_name,
                source_name=raw.source_name,
                country=raw.country,
                language=raw.language,
                published_at=raw.published_at,
                fetched_at=datetime.now(timezone.utc),
                content_snippet=raw.snippet,
                topics=json.dumps(meta) if meta else None,
                search_keywords=kw_tag,
            )
            session.add(article)
            new_count += 1

        session.commit()
        log.finished_at = datetime.now(timezone.utc)
        log.articles_found = len(articles)
        log.articles_new = new_count
        log.status = "success"
        session.commit()
        logger.info(f"[{source_name}] Saved {new_count} new, updated {updated_count} articles (total fetched: {len(articles)})")
    except Exception as e:
        session.rollback()
        log.status = "error"
        log.error_message = str(e)[:500]
        log.finished_at = datetime.now(timezone.utc)
        session.commit()
        logger.error(f"[{source_name}] Error saving: {e}")
    finally:
        session.close()


async def fetch_gdelt_job():
    fetcher = GDELTFetcher(queries=settings.SEARCH_QUERIES)
    articles = await fetcher.fetch(hours=24)
    await _save_articles(articles, "gdelt")


async def fetch_google_news_job():
    fetcher = GoogleNewsFetcher(queries=settings.GOOGLE_NEWS_QUERIES)
    articles = await fetcher.fetch(hours=24)
    await _save_articles(articles, "google_news")


async def fetch_mediacloud_job():
    fetcher = MediaCloudFetcher(api_key=settings.MEDIACLOUD_API_KEY, queries=settings.SEARCH_QUERIES)
    articles = await fetcher.fetch(hours=24)
    await _save_articles(articles, "mediacloud")


async def analysis_job():
    session = SessionLocal()
    try:
        run_full_analysis(session)
    finally:
        session.close()


# --- App Lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    global SessionLocal
    SessionLocal = init_db(settings.DATABASE_URL)
    logger.info("Database initialized")

    # Schedule periodic jobs
    scheduler.add_job(fetch_gdelt_job, "interval", minutes=settings.GDELT_INTERVAL, id="gdelt", next_run_time=datetime.now())
    scheduler.add_job(fetch_google_news_job, "interval", minutes=settings.GOOGLE_NEWS_INTERVAL, id="google_news", next_run_time=datetime.now())
    scheduler.add_job(fetch_mediacloud_job, "interval", minutes=settings.MEDIACLOUD_INTERVAL, id="mediacloud", next_run_time=datetime.now())
    scheduler.add_job(analysis_job, "interval", minutes=settings.ANALYSIS_INTERVAL, id="analysis", next_run_time=datetime.now() + timedelta(minutes=2))
    scheduler.start()
    logger.info("Scheduler started")

    yield

    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(title="Luduan Global News Monitor", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


# --- Helper ---

def _get_db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# --- Routes ---

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/stats")
def get_stats(
    days: int = Query(30, ge=1, le=365),
    source: str = Query(None),
    db: Session = Depends(_get_db),
):
    total = db.query(func.count(Article.id)).scalar() or 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    range_q = db.query(Article).filter(Article.published_at >= cutoff)
    if source:
        range_q = range_q.filter(Article.source == source)

    articles_in_range = range_q.count()

    source_q = db.query(Article.source, func.count(Article.id)).filter(Article.published_at >= cutoff)
    if source:
        source_q = source_q.filter(Article.source == source)
    source_counts = dict(source_q.group_by(Article.source).all())

    country_q = (
        db.query(Article.country, func.count(Article.id))
        .filter(Article.published_at >= cutoff, Article.country.isnot(None), Article.country != "")
    )
    if source:
        country_q = country_q.filter(Article.source == source)
    country_counts = dict(
        country_q.group_by(Article.country)
        .order_by(desc(func.count(Article.id)))
        .limit(20)
        .all()
    )

    sent_q = db.query(func.avg(Article.sentiment_score)).filter(
        Article.published_at >= cutoff, Article.sentiment_score.isnot(None)
    )
    if source:
        sent_q = sent_q.filter(Article.source == source)
    avg_sentiment = sent_q.scalar()

    dist_q = (
        db.query(Article.sentiment_label, func.count(Article.id))
        .filter(Article.published_at >= cutoff, Article.sentiment_label.isnot(None))
    )
    if source:
        dist_q = dist_q.filter(Article.source == source)
    sentiment_dist = dict(dist_q.group_by(Article.sentiment_label).all())

    last_fetch = (
        db.query(FetchLog)
        .filter(FetchLog.status == "success")
        .order_by(desc(FetchLog.finished_at))
        .first()
    )

    return {
        "total_articles": total,
        "articles_in_range": articles_in_range,
        "source_counts": source_counts,
        "country_counts": country_counts,
        "avg_sentiment": round(avg_sentiment or 0, 4),
        "sentiment_distribution": sentiment_dist,
        "last_fetch": last_fetch.finished_at.isoformat() if last_fetch else None,
    }


@app.get("/api/articles")
def get_articles(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    source: str = Query(None),
    query: str = Query(None),
    country: str = Query(None),
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(_get_db),
):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    q = db.query(Article).filter(Article.published_at >= cutoff)

    if source:
        q = q.filter(Article.source == source)
    if country:
        q = q.filter(Article.country == country)
    if query:
        q = q.filter(Article.title.ilike(f"%{query}%"))

    total = q.count()
    articles = q.order_by(desc(Article.published_at)).offset((page - 1) * size).limit(size).all()

    return {
        "total": total,
        "page": page,
        "size": size,
        "articles": [
            {
                "id": a.id,
                "title": a.title,
                "url": a.url,
                "source": a.source,
                "source_name": a.source_name,
                "country": a.country,
                "language": a.language,
                "published_at": a.published_at.isoformat() if a.published_at else None,
                "sentiment_score": a.sentiment_score,
                "sentiment_label": a.sentiment_label,
                "snippet": a.content_snippet,
            }
            for a in articles
        ],
    }


@app.get("/api/trends")
def get_trends(days: int = Query(30, ge=1, le=365), db: Session = Depends(_get_db)):
    cutoff = date.today() - timedelta(days=days)
    snapshots = (
        db.query(TrendSnapshot)
        .filter(TrendSnapshot.date >= cutoff)
        .order_by(TrendSnapshot.date)
        .all()
    )

    data = {}
    for s in snapshots:
        day_str = s.date.isoformat() if isinstance(s.date, date) else str(s.date)
        if day_str not in data:
            data[day_str] = {}
        data[day_str][s.source] = {
            "count": s.article_count,
            "sentiment": round(s.avg_sentiment or 0, 4),
        }

    return {"trends": data}


@app.get("/api/sentiment")
def get_sentiment(days: int = Query(30, ge=1, le=365), db: Session = Depends(_get_db)):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (
        db.query(
            func.date(Article.published_at).label("day"),
            Article.sentiment_label,
            func.count(Article.id).label("cnt"),
        )
        .filter(Article.published_at >= cutoff, Article.sentiment_label.isnot(None))
        .group_by(func.date(Article.published_at), Article.sentiment_label)
        .all()
    )

    data = {}
    for row in rows:
        day_str = str(row.day)
        if day_str not in data:
            data[day_str] = {"positive": 0, "negative": 0, "neutral": 0}
        if row.sentiment_label in data[day_str]:
            data[day_str][row.sentiment_label] = row.cnt

    return {"sentiment": data}


@app.get("/api/topics")
def get_topics(db: Session = Depends(_get_db)):
    clusters = db.query(TopicCluster).order_by(desc(TopicCluster.article_count)).all()
    return {
        "topics": [
            {
                "label": c.label,
                "keywords": json.loads(c.keywords) if c.keywords else [],
                "article_count": c.article_count,
            }
            for c in clusters
        ]
    }


@app.get("/api/wordcloud")
def get_wordcloud(days: int = Query(7, ge=1, le=90), db: Session = Depends(_get_db)):
    keywords = extract_keywords(db, days=days)
    return {"keywords": keywords}


@app.get("/api/keyword_search")
def keyword_search(
    keywords: str = Query(..., description="Comma-separated keywords"),
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(_get_db),
):
    """Search articles by multi-language keywords (OR logic across comma-separated terms)."""
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    if not kw_list:
        return {"total": 0, "sentiment": {}, "by_source": {}, "by_country": {}, "daily_trend": {}}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    from sqlalchemy import or_
    conditions = [Article.title.ilike(f"%{kw}%") for kw in kw_list]
    conditions += [Article.content_snippet.ilike(f"%{kw}%") for kw in kw_list]
    conditions += [Article.search_keywords.ilike(f"%{kw}%") for kw in kw_list]

    base_q = db.query(Article).filter(Article.published_at >= cutoff, or_(*conditions))

    # Total count
    total = base_q.count()

    # Sentiment breakdown
    sent_rows = (
        base_q.with_entities(Article.sentiment_label, func.count(Article.id))
        .filter(Article.sentiment_label.isnot(None))
        .group_by(Article.sentiment_label)
        .all()
    )
    sentiment = {"positive": 0, "neutral": 0, "negative": 0}
    for label, cnt in sent_rows:
        if label in sentiment:
            sentiment[label] = cnt

    # By source
    source_rows = (
        base_q.with_entities(Article.source, func.count(Article.id))
        .group_by(Article.source)
        .all()
    )
    by_source = dict(source_rows)

    # By country
    country_rows = (
        base_q.with_entities(Article.country, func.count(Article.id))
        .filter(Article.country.isnot(None), Article.country != "")
        .group_by(Article.country)
        .order_by(desc(func.count(Article.id)))
        .limit(15)
        .all()
    )
    by_country = dict(country_rows)

    # Daily trend
    trend_rows = (
        base_q.with_entities(func.date(Article.published_at).label("day"), func.count(Article.id).label("cnt"))
        .group_by(func.date(Article.published_at))
        .order_by(func.date(Article.published_at))
        .all()
    )
    daily_trend = {str(row.day): row.cnt for row in trend_rows if row.day}

    return {
        "keywords": kw_list,
        "days": days,
        "total": total,
        "sentiment": sentiment,
        "by_source": by_source,
        "by_country": by_country,
        "daily_trend": daily_trend,
    }


@app.post("/api/keyword_fetch")
async def keyword_fetch(keywords: str = Query(..., description="Comma-separated keywords")):
    """Live fetch from all sources for specific keywords, then save to DB."""
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    if not kw_list:
        return {"status": "error", "message": "No keywords provided"}

    results = {}
    # Fetch from GDELT (no API key needed)
    try:
        gdelt = GDELTFetcher(queries=kw_list)
        articles = await gdelt.fetch(hours=720, max_records=250)  # 30 days
        await _save_articles(articles, "gdelt", search_keywords=kw_list)
        results["gdelt"] = len(articles)
    except Exception as e:
        results["gdelt"] = f"error: {e}"
        logger.error(f"Keyword fetch GDELT error: {e}")

    # Fetch from Google News
    try:
        google = GoogleNewsFetcher(queries=kw_list)
        articles = await google.fetch(hours=720)
        await _save_articles(articles, "google_news", search_keywords=kw_list)
        results["google_news"] = len(articles)
    except Exception as e:
        results["google_news"] = f"error: {e}"
        logger.error(f"Keyword fetch Google error: {e}")

    # Fetch from MediaCloud
    try:
        mc = MediaCloudFetcher(api_key=settings.MEDIACLOUD_API_KEY, queries=kw_list)
        articles = await mc.fetch(hours=720)
        await _save_articles(articles, "mediacloud", search_keywords=kw_list)
        results["mediacloud"] = len(articles)
    except Exception as e:
        results["mediacloud"] = f"error: {e}"
        logger.error(f"Keyword fetch MediaCloud error: {e}")

    # Run sentiment analysis on new articles
    asyncio.create_task(analysis_job())

    return {"status": "ok", "keywords": kw_list, "fetched": results}


@app.get("/api/sources")
def get_sources(db: Session = Depends(_get_db)):
    rows = (
        db.query(
            Article.source,
            func.count(Article.id).label("count"),
            func.avg(Article.sentiment_score).label("avg_sentiment"),
            func.max(Article.fetched_at).label("last_fetch"),
        )
        .group_by(Article.source)
        .all()
    )
    return {
        "sources": [
            {
                "name": r.source,
                "count": r.count,
                "avg_sentiment": round(r.avg_sentiment or 0, 4),
                "last_fetch": r.last_fetch.isoformat() if r.last_fetch else None,
            }
            for r in rows
        ]
    }


@app.post("/api/fetch/{source}")
async def trigger_fetch(source: str):
    jobs = {
        "gdelt": fetch_gdelt_job,
        "google_news": fetch_google_news_job,
        "mediacloud": fetch_mediacloud_job,
        "analysis": analysis_job,
    }
    if source not in jobs:
        return JSONResponse({"error": f"Unknown source: {source}"}, status_code=400)

    asyncio.create_task(jobs[source]())
    return {"status": "started", "source": source}


@app.get("/api/jobs")
def get_jobs():
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
        })
    return {"jobs": jobs}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.HOST, port=settings.PORT, reload=True)
