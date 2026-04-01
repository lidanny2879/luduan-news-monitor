import hashlib
from datetime import datetime, date

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Date,
    Text,
    Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class Article(Base):
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url_hash = Column(String(64), unique=True, index=True)
    title = Column(String(500), nullable=False)
    url = Column(String(2000), nullable=False)
    source = Column(String(50), nullable=False)  # mediacloud / google_news / gdelt
    source_name = Column(String(200))  # e.g. Reuters, BBC
    country = Column(String(10))
    language = Column(String(10))
    published_at = Column(DateTime, index=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    content_snippet = Column(Text)
    sentiment_score = Column(Float, nullable=True)
    sentiment_label = Column(String(20), nullable=True)
    topics = Column(Text, nullable=True)
    search_keywords = Column(Text, nullable=True)  # comma-separated keywords that found this article

    __table_args__ = (
        Index("idx_source_published", "source", "published_at"),
        Index("idx_country", "country"),
    )

    @staticmethod
    def make_url_hash(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()


class FetchLog(Base):
    __tablename__ = "fetch_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime)
    articles_found = Column(Integer, default=0)
    articles_new = Column(Integer, default=0)
    status = Column(String(20), default="running")
    error_message = Column(Text, nullable=True)


class TopicCluster(Base):
    __tablename__ = "topic_clusters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(String(200))
    keywords = Column(Text)  # JSON list
    article_count = Column(Integer)
    computed_at = Column(DateTime, default=datetime.utcnow)


class TrendSnapshot(Base):
    __tablename__ = "trend_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, index=True)
    source = Column(String(50))
    article_count = Column(Integer)
    avg_sentiment = Column(Float)
    top_keywords = Column(Text)  # JSON
    computed_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_trend_date_source", "date", "source"),)


_engine = None
_SessionLocal = None


def init_db(db_url: str = "sqlite:///data/news_monitor.db"):
    global _engine, _SessionLocal
    # Ensure the directory for the SQLite file exists
    if db_url.startswith("sqlite:///"):
        import os
        db_path = db_url.replace("sqlite:///", "")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(_engine)
    _SessionLocal = sessionmaker(bind=_engine)
    return _SessionLocal


def get_session():
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    session = _SessionLocal()
    try:
        yield session
    finally:
        session.close()
