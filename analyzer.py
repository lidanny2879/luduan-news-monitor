import json
import logging
import re
from collections import Counter
from datetime import datetime, timedelta, timezone, date

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import Article, TopicCluster, TrendSnapshot

logger = logging.getLogger(__name__)

# Stop words for keyword extraction
STOP_WORDS_EN = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "must", "and", "or",
    "but", "if", "then", "than", "that", "this", "these", "those", "it",
    "its", "of", "in", "on", "at", "to", "for", "with", "by", "from",
    "as", "into", "about", "between", "through", "after", "before",
    "during", "without", "under", "over", "up", "down", "out", "off",
    "not", "no", "nor", "so", "very", "too", "also", "just", "more",
    "most", "other", "some", "such", "only", "own", "same", "all",
    "each", "every", "both", "few", "many", "much", "any", "new",
    "said", "says", "news", "report", "reports", "according", "reuters",
    "ap", "afp", "one", "two", "three", "four", "five", "first", "second",
    "people", "year", "years", "time", "day", "days", "week", "month",
    "he", "she", "they", "we", "you", "me", "him", "her", "us", "our",
    "who", "what", "when", "where", "how", "why", "which", "there",
    "here", "now", "get", "got", "go", "make", "made", "take", "come",
    "still", "even", "back", "after", "over", "last", "set", "top",
    "key", "big", "see", "way", "long", "look", "world", "like", "use",
    "de", "la", "le", "les", "des", "du", "en", "et", "un", "une",
    "global", "market", "markets", "update", "latest", "breaking",
    "million", "billion", "percent", "per", "cent", "amid", "says",
    "could", "may", "state", "government", "international", "official",
}
STOP_WORDS_ZH = {
    "的", "了", "在", "是", "和", "与", "对", "为", "中", "年", "月", "日",
    "我", "你", "他", "她", "它", "们", "这", "那", "个", "有", "不", "也",
    "都", "到", "被", "把", "让", "从", "向", "又", "就", "还", "要", "会",
    "能", "可以", "已", "上", "下", "来", "去", "着", "过", "呢", "吗",
    "啊", "吧", "得", "地", "很", "最", "更", "等", "如", "但", "而",
    "所", "以", "因", "或", "其", "之", "于", "及", "发布", "公告",
    "记者", "报道", "新闻", "消息", "表示", "以来", "进行", "通过",
    "已经", "可能", "一个", "今天", "昨天", "今年", "去年",
}
STOP_WORDS_JA = {
    "が", "の", "を", "に", "は", "で", "と", "も", "へ", "や", "か",
    "から", "まで", "より", "など", "して", "する", "した", "です",
    "ます", "ない", "ある", "いる", "れる", "られ", "こと", "もの",
    "ため", "さん", "ください",
}
STOP_WORDS = STOP_WORDS_EN | STOP_WORDS_ZH | STOP_WORDS_JA

# Common media source names to filter from word cloud
MEDIA_NAMES = {
    "搜狐网", "搜狐", "新浪", "新浪财经", "网易", "腾讯", "腾讯网", "凤凰网",
    "百度", "澎湃新闻", "澎湃", "央视", "央视网", "新华社", "新华网", "人民网",
    "人民日报", "环球网", "环球时报", "光明网", "光明日报", "中新网", "中国新闻网",
    "参考消息", "观察者网", "界面新闻", "财新", "第一财经", "经济日报",
    "手机网易网", "搜狐焦点", "中国日报", "chinanews",
    "itbear", "比尔科技", "手机搜狐网",
    "reuters", "associated", "press", "guardian", "times", "washington",
    "post", "york", "cnn", "bbc", "bloomberg", "cnbc",
}


def compute_sentiment(session: Session, batch_size: int = 500) -> int:
    """Compute sentiment for articles that don't have it yet using TextBlob."""
    try:
        from textblob import TextBlob
    except ImportError:
        logger.warning("TextBlob not installed, skipping sentiment analysis")
        return 0

    articles = (
        session.query(Article)
        .filter(Article.sentiment_score.is_(None))
        .limit(batch_size)
        .all()
    )

    count = 0
    for article in articles:
        text = f"{article.title} {article.content_snippet or ''}"
        if not text.strip():
            continue

        # Use GDELT tone hint if available
        if article.source == "gdelt" and article.topics:
            try:
                meta = json.loads(article.topics)
                if "tone_hint" in meta:
                    score = float(meta["tone_hint"]) / 10.0  # normalize to -1..1
                    score = max(-1.0, min(1.0, score))
                    article.sentiment_score = score
                    article.sentiment_label = _score_to_label(score)
                    count += 1
                    continue
            except (json.JSONDecodeError, ValueError):
                pass

        try:
            blob = TextBlob(text)
            score = blob.sentiment.polarity  # -1 to 1
            article.sentiment_score = round(score, 4)
            article.sentiment_label = _score_to_label(score)
            count += 1
        except Exception:
            article.sentiment_score = 0.0
            article.sentiment_label = "neutral"

    session.commit()
    logger.info(f"Computed sentiment for {count} articles")
    return count


def _score_to_label(score: float) -> str:
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    return "neutral"


def compute_trends(session: Session, days: int = 30) -> int:
    """Compute daily trend snapshots."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Clear old snapshots for recomputation
    session.query(TrendSnapshot).filter(
        TrendSnapshot.computed_at < datetime.now(timezone.utc) - timedelta(hours=6)
    ).delete()

    rows = (
        session.query(
            func.date(Article.published_at).label("day"),
            Article.source,
            func.count(Article.id).label("cnt"),
            func.avg(Article.sentiment_score).label("avg_sent"),
        )
        .filter(Article.published_at >= cutoff)
        .group_by(func.date(Article.published_at), Article.source)
        .all()
    )

    count = 0
    for row in rows:
        if row.day is None:
            continue
        day = row.day if isinstance(row.day, date) else datetime.strptime(str(row.day), "%Y-%m-%d").date()
        snapshot = TrendSnapshot(
            date=day,
            source=row.source,
            article_count=row.cnt,
            avg_sentiment=round(row.avg_sent or 0, 4),
            top_keywords="[]",
            computed_at=datetime.now(timezone.utc),
        )
        session.add(snapshot)
        count += 1

    session.commit()
    logger.info(f"Computed {count} trend snapshots")
    return count


def compute_topics(session: Session, days: int = 7, n_clusters: int = 12) -> int:
    """Cluster recent articles into topics using TF-IDF + KMeans."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.cluster import MiniBatchKMeans
    except ImportError:
        logger.warning("scikit-learn not installed, skipping topic clustering")
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    articles = (
        session.query(Article)
        .filter(Article.published_at >= cutoff)
        .all()
    )

    if len(articles) < n_clusters * 2:
        logger.info(f"Not enough articles ({len(articles)}) for clustering")
        return 0

    texts = [f"{a.title} {a.content_snippet or ''}" for a in articles]

    vectorizer = TfidfVectorizer(
        max_features=5000,
        stop_words="english",
        max_df=0.8,
        min_df=2,
    )
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()

    kmeans = MiniBatchKMeans(n_clusters=min(n_clusters, len(articles) // 2), random_state=42)
    labels = kmeans.fit_predict(tfidf_matrix)

    # Clear old clusters
    session.query(TopicCluster).delete()

    count = 0
    for i in range(kmeans.n_clusters):
        center = kmeans.cluster_centers_[i]
        top_indices = center.argsort()[-8:][::-1]
        keywords = [str(feature_names[idx]) for idx in top_indices]
        article_count = int((labels == i).sum())

        cluster = TopicCluster(
            label=keywords[0].title() if keywords else f"Topic {i}",
            keywords=json.dumps(keywords),
            article_count=article_count,
            computed_at=datetime.now(timezone.utc),
        )
        session.add(cluster)
        count += 1

    session.commit()
    logger.info(f"Computed {count} topic clusters")
    return count


def _clean_title(title: str) -> str:
    """Remove trailing media source names from title (e.g. '标题 - 新浪财经')."""
    # Remove " - SourceName" or " | SourceName" at end
    title = re.sub(r'\s*[-|–—]\s*[^-|–—]{2,20}$', '', title)
    # Remove common suffixes like (组图) [视频]
    title = re.sub(r'[(\[（【][^)\]）】]*[)\]）】]', '', title)
    return title.strip()


def extract_keywords(session: Session, days: int = 7, top_n: int = 100) -> list[dict]:
    """Extract top keywords from recent article titles with better multi-language support."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    articles = (
        session.query(Article.title)
        .filter(Article.published_at >= cutoff)
        .all()
    )

    counter = Counter()
    media_lower = {m.lower() for m in MEDIA_NAMES}

    for (title,) in articles:
        title = _clean_title(title)

        # Extract English words (2+ letters)
        en_words = re.findall(r'[a-zA-Z]{3,}', title)
        for w in en_words:
            wl = w.lower()
            if wl not in STOP_WORDS and wl not in media_lower:
                counter[wl] += 1

        # Extract Chinese 2-4 character phrases (simple n-gram approach)
        zh_segments = re.findall(r'[\u4e00-\u9fff]+', title)
        for seg in zh_segments:
            if seg.lower() in media_lower or seg in STOP_WORDS:
                continue
            if 2 <= len(seg) <= 5:
                # Short segment is likely a word/phrase itself
                counter[seg] += 1
            else:
                # For longer segments, extract 2-4 char ngrams
                for n in (4, 3, 2):
                    for i in range(len(seg) - n + 1):
                        gram = seg[i:i+n]
                        if gram not in STOP_WORDS and gram not in media_lower:
                            counter[gram] += 1

        # Extract Japanese katakana words
        ja_words = re.findall(r'[\u30a0-\u30ff]{2,}', title)
        for w in ja_words:
            if w not in STOP_WORDS:
                counter[w] += 1

    # Filter: require minimum frequency and remove media names
    min_freq = max(2, len(articles) // 200)
    filtered = [
        {"name": word, "value": count}
        for word, count in counter.most_common(top_n * 2)
        if count >= min_freq and word.lower() not in media_lower
    ]
    return filtered[:top_n]


def run_full_analysis(session: Session):
    """Run all analysis steps."""
    logger.info("Starting full analysis...")
    compute_sentiment(session)
    compute_trends(session)
    compute_topics(session)
    logger.info("Full analysis complete")
