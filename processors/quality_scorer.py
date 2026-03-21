"""Data quality scoring — scores each article on multiple dimensions.

Dimensions (0-20 points each, 0-100 total):
1. Completeness: has title, body, author, date, source
2. Freshness: how recent the content is
3. Uniqueness: not a duplicate (checked via dedup hash)
4. Relevance: keyword match to finance/markets
5. Credibility: source reputation tier

Items below threshold (default 30) are filtered out.
Quality scores are stored alongside articles in the database.
"""

import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Minimum quality score to keep an article (0-100)
DEFAULT_THRESHOLD = 30

# Source credibility tiers (0-20 scale)
SOURCE_CREDIBILITY = {
    # Tier 1: Official / highly reputable (18-20)
    "rbi_circulars": 20, "sebi_circulars": 20, "fred_api": 20,
    "rbi_dbie": 20, "sec_edgar": 19, "central_bank": 19,
    "imf_data": 19, "world_bank": 19, "nse_bhavcopy": 18,
    "bse_api": 18, "ccil_rates": 18, "data_gov_in": 18,
    # Tier 2: Major news outlets (14-17)
    "reuters": 17, "bloomberg": 17, "cnbc": 16, "moneycontrol": 15,
    "coindesk": 15, "rss": 14, "web": 13,
    # Tier 3: Social/community (8-13)
    "twitter": 12, "hackernews": 12, "reddit": 11, "github": 11,
    "telegram": 10, "mastodon": 10, "youtube": 10, "discord": 9,
    # Tier 4: Unverified (4-7)
    "darkweb": 5, "unknown": 7,
}

# Finance/market relevance keywords (weighted)
RELEVANCE_KEYWORDS = {
    # High relevance (3 points each)
    "interest rate": 3, "monetary policy": 3, "rbi": 3, "federal reserve": 3,
    "inflation": 3, "gdp": 3, "repo rate": 3, "fiscal deficit": 3,
    "treasury": 3, "bond yield": 3, "credit policy": 3,
    # Medium relevance (2 points each)
    "stock market": 2, "nifty": 2, "sensex": 2, "ipo": 2, "earnings": 2,
    "bitcoin": 2, "cryptocurrency": 2, "forex": 2, "crude oil": 2,
    "defi": 2, "sec filing": 2, "quarterly results": 2,
    "rupee": 2, "dollar": 2, "fii": 2, "mutual fund": 2,
    # Low relevance (1 point each)
    "market": 1, "bank": 1, "finance": 1, "economy": 1, "trade": 1,
    "investment": 1, "portfolio": 1, "hedge fund": 1, "venture capital": 1,
    "crypto": 1, "blockchain": 1, "regulation": 1, "compliance": 1,
}


class QualityScorer:
    """Scores articles on a 0-100 quality scale across five dimensions."""

    def __init__(self, threshold: int = DEFAULT_THRESHOLD):
        self.threshold = threshold
        self._seen_hashes: set = set()
        self._stats = {
            "total_scored": 0,
            "passed": 0,
            "filtered": 0,
            "avg_score": 0.0,
            "dimension_avgs": {
                "completeness": 0.0,
                "freshness": 0.0,
                "uniqueness": 0.0,
                "relevance": 0.0,
                "credibility": 0.0,
            },
        }

    def score_completeness(self, article: dict) -> int:
        """0-20: Does the article have all key fields?"""
        points = 0
        if article.get("title") and len(article["title"].strip()) > 5:
            points += 4
        if article.get("full_text") and len(article["full_text"].strip()) > 50:
            points += 6
        elif article.get("full_text") and len(article["full_text"].strip()) > 10:
            points += 3
        if article.get("author"):
            points += 3
        if article.get("published_at"):
            points += 4
        if article.get("source"):
            points += 3
        return min(points, 20)

    def score_freshness(self, article: dict) -> int:
        """0-20: How recent is the content?"""
        published = article.get("published_at")
        if not published:
            return 5  # Unknown age, give partial credit

        if isinstance(published, str):
            try:
                published = datetime.fromisoformat(published.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return 5

        if not isinstance(published, datetime):
            return 5

        now = datetime.now(timezone.utc)
        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)

        age = now - published
        if age < timedelta(hours=1):
            return 20
        elif age < timedelta(hours=6):
            return 17
        elif age < timedelta(hours=24):
            return 14
        elif age < timedelta(days=3):
            return 10
        elif age < timedelta(days=7):
            return 6
        elif age < timedelta(days=30):
            return 3
        return 1

    def score_uniqueness(self, article: dict) -> int:
        """0-20: Is this a duplicate of something we've already seen?"""
        # Build content hash from title + first 200 chars of body
        title = (article.get("title") or "").strip().lower()
        body = (article.get("full_text") or "")[:200].strip().lower()
        content = f"{title}|{body}"
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

        if content_hash in self._seen_hashes:
            return 0  # Exact duplicate
        self._seen_hashes.add(content_hash)

        # Cap in-memory set
        if len(self._seen_hashes) > 100000:
            # Evict oldest half
            items = list(self._seen_hashes)
            self._seen_hashes = set(items[len(items) // 2:])

        # Check URL-based duplication
        url_hash = article.get("url_hash")
        if url_hash and url_hash in self._seen_hashes:
            return 5  # Same URL, slightly different content
        if url_hash:
            self._seen_hashes.add(url_hash)

        return 20

    def score_relevance(self, article: dict) -> int:
        """0-20: How relevant is this to finance/markets?"""
        text = (
            (article.get("title") or "") + " " +
            (article.get("full_text") or "")[:1000]
        ).lower()

        if not text.strip():
            return 0

        total_points = 0
        for keyword, weight in RELEVANCE_KEYWORDS.items():
            if keyword in text:
                total_points += weight

        # Scale: 0 points -> 0, 5+ points -> 20
        return min(int(total_points * 4), 20)

    def score_credibility(self, article: dict) -> int:
        """0-20: Source reputation score."""
        source = (article.get("source") or "unknown").lower()

        # Direct match
        if source in SOURCE_CREDIBILITY:
            return SOURCE_CREDIBILITY[source]

        # Partial match (e.g., "reuters_business" matches "reuters")
        for key, score in SOURCE_CREDIBILITY.items():
            if key in source or source in key:
                return score

        return SOURCE_CREDIBILITY.get("unknown", 7)

    def score(self, article: dict) -> dict:
        """Score an article across all dimensions. Returns detailed breakdown."""
        completeness = self.score_completeness(article)
        freshness = self.score_freshness(article)
        uniqueness = self.score_uniqueness(article)
        relevance = self.score_relevance(article)
        credibility = self.score_credibility(article)

        total = completeness + freshness + uniqueness + relevance + credibility

        return {
            "total": total,
            "passed": total >= self.threshold,
            "dimensions": {
                "completeness": completeness,
                "freshness": freshness,
                "uniqueness": uniqueness,
                "relevance": relevance,
                "credibility": credibility,
            },
        }

    def score_batch(self, articles: list[dict]) -> list[dict]:
        """Score a batch of articles. Returns list of (article, score) pairs."""
        results = []
        for article in articles:
            score_data = self.score(article)
            self._update_stats(score_data)
            results.append({
                "article": article,
                "quality_score": score_data,
            })
        return results

    def filter_batch(self, articles: list[dict]) -> tuple[list[dict], list[dict]]:
        """Score and filter a batch. Returns (passed, filtered_out)."""
        passed = []
        filtered = []
        for article in articles:
            score_data = self.score(article)
            self._update_stats(score_data)
            article["_quality_score"] = score_data["total"]
            article["_quality_dimensions"] = score_data["dimensions"]
            if score_data["passed"]:
                passed.append(article)
            else:
                filtered.append(article)
        return passed, filtered

    def _update_stats(self, score_data: dict):
        """Update running statistics."""
        self._stats["total_scored"] += 1
        if score_data["passed"]:
            self._stats["passed"] += 1
        else:
            self._stats["filtered"] += 1

        n = self._stats["total_scored"]
        # Running average
        self._stats["avg_score"] = (
            self._stats["avg_score"] * (n - 1) + score_data["total"]
        ) / n

        for dim, val in score_data["dimensions"].items():
            prev = self._stats["dimension_avgs"][dim]
            self._stats["dimension_avgs"][dim] = (prev * (n - 1) + val) / n

    @property
    def stats(self) -> dict:
        return {**self._stats}

    def store_scores(self, scored_articles: list[dict]):
        """Persist quality scores to the database (bulk update)."""
        try:
            from api.database import SessionLocal
            from storage.models import Article

            db = SessionLocal()
            try:
                for item in scored_articles:
                    article = item["article"]
                    score = item["quality_score"]
                    article_id = article.get("id")
                    if article_id:
                        db.query(Article).filter(Article.id == article_id).update(
                            {"category": f"q{score['total']}_{article.get('category', 'news')}"},
                            synchronize_session=False,
                        )
                db.commit()
            finally:
                db.close()
        except Exception as e:
            logger.error(f"[QualityScorer] Store failed: {e}")
