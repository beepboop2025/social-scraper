"""Twitter/X list monitoring — wraps existing twitter_scraper.py."""

import hashlib, logging
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class TwitterListCollector(BaseCollector):
    name = "twitter_lists"
    source_type = "twitter"

    def __init__(self, config: dict):
        super().__init__(config)
        self.queries = config.get("queries", [])
        self.cookies_path = config.get("cookies_path")

    async def collect(self) -> list[dict]:
        records = []
        try:
            from twitter_scraper import TwitterScraper
            scraper = TwitterScraper(cookies_path=self.cookies_path)
            for query in self.queries:
                try:
                    tweets = await scraper.search(query, limit=20)
                    for t in tweets:
                        records.append({
                            "title": query,
                            "full_text": t.unified.text if hasattr(t, 'unified') else str(t),
                            "url": t.unified.source_url if hasattr(t, 'unified') else "",
                            "author": t.unified.author.username if hasattr(t, 'unified') else "",
                            "published_at": t.unified.created_at.isoformat() if hasattr(t, 'unified') else "",
                            "query": query,
                        })
                except Exception as e:
                    logger.warning(f"[Twitter] Query '{query}' failed: {e}")
        except ImportError:
            logger.warning("[Twitter] twitter_scraper not available")
        logger.info(f"[Twitter] Collected {len(records)} tweets")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            url = r.get("url", "")
            rows.append({
                "title": r.get("title", ""),
                "full_text": r.get("full_text", ""),
                "url": url,
                "url_hash": hashlib.sha256(url.encode()).hexdigest()[:32] if url else None,
                "author": r.get("author", ""),
                "published_at": datetime.fromisoformat(r["published_at"]) if r.get("published_at") else datetime.now(timezone.utc),
                "category": "twitter",
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return True
