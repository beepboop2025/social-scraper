"""Telegram channel collector — wraps existing telegram_scraper.py for the plugin system."""

import hashlib, logging
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class TelegramCollector(BaseCollector):
    name = "telegram_channels"
    source_type = "telegram"

    def __init__(self, config: dict):
        super().__init__(config)
        self.channels = config.get("channels", [])
        self.api_id = config.get("api_id")
        self.api_hash = config.get("api_hash")

    async def collect(self) -> list[dict]:
        records = []
        try:
            from telegram_scraper import TelegramScraper
            scraper = TelegramScraper(
                api_id=int(self.api_id) if self.api_id else None,
                api_hash=self.api_hash,
            )
            for channel in self.channels:
                try:
                    messages = await scraper.scrape_channel(channel, limit=20)
                    for msg in messages:
                        records.append({
                            "title": "",
                            "full_text": msg.unified.text if hasattr(msg, 'unified') else str(msg),
                            "url": msg.unified.source_url if hasattr(msg, 'unified') else "",
                            "author": channel,
                            "published_at": msg.unified.created_at.isoformat() if hasattr(msg, 'unified') else "",
                            "channel": channel,
                        })
                except Exception as e:
                    logger.warning(f"[Telegram] Channel {channel} failed: {e}")
        except ImportError:
            logger.warning("[Telegram] telegram_scraper not available")
        logger.info(f"[Telegram] Collected {len(records)} messages")
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
                "category": "telegram",
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return True
