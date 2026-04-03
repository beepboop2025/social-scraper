"""LiquiFi connector — pushes treasury-relevant social intelligence.

LiquiFi is an Indian treasury management dashboard that cares about:
- RBI announcements (repo rate, CRR, SLR, MIBOR)
- USD/INR movements
- SOFR updates
- Interest rate discussions
- Indian banking sector news
- Treasury bond market sentiment
- Counterparty risk signals

This connector filters scraped data for treasury relevance and pushes
it to LiquiFi's WebSocket /ws/rates stream or REST API.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from models import Platform, ScrapedItem

logger = logging.getLogger(__name__)

# Treasury-relevant keywords for LiquiFi
TREASURY_KEYWORDS = {
    "rates": [
        "repo rate", "reverse repo", "mibor", "sofr", "libor",
        "overnight rate", "call money", "cblo", "interest rate",
        "policy rate", "bank rate", "cds rate", "cp rate",
    ],
    "regulatory": [
        "crr", "slr", "lcr", "nsfr", "alm",
        "rbi circular", "rbi notification", "monetary policy",
        "credit policy", "statutory ratio",
    ],
    "forex": [
        "usd/inr", "usdinr", "rupee", "dollar rupee",
        "forex reserve", "fx intervention", "capital flow",
    ],
    "bonds": [
        "g-sec", "government securities", "treasury bill",
        "t-bill", "gilt", "bond yield", "10 year yield",
        "sovereign bond", "state development loan",
    ],
    "liquidity": [
        "liquidity adjustment", "laf", "msf", "standing facility",
        "open market", "omo", "vrrr", "variable rate",
        "surplus liquidity", "deficit", "system liquidity",
    ],
    "banking": [
        "sbi", "hdfc bank", "icici bank", "axis bank",
        "kotak", "bank of baroda", "pnb", "yes bank",
        "banking sector", "npa", "credit growth",
    ],
    "macro_india": [
        "india gdp", "india inflation", "cpi india",
        "wpi india", "iip", "pmi india", "gst collection",
        "fiscal deficit", "current account",
    ],
}


class LiquiFiConnector:
    """Push treasury-relevant data into LiquiFi's pipeline.

    Modes:
    1. WebSocket: Push rate-relevant news into the /ws/rates stream
    2. REST API: POST to LiquiFi backend endpoints
    3. Redis: Write to LiquiFi's Redis cache for real-time display

    The connector scores each item for treasury relevance (0-1) and
    only pushes items scoring above the threshold.
    """

    def __init__(
        self,
        api_url: Optional[str] = None,
        redis_url: Optional[str] = None,
        relevance_threshold: float = 0.3,
    ):
        self.api_url = api_url or "http://localhost:8000"
        self.redis_url = redis_url or "redis://localhost:6379"
        self.relevance_threshold = relevance_threshold
        self._redis = None
        self._http = httpx.AsyncClient(timeout=15)

    async def _get_redis(self):
        if self._redis is None:
            conn = None
            try:
                import redis.asyncio as aioredis
                conn = aioredis.from_url(self.redis_url, decode_responses=True)
                await conn.ping()
                self._redis = conn
            except Exception as e:
                logger.warning(f"[LiquiFi] Redis connection failed: {e}")
                if conn is not None:
                    try:
                        await conn.close()
                    except Exception:
                        pass
                self._redis = None
        return self._redis

    # Short keywords that need word-boundary matching to avoid false positives
    # (e.g. "omo" in "tomorrow", "laf" in "Lafayette")
    _BOUNDARY_PATTERNS: dict[str, re.Pattern] = {
        kw: re.compile(r"\b" + re.escape(kw) + r"\b")
        for cat_kws in TREASURY_KEYWORDS.values()
        for kw in cat_kws
        if len(kw) <= 4
    }

    def score_treasury_relevance(self, item: ScrapedItem) -> tuple[float, list[str]]:
        """Score how relevant an item is to treasury operations.

        Returns (score, matched_categories).
        """
        text = item.unified.text.lower()
        title = item.unified.raw_metadata.get("title", "").lower()
        full_text = f"{text} {title}"

        score = 0.0
        matched_categories = []

        for category, keywords in TREASURY_KEYWORDS.items():
            category_hits = 0
            for kw in keywords:
                if kw in self._BOUNDARY_PATTERNS:
                    if self._BOUNDARY_PATTERNS[kw].search(full_text):
                        category_hits += 1
                elif kw in full_text:
                    category_hits += 1
            if category_hits > 0:
                matched_categories.append(category)
                score += min(category_hits * 0.15, 0.4)

        # Bonus for central bank sources
        if item.unified.platform == Platform.CENTRAL_BANK:
            score += 0.3
            if "rbi" in full_text:
                score += 0.2  # Extra weight for RBI (LiquiFi is India-focused)

        # Bonus for SEC filings of tracked banks
        if item.unified.platform == Platform.SEC_EDGAR:
            score += 0.1

        return min(score, 1.0), matched_categories

    def _transform_for_liquifi(
        self, items: list[ScrapedItem], scores: dict | None = None
    ) -> dict:
        """Transform items into LiquiFi-compatible format.

        Args:
            scores: Optional dict of {item_id: (relevance, categories)} to avoid
                    recomputing and to avoid mutating shared item metadata.
        """
        news_items = []
        rate_signals = []

        for item in items:
            if scores and item.unified.id in scores:
                relevance, categories = scores[item.unified.id]
            else:
                relevance, categories = self.score_treasury_relevance(item)

            news_entry = {
                "id": item.unified.id,
                "title": item.unified.raw_metadata.get("title", item.unified.text[:100]),
                "body": item.unified.text[:1000],
                "source": item.unified.source_channel or item.unified.platform.value,
                "platform": item.unified.platform.value,
                "url": item.unified.source_url,
                "published_at": item.unified.created_at.isoformat(),
                "relevance_score": relevance,
                "categories": categories,
                "author": item.unified.author.display_name,
                "sentiment": item.unified.raw_metadata.get("sentiment"),
            }
            news_items.append(news_entry)

            # Extract rate signals from highly relevant items
            if relevance >= 0.5:
                text_lower = item.unified.text.lower()
                signal = {
                    "source": item.unified.platform.value,
                    "timestamp": item.unified.created_at.isoformat(),
                    "relevance": relevance,
                    "categories": categories,
                    "summary": item.unified.text[:200],
                }

                # Detect specific rate mentions
                rate_patterns = [
                    (r"repo\s*rate.*?(\d+\.?\d*)\s*%", "repo_rate", (0.0, 15.0)),
                    (r"mibor.*?(\d+\.?\d*)\s*%", "mibor", (0.0, 20.0)),
                    (r"sofr.*?(\d+\.?\d*)\s*%", "sofr", (0.0, 15.0)),
                    (r"usd[/\s]*inr.{0,30}?(\d+\.?\d*)", "usdinr", (40.0, 150.0)),
                    (r"crr.*?(\d+\.?\d*)\s*%", "crr", (0.0, 15.0)),
                    (r"slr.*?(\d+\.?\d*)\s*%", "slr", (0.0, 45.0)),
                ]
                for pattern, rate_name, valid_range in rate_patterns:
                    match = re.search(pattern, text_lower)
                    if match:
                        value = float(match.group(1))
                        if valid_range and not (valid_range[0] <= value <= valid_range[1]):
                            continue
                        signal[rate_name] = value

                rate_signals.append(signal)

        return {
            "news": news_items,
            "rate_signals": rate_signals,
            "source": "social_scraper",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_items": len(items),
        }

    async def push_via_redis(self, items: list[ScrapedItem], payload: dict) -> bool:
        """Push treasury news directly into LiquiFi's Redis."""
        r = await self._get_redis()
        if not r:
            return False

        try:
            # Write to LiquiFi's news cache
            await r.set(
                "liquifi:treasury_news",
                json.dumps(payload, default=str),
                ex=600,  # 10min TTL
            )

            # Write rate signals separately for quick access
            if payload["rate_signals"]:
                await r.set(
                    "liquifi:rate_signals",
                    json.dumps(payload["rate_signals"], default=str),
                    ex=300,
                )

            # Publish notification — best-effort; data is already cached
            try:
                await r.publish("liquifi:updates", json.dumps({
                    "type": "treasury_news",
                    "count": len(items),
                    "signals": len(payload["rate_signals"]),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }))
            except Exception as pub_err:
                logger.warning(f"[LiquiFi] Redis publish notification failed (data cached OK): {pub_err}")

            logger.info(
                f"[LiquiFi] Pushed {len(items)} news items, "
                f"{len(payload['rate_signals'])} rate signals to Redis"
            )
            return True
        except Exception as e:
            logger.error(f"[LiquiFi] Redis push failed: {e}")
            return False

    async def push_via_api(self, items: list[ScrapedItem], payload: dict) -> bool:
        """Push via LiquiFi's REST API."""
        try:
            resp = await self._http.post(
                f"{self.api_url}/api/social-intel",
                json=payload,
            )
            if resp.status_code in (200, 201):
                logger.info(f"[LiquiFi] API push {len(items)} items")
                return True
            else:
                body = resp.text[:200] if resp.text else "(empty)"
                logger.warning(
                    f"[LiquiFi] API push failed: {resp.status_code} — {body}"
                )
                return False
        except Exception as e:
            logger.error(f"[LiquiFi] API push error: {e}")
            return False

    async def push(self, items: list[ScrapedItem]) -> dict:
        """Filter and push treasury-relevant items to LiquiFi.

        Only items scoring above the relevance threshold are pushed.
        Scores are stored in a separate lookup to avoid mutating shared items.
        """
        relevant_items = []
        scores = {}
        for item in items:
            score, categories = self.score_treasury_relevance(item)
            if score >= self.relevance_threshold:
                scores[item.unified.id] = (score, categories)
                relevant_items.append(item)

        if not relevant_items:
            return {"pushed": 0, "filtered_out": len(items), "success": True}

        # Build payload once, pass to both Redis and API paths
        payload = self._transform_for_liquifi(relevant_items, scores)
        success = await self.push_via_redis(relevant_items, payload)
        if not success:
            success = await self.push_via_api(relevant_items, payload)

        return {
            "pushed": len(relevant_items),
            "filtered_out": len(items) - len(relevant_items),
            "success": success,
        }

    async def close(self):
        """Close HTTP and Redis connections."""
        try:
            await self._http.aclose()
        except Exception as e:
            logger.debug(f"[LiquiFi] HTTP close error: {e}")
        if self._redis:
            try:
                await self._redis.close()
            except Exception as e:
                logger.debug(f"[LiquiFi] Redis close error: {e}")
            self._redis = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
