"""Hacker News scraper — stories and comments via the official Firebase API."""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Financial keywords to filter HN stories
# Only multi-word phrases or unambiguous terms belong here (substring match).
FINANCIAL_KEYWORDS = [
    "crypto", "bitcoin", "ethereum", "trading",
    "interest rate", "inflation", "recession", "gdp",
    "startup funding", "ipo", "acquisition", "valuation",
    "fintech", "defi", "treasury",
    "regulation", "hedge fund", "venture capital",
    # Specific yield phrases to avoid matching programming "yield"
    "yield curve", "bond yield", "treasury yield", "dividend yield",
    # Multi-word stock/market/bank phrases that are unambiguous
    "stock market", "stock exchange", "stock price",
    "central bank", "banking sector", "investment bank",
    "bond market", "corporate bond",
    "market cap", "bear market", "bull market",
]

# Short keywords needing word-boundary matching to avoid false positives
# e.g. "fed" matches "federated", "stock" matches "livestock",
# "bank" matches "databank", "market" matches "supermarket"
_FINANCIAL_WORD_RE = re.compile(
    r'\b(?:sec|fed|stock|bank|bond|market)\b', re.IGNORECASE
)


class HackerNewsScraper(BaseScraper):
    """Scrape Hacker News via the official API (no auth needed).

    Covers: top, new, best, ask, show stories + comments.
    Filters for financial relevance when needed.
    """

    platform = Platform.HACKERNEWS
    name = "hackernews"

    BASE_URL = "https://hacker-news.firebaseio.com/v0"

    MAX_CONCURRENT = 20  # Limit parallel requests to HN API

    def __init__(self, **kwargs):
        super().__init__(rate_limit=60, **kwargs)
        self._http = httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": "SocialScraper/3.0 (HackerNews; +https://github.com)"},
        )
        self._sem = asyncio.Semaphore(self.MAX_CONCURRENT)

    async def _get_item(self, item_id: int) -> Optional[dict]:
        async with self._sem:
            try:
                resp = await self._http.get(f"{self.BASE_URL}/item/{item_id}.json")
            except httpx.HTTPError as e:
                logger.warning(f"[HN] Network error fetching item {item_id}: {e}")
                return None
            if resp.status_code != 200:
                logger.debug(f"[HN] Non-200 status ({resp.status_code}) for item {item_id}")
                return None
            try:
                return resp.json()
            except Exception:
                logger.warning(f"[HN] Malformed JSON for item {item_id}")
                return None

    async def _get_stories(self, category: str = "topstories", limit: int = 100) -> list[int]:
        try:
            resp = await self._http.get(f"{self.BASE_URL}/{category}.json")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error(f"[HN] Failed to fetch {category} story list: {e}")
            return []
        try:
            ids = resp.json()
        except Exception:
            logger.error(f"[HN] Failed to parse story list JSON for {category}")
            return []
        if not isinstance(ids, list):
            logger.error(f"[HN] Expected list for {category}, got {type(ids).__name__}")
            return []
        return ids[:limit]

    def _parse_story(self, item: dict) -> ScrapedItem:
        content = ScrapedContent(
            id=self.make_id("hn", str(item.get("id", ""))),
            platform=Platform.HACKERNEWS,
            content_type=ContentType.POST,
            text=f"{item.get('title', '')}\n\n{item.get('text', '')}".strip(),
            author=AuthorInfo(
                username=item.get("by", "unknown"),
                display_name=item.get("by", "unknown"),
            ),
            engagement=EngagementMetrics(
                likes=item.get("score", 0),
                replies=item.get("descendants", 0),
            ),
            created_at=datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc),
            source_url=item.get("url") or f"https://news.ycombinator.com/item?id={item.get('id')}",
            urls=[item["url"]] if item.get("url") else [],
            raw_metadata={
                "hn_id": item.get("id"),
                "type": item.get("type"),
                "url": item.get("url"),
                "title": item.get("title"),
                "kids_count": len(item.get("kids", [])),
            },
        )
        return ScrapedItem(unified=content)

    def _parse_comment(self, item: dict) -> Optional[ScrapedItem]:
        text = item.get("text", "")
        if not text or item.get("deleted") or item.get("dead"):
            return None

        content = ScrapedContent(
            id=self.make_id("hn", "comment", str(item.get("id", ""))),
            platform=Platform.HACKERNEWS,
            content_type=ContentType.COMMENT,
            text=text,
            author=AuthorInfo(
                username=item.get("by", "unknown"),
                display_name=item.get("by", "unknown"),
            ),
            engagement=EngagementMetrics(),
            created_at=datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc),
            is_reply=True,
            parent_id=str(item.get("parent", "")),
            source_url=f"https://news.ycombinator.com/item?id={item.get('id')}",
            raw_metadata={
                "hn_id": item.get("id"),
                "parent": item.get("parent"),
                "kids_count": len(item.get("kids", [])),
            },
        )
        return ScrapedItem(unified=content)

    def _is_financial(self, item: dict) -> bool:
        text = f"{item.get('title', '')} {item.get('text', '')} {item.get('url', '')}".lower()
        if any(kw in text for kw in FINANCIAL_KEYWORDS):
            return True
        return bool(_FINANCIAL_WORD_RE.search(text))

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Fetch top stories from HN (query used as category: topstories/newstories/beststories)."""
        category = query if query in ("topstories", "newstories", "beststories", "askstories", "showstories") else "topstories"
        story_ids = await self._get_stories(category, limit * 2)

        tasks = [self._get_item(sid) for sid in story_ids[:limit * 2]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"[HN] Failed to fetch story: {r}")
                continue
            if isinstance(r, dict) and r.get("type") == "story":
                items.append(self._parse_story(r))
            if len(items) >= limit:
                break

        return items

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Channel = story category (top/new/best)."""
        return await self.scrape(channel_id, limit)

    async def scrape_financial(self, limit: int = 50) -> list[ScrapedItem]:
        """Scrape top stories filtered for financial relevance."""
        story_ids = await self._get_stories("topstories", 200)

        tasks = [self._get_item(sid) for sid in story_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"[HN] Failed to fetch story: {r}")
                continue
            if isinstance(r, dict) and r.get("type") == "story" and self._is_financial(r):
                items.append(self._parse_story(r))
            if len(items) >= limit:
                break

        logger.info(f"[HN] Found {len(items)} financial stories out of {len(story_ids)} total")
        return items

    async def scrape_story_comments(self, story_id: int, limit: int = 50) -> list[ScrapedItem]:
        """Scrape comments on a specific story."""
        story = await self._get_item(story_id)
        if not story or not story.get("kids"):
            return []

        kid_ids = story["kids"][:limit]
        tasks = [self._get_item(kid) for kid in kid_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items = []
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"[HN] Failed to fetch comment: {r}")
                continue
            if isinstance(r, dict):
                item = self._parse_comment(r)
                if item:
                    items.append(item)

        return items

    async def close(self):
        """Clean up HTTP client."""
        await self._http.aclose()
