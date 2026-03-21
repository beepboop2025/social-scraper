"""YouTube scraper — financial channels, video metadata, comments, and transcripts."""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, MediaItem, MediaType,
    Platform, ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Quota exhaustion cooldown: 24 hours in seconds
_QUOTA_COOLDOWN_SECONDS = 86400

# Financial YouTube channels to monitor
DEFAULT_CHANNELS = [
    # Channel IDs of popular finance YouTubers
    # Users configure these via env/config
]

FINANCIAL_SEARCH_QUERIES = [
    "stock market today",
    "crypto news today",
    "forex analysis",
    "RBI monetary policy",
    "Federal Reserve",
    "treasury yields",
    "earnings report",
    "market crash",
    "Indian stock market",
    "DeFi news",
]


class YouTubeScraper(BaseScraper):
    """Scrape YouTube via Data API v3.

    Collects video metadata, comments, and can extract transcripts
    for financial content analysis.
    """

    platform = Platform.YOUTUBE
    name = "youtube"

    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(rate_limit=50, **kwargs)
        self.api_key = api_key
        self._http = httpx.AsyncClient(timeout=30)
        self._quota_exhausted_at: float = 0.0  # timestamp when quota was exhausted

    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()

    def _is_quota_exhausted(self) -> bool:
        """Check if we're in a quota cooldown period."""
        if self._quota_exhausted_at == 0.0:
            return False
        elapsed = time.monotonic() - self._quota_exhausted_at
        if elapsed < _QUOTA_COOLDOWN_SECONDS:
            return True
        # Cooldown expired, reset
        self._quota_exhausted_at = 0.0
        return False

    async def _get(self, endpoint: str, params: dict) -> dict:
        if self._is_quota_exhausted():
            remaining = _QUOTA_COOLDOWN_SECONDS - (time.monotonic() - self._quota_exhausted_at)
            logger.warning(
                "YouTube API quota exhausted",
                extra={
                    "source": self.name,
                    "cooldown_remaining_hours": round(remaining / 3600, 1),
                },
            )
            return {"items": []}

        if self.api_key:
            params["key"] = self.api_key
        resp = await self._http.get(f"{self.BASE_URL}/{endpoint}", params=params)

        # Detect quota exhaustion (403 with specific reason)
        if resp.status_code == 403:
            try:
                error_data = resp.json()
                errors = error_data.get("error", {}).get("errors", [])
                is_quota = any(
                    e.get("reason") in ("quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded")
                    for e in errors
                )
            except Exception:
                is_quota = True  # Assume quota if we can't parse the error

            if is_quota:
                self._quota_exhausted_at = time.monotonic()
                logger.warning(
                    "YouTube API quota exhausted - entering 24h cooldown. "
                    "Consider increasing quota or using RSS fallback for channels.",
                    extra={
                        "source": self.name,
                        "error_type": "QuotaExhausted",
                        "endpoint": endpoint,
                    },
                )
                return {"items": []}

        resp.raise_for_status()
        return resp.json()

    def _parse_video(self, item: dict) -> ScrapedItem:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        video_id = item.get("id", "")
        if isinstance(video_id, dict):
            video_id = video_id.get("videoId", "")

        published = snippet.get("publishedAt", "")
        try:
            created = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            created = datetime.now(timezone.utc)

        content = ScrapedContent(
            id=self.make_id("youtube", video_id),
            platform=Platform.YOUTUBE,
            content_type=ContentType.POST,
            text=f"{snippet.get('title', '')}\n\n{snippet.get('description', '')}",
            author=AuthorInfo(
                username=snippet.get("channelId", ""),
                display_name=snippet.get("channelTitle", "Unknown"),
                id=snippet.get("channelId"),
            ),
            engagement=EngagementMetrics(
                likes=int(stats.get("likeCount", 0)),
                replies=int(stats.get("commentCount", 0)),
                views=int(stats.get("viewCount", 0)),
            ),
            media=[
                MediaItem(
                    type=MediaType.VIDEO,
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    thumbnail_url=snippet.get("thumbnails", {}).get("high", {}).get("url"),
                )
            ],
            created_at=created,
            source_url=f"https://www.youtube.com/watch?v={video_id}",
            source_channel=snippet.get("channelTitle", ""),
            hashtags=re.findall(r"#(\w+)", snippet.get("description", "")),
            raw_metadata={
                "video_id": video_id,
                "category_id": snippet.get("categoryId"),
                "tags": snippet.get("tags", []),
                "live_broadcast_content": snippet.get("liveBroadcastContent"),
                "duration": item.get("contentDetails", {}).get("duration"),
                "definition": item.get("contentDetails", {}).get("definition"),
                "favorite_count": stats.get("favoriteCount", "0"),
            },
        )
        return ScrapedItem(unified=content)

    def _parse_comment(self, item: dict) -> ScrapedItem:
        snippet = item.get("snippet", {})
        top = snippet.get("topLevelComment", {}).get("snippet", snippet)

        published = top.get("publishedAt", "")
        try:
            created = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            created = datetime.now(timezone.utc)

        content = ScrapedContent(
            id=self.make_id("youtube", "comment", item.get("id", "")),
            platform=Platform.YOUTUBE,
            content_type=ContentType.COMMENT,
            text=top.get("textDisplay", ""),
            author=AuthorInfo(
                username=top.get("authorChannelId", {}).get("value", ""),
                display_name=top.get("authorDisplayName", "Unknown"),
                avatar_url=top.get("authorProfileImageUrl"),
            ),
            engagement=EngagementMetrics(
                likes=top.get("likeCount", 0),
                replies=snippet.get("totalReplyCount", 0),
            ),
            created_at=created,
            is_reply=False,
            source_url=f"https://www.youtube.com/watch?v={top.get('videoId', '')}",
            raw_metadata={
                "video_id": top.get("videoId"),
                "comment_id": item.get("id"),
                "updated_at": top.get("updatedAt"),
            },
        )
        return ScrapedItem(unified=content)

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Search YouTube for videos matching query."""
        if not self.api_key:
            logger.error("[YouTube] API key required")
            return []

        data = await self._get("search", {
            "part": "snippet",
            "q": query,
            "type": "video",
            "order": "date",
            "maxResults": min(limit, 50),
            "relevanceLanguage": "en",
        })

        video_ids = [
            item["id"]["videoId"]
            for item in data.get("items", [])
            if item.get("id", {}).get("videoId")
        ]

        if not video_ids:
            return []

        # Fetch full video details with statistics
        details = await self._get("videos", {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids),
        })

        items = [self._parse_video(v) for v in details.get("items", [])]
        return items[:limit]

    async def _scrape_channel_rss_fallback(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Fallback: scrape channel via YouTube RSS feed (no quota cost)."""
        try:
            rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            resp = await self._http.get(rss_url)
            if resp.status_code != 200:
                return []

            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}

            items = []
            for entry in root.findall("atom:entry", ns)[:limit]:
                video_id = entry.find("yt:videoId", ns)
                title_el = entry.find("atom:title", ns)
                published_el = entry.find("atom:published", ns)
                author_el = entry.find("atom:author/atom:name", ns)

                vid = video_id.text if video_id is not None else ""
                title = title_el.text if title_el is not None else ""
                author_name = author_el.text if author_el is not None else "Unknown"

                try:
                    published = published_el.text if published_el is not None else ""
                    created = datetime.fromisoformat(published.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    created = datetime.now(timezone.utc)

                content = ScrapedContent(
                    id=self.make_id("youtube", vid),
                    platform=Platform.YOUTUBE,
                    content_type=ContentType.POST,
                    text=title,
                    author=AuthorInfo(
                        username=channel_id,
                        display_name=author_name,
                        id=channel_id,
                    ),
                    engagement=EngagementMetrics(),
                    created_at=created,
                    source_url=f"https://www.youtube.com/watch?v={vid}",
                    source_channel=author_name,
                    raw_metadata={"video_id": vid, "source": "rss_fallback"},
                )
                items.append(ScrapedItem(unified=content))

            logger.info(f"[YouTube] RSS fallback: got {len(items)} videos for channel {channel_id}")
            return items
        except Exception as e:
            logger.warning(
                "YouTube RSS fallback failed",
                extra={"source": self.name, "channel_id": channel_id, "error_type": type(e).__name__},
            )
            return []

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape latest videos from a YouTube channel."""
        if not self.api_key or self._is_quota_exhausted():
            logger.info(f"[YouTube] Using RSS fallback for channel {channel_id}")
            return await self._scrape_channel_rss_fallback(channel_id, limit)

        data = await self._get("search", {
            "part": "snippet",
            "channelId": channel_id,
            "type": "video",
            "order": "date",
            "maxResults": min(limit, 50),
        })

        # If API returned empty (possibly due to quota), try RSS fallback
        if not data.get("items"):
            if self._is_quota_exhausted():
                return await self._scrape_channel_rss_fallback(channel_id, limit)
            return []

        video_ids = [
            item["id"]["videoId"]
            for item in data.get("items", [])
            if item.get("id", {}).get("videoId")
        ]

        if not video_ids:
            return []

        details = await self._get("videos", {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids),
        })

        return [self._parse_video(v) for v in details.get("items", [])]

    async def scrape_comments(self, video_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape comments on a video."""
        if not self.api_key:
            return []

        data = await self._get("commentThreads", {
            "part": "snippet",
            "videoId": video_id,
            "order": "relevance",
            "maxResults": min(limit, 100),
        })

        return [self._parse_comment(c) for c in data.get("items", [])]

    async def scrape_financial_content(self, limit_per_query: int = 20) -> list[ScrapedItem]:
        """Scrape all financial search queries."""
        all_items = []
        for query in FINANCIAL_SEARCH_QUERIES:
            items = await self.safe_scrape(query, limit_per_query)
            all_items.extend(items)
            await asyncio.sleep(0.5)
        logger.info(f"[YouTube] Scraped {len(all_items)} videos from {len(FINANCIAL_SEARCH_QUERIES)} queries")
        return all_items
