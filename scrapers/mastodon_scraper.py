"""Mastodon scraper — decentralized social media monitoring."""

import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Popular Mastodon instances with financial/tech communities
DEFAULT_INSTANCES = [
    "mastodon.social",
    "fosstodon.org",
    "infosec.exchange",
    "hachyderm.io",
    "techhub.social",
]


class MastodonScraper(BaseScraper):
    """Scrape Mastodon instances via the public API.

    No authentication required for public timelines.
    Supports searching across instances and monitoring hashtags.
    """

    platform = Platform.MASTODON
    name = "mastodon"

    def __init__(
        self,
        instances: Optional[list[str]] = None,
        access_token: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(rate_limit=30, **kwargs)
        self.instances = instances or DEFAULT_INSTANCES
        self.access_token = access_token
        self._http = httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": "SocialScraper/3.0"},
        )

    def _parse_status(self, status: dict, instance: str = "") -> ScrapedItem:
        account = status.get("account", {})

        # Strip HTML from content
        import re
        text = re.sub(r"<[^>]+>", "", status.get("content", ""))

        content = ScrapedContent(
            id=self.make_id("mastodon", instance, status.get("id", "")),
            platform=Platform.MASTODON,
            content_type=ContentType.REPLY if status.get("in_reply_to_id") else ContentType.POST,
            text=text,
            language=status.get("language"),
            author=AuthorInfo(
                id=account.get("id"),
                username=f"{account.get('username', '')}@{instance}" if instance else account.get("username", ""),
                display_name=account.get("display_name", account.get("username", "")),
                verified=bool(account.get("locked", False)),
                follower_count=account.get("followers_count"),
                following_count=account.get("following_count"),
                avatar_url=account.get("avatar"),
                description=account.get("note"),
            ),
            engagement=EngagementMetrics(
                likes=status.get("favourites_count", 0),
                reposts=status.get("reblogs_count", 0),
                replies=status.get("replies_count", 0),
            ),
            created_at=datetime.fromisoformat(status["created_at"].replace("Z", "+00:00"))
            if status.get("created_at") else datetime.now(timezone.utc),
            is_reply=bool(status.get("in_reply_to_id")),
            parent_id=status.get("in_reply_to_id"),
            source_url=status.get("url", ""),
            source_channel=instance,
            hashtags=[t.get("name", "") for t in status.get("tags", [])],
            urls=[m.get("url", "") for m in status.get("media_attachments", [])],
            raw_metadata={
                "instance": instance,
                "visibility": status.get("visibility"),
                "sensitive": status.get("sensitive", False),
                "spoiler_text": status.get("spoiler_text", ""),
                "reblog": bool(status.get("reblog")),
                "card": status.get("card"),
                "poll": status.get("poll"),
            },
        )
        return ScrapedItem(unified=content)

    async def _get_instance_api(self, instance: str, endpoint: str, params: Optional[dict] = None) -> list:
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        resp = await self._http.get(
            f"https://{instance}/api/v1/{endpoint}",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Search across configured instances."""
        all_items = []
        for instance in self.instances:
            try:
                statuses = await self._get_instance_api(
                    instance, "timelines/tag/" + query.lstrip("#"),
                    params={"limit": min(limit, 40)},
                )
                for s in statuses:
                    all_items.append(self._parse_status(s, instance))
            except Exception as e:
                logger.warning(f"[Mastodon] {instance} search failed: {e}")
        return all_items[:limit]

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape public timeline of an instance."""
        try:
            statuses = await self._get_instance_api(
                channel_id, "timelines/public",
                params={"limit": min(limit, 40), "local": "true"},
            )
            return [self._parse_status(s, channel_id) for s in statuses]
        except Exception as e:
            logger.warning(f"[Mastodon] {channel_id} timeline failed: {e}")
            return []

    async def scrape_hashtag(self, hashtag: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape a hashtag across all instances."""
        return await self.scrape(hashtag, limit)

    async def scrape_financial_hashtags(self, limit_per_tag: int = 20) -> list[ScrapedItem]:
        """Scrape financial hashtags across instances."""
        tags = [
            "finance", "stocks", "crypto", "bitcoin", "ethereum",
            "trading", "markets", "economics", "fintech", "defi",
            "investing", "inflation", "interestrates",
        ]
        all_items = []
        for tag in tags:
            items = await self.scrape(tag, limit_per_tag)
            all_items.extend(items)
        logger.info(f"[Mastodon] Scraped {len(all_items)} posts from {len(tags)} hashtags")
        return all_items
