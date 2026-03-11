"""Discord scraper — monitors financial Discord servers via bot token."""

import asyncio
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


class DiscordScraper(BaseScraper):
    """Scrape Discord channels via Bot API.

    Requires a bot token with MESSAGE_CONTENT intent enabled.
    Add the bot to financial servers to monitor channels.
    """

    platform = Platform.DISCORD
    name = "discord"

    BASE_URL = "https://discord.com/api/v10"

    def __init__(self, bot_token: Optional[str] = None, **kwargs):
        super().__init__(rate_limit=30, **kwargs)
        self.bot_token = bot_token
        self._http = httpx.AsyncClient(
            timeout=30,
            headers={
                "Authorization": f"Bot {bot_token}" if bot_token else "",
                "Content-Type": "application/json",
            },
        )

    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()

    async def _get(self, endpoint: str, params: Optional[dict] = None) -> dict | list:
        resp = await self._http.get(f"{self.BASE_URL}{endpoint}", params=params)
        resp.raise_for_status()
        return resp.json()

    def _parse_message(self, msg: dict, channel_name: str = "", guild_name: str = "") -> ScrapedItem:
        author = msg.get("author", {})
        reactions_count = sum(r.get("count", 0) for r in msg.get("reactions", []))

        content = ScrapedContent(
            id=self.make_id("discord", msg["id"]),
            platform=Platform.DISCORD,
            content_type=ContentType.REPLY if msg.get("referenced_message") else ContentType.POST,
            text=msg.get("content", ""),
            author=AuthorInfo(
                id=author.get("id"),
                username=author.get("username", "Unknown"),
                display_name=author.get("global_name") or author.get("username", "Unknown"),
                verified=author.get("verified", False),
                avatar_url=f"https://cdn.discordapp.com/avatars/{author.get('id')}/{author.get('avatar')}.png"
                if author.get("avatar") else None,
            ),
            engagement=EngagementMetrics(
                likes=reactions_count,
                replies=0,
            ),
            created_at=datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00")),
            is_reply=bool(msg.get("referenced_message")),
            parent_id=msg.get("referenced_message", {}).get("id") if msg.get("referenced_message") else None,
            source_channel=f"#{channel_name}" if channel_name else msg.get("channel_id"),
            urls=[a["url"] for a in msg.get("attachments", [])],
            raw_metadata={
                "guild_name": guild_name,
                "channel_id": msg.get("channel_id"),
                "message_id": msg["id"],
                "pinned": msg.get("pinned", False),
                "type": msg.get("type", 0),
                "embeds_count": len(msg.get("embeds", [])),
                "attachments_count": len(msg.get("attachments", [])),
                "reactions": [
                    {"emoji": r.get("emoji", {}).get("name"), "count": r.get("count", 0)}
                    for r in msg.get("reactions", [])
                ],
            },
        )
        return ScrapedItem(unified=content)

    async def scrape(self, query: str, limit: int = 100) -> list[ScrapedItem]:
        """Search is not available via bot API — use scrape_channel instead."""
        logger.warning("[Discord] Search not available via bot API; use scrape_channel with a channel ID.")
        return []

    async def scrape_channel(self, channel_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape messages from a Discord channel."""
        if not self.bot_token:
            logger.error("[Discord] Bot token required")
            return []

        messages = await self._get(f"/channels/{channel_id}/messages", params={"limit": min(limit, 100)})
        if not isinstance(messages, list):
            return []

        # Get channel info for context
        try:
            channel_info = await self._get(f"/channels/{channel_id}")
            channel_name = channel_info.get("name", channel_id)
            guild_id = channel_info.get("guild_id")
            guild_name = ""
            if guild_id:
                guild_info = await self._get(f"/guilds/{guild_id}")
                guild_name = guild_info.get("name", "")
        except Exception:
            channel_name = channel_id
            guild_name = ""

        items = []
        for msg in messages:
            if msg.get("content") or msg.get("embeds"):
                items.append(self._parse_message(msg, channel_name, guild_name))

        logger.info(f"[Discord] Scraped {len(items)} messages from #{channel_name}")
        return items

    async def scrape_guild_channels(self, guild_id: str, limit_per_channel: int = 50) -> list[ScrapedItem]:
        """Scrape all text channels in a guild."""
        if not self.bot_token:
            return []

        channels = await self._get(f"/guilds/{guild_id}/channels")
        text_channels = [c for c in channels if c.get("type") == 0]  # type 0 = text

        all_items = []
        for ch in text_channels:
            items = await self.safe_scrape_channel(ch["id"], limit_per_channel)
            all_items.extend(items)
            await asyncio.sleep(0.5)

        return all_items
