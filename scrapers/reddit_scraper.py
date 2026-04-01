"""Reddit scraper — monitors subreddits for financial + social intelligence."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote, urlparse, urlunparse

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Financial subreddits relevant to DragonScope + LiquiFi
DEFAULT_SUBREDDITS = [
    "wallstreetbets", "stocks", "investing", "cryptocurrency",
    "IndiaInvestments", "IndianStreetBets", "personalfinance",
    "SecurityAnalysis", "options", "Forex", "economy",
    "CryptoCurrency", "defi", "ethtrader", "Bitcoin",
    "algotrading", "quantfinance", "thetagang",
]


class RedditScraper(BaseScraper):
    """Scrape Reddit posts and comments via the public JSON API.

    Uses reddit's .json endpoint (no API key needed for read-only).
    Falls back to authenticated API via PRAW if credentials provided.
    """

    platform = Platform.REDDIT
    name = "reddit"

    SUBREDDITS = DEFAULT_SUBREDDITS

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        user_agent: str = "SocialScraper/3.0",
        **kwargs,
    ):
        super().__init__(rate_limit=20, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self._token: Optional[str] = None
        self._token_expiry = 0.0
        self._http = httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": self.user_agent},
            follow_redirects=True,
        )

    async def _get_oauth_token(self) -> Optional[str]:
        """Get OAuth token for authenticated Reddit API access."""
        if not self.client_id or not self.client_secret:
            return None
        import time
        if self._token and time.time() < self._token_expiry:
            return self._token
        try:
            resp = await self._http.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials"},
                headers={"User-Agent": self.user_agent},
            )
            if resp.status_code == 200:
                data = resp.json()
                self._token = data["access_token"]
                self._token_expiry = time.time() + data.get("expires_in", 3600) - 60
                return self._token
        except Exception as e:
            logger.warning(f"[Reddit] OAuth token request failed: {e}")
        return None

    async def _fetch_json(self, url: str) -> dict | list:
        """Fetch JSON from Reddit, using OAuth if available."""
        token = await self._get_oauth_token()
        if token:
            # Normalize both www and non-www reddit URLs to oauth.reddit.com
            url = url.replace("https://www.reddit.com", "https://oauth.reddit.com")
            url = url.replace("https://reddit.com", "https://oauth.reddit.com")
            resp = await self._http.get(
                url,
                headers={"Authorization": f"Bearer {token}", "User-Agent": self.user_agent},
            )
        else:
            # Ensure .json is in the URL path (before query string), not appended to the full URL
            parsed = urlparse(url)
            if not parsed.path.endswith(".json"):
                parsed = parsed._replace(path=parsed.path.rstrip("/") + ".json")
                url = urlunparse(parsed)
            resp = await self._http.get(url)

        if resp.status_code == 429:
            raw_retry = resp.headers.get("Retry-After", "10")
            try:
                retry_after = int(raw_retry)
            except (ValueError, TypeError):
                # Retry-After can be a date string per RFC 7231; fall back to 30s
                logger.debug(f"[Reddit] Non-integer Retry-After header: {raw_retry!r}")
                retry_after = 30
            retry_after = min(retry_after, 120)  # Cap at 2 minutes
            logger.warning(f"[Reddit] Rate limited on {url}, backing off {retry_after}s")
            await asyncio.sleep(retry_after)
            raise httpx.HTTPStatusError(
                f"429 rate limited", request=resp.request, response=resp
            )

        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            logger.error(f"[Reddit] Failed to parse JSON from {url} (status={resp.status_code})")
            return {}

    def _parse_post(self, post_data: dict, subreddit: str = "") -> ScrapedItem:
        data = post_data.get("data", post_data)
        created_utc = data.get("created_utc", 0)

        content = ScrapedContent(
            id=self.make_id("reddit", data.get("id", "")),
            platform=Platform.REDDIT,
            content_type=ContentType.POST,
            text=data.get("selftext", "") or data.get("title", ""),
            author=AuthorInfo(
                username=data.get("author", "[deleted]"),
                display_name=data.get("author", "[deleted]"),
                id=data.get("author_fullname"),
            ),
            engagement=EngagementMetrics(
                likes=data.get("ups", 0),
                replies=data.get("num_comments", 0),
                views=None,
                reposts=data.get("num_crossposts", 0),
            ),
            created_at=datetime.fromtimestamp(created_utc, tz=timezone.utc),
            source_url=f"https://reddit.com{data.get('permalink', '')}",
            source_channel=f"r/{data.get('subreddit', subreddit)}",
            hashtags=[],
            urls=[data["url"]] if data.get("url") and not data["url"].startswith("https://www.reddit.com") else [],
            raw_metadata={
                "score": data.get("score", 0),
                "upvote_ratio": data.get("upvote_ratio", 0),
                "is_self": data.get("is_self", True),
                "link_flair_text": data.get("link_flair_text"),
                "subreddit": data.get("subreddit", subreddit),
                "domain": data.get("domain"),
                "over_18": data.get("over_18", False),
                "stickied": data.get("stickied", False),
                "gilded": data.get("gilded", 0),
                "title": data.get("title", ""),
            },
            search_query=subreddit,
        )
        return ScrapedItem(unified=content)

    def _parse_comment(self, comment_data: dict) -> Optional[ScrapedItem]:
        data = comment_data.get("data", comment_data)
        if data.get("body") in (None, "[removed]", "[deleted]"):
            return None

        content = ScrapedContent(
            id=self.make_id("reddit", "comment", data.get("id", "")),
            platform=Platform.REDDIT,
            content_type=ContentType.COMMENT,
            text=data.get("body", ""),
            author=AuthorInfo(
                username=data.get("author", "[deleted]"),
                display_name=data.get("author", "[deleted]"),
            ),
            engagement=EngagementMetrics(
                likes=data.get("ups", 0),
                replies=0,
            ),
            created_at=datetime.fromtimestamp(
                data.get("created_utc", 0), tz=timezone.utc
            ),
            is_reply=True,
            parent_id=data.get("parent_id"),
            source_url=f"https://reddit.com{data.get('permalink', '')}",
            source_channel=f"r/{data.get('subreddit', '')}",
            raw_metadata={
                "score": data.get("score", 0),
                "gilded": data.get("gilded", 0),
                "controversiality": data.get("controversiality", 0),
            },
        )
        return ScrapedItem(unified=content)

    async def scrape(self, query: str, limit: int = 100) -> list[ScrapedItem]:
        """Search Reddit for posts matching query."""
        url = f"https://www.reddit.com/search.json?q={quote(query)}&sort=new&limit={min(limit, 100)}"
        data = await self._fetch_json(url)
        items = []
        for child in data.get("data", {}).get("children", []):
            items.append(self._parse_post(child))
        return items[:limit]

    async def scrape_channel(self, channel_id: str, limit: int = 100) -> list[ScrapedItem]:
        """Scrape a subreddit's latest posts."""
        subreddit = channel_id.removeprefix("r/").removeprefix("/")
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={min(limit, 100)}"
        data = await self._fetch_json(url)
        items = []
        for child in data.get("data", {}).get("children", []):
            items.append(self._parse_post(child, subreddit))
        return items[:limit]

    async def scrape_comments(self, post_url: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape comments on a specific post."""
        url = post_url.rstrip("/") + f".json?limit={limit}"
        data = await self._fetch_json(url)
        items = []
        if isinstance(data, list) and len(data) > 1:
            for child in data[1].get("data", {}).get("children", []):
                item = self._parse_comment(child)
                if item:
                    items.append(item)
        return items

    async def scrape_all_financial(self, limit_per_sub: int = 25) -> list[ScrapedItem]:
        """Scrape all configured financial subreddits."""
        all_items = []
        for sub in self.SUBREDDITS:
            items = await self.safe_scrape_channel(sub, limit_per_sub)
            all_items.extend(items)
            await asyncio.sleep(1.0)  # Be nice to Reddit
        logger.info(f"[Reddit] Scraped {len(all_items)} posts from {len(self.SUBREDDITS)} subreddits")
        return all_items

    async def close(self):
        """Clean up HTTP client."""
        await self._http.aclose()
