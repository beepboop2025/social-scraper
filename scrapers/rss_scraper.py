"""RSS/Atom feed scraper — monitors hundreds of financial news sources."""

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
import defusedxml.ElementTree as ET

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Default financial RSS feeds
DEFAULT_FEEDS = {
    # Major News
    "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
    "reuters_markets": "https://feeds.reuters.com/reuters/marketsNews",
    "bloomberg_markets": "https://feeds.bloomberg.com/markets/news.rss",
    "cnbc_finance": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "ft_markets": "https://www.ft.com/markets?format=rss",
    "wsj_markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    # Crypto
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "the_block": "https://www.theblock.co/rss.xml",
    # India Financial
    "moneycontrol": "https://www.moneycontrol.com/rss/latestnews.xml",
    "livemint_markets": "https://www.livemint.com/rss/markets",
    "et_markets": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "rbi_press": "https://rbi.org.in/scripts/BS_PressReleaseDisplay.aspx?format=rss",
    # Central Banks
    "fed_press": "https://www.federalreserve.gov/feeds/press_all.xml",
    "ecb_press": "https://www.ecb.europa.eu/rss/press.html",
    # Tech/Startup Finance
    "techcrunch_fintech": "https://techcrunch.com/category/fintech/feed/",
    "ycombinator": "https://news.ycombinator.com/rss",
    # DeFi
    "defipulse": "https://defipulse.com/blog/feed/",
    "rekt_news": "https://rekt.news/rss.xml",
    # Research
    "arxiv_qfin": "https://rss.arxiv.org/rss/q-fin",
    "ssrn_finance": "https://papers.ssrn.com/sol3/Jeljour_results.cfm?form_name=journalBrowse&journal_id=354236&Network=no&lim=false&npage=1&output=rss",
}

# Atom namespace
ATOM_NS = "{http://www.w3.org/2005/Atom}"


class RSSScraper(BaseScraper):
    """Scrape RSS and Atom feeds for financial news.

    Handles both RSS 2.0 and Atom feed formats.
    No authentication required.
    """

    platform = Platform.RSS
    name = "rss"

    def __init__(self, feeds: Optional[dict[str, str]] = None, **kwargs):
        super().__init__(rate_limit=60, **kwargs)
        self.feeds = feeds or DEFAULT_FEEDS
        self._http = httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": "SocialScraper/3.0 (Financial RSS Reader)"},
            follow_redirects=True,
        )

    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()

    def _parse_date(self, date_str: Optional[str]) -> datetime:
        if not date_str:
            return datetime.now(timezone.utc)
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            pass
        # Try ISO format
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)

    def _strip_html(self, text: str) -> str:
        return re.sub(r"<[^>]+>", "", text).strip()

    def _parse_rss_item(self, item: ET.Element, feed_name: str) -> ScrapedItem:
        """Parse an RSS 2.0 <item> element."""
        title = item.findtext("title", "")
        description = self._strip_html(item.findtext("description", ""))
        link = item.findtext("link", "")
        pub_date = item.findtext("pubDate")
        author = item.findtext("author") or item.findtext("{http://purl.org/dc/elements/1.1/}creator", "")
        guid = item.findtext("guid", link)
        categories = [c.text for c in item.findall("category") if c.text]

        content = ScrapedContent(
            id=self.make_id("rss", hashlib.md5((guid or link or title).encode()).hexdigest()),
            platform=Platform.RSS,
            content_type=ContentType.ARTICLE,
            text=f"{title}\n\n{description}",
            author=AuthorInfo(
                username=feed_name,
                display_name=author or feed_name,
            ),
            engagement=EngagementMetrics(),
            created_at=self._parse_date(pub_date),
            source_url=link,
            source_channel=feed_name,
            hashtags=categories,
            urls=[link] if link else [],
            raw_metadata={
                "feed_name": feed_name,
                "title": title,
                "guid": guid,
                "categories": categories,
            },
            tags=categories,
        )
        return ScrapedItem(unified=content)

    def _parse_atom_entry(self, entry: ET.Element, feed_name: str) -> ScrapedItem:
        """Parse an Atom <entry> element."""
        title = entry.findtext(f"{ATOM_NS}title", "")
        summary = self._strip_html(entry.findtext(f"{ATOM_NS}summary", ""))
        content_text = self._strip_html(entry.findtext(f"{ATOM_NS}content", ""))

        link_el = entry.find(f"{ATOM_NS}link[@rel='alternate']")
        if link_el is None:
            link_el = entry.find(f"{ATOM_NS}link")
        link = link_el.get("href", "") if link_el is not None else ""

        published = entry.findtext(f"{ATOM_NS}published") or entry.findtext(f"{ATOM_NS}updated")
        author_el = entry.find(f"{ATOM_NS}author")
        author_name = ""
        if author_el is not None:
            author_name = author_el.findtext(f"{ATOM_NS}name", "")

        entry_id = entry.findtext(f"{ATOM_NS}id", link)
        categories = [c.get("term", "") for c in entry.findall(f"{ATOM_NS}category") if c.get("term")]

        content = ScrapedContent(
            id=self.make_id("rss", hashlib.md5((entry_id or link or title).encode()).hexdigest()),
            platform=Platform.RSS,
            content_type=ContentType.ARTICLE,
            text=f"{title}\n\n{content_text or summary}",
            author=AuthorInfo(
                username=feed_name,
                display_name=author_name or feed_name,
            ),
            engagement=EngagementMetrics(),
            created_at=self._parse_date(published),
            source_url=link,
            source_channel=feed_name,
            hashtags=categories,
            urls=[link] if link else [],
            raw_metadata={
                "feed_name": feed_name,
                "title": title,
                "entry_id": entry_id,
                "categories": categories,
            },
            tags=categories,
        )
        return ScrapedItem(unified=content)

    async def _fetch_feed(self, feed_url: str, feed_name: str, limit: int = 50) -> list[ScrapedItem]:
        """Fetch and parse a single RSS/Atom feed."""
        resp = await self._http.get(feed_url)
        resp.raise_for_status()

        root = ET.fromstring(resp.text)
        items = []

        # Detect RSS vs Atom
        if root.tag == "rss" or root.find("channel") is not None:
            channel = root.find("channel") or root
            for item in channel.findall("item")[:limit]:
                items.append(self._parse_rss_item(item, feed_name))
        elif root.tag == f"{ATOM_NS}feed" or root.tag == "feed":
            for entry in root.findall(f"{ATOM_NS}entry")[:limit]:
                items.append(self._parse_atom_entry(entry, feed_name))
            if not items:
                for entry in root.findall("entry")[:limit]:
                    items.append(self._parse_atom_entry(entry, feed_name))

        return items

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape a specific feed by name or URL."""
        if query in self.feeds:
            return await self._fetch_feed(self.feeds[query], query, limit)
        elif query.startswith("http"):
            return await self._fetch_feed(query, "custom", limit)
        return []

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape a feed by name."""
        return await self.scrape(channel_id, limit)

    async def scrape_all_feeds(self, limit_per_feed: int = 20) -> list[ScrapedItem]:
        """Scrape all configured RSS feeds."""
        all_items = []
        for name, url in self.feeds.items():
            try:
                await self.rate_limiter.acquire()
                items = await self._fetch_feed(url, name, limit_per_feed)
                all_items.extend(items)
            except Exception as e:
                logger.warning(f"[RSS] Failed to fetch {name}: {e}")
            await asyncio.sleep(0.3)

        logger.info(f"[RSS] Scraped {len(all_items)} articles from {len(self.feeds)} feeds")
        return all_items
