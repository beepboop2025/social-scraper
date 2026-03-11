"""Generic web scraper — extracts content from any website."""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Financial sites to periodically scrape
DEFAULT_TARGETS = [
    # These are the main pages — the scraper extracts article links
    "https://www.investing.com/news/",
    "https://finance.yahoo.com/news/",
    "https://www.marketwatch.com/latest-news",
    "https://www.zerohedge.com/",
    "https://www.moneycontrol.com/news/business/markets/",
    "https://www.livemint.com/market",
]


class WebScraper(BaseScraper):
    """Scrape arbitrary web pages using BeautifulSoup.

    Extracts: title, body text, meta description, author, date, links.
    Supports article extraction heuristics for news sites.
    """

    platform = Platform.WEB
    name = "web"

    def __init__(self, **kwargs):
        super().__init__(rate_limit=20, **kwargs)
        self._http = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            follow_redirects=True,
        )

    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()

    def _extract_date(self, soup: BeautifulSoup) -> datetime:
        """Try to extract publication date from meta tags or content."""
        # Common meta tag patterns
        for attr in ["article:published_time", "datePublished", "pubdate", "date"]:
            tag = soup.find("meta", attrs={"property": attr}) or soup.find("meta", attrs={"name": attr})
            if tag and tag.get("content"):
                try:
                    return datetime.fromisoformat(tag["content"].replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

        # Time element
        time_el = soup.find("time")
        if time_el and time_el.get("datetime"):
            try:
                return datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        return datetime.now(timezone.utc)

    def _extract_author(self, soup: BeautifulSoup) -> str:
        """Try to extract author from meta tags or bylines."""
        for attr in ["author", "article:author", "dcterms.creator"]:
            tag = soup.find("meta", attrs={"name": attr}) or soup.find("meta", attrs={"property": attr})
            if tag and tag.get("content"):
                return tag["content"]

        # Byline patterns
        byline = soup.find(class_=re.compile(r"byline|author", re.I))
        if byline:
            return byline.get_text(strip=True)[:100]

        return ""

    def _extract_article_text(self, soup: BeautifulSoup) -> str:
        """Extract main article text using heuristics."""
        # Try article tag first
        article = soup.find("article")
        if article:
            paragraphs = article.find_all("p")
            if paragraphs:
                return "\n\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)

        # Fall back to largest text block
        all_p = soup.find_all("p")
        texts = [p.get_text(strip=True) for p in all_p if len(p.get_text(strip=True)) > 30]
        return "\n\n".join(texts[:20])  # Cap at 20 paragraphs

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Extract all links from the page."""
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(base_url, href)
            if full_url.startswith("http"):
                links.append(full_url)
        return links

    async def scrape_page(self, url: str) -> Optional[ScrapedItem]:
        """Scrape a single web page."""
        resp = await self._http.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        title = ""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)

        # Meta description
        meta_desc = ""
        desc_tag = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if desc_tag:
            meta_desc = desc_tag.get("content", "")

        body_text = self._extract_article_text(soup)
        author = self._extract_author(soup)
        pub_date = self._extract_date(soup)
        domain = urlparse(url).netloc

        content = ScrapedContent(
            id=self.make_id("web", hashlib.md5(url.encode()).hexdigest()),
            platform=Platform.WEB,
            content_type=ContentType.ARTICLE,
            text=f"{title}\n\n{body_text}" if body_text else f"{title}\n\n{meta_desc}",
            author=AuthorInfo(
                username=domain,
                display_name=author or domain,
            ),
            engagement=EngagementMetrics(),
            created_at=pub_date,
            source_url=url,
            source_channel=domain,
            urls=self._extract_links(soup, url)[:20],
            raw_metadata={
                "title": title,
                "meta_description": meta_desc,
                "domain": domain,
                "content_length": len(body_text),
            },
        )
        return ScrapedItem(unified=content)

    async def scrape_site_articles(self, url: str, limit: int = 20) -> list[ScrapedItem]:
        """Scrape a site's index page and extract article links, then scrape each."""
        resp = await self._http.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        links = self._extract_links(soup, url)

        # Filter for article-like URLs (contains /news/, /article/, year patterns, etc.)
        article_pattern = re.compile(r"/(news|article|story|post|blog|20\d{2})/", re.I)
        article_links = [l for l in links if article_pattern.search(l)]
        article_links = list(dict.fromkeys(article_links))[:limit]  # Dedupe, limit

        items = []
        for link in article_links:
            try:
                await self.rate_limiter.acquire()
                item = await self.scrape_page(link)
                if item:
                    items.append(item)
            except Exception as e:
                logger.warning(f"[Web] Failed to scrape {link}: {e}")

        return items

    async def scrape(self, query: str, limit: int = 20) -> list[ScrapedItem]:
        """Query = URL to scrape."""
        if query.startswith("http"):
            return await self.scrape_site_articles(query, limit)
        return []

    async def scrape_channel(self, channel_id: str, limit: int = 20) -> list[ScrapedItem]:
        """Channel = website URL."""
        return await self.scrape(channel_id, limit)

    async def scrape_all_targets(self, limit_per_site: int = 10) -> list[ScrapedItem]:
        """Scrape all default financial news sites."""
        all_items = []
        for url in DEFAULT_TARGETS:
            items = await self.safe_scrape(url, limit_per_site)
            all_items.extend(items)
        logger.info(f"[Web] Scraped {len(all_items)} articles from {len(DEFAULT_TARGETS)} sites")
        return all_items
