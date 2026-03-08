"""Dark web scraper — threat intelligence monitoring via Tor SOCKS5 proxy.

IMPORTANT: This scraper is for DEFENSIVE security intelligence only.
It monitors publicly accessible .onion threat intel feeds and paste sites
to detect financial data leaks, credential dumps, and emerging threats.
No marketplace transactions, no credential harvesting.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from models import (
    AuthorInfo, ContentType, DarkWebContent, EngagementMetrics,
    Platform, ScrapedContent, ScrapedItem, ThreatLevel,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Threat intel keywords for financial sector
FINANCIAL_THREAT_KEYWORDS = [
    "bank leak", "credit card", "financial data", "swift",
    "banking trojan", "atm malware", "payment fraud",
    "cryptocurrency theft", "exchange hack", "wallet drain",
    "insider trading", "market manipulation", "pump and dump",
    "ransomware", "data breach", "credential dump",
    "kyc bypass", "money laundering", "sanctions evasion",
]

# IOC patterns (indicators of compromise)
IOC_PATTERNS = {
    "ip": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "domain": re.compile(r"\b[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z]{2,})+\b"),
    "hash_md5": re.compile(r"\b[a-fA-F0-9]{32}\b"),
    "hash_sha256": re.compile(r"\b[a-fA-F0-9]{64}\b"),
    "email": re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"),
    "btc_address": re.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b"),
    "eth_address": re.compile(r"\b0x[a-fA-F0-9]{40}\b"),
}


class DarkWebScraper(BaseScraper):
    """Scrape dark web threat intelligence via Tor SOCKS5 proxy.

    Requires a Tor proxy running on localhost:9050 (or configured address).
    Only monitors public threat intel feeds and paste sites for defensive purposes.
    """

    platform = Platform.DARKWEB
    name = "darkweb"

    def __init__(
        self,
        tor_proxy: str = "socks5://127.0.0.1:9050",
        surface_fallback: bool = True,
        **kwargs,
    ):
        super().__init__(rate_limit=5, max_retries=2, **kwargs)
        self.tor_proxy = tor_proxy
        self.surface_fallback = surface_fallback

        # Tor-routed client for .onion sites
        self._tor_http = httpx.AsyncClient(
            timeout=60,
            proxy=tor_proxy,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
            verify=False,
        )
        # Surface web client for clearnet threat intel
        self._surface_http = httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "SocialScraper/3.0 ThreatIntel"},
            follow_redirects=True,
        )

    # --- Surface web threat intel sources (no Tor needed) ---

    SURFACE_FEEDS = [
        # Paste monitoring (clearnet mirrors)
        "https://psbdmp.ws/api/v3/getlatest",
        # Threat intel feeds
        "https://otx.alienvault.com/api/v1/pulses/subscribed?limit=20",
        "https://threatfeeds.io/api/feeds/",
        # Breach notification
        "https://haveibeenpwned.com/api/v3/latestbreach",
    ]

    RANSOMWARE_FEEDS = [
        # Ransomware tracking (clearnet)
        "https://ransomwatch.telemetry.ltd/posts",
        "https://raw.githubusercontent.com/joshhighet/ransomwatch/main/posts.json",
    ]

    def _classify_threat(self, text: str) -> tuple[ThreatLevel, list[str], float]:
        """Classify threat level and financial relevance."""
        text_lower = text.lower()
        categories = []
        score = 0.0

        # Financial relevance scoring
        for kw in FINANCIAL_THREAT_KEYWORDS:
            if kw in text_lower:
                categories.append(kw)
                score += 0.15

        # Threat level based on content
        critical_terms = ["0day", "zero-day", "active exploit", "swift", "bank breach"]
        high_terms = ["ransomware", "data breach", "credential dump", "exchange hack"]
        medium_terms = ["vulnerability", "phishing", "malware", "trojan"]

        if any(t in text_lower for t in critical_terms):
            level = ThreatLevel.CRITICAL
        elif any(t in text_lower for t in high_terms):
            level = ThreatLevel.HIGH
        elif any(t in text_lower for t in medium_terms):
            level = ThreatLevel.MEDIUM
        else:
            level = ThreatLevel.LOW

        return level, categories, min(score, 1.0)

    def _extract_iocs(self, text: str) -> list[str]:
        """Extract indicators of compromise from text."""
        iocs = []
        for ioc_type, pattern in IOC_PATTERNS.items():
            matches = pattern.findall(text)
            for m in matches[:10]:  # Cap per type
                iocs.append(f"{ioc_type}:{m}")
        return iocs

    def _parse_threat_item(
        self,
        text: str,
        source_url: str,
        title: str = "",
        author: str = "",
        created_at: Optional[datetime] = None,
    ) -> ScrapedItem:
        """Parse a threat intelligence item."""
        threat_level, categories, fin_relevance = self._classify_threat(text)
        iocs = self._extract_iocs(text)
        domain = urlparse(source_url).netloc

        darkweb_meta = DarkWebContent(
            onion_url=source_url if ".onion" in source_url else None,
            surface_mirror=source_url if ".onion" not in source_url else None,
            threat_level=threat_level,
            threat_categories=categories,
            financial_relevance=fin_relevance,
            iocs=iocs,
        )

        content = ScrapedContent(
            id=self.make_id("darkweb", hashlib.md5((source_url + text[:100]).encode()).hexdigest()),
            platform=Platform.DARKWEB,
            content_type=ContentType.THREAT_INTEL,
            text=f"{title}\n\n{text}" if title else text,
            author=AuthorInfo(
                username=author or domain,
                display_name=author or domain,
            ),
            engagement=EngagementMetrics(),
            created_at=created_at or datetime.now(timezone.utc),
            source_url=source_url,
            source_channel=domain,
            raw_metadata={
                "threat_level": threat_level.value,
                "threat_categories": categories,
                "financial_relevance": fin_relevance,
                "iocs_count": len(iocs),
                "iocs": iocs[:20],
                "darkweb": darkweb_meta.model_dump(),
            },
            tags=categories,
        )
        return ScrapedItem(unified=content)

    async def _scrape_onion(self, url: str) -> list[ScrapedItem]:
        """Scrape a .onion site via Tor proxy."""
        resp = await self._tor_http.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("title")
        title_text = title.get_text(strip=True) if title else ""

        # Extract all text blocks
        texts = []
        for p in soup.find_all(["p", "pre", "div", "li"]):
            t = p.get_text(strip=True)
            if len(t) > 30:
                texts.append(t)

        if not texts:
            return []

        # Create one item per significant text block
        items = []
        for text in texts[:20]:
            item = self._parse_threat_item(text, url, title=title_text)
            items.append(item)

        return items

    async def _scrape_paste_api(self) -> list[ScrapedItem]:
        """Scrape recent pastes from paste monitoring services."""
        items = []
        try:
            resp = await self._surface_http.get("https://psbdmp.ws/api/v3/getlatest")
            if resp.status_code == 200:
                pastes = resp.json() if isinstance(resp.json(), list) else resp.json().get("data", [])
                for paste in pastes[:50]:
                    text = paste.get("text", "") or paste.get("content", "")
                    if text and any(kw in text.lower() for kw in FINANCIAL_THREAT_KEYWORDS):
                        item = self._parse_threat_item(
                            text,
                            source_url=paste.get("url", "https://psbdmp.ws"),
                            title=paste.get("title", "Paste"),
                            created_at=datetime.now(timezone.utc),
                        )
                        items.append(item)
        except Exception as e:
            logger.warning(f"[DarkWeb] Paste API error: {e}")
        return items

    async def _scrape_ransomware_feed(self) -> list[ScrapedItem]:
        """Scrape ransomware group monitoring feeds."""
        items = []
        for feed_url in self.RANSOMWARE_FEEDS:
            try:
                resp = await self._surface_http.get(feed_url)
                if resp.status_code == 200:
                    data = resp.json()
                    posts = data if isinstance(data, list) else data.get("posts", [])
                    for post in posts[:30]:
                        title = post.get("post_title", "") or post.get("title", "")
                        group = post.get("group_name", "") or post.get("group", "")
                        desc = post.get("description", "") or post.get("body", "")
                        discovered = post.get("discovered", "") or post.get("date", "")

                        try:
                            created = datetime.fromisoformat(discovered.replace("Z", "+00:00"))
                        except (ValueError, TypeError, AttributeError):
                            created = datetime.now(timezone.utc)

                        item = self._parse_threat_item(
                            f"{title}\n\n{desc}",
                            source_url=post.get("post_url", feed_url),
                            title=f"[{group}] {title}",
                            author=group,
                            created_at=created,
                        )
                        items.append(item)
            except Exception as e:
                logger.warning(f"[DarkWeb] Ransomware feed error: {e}")

        return items

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape by URL (onion or surface threat intel)."""
        if ".onion" in query:
            return await self._scrape_onion(query)
        return []

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Channel = onion URL or feed name."""
        return await self.scrape(channel_id, limit)

    async def scrape_all_threat_intel(self) -> list[ScrapedItem]:
        """Full threat intel sweep — pastes, ransomware, surface feeds."""
        all_items = []

        # Surface threat intel (no Tor needed)
        paste_items = await self._scrape_paste_api()
        all_items.extend(paste_items)

        ransomware_items = await self._scrape_ransomware_feed()
        all_items.extend(ransomware_items)

        logger.info(
            f"[DarkWeb] Collected {len(all_items)} threat intel items "
            f"(pastes={len(paste_items)}, ransomware={len(ransomware_items)})"
        )
        return all_items

    async def health_check(self) -> dict:
        """Check Tor connectivity."""
        base = await super().health_check()
        try:
            resp = await self._tor_http.get("https://check.torproject.org/api/ip")
            tor_data = resp.json()
            base["tor_connected"] = tor_data.get("IsTor", False)
            base["tor_ip"] = tor_data.get("IP", "unknown")
        except Exception:
            base["tor_connected"] = False
            base["tor_ip"] = "unreachable"
        return base
