"""Central bank scraper — RBI, Federal Reserve, ECB announcements."""

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from xml.etree import ElementTree as ET

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class CentralBankScraper(BaseScraper):
    """Scrape central bank announcements, press releases, and policy decisions.

    Covers: RBI (India), Federal Reserve (US), ECB (Europe), BoE (UK), BoJ (Japan).
    Critical for LiquiFi (treasury rates) and DragonScope (macro analysis).
    """

    platform = Platform.CENTRAL_BANK
    name = "central_bank"

    def __init__(self, **kwargs):
        super().__init__(rate_limit=15, **kwargs)
        self._http = httpx.AsyncClient(
            timeout=30,
            headers={"User-Agent": "SocialScraper/3.0 Financial Research"},
            follow_redirects=True,
        )

    def _strip_html(self, text: str) -> str:
        return re.sub(r"<[^>]+>", "", text).strip()

    # ── RBI (Reserve Bank of India) ──────────────────────────────

    async def scrape_rbi(self, limit: int = 30) -> list[ScrapedItem]:
        """Scrape RBI press releases and policy announcements."""
        items = []

        # RBI press releases RSS
        try:
            resp = await self._http.get("https://rbi.org.in/scripts/BS_PressReleaseDisplay.aspx?format=rss")
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                channel = root.find("channel") or root
                for item_el in channel.findall("item")[:limit]:
                    title = item_el.findtext("title", "")
                    desc = self._strip_html(item_el.findtext("description", ""))
                    link = item_el.findtext("link", "")
                    pub_date = item_el.findtext("pubDate", "")

                    try:
                        from email.utils import parsedate_to_datetime
                        created = parsedate_to_datetime(pub_date)
                    except Exception:
                        created = datetime.now(timezone.utc)

                    content = ScrapedContent(
                        id=self.make_id("rbi", hashlib.md5(link.encode()).hexdigest()),
                        platform=Platform.CENTRAL_BANK,
                        content_type=ContentType.ANNOUNCEMENT,
                        text=f"[RBI] {title}\n\n{desc}",
                        author=AuthorInfo(
                            username="RBI",
                            display_name="Reserve Bank of India",
                        ),
                        engagement=EngagementMetrics(),
                        created_at=created,
                        source_url=link,
                        source_channel="rbi.org.in",
                        raw_metadata={
                            "bank": "RBI",
                            "country": "India",
                            "title": title,
                            "treasury_relevant": any(
                                kw in title.lower()
                                for kw in ["repo", "rate", "policy", "mibor", "liquidity", "crr", "slr", "inflation"]
                            ),
                        },
                        tags=["RBI", "India", "monetary_policy"],
                    )
                    items.append(ScrapedItem(unified=content))
        except Exception as e:
            logger.warning(f"[CentralBank] RBI RSS failed: {e}")

        # RBI notifications page scrape
        try:
            resp = await self._http.get("https://rbi.org.in/scripts/NotificationUser.aspx")
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                table = soup.find("table", id="ctl00_ContentPlaceHolder1_grdBSDM")
                if table:
                    for row in table.find_all("tr")[1:limit]:
                        cols = row.find_all("td")
                        if len(cols) >= 3:
                            date_text = cols[0].get_text(strip=True)
                            title = cols[1].get_text(strip=True)
                            link_tag = cols[1].find("a")
                            link = f"https://rbi.org.in{link_tag['href']}" if link_tag and link_tag.get("href") else ""

                            try:
                                created = datetime.strptime(date_text, "%b %d, %Y").replace(tzinfo=timezone.utc)
                            except (ValueError, TypeError):
                                created = datetime.now(timezone.utc)

                            content = ScrapedContent(
                                id=self.make_id("rbi", "notification", hashlib.md5(title.encode()).hexdigest()),
                                platform=Platform.CENTRAL_BANK,
                                content_type=ContentType.ANNOUNCEMENT,
                                text=f"[RBI Notification] {title}",
                                author=AuthorInfo(username="RBI", display_name="Reserve Bank of India"),
                                engagement=EngagementMetrics(),
                                created_at=created,
                                source_url=link,
                                source_channel="rbi.org.in",
                                raw_metadata={"bank": "RBI", "type": "notification"},
                                tags=["RBI", "India", "notification"],
                            )
                            items.append(ScrapedItem(unified=content))
        except Exception as e:
            logger.warning(f"[CentralBank] RBI notifications scrape failed: {e}")

        return items

    # ── Federal Reserve ──────────────────────────────────────────

    async def scrape_fed(self, limit: int = 30) -> list[ScrapedItem]:
        """Scrape Federal Reserve press releases and statements."""
        items = []

        try:
            resp = await self._http.get("https://www.federalreserve.gov/feeds/press_all.xml")
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                ns = "{http://www.w3.org/2005/Atom}"

                for entry in root.findall(f"{ns}entry")[:limit]:
                    title = entry.findtext(f"{ns}title", "")
                    summary = self._strip_html(entry.findtext(f"{ns}summary", ""))
                    link_el = entry.find(f"{ns}link")
                    link = link_el.get("href", "") if link_el is not None else ""
                    updated = entry.findtext(f"{ns}updated", "")

                    try:
                        created = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        created = datetime.now(timezone.utc)

                    content = ScrapedContent(
                        id=self.make_id("fed", hashlib.md5((link or title).encode()).hexdigest()),
                        platform=Platform.CENTRAL_BANK,
                        content_type=ContentType.ANNOUNCEMENT,
                        text=f"[Federal Reserve] {title}\n\n{summary}",
                        author=AuthorInfo(
                            username="FederalReserve",
                            display_name="Federal Reserve",
                        ),
                        engagement=EngagementMetrics(),
                        created_at=created,
                        source_url=link,
                        source_channel="federalreserve.gov",
                        raw_metadata={
                            "bank": "Fed",
                            "country": "US",
                            "title": title,
                            "treasury_relevant": any(
                                kw in title.lower()
                                for kw in ["rate", "fomc", "policy", "inflation", "employment", "treasury", "sofr"]
                            ),
                        },
                        tags=["Fed", "US", "monetary_policy"],
                    )
                    items.append(ScrapedItem(unified=content))
        except Exception as e:
            logger.warning(f"[CentralBank] Fed RSS failed: {e}")

        return items

    # ── ECB (European Central Bank) ──────────────────────────────

    async def scrape_ecb(self, limit: int = 20) -> list[ScrapedItem]:
        """Scrape ECB press releases."""
        items = []

        try:
            resp = await self._http.get("https://www.ecb.europa.eu/rss/press.html")
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                channel = root.find("channel") or root

                for item_el in channel.findall("item")[:limit]:
                    title = item_el.findtext("title", "")
                    desc = self._strip_html(item_el.findtext("description", ""))
                    link = item_el.findtext("link", "")
                    pub_date = item_el.findtext("pubDate", "")

                    try:
                        from email.utils import parsedate_to_datetime
                        created = parsedate_to_datetime(pub_date)
                    except Exception:
                        created = datetime.now(timezone.utc)

                    content = ScrapedContent(
                        id=self.make_id("ecb", hashlib.md5((link or title).encode()).hexdigest()),
                        platform=Platform.CENTRAL_BANK,
                        content_type=ContentType.ANNOUNCEMENT,
                        text=f"[ECB] {title}\n\n{desc}",
                        author=AuthorInfo(
                            username="ECB",
                            display_name="European Central Bank",
                        ),
                        engagement=EngagementMetrics(),
                        created_at=created,
                        source_url=link,
                        source_channel="ecb.europa.eu",
                        raw_metadata={
                            "bank": "ECB",
                            "country": "EU",
                            "title": title,
                        },
                        tags=["ECB", "EU", "monetary_policy"],
                    )
                    items.append(ScrapedItem(unified=content))
        except Exception as e:
            logger.warning(f"[CentralBank] ECB RSS failed: {e}")

        return items

    # ── Main interface ───────────────────────────────────────────

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Query = bank name (rbi, fed, ecb) or 'all'."""
        q = query.lower()
        if q == "rbi":
            return await self.scrape_rbi(limit)
        elif q in ("fed", "federal_reserve"):
            return await self.scrape_fed(limit)
        elif q == "ecb":
            return await self.scrape_ecb(limit)
        elif q == "all":
            return await self.scrape_all()
        return []

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        return await self.scrape(channel_id, limit)

    async def scrape_all(self) -> list[ScrapedItem]:
        """Scrape all central banks."""
        rbi = await self.scrape_rbi()
        fed = await self.scrape_fed()
        ecb = await self.scrape_ecb()
        all_items = rbi + fed + ecb
        logger.info(
            f"[CentralBank] Scraped {len(all_items)} announcements "
            f"(RBI={len(rbi)}, Fed={len(fed)}, ECB={len(ecb)})"
        )
        return all_items
