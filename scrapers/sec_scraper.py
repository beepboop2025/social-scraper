"""SEC EDGAR scraper — real-time SEC filing monitoring."""

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

# Filing types to monitor
MONITORED_FORMS = [
    "8-K",      # Current reports (material events)
    "10-K",     # Annual reports
    "10-Q",     # Quarterly reports
    "4",        # Insider trading
    "SC 13D",   # Beneficial ownership >5%
    "SC 13G",   # Passive beneficial ownership
    "13F-HR",   # Institutional holdings
    "S-1",      # IPO registration
    "DEF 14A",  # Proxy statements
    "6-K",      # Foreign private issuer reports
]

# Companies to specifically track
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META",
    "JPM", "BAC", "GS", "MS", "C", "WFC",  # Banks
    "BRK-B", "V", "MA",  # Financial
    "COIN", "MSTR",  # Crypto-adjacent
]


class SECScraper(BaseScraper):
    """Scrape SEC EDGAR for financial filings in real-time.

    Uses the EDGAR full-text search API and EFTS API.
    No API key required, but respects SEC rate limits (10 req/sec).
    """

    platform = Platform.SEC_EDGAR
    name = "sec_edgar"

    EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
    EDGAR_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
    FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index"

    def __init__(self, **kwargs):
        super().__init__(rate_limit=8, **kwargs)  # SEC allows 10 req/sec
        self._http = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "SocialScraper research@example.com",
                "Accept": "application/json",
            },
        )

    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()

    def _parse_filing(self, filing: dict) -> ScrapedItem:
        """Parse a filing from EDGAR search results."""
        form_type = filing.get("form_type", "") or filing.get("forms", "")
        entity_name = filing.get("entity_name", "") or filing.get("display_names", [""])[0]
        file_date = filing.get("file_date", "") or filing.get("period_of_report", "")
        description = filing.get("file_description", "") or ""

        try:
            created = datetime.strptime(file_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            created = datetime.now(timezone.utc)

        # Build filing URL
        accession = filing.get("accession_number", "") or filing.get("accession_no", "")
        cik = str(filing.get("entity_cik", "") or filing.get("ciks", [""])[0])
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}"

        tickers = filing.get("tickers", []) or []
        if isinstance(tickers, str):
            tickers = [tickers]

        content = ScrapedContent(
            id=self.make_id("sec", accession or f"{cik}-{file_date}"),
            platform=Platform.SEC_EDGAR,
            content_type=ContentType.FILING,
            text=f"[{form_type}] {entity_name}\n\n{description}".strip(),
            author=AuthorInfo(
                username=cik,
                display_name=entity_name,
            ),
            engagement=EngagementMetrics(),
            created_at=created,
            source_url=filing_url,
            source_channel="sec.gov",
            hashtags=tickers,
            raw_metadata={
                "form_type": form_type,
                "cik": cik,
                "accession_number": accession,
                "entity_name": entity_name,
                "tickers": tickers,
                "file_date": file_date,
                "period_of_report": filing.get("period_of_report"),
            },
            tags=[form_type] + tickers,
        )
        return ScrapedItem(unified=content)

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Search EDGAR full-text search."""
        resp = await self._http.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={
                "q": query,
                "dateRange": "custom",
                "startdt": "2026-01-01",
                "enddt": "2026-12-31",
                "forms": ",".join(MONITORED_FORMS),
            },
        )
        if resp.status_code != 200:
            # Fall back to RSS feed
            return await self._scrape_rss(query, limit)

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        return [self._parse_filing(h.get("_source", {})) for h in hits[:limit]]

    async def _scrape_rss(self, query: str = "", limit: int = 50) -> list[ScrapedItem]:
        """Fall back to EDGAR RSS feed for recent filings."""
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            "action": "getcompany",
            "type": "8-K",
            "dateb": "",
            "owner": "include",
            "count": min(limit, 40),
            "search_text": query,
            "output": "atom",
        }
        resp = await self._http.get(url, params=params)
        if resp.status_code != 200:
            return []

        # Parse Atom feed
        import defusedxml.ElementTree as ET
        root = ET.fromstring(resp.text)
        ns = "{http://www.w3.org/2005/Atom}"

        items = []
        for entry in root.findall(f"{ns}entry")[:limit]:
            title = entry.findtext(f"{ns}title", "")
            summary = entry.findtext(f"{ns}summary", "")
            link_el = entry.find(f"{ns}link")
            link = link_el.get("href", "") if link_el is not None else ""
            updated = entry.findtext(f"{ns}updated", "")

            try:
                created = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                created = datetime.now(timezone.utc)

            # Extract form type and company from title
            form_match = re.match(r"(\S+)\s*-\s*(.+)", title)
            form_type = form_match.group(1) if form_match else ""
            company = form_match.group(2) if form_match else title

            content = ScrapedContent(
                id=self.make_id("sec", "rss", link or title),
                platform=Platform.SEC_EDGAR,
                content_type=ContentType.FILING,
                text=f"[{form_type}] {company}\n\n{summary}",
                author=AuthorInfo(
                    username="sec.gov",
                    display_name=company,
                ),
                engagement=EngagementMetrics(),
                created_at=created,
                source_url=link,
                source_channel="sec.gov",
                raw_metadata={
                    "form_type": form_type,
                    "company": company,
                    "rss_source": True,
                },
                tags=[form_type],
            )
            items.append(ScrapedItem(unified=content))

        return items

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Channel = ticker symbol or CIK."""
        return await self.scrape(channel_id, limit)

    async def scrape_recent_filings(self, forms: Optional[list[str]] = None, limit: int = 50) -> list[ScrapedItem]:
        """Scrape the most recent filings of specified types."""
        forms = forms or MONITORED_FORMS
        all_items = []
        for form in forms:
            items = await self._scrape_rss(form, limit_per_form := limit // len(forms))
            all_items.extend(items)
            await asyncio.sleep(0.2)

        logger.info(f"[SEC] Scraped {len(all_items)} filings across {len(forms)} form types")
        return all_items

    async def scrape_insider_trades(self, limit: int = 50) -> list[ScrapedItem]:
        """Scrape Form 4 insider trading filings."""
        return await self._scrape_rss("4", limit)

    async def scrape_all(self) -> list[ScrapedItem]:
        """Full SEC scrape — recent filings + insider trades."""
        filings = await self.scrape_recent_filings()
        insider = await self.scrape_insider_trades(30)
        all_items = filings + insider
        logger.info(f"[SEC] Total scraped: {len(all_items)} (filings={len(filings)}, insider={len(insider)})")
        return all_items
