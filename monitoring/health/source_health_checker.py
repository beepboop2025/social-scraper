"""Main health checker for all EconScraper data sources."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import httpx
from bs4 import BeautifulSoup

IST = timezone(timedelta(hours=5, minutes=30))

# --- Data models ---

class HealthStatus(Enum):
    HEALTHY = "healthy"
    WARNING = "warning"
    BROKEN = "broken"
    DEGRADED = "degraded"


@dataclass
class HealthCheckResult:
    source_name: str
    status: HealthStatus
    response_time_ms: float
    last_data_date: datetime | None
    expected_structure_match: bool
    notes: str
    checked_at: datetime = field(default_factory=lambda: datetime.now(IST))


# --- Source-specific checkers ---

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

TIMEOUT = 30.0

RSS_FEEDS = {
    "Reuters Business": "https://news.google.com/rss/search?q=reuters+business+india&hl=en-IN&gl=IN&ceid=IN:en",
    "ET Economy": "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms",
    "Livemint Economy": "https://www.livemint.com/rss/economy",
    "Business Standard": "https://www.business-standard.com/rss/economy-102.rss",
    "Moneycontrol": "https://www.moneycontrol.com/rss/economy.xml",
}


def _parse_date_fuzzy(text: str) -> datetime | None:
    """Try common date formats found on Indian financial sites."""
    import re as _re
    patterns = [
        (r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", "%d/%m/%Y"),
        (r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", "%Y-%m-%d"),
        (r"(\w+ \d{1,2},? \d{4})", None),
    ]
    # Try explicit patterns first
    for pat, fmt in patterns:
        m = _re.search(pat, text)
        if m and fmt:
            try:
                raw = m.group().replace("-", "/")
                return datetime.strptime(raw, fmt).replace(tzinfo=IST)
            except ValueError:
                continue
    # Try dateutil as fallback
    try:
        from dateutil.parser import parse as du_parse
        return du_parse(text, fuzzy=True, dayfirst=True).replace(tzinfo=IST)
    except Exception:
        return None


def _is_weekday_stale(last_date: datetime | None, max_days: int = 3) -> bool:
    """Check if data is stale, accounting for weekends."""
    if not last_date:
        return True
    now = datetime.now(IST)
    delta = (now - last_date).days
    # Allow extra days for weekends
    if now.weekday() == 0:  # Monday
        max_days += 2
    elif now.weekday() == 6:  # Sunday
        max_days += 1
    return delta > max_days


async def check_rbi_dbie(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check RBI DBIE portal."""
    url = "https://dbie.rbi.org.in"
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True
    last_date = None

    try:
        resp = await client.get(url, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("RBI DBIE", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}", datetime.now(IST))

        text = resp.text
        if "Weekly Statistical Supplement" not in text:
            notes_parts.append("'Weekly Statistical Supplement' text not found — possible redesign")
            status = HealthStatus.WARNING
            structure_match = False

        # Try to find latest data date
        soup = BeautifulSoup(text, "lxml")
        date_candidates = soup.find_all(string=re.compile(r"\d{1,2}\s+\w+\s+\d{4}"))
        for candidate in date_candidates[:10]:
            parsed = _parse_date_fuzzy(candidate.strip())
            if parsed and parsed < datetime.now(IST):
                if last_date is None or parsed > last_date:
                    last_date = parsed
        if last_date and (datetime.now(IST) - last_date).days > 10:
            notes_parts.append(f"Latest data date {last_date.date()} is >10 days old")
            if status == HealthStatus.HEALTHY:
                status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("RBI DBIE", status, elapsed, last_date, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("RBI DBIE", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("RBI DBIE", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_nse(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check NSE India."""
    url = "https://www.nseindia.com"
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True

    try:
        resp = await client.get(url, headers=NSE_HEADERS, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("NSE India", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        # Check bhavcopy pattern
        bhavcopy_resp = await client.get(
            "https://www.nseindia.com/market-data/live-equity-market",
            headers=NSE_HEADERS, follow_redirects=True
        )
        if bhavcopy_resp.status_code != 200:
            notes_parts.append(f"Market data page returned {bhavcopy_resp.status_code}")
            status = HealthStatus.WARNING
            structure_match = False

        # Check FII/DII
        text = bhavcopy_resp.text if bhavcopy_resp.status_code == 200 else resp.text
        if "FII" not in text and "DII" not in text and "Foreign" not in text:
            notes_parts.append("FII/DII section not found on market data page")
            if status == HealthStatus.HEALTHY:
                status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("NSE India", status, elapsed, None, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("NSE India", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("NSE India", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_ccil(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check CCIL / FBIL reference rates."""
    url = "https://www.ccilindia.com/web/ccil/home"
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True
    last_date = None

    try:
        resp = await client.get(url, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("CCIL", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        if elapsed > 10000:
            notes_parts.append(f"Slow response: {elapsed:.0f}ms")
            status = HealthStatus.DEGRADED

        text = resp.text
        soup = BeautifulSoup(text, "lxml")

        # Check for yield curve / rate data
        if "reference rate" not in text.lower() and "fbil" not in text.lower():
            notes_parts.append("FBIL reference rate content not found")
            status = HealthStatus.WARNING
            structure_match = False

        # Find latest date
        date_cells = soup.find_all(string=re.compile(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}"))
        for cell in date_cells[:10]:
            parsed = _parse_date_fuzzy(cell.strip())
            if parsed and parsed < datetime.now(IST):
                if last_date is None or parsed > last_date:
                    last_date = parsed

        if _is_weekday_stale(last_date, max_days=3):
            notes_parts.append(f"Latest rate date may be stale: {last_date.date() if last_date else 'not found'}")
            if status == HealthStatus.HEALTHY:
                status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("CCIL", status, elapsed, last_date, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("CCIL", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("CCIL", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_fred(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check FRED API."""
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True

    try:
        # Check FRED main page first (no API key needed)
        resp = await client.get("https://fred.stlouisfed.org", follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("FRED API", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        # Try the API endpoint — uses environment FRED_API_KEY or skips
        import os
        api_key = os.environ.get("FRED_API_KEY", "")
        if api_key:
            api_url = f"https://api.stlouisfed.org/fred/series?series_id=GDP&api_key={api_key}&file_type=json"
            api_resp = await client.get(api_url, follow_redirects=True)

            # Check for deprecation headers
            deprecation_headers = ["deprecation", "sunset", "x-deprecation"]
            for h in deprecation_headers:
                if h in api_resp.headers:
                    notes_parts.append(f"Deprecation header found: {h}={api_resp.headers[h]}")
                    status = HealthStatus.WARNING

            if api_resp.status_code == 200:
                try:
                    data = api_resp.json()
                    if "seriess" not in data:
                        notes_parts.append("Unexpected JSON structure — 'seriess' key missing")
                        structure_match = False
                        status = HealthStatus.WARNING
                except Exception:
                    notes_parts.append("API response is not valid JSON")
                    structure_match = False
                    status = HealthStatus.WARNING
            else:
                notes_parts.append(f"API returned {api_resp.status_code} (key may be invalid)")
                status = HealthStatus.WARNING
        else:
            notes_parts.append("FRED_API_KEY not set — skipped API validation")

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("FRED API", status, elapsed, None, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("FRED API", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("FRED API", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_sebi(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check SEBI circulars page."""
    url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0"
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True
    last_date = None

    try:
        resp = await client.get(url, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("SEBI", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        soup = BeautifulSoup(resp.text, "lxml")

        # Check for table/list structure
        tables = soup.find_all("table")
        listings = soup.find_all("div", class_=re.compile(r"list|result|circular", re.I))
        if not tables and not listings:
            notes_parts.append("No table or listing structure found — possible redesign")
            structure_match = False
            status = HealthStatus.WARNING

        # Find latest circular date
        date_cells = soup.find_all(string=re.compile(r"\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{4}"))
        for cell in date_cells[:10]:
            parsed = _parse_date_fuzzy(cell.strip())
            if parsed and parsed < datetime.now(IST):
                if last_date is None or parsed > last_date:
                    last_date = parsed

        if last_date and (datetime.now(IST) - last_date).days > 7:
            notes_parts.append(f"Latest circular date {last_date.date()} is >7 days old")
            if status == HealthStatus.HEALTHY:
                status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("SEBI", status, elapsed, last_date, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("SEBI", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("SEBI", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_rbi_circulars(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check RBI Notifications page."""
    url = "https://rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True
    last_date = None

    try:
        resp = await client.get(url, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("RBI Circulars", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        soup = BeautifulSoup(resp.text, "lxml")
        date_cells = soup.find_all(string=re.compile(r"\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{4}"))
        for cell in date_cells[:10]:
            parsed = _parse_date_fuzzy(cell.strip())
            if parsed and parsed < datetime.now(IST):
                if last_date is None or parsed > last_date:
                    last_date = parsed

        if last_date and (datetime.now(IST) - last_date).days > 7:
            notes_parts.append(f"Latest notification {last_date.date()} is >7 days old")
            if status == HealthStatus.HEALTHY:
                status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("RBI Circulars", status, elapsed, last_date, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("RBI Circulars", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("RBI Circulars", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_data_gov_in(client: httpx.AsyncClient) -> HealthCheckResult:
    """Check data.gov.in API."""
    url = "https://data.gov.in"
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True

    try:
        resp = await client.get(url, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult("data.gov.in", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        if elapsed > 15000:
            notes_parts.append(f"Very slow response: {elapsed:.0f}ms")
            status = HealthStatus.DEGRADED

        # Check that the site still has data catalog references
        text = resp.text.lower()
        if "catalog" not in text and "dataset" not in text and "api" not in text:
            notes_parts.append("Expected data catalog content not found")
            structure_match = False
            status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append("OK")
        return HealthCheckResult("data.gov.in", status, elapsed, None, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult("data.gov.in", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult("data.gov.in", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


async def check_single_rss(client: httpx.AsyncClient, name: str, url: str) -> HealthCheckResult:
    """Check a single RSS feed."""
    t0 = time.monotonic()
    notes_parts: list[str] = []
    status = HealthStatus.HEALTHY
    structure_match = True
    last_date = None

    try:
        resp = await client.get(url, follow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        if resp.status_code != 200:
            return HealthCheckResult(f"RSS: {name}", HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")

        content_type = resp.headers.get("content-type", "")
        text = resp.text

        # Basic XML validation
        if not text.strip().startswith("<?xml") and "<rss" not in text[:500] and "<feed" not in text[:500]:
            notes_parts.append("Response is not valid XML/RSS")
            structure_match = False
            status = HealthStatus.BROKEN
        else:
            soup = BeautifulSoup(text, "lxml-xml")
            items = soup.find_all("item") or soup.find_all("entry")

            if not items:
                notes_parts.append("Feed has 0 entries")
                status = HealthStatus.BROKEN
                structure_match = False
            else:
                # Check required fields
                first = items[0]
                missing = []
                for field_name in ["title", "link"]:
                    if not first.find(field_name):
                        missing.append(field_name)
                if not first.find("pubDate") and not first.find("published") and not first.find("updated"):
                    missing.append("date field")
                if missing:
                    notes_parts.append(f"Missing fields: {', '.join(missing)}")
                    status = HealthStatus.WARNING

                # Check freshness
                date_tag = first.find("pubDate") or first.find("published") or first.find("updated")
                if date_tag and date_tag.string:
                    last_date = _parse_date_fuzzy(date_tag.string.strip())
                    if last_date and (datetime.now(IST) - last_date).total_seconds() > 48 * 3600:
                        notes_parts.append(f"Latest entry is >48h old ({last_date.date()})")
                        if status == HealthStatus.HEALTHY:
                            status = HealthStatus.WARNING

        if not notes_parts:
            notes_parts.append(f"OK — {len(items) if 'items' in dir() else '?'} entries")
        return HealthCheckResult(f"RSS: {name}", status, elapsed, last_date, structure_match,
                                 "; ".join(notes_parts))
    except httpx.TimeoutException:
        return HealthCheckResult(f"RSS: {name}", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, "Timeout")
    except Exception as e:
        return HealthCheckResult(f"RSS: {name}", HealthStatus.BROKEN,
                                 (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")


# --- Main entry points ---

async def check_all_sources() -> list[HealthCheckResult]:
    """Run full health checks on all sources concurrently."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT), follow_redirects=True) as client:
        tasks = [
            check_rbi_dbie(client),
            check_nse(client),
            check_ccil(client),
            check_fred(client),
            check_sebi(client),
            check_rbi_circulars(client),
            check_data_gov_in(client),
        ]
        # Add RSS feed checks
        for name, url in RSS_FEEDS.items():
            tasks.append(check_single_rss(client, name, url))

        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to BROKEN results
    final: list[HealthCheckResult] = []
    source_names = (
        ["RBI DBIE", "NSE India", "CCIL", "FRED API", "SEBI", "RBI Circulars", "data.gov.in"]
        + [f"RSS: {n}" for n in RSS_FEEDS]
    )
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            name = source_names[i] if i < len(source_names) else f"Source #{i}"
            final.append(HealthCheckResult(name, HealthStatus.BROKEN, 0, None, False,
                                           f"Unhandled error: {result}"))
        else:
            final.append(result)
    return final


async def quick_check_all_sources() -> list[HealthCheckResult]:
    """Quick HTTP-only reachability check (HEAD requests where possible)."""
    urls = {
        "RBI DBIE": "https://dbie.rbi.org.in",
        "NSE India": "https://www.nseindia.com",
        "CCIL": "https://www.ccilindia.com",
        "FRED API": "https://fred.stlouisfed.org",
        "SEBI": "https://www.sebi.gov.in",
        "RBI Circulars": "https://rbi.org.in",
        "data.gov.in": "https://data.gov.in",
    }
    for name, feed_url in RSS_FEEDS.items():
        urls[f"RSS: {name}"] = feed_url

    # Use browser-like headers for all sources to avoid 403s
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    async def _quick(client: httpx.AsyncClient, name: str, url: str) -> HealthCheckResult:
        t0 = time.monotonic()
        headers = {**browser_headers}
        if "nse" in name.lower():
            headers["Referer"] = "https://www.nseindia.com/"
        try:
            # Go straight to GET — many Indian sites block HEAD
            resp = await client.get(url, headers=headers, follow_redirects=True)
            elapsed = (time.monotonic() - t0) * 1000
            if resp.status_code < 400:
                st = HealthStatus.DEGRADED if elapsed > 10000 else HealthStatus.HEALTHY
                return HealthCheckResult(name, st, elapsed, None, True,
                                         f"HTTP {resp.status_code} — {elapsed:.0f}ms")
            # 403 from sites with anti-bot (NSE, some RSS) means site is UP but blocking
            if resp.status_code == 403:
                return HealthCheckResult(name, HealthStatus.DEGRADED, elapsed, None, True,
                                         f"HTTP 403 (anti-bot) — site is reachable")
            return HealthCheckResult(name, HealthStatus.BROKEN, elapsed, None, False,
                                     f"HTTP {resp.status_code}")
        except httpx.TimeoutException:
            return HealthCheckResult(name, HealthStatus.BROKEN,
                                     (time.monotonic() - t0) * 1000, None, False, "Timeout")
        except Exception as e:
            return HealthCheckResult(name, HealthStatus.BROKEN,
                                     (time.monotonic() - t0) * 1000, None, False, f"Error: {e}")

    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT), follow_redirects=True) as client:
        tasks = [_quick(client, name, url) for name, url in urls.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    final: list[HealthCheckResult] = []
    names = list(urls.keys())
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final.append(HealthCheckResult(names[i], HealthStatus.BROKEN, 0, None, False, str(r)))
        else:
            final.append(r)
    return final


def format_results_table(results: list[HealthCheckResult]) -> str:
    """Format results as a readable table for terminal output."""
    status_icons = {
        HealthStatus.HEALTHY: "\033[92m HEALTHY \033[0m",
        HealthStatus.WARNING: "\033[93m WARNING \033[0m",
        HealthStatus.BROKEN: "\033[91m BROKEN  \033[0m",
        HealthStatus.DEGRADED: "\033[93m DEGRADED\033[0m",
    }
    lines = [
        "",
        "=" * 90,
        f"  EconScraper Health Check — {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}",
        "=" * 90,
        f"  {'Source':<22} {'Status':<20} {'Time':>8}  {'Notes'}",
        "-" * 90,
    ]
    for r in results:
        icon = status_icons.get(r.status, r.status.value)
        time_str = f"{r.response_time_ms:.0f}ms"
        lines.append(f"  {r.source_name:<22} {icon}  {time_str:>8}  {r.notes[:40]}")
    lines.append("-" * 90)

    healthy = sum(1 for r in results if r.status == HealthStatus.HEALTHY)
    lines.append(f"  {healthy}/{len(results)} sources healthy")
    lines.append("=" * 90)
    return "\n".join(lines)
