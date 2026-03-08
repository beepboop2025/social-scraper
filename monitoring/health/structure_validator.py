"""Deep structure validation — detects silent breaking changes in data sources."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup

from monitoring.health.source_health_checker import (
    HealthCheckResult,
    HealthStatus,
    NSE_HEADERS,
    TIMEOUT,
    RSS_FEEDS,
    IST,
)

BASELINES_DIR = Path(__file__).parent / "baselines"


def _load_baseline(source_name: str) -> dict | None:
    """Load a baseline JSON file for a source."""
    path = BASELINES_DIR / f"{source_name}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _save_baseline(source_name: str, data: dict) -> None:
    """Save a baseline JSON file."""
    BASELINES_DIR.mkdir(parents=True, exist_ok=True)
    path = BASELINES_DIR / f"{source_name}.json"
    data["created_at"] = datetime.now(IST).isoformat()
    path.write_text(json.dumps(data, indent=2, default=str))


def _extract_html_fingerprint(html: str, url: str) -> dict:
    """Extract structural fingerprints from an HTML page."""
    soup = BeautifulSoup(html, "lxml")

    # Key CSS selectors that indicate data tables
    tables = soup.find_all("table")
    table_classes = []
    for t in tables:
        cls = t.get("class", [])
        if cls:
            table_classes.append(".".join(cls))

    # Section headers
    headers = []
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "th"]):
        text = tag.get_text(strip=True)
        if text and len(text) < 100:
            headers.append(text)

    # Download link URL patterns
    download_patterns = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(ext in href.lower() for ext in [".csv", ".xls", ".xlsx", ".pdf", ".zip"]):
            # Convert to regex pattern (replace dates/numbers with wildcards)
            pattern = re.sub(r"\d{4}[-/]\d{2}[-/]\d{2}", r"\\d{4}[-/]\\d{2}[-/]\\d{2}", href)
            pattern = re.sub(r"\d{8,}", r"\\d+", pattern)
            download_patterns.append(pattern)

    return {
        "url": url,
        "http_status": 200,
        "content_type": "text/html",
        "key_selectors": table_classes[:20],
        "table_count": len(tables),
        "section_headers": headers[:30],
        "download_url_patterns": list(set(download_patterns))[:20],
        "response_size_range": [max(0, len(html) - 5000), len(html) + 5000],
    }


def _extract_api_fingerprint(data: dict | list, url: str) -> dict:
    """Extract structural fingerprints from a JSON API response."""
    def _schema(obj: Any, depth: int = 0) -> Any:
        if depth > 3:
            return type(obj).__name__
        if isinstance(obj, dict):
            return {k: _schema(v, depth + 1) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_schema(obj[0], depth + 1)] if obj else ["empty"]
        return type(obj).__name__

    return {
        "url": url,
        "http_status": 200,
        "content_type": "application/json",
        "json_schema": _schema(data),
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
        "response_size_range": [0, 100000],
    }


def _extract_rss_fingerprint(xml_text: str, url: str) -> dict:
    """Extract structural fingerprints from an RSS feed."""
    soup = BeautifulSoup(xml_text, "lxml-xml")
    items = soup.find_all("item") or soup.find_all("entry")

    # Check which fields are present in items
    item_fields = set()
    for item in items[:5]:
        for child in item.children:
            if hasattr(child, "name") and child.name:
                item_fields.add(child.name)

    is_atom = bool(soup.find("feed"))

    return {
        "url": url,
        "http_status": 200,
        "content_type": "application/xml",
        "format": "atom" if is_atom else "rss2",
        "item_count_range": [max(0, len(items) - 5), len(items) + 10],
        "item_fields": sorted(item_fields),
        "response_size_range": [max(0, len(xml_text) - 5000), len(xml_text) + 5000],
    }


def _compare_fingerprints(current: dict, baseline: dict) -> tuple[bool, list[str]]:
    """Compare current fingerprint against baseline. Returns (match, diffs)."""
    diffs: list[str] = []

    # Check table count (HTML)
    if "table_count" in baseline:
        base_count = baseline["table_count"]
        curr_count = current.get("table_count", 0)
        if abs(curr_count - base_count) > max(2, base_count * 0.5):
            diffs.append(f"Table count changed: {base_count} -> {curr_count}")

    # Check section headers (HTML)
    if "section_headers" in baseline:
        base_headers = set(baseline["section_headers"])
        curr_headers = set(current.get("section_headers", []))
        missing = base_headers - curr_headers
        if missing and len(missing) > len(base_headers) * 0.3:
            diffs.append(f"Missing headers: {list(missing)[:5]}")

    # Check key selectors (HTML)
    if "key_selectors" in baseline:
        base_sel = set(baseline["key_selectors"])
        curr_sel = set(current.get("key_selectors", []))
        missing_sel = base_sel - curr_sel
        if missing_sel:
            diffs.append(f"Missing CSS selectors: {list(missing_sel)[:5]}")

    # Check response size
    if "response_size_range" in baseline:
        lo, hi = baseline["response_size_range"]
        curr_size = current.get("response_size_range", [0, 0])
        curr_mid = (curr_size[0] + curr_size[1]) // 2
        if curr_mid < lo * 0.3 or curr_mid > hi * 3:
            diffs.append(f"Response size drastically changed")

    # Check JSON schema (API)
    if "top_level_keys" in baseline:
        base_keys = set(baseline["top_level_keys"])
        curr_keys = set(current.get("top_level_keys", []))
        missing_keys = base_keys - curr_keys
        new_keys = curr_keys - base_keys
        if missing_keys:
            diffs.append(f"Missing API keys: {list(missing_keys)}")
        if new_keys:
            diffs.append(f"New API keys (info): {list(new_keys)}")

    # Check RSS item fields
    if "item_fields" in baseline:
        base_fields = set(baseline["item_fields"])
        curr_fields = set(current.get("item_fields", []))
        missing_fields = base_fields - curr_fields
        if missing_fields:
            diffs.append(f"Missing RSS fields: {list(missing_fields)}")

    # Check item count range (RSS)
    if "item_count_range" in baseline:
        lo, hi = baseline["item_count_range"]
        curr_range = current.get("item_count_range", [0, 0])
        if curr_range[1] == 0:
            diffs.append("RSS feed has 0 items")

    match = len(diffs) == 0
    return match, diffs


# --- Source-specific validators ---

SOURCE_CONFIGS: dict[str, dict] = {
    "rbi_dbie": {
        "url": "https://dbie.rbi.org.in",
        "type": "html",
        "headers": {},
    },
    "nse": {
        "url": "https://www.nseindia.com/market-data/live-equity-market",
        "type": "html",
        "headers": NSE_HEADERS,
    },
    "ccil": {
        "url": "https://www.ccilindia.com/web/ccil/home",
        "type": "html",
        "headers": {},
    },
    "sebi": {
        "url": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0",
        "type": "html",
        "headers": {},
    },
    "rbi_circulars": {
        "url": "https://rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx",
        "type": "html",
        "headers": {},
    },
    "fred": {
        "url": "https://fred.stlouisfed.org",
        "type": "html",
        "headers": {},
    },
    "data_gov_in": {
        "url": "https://data.gov.in",
        "type": "html",
        "headers": {},
    },
}


async def validate_source(client: httpx.AsyncClient, source_name: str) -> tuple[str, bool, list[str]]:
    """Validate a single source against its baseline. Returns (source_name, match, diffs)."""
    config = SOURCE_CONFIGS.get(source_name)
    if not config:
        return source_name, False, [f"Unknown source: {source_name}"]

    baseline = _load_baseline(source_name)

    try:
        resp = await client.get(config["url"], headers=config.get("headers", {}), follow_redirects=True)
        if resp.status_code >= 400:
            return source_name, False, [f"HTTP {resp.status_code}"]

        if config["type"] == "html":
            fingerprint = _extract_html_fingerprint(resp.text, config["url"])
        elif config["type"] == "json":
            fingerprint = _extract_api_fingerprint(resp.json(), config["url"])
        else:
            return source_name, True, []

        if baseline is None:
            return source_name, True, ["No baseline exists — run `baseline` command to create one"]

        match, diffs = _compare_fingerprints(fingerprint, baseline)
        return source_name, match, diffs

    except Exception as e:
        return source_name, False, [f"Error: {e}"]


async def validate_rss_feeds(client: httpx.AsyncClient) -> list[tuple[str, bool, list[str]]]:
    """Validate all RSS feeds against baselines."""
    baseline = _load_baseline("rss_feeds")
    results = []

    for name, url in RSS_FEEDS.items():
        try:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code >= 400:
                results.append((f"RSS: {name}", False, [f"HTTP {resp.status_code}"]))
                continue

            fingerprint = _extract_rss_fingerprint(resp.text, url)

            if baseline is None or name not in baseline.get("feeds", {}):
                results.append((f"RSS: {name}", True, ["No baseline"]))
                continue

            match, diffs = _compare_fingerprints(fingerprint, baseline["feeds"][name])
            results.append((f"RSS: {name}", match, diffs))
        except Exception as e:
            results.append((f"RSS: {name}", False, [f"Error: {e}"]))

    return results


async def validate_all() -> list[tuple[str, bool, list[str]]]:
    """Validate all sources against baselines."""
    import asyncio

    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT), follow_redirects=True) as client:
        tasks = [validate_source(client, name) for name in SOURCE_CONFIGS]
        source_results = await asyncio.gather(*tasks, return_exceptions=True)

        rss_results = await validate_rss_feeds(client)

    final = []
    for r in source_results:
        if isinstance(r, Exception):
            final.append(("unknown", False, [str(r)]))
        else:
            final.append(r)
    final.extend(rss_results)
    return final


# --- Baseline management ---

async def update_baseline(source_name: str | None = None) -> list[str]:
    """
    Update baselines by fetching current structure.
    If source_name is None, update all baselines.
    """
    updated: list[str] = []

    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT), follow_redirects=True) as client:
        sources_to_update = (
            {source_name: SOURCE_CONFIGS[source_name]}
            if source_name and source_name in SOURCE_CONFIGS
            else SOURCE_CONFIGS
        )

        for name, config in sources_to_update.items():
            try:
                resp = await client.get(config["url"], headers=config.get("headers", {}),
                                         follow_redirects=True)
                if resp.status_code >= 400:
                    updated.append(f"  SKIP {name}: HTTP {resp.status_code}")
                    continue

                if config["type"] == "html":
                    fingerprint = _extract_html_fingerprint(resp.text, config["url"])
                elif config["type"] == "json":
                    fingerprint = _extract_api_fingerprint(resp.json(), config["url"])
                else:
                    continue

                _save_baseline(name, fingerprint)
                updated.append(f"  OK   {name}")
            except Exception as e:
                updated.append(f"  FAIL {name}: {e}")

        # RSS feeds baseline
        if source_name is None or source_name == "rss_feeds":
            rss_baseline = {"feeds": {}}
            for name, url in RSS_FEEDS.items():
                try:
                    resp = await client.get(url, follow_redirects=True)
                    if resp.status_code < 400:
                        rss_baseline["feeds"][name] = _extract_rss_fingerprint(resp.text, url)
                        updated.append(f"  OK   RSS: {name}")
                    else:
                        updated.append(f"  SKIP RSS: {name}: HTTP {resp.status_code}")
                except Exception as e:
                    updated.append(f"  FAIL RSS: {name}: {e}")

            _save_baseline("rss_feeds", rss_baseline)

    return updated
