"""Standalone collector runner — no Docker, no Postgres, no Redis needed.

Raw data is saved to ./data/raw/{source}/{date}/*.json
DB upserts fail gracefully (logged as warnings).
"""

import asyncio
import logging
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Suppress noisy warnings from failed DB/Redis calls
logging.getLogger("core.base_collector").setLevel(logging.INFO)


# ── Collector configs (no API keys needed) ──────────────────

RSS_CONFIG = {
    "schedule": "*/5 * * * *",
    "timeout": 30,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "feeds": [
        {"name": "et_economy", "url": "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms", "category": "india_economy"},
        {"name": "et_markets", "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "category": "india_markets"},
        {"name": "mint_economy", "url": "https://www.livemint.com/rss/economy", "category": "india_economy"},
        {"name": "mint_markets", "url": "https://www.livemint.com/rss/markets", "category": "india_markets"},
        {"name": "moneycontrol", "url": "https://www.moneycontrol.com/rss/latestnews.xml", "category": "india_markets"},
        {"name": "coindesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "category": "crypto"},
        {"name": "cnbc", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664", "category": "global_markets"},
        {"name": "fed_press", "url": "https://www.federalreserve.gov/feeds/press_all.xml", "category": "fed_policy"},
        {"name": "arxiv_qfin", "url": "https://rss.arxiv.org/rss/q-fin", "category": "research"},
    ],
}

RBI_CIRCULARS_CONFIG = {
    "schedule": "0 */4 * * *",
    "timeout": 30,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "base_url": "https://rbi.org.in",
    "types": ["press_releases", "notifications", "circulars"],
}

RBI_DBIE_CONFIG = {
    "schedule": "0 6 * * *",
    "timeout": 45,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "base_url": "https://dbie.rbi.org.in",
    "datasets": [
        "weekly_statistical_supplement",
        "forex_reserves",
        "interest_rates",
        "exchange_rates",
    ],
}

SEBI_CONFIG = {
    "schedule": "0 */6 * * *",
    "timeout": 30,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "base_url": "https://www.sebi.gov.in",
    "types": ["circulars", "orders", "press_releases"],
}

WORLD_BANK_CONFIG = {
    "schedule": "0 0 * * 0",
    "timeout": 60,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "indicators": ["NY.GDP.MKTP.CD", "FP.CPI.TOTL.ZG", "BN.CAB.XOKA.CD"],
    "countries": ["IN", "US", "CN", "GB", "JP", "DE"],
}

IMF_CONFIG = {
    "schedule": "0 0 1 * *",
    "timeout": 60,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "datasets": ["IFS", "DOT", "BOP"],
}

CCIL_CONFIG = {
    "schedule": "0 17 * * 1-5",
    "timeout": 30,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "data_types": ["fbil_reference_rates", "sovereign_yield_curve", "mibor"],
}

NSE_CONFIG = {
    "schedule": "30 18 * * 1-5",
    "timeout": 45,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "types": ["equity", "fii_dii"],
}

BSE_CONFIG = {
    "schedule": "0 19 * * 1-5",
    "timeout": 30,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "types": ["corporate_actions", "board_meetings"],
}

# Also try FRED if API key is set
FRED_CONFIG = {
    "schedule": "0 */6 * * *",
    "timeout": 30,
    "retry_count": 2,
    "retry_backoff": 2.0,
    "api_key": "",  # filled from env
    "series": ["FEDFUNDS", "CPIAUCSL", "DGS10", "DGS2", "UNRATE", "GDP", "SOFR", "VIXCLS"],
}


async def run_one(name: str, collector_cls, config: dict) -> dict:
    """Run a single collector and return the result."""
    logger.info(f"{'─'*60}")
    logger.info(f"Starting: {name}")
    try:
        collector = collector_cls(config)
        result = await collector.run()
        status = result.get("status", "unknown")
        records = result.get("records_collected", 0)
        duration = result.get("duration_seconds", 0)
        error = result.get("error", "")

        if status == "success":
            logger.info(f"  {name}: {records} records in {duration}s")
        else:
            logger.warning(f"  {name}: FAILED — {error[:120]}")
        return {"name": name, **result}
    except Exception as e:
        logger.error(f"  {name}: ERROR — {e}")
        return {"name": name, "status": "error", "error": str(e), "records_collected": 0}


async def main():
    import os
    start = time.monotonic()

    print("\n" + "=" * 60)
    print("  EconScraper — Standalone Collection Run")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Import collectors
    collectors_to_run = []

    # RSS Feeds — always works
    from collectors.rss_feeds import RSSCollector
    collectors_to_run.append(("RSS Feeds", RSSCollector, RSS_CONFIG))

    # RBI Circulars
    from collectors.rbi_circulars import RBICirculars
    collectors_to_run.append(("RBI Circulars", RBICirculars, RBI_CIRCULARS_CONFIG))

    # RBI DBIE
    from collectors.rbi_dbie import RBIDbie
    collectors_to_run.append(("RBI DBIE", RBIDbie, RBI_DBIE_CONFIG))

    # SEBI
    from collectors.sebi_circulars import SEBICollector
    collectors_to_run.append(("SEBI Circulars", SEBICollector, SEBI_CONFIG))

    # CCIL Rates
    from collectors.ccil_rates import CCILCollector
    collectors_to_run.append(("CCIL Rates", CCILCollector, CCIL_CONFIG))

    # World Bank
    from collectors.world_bank import WorldBankCollector
    collectors_to_run.append(("World Bank", WorldBankCollector, WORLD_BANK_CONFIG))

    # IMF
    from collectors.imf_data import IMFCollector
    collectors_to_run.append(("IMF Data", IMFCollector, IMF_CONFIG))

    # NSE Bhavcopy
    from collectors.nse_bhavcopy import NSEBhavcopy
    collectors_to_run.append(("NSE Bhavcopy", NSEBhavcopy, NSE_CONFIG))

    # BSE
    from collectors.bse_api import BSECollector
    collectors_to_run.append(("BSE API", BSECollector, BSE_CONFIG))

    # FRED — only if API key is set
    fred_key = os.environ.get("FRED_API_KEY", "")
    if fred_key:
        from collectors.fred_api import FredCollector
        FRED_CONFIG["api_key"] = fred_key
        collectors_to_run.append(("FRED API", FredCollector, FRED_CONFIG))
    else:
        logger.info("Skipping FRED — no FRED_API_KEY set")

    # Run all collectors sequentially (to avoid thundering herd)
    results = []
    for name, cls, cfg in collectors_to_run:
        result = await run_one(name, cls, cfg)
        results.append(result)

    # Summary
    elapsed = time.monotonic() - start
    print("\n" + "=" * 60)
    print("  COLLECTION SUMMARY")
    print("=" * 60)

    total_records = 0
    succeeded = 0
    failed = 0
    for r in results:
        status_icon = "OK" if r["status"] == "success" else "FAIL"
        records = r.get("records_collected", 0)
        total_records += records
        if r["status"] == "success":
            succeeded += 1
        else:
            failed += 1
        error_note = f" — {r.get('error', '')[:60]}" if r["status"] != "success" else ""
        print(f"  [{status_icon:>4}] {r['name']:<20} {records:>5} records{error_note}")

    print(f"\n  {succeeded} succeeded, {failed} failed")
    print(f"  {total_records} total records collected")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Raw data saved to: ./data/raw/")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
