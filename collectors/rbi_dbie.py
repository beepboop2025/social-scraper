"""RBI DBIE (Database on Indian Economy) collector.

Fetches structured data: forex reserves, money supply, sectoral credit,
interest rates, exchange rates from RBI's data portal.
"""

import logging
import re
from datetime import datetime, timezone

import pandas as pd

from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class RBIDbie(BaseCollector):
    name = "rbi_dbie"
    source_type = "api"

    BASE_URL = "https://dbie.rbi.org.in/DBIE"

    def __init__(self, config: dict):
        super().__init__(config)
        self.datasets = config.get("datasets", [])
        self.base_url = config.get("base_url", self.BASE_URL)

    async def collect(self) -> list[dict]:
        records = []

        for dataset in self.datasets:
            try:
                if dataset == "forex_reserves":
                    records.extend(await self._collect_forex_reserves())
                elif dataset == "weekly_statistical_supplement":
                    records.extend(await self._collect_wss())
                elif dataset == "money_supply":
                    records.extend(await self._collect_money_supply())
                elif dataset == "interest_rates":
                    records.extend(await self._collect_interest_rates())
                elif dataset == "exchange_rates":
                    records.extend(await self._collect_exchange_rates())
                elif dataset == "sectoral_credit":
                    records.extend(await self._collect_sectoral_credit())
            except Exception as e:
                logger.warning(f"[RBI-DBIE] {dataset} failed: {e}")

        logger.info(f"[RBI-DBIE] Collected {len(records)} data points from {len(self.datasets)} datasets")
        return records

    async def _collect_forex_reserves(self) -> list[dict]:
        """Fetch India's forex reserves data."""
        resp = await self._http.get(
            f"{self.base_url}/dbie/api/data",
            params={"series": "forex_reserves", "format": "json"},
        )
        if resp.status_code != 200:
            # Try scraping the page
            return await self._scrape_rbi_page("forex-reserves")
        data = resp.json()
        return self._normalize_rbi_response(data, "forex_reserves")

    async def _collect_wss(self) -> list[dict]:
        return await self._scrape_rbi_page("weekly-statistical-supplement")

    async def _collect_money_supply(self) -> list[dict]:
        return await self._scrape_rbi_page("money-supply")

    async def _collect_interest_rates(self) -> list[dict]:
        """Key RBI policy rates: repo, reverse repo, MSF, bank rate."""
        records = []
        # These are well-known rates — scrape from RBI's current rates page
        try:
            resp = await self._http.get("https://rbi.org.in/scripts/BS_NSDPDisplay.aspx?param=4")
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                tables = soup.find_all("table")
                for table in tables:
                    for row in table.find_all("tr"):
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            name = cols[0].get_text(strip=True).lower()
                            val_text = cols[-1].get_text(strip=True)
                            match = re.search(r"(\d+\.?\d*)", val_text)
                            if match and any(kw in name for kw in ["repo", "bank rate", "msf", "reverse", "crr", "slr", "sdf", "standing deposit"]):
                                records.append({
                                    "indicator": f"rbi_{name.replace(' ', '_')}",
                                    "value": float(match.group(1)),
                                    "date": datetime.now(timezone.utc).isoformat(),
                                    "dataset": "interest_rates",
                                })
        except Exception as e:
            logger.warning(f"[RBI-DBIE] Interest rates scrape failed: {e}")
        return records

    async def _collect_exchange_rates(self) -> list[dict]:
        """RBI reference rates for major currencies."""
        records = []
        try:
            resp = await self._http.get("https://rbi.org.in/scripts/ReferenceRateArchive.aspx")
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                table = soup.find("table", id=lambda x: x and "grdReport" in str(x))
                if table:
                    for row in table.find_all("tr")[1:5]:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            currency = cols[0].get_text(strip=True)
                            rate_text = cols[1].get_text(strip=True)
                            match = re.search(r"(\d+\.?\d*)", rate_text)
                            if match:
                                records.append({
                                    "indicator": f"rbi_ref_{currency.lower().replace('/', '_')}",
                                    "value": float(match.group(1)),
                                    "date": datetime.now(timezone.utc).isoformat(),
                                    "dataset": "exchange_rates",
                                })
        except Exception as e:
            logger.warning(f"[RBI-DBIE] Exchange rates failed: {e}")
        return records

    async def _collect_sectoral_credit(self) -> list[dict]:
        return await self._scrape_rbi_page("sectoral-credit")

    # RBI publication page IDs — numeric IDs required by PublicationsView.aspx
    _RBI_PAGE_IDS = {
        "weekly-statistical-supplement": "11004",
        "money-supply": "11040",
        "forex-reserves": "10004",
        "sectoral-credit": "11043",
    }

    async def _scrape_rbi_page(self, page: str) -> list[dict]:
        """Generic RBI page scraper for tabular data."""
        records = []
        page_id = self._RBI_PAGE_IDS.get(page)
        if not page_id:
            logger.warning(f"[RBI-DBIE] No known page ID for '{page}', skipping scrape fallback")
            return records
        try:
            resp = await self._http.get(f"https://rbi.org.in/scripts/PublicationsView.aspx?Id={page_id}")
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                tables = soup.find_all("table")
                for table in tables[:3]:
                    for row in table.find_all("tr"):
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            name = cols[0].get_text(strip=True)
                            val = cols[-1].get_text(strip=True)
                            match = re.search(r"[\d,]+\.?\d*", val)
                            if match and name:
                                records.append({
                                    "indicator": f"rbi_{page}_{name[:50].lower().replace(' ', '_')}",
                                    "value": float(match.group().replace(",", "")),
                                    "date": datetime.now(timezone.utc).isoformat(),
                                    "dataset": page,
                                })
        except Exception as e:
            logger.warning(f"[RBI-DBIE] Scrape {page} failed: {e}")
        return records

    def _normalize_rbi_response(self, data: dict, dataset: str) -> list[dict]:
        records = []
        items = data if isinstance(data, list) else data.get("data", data.get("records", []))
        for item in items if isinstance(items, list) else []:
            for key, value in item.items():
                if isinstance(value, (int, float)):
                    records.append({
                        "indicator": f"rbi_{dataset}_{key}",
                        "value": float(value),
                        "date": item.get("date", datetime.now(timezone.utc).isoformat()),
                        "dataset": dataset,
                    })
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            date_str = r.get("date", "")
            try:
                date = datetime.fromisoformat(date_str) if date_str else datetime.now(timezone.utc)
            except (ValueError, TypeError):
                date = datetime.now(timezone.utc)
            rows.append({
                "indicator": r.get("indicator", ""),
                "date": date,
                "value": r.get("value"),
                "unit": r.get("unit", ""),
                "metadata": {"dataset": r.get("dataset", "")},
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return "indicator" in df.columns and "date" in df.columns
