"""FRED (Federal Reserve Economic Data) collector.

Fetches US macro indicators: Fed Funds Rate, CPI, Treasury yields,
unemployment, GDP, SOFR, VIX, credit spreads.
"""

import logging
from datetime import datetime, timezone

import pandas as pd

from core.base_collector import BaseCollector
from core.exceptions import RateLimitError, SourceDownError

logger = logging.getLogger(__name__)


class FredCollector(BaseCollector):
    name = "fred_api"
    source_type = "api"

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key = config.get("api_key", "")
        self.series_ids = config.get("series", [])

    async def collect(self) -> list[dict]:
        if not self.api_key:
            raise SourceDownError(self.name, reason="FRED_API_KEY not set")

        records = []
        for series_id in self.series_ids:
            resp = await self._http.get(
                f"{self.BASE_URL}/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": self.api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 30,
                },
            )
            if resp.status_code == 429:
                raise RateLimitError(self.name, retry_after=60)
            if resp.status_code != 200:
                logger.warning(f"[FRED] {series_id} returned {resp.status_code}")
                continue

            data = resp.json()
            for obs in data.get("observations", []):
                if obs.get("value") != ".":
                    records.append({
                        "series_id": series_id,
                        "date": obs["date"],
                        "value": obs["value"],
                        "realtime_start": obs.get("realtime_start"),
                    })

        logger.info(f"[FRED] Collected {len(records)} observations across {len(self.series_ids)} series")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            try:
                rows.append({
                    "indicator": r["series_id"],
                    "date": datetime.strptime(r["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc),
                    "value": float(r["value"]),
                    "unit": "",
                    "metadata": {"realtime_start": r.get("realtime_start")},
                })
            except (ValueError, KeyError) as e:
                logger.warning(f"[FRED] Parse error: {e}")
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        required = ["indicator", "date", "value"]
        for col in required:
            if col not in df.columns:
                from core.exceptions import SchemaChangedError
                raise SchemaChangedError(self.name, required, list(df.columns))
        return True
