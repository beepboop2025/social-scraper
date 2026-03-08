"""World Bank API collector — GDP, inflation, trade indicators."""

import logging
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class WorldBankCollector(BaseCollector):
    name = "world_bank"
    source_type = "api"
    BASE_URL = "https://api.worldbank.org/v2"

    def __init__(self, config: dict):
        super().__init__(config)
        self.indicators = config.get("indicators", ["NY.GDP.MKTP.CD"])
        self.countries = config.get("countries", ["IN", "US", "CN"])

    async def collect(self) -> list[dict]:
        records = []
        for country in self.countries:
            for indicator in self.indicators:
                try:
                    resp = await self._http.get(
                        f"{self.BASE_URL}/country/{country}/indicator/{indicator}",
                        params={"format": "json", "per_page": 20, "date": "2020:2026"},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if len(data) > 1:
                            for item in data[1] or []:
                                if item.get("value") is not None:
                                    records.append({
                                        "indicator": indicator,
                                        "country": country,
                                        "date": item.get("date", ""),
                                        "value": item["value"],
                                        "country_name": item.get("country", {}).get("value", ""),
                                        "indicator_name": item.get("indicator", {}).get("value", ""),
                                    })
                except Exception as e:
                    logger.warning(f"[WorldBank] {country}/{indicator} failed: {e}")
        logger.info(f"[WorldBank] Collected {len(records)} data points")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            rows.append({
                "indicator": f"wb_{r['country']}_{r['indicator']}",
                "date": datetime(int(r["date"]), 1, 1, tzinfo=timezone.utc) if r.get("date", "").isdigit() else datetime.now(timezone.utc),
                "value": float(r["value"]) if r.get("value") else None,
                "unit": "",
                "metadata": {"country": r.get("country"), "indicator_name": r.get("indicator_name")},
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return "indicator" in df.columns
