"""IMF Data API collector."""

import logging
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class IMFCollector(BaseCollector):
    name = "imf_data"
    source_type = "api"
    BASE_URL = "http://dataservices.imf.org/REST/SDMX_JSON.svc"

    def __init__(self, config: dict):
        super().__init__(config)
        self.datasets = config.get("datasets", ["IFS"])

    async def collect(self) -> list[dict]:
        records = []
        for dataset in self.datasets:
            try:
                resp = await self._http.get(
                    f"{self.BASE_URL}/CompactData/{dataset}/A.IN+US+CN..?startPeriod=2020&endPeriod=2026"
                )
                if resp.status_code == 200:
                    data = resp.json()
                    series_list = data.get("CompactData", {}).get("DataSet", {}).get("Series", [])
                    if not isinstance(series_list, list):
                        series_list = [series_list]
                    for series in series_list:
                        indicator = series.get("@INDICATOR", "")
                        country = series.get("@REF_AREA", "")
                        obs = series.get("Obs", [])
                        if not isinstance(obs, list):
                            obs = [obs]
                        for o in obs:
                            if o.get("@OBS_VALUE"):
                                records.append({
                                    "indicator": f"imf_{dataset}_{country}_{indicator}",
                                    "date": o.get("@TIME_PERIOD", ""),
                                    "value": float(o["@OBS_VALUE"]),
                                    "country": country,
                                    "dataset": dataset,
                                })
            except Exception as e:
                logger.warning(f"[IMF] {dataset} failed: {e}")
        logger.info(f"[IMF] Collected {len(records)} data points")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            date_str = r.get("date", "")
            try:
                if len(date_str) == 4:
                    date = datetime(int(date_str), 1, 1, tzinfo=timezone.utc)
                else:
                    date = datetime.fromisoformat(date_str) if date_str else datetime.now(timezone.utc)
            except Exception:
                date = datetime.now(timezone.utc)
            rows.append({
                "indicator": r.get("indicator", ""),
                "date": date,
                "value": r.get("value"),
                "unit": "",
                "metadata": {"country": r.get("country"), "dataset": r.get("dataset")},
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return "indicator" in df.columns
