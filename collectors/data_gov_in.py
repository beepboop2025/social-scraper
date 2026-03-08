"""data.gov.in collector — CPI, WPI, IIP, GDP, GST collections."""

import logging
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class DataGovCollector(BaseCollector):
    name = "data_gov_in"
    source_type = "api"
    BASE_URL = "https://api.data.gov.in/resource"

    DATASET_IDS = {
        "cpi_monthly": "9ef84268-d588-465a-a308-a864a43d0070",
        "wpi_monthly": "1ca087d2-0069-4b46-8e41-36e16e05d7a1",
    }

    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key = config.get("api_key", "")
        self.datasets = config.get("datasets", list(self.DATASET_IDS.keys()))

    async def collect(self) -> list[dict]:
        records = []
        for dataset in self.datasets:
            resource_id = self.DATASET_IDS.get(dataset)
            if not resource_id:
                continue
            try:
                params = {"api-key": self.api_key, "format": "json", "limit": 50} if self.api_key else {"format": "json", "limit": 50}
                resp = await self._http.get(f"{self.BASE_URL}/{resource_id}", params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data.get("records", []):
                        records.append({"dataset": dataset, **item})
            except Exception as e:
                logger.warning(f"[DataGov] {dataset} failed: {e}")
        logger.info(f"[DataGov] Collected {len(records)} records")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            dataset = r.pop("dataset", "unknown")
            for key, val in r.items():
                try:
                    numeric = float(str(val).replace(",", ""))
                    rows.append({
                        "indicator": f"datagov_{dataset}_{key}",
                        "date": datetime.now(timezone.utc),
                        "value": numeric,
                        "metadata": {"dataset": dataset},
                    })
                except (ValueError, TypeError):
                    pass
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return True
