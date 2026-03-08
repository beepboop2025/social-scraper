"""BSE corporate actions, board meetings, results collector."""

import hashlib, logging
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class BSECollector(BaseCollector):
    name = "bse_api"
    source_type = "structured"
    BSE_URL = "https://api.bseindia.com/BseIndiaAPI/api"

    def __init__(self, config: dict):
        super().__init__(config)
        self.types = config.get("types", ["corporate_actions"])
        self._http.headers.update({"Referer": "https://www.bseindia.com/"})

    async def collect(self) -> list[dict]:
        records = []
        if "corporate_actions" in self.types:
            try:
                resp = await self._http.get(f"{self.BSE_URL}/DefaultData/GetCAData")
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        for item in data[:50]:
                            records.append({"type": "corporate_action", **item})
            except Exception as e:
                logger.warning(f"[BSE] Corporate actions failed: {e}")
        logger.info(f"[BSE] Collected {len(records)} records")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            for key, val in r.items():
                if isinstance(val, (int, float)) and key != "type":
                    rows.append({
                        "indicator": f"bse_{r.get('type', '')}_{key}",
                        "date": datetime.now(timezone.utc),
                        "value": float(val),
                        "metadata": r,
                    })
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["indicator", "date", "value", "metadata"])

    def validate(self, df: pd.DataFrame) -> bool:
        return True
