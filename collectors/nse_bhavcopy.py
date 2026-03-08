"""NSE daily bhavcopy + FII/DII flows collector."""

import io, logging
from datetime import datetime, timezone, timedelta
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class NSEBhavcopy(BaseCollector):
    name = "nse_bhavcopy"
    source_type = "structured"

    NSE_URL = "https://www.nseindia.com"

    def __init__(self, config: dict):
        super().__init__(config)
        self.types = config.get("types", ["equity", "fii_dii"])
        self._http.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://www.nseindia.com/",
        })

    async def _get_nse_cookies(self):
        """NSE requires a session cookie from the homepage."""
        resp = await self._http.get(self.NSE_URL)
        return resp.cookies

    async def collect(self) -> list[dict]:
        records = []
        try:
            await self._get_nse_cookies()
        except Exception as e:
            logger.warning(f"[NSE] Cookie fetch failed: {e}")

        if "fii_dii" in self.types:
            try:
                resp = await self._http.get(f"{self.NSE_URL}/api/fiidiiTradeReact")
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data if isinstance(data, list) else [data]:
                        records.append({
                            "indicator": f"nse_fii_dii",
                            "data": item,
                            "type": "fii_dii",
                        })
            except Exception as e:
                logger.warning(f"[NSE] FII/DII failed: {e}")

        if "equity" in self.types:
            try:
                today = datetime.now(timezone.utc) - timedelta(hours=5, minutes=30)
                date_str = today.strftime("%d%m%Y")
                resp = await self._http.get(
                    f"{self.NSE_URL}/api/historical/cm/equity?symbol=NIFTY 50",
                )
                if resp.status_code == 200:
                    data = resp.json()
                    records.append({"indicator": "nse_equity_snapshot", "data": data, "type": "equity"})
            except Exception as e:
                logger.warning(f"[NSE] Equity snapshot failed: {e}")

        logger.info(f"[NSE] Collected {len(records)} datasets")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            data = r.get("data", {})
            if isinstance(data, dict):
                for key, val in data.items():
                    if isinstance(val, (int, float)):
                        rows.append({
                            "indicator": f"nse_{r.get('type', '')}_{key}",
                            "date": datetime.now(timezone.utc),
                            "value": float(val),
                            "unit": "INR",
                            "metadata": {"type": r.get("type")},
                        })
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        for key, val in item.items():
                            if isinstance(val, (int, float)):
                                rows.append({
                                    "indicator": f"nse_{r.get('type', '')}_{key}",
                                    "date": datetime.now(timezone.utc),
                                    "value": float(val),
                                    "unit": "INR",
                                    "metadata": item,
                                })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return True
