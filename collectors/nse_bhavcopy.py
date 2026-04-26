"""NSE daily bhavcopy + FII/DII flows collector."""

import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)

# IST timezone (+5:30)
_IST = timezone(timedelta(hours=5, minutes=30))


class NSEBhavcopy(BaseCollector):
    name = "nse_bhavcopy"
    source_type = "structured"

    NSE_URL = "https://www.nseindia.com"

    def __init__(self, config: dict):
        super().__init__(config)
        self.types = config.get("types", ["equity", "fii_dii"])
        self._http.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/json",
            "Referer": "https://www.nseindia.com/",
        })

    async def _get_nse_cookies(self):
        """NSE requires a session cookie from the homepage.

        The httpx client auto-persists cookies from the response, so we
        don't need the return value — just hitting the homepage is enough.
        Raises on non-200 so the caller aborts rather than making
        unauthenticated API calls that silently return no data.
        """
        resp = await self._http.get(self.NSE_URL)
        if resp.status_code != 200:
            raise RuntimeError(f"NSE homepage returned {resp.status_code}, cookies not set")

    async def collect(self) -> list[dict]:
        records = []
        try:
            await self._get_nse_cookies()
        except Exception as e:
            logger.error(f"[NSE] Cookie prefetch failed — aborting collection (all API calls require session cookies): {e}")
            return records

        if "fii_dii" in self.types:
            try:
                resp = await self._http.get(f"{self.NSE_URL}/api/fiidiiTradeReact")
                if resp.status_code != 200:
                    logger.warning(f"[NSE] FII/DII API returned HTTP {resp.status_code}")
                else:
                    data = resp.json()
                    for item in data if isinstance(data, list) else [data]:
                        records.append({
                            "indicator": "nse_fii_dii",
                            "data": item,
                            "type": "fii_dii",
                        })
            except Exception as e:
                logger.warning(f"[NSE] FII/DII failed: {e}")

        if "equity" in self.types:
            try:
                resp = await self._http.get(
                    f"{self.NSE_URL}/api/historical/cm/equity",
                    params={"symbol": "NIFTY 50"},
                )
                if resp.status_code != 200:
                    logger.warning(f"[NSE] Equity API returned HTTP {resp.status_code}")
                else:
                    data = resp.json()
                    records.append({"indicator": "nse_equity_snapshot", "data": data, "type": "equity"})
            except Exception as e:
                logger.warning(f"[NSE] Equity snapshot failed: {e}")

        if "derivatives" in self.types:
            try:
                ist_now = datetime.now(_IST)
                date_str = ist_now.strftime("%d-%m-%Y")
                resp = await self._http.get(
                    f"{self.NSE_URL}/api/reports",
                    params={"archives": f"[{{\"name\":\"F&O - Loss Data\",\"type\":\"archives\",\"reportType\":\"derivatives\",\"dt\":\"{date_str}\"}}]"},
                )
                if resp.status_code != 200:
                    logger.warning(f"[NSE] Derivatives API returned HTTP {resp.status_code}")
                else:
                    data = resp.json()
                    records.append({"indicator": "nse_derivatives_snapshot", "data": data, "type": "derivatives"})
            except Exception as e:
                logger.warning(f"[NSE] Derivatives snapshot failed: {e}")

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
                                    "metadata": {
                                        k: v for k, v in item.items()
                                        if isinstance(v, (str, int, float, bool))
                                    },
                                })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return True
        return "indicator" in df.columns and "value" in df.columns
