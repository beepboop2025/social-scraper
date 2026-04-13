"""CCIL/FBIL rates collector — MIBOR, TREPS, yield curve, CP/CD rates.

Critical for LiquiFi treasury management.
"""

import logging
import re
from datetime import datetime, timezone

import pandas as pd

from core.base_collector import BaseCollector
from core.exceptions import SourceDownError

logger = logging.getLogger(__name__)


class CCILCollector(BaseCollector):
    name = "ccil_rates"
    source_type = "api"

    FBIL_URL = "https://www.fbil.org.in"

    def __init__(self, config: dict):
        super().__init__(config)
        self.data_types = config.get("data_types", ["fbil_reference_rates", "mibor"])

    async def collect(self) -> list[dict]:
        records = []
        failures = []

        for dtype in self.data_types:
            try:
                if dtype == "fbil_reference_rates":
                    records.extend(await self._collect_fbil_rates())
                elif dtype == "mibor":
                    records.extend(await self._collect_mibor())
                elif dtype == "treps_rates":
                    records.extend(await self._collect_treps())
                elif dtype == "sovereign_yield_curve":
                    records.extend(await self._collect_yield_curve())
                elif dtype in ("cp_rates", "cd_rates"):
                    records.extend(await self._collect_money_market(dtype))
            except Exception as e:
                failures.append(dtype)
                logger.warning(f"[CCIL] {dtype} collection failed: {e}")

        if not records and failures:
            raise SourceDownError(
                self.name,
                url=self.FBIL_URL,
                status_code=0,
            )

        logger.info(f"[CCIL] Collected {len(records)} rate observations")
        return records

    async def _collect_fbil_rates(self) -> list[dict]:
        """FBIL benchmark rates — MIBOR O/N, Term MIBOR, MIFOR."""
        records = []

        # Known metadata fields that are NOT rate observations
        _meta_fields = {"status", "status_code", "count", "total", "version",
                        "timestamp", "id", "page", "size", "error", "code"}

        # Try JSON API first
        try:
            resp = await self._http.get(f"{self.FBIL_URL}/api/ratesapi")
            if resp.status_code == 200:
                data = resp.json()
                if not isinstance(data, (dict, list)):
                    logger.warning(f"[CCIL] FBIL API returned unexpected type: {type(data).__name__}")
                else:
                    for item in data if isinstance(data, list) else [data]:
                        if not isinstance(item, dict):
                            continue
                        for key, value in item.items():
                            if isinstance(value, (int, float)) and key.lower() not in _meta_fields:
                                records.append({
                                    "indicator": f"fbil_{key}",
                                    "value": float(value),
                                    "date": datetime.now(timezone.utc).isoformat(),
                                    "source_type": "fbil_reference_rates",
                                })
                if records:
                    return records
        except Exception as e:
            logger.warning(f"[CCIL] FBIL API failed: {e}")

        # Fallback: scrape HTML page
        try:
            from bs4 import BeautifulSoup
            resp = await self._http.get(f"{self.FBIL_URL}")
            if resp.status_code != 200:
                logger.warning(f"[CCIL] FBIL HTML page returned {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            rate_tables = soup.find_all("table")
            for table in rate_tables:
                for row in table.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 2:
                        name = cols[0].get_text(strip=True)
                        val_text = cols[-1].get_text(strip=True)
                        try:
                            # Extract first decimal number from text
                            # (re.sub can produce "1.2.3" from "1.2% on 15.3")
                            match = re.search(r"\d+(?:\.\d+)?", val_text)
                            if not match:
                                continue
                            val = float(match.group())
                            records.append({
                                "indicator": f"fbil_{name.lower().replace(' ', '_')}",
                                "value": val,
                                "date": datetime.now(timezone.utc).isoformat(),
                                "source_type": "fbil_reference_rates",
                            })
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            logger.warning(f"[CCIL] FBIL HTML scrape failed: {e}")

        return records

    async def _collect_mibor(self) -> list[dict]:
        """MIBOR overnight, 14-day, 1-month, 3-month via FBIL."""
        try:
            resp = await self._http.get(f"{self.FBIL_URL}/api/mibor")
            if resp.status_code != 200:
                logger.warning(f"[CCIL] MIBOR endpoint returned {resp.status_code}")
                return []
            data = resp.json()
            if not isinstance(data, dict):
                logger.warning(f"[CCIL] MIBOR returned non-dict response: {type(data).__name__}")
                return []
            records = []
            tenors = {"overnight": "O/N", "14day": "14D", "1month": "1M", "3month": "3M"}
            for tenor_key, tenor_label in tenors.items():
                value = data.get(tenor_key) or data.get(tenor_label)
                if value is not None:
                    try:
                        records.append({
                            "indicator": f"mibor_{tenor_key}",
                            "value": float(value),
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_type": "mibor",
                            "metadata": {"tenor": tenor_label},
                        })
                    except (ValueError, TypeError):
                        logger.warning(f"[CCIL] MIBOR {tenor_key}: non-numeric value {value!r}")
            return records
        except Exception as e:
            logger.warning(f"[CCIL] MIBOR collection failed: {e}")
            return []

    async def _collect_treps(self) -> list[dict]:
        """TREPS (Triparty Repo) rates."""
        try:
            resp = await self._http.get(f"{self.FBIL_URL}/api/treps")
            if resp.status_code != 200:
                logger.warning(f"[CCIL] TREPS endpoint returned {resp.status_code}")
                return []
            data = resp.json()
            if not isinstance(data, dict):
                logger.warning(f"[CCIL] TREPS returned non-dict response: {type(data).__name__}")
                return []
            value = data.get("weighted_avg") or data.get("rate")
            if value is None:
                logger.warning("[CCIL] TREPS response missing rate value")
                return []
            try:
                float_value = float(value)
            except (ValueError, TypeError):
                logger.warning(f"[CCIL] TREPS: non-numeric value {value!r}")
                return []
            return [{
                "indicator": "treps_weighted_avg",
                "value": float_value,
                "date": datetime.now(timezone.utc).isoformat(),
                "source_type": "treps_rates",
            }]
        except Exception as e:
            logger.warning(f"[CCIL] TREPS collection failed: {e}")
            return []

    async def _collect_yield_curve(self) -> list[dict]:
        """Sovereign yield curve — various tenors."""
        try:
            resp = await self._http.get(f"{self.FBIL_URL}/api/yield-curve")
            if resp.status_code != 200:
                logger.warning(f"[CCIL] Yield curve endpoint returned {resp.status_code}")
                return []
            data = resp.json()
            if not isinstance(data, dict):
                logger.warning(f"[CCIL] Yield curve returned non-dict response: {type(data).__name__}")
                return []
            records = []
            tenors = ["3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "30Y"]
            for t in tenors:
                value = data.get(t) or data.get(t.lower())
                if value is not None:
                    try:
                        records.append({
                            "indicator": f"gsec_yield_{t.lower()}",
                            "value": float(value),
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_type": "sovereign_yield_curve",
                            "metadata": {"tenor": t},
                        })
                    except (ValueError, TypeError):
                        logger.warning(f"[CCIL] Yield curve {t}: non-numeric value {value!r}")
            return records
        except Exception as e:
            logger.warning(f"[CCIL] Yield curve collection failed: {e}")
            return []

    async def _collect_money_market(self, dtype: str) -> list[dict]:
        """CP and CD rates."""
        prefix = "cp" if dtype == "cp_rates" else "cd"
        try:
            resp = await self._http.get(f"{self.FBIL_URL}/api/{prefix}-rates")
            if resp.status_code != 200:
                logger.warning(f"[CCIL] {prefix.upper()} rates endpoint returned {resp.status_code}")
                return []
            data = resp.json()
            if not isinstance(data, dict):
                logger.warning(f"[CCIL] {prefix.upper()} rates returned non-dict response: {type(data).__name__}")
                return []
            records = []
            tenors = ["1M", "3M", "6M", "12M"]
            for t in tenors:
                value = data.get(t) or data.get(t.lower())
                if value is not None:
                    try:
                        records.append({
                            "indicator": f"{prefix}_rate_{t.lower()}",
                            "value": float(value),
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_type": dtype,
                            "metadata": {"tenor": t},
                        })
                    except (ValueError, TypeError):
                        logger.warning(f"[CCIL] {prefix.upper()} {t}: non-numeric value {value!r}")
            return records
        except Exception as e:
            logger.warning(f"[CCIL] {prefix.upper()} rates collection failed: {e}")
            return []

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
                "unit": "%",
                "metadata": r.get("metadata", {}),
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        required = ["indicator", "date", "value"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            from core.exceptions import SchemaChangedError
            raise SchemaChangedError(self.name, required, list(df.columns))
        return True
