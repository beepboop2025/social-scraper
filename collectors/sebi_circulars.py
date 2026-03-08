"""SEBI circulars and orders collector."""

import hashlib, logging, re
from datetime import datetime, timezone
import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class SEBICollector(BaseCollector):
    name = "sebi_circulars"
    source_type = "circular"

    def __init__(self, config: dict):
        super().__init__(config)

    async def collect(self) -> list[dict]:
        records = []
        try:
            resp = await self._http.get("https://www.sebi.gov.in/sebiweb/ajax/getLatestCircular.jsp")
            if resp.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                for item in soup.find_all("li")[:30]:
                    link = item.find("a")
                    if link:
                        records.append({
                            "title": link.get_text(strip=True),
                            "url": f"https://www.sebi.gov.in{link['href']}" if link.get("href", "").startswith("/") else link.get("href", ""),
                            "type": "circular",
                        })
        except Exception as e:
            logger.warning(f"[SEBI] Circulars failed: {e}")

        logger.info(f"[SEBI] Collected {len(records)} circulars")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            url = r.get("url", "")
            rows.append({
                "title": r.get("title", ""),
                "full_text": r.get("title", ""),
                "url": url,
                "url_hash": hashlib.sha256(url.encode()).hexdigest()[:32] if url else None,
                "author": "SEBI",
                "published_at": datetime.now(timezone.utc),
                "category": "sebi_circular",
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return True
