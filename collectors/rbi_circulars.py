"""RBI circulars and notifications collector."""

import hashlib, logging, re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import pandas as pd
from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class RBICirculars(BaseCollector):
    name = "rbi_circulars"
    source_type = "circular"

    def __init__(self, config: dict):
        super().__init__(config)
        self.types = config.get("types", ["press_releases", "notifications"])

    async def collect(self) -> list[dict]:
        records = []
        # Press releases RSS
        try:
            resp = await self._http.get("https://rbi.org.in/scripts/BS_PressReleaseDisplay.aspx?format=rss")
            if resp.status_code == 200:
                from xml.etree import ElementTree as ET
                root = ET.fromstring(resp.text)
                channel = root.find("channel") or root
                for item in channel.findall("item")[:30]:
                    records.append({
                        "title": item.findtext("title", ""),
                        "description": re.sub(r"<[^>]+>", "", item.findtext("description", "")),
                        "url": item.findtext("link", ""),
                        "published_at": item.findtext("pubDate", ""),
                        "type": "press_release",
                    })
        except Exception as e:
            logger.warning(f"[RBI-Circulars] Press releases failed: {e}")

        # Notifications page
        if "notifications" in self.types:
            try:
                resp = await self._http.get("https://rbi.org.in/scripts/NotificationUser.aspx")
                if resp.status_code == 200:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(resp.text, "html.parser")
                    table = soup.find("table", id=lambda x: x and "grdBSDM" in str(x))
                    if table:
                        for row in table.find_all("tr")[1:20]:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                link = cols[1].find("a")
                                records.append({
                                    "title": cols[1].get_text(strip=True),
                                    "url": f"https://rbi.org.in{link['href']}" if link and link.get("href") else "",
                                    "published_at": cols[0].get_text(strip=True),
                                    "type": "notification",
                                })
            except Exception as e:
                logger.warning(f"[RBI-Circulars] Notifications failed: {e}")

        logger.info(f"[RBI-Circulars] Collected {len(records)} items")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            url = r.get("url", "")
            pub = r.get("published_at", "")
            try:
                date = parsedate_to_datetime(pub) if pub else datetime.now(timezone.utc)
            except Exception:
                try:
                    date = datetime.strptime(pub, "%b %d, %Y").replace(tzinfo=timezone.utc)
                except Exception:
                    date = datetime.now(timezone.utc)
            rows.append({
                "title": r.get("title", ""),
                "full_text": r.get("description", r.get("title", "")),
                "url": url,
                "url_hash": hashlib.sha256(url.encode()).hexdigest()[:32] if url else None,
                "author": "RBI",
                "published_at": date,
                "category": f"rbi_{r.get('type', 'circular')}",
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return "title" in df.columns
