"""Multi-feed RSS collector — fully config-driven.

Add new feeds in sources.yaml → no code changes needed.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

import pandas as pd

from core.base_collector import BaseCollector

logger = logging.getLogger(__name__)

ATOM_NS = "{http://www.w3.org/2005/Atom}"


class RSSCollector(BaseCollector):
    name = "rss_feeds"
    source_type = "rss"

    def __init__(self, config: dict):
        super().__init__(config)
        self.feeds = config.get("feeds", [])

    def _strip_html(self, text: str) -> str:
        return re.sub(r"<[^>]+>", "", text or "").strip()

    def _parse_date(self, date_str: str) -> datetime:
        if not date_str:
            return datetime.now(timezone.utc)
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)

    async def collect(self) -> list[dict]:
        records = []
        for feed_cfg in self.feeds:
            url = feed_cfg["url"]
            name = feed_cfg.get("name", url)
            category = feed_cfg.get("category", "general")

            try:
                resp = await self._http.get(url)
                if resp.status_code != 200:
                    logger.warning(f"[RSS] {name} returned {resp.status_code}")
                    continue

                root = ET.fromstring(resp.text)

                # RSS 2.0
                if root.tag == "rss" or root.find("channel") is not None:
                    channel = root.find("channel") or root
                    for item in channel.findall("item")[:20]:
                        records.append({
                            "feed_name": name,
                            "category": category,
                            "title": item.findtext("title", ""),
                            "description": self._strip_html(item.findtext("description", "")),
                            "url": item.findtext("link", ""),
                            "author": item.findtext("author") or item.findtext("{http://purl.org/dc/elements/1.1/}creator", ""),
                            "published_at": item.findtext("pubDate", ""),
                            "guid": item.findtext("guid", ""),
                        })
                # Atom
                elif root.tag in (f"{ATOM_NS}feed", "feed"):
                    for entry in root.findall(f"{ATOM_NS}entry")[:20]:
                        link_el = entry.find(f"{ATOM_NS}link[@rel='alternate']") or entry.find(f"{ATOM_NS}link")
                        link = link_el.get("href", "") if link_el is not None else ""
                        records.append({
                            "feed_name": name,
                            "category": category,
                            "title": entry.findtext(f"{ATOM_NS}title", ""),
                            "description": self._strip_html(
                                entry.findtext(f"{ATOM_NS}summary", "") or
                                entry.findtext(f"{ATOM_NS}content", "")
                            ),
                            "url": link,
                            "author": (entry.find(f"{ATOM_NS}author") or ET.Element("x")).findtext(f"{ATOM_NS}name", ""),
                            "published_at": entry.findtext(f"{ATOM_NS}published") or entry.findtext(f"{ATOM_NS}updated", ""),
                            "guid": entry.findtext(f"{ATOM_NS}id", link),
                        })
            except Exception as e:
                logger.warning(f"[RSS] Failed to fetch {name}: {e}")

        logger.info(f"[RSS] Collected {len(records)} articles from {len(self.feeds)} feeds")
        return records

    async def parse(self, raw_data: list[dict]) -> pd.DataFrame:
        rows = []
        for r in raw_data:
            url = r.get("url", "")
            rows.append({
                "title": r.get("title", ""),
                "full_text": r.get("description", ""),
                "url": url,
                "url_hash": hashlib.sha256(url.encode()).hexdigest()[:32] if url else None,
                "author": r.get("author", ""),
                "published_at": self._parse_date(r.get("published_at", "")),
                "category": r.get("category", "general"),
                "source_feed": r.get("feed_name", ""),
            })
        return pd.DataFrame(rows)

    def validate(self, df: pd.DataFrame) -> bool:
        return "title" in df.columns and "url" in df.columns
