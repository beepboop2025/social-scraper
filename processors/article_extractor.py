"""Full-text extraction from article URLs.

Uses trafilatura for high-quality article extraction with
BeautifulSoup as fallback. Updates the article's full_text field.
"""

import logging
from datetime import datetime, timezone

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)

MAX_HTML_BYTES = 10 * 1024 * 1024  # 10 MB limit for HTML pages

_HEADERS = {
    "User-Agent": "EconScraper/4.0 (article-extractor; +https://github.com)",
}


class ArticleExtractor(BaseProcessor):
    name = "article_extractor"
    batch_size = 20

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.timeout = self.config.get("timeout", 15)
        self.fallback_to_description = self.config.get("fallback_to_description", True)

    def process_one(self, article: dict) -> dict:
        url = article.get("url", "")
        article_id = article.get("id")

        if not url or article.get("full_text", "").strip():
            return {"article_id": article_id, "status": "skipped", "reason": "no_url_or_has_text"}

        text = self._extract_trafilatura(url)
        if not text:
            text = self._extract_beautifulsoup(url)

        if text:
            return {
                "article_id": article_id,
                "status": "extracted",
                "full_text": text[:50000],
                "char_count": len(text),
            }

        if self.fallback_to_description and article.get("title"):
            return {
                "article_id": article_id,
                "status": "fallback",
                "full_text": article["title"],
            }

        return {"article_id": article_id, "status": "failed"}

    def _extract_trafilatura(self, url: str) -> str | None:
        try:
            import trafilatura

            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                return trafilatura.extract(
                    downloaded,
                    include_comments=False,
                    include_tables=True,
                    favor_recall=True,
                )
        except Exception as e:
            logger.debug(f"[ArticleExtractor] trafilatura failed for {url}: {e}")
        return None

    def _extract_beautifulsoup(self, url: str) -> str | None:
        try:
            import httpx
            from bs4 import BeautifulSoup

            resp = httpx.get(url, timeout=self.timeout, follow_redirects=True, headers=_HEADERS)
            if resp.status_code != 200:
                return None

            if len(resp.content) > MAX_HTML_BYTES:
                logger.warning(f"[ArticleExtractor] Skipping {url}: {len(resp.content)} bytes exceeds HTML limit")
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
                tag.decompose()

            article_tag = soup.find("article") or soup.find("main") or soup.find("body")
            if article_tag:
                paragraphs = article_tag.find_all("p")
                text = "\n\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)
                if len(text) > 100:
                    return text
        except Exception as e:
            logger.debug(f"[ArticleExtractor] BS4 failed for {url}: {e}")
        return None

    def _store_results(self, results: list[dict], db):
        from storage.models import Article

        for r in results:
            if r.get("status") in ("extracted", "fallback") and r.get("full_text"):
                article = db.query(Article).filter(Article.id == r["article_id"]).first()
                if article:
                    article.full_text = r["full_text"]
        try:
            db.commit()
        except Exception as e:
            logger.error(f"[ArticleExtractor] Failed to store results: {e}")
            db.rollback()
