"""PDF content extraction for circulars and regulatory documents.

Downloads PDFs from article URLs, extracts text using pdfplumber,
and updates the article's full_text field.
"""

import io
import logging
import tempfile

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)

PDF_EXTENSIONS = (".pdf",)
PDF_CONTENT_TYPES = ("application/pdf",)
_HEADERS = {
    "User-Agent": "EconScraper/4.0 (pdf-extractor; +https://github.com)",
}


MAX_PDF_BYTES = 50 * 1024 * 1024  # 50 MB hard limit


class PDFExtractor(BaseProcessor):
    name = "pdf_extractor"
    batch_size = 10

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.max_pages = self.config.get("max_pages", 50)
        self.timeout = self.config.get("timeout", 30)
        self.max_bytes = self.config.get("max_bytes", MAX_PDF_BYTES)

    def process_one(self, article: dict) -> dict:
        url = article.get("url", "")
        article_id = article.get("id")

        if not url:
            return {"article_id": article_id, "status": "skipped", "reason": "no_url"}

        is_pdf = any(url.lower().endswith(ext) for ext in PDF_EXTENSIONS)
        if not is_pdf and article.get("full_text", "").strip():
            return {"article_id": article_id, "status": "skipped", "reason": "not_pdf"}

        if not is_pdf:
            is_pdf = self._check_content_type(url)
            if not is_pdf:
                return {"article_id": article_id, "status": "skipped", "reason": "not_pdf"}

        text = self._extract_pdf(url)
        if text and len(text.strip()) > 50:
            return {
                "article_id": article_id,
                "status": "extracted",
                "full_text": text[:100000],
                "char_count": len(text),
            }

        return {"article_id": article_id, "status": "failed"}

    def _check_content_type(self, url: str) -> bool:
        try:
            import httpx

            resp = httpx.head(url, timeout=10, follow_redirects=True, headers=_HEADERS)
            ct = resp.headers.get("content-type", "")
            return any(t in ct for t in PDF_CONTENT_TYPES)
        except Exception:
            return False

    def _extract_pdf(self, url: str) -> str | None:
        try:
            import httpx
            import pdfplumber

            # Check size before downloading to avoid OOM on huge PDFs
            try:
                head = httpx.head(url, timeout=10, follow_redirects=True, headers=_HEADERS)
                content_length = int(head.headers.get("content-length", 0))
                if content_length > self.max_bytes:
                    logger.warning(f"[PDFExtractor] Skipping {url}: {content_length} bytes exceeds {self.max_bytes} limit")
                    return None
            except (httpx.HTTPError, ValueError):
                pass  # HEAD failed or no content-length — proceed cautiously

            resp = httpx.get(url, timeout=self.timeout, follow_redirects=True, headers=_HEADERS)
            if resp.status_code != 200:
                return None

            if len(resp.content) > self.max_bytes:
                logger.warning(f"[PDFExtractor] Skipping {url}: downloaded {len(resp.content)} bytes exceeds limit")
                return None

            pages_text = []
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                for i, page in enumerate(pdf.pages):
                    if i >= self.max_pages:
                        break
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)

            return "\n\n".join(pages_text) if pages_text else None
        except Exception as e:
            logger.debug(f"[PDFExtractor] Failed for {url}: {e}")
            return None

    def _store_results(self, results: list[dict], db):
        from storage.models import Article

        for r in results:
            if r.get("status") == "extracted" and r.get("full_text"):
                article = db.query(Article).filter(Article.id == r["article_id"]).first()
                if article:
                    article.full_text = r["full_text"]
        try:
            db.commit()
        except Exception as e:
            logger.error(f"[PDFExtractor] Failed to store results: {e}")
            db.rollback()
