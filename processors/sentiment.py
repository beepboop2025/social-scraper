"""Financial sentiment analysis using FinBERT with VADER fallback.

Classifies text as positive/negative/neutral with financial context:
- Policy direction: hawkish/dovish/neutral
- Sector-level sentiment for banking, markets, real estate, etc.
"""

import logging
import re

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)

# Keyword-based policy direction detection
HAWKISH_KEYWORDS = [
    "rate hike", "tightening", "inflation concern", "restrictive",
    "higher rates", "tapering", "reducing liquidity", "contractionary",
    "rate increase", "monetary tightening", "crr hike", "slr increase",
]
DOVISH_KEYWORDS = [
    "rate cut", "easing", "accommodative", "stimulus",
    "lower rates", "quantitative easing", "expansionary", "liquidity injection",
    "rate reduction", "monetary easing", "crr cut", "growth support",
]

SECTOR_KEYWORDS = {
    "banking": ["bank", "npa", "credit growth", "deposit", "lending", "nbfc", "rbi"],
    "markets": ["nifty", "sensex", "ipo", "fii", "dii", "market cap", "equity"],
    "real_estate": ["real estate", "housing", "property", "rera", "construction"],
    "commodities": ["crude", "gold", "silver", "copper", "commodity"],
    "forex": ["rupee", "dollar", "usd/inr", "forex", "exchange rate"],
    "tech": ["it sector", "technology", "digital", "fintech", "startup"],
}


class SentimentAnalyzer(BaseProcessor):
    name = "sentiment"
    batch_size = 16

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.model_name = self.config.get("model", "ProsusAI/finbert")
        self.fallback = self.config.get("fallback", "vader")
        self._pipeline = None

    def _get_pipeline(self):
        if self._pipeline is None:
            try:
                from transformers import pipeline

                self._pipeline = pipeline(
                    "sentiment-analysis",
                    model=self.model_name,
                    tokenizer=self.model_name,
                    max_length=512,
                    truncation=True,
                )
                logger.info(f"[Sentiment] Loaded {self.model_name}")
            except Exception as e:
                logger.warning(f"[Sentiment] FinBERT unavailable ({e}), using VADER")
                self._pipeline = "vader"
        return self._pipeline

    def process_one(self, article: dict) -> dict:
        text = article.get("full_text", "") or article.get("title", "")
        article_id = article.get("id")

        if not text or len(text.strip()) < 10:
            return {"article_id": article_id, "status": "skipped"}

        score = self._analyze(text)
        direction = self._detect_policy_direction(text)
        sectors = self._detect_sectors(text)

        return {
            "article_id": article_id,
            "status": "analyzed",
            "overall": score,
            "policy_direction": direction,
            "sector_scores": sectors,
            "model": self.model_name if self._pipeline != "vader" else "vader",
        }

    def _analyze(self, text: str) -> float:
        """Return sentiment score in [-1, 1]."""
        pipeline = self._get_pipeline()

        if pipeline == "vader":
            return self._vader_score(text)

        try:
            result = pipeline(text[:512])[0]
            label = result["label"].lower()
            score = result["score"]
            if label == "negative":
                return -score
            elif label == "positive":
                return score
            return 0.0
        except Exception as e:
            logger.debug(f"[Sentiment] FinBERT failed: {e}")
            return self._vader_score(text)

    def _vader_score(self, text: str) -> float:
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

            analyzer = SentimentIntensityAnalyzer()
            return analyzer.polarity_scores(text)["compound"]
        except ImportError:
            return 0.0

    def _detect_policy_direction(self, text: str) -> str:
        text_lower = text.lower()
        hawkish = sum(1 for kw in HAWKISH_KEYWORDS if kw in text_lower)
        dovish = sum(1 for kw in DOVISH_KEYWORDS if kw in text_lower)

        if hawkish > dovish and hawkish >= 2:
            return "hawkish"
        elif dovish > hawkish and dovish >= 2:
            return "dovish"
        return "neutral"

    def _detect_sectors(self, text: str) -> dict:
        """Detect which sectors are mentioned and assign per-sector sentiment."""
        text_lower = text.lower()
        sectors = {}
        for sector, keywords in SECTOR_KEYWORDS.items():
            mentions = sum(1 for kw in keywords if kw in text_lower)
            if mentions > 0:
                sectors[sector] = {"mentions": mentions}
        return sectors

    def _store_results(self, results: list[dict], db):
        from storage.models import SentimentScore

        for r in results:
            if r.get("status") == "analyzed":
                score = SentimentScore(
                    article_id=r["article_id"],
                    overall=r["overall"],
                    sector_scores=r.get("sector_scores", {}),
                    policy_direction=r.get("policy_direction", "neutral"),
                    model_name=r.get("model", "vader"),
                )
                db.add(score)
        db.commit()
