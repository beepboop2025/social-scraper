"""Topic classification for economic/financial articles.

Uses keyword-based classification with TF-IDF-like scoring
across 13 economic topics. Each article can have multiple topics.
"""

import logging
import re
from collections import defaultdict

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)

# Topic keyword mappings with weights
TOPIC_KEYWORDS: dict[str, list[tuple[str, float]]] = {
    "monetary_policy": [
        ("repo rate", 3.0), ("reverse repo", 3.0), ("monetary policy", 3.0),
        ("mpc", 2.5), ("interest rate", 2.0), ("policy rate", 2.5),
        ("laf", 2.0), ("msf", 2.0), ("bank rate", 2.0), ("crr", 2.0),
        ("slr", 2.0), ("rbi", 1.5), ("liquidity", 1.5), ("accommodation", 2.0),
    ],
    "fiscal_policy": [
        ("budget", 2.5), ("fiscal deficit", 3.0), ("government spending", 2.5),
        ("tax", 2.0), ("gst", 2.5), ("subsidy", 2.0), ("disinvestment", 2.5),
        ("public debt", 2.5), ("revenue", 1.5), ("expenditure", 1.5),
        ("fiscal policy", 3.0), ("finance ministry", 2.0),
    ],
    "inflation": [
        ("inflation", 3.0), ("cpi", 2.5), ("wpi", 2.5), ("price", 1.5),
        ("consumer price", 2.5), ("wholesale price", 2.5), ("deflation", 2.5),
        ("food inflation", 3.0), ("core inflation", 3.0), ("price stability", 2.5),
    ],
    "employment": [
        ("employment", 2.5), ("unemployment", 2.5), ("jobs", 2.0),
        ("labor", 2.0), ("labour", 2.0), ("workforce", 2.0), ("payroll", 2.0),
        ("hiring", 2.0), ("layoff", 2.0), ("nfp", 2.5),
    ],
    "gdp_growth": [
        ("gdp", 3.0), ("growth rate", 2.5), ("economic growth", 3.0),
        ("gva", 2.5), ("recession", 2.5), ("expansion", 2.0), ("slowdown", 2.0),
        ("pmi", 2.0), ("iip", 2.5), ("industrial production", 2.5),
    ],
    "trade_balance": [
        ("trade", 2.0), ("export", 2.0), ("import", 2.0), ("trade deficit", 3.0),
        ("current account", 2.5), ("bop", 2.5), ("balance of payments", 3.0),
        ("tariff", 2.0), ("trade surplus", 3.0), ("trade war", 2.5),
    ],
    "banking_sector": [
        ("bank", 1.5), ("npa", 2.5), ("credit growth", 2.5), ("deposit", 1.5),
        ("lending", 2.0), ("nbfc", 2.5), ("asset quality", 2.5),
        ("provisioning", 2.0), ("capital adequacy", 2.5), ("priority sector", 2.5),
    ],
    "capital_markets": [
        ("nifty", 2.5), ("sensex", 2.5), ("ipo", 2.5), ("fii", 2.5),
        ("dii", 2.5), ("market cap", 2.0), ("equity", 1.5), ("sebi", 2.0),
        ("stock market", 2.5), ("mutual fund", 2.0), ("derivative", 2.0),
    ],
    "cryptocurrency": [
        ("bitcoin", 3.0), ("crypto", 2.5), ("blockchain", 2.5), ("ethereum", 3.0),
        ("cbdc", 3.0), ("digital currency", 3.0), ("defi", 2.5), ("token", 1.5),
        ("digital rupee", 3.0), ("virtual digital asset", 3.0),
    ],
    "real_estate": [
        ("real estate", 3.0), ("housing", 2.5), ("property", 2.0), ("rera", 3.0),
        ("construction", 2.0), ("mortgage", 2.5), ("home loan", 2.5),
        ("affordable housing", 3.0), ("rent", 1.5),
    ],
    "commodities": [
        ("crude oil", 3.0), ("gold", 2.0), ("silver", 2.0), ("copper", 2.0),
        ("commodity", 2.5), ("opec", 2.5), ("natural gas", 2.5),
        ("metal", 1.5), ("agricultural", 1.5), ("mcx", 2.5),
    ],
    "regulatory": [
        ("regulation", 2.0), ("circular", 2.0), ("compliance", 2.0),
        ("sebi", 2.0), ("rbi", 1.5), ("notification", 1.5), ("guidelines", 2.0),
        ("mandate", 2.0), ("directive", 2.0), ("amendment", 2.0),
    ],
    "geopolitical": [
        ("geopolitical", 3.0), ("sanctions", 2.5), ("war", 2.0), ("conflict", 2.0),
        ("diplomatic", 2.0), ("bilateral", 2.0), ("g20", 2.5), ("brics", 2.5),
        ("trade agreement", 2.5), ("foreign policy", 2.5),
    ],
}

MIN_SCORE_THRESHOLD = 4.0  # Minimum weighted score to assign a topic


class TopicClassifier(BaseProcessor):
    name = "topic_classifier"
    batch_size = 50

    def process_one(self, article: dict) -> dict:
        text = article.get("full_text", "") or article.get("title", "")
        article_id = article.get("id")

        if not text or len(text.strip()) < 10:
            return {"article_id": article_id, "status": "skipped", "topics": []}

        text_lower = text.lower()
        topic_scores: dict[str, float] = {}

        for topic, keywords in TOPIC_KEYWORDS.items():
            score = 0.0
            for keyword, weight in keywords:
                count = text_lower.count(keyword)
                if count > 0:
                    score += weight * min(count, 3)  # Cap per-keyword contribution
            if score >= MIN_SCORE_THRESHOLD:
                topic_scores[topic] = round(score, 2)

        # Normalize to [0, 1] confidence
        if topic_scores:
            max_score = max(topic_scores.values())
            topics = [
                {"topic": t, "confidence": round(s / max_score, 3)}
                for t, s in sorted(topic_scores.items(), key=lambda x: -x[1])
            ]
        else:
            topics = [{"topic": "general", "confidence": 0.5}]

        return {
            "article_id": article_id,
            "status": "classified",
            "topics": topics,
        }

    def _store_results(self, results: list[dict], db):
        from storage.models import ArticleTopic

        for r in results:
            if r.get("status") == "classified":
                for t in r.get("topics", []):
                    db.add(ArticleTopic(
                        article_id=r["article_id"],
                        topic=t["topic"],
                        confidence=t.get("confidence", 1.0),
                    ))
        try:
            db.commit()
        except Exception as e:
            logger.error(f"[TopicClassifier] Failed to store results: {e}")
            db.rollback()
