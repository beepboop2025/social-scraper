"""Named Entity Recognition for economic/financial articles.

Uses spaCy for standard NER (ORG, PERSON, GPE, MONEY, DATE) plus
custom pattern matching for Indian financial entities and policy terms.
"""

import logging
import re

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)

# Custom entity patterns for Indian financial domain
CUSTOM_ORGS = {
    "RBI", "SEBI", "NSE", "BSE", "CCIL", "FIMMDA", "FBIL", "NPCI",
    "IRDAI", "PFRDA", "SIDBI", "NABARD", "NHB", "EXIM Bank",
    "SBI", "HDFC", "ICICI", "Axis Bank", "Kotak", "PNB",
    "IMF", "World Bank", "BIS", "Fed", "ECB", "BOJ", "BOE",
}

POLICY_TERMS = {
    "CRR", "SLR", "LCR", "NSFR", "ALM", "LAF", "MSF",
    "MIBOR", "TREPS", "CBLO", "SOFR", "LIBOR", "T-Bill",
    "G-Sec", "SDL", "CP", "CD", "NCD", "FRA", "IRS", "OIS",
    "QE", "QT", "OMO", "VRR", "VRRR", "CPI", "WPI", "IIP",
    "GDP", "GVA", "PMI", "NPA", "GNPA", "NNPA", "PCR",
}

TICKER_PATTERN = re.compile(r'\$([A-Z]{1,5})\b')
MONEY_PATTERN = re.compile(
    r'(?:Rs\.?|INR|USD|\$|₹)\s*[\d,]+(?:\.\d+)?\s*(?:cr(?:ore)?|lakh|billion|million|trillion|bn|mn)?',
    re.IGNORECASE,
)
PERCENTAGE_PATTERN = re.compile(r'[\d.]+\s*(?:%|percent|basis points|bps)', re.IGNORECASE)


class EntityExtractor(BaseProcessor):
    name = "entity_extractor"
    batch_size = 50

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.spacy_model = self.config.get("spacy_model", "en_core_web_sm")
        self._nlp = None

    def _get_nlp(self):
        if self._nlp is None:
            try:
                import spacy
                self._nlp = spacy.load(self.spacy_model)
                logger.info(f"[EntityExtractor] Loaded {self.spacy_model}")
            except Exception as e:
                logger.warning(f"[EntityExtractor] spaCy unavailable ({e}), using regex only")
                self._nlp = "regex"
        return self._nlp

    def process_one(self, article: dict) -> dict:
        text = article.get("full_text", "") or article.get("title", "")
        article_id = article.get("id")

        if not text or len(text.strip()) < 10:
            return {"article_id": article_id, "status": "skipped", "entities": []}

        entities = []

        # spaCy NER
        nlp = self._get_nlp()
        if nlp != "regex":
            try:
                doc = nlp(text[:10000])
                for ent in doc.ents:
                    if ent.label_ in ("ORG", "PERSON", "GPE", "MONEY", "DATE", "NORP"):
                        entities.append({
                            "type": ent.label_,
                            "value": ent.text.strip(),
                            "confidence": 0.8,
                        })
            except Exception as e:
                logger.debug(f"[EntityExtractor] spaCy failed: {e}")

        # Custom pattern matching
        entities.extend(self._extract_custom_entities(text))

        # Deduplicate
        seen = set()
        unique = []
        for e in entities:
            key = (e["type"], e["value"].lower())
            if key not in seen:
                seen.add(key)
                unique.append(e)

        return {
            "article_id": article_id,
            "status": "extracted",
            "entities": unique,
            "count": len(unique),
        }

    def _extract_custom_entities(self, text: str) -> list[dict]:
        entities = []

        # Financial organizations (word-boundary match to avoid "Fed" in "Federal", etc.)
        for org in CUSTOM_ORGS:
            if re.search(rf'\b{re.escape(org)}\b', text):
                entities.append({"type": "FIN_ORG", "value": org, "confidence": 1.0})

        # Policy terms
        for term in POLICY_TERMS:
            if re.search(rf'\b{re.escape(term)}\b', text):
                entities.append({"type": "POLICY", "value": term, "confidence": 1.0})

        # Stock tickers
        for match in TICKER_PATTERN.finditer(text):
            entities.append({"type": "TICKER", "value": match.group(1), "confidence": 0.9})

        # Money amounts
        for match in MONEY_PATTERN.finditer(text):
            entities.append({"type": "MONEY", "value": match.group().strip(), "confidence": 0.9})

        # Percentages / basis points
        for match in PERCENTAGE_PATTERN.finditer(text):
            entities.append({"type": "RATE", "value": match.group().strip(), "confidence": 0.9})

        return entities

    def _store_results(self, results: list[dict], db):
        from storage.models import Entity

        for r in results:
            if r.get("status") == "extracted":
                for ent in r.get("entities", []):
                    db.add(Entity(
                        article_id=r["article_id"],
                        entity_type=ent["type"],
                        entity_value=ent["value"],
                        confidence=ent.get("confidence", 1.0),
                    ))
        try:
            db.commit()
        except Exception as e:
            logger.error(f"[EntityExtractor] Failed to store results: {e}")
            db.rollback()
