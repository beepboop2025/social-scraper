"""Multi-language detection and routing for NLP processing.

- Detects article language using langdetect
- Routes to appropriate sentiment model:
  - English: FinBERT (existing)
  - Hindi/regional: XLM-RoBERTa multilingual
  - Other: VADER fallback
- Tracks language distribution in metrics
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Cache language detection results
_langdetect_available = None
_langdetect_seeded = False


def _check_langdetect():
    """Check if langdetect is available. Seeds DetectorFactory once."""
    global _langdetect_available, _langdetect_seeded
    if _langdetect_available is None:
        try:
            import langdetect
            _langdetect_available = True
        except ImportError:
            _langdetect_available = False
            logger.warning("[LangDetect] langdetect not installed, defaulting to 'en'")
    if _langdetect_available and not _langdetect_seeded:
        from langdetect import DetectorFactory
        DetectorFactory.seed = 42
        _langdetect_seeded = True
    return _langdetect_available


def detect_language(text: str) -> str:
    """Detect the language of text. Returns ISO 639-1 code (e.g., 'en', 'hi').

    Falls back to 'en' if detection fails or langdetect is not installed.
    """
    if not text or len(text.strip()) < 20:
        return "en"

    if not _check_langdetect():
        return "en"

    try:
        from langdetect import detect
        lang = detect(text[:1000])
        return lang
    except Exception:
        return "en"


def detect_language_with_confidence(text: str) -> dict:
    """Detect language with confidence scores."""
    if not text or len(text.strip()) < 20:
        return {"language": "en", "confidence": 1.0, "alternatives": []}

    if not _check_langdetect():
        return {"language": "en", "confidence": 1.0, "alternatives": []}

    try:
        from langdetect import detect_langs
        results = detect_langs(text[:1000])
        if not results:
            return {"language": "en", "confidence": 1.0, "alternatives": []}

        primary = results[0]
        alternatives = [
            {"language": str(r.lang), "confidence": round(r.prob, 3)}
            for r in results[1:4]
        ]
        return {
            "language": str(primary.lang),
            "confidence": round(primary.prob, 3),
            "alternatives": alternatives,
        }
    except Exception:
        return {"language": "en", "confidence": 1.0, "alternatives": []}


# Languages supported by multilingual sentiment models
MULTILINGUAL_LANGUAGES = {
    "hi", "bn", "te", "ta", "mr", "gu", "kn", "ml", "pa",  # Indian languages
    "zh", "ja", "ko",  # East Asian
    "ar", "fa", "ur",  # Arabic script
    "de", "fr", "es", "pt", "it", "ru", "nl", "pl", "tr",  # European
}

ENGLISH_LANGUAGES = {"en"}


def get_sentiment_model_for_language(lang: str) -> str:
    """Determine which sentiment model to use for a given language.

    Returns:
        "finbert" for English financial text
        "xlm-roberta" for multilingual text
        "vader" as fallback for unsupported languages
    """
    if lang in ENGLISH_LANGUAGES:
        return "finbert"
    elif lang in MULTILINGUAL_LANGUAGES:
        return "xlm-roberta"
    else:
        return "vader"


class LanguageStats:
    """Track language distribution across processed articles."""

    def __init__(self):
        self._counts: dict[str, int] = {}
        self._total = 0

    def record(self, language: str):
        self._counts[language] = self._counts.get(language, 0) + 1
        self._total += 1

    @property
    def distribution(self) -> dict:
        if self._total == 0:
            return {}
        return {
            lang: {
                "count": count,
                "percentage": round(count / self._total * 100, 1),
            }
            for lang, count in sorted(self._counts.items(), key=lambda x: -x[1])
        }

    @property
    def total(self) -> int:
        return self._total

    def to_dict(self) -> dict:
        return {
            "total_processed": self._total,
            "languages": self.distribution,
            "unique_languages": len(self._counts),
        }


# Module-level stats tracker
_language_stats = LanguageStats()


def get_language_stats() -> LanguageStats:
    return _language_stats
