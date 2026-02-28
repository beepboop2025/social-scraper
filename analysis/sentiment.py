"""Sentiment analysis using VADER (rule-based, no GPU needed)."""

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()


def analyze_sentiment(text: str) -> dict:
    """Analyze sentiment of text using VADER.

    Returns:
        dict with keys: label (positive/negative/neutral), compound, confidence,
        and individual scores (pos, neg, neu).
    """
    if not text or not text.strip():
        return {"label": "neutral", "compound": 0.0, "confidence": 0.0, "pos": 0, "neg": 0, "neu": 1}

    scores = _analyzer.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    confidence = abs(compound)

    return {
        "label": label,
        "compound": round(compound, 4),
        "confidence": round(confidence, 4),
        "pos": round(scores["pos"], 4),
        "neg": round(scores["neg"], 4),
        "neu": round(scores["neu"], 4),
    }


def batch_sentiment(texts: list[str]) -> list[dict]:
    """Analyze sentiment for a batch of texts."""
    return [analyze_sentiment(t) for t in texts]
