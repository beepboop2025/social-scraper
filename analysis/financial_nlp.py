"""Financial NLP — specialized analysis for market-relevant content.

Extracts:
- Ticker symbols from text ($AAPL, TSLA, BTC, etc.)
- Price mentions and targets
- Earnings-related content
- Sentiment with financial context (bearish/bullish vs positive/negative)
- Regulatory keywords and impact assessment
"""

import re
import logging
from typing import Optional

from analysis.sentiment import analyze_sentiment

logger = logging.getLogger(__name__)

# Common ticker patterns
TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")
CRYPTO_PATTERN = re.compile(r"\b(BTC|ETH|SOL|ADA|DOT|AVAX|MATIC|LINK|UNI|AAVE)\b", re.I)

# Price mention patterns
PRICE_PATTERNS = [
    re.compile(r"\$(\d{1,7}(?:,\d{3})*(?:\.\d{1,2})?)\b"),  # $123.45
    re.compile(r"₹(\d{1,7}(?:,\d{3})*(?:\.\d{1,2})?)\b"),    # ₹123.45
    re.compile(r"(\d+\.?\d*)\s*%\s*(increase|decrease|up|down|gain|loss|drop|rise)", re.I),
]

# Bullish/bearish keywords (financial sentiment)
BULLISH_KEYWORDS = [
    "bullish", "buy", "long", "moon", "pump", "rally",
    "breakout", "upside", "accumulate", "outperform", "upgrade",
    "beat expectations", "strong earnings", "revenue growth",
    "rate cut", "dovish", "stimulus", "easing",
]

BEARISH_KEYWORDS = [
    "bearish", "sell", "short", "dump", "crash", "correction",
    "breakdown", "downside", "distribute", "underperform", "downgrade",
    "miss expectations", "revenue decline", "profit warning",
    "rate hike", "hawkish", "tightening", "recession",
]

# Earnings-related
EARNINGS_KEYWORDS = [
    "earnings", "eps", "revenue", "profit", "loss",
    "quarterly results", "annual report", "guidance",
    "beat", "miss", "estimate", "consensus",
    "margin", "ebitda", "cash flow",
]

# Indian treasury specific
TREASURY_INDIA_KEYWORDS = [
    "repo rate", "reverse repo", "mibor", "cblo", "call money",
    "crr", "slr", "lcr", "nsfr", "laf", "msf",
    "g-sec", "treasury bill", "t-bill", "sdl",
    "rbi policy", "monetary policy committee", "mpc",
    "rupee", "usd/inr", "forex reserve",
]


def extract_tickers(text: str) -> list[str]:
    """Extract stock/crypto ticker symbols from text."""
    tickers = set()

    # $TICKER format
    for match in TICKER_PATTERN.findall(text):
        tickers.add(match.upper())

    # Crypto symbols
    for match in CRYPTO_PATTERN.findall(text):
        tickers.add(match.upper())

    return sorted(tickers)


def extract_price_mentions(text: str) -> list[dict]:
    """Extract price mentions and percentage changes."""
    mentions = []

    for pattern in PRICE_PATTERNS:
        for match in pattern.finditer(text):
            context_start = max(0, match.start() - 50)
            context_end = min(len(text), match.end() + 50)
            mentions.append({
                "value": match.group(0),
                "context": text[context_start:context_end].strip(),
                "position": match.start(),
            })

    return mentions[:10]  # Cap


def analyze_financial_sentiment(text: str) -> dict:
    """Analyze sentiment with financial context.

    Distinguishes between general positive/negative and financial bullish/bearish.
    """
    base_sentiment = analyze_sentiment(text)
    text_lower = text.lower()

    bullish_score = sum(1 for kw in BULLISH_KEYWORDS if kw in text_lower)
    bearish_score = sum(1 for kw in BEARISH_KEYWORDS if kw in text_lower)

    if bullish_score > bearish_score:
        financial_label = "bullish"
        financial_score = min(bullish_score / 5, 1.0)
    elif bearish_score > bullish_score:
        financial_label = "bearish"
        financial_score = -min(bearish_score / 5, 1.0)
    else:
        financial_label = "neutral"
        financial_score = 0.0

    return {
        **base_sentiment,
        "financial_label": financial_label,
        "financial_score": round(financial_score, 4),
        "bullish_signals": bullish_score,
        "bearish_signals": bearish_score,
    }


def is_earnings_related(text: str) -> bool:
    """Check if text is related to earnings/financial results."""
    text_lower = text.lower()
    return sum(1 for kw in EARNINGS_KEYWORDS if kw in text_lower) >= 2


def is_treasury_relevant(text: str) -> tuple[bool, list[str]]:
    """Check if text is relevant to Indian treasury operations."""
    text_lower = text.lower()
    matches = [kw for kw in TREASURY_INDIA_KEYWORDS if kw in text_lower]
    return len(matches) >= 1, matches


def analyze_financial_content(text: str) -> dict:
    """Full financial analysis of text content.

    Returns comprehensive analysis including tickers, prices,
    sentiment, earnings relevance, and treasury relevance.
    """
    tickers = extract_tickers(text)
    prices = extract_price_mentions(text)
    sentiment = analyze_financial_sentiment(text)
    earnings = is_earnings_related(text)
    treasury_relevant, treasury_matches = is_treasury_relevant(text)

    return {
        "tickers": tickers,
        "price_mentions": prices,
        "sentiment": sentiment,
        "earnings_related": earnings,
        "treasury_relevant": treasury_relevant,
        "treasury_keywords": treasury_matches,
        "has_financial_content": bool(tickers or prices or earnings or treasury_relevant),
    }


def batch_financial_analysis(texts: list[str]) -> list[dict]:
    """Analyze a batch of texts for financial content."""
    return [analyze_financial_content(t) for t in texts]
