"""Data transforms applied in the pipeline between raw and enriched stages."""

import re
from datetime import datetime


def clean_text(text: str) -> str:
    """Remove excessive whitespace, URLs for analysis, normalize unicode."""
    if not text:
        return ""
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_hashtags(text: str) -> list[str]:
    """Extract hashtags from text."""
    return re.findall(r"#(\w+)", text) if text else []


def extract_mentions(text: str) -> list[str]:
    """Extract @mentions from text."""
    return re.findall(r"@(\w+)", text) if text else []


def compute_engagement_score(likes: int, reposts: int, replies: int, views: int | None) -> float:
    """Compute a normalized engagement score."""
    base = likes + reposts * 2 + replies * 3
    if views and views > 0:
        return round(base / views * 1000, 2)  # Engagement rate per 1000 views
    return float(base)


def enrich_item(raw_item: dict) -> dict:
    """Apply all transformations to enrich a raw scraped item."""
    item = raw_item.copy()
    text = item.get("text", "")

    item["clean_text"] = clean_text(text)
    item["hashtags"] = extract_hashtags(text)
    item["mentions"] = extract_mentions(text)
    item["word_count"] = len(text.split()) if text else 0
    item["engagement_score"] = compute_engagement_score(
        item.get("likes", 0),
        item.get("reposts", 0),
        item.get("replies", 0),
        item.get("views"),
    )
    item["enriched_at"] = datetime.utcnow().isoformat()

    return item
