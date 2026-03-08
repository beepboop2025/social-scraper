"""Financial analysis API routes — ticker extraction, financial sentiment, threat intel."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/financial", tags=["Financial Analysis"])


class TextInput(BaseModel):
    text: str
    include_threat: bool = False


class BatchInput(BaseModel):
    texts: list[str]


@router.post("/analyze")
async def analyze_financial(input: TextInput):
    """Full financial analysis of text — tickers, sentiment, prices, treasury relevance."""
    from analysis.financial_nlp import analyze_financial_content
    result = analyze_financial_content(input.text)

    if input.include_threat:
        from analysis.threat_intel import analyze_threat
        result["threat_analysis"] = analyze_threat(input.text)

    return result


@router.post("/sentiment")
async def financial_sentiment(input: TextInput):
    """Financial-specific sentiment (bullish/bearish, not just positive/negative)."""
    from analysis.financial_nlp import analyze_financial_sentiment
    return analyze_financial_sentiment(input.text)


@router.post("/tickers")
async def extract_tickers(input: TextInput):
    """Extract ticker symbols from text."""
    from analysis.financial_nlp import extract_tickers
    tickers = extract_tickers(input.text)
    return {"tickers": tickers, "count": len(tickers)}


@router.post("/threat")
async def threat_analysis(input: TextInput):
    """Analyze text for threat intelligence."""
    from analysis.threat_intel import analyze_threat
    return analyze_threat(input.text)


@router.post("/batch-analyze")
async def batch_financial_analysis(input: BatchInput):
    """Analyze a batch of texts for financial content."""
    from analysis.financial_nlp import batch_financial_analysis
    results = batch_financial_analysis(input.texts)
    return {"results": results, "count": len(results)}


@router.get("/treasury-feed")
async def treasury_feed(limit: int = 50):
    """Get the latest treasury-relevant scraped content for LiquiFi."""
    try:
        import json
        import os
        import redis.asyncio as aioredis

        r = aioredis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379"),
            decode_responses=True,
        )

        # Get LiquiFi feed data
        data = await r.get("liquifi:treasury_news")
        await r.close()

        if data:
            return json.loads(data)
        return {"news": [], "rate_signals": [], "message": "No treasury data available yet"}
    except Exception as e:
        return {"error": str(e)}


@router.get("/threat-feed")
async def threat_feed(limit: int = 50):
    """Get the latest threat intelligence feed."""
    try:
        from api.database import SessionLocal
        from api.models import ScrapedPost
        from sqlalchemy import desc

        db = SessionLocal()
        try:
            posts = (
                db.query(ScrapedPost)
                .filter(ScrapedPost.platform == "darkweb")
                .order_by(desc(ScrapedPost.scraped_at))
                .limit(limit)
                .all()
            )

            items = []
            for post in posts:
                from analysis.threat_intel import classify_threat
                classification = classify_threat(post.text or "")
                items.append({
                    "id": post.id,
                    "text": post.text[:500],
                    "source": post.source_channel,
                    "url": post.source_url,
                    "scraped_at": post.scraped_at.isoformat() if post.scraped_at else None,
                    "threat": classification,
                })

            return {"threats": items, "count": len(items)}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}
