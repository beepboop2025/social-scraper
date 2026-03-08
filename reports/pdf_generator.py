"""
Daily World Intelligence Report — PDF Generator
================================================

Queries the social_scraper database for the last 24 hours of collected data
and generates a visually rich, multi-page PDF report with:
- Executive summary (LLM-generated)
- Market data tables and charts
- Sentiment analysis with pie/bar charts
- Social media pulse highlights
- Top headlines

Uses ReportLab for PDF composition and Matplotlib for charts.
"""

import io
import logging
import os
import tempfile
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch, mm
from reportlab.platypus import (
    BaseDocTemplate, Frame, Image, NextPageTemplate, PageBreak, PageTemplate,
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)
from reportlab.graphics.shapes import Drawing, Rect, Circle, String, Line
from reportlab.graphics import renderPDF

logger = logging.getLogger(__name__)

# ─── Color Palette ───────────────────────────────────────────────
BLUE_PRIMARY = colors.HexColor("#1565C0")
BLUE_DARK = colors.HexColor("#0D47A1")
BLUE_LIGHT = colors.HexColor("#E3F2FD")
AMBER = colors.HexColor("#FF6F00")
TEAL = colors.HexColor("#00897B")
GREEN = colors.HexColor("#2E7D32")
RED = colors.HexColor("#C62828")
GRAY_BG = colors.HexColor("#F5F5F5")
GRAY_TEXT = colors.HexColor("#616161")
DARK_TEXT = colors.HexColor("#212121")
WHITE = colors.white

# Chart colors
CHART_COLORS = ["#1565C0", "#FF6F00", "#00897B", "#C62828", "#6A1B9A",
                "#EF6C00", "#00838F", "#AD1457", "#558B2F", "#4527A0"]

PAGE_W, PAGE_H = A4
MARGIN = 45


# ─── Styles ──────────────────────────────────────────────────────
def _build_styles():
    ss = getSampleStyleSheet()
    styles = {
        "cover_title": ParagraphStyle(
            "cover_title", parent=ss["Title"],
            fontSize=32, leading=38, textColor=BLUE_DARK,
            alignment=TA_CENTER, spaceAfter=6,
        ),
        "cover_subtitle": ParagraphStyle(
            "cover_subtitle", parent=ss["Normal"],
            fontSize=14, leading=18, textColor=GRAY_TEXT,
            alignment=TA_CENTER, spaceAfter=20,
        ),
        "section_title": ParagraphStyle(
            "section_title", parent=ss["Heading1"],
            fontSize=18, leading=22, textColor=BLUE_PRIMARY,
            spaceBefore=16, spaceAfter=8,
            borderColor=BLUE_PRIMARY, borderWidth=0,
        ),
        "subsection": ParagraphStyle(
            "subsection", parent=ss["Heading2"],
            fontSize=13, leading=16, textColor=BLUE_DARK,
            spaceBefore=10, spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body", parent=ss["Normal"],
            fontSize=10, leading=14, textColor=DARK_TEXT,
            alignment=TA_JUSTIFY, spaceAfter=6,
        ),
        "body_small": ParagraphStyle(
            "body_small", parent=ss["Normal"],
            fontSize=8.5, leading=12, textColor=GRAY_TEXT,
            spaceAfter=4,
        ),
        "stat_number": ParagraphStyle(
            "stat_number", parent=ss["Normal"],
            fontSize=22, leading=26, textColor=BLUE_PRIMARY,
            alignment=TA_CENTER,
        ),
        "stat_label": ParagraphStyle(
            "stat_label", parent=ss["Normal"],
            fontSize=9, leading=12, textColor=GRAY_TEXT,
            alignment=TA_CENTER,
        ),
        "headline": ParagraphStyle(
            "headline", parent=ss["Normal"],
            fontSize=10, leading=13, textColor=DARK_TEXT,
            spaceBefore=2, spaceAfter=2,
            leftIndent=15,
        ),
        "table_header": ParagraphStyle(
            "table_header", parent=ss["Normal"],
            fontSize=9, leading=11, textColor=WHITE,
            alignment=TA_CENTER,
        ),
        "table_cell": ParagraphStyle(
            "table_cell", parent=ss["Normal"],
            fontSize=9, leading=11, textColor=DARK_TEXT,
        ),
        "footer": ParagraphStyle(
            "footer", parent=ss["Normal"],
            fontSize=7, leading=9, textColor=GRAY_TEXT,
            alignment=TA_CENTER,
        ),
        "quote_box": ParagraphStyle(
            "quote_box", parent=ss["Normal"],
            fontSize=10, leading=14, textColor=BLUE_DARK,
            alignment=TA_LEFT, leftIndent=10, rightIndent=10,
        ),
    }
    return styles


# ─── Chart Generation ────────────────────────────────────────────

def _chart_to_image(fig, width=5, height=3, dpi=150):
    """Render a matplotlib figure to a ReportLab Image flowable."""
    buf = io.BytesIO()
    fig.set_size_inches(width, height)
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight",
                facecolor="#FAFAFA", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    img = Image(buf, width=width * inch, height=height * inch)
    return img


def make_sentiment_pie(sentiments: dict) -> Image:
    """Sentiment distribution pie chart with cartoon styling."""
    fig, ax = plt.subplots()
    labels = list(sentiments.keys())
    sizes = list(sentiments.values())
    color_map = {
        "Bullish": "#2E7D32", "Bearish": "#C62828", "Neutral": "#9E9E9E",
        "Hawkish": "#E65100", "Dovish": "#1565C0",
        "Positive": "#2E7D32", "Negative": "#C62828",
    }
    pie_colors = [color_map.get(l, CHART_COLORS[i % len(CHART_COLORS)])
                  for i, l in enumerate(labels)]

    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, autopct="%1.0f%%", startangle=90,
        colors=pie_colors, textprops={"fontsize": 10},
        wedgeprops={"edgecolor": "white", "linewidth": 2},
        pctdistance=0.75,
    )
    for t in autotexts:
        t.set_fontsize(9)
        t.set_color("white")
        t.set_fontweight("bold")

    ax.set_title("Sentiment Distribution", fontsize=13, fontweight="bold",
                 color="#212121", pad=15)
    return _chart_to_image(fig, 4.5, 3.5)


def make_topic_bar(topics: list[tuple[str, int]], max_topics: int = 8) -> Image:
    """Horizontal bar chart of top topics."""
    topics = topics[:max_topics]
    if not topics:
        return None

    fig, ax = plt.subplots()
    names = [t[0][:25] for t in reversed(topics)]
    counts = [t[1] for t in reversed(topics)]

    bars = ax.barh(names, counts, color=CHART_COLORS[:len(names)],
                   edgecolor="white", linewidth=1.5, height=0.6)
    ax.set_xlabel("Article Count", fontsize=10, color="#616161")
    ax.set_title("Top Topics", fontsize=13, fontweight="bold",
                 color="#212121", pad=12)
    ax.tick_params(axis="y", labelsize=9)
    ax.tick_params(axis="x", labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#E0E0E0")
    ax.spines["bottom"].set_color("#E0E0E0")

    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                str(count), va="center", fontsize=9, color="#616161")

    plt.tight_layout()
    return _chart_to_image(fig, 5, 3)


def make_source_activity(source_counts: dict) -> Image:
    """Bar chart of articles per source."""
    if not source_counts:
        return None

    fig, ax = plt.subplots()
    sources = list(source_counts.keys())[:10]
    counts = [source_counts[s] for s in sources]

    bars = ax.bar(sources, counts, color=CHART_COLORS[:len(sources)],
                  edgecolor="white", linewidth=1.5, width=0.6)
    ax.set_ylabel("Articles", fontsize=10, color="#616161")
    ax.set_title("Data Collection by Source", fontsize=13, fontweight="bold",
                 color="#212121", pad=12)
    ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.tick_params(axis="y", labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for bar, count in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                str(count), ha="center", fontsize=8, color="#616161")

    plt.tight_layout()
    return _chart_to_image(fig, 5.5, 3)


def make_ticker_chart(tickers: list[tuple[str, int]], max_tickers: int = 10) -> Image:
    """Horizontal bar chart of most mentioned tickers."""
    tickers = tickers[:max_tickers]
    if not tickers:
        return None

    fig, ax = plt.subplots()
    names = [t[0] for t in reversed(tickers)]
    counts = [t[1] for t in reversed(tickers)]

    bars = ax.barh(names, counts, color="#1565C0", edgecolor="white",
                   linewidth=1.5, height=0.5)
    ax.set_xlabel("Mentions", fontsize=10, color="#616161")
    ax.set_title("Most Mentioned Tickers", fontsize=13, fontweight="bold",
                 color="#212121", pad=12)
    ax.tick_params(axis="y", labelsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height() / 2,
                str(count), va="center", fontsize=9, color="#616161")

    plt.tight_layout()
    return _chart_to_image(fig, 4.5, 3)


def make_sentiment_timeline(hourly_sentiment: list[tuple[str, float]]) -> Image:
    """Line chart of sentiment over the last 24 hours."""
    if not hourly_sentiment or len(hourly_sentiment) < 2:
        return None

    fig, ax = plt.subplots()
    hours = [h[0] for h in hourly_sentiment]
    scores = [h[1] for h in hourly_sentiment]

    ax.plot(hours, scores, color="#1565C0", linewidth=2.5, marker="o",
            markersize=5, markerfacecolor="#FF6F00", markeredgecolor="white",
            markeredgewidth=1.5)
    ax.fill_between(range(len(hours)), scores, alpha=0.15, color="#1565C0")
    ax.axhline(y=0, color="#E0E0E0", linestyle="--", linewidth=1)

    ax.set_title("Sentiment Over 24 Hours", fontsize=13, fontweight="bold",
                 color="#212121", pad=12)
    ax.set_ylabel("Sentiment Score", fontsize=10, color="#616161")
    ax.tick_params(axis="x", rotation=45, labelsize=7)
    ax.tick_params(axis="y", labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    return _chart_to_image(fig, 5.5, 2.8)


def make_econ_indicators_chart(indicators: list[dict]) -> Image:
    """Grouped bar chart of key economic indicators."""
    if not indicators:
        return None

    fig, ax = plt.subplots()
    names = [d["indicator"][:20] for d in indicators[:8]]
    values = [float(d["value"]) if d.get("value") else 0 for d in indicators[:8]]

    bar_colors = ["#2E7D32" if v >= 0 else "#C62828" for v in values]
    bars = ax.barh(list(reversed(names)), list(reversed(values)),
                   color=list(reversed(bar_colors)),
                   edgecolor="white", linewidth=1.5, height=0.5)

    ax.set_title("Key Economic Indicators", fontsize=13, fontweight="bold",
                 color="#212121", pad=12)
    ax.axvline(x=0, color="#E0E0E0", linestyle="-", linewidth=1)
    ax.tick_params(axis="y", labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    return _chart_to_image(fig, 5, 3)


# ─── Decorative Drawings ────────────────────────────────────────

def draw_stat_card(canvas, x, y, w, h, number, label, accent_color):
    """Draw a rounded stat card on the canvas."""
    canvas.saveState()
    canvas.setFillColor(colors.HexColor("#FFFFFF"))
    canvas.setStrokeColor(colors.HexColor("#E0E0E0"))
    canvas.setLineWidth(1)
    canvas.roundRect(x, y, w, h, 8, fill=1, stroke=1)

    # Accent bar at top
    canvas.setFillColor(accent_color)
    canvas.roundRect(x, y + h - 6, w, 6, 3, fill=1, stroke=0)

    # Number
    canvas.setFillColor(accent_color)
    canvas.setFont("Helvetica-Bold", 20)
    canvas.drawCentredString(x + w / 2, y + h / 2 - 2, str(number))

    # Label
    canvas.setFillColor(colors.HexColor("#757575"))
    canvas.setFont("Helvetica", 8)
    canvas.drawCentredString(x + w / 2, y + 10, label)
    canvas.restoreState()


def draw_section_divider(canvas, x, y, width):
    """Draw a decorative section divider."""
    canvas.saveState()
    canvas.setStrokeColor(BLUE_PRIMARY)
    canvas.setLineWidth(2)
    canvas.line(x, y, x + 40, y)
    canvas.setStrokeColor(colors.HexColor("#E0E0E0"))
    canvas.setLineWidth(0.5)
    canvas.line(x + 45, y, x + width, y)
    canvas.restoreState()


def draw_bull_icon(canvas, x, y, size=30):
    """Draw a simple bull icon (upward triangle + circle)."""
    canvas.saveState()
    canvas.setFillColor(GREEN)
    # Body circle
    canvas.circle(x + size / 2, y + size / 2, size / 3, fill=1, stroke=0)
    # Horns (upward triangles)
    p = canvas.beginPath()
    p.moveTo(x + size * 0.2, y + size * 0.7)
    p.lineTo(x + size * 0.35, y + size)
    p.lineTo(x + size * 0.5, y + size * 0.7)
    p.close()
    canvas.drawPath(p, fill=1, stroke=0)
    p2 = canvas.beginPath()
    p2.moveTo(x + size * 0.5, y + size * 0.7)
    p2.lineTo(x + size * 0.65, y + size)
    p2.lineTo(x + size * 0.8, y + size * 0.7)
    p2.close()
    canvas.drawPath(p2, fill=1, stroke=0)
    # Eyes
    canvas.setFillColor(WHITE)
    canvas.circle(x + size * 0.4, y + size * 0.55, 2, fill=1, stroke=0)
    canvas.circle(x + size * 0.6, y + size * 0.55, 2, fill=1, stroke=0)
    canvas.restoreState()


def draw_bear_icon(canvas, x, y, size=30):
    """Draw a simple bear icon (downward triangle + circle)."""
    canvas.saveState()
    canvas.setFillColor(RED)
    # Body circle
    canvas.circle(x + size / 2, y + size / 2, size / 3, fill=1, stroke=0)
    # Ears
    canvas.circle(x + size * 0.25, y + size * 0.8, size / 6, fill=1, stroke=0)
    canvas.circle(x + size * 0.75, y + size * 0.8, size / 6, fill=1, stroke=0)
    # Eyes
    canvas.setFillColor(WHITE)
    canvas.circle(x + size * 0.4, y + size * 0.55, 2, fill=1, stroke=0)
    canvas.circle(x + size * 0.6, y + size * 0.55, 2, fill=1, stroke=0)
    # Frown
    canvas.setStrokeColor(WHITE)
    canvas.setLineWidth(1.5)
    canvas.arc(x + size * 0.3, y + size * 0.2, x + size * 0.7, y + size * 0.45,
               startAng=0, extent=180)
    canvas.restoreState()


def draw_globe_icon(canvas, x, y, size=30):
    """Draw a simple globe icon."""
    canvas.saveState()
    canvas.setFillColor(BLUE_PRIMARY)
    canvas.circle(x + size / 2, y + size / 2, size / 2.2, fill=1, stroke=0)
    # Meridians
    canvas.setStrokeColor(colors.HexColor("#E3F2FD"))
    canvas.setLineWidth(1)
    canvas.ellipse(x + size * 0.2, y + size * 0.1,
                   x + size * 0.8, y + size * 0.9, fill=0, stroke=1)
    canvas.line(x + size * 0.1, y + size / 2, x + size * 0.9, y + size / 2)
    canvas.restoreState()


# ─── Page Decoration ─────────────────────────────────────────────

def _cover_page(canvas, doc):
    """Custom cover page decoration."""
    canvas.saveState()

    # Blue gradient header band
    for i in range(120):
        frac = i / 120
        r = int(13 + (21 - 13) * frac)
        g = int(71 + (101 - 71) * frac)
        b = int(161 + (192 - 161) * frac)
        canvas.setFillColor(colors.Color(r / 255, g / 255, b / 255))
        canvas.rect(0, PAGE_H - i - 1, PAGE_W, 1, fill=1, stroke=0)

    # Title in white
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 34)
    canvas.drawCentredString(PAGE_W / 2, PAGE_H - 55, "WORLD INTELLIGENCE")
    canvas.setFont("Helvetica", 16)
    canvas.drawCentredString(PAGE_W / 2, PAGE_H - 78, "Daily Briefing Report")

    # Date
    canvas.setFont("Helvetica-Bold", 14)
    canvas.drawCentredString(PAGE_W / 2, PAGE_H - 102,
                             date.today().strftime("%A, %B %d, %Y"))

    # Globe icon centered
    draw_globe_icon(canvas, PAGE_W / 2 - 25, PAGE_H - 160, 50)

    # Bottom bar
    canvas.setFillColor(BLUE_DARK)
    canvas.rect(0, 0, PAGE_W, 30, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica", 7)
    canvas.drawCentredString(PAGE_W / 2, 12,
                             "Social Scraper Intelligence Platform  |  Confidential")

    canvas.restoreState()


def _normal_page(canvas, doc):
    """Normal page decoration with header and footer."""
    canvas.saveState()

    # Header band
    canvas.setFillColor(BLUE_DARK)
    canvas.rect(0, PAGE_H - 32, PAGE_W, 32, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawString(MARGIN, PAGE_H - 22, "WORLD INTELLIGENCE REPORT")
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 22,
                           date.today().strftime("%d %b %Y"))

    # Footer
    canvas.setFillColor(colors.HexColor("#E0E0E0"))
    canvas.line(MARGIN, 35, PAGE_W - MARGIN, 35)
    canvas.setFillColor(GRAY_TEXT)
    canvas.setFont("Helvetica", 7)
    canvas.drawString(MARGIN, 22, "Social Scraper Intelligence Platform")
    canvas.drawRightString(PAGE_W - MARGIN, 22, f"Page {doc.page}")

    canvas.restoreState()


# ─── Data Fetching ───────────────────────────────────────────────

def _fetch_report_data() -> dict:
    """Query database for last 24h of data."""
    try:
        from api.database import SessionLocal
        from storage.models import (
            Article, DailyDigest, EconomicData,
            Entity, SentimentScore, ArticleTopic, CollectionLog,
        )
    except ImportError:
        logger.warning("Database modules not available, using sample data")
        return _sample_data()

    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        today = date.today()

        # Articles
        articles = (
            db.query(Article)
            .filter(Article.collected_at >= cutoff)
            .order_by(Article.collected_at.desc())
            .limit(200)
            .all()
        )

        article_ids = [a.id for a in articles]

        # Sentiments
        sentiments = []
        if article_ids:
            sentiments = (
                db.query(SentimentScore)
                .filter(SentimentScore.article_id.in_(article_ids))
                .all()
            )

        # Entities
        entities = []
        if article_ids:
            entities = (
                db.query(Entity)
                .filter(Entity.article_id.in_(article_ids))
                .all()
            )

        # Topics
        topics = []
        if article_ids:
            topics = (
                db.query(ArticleTopic)
                .filter(ArticleTopic.article_id.in_(article_ids))
                .all()
            )

        # Economic data
        econ_data = (
            db.query(EconomicData)
            .filter(EconomicData.collected_at >= cutoff)
            .order_by(EconomicData.collected_at.desc())
            .limit(30)
            .all()
        )

        # Daily digest (LLM summary)
        digest = db.query(DailyDigest).filter(DailyDigest.date == today).first()
        if not digest:
            yesterday = today - timedelta(days=1)
            digest = db.query(DailyDigest).filter(DailyDigest.date == yesterday).first()

        # Collection logs (source health)
        logs = (
            db.query(CollectionLog)
            .filter(CollectionLog.run_at >= cutoff)
            .order_by(CollectionLog.run_at.desc())
            .all()
        )

        # ── Aggregate ──────────────────────────────────
        # Source counts
        source_counts = Counter(a.source for a in articles)

        # Sentiment distribution
        bullish = sum(1 for s in sentiments if s.overall > 0.2)
        bearish = sum(1 for s in sentiments if s.overall < -0.2)
        neutral = len(sentiments) - bullish - bearish
        sentiment_dist = {"Bullish": bullish, "Bearish": bearish, "Neutral": neutral}

        # Policy direction
        hawkish = sum(1 for s in sentiments if s.policy_direction == "hawkish")
        dovish = sum(1 for s in sentiments if s.policy_direction == "dovish")
        policy_neutral = len(sentiments) - hawkish - dovish

        # Topic counts
        topic_counts = Counter(t.topic for t in topics)
        top_topics = topic_counts.most_common(10)

        # Entity extraction — tickers
        ticker_counts = Counter()
        org_counts = Counter()
        for e in entities:
            if e.entity_type in ("TICKER", "SYMBOL"):
                ticker_counts[e.entity_value] += 1
            elif e.entity_type in ("ORG", "FIN_ORG"):
                org_counts[e.entity_value] += 1

        # Hourly sentiment timeline
        hourly_sent = {}
        for s in sentiments:
            hour_key = s.created_at.strftime("%H:00") if s.created_at else "??"
            if hour_key not in hourly_sent:
                hourly_sent[hour_key] = []
            hourly_sent[hour_key].append(s.overall)
        hourly_timeline = [
            (h, sum(vals) / len(vals))
            for h, vals in sorted(hourly_sent.items())
        ]

        # Headlines
        headlines = []
        for a in articles[:20]:
            if a.title:
                headlines.append({
                    "title": a.title,
                    "source": a.source,
                    "category": a.category or "",
                    "url": a.url or "",
                    "published": a.published_at.strftime("%H:%M") if a.published_at else "",
                })

        # Economic indicators
        econ_indicators = []
        seen_indicators = set()
        for e in econ_data:
            if e.indicator not in seen_indicators:
                seen_indicators.add(e.indicator)
                econ_indicators.append({
                    "indicator": e.indicator,
                    "value": float(e.value) if e.value else 0,
                    "source": e.source,
                    "unit": e.unit or "",
                })

        # Collection health
        source_health = {}
        for log in logs:
            if log.source not in source_health:
                source_health[log.source] = log.status

        active_sources = sum(1 for s in source_health.values() if s == "success")
        failed_sources = sum(1 for s in source_health.values() if s == "failed")

        avg_sentiment = (
            sum(s.overall for s in sentiments) / len(sentiments)
            if sentiments else 0.0
        )

        return {
            "date": today,
            "total_articles": len(articles),
            "total_sources": len(source_counts),
            "active_sources": active_sources,
            "failed_sources": failed_sources,
            "avg_sentiment": round(avg_sentiment, 3),
            "sentiment_dist": sentiment_dist,
            "policy": {"hawkish": hawkish, "dovish": dovish, "neutral": policy_neutral},
            "source_counts": dict(source_counts.most_common(12)),
            "top_topics": top_topics,
            "top_tickers": ticker_counts.most_common(10),
            "top_orgs": org_counts.most_common(10),
            "hourly_sentiment": hourly_timeline,
            "headlines": headlines,
            "econ_indicators": econ_indicators[:12],
            "digest_summary": digest.summary if digest else None,
            "digest_themes": digest.top_themes if digest else [],
            "new_circulars": digest.new_circulars if digest else [],
            "source_health": source_health,
        }
    except Exception as e:
        logger.error(f"DB query failed: {e}")
        return _sample_data()
    finally:
        db.close()


def _sample_data() -> dict:
    """Fallback: fetch LIVE data from RSS feeds and free APIs when DB is down."""
    logger.info("DB unavailable — fetching live data from RSS feeds and APIs...")
    return _fetch_live_web_data()


def _fetch_live_web_data() -> dict:
    """Fetch real data from RSS feeds, Hacker News API, and free sources.

    This runs when the database is unavailable, ensuring the report
    always has real content. Uses only free, no-auth-required sources.
    """
    import xml.etree.ElementTree as ET
    import re

    try:
        import httpx
    except ImportError:
        logger.error("httpx not available for live data fetch")
        return _empty_data()

    headlines = []
    source_counts = Counter()
    all_text = []

    # ── RSS Feeds (no auth needed) ────────────────────
    rss_feeds = {
        "Reuters": "https://feeds.reuters.com/reuters/businessNews",
        "CNBC": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "Moneycontrol": "https://www.moneycontrol.com/rss/latestnews.xml",
        "ET Economy": "https://economictimes.indiatimes.com/news/economy/rssfeedstopstories.cms",
        "Livemint": "https://www.livemint.com/rss/economy",
        "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "Fed Reserve": "https://www.federalreserve.gov/feeds/press_all.xml",
        "arXiv QFin": "https://rss.arxiv.org/rss/q-fin",
    }

    client = httpx.Client(timeout=12, follow_redirects=True, headers={
        "User-Agent": "EconScraper/4.1 (news aggregator)"
    })

    for source_name, url in rss_feeds.items():
        try:
            resp = client.get(url)
            if resp.status_code != 200:
                continue

            root = ET.fromstring(resp.text)

            # Handle both RSS 2.0 and Atom feeds
            items = root.findall(".//item")
            if not items:
                # Try Atom format
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                items = root.findall(".//atom:entry", ns)

            count = 0
            for item in items[:8]:
                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue

                title = title_el.text.strip()
                link_el = item.find("link")
                link = ""
                if link_el is not None:
                    link = link_el.text or link_el.get("href", "")

                pub_el = item.find("pubDate")
                pub_time = ""
                if pub_el is not None and pub_el.text:
                    # Extract just the time portion
                    try:
                        from email.utils import parsedate_to_datetime
                        dt = parsedate_to_datetime(pub_el.text)
                        pub_time = dt.strftime("%H:%M")
                    except Exception:
                        pub_time = ""

                desc_el = item.find("description")
                desc = desc_el.text[:300] if desc_el is not None and desc_el.text else ""

                headlines.append({
                    "title": title,
                    "source": source_name,
                    "category": "news",
                    "url": link,
                    "published": pub_time,
                })
                all_text.append(f"{title} {desc}")
                count += 1

            if count > 0:
                source_counts[source_name] = count

        except Exception as e:
            logger.debug(f"RSS fetch failed for {source_name}: {e}")

    # ── Hacker News API (free, no auth) ───────────────
    try:
        resp = client.get("https://hacker-news.firebaseio.com/v0/topstories.json")
        if resp.status_code == 200:
            story_ids = resp.json()[:10]
            hn_count = 0
            for sid in story_ids:
                try:
                    sr = client.get(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json")
                    if sr.status_code == 200:
                        story = sr.json()
                        if story and story.get("title"):
                            headlines.append({
                                "title": story["title"],
                                "source": "Hacker News",
                                "category": "tech",
                                "url": story.get("url", ""),
                                "published": "",
                            })
                            all_text.append(story["title"])
                            hn_count += 1
                except Exception:
                    pass
            if hn_count:
                source_counts["Hacker News"] = hn_count
    except Exception as e:
        logger.debug(f"HN fetch failed: {e}")

    # ── CoinGecko crypto prices (free, no auth) ────────
    crypto_prices = {}
    try:
        resp = client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin,ethereum,solana,ripple,cardano", "vs_currencies": "usd", "include_24hr_change": "true"},
        )
        if resp.status_code == 200:
            cg = resp.json()
            for coin_id, data in cg.items():
                name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP", "cardano": "ADA"}
                sym = name_map.get(coin_id, coin_id.upper())
                crypto_prices[sym] = {
                    "price": data.get("usd", 0),
                    "change_24h": data.get("usd_24h_change", 0),
                }
    except Exception as e:
        logger.debug(f"CoinGecko fetch failed: {e}")

    # ── CoinGecko global market data ──────────────────
    try:
        resp = client.get("https://api.coingecko.com/api/v3/global")
        if resp.status_code == 200:
            gd = resp.json().get("data", {})
            total_mcap = gd.get("total_market_cap", {}).get("usd", 0)
            btc_dominance = gd.get("market_cap_percentage", {}).get("btc", 0)
            if total_mcap:
                crypto_prices["_global"] = {
                    "total_mcap_t": round(total_mcap / 1e12, 2),
                    "btc_dominance": round(btc_dominance, 1),
                }
    except Exception:
        pass

    client.close()

    # ── Analyze collected text for sentiment + topics ──
    bullish_kw = ["rally", "surge", "gain", "bull", "buy", "growth", "up",
                  "rise", "jump", "soar", "beat", "strong", "record", "high",
                  "cut", "stimulus", "dovish", "easing", "recovery"]
    bearish_kw = ["crash", "fall", "drop", "bear", "sell", "decline", "down",
                  "loss", "plunge", "miss", "weak", "low", "recession",
                  "hike", "hawkish", "tightening", "slump", "warning"]

    bullish = 0
    bearish = 0
    neutral = 0
    ticker_counts = Counter()
    topic_counts = Counter()

    topic_keywords = {
        "Markets": ["stock", "market", "index", "s&p", "nasdaq", "dow", "nifty", "sensex"],
        "Crypto": ["bitcoin", "btc", "eth", "crypto", "blockchain", "defi", "token"],
        "Central Banks": ["fed", "rbi", "ecb", "rate", "monetary", "policy", "inflation"],
        "Economy": ["gdp", "economy", "growth", "employment", "jobs", "trade", "fiscal"],
        "India": ["india", "rbi", "rupee", "nse", "bse", "sebi", "nifty", "sensex"],
        "Tech": ["ai", "tech", "startup", "software", "chip", "semiconductor"],
        "Energy": ["oil", "gas", "energy", "opec", "crude", "solar", "renewable"],
        "Earnings": ["earnings", "revenue", "profit", "quarterly", "results"],
        "Geopolitics": ["war", "sanction", "tariff", "trade war", "election", "geopolitical"],
        "Bonds": ["bond", "yield", "treasury", "g-sec", "fixed income", "debt"],
    }

    ticker_re = re.compile(r"\$([A-Z]{1,5})\b")
    crypto_re = re.compile(r"\b(BTC|ETH|SOL|ADA|XRP|DOGE|AVAX|MATIC)\b", re.I)

    for text in all_text:
        text_lower = text.lower()
        b_score = sum(1 for kw in bullish_kw if kw in text_lower)
        be_score = sum(1 for kw in bearish_kw if kw in text_lower)
        if b_score > be_score:
            bullish += 1
        elif be_score > b_score:
            bearish += 1
        else:
            neutral += 1

        # Tickers
        for m in ticker_re.findall(text):
            ticker_counts[m.upper()] += 1
        for m in crypto_re.findall(text):
            ticker_counts[m.upper()] += 1

        # Topics
        for topic, kws in topic_keywords.items():
            if any(kw in text_lower for kw in kws):
                topic_counts[topic] += 1

    total_articles = len(headlines)
    sentiment_total = bullish + bearish + neutral
    avg_sent = (bullish - bearish) / max(sentiment_total, 1)

    # Build summary from top headlines
    top_titles = [h["title"] for h in headlines[:6]]
    summary_text = (
        f"Today's briefing covers {total_articles} articles from "
        f"{len(source_counts)} live sources. "
    )
    if top_titles:
        summary_text += "Key stories: " + " | ".join(top_titles[:4]) + ". "
    if bullish > bearish:
        summary_text += f"Overall market sentiment leans bullish ({bullish} bullish vs {bearish} bearish signals). "
    elif bearish > bullish:
        summary_text += f"Overall market sentiment leans bearish ({bearish} bearish vs {bullish} bullish signals). "
    else:
        summary_text += "Market sentiment is mixed with no clear directional bias. "

    return {
        "date": date.today(),
        "total_articles": total_articles,
        "total_sources": len(source_counts),
        "active_sources": len(source_counts),
        "failed_sources": len(rss_feeds) - len(source_counts),
        "avg_sentiment": round(avg_sent, 3),
        "sentiment_dist": {"Bullish": bullish, "Bearish": bearish, "Neutral": neutral},
        "policy": {
            "hawkish": sum(1 for t in all_text if any(k in t.lower() for k in ["hawkish", "hike", "tightening"])),
            "dovish": sum(1 for t in all_text if any(k in t.lower() for k in ["dovish", "cut", "easing"])),
            "neutral": max(0, len(all_text) - sum(1 for t in all_text if any(k in t.lower() for k in ["hawkish", "hike", "dovish", "cut"]))),
        },
        "source_counts": dict(source_counts.most_common(12)),
        "top_topics": topic_counts.most_common(10),
        "top_tickers": ticker_counts.most_common(10),
        "top_orgs": [],
        "hourly_sentiment": [],
        "headlines": headlines[:20],
        "econ_indicators": _build_live_indicators(crypto_prices),
        "digest_summary": summary_text,
        "digest_themes": [{"topic": t, "count": c} for t, c in topic_counts.most_common(5)],
        "new_circulars": [],
        "source_health": {name: "success" for name in source_counts},
    }


def _build_live_indicators(crypto_prices: dict) -> list[dict]:
    """Build economic indicator rows from live API data."""
    indicators = []

    # Crypto prices
    for sym in ["BTC", "ETH", "SOL", "XRP", "ADA"]:
        if sym in crypto_prices:
            cp = crypto_prices[sym]
            indicators.append({
                "indicator": f"{sym} Price",
                "value": cp["price"],
                "source": "CoinGecko",
                "unit": "USD",
            })
            indicators.append({
                "indicator": f"{sym} 24h Change",
                "value": round(cp.get("change_24h", 0), 2),
                "source": "CoinGecko",
                "unit": "%",
            })

    # Global crypto market
    if "_global" in crypto_prices:
        g = crypto_prices["_global"]
        indicators.append({
            "indicator": "Crypto Total Market Cap",
            "value": g["total_mcap_t"],
            "source": "CoinGecko",
            "unit": "T USD",
        })
        indicators.append({
            "indicator": "BTC Dominance",
            "value": g["btc_dominance"],
            "source": "CoinGecko",
            "unit": "%",
        })

    return indicators


def _empty_data() -> dict:
    """Absolute fallback when nothing works."""
    return {
        "date": date.today(),
        "total_articles": 0, "total_sources": 0, "active_sources": 0,
        "failed_sources": 0, "avg_sentiment": 0.0,
        "sentiment_dist": {"Bullish": 0, "Bearish": 0, "Neutral": 0},
        "policy": {"hawkish": 0, "dovish": 0, "neutral": 0},
        "source_counts": {}, "top_topics": [], "top_tickers": [],
        "top_orgs": [], "hourly_sentiment": [], "headlines": [],
        "econ_indicators": [],
        "digest_summary": "Unable to fetch data. Check internet connection and try again.",
        "digest_themes": [], "new_circulars": [], "source_health": {},
    }


# ─── PDF Builder ─────────────────────────────────────────────────

def generate_report(output_path: str = None) -> str:
    """Generate the daily world intelligence report PDF.

    Returns the path to the generated PDF file.
    """
    if output_path is None:
        output_dir = Path(os.getenv("REPORT_OUTPUT_DIR",
                                     os.path.expanduser("~/social_scraper/reports/output")))
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(output_dir / f"world_report_{date.today()}.pdf")

    data = _fetch_report_data()
    styles = _build_styles()

    doc = BaseDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=MARGIN + 35,
        bottomMargin=MARGIN + 10,
        title="World Intelligence Report",
        author="Social Scraper Platform",
    )

    # Frames
    cover_frame = Frame(MARGIN, MARGIN + 10, PAGE_W - 2 * MARGIN, PAGE_H - 2 * MARGIN - 80,
                        id="cover")
    normal_frame = Frame(MARGIN, MARGIN + 10, PAGE_W - 2 * MARGIN, PAGE_H - 2 * MARGIN - 50,
                         id="normal")

    doc.addPageTemplates([
        PageTemplate(id="Cover", frames=cover_frame, onPage=_cover_page),
        PageTemplate(id="Normal", frames=normal_frame, onPage=_normal_page),
    ])

    story = []

    # ── PAGE 1: Cover + Stats ─────────────────────────
    story.append(Spacer(1, 140))

    # Stat cards as table
    stats = [
        (str(data["total_articles"]), "Articles Collected"),
        (str(data["total_sources"]), "Active Sources"),
        (f"{data['avg_sentiment']:+.2f}", "Avg Sentiment"),
        (str(data["failed_sources"]), "Source Alerts"),
    ]
    stat_data = [[
        Paragraph(f'<font size="20" color="{BLUE_PRIMARY.hexval()}">'
                  f'<b>{s[0]}</b></font><br/>'
                  f'<font size="8" color="#757575">{s[1]}</font>', styles["body"])
        for s in stats
    ]]
    stat_table = Table(stat_data, colWidths=[(PAGE_W - 2 * MARGIN) / 4] * 4)
    stat_table.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOX", (0, 0), (0, 0), 1, colors.HexColor("#E0E0E0")),
        ("BOX", (1, 0), (1, 0), 1, colors.HexColor("#E0E0E0")),
        ("BOX", (2, 0), (2, 0), 1, colors.HexColor("#E0E0E0")),
        ("BOX", (3, 0), (3, 0), 1, colors.HexColor("#E0E0E0")),
        ("BACKGROUND", (0, 0), (-1, -1), WHITE),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("ROUNDEDCORNERS", [6, 6, 6, 6]),
    ]))
    story.append(stat_table)
    story.append(Spacer(1, 20))

    # Executive Summary
    story.append(Paragraph("EXECUTIVE SUMMARY", styles["section_title"]))
    summary_text = data.get("digest_summary") or "Report data is being collected. Summary will be available once scrapers complete their cycle."
    # Wrap in a light box
    summary_data = [[Paragraph(summary_text, styles["body"])]]
    summary_box = Table(summary_data, colWidths=[PAGE_W - 2 * MARGIN - 20])
    summary_box.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BLUE_LIGHT),
        ("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#BBDEFB")),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
    ]))
    story.append(summary_box)

    # ── PAGE 2: Sentiment & Topics ────────────────────
    story.append(NextPageTemplate("Normal"))
    story.append(PageBreak())

    story.append(Paragraph("SENTIMENT ANALYSIS", styles["section_title"]))

    # Sentiment pie chart
    if any(v > 0 for v in data["sentiment_dist"].values()):
        pie_img = make_sentiment_pie(data["sentiment_dist"])
        story.append(pie_img)
    else:
        story.append(Paragraph("No sentiment data available yet.", styles["body_small"]))

    story.append(Spacer(1, 10))

    # Policy direction summary
    policy = data["policy"]
    if any(v > 0 for v in policy.values()):
        story.append(Paragraph("POLICY SIGNALS", styles["subsection"]))
        policy_text = (
            f"<b>Hawkish:</b> {policy['hawkish']} signals  |  "
            f"<b>Dovish:</b> {policy['dovish']} signals  |  "
            f"<b>Neutral:</b> {policy['neutral']}"
        )
        story.append(Paragraph(policy_text, styles["body"]))

    story.append(Spacer(1, 10))

    # Sentiment timeline
    timeline_img = make_sentiment_timeline(data["hourly_sentiment"])
    if timeline_img:
        story.append(Paragraph("24-HOUR SENTIMENT TREND", styles["subsection"]))
        story.append(timeline_img)

    # ── PAGE 3: Topics & Tickers ──────────────────────
    story.append(PageBreak())

    story.append(Paragraph("TOPIC ANALYSIS", styles["section_title"]))
    topic_img = make_topic_bar(data["top_topics"])
    if topic_img:
        story.append(topic_img)
    else:
        story.append(Paragraph("No topic data available yet.", styles["body_small"]))

    story.append(Spacer(1, 15))

    # Tickers
    story.append(Paragraph("MARKET MENTIONS", styles["section_title"]))
    ticker_img = make_ticker_chart(data["top_tickers"])
    if ticker_img:
        story.append(ticker_img)
    elif data["top_orgs"]:
        story.append(Paragraph("TOP MENTIONED ORGANIZATIONS", styles["subsection"]))
        for org, count in data["top_orgs"][:8]:
            story.append(Paragraph(f"&bull; <b>{org}</b> — {count} mentions", styles["body"]))
    else:
        story.append(Paragraph("No ticker/entity data available yet.", styles["body_small"]))

    # ── PAGE 4: Data Collection & Economic Data ───────
    story.append(PageBreak())

    story.append(Paragraph("DATA COLLECTION OVERVIEW", styles["section_title"]))
    source_img = make_source_activity(data["source_counts"])
    if source_img:
        story.append(source_img)
    else:
        story.append(Paragraph("No collection data available yet.", styles["body_small"]))

    story.append(Spacer(1, 15))

    # Economic indicators
    if data["econ_indicators"]:
        story.append(Paragraph("KEY ECONOMIC INDICATORS", styles["section_title"]))

        econ_header = [
            Paragraph("<b>Indicator</b>", styles["table_header"]),
            Paragraph("<b>Value</b>", styles["table_header"]),
            Paragraph("<b>Source</b>", styles["table_header"]),
            Paragraph("<b>Unit</b>", styles["table_header"]),
        ]
        econ_rows = [econ_header]
        for ind in data["econ_indicators"][:10]:
            econ_rows.append([
                Paragraph(ind["indicator"][:30], styles["table_cell"]),
                Paragraph(f"{ind['value']:.4g}", styles["table_cell"]),
                Paragraph(ind["source"], styles["table_cell"]),
                Paragraph(ind["unit"], styles["table_cell"]),
            ])

        col_widths = [180, 80, 100, 60]
        econ_table = Table(econ_rows, colWidths=col_widths)
        econ_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("BACKGROUND", (0, 1), (-1, -1), WHITE),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#F5F5F5")]),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E0E0E0")),
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ]))
        story.append(econ_table)

        econ_chart = make_econ_indicators_chart(data["econ_indicators"])
        if econ_chart:
            story.append(Spacer(1, 10))
            story.append(econ_chart)

    # ── PAGE 5: Headlines ─────────────────────────────
    story.append(PageBreak())

    story.append(Paragraph("TOP HEADLINES", styles["section_title"]))

    if data["headlines"]:
        for i, h in enumerate(data["headlines"][:15], 1):
            source_tag = f'<font color="{BLUE_PRIMARY.hexval()}" size="8">[{h["source"]}]</font>'
            time_tag = f'<font color="#9E9E9E" size="8">{h["published"]}</font>' if h["published"] else ""
            story.append(Paragraph(
                f'<b>{i}.</b> {h["title"]}  {source_tag} {time_tag}',
                styles["headline"]
            ))
            story.append(Spacer(1, 3))
    else:
        story.append(Paragraph("No headlines collected in the last 24 hours.", styles["body_small"]))

    story.append(Spacer(1, 20))

    # New Circulars
    if data["new_circulars"]:
        story.append(Paragraph("REGULATORY UPDATES", styles["section_title"]))
        for circ in data["new_circulars"][:8]:
            title = circ.get("title", "Untitled")
            story.append(Paragraph(f"&bull; {title}", styles["body"]))

    # ── PAGE 6: Source Health ─────────────────────────
    if data["source_health"]:
        story.append(PageBreak())
        story.append(Paragraph("SOURCE HEALTH STATUS", styles["section_title"]))

        health_header = [
            Paragraph("<b>Source</b>", styles["table_header"]),
            Paragraph("<b>Status</b>", styles["table_header"]),
        ]
        health_rows = [health_header]
        status_colors = {
            "success": "#2E7D32", "failed": "#C62828",
            "partial": "#FF6F00", "running": "#1565C0",
        }
        for source, status in sorted(data["source_health"].items()):
            color = status_colors.get(status, "#616161")
            health_rows.append([
                Paragraph(source, styles["table_cell"]),
                Paragraph(f'<font color="{color}"><b>{status.upper()}</b></font>',
                          styles["table_cell"]),
            ])

        health_table = Table(health_rows, colWidths=[280, 120])
        health_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BLUE_DARK),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#F5F5F5")]),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E0E0E0")),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ]))
        story.append(health_table)

    # ── Build PDF ─────────────────────────────────────
    doc.build(story)
    logger.info(f"Report generated: {output_path}")
    return output_path
