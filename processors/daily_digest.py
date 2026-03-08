"""Daily digest generator — LLM-powered summary of the day's data.

Aggregates articles, sentiment, entities, and economic data into
a daily briefing. Uses Anthropic Claude or Ollama for summarization.
Stores the digest and optionally sends via Telegram.
"""

import logging
import os
from datetime import date, datetime, timedelta, timezone

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class DailyDigestGenerator(BaseProcessor):
    name = "daily_digest"
    batch_size = 50

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.llm_model = self.config.get("llm_model", "claude-sonnet-4-20250514")
        self.ollama_model = self.config.get("ollama_model", "llama3")
        self.ollama_url = self.config.get("ollama_url", "http://localhost:11434")
        self.max_articles = self.config.get("max_articles", 50)
        self.send_telegram = self.config.get("send_telegram", False)

    def process_one(self, article: dict) -> dict:
        return {"status": "use_run"}

    def run(self) -> dict:
        """Generate today's digest from all collected data."""
        from api.database import SessionLocal
        from storage.models import (
            Article, DailyDigest, EconomicData,
            Entity, SentimentScore, ArticleTopic,
        )

        db = SessionLocal()
        try:
            today = date.today()
            yesterday = today - timedelta(days=1)

            # Check if digest already exists
            existing = db.query(DailyDigest).filter(DailyDigest.date == today).first()
            if existing:
                return {"status": "exists", "date": str(today)}

            # Gather today's articles
            cutoff = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=timezone.utc)
            articles = (
                db.query(Article)
                .filter(Article.collected_at >= cutoff)
                .order_by(Article.collected_at.desc())
                .limit(self.max_articles)
                .all()
            )

            if not articles:
                return {"status": "no_data", "date": str(today)}

            # Gather sentiment scores
            article_ids = [a.id for a in articles]
            sentiments = (
                db.query(SentimentScore)
                .filter(SentimentScore.article_id.in_(article_ids))
                .all()
            )

            # Gather top entities
            entities = (
                db.query(Entity.entity_type, Entity.entity_value)
                .filter(Entity.article_id.in_(article_ids))
                .all()
            )

            # Gather topics
            topics = (
                db.query(ArticleTopic.topic, ArticleTopic.confidence)
                .filter(ArticleTopic.article_id.in_(article_ids))
                .all()
            )

            # Gather key economic data
            econ_data = (
                db.query(EconomicData)
                .filter(EconomicData.collected_at >= cutoff)
                .order_by(EconomicData.collected_at.desc())
                .limit(20)
                .all()
            )

            # Build context for LLM
            context = self._build_context(articles, sentiments, entities, topics, econ_data)
            summary = self._generate_summary(context)

            # Compute aggregates
            avg_sentiment = (
                sum(s.overall for s in sentiments) / len(sentiments)
                if sentiments else 0.0
            )
            topic_counts: dict[str, int] = {}
            for t in topics:
                topic_counts[t[0]] = topic_counts.get(t[0], 0) + 1
            top_themes = sorted(topic_counts.items(), key=lambda x: -x[1])[:5]

            # Store digest
            digest = DailyDigest(
                date=today,
                summary=summary,
                top_themes=[{"topic": t, "count": c} for t, c in top_themes],
                sentiment_summary={
                    "average": round(avg_sentiment, 3),
                    "total_articles": len(articles),
                    "analyzed": len(sentiments),
                },
                key_data_releases=[
                    {"indicator": e.indicator, "value": float(e.value) if e.value else None, "source": e.source}
                    for e in econ_data[:10]
                ],
                new_circulars=[
                    {"title": a.title, "url": a.url}
                    for a in articles
                    if a.category in ("rbi_circular", "sebi_circular")
                ][:10],
            )
            db.add(digest)
            db.commit()

            if self.send_telegram:
                self._send_telegram(summary)

            return {
                "status": "success",
                "date": str(today),
                "articles": len(articles),
                "summary_length": len(summary),
            }
        except Exception as e:
            logger.error(f"[DailyDigest] Failed: {e}")
            return {"status": "error", "error": str(e)}
        finally:
            db.close()

    def _build_context(self, articles, sentiments, entities, topics, econ_data) -> str:
        lines = [f"=== Daily Economic Briefing ({date.today()}) ===\n"]

        lines.append(f"\n--- {len(articles)} Articles Collected ---")
        for a in articles[:20]:
            lines.append(f"- [{a.source}] {a.title or '(no title)'}")

        if sentiments:
            avg = sum(s.overall for s in sentiments) / len(sentiments)
            hawk = sum(1 for s in sentiments if s.policy_direction == "hawkish")
            dove = sum(1 for s in sentiments if s.policy_direction == "dovish")
            lines.append(f"\n--- Sentiment: avg={avg:.2f}, hawkish={hawk}, dovish={dove} ---")

        if entities:
            org_counts: dict[str, int] = {}
            for etype, evalue in entities:
                if etype in ("ORG", "FIN_ORG"):
                    org_counts[evalue] = org_counts.get(evalue, 0) + 1
            top_orgs = sorted(org_counts.items(), key=lambda x: -x[1])[:10]
            lines.append(f"\n--- Top Entities: {', '.join(f'{o}({c})' for o, c in top_orgs)} ---")

        if econ_data:
            lines.append("\n--- Key Data Releases ---")
            for e in econ_data[:10]:
                lines.append(f"- {e.indicator}: {e.value} ({e.source})")

        return "\n".join(lines)

    def _generate_summary(self, context: str) -> str:
        """Generate summary using Claude API or Ollama fallback."""
        prompt = (
            "You are an economic analyst. Based on the following data collected today, "
            "write a concise daily briefing (3-5 paragraphs) covering:\n"
            "1. Key market/economic developments\n"
            "2. Policy signals (hawkish/dovish)\n"
            "3. Notable data releases\n"
            "4. Regulatory updates\n"
            "5. Outlook/risks\n\n"
            f"{context}"
        )

        # Try Anthropic Claude
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if api_key:
            try:
                import anthropic

                client = anthropic.Anthropic(api_key=api_key)
                message = client.messages.create(
                    model=self.llm_model,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": prompt}],
                )
                return message.content[0].text
            except Exception as e:
                logger.warning(f"[DailyDigest] Claude API failed: {e}")

        # Fallback to Ollama
        try:
            import httpx

            resp = httpx.post(
                f"{self.ollama_url}/api/generate",
                json={"model": self.ollama_model, "prompt": prompt, "stream": False},
                timeout=60,
            )
            if resp.status_code == 200:
                return resp.json().get("response", "")
        except Exception as e:
            logger.warning(f"[DailyDigest] Ollama failed: {e}")

        # Final fallback: rule-based summary
        return f"Daily briefing for {date.today()}: Collected data from multiple sources. See dashboard for details."

    def _send_telegram(self, summary: str):
        """Send digest via Telegram bot."""
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_ALERT_CHAT_ID")
        if not bot_token or not chat_id:
            return

        try:
            import httpx

            httpx.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": f"📊 *EconScraper Daily Digest*\n\n{summary[:4000]}",
                    "parse_mode": "Markdown",
                },
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"[DailyDigest] Telegram send failed: {e}")
