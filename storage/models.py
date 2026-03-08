"""SQLAlchemy models for the econscraper platform.

8 tables:
1. economic_data   — structured time-series (TimescaleDB hypertable)
2. articles        — unstructured news/circulars
3. article_embeddings — pgvector embeddings for RAG
4. sentiment_scores — financial sentiment analysis
5. entities        — NER results
6. article_topics  — topic classification
7. daily_digests   — LLM-generated summaries
8. collection_logs — audit trail for all collection runs
"""

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Enum as SAEnum, Float,
    ForeignKey, Index, Integer, Numeric, String, Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from api.database import Base


class EconomicData(Base):
    """Structured economic time-series data.

    TimescaleDB hypertable on `date` column.
    Sources: FRED, RBI DBIE, CCIL, NSE, data.gov.in, World Bank, IMF.
    """
    __tablename__ = "economic_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False)
    indicator = Column(String(128), nullable=False)
    date = Column(DateTime(timezone=True), nullable=False)
    value = Column(Numeric, nullable=True)
    unit = Column(String(32), default="")
    extra_data = Column("metadata", JSONB, default=dict)
    collected_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    raw_path = Column(Text, nullable=True)

    __table_args__ = (
        Index("idx_econ_source_indicator_date", "source", "indicator", "date", unique=True),
        Index("idx_econ_date", "date"),
        Index("idx_econ_source", "source"),
    )


class Article(Base):
    """Unstructured articles from RSS, circulars, Telegram, Twitter."""
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False)
    source_type = Column(String(32), nullable=False)  # rss, circular, telegram, twitter
    url = Column(Text, nullable=True)
    url_hash = Column(String(64), unique=True, nullable=True)
    title = Column(Text, nullable=True)
    author = Column(String(256), nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=True)
    collected_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    full_text = Column(Text, nullable=True)
    raw_path = Column(Text, nullable=True)
    category = Column(String(64), nullable=True)
    is_processed = Column(Boolean, default=False)

    # Relationships
    embeddings = relationship("ArticleEmbedding", back_populates="article", cascade="all, delete-orphan")
    sentiments = relationship("SentimentScore", back_populates="article", cascade="all, delete-orphan")
    entities = relationship("Entity", back_populates="article", cascade="all, delete-orphan")
    topics = relationship("ArticleTopic", back_populates="article", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_article_url_hash", "url_hash"),
        Index("idx_article_published", "published_at"),
        Index("idx_article_source", "source"),
        Index("idx_article_processed", "is_processed"),
        Index("idx_article_category", "category"),
    )


class ArticleEmbedding(Base):
    """Vector embeddings for semantic search (pgvector)."""
    __tablename__ = "article_embeddings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False)
    # pgvector column — created via raw SQL in migration since SQLAlchemy
    # doesn't natively support the vector type. Store as JSON fallback.
    embedding_json = Column(JSONB, nullable=True)  # list[float] of dimension 384
    model_name = Column(String(64), default="all-MiniLM-L6-v2")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    article = relationship("Article", back_populates="embeddings")


class SentimentScore(Base):
    """Financial sentiment analysis results."""
    __tablename__ = "sentiment_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False)
    overall = Column(Float, nullable=False)
    sector_scores = Column(JSONB, default=dict)
    policy_direction = Column(String(16), default="neutral")  # hawkish/dovish/neutral
    model_name = Column(String(64), default="finbert")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    article = relationship("Article", back_populates="sentiments")


class Entity(Base):
    """Named entities extracted from articles."""
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False)
    entity_type = Column(String(32), nullable=False)  # ORG, PERSON, MONEY, POLICY, etc.
    entity_value = Column(Text, nullable=False)
    confidence = Column(Float, default=1.0)

    article = relationship("Article", back_populates="entities")

    __table_args__ = (
        Index("idx_entity_type", "entity_type"),
        Index("idx_entity_value", "entity_value"),
    )


class ArticleTopic(Base):
    """Topic classification for articles."""
    __tablename__ = "article_topics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False)
    topic = Column(String(64), nullable=False)
    confidence = Column(Float, default=1.0)

    article = relationship("Article", back_populates="topics")

    __table_args__ = (
        Index("idx_topic_name", "topic"),
    )


class DailyDigest(Base):
    """LLM-generated daily summaries."""
    __tablename__ = "daily_digests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, unique=True, nullable=False)
    summary = Column(Text, nullable=True)
    top_themes = Column(JSONB, default=list)
    sentiment_summary = Column(JSONB, default=dict)
    key_data_releases = Column(JSONB, default=list)
    new_circulars = Column(JSONB, default=list)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class CollectionLog(Base):
    """Audit log for every collection run."""
    __tablename__ = "collection_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False)
    status = Column(String(16), nullable=False)  # success, failed, partial
    records_collected = Column(Integer, default=0)
    duration_seconds = Column(Float, default=0)
    error_message = Column(Text, nullable=True)
    run_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("idx_log_source", "source"),
        Index("idx_log_status", "status"),
        Index("idx_log_run_at", "run_at"),
    )
