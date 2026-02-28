"""SQLAlchemy ORM models for social scraper platform."""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Float, Boolean,
    ForeignKey, JSON, Index, Enum as SAEnum,
)
from sqlalchemy.orm import relationship
from api.database import Base


class ScrapedPost(Base):
    __tablename__ = "scraped_posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(32), nullable=False, index=True)
    platform_id = Column(String(128), nullable=False)
    content_type = Column(String(32), default="post")
    text = Column(Text, default="")
    raw_text = Column(Text)
    language = Column(String(8))

    # Author
    author_id = Column(String(128))
    author_username = Column(String(128))
    author_display_name = Column(String(256))
    author_verified = Column(Boolean, default=False)
    author_followers = Column(Integer)

    # Engagement
    likes = Column(Integer, default=0)
    replies = Column(Integer, default=0)
    reposts = Column(Integer, default=0)
    views = Column(Integer)
    bookmarks = Column(Integer, default=0)

    # Entities
    hashtags = Column(JSON, default=list)
    mentions = Column(JSON, default=list)
    urls = Column(JSON, default=list)
    media = Column(JSON, default=list)

    # Thread
    is_reply = Column(Boolean, default=False)
    parent_id = Column(String(128))
    source_url = Column(String(512))
    source_channel = Column(String(256))

    # Metadata
    search_query = Column(String(512))
    batch_id = Column(String(64), index=True)
    raw_metadata = Column(JSON, default=dict)

    # Timestamps
    created_at = Column(DateTime, nullable=False)
    scraped_at = Column(DateTime, default=datetime.utcnow)

    # Full-text search support
    __table_args__ = (
        Index("idx_posts_platform_id", "platform", "platform_id", unique=True),
        Index("idx_posts_created_at", "created_at"),
        Index("idx_posts_author", "author_username"),
    )

    # Relationship
    analysis = relationship("AnalysisResult", back_populates="post", cascade="all, delete-orphan")


class ScrapedProfile(Base):
    __tablename__ = "scraped_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(32), nullable=False)
    platform_user_id = Column(String(128), nullable=False)
    username = Column(String(128))
    display_name = Column(String(256))
    bio = Column(Text)
    follower_count = Column(Integer)
    following_count = Column(Integer)
    verified = Column(Boolean, default=False)
    location = Column(String(256))
    profile_url = Column(String(512))
    avatar_url = Column(String(512))
    raw_metadata = Column(JSON, default=dict)
    scraped_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_profiles_platform_user", "platform", "platform_user_id", unique=True),
    )


class AnalysisResult(Base):
    __tablename__ = "analysis_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey("scraped_posts.id", ondelete="CASCADE"), nullable=False)
    analysis_type = Column(String(32), nullable=False)  # sentiment, topic, entity, summary
    result = Column(JSON, nullable=False)
    confidence = Column(Float)
    model_version = Column(String(64))
    created_at = Column(DateTime, default=datetime.utcnow)

    post = relationship("ScrapedPost", back_populates="analysis")

    __table_args__ = (
        Index("idx_analysis_type", "analysis_type"),
    )


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), unique=True, nullable=False)
    platform = Column(String(32), nullable=False)
    query = Column(String(512))
    channel = Column(String(256))
    status = Column(String(32), default="pending")  # pending, running, completed, failed
    items_scraped = Column(Integer, default=0)
    items_failed = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_jobs_status", "status"),
    )
