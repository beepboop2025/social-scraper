"""
Data Models for Social Media Scraper
Defines structured data schemas for Twitter and Telegram content
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from pydantic import BaseModel, Field, HttpUrl


class Platform(str, Enum):
    """Supported social media platforms"""
    TWITTER = "twitter"
    TELEGRAM = "telegram"
    REDDIT = "reddit"
    DISCORD = "discord"
    YOUTUBE = "youtube"
    HACKERNEWS = "hackernews"
    MASTODON = "mastodon"
    GITHUB = "github"
    RSS = "rss"
    WEB = "web"
    DARKWEB = "darkweb"
    SEC_EDGAR = "sec_edgar"
    CENTRAL_BANK = "central_bank"


class ContentType(str, Enum):
    """Type of content being scraped"""
    POST = "post"
    COMMENT = "comment"
    REPLY = "reply"
    FORWARD = "forward"
    MEDIA = "media"
    ARTICLE = "article"
    FILING = "filing"
    ANNOUNCEMENT = "announcement"
    TRANSCRIPT = "transcript"
    THREAT_INTEL = "threat_intel"
    ISSUE = "issue"
    DISCUSSION = "discussion"
    RELEASE = "release"


class MediaType(str, Enum):
    """Types of media attachments"""
    IMAGE = "image"
    VIDEO = "video"
    GIF = "gif"
    AUDIO = "audio"
    DOCUMENT = "document"
    LINK = "link"
    POLL = "poll"


class MediaItem(BaseModel):
    """Media attachment item"""
    type: MediaType
    url: Optional[str] = None
    local_path: Optional[str] = None
    filename: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration_seconds: Optional[float] = None
    thumbnail_url: Optional[str] = None


class EngagementMetrics(BaseModel):
    """Engagement statistics for a post"""
    likes: int = 0
    replies: int = 0
    reposts: int = 0
    quotes: int = 0
    bookmarks: int = 0
    views: Optional[int] = None
    shares: Optional[int] = None
    forwards: Optional[int] = None


class AuthorInfo(BaseModel):
    """Author/Creator information"""
    id: Optional[str] = None
    username: Optional[str] = None
    display_name: str
    profile_url: Optional[str] = None
    avatar_url: Optional[str] = None
    verified: bool = False
    follower_count: Optional[int] = None
    following_count: Optional[int] = None
    description: Optional[str] = None
    location: Optional[str] = None
    created_at: Optional[datetime] = None


class ScrapedContent(BaseModel):
    """Main content model - unified structure for all platforms"""
    # Identification
    id: str
    platform: Platform
    content_type: ContentType
    
    # Content
    text: str
    raw_text: Optional[str] = None
    language: Optional[str] = None
    
    # Author/Source
    author: AuthorInfo
    
    # Media
    media: List[MediaItem] = Field(default_factory=list)
    urls: List[str] = Field(default_factory=list)
    hashtags: List[str] = Field(default_factory=list)
    mentions: List[str] = Field(default_factory=list)
    
    # Engagement
    engagement: EngagementMetrics = Field(default_factory=EngagementMetrics)
    
    # Timestamps
    created_at: datetime
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    edited_at: Optional[datetime] = None
    
    # Thread/Reply info
    is_reply: bool = False
    is_thread: bool = False
    parent_id: Optional[str] = None
    thread_id: Optional[str] = None
    reply_to_user: Optional[str] = None
    
    # Source info
    source_url: Optional[str] = None
    source_channel: Optional[str] = None
    
    # Metadata
    raw_metadata: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    
    # Search/Query context
    search_query: Optional[str] = None
    collection_batch_id: Optional[str] = None


class TwitterSpecific(BaseModel):
    """Twitter-specific fields"""
    tweet_id: str
    conversation_id: Optional[str] = None
    reply_settings: Optional[str] = None
    source: Optional[str] = None
    possibly_sensitive: bool = False
    edit_history_tweet_ids: List[str] = Field(default_factory=list)
    referenced_tweets: List[Dict[str, str]] = Field(default_factory=list)
    public_metrics: Dict[str, int] = Field(default_factory=dict)
    context_annotations: List[Dict[str, Any]] = Field(default_factory=list)
    entities: Dict[str, Any] = Field(default_factory=dict)
    geo: Optional[Dict[str, Any]] = None


class TelegramSpecific(BaseModel):
    """Telegram-specific fields"""
    message_id: int
    channel_id: Optional[int] = None
    channel_title: Optional[str] = None
    forward_from_chat: Optional[str] = None
    forward_from_message_id: Optional[int] = None
    forward_date: Optional[datetime] = None
    is_automatic_forward: bool = False
    has_protected_content: bool = False
    reply_to_message_id: Optional[int] = None
    via_bot: Optional[str] = None
    edit_date: Optional[datetime] = None
    media_group_id: Optional[str] = None
    caption: Optional[str] = None
    caption_entities: List[Dict[str, Any]] = Field(default_factory=list)
    contact: Optional[Dict[str, Any]] = None
    location: Optional[Dict[str, Any]] = None
    venue: Optional[Dict[str, Any]] = None
    poll: Optional[Dict[str, Any]] = None
    dice: Optional[Dict[str, Any]] = None


class ScrapedItem(BaseModel):
    """Wrapper that combines unified content with platform-specific data"""
    unified: ScrapedContent
    platform_specific: Optional[Union[TwitterSpecific, TelegramSpecific]] = None


class NewsEvent(BaseModel):
    """News event aggregation model"""
    event_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    sources: List[Platform] = Field(default_factory=list)
    items: List[ScrapedItem] = Field(default_factory=list)
    first_seen_at: datetime
    last_updated_at: datetime
    location: Optional[str] = None
    sentiment: Optional[str] = None
    importance_score: Optional[float] = None
    category: Optional[str] = None


class DestinationTag(str, Enum):
    """Which downstream app should receive this data"""
    DRAGONSCOPE = "dragonscope"
    LIQUIFI = "liquifi"
    BOTH = "both"
    NONE = "none"


class ThreatLevel(str, Enum):
    """Threat level for dark web intelligence"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DarkWebContent(BaseModel):
    """Dark web specific content metadata"""
    onion_url: Optional[str] = None
    surface_mirror: Optional[str] = None
    marketplace: Optional[str] = None
    threat_level: ThreatLevel = ThreatLevel.LOW
    threat_categories: List[str] = Field(default_factory=list)
    financial_relevance: float = 0.0
    actors: List[str] = Field(default_factory=list)
    iocs: List[str] = Field(default_factory=list)  # indicators of compromise
    leak_type: Optional[str] = None


class FinancialSignal(BaseModel):
    """Financial signal extracted from social content"""
    tickers: List[str] = Field(default_factory=list)
    sentiment_score: float = 0.0
    price_mentions: List[Dict[str, Any]] = Field(default_factory=list)
    earnings_related: bool = False
    regulatory_related: bool = False
    macro_related: bool = False
    treasury_related: bool = False
    destination: DestinationTag = DestinationTag.NONE
    signal_strength: float = 0.0  # 0-1
    asset_classes: List[str] = Field(default_factory=list)


class ScrapingConfig(BaseModel):
    """Configuration for scraping operations"""
    # Platform settings
    twitter_enabled: bool = True
    telegram_enabled: bool = True
    reddit_enabled: bool = True
    discord_enabled: bool = False
    youtube_enabled: bool = True
    hackernews_enabled: bool = True
    mastodon_enabled: bool = True
    github_enabled: bool = True
    rss_enabled: bool = True
    web_enabled: bool = True
    darkweb_enabled: bool = False
    sec_enabled: bool = True
    centralbank_enabled: bool = True

    # Rate limiting
    twitter_delay_seconds: float = 1.0
    telegram_delay_seconds: float = 1.0
    max_requests_per_minute: int = 30
    
    # Content filtering
    min_engagement_score: int = 0
    include_replies: bool = False
    include_retweets: bool = True
    include_forwards: bool = True
    language_filter: Optional[List[str]] = None
    date_since: Optional[datetime] = None
    date_until: Optional[datetime] = None
    
    # Storage
    output_format: str = "json"  # json, csv, both
    output_directory: str = "./data"
    save_media: bool = False
    media_directory: str = "./media"
    
    # Batch settings
    batch_size: int = 100
    max_items_per_query: Optional[int] = None


class ScrapingResult(BaseModel):
    """Result of a scraping operation"""
    batch_id: str
    platform: Platform
    query: str
    items_scraped: int
    items_failed: int = 0
    start_time: datetime
    end_time: Optional[datetime] = None
    items: List[ScrapedItem] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
