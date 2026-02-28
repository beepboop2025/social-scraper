"""
Configuration Module
Handles configuration loading and environment variables
"""

import os
import json
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass, asdict


@dataclass
class TwitterConfig:
    """Twitter configuration"""
    enabled: bool = True
    cookies_path: Optional[str] = None
    username: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    bearer_token: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    access_token: Optional[str] = None
    access_token_secret: Optional[str] = None
    
    def __post_init__(self):
        if self.cookies_path and not Path(self.cookies_path).exists():
            # Try relative to project
            alt_path = Path(__file__).parent / self.cookies_path
            if alt_path.exists():
                self.cookies_path = str(alt_path)


@dataclass
class TelegramConfig:
    """Telegram configuration"""
    enabled: bool = True
    bot_token: Optional[str] = None
    api_id: Optional[int] = None
    api_hash: Optional[str] = None
    phone_number: Optional[str] = None
    session_name: str = "telegram_scraper"


@dataclass
class ScrapingSettings:
    """General scraping settings"""
    output_directory: str = "./data"
    media_directory: str = "./media"
    output_format: str = "both"  # json, csv, both
    save_media: bool = False
    batch_size: int = 100
    max_items_per_query: Optional[int] = None
    rate_limit_delay: float = 1.0
    include_replies: bool = False
    include_retweets: bool = True
    date_since: Optional[str] = None
    date_until: Optional[str] = None
    language_filter: Optional[List[str]] = None


@dataclass
class NewsSourcesConfig:
    """News sources configuration"""
    telegram_channels: List[str] = None
    twitter_accounts: List[str] = None
    twitter_search_queries: List[str] = None
    keywords: List[str] = None
    
    def __post_init__(self):
        if self.telegram_channels is None:
            self.telegram_channels = []
        if self.twitter_accounts is None:
            self.twitter_accounts = []
        if self.twitter_search_queries is None:
            self.twitter_search_queries = []
        if self.keywords is None:
            self.keywords = []


@dataclass
class Config:
    """Main configuration class"""
    twitter: TwitterConfig = None
    telegram: TelegramConfig = None
    settings: ScrapingSettings = None
    news_sources: NewsSourcesConfig = None
    
    def __post_init__(self):
        if self.twitter is None:
            self.twitter = TwitterConfig()
        if self.telegram is None:
            self.telegram = TelegramConfig()
        if self.settings is None:
            self.settings = ScrapingSettings()
        if self.news_sources is None:
            self.news_sources = NewsSourcesConfig()
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        config = cls()
        
        # Twitter settings
        config.twitter.enabled = os.getenv('TWITTER_ENABLED', 'true').lower() == 'true'
        config.twitter.cookies_path = os.getenv('TWITTER_COOKIES_PATH')
        config.twitter.username = os.getenv('TWITTER_USERNAME')
        config.twitter.email = os.getenv('TWITTER_EMAIL')
        config.twitter.password = os.getenv('TWITTER_PASSWORD')
        config.twitter.bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        config.twitter.api_key = os.getenv('TWITTER_API_KEY')
        config.twitter.api_secret = os.getenv('TWITTER_API_SECRET')
        config.twitter.access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        config.twitter.access_token_secret = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        
        # Telegram settings
        config.telegram.enabled = os.getenv('TELEGRAM_ENABLED', 'true').lower() == 'true'
        config.telegram.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        config.telegram.api_id = int(os.getenv('TELEGRAM_API_ID')) if os.getenv('TELEGRAM_API_ID') else None
        config.telegram.api_hash = os.getenv('TELEGRAM_API_HASH')
        config.telegram.phone_number = os.getenv('TELEGRAM_PHONE_NUMBER')
        config.telegram.session_name = os.getenv('TELEGRAM_SESSION_NAME', 'telegram_scraper')
        
        # Settings
        config.settings.output_directory = os.getenv('OUTPUT_DIRECTORY', './data')
        config.settings.media_directory = os.getenv('MEDIA_DIRECTORY', './media')
        config.settings.output_format = os.getenv('OUTPUT_FORMAT', 'both')
        config.settings.save_media = os.getenv('SAVE_MEDIA', 'false').lower() == 'true'
        config.settings.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        config.settings.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', '1.0'))
        config.settings.include_replies = os.getenv('INCLUDE_REPLIES', 'false').lower() == 'true'
        config.settings.include_retweets = os.getenv('INCLUDE_RETWEETS', 'true').lower() == 'true'
        
        # Parse list env vars
        if os.getenv('LANGUAGE_FILTER'):
            config.settings.language_filter = os.getenv('LANGUAGE_FILTER').split(',')
        
        # News sources
        if os.getenv('TELEGRAM_CHANNELS'):
            config.news_sources.telegram_channels = os.getenv('TELEGRAM_CHANNELS').split(',')
        if os.getenv('TWITTER_ACCOUNTS'):
            config.news_sources.twitter_accounts = os.getenv('TWITTER_ACCOUNTS').split(',')
        if os.getenv('TWITTER_QUERIES'):
            config.news_sources.twitter_search_queries = os.getenv('TWITTER_QUERIES').split(',')
        if os.getenv('NEWS_KEYWORDS'):
            config.news_sources.keywords = os.getenv('NEWS_KEYWORDS').split(',')
        
        return config
    
    @classmethod
    def from_json(cls, filepath: str) -> 'Config':
        """Load configuration from JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return cls(
            twitter=TwitterConfig(**data.get('twitter', {})),
            telegram=TelegramConfig(**data.get('telegram', {})),
            settings=ScrapingSettings(**data.get('settings', {})),
            news_sources=NewsSourcesConfig(**data.get('news_sources', {}))
        )
    
    def to_json(self, filepath: str):
        """Save configuration to JSON file"""
        data = {
            'twitter': asdict(self.twitter),
            'telegram': asdict(self.telegram),
            'settings': asdict(self.settings),
            'news_sources': asdict(self.news_sources)
        }
        
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def create_sample_config(self, filepath: str = "config.json"):
        """Create a sample configuration file"""
        sample = Config(
            twitter=TwitterConfig(
                enabled=True,
                cookies_path="cookies.json",
            ),
            telegram=TelegramConfig(
                enabled=True,
                session_name="telegram_scraper",
            ),
            settings=ScrapingSettings(
                output_directory="./data",
                output_format="both",
                batch_size=100,
            ),
            news_sources=NewsSourcesConfig(
                telegram_channels=[
                    "@bbcnews",
                    "@cnn",
                    "@reuters_world",
                ],
                twitter_search_queries=[
                    "breaking news",
                    "world news",
                    "tech news",
                ],
                keywords=[
                    "breaking",
                    "urgent",
                    "announcement",
                ]
            )
        )
        
        sample.to_json(filepath)
        return filepath


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from file or environment"""
    # Try config file first
    if config_path and Path(config_path).exists():
        return Config.from_json(config_path)
    
    # Try default config file
    if Path('config.json').exists():
        return Config.from_json('config.json')
    
    # Fall back to environment variables
    return Config.from_env()
