"""Scrapers package — all platform-specific scrapers."""

from scrapers.base import BaseScraper
from scrapers.reddit_scraper import RedditScraper
from scrapers.discord_scraper import DiscordScraper
from scrapers.youtube_scraper import YouTubeScraper
from scrapers.hackernews_scraper import HackerNewsScraper
from scrapers.rss_scraper import RSSScraper
from scrapers.web_scraper import WebScraper
from scrapers.darkweb_scraper import DarkWebScraper
from scrapers.mastodon_scraper import MastodonScraper
from scrapers.github_scraper import GitHubScraper
from scrapers.sec_scraper import SECScraper
from scrapers.centralbank_scraper import CentralBankScraper

__all__ = [
    "BaseScraper",
    "RedditScraper",
    "DiscordScraper",
    "YouTubeScraper",
    "HackerNewsScraper",
    "RSSScraper",
    "WebScraper",
    "DarkWebScraper",
    "MastodonScraper",
    "GitHubScraper",
    "SECScraper",
    "CentralBankScraper",
]
