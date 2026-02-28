"""
Twitter Scraper Module
Uses twikit library for Twitter data extraction
"""

import asyncio
import json
import re
from datetime import datetime
from typing import List, Optional, Dict, Any, AsyncGenerator
from pathlib import Path
import logging

from models import (
    ScrapedContent, ScrapedItem, AuthorInfo, EngagementMetrics,
    MediaItem, MediaType, Platform, ContentType, TwitterSpecific
)

logger = logging.getLogger(__name__)


class TwitterScraper:
    """Twitter scraper using twikit library"""
    
    def __init__(self, cookies_path: Optional[str] = None, language: str = 'en-US'):
        self.cookies_path = cookies_path
        self.language = language
        self.client = None
        self._initialized = False
        
    async def initialize(self):
        """Initialize the twikit client"""
        try:
            from twikit import Client
            self.client = Client(self.language)
            
            if self.cookies_path and Path(self.cookies_path).exists():
                self.client.load_cookies(self.cookies_path)
                logger.info(f"Loaded cookies from {self.cookies_path}")
            else:
                logger.warning("No cookies file found. Some operations may be limited.")
                logger.info("To get full access, login and save cookies first.")
            
            self._initialized = True
        except ImportError:
            logger.error("twikit library not installed. Install with: pip install twikit")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Twitter client: {e}")
            raise
    
    async def login(self, username: str, email: str, password: str, 
                    cookies_output_path: str = "cookies.json"):
        """Login to Twitter and save cookies"""
        if not self._initialized:
            await self.initialize()
        
        try:
            await self.client.login(
                auth_info_1=username,
                auth_info_2=email,
                password=password
            )
            self.client.save_cookies(cookies_output_path)
            logger.info(f"Login successful. Cookies saved to {cookies_output_path}")
        except Exception as e:
            logger.error(f"Login failed: {e}")
            raise
    
    def _parse_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from text"""
        return re.findall(r'#\w+', text)
    
    def _parse_mentions(self, text: str) -> List[str]:
        """Extract mentions from text"""
        return re.findall(r'@\w+', text)
    
    def _parse_urls(self, text: str) -> List[str]:
        """Extract URLs from text"""
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        return re.findall(url_pattern, text)
    
    def _convert_media(self, tweet_media: List[Any]) -> List[MediaItem]:
        """Convert twikit media to our MediaItem format"""
        media_items = []
        
        for media in tweet_media:
            try:
                media_type = MediaType.IMAGE
                if hasattr(media, 'type'):
                    if media.type == 'video':
                        media_type = MediaType.VIDEO
                    elif media.type == 'animated_gif':
                        media_type = MediaType.GIF
                
                media_item = MediaItem(
                    type=media_type,
                    url=getattr(media, 'media_url_https', None) or getattr(media, 'url', None),
                    width=getattr(media, 'sizes', {}).get('large', {}).get('w') if hasattr(media, 'sizes') else None,
                    height=getattr(media, 'sizes', {}).get('large', {}).get('h') if hasattr(media, 'sizes') else None,
                )
                media_items.append(media_item)
            except Exception as e:
                logger.warning(f"Failed to parse media item: {e}")
        
        return media_items
    
    def _tweet_to_scraped_content(self, tweet: Any, search_query: Optional[str] = None,
                                   batch_id: Optional[str] = None) -> ScrapedContent:
        """Convert a twikit tweet to our ScrapedContent model"""
        
        # Get user info
        user = getattr(tweet, 'user', None)
        author = AuthorInfo(
            id=str(getattr(user, 'id', '')) if user else None,
            username=getattr(user, 'screen_name', None) if user else None,
            display_name=getattr(user, 'name', 'Unknown') if user else 'Unknown',
            profile_url=f"https://twitter.com/{getattr(user, 'screen_name', '')}" if user and getattr(user, 'screen_name', None) else None,
            avatar_url=getattr(user, 'profile_image_url_https', None) if user else None,
            verified=getattr(user, 'verified', False) if user else False,
            follower_count=getattr(user, 'followers_count', None) if user else None,
            following_count=getattr(user, 'friends_count', None) if user else None,
            description=getattr(user, 'description', None) if user else None,
            location=getattr(user, 'location', None) if user else None,
        )
        
        # Get engagement metrics
        engagement = EngagementMetrics(
            likes=getattr(tweet, 'favorite_count', 0) or 0,
            replies=getattr(tweet, 'reply_count', 0) or 0,
            reposts=getattr(tweet, 'retweet_count', 0) or 0,
            quotes=getattr(tweet, 'quote_count', 0) or 0,
            bookmarks=getattr(tweet, 'bookmark_count', 0) or 0,
            views=getattr(tweet, 'view_count', None),
        )
        
        # Get media
        extended_entities = getattr(tweet, 'extended_entities', {})
        media_list = extended_entities.get('media', []) if isinstance(extended_entities, dict) else []
        if not media_list and hasattr(tweet, 'media'):
            media_list = tweet.media if isinstance(tweet.media, list) else []
        media_items = self._convert_media(media_list)
        
        # Get text
        text = getattr(tweet, 'full_text', '') or getattr(tweet, 'text', '')
        
        # Parse timestamps
        created_at = getattr(tweet, 'created_at', None)
        if created_at and isinstance(created_at, str):
            try:
                created_at = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
            except (ValueError, TypeError):
                created_at = datetime.utcnow()
        else:
            created_at = datetime.utcnow()
        
        # Determine if reply
        is_reply = bool(getattr(tweet, 'in_reply_to_status_id', None))
        
        return ScrapedContent(
            id=str(getattr(tweet, 'id', '')),
            platform=Platform.TWITTER,
            content_type=ContentType.POST,
            text=text,
            raw_text=text,
            author=author,
            media=media_items,
            urls=self._parse_urls(text),
            hashtags=self._parse_hashtags(text),
            mentions=self._parse_mentions(text),
            engagement=engagement,
            created_at=created_at,
            is_reply=is_reply,
            is_thread=bool(getattr(tweet, 'conversation_id', None)) and is_reply,
            parent_id=str(getattr(tweet, 'in_reply_to_status_id', '')) if getattr(tweet, 'in_reply_to_status_id', None) else None,
            thread_id=str(getattr(tweet, 'conversation_id', '')) if getattr(tweet, 'conversation_id', None) else None,
            reply_to_user=str(getattr(tweet, 'in_reply_to_screen_name', '')) if getattr(tweet, 'in_reply_to_screen_name', None) else None,
            source_url=f"https://twitter.com/{author.username}/status/{getattr(tweet, 'id', '')}" if author.username else None,
            search_query=search_query,
            collection_batch_id=batch_id,
            raw_metadata={
                'source': getattr(tweet, 'source', None),
                'lang': getattr(tweet, 'lang', None),
                'possibly_sensitive': getattr(tweet, 'possibly_sensitive', False),
            }
        )
    
    async def search_tweets(self, query: str, category: str = 'Top', 
                           count: int = 20,
                           search_query: Optional[str] = None,
                           batch_id: Optional[str] = None) -> List[ScrapedItem]:
        """Search for tweets"""
        if not self._initialized:
            await self.initialize()
        
        items = []
        try:
            tweets = await self.client.search_tweet(query, category, count=count)
            
            for tweet in tweets:
                try:
                    content = self._tweet_to_scraped_content(
                        tweet, 
                        search_query=search_query or query,
                        batch_id=batch_id
                    )
                    
                    # Create Twitter-specific data
                    twitter_specific = TwitterSpecific(
                        tweet_id=str(getattr(tweet, 'id', '')),
                        conversation_id=str(getattr(tweet, 'conversation_id', '')) if getattr(tweet, 'conversation_id', None) else None,
                        possibly_sensitive=getattr(tweet, 'possibly_sensitive', False),
                        edit_history_tweet_ids=getattr(tweet, 'edit_history_tweet_ids', []),
                        public_metrics={
                            'retweet_count': getattr(tweet, 'retweet_count', 0),
                            'reply_count': getattr(tweet, 'reply_count', 0),
                            'like_count': getattr(tweet, 'favorite_count', 0),
                            'quote_count': getattr(tweet, 'quote_count', 0),
                        }
                    )
                    
                    items.append(ScrapedItem(
                        unified=content,
                        platform_specific=twitter_specific
                    ))
                    
                except Exception as e:
                    logger.warning(f"Failed to process tweet: {e}")
            
            logger.info(f"Scraped {len(items)} tweets for query: {query}")
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
        
        return items
    
    async def get_user_tweets(self, username: str, count: int = 20,
                              batch_id: Optional[str] = None) -> List[ScrapedItem]:
        """Get tweets from a specific user"""
        if not self._initialized:
            await self.initialize()
        
        items = []
        try:
            user = await self.client.get_user_by_screen_name(username)
            tweets = await user.get_tweets(count=count)
            
            for tweet in tweets:
                try:
                    content = self._tweet_to_scraped_content(
                        tweet,
                        search_query=f"user:{username}",
                        batch_id=batch_id
                    )
                    
                    twitter_specific = TwitterSpecific(
                        tweet_id=str(getattr(tweet, 'id', '')),
                        conversation_id=str(getattr(tweet, 'conversation_id', '')) if getattr(tweet, 'conversation_id', None) else None,
                    )
                    
                    items.append(ScrapedItem(
                        unified=content,
                        platform_specific=twitter_specific
                    ))
                    
                except Exception as e:
                    logger.warning(f"Failed to process tweet: {e}")
            
            logger.info(f"Scraped {len(items)} tweets from user: @{username}")
            
        except Exception as e:
            logger.error(f"Failed to get user tweets: {e}")
            raise
        
        return items
    
    async def get_tweet_by_id(self, tweet_id: str) -> Optional[ScrapedItem]:
        """Get a specific tweet by ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            tweet = await self.client.get_tweet_by_id(tweet_id)
            content = self._tweet_to_scraped_content(tweet)
            
            twitter_specific = TwitterSpecific(
                tweet_id=str(getattr(tweet, 'id', '')),
            )
            
            return ScrapedItem(
                unified=content,
                platform_specific=twitter_specific
            )
            
        except Exception as e:
            logger.error(f"Failed to get tweet {tweet_id}: {e}")
            return None
    
    async def get_trends(self) -> List[Dict[str, Any]]:
        """Get current trending topics"""
        if not self._initialized:
            await self.initialize()
        
        try:
            trends = await self.client.get_trends()
            return [
                {
                    'name': trend.name,
                    'query': trend.query,
                    'tweet_count': getattr(trend, 'tweet_count', None)
                }
                for trend in trends
            ]
        except Exception as e:
            logger.error(f"Failed to get trends: {e}")
            return []
    
    async def stream_search(self, queries: List[str],
                           callback=None,
                           interval: int = 60,
                           batch_id: Optional[str] = None) -> AsyncGenerator[ScrapedItem, None]:
        """Stream search results periodically"""
        seen_ids = set()
        MAX_SEEN_IDS = 10000

        while True:
            for query in queries:
                try:
                    items = await self.search_tweets(query, count=10, batch_id=batch_id)
                    
                    for item in items:
                        if item.unified.id not in seen_ids:
                            seen_ids.add(item.unified.id)
                            # Evict oldest entries to prevent unbounded memory growth
                            if len(seen_ids) > MAX_SEEN_IDS:
                                to_remove = list(seen_ids)[:MAX_SEEN_IDS // 2]
                                seen_ids.difference_update(to_remove)
                            if callback:
                                await callback(item)
                            yield item
                            
                except Exception as e:
                    logger.error(f"Error in stream for query {query}: {e}")
            
            await asyncio.sleep(interval)


# Synchronous wrapper for easier usage
def create_twitter_scraper(cookies_path: Optional[str] = None) -> TwitterScraper:
    """Create a Twitter scraper instance"""
    return TwitterScraper(cookies_path=cookies_path)


async def quick_search(query: str, cookies_path: Optional[str] = None,
                       count: int = 20) -> List[ScrapedItem]:
    """Quick search function"""
    scraper = TwitterScraper(cookies_path=cookies_path)
    await scraper.initialize()
    try:
        return await scraper.search_tweets(query, count=count)
    except Exception:
        raise
