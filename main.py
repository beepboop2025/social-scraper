"""
Social Media Scraper - Main Entry Point
Unified scraper for Twitter and Telegram with structured data output
"""

import asyncio
import argparse
import logging
import sys
import uuid
from datetime import datetime
from typing import List, Optional

from config import load_config, Config
from models import ScrapedItem, ScrapingResult, Platform
from twitter_scraper import TwitterScraper
from telegram_scraper import TelegramScraper
from storage import StorageManager, NewsAggregator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SocialMediaScraper:
    """Main scraper class that coordinates Twitter and Telegram scrapers"""
    
    def __init__(self, config: Config):
        self.config = config
        self.twitter_scraper: Optional[TwitterScraper] = None
        self.telegram_scraper: Optional[TelegramScraper] = None
        self.storage = StorageManager(
            output_dir=config.settings.output_directory,
            media_dir=config.settings.media_directory
        )
        self.news_aggregator = NewsAggregator(self.storage)
        self._initialized = False
    
    async def initialize(self):
        """Initialize all enabled scrapers"""
        if self.config.twitter.enabled:
            try:
                self.twitter_scraper = TwitterScraper(
                    cookies_path=self.config.twitter.cookies_path
                )
                await self.twitter_scraper.initialize()
                logger.info("Twitter scraper initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Twitter scraper: {e}")
                self.twitter_scraper = None
        
        if self.config.telegram.enabled:
            try:
                self.telegram_scraper = TelegramScraper(
                    bot_token=self.config.telegram.bot_token,
                    api_id=self.config.telegram.api_id,
                    api_hash=self.config.telegram.api_hash,
                    phone_number=self.config.telegram.phone_number,
                    session_name=self.config.telegram.session_name
                )
                await self.telegram_scraper.initialize()
                logger.info("Telegram scraper initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Telegram scraper: {e}")
                self.telegram_scraper = None
        
        self._initialized = True
    
    async def close(self):
        """Close all connections"""
        if self.telegram_scraper:
            await self.telegram_scraper.close()
            logger.info("Telegram scraper closed")
    
    async def scrape_twitter(self, queries: List[str], 
                            count: int = 20,
                            batch_id: Optional[str] = None) -> ScrapingResult:
        """Scrape Twitter for given queries"""
        if not self.twitter_scraper:
            raise RuntimeError("Twitter scraper not initialized")
        
        batch_id = batch_id or str(uuid.uuid4())[:8]
        result = ScrapingResult(
            batch_id=batch_id,
            platform=Platform.TWITTER,
            query="; ".join(queries),
            items_scraped=0,
            start_time=datetime.utcnow()
        )
        
        all_items = []
        for query in queries:
            try:
                logger.info(f"Searching Twitter for: {query}")
                items = await self.twitter_scraper.search_tweets(
                    query=query,
                    count=count,
                    search_query=query,
                    batch_id=batch_id
                )
                all_items.extend(items)
                
                # Rate limiting
                await asyncio.sleep(self.config.settings.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error scraping Twitter for '{query}': {e}")
                result.errors.append(f"{query}: {str(e)}")
        
        result.items = all_items
        result.items_scraped = len(all_items)
        result.end_time = datetime.utcnow()
        
        # Save results
        self.storage.save_result(
            result,
            save_json=self.config.settings.output_format in ('json', 'both'),
            save_csv=self.config.settings.output_format in ('csv', 'both')
        )
        
        return result
    
    async def scrape_telegram_channels(self, channels: List[str],
                                       limit: int = 50,
                                       keywords: Optional[List[str]] = None,
                                       batch_id: Optional[str] = None) -> ScrapingResult:
        """Scrape Telegram channels"""
        if not self.telegram_scraper:
            raise RuntimeError("Telegram scraper not initialized")
        
        batch_id = batch_id or str(uuid.uuid4())[:8]
        result = ScrapingResult(
            batch_id=batch_id,
            platform=Platform.TELEGRAM,
            query="; ".join(channels),
            items_scraped=0,
            start_time=datetime.utcnow()
        )
        
        try:
            logger.info(f"Scraping Telegram channels: {channels}")
            items = await self.telegram_scraper.get_news_from_channels(
                channels=channels,
                keywords=keywords,
                limit_per_channel=limit,
                batch_id=batch_id
            )
            
            result.items = items
            result.items_scraped = len(items)
            
        except Exception as e:
            logger.error(f"Error scraping Telegram: {e}")
            result.errors.append(str(e))
        
        result.end_time = datetime.utcnow()
        
        # Save results
        self.storage.save_result(
            result,
            save_json=self.config.settings.output_format in ('json', 'both'),
            save_csv=self.config.settings.output_format in ('csv', 'both')
        )
        
        return result
    
    async def scrape_news(self, batch_id: Optional[str] = None) -> List[ScrapingResult]:
        """Scrape news from configured sources"""
        results = []
        batch_id = batch_id or str(uuid.uuid4())[:8]
        
        # Twitter news
        if self.twitter_scraper and self.config.news_sources.twitter_search_queries:
            result = await self.scrape_twitter(
                queries=self.config.news_sources.twitter_search_queries,
                count=50,
                batch_id=f"{batch_id}_twitter"
            )
            results.append(result)
        
        # Telegram news
        if self.telegram_scraper and self.config.news_sources.telegram_channels:
            result = await self.scrape_telegram_channels(
                channels=self.config.news_sources.telegram_channels,
                limit=50,
                keywords=self.config.news_sources.keywords,
                batch_id=f"{batch_id}_telegram"
            )
            results.append(result)
        
        # Create combined report
        all_items = []
        for result in results:
            all_items.extend(result.items)
        
        if all_items:
            self.news_aggregator.export_news_report(
                all_items,
                report_name=f"news_report_{batch_id}"
            )
        
        return results
    
    async def stream_news(self, interval: int = 300):
        """Stream news continuously from configured sources"""
        batch_id = str(uuid.uuid4())[:8]
        
        logger.info(f"Starting news stream with {interval}s interval")
        
        # Create combined stream
        streams = []
        
        if self.twitter_scraper and self.config.news_sources.twitter_search_queries:
            streams.append(self.twitter_scraper.stream_search(
                queries=self.config.news_sources.twitter_search_queries,
                interval=interval,
                batch_id=batch_id
            ))
        
        if self.telegram_scraper and self.config.news_sources.telegram_channels:
            streams.append(self.telegram_scraper.stream_channel_updates(
                channels=self.config.news_sources.telegram_channels,
                interval=interval,
                batch_id=batch_id
            ))
        
        # Merge streams
        items_buffer = []
        
        async def process_item(item: ScrapedItem):
            """Process a single streamed item"""
            text_preview = item.unified.text[:100] if item.unified.text else ""
            logger.info(f"New item from {item.unified.platform.value}: {text_preview}...")
            items_buffer.append(item)

            # Flush buffer periodically
            if len(items_buffer) >= 10:
                output_path = f"{self.config.settings.output_directory}/stream_{batch_id}.jsonl"
                for buffered_item in items_buffer:
                    self.storage.append_jsonl(buffered_item, output_path)
                items_buffer.clear()
        
        try:
            # Run streams concurrently
            await asyncio.gather(*[
                self._consume_stream(stream, process_item)
                for stream in streams
            ])
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Stream stopped by user")
        finally:
            # Flush remaining buffered items
            if items_buffer:
                output_path = f"{self.config.settings.output_directory}/stream_{batch_id}.jsonl"
                for buffered_item in items_buffer:
                    self.storage.append_jsonl(buffered_item, output_path)
                items_buffer.clear()
                logger.info("Flushed remaining stream buffer")
    
    async def _consume_stream(self, stream, callback):
        """Consume items from a stream"""
        async for item in stream:
            await callback(item)


async def interactive_login(scraper: SocialMediaScraper, platform: str):
    """Interactive login for a platform"""
    if platform == 'twitter':
        if not scraper.twitter_scraper:
            print("Twitter scraper not available")
            return
        
        print("\n=== Twitter Login ===")
        username = input("Username: ")
        email = input("Email: ")
        password = input("Password: ")
        
        try:
            await scraper.twitter_scraper.login(username, email, password)
            print("✓ Login successful! Cookies saved.")
        except Exception as e:
            print(f"✗ Login failed: {e}")
    
    elif platform == 'telegram':
        if not scraper.telegram_scraper:
            print("Telegram scraper not available")
            return
        
        print("\n=== Telegram Login ===")
        print("Telegram will send a code to your phone/app")
        print("Make sure you have api_id and api_hash configured")


def print_results(results: List[ScrapingResult]):
    """Print scraping results in a readable format"""
    print("\n" + "="*60)
    print("SCRAPING RESULTS")
    print("="*60)
    
    for result in results:
        print(f"\nPlatform: {result.platform.value.upper()}")
        print(f"Query: {result.query}")
        print(f"Items scraped: {result.items_scraped}")
        print(f"Duration: {result.duration_seconds:.2f}s")
        
        if result.errors:
            print(f"Errors: {len(result.errors)}")
            for error in result.errors[:3]:
                print(f"  - {error}")
        
        # Sample items
        if result.items:
            print("\nSample items:")
            for item in result.items[:3]:
                text = item.unified.text[:80].replace('\n', ' ')
                author = item.unified.author.username or item.unified.author.display_name
                print(f"  [@{author}] {text}...")
    
    print("\n" + "="*60)


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Social Media Scraper - Twitter and Telegram"
    )
    parser.add_argument(
        '--config', '-c',
        help='Path to config file',
        default=None
    )
    parser.add_argument(
        '--init-config',
        action='store_true',
        help='Create a sample configuration file'
    )
    parser.add_argument(
        '--login',
        choices=['twitter', 'telegram'],
        help='Interactive login for a platform'
    )
    parser.add_argument(
        '--scrape',
        choices=['twitter', 'telegram', 'news', 'all'],
        help='What to scrape'
    )
    parser.add_argument(
        '--query', '-q',
        help='Search query (for Twitter)'
    )
    parser.add_argument(
        '--channel', '-ch',
        help='Telegram channel name'
    )
    parser.add_argument(
        '--count', '-n',
        type=int,
        default=20,
        help='Number of items to scrape (default: 20)'
    )
    parser.add_argument(
        '--stream',
        action='store_true',
        help='Stream mode - continuous scraping'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output directory',
        default='./data'
    )
    parser.add_argument(
        '--format', '-f',
        choices=['json', 'csv', 'both'],
        default='both',
        help='Output format'
    )
    
    args = parser.parse_args()
    
    # Initialize config
    if args.init_config:
        config = Config()
        path = config.create_sample_config()
        print(f"Sample config created: {path}")
        print("Edit this file with your credentials and settings")
        return
    
    config = load_config(args.config)
    
    # Override with command line args (only if explicitly provided, not defaults)
    if '--output' in sys.argv or '-o' in sys.argv:
        config.settings.output_directory = args.output
    if '--format' in sys.argv or '-f' in sys.argv:
        config.settings.output_format = args.format
    
    # Create scraper
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        # Handle login
        if args.login:
            await interactive_login(scraper, args.login)
            return
        
        # Handle scraping
        if args.stream:
            await scraper.stream_news()

        elif args.scrape == 'all':
            # Scrape everything: Twitter + Telegram + news
            results = []
            if args.query or config.news_sources.twitter_search_queries:
                queries = [args.query] if args.query else config.news_sources.twitter_search_queries
                result = await scraper.scrape_twitter(queries, count=args.count)
                results.append(result)
            if args.channel or config.news_sources.telegram_channels:
                channels = [args.channel] if args.channel else config.news_sources.telegram_channels
                result = await scraper.scrape_telegram_channels(
                    channels, limit=args.count,
                    keywords=config.news_sources.keywords
                )
                results.append(result)
            if not results:
                results = await scraper.scrape_news()
            print_results(results)

        elif args.scrape == 'twitter':
            queries = [args.query] if args.query else config.news_sources.twitter_search_queries
            if not queries:
                print("No queries specified. Use --query or configure in config file")
                return
            result = await scraper.scrape_twitter(queries, count=args.count)
            print_results([result])

        elif args.scrape == 'telegram':
            channels = [args.channel] if args.channel else config.news_sources.telegram_channels
            if not channels:
                print("No channels specified. Use --channel or configure in config file")
                return
            result = await scraper.scrape_telegram_channels(
                channels, limit=args.count,
                keywords=config.news_sources.keywords
            )
            print_results([result])

        elif args.scrape == 'news':
            results = await scraper.scrape_news()
            print_results(results)

        else:
            parser.print_help()
    
    finally:
        await scraper.close()


# For programmatic usage
async def quick_scrape_twitter(queries: List[str], 
                               cookies_path: Optional[str] = None,
                               count: int = 20) -> List[ScrapedItem]:
    """Quick scrape Twitter without configuration"""
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = cookies_path
    config.telegram.enabled = False
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        result = await scraper.scrape_twitter(queries, count=count)
        return result.items
    finally:
        await scraper.close()


async def quick_scrape_telegram(channels: List[str],
                                api_id: int,
                                api_hash: str,
                                limit: int = 50) -> List[ScrapedItem]:
    """Quick scrape Telegram without configuration"""
    config = Config()
    config.twitter.enabled = False
    config.telegram.enabled = True
    config.telegram.api_id = api_id
    config.telegram.api_hash = api_hash
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        result = await scraper.scrape_telegram_channels(channels, limit=limit)
        return result.items
    finally:
        await scraper.close()


if __name__ == '__main__':
    asyncio.run(main())
