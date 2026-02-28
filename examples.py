"""
Social Media Scraper - Usage Examples
=====================================

This file contains various examples of how to use the scraper.
Run individual examples or import functions for your own use.
"""

import asyncio
from datetime import datetime
from typing import List

# Import the scraper modules
from main import SocialMediaScraper, quick_scrape_twitter, quick_scrape_telegram
from config import Config, load_config
from storage import StorageManager, NewsAggregator


# ==================== EXAMPLE 1: Quick Twitter Search ====================

async def example_twitter_search():
    """Example: Search Twitter for specific queries"""
    print("\n=== Example 1: Twitter Search ===\n")
    
    # Create config with cookies
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"  # Path to your cookies file
    config.telegram.enabled = False
    config.settings.output_format = "both"
    
    # Create scraper
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        # Search for multiple queries
        queries = ["breaking news", "tech news", "stock market"]
        result = await scraper.scrape_twitter(queries, count=10)
        
        print(f"Scraped {result.items_scraped} tweets")
        print(f"Saved to: {scraper.storage.output_dir}")
        
        # Print sample results
        for item in result.items[:3]:
            print(f"\n[@{item.unified.author.username}]")
            print(f"Text: {item.unified.text[:100]}...")
            print(f"Likes: {item.unified.engagement.likes}")
    
    finally:
        await scraper.close()


# ==================== EXAMPLE 2: Telegram News Channels ====================

async def example_telegram_channels():
    """Example: Scrape news from Telegram channels"""
    print("\n=== Example 2: Telegram Channels ===\n")
    
    # You need to get api_id and api_hash from https://my.telegram.org
    config = Config()
    config.twitter.enabled = False
    config.telegram.enabled = True
    config.telegram.api_id = 12345678  # Replace with your API ID
    config.telegram.api_hash = "your_api_hash"  # Replace with your API hash
    config.telegram.phone_number = "+1234567890"  # Your phone number
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        # Scrape from news channels
        channels = ["@bbcnews", "@cnn", "@reuters_world"]
        keywords = ["breaking", "urgent", "update"]
        
        result = await scraper.scrape_telegram_channels(
            channels=channels,
            limit=20,
            keywords=keywords
        )
        
        print(f"Scraped {result.items_scraped} messages")
        
        # Print sample results
        for item in result.items[:3]:
            print(f"\n[{item.unified.source_channel}]")
            print(f"Text: {item.unified.text[:100]}...")
            print(f"Views: {item.unified.engagement.views}")
    
    finally:
        await scraper.close()


# ==================== EXAMPLE 3: Combined News Scraping ====================

async def example_combined_news():
    """Example: Scrape news from both Twitter and Telegram"""
    print("\n=== Example 3: Combined News Scraping ===\n")
    
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"
    config.telegram.enabled = True
    config.telegram.api_id = 12345678
    config.telegram.api_hash = "your_api_hash"
    
    # Configure news sources
    config.news_sources.twitter_search_queries = [
        "breaking news",
        "world news",
        "technology"
    ]
    config.news_sources.telegram_channels = [
        "@bbcnews",
        "@TechCrunch"
    ]
    config.news_sources.keywords = ["breaking", "urgent"]
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        # Scrape news from all configured sources
        results = await scraper.scrape_news()
        
        total_items = sum(r.items_scraped for r in results)
        print(f"Total items scraped: {total_items}")
        
        # Get statistics
        all_items = []
        for result in results:
            all_items.extend(result.items)
        
        stats = scraper.storage.get_statistics(all_items)
        print(f"\nStatistics:")
        print(f"  Platforms: {stats.get('platforms', {})}")
        print(f"  Unique authors: {stats.get('unique_authors', 0)}")
        print(f"  Top hashtags: {stats.get('top_hashtags', [])[:5]}")
    
    finally:
        await scraper.close()


# ==================== EXAMPLE 4: Stream Continuous Updates ====================

async def example_streaming():
    """Example: Stream continuous updates from sources"""
    print("\n=== Example 4: Streaming Updates ===\n")
    
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"
    
    config.news_sources.twitter_search_queries = [
        "breaking news",
        "stock market"
    ]
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    print("Streaming for 60 seconds (press Ctrl+C to stop)...")
    
    try:
        # Stream for 60 seconds
        import signal
        
        stop_event = asyncio.Event()
        
        def signal_handler():
            stop_event.set()
        
        # Set up signal handler
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
        
        # Run stream with timeout
        await asyncio.wait_for(
            scraper.stream_news(interval=30),
            timeout=60
        )
        
    except asyncio.TimeoutError:
        print("\nStreaming completed (60s timeout)")
    except KeyboardInterrupt:
        print("\nStreaming stopped by user")
    finally:
        await scraper.close()


# ==================== EXAMPLE 5: Custom Processing ====================

async def example_custom_processing():
    """Example: Process scraped data with custom logic"""
    print("\n=== Example 5: Custom Processing ===\n")
    
    # Scrape some data
    items = await quick_scrape_twitter(
        queries=["AI", "machine learning"],
        cookies_path="cookies.json",
        count=20
    )
    
    print(f"Scraped {len(items)} items")
    
    # Custom processing
    high_engagement = [
        item for item in items
        if item.unified.engagement.likes > 100
    ]
    print(f"High engagement items (>100 likes): {len(high_engagement)}")
    
    # Group by hashtag
    from collections import defaultdict
    by_hashtag = defaultdict(list)
    
    for item in items:
        for hashtag in item.unified.hashtags:
            by_hashtag[hashtag].append(item)
    
    print("\nItems by hashtag:")
    for hashtag, hashtag_items in sorted(by_hashtag.items(), 
                                          key=lambda x: len(x[1]), 
                                          reverse=True)[:5]:
        print(f"  {hashtag}: {len(hashtag_items)} items")
    
    # Save to custom location
    storage = StorageManager(output_dir="./custom_data")
    storage.save_json(items, filename="ai_tweets.json")
    print("\nSaved to ./custom_data/ai_tweets.json")


# ==================== EXAMPLE 6: News Aggregation ====================

async def example_news_aggregation():
    """Example: Aggregate and analyze news data"""
    print("\n=== Example 6: News Aggregation ===\n")
    
    # Scrape data
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        result = await scraper.scrape_twitter(
            queries=["technology", "science"],
            count=50
        )
        
        # Aggregate by keywords
        aggregator = NewsAggregator(scraper.storage)
        
        keyword_groups = aggregator.aggregate_by_keywords(
            result.items,
            keywords=["AI", "crypto", "climate", "health"]
        )
        
        print("News by keyword:")
        for keyword, items in keyword_groups.items():
            if keyword != '_other':
                print(f"  {keyword}: {len(items)} items")
        
        # Detect trending topics
        trending = aggregator.detect_trending_topics(result.items, top_n=10)
        print("\nTrending hashtags:")
        for tag, count in trending['top_hashtags']:
            print(f"  {tag}: {count}")
        
        # Export report
        report_path = aggregator.export_news_report(
            result.items,
            report_name="tech_news_report"
        )
        print(f"\nReport exported to: {report_path}")
    
    finally:
        await scraper.close()


# ==================== EXAMPLE 7: Twitter User Timeline ====================

async def example_user_timeline():
    """Example: Get tweets from specific users"""
    print("\n=== Example 7: User Timeline ===\n")
    
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"
    
    from twitter_scraper import TwitterScraper
    
    scraper = TwitterScraper(cookies_path="cookies.json")
    await scraper.initialize()
    
    try:
        # Get tweets from specific users
        users = ["elonmusk", "cnn", "bbcworld"]
        
        for username in users:
            print(f"\nFetching tweets from @{username}...")
            items = await scraper.get_user_tweets(username, count=10)
            
            print(f"  Found {len(items)} tweets")
            for item in items[:2]:
                print(f"  - {item.unified.text[:80]}...")
    
    finally:
        pass  # TwitterScraper doesn't need explicit close


# ==================== EXAMPLE 8: Export and Load Data ====================

async def example_export_load():
    """Example: Export and reload data"""
    print("\n=== Example 8: Export and Load ===\n")
    
    # Scrape some data
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"
    
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        result = await scraper.scrape_twitter(
            queries=["python"],
            count=10
        )
        
        # Data is already saved by the scraper
        # Let's find and load it
        import glob
        
        json_files = glob.glob(f"{config.settings.output_directory}/*twitter*.json")
        if json_files:
            latest_file = max(json_files, key=lambda x: 
                             datetime.fromtimestamp(
                                 __import__('os').path.getctime(x)
                             ))
            
            print(f"Loading from: {latest_file}")
            loaded_items = scraper.storage.load_json(latest_file)
            print(f"Loaded {len(loaded_items)} items")
            
            # Verify
            for original, loaded in zip(result.items[:3], loaded_items[:3]):
                assert original.unified.id == loaded.unified.id
                print(f"✓ Verified: {original.unified.id}")
    
    finally:
        await scraper.close()


# ==================== RUN EXAMPLES ====================

EXAMPLES = {
    '1': ('Twitter Search', example_twitter_search),
    '2': ('Telegram Channels', example_telegram_channels),
    '3': ('Combined News', example_combined_news),
    '4': ('Streaming', example_streaming),
    '5': ('Custom Processing', example_custom_processing),
    '6': ('News Aggregation', example_news_aggregation),
    '7': ('User Timeline', example_user_timeline),
    '8': ('Export and Load', example_export_load),
}


async def run_example(example_number: str):
    """Run a specific example"""
    if example_number in EXAMPLES:
        name, func = EXAMPLES[example_number]
        print(f"\nRunning: {name}")
        try:
            await func()
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"Unknown example: {example_number}")
        print_available_examples()


def print_available_examples():
    """Print list of available examples"""
    print("\nAvailable examples:")
    for num, (name, _) in EXAMPLES.items():
        print(f"  {num}. {name}")


async def main():
    """Main function to run examples"""
    import sys
    
    if len(sys.argv) > 1:
        example_num = sys.argv[1]
        await run_example(example_num)
    else:
        print_available_examples()
        print("\nUsage: python examples.py <number>")
        print("Or run all examples:")
        
        # Run a simple test
        print("\n--- Quick Test ---")
        print("Testing configuration loading...")
        config = Config()
        print(f"✓ Config created successfully")
        print(f"  Output dir: {config.settings.output_directory}")
        print(f"  Twitter enabled: {config.twitter.enabled}")
        print(f"  Telegram enabled: {config.telegram.enabled}")


if __name__ == '__main__':
    asyncio.run(main())
