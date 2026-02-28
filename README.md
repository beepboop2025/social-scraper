# Social Media Scraper

A comprehensive Python-based scraper for Twitter and Telegram with structured data output, designed for news aggregation and social media monitoring.

## Features

### Twitter Scraper
- Search tweets by keywords/queries
- Get user timelines
- Stream continuous updates
- Get trending topics
- Full engagement metrics (likes, retweets, replies, views)
- Media extraction (images, videos)

### Telegram Scraper
- Scrape messages from channels and groups
- Support for both Bot API and MTProto (User API)
- Keyword filtering
- Forward detection
- View counts for channel posts
- Media extraction

### Data Structure
- **Unified Data Model**: Consistent structure across all platforms
- **Pydantic Models**: Type-safe, validated data
- **Multiple Formats**: JSON, CSV, JSONL export
- **Rich Metadata**: Author info, engagement metrics, hashtags, mentions, media

## Installation

```bash
# Clone or create the project directory
cd social_scraper

# Install dependencies
pip install -r requirements.txt
```

### Requirements
- Python 3.8+
- Twitter cookies file (for Twitter scraping)
- Telegram API credentials (for Telegram scraping)

## Quick Start

### 1. Configuration

Create a configuration file:

```bash
python main.py --init-config
```

This creates `config.json` with sample settings. Edit it with your credentials.

### 2. Twitter Setup

You need to login to Twitter first to generate cookies:

```bash
python main.py --login twitter
```

Or manually create a cookies file by logging into Twitter in your browser and exporting cookies.

### 3. Telegram Setup

Get your API credentials from https://my.telegram.org:
- `api_id`
- `api_hash`

Set them in your config file or environment variables.

### 4. Run Scraping

```bash
# Scrape Twitter
python main.py --scrape twitter --query "breaking news"

# Scrape Telegram channels
python main.py --scrape telegram --channel @bbcnews

# Scrape both (configured sources)
python main.py --scrape news

# Stream continuously
python main.py --scrape news --stream
```

## Data Structure

### ScrapedContent Model

```python
{
    "id": "unique_id",
    "platform": "twitter|telegram",
    "content_type": "post|comment|reply|forward",
    "text": "Post content",
    "raw_text": "Original unprocessed text",
    "language": "en",
    
    "author": {
        "id": "author_id",
        "username": "@username",
        "display_name": "Display Name",
        "verified": true,
        "follower_count": 1000,
        ...
    },
    
    "media": [
        {
            "type": "image|video|gif|audio|document",
            "url": "media_url",
            "filename": "file.jpg",
            ...
        }
    ],
    
    "engagement": {
        "likes": 100,
        "replies": 20,
        "reposts": 50,
        "views": 1000
    },
    
    "hashtags": ["#news", "#breaking"],
    "mentions": ["@user1", "@user2"],
    "urls": ["https://example.com"],
    
    "created_at": "2024-01-01T12:00:00",
    "scraped_at": "2024-01-01T12:05:00",
    
    "is_reply": false,
    "is_thread": false,
    "source_url": "https://twitter.com/...",
    "search_query": "breaking news"
}
```

## Usage Examples

### Example 1: Simple Twitter Search

```python
import asyncio
from main import quick_scrape_twitter

async def main():
    items = await quick_scrape_twitter(
        queries=["AI news", "technology"],
        cookies_path="cookies.json",
        count=20
    )
    
    for item in items:
        print(f"@{item.unified.author.username}: {item.unified.text[:50]}...")

asyncio.run(main())
```

### Example 2: Telegram Channel Scraping

```python
import asyncio
from main import quick_scrape_telegram

async def main():
    items = await quick_scrape_telegram(
        channels=["@bbcnews", "@cnn"],
        api_id=12345678,
        api_hash="your_api_hash",
        limit=50
    )
    
    for item in items:
        print(f"[{item.unified.source_channel}] {item.unified.text[:50]}...")

asyncio.run(main())
```

### Example 3: Full Control with Config

```python
import asyncio
from main import SocialMediaScraper
from config import Config

async def main():
    # Create config
    config = Config()
    config.twitter.enabled = True
    config.twitter.cookies_path = "cookies.json"
    config.telegram.enabled = True
    config.telegram.api_id = 12345678
    config.telegram.api_hash = "your_api_hash"
    
    # Configure news sources
    config.news_sources.twitter_search_queries = ["breaking", "news"]
    config.news_sources.telegram_channels = ["@bbcnews"]
    
    # Create and run scraper
    scraper = SocialMediaScraper(config)
    await scraper.initialize()
    
    try:
        results = await scraper.scrape_news()
        
        for result in results:
            print(f"Platform: {result.platform}")
            print(f"Items: {result.items_scraped}")
    finally:
        await scraper.close()

asyncio.run(main())
```

### Example 4: News Aggregation

```python
from storage import NewsAggregator, StorageManager

storage = StorageManager()
aggregator = NewsAggregator(storage)

# Group by keywords
groups = aggregator.aggregate_by_keywords(
    items,
    keywords=["breaking", "urgent", "update"]
)

# Detect trending
trending = aggregator.detect_trending_topics(items, top_n=10)

# Export report
report_path = aggregator.export_news_report(
    items,
    report_name="daily_news"
)
```

## Environment Variables

You can also configure via environment variables:

```bash
# Twitter
export TWITTER_ENABLED=true
export TWITTER_COOKIES_PATH=cookies.json

# Telegram
export TELEGRAM_ENABLED=true
export TELEGRAM_API_ID=12345678
export TELEGRAM_API_HASH=your_api_hash

# Settings
export OUTPUT_DIRECTORY=./data
export OUTPUT_FORMAT=both
```

## Command Line Options

```
usage: main.py [-h] [--config CONFIG] [--init-config] 
               [--login {twitter,telegram}]
               [--scrape {twitter,telegram,news,all}]
               [--query QUERY] [--channel CHANNEL]
               [--count COUNT] [--stream]
               [--output OUTPUT] [--format {json,csv,both}]

Options:
  -h, --help            Show help message
  -c, --config          Config file path
  --init-config         Create sample config
  --login               Interactive login
  --scrape              What to scrape
  -q, --query           Search query
  -ch, --channel        Telegram channel
  -n, --count           Number of items (default: 20)
  --stream              Continuous streaming mode
  -o, --output          Output directory
  -f, --format          Output format
```

## Output Files

The scraper creates several files in your output directory:

- `scraped_<batch_id>_<timestamp>.json` - Full JSON data
- `scraped_<batch_id>_<timestamp>.csv` - CSV export
- `metadata_<platform>_<batch_id>_<timestamp>.json` - Metadata about the scrape
- `news_report_<batch_id>.json` - Aggregated news report

## Project Structure

```
social_scraper/
├── main.py                 # Entry point
├── config.py              # Configuration management
├── models.py              # Data models
├── twitter_scraper.py     # Twitter scraping module
├── telegram_scraper.py    # Telegram scraping module
├── storage.py             # Data storage and export
├── examples.py            # Usage examples
├── requirements.txt       # Dependencies
├── .env.example          # Example environment file
└── README.md             # This file
```

## Rate Limiting

The scraper includes built-in rate limiting to avoid getting blocked:

- Twitter: 1 second delay between requests
- Telegram: 1 second delay between channel requests

Adjust in configuration:
```python
config.settings.rate_limit_delay = 2.0  # 2 seconds
```

## Advanced Usage

See `examples.py` for more advanced usage:

1. Twitter Search
2. Telegram Channels
3. Combined News Scraping
4. Streaming Updates
5. Custom Processing
6. News Aggregation
7. User Timelines
8. Export and Load

Run examples:
```bash
python examples.py 1  # Run example 1
```

## Troubleshooting

### Twitter Login Issues
- Make sure cookies file exists and is valid
- Try logging in again: `python main.py --login twitter`
- Check if account has 2FA enabled

### Telegram API Issues
- Verify api_id and api_hash are correct
- Make sure phone number is in international format (+1234567890)
- Check if you received the login code

### Rate Limiting
- Increase delay in config: `rate_limit_delay = 2.0`
- Reduce batch size
- Use multiple sessions/accounts

## License

MIT License - Feel free to use and modify as needed.

## Contributing

Contributions welcome! Please ensure:
- Code follows existing style
- Add tests for new features
- Update documentation
