# Data Structure Reference

## Overview

The scraper uses a unified data model that provides consistent structure across Twitter and Telegram platforms.

## Core Models

### 1. ScrapedContent (Unified Model)

Main content structure used for all platforms:

```python
{
    # Identification
    "id": "tweet_id_or_message_id",
    "platform": "twitter" | "telegram",
    "content_type": "post" | "comment" | "reply" | "forward" | "media",
    
    # Content
    "text": "The main text content",
    "raw_text": "Original unprocessed text",
    "language": "en" | "es" | etc,
    
    # Author Information
    "author": {
        "id": "author_id",
        "username": "@username",
        "display_name": "Display Name",
        "profile_url": "https://...",
        "avatar_url": "https://...",
        "verified": true | false,
        "follower_count": 1000,
        "following_count": 500,
        "description": "Bio text",
        "location": "City, Country",
        "created_at": "2020-01-01T00:00:00"
    },
    
    # Media Attachments
    "media": [
        {
            "type": "image" | "video" | "gif" | "audio" | "document" | "link" | "poll",
            "url": "https://...",
            "local_path": "/path/to/downloaded/file",
            "filename": "image.jpg",
            "mime_type": "image/jpeg",
            "size_bytes": 1024,
            "width": 1920,
            "height": 1080,
            "duration_seconds": 60.0,
            "thumbnail_url": "https://..."
        }
    ],
    
    # Engagement Metrics
    "engagement": {
        "likes": 100,
        "replies": 20,
        "reposts": 50,
        "quotes": 10,
        "bookmarks": 5,
        "views": 1000,
        "shares": 30,
        "forwards": 25
    },
    
    # Extracted Entities
    "urls": ["https://example.com"],
    "hashtags": ["#news", "#breaking"],
    "mentions": ["@user1", "@user2"],
    
    # Timestamps
    "created_at": "2024-01-01T12:00:00",
    "scraped_at": "2024-01-01T12:05:00",
    "edited_at": "2024-01-01T12:10:00",
    
    # Thread/Reply Info
    "is_reply": false,
    "is_thread": false,
    "parent_id": "parent_post_id",
    "thread_id": "thread_id",
    "reply_to_user": "@username",
    
    # Source Information
    "source_url": "https://twitter.com/..." | "https://t.me/...",
    "source_channel": "@channel_name",
    
    # Metadata
    "raw_metadata": { ... },  # Platform-specific raw data
    "tags": ["tag1", "tag2"],
    "category": "news" | "entertainment" | etc,
    "search_query": "the search query that found this",
    "collection_batch_id": "batch_123"
}
```

### 2. Platform-Specific Models

#### Twitter-Specific Fields

```python
{
    "tweet_id": "1234567890",
    "conversation_id": "1234567890",
    "reply_settings": "everyone" | "followers" | "mentioned",
    "source": "Twitter Web App",
    "possibly_sensitive": false,
    "edit_history_tweet_ids": ["1234567890"],
    "referenced_tweets": [
        {"type": "retweeted", "id": "9876543210"}
    ],
    "public_metrics": {
        "retweet_count": 50,
        "reply_count": 20,
        "like_count": 100,
        "quote_count": 10
    },
    "context_annotations": [...],
    "entities": {...},
    "geo": {
        "place_id": "...",
        "coordinates": {...}
    }
}
```

#### Telegram-Specific Fields

```python
{
    "message_id": 12345,
    "channel_id": -1001234567890,
    "channel_title": "Channel Name",
    "forward_from_chat": "Original Channel",
    "forward_from_message_id": 54321,
    "forward_date": "2024-01-01T10:00:00",
    "is_automatic_forward": false,
    "has_protected_content": false,
    "reply_to_message_id": 11111,
    "via_bot": "@bot_name",
    "edit_date": "2024-01-01T12:00:00",
    "media_group_id": "group_123",
    "caption": "Media caption",
    "caption_entities": [...],
    "contact": {...},
    "location": {...},
    "venue": {...},
    "poll": {...},
    "dice": {...}
}
```

### 3. ScrapedItem (Wrapper)

Combines unified content with platform-specific data:

```python
{
    "unified": { ... ScrapedContent ... },
    "platform_specific": { ... TwitterSpecific or TelegramSpecific ... }
}
```

## Output Formats

### JSON Export

Full structured data with all fields preserved:

```json
[
  {
    "unified": { ... },
    "platform_specific": { ... }
  }
]
```

### CSV Export

Flattened structure for easy analysis in Excel/Sheets:

| Field | Description |
|-------|-------------|
| id | Content ID |
| platform | twitter/telegram |
| text | Content text |
| author_username | @username |
| author_display_name | Display Name |
| likes | Like count |
| replies | Reply count |
| reposts | Repost count |
| views | View count |
| created_at | ISO timestamp |
| hashtags | Comma-separated |
| mentions | Comma-separated |
| urls | Comma-separated |
| source_url | Direct link |

### JSONL Export

One JSON object per line (good for streaming):

```jsonl
{"unified": {...}, "platform_specific": {...}}
{"unified": {...}, "platform_specific": {...}}
```

## Data Validation

All data is validated using Pydantic models:

- **Type Safety**: All fields have proper types
- **Validation**: Invalid data is caught early
- **Serialization**: Easy conversion to/from JSON
- **Documentation**: Self-documenting field names

## Metadata Files

Each scrape generates a metadata file:

```json
{
    "batch_id": "abc123",
    "platform": "twitter",
    "query": "breaking news",
    "items_scraped": 100,
    "items_failed": 0,
    "start_time": "2024-01-01T12:00:00",
    "end_time": "2024-01-01T12:05:00",
    "duration_seconds": 300,
    "errors": [],
    "saved_files": {
        "json": "data/scraped_abc123_20240101_120000.json",
        "csv": "data/scraped_abc123_20240101_120000.csv",
        "metadata": "data/metadata_twitter_abc123_20240101_120000.json"
    }
}
```

## Statistics

The storage module can generate statistics:

```python
{
    "total_items": 1000,
    "platforms": {
        "twitter": 600,
        "telegram": 400
    },
    "unique_authors": 150,
    "top_authors": [
        ("@user1", 50),
        ("@user2", 40)
    ],
    "top_hashtags": [
        ("#news", 200),
        ("#breaking", 150)
    ],
    "date_range": {
        "earliest": "2024-01-01T00:00:00",
        "latest": "2024-01-01T23:59:59"
    }
}
```

## News Report Structure

Aggregated news reports include:

```python
{
    "report_name": "news_report_batch123",
    "generated_at": "2024-01-01T12:00:00",
    "statistics": { ... },
    "trending": {
        "top_hashtags": [...],
        "top_words": [...]
    },
    "items": [ ... ]
}
```

## Field Mappings

### Twitter to Unified Mapping

| Twitter Field | Unified Field |
|---------------|---------------|
| `id` | `id` |
| `full_text` | `text` |
| `user.screen_name` | `author.username` |
| `user.name` | `author.display_name` |
| `favorite_count` | `engagement.likes` |
| `retweet_count` | `engagement.reposts` |
| `reply_count` | `engagement.replies` |
| `quote_count` | `engagement.quotes` |
| `view_count` | `engagement.views` |
| `created_at` | `created_at` |
| `entities.hashtags` | `hashtags` |
| `entities.user_mentions` | `mentions` |
| `entities.urls` | `urls` |

### Telegram to Unified Mapping

| Telegram Field | Unified Field |
|----------------|---------------|
| `message_id` | `id` |
| `text` / `caption` | `text` |
| `sender.username` | `author.username` |
| `sender.first_name` | `author.display_name` |
| `views` | `engagement.views` |
| `forwards` | `engagement.forwards` |
| `date` | `created_at` |
| `edit_date` | `edited_at` |
| `forward` | `is_forward` |
| `reply_to_msg_id` | `parent_id` |

## Usage with Pandas

```python
import pandas as pd
from storage import StorageManager

storage = StorageManager()
items = storage.load_json("data/scraped_xxx.json")

# Convert to DataFrame
rows = [item.unified.model_dump() for item in items]
df = pd.DataFrame(rows)

# Analyze
print(df.groupby('platform').size())
print(df['engagement'].apply(lambda x: x['likes']).describe())
```
