"""
Storage Module
Handles saving scraped data in various formats (JSON, CSV, etc.)
"""

import json
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
import logging

from models import ScrapedItem, ScrapingResult, ScrapedContent

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class StorageManager:
    """Manages storage of scraped data"""
    
    def __init__(self, output_dir: str = "./data", media_dir: str = "./media"):
        self.output_dir = Path(output_dir)
        self.media_dir = Path(media_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.media_dir.mkdir(parents=True, exist_ok=True)
    
    def _generate_filename(self, prefix: str, extension: str, 
                          batch_id: Optional[str] = None) -> str:
        """Generate a filename with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if batch_id:
            return f"{prefix}_{batch_id}_{timestamp}.{extension}"
        return f"{prefix}_{timestamp}.{extension}"
    
    def save_json(self, items: List[ScrapedItem], 
                  filename: Optional[str] = None,
                  batch_id: Optional[str] = None) -> str:
        """Save items to JSON file"""
        if filename is None:
            filename = self._generate_filename("scraped", "json", batch_id)
        
        filepath = self.output_dir / filename
        
        # Convert to dict
        data = [item.model_dump() for item in items]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, cls=DateTimeEncoder, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(items)} items to {filepath}")
        return str(filepath)
    
    def save_jsonl(self, items: List[ScrapedItem],
                   filename: Optional[str] = None,
                   batch_id: Optional[str] = None) -> str:
        """Save items to JSON Lines file (one JSON per line)"""
        if filename is None:
            filename = self._generate_filename("scraped", "jsonl", batch_id)
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for item in items:
                json.dump(item.model_dump(), f, cls=DateTimeEncoder, ensure_ascii=False)
                f.write('\n')
        
        logger.info(f"Saved {len(items)} items to {filepath} (JSONL)")
        return str(filepath)
    
    def save_csv(self, items: List[ScrapedItem],
                 filename: Optional[str] = None,
                 batch_id: Optional[str] = None) -> str:
        """Save items to CSV file (flattened structure)"""
        if filename is None:
            filename = self._generate_filename("scraped", "csv", batch_id)
        
        filepath = self.output_dir / filename
        
        if not items:
            logger.warning("No items to save to CSV")
            return str(filepath)
        
        # Flatten the data for CSV
        flattened = []
        for item in items:
            flat = self._flatten_item(item)
            flattened.append(flat)
        
        # Get all possible fields
        fieldnames = set()
        for flat in flattened:
            fieldnames.update(flat.keys())
        fieldnames = sorted(fieldnames)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flattened)
        
        logger.info(f"Saved {len(items)} items to {filepath} (CSV)")
        return str(filepath)
    
    def _flatten_item(self, item: ScrapedItem) -> Dict[str, Any]:
        """Flatten a ScrapedItem for CSV export"""
        u = item.unified
        
        flat = {
            'id': u.id,
            'platform': u.platform.value,
            'content_type': u.content_type.value,
            'text': u.text,
            'language': u.language,
            'author_id': u.author.id,
            'author_username': u.author.username,
            'author_display_name': u.author.display_name,
            'author_verified': u.author.verified,
            'author_follower_count': u.author.follower_count,
            'likes': u.engagement.likes,
            'replies': u.engagement.replies,
            'reposts': u.engagement.reposts,
            'quotes': u.engagement.quotes,
            'bookmarks': u.engagement.bookmarks,
            'views': u.engagement.views,
            'forwards': u.engagement.forwards,
            'created_at': u.created_at.isoformat() if u.created_at else None,
            'scraped_at': u.scraped_at.isoformat() if u.scraped_at else None,
            'is_reply': u.is_reply,
            'is_thread': u.is_thread,
            'parent_id': u.parent_id,
            'source_url': u.source_url,
            'source_channel': u.source_channel,
            'hashtags': ','.join(u.hashtags),
            'mentions': ','.join(u.mentions),
            'urls': ','.join(u.urls),
            'media_count': len(u.media),
            'search_query': u.search_query,
            'batch_id': u.collection_batch_id,
        }
        
        # Add platform-specific fields
        if item.platform_specific:
            if hasattr(item.platform_specific, 'tweet_id'):
                # Twitter specific
                flat['twitter_tweet_id'] = item.platform_specific.tweet_id
                flat['twitter_conversation_id'] = item.platform_specific.conversation_id
                flat['twitter_possibly_sensitive'] = item.platform_specific.possibly_sensitive
            elif hasattr(item.platform_specific, 'message_id'):
                # Telegram specific
                flat['telegram_message_id'] = item.platform_specific.message_id
                flat['telegram_channel_id'] = item.platform_specific.channel_id
                flat['telegram_channel_title'] = item.platform_specific.channel_title
                flat['telegram_is_forward'] = item.platform_specific.is_automatic_forward
                flat['telegram_forward_from'] = item.platform_specific.forward_from_chat
        
        return flat
    
    def save_result(self, result: ScrapingResult,
                    save_json: bool = True,
                    save_csv: bool = True) -> Dict[str, str]:
        """Save a ScrapingResult in multiple formats"""
        saved_files = {}
        
        if save_json and result.items:
            filepath = self.save_json(
                result.items, 
                batch_id=result.batch_id
            )
            saved_files['json'] = filepath
        
        if save_csv and result.items:
            filepath = self.save_csv(
                result.items,
                batch_id=result.batch_id
            )
            saved_files['csv'] = filepath
        
        # Save metadata
        metadata = {
            'batch_id': result.batch_id,
            'platform': result.platform.value,
            'query': result.query,
            'items_scraped': result.items_scraped,
            'items_failed': result.items_failed,
            'start_time': result.start_time.isoformat(),
            'end_time': result.end_time.isoformat() if result.end_time else None,
            'duration_seconds': result.duration_seconds,
            'errors': result.errors,
            'saved_files': saved_files,
        }
        
        meta_filename = self._generate_filename(
            f"metadata_{result.platform.value}", 
            "json", 
            result.batch_id
        )
        meta_filepath = self.output_dir / meta_filename
        
        with open(meta_filepath, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, cls=DateTimeEncoder, indent=2)
        
        saved_files['metadata'] = str(meta_filepath)
        
        return saved_files
    
    def load_json(self, filepath: str) -> List[ScrapedItem]:
        """Load items from JSON file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        items = []
        for item_data in data:
            try:
                item = ScrapedItem.model_validate(item_data)
                items.append(item)
            except Exception as e:
                logger.warning(f"Failed to parse item: {e}")
        
        logger.info(f"Loaded {len(items)} items from {filepath}")
        return items
    
    def load_jsonl(self, filepath: str) -> List[ScrapedItem]:
        """Load items from JSON Lines file"""
        items = []
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item_data = json.loads(line)
                    item = ScrapedItem.model_validate(item_data)
                    items.append(item)
                except Exception as e:
                    logger.warning(f"Failed to parse line: {e}")
        
        logger.info(f"Loaded {len(items)} items from {filepath} (JSONL)")
        return items
    
    def append_jsonl(self, item: ScrapedItem, filepath: str):
        """Append a single item to JSON Lines file"""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'a', encoding='utf-8') as f:
            json.dump(item.model_dump(), f, cls=DateTimeEncoder, ensure_ascii=False)
            f.write('\n')
    
    def get_statistics(self, items: List[ScrapedItem]) -> Dict[str, Any]:
        """Get statistics about scraped items"""
        if not items:
            return {}
        
        platforms = {}
        authors = {}
        hashtags = {}
        dates = []
        
        for item in items:
            # Platform count
            platform = item.unified.platform.value
            platforms[platform] = platforms.get(platform, 0) + 1
            
            # Author count
            author = item.unified.author.username or item.unified.author.display_name
            authors[author] = authors.get(author, 0) + 1
            
            # Hashtag count
            for tag in item.unified.hashtags:
                hashtags[tag] = hashtags.get(tag, 0) + 1
            
            # Dates
            dates.append(item.unified.created_at)
        
        return {
            'total_items': len(items),
            'platforms': platforms,
            'unique_authors': len(authors),
            'top_authors': sorted(authors.items(), key=lambda x: x[1], reverse=True)[:10],
            'top_hashtags': sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:10],
            'date_range': {
                'earliest': min(dates).isoformat() if dates else None,
                'latest': max(dates).isoformat() if dates else None,
            }
        }
    
    def export_by_platform(self, items: List[ScrapedItem],
                          batch_id: Optional[str] = None) -> Dict[str, str]:
        """Export items separated by platform"""
        from collections import defaultdict
        
        by_platform = defaultdict(list)
        for item in items:
            by_platform[item.unified.platform.value].append(item)
        
        saved_files = {}
        for platform, platform_items in by_platform.items():
            filename = self._generate_filename(f"{platform}", "json", batch_id)
            filepath = self.save_json(platform_items, filename=filename)
            saved_files[platform] = filepath
        
        return saved_files


class NewsAggregator:
    """Aggregates news events from multiple sources"""
    
    def __init__(self, storage: StorageManager):
        self.storage = storage
    
    def aggregate_by_keywords(self, items: List[ScrapedItem],
                             keywords: List[str]) -> Dict[str, List[ScrapedItem]]:
        """Group items by keywords"""
        from collections import defaultdict
        
        groups = defaultdict(list)
        
        for item in items:
            text = item.unified.text.lower()
            matched = False
            
            for keyword in keywords:
                if keyword.lower() in text:
                    groups[keyword].append(item)
                    matched = True
            
            if not matched:
                groups['_other'].append(item)
        
        return dict(groups)
    
    def aggregate_by_time(self, items: List[ScrapedItem],
                         interval_hours: int = 24) -> Dict[str, List[ScrapedItem]]:
        """Group items by time intervals"""
        from collections import defaultdict
        
        groups = defaultdict(list)
        
        for item in items:
            dt = item.unified.created_at
            # Round to interval
            interval_key = dt.replace(
                hour=(dt.hour // interval_hours) * interval_hours,
                minute=0,
                second=0,
                microsecond=0
            )
            groups[interval_key.isoformat()].append(item)
        
        return dict(groups)
    
    def detect_trending_topics(self, items: List[ScrapedItem],
                               top_n: int = 10) -> List[tuple]:
        """Detect trending hashtags/keywords"""
        from collections import Counter
        
        hashtag_counts = Counter()
        word_counts = Counter()
        
        for item in items:
            # Count hashtags
            for tag in item.unified.hashtags:
                hashtag_counts[tag.lower()] += 1
            
            # Count words (simple approach)
            words = item.unified.text.lower().split()
            for word in words:
                if len(word) > 4 and word.isalpha():
                    word_counts[word] += 1
        
        return {
            'top_hashtags': hashtag_counts.most_common(top_n),
            'top_words': word_counts.most_common(top_n),
        }
    
    def export_news_report(self, items: List[ScrapedItem],
                          report_name: Optional[str] = None) -> str:
        """Export a comprehensive news report"""
        if report_name is None:
            report_name = f"news_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        stats = self.storage.get_statistics(items)
        trending = self.detect_trending_topics(items)
        
        report = {
            'report_name': report_name,
            'generated_at': datetime.now().isoformat(),
            'statistics': stats,
            'trending': trending,
            'items': [item.model_dump() for item in items]
        }
        
        filepath = self.storage.output_dir / f"{report_name}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, cls=DateTimeEncoder, ensure_ascii=False, indent=2)
        
        logger.info(f"Exported news report to {filepath}")
        return str(filepath)
