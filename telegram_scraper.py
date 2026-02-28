"""
Telegram Scraper Module
Uses python-telegram-bot and telethon for Telegram data extraction
Supports both bot API and user API (MTProto) for news channels
"""

import asyncio
import re
from datetime import datetime
from typing import List, Optional, Dict, Any, AsyncGenerator, Union
from pathlib import Path
import logging

from models import (
    ScrapedContent, ScrapedItem, AuthorInfo, EngagementMetrics,
    MediaItem, MediaType, Platform, ContentType, TelegramSpecific
)

logger = logging.getLogger(__name__)


class TelegramScraper:
    """Telegram scraper supporting both Bot API and MTProto (user) API"""
    
    def __init__(self, 
                 bot_token: Optional[str] = None,
                 api_id: Optional[int] = None,
                 api_hash: Optional[str] = None,
                 phone_number: Optional[str] = None,
                 session_name: str = "telegram_scraper"):
        self.bot_token = bot_token
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.session_name = session_name
        
        self.bot = None
        self.client = None  # Telethon client
        self._initialized = False
        self._use_bot = bot_token is not None
        self._use_user = api_id and api_hash
    
    async def initialize(self):
        """Initialize Telegram clients"""
        if self._use_bot:
            try:
                from telegram import Bot
                self.bot = Bot(token=self.bot_token)
                bot_info = await self.bot.get_me()
                logger.info(f"Bot initialized: @{bot_info.username}")
            except ImportError:
                logger.error("python-telegram-bot not installed. Install with: pip install python-telegram-bot")
                raise
            except Exception as e:
                logger.error(f"Failed to initialize bot: {e}")
                self.bot = None
        
        if self._use_user:
            try:
                from telethon import TelegramClient
                self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
                await self.client.start(phone=self.phone_number)
                logger.info("Telethon client initialized")
            except ImportError:
                logger.error("telethon not installed. Install with: pip install telethon")
                raise
            except Exception as e:
                logger.error(f"Failed to initialize telethon: {e}")
                self.client = None
        
        self._initialized = True
        
        if not self.bot and not self.client:
            logger.warning("No Telegram clients initialized. Provide bot_token or api_id/api_hash.")
    
    async def close(self):
        """Close connections"""
        if self.client:
            await self.client.disconnect()
            logger.info("Telethon client disconnected")
    
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
    
    def _convert_telegram_media(self, message) -> List[MediaItem]:
        """Convert telegram media to our MediaItem format"""
        media_items = []
        
        try:
            if message.photo:
                media_items.append(MediaItem(
                    type=MediaType.IMAGE,
                    filename=getattr(message.photo, 'file_name', None),
                ))
            
            if message.video:
                media_items.append(MediaItem(
                    type=MediaType.VIDEO,
                    duration_seconds=getattr(message.video, 'duration', None),
                    filename=getattr(message.video, 'file_name', None),
                ))
            
            if message.document:
                mime_type = getattr(message.document, 'mime_type', '')
                media_type = MediaType.DOCUMENT
                if mime_type.startswith('image/'):
                    media_type = MediaType.IMAGE
                elif mime_type.startswith('video/'):
                    media_type = MediaType.VIDEO
                elif mime_type.startswith('audio/'):
                    media_type = MediaType.AUDIO
                
                media_items.append(MediaItem(
                    type=media_type,
                    mime_type=mime_type,
                    size_bytes=getattr(message.document, 'size', None),
                    filename=getattr(message.document, 'file_name', None),
                ))
            
            if message.audio:
                media_items.append(MediaItem(
                    type=MediaType.AUDIO,
                    duration_seconds=getattr(message.audio, 'duration', None),
                    filename=getattr(message.audio, 'file_name', None),
                ))
            
            if message.voice:
                media_items.append(MediaItem(
                    type=MediaType.AUDIO,
                    duration_seconds=getattr(message.voice, 'duration', None),
                ))
            
            if message.poll:
                media_items.append(MediaItem(
                    type=MediaType.POLL,
                ))
            
            if message.location:
                media_items.append(MediaItem(
                    type=MediaType.LINK,
                ))
                
        except Exception as e:
            logger.warning(f"Failed to parse media: {e}")
        
        return media_items
    
    def _message_to_scraped_content(self, message, channel_name: str = "",
                                    search_query: Optional[str] = None,
                                    batch_id: Optional[str] = None) -> ScrapedContent:
        """Convert a telegram message to our ScrapedContent model"""
        
        # Get sender info
        sender = getattr(message, 'sender', None)
        if sender:
            author = AuthorInfo(
                id=str(getattr(sender, 'id', '')),
                username=getattr(sender, 'username', None),
                display_name=getattr(sender, 'first_name', '') or getattr(sender, 'title', 'Unknown'),
                verified=getattr(sender, 'verified', False) if hasattr(sender, 'verified') else False,
            )
        else:
            author = AuthorInfo(
                display_name=channel_name or "Unknown",
                username=channel_name,
            )
        
        # Get text content
        text = message.text or message.caption or ""
        
        # Get media
        media_items = self._convert_telegram_media(message)
        
        # Get engagement (views for channels)
        views = getattr(message, 'views', None)
        forwards = getattr(message, 'forwards', None)
        engagement = EngagementMetrics(
            views=views,
            forwards=forwards,
        )
        
        # Parse timestamps
        created_at = message.date or datetime.utcnow()
        edited_at = getattr(message, 'edit_date', None)
        
        # Determine if forward
        is_forward = bool(getattr(message, 'forward', None))
        forward_from_chat = None
        forward_from_message_id = None
        forward_date = None
        
        if is_forward:
            forward = message.forward
            forward_from_chat = getattr(forward, 'chat', None)
            if forward_from_chat:
                forward_from_chat = getattr(forward_from_chat, 'title', None) or getattr(forward_from_chat, 'username', None)
            forward_from_message_id = getattr(forward, 'channel_post', None)
            forward_date = getattr(forward, 'date', None)
        
        # Build source URL
        channel_username = channel_name.lstrip('@')
        source_url = f"https://t.me/{channel_username}/{message.id}" if channel_username else None
        
        return ScrapedContent(
            id=str(message.id),
            platform=Platform.TELEGRAM,
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
            edited_at=edited_at,
            is_reply=bool(getattr(message, 'reply_to_msg_id', None)),
            parent_id=str(message.reply_to_msg_id) if getattr(message, 'reply_to_msg_id', None) else None,
            source_url=source_url,
            source_channel=channel_name,
            search_query=search_query,
            collection_batch_id=batch_id,
            raw_metadata={
                'is_forward': is_forward,
                'forward_from_chat': forward_from_chat,
                'forward_from_message_id': forward_from_message_id,
                'forward_date': forward_date.isoformat() if forward_date else None,
                'via_bot': getattr(message, 'via_bot_id', None),
                'has_protected_content': getattr(message, 'has_protected_content', False),
                'media_group_id': getattr(message, 'grouped_id', None),
                'silent': getattr(message, 'silent', False),
            }
        )
    
    async def get_channel_messages(self, channel_name: str, 
                                   limit: int = 100,
                                   search_query: Optional[str] = None,
                                   batch_id: Optional[str] = None) -> List[ScrapedItem]:
        """Get messages from a channel/group using MTProto (requires user API)"""
        if not self.client:
            logger.error("MTProto client not available. Provide api_id and api_hash.")
            return []
        
        items = []
        try:
            from telethon.tl.functions.messages import GetHistoryRequest
            
            # Normalize channel name
            channel_name = channel_name.lstrip('@')
            
            # Get entity
            entity = await self.client.get_entity(channel_name)
            
            # Get messages
            messages = await self.client.get_messages(entity, limit=limit)
            
            for message in messages:
                if not message:  # Skip None messages
                    continue
                    
                try:
                    content = self._message_to_scraped_content(
                        message,
                        channel_name=f"@{channel_name}",
                        search_query=search_query,
                        batch_id=batch_id
                    )
                    
                    # Create Telegram-specific data
                    telegram_specific = TelegramSpecific(
                        message_id=message.id,
                        channel_id=getattr(entity, 'id', None),
                        channel_title=getattr(entity, 'title', None),
                        forward_from_chat=content.raw_metadata.get('forward_from_chat'),
                        forward_from_message_id=content.raw_metadata.get('forward_from_message_id'),
                        forward_date=datetime.fromisoformat(content.raw_metadata.get('forward_date')) if content.raw_metadata.get('forward_date') else None,
                        is_automatic_forward=content.raw_metadata.get('is_forward', False),
                        has_protected_content=content.raw_metadata.get('has_protected_content', False),
                        reply_to_message_id=message.reply_to_msg_id if hasattr(message, 'reply_to_msg_id') else None,
                        via_bot=str(content.raw_metadata.get('via_bot')) if content.raw_metadata.get('via_bot') else None,
                        edit_date=content.edited_at,
                        media_group_id=str(content.raw_metadata.get('media_group_id')) if content.raw_metadata.get('media_group_id') else None,
                        caption=message.caption if hasattr(message, 'caption') else None,
                    )
                    
                    items.append(ScrapedItem(
                        unified=content,
                        platform_specific=telegram_specific
                    ))
                    
                except Exception as e:
                    logger.warning(f"Failed to process message {getattr(message, 'id', '?')}: {e}")
            
            logger.info(f"Scraped {len(items)} messages from channel: @{channel_name}")
            
        except Exception as e:
            logger.error(f"Failed to get channel messages: {e}")
        
        return items
    
    async def get_news_from_channels(self, 
                                     channels: List[str],
                                     keywords: Optional[List[str]] = None,
                                     limit_per_channel: int = 50,
                                     batch_id: Optional[str] = None) -> List[ScrapedItem]:
        """Get news from multiple channels, optionally filtered by keywords"""
        all_items = []
        
        for channel in channels:
            try:
                items = await self.get_channel_messages(
                    channel, 
                    limit=limit_per_channel,
                    batch_id=batch_id
                )
                
                # Filter by keywords if provided
                if keywords:
                    filtered_items = []
                    for item in items:
                        text = item.unified.text.lower()
                        if any(kw.lower() in text for kw in keywords):
                            item.unified.tags.extend([f"keyword:{kw}" for kw in keywords if kw.lower() in text])
                            filtered_items.append(item)
                    items = filtered_items
                
                all_items.extend(items)
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error processing channel {channel}: {e}")
        
        return all_items
    
    async def search_public_messages(self, query: str, 
                                     limit: int = 50,
                                     batch_id: Optional[str] = None) -> List[ScrapedItem]:
        """Search for messages globally (requires user API)"""
        if not self.client:
            logger.error("MTProto client not available for global search")
            return []
        
        items = []
        try:
            from telethon.tl.functions.messages import SearchGlobalRequest
            from telethon.tl.types import InputPeerEmpty

            result = await self.client(SearchGlobalRequest(
                q=query,
                filter=None,
                min_date=None,
                max_date=None,
                offset_rate=0,
                offset_peer=InputPeerEmpty(),
                offset_id=0,
                limit=limit
            ))
            
            for message in result.messages:
                if not message:
                    continue
                    
                try:
                    # Find matching chat
                    chat = None
                    for c in result.chats:
                        if hasattr(message.peer_id, 'channel_id') and c.id == message.peer_id.channel_id:
                            chat = c
                            break
                    
                    channel_name = getattr(chat, 'username', None) or getattr(chat, 'title', 'unknown')
                    
                    content = self._message_to_scraped_content(
                        message,
                        channel_name=channel_name,
                        search_query=query,
                        batch_id=batch_id
                    )
                    
                    telegram_specific = TelegramSpecific(
                        message_id=message.id,
                        channel_id=getattr(chat, 'id', None),
                        channel_title=getattr(chat, 'title', None),
                    )
                    
                    items.append(ScrapedItem(
                        unified=content,
                        platform_specific=telegram_specific
                    ))
                    
                except Exception as e:
                    logger.warning(f"Failed to process search result: {e}")
            
            logger.info(f"Found {len(items)} messages for query: {query}")
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
        
        return items
    
    async def stream_channel_updates(self, 
                                     channels: List[str],
                                     callback=None,
                                     interval: int = 60,
                                     batch_id: Optional[str] = None) -> AsyncGenerator[ScrapedItem, None]:
        """Stream new messages from channels periodically"""
        seen_ids = {channel: set() for channel in channels}
        
        # Initial load to populate seen_ids
        for channel in channels:
            try:
                items = await self.get_channel_messages(channel, limit=50, batch_id=batch_id)
                for item in items:
                    seen_ids[channel].add(item.unified.id)
            except Exception as e:
                logger.error(f"Error in initial load for {channel}: {e}")
        
        logger.info(f"Starting stream for {len(channels)} channels")
        
        while True:
            for channel in channels:
                try:
                    items = await self.get_channel_messages(channel, limit=10, batch_id=batch_id)
                    
                    # Sort by date, newest first
                    items.sort(key=lambda x: x.unified.created_at, reverse=True)
                    
                    for item in items:
                        if item.unified.id not in seen_ids[channel]:
                            seen_ids[channel].add(item.unified.id)
                            item.unified.tags.append("new")
                            if callback:
                                await callback(item)
                            yield item
                            
                except Exception as e:
                    logger.error(f"Error in stream for {channel}: {e}")
            
            await asyncio.sleep(interval)
    
    async def get_bot_updates(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get updates from bot (messages sent to bot)"""
        if not self.bot:
            logger.error("Bot not initialized")
            return []
        
        updates = []
        try:
            from telegram import Update
            bot_updates = await self.bot.get_updates(limit=limit)
            
            for update in bot_updates:
                try:
                    update_data = {
                        'update_id': update.update_id,
                        'date': update.message.date if update.message else None,
                        'text': update.message.text if update.message else None,
                        'chat_id': update.message.chat_id if update.message else None,
                        'from_user': update.message.from_user.username if update.message and update.message.from_user else None,
                    }
                    updates.append(update_data)
                except Exception as e:
                    logger.warning(f"Failed to process update: {e}")
            
            logger.info(f"Retrieved {len(updates)} bot updates")
            
        except Exception as e:
            logger.error(f"Failed to get bot updates: {e}")
        
        return updates


# Synchronous wrappers
def create_telegram_scraper(
    bot_token: Optional[str] = None,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
    phone_number: Optional[str] = None
) -> TelegramScraper:
    """Create a Telegram scraper instance"""
    return TelegramScraper(
        bot_token=bot_token,
        api_id=api_id,
        api_hash=api_hash,
        phone_number=phone_number
    )


async def quick_channel_scrape(channel_name: str, 
                               api_id: int, 
                               api_hash: str,
                               limit: int = 50) -> List[ScrapedItem]:
    """Quick scrape function for a channel"""
    scraper = TelegramScraper(api_id=api_id, api_hash=api_hash)
    await scraper.initialize()
    try:
        return await scraper.get_channel_messages(channel_name, limit=limit)
    finally:
        await scraper.close()
