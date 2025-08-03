import logging
import asyncio
import nest_asyncio
nest_asyncio.apply()
import secrets
import string
import random
import os
import json
import aiofiles
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor
import weakref
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError

BOT_TOKEN = "8045181368:AAFKWDYx5o1JQZOmDiNQQgr8qnBUnmosQ7s"
OWNER_ID = 7554767330
BOT_USERNAME = "SIN_CITY_C_BOT"
OWNER_USERNAME = "Void_Realm"

# VPS Storage Paths
STORAGE_PATH = "/var/bot_storage"
USERS_FILE = os.path.join(STORAGE_PATH, "users.json")
MEDIA_FILE = os.path.join(STORAGE_PATH, "media.json")
KEYS_FILE = os.path.join(STORAGE_PATH, "keys.json")
SEEN_MEDIA_FILE = os.path.join(STORAGE_PATH, "seen_media.json")
MEDIA_FILES_DIR = os.path.join(STORAGE_PATH, "media_files")

# Performance constants
MAX_CONCURRENT_SENDS = 20
BATCH_SIZE = 50
SEND_DELAY = 0.03  # Reduced delay for faster sending
MAX_RETRIES = 3
RETRY_DELAY = 1

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AsyncSemaphore:
    """Custom semaphore for rate limiting"""
    def __init__(self, value: int):
        self._semaphore = asyncio.Semaphore(value)
        self._value = value
    
    async def __aenter__(self):
        await self._semaphore.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()

class VaultBot:
    def __init__(self):
        self.users = {}
        self.media = {}
        self.access_keys = {}
        self.seen_media = {}
        self.message_id_to_alias = {}
        self.active_users_cache = set()
        self.last_cache_update = datetime.now()
        self.cache_ttl = timedelta(minutes=5)
        
        # Thread pool for file operations
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Rate limiting
        self.send_semaphore = AsyncSemaphore(MAX_CONCURRENT_SENDS)
        
        # Batch processing queues
        self.broadcast_queue = asyncio.Queue()
        self.media_processing_queue = asyncio.Queue()
        
        self.init_storage()
        asyncio.create_task(self.load_data_async())
        asyncio.create_task(self.start_background_tasks())

    def init_storage(self):
        """Initialize storage directories and files"""
        try:
            os.makedirs(STORAGE_PATH, exist_ok=True)
            os.makedirs(MEDIA_FILES_DIR, exist_ok=True)
            
            # Initialize empty JSON files if they don't exist
            for file_path in [USERS_FILE, MEDIA_FILE, KEYS_FILE, SEEN_MEDIA_FILE]:
                if not os.path.exists(file_path):
                    with open(file_path, 'w') as f:
                        json.dump({}, f)
        except Exception as e:
            logger.error(f"Error initializing storage: {e}")

    async def load_data_async(self):
        """Load data from JSON files asynchronously"""
        try:
            tasks = []
            
            async def load_file(file_path: str, attr_name: str):
                try:
                    async with aiofiles.open(file_path, 'r') as f:
                        content = await f.read()
                        setattr(self, attr_name, json.loads(content) if content.strip() else {})
                except Exception as e:
                    logger.error(f"Error loading {file_path}: {e}")
                    setattr(self, attr_name, {})
            
            tasks.extend([
                load_file(USERS_FILE, 'users'),
                load_file(MEDIA_FILE, 'media'),
                load_file(KEYS_FILE, 'access_keys'),
                load_file(SEEN_MEDIA_FILE, 'seen_media')
            ])
            
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Data loaded successfully")
            
        except Exception as e:
            logger.error(f"Error in load_data_async: {e}")

    async def save_data_async(self):
        """Save data to JSON files asynchronously"""
        try:
            tasks = []
            
            async def save_file(file_path: str, data: dict):
                try:
                    async with aiofiles.open(file_path, 'w') as f:
                        await f.write(json.dumps(data, indent=2))
                except Exception as e:
                    logger.error(f"Error saving {file_path}: {e}")
            
            tasks.extend([
                save_file(USERS_FILE, self.users),
                save_file(MEDIA_FILE, self.media),
                save_file(KEYS_FILE, self.access_keys),
                save_file(SEEN_MEDIA_FILE, self.seen_media)
            ])
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error in save_data_async: {e}")

    def save_data(self):
        """Synchronous save for backward compatibility"""
        asyncio.create_task(self.save_data_async())

    async def download_media_async(self, file, file_id: str, file_type: str) -> Optional[str]:
        """Download media file to VPS storage asynchronously"""
        try:
            file_extension = {
                'photo': '.jpg',
                'video': '.mp4',
                'document': ''
            }.get(file_type, '')
            
            if file_type == 'document' and hasattr(file, 'file_name'):
                file_extension = os.path.splitext(file.file_name)[1]
            
            filename = f"{file_id}{file_extension}"
            file_path = os.path.join(MEDIA_FILES_DIR, filename)
            
            # Use asyncio to run the download in thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self.thread_pool,
                lambda: asyncio.run(file.download_to_drive(file_path))
            )
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading media {file_id}: {e}")
            return None

    def get_active_users_cached(self) -> Set[str]:
        """Get cached list of active user IDs"""
        now = datetime.now()
        if now - self.last_cache_update > self.cache_ttl:
            self.active_users_cache = set(
                user_id for user_id, user_data in self.users.items()
                if not user_data.get('is_banned', False) and user_data.get('is_active', False)
            )
            self.last_cache_update = now
        
        return self.active_users_cache

    def get_active_users(self) -> List[str]:
        """Get list of active user IDs"""
        return list(self.get_active_users_cached())

    def get_user_alias(self, user_id: str) -> Optional[str]:
        """Get user alias by ID"""
        return self.users.get(user_id, {}).get('alias')

    def is_user_registered(self, user_id: str) -> bool:
        """Check if user is registered"""
        return user_id in self.users

    def register_user(self, user_id: str, username: str, alias: str, is_premium: bool = False):
        """Register a new user"""
        self.users[user_id] = {
            'username': username,
            'alias': alias,
            'join_date': datetime.now().isoformat(),
            'is_active': is_premium,
            'is_admin': False,
            'is_premium': is_premium,
            'is_banned': False,
            'daily_media_count': 0,
            'media_count': 0,
            'last_active': datetime.now().isoformat(),
            'last_daily_reset': datetime.now().strftime("%Y-%m-%d")
        }
        # Invalidate cache
        self.last_cache_update = datetime.min
        self.save_data()

    def validate_key(self, key: str) -> Optional[bool]:
        """Validate access key and return premium status"""
        if key in self.access_keys:
            key_data = self.access_keys[key]
            if key_data['uses_remaining'] > 0:
                key_data['uses_remaining'] -= 1
                self.save_data()
                return key_data.get('is_premium', False)
        return None

    def create_access_key(self, uses: int, creator_id: str, is_premium: bool = False) -> str:
        """Create new access key"""
        key = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        self.access_keys[key] = {
            'uses_remaining': uses,
            'is_premium': is_premium,
            'created_by': creator_id,
            'created_at': datetime.now().isoformat()
        }
        self.save_data()
        return key

    def generate_alias(self) -> str:
        """Generate random alias"""
        stylish_aliases = [
            "🌌 Amber Prism", "💫 Binary Shard", "🌀 Chrome Vortex", "🔮 Ghost Lantern",
            "⚡️ Neon Echo", "🌙 Lunar Matrix", "🧬 Cyber Oracle", "🔥 Phoenix Code",
            "🛡 Steel Guardian", "📡 Signal Beacon", "🧊 Crystal Core", "🐚 Ocean Relic",
            "🕯 Mystic Flame", "🌿 Forest Sage", "🌟 Star Voyager", "🪐 Cosmic Drift"
        ]

        base_names = [
            "Amber Code", "Binary Signal", "Chrome Totem", "Twilight Glyph", "Phantom Core",
            "Neon Pulse", "Lunar Whisper", "Cyber Nexus", "Phoenix Rise", "Steel Forge"
        ]

        themes = ["Binary", "Chrome", "Obsidian", "Phantom", "Cyber", "Lunar", "Nova", "Echo", "Crimson", "Twilight"]
        suffixes = ["Glyph", "Spire", "Oracle", "Relic", "Matrix", "Shard", "Sanctum", "Beacon", "Rift", "Lantern"]
        emojis = ["⚡️", "💫", "🌌", "🌙", "🧬", "🔥", "🔮", "🎭", "🛡", "📡", "🧊", "🐚", "🕯", "🌀", "🌿", "🌟", "🪐", "💾"]

        while len(stylish_aliases) < 900:
            alias = f"{secrets.choice(emojis)} {secrets.choice(themes)} {secrets.choice(suffixes)}"
            if alias not in stylish_aliases:
                stylish_aliases.append(alias)

        return secrets.choice(stylish_aliases)

    async def send_message_with_retry(self, context: ContextTypes.DEFAULT_TYPE, 
                                    chat_id: int, **kwargs) -> Optional[object]:
        """Send message with retry logic and error handling"""
        for attempt in range(MAX_RETRIES):
            try:
                async with self.send_semaphore:
                    if 'photo' in kwargs:
                        return await context.bot.send_photo(chat_id=chat_id, **kwargs)
                    elif 'video' in kwargs:
                        return await context.bot.send_video(chat_id=chat_id, **kwargs)
                    elif 'document' in kwargs:
                        return await context.bot.send_document(chat_id=chat_id, **kwargs)
                    else:
                        return await context.bot.send_message(chat_id=chat_id, **kwargs)
                        
            except RetryAfter as e:
                wait_time = e.retry_after + 0.1
                await asyncio.sleep(wait_time)
                continue
                
            except (TimedOut, NetworkError) as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                logger.error(f"Network error sending to {chat_id}: {e}")
                return None
                
            except TelegramError as e:
                if "bot was blocked" in str(e).lower():
                    # Mark user as inactive
                    user_id = str(chat_id)
                    if user_id in self.users:
                        self.users[user_id]['is_active'] = False
                        self.last_cache_update = datetime.min  # Invalidate cache
                        self.save_data()
                    return None
                elif "chat not found" in str(e).lower():
                    return None
                else:
                    logger.error(f"Telegram error sending to {chat_id}: {e}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return None
                    
            except Exception as e:
                logger.error(f"Unexpected error sending to {chat_id}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return None
                
        return None

    async def broadcast_media_batch(self, context: ContextTypes.DEFAULT_TYPE, 
                                  recipients: List[str], media_data: dict) -> Tuple[int, int]:
        """Broadcast media to a batch of recipients"""
        tasks = []
        file_id = media_data['file_id']
        file_type = media_data['file_type']
        uploader_id = media_data['user_id']
        alias = self.get_user_alias(uploader_id) or "Unknown"
        caption = f"📤 Shared by [{alias}](https://t.me/{BOT_USERNAME}?start={alias})"
        
        for user_id in recipients:
            if user_id == uploader_id:  # Skip uploader
                continue
                
            kwargs = {'caption': caption, 'parse_mode': ParseMode.MARKDOWN}
            
            if file_type == 'photo':
                kwargs['photo'] = file_id
            elif file_type == 'video':
                kwargs['video'] = file_id
            elif file_type == 'document':
                kwargs['document'] = file_id
            else:
                continue
            
            task = self.send_message_with_retry(context, int(user_id), **kwargs)
            tasks.append((user_id, task))
        
        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        success_count = 0
        fail_count = 0
        
        for (user_id, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                fail_count += 1
                logger.error(f"Failed to send to {user_id}: {result}")
            elif result is not None:
                success_count += 1
                # Mark as seen
                if user_id not in self.seen_media:
                    self.seen_media[user_id] = []
                self.seen_media[user_id].append(file_id)
            else:
                fail_count += 1
        
        return success_count, fail_count

    async def broadcast_message_batch(self, context: ContextTypes.DEFAULT_TYPE,
                                    recipients: List[str], message: str, 
                                    sender_alias: str) -> Tuple[int, int]:
        """Broadcast text message to a batch of recipients"""
        tasks = []
        
        for user_id in recipients:
            task = self.send_message_with_retry(
                context, int(user_id),
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
            tasks.append((user_id, task))
        
        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        success_count = 0
        fail_count = 0
        
        for (user_id, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                fail_count += 1
            elif result is not None:
                success_count += 1
                # Store message mapping for replies
                self.message_id_to_alias[result.message_id] = sender_alias
            else:
                fail_count += 1
        
        return success_count, fail_count

    async def start_background_tasks(self):
        """Start background processing tasks"""
        # Background task for processing broadcast queue
        asyncio.create_task(self.process_broadcast_queue())
        # Background task for processing media queue
        asyncio.create_task(self.process_media_queue())

    async def process_broadcast_queue(self):
        """Process broadcast messages in batches"""
        while True:
            try:
                # Wait for items in queue
                item = await self.broadcast_queue.get()
                if item is None:  # Shutdown signal
                    break
                
                context, recipients, message_data, message_type = item
                
                # Process in batches
                for i in range(0, len(recipients), BATCH_SIZE):
                    batch = recipients[i:i + BATCH_SIZE]
                    
                    if message_type == 'media':
                        await self.broadcast_media_batch(context, batch, message_data)
                    elif message_type == 'text':
                        message, sender_alias = message_data
                        await self.broadcast_message_batch(context, batch, message, sender_alias)
                    
                    # Small delay between batches
                    if i + BATCH_SIZE < len(recipients):
                        await asyncio.sleep(SEND_DELAY)
                
                self.broadcast_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in broadcast queue processing: {e}")
                await asyncio.sleep(1)

    async def process_media_queue(self):
        """Process media downloads and storage"""
        while True:
            try:
                item = await self.media_processing_queue.get()
                if item is None:  # Shutdown signal
                    break
                
                file_obj, file_id, file_type, user_id, message_id = item
                
                # Download media asynchronously
                file_path = await self.download_media_async(file_obj, file_id, file_type)
                
                if file_path:
                    # Save media info to database
                    self.media[file_id] = {
                        'file_id': file_id,
                        'user_id': user_id,
                        'file_type': file_type,
                        'file_path': file_path,
                        'message_id': message_id,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # Update user stats
                    if user_id in self.users:
                        user_data = self.users[user_id]
                        user_data['media_count'] = user_data.get('media_count', 0) + 1
                        user_data['daily_media_count'] = user_data.get('daily_media_count', 0) + 1
                        user_data['last_active'] = datetime.now().isoformat()
                        
                        # Activate user if needed (30 media threshold)
                        if not user_data.get('is_active', False) and not user_data.get('is_premium', False):
                            if user_data.get('daily_media_count', 0) >= 30:
                                user_data['is_active'] = True
                                self.last_cache_update = datetime.min  # Invalidate cache
                    
                    await self.save_data_async()
                
                self.media_processing_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in media queue processing: {e}")
                await asyncio.sleep(1)

# Initialize bot
vault_bot = VaultBot()

def reset_daily_counts():
    """Reset daily media counts for non-premium users"""
    now = datetime.now()
    
    for user_id, user_data in vault_bot.users.items():
        last_reset_str = user_data.get('last_daily_reset')
        
        if last_reset_str:
            try:
                last_reset = datetime.strptime(last_reset_st
