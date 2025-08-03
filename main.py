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
from typing import Optional, List, Dict, Tuple
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)
from telegram.error import RetryAfter, NetworkError, BadRequest
import time
from concurrent.futures import ThreadPoolExecutor
import weakref

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

# Performance and Rate Limiting Settings
MAX_CONCURRENT_UPLOADS = 50
MAX_CONCURRENT_DOWNLOADS = 20
BATCH_SIZE = 10
FLOOD_WAIT_MULTIPLIER = 1.5
MAX_RETRIES = 3
CHUNK_SIZE = 8192  # For file operations

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thread pool for CPU-bound operations
thread_pool = ThreadPoolExecutor(max_workers=10)

class AsyncRateLimiter:
    """Rate limiter to prevent flood waits"""
    def __init__(self):
        self.last_request = {}
        self.request_counts = {}
        self.locks = weakref.WeakValueDictionary()

    async def wait_if_needed(self, chat_id: int, min_interval: float = 0.1):
        """Wait if needed to avoid rate limiting"""
        if chat_id not in self.locks:
            self.locks[chat_id] = asyncio.Lock()
        
        async with self.locks[chat_id]:
            now = time.time()
            last_time = self.last_request.get(chat_id, 0)
            time_diff = now - last_time
            
            if time_diff < min_interval:
                wait_time = min_interval - time_diff
                await asyncio.sleep(wait_time)
            
            self.last_request[chat_id] = time.time()

rate_limiter = AsyncRateLimiter()

class VaultBot:
    def __init__(self):
        self.users = {}
        self.media = {}
        self.access_keys = {}
        self.seen_media = {}
        self.message_id_to_alias = {}
        self.upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.file_locks = weakref.WeakValueDictionary()
        self.init_storage()
        asyncio.create_task(self.load_data_async())

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
            logger.error(f"Error initializing storage: {repr(e)}")

    async def load_data_async(self):
        """Load data from JSON files asynchronously"""
        try:
            async with aiofiles.open(USERS_FILE, 'r') as f:
                content = await f.read()
                self.users = json.loads(content) if content else {}

            async with aiofiles.open(MEDIA_FILE, 'r') as f:
                content = await f.read()
                self.media = json.loads(content) if content else {}

            async with aiofiles.open(KEYS_FILE, 'r') as f:
                content = await f.read()
                self.access_keys = json.loads(content) if content else {}

            async with aiofiles.open(SEEN_MEDIA_FILE, 'r') as f:
                content = await f.read()
                self.seen_media = json.loads(content) if content else {}

            logger.info("Data loaded successfully")
        except Exception as e:
            logger.error(f"Error loading data: {repr(e)}")

    async def save_data_async(self):
        """Save data to JSON files asynchronously"""
        try:
            tasks = []

            async def save_file(file_path, data):
                async with aiofiles.open(file_path, 'w') as f:
                    await f.write(json.dumps(data, indent=2))

            tasks.append(save_file(USERS_FILE, self.users))
            tasks.append(save_file(MEDIA_FILE, self.media))
            tasks.append(save_file(KEYS_FILE, self.access_keys))
            tasks.append(save_file(SEEN_MEDIA_FILE, self.seen_media))

            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error saving data: {repr(e)}")

    async def download_media_optimized(self, file, file_id: str, file_type: str) -> Optional[str]:
        """Download media file to VPS storage with optimization"""
        async with self.download_semaphore:
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

                # Check if file already exists
                if os.path.exists(file_path):
                    return file_path

                # Use file lock to prevent concurrent downloads of same file
                if file_id not in self.file_locks:
                    self.file_locks[file_id] = asyncio.Lock()

                async with self.file_locks[file_id]:
                    # Double check after acquiring lock
                    if os.path.exists(file_path):
                        return file_path

                    # Download with chunked reading for large files
                    temp_path = f"{file_path}.tmp"
                    await file.download_to_drive(temp_path)

                    # Atomic move
                    os.rename(temp_path, file_path)

                return file_path

            except Exception as e:
                logger.error(f"Error downloading media {file_id}: {repr(e)}")

                # Clean up temp file if exists
                temp_path = f"{file_path}.tmp"
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
                return None

    def get_active_users(self) -> List[str]:
        """Get list of active user IDs"""
        return [
            user_id for user_id, user_data in self.users.items()
            if not user_data.get('is_banned', False) and user_data.get('is_active', False)
        ]

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
        asyncio.create_task(self.save_data_async())

    def validate_key(self, key: str) -> Optional[bool]:
        """Validate access key and return premium status"""
        if key in self.access_keys:
            key_data = self.access_keys[key]
            if key_data['uses_remaining'] > 0:
                key_data['uses_remaining'] -= 1
                asyncio.create_task(self.save_data_async())
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
        asyncio.create_task(self.save_data_async())
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

# Initialize bot
vault_bot = VaultBot()

async def safe_send_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, **kwargs) -> bool:
    """Safely send message with retry logic and flood wait handling"""
    for attempt in range(MAX_RETRIES):
        try:
            await rate_limiter.wait_if_needed(chat_id)
            await context.bot.send_message(chat_id=chat_id, **kwargs)
            return True
        except RetryAfter as e:
            wait_time = e.retry_after * FLOOD_WAIT_MULTIPLIER
            logger.warning(f"Rate limited for {wait_time}s, waiting...")
            await asyncio.sleep(wait_time)
        except (NetworkError, TimeLimitExceeded) as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Network error sending to {chat_id}: {e}")
        except BadRequest as e:
            if "bot was blocked" in str(e).lower():
                # Mark user as inactive
                user_id = str(chat_id)
                if user_id in vault_bot.users:
                    vault_bot.users[user_id]['is_active'] = False
                    await vault_bot.save_data_async()
                logger.info(f"User {chat_id} blocked bot, marked inactive")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending to {chat_id}: {e}")
            return False
    return False

async def safe_send_media(context: ContextTypes.DEFAULT_TYPE, chat_id: int, media_type: str, 
                         file_data, **kwargs) -> bool:
    """Safely send media with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            await rate_limiter.wait_if_needed(chat_id, min_interval=0.2)  # Longer interval for media
            
            if media_type == 'photo':
                await context.bot.send_photo(chat_id=chat_id, photo=file_data, **kwargs)
            elif media_type == 'video':
                await context.bot.send_video(chat_id=chat_id, video=file_data, **kwargs)
            elif media_type == 'document':
                await context.bot.send_document(chat_id=chat_id, document=file_data, **kwargs)
            
            return True
            
        except RetryAfter as e:
            wait_time = e.retry_after * FLOOD_WAIT_MULTIPLIER
            logger.warning(f"Rate limited for {wait_time}s, waiting...")
            await asyncio.sleep(wait_time)
        except (NetworkError, TimeLimitExceeded) as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"Network error sending media to {chat_id}: {e}")
        except BadRequest as e:
            if "bot was blocked" in str(e).lower():
                user_id = str(chat_id)
                if user_id in vault_bot.users:
                    vault_bot.users[user_id]['is_active'] = False
                    await vault_bot.save_data_async()
            elif "file too large" in str(e).lower():
                logger.warning(f"File too large for {chat_id}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending media to {chat_id}: {e}")
            return False
    return False

def reset_daily_counts():
    """Reset daily media counts for non-premium users"""
    now = datetime.now()
    
    for user_id, user_data in vault_bot.users.items():
        last_reset_str = user_data.get('last_daily_reset')
        
        if last_reset_str:
            try:
                last_reset = datetime.strptime(last_reset_str, "%Y-%m-%d")
            except:
                last_reset = datetime.strptime(last_reset_str, "%Y-%m-%d %H:%M:%S")
        else:
            last_reset = None

        if not last_reset or (now - last_reset) >= timedelta(hours=24):
            user_data['daily_media_count'] = 0
            if not user_data.get('is_premium', False):
                user_data['is_active'] = False
            user_data['last_daily_reset'] = now.strftime("%Y-%m-%d")
    
    asyncio.create_task(vault_bot.save_data_async())

async def is_banned_user(update: Update) -> bool:
    """Check if user is banned"""
    user_id = str(update.effective_user.id)
    return vault_bot.users.get(user_id, {}).get('is_banned', False)

from functools import wraps

def ban_protected():
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            if await is_banned_user(update):
                await update.message.reply_text("❌ You are banned to use this bot.")
                return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

async def periodic_reset():
    """Periodic reset task"""
    while True:
        try:
            reset_daily_counts()
            logger.info("✅ Daily media counts reset.")
            await asyncio.sleep(24 * 60 * 60)  # 24 hours
        except Exception as e:
            logger.error(f"Error in periodic reset: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

# ---------- Registration Handler ----------
@ban_protected()
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    username = user.username or "anonymous"

    if vault_bot.is_user_registered(user_id):
        alias = vault_bot.get_user_alias(user_id)
        await update.message.reply_text(f"🎭 Welcome back, {alias}! Use /syncmedia to continue.")
        return

    if context.args:
        key = context.args[0]
        is_premium = vault_bot.validate_key(key)
        if is_premium is not None:
            alias = vault_bot.generate_alias()
            vault_bot.register_user(user_id, username, alias, is_premium)
            await update.message.reply_text(
                f"✅ Registered as *{alias}*\n"
                f"{'💎 *Premium Access Enabled*' if is_premium else '🧍 *Normal Access*'}\n\n"
                "Use /syncmedia to get media.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text("❌ Invalid or expired access key.")
    else:
        welcome_message = (
            "🔐 *Welcome to the Private Media Vault!*\n\n"
            "You've discovered a *secret space* where premium anonymous content is shared by trusted members only.\n\n"
            "To enter the vault, you'll need a verified Access Key 🔑\n\n"
            "💬 *Want your key?*\n"
            "Message [@void_realm](https://t.me/void_realm) now and request exclusive access.\n"
            "_(Only serious users are accepted.)_\n\n"
            "Once approved, use:\n"
            "`/start <your_key>`\n\n"
            "—\n\n"
            "📖 Need help using the bot?\n"
            "Type or tap 👉 /help to view all commands"
        )
        await update.message.reply_text(welcome_message, parse_mode=ParseMode.MARKDOWN)

        try:
            await context.bot.pin_chat_message(
                chat_id=update.effective_chat.id,
                message_id=update.message.message_id
            )
        except Exception as e:
            logger.warning(f"Could not pin message: {e}")

async def getkey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate access keys (Admin only)"""
    user_id = str(update.effective_user.id)
    
    # Check if user is owner or admin
    if int(user_id) == OWNER_ID:
        is_admin = True
    else:
        is_admin = vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /getkey <uses> [premium]")
        return

    uses = int(context.args[0])
    is_premium = len(context.args) > 1 and context.args[1].lower() == "premium"
    key = vault_bot.create_access_key(uses, user_id, is_premium)
    label = "💎 Premium" if is_premium else "🧍 Normal"

    await update.message.reply_text(
        f"🔑 <b>{label} Key Created</b>:\n<code>{key}</code>\n\nUse: https://t.me/{BOT_USERNAME}?start={key}",
        parse_mode=ParseMode.HTML
    )

@ban_protected()
async def mystats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user statistics"""
    user_id = str(update.effective_user.id)
    user_data = vault_bot.users.get(user_id)
    
    if not user_data:
        await update.message.reply_text("❌ You're not registered.")
        return

    alias = user_data['alias']
    media_count = user_data.get('media_count', 0)
    daily_count = user_data.get('daily_media_count', 0)
    is_premium = user_data.get('is_premium', False)
    is_active = user_data.get('is_active', False)
    status = "💎 Premium ✅ Active" if is_premium else ("✅ Active" if is_active else "⚠️ Inactive (Send 30 media to re-activate)")

    msg = (
        f"📊 *Your Stats*\n"
        f"🎭 Alias: `{alias}`\n"
        f"📁 Total Media: `{media_count}`\n"
        f"📆 Today's Uploads: `{daily_count}`\n"
        f"🔰 Status: {status}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

@ban_protected()
async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top contributors"""
    user_id = str(update.effective_user.id)
    
    if not vault_bot.users.get(user_id):
        await update.message.reply_text(
            "❌ You need to register first to use this command.\nUse your key with: /start <key>",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Get top users by media count
    top_users = sorted(
        [(data['alias'], data.get('media_count', 0)) for data in vault_bot.users.values() 
         if not data.get('is_banned', False)],
        key=lambda x: x[1], reverse=True
    )[:5]

    if not top_users:
        await update.message.reply_text("🏆 No contributors yet.")
        return

    msg = "🏆 *Top Contributors*\n\n"
    for i, (alias, count) in enumerate(top_users, start=1):
        msg += f"{i}. {alias} — {count} media\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban user (Admin only)"""
    user_id = str(update.effective_user.id)
    
    if int(user_id) == OWNER_ID:
        is_admin = True
    else:
        is_admin = vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    # Ban by reply
    if update.message.reply_to_message:
        replied_user_id = str(update.message.reply_to_message.from_user.id)
        
        if replied_user_id in vault_bot.users:
            vault_bot.users[replied_user_id]['is_banned'] = True
            await vault_bot.save_data_async()
            await update.message.reply_text(f"✅ User ID `{replied_user_id}` has been banned.", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ User not found in database.")
        return

    # Ban by alias
    if context.args:
        alias_input = " ".join(context.args).strip().lower()
        
        matched_user = None
        for uid, data in vault_bot.users.items():
            if alias_input in data['alias'].lower():
                matched_user = (uid, data)
                break

        if not matched_user:
            await update.message.reply_text(f"❌ Alias `{alias_input}` not found.", parse_mode=ParseMode.MARKDOWN)
            return

        user_id_to_ban, user_data = matched_user
        vault_bot.users[user_id_to_ban]['is_banned'] = True
        await vault_bot.save_data_async()
        
        await update.message.reply_text(f"✅ User `{user_data['alias']}` has been banned.", parse_mode=ParseMode.MARKDOWN)
        return

    await update.message.reply_text("❌ Usage: /ban <alias> or reply to a user's message to ban them.")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban user (Admin only)"""
    user_id = str(update.effective_user.id)
    
    if int(user_id) == OWNER_ID:
        is_admin = True
    else:
        is_admin = vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    # Unban by reply
    if update.message.reply_to_message:
        replied_user_id = str(update.message.reply_to_message.from_user.id)
        
        if replied_user_id in vault_bot.users:
            vault_bot.users[replied_user_id]['is_banned'] = False
            await vault_bot.save_data_async()
            await update.message.reply_text(f"✅ User ID `{replied_user_id}` has been unbanned.", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ User not found in database.")
        return

    # Unban by alias
    if context.args:
        alias_input = " ".join(context.args).strip().lower()
        
        matched_user = None
        for uid, data in vault_bot.users.items():
            if alias_input in data['alias'].lower():
                matched_user = (uid, data)
                break

        if not matched_user:
            await update.message.reply_text(f"❌ Alias `{alias_input}` not found.", parse_mode=ParseMode.MARKDOWN)
            return

        user_id_to_unban, user_data = matched_user
        vault_bot.users[user_id_to_unban]['is_banned'] = False
        await vault_bot.save_data_async()
        
        await update.message.reply_text(f"✅ User `{user_data['alias']}` has been unbanned.", parse_mode=ParseMode.MARKDOWN)
        return

    await update.message.reply_text("❌ Usage: /unban <alias> or reply to a user's message to unban them.")

async def pin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pin message (Admin only)"""
    user_id = str(update.effective_user.id)

    # Make OWNER_ID auto-admin
    if int(user_id) == OWNER_ID:
        if user_id in vault_bot.users:
            vault_bot.users[user_id]['is_admin'] = True
            await vault_bot.save_data_async()

    # Check if admin
    is_admin = vault_bot.users.get(user_id, {}).get('is_admin', False)
    if not is_admin:
        await update.message.reply_text("❌ Only admins can pin messages.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to the message you want to pin.")
        return

    try:
        await context.bot.pin_chat_message(
            chat_id=update.effective_chat.id,
            message_id=update.message.reply_to_message.message_id,
            disable_notification=True
        )
        await update.message.reply_text("📌 Message pinned successfully.")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to pin message: {e}")

@ban_protected()
async def syncmedia(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sync media for users with optimized performance"""
    user_id = str(update.effective_user.id)
    user_data = vault_bot.users.get(user_id)
    
    if not user_data:
        await update.message.reply_text("❌ You are not registered.")
        return

    is_premium = user_data.get('is_premium', False)
    is_active = user_data.get('is_active', False)

    # Inactive & Not Premium
    if not is_premium and not is_active:
        await update.message.reply_text(
            "⚠️ You're currently *inactive*.\n"
            "To activate your access, please send at least *30 media files* within 24 hours.\n\n"
            "Once activated, you can upgrade to premium for full access.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Active but Not Premium
    if not is_premium:
        await update.message.reply_text(
            "🚫 *This feature is reserved for 💎 Premium users only.*\n\n"
            "You've been activated as a regular member, but only premium users can access synced content.\n\n"
            "✨ *Upgrade now to unlock the full vault!*\n"
            "👉 Message [@void_realm](https://t.me/Void_realm)",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Get seen media for user
    seen = set(vault_bot.seen_media.get(user_id, []))

    # Get media to send
    if is_premium and not seen:
        # Send all media for new premium users
        media_to_send = list(vault_bot.media.values())
    else:
        # Send only recent media (last 24 hours)
        since = datetime.now() - timedelta(hours=24)
        media_to_send = [
            media for media in vault_bot.media.values()
            if datetime.fromisoformat(media['timestamp']) >= since
        ]
    
    # Filter out already seen media
    media_to_send = [media for media in media_to_send if media['file_id'] not in seen]

    if not media_to_send:
        await update.message.reply_text("✅ No new media to sync!")
        return

    # Send progress message
    progress_msg = await update.message.reply_text(f"🔄 Syncing {len(media_to_send)} media items...")

    count = 0
    failed_count = 0
    
    # Process media in batches for better performance
    for i in range(0, len(media_to_send), BATCH_SIZE):
        batch = media_to_send[i:i + BATCH_SIZE]
        tasks = []
        
        for media in batch:
            task = send_single_media(context, user_id, media)
            tasks.append(task)
        
        # Execute batch concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for j, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error sending media: {result}")
                failed_count += 1
            elif result:
                # Mark as seen
                if user_id not in vault_bot.seen_media:
                    vault_bot.seen_media[user_id] = []
                vault_bot.seen_media[user_id].append(batch[j]['file_id'])
                count += 1
            else:
                failed_count += 1

        # Update progress every batch
        try:
            await progress_msg.edit_text(f"🔄 Synced: {count}/{len(media_to_send)} | Failed: {failed_count}")
        except:
            pass

    await vault_bot.save_data_async()
    
    # Final message
    try:
        await progress_msg.edit_text(f"✅ Sync complete! Sent: {count} | Failed: {failed_count}")
    except:
        await update.message.reply_text(f"✅ Sync complete! Sent: {count} | Failed: {failed_count}")

async def send_single_media(context: ContextTypes.DEFAULT_TYPE, user_id: str, media: dict) -> bool:
    """Send a single media item with error handling"""
    try:
        file_id = media['file_id']
        file_type = media['file_type']
        uploader_id = media['user_id']
        file_path = media.get('file_path')
        
        alias = vault_bot.get_user_alias(uploader_id) or "Unknown"
        caption = f"📤 Shared by [{alias}](https://t.me/{BOT_USERNAME}?start={alias})"

        # Try to send from file_id first (if still valid)
        try:
            return await safe_send_media(context, int(user_id), file_type, file_id, 
                                       caption=caption, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            # If file_id fails, try to send from stored file
            if file_path and os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    return await safe_send_media(context, int(user_id), file_type, f, 
                                               caption=caption, parse_mode=ParseMode.MARKDOWN)
            return False
            
    except Exception as e:
        logger.error(f"Error in send_single_media: {e}")
        return False

async def handle_anon_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle anonymous messages with optimized broadcasting"""
    user_id = str(update.effective_user.id)
    user_data = vault_bot.users.get(user_id)
    
    if not user_data or user_data.get('is_banned', False) or not user_data.get('is_active', False):
        return  # Banned or inactive users are ignored

    sender_alias = user_data['alias']
    message = update.message.text

    # Block messages with links or usernames
    if not message or any(keyword in message.lower() for keyword in ["http://", "https://", "t.me/", "@"]):
        return

    # Check if this is a reply
    reply_to_msg = update.message.reply_to_message
    if reply_to_msg and reply_to_msg.message_id in vault_bot.message_id_to_alias:
        replied_alias = vault_bot.message_id_to_alias[reply_to_msg.message_id]
        message_to_send = f"🔁 *{sender_alias} replied to {replied_alias}*:\n{message}"
    else:
        message_to_send = f"🧑‍💬 *{sender_alias}*: {message}"

    # Broadcast to all active users except sender
    active_users = [uid for uid in vault_bot.get_active_users() if uid != user_id]
    
    if not active_users:
        return

    # Send messages in batches
    tasks = []
    for target_id in active_users:
        task = safe_send_message(context, int(target_id), text=message_to_send, parse_mode=ParseMode.MARKDOWN)
        tasks.append(task)
    
    # Execute all sends concurrently
    await asyncio.gather(*tasks, return_exceptions=True)

async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle media uploads with optimized performance and 2GB support"""
    async with vault_bot.upload_semaphore:
        user_id = str(update.effective_user.id)

        # Block unregistered users
        if not vault_bot.users.get(user_id):
            await update.message.reply_text("❌ You are not registered. Use a valid key to join the bot.")
            return

        # Ban check
        if await is_banned_user(update):
            await update.message.reply_text("❌ You are banned to use this bot.")
            return

        message = update.message
        file_type = None
        file_id = None
        file_obj = None
        file_size = 0

        # Determine media type and check size (2GB = 2147483648 bytes)
        if message.photo:
            file_id = message.photo[-1].file_id
            file_type = 'photo'
            file_size = message.photo[-1].file_size or 0
        elif message.video:
            file_id = message.video.file_id
            file_type = 'video'
            file_size = message.video.file_size or 0
        elif message.document:
            file_id = message.document.file_id
            file_type = 'document'
            file_size = message.document.file_size or 0
        else:
            return

        # Check file size limit (2GB)
        if file_size > 2147483648:  # 2GB in bytes
            await update.message.reply_text(
                "❌ File too large! Maximum size allowed is 2GB.\n"
                f"Your file: {file_size / (1024*1024*1024):.2f} GB"
            )
            return

        try:
            file_obj = await context.bot.get_file(file_id)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to process file: {e}")
            return

        # Show upload progress for large files
        progress_msg = None
        if file_size > 50 * 1024 * 1024:  # Show progress for files > 50MB
            progress_msg = await update.message.reply_text("📤 Uploading large file...")

        # Download media to VPS storage
        file_path = await vault_bot.download_media_optimized(file_obj, file_id, file_type)
        if not file_path:
            if progress_msg:
                await progress_msg.edit_text("❌ Failed to save media to storage.")
            else:
                await update.message.reply_text("❌ Failed to save media to storage.")
            return

        # Save media info to database
        vault_bot.media[file_id] = {
            'file_id': file_id,
            'user_id': user_id,
            'file_type': file_type,
            'file_path': file_path,
            'file_size': file_size,
            'message_id': message.message_id,
            'timestamp': datetime.now().isoformat()
        }

        # Update user media counters
        user_data = vault_bot.users[user_id]
        user_data['media_count'] = user_data.get('media_count', 0) + 1
        user_data['daily_media_count'] = user_data.get('daily_media_count', 0) + 1
        user_data['last_active'] = datetime.now().isoformat()

        # Activate user if needed (30 media threshold)
        if not user_data.get('is_active', False) and not user_data.get('is_premium', False):
            if user_data.get('daily_media_count', 0) >= 30:
                user_data['is_active'] = True
                await update.message.reply_text("🎉 Congratulations! You are now *activated*!", parse_mode=ParseMode.MARKDOWN)

        await vault_bot.save_data_async()

        # Broadcast to all active users except uploader
        active_users = [uid for uid in vault_bot.get_active_users() if uid != user_id]
        alias = vault_bot.get_user_alias(user_id)
        full_caption = f"📤 Shared by [{alias}](https://t.me/{BOT_USERNAME}?start={alias})"

        if progress_msg:
            await progress_msg.edit_text(f"📡 Broadcasting to {len(active_users)} users...")

        # Broadcast in batches for better performance
        success_count = 0
        for i in range(0, len(active_users), BATCH_SIZE):
            batch_users = active_users[i:i + BATCH_SIZE]
            tasks = []
            
            for uid in batch_users:
                task = broadcast_single_media(context, uid, file_id, file_type, full_caption)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error broadcasting to {batch_users[j]}: {result}")
                elif result:
                    # Mark media as seen
                    uid = batch_users[j]
                    if uid not in vault_bot.seen_media:
                        vault_bot.seen_media[uid] = []
                    vault_bot.seen_media[uid].append(file_id)
                    success_count += 1

        await vault_bot.save_data_async()
        
        # Update final status
        if progress_msg:
            await progress_msg.edit_text(f"✅ Media shared with {success_count}/{len(active_users)} users!")
        elif success_count > 0:
            await update.message.reply_text(f"✅ Media shared with {success_count} users!")

async def broadcast_single_media(context: ContextTypes.DEFAULT_TYPE, user_id: str, 
                               file_id: str, file_type: str, caption: str) -> bool:
    """Broadcast single media to a user"""
    try:
        return await safe_send_media(context, int(user_id), file_type, file_id, 
                                   caption=caption, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error broadcasting to {user_id}: {e}")
        return False

@ban_protected()
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message"""
    help_text = (
        "👋 *Welcome to the Media Vault!*\n\n"
        "Here's what you can do:\n\n"
        "🔐 /start <key> – Register using an access key\n"
        "🧲 /syncmedia – Get new media from vault\n"
        "📊 /mystats – See your stats and progress\n"
        "🏆 /top – View top contributors\n"
        "📌 /pin – Pin a message (admins only)\n"
        "🚨 /report – Report inappropriate media\n\n"
        "💾 *File Support:* Up to 2GB files supported!\n"
        "⚡️ *Performance:* Optimized for multiple users\n\n"
        "❓ Need access? Message [@void_realm](https://t.me/void_realm)\n"
        "🔗 Bot: [@Tezterro_bot](https://t.me/Tezterro_bot)"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin panel"""
    user_id = str(update.effective_user.id)
    is_admin = (int(user_id) == OWNER_ID) or vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    # Promote another user to admin
    if context.args:
        try:
            promote_id = str(int(context.args[0]))
        except:
            await update.message.reply_text("❌ Invalid user ID.")
            return

        if promote_id in vault_bot.users:
            vault_bot.users[promote_id]['is_admin'] = True
            await vault_bot.save_data_async()
            await update.message.reply_text(f"✅ User ID {promote_id} is now an admin.", parse_mode=ParseMode.MARKDOWN)

            # Notify the new admin
            admin_info = (
                "🎉 You've been promoted to *Admin*!\n\n"
                "Here are your powers:\n\n"
                "🎟️ /getkey <uses> [premium] – Generate access key\n"
                "📌 /pin – Pin a message\n"
                "🚫 /ban <alias> – Ban a user\n"
                "✅ /unban <alias> – Unban a user\n"
                "💎 /upgrade <user_id> – Upgrade to Premium\n"
                "📢 /broadcast (reply) – Message all users\n"
                "🗑️ /delete (reply) – Delete any media/message\n"
                "🔄 /reset – Reset daily limits\n"
                "🆔 /admin <user_id> – Promote new admin"
            )

            try:
                await context.bot.send_message(chat_id=int(promote_id), text=admin_info, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                await update.message.reply_text(f"⚠️ Admin promoted but couldn't DM user.\n{e}")
        else:
            await update.message.reply_text(f"❌ User ID {promote_id} not found.", parse_mode=ParseMode.MARKDOWN)
        return

    # Show Admin Panel
    total_users = len(vault_bot.users)
    active_users = len(vault_bot.get_active_users())
    total_media = len(vault_bot.media)
    
    admin_text = (
        "👑 *Admin Panel*\n\n"
        f"📊 *Stats:*\n"
        f"• Total Users: {total_users}\n"
        f"• Active Users: {active_users}\n"
        f"• Total Media: {total_media}\n\n"
        "🎟️ /getkey <uses> [premium] – Generate access key\n"
        "🚫 /ban <alias> – Ban a user\n"
        "✅ /unban <alias> – Unban a user\n"
        "💎 /upgrade <user_id> – Upgrade to Premium\n"
        "📢 /broadcast (reply) – Message all active users\n"
        "📌 /pin – Pin a message in group\n"
        "🗑️ /delete (reply) – Delete any message/media\n"
        "🔄 /reset – Reset daily limits\n"
        "🆔 /admin <user_id> – Promote user as admin\n"
        "🧹 /cleanup – Clean up old media files"
    )
    await update.message.reply_text(admin_text, parse_mode=ParseMode.MARKDOWN)

@ban_protected()
async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Report media/message"""
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to the media or message you want to report.")
        return

    reporter_id = str(update.effective_user.id)
    user_data = vault_bot.users.get(reporter_id)
    
    if not user_data or user_data.get('is_banned', False) or not user_data.get('is_active', False):
        await update.message.reply_text("❌ Only *active* users can use /report.", parse_mode=ParseMode.MARKDOWN)
        return

    reporter_alias = user_data.get('alias', 'Unknown')
    reported_msg = update.message.reply_to_message
    reported_mid = reported_msg.message_id

    # Try finding uploader from media DB
    uploader_alias = "Unknown"
    for media in vault_bot.media.values():
        if media['message_id'] == reported_mid:
            uploader_alias = vault_bot.get_user_alias(media['user_id']) or "Unknown"
            break

    # Extract content
    content = reported_msg.caption or reported_msg.text or "⚠️ No text or caption available."
    content = content.replace("`", "'")  # Escape backticks

    # Report message
    report_text = (
        f"🚨 *New Report*\n\n"
        f"👤 Reporter: *{reporter_alias}* (ID: `{reporter_id}`)\n"
        f"📤 Reported User (Uploader): *{uploader_alias}*\n"
        f"🧾 Message ID: `{reported_mid}`\n\n"
        f"📩 *Reported Content:*\n"
        f"`{content}`"
    )

    # Send to OWNER and all admins concurrently
    admin_ids = [str(OWNER_ID)]
    for uid, user_data in vault_bot.users.items():
        if user_data.get('is_admin', False) and int(uid) != OWNER_ID:
            admin_ids.append(uid)

    tasks = []
    for admin_id in admin_ids:
        task = safe_send_message(context, int(admin_id), text=report_text, parse_mode=ParseMode.MARKDOWN)
        tasks.append(task)
    
    await asyncio.gather(*tasks, return_exceptions=True)
    await update.message.reply_text("✅ Report submitted to admins.")

@ban_protected()
async def delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete media/message (Admin only)"""
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to the media/message you want to delete.")
        return

    deleter_id = str(update.effective_user.id)
    is_admin = (int(deleter_id) == OWNER_ID) or vault_bot.users.get(deleter_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("⛔️ Only admins can delete.")
        return

    target_msg = update.message.reply_to_message
    target_mid = target_msg.message_id

    # Try to delete message
    try:
        await target_msg.delete()
        await update.message.reply_text("🗑 Message deleted.")

        # Remove from media DB and delete file
        for file_id, media in list(vault_bot.media.items()):
            if media['message_id'] == target_mid:
                # Delete physical file
                file_path = media.get('file_path')
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.info(f"Deleted file: {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete file {file_path}: {e}")
                
                # Remove from database
                del vault_bot.media[file_id]
                break
        
        await vault_bot.save_data_async()

    except Exception as e:
        await update.message.reply_text("⚠️ Could not delete message.")
        logger.error(f"Delete error: {e}")

async def manual_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual reset (Admin only)"""
    user_id = str(update.effective_user.id)
    is_admin = (int(user_id) == OWNER_ID) or vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ Only owner or admin can use this command.")
        return

    reset_daily_counts()
    await update.message.reply_text("🔁 All non-premium users have been reset.\nThey need to send 30 media again to activate.")

async def upgrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upgrade user to premium (Admin only)"""
    user_id = str(update.effective_user.id)
    is_admin = (int(user_id) == OWNER_ID) or vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /upgrade <user_id>")
        return

    target_user_id = str(int(context.args[0]))
    
    if target_user_id not in vault_bot.users:
        await update.message.reply_text("❌ User not found.")
        return

    user_data = vault_bot.users[target_user_id]
    if user_data.get('is_premium', False):
        await update.message.reply_text("ℹ️ User is already Premium.")
        return

    # Upgrade user
    user_data['is_premium'] = True
    user_data['is_active'] = True
    await vault_bot.save_data_async()

    await update.message.reply_text(f"✅ User ID {target_user_id} upgraded to Premium.")

    # Notify the user
    try:
        await context.bot.send_message(
            chat_id=int(target_user_id),
            text="🎉 You've been upgraded to 💎 *Premium* status!\nUse /syncmedia to access full content.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Upgraded but couldn't DM user: {e}")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message (Admin only) with optimized performance"""
    user_id = str(update.effective_user.id)
    is_admin = (int(user_id) == OWNER_ID) or vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to the message you want to broadcast.")
        return

    source_msg = update.message.reply_to_message
    users = vault_bot.get_active_users()
    
    if not users:
        await update.message.reply_text("⚠️ No active users to broadcast to.")
        return

    progress_msg = await update.message.reply_text(f"📡 Broadcasting to {len(users)} users...")

    # Pin in groups (safe check)
    try:
        if update.effective_chat.type in ["group", "supergroup"]:
            await context.bot.pin_chat_message(
                chat_id=update.effective_chat.id,
                message_id=source_msg.message_id
            )
    except Exception as e:
        logger.error(f"Failed to pin: {e}")

    # Broadcast message in batches
    success, fail = 0, 0
    
    for i in range(0, len(users), BATCH_SIZE):
        batch_users = users[i:i + BATCH_SIZE]
        tasks = []
        
        for uid in batch_users:
            if source_msg.text or source_msg.caption:
                caption = source_msg.text or source_msg.caption
                broadcast_caption = f"📢 *Announcement!*\n\n{caption}"
                task = safe_send_message(context, int(uid), text=broadcast_caption, parse_mode=ParseMode.MARKDOWN)
            else:
                task = safe_copy_message(context, int(uid), update.effective_chat.id, source_msg.message_id)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for j, result in enumerate(results):
            uid = batch_users[j]
            if isinstance(result, Exception):
                fail += 1
                logger.error(f"Broadcast error to {uid}: {result}")
            elif result:
                success += 1
            else:
                fail += 1
                # Handle blocked bot case
                if uid in vault_bot.users:
                    vault_bot.users[uid]['is_active'] = False

        # Update progress
        try:
            await progress_msg.edit_text(f"📡 Broadcasting... {success + fail}/{len(users)}")
        except:
            pass

    await vault_bot.save_data_async()
    
    try:
        await progress_msg.edit_text(f"📢 Broadcast complete.\n✅ Sent: {success}\n❌ Failed: {fail}")
    except:
        await update.message.reply_text(f"📢 Broadcast complete.\n✅ Sent: {success}\n❌ Failed: {fail}")

async def safe_copy_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, from_chat_id: int, message_id: int) -> bool:
    """Safely copy message with error handling"""
    try:
        await rate_limiter.wait_if_needed(chat_id)
        await context.bot.copy_message(chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)
        return True
    except Exception as e:
        logger.error(f"Error copying message to {chat_id}: {e}")
        return False

async def cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clean up old media files (Admin only)"""
    user_id = str(update.effective_user.id)
    is_admin = (int(user_id) == OWNER_ID) or vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    try:
        # Get files older than 30 days
        cutoff_date = datetime.now() - timedelta(days=30)
        old_media = []
        
        for file_id, media in vault_bot.media.items():
            try:
                media_date = datetime.fromisoformat(media['timestamp'])
                if media_date < cutoff_date:
                    old_media.append((file_id, media))
            except:
                continue

        if not old_media:
            await update.message.reply_text("✅ No old files to clean up.")
            return

        progress_msg = await update.message.reply_text(f"🧹 Cleaning up {len(old_media)} old files...")
        
        cleaned_count = 0
        failed_count = 0
        
        for file_id, media in old_media:
            try:
                file_path = media.get('file_path')
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    cleaned_count += 1
                
                # Remove from database
                del vault_bot.media[file_id]
                
            except Exception as e:
                logger.error(f"Error cleaning file {file_id}: {e}")
                failed_count += 1

        await vault_bot.save_data_async()
        
        try:
            await progress_msg.edit_text(
                f"🧹 Cleanup complete!\n"
                f"✅ Cleaned: {cleaned_count} files\n"
                f"❌ Failed: {failed_count} files"
            )
        except:
            await update.message.reply_text(
                f"🧹 Cleanup complete!\n"
                f"✅ Cleaned: {cleaned_count} files\n"
                f"❌ Failed: {failed_count} files"
            )

    except Exception as e:
        await update.message.reply_text(f"❌ Cleanup failed: {e}")
        logger.error(f"Cleanup error: {e}")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics (Admin only)"""
    user_id = str(update.effective_user.id)
    is_admin = (int(user_id) == OWNER_ID) or vault_bot.users.get(user_id, {}).get('is_admin', False)

    if not is_admin:
        await update.message.reply_text("❌ You're not authorized to use this.")
        return

    try:
        # Calculate statistics
        total_users = len(vault_bot.users)
        active_users = len([u for u in vault_bot.users.values() if u.get('is_active', False)])
        premium_users = len([u for u in vault_bot.users.values() if u.get('is_premium', False)])
        banned_users = len([u for u in vault_bot.users.values() if u.get('is_banned', False)])
        
        total_media = len(vault_bot.media)
        total_keys = len(vault_bot.access_keys)
        active_keys = len([k for k in vault_bot.access_keys.values() if k.get('uses_remaining', 0) > 0])
        
        # Calculate total storage used
        total_size = 0
        for media in vault_bot.media.values():
            file_path = media.get('file_path')
            if file_path and os.path.exists(file_path):
                try:
                    total_size += os.path.getsize(file_path)
                except:
                    pass
        
        size_gb = total_size / (1024 * 1024 * 1024)
        
        # Top contributors
        top_users = sorted(
            [(data['alias'], data.get('media_count', 0)) for data in vault_bot.users.values() 
             if not data.get('is_banned', False)],
            key=lambda x: x[1], reverse=True
        )[:3]
        
        stats_text = (
            f"📊 *Bot Statistics*\n\n"
            f"👥 *Users:*\n"
            f"• Total: {total_users}\n"
            f"• Active: {active_users}\n"
            f"• Premium: {premium_users}\n"
            f"• Banned: {banned_users}\n\n"
            f"📁 *Media:*\n"
            f"• Total Files: {total_media}\n"
            f"• Storage Used: {size_gb:.2f} GB\n\n"
            f"🔑 *Access Keys:*\n"
            f"• Total: {total_keys}\n"
            f"• Active: {active_keys}\n\n"
            f"🏆 *Top Contributors:*\n"
        )
        
        for i, (alias, count) in enumerate(top_users, 1):
            stats_text += f"• {alias}: {count} media\n"
        
        await update.message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error generating stats: {e}")
        logger.error(f"Stats error: {e}")

async def main():
    """Main function to run the bot with enhanced configuration"""
    # Configure application with optimized settings
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Configure for better performance
    app.bot_data['max_connections'] = 100
    app.bot_data['connection_pool_size'] = 20
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("getkey", getkey))
    app.add_handler(CommandHandler("mystats", mystats))
    app.add_handler(CommandHandler("top", top))
    app.add_handler(CommandHandler("ban", ban))
    app.add_handler(CommandHandler("unban", unban))
    app.add_handler(CommandHandler("pin", pin))
    app.add_handler(CommandHandler("syncmedia", syncmedia))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("reset", manual_reset))
    app.add_handler(CommandHandler("delete", delete))
    app.add_handler(CommandHandler("upgrade", upgrade))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("cleanup", cleanup))
    app.add_handler(CommandHandler("stats", stats))
    
    # Add message handlers
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL, handle_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_anon_message))
    
    # Start background tasks
    asyncio.create_task(periodic_reset())
    
    # Log startup
    logger.info("🚀 Bot started with enhanced performance features:")
    logger.info(f"   • Max concurrent uploads: {MAX_CONCURRENT_UPLOADS}")
    logger.info(f"   • Max concurrent downloads: {MAX_CONCURRENT_DOWNLOADS}")
    logger.info(f"   • Batch size: {BATCH_SIZE}")
    logger.info(f"   • File size limit: 2GB")
    logger.info(f"   • Rate limiting: Enabled")

    # Run the bot with error handling
    try:
        await app.run_polling(
            drop_pending_updates=True,
            pool_timeout=20,
            connect_timeout=20,
            read_timeout=20,
            write_timeout=20
        )
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise

if __name__ == "__main__":
    import asyncio
    import nest_asyncio
    import signal
    import sys

    nest_asyncio.apply()

    def signal_handler(sig, frame):
        logger.info("Shutting down bot gracefully...")
        # Save data before exit
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(vault_bot.save_data_async())
        except:
            pass
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
