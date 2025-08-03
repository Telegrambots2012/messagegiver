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
