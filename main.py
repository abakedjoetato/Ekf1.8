#!/usr/bin/env python3
"""
Emerald's Killfeed - Discord Bot for Deadside PvP Engine
Full production-grade bot with killfeed parsing, stats, economy, and premium features
"""

import asyncio
import logging
import os
import sys
import json
import hashlib
import re
import time
from pathlib import Path

# Clean up any conflicting discord modules before importing
for module_name in list(sys.modules.keys()):
    if module_name == 'discord' or module_name.startswith('discord.'):
        del sys.modules[module_name]

# Import py-cord v2.6.1
try:
    import discord
    from discord.ext import commands
    print(f"✅ Successfully imported py-cord")
except ImportError as e:
    print(f"❌ Error importing py-cord: {e}")
    print("Please ensure py-cord 2.6.1 is installed")
    sys.exit(1)

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.models.database import DatabaseManager
from bot.parsers.killfeed_parser import KillfeedParser
from bot.parsers.historical_parser import HistoricalParser
from bot.parsers.unified_log_parser import UnifiedLogParser

# Load environment variables (optional for Railway)
load_dotenv()

# Detect Railway environment
RAILWAY_ENV = os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_STATIC_URL")
if RAILWAY_ENV:
    print(f"🚂 Running on Railway environment")
else:
    print("🖥️ Running in local/development environment")

# Import Railway keep-alive server
from keep_alive import keep_alive

# Set runtime mode to production
MODE = os.getenv("MODE", "production")
print(f"Runtime mode set to: {MODE}")

# Start keep-alive server for Railway deployment
if MODE == "production" or RAILWAY_ENV:
    print("🚀 Starting Railway keep-alive server...")
    keep_alive()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Command hash calculation function
def compute_command_hash(bot):
    """
    Computes a hash of the application command schema to detect changes.
    This allows us to only sync commands when the structure has changed.

    Args:
        bot: The bot instance with application_commands

    Returns:
        str: SHA-256 hash of the command structure
    """
    # Get commands from the correct attribute
    if hasattr(bot, 'pending_application_commands') and bot.pending_application_commands:
        commands = [cmd.to_dict() for cmd in bot.pending_application_commands]
        cmd_source = "pending_application_commands"
    elif hasattr(bot, 'application_commands') and bot.application_commands:
        commands = [cmd.to_dict() for cmd in bot.application_commands]
        cmd_source = "application_commands"
    else:
        # Fallback - empty command structure will force sync once
        commands = []
        cmd_source = "none_found"

    # Debug for observation
    cmd_count = len(commands)
    logger.info(f"🔍 Computing hash from {cmd_count} commands using {cmd_source}")

    # Sort all commands and their properties for consistent hashing
    raw = json.dumps(commands, sort_keys=True).encode('utf-8')
    hash_value = hashlib.sha256(raw).hexdigest()

    # Log hash details for debugging
    logger.info(f"🔑 Generated command hash: {hash_value[:10]}... from {cmd_count} commands")

    return hash_value

class EmeraldKillfeedBot(commands.Bot):
    """Main bot class for Emerald's Killfeed"""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            status=discord.Status.online,
            activity=discord.Game(name="Emerald's Killfeed v2.0")
        )

        # Initialize variables
        self.db_manager = None
        self.scheduler = AsyncIOScheduler()
        self.killfeed_parser = None
        self.log_parser = None
        self.historical_parser = None
        self.unified_log_parser = None
        self.ssh_connections = []

        # Missing essential properties
        self.assets_path = Path('./assets')
        self.dev_data_path = Path('./dev_data')
        self.dev_mode = os.getenv('DEV_MODE', 'false').lower() == 'true'

        logger.info("Bot initialized in production mode")

    async def load_cogs(self):
        """Load all bot cogs"""
        try:
            # List of cogs to load
            cogs = [
                'bot.cogs.core',
                'bot.cogs.admin_channels',
                'bot.cogs.admin_batch',
                'bot.cogs.linking',
                'bot.cogs.stats',
                'bot.cogs.leaderboards_fixed',
                'bot.cogs.automated_leaderboard',
                'bot.cogs.economy',
                'bot.cogs.gambling',
                'bot.cogs.bounties',
                'bot.cogs.factions',
                'bot.cogs.premium',
                'bot.cogs.parsers'
            ]

            loaded_cogs = []
            failed_cogs = []

            for cog in cogs:
                try:
                    self.load_extension(cog)
                    loaded_cogs.append(cog)
                    logger.info(f"✅ Successfully loaded cog: {cog}")
                except Exception as e:
                    failed_cogs.append(cog)
                    logger.error(f"❌ Failed to load cog {cog}: {e}")

            # Verify commands are registered
            try:
                command_count = len(self.application_commands) if hasattr(self, 'application_commands') else 0
                logger.info(f"📊 Loaded {len(loaded_cogs)}/{len(cogs)} cogs successfully")
                logger.info(f"📊 Total slash commands registered: {command_count}")

                # Debug: List actual commands found
                if command_count > 0:
                    command_names = [cmd.name for cmd in self.application_commands]
                    logger.info(f"🔍 Commands found: {', '.join(command_names)}")
                else:
                    logger.info("ℹ️ Commands will be synced after connection")
            except Exception as e:
                logger.warning(f"Command count check failed: {e}")

            if failed_cogs:
                logger.error(f"❌ Failed cogs: {failed_cogs}")
                return False
            else:
                logger.info("✅ All cogs loaded and commands registered successfully")
                return True

        except Exception as e:
            logger.error(f"❌ Critical failure loading cogs: {e}")
            return False

    async def register_commands_safely(self):
        """
        Rate-limit Safe Guild Command Registration System
        
        Uses per-guild hashing to avoid unnecessary syncing and prevent rate limits.
        Only syncs when commands actually change or for new guilds.
        """
        command_count = len(self.pending_application_commands) if hasattr(self, 'pending_application_commands') else 0
        logger.info(f"📊 {command_count} commands registered locally")

        if command_count == 0:
            logger.warning("⚠️ No commands to sync")
            return

        # Compute current command hash
        current_hash = compute_command_hash(self)
        hash_file_path = "command_hash.txt"
        
        # Read previous hash
        previous_hash = ''
        if os.path.exists(hash_file_path):
            try:
                with open(hash_file_path, 'r') as f:
                    previous_hash = f.read().strip()
            except Exception as e:
                logger.warning(f"⚠️ Could not read previous hash: {e}")

        # Check if commands changed
        hash_changed = current_hash != previous_hash
        
        # Load processed guilds with their hashes
        guild_hash_file = "guild_command_hashes.txt"
        guild_hashes = {}
        if os.path.exists(guild_hash_file):
            try:
                with open(guild_hash_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if ':' in line:
                            guild_id_str, guild_hash = line.split(':', 1)
                            try:
                                guild_hashes[int(guild_id_str)] = guild_hash
                            except ValueError:
                                pass
                logger.info(f"📊 Loaded hashes for {len(guild_hashes)} guilds")
            except Exception as e:
                logger.warning(f"⚠️ Could not read guild hashes: {e}")

        # Determine which guilds need syncing
        guilds_to_sync = []
        for guild in self.guilds:
            guild_needs_sync = (
                hash_changed or  # Commands changed globally
                guild.id not in guild_hashes or  # New guild
                guild_hashes.get(guild.id) != current_hash  # Guild has old hash
            )
            
            if guild_needs_sync:
                guilds_to_sync.append(guild)

        if not guilds_to_sync:
            logger.info(f"✅ All {len(self.guilds)} guilds are up to date")
            return

        logger.info(f"🔄 Need to sync {len(guilds_to_sync)} out of {len(self.guilds)} guilds")
        
        # Limit concurrent syncing to avoid rate limits
        MAX_GUILDS_PER_BATCH = 3
        DELAY_BETWEEN_SYNCS = 5  # seconds
        
        synced_count = 0
        failed_count = 0
        
        for i in range(0, len(guilds_to_sync), MAX_GUILDS_PER_BATCH):
            batch = guilds_to_sync[i:i + MAX_GUILDS_PER_BATCH]
            
            for guild in batch:
                try:
                    logger.info(f"🔄 Syncing commands to {guild.name} (ID: {guild.id})")
                    await self.sync_commands(guild_ids=[guild.id])
                    
                    # Update guild hash
                    guild_hashes[guild.id] = current_hash
                    synced_count += 1
                    
                    logger.info(f"✅ Successfully synced to {guild.name}")
                    
                    # Small delay between individual syncs
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    failed_count += 1
                    error_msg = str(e)
                    if "rate limited" in error_msg.lower():
                        logger.error(f"❌ Rate limited while syncing to {guild.name} - stopping sync")
                        # Save progress and exit
                        self._save_guild_hashes(guild_hashes, guild_hash_file)
                        return
                    else:
                        logger.error(f"❌ Failed to sync to {guild.name}: {e}")
            
            # Longer delay between batches
            if i + MAX_GUILDS_PER_BATCH < len(guilds_to_sync):
                logger.info(f"⏳ Waiting {DELAY_BETWEEN_SYNCS}s before next batch...")
                await asyncio.sleep(DELAY_BETWEEN_SYNCS)

        # Save updated guild hashes
        self._save_guild_hashes(guild_hashes, guild_hash_file)
        
        # Save global hash
        try:
            with open(hash_file_path, 'w') as f:
                f.write(current_hash)
        except Exception as e:
            logger.warning(f"⚠️ Could not save command hash: {e}")

        logger.info(f"🎉 Sync complete: {synced_count} success, {failed_count} failed")

    def _save_guild_hashes(self, guild_hashes: dict, file_path: str):
        """Save guild command hashes to file"""
        try:
            with open(file_path, 'w') as f:
                for guild_id, hash_value in guild_hashes.items():
                    f.write(f"{guild_id}:{hash_value}\n")
            logger.info(f"💾 Saved hashes for {len(guild_hashes)} guilds")
        except Exception as e:
            logger.error(f"❌ Failed to save guild hashes: {e}")

    

    def save_command_hash(self, hash_value, file_path):
        """
        Save command hash to file
        """
        try:
            with open(file_path, 'w') as f:
                f.write(hash_value)
            logger.info(f"💾 Saved command hash: {hash_value[:10]}... to {file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save hash: {e}")
            return False

    async def cleanup_connections(self):
        """Clean up AsyncSSH connections on shutdown"""
        try:
            if hasattr(self, 'killfeed_parser') and self.killfeed_parser:
                await self.killfeed_parser.cleanup_sftp_connections()

            if hasattr(self, 'log_parser') and self.log_parser:
                # Clean up log parser SFTP connections
                for pool_key, conn in list(self.log_parser.sftp_pool.items()):
                    try:
                        conn.close()
                    except:
                        pass
                self.log_parser.sftp_pool.clear()

            logger.info("Cleaned up all SFTP connections")

        except Exception as e:
            logger.error(f"Failed to cleanup connections: {e}")

    async def setup_database(self):
        """Setup MongoDB connection"""
        mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGO_URI')
        if not mongo_uri:
            logger.error("MongoDB URI not found in environment variables")
            return False

        try:
            self.mongo_client = AsyncIOMotorClient(mongo_uri)
            self.database = self.mongo_client.emerald_killfeed

            # Initialize database manager with PHASE 1 architecture
            from bot.models.database import DatabaseManager
            self.db_manager = DatabaseManager(self.mongo_client)
            # For backward compatibility
            self.database = self.db_manager

            # Test connection
            await self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")

            # Initialize database indexes
            await self.db_manager.initialize_indexes()
            logger.info("Database architecture initialized (PHASE 1)")

            # Initialize batch sender for rate limit management
            from bot.utils.batch_sender import BatchSender
            self.batch_sender = BatchSender(self)

            # Initialize parsers (PHASE 2) - Data parsers for killfeed & log events
            self.killfeed_parser = KillfeedParser(self)
            self.historical_parser = HistoricalParser(self)
            self.unified_parser = UnifiedLogParser(self)
            logger.info("Parsers initialized (PHASE 2) + Unified Log Parser + Batch Sender")

            return True

        except Exception as e:
            logger.error("Failed to connect to MongoDB: %s", e)
            return False

    def setup_scheduler(self):
        """Setup background job scheduler"""
        try:
            self.scheduler.start()
            logger.info("Background job scheduler started")
            return True
        except Exception as e:
            logger.error("Failed to start scheduler: %s", e)
            return False

    async def on_ready(self):
        """Called when bot is ready and connected to Discord - MULTI-GUILD RATE LIMIT SAFE VERSION"""
        # Only run setup once
        if hasattr(self, '_setup_complete'):
            return

        logger.info("🚀 Bot is ready! Loading cogs first...")

        # CRITICAL: Load cogs FIRST before anything else
        try:
            logger.info("🔧 Loading cogs for command registration...")
            cogs_success = await self.load_cogs()
            logger.info(f"🎯 Cog loading: {'✅ Complete' if cogs_success else '❌ Failed'}")

            # Give py-cord time to process async setup functions
            await asyncio.sleep(2.0)  # Allow more time for py-cord to process command registration

            # Force a fresh command sync by removing the hash file
            hash_file_path = "command_hash.txt"
            if os.path.exists(hash_file_path):
                os.remove(hash_file_path)
                logger.info("🔄 Removed command hash to force fresh sync")

            # Use the specialized Multi-Guild Command Registration System
            await self.register_commands_safely()

            logger.info("🚀 Now starting database and parser setup...")

            # Connect to MongoDB
            db_success = await self.setup_database()
            logger.info(f"📊 Database setup: {'✅ Success' if db_success else '❌ Failed'}")

            # Start scheduler
            scheduler_success = self.setup_scheduler()
            logger.info(f"⏰ Scheduler setup: {'✅ Success' if scheduler_success else '❌ Failed'}")

            # Schedule parsers (PHASE 2)
            if self.killfeed_parser:
                self.killfeed_parser.schedule_killfeed_parser()
                logger.info("📡 Killfeed parser scheduled")
            if self.unified_parser:
                self.scheduler.add_job(
                    self.unified_parser.run_log_parser,
                    'interval',
                    seconds=180,
                    id='unified_log_parser'
                )
                logger.info("📜 Unified log parser scheduled")

            # Bot ready messages
            if self.user:
                logger.info("✅ Bot logged in as %s (ID: %s)", self.user.name, self.user.id)
            logger.info("✅ Connected to %d guilds", len(self.guilds))

            for guild in self.guilds:
                logger.info(f"📡 Bot connected to: {guild.name} (ID: {guild.id})")

            # Verify assets exist
            if self.assets_path.exists():
                assets = list(self.assets_path.glob('*.png'))
                logger.info("📁 Found %d asset files", len(assets))
            else:
                logger.warning("⚠️ Assets directory not found")

            # Verify dev data exists (for testing)
            if self.dev_mode:
                csv_files = list(self.dev_data_path.glob('csv/*.csv'))
                log_files = list(self.dev_data_path.glob('logs/*.log'))
                logger.info("🧪 Dev mode: Found %d CSV files and %d log files", len(csv_files), len(log_files))

            logger.info("🎉 Bot setup completed successfully!")
            self._setup_complete = True

        except Exception as e:
            logger.error(f"❌ Critical error in bot setup: {e}")
            raise

    async def on_guild_join(self, guild):
        """Called when bot joins a new guild"""
        logger.info("Joined guild: %s (ID: %s)", guild.name, guild.id)

        # Sync commands to the new guild immediately
        logger.info(f"🔄 New guild joined - syncing commands to: {guild.name}")
        try:
            await self.sync_commands(guild_ids=[guild.id])
            logger.info(f"✅ Successfully synced commands to new guild {guild.name}")

            # Update guild hash tracking
            current_hash = compute_command_hash(self)
            guild_hash_file = "guild_command_hashes.txt"
            
            with open(guild_hash_file, 'a') as f:
                f.write(f"{guild.id}:{current_hash}\n")
            logger.info(f"✅ Tracked new guild hash")

        except Exception as e:
            logger.error(f"❌ Failed to sync commands to new guild {guild.name}: {e}")

    async def on_guild_remove(self, guild):
        """Called when bot is removed from a guild"""
        logger.info("Left guild: %s (ID: %s)", guild.name, guild.id)

    async def close(self):
        """Clean shutdown"""
        logger.info("Shutting down bot...")

        # Clean up SFTP connections
        await self.cleanup_connections()

        # Shutdown log parser to save state
        if hasattr(self, 'unified_parser') and self.unified_parser:
            # Unified parser cleanup handled in close() method
            pass

        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

        if hasattr(self, 'mongo_client') and self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")

        await super().close()
        logger.info("Bot shutdown complete")

    async def shutdown(self):
        """Graceful shutdown"""
        try:
            # Flush any remaining batched messages
            if hasattr(self, 'batch_sender'):
                logger.info("Flushing remaining batched messages...")
                await self.batch_sender.flush_all_queues()
                logger.info("Batch sender flushed")

            # Clean up SFTP connections
            await self.cleanup_connections()

            # Shutdown log parser to save state
            if hasattr(self, 'unified_parser') and self.unified_parser:
                # Unified parser cleanup handled in close() method
                pass

            if self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("Scheduler stopped")

            if hasattr(self, 'mongo_client') and self.mongo_client:
                self.mongo_client.close()
                logger.info("MongoDB connection closed")

            await super().close()
            logger.info("Bot shutdown complete")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

async def main():
    """Main entry point"""
    # Check required environment variables for Railway deployment
    bot_token = os.getenv('BOT_TOKEN') or os.getenv('DISCORD_TOKEN')
    mongo_uri = os.getenv('MONGO_URI') or os.getenv('MONGODB_URI')
    tip4serv_key = os.getenv('TIP4SERV_KEY')  # Optional service key

    # Railway environment detection
    railway_env = os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('RAILWAY_STATIC_URL')
    if railway_env:
        print(f"✅ Railway environment detected")

    # Validate required secrets
    if not bot_token:
        logger.error("❌ BOT_TOKEN not found in environment variables")
        logger.error("Please set BOT_TOKEN in your Railway environment variables")
        return

    if not mongo_uri:
        logger.error("❌ MONGO_URI not found in environment variables") 
        logger.error("Please set MONGO_URI in your Railway environment variables")
        return

    # Log startup success
    logger.info(f"✅ Bot starting with token: {'*' * 20}...{bot_token[-4:] if bot_token else 'MISSING'}")
    logger.info(f"✅ MongoDB URI configured: {'*' * 20}...{mongo_uri[-10:] if mongo_uri else 'MISSING'}")
    if tip4serv_key:
        logger.info(f"✅ TIP4SERV_KEY configured: {'*' * 10}...{tip4serv_key[-4:]}")
    else:
        logger.info("ℹ️ TIP4SERV_KEY not configured (optional)")

    # Create and run bot
    print("Creating bot instance...")
    bot = EmeraldKillfeedBot()

    try:
        await bot.start(bot_token)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error("Error in bot execution: %s", e)
        raise
    finally:
        if not bot.is_closed():
            await bot.close()

if __name__ == "__main__":
    # Run the bot
    print("Starting main bot execution...")
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Critical error in main execution: {e}")
        import traceback
        traceback.print_exc()