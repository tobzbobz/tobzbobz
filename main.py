import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging
from dotenv import load_dotenv
import os
import traceback
from aiohttp import web
from datetime import datetime
from pathlib import Path

load_dotenv()
token = os.getenv('DISCORD_TOKEN')

from database import db, ensure_database_connected

# ========================================
# LOGGING CONFIGURATION - CLEANED UP
# ========================================
logging.basicConfig(
    level=logging.INFO,  # Changed from DEBUG to INFO
    format='%(asctime)s:%(levelname)s:%(name)s: %(message)s',
    handlers=[
        logging.FileHandler('discord.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)

# Reduce noise from libraries
logging.getLogger('discord').setLevel(logging.WARNING)
logging.getLogger('discord.http').setLevel(logging.WARNING)
logging.getLogger('discord.gateway').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ========================================
# CONFIGURATION
# ========================================
GUILD_IDS = [1282916959062851634, 1425867713183744023, 1430002479239532747, 1420770769562243083]

GUILD_COGS = {
    1282916959062851634: ['!mod', 'disclaimer', 'ghost', 'inactive_ticket', 'react', 'say', 'status', 'watches',
                          'wentwrong', 'role', 'ping', 'callsign', 'case', 'shift', 'x', 'autopublish', 'vc', 'topic',
                          'purge', 'pings', 'moderation', 'timeout', 'role_watcher', 'so_apps'],
    1425867713183744023: ['!mod', 'disclaimer', 'ghost', 'inactive_ticket', 'react', 'say', 'status', 'watches',
                          'wentwrong', 'role', 'ping', 'callsign', 'case', 'shift'],
    1430002479239532747: ['autorole', 'other'],
    1420770769562243083: ['erlc']
}

GLOBAL_COGS = ['logging_bot']
OWNER_ID = 678475709257089057
STATUS_CHANNEL_ID = 1429492069289693184
DEVELOPMENT_MODE = False
AGGRESSIVE_SYNC = False


# ========================================
# WEB SERVER
# ========================================
async def health_check(request):
    """Health check endpoint"""
    db_status = "connected" if db.pool else "disconnected"
    bot_status = "online" if client.is_ready() else "starting"

    return web.Response(
        text=f"Bot Status: {bot_status}\nDatabase: {db_status}\nTimestamp: {datetime.utcnow().isoformat()}",
        content_type="text/plain"
    )


async def log_view(request):
    """View bot logs via web"""
    page = int(request.query.get('page', 1))
    size = int(request.query.get('size', 50))
    log_path = 'discord.log'

    if not os.path.exists(log_path):
        return web.Response(text="Log file not found.", status=404)

    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    total_lines = len(lines)
    start = (page - 1) * size
    end = start + size
    page_lines = lines[start:end]
    content = ''.join(page_lines)

    return web.Response(text=content, content_type='text/plain')


async def start_web_server():
    """Start web server for health checks"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_get('/logs', log_view)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info('Health server started on port 8080')


# ========================================
# DATABASE MONITORING
# ========================================
@tasks.loop(minutes=5)
async def monitor_database_health():
    """Monitor database connection health"""
    try:
        if db.pool:
            size = db.pool.get_size()
            idle = db.pool.get_idle_size()
            max_size = db.pool.get_max_size()
            logger.debug(f"DB Pool: {size}/{max_size} connections ({idle} idle)")

            if not await db.ensure_connected():
                logger.error("Database health check failed!")
        else:
            logger.warning("No database pool exists!")
            await db.connect()
    except Exception as e:
        logger.error(f"Database monitoring error: {e}")


@monitor_database_health.before_loop
async def before_monitor():
    await client.wait_until_ready()


# ========================================
# BOT CLIENT
# ========================================
class Client(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_cogs = []
        self.failed_cogs = []
        self.guild_cog_map = {}
        self.db = db

    async def load_all_cogs(self):
        """Automatically load all cogs from the cogs folder"""
        cogs_folder = Path('cogs')

        if not cogs_folder.exists():
            logger.warning('Cogs folder not found!')
            return

        cog_files = [f.stem for f in cogs_folder.glob('*.py') if f.stem != '__init__']
        logger.info(f'Found {len(cog_files)} cog files')

        # Determine which cogs to load
        cogs_to_load = set(GLOBAL_COGS)
        for guild_id in GUILD_IDS:
            if guild_id in GUILD_COGS:
                cogs_to_load.update(GUILD_COGS[guild_id])

        # Load each cog
        for cog_name in cog_files:
            if cogs_to_load and cog_name not in cogs_to_load:
                logger.debug(f'Skipping {cog_name} (not configured)')
                continue

            try:
                await self.load_extension(f'cogs.{cog_name}')
                self.loaded_cogs.append(cog_name)

                # Map cog to guilds
                for guild_id in GUILD_IDS:
                    if guild_id in GUILD_COGS:
                        if cog_name in GUILD_COGS[guild_id] or cog_name in GLOBAL_COGS:
                            if cog_name not in self.guild_cog_map:
                                self.guild_cog_map[cog_name] = []
                            self.guild_cog_map[cog_name].append(guild_id)

                logger.info(f'Loaded cog: {cog_name}')
            except Exception as e:
                self.failed_cogs.append(cog_name)
                logger.error(f'Failed to load {cog_name}: {e}')
                traceback.print_exc()

        logger.info(f'Cog loading complete: {len(self.loaded_cogs)} loaded, {len(self.failed_cogs)} failed')

    async def reload_all_cogs(self):
        """Reload all loaded cogs"""
        reloaded = []
        failed_reload = []

        for cog_name in self.loaded_cogs:
            try:
                await self.reload_extension(f'cogs.{cog_name}')
                reloaded.append(cog_name)
            except Exception as e:
                failed_reload.append(cog_name)
                logger.error(f'Failed to reload {cog_name}: {e}')

        logger.info(f'Reloaded {len(reloaded)} cogs')
        if failed_reload:
            logger.warning(f'Failed to reload: {", ".join(failed_reload)}')

    def is_cog_enabled_for_guild(self, cog_name: str, guild_id: int) -> bool:
        """Check if a cog is enabled for a specific guild"""
        if cog_name in GLOBAL_COGS:
            return True
        if guild_id in GUILD_COGS:
            return cog_name in GUILD_COGS[guild_id]
        return True

    async def setup_hook(self):
        """Setup hook - runs before bot connects to Discord"""
        try:
            # 1. Connect to database
            logger.info('Connecting to database...')
            connected = await ensure_database_connected()
            if not connected:
                logger.warning('Database connection failed! Bot may not work correctly.')

            # 2. Start web server
            logger.info('Starting web server...')
            await start_web_server()

            # 3. Load all cogs
            logger.info('Loading cogs...')
            await self.load_all_cogs()

            # 4. Sync commands
            logger.info('Syncing commands...')
            await self.sync_commands()

        except Exception as e:
            logger.error(f'Error in setup_hook: {e}')
            traceback.print_exc()

    async def sync_commands(self):
        """Sync commands to Discord"""
        try:
            if AGGRESSIVE_SYNC:
                logger.warning('AGGRESSIVE SYNC MODE - Clearing all commands')

                # Clear global commands
                self.tree.clear_commands(guild=None)
                await self.tree.sync()
                logger.info('Cleared global commands')

                # Clear guild commands
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    self.tree.clear_commands(guild=guild)
                    await self.tree.sync(guild=guild)
                logger.info(f'Cleared commands for {len(GUILD_IDS)} guilds')

                # Reload cogs to re-register commands
                await self.reload_all_cogs()

            # Sync to each guild
            for guild_id in GUILD_IDS:
                guild = discord.Object(id=guild_id)

                # Copy global commands to guild
                self.tree.copy_global_to(guild=guild)

                # Sync
                synced = await self.tree.sync(guild=guild)

                # Get cog info for this guild
                guild_cogs = GUILD_COGS.get(guild_id, [])
                cog_info = f" [{len(guild_cogs)} cogs]" if guild_cogs else " [no cogs]"

                logger.info(f'Guild {guild_id}: {len(synced)} commands{cog_info}')

                if len(synced) == 0 and guild_cogs:
                    logger.warning(f'Guild {guild_id} has {len(guild_cogs)} cogs but 0 commands!')

        except Exception as e:
            logger.error(f'Command sync failed: {e}')
            traceback.print_exc()

    async def update_status_channel(self, status: str):
        """Update the status channel name"""
        if not STATUS_CHANNEL_ID:
            return

        try:
            status_channel = await self.fetch_channel(STATUS_CHANNEL_ID)
            if not status_channel:
                return

            current_name = status_channel.name

            if status == 'online':
                desired_name = '🟡・Utilities Bot Status | Development' if DEVELOPMENT_MODE else '🟢・Utilities Bot Status | Online'
            elif status == 'offline':
                desired_name = '🔴・Utilities Bot Status | Offline'
            else:
                return

            if current_name == desired_name:
                return

            await status_channel.edit(name=desired_name)
            logger.info(f'Updated status channel: {current_name} → {desired_name}')

        except discord.Forbidden:
            logger.warning('Missing permissions to edit status channel')
        except discord.HTTPException as e:
            if e.code == 50035:
                logger.debug('Rate limited when updating status channel')
            else:
                logger.error(f'HTTP error updating status channel: {e}')
        except Exception as e:
            logger.error(f'Failed to update status channel: {e}')

    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'Logged in as {self.user}')
        logger.info(f'Summary: {len(self.loaded_cogs)} cogs loaded, {len(self.failed_cogs)} failed')

        await self.update_status_channel('online')

        # Start database monitoring
        if not monitor_database_health.is_running():
            monitor_database_health.start()

    async def close(self):
        """Called when bot is shutting down"""
        if not DEVELOPMENT_MODE:
            await self.update_status_channel('offline')

        await db.close()
        await super().close()


# ========================================
# CREATE BOT INSTANCE
# ========================================
intents = discord.Intents.all()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.presences = True
intents.voice_states = True

client = Client(command_prefix='!', intents=intents)


# ========================================
# OWNER COMMANDS
# ========================================
@client.command(name='forcesync')
@commands.is_owner()
async def force_sync(ctx):
    """Force sync all commands (owner only)"""
    await ctx.send("🔄 Force syncing commands...")

    try:
        sync_results = []
        for guild_id in GUILD_IDS:
            guild = discord.Object(id=guild_id)
            synced = await client.tree.sync(guild=guild)
            sync_results.append(f"{guild_id}: {len(synced)} commands")

        result_text = "\n".join(sync_results)
        await ctx.send(f"✅ Synced commands:\n```{result_text}```")
    except Exception as e:
        await ctx.send(f"❌ Sync failed: {e}")
        traceback.print_exc()


@client.command(name='listcogs')
@commands.is_owner()
async def list_cogs(ctx):
    """List all loaded cogs (owner only)"""
    loaded = "\n".join([f"• {cog}" for cog in client.loaded_cogs]) or "None"
    failed = "\n".join([f"• {cog}" for cog in client.failed_cogs]) or "None"

    embed = discord.Embed(title="Loaded Cogs", color=discord.Color.blue())
    embed.add_field(name="✅ Loaded", value=loaded, inline=False)
    embed.add_field(name="❌ Failed", value=failed, inline=False)
    await ctx.send(embed=embed)


@client.command(name='debugsync')
@commands.is_owner()
async def debug_sync(ctx):
    """Debug command syncing issues"""
    await ctx.send("🔍 Debugging command sync...")

    debug_info = []
    debug_info.append(f"**Loaded Cogs:** {len(client.loaded_cogs)}")
    debug_info.append(f"**Tree Commands:** {len(client.tree.get_commands())}")

    for guild_id in GUILD_IDS:
        guild_obj = discord.Object(id=guild_id)
        guild_commands = client.tree.get_commands(guild=guild_obj)
        debug_info.append(f"**Guild {guild_id}:** {len(guild_commands)} commands")

    await ctx.send("\n".join(debug_info))


# ========================================
# RUN BOT
# ========================================
if __name__ == "__main__":
    try:
        logger.info("Starting bot...")
        client.run(token)
    except Exception as e:
        logger.critical(f'Fatal error: {e}')
        traceback.print_exc()