import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging
from dotenv import load_dotenv
import os
import json
import traceback
import sys
from aiohttp import web
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

load_dotenv()
token = os.getenv('DISCORD_TOKEN')

from database import db, ensure_database_connected

# Define both guild IDs
GUILD_IDS = [1282916959062851634, 1425867713183744023, 1430002479239532747, 1420770769562243083]

# Guild-specific cog configuration
GUILD_COGS = {
    1282916959062851634: ['!mod', 'disclaimer', 'ghost', 'inactive_ticket', 'react', 'say', 'status', 'watches',
                          'wentwrong', 'role', 'ping', 'callsign', 'case','shift','x','autopublish','vc','topic','purge','pings','moderation'],
    1425867713183744023: ['!mod', 'disclaimer', 'ghost', 'inactive_ticket', 'react', 'say', 'status', 'watches',
                          'wentwrong', 'role', 'ping', 'callsign', 'case','shift'],
    1430002479239532747: ['autorole', 'other'],
    1420770769562243083: ['erlc']
}

# Cogs that should load in ALL servers
GLOBAL_COGS = ['logging_bot']

# YOUR DISCORD USER ID
OWNER_ID = 678475709257089057

# Status channel ID
STATUS_CHANNEL_ID = 1429492069289693184

# Development mode
DEVELOPMENT_MODE = False

# Aggressive command sync
AGGRESSIVE_SYNC = False  # ← CHANGE THIS TO True TEMPORARILY


async def health_check(request):
    """Health check endpoint"""
    # Check database connection
    db_status = "connected" if db.pool else "disconnected"
    return web.Response(text=f"Bot is alive! Database: {db_status}")


async def start_web_server():
    """Start web server for health checks"""
    app = web.Application()
    app.router.add_get('/', health_check)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    print('🌐 Health server started on port 8080')


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
            print('⚠️ Cogs folder not found!')
            return

        cog_files = [f.stem for f in cogs_folder.glob('*.py') if f.stem != '__init__']

        cogs_to_load = set(GLOBAL_COGS)

        for guild_id in GUILD_IDS:
            if guild_id in GUILD_COGS:
                valid_cogs = [cog for cog in GUILD_COGS[guild_id] if cog is not None]
                cogs_to_load.update(GUILD_COGS[guild_id])

        if not cogs_to_load:
            cogs_to_load = set(cog_files)

        for cog_name in cog_files:
            if cogs_to_load and cog_name not in cogs_to_load:
                print(f'⏭️ Skipping {cog_name} (not configured for any guild)')
                continue

            try:
                await self.load_extension(f'cogs.{cog_name}')
                self.loaded_cogs.append(cog_name)

                for guild_id in GUILD_IDS:
                    if guild_id in GUILD_COGS:
                        if cog_name in GUILD_COGS[guild_id] or cog_name in GLOBAL_COGS:
                            if cog_name not in self.guild_cog_map:
                                self.guild_cog_map[cog_name] = []
                            self.guild_cog_map[cog_name].append(guild_id)

                print(f'✅ Loaded {cog_name}')
            except Exception as e:
                self.failed_cogs.append(cog_name)
                print(f'❌ Failed to load {cog_name}: {e}')
                traceback.print_exc()  # ← Added to see full error

        if self.loaded_cogs:
            print(f'✅ Loaded cogs: {", ".join(self.loaded_cogs)}')
        if self.failed_cogs:
            print(f'❌ Failed cogs: {", ".join(self.failed_cogs)}')

        if self.guild_cog_map:
            print(f'\n📋 Guild-Cog Mapping:')
            for cog_name, guild_ids in self.guild_cog_map.items():
                guild_names = [str(gid) for gid in guild_ids]
                print(f'  • {cog_name}: {", ".join(guild_names)}')

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
                print(f'❌ Failed to reload {cog_name}: {e}')

        if reloaded:
            print(f'✅ Reloaded cogs: {", ".join(reloaded)}')
        if failed_reload:
            print(f'❌ Failed to reload: {", ".join(failed_reload)}')

    def is_cog_enabled_for_guild(self, cog_name: str, guild_id: int) -> bool:
        """Check if a cog is enabled for a specific guild"""
        if cog_name in GLOBAL_COGS:
            return True

        if guild_id in GUILD_COGS:
            return cog_name in GUILD_COGS[guild_id]

        return True

    async def setup_hook(self):
        # Connect to database FIRST
        print('🔌 Connecting to database...')
        connected = await ensure_database_connected()
        if not connected:
            print('⚠️ WARNING: Database connection failed! Bot may not work correctly.')

        # Load all cogs automatically
        await self.load_all_cogs()

        # Build guild-specific command mapping
        guild_commands = {}

        asyncio.create_task(start_web_server())

        for guild_id, cog_list in GUILD_COGS.items():
            if cog_list:
                valid_cogs = [cog for cog in cog_list if cog is not None]
                if valid_cogs:
                    guild_commands[guild_id] = valid_cogs

        # Sync commands to guilds
        try:
            if AGGRESSIVE_SYNC:
                print('⚠️ AGGRESSIVE SYNC MODE ENABLED - Clearing and resyncing commands')

                self.tree.clear_commands(guild=None)
                await self.tree.sync()
                print('✅ Cleared global commands')

                cleared_guilds = []
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    self.tree.clear_commands(guild=guild)
                    await self.tree.sync(guild=guild)
                    cleared_guilds.append(str(guild_id))

                print(f'✅ Cleared commands for guilds: {", ".join(cleared_guilds)}')

                await self.reload_all_cogs()

                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    if guild_id in guild_commands:
                        self.tree.copy_global_to(guild=guild)
                        print(f'📋 Copied commands to guild {guild_id}')
                    else:
                        print(f'⏭️ Skipped guild {guild_id} (no cogs configured)')

                sync_results = []
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    synced = await self.tree.sync(guild=guild)

                    cog_info = ""
                    if guild_id in guild_commands:
                        cog_info = f" [{', '.join(guild_commands[guild_id])}]"

                    sync_results.append(f"{guild_id} ({len(synced)} commands){cog_info}")

                print(f'✅ Synced commands to guilds: {", ".join(sync_results)}')

            else:
                sync_results = []
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)

                    if guild_id in guild_commands:
                        self.tree.copy_global_to(guild=guild)

                    synced = await self.tree.sync(guild=guild)

                    cog_info = ""
                    if guild_id in guild_commands:
                        cog_info = f" [{', '.join(guild_commands[guild_id])}]"
                    else:
                        cog_info = " [no cogs]"

                    sync_results.append(f"{guild_id} ({len(synced)} commands){cog_info}")

                print(f'✅ Synced commands to guilds: {", ".join(sync_results)}')
        except Exception as e:
            print(f'❌ Error syncing commands: {e}')
            traceback.print_exc()

    async def update_status_channel(self, status: str):
        """Update the status channel name with error handling"""
        if not STATUS_CHANNEL_ID:
            return

        try:
            status_channel = await self.fetch_channel(STATUS_CHANNEL_ID)
            if not status_channel:
                print(f'❌ Status channel {STATUS_CHANNEL_ID} not found')
                return

            current_name = status_channel.name

            if status == 'online':
                if DEVELOPMENT_MODE:
                    desired_name = '🟡・Utilities Bot Status | Development'
                else:
                    desired_name = '🟢・Utilities Bot Status | Online'
            elif status == 'offline':
                desired_name = '🔴・Utilities Bot Status | Offline'
            else:
                return

            if current_name == desired_name:
                print(f'ℹ️ Status channel already correct: {current_name}')
                return

            await status_channel.edit(name=desired_name)
            print(f'✅ Updated status channel: "{current_name}" → "{desired_name}"')

        except discord.Forbidden:
            print(f'❌ Missing permissions to edit status channel')
        except discord.HTTPException as e:
            if e.code == 50035:
                print(f'⚠️ Rate limited when updating status channel (2 updates per 10 minutes max)')
            else:
                print(f'❌ HTTP error updating status channel: {e}')
        except Exception as e:
            print(f'❌ Failed to update status channel: {e}')

    async def on_ready(self):
        print(f'Logged in as {self.user}')
        print(f'📊 Summary: {len(self.loaded_cogs)} cogs loaded, {len(self.failed_cogs)} failed')

        await self.update_status_channel('online')

    async def close(self):
        """Called when the bot is shutting down"""
        if not DEVELOPMENT_MODE:
            await self.update_status_channel('offline')

        # Close database connection
        await db.close()

        await super().close()

    async def send_error_dm(self, title: str, error: Exception, interaction: discord.Interaction = None):
        """Send error to owner - DEPRECATED, kept for compatibility"""
        # Errors are now handled by LoggingCog, this is just for backward compatibility
        pass


# ========================================
# Create bot instance
# ========================================
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.presences = True
intents.voice_states = True
client = Client(command_prefix='!', intents=intents)


# ========================================
# Owner-only commands (AFTER client is created)
# ========================================

@client.command(name='forcesync')
@commands.is_owner()
@app_commands.checks.has_permissions(administrator=True)
async def force_sync(ctx):
    """Force sync all commands (owner only)"""
    await ctx.send("🔄 Force syncing commands...")

    try:
        # Sync to all configured guilds
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
@app_commands.checks.has_permissions(administrator=True)
async def list_cogs(ctx):
    """List all loaded cogs (owner only)"""
    loaded = "\n".join([f"• {cog}" for cog in client.loaded_cogs]) or "None"
    failed = "\n".join([f"• {cog}" for cog in client.failed_cogs]) or "None"

    embed = discord.Embed(title="Loaded Cogs", color=discord.Color.blue())
    embed.add_field(name="✅ Loaded", value=loaded, inline=False)
    embed.add_field(name="❌ Failed", value=failed, inline=False)
    await ctx.send(embed=embed)


# ========================================
# Run the bot
# ========================================
try:
    client.run(token, log_handler=handler, log_level=logging.DEBUG)
except Exception as e:
    print(f'Fatal error: {e}')
    traceback.print_exc()