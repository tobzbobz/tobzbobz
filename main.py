import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging
from dotenv import load_dotenv
import os
import json
import traceback
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

load_dotenv()
token = os.getenv('DISCORD_TOKEN')

# Define both guild IDs
GUILD_IDS = [1282916959062851634, 1425867713183744023, 1430002479239532747, 1420770769562243083]
#           HNZRP | FENZ & HHSTJ,       TCCTA,                IIaF&HU,        The Tobs Army
# Guild-specific cog configuration
# Format: {guild_id: [list of cog names to load]}
GUILD_COGS = {
    1282916959062851634: ['!mod', 'disclaimer', 'ghost', 'inactive_ticket', 'react', 'say', 'status', 'watches', 'wentwrong', 'role','ping', 'callsign'],  # Server 1 gets these cogs
    1425867713183744023: ['!mod', 'disclaimer', 'ghost', 'inactive_ticket', 'react', 'say', 'status', 'watches', 'wentwrong', 'role','ping', 'callsign'],  # Server 2 gets these cogs
    1430002479239532747: ['autorole', 'other'], # Server 3 gets all
    1420770769562243083: []
}

# Cogs that should load in ALL servers (optional)
GLOBAL_COGS = ['logging_bot']  # e.g., ['help', 'info'] - these load everywhere

# YOUR DISCORD USER ID - Replace with your actual Discord user ID
OWNER_ID = 678475709257089057  # Replace this with your Discord user ID

# Status channel ID (set to None to disable)
STATUS_CHANNEL_ID = 1429492069289693184  # Replace with your channel ID to enable status updates
# Enable before server use. Channel ID: 1429492069289693184

# Development mode - Set to True to use development status instead of online
DEVELOPMENT_MODE = True   # Set to True for 🟠 Development status

# Aggressive command sync - Set to True to clear all commands before syncing (useful for updates)
AGGRESSIVE_SYNC = False  # Set to True when adding/removing commands, then back to False


class Client(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_cogs = []
        self.failed_cogs = []
        self.guild_cog_map = {}  # Track which cogs are for which guilds

    async def load_all_cogs(self):
        """Automatically load all cogs from the cogs folder"""
        cogs_folder = Path('cogs')

        if not cogs_folder.exists():
            print('⚠️ Cogs folder not found!')
            return

        # Get all Python files in cogs folder
        cog_files = [f.stem for f in cogs_folder.glob('*.py') if f.stem != '__init__']

        # Determine which cogs to load based on guild configuration
        cogs_to_load = set(GLOBAL_COGS)  # Start with global cogs

        for guild_id in GUILD_IDS:
            if guild_id in GUILD_COGS:
                valid_cogs = [cog for cog in GUILD_COGS[guild_id] if cog is not None]
                cogs_to_load.update(GUILD_COGS[guild_id])

        # If no configuration, load all cogs
        if not cogs_to_load:
            cogs_to_load = set(cog_files)

        for cog_name in cog_files:
            # Skip cogs not in our load list
            if cogs_to_load and cog_name not in cogs_to_load:
                print(f'⏭️ Skipping {cog_name} (not configured for any guild)')
                continue

            try:
                await self.load_extension(f'cogs.{cog_name}')
                self.loaded_cogs.append(cog_name)

                # Track which guilds this cog is for
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
                # Don't send DM here - let logging cog handle it

        # Print summary
        if self.loaded_cogs:
            print(f'✅ Loaded cogs: {", ".join(self.loaded_cogs)}')
        if self.failed_cogs:
            print(f'❌ Failed cogs: {", ".join(self.failed_cogs)}')

        # Print guild-specific mapping
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
        # Global cogs are enabled everywhere
        if cog_name in GLOBAL_COGS:
            return True

        # Check guild-specific configuration
        if guild_id in GUILD_COGS:
            return cog_name in GUILD_COGS[guild_id]

        # If no configuration exists, allow all cogs
        return True

    async def setup_hook(self):
        # Load all cogs automatically
        await self.load_all_cogs()

        # Build guild-specific command mapping
        guild_commands = {}  # {guild_id: [list of cog names]}

        for guild_id, cog_list in GUILD_COGS.items():
            if cog_list:  # Skip empty lists or None
                # Filter out None values from cog list
                valid_cogs = [cog for cog in cog_list if cog is not None]
                if valid_cogs:
                    guild_commands[guild_id] = valid_cogs

        # Sync commands to guilds
        try:
            if AGGRESSIVE_SYNC:
                print('⚠️ AGGRESSIVE SYNC MODE ENABLED - Clearing and resyncing commands')

                # Clear global commands
                self.tree.clear_commands(guild=None)
                await self.tree.sync()
                print('✅ Cleared global commands')

                # Clear and sync each guild
                cleared_guilds = []
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    self.tree.clear_commands(guild=guild)
                    await self.tree.sync(guild=guild)
                    cleared_guilds.append(str(guild_id))

                print(f'✅ Cleared commands for guilds: {", ".join(cleared_guilds)}')

                # Reload all cogs to re-register commands
                await self.reload_all_cogs()

                # Copy commands to guilds (only if they have cogs configured)
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    # Only copy if this guild has cogs configured
                    if guild_id in guild_commands:
                        self.tree.copy_global_to(guild=guild)
                        print(f'📋 Copied commands to guild {guild_id}')
                    else:
                        print(f'⏭️ Skipped guild {guild_id} (no cogs configured)')

                # Sync the commands
                sync_results = []
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)
                    synced = await self.tree.sync(guild=guild)

                    # Show which cogs are enabled for this guild
                    cog_info = ""
                    if guild_id in guild_commands:
                        cog_info = f" [{', '.join(guild_commands[guild_id])}]"

                    sync_results.append(f"{guild_id} ({len(synced)} commands){cog_info}")

                print(f'✅ Synced commands to guilds: {", ".join(sync_results)}')

            else:
                # Normal sync (faster, use this most of the time)
                sync_results = []
                for guild_id in GUILD_IDS:
                    guild = discord.Object(id=guild_id)

                    # Only copy if this guild has cogs configured
                    if guild_id in guild_commands:
                        self.tree.copy_global_to(guild=guild)

                    synced = await self.tree.sync(guild=guild)

                    # Show which cogs are enabled for this guild
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
            # Fetch channel instead of getting from cache
            status_channel = await self.fetch_channel(STATUS_CHANNEL_ID)
            if not status_channel:
                print(f'❌ Status channel {STATUS_CHANNEL_ID} not found')
                return

            current_name = status_channel.name

            # Determine desired name
            if status == 'online':
                if DEVELOPMENT_MODE:
                    desired_name = '🟡・Utilities Bot Status | Development'
                else:
                    desired_name = '🟢・Utilities Bot Status | Online'
            elif status == 'offline':
                desired_name = '🔴・Utilities Bot Status | Offline'
            else:
                return

            # Only update if name has changed (prevents unnecessary rate limit usage)
            if current_name == desired_name:
                print(f'ℹ️ Status channel already correct: {current_name}')
                return

            await status_channel.edit(name=desired_name)
            print(f'✅ Updated status channel: "{current_name}" → "{desired_name}"')

        except discord.Forbidden:
            print(f'❌ Missing permissions to edit status channel')
        except discord.HTTPException as e:
            if e.code == 50035:  # Rate limited
                print(f'⚠️ Rate limited when updating status channel (2 updates per 10 minutes max)')
            else:
                print(f'❌ HTTP error updating status channel: {e}')
        except Exception as e:
            print(f'❌ Failed to update status channel: {e}')

    async def on_ready(self):
        print(f'Logged in as {self.user}')
        print(f'📊 Summary: {len(self.loaded_cogs)} cogs loaded, {len(self.failed_cogs)} failed')

        # Update status channel on startup
        await self.update_status_channel('online')

    async def close(self):
        """Called when the bot is shutting down"""
        # Update status channel on shutdown
        if not DEVELOPMENT_MODE:
            await self.update_status_channel('offline')

        # Call parent close method
        await super().close()

handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.presences = True
intents.voice_states = True
client = Client(command_prefix='!', intents=intents)

try:
    client.run(token, log_handler=handler, log_level=logging.DEBUG)
except Exception as e:
    print(f'Fatal error: {e}')
    traceback.print_exc()