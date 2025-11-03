import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
import asyncio
import json
from datetime import datetime, timezone
from typing import Optional, Literal
import logging

logger = logging.getLogger(__name__)


class ERLC(commands.GroupCog, name="erlc"):
    """A cog to interact with the ER:LC API."""

    def __init__(self, bot):
        self.bot = bot
        self.base_url = "https://api.policeroleplay.community"
        self.session: Optional[aiohttp.ClientSession] = None
        self.db = bot.db
        self.OWNER_ID = 678475709257089057

        # Cache configurations in memory for faster access
        self.guild_configs = {}

        # Store last seen log IDs to prevent duplicate posts
        self.last_logs = {}

        # Configurable log check interval (in seconds)
        self.log_check_interval = 30

        # Command color mapping - comprehensive list
        self.command_colors = {
            # Moderation commands (Red shades)
            ':ban': discord.Color.red(),
            ':unban': discord.Color.orange(),
            ':kick': discord.Color.orange(),
            ':shutdown': discord.Color.dark_red(),
            ':temp-ban': discord.Color.from_rgb(255, 69, 0),

            # Admin/Staff commands (Gold/Yellow shades)
            ':admin': discord.Color.gold(),
            ':unadmin': discord.Color.from_rgb(218, 165, 32),
            ':mod': discord.Color.gold(),
            ':unmod': discord.Color.from_rgb(218, 165, 32),
            ':co-owner': discord.Color.from_rgb(255, 215, 0),
            ':unco-owner': discord.Color.from_rgb(218, 165, 32),
            ':owner': discord.Color.from_rgb(255, 223, 0),
            ':unowner': discord.Color.from_rgb(218, 165, 32),

            # Teleport commands (Green shades)
            ':tp': discord.Color.green(),
            ':bring': discord.Color.from_rgb(50, 205, 50),
            ':to': discord.Color.from_rgb(34, 139, 34),
            ':goto': discord.Color.from_rgb(34, 139, 34),
            ':rejoin': discord.Color.from_rgb(60, 179, 113),

            # Communication commands (Blue/Purple shades)
            ':h': discord.Color.blue(),
            ':hint': discord.Color.blue(),
            ':m': discord.Color.from_rgb(70, 130, 180),
            ':pm': discord.Color.purple(),
            ':w': discord.Color.purple(),
            ':whisper': discord.Color.purple(),
            ':announce': discord.Color.from_rgb(65, 105, 225),
            ':rc': discord.Color.from_rgb(100, 149, 237),

            # Team commands (Teal/Cyan shades)
            ':team': discord.Color.teal(),
            ':changeteam': discord.Color.teal(),
            ':clearteam': discord.Color.from_rgb(72, 209, 204),
            ':setteam': discord.Color.teal(),

            # Vehicle commands (Magenta/Pink shades)
            ':spawnvehicle': discord.Color.magenta(),
            ':spawncustomvehicle': discord.Color.from_rgb(255, 0, 255),
            ':clearvehicles': discord.Color.from_rgb(219, 112, 147),
            ':respawnvehicles': discord.Color.from_rgb(255, 20, 147),

            # Server control (Dark colors)
            ':startrain': discord.Color.from_rgb(105, 105, 105),
            ':stoprain': discord.Color.from_rgb(169, 169, 169),
            ':lockserver': discord.Color.from_rgb(128, 0, 0),
            ':unlockserver': discord.Color.from_rgb(139, 69, 19),
            ':time': discord.Color.from_rgb(75, 0, 130),
            ':blackout': discord.Color.from_rgb(25, 25, 25),
            ':unblackout': discord.Color.from_rgb(220, 220, 220),

            # Player state commands (Light colors)
            ':kill': discord.Color.dark_red(),
            ':respawn': discord.Color.from_rgb(144, 238, 144),
            ':freeze': discord.Color.from_rgb(176, 224, 230),
            ':unfreeze': discord.Color.from_rgb(255, 182, 193),
            ':blind': discord.Color.from_rgb(47, 79, 79),
            ':unblind': discord.Color.from_rgb(245, 245, 220),

            # Permission commands (Orange shades)
            ':serverban': discord.Color.from_rgb(255, 140, 0),
            ':unserverban': discord.Color.from_rgb(255, 165, 0),
            ':handcuff': discord.Color.from_rgb(192, 192, 192),
            ':arrest': discord.Color.from_rgb(178, 34, 34),
            ':unarrest': discord.Color.from_rgb(240, 128, 128),

            # System commands (Gray/White shades)
            ':credits': discord.Color.light_gray(),
            ':info': discord.Color.from_rgb(176, 196, 222),
            ':cmds': discord.Color.from_rgb(119, 136, 153),
            ':command': discord.Color.from_rgb(119, 136, 153),
            ':clear': discord.Color.from_rgb(211, 211, 211),
            ':logs': discord.Color.from_rgb(128, 128, 128),
        }

    def get_command_color(self, command: str) -> discord.Color:
        """Get color based on command type."""
        command_lower = command.lower()
        for cmd, color in self.command_colors.items():
            if command_lower.startswith(cmd):
                return color
        return discord.Color.purple()

    def format_player_link(self, player_name: str, player_id: Optional[str] = None) -> str:
        """Format player name as a clickable Roblox profile link."""
        if player_id:
            return f"[{player_name}](https://www.roblox.com/users/{player_id}/profile)"
        return player_name

    def format_timestamp(self, timestamp: int) -> str:
        """Format timestamp as Discord short time with seconds."""
        return f"<t:{timestamp}:T>"

    async def get_player_id_from_name(self, player_name: str) -> Optional[str]:
        """Get Roblox user ID from username using Roblox API."""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()

            async with self.session.post(
                    'https://users.roblox.com/v1/usernames/users',
                    json={'usernames': [player_name], 'excludeBannedUsers': False}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('data') and len(data['data']) > 0:
                        return str(data['data'][0]['id'])
        except Exception as e:
            logger.error(f"Failed to get Roblox ID for {player_name}: {e}")
        return None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Check if the user is authorized to use ERLC commands."""
        if interaction.user.id != self.OWNER_ID:
            await interaction.response.send_message(
                "❌ You are not authorized to use ERLC commands.",
                ephemeral=True
            )
            return False
        return True

    async def cog_load(self):
        """Initialize the aiohttp session and load configs from database."""
        self.session = aiohttp.ClientSession()

        if not self.db or not self.db.pool:
            logger.error("❌ Database not connected when ERLC cog loaded!")
            print("❌ ERLC: Database not available!")
        else:
            logger.info("✅ ERLC: Database connection available")

        await self.load_all_configs()
        self.log_monitor.start()

    async def cog_unload(self):
        """Clean up the aiohttp session when cog unloads."""
        self.log_monitor.cancel()
        if self.session:
            await self.session.close()

    async def load_all_configs(self):
        """Load all ERLC configurations from database on startup."""
        try:
            configs = await self.db.fetch(
                "SELECT * FROM bot_settings WHERE setting_key LIKE 'erlc_%'"
            )

            guild_data = {}
            for row in configs:
                guild_id = row['guild_id']
                key = row['setting_key'].replace('erlc_', '')
                value = json.loads(row['setting_value'])

                if guild_id not in guild_data:
                    guild_data[guild_id] = {}
                guild_data[guild_id][key] = value

            for guild_id, data in guild_data.items():
                if 'config' in data:
                    self.guild_configs[guild_id] = data['config']

            logger.info(f"Loaded ERLC configs for {len(self.guild_configs)} guilds")
        except Exception as e:
            logger.error(f"Failed to load ERLC configs: {e}")

    async def set_config(self, guild_id: int, server_key: str, channel_id: int, webhook_url: Optional[str] = None,
                         joins_channel: Optional[int] = None, kills_channel: Optional[int] = None,
                         commands_channel: Optional[int] = None, modcalls_channel: Optional[int] = None):
        """Set configuration for a guild and save to database."""
        config = {
            'server_key': server_key,
            'channel_id': channel_id,
            'webhook_url': webhook_url,
            'log_channels': {
                'joins': joins_channel or channel_id,
                'kills': kills_channel or channel_id,
                'commands': commands_channel or channel_id,
                'modcalls': modcalls_channel or channel_id
            },
            'log_monitoring': {
                'joins': False,
                'kills': False,
                'commands': False,
                'modcalls': False
            }
        }

        self.guild_configs[guild_id] = config

        try:
            success = await self.db.set_setting(guild_id, 'erlc_config', config)
            if success:
                logger.info(f"✅ Saved ERLC config to database for guild {guild_id}")
            else:
                logger.error(f"❌ Failed to save ERLC config to database for guild {guild_id}")
            return success
        except Exception as e:
            logger.error(f"❌ Error saving ERLC config: {e}")
            import traceback
            traceback.print_exc()
            return False

    def get_config(self, guild_id: int):
        """Get configuration for a guild."""
        return self.guild_configs.get(guild_id)

    async def set_log_monitoring(self, guild_id: int, log_type: str, enabled: bool):
        """Enable/disable specific log monitoring and save to database."""
        config = self.get_config(guild_id)
        if config and log_type in config['log_monitoring']:
            config['log_monitoring'][log_type] = enabled
            await self.db.set_setting(guild_id, 'erlc_config', config)

    async def make_request(self, endpoint: str, server_key: str, method: str = "GET", json_data: dict = None):
        """Make a request to the ER:LC API with proper rate limit handling."""
        if not self.session:
            self.session = aiohttp.ClientSession()

        headers = {
            'server-key': server_key,
            'Accept': '*/*'
        }

        if method == "POST":
            headers['Content-Type'] = 'application/json'

        url = f"{self.base_url}{endpoint}"

        try:
            async with self.session.request(method, url, headers=headers, json=json_data) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get('X-RateLimit-Reset', 5))
                    bucket = resp.headers.get('X-RateLimit-Bucket', 'unknown')
                    logger.warning(f"Rate limited on bucket {bucket}. Retry after {retry_after}s")
                    return {
                        'error': f"Rate limited. Retry after {retry_after} seconds.",
                        'retry_after': retry_after
                    }

                remaining = resp.headers.get('X-RateLimit-Remaining')
                limit = resp.headers.get('X-RateLimit-Limit')
                logger.debug(f"Rate limit: {remaining}/{limit} remaining")

                if resp.status == 401:
                    return {'error': 'Unauthorized - Invalid API key'}
                elif resp.status == 403:
                    return {'error': 'Forbidden - Server key may have been regenerated'}
                elif resp.status == 400:
                    return {'error': 'Bad request'}
                elif resp.status == 204:
                    return {'success': True, 'message': 'Command executed successfully'}
                elif resp.status == 503:
                    return {'error': 'Problem communicating with Roblox'}

                if resp.content_type == 'application/json':
                    return await resp.json()
                else:
                    text = await resp.text()
                    return {'raw': text}

        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {e}")
            return {'error': f'Request failed: {str(e)}'}

    async def send_to_channel(self, guild_id: int, embed: discord.Embed, use_webhook: bool = True,
                              log_type: Optional[str] = None):
        """Send an embed to the configured channel or webhook."""
        config = self.get_config(guild_id)
        if not config:
            return False

        if log_type and 'log_channels' in config:
            channel_id = config['log_channels'].get(log_type, config['channel_id'])
        else:
            channel_id = config['channel_id']

        try:
            if use_webhook and config.get('webhook_url'):
                webhook = discord.Webhook.from_url(
                    config['webhook_url'],
                    session=self.session
                )
                await webhook.send(embed=embed)
            else:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    await channel.send(embed=embed)
                else:
                    return False
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def create_embed(self, title: str, data: dict, color: discord.Color = discord.Color.blue()) -> discord.Embed:
        """Create a formatted embed from API data."""
        embed = discord.Embed(
            title=title,
            color=color,
            timestamp=datetime.now(timezone.utc)
        )

        if 'error' in data:
            embed.color = discord.Color.red()
            embed.description = f"❌ {data['error']}"
        else:
            if isinstance(data, dict):
                for key, value in data.items():
                    if key not in ['error', 'success']:
                        embed.add_field(
                            name=key.replace('_', ' ').title(),
                            value=str(value)[:1024],
                            inline=True
                        )
            elif isinstance(data, list):
                embed.description = f"Found {len(data)} items"
                for i, item in enumerate(data[:25]):
                    if isinstance(item, dict):
                        field_text = '\n'.join([f"**{k}**: {v}" for k, v in item.items()])
                        embed.add_field(
                            name=f"Item {i + 1}",
                            value=field_text[:1024],
                            inline=False
                        )

        embed.set_footer(text="ER:LC API")
        return embed

    def filter_players(self, players: list, team: Optional[str] = None, callsign: Optional[str] = None,
                       permission: Optional[str] = None, player_query: Optional[str] = None) -> list:
        """Filter players based on criteria."""
        filtered = players

        if team:
            filtered = [p for p in filtered if p.get('Team', '').lower() == team.lower()]

        if callsign:
            filtered = [p for p in filtered if p.get('Callsign', '').lower() == callsign.lower()]

        if permission:
            filtered = [p for p in filtered if p.get('Permission', '').lower() == permission.lower()]

        if player_query:
            filtered = [p for p in filtered if player_query.lower() in p.get('Player', '').lower()]

        return filtered

    def filter_vehicles(self, vehicles: list, livery: Optional[str] = None,
                        name: Optional[str] = None, owner: Optional[str] = None) -> list:
        """Filter vehicles based on criteria."""
        filtered = vehicles

        if livery:
            filtered = [v for v in filtered if livery.lower() in v.get('Texture', '').lower()]

        if name:
            filtered = [v for v in filtered if name.lower() in v.get('Name', '').lower()]

        if owner:
            filtered = [v for v in filtered if owner.lower() in v.get('Owner', '').lower()]

        return filtered

    @tasks.loop(seconds=30)
    async def log_monitor(self):
        """Monitor logs and post new entries to configured channels."""
        for guild_id, config in self.guild_configs.items():
            if not config.get('log_monitoring'):
                continue

            server_key = config['server_key']
            monitoring = config['log_monitoring']

            if monitoring.get('joins'):
                data = await self.make_request('/v1/server/joinlogs', server_key)
                if isinstance(data, list) and data:
                    await self.process_join_logs(guild_id, data)

            if monitoring.get('kills'):
                data = await self.make_request('/v1/server/killlogs', server_key)
                if isinstance(data, list) and data:
                    await self.process_kill_logs(guild_id, data)

            if monitoring.get('commands'):
                data = await self.make_request('/v1/server/commandlogs', server_key)
                if isinstance(data, list) and data:
                    await self.process_command_logs(guild_id, data)

            if monitoring.get('modcalls'):
                data = await self.make_request('/v1/server/modcalls', server_key)
                if isinstance(data, list) and data:
                    await self.process_modcall_logs(guild_id, data)

            await asyncio.sleep(1)

    @log_monitor.before_loop
    async def before_log_monitor(self):
        """Wait until the bot is ready before starting the log monitor."""
        await self.bot.wait_until_ready()

    def change_log_interval(self, seconds: int):
        """Change the log monitoring interval."""
        if seconds < 10:
            seconds = 10
        self.log_check_interval = seconds
        self.log_monitor.change_interval(seconds=seconds)

    async def process_join_logs(self, guild_id: int, logs: list):
        """Process and send join/leave logs."""
        key = f"{guild_id}_joins"
        last_seen = self.last_logs.get(key, set())

        for log in logs[-10:]:
            log_id = f"{log.get('Player', '')}_{log.get('Timestamp', '')}"
            if log_id not in last_seen:
                player_name = log.get('Player', 'Unknown')
                player_id = await self.get_player_id_from_name(player_name)
                player_link = self.format_player_link(player_name, player_id)
                timestamp = self.format_timestamp(log.get('Timestamp', 0))
                action = "joined" if log.get('Join') else "left"

                embed = discord.Embed(
                    title="🚪 Player Join/Leave",
                    description=f"Player {player_link} {action} the server at {timestamp}",
                    color=discord.Color.green() if log.get('Join') else discord.Color.orange(),
                    timestamp=datetime.fromtimestamp(log.get('Timestamp', 0), tz=timezone.utc)
                )
                embed.set_footer(text="ER:LC API")

                await self.send_to_channel(guild_id, embed, log_type='joins')
                last_seen.add(log_id)

        self.last_logs[key] = last_seen

    async def process_kill_logs(self, guild_id: int, logs: list):
        """Process and send kill logs."""
        key = f"{guild_id}_kills"
        last_seen = self.last_logs.get(key, set())

        for log in logs[-10:]:
            log_id = f"{log.get('Killer', '')}_{log.get('Killed', '')}_{log.get('Timestamp', '')}"
            if log_id not in last_seen:
                killer_name = log.get('Killer', 'Unknown')
                killed_name = log.get('Killed', 'Unknown')

                killer_id = await self.get_player_id_from_name(killer_name)
                killed_id = await self.get_player_id_from_name(killed_name)

                killer_link = self.format_player_link(killer_name, killer_id)
                killed_link = self.format_player_link(killed_name, killed_id)
                timestamp = self.format_timestamp(log.get('Timestamp', 0))

                description = f"{killer_link} killed {killed_link} at {timestamp}"
                if log.get('Weapon'):
                    description += f"\n**Weapon:** {log.get('Weapon')}"

                embed = discord.Embed(
                    title="💀 Kill Log",
                    description=description,
                    color=discord.Color.red(),
                    timestamp=datetime.fromtimestamp(log.get('Timestamp', 0), tz=timezone.utc)
                )
                embed.set_footer(text="ER:LC API")

                await self.send_to_channel(guild_id, embed, log_type='kills')
                last_seen.add(log_id)

        self.last_logs[key] = last_seen

    async def process_command_logs(self, guild_id: int, logs: list):
        """Process and send command logs."""
        key = f"{guild_id}_commands"
        last_seen = self.last_logs.get(key, set())

        for log in logs[-10:]:
            log_id = f"{log.get('Player', '')}_{log.get('Command', '')}_{log.get('Timestamp', '')}"
            if log_id not in last_seen:
                player_name = log.get('Player', 'Unknown')
                player_id = await self.get_player_id_from_name(player_name)
                player_link = self.format_player_link(player_name, player_id)
                command = log.get('Command', 'Unknown')
                timestamp = self.format_timestamp(log.get('Timestamp', 0))

                color = self.get_command_color(command)

                embed = discord.Embed(
                    title="⚙️ Command Executed",
                    description=f"{player_link} executed `{command}` at {timestamp}",
                    color=color,
                    timestamp=datetime.fromtimestamp(log.get('Timestamp', 0), tz=timezone.utc)
                )
                embed.set_footer(text="ER:LC API")

                await self.send_to_channel(guild_id, embed, log_type='commands')
                last_seen.add(log_id)

        self.last_logs[key] = last_seen

    async def process_modcall_logs(self, guild_id: int, logs: list):
        """Process and send modcall logs."""
        key = f"{guild_id}_modcalls"
        last_seen = self.last_logs.get(key, set())

        for log in logs[-10:]:
            log_id = f"{log.get('Caller', '')}_{log.get('Timestamp', '')}"
            if log_id not in last_seen:
                caller_name = log.get('Caller', 'Unknown')
                caller_id = await self.get_player_id_from_name(caller_name)
                caller_link = self.format_player_link(caller_name, caller_id)
                timestamp = self.format_timestamp(log.get('Timestamp', 0))

                if log.get('Moderator'):
                    mod_name = log.get('Moderator')
                    mod_id = await self.get_player_id_from_name(mod_name)
                    mod_link = self.format_player_link(mod_name, mod_id)
                    description = f"{mod_link} responded to {caller_link} at {timestamp}"
                    color = discord.Color.green()
                    title = "📞 Moderator Call - Responded"
                else:
                    description = f"{caller_link} called for a moderator at {timestamp}\n⏳ Waiting for response"
                    color = discord.Color.gold()
                    title = "📞 Moderator Call - Pending"

                embed = discord.Embed(
                    title=title,
                    description=description,
                    color=color,
                    timestamp=datetime.fromtimestamp(log.get('Timestamp', 0), tz=timezone.utc)
                )
                embed.set_footer(text="ER:LC API")

                await self.send_to_channel(guild_id, embed, log_type='modcalls')
                last_seen.add(log_id)

        self.last_logs[key] = last_seen

    # Configuration Commands
    @app_commands.command(name="setup", description="Setup the ER:LC API configuration")
    @app_commands.describe(
        server_key="Your ER:LC server API key",
        channel="Default channel to send updates to",
        webhook_url="Optional: Webhook URL for posting",
        joins_channel="Optional: Separate channel for join/leave logs",
        kills_channel="Optional: Separate channel for kill logs",
        commands_channel="Optional: Separate channel for command logs",
        modcalls_channel="Optional: Separate channel for mod call logs"
    )
    @app_commands.default_permissions(administrator=True)
    async def setup(
            self,
            interaction: discord.Interaction,
            server_key: str,
            channel: discord.TextChannel,
            webhook_url: Optional[str] = None,
            joins_channel: Optional[discord.TextChannel] = None,
            kills_channel: Optional[discord.TextChannel] = None,
            commands_channel: Optional[discord.TextChannel] = None,
            modcalls_channel: Optional[discord.TextChannel] = None
    ):
        """Setup the ER:LC API configuration for this server."""
        await interaction.response.defer(ephemeral=True)

        success = await self.set_config(
            interaction.guild_id,
            server_key,
            channel.id,
            webhook_url,
            joins_channel.id if joins_channel else None,
            kills_channel.id if kills_channel else None,
            commands_channel.id if commands_channel else None,
            modcalls_channel.id if modcalls_channel else None
        )

        if not success:
            await interaction.followup.send(
                "❌ Failed to save configuration to database. Please check bot logs.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title="✅ Configuration Saved",
            description=f"ER:LC API has been configured for this server and saved to database.",
            color=discord.Color.green()
        )
        embed.add_field(name="Default Channel", value=channel.mention, inline=False)
        embed.add_field(name="Using Webhook", value="Yes" if webhook_url else "No", inline=True)

        if joins_channel:
            embed.add_field(name="Join/Leave Logs", value=joins_channel.mention, inline=True)
        if kills_channel:
            embed.add_field(name="Kill Logs", value=kills_channel.mention, inline=True)
        if commands_channel:
            embed.add_field(name="Command Logs", value=commands_channel.mention, inline=True)
        if modcalls_channel:
            embed.add_field(name="Mod Call Logs", value=modcalls_channel.mention, inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="logs", description="Configure automatic log monitoring")
    @app_commands.describe(
        log_type="Type of log to monitor",
        enabled="Enable or disable monitoring"
    )
    @app_commands.default_permissions(administrator=True)
    async def configure_logs(
            self,
            interaction: discord.Interaction,
            log_type: Literal["joins", "kills", "commands", "modcalls"],
            enabled: bool
    ):
        """Configure automatic log monitoring."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message(
                "❌ Please setup the API first using `/erlc setup`",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        await self.set_log_monitoring(interaction.guild_id, log_type, enabled)

        embed = discord.Embed(
            title="✅ Log Monitoring Updated",
            description=f"{log_type.title()} monitoring has been **{'enabled' if enabled else 'disabled'}**",
            color=discord.Color.green() if enabled else discord.Color.red()
        )

        channel_id = config['log_channels'].get(log_type, config['channel_id'])
        channel = self.bot.get_channel(channel_id)
        if channel and enabled:
            embed.add_field(name="Channel", value=channel.mention, inline=True)

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="interval", description="Change the log monitoring check interval")
    @app_commands.describe(
        seconds="Interval in seconds (minimum 10 seconds to respect rate limits)"
    )
    @app_commands.default_permissions(administrator=True)
    async def set_interval(self, interaction: discord.Interaction, seconds: int):
        """Change the log monitoring interval."""
        if seconds < 10:
            await interaction.response.send_message(
                "❌ Interval must be at least 10 seconds to respect API rate limits.",
                ephemeral=True
            )
            return

        self.change_log_interval(seconds)

        embed = discord.Embed(
            title="✅ Interval Updated",
            description=f"Log monitoring will now check every **{seconds} seconds**",
            color=discord.Color.green()
        )
        embed.add_field(
            name="⚠️ Note",
            value="Lower intervals may hit rate limits if monitoring multiple log types.",
            inline=False
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # API Data Commands
    @app_commands.command(name="server", description="Get server status information")
    async def get_server(self, interaction: discord.Interaction):
        """Fetch and display server status."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message("❌ Please setup the API first using `/erlc setup`", ephemeral=True)
            return

        await interaction.response.defer()

        data = await self.make_request('/v1/server', config['server_key'])
        embed = self.create_embed("🎮 Server Status", data)

        await interaction.followup.send(embed=embed)
        await self.send_to_channel(interaction.guild_id, embed)

    @app_commands.command(name="players", description="Get list of players in server with optional filters")
    @app_commands.describe(
        team="Filter by team name",
        callsign="Filter by callsign",
        permission="Filter by permission level",
        player="Filter by player name or Roblox ID"
    )
    async def get_players(
            self,
            interaction: discord.Interaction,
            team: Optional[str] = None,
            callsign: Optional[str] = None,
            permission: Optional[Literal["Normal", "Server Administrator", "Server Owner", "Server Moderator"]] = None,
            player: Optional[str] = None
    ):
        """Fetch and display current players with optional filtering."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message("❌ Please setup the API first using `/erlc setup`", ephemeral=True)
            return

        await interaction.response.defer()

        data = await self.make_request('/v1/server/players', config['server_key'])

        if isinstance(data, list):
            filtered_data = self.filter_players(data, team, callsign, permission, player)

            embed = discord.Embed(
                title="👥 Current Players",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )

            filters_applied = []
            if team:
                filters_applied.append(f"Team: {team}")
            if callsign:
                filters_applied.append(f"Callsign: {callsign}")
            if permission:
                filters_applied.append(f"Permission: {permission}")
            if player:
                filters_applied.append(f"Player: {player}")

            if filters_applied:
                embed.description = f"**Filters:** {', '.join(filters_applied)}\n**Results:** {len(filtered_data)} players"
            else:
                embed.description = f"**Total Players:** {len(filtered_data)}"

            for i, p in enumerate(filtered_data[:25]):
                player_name = p.get('Player', 'Unknown')
                player_id = await self.get_player_id_from_name(player_name)
                player_link = self.format_player_link(player_name, player_id)

                player_info = []
                player_info.append(f"**Player:** {player_link}")
                player_info.append(f"**Permission:** {p.get('Permission', 'N/A')}")
                if p.get('Team'):
                    player_info.append(f"**Team:** {p.get('Team')}")
                if p.get('Callsign'):
                    player_info.append(f"**Callsign:** {p.get('Callsign')}")

                embed.add_field(
                    name=f"Player {i + 1}",
                    value='\n'.join(player_info),
                    inline=False
                )

            if len(filtered_data) > 25:
                embed.add_field(
                    name="⚠️ Note",
                    value=f"Showing first 25 of {len(filtered_data)} players",
                    inline=False
                )

            embed.set_footer(text="ER:LC API")
        else:
            embed = self.create_embed("👥 Current Players", data)

        await interaction.followup.send(embed=embed)
        await self.send_to_channel(interaction.guild_id, embed)

    @app_commands.command(name="vehicles", description="Get list of vehicles in server with optional filters")
    @app_commands.describe(
        livery="Filter by vehicle livery/texture",
        name="Filter by vehicle name",
        owner="Filter by owner username"
    )
    async def get_vehicles(
            self,
            interaction: discord.Interaction,
            livery: Optional[str] = None,
            name: Optional[str] = None,
            owner: Optional[str] = None
    ):
        """Fetch and display current vehicles with optional filtering."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message("❌ Please setup the API first using `/erlc setup`", ephemeral=True)
            return

        await interaction.response.defer()

        data = await self.make_request('/v1/server/vehicles', config['server_key'])

        if isinstance(data, list):
            filtered_data = self.filter_vehicles(data, livery, name, owner)

            embed = discord.Embed(
                title="🚗 Server Vehicles",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )

            filters_applied = []
            if livery:
                filters_applied.append(f"Livery: {livery}")
            if name:
                filters_applied.append(f"Name: {name}")
            if owner:
                filters_applied.append(f"Owner: {owner}")

            if filters_applied:
                embed.description = f"**Filters:** {', '.join(filters_applied)}\n**Results:** {len(filtered_data)} vehicles"
            else:
                embed.description = f"**Total Vehicles:** {len(filtered_data)}"

            for i, v in enumerate(filtered_data[:25]):
                owner_name = v.get('Owner', 'N/A')
                owner_id = await self.get_player_id_from_name(owner_name) if owner_name != 'N/A' else None
                owner_link = self.format_player_link(owner_name, owner_id) if owner_name != 'N/A' else 'N/A'

                vehicle_info = []
                vehicle_info.append(f"**Name:** {v.get('Name', 'Unknown')}")
                vehicle_info.append(f"**Livery:** {v.get('Texture', 'N/A')}")
                vehicle_info.append(f"**Owner:** {owner_link}")

                embed.add_field(
                    name=f"Vehicle {i + 1}",
                    value='\n'.join(vehicle_info),
                    inline=False
                )

            if len(filtered_data) > 25:
                embed.add_field(
                    name="⚠️ Note",
                    value=f"Showing first 25 of {len(filtered_data)} vehicles",
                    inline=False
                )

            embed.set_footer(text="ER:LC API")
        else:
            embed = self.create_embed("🚗 Server Vehicles", data)

        await interaction.followup.send(embed=embed)
        await self.send_to_channel(interaction.guild_id, embed)

    @app_commands.command(name="staff", description="Get server staff list")
    async def get_staff(self, interaction: discord.Interaction):
        """Fetch and display server staff."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message("❌ Please setup the API first using `/erlc setup`", ephemeral=True)
            return

        await interaction.response.defer()

        data = await self.make_request('/v1/server/staff', config['server_key'])

        if isinstance(data, list):
            embed = discord.Embed(
                title="👮 Server Staff",
                description=f"**Total Staff:** {len(data)}",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )

            for i, staff in enumerate(data[:25]):
                staff_name = staff.get('Player', 'Unknown')
                staff_id = await self.get_player_id_from_name(staff_name)
                staff_link = self.format_player_link(staff_name, staff_id)

                staff_info = [f"**Player:** {staff_link}"]

                if staff.get('Permission'):
                    staff_info.append(f"**Role:** {staff.get('Permission')}")

                embed.add_field(
                    name=f"Staff {i + 1}",
                    value='\n'.join(staff_info),
                    inline=False
                )

            if len(data) > 25:
                embed.add_field(
                    name="⚠️ Note",
                    value=f"Showing first 25 of {len(data)} staff members",
                    inline=False
                )

            embed.set_footer(text="ER:LC API")
        else:
            embed = self.create_embed("👮 Server Staff", data)

        await interaction.followup.send(embed=embed)
        await self.send_to_channel(interaction.guild_id, embed)

    @app_commands.command(name="banned", description="Check server bans or search for a specific player")
    @app_commands.describe(
        player="Optional: Search for a specific player name or Roblox ID"
    )
    async def get_bans(self, interaction: discord.Interaction, player: Optional[str] = None):
        """Fetch and display server bans with profile links."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message("❌ Please setup the API first using `/erlc setup`", ephemeral=True)
            return

        await interaction.response.defer()

        data = await self.make_request('/v1/server/bans', config['server_key'])

        if isinstance(data, list):
            if player:
                found = [b for b in data if player.lower() in b.get('Player', '').lower()]

                if found:
                    embed = discord.Embed(
                        title="🔨 Ban Status: BANNED",
                        description=f"Player **{player}** is currently banned.",
                        color=discord.Color.red(),
                        timestamp=datetime.now(timezone.utc)
                    )

                    for ban in found:
                        ban_player = ban.get('Player', 'Unknown')
                        player_id = await self.get_player_id_from_name(ban_player)
                        player_link = self.format_player_link(ban_player, player_id)

                        ban_info = [f"**Player:** {player_link}"]

                        if ban.get('Reason'):
                            ban_info.append(f"**Reason:** {ban.get('Reason')}")
                        if ban.get('Moderator'):
                            mod_name = ban.get('Moderator')
                            mod_id = await self.get_player_id_from_name(mod_name)
                            mod_link = self.format_player_link(mod_name, mod_id)
                            ban_info.append(f"**Banned By:** {mod_link}")
                        if ban.get('Timestamp'):
                            ban_time = datetime.fromtimestamp(ban.get('Timestamp'), tz=timezone.utc)
                            ban_info.append(f"**Date:** {ban_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

                        embed.add_field(
                            name="Ban Details",
                            value='\n'.join(ban_info),
                            inline=False
                        )
                else:
                    embed = discord.Embed(
                        title="✅ Ban Status: NOT BANNED",
                        description=f"Player **{player}** is not currently banned.",
                        color=discord.Color.green(),
                        timestamp=datetime.now(timezone.utc)
                    )
            else:
                embed = discord.Embed(
                    title="🔨 Server Bans",
                    description=f"**Total Bans:** {len(data)}",
                    color=discord.Color.red(),
                    timestamp=datetime.now(timezone.utc)
                )

                for i, ban in enumerate(data[:25]):
                    ban_player = ban.get('Player', 'Unknown')
                    player_id = await self.get_player_id_from_name(ban_player)
                    player_link = self.format_player_link(ban_player, player_id)

                    ban_info = [f"**Player:** {player_link}"]

                    if ban.get('Reason'):
                        ban_info.append(f"**Reason:** {ban.get('Reason')}")
                    if ban.get('Moderator'):
                        mod_name = ban.get('Moderator')
                        mod_id = await self.get_player_id_from_name(mod_name)
                        mod_link = self.format_player_link(mod_name, mod_id)
                        ban_info.append(f"**By:** {mod_link}")

                    embed.add_field(
                        name=f"Ban {i + 1}",
                        value='\n'.join(ban_info),
                        inline=False
                    )

                if len(data) > 25:
                    embed.add_field(
                        name="⚠️ Note",
                        value=f"Showing first 25 of {len(data)} bans",
                        inline=False
                    )

            embed.set_footer(text="ER:LC API")
        else:
            embed = self.create_embed("🔨 Server Bans", data)

        await interaction.followup.send(embed=embed)

    @app_commands.command(name="command", description="Execute a command on the server")
    @app_commands.describe(command="The command to execute (e.g., :h Hello everyone!)")
    @app_commands.default_permissions(administrator=True)
    async def send_command(self, interaction: discord.Interaction, command: str):
        """Send a command to the ER:LC server."""
        config = self.get_config(interaction.guild_id)
        if not config:
            await interaction.response.send_message("❌ Please setup the API first using `/erlc setup`", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        data = await self.make_request(
            '/v1/server/command',
            config['server_key'],
            method="POST",
            json_data={'command': command}
        )

        if data.get('success'):
            embed = discord.Embed(
                title="✅ Command Executed",
                description=f"Command `{command}` was executed successfully.",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="❌ Command Failed",
                description=data.get('error', 'Unknown error'),
                color=discord.Color.red()
            )

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    """Setup function to add the cog to the bot."""
    await bot.add_cog(ERLC(bot))