import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
from typing import Optional, Union
import os

# Configuration
YOUR_USER_ID = 678475709257089057  # Replace with your user ID

# Import the database instance
try:
    from database import db

    DATABASE_AVAILABLE = True
except ImportError:
    print(
        "<:Warn:1437771973970104471> Warning: database.py not found. Role monitoring will not persist across restarts.")
    DATABASE_AVAILABLE = False
    db = None

# In-memory fallback configuration
MONITORED_ROLES = {
    # role_id: {
    #     'whitelist': [user_ids],
    #     'channel_id': channel_id (or None for no alerts),
    #     'ping_users': [user_ids_to_ping] (or None to ping whitelist)
    # }
}


async def load_config():
    """Load configuration from database"""
    global MONITORED_ROLES

    if not DATABASE_AVAILABLE or not db.pool:
        print("ℹ️ Database not available, using in-memory storage")
        return

    try:
        # Load all role monitor configurations
        configs = await db.fetch('SELECT * FROM role_monitor_config')

        for config in configs:
            role_id = config['role_id']
            MONITORED_ROLES[role_id] = {
                'whitelist': config['whitelist'],
                'channel_id': config['channel_id'],
                'ping_users': config['ping_users']
            }

        print(f"<:Accepted:1426930333789585509> Loaded {len(MONITORED_ROLES)} monitored roles from database")
    except Exception as e:
        print(f"<:Denied:1426930694633816248> Error loading role monitor config from database: {e}")
        print("ℹ️ Falling back to in-memory storage")


async def save_config():
    """Save configuration to database"""
    if not DATABASE_AVAILABLE or not db.pool:
        return True

    try:
        async with db.pool.acquire() as conn:
            # Clear existing configs
            await conn.execute('DELETE FROM role_monitor_config')

            # Insert all current configs
            for role_id, config in MONITORED_ROLES.items():
                await conn.execute(
                    '''INSERT INTO role_monitor_config
                           (role_id, whitelist, channel_id, ping_users)
                       VALUES ($1, $2, $3, $4)''',
                    role_id,
                    config['whitelist'],
                    config.get('channel_id'),
                    config.get('ping_users')
                )

        return True
    except Exception as e:
        print(f"<:Denied:1426930694633816248> Error saving role monitor config to database: {e}")
        return False


def is_owner():
    """Check if user is the bot owner"""

    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.id != YOUR_USER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only.",
                ephemeral=True
            )
            return False
        return True

    return app_commands.check(predicate)


class RoleWatcherCog(commands.Cog):
    """Monitor role assignments and alert on unauthorized grants"""

    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        """Called when the cog is loaded"""
        await load_config()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Monitor role changes and alert if unauthorized users give monitored roles"""

        # Check if any monitored roles were added
        added_roles = [role for role in after.roles if role not in before.roles]

        if not added_roles:
            return

        # Check if any added roles are monitored
        monitored_added = [role for role in added_roles if role.id in MONITORED_ROLES]

        if not monitored_added:
            return

        # Get the most recent audit log entry for role updates
        try:
            async for entry in after.guild.audit_logs(
                    limit=1,
                    action=discord.AuditLogAction.member_role_update
            ):
                # Check if this entry is for our member and is recent (within last 2 seconds)
                if entry.target.id != after.id:
                    continue

                if (discord.utils.utcnow() - entry.created_at).total_seconds() > 2:
                    continue

                # Found the role change
                moderator = entry.user

                # Check each monitored role that was added
                for role in monitored_added:
                    config = MONITORED_ROLES[role.id]
                    whitelist = config['whitelist']

                    # If moderator is not in whitelist, send alert
                    if moderator.id not in whitelist:
                        await self.send_role_alert(
                            guild=after.guild,
                            target=after,
                            role=role,
                            moderator=moderator,
                            authorized=False,
                            config=config
                        )
                    else:
                        # Optional: Log authorized role grants too
                        await self.send_role_alert(
                            guild=after.guild,
                            target=after,
                            role=role,
                            moderator=moderator,
                            authorized=True,
                            config=config
                        )

                break  # Found the entry we need

        except discord.Forbidden:
            print("<:Denied:1426930694633816248> Missing permissions to access audit logs")
        except Exception as e:
            print(f"<:Denied:1426930694633816248> Error checking role monitor: {e}")

    async def send_role_alert(self, guild: discord.Guild, target: discord.Member,
                              role: discord.Role, moderator: discord.Member,
                              authorized: bool, config: dict):
        """Send an alert about role assignment"""

        # Determine which channel to use
        channel_id = config.get('channel_id')

        if not channel_id:
            return

        try:
            channel = guild.get_channel(channel_id)
            if not channel:
                print(f"<:Warn:1437771973970104471> Warning: Channel {channel_id} not found for role {role.name}")
                return

            # Check if it's a text-based channel (includes voice channel text chats)
            if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel)):
                print(f"<:Warn:1437771973970104471> Warning: Channel {channel_id} is not a text-based channel")
                return

            # Create embed
            if authorized:
                embed = discord.Embed(
                    title="<:Accepted:1426930333789585509> Authorized Role Grant",
                    color=discord.Color.green(),
                    timestamp=discord.utils.utcnow()
                )
                ping_content = None
            else:
                embed = discord.Embed(
                    title="<:Warn:1437771973970104471> UNAUTHORIZED Role Grant",
                    color=discord.Color.red(),
                    timestamp=discord.utils.utcnow()
                )
                # Determine who to ping
                ping_users = config.get('ping_users')
                if ping_users:
                    # Use custom ping list
                    ping_content = " ".join([f"<@{user_id}>" for user_id in ping_users])
                else:
                    # Default to pinging whitelist
                    ping_content = " ".join([f"<@{user_id}>" for user_id in config['whitelist']])

            embed.add_field(
                name="Role Granted",
                value=f"{role.mention}\n`{role.name}` (ID: `{role.id}`)",
                inline=False
            )

            embed.add_field(
                name="Target User",
                value=f"{target.mention}\n`{target.name}` (ID: `{target.id}`)",
                inline=True
            )

            embed.add_field(
                name="Granted By",
                value=f"{moderator.mention}\n`{moderator.name}` (ID: `{moderator.id}`)",
                inline=True
            )

            if not authorized:
                whitelist = config['whitelist']
                whitelist_names = []
                for user_id in whitelist:
                    user = guild.get_member(user_id)
                    if user:
                        whitelist_names.append(f"{user.mention}")
                    else:
                        whitelist_names.append(f"<@{user_id}>")

                embed.add_field(
                    name="Authorized Users",
                    value="\n".join(whitelist_names) if whitelist_names else "None configured",
                    inline=False
                )

                embed.add_field(
                    name="<:Warn:1437771973970104471> Action Required",
                    value="This role was granted by an unauthorized user. Please review and take appropriate action.",
                    inline=False
                )

            embed.set_thumbnail(url=target.display_avatar.url)
            embed.set_footer(text="Role Monitor System")

            # Log to database if available
            if DATABASE_AVAILABLE and db.pool:
                try:
                    await db.log_action(
                        guild_id=guild.id,
                        user_id=moderator.id,
                        action="role_grant" if authorized else "unauthorized_role_grant",
                        details={
                            'role_id': role.id,
                            'role_name': role.name,
                            'target_id': target.id,
                            'target_name': str(target),
                            'authorized': authorized
                        }
                    )
                except Exception as e:
                    print(f"<:Warn:1437771973970104471> Failed to log role grant to database: {e}")

            await channel.send(content=ping_content, embed=embed)

        except Exception as e:
            print(f"<:Denied:1426930694633816248> Failed to send role alert: {e}")

    @app_commands.command(name="role-monitor", description="Configure role monitoring")
    @app_commands.describe(
        action="Action to perform",
        role="Role to monitor/configure",
        user="User to add/remove",
        channel="Channel for alerts (or type 'CURRENT' for this channel)"
    )
    @is_owner()
    async def role_monitor_config(
            self,
            interaction: discord.Interaction,
            action: str,
            role: Optional[discord.Role] = None,
            user: Optional[discord.Member] = None,
            channel: Optional[Union[discord.TextChannel, discord.VoiceChannel, discord.StageChannel]] = None
    ):
        """Configure role monitoring system"""

        if action == "list":
            # List all monitored roles and their configurations
            if not MONITORED_ROLES:
                await interaction.response.send_message(
                    "No roles are currently being monitored.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="Role Monitor Configuration",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )

            for role_id, config in MONITORED_ROLES.items():
                role_obj = interaction.guild.get_role(role_id)
                role_name = role_obj.name if role_obj else f"Unknown Role (ID: {role_id})"

                # Whitelist users
                whitelist_users = []
                for user_id in config['whitelist']:
                    user_obj = interaction.guild.get_member(user_id)
                    if user_obj:
                        whitelist_users.append(f"{user_obj.mention}")
                    else:
                        whitelist_users.append(f"<@{user_id}>")

                # Channel info
                role_channel_id = config.get('channel_id')
                if role_channel_id:
                    channel_info = f"<#{role_channel_id}>"
                else:
                    channel_info = "No channel set (alerts disabled)"

                # Ping users info
                ping_users = config.get('ping_users')
                if ping_users:
                    ping_info = ", ".join([f"<@{uid}>" for uid in ping_users])
                else:
                    ping_info = "Whitelist users"

                field_value = (
                    f"**Authorized:** {', '.join(whitelist_users) if whitelist_users else 'None'}\n"
                    f"**Alert Channel:** {channel_info}\n"
                    f"**Ping on Alert:** {ping_info}"
                )

                embed.add_field(
                    name=f"🎭 {role_name}",
                    value=field_value,
                    inline=False
                )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        elif action == "toggle":
            if not role or not user:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify both a role and user to toggle.",
                    ephemeral=True
                )
                return

            # Create monitoring entry if doesn't exist
            if role.id not in MONITORED_ROLES:
                MONITORED_ROLES[role.id] = {
                    'whitelist': [],
                    'channel_id': None,
                    'ping_users': None
                }

            # Toggle logic
            if user.id in MONITORED_ROLES[role.id]['whitelist']:
                # Remove user
                MONITORED_ROLES[role.id]['whitelist'].remove(user.id)

                # Remove role from monitoring if no users left
                if not MONITORED_ROLES[role.id]['whitelist']:
                    del MONITORED_ROLES[role.id]
                    await save_config()
                    await interaction.response.send_message(
                        f"<:Accepted:1426930333789585509> Removed {user.mention} from {role.mention}. Role is no longer monitored (no authorized users).",
                        ephemeral=True
                    )
                else:
                    await save_config()
                    await interaction.response.send_message(
                        f"<:Accepted:1426930333789585509> Removed {user.mention} from authorized users for {role.mention}.",
                        ephemeral=True
                    )
            else:
                # Add user
                MONITORED_ROLES[role.id]['whitelist'].append(user.id)
                await save_config()
                await interaction.response.send_message(
                    f"<:Accepted:1426930333789585509> Added {user.mention} to authorized users for {role.mention}.",
                    ephemeral=True
                )

        elif action == "set-role-channel":
            if not role:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a role to configure.",
                    ephemeral=True
                )
                return

            if role.id not in MONITORED_ROLES:
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> {role.mention} is not being monitored. Add authorized users first.",
                    ephemeral=True
                )
                return

            if not channel:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a channel for alerts (or type 'CURRENT').",
                    ephemeral=True
                )
                return

            MONITORED_ROLES[role.id]['channel_id'] = channel.id
            await save_config()

            await interaction.response.send_message(
                f"<:Accepted:1426930333789585509> Set alert channel for {role.mention} to {channel.mention}.",
                ephemeral=True
            )

        elif action == "set-role-channel-current":
            if not role:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a role to configure.",
                    ephemeral=True
                )
                return

            if role.id not in MONITORED_ROLES:
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> {role.mention} is not being monitored. Add authorized users first.",
                    ephemeral=True
                )
                return

            # Use the current channel
            current_channel = interaction.channel

            # Verify it's a text-based channel
            if not isinstance(current_channel, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel)):
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> This channel cannot receive messages.",
                    ephemeral=True
                )
                return

            MONITORED_ROLES[role.id]['channel_id'] = current_channel.id
            await save_config()

            await interaction.response.send_message(
                f"<:Accepted:1426930333789585509> Set alert channel for {role.mention} to this channel ({current_channel.mention}).",
                ephemeral=True
            )

        elif action == "toggle-ping-user":
            if not role:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a role to configure.",
                    ephemeral=True
                )
                return

            if role.id not in MONITORED_ROLES:
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> {role.mention} is not being monitored. Add authorized users first.",
                    ephemeral=True
                )
                return

            if not user:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a user to toggle in the ping list.",
                    ephemeral=True
                )
                return

            # Initialize ping_users list if it doesn't exist
            if not MONITORED_ROLES[role.id].get('ping_users'):
                MONITORED_ROLES[role.id]['ping_users'] = []

            # Toggle logic
            if user.id in MONITORED_ROLES[role.id]['ping_users']:
                # Remove from ping list
                MONITORED_ROLES[role.id]['ping_users'].remove(user.id)
                if not MONITORED_ROLES[role.id]['ping_users']:
                    MONITORED_ROLES[role.id]['ping_users'] = None
                await save_config()
                await interaction.response.send_message(
                    f"<:Accepted:1426930333789585509> Removed {user.mention} from ping list for {role.mention}.",
                    ephemeral=True
                )
            else:
                # Add to ping list
                MONITORED_ROLES[role.id]['ping_users'].append(user.id)
                await save_config()
                await interaction.response.send_message(
                    f"<:Accepted:1426930333789585509> Added {user.mention} to ping list for {role.mention} alerts.",
                    ephemeral=True
                )

        elif action == "clear-ping-users":
            if not role:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a role.",
                    ephemeral=True
                )
                return

            if role.id not in MONITORED_ROLES:
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> {role.mention} is not being monitored.",
                    ephemeral=True
                )
                return

            MONITORED_ROLES[role.id]['ping_users'] = None
            await save_config()

            await interaction.response.send_message(
                f"<:Accepted:1426930333789585509> Cleared ping list for {role.mention}. Will now ping whitelist users on alerts.",
                ephemeral=True
            )

        elif action == "toggle-monitoring":
            if not role:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Please specify a role to toggle monitoring.",
                    ephemeral=True
                )
                return

            if role.id in MONITORED_ROLES:
                # Stop monitoring
                del MONITORED_ROLES[role.id]
                await save_config()
                await interaction.response.send_message(
                    f"<:Accepted:1426930333789585509> Stopped monitoring {role.mention}.",
                    ephemeral=True
                )
            else:
                # Start monitoring
                MONITORED_ROLES[role.id] = {
                    'whitelist': [],
                    'channel_id': None,
                    'ping_users': None
                }
                await save_config()
                await interaction.response.send_message(
                    f"<:Accepted:1426930333789585509> Started monitoring {role.mention}. Remember to add authorized users and set an alert channel!",
                    ephemeral=True
                )

        else:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Invalid action. Use autocomplete to see available actions.",
                ephemeral=True
            )

    @role_monitor_config.autocomplete('action')
    async def action_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        """Autocomplete for action parameter"""
        actions = [
            app_commands.Choice(name="List Monitored Roles", value="list"),
            app_commands.Choice(name="Toggle User Authorization", value="toggle"),
            app_commands.Choice(name="Set Role Alert Channel", value="set-role-channel"),
            app_commands.Choice(name="Set Alert Channel to CURRENT", value="set-role-channel-current"),
            app_commands.Choice(name="Toggle User in Ping List", value="toggle-ping-user"),
            app_commands.Choice(name="Clear Ping List (use whitelist)", value="clear-ping-users"),
            app_commands.Choice(name="Toggle Role Monitoring", value="toggle-monitoring"),
        ]

        if current:
            return [choice for choice in actions if current.lower() in choice.name.lower()]

        return actions


async def setup(bot):
    await bot.add_cog(RoleWatcherCog(bot))