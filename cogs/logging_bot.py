import discord
from discord.ext import commands
from discord import app_commands
import traceback
import json
from datetime import datetime
import sys

# ==================== CONFIGURATION ====================
# Channel IDs for different log types
COMMAND_LOG_CHANNEL_ID = 1430962630465687624  # Command usage logs
GUILD_LOG_CHANNEL_ID = 1430962791543603320  # Basic guild events (join/leave)

# Error log channels
PERMISSION_ERROR_CHANNEL_ID = 1430958941940224213  # Forbidden, Missing Permissions
COMMAND_ERROR_CHANNEL_ID = 1430959018574217226  # CommandNotFound, BadArgument, etc.
CRITICAL_ERROR_CHANNEL_ID = 1430959142834540544  # Unexpected/serious errors
NETWORK_ERROR_CHANNEL_ID = 1430960127489347704  # HTTP, Discord API errors
DATABASE_ERROR_CHANNEL_ID = 1430962285458886746  # File/JSON errors
GENERAL_ERROR_CHANNEL_ID = 1430960823903195300  # Uncategorized errors

# Guild event log channels (NEW)
MEMBER_LOG_CHANNEL_ID = 1432522282315550874  # Member join/leave/update
ROLE_LOG_CHANNEL_ID = 1432522578408374282  # Role changes (None = disabled)
CHANNEL_LOG_CHANNEL_ID = 1432522672188821575  # Channel create/delete/update
MESSAGE_LOG_CHANNEL_ID = 1432522723384627221  # Message edit/delete
MODERATION_LOG_CHANNEL_ID = 1432522769836412938  # Bans, kicks, timeouts
SERVER_LOG_CHANNEL_ID = 1432522823787610185  # Server settings changes
VOICE_LOG_CHANNEL_ID = 1432522870751232010  # Voice state changes

# Enable/disable specific log types
LOG_SLASH_COMMANDS = True
LOG_PREFIX_COMMANDS = True
LOG_ERRORS = True
LOG_GUILD_EVENTS = True

# Enable/disable specific guild event types
LOG_MEMBER_EVENTS = True  # Join/leave/nickname/role changes
LOG_ROLE_EVENTS = True  # Role create/delete/edit
LOG_CHANNEL_EVENTS = True  # Channel create/delete/edit
LOG_MESSAGE_EVENTS = True  # Message edit/delete
LOG_MODERATION_EVENTS = True  # Bans/kicks/timeouts
LOG_SERVER_EVENTS = True  # Server settings changes
LOG_VOICE_EVENTS = True  # Voice join/leave/mute/deafen

# Bot owner ID for critical error pings
OWNER_ID = 678475709257089057


# =======================================================


class LoggingCog(commands.Cog):
    """Handles all bot logging - commands, errors, and events"""

    def __init__(self, bot):
        self.bot = bot
        # Store original error handlers
        self.bot.tree.on_error = self.on_app_command_error

    async def send_log(self, channel_id: int, embed: discord.Embed, content: str = None):
        """Send a log embed to the specified channel"""
        if not channel_id:
            return

        try:
            channel = await self.bot.fetch_channel(channel_id)
            if channel:
                await channel.send(content=content, embed=embed)
        except Exception as e:
            print(f"Failed to send log to channel {channel_id}: {e}")

    # ==================== COMMAND LOGGING ====================

    @commands.Cog.listener()
    async def on_app_command_completion(self, interaction: discord.Interaction, command: app_commands.Command):
        """Log slash command usage"""
        if not LOG_SLASH_COMMANDS or not COMMAND_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="📝 Slash Command Used",
            color=discord.Color(0xadd8e6),
            timestamp=discord.utils.utcnow()
        )

        # Command info - Get full command path for group commands
        if command.parent:
            command_path = f"/{command.parent.name} {command.name}"
        else:
            command_path = f"/{command.name}"

        embed.add_field(
            name="Command",
            value=f"`{command_path}`",
            inline=True
        )

        # User info
        embed.add_field(
            name="User",
            value=f"{interaction.user.mention}\n`{interaction.user} ({interaction.user.id})`",
            inline=True
        )

        # Guild info
        if interaction.guild:
            embed.add_field(
                name="Guild",
                value=f"{interaction.guild.name}\n`{interaction.guild.id}`",
                inline=True
            )

            embed.add_field(
                name="Channel",
                value=f"{interaction.channel.mention}\n`{interaction.channel.name}`",
                inline=True
            )
        else:
            embed.add_field(
                name="Location",
                value="DM",
                inline=True
            )

        # Command parameters (if any)
        if interaction.namespace:
            params = []
            for name, value in interaction.namespace.__dict__.items():
                if value is not None and not name.startswith('_'):
                    # Truncate long values
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:97] + "..."
                    params.append(f"**{name}:** `{value_str}`")

            if params:
                embed.add_field(
                    name="Parameters",
                    value="\n".join(params),
                    inline=False
                )

        await self.send_log(COMMAND_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_command(self, ctx: commands.Context):
        """Log prefix command usage"""
        if not LOG_PREFIX_COMMANDS or not COMMAND_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="📝 Prefix Command Used",
            color=discord.Color(0xadd8e6),
            timestamp=discord.utils.utcnow()
        )

        # Command info
        embed.add_field(
            name="Command",
            value=f"`{ctx.prefix}{ctx.command}`",
            inline=True
        )

        # User info
        embed.add_field(
            name="User",
            value=f"{ctx.author.mention}\n`{ctx.author} ({ctx.author.id})`",
            inline=True
        )

        # Guild info
        if ctx.guild:
            embed.add_field(
                name="Guild",
                value=f"{ctx.guild.name}\n`{ctx.guild.id}`",
                inline=True
            )

            embed.add_field(
                name="Channel",
                value=f"{ctx.channel.mention}\n`{ctx.channel.name}`",
                inline=True
            )
        else:
            embed.add_field(
                name="Location",
                value="DM",
                inline=True
            )

        # Command arguments
        if ctx.args[2:]:  # Skip self and ctx
            args_str = ' '.join(str(arg) for arg in ctx.args[2:])
            if len(args_str) > 1024:
                args_str = args_str[:1021] + "..."
            embed.add_field(
                name="Arguments",
                value=f"`{args_str}`",
                inline=False
            )

        # Message link
        if ctx.message:
            embed.add_field(
                name="Message",
                value=f"[Jump to Message]({ctx.message.jump_url})",
                inline=False
            )

        await self.send_log(COMMAND_LOG_CHANNEL_ID, embed)

    # ==================== ERROR LOGGING ====================

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Log slash command errors to appropriate channels"""
        if not LOG_ERRORS:
            return

        # Unwrap CommandInvokeError to get the real error
        original_error = error
        if isinstance(error, app_commands.CommandInvokeError):
            error = error.original

        # Determine which channel based on error type
        channel_id = None
        error_category = "Unknown"

        if isinstance(error, (discord.Forbidden, app_commands.MissingPermissions,
                              app_commands.BotMissingPermissions)):
            channel_id = PERMISSION_ERROR_CHANNEL_ID
            error_category = "Permission Error"

        elif isinstance(error, (discord.HTTPException, discord.NotFound, discord.DiscordServerError)):
            channel_id = NETWORK_ERROR_CHANNEL_ID
            error_category = "Network/API Error"

        elif isinstance(error, (app_commands.CommandNotFound, app_commands.CommandOnCooldown)):
            channel_id = COMMAND_ERROR_CHANNEL_ID
            error_category = "Command Input Error"

        elif isinstance(error, (FileNotFoundError, json.JSONDecodeError, KeyError)):
            channel_id = DATABASE_ERROR_CHANNEL_ID
            error_category = "Database/File Error"

        elif isinstance(error, Exception):
            channel_id = CRITICAL_ERROR_CHANNEL_ID
            error_category = "Critical Error"

        else:
            channel_id = GENERAL_ERROR_CHANNEL_ID
            error_category = "Uncategorized Error"

        # If no specific channel set, use general
        if not channel_id:
            channel_id = GENERAL_ERROR_CHANNEL_ID

        # 🚨 NEW: Ping owner for critical errors NOT caused by owner
        ping_content = None
        if error_category == "Critical Error" and interaction.user.id != OWNER_ID:
            ping_content = f"<@{OWNER_ID}>"

        # Log to channel
        if channel_id:
            embed = discord.Embed(
                title=f"{error_category} ❌",
                color=self._get_error_color(error_category),
                timestamp=discord.utils.utcnow()
            )

            # Add a category field to make it clear
            embed.add_field(
                name="🏷️ Category",
                value=f"`{error_category}`",
                inline=True
            )

            # Command info - Get full command path for group commands
            if interaction.command:
                if interaction.command.parent:
                    command_path = f"/{interaction.command.parent.name} {interaction.command.name}"
                else:
                    command_path = f"/{interaction.command.name}"

                embed.add_field(
                    name="Command",
                    value=f"`{command_path}`",
                    inline=True
                )

            # User info
            embed.add_field(
                name="User",
                value=f"{interaction.user.mention}\n`{interaction.user} ({interaction.user.id})`",
                inline=True
            )

            # Guild info
            if interaction.guild:
                embed.add_field(
                    name="Guild",
                    value=f"{interaction.guild.name}\n`{interaction.guild.id}`",
                    inline=True
                )

            # Error info
            embed.add_field(
                name="Error Type",
                value=f"`{type(error).__name__}`",
                inline=False
            )

            error_msg = str(error)
            if len(error_msg) > 1024:
                error_msg = error_msg[:1021] + "..."

            embed.add_field(
                name="Error Message",
                value=f"```{error_msg}```",
                inline=False
            )

            # Traceback
            tb = ''.join(traceback.format_exception(type(error), error, error.__traceback__))
            if len(tb) > 1024:
                tb = "..." + tb[-1021:]

            embed.add_field(
                name="Traceback",
                value=f"```python\n{tb}```",
                inline=False
            )

            await self.send_log(channel_id, embed, content=ping_content)

        # Inform the user
        error_embed = discord.Embed(
            description="An error occurred while executing this command. It has been logged ❌",
            color=discord.Color(0xf24d4d)
        )

        try:
            if interaction.response.is_done():
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
        except:
            pass

    def _get_error_color(self, category: str) -> discord.Color:
        """Get color based on error category"""
        colors = {
            "Permission Error": discord.Color(0xffa756),
            "Network/API Error": discord.Color(0xadd8e6),
            "Command Input Error": discord.Color(0xffee8c),
            "Database/File Error": discord.Color(0xcbc3e3),
            "Critical Error": discord.Color(0xf24d4d),
            "Uncategorized Error": discord.Color(0xd4e2eb)
        }
        return colors.get(category, discord.Color(0xf24d4d))

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Log prefix command errors to appropriate channels"""
        if not LOG_ERRORS:
            return

        # Skip CommandNotFound (too spammy) - optional
        if isinstance(error, commands.CommandNotFound):
            return

        # Determine channel
        channel_id = None
        error_category = "Unknown"

        if isinstance(error, (commands.MissingPermissions, commands.BotMissingPermissions,
                              discord.Forbidden)):
            channel_id = PERMISSION_ERROR_CHANNEL_ID
            error_category = "Permission Error"

        elif isinstance(error, (discord.HTTPException, discord.NotFound)):
            channel_id = NETWORK_ERROR_CHANNEL_ID
            error_category = "Network/API Error"

        elif isinstance(error, (commands.MissingRequiredArgument, commands.BadArgument,
                                commands.CommandOnCooldown, commands.CommandNotFound)):
            channel_id = COMMAND_ERROR_CHANNEL_ID
            error_category = "Command Input Error"

        elif isinstance(error, (FileNotFoundError, KeyError)):
            channel_id = DATABASE_ERROR_CHANNEL_ID
            error_category = "Database/File Error"

        else:
            channel_id = CRITICAL_ERROR_CHANNEL_ID
            error_category = "Critical Error"

        if not channel_id:
            channel_id = GENERAL_ERROR_CHANNEL_ID

        # 🚨 NEW: Ping owner for critical errors NOT caused by owner
        ping_content = None
        if error_category == "Critical Error" and ctx.author.id != OWNER_ID:
            ping_content = f"<@{OWNER_ID}>"

        # Log to channel
        if channel_id:
            embed = discord.Embed(
                title="Prefix Command Error ❌",
                color=discord.Color(0xf24d4d),
                timestamp=discord.utils.utcnow()
            )

            # Command info
            embed.add_field(
                name="Command",
                value=f"`{ctx.prefix}{ctx.command}`" if ctx.command else "Unknown",
                inline=True
            )

            # User info
            embed.add_field(
                name="User",
                value=f"{ctx.author.mention}\n`{ctx.author} ({ctx.author.id})`",
                inline=True
            )

            # Guild info
            if ctx.guild:
                embed.add_field(
                    name="Guild",
                    value=f"{ctx.guild.name}\n`{ctx.guild.id}`",
                    inline=True
                )

            # Error info
            embed.add_field(
                name="Error Type",
                value=f"`{type(error).__name__}`",
                inline=False
            )

            error_msg = str(error)
            if len(error_msg) > 1024:
                error_msg = error_msg[:1021] + "..."

            embed.add_field(
                name="Error Message",
                value=f"```{error_msg}```",
                inline=False
            )

            # Traceback
            tb = ''.join(traceback.format_exception(type(error), error, error.__traceback__))
            if len(tb) > 1024:
                tb = "..." + tb[-1021:]

            embed.add_field(
                name="Traceback",
                value=f"```python\n{tb}```",
                inline=False
            )

            # Message link
            if ctx.message:
                embed.add_field(
                    name="Message",
                    value=f"[Jump to Message]({ctx.message.jump_url})",
                    inline=False
                )

            await self.send_log(channel_id, embed, content=ping_content)

    # ==================== MEMBER EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Log when a member joins"""
        if not LOG_GUILD_EVENTS or not LOG_MEMBER_EVENTS or not MEMBER_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="👋 Member Joined",
            color=discord.Color(0x2ecc71),
            timestamp=discord.utils.utcnow()
        )

        embed.set_thumbnail(url=member.display_avatar.url)

        embed.add_field(
            name="Member",
            value=f"{member.mention}\n`{member} ({member.id})`",
            inline=True
        )

        embed.add_field(
            name="Account Created",
            value=discord.utils.format_dt(member.created_at, style='R'),
            inline=True
        )

        embed.add_field(
            name="Member Count",
            value=f"{member.guild.member_count}",
            inline=True
        )

        # Check account age (flag suspicious accounts)
        account_age = (discord.utils.utcnow() - member.created_at).days
        if account_age < 7:
            embed.add_field(
                name="⚠️ Warning",
                value=f"Account is only {account_age} day(s) old",
                inline=False
            )

        await self.send_log(MEMBER_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Log when a member leaves"""
        if not LOG_GUILD_EVENTS or not LOG_MEMBER_EVENTS or not MEMBER_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="👋 Member Left",
            color=discord.Color(0xffa756),
            timestamp=discord.utils.utcnow()
        )

        embed.set_thumbnail(url=member.display_avatar.url)

        embed.add_field(
            name="Member",
            value=f"{member.mention}\n`{member} ({member.id})`",
            inline=True
        )

        embed.add_field(
            name="Joined Server",
            value=discord.utils.format_dt(member.joined_at, style='R') if member.joined_at else "Unknown",
            inline=True
        )

        embed.add_field(
            name="Member Count",
            value=f"{member.guild.member_count}",
            inline=True
        )

        # List roles (excluding @everyone)
        roles = [f"{role.name}" for role in member.roles if role.name != "@everyone"]
        if roles:
            roles_str = ", ".join(roles)
            if len(roles_str) > 1024:
                roles_str = roles_str[:1021] + "..."
            embed.add_field(
                name="Roles",
                value=roles_str,
                inline=False
            )

        await self.send_log(MEMBER_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Log member updates (role changes, nickname changes)"""
        if not LOG_GUILD_EVENTS or not LOG_MEMBER_EVENTS:
            return

        # Nickname change
        if before.nick != after.nick:
            if not MEMBER_LOG_CHANNEL_ID:
                return

            embed = discord.Embed(
                title="✏️ Nickname Changed",
                color=discord.Color(0xadd8e6),
                timestamp=discord.utils.utcnow()
            )

            embed.set_thumbnail(url=after.display_avatar.url)

            embed.add_field(
                name="Member",
                value=f"{after.mention}\n`{after} ({after.id})`",
                inline=False
            )

            embed.add_field(
                name="Before",
                value=f"`{before.nick or before.name}`",
                inline=True
            )

            embed.add_field(
                name="After",
                value=f"`{after.nick or after.name}`",
                inline=True
            )

            # Try to get who changed it from audit logs
            try:
                async for entry in after.guild.audit_logs(limit=5, action=discord.AuditLogAction.member_update):
                    if entry.target.id == after.id:
                        embed.add_field(
                            name="Changed By",
                            value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                            inline=False
                        )
                        break
            except:
                pass

            await self.send_log(MEMBER_LOG_CHANNEL_ID, embed)

        # Role changes
        if before.roles != after.roles:
            channel_id = ROLE_LOG_CHANNEL_ID if ROLE_LOG_CHANNEL_ID else MEMBER_LOG_CHANNEL_ID
            if not channel_id:
                return

            added_roles = [role for role in after.roles if role not in before.roles]
            removed_roles = [role for role in before.roles if role not in after.roles]

            if added_roles or removed_roles:
                embed = discord.Embed(
                    title="🎭 Roles Updated",
                    color=discord.Color(0xcbc3e3),
                    timestamp=discord.utils.utcnow()
                )

                embed.set_thumbnail(url=after.display_avatar.url)

                embed.add_field(
                    name="Member",
                    value=f"{after.mention}\n`{after} ({after.id})`",
                    inline=False
                )

                if added_roles:
                    roles_str = ", ".join([f"{role.mention}" for role in added_roles])
                    if len(roles_str) > 1024:
                        roles_str = roles_str[:1021] + "..."
                    embed.add_field(
                        name="Roles Added ✅",
                        value=roles_str,
                        inline=False
                    )

                if removed_roles:
                    roles_str = ", ".join([f"{role.mention}" for role in removed_roles])
                    if len(roles_str) > 1024:
                        roles_str = roles_str[:1021] + "..."
                    embed.add_field(
                        name="Roles Removed ❌",
                        value=roles_str,
                        inline=False
                    )

                # Try to get who changed it from audit logs
                try:
                    async for entry in after.guild.audit_logs(limit=5,
                                                              action=discord.AuditLogAction.member_role_update):
                        if entry.target.id == after.id:
                            embed.add_field(
                                name="Changed By",
                                value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                                inline=False
                            )
                            break
                except:
                    pass

                await self.send_log(channel_id, embed)

    # ==================== ROLE EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        """Log when a role is created"""
        if not LOG_GUILD_EVENTS or not LOG_ROLE_EVENTS or not ROLE_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="🎭 Role Created",
            color=discord.Color(0x2ecc71),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Role",
            value=f"{role.mention}\n`{role.name} ({role.id})`",
            inline=True
        )

        embed.add_field(
            name="Color",
            value=f"`{str(role.color)}`",
            inline=True
        )

        embed.add_field(
            name="Position",
            value=f"`{role.position}`",
            inline=True
        )

        # Try to get who created it from audit logs
        try:
            async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
                if entry.target.id == role.id:
                    embed.add_field(
                        name="Created By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(ROLE_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        """Log when a role is deleted"""
        if not LOG_GUILD_EVENTS or not LOG_ROLE_EVENTS or not ROLE_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="🎭 Role Deleted",
            color=discord.Color(0xf24d4d),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Role",
            value=f"`{role.name} ({role.id})`",
            inline=True
        )

        embed.add_field(
            name="Color",
            value=f"`{str(role.color)}`",
            inline=True
        )

        # Try to get who deleted it from audit logs
        try:
            async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
                if entry.target.id == role.id:
                    embed.add_field(
                        name="Deleted By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(ROLE_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        """Log when a role is updated"""
        if not LOG_GUILD_EVENTS or not LOG_ROLE_EVENTS or not ROLE_LOG_CHANNEL_ID:
            return

        changes = []

        if before.name != after.name:
            changes.append(f"**Name:** `{before.name}` → `{after.name}`")

        if before.color != after.color:
            changes.append(f"**Color:** `{before.color}` → `{after.color}`")

        if before.permissions != after.permissions:
            changes.append("**Permissions:** Changed")

        if before.hoist != after.hoist:
            changes.append(f"**Hoisted:** `{before.hoist}` → `{after.hoist}`")

        if before.mentionable != after.mentionable:
            changes.append(f"**Mentionable:** `{before.mentionable}` → `{after.mentionable}`")

        if not changes:
            return

        embed = discord.Embed(
            title="🎭 Role Updated",
            color=discord.Color(0xadd8e6),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Role",
            value=f"{after.mention}\n`{after.name} ({after.id})`",
            inline=False
        )

        embed.add_field(
            name="Changes",
            value="\n".join(changes),
            inline=False
        )

        # Try to get who updated it from audit logs
        try:
            async for entry in after.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
                if entry.target.id == after.id:
                    embed.add_field(
                        name="Updated By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(ROLE_LOG_CHANNEL_ID, embed)

    # ==================== CHANNEL EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel):
        """Log when a channel is created"""
        if not LOG_GUILD_EVENTS or not LOG_CHANNEL_EVENTS or not CHANNEL_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="📺 Channel Created",
            color=discord.Color(0x2ecc71),
            timestamp=discord.utils.utcnow()
        )

        channel_type = "Text" if isinstance(channel, discord.TextChannel) else "Voice" if isinstance(channel,
                                                                                                     discord.VoiceChannel) else "Category" if isinstance(
            channel, discord.CategoryChannel) else "Other"

        embed.add_field(
            name="Channel",
            value=f"{channel.mention if hasattr(channel, 'mention') else channel.name}\n`{channel.name} ({channel.id})`",
            inline=True
        )

        embed.add_field(
            name="Type",
            value=f"`{channel_type}`",
            inline=True
        )

        if hasattr(channel, 'category') and channel.category:
            embed.add_field(
                name="Category",
                value=f"`{channel.category.name}`",
                inline=True
            )

        # Try to get who created it from audit logs
        try:
            async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
                if entry.target.id == channel.id:
                    embed.add_field(
                        name="Created By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(CHANNEL_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel):
        """Log when a channel is deleted"""
        if not LOG_GUILD_EVENTS or not LOG_CHANNEL_EVENTS or not CHANNEL_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="📺 Channel Deleted",
            color=discord.Color(0xf24d4d),
            timestamp=discord.utils.utcnow()
        )

        channel_type = "Text" if isinstance(channel, discord.TextChannel) else "Voice" if isinstance(channel,
                                                                                                     discord.VoiceChannel) else "Category" if isinstance(
            channel, discord.CategoryChannel) else "Other"

        embed.add_field(
            name="Channel",
            value=f"`{channel.name} ({channel.id})`",
            inline=True
        )

        embed.add_field(
            name="Type",
            value=f"`{channel_type}`",
            inline=True
        )

        # Try to get who deleted it from audit logs
        try:
            async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
                if entry.target.id == channel.id:
                    embed.add_field(
                        name="Deleted By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(CHANNEL_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before, after):
        """Log when a channel is updated"""
        if not LOG_GUILD_EVENTS or not LOG_CHANNEL_EVENTS or not CHANNEL_LOG_CHANNEL_ID:
            return

        changes = []

        if before.name != after.name:
            changes.append(f"**Name:** `{before.name}` → `{after.name}`")

        if hasattr(before, 'topic') and hasattr(after, 'topic') and before.topic != after.topic:
            before_topic = before.topic[:50] if before.topic else "None"
            after_topic = after.topic[:50] if after.topic else "None"
            changes.append(f"**Topic:** `{before_topic}` → `{after_topic}`")

        if hasattr(before, 'nsfw') and hasattr(after, 'nsfw') and before.nsfw != after.nsfw:
            changes.append(f"**NSFW:** `{before.nsfw}` → `{after.nsfw}`")

        if hasattr(before, 'slowmode_delay') and hasattr(after,
                                                         'slowmode_delay') and before.slowmode_delay != after.slowmode_delay:
            changes.append(f"**Slowmode:** `{before.slowmode_delay}s` → `{after.slowmode_delay}s`")

        if not changes:
            return

        embed = discord.Embed(
            title="📺 Channel Updated",
            color=discord.Color(0xadd8e6),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Channel",
            value=f"{after.mention if hasattr(after, 'mention') else after.name}\n`{after.name} ({after.id})`",
            inline=False
        )

        embed.add_field(
            name="Changes",
            value="\n".join(changes),
            inline=False
        )

        # Try to get who updated it from audit logs
        try:
            async for entry in after.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_update):
                if entry.target.id == after.id:
                    embed.add_field(
                        name="Updated By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(CHANNEL_LOG_CHANNEL_ID, embed)

    # ==================== MESSAGE EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        """Log when a message is deleted"""
        if not LOG_GUILD_EVENTS or not LOG_MESSAGE_EVENTS or not MESSAGE_LOG_CHANNEL_ID:
            return

        # Ignore bot messages
        if message.author.bot:
            return

        embed = discord.Embed(
            title="🗑️ Message Deleted",
            color=discord.Color(0xf24d4d),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Author",
            value=f"{message.author.mention}\n`{message.author} ({message.author.id})`",
            inline=True
        )

        embed.add_field(
            name="Channel",
            value=f"{message.channel.mention}\n`{message.channel.name}`",
            inline=True
        )

        # Message content (truncated)
        content = message.content if message.content else "*No text content*"
        if len(content) > 1024:
            content = content[:1021] + "..."

        embed.add_field(
            name="Content",
            value=content,
            inline=False
        )

        # Attachments
        if message.attachments:
            attachments_list = [f"[{att.filename}]({att.url})" for att in message.attachments]
            embed.add_field(
                name="Attachments",
                value="\n".join(attachments_list[:5]),
                inline=False
            )

        # Try to get who deleted it from audit logs
        try:
            async for entry in message.guild.audit_logs(limit=1, action=discord.AuditLogAction.message_delete):
                if entry.target.id == message.author.id and (
                        discord.utils.utcnow() - entry.created_at).total_seconds() < 5:
                    embed.add_field(
                        name="Deleted By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=False
                    )
                    break
        except:
            pass

        await self.send_log(MESSAGE_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """Log when a message is edited"""
        if not LOG_GUILD_EVENTS or not LOG_MESSAGE_EVENTS or not MESSAGE_LOG_CHANNEL_ID:
            return

        # Ignore bot messages and embeds
        if before.author.bot or before.content == after.content:
            return

        embed = discord.Embed(
            title="✏️ Message Edited",
            color=discord.Color(0xadd8e6),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Author",
            value=f"{after.author.mention}\n`{after.author} ({after.author.id})`",
            inline=True
        )

        embed.add_field(
            name="Channel",
            value=f"{after.channel.mention}\n`{after.channel.name}`",
            inline=True
        )

        # Before content
        before_content = before.content if before.content else "*No text content*"
        if len(before_content) > 1024:
            before_content = before_content[:1021] + "..."

        embed.add_field(
            name="Before",
            value=before_content,
            inline=False
        )

        # After content
        after_content = after.content if after.content else "*No text content*"
        if len(after_content) > 1024:
            after_content = after_content[:1021] + "..."

        embed.add_field(
            name="After",
            value=after_content,
            inline=False
        )

        embed.add_field(
            name="Message Link",
            value=f"[Jump to Message]({after.jump_url})",
            inline=False
        )

        await self.send_log(MESSAGE_LOG_CHANNEL_ID, embed)

    # ==================== MODERATION EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        """Log when a member is banned"""
        if not LOG_GUILD_EVENTS or not LOG_MODERATION_EVENTS or not MODERATION_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="🔨 Member Banned",
            color=discord.Color(0xf24d4d),
            timestamp=discord.utils.utcnow()
        )

        embed.set_thumbnail(url=user.display_avatar.url)

        embed.add_field(
            name="User",
            value=f"{user.mention}\n`{user} ({user.id})`",
            inline=True
        )

        # Try to get ban info from audit logs
        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
                if entry.target.id == user.id:
                    embed.add_field(
                        name="Banned By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=True
                    )
                    if entry.reason:
                        embed.add_field(
                            name="Reason",
                            value=f"`{entry.reason}`",
                            inline=False
                        )
                    break
        except:
            pass

        await self.send_log(MODERATION_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        """Log when a member is unbanned"""
        if not LOG_GUILD_EVENTS or not LOG_MODERATION_EVENTS or not MODERATION_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="🔓 Member Unbanned",
            color=discord.Color(0x2ecc71),
            timestamp=discord.utils.utcnow()
        )

        embed.set_thumbnail(url=user.display_avatar.url)

        embed.add_field(
            name="User",
            value=f"{user.mention}\n`{user} ({user.id})`",
            inline=True
        )

        # Try to get unban info from audit logs
        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.unban):
                if entry.target.id == user.id:
                    embed.add_field(
                        name="Unbanned By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=True
                    )
                    if entry.reason:
                        embed.add_field(
                            name="Reason",
                            value=f"`{entry.reason}`",
                            inline=False
                        )
                    break
        except:
            pass

        await self.send_log(MODERATION_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_member_kick(self, guild: discord.Guild, user: discord.User):
        """Log when a member is kicked (via audit logs)"""
        if not LOG_GUILD_EVENTS or not LOG_MODERATION_EVENTS or not MODERATION_LOG_CHANNEL_ID:
            return

        # This requires audit log checking since there's no direct kick event
        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
                if (discord.utils.utcnow() - entry.created_at).total_seconds() < 5:
                    embed = discord.Embed(
                        title="👢 Member Kicked",
                        color=discord.Color(0xffa756),
                        timestamp=discord.utils.utcnow()
                    )

                    embed.set_thumbnail(url=entry.target.display_avatar.url)

                    embed.add_field(
                        name="User",
                        value=f"{entry.target.mention}\n`{entry.target} ({entry.target.id})`",
                        inline=True
                    )

                    embed.add_field(
                        name="Kicked By",
                        value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                        inline=True
                    )

                    if entry.reason:
                        embed.add_field(
                            name="Reason",
                            value=f"`{entry.reason}`",
                            inline=False
                        )

                    await self.send_log(MODERATION_LOG_CHANNEL_ID, embed)
                    break
        except:
            pass

    # ==================== SERVER EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_guild_update(self, before: discord.Guild, after: discord.Guild):
        """Log when server settings are updated"""
        if not LOG_GUILD_EVENTS or not LOG_SERVER_EVENTS or not SERVER_LOG_CHANNEL_ID:
            return

        changes = []

        if before.name != after.name:
            changes.append(f"**Name:** `{before.name}` → `{after.name}`")

        if before.icon != after.icon:
            changes.append("**Icon:** Changed")

        if before.banner != after.banner:
            changes.append("**Banner:** Changed")

        if before.verification_level != after.verification_level:
            changes.append(f"**Verification Level:** `{before.verification_level}` → `{after.verification_level}`")

        if before.default_notifications != after.default_notifications:
            changes.append(
                f"**Default Notifications:** `{before.default_notifications}` → `{after.default_notifications}`")

        if not changes:
            return

        embed = discord.Embed(
            title="🏠 Server Updated",
            color=discord.Color(0xadd8e6),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Server",
            value=f"`{after.name} ({after.id})`",
            inline=False
        )

        embed.add_field(
            name="Changes",
            value="\n".join(changes),
            inline=False
        )

        # Try to get who updated it from audit logs
        try:
            async for entry in after.audit_logs(limit=1, action=discord.AuditLogAction.guild_update):
                embed.add_field(
                    name="Updated By",
                    value=f"{entry.user.mention}\n`{entry.user} ({entry.user.id})`",
                    inline=False
                )
                break
        except:
            pass

        await self.send_log(SERVER_LOG_CHANNEL_ID, embed)

    # ==================== VOICE EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState,
                                    after: discord.VoiceState):
        """Log voice state changes"""
        if not LOG_GUILD_EVENTS or not LOG_VOICE_EVENTS or not VOICE_LOG_CHANNEL_ID:
            return

        # Ignore bot voice state changes
        if member.bot:
            return

        # Member joined voice
        if before.channel is None and after.channel is not None:
            embed = discord.Embed(
                title="🔊 Voice Join",
                color=discord.Color(0x2ecc71),
                timestamp=discord.utils.utcnow()
            )

            embed.add_field(
                name="Member",
                value=f"{member.mention}\n`{member} ({member.id})`",
                inline=True
            )

            embed.add_field(
                name="Channel",
                value=f"{after.channel.mention}\n`{after.channel.name}`",
                inline=True
            )

            await self.send_log(VOICE_LOG_CHANNEL_ID, embed)

        # Member left voice
        elif before.channel is not None and after.channel is None:
            embed = discord.Embed(
                title="🔇 Voice Leave",
                color=discord.Color(0xf24d4d),
                timestamp=discord.utils.utcnow()
            )

            embed.add_field(
                name="Member",
                value=f"{member.mention}\n`{member} ({member.id})`",
                inline=True
            )

            embed.add_field(
                name="Channel",
                value=f"`{before.channel.name}`",
                inline=True
            )

            await self.send_log(VOICE_LOG_CHANNEL_ID, embed)

        # Member moved voice channels
        elif before.channel != after.channel:
            embed = discord.Embed(
                title="🔄 Voice Move",
                color=discord.Color(0xadd8e6),
                timestamp=discord.utils.utcnow()
            )

            embed.add_field(
                name="Member",
                value=f"{member.mention}\n`{member} ({member.id})`",
                inline=False
            )

            embed.add_field(
                name="From",
                value=f"`{before.channel.name}`",
                inline=True
            )

            embed.add_field(
                name="To",
                value=f"{after.channel.mention}\n`{after.channel.name}`",
                inline=True
            )

            await self.send_log(VOICE_LOG_CHANNEL_ID, embed)

        # Member muted/unmuted
        elif before.self_mute != after.self_mute or before.mute != after.mute:
            if after.self_mute or after.mute:
                embed = discord.Embed(
                    title="🔇 Voice Mute",
                    color=discord.Color(0xffa756),
                    timestamp=discord.utils.utcnow()
                )
            else:
                embed = discord.Embed(
                    title="🔊 Voice Unmute",
                    color=discord.Color(0x2ecc71),
                    timestamp=discord.utils.utcnow()
                )

            embed.add_field(
                name="Member",
                value=f"{member.mention}\n`{member} ({member.id})`",
                inline=True
            )

            embed.add_field(
                name="Channel",
                value=f"{after.channel.mention}\n`{after.channel.name}`",
                inline=True
            )

            mute_type = "Server Muted" if after.mute else "Self Muted" if after.self_mute else "Unmuted"
            embed.add_field(
                name="Type",
                value=f"`{mute_type}`",
                inline=True
            )

            await self.send_log(VOICE_LOG_CHANNEL_ID, embed)


async def setup(bot):
    """Load the logging cog"""
    await bot.add_cog(LoggingCog(bot))