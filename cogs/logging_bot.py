import discord
from discord.ext import commands
from discord import app_commands
import traceback
import json
from datetime import datetime
import sys

# ==================== CONFIGURATION ====================
# Channel IDs for different log types
COMMAND_LOG_CHANNEL_ID = 1430962630465687624  # Replace with your command log channel ID
GUILD_LOG_CHANNEL_ID = 1430962791543603320  # Replace with your guild events log channel ID

PERMISSION_ERROR_CHANNEL_ID = 1430958941940224213  # For Forbidden, Missing Permissions errors
COMMAND_ERROR_CHANNEL_ID = 1430959018574217226     # For CommandNotFound, BadArgument, etc.
CRITICAL_ERROR_CHANNEL_ID = 1430959142834540544    # For unexpected/serious errors
NETWORK_ERROR_CHANNEL_ID = 1430960127489347704
DATABASE_ERROR_CHANNEL_ID = 1430962285458886746  # File/JSON errors
GENERAL_ERROR_CHANNEL_ID = 1430960823903195300

# Enable/disable specific log types
LOG_SLASH_COMMANDS = True
LOG_PREFIX_COMMANDS = True
LOG_ERRORS = True
LOG_GUILD_EVENTS = True  # Join/leave, role changes, etc.

# Bot owner ID for critical error DMs
OWNER_ID = 678475709257089057


# =======================================================


class LoggingCog(commands.Cog):
    """Handles all bot logging - commands, errors, and events"""

    def __init__(self, bot):
        self.bot = bot
        # Store original error handlers
        self.bot.tree.on_error = self.on_app_command_error

    async def send_log(self, channel_id: int, embed: discord.Embed):
        """Send a log embed to the specified channel"""
        if not channel_id:
            return

        try:
            channel = await self.bot.fetch_channel(channel_id)
            if channel:
                await channel.send(embed=embed)
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
            color=discord.Color.blue(),
            timestamp=discord.utils.utcnow()
        )

        # Command info
        embed.add_field(
            name="Command",
            value=f"`/{command.name}`",
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
                    params.append(f"**{name}:** `{value}`")

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
            color=discord.Color.blue(),
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
            embed.add_field(
                name="Arguments",
                value=f"`{' '.join(str(arg) for arg in ctx.args[2:])}`",
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

        elif isinstance(error, (app_commands.CommandNotFound,
                                app_commands.CommandOnCooldown)):
            channel_id = COMMAND_ERROR_CHANNEL_ID
            error_category = "Command Input Error"

        elif isinstance(error, (FileNotFoundError, json.JSONDecodeError, KeyError)):
            channel_id = DATABASE_ERROR_CHANNEL_ID
            error_category = "Database/File Error"

        elif isinstance(error, (app_commands.CommandInvokeError, Exception)):
            channel_id = CRITICAL_ERROR_CHANNEL_ID
            error_category = "Critical Error"

        else:
            channel_id = GENERAL_ERROR_CHANNEL_ID
            error_category = "Uncategorized Error"

        # If no specific channel set, use general
        if not channel_id:
            channel_id = GENERAL_ERROR_CHANNEL_ID

        # Log to channel
        if channel_id:
            embed = discord.Embed(
                title=f"{error_category} <:Denied:1426930694633816248>",  # Shows category in title
                color=self._get_error_color(error_category),  # Color based on type
                timestamp=discord.utils.utcnow()
            )

            # Add a category field to make it clear
            embed.add_field(
                name="🏷️ Category",
                value=f"`{error_category}`",
                inline=True
            )

            # Command info
            if interaction.command:
                embed.add_field(
                    name="Command",
                    value=f"`/{interaction.command.name}`",
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

            await self.send_log(channel_id, embed)

        # Also send DM to owner for critical errors
        try:
            owner = await self.bot.fetch_user(OWNER_ID)
            dm_embed = discord.Embed(
                title="🚨 Critical Command Error",
                description=f"**Command:** `/{interaction.command.name if interaction.command else 'Unknown'}`\n**Error:** {type(error).__name__}",
                color=discord.Color.red(),
                timestamp=discord.utils.utcnow()
            )

            if interaction.guild:
                dm_embed.add_field(
                    name="Location",
                    value=f"{interaction.guild.name} ({interaction.guild.id})",
                    inline=False
                )

            await owner.send(embed=dm_embed)
        except:
            pass

        # Inform the user
        error_embed = discord.Embed(
            description="An error occurred while executing this command. The bot owner has been notified <:Denied:1426930694633816248>",
            color=discord.Color.red()
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
            "Permission Error": discord.Color.orange(),
            "Network/API Error": discord.Color.blue(),
            "Command Input Error": discord.Color.yellow(),
            "Database/File Error": discord.Color.purple(),
            "Critical Error": discord.Color.red(),
            "Uncategorized Error": discord.Color.greyple()
        }
        return colors.get(category, discord.Color.red())


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

        # Log to channel
        if channel_id:
            embed = discord.Embed(
                title="Prefix Command Error <:Denied:1426930694633816248>",
                color=discord.Color.red(),
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

            await self.send_log(ERROR_LOG_CHANNEL_ID, embed)

    # ==================== GUILD EVENT LOGGING ====================

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Log when a member joins"""
        if not LOG_GUILD_EVENTS or not GUILD_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="👋 Member Joined",
            color=discord.Color.green(),
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

        await self.send_log(GUILD_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Log when a member leaves"""
        if not LOG_GUILD_EVENTS or not GUILD_LOG_CHANNEL_ID:
            return

        embed = discord.Embed(
            title="👋 Member Left",
            color=discord.Color.orange(),
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
        roles = [f"{role.name} ({role.id})" for role in member.roles if role.name != "@everyone"]
        if roles:
            embed.add_field(
                name="Roles",
                value=", ".join(roles),
                inline=False
            )

        await self.send_log(GUILD_LOG_CHANNEL_ID, embed)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Log member updates (role changes, nickname changes)"""
        if not LOG_GUILD_EVENTS or not GUILD_LOG_CHANNEL_ID:
            return

        # Nickname change
        if before.nick != after.nick:
            embed = discord.Embed(
                title="✏️ Nickname Changed",
                color=discord.Color.blue(),
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

            await self.send_log(GUILD_LOG_CHANNEL_ID, embed)

        # Role changes
        if before.roles != after.roles:
            added_roles = [role for role in after.roles if role not in before.roles]
            removed_roles = [role for role in before.roles if role not in after.roles]

            if added_roles or removed_roles:
                embed = discord.Embed(
                    title="🎭 Roles Updated",
                    color=discord.Color.purple(),
                    timestamp=discord.utils.utcnow()
                )

                embed.set_thumbnail(url=after.display_avatar.url)

                embed.add_field(
                    name="Member",
                    value=f"{after.mention}\n`{after} ({after.id})`",
                    inline=False
                )

                if added_roles:
                    embed.add_field(
                        name="✅ Roles Added",
                        value=", ".join([f"{role.name} ({role.id})" for role in added_roles]),
                        inline=False
                    )

                if removed_roles:
                    embed.add_field(
                        name="Roles Removed <:Denied:1426930694633816248>",
                        value=", ".join([f"{role.name} ({role.id})" for role in removed_roles]),
                        inline=False
                    )

                await self.send_log(GUILD_LOG_CHANNEL_ID, embed)


async def setup(bot):
    """Load the logging cog"""
    await bot.add_cog(LoggingCog(bot))