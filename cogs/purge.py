import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
from typing import Optional

# Configuration - Role IDs that can use /purge commands
ALLOWED_ROLE_IDS = [
    1389550689113473024,  # Replace with your role IDs
    1389113393511923863,
    1389113460687765534,
    1285474077556998196,
    1365536209681514636
]

LOG_CHANNEL_ID = 1435499104221532231  # Replace with your logging channel ID


def has_allowed_roles():
    """Check if user has any of the allowed roles"""

    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False

        user_role_ids = [role.id for role in interaction.user.roles]
        has_role = any(role_id in user_role_ids for role_id in ALLOWED_ROLE_IDS)

        if not has_role:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use this command.",
                ephemeral=True
            )
        return has_role

    return app_commands.check(predicate)


class PurgeCog(commands.Cog):
    """Message purge commands"""

    def __init__(self, bot):
        self.bot = bot

    purge = app_commands.Group(name="purge", description="Message purge commands")

    async def log_purge(self, interaction: discord.Interaction, purge_type: str, count: int, deleted_count: int,
                        target: Optional[discord.User] = None):
        """Log purge action to logging channel"""
        log_channel = self.bot.get_channel(LOG_CHANNEL_ID)
        if not log_channel:
            return

        embed = discord.Embed(
            title="Messages Purged",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )

        embed.add_field(
            name="Moderator:",
            value=f"{interaction.user.mention} (`{interaction.user.name}` - `{interaction.user.id}`)",
            inline=True
        )

        embed.add_field(
            name="Channel:",
            value=f"{interaction.channel.mention} (`#{interaction.channel.name}`)",
            inline=True
        )

        embed.add_field(
            name="Purge Type:",
            value=f"`{purge_type}`",
            inline=True
        )

        if target:
            embed.add_field(
                name="Target User:",
                value=f"{target.mention} (`{target.name}` - `{target.id}`)",
                inline=False
            )

        embed.add_field(
            name="Messages Deleted:",
            value=f"**{deleted_count}** out of {count} requested",
            inline=False
        )

        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.set_footer(text=f"User ID: {interaction.user.id}")

        await log_channel.send(embed=embed)

    @purge.command(name="user", description="Purge messages from a specific user")
    @app_commands.describe(
        count="Number of messages to check (max 100)",
        user="The user whose messages to delete (defaults to you)"
    )
    @has_allowed_roles()
    async def purge_user(
            self,
            interaction: discord.Interaction,
            count: app_commands.Range[int, 1, 100],
            user: Optional[discord.User] = None
    ):
        """Purge messages from a specific user"""

        await interaction.response.defer(ephemeral=True)

        # Default to the command user if no user specified
        target_user = user if user else interaction.user

        # Delete messages
        def check(message):
            return message.author.id == target_user.id

        try:
            deleted = await interaction.channel.purge(limit=count, check=check)
            deleted_count = len(deleted)

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Successfully deleted **{deleted_count}** message(s) from {target_user.mention}.",
                ephemeral=True
            )

            # Log the action
            await self.log_purge(interaction, "User", count, deleted_count, target_user)

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> I don't have permission to delete messages in this channel.",
                ephemeral=True
            )
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> An error occurred while deleting messages: {str(e)}",
                ephemeral=True
            )

    @purge.command(name="bot", description="Purge messages from bots")
    @app_commands.describe(
        count="Number of messages to check (max 100)"
    )
    @has_allowed_roles()
    async def purge_bot(
            self,
            interaction: discord.Interaction,
            count: app_commands.Range[int, 1, 100]
    ):
        """Purge messages from bots"""

        await interaction.response.defer(ephemeral=True)

        # Delete bot messages
        def check(message):
            return message.author.bot

        try:
            deleted = await interaction.channel.purge(limit=count, check=check)
            deleted_count = len(deleted)

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Successfully deleted **{deleted_count}** bot message(s).",
                ephemeral=True
            )

            # Log the action
            await self.log_purge(interaction, "Bot", count, deleted_count)

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> I don't have permission to delete messages in this channel.",
                ephemeral=True
            )
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> An error occurred while deleting messages: {str(e)}",
                ephemeral=True
            )

    @purge.command(name="all", description="Purge all messages")
    @app_commands.describe(
        count="Number of messages to delete (max 100)"
    )
    @has_allowed_roles()
    async def purge_all(
            self,
            interaction: discord.Interaction,
            count: app_commands.Range[int, 1, 100]
    ):
        """Purge all messages"""

        await interaction.response.defer(ephemeral=True)

        # Delete all messages
        try:
            deleted = await interaction.channel.purge(limit=count)
            deleted_count = len(deleted)

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Successfully deleted **{deleted_count}** message(s).",
                ephemeral=True
            )

            # Log the action
            await self.log_purge(interaction, "All", count, deleted_count)

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> I don't have permission to delete messages in this channel.",
                ephemeral=True
            )
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> An error occurred while deleting messages: {str(e)}",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(PurgeCog(bot))