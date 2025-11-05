import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
from typing import Optional
from database import db

# Configuration
MODERATION_LOG_CHANNEL_ID = 1435489971342409809  # Replace with your moderation log channel ID
STAFF_ROLE_ID = 1234567890  # Replace with your staff role ID
COOLDOWN_MINUTES = 1  # Cooldown between requests


class TopicCog(commands.Cog):
    """Topic management commands"""

    topic_group = app_commands.Group(name="topic", description="Topic management commands")

    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        """Create table for topic change logs"""
        async with db.pool.acquire() as conn:
            await conn.execute('''
                               CREATE TABLE IF NOT EXISTS topic_change_requests
                               (
                                   id
                                   SERIAL
                                   PRIMARY
                                   KEY,
                                   user_id
                                   BIGINT
                                   NOT
                                   NULL,
                                   username
                                   TEXT
                                   NOT
                                   NULL,
                                   channel_id
                                   BIGINT
                                   NOT
                                   NULL,
                                   channel_name
                                   TEXT
                                   NOT
                                   NULL,
                                   guild_id
                                   BIGINT
                                   NOT
                                   NULL,
                                   new_topic
                                   TEXT
                                   NOT
                                   NULL,
                                   reason
                                   TEXT,
                                   requested_at
                                   TIMESTAMP
                                   DEFAULT
                                   NOW
                               (
                               ),
                                   message_id BIGINT
                                   )
                               ''')

    @topic_group.command(name="change", description="Request a topic change from staff")
    @app_commands.describe(
        reason="Why you want to change the topic."
    )
    async def topic_change(
            self,
            interaction: discord.Interaction,
            new_topic: str,
            reason: Optional[str]
    ):
        """Request a topic change in the current channel"""

        await interaction.response.defer()

        # Check cooldown
        async with db.pool.acquire() as conn:
            last_request = await conn.fetchrow(
                'SELECT requested_at FROM topic_change_requests WHERE user_id = $1 ORDER BY requested_at DESC LIMIT 1',
                interaction.user.id
            )

            if last_request:
                time_passed = datetime.utcnow() - last_request['requested_at']
                remaining = COOLDOWN_MINUTES - (time_passed.total_seconds() / 60)
                if remaining > 0:
                    remaining_minutes = int(remaining) + 1
                    await interaction.followup.send(
                        f"⏱️ You're on cooldown! Please wait **{remaining_minutes} minutes** before requesting another topic change.",
                        ephemeral=True
                    )
                    return

        # Create embed for user confirmation
        user_embed = discord.Embed(
            title="Topic Change",
            description="A staff member has been asked to change the topic; failure to do so will result in moderation.\n‎",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )

        user_embed.add_field(
            name="*Requested by*",
            value=f"*@{interaction.user.name}*",
            inline=False
        )

        user_embed.set_thumbnail(url=interaction.guild.icon.url if interaction.guild.icon else None)

        message = await interaction.followup.send(embed=user_embed)

        # Log to database
        async with db.pool.acquire() as conn:
            await conn.execute('''
                               INSERT INTO topic_change_requests
                               (user_id, username, channel_id, channel_name, guild_id, new_topic, reason, message_id)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                               ''',
                               interaction.user.id,
                               str(interaction.user),
                               interaction.channel.id,
                               interaction.channel.name,
                               interaction.guild.id,
                               new_topic,
                               reason,
                               message.id
                               )

        # Create detailed embed for staff log
        staff_embed = discord.Embed(
            title="Topic Change Request",
            color=discord.Color.gold(),
            timestamp=datetime.utcnow()
        )

        staff_embed.add_field(
            name="Requested by:",
            value=f"{interaction.user.mention} (`{interaction.user.name}` - `{interaction.user.id}`)",
            inline=True
        )

        staff_embed.add_field(
            name="Channel:",
            value=f"{interaction.channel.mention} (`#{interaction.channel.name}`)",
            inline=True
        )

        staff_embed.add_field(
            name="Reason:",
            value=f"```{reason}```",
            inline=False
        )

        staff_embed.set_thumbnail(url=interaction.user.display_avatar.url)
        staff_embed.set_footer(text=f"User ID: {interaction.user.id} | Request ID: {message.id}")

        # Send to moderation log channel
        log_channel = self.bot.get_channel(MODERATION_LOG_CHANNEL_ID)
        if log_channel:
            staff_role = interaction.guild.get_role(STAFF_ROLE_ID)
            await log_channel.send(
                content=f"{staff_role.mention if staff_role else '@Staff'} - Topic Change Request",
                embed=staff_embed
            )

async def setup(bot):
    await bot.add_cog(TopicCog(bot))
