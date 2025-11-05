import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
import time

ALLOWED_ROLE_IDS = [
    1389550689113473024,
    1389157641799991347,
    1389111326571499590  # Replace with your role IDs
]


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


class PingCog(commands.Cog):
    """Ping and latency commands"""

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="ping", description="Check the bot's latency")
    async def ping(self, interaction: discord.Interaction):
        """Display bot latency and API ping"""

        # Measure response time (interaction latency)
        start_time = time.time()
        await interaction.response.defer()
        end_time = time.time()

        # Calculate latencies
        interaction_latency = (end_time - start_time) * 1000  # Convert to ms
        websocket_latency = self.bot.latency * 1000  # Discord.py provides this in seconds

        # Determine status emoji and color based on latency
        if websocket_latency < 100:
            status_emoji = "🟢"
            color = discord.Color.green()
            status_text = "Excellent"
        elif websocket_latency < 200:
            status_emoji = "🟡"
            color = discord.Color.yellow()
            status_text = "Good"
        elif websocket_latency < 300:
            status_emoji = "🟠"
            color = discord.Color.orange()
            status_text = "Fair"
        else:
            status_emoji = "🔴"
            color = discord.Color.red()
            status_text = "Poor"

        # Create embed
        embed = discord.Embed(
            title=f"{status_emoji} Pong!",
            description=f"**Connection Status:** {status_text}",
            color=color,
            timestamp=datetime.now()
        )

        embed.add_field(
            name="WebSocket Latency",
            value=f"`{websocket_latency:.2f}ms`",
            inline=True
        )

        embed.add_field(
            name="API Latency",
            value=f"`{interaction_latency:.2f}ms`",
            inline=True
        )

        embed.add_field(
            name="Average Round-Trip",
            value=f"`{(websocket_latency + interaction_latency) / 2:.2f}ms`",
            inline=True
        )

        embed.set_footer(
            text=f"Requested by {interaction.user.name}",
            icon_url=interaction.user.display_avatar.url
        )

        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(PingCog(bot))