import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
from typing import Optional

# Configuration - Your User ID (replace with your actual Discord user ID)
YOUR_USER_ID = 678475709257089057  # Replace with your user ID


def is_owner():
    """Check if user is the bot owner"""

    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.id != YOUR_USER_ID:
            await interaction.response.send_message(
                "❌ This command is restricted to the bot owner only.",
                ephemeral=True
            )
            return False
        return True

    return app_commands.check(predicate)


class ModerateCog(commands.Cog):
    """Troll moderation commands"""

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="moderate", description="Moderate a user (owner only)")
    @app_commands.describe(
        user="The user to moderate",
        timeout="Timeout duration (e.g., '5m', '1h', '1d')",
        server_mute="Mute the user in voice channels",
        server_deafen="Deafen the user in voice channels",
        disconnect="Disconnect user from voice channel",
        move_to="Move user to a specific voice channel"
    )
    @is_owner()
    async def moderate(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            timeout: Optional[str] = None,
            server_mute: Optional[bool] = None,
            server_deafen: Optional[bool] = None,
            disconnect: Optional[bool] = None,
            move_to: Optional[discord.VoiceChannel] = None
    ):
        """Moderate a user with various troll options"""

        await interaction.response.defer(ephemeral=True)

        actions_taken = []
        errors = []

        # Check if any action was specified
        if not any([timeout, server_mute is not None, server_deafen is not None, disconnect, move_to]):
            await interaction.followup.send(
                "❌ Please specify at least one moderation action.",
                ephemeral=True
            )
            return

        # 1. Timeout
        if timeout:
            try:
                duration = self.parse_duration(timeout)
                if duration:
                    until = discord.utils.utcnow() + duration
                    await user.timeout(until, reason=f"Moderated by {interaction.user.name}")
                    actions_taken.append(f"⏱️ Timed out for {timeout}")
                else:
                    errors.append("Invalid timeout format (use: 5m, 1h, 2d)")
            except discord.Forbidden:
                errors.append("Failed to timeout (missing permissions)")
            except discord.HTTPException as e:
                errors.append(f"Failed to timeout: {str(e)}")

        # 2. Server Mute
        if server_mute is not None:
            try:
                await user.edit(mute=server_mute, reason=f"Moderated by {interaction.user.name}")
                action_text = "muted" if server_mute else "unmuted"
                actions_taken.append(f"🔇 Server {action_text}")
            except discord.Forbidden:
                errors.append("Failed to mute/unmute (missing permissions)")
            except discord.HTTPException as e:
                errors.append(f"Failed to mute/unmute: {str(e)}")

        # 3. Server Deafen
        if server_deafen is not None:
            try:
                await user.edit(deafen=server_deafen, reason=f"Moderated by {interaction.user.name}")
                action_text = "deafened" if server_deafen else "undeafened"
                actions_taken.append(f"🔈 Server {action_text}")
            except discord.Forbidden:
                errors.append("Failed to deafen/undeafen (missing permissions)")
            except discord.HTTPException as e:
                errors.append(f"Failed to deafen/undeafen: {str(e)}")

        # 4. Disconnect from Voice
        if disconnect:
            try:
                if user.voice:
                    await user.move_to(None, reason=f"Disconnected by {interaction.user.name}")
                    actions_taken.append("🚪 Disconnected from voice")
                else:
                    errors.append("User is not in a voice channel")
            except discord.Forbidden:
                errors.append("Failed to disconnect (missing permissions)")
            except discord.HTTPException as e:
                errors.append(f"Failed to disconnect: {str(e)}")

        # 5. Move to Voice Channel
        if move_to:
            try:
                if user.voice:
                    await user.move_to(move_to, reason=f"Moved by {interaction.user.name}")
                    actions_taken.append(f"🔀 Moved to {move_to.mention}")
                else:
                    errors.append("User is not in a voice channel")
            except discord.Forbidden:
                errors.append("Failed to move user (missing permissions)")
            except discord.HTTPException as e:
                errors.append(f"Failed to move user: {str(e)}")

        # Create response embed
        embed = discord.Embed(
            title="🛡️ Moderation Action",
            color=discord.Color.orange() if not errors else discord.Color.red(),
            timestamp=datetime.now()
        )

        embed.add_field(
            name="Target:",
            value=f"{user.mention} (`{user.name}` - `{user.id}`)",
            inline=False
        )

        if actions_taken:
            embed.add_field(
                name="✅ Actions Taken:",
                value="\n".join(actions_taken),
                inline=False
            )

        if errors:
            embed.add_field(
                name="❌ Errors:",
                value="\n".join(errors),
                inline=False
            )

        embed.set_thumbnail(url=user.display_avatar.url)
        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)

    def parse_duration(self, duration_str: str) -> Optional[timedelta]:
        """Parse duration string like '5m', '1h', '2d' into timedelta"""
        try:
            # Extract number and unit
            duration_str = duration_str.lower().strip()

            # Parse the number
            num_str = ""
            unit = ""

            for char in duration_str:
                if char.isdigit():
                    num_str += char
                else:
                    unit += char

            if not num_str:
                return None

            amount = int(num_str)

            # Convert to timedelta based on unit
            if unit in ['s', 'sec', 'second', 'seconds']:
                return timedelta(seconds=amount)
            elif unit in ['m', 'min', 'minute', 'minutes']:
                return timedelta(minutes=amount)
            elif unit in ['h', 'hr', 'hour', 'hours']:
                return timedelta(hours=amount)
            elif unit in ['d', 'day', 'days']:
                return timedelta(days=amount)
            elif unit in ['w', 'week', 'weeks']:
                return timedelta(weeks=amount)
            else:
                return None

        except (ValueError, AttributeError):
            return None

async def setup(bot):
    await bot.add_cog(ModerateCog(bot))