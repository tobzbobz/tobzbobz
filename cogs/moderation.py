import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict, deque
from database import db

# Configuration - Your User ID (replace with your actual Discord user ID)
YOUR_USER_ID = 678475709257089057  # Replace with your user ID

SOUNDBOARD_LIMIT = 5  # Number of soundboards allowed
SOUNDBOARD_TIMESPAN = 15  # Time window in seconds
SOUNDBOARD_ENABLED = True  # Can be toggled on/off

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
        # Track soundboard usage: {user_id: deque of timestamps}
        self.soundboard_usage = defaultdict(lambda: deque(maxlen=SOUNDBOARD_LIMIT))
        self.soundboard_enabled = SOUNDBOARD_ENABLED
        # Store disconnect logs
        self.disconnect_logs = []  # Add this line

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

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState,
                                    after: discord.VoiceState):
        """Detect soundboard spam and disconnect users"""

        # Check if soundboard detection is enabled
        if not self.soundboard_enabled:
            return

        # Check if member is the bot owner (don't moderate yourself)
        if member.id == YOUR_USER_ID:
            return

        # Check if a soundboard was used
        # Discord flags self_stream, self_video, self_deaf, self_mute changes
        # Soundboard usage triggers a voice state update
        if after.self_stream == before.self_stream and after.self_video == before.self_video:
            # Check if member is in a voice channel
            if after.channel:
                # Record the soundboard usage timestamp
                now = datetime.now()
                user_history = self.soundboard_usage[member.id]
                user_history.append(now)

                # Check if user has exceeded the limit
                if len(user_history) >= SOUNDBOARD_LIMIT:
                    # Check if all uses were within the timespan
                    oldest = user_history[0]
                    time_diff = (now - oldest).total_seconds()

                    if time_diff <= SOUNDBOARD_TIMESPAN:
                        # SPAM DETECTED! Disconnect the user
                        try:
                            await member.move_to(None, reason="Soundboard spam detected")

                            # Save to database
                            await db.add_soundboard_disconnect(
                                user_id=member.id,
                                user_name=member.name,
                                channel_id=after.channel.id,
                                channel_name=after.channel.name,
                                guild_id=member.guild.id,
                                count=len(user_history),
                                timespan=time_diff
                            )

                            # Log the action
                            print(
                                f"🚫 Disconnected {member.name} for soundboard spam ({SOUNDBOARD_LIMIT} uses in {time_diff:.1f}s)")

                            # Optional: Send them a DM
                            try:
                                embed = discord.Embed(
                                    title="⚠️ Soundboard Spam Detected",
                                    description=f"You were disconnected for using {SOUNDBOARD_LIMIT} soundboards in {time_diff:.1f} seconds.",
                                    color=discord.Color.orange()
                                )
                                embed.add_field(
                                    name="Limit",
                                    value=f"Max {SOUNDBOARD_LIMIT} soundboards per {SOUNDBOARD_TIMESPAN} seconds"
                                )
                                await member.send(embed=embed)
                            except:
                                pass  # User might have DMs disabled

                            # Clear their history after punishment
                            self.soundboard_usage[member.id].clear()

                        except discord.Forbidden:
                            print(f"❌ Failed to disconnect {member.name} (missing permissions)")
                        except discord.HTTPException as e:
                            print(f"❌ Failed to disconnect {member.name}: {e}")

    @app_commands.command(name="sound-spam", description="Toggle soundboard spam detection")
    @app_commands.describe(enabled="Enable or disable soundboard spam detection")
    @is_owner()
    async def soundboard_limiter(self, interaction: discord.Interaction, enabled: bool):
        """Toggle soundboard spam detection on/off"""
        self.soundboard_enabled = enabled

        status = "✅ Enabled" if enabled else "❌ Disabled"

        embed = discord.Embed(
            title="🔊 Soundboard Spam Detection",
            description=f"Status: **{status}**",
            color=discord.Color.green() if enabled else discord.Color.red()
        )

        embed.add_field(
            name="Settings",
            value=f"Limit: {SOUNDBOARD_LIMIT} uses\nTimespan: {SOUNDBOARD_TIMESPAN} seconds"
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="soundboard", description="View soundboard spam disconnection logs")
    @app_commands.describe(
        user="Filter logs by a specific user (optional)",
        limit="Number of recent logs to show (default: 10)"
    )
    @is_owner()
    async def soundboard_logs(
            self,
            interaction: discord.Interaction,
            user: Optional[discord.Member] = None,
            limit: int = 10
    ):
        """View logs of soundboard spam disconnections"""

        await interaction.response.defer(ephemeral=True)

        try:
            # Get logs from database
            logs = await db.get_soundboard_logs(
                guild_id=interaction.guild_id,
                user_id=user.id if user else None,
                limit=limit
            )

            # Get total count
            total_count = await db.get_soundboard_log_count(
                guild_id=interaction.guild_id,
                user_id=user.id if user else None
            )

        except Exception as e:
            await interaction.followup.send(
                f"❌ Database error: {str(e)}",
                ephemeral=True
            )
            return

        # Check if there are any logs
        if not logs:
            embed = discord.Embed(
                title="📋 Soundboard Disconnect Logs",
                description=f"No disconnections recorded{f' for {user.mention}' if user else ''}.",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed = discord.Embed(
            title="📋 Soundboard Disconnect Logs",
            description=f"Showing {len(logs)} most recent disconnection(s)",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )

        for log in logs:
            # Get user info
            user_obj = interaction.guild.get_member(log['user_id'])
            user_display = f"<@{log['user_id']}>" if user_obj else f"{log['user_name']} (ID: {log['user_id']})"

            # Get channel info
            channel_obj = interaction.guild.get_channel(log['channel_id'])
            channel_display = channel_obj.mention if channel_obj else f"#{log['channel_name']} (ID: {log['channel_id']})"

            # Format timestamp
            timestamp_dt = log['timestamp']
            time_str = timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
            relative_time = discord.utils.format_dt(timestamp_dt, style='R')

            # Build field value
            field_value = (
                f"**User:** {user_display}\n"
                f"**Channel:** {channel_display}\n"
                f"**Uses:** {log['count']} in {log['timespan']:.1f}s\n"
                f"**Time:** {time_str} ({relative_time})"
            )

            embed.add_field(
                name=f"🚫 Disconnect #{log['id']}",
                value=field_value,
                inline=False
            )

        embed.set_footer(
            text=f"Total disconnections: {total_count} | Limit: {SOUNDBOARD_LIMIT} per {SOUNDBOARD_TIMESPAN}s")

        await interaction.followup.send(embed=embed, ephemeral=True)

async def setup(bot):
    await bot.add_cog(ModerateCog(bot))