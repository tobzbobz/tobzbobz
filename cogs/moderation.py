import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict, deque
from database import db

# Configuration
YOUR_USER_ID = 678475709257089057  # Replace with your user ID
MOD_LOGS_CHANNEL_ID = 1435597032474542161  # Set this to your mod-logs channel ID (e.g., 1234567890)

# Soundboard Spam Detection Settings
SOUNDBOARD_ENABLED = True  # Master toggle

# Method 1: Direct voice state changes (catches some soundboard usage)
VOICE_STATE_LIMIT = 5  # Number of voice state changes
VOICE_STATE_TIMESPAN = 15  # Within this many seconds

# Method 2: Channel hopping (people moving around while spamming)
CHANNEL_HOP_LIMIT = 6  # Number of channel switches
CHANNEL_HOP_TIMESPAN = 10  # Within this many seconds

# Method 3: Join/Leave spam (repeated disconnects/reconnects)
JOIN_LEAVE_LIMIT = 3  # Number of join/leave cycles
JOIN_LEAVE_TIMESPAN = 10  # Within this many seconds

# Method 4: Rapid mute/unmute (some users toggle mute while soundboarding)
MUTE_TOGGLE_LIMIT = 6  # Number of mute toggles
MUTE_TOGGLE_TIMESPAN = 15  # Within this many seconds


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
        self.soundboard_enabled = SOUNDBOARD_ENABLED

        # Track different spam patterns per user
        self.voice_state_changes = defaultdict(lambda: deque(maxlen=VOICE_STATE_LIMIT))
        self.channel_hops = defaultdict(lambda: deque(maxlen=CHANNEL_HOP_LIMIT))
        self.join_leave_cycles = defaultdict(lambda: deque(maxlen=JOIN_LEAVE_LIMIT))
        self.mute_toggles = defaultdict(lambda: deque(maxlen=MUTE_TOGGLE_LIMIT))

        # Track last channel for hop detection
        self.last_channel = {}

    async def send_to_mod_logs(self, guild: discord.Guild, embed: discord.Embed):
        """Send an embed to the mod logs channel"""
        if MOD_LOGS_CHANNEL_ID:
            try:
                channel = guild.get_channel(MOD_LOGS_CHANNEL_ID)
                if channel and isinstance(channel, discord.TextChannel):
                    await channel.send(embed=embed)
            except Exception as e:
                print(f"❌ Failed to send to mod logs: {e}")

    async def handle_spam_detection(self, member: discord.Member, channel: discord.VoiceChannel,
                                    spam_type: str, count: int, timespan: float):
        """Handle detected spam - disconnect user and log"""
        try:
            await member.move_to(None, reason=f"{spam_type} spam detected (auto-moderation)")

            # Save to database
            await db.add_soundboard_disconnect(
                user_id=member.id,
                user_name=member.name,
                channel_id=channel.id,
                channel_name=channel.name,
                guild_id=member.guild.id,
                count=count,
                timespan=timespan
            )

            # Log to console
            print(f"🚫 Disconnected {member.name} for {spam_type} spam ({count} in {timespan:.1f}s)")

            # Send to mod logs
            embed = discord.Embed(
                title="🚫 Voice Spam - Auto Disconnect",
                description=f"{member.mention} was automatically disconnected for {spam_type} spam.",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            embed.add_field(name="User", value=f"{member.mention} (`{member.name}` - `{member.id}`)", inline=False)
            embed.add_field(name="Channel", value=channel.mention, inline=True)
            embed.add_field(name="Spam Type", value=spam_type, inline=True)
            embed.add_field(name="Detection", value=f"{count} events in {timespan:.1f}s", inline=True)
            embed.set_thumbnail(url=member.display_avatar.url)
            embed.set_footer(text="Auto-Moderation System")

            await self.send_to_mod_logs(member.guild, embed)

            # Clear all tracking for this user after punishment
            self.voice_state_changes[member.id].clear()
            self.channel_hops[member.id].clear()
            self.join_leave_cycles[member.id].clear()
            self.mute_toggles[member.id].clear()

            return True

        except discord.Forbidden:
            print(f"❌ Failed to disconnect {member.name} (missing permissions)")
            return False
        except discord.HTTPException as e:
            print(f"❌ Failed to disconnect {member.name}: {e}")
            return False

    @app_commands.command(name="moderate", description="Moderate a user (owner only)")
    @app_commands.describe(
        user="The user to moderate",
        reason="Reason for moderation",
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
            reason: Optional[str] = "No reason provided",
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
                    await user.timeout(until, reason=f"{reason} | By {interaction.user.name}")
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
                await user.edit(mute=server_mute, reason=f"{reason} | By {interaction.user.name}")
                action_text = "muted" if server_mute else "unmuted"
                actions_taken.append(f"🔇 Server {action_text}")
            except discord.Forbidden:
                errors.append("Failed to mute/unmute (missing permissions)")
            except discord.HTTPException as e:
                errors.append(f"Failed to mute/unmute: {str(e)}")

        # 3. Server Deafen
        if server_deafen is not None:
            try:
                await user.edit(deafen=server_deafen, reason=f"{reason} | By {interaction.user.name}")
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
                    await user.move_to(None, reason=f"{reason} | By {interaction.user.name}")
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
                    await user.move_to(move_to, reason=f"{reason} | By {interaction.user.name}")
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

        embed.add_field(
            name="Reason:",
            value=reason,
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

        # Send to user (ephemeral)
        await interaction.followup.send(embed=embed, ephemeral=True)

        # Send to mod logs if actions were successful
        if actions_taken:
            await self.send_to_mod_logs(interaction.guild, embed)

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
        """Multi-layered spam detection system"""

        # Check if spam detection is enabled
        if not self.soundboard_enabled:
            return

        # Don't moderate the bot owner
        if member.id == YOUR_USER_ID:
            return

        # Only track if user is currently in a voice channel
        if not after.channel:
            # User left voice - clear their last channel tracking
            if member.id in self.last_channel:
                del self.last_channel[member.id]
            return

        now = datetime.now()

        # === METHOD 1: Track ANY voice state change (catches some soundboard activity) ===
        # Record this state change
        self.voice_state_changes[member.id].append(now)

        if len(self.voice_state_changes[member.id]) >= VOICE_STATE_LIMIT:
            oldest = self.voice_state_changes[member.id][0]
            time_diff = (now - oldest).total_seconds()

            if time_diff <= VOICE_STATE_TIMESPAN:
                # Too many state changes too quickly
                await self.handle_spam_detection(
                    member, after.channel, "Voice State",
                    len(self.voice_state_changes[member.id]), time_diff
                )
                return

        # === METHOD 2: Channel Hopping Detection ===
        if before.channel != after.channel and before.channel is not None:
            # User switched channels (not just joining)
            self.channel_hops[member.id].append(now)

            if len(self.channel_hops[member.id]) >= CHANNEL_HOP_LIMIT:
                oldest = self.channel_hops[member.id][0]
                time_diff = (now - oldest).total_seconds()

                if time_diff <= CHANNEL_HOP_TIMESPAN:
                    # Hopping between channels too fast
                    await self.handle_spam_detection(
                        member, after.channel, "Channel Hopping",
                        len(self.channel_hops[member.id]), time_diff
                    )
                    return

        # === METHOD 3: Join/Leave Spam Detection ===
        if before.channel is None and after.channel is not None:
            # User joined voice
            self.join_leave_cycles[member.id].append(('join', now))
        elif before.channel is not None and after.channel is None:
            # User left voice
            self.join_leave_cycles[member.id].append(('leave', now))

        # Check for rapid join/leave cycles
        if len(self.join_leave_cycles[member.id]) >= JOIN_LEAVE_LIMIT:
            oldest_time = self.join_leave_cycles[member.id][0][1]
            time_diff = (now - oldest_time).total_seconds()

            if time_diff <= JOIN_LEAVE_TIMESPAN:
                # Too many joins/leaves
                if after.channel:  # Only disconnect if they're currently in a channel
                    await self.handle_spam_detection(
                        member, after.channel, "Join/Leave Cycling",
                        len(self.join_leave_cycles[member.id]), time_diff
                    )
                    return

        # === METHOD 4: Rapid Mute/Unmute Detection ===
        if before.self_mute != after.self_mute:
            # User toggled their mute
            self.mute_toggles[member.id].append(now)

            if len(self.mute_toggles[member.id]) >= MUTE_TOGGLE_LIMIT:
                oldest = self.mute_toggles[member.id][0]
                time_diff = (now - oldest).total_seconds()

                if time_diff <= MUTE_TOGGLE_TIMESPAN:
                    # Toggling mute too rapidly
                    await self.handle_spam_detection(
                        member, after.channel, "Mute Toggle",
                        len(self.mute_toggles[member.id]), time_diff
                    )
                    return

    @app_commands.command(name="spam-config", description="View/configure spam detection settings")
    @app_commands.describe(
        enabled="Enable or disable spam detection",
        method="Which detection method to view/adjust"
    )
    @is_owner()
    async def spam_config(
            self,
            interaction: discord.Interaction,
            enabled: Optional[bool] = None,
            method: Optional[str] = None
    ):
        """View or configure spam detection settings"""

        if enabled is not None:
            self.soundboard_enabled = enabled

        status = "✅ Enabled" if self.soundboard_enabled else "❌ Disabled"

        embed = discord.Embed(
            title="🛡️ Multi-Method Spam Detection",
            description=f"Overall Status: **{status}**",
            color=discord.Color.green() if self.soundboard_enabled else discord.Color.red(),
            timestamp=datetime.now()
        )

        # Method 1
        embed.add_field(
            name="📊 Method 1: Voice State Changes",
            value=f"Limit: {VOICE_STATE_LIMIT} changes in {VOICE_STATE_TIMESPAN}s\n"
                  f"*Catches rapid state updates (potential soundboards)*",
            inline=False
        )

        # Method 2
        embed.add_field(
            name="🔀 Method 2: Channel Hopping",
            value=f"Limit: {CHANNEL_HOP_LIMIT} hops in {CHANNEL_HOP_TIMESPAN}s\n"
                  f"*Catches users jumping between channels*",
            inline=False
        )

        # Method 3
        embed.add_field(
            name="🚪 Method 3: Join/Leave Cycling",
            value=f"Limit: {JOIN_LEAVE_LIMIT} cycles in {JOIN_LEAVE_TIMESPAN}s\n"
                  f"*Catches repeated connect/disconnect*",
            inline=False
        )

        # Method 4
        embed.add_field(
            name="🔇 Method 4: Mute Toggling",
            value=f"Limit: {MUTE_TOGGLE_LIMIT} toggles in {MUTE_TOGGLE_TIMESPAN}s\n"
                  f"*Catches rapid mute/unmute spam*",
            inline=False
        )

        embed.set_footer(text="All methods run simultaneously for maximum detection")

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
                title="📋 Spam Disconnect Logs",
                description=f"No disconnections recorded{f' for {user.mention}' if user else ''}.",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed = discord.Embed(
            title="📋 Spam Disconnect Logs",
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
                f"**Detection:** {log['count']} events in {log['timespan']:.1f}s\n"
                f"**Time:** {time_str} ({relative_time})"
            )

            embed.add_field(
                name=f"🚫 Disconnect #{log['id']}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Total disconnections: {total_count} | Multi-method detection active")

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(ModerateCog(bot))