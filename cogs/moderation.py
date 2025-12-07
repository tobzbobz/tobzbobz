import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
from typing import Optional, List
from collections import defaultdict, deque
from database import db
import random

# Configuration
YOUR_USER_ID = 678475709257089057  # Replace with your user ID
MOD_LOGS_CHANNEL_ID = 1435597032474542161  # Set this to your mod-logs channel ID (e.g., 1234567890)

SUPERVISORS = [1365536209681514636, 1285474077556998196, 1389113393511923863, 1389113460687765534]
LEADERS = [1389113393511923863, 1285474077556998196]
LOCKS = [1389550689113473024]

PROTECTION_ENABLED = True
RETALIATION_ENABLED = True

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
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only.",
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
        self.retaliation = RETALIATION_ENABLED
        self.protection = PROTECTION_ENABLED

        # Track different spam patterns per user
        self.voice_state_changes = defaultdict(lambda: deque(maxlen=VOICE_STATE_LIMIT))
        self.channel_hops = defaultdict(lambda: deque(maxlen=CHANNEL_HOP_LIMIT))
        self.join_leave_cycles = defaultdict(lambda: deque(maxlen=JOIN_LEAVE_LIMIT))
        self.mute_toggles = defaultdict(lambda: deque(maxlen=MUTE_TOGGLE_LIMIT))

        # Track last channel for hop detection
        self.last_channel = {}

    vc_group = app_commands.Group(name="vc", description="Voice channel moderation commands")
    vca_group = app_commands.Group(name="vca", description="Advanced voice channel moderation commands")

    async def send_to_mod_logs(self, guild: discord.Guild, embed: discord.Embed):
        """Send an embed to the mod logs channel"""
        if MOD_LOGS_CHANNEL_ID:
            try:
                channel = guild.get_channel(MOD_LOGS_CHANNEL_ID)
                if channel and isinstance(channel, discord.TextChannel):
                    await channel.send(embed=embed)
            except Exception as e:
                print(f"<:Denied:1426930694633816248> Failed to send to mod logs: {e}")

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
            print(f"Disconnected {member.name} for {spam_type} spam ({count} in {timespan:.1f}s)")

            # Send to mod logs
            embed = discord.Embed(
                title="Voice Spam - Auto Disconnect",
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
            print(f"<:Denied:1426930694633816248> Failed to disconnect {member.name} (missing permissions)")
            return False
        except discord.HTTPException as e:
            print(f"<:Denied:1426930694633816248> Failed to disconnect {member.name}: {e}")
            return False

    @app_commands.command(name="moderate", description="Moderate or troll others")
    @app_commands.describe(
        users="Affected users",
        reason="Reason for moderation",
        timeout="Timeout duration",
        server_mute="Toggle server mute",
        server_deafen="Toggle server deafen",
        disconnect="Disconnect from VC",
    )
    @is_owner()
    async def moderate(
            self,
            interaction: discord.Interaction,
            users: str,
            reason: Optional[str] = "No reason provided",
            timeout: Optional[str] = None,
            server_mute: Optional[bool] = None,
            server_deafen: Optional[bool] = None,
            disconnect: Optional[bool] = None,
    ):
        """Moderate multiple users with various options including sequential channel moves"""

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Moderating User(s)",
                                                ephemeral=True)

        # Parse users (by mention, ID, or name)
        member_list = await self.parse_users(interaction, users)

        if not member_list:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No valid users found. Use mentions, IDs, or names.",
                ephemeral=True
            )
            return

        # Check if any action was specified
        if not any([timeout, server_mute is not None, server_deafen is not None, disconnect]):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please specify at least one moderation action.",
                ephemeral=True
            )
            return

        # Process each user
        all_results = []
        for user in member_list:
            result = await self.moderate_single_user(
                interaction, user, reason, timeout, server_mute,
                server_deafen, disconnect
            )
            all_results.append((user, result))

        # Create summary embed
        embed = discord.Embed(
            title="Mass Moderation Action",
            description=f"Moderated {len(member_list)} user(s)",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason:", value=reason, inline=False)

        # Summarize results
        for user, (actions_taken, errors) in all_results:
            status = "<:Accepted:1426930333789585509>" if actions_taken and not errors else "<:Warn:1437771973970104471>" if actions_taken else "<:Denied:1426930694633816248>"

            field_value = ""
            if actions_taken:
                field_value += "**Actions:** " + ", ".join(actions_taken) + "\n"
            if errors:
                field_value += "**Errors:** " + ", ".join(errors)

            if not field_value:
                field_value = "No actions performed"

            embed.add_field(
                name=f"{status} {user.name}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)

        # Send to mod logs
        await self.send_to_mod_logs(interaction.guild, embed)

    @staticmethod
    def has_role_level(required_roles: List[int]):
        """Check if user has one of the required roles"""

        async def predicate(interaction: discord.Interaction) -> bool:
            # Owner always has access
            if interaction.user.id == YOUR_USER_ID:
                return True

            # Check if user has any of the required roles
            user_role_ids = [role.id for role in interaction.user.roles]
            if any(role_id in required_roles for role_id in user_role_ids):
                return True

            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use this command.",
                ephemeral=True
            )
            return False

        return app_commands.check(predicate)

    async def parse_users(self, interaction: discord.Interaction, users_str: str) -> List[discord.Member]:
        """Parse user string into list of members"""
        members = []
        parts = users_str.split()

        for part in parts:
            member = None

            # Try as mention
            if part.startswith('<@') and part.endswith('>'):
                user_id = part.strip('<@!>')
                try:
                    member = interaction.guild.get_member(int(user_id))
                except ValueError:
                    pass

            # Try as ID
            if not member:
                try:
                    member = interaction.guild.get_member(int(part))
                except ValueError:
                    pass

            # Try as name
            if not member:
                member = discord.utils.find(lambda m: m.name.lower() == part.lower(), interaction.guild.members)

            if member and member not in members:
                members.append(member)

        return members

    async def moderate_single_user(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            reason: str,
            timeout: Optional[str],
            server_mute: Optional[bool],
            server_deafen: Optional[bool],
            disconnect: Optional[bool],
    ):
        """Moderate a single user and return results"""
        actions_taken = []
        errors = []

        # 1. Timeout
        if timeout:
            try:
                duration = self.parse_duration(timeout)
                if duration:
                    # Check if timeout exceeds 4m 50s (290 seconds)
                    if duration.total_seconds() > 290:
                        # Reason is required for long timeouts
                        if not reason or reason == "No reason provided":
                            errors.append("Reason required for timeouts over 4m 50s")
                            return actions_taken, errors

                    until = discord.utils.utcnow() + duration
                    await user.timeout(until, reason=f"{reason} | By {interaction.user.name}")
                    actions_taken.append(f"Timed out for {timeout}")
                else:
                    errors.append("Invalid timeout format")
            except discord.Forbidden:
                errors.append("Failed to timeout (permissions)")
            except discord.HTTPException as e:
                errors.append(f"Timeout failed: {str(e)[:30]}")

        # 2. Server Mute (Toggle behavior)
        if server_mute is not None:
            try:
                # Determine target state (toggle if True, otherwise explicitly set)
                if server_mute and user.voice:
                    target_mute = not user.voice.mute
                else:
                    target_mute = server_mute

                await user.edit(mute=target_mute, reason=f"{reason} | By {interaction.user.name}")
                action_text = "muted" if target_mute else "unmuted"
                actions_taken.append(f"Server {action_text}")
            except discord.Forbidden:
                errors.append("Failed to mute (permissions)")
            except discord.HTTPException as e:
                errors.append(f"Mute failed: {str(e)[:30]}")

        # 3. Server Deafen (Toggle behavior)
        if server_deafen is not None:
            try:
                # Determine target state (toggle if True, otherwise explicitly set)
                if server_deafen and user.voice:
                    target_deafen = not user.voice.deaf
                else:
                    target_deafen = server_deafen

                await user.edit(deafen=target_deafen, reason=f"{reason} | By {interaction.user.name}")
                action_text = "deafened" if target_deafen else "undeafened"
                actions_taken.append(f"Server {action_text}")
            except discord.Forbidden:
                errors.append("Failed to deafen (permissions)")
            except discord.HTTPException as e:
                errors.append(f"Deafen failed: {str(e)[:30]}")

        # 4. Disconnect from Voice
        if disconnect:
            try:
                if user.voice:
                    await user.move_to(None, reason=f"{reason} | By {interaction.user.name}")
                    actions_taken.append("Disconnected from voice")
                else:
                    errors.append("User not in voice")
            except discord.Forbidden:
                errors.append("Failed to disconnect (permissions)")
            except discord.HTTPException as e:
                errors.append(f"Disconnect failed: {str(e)[:30]}")

        return actions_taken, errors

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

        # === OWNER PROTECTION & RETALIATION ===
        if member.id == YOUR_USER_ID:
            try:
                perpetrator = None

                # Check for server mute
                if after.mute and not before.mute:
                    perpetrator = None

                    if self.retaliation or self.protection:
                        import asyncio
                        await asyncio.sleep(0.5)  # Changed from 1 second to 0.5 seconds

                        perpetrator = await self.find_voice_action_perpetrator(
                            member.guild,
                            discord.AuditLogAction.member_update,
                            member
                        )

                    if self.protection:
                        await member.edit(mute=False, reason="ceebs")
                        print(f"[Owner Protection] Undid server mute on {member.name}")

                    if self.retaliation and perpetrator:
                        await self.retaliate_voice_action(perpetrator, "mute", member.guild)

                # Check for server deafen
                if after.deaf and not before.deaf:
                    perpetrator = None

                    if self.retaliation or self.protection:
                        import asyncio
                        await asyncio.sleep(0.5)  # Changed from 1 second to 0.5 seconds

                        perpetrator = await self.find_voice_action_perpetrator(
                            member.guild,
                            discord.AuditLogAction.member_update,
                            member
                        )

                    if self.protection:
                        await member.edit(deafen=False, reason="ceebs")
                        print(f"[Owner Protection] Undid server deafen on {member.name}")

                    if self.retaliation and perpetrator:
                        await self.retaliate_voice_action(perpetrator, "deafen", member.guild)

                        print("[Debug] Searching for perpetrator in audit logs...")
                        perpetrator = await self.find_voice_action_perpetrator(
                            member.guild,
                            discord.AuditLogAction.member_update,
                            member
                        )

                    if perpetrator:
                        print(f"[Debug] Found perpetrator: {perpetrator.name}")
                    else:
                        print("[Debug] No perpetrator found")

                    if self.protection:
                        await member.edit(mute=False, reason="ceebs")
                        print(f"[Owner Protection] Undid server mute on {member.name}")

                    if self.retaliation and perpetrator:
                        await self.retaliate_voice_action(perpetrator, "mute", member.guild)

                # Check for server deafen
                if after.deaf and not before.deaf:
                    print(f"[Debug] Owner was deafened! Retaliation enabled: {self.retaliation}")

                    # Find who deafened us via audit log
                    if self.retaliation:
                        print("[Debug] Waiting for audit log entry...")
                        import asyncio
                        await asyncio.sleep(1)  # Wait 500ms for audit log

                        print("[Debug] Searching for perpetrator in audit logs...")
                        perpetrator = await self.find_voice_action_perpetrator(
                            member.guild,
                            discord.AuditLogAction.member_update,
                            member
                        )

                        if perpetrator:
                            print(f"[Debug] Found perpetrator: {perpetrator.name}")
                        else:
                            print("[Debug] No perpetrator found")

                    if self.protection:
                        await member.edit(deafen=False, reason="ceebs")
                        print(f"[Owner Protection] Undid server deafen on {member.name}")

                    if self.retaliation and perpetrator:
                        print(f"[Debug] Attempting retaliation against {perpetrator.name}")
                        await self.retaliate_voice_action(perpetrator, "deafen", member.guild)

            except Exception as e:
                print(f"[Owner Protection/Retaliation] Failed: {e}")
                import traceback
                traceback.print_exc()

            # Don't run spam detection on owner
            return

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

    async def find_voice_action_perpetrator(self, guild: discord.Guild,
                                            action: discord.AuditLogAction,
                                            target: discord.Member) -> Optional[discord.Member]:
        """Find who performed a voice action on the target user via audit logs"""
        try:
            # Look for recent audit log entries (within last 5 seconds)
            cutoff_time = discord.utils.utcnow() - timedelta(seconds=5)

            async for entry in guild.audit_logs(
                    action=action,
                    limit=20,  # Increased from 15
                    after=cutoff_time
            ):
                # Check if this entry targets our user
                if entry.target and entry.target.id == target.id:
                    # Check for mute/deaf changes
                    if hasattr(entry, 'changes') and entry.changes:
                        for change in entry.changes:
                            if change.key in ['mute', 'deaf']:
                                # Verify this isn't the bot itself
                                if entry.user.id != guild.me.id:
                                    print(f"[Retaliation] Found perpetrator: {entry.user.name} (changed {change.key})")
                                    return entry.user

                    # Fallback: if we found an entry for our target but no clear changes
                    if entry.user.id != guild.me.id:
                        print(f"[Retaliation] Found entry for target by: {entry.user.name}")
                        return entry.user

            print("[Retaliation] No perpetrator found in audit logs")
            return None

        except discord.Forbidden:
            print("[Retaliation] Missing 'View Audit Log' permission!")
            return None
        except Exception as e:
            print(f"[Retaliation] Error finding perpetrator: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def retaliate_voice_action(self, perpetrator: discord.Member,
                                     action_type: str, guild: discord.Guild):
        """Retaliate by applying the same action to the perpetrator"""
        try:
            # Don't retaliate against yourself or bots
            if perpetrator.id == YOUR_USER_ID or perpetrator.bot:
                return

            if action_type == "mute":
                await perpetrator.edit(mute=True, reason="ceebs")
                print(f"[Retaliation] Muted {perpetrator.name} for muting owner")

                # Log to mod logs
                embed = discord.Embed(
                    title="Retaliatory Action - Mute",
                    description=f"{perpetrator.mention} was automatically muted for muting <@{YOUR_USER_ID}>",
                    color=discord.Color.orange(),
                    timestamp=datetime.now()
                )
                embed.add_field(name="Perpetrator", value=f"{perpetrator.mention} (`{perpetrator.name}`)", inline=False)
                embed.add_field(name="Action", value="Server Muted", inline=True)
                embed.set_thumbnail(url=perpetrator.display_avatar.url)
                embed.set_footer(text="Retaliatory Auto-Moderation")
                await self.send_to_mod_logs(guild, embed)

            elif action_type == "deafen":
                await perpetrator.edit(deafen=True, reason="Retaliation: Deafened bot owner")
                print(f"[Retaliation] Deafened {perpetrator.name} for deafening owner")

                # Log to mod logs
                embed = discord.Embed(
                    title="Retaliatory Action - Deafen",
                    description=f"{perpetrator.mention} was automatically deafened for deafening <@{YOUR_USER_ID}>",
                    color=discord.Color.orange(),
                    timestamp=datetime.now()
                )
                embed.add_field(name="Perpetrator", value=f"{perpetrator.mention} (`{perpetrator.name}`)", inline=False)
                embed.add_field(name="Action", value="Server Deafened", inline=True)
                embed.set_thumbnail(url=perpetrator.display_avatar.url)
                embed.set_footer(text="Retaliatory Auto-Moderation")
                await self.send_to_mod_logs(guild, embed)

        except discord.Forbidden:
            print(f"[Retaliation] Failed to retaliate against {perpetrator.name} (missing permissions)")
        except Exception as e:
            print(f"[Retaliation] Error retaliating: {e}")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Auto-undo timeout on bot owner"""

        # Check if protection is enabled
        if not self.protection:
            return

        # Check if this is the bot owner
        if after.id != YOUR_USER_ID:
            return

        try:
            # Check if owner was just timed out
            if after.timed_out_until and not before.timed_out_until:
                await after.timeout(None, reason="ceebs")
                print(f"[Owner Protection] Undid timeout on {after.name}")

        except Exception as e:
            print(f"[Owner Protection] Failed to undo timeout: {e}")

    @app_commands.command(name="spam-config", description="Configure moderation settings")
    @app_commands.describe(
        enabled="Enable or disable spam detection",
        protection="Enable or disable auto-protections",
        retaliation="Enable or disable retaliatory muting/deafening"

    )
    @is_owner()
    async def spam_config(
            self,
            interaction: discord.Interaction,
            enabled: Optional[bool] = None,
            protection: Optional[bool] = None,
            retaliation: Optional[bool] = None
    ):
        """View or configure spam detection settings"""

        if enabled is not None:
            self.soundboard_enabled = enabled

        if protection is not None:
            self.protection = protection

        if retaliation is not None:  # Add this block
            self.retaliation = retaliation

        status = "<:Accepted:1426930333789585509> Enabled" if self.soundboard_enabled else "<:Denied:1426930694633816248> Disabled"

        embed = discord.Embed(
            title="Multi-Method Spam Detection",
            description=f"Overall Status: **{status}**",
            color=discord.Color.green() if self.soundboard_enabled else discord.Color.red(),
            timestamp=datetime.now()
        )

        # Method 1
        embed.add_field(
            name="Method 1: Voice State Changes",
            value=f"Limit: {VOICE_STATE_LIMIT} changes in {VOICE_STATE_TIMESPAN}s\n"
                  f"*Catches rapid state updates (potential soundboards)*",
            inline=False
        )

        # Method 2
        embed.add_field(
            name="Method 2: Channel Hopping",
            value=f"Limit: {CHANNEL_HOP_LIMIT} hops in {CHANNEL_HOP_TIMESPAN}s\n"
                  f"*Catches users jumping between channels*",
            inline=False
        )

        # Method 3
        embed.add_field(
            name="Method 3: Join/Leave Cycling",
            value=f"Limit: {JOIN_LEAVE_LIMIT} cycles in {JOIN_LEAVE_TIMESPAN}s\n"
                  f"*Catches repeated connect/disconnect*",
            inline=False
        )

        # Method 4
        embed.add_field(
            name="Method 4: Mute Toggling",
            value=f"Limit: {MUTE_TOGGLE_LIMIT} toggles in {MUTE_TOGGLE_TIMESPAN}s\n"
                  f"*Catches rapid mute/unmute spam*",
            inline=False
        )

        # Owner Protection Status
        protection_status = "<:Accepted:1426930333789585509> Enabled" if self.protection else "<:Denied:1426930694633816248> Disabled"
        embed.add_field(
            name="Owner Auto-Protection",
            value=f"Status: **{protection_status}**\n*Automatically undoes mute/deafen/timeout on bot owner*",
            inline=False
        )

        # Add Retaliation Status (add this new field)
        retaliation_status = "<:Accepted:1426930333789585509> Enabled" if self.retaliation else "<:Denied:1426930694633816248> Disabled"
        embed.add_field(
            name="Owner Retaliation",
            value=f"Status: **{retaliation_status}**\n*Automatically mutes/deafens whoever mutes/deafens the bot owner*",
            inline=False
        )

        embed.set_footer(text="All methods run simultaneously for maximum detection")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="soundboard", description="View soundboard spam disconnection logs")
    @app_commands.describe(
        user="Filter logs by a specific user",
        limit="Number of recent logs to show (defaults to 10)"
    )
    @is_owner()
    async def soundboard_logs(
            self,
            interaction: discord.Interaction,
            user: Optional[discord.Member] = None,
            limit: int = 10
    ):
        """View logs of soundboard spam disconnections"""

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Checking Logs",
                                                ephemeral=True)

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
                f"<:Denied:1426930694633816248> Database error: {str(e)}",
                ephemeral=True
            )
            return

        # Check if there are any logs
        if not logs:
            embed = discord.Embed(
                title="Spam Disconnect Logs",
                description=f"No disconnections recorded{f' for {user.mention}' if user else ''}.",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed = discord.Embed(
            title="Spam Disconnect Logs",
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
                name=f"Disconnect #{log['id']}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Total disconnections: {total_count} | Multi-method detection active")

        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @vc_group.command(name="move", description="Move all users from one channel to another")
    @app_commands.describe(
        to="Destination channel",
        origin="Source channel (owner can leave empty for all channels)",
        reason="Reason for the move"
    )
    @has_role_level(SUPERVISORS + LEADERS + LOCKS + [YOUR_USER_ID])
    async def vc_move(
            self,
            interaction: discord.Interaction,
            to: discord.VoiceChannel,
            origin: Optional[discord.VoiceChannel] = None,
            reason: Optional[str] = "No reason provided"
    ):
        """Move all users from one channel to another"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Moving users...", ephemeral=True)

        # If no origin and user is owner, move from ALL channels
        if not origin:
            if interaction.user.id == YOUR_USER_ID:
                source_channels = [c for c in interaction.guild.voice_channels if len(c.members) > 0]
                if not source_channels:
                    await interaction.followup.send("<:Denied:1426930694633816248> No users in any voice channels.",
                                                    ephemeral=True)
                    return
            else:
                await interaction.followup.send("<:Denied:1426930694633816248> You must specify a source channel!",
                                                ephemeral=True)
                return
        else:
            source_channels = [origin]

        # Collect members
        members_to_move = []
        for channel in source_channels:
            members_to_move.extend(channel.members)
        members_to_move = list(set(members_to_move))

        if not members_to_move:
            await interaction.followup.send("<:Denied:1426930694633816248> No users found in source channel(s).",
                                            ephemeral=True)
            return

        # Perform moves (owner included)
        results = await self.perform_bulk_moves(
            members=members_to_move,
            dest_channels=[to],
            reason=reason,
            moderator=interaction.user,
            include_me=True
        )

        await self.send_move_summary(interaction, results, f"Move to {to.name}", reason)

    # /vc swap - Supervisor+ (single channels only)
    @vc_group.command(name="swap", description="Swap users between two voice channels")
    @app_commands.describe(
        a="First channel",
        b="Second channel",
        reason="Reason for the swap"
    )
    @has_role_level(SUPERVISORS + LEADERS + LOCKS + [YOUR_USER_ID])
    async def vc_swap(
            self,
            interaction: discord.Interaction,
            a: discord.VoiceChannel,
            b: discord.VoiceChannel,
            reason: Optional[str] = "No reason provided"
    ):
        """Swap users between two channels"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Swapping users...", ephemeral=True)

        # Collect members from both channels
        members_a = list(a.members)
        members_b = list(b.members)

        if not members_a and not members_b:
            await interaction.followup.send("<:Denied:1426930694633816248> No users in either channel.", ephemeral=True)
            return

        results = {'success': [], 'failed': [], 'not_in_voice': []}

        # Move A to B
        if members_a:
            results_a = await self.perform_bulk_moves(
                members=members_a,
                dest_channels=[b],
                reason=f"Swap (A→B): {reason}",
                moderator=interaction.user,
                include_me=True
            )
            results['success'].extend(results_a['success'])
            results['failed'].extend(results_a['failed'])
            results['not_in_voice'].extend(results_a['not_in_voice'])

        await asyncio.sleep(0.3)

        # Move B to A
        if members_b:
            results_b = await self.perform_bulk_moves(
                members=members_b,
                dest_channels=[a],
                reason=f"Swap (B→A): {reason}",
                moderator=interaction.user,
                include_me=True
            )
            results['success'].extend(results_b['success'])
            results['failed'].extend(results_b['failed'])
            results['not_in_voice'].extend(results_b['not_in_voice'])

        await self.send_move_summary(
            interaction, results,
            f"Swapped {len(members_a)} ↔️ {len(members_b)} users",
            reason
        )

    # /vc wipe - Leader+ (single channel, owner can leave empty for all)
    @vc_group.command(name="wipe", description="Disconnect all users from a voice channel")
    @app_commands.describe(
        channel="Channel to empty",
        reason="Reason for wiping"
    )
    @has_role_level(LEADERS + LOCKS + [YOUR_USER_ID])
    async def vc_wipe(
            self,
            interaction: discord.Interaction,
            channel: Optional[discord.VoiceChannel] = None,
            reason: Optional[str] = "No reason provided"
    ):
        """Empty a voice channel"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Wiping channel(s)...", ephemeral=True)

        # Check if wiping all channels (owner only)
        if not channel:
            if interaction.user.id == YOUR_USER_ID:
                source_channels = [c for c in interaction.guild.voice_channels if len(c.members) > 0]
                if not source_channels:
                    await interaction.followup.send("<:Denied:1426930694633816248> No users in any voice channels.",
                                                    ephemeral=True)
                    return

                # Require reason for major action
                if reason == "No reason provided":
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Reason required for wiping all channels!", ephemeral=True)
                    return
            else:
                await interaction.followup.send("<:Denied:1426930694633816248> You must specify a channel!",
                                                ephemeral=True)
                return
        else:
            source_channels = [channel]

        # Collect members
        members_to_disconnect = []
        for ch in source_channels:
            members_to_disconnect.extend(ch.members)
        members_to_disconnect = list(set(members_to_disconnect))

        if not members_to_disconnect:
            await interaction.followup.send("<:Denied:1426930694633816248> No users to disconnect.", ephemeral=True)
            return

        # Disconnect all (owner included)
        results = {'success': [], 'failed': [], 'not_in_voice': []}
        tasks = []
        for member in members_to_disconnect:
            task = self.safe_disconnect_member(member, reason, interaction.user, results)
            tasks.append(task)

        # Execute in batches
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.2)

        # Send summary
        embed = discord.Embed(
            title="Wipe Operation",
            description=f"**Channels:** {len(source_channels)}\n**Users Disconnected:** {len(results['success'])}",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        embed.add_field(name="Reason", value=reason, inline=False)

        if results['success']:
            success_names = ", ".join([m['member'].name for m in results['success'][:15]])
            if len(results['success']) > 15:
                success_names += f" +{len(results['success']) - 15} more"
            embed.add_field(name="<:Accepted:1426930333789585509> Disconnected", value=success_names, inline=False)

        if results['failed']:
            failed_text = "\n".join([f"• {item['member'].name}: {item['error']}" for item in results['failed'][:5]])
            if len(results['failed']) > 5:
                failed_text += f"\n*... and {len(results['failed']) - 5} more*"
            embed.add_field(name="<:Denied:1426930694633816248> Failed", value=failed_text, inline=False)

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.send_to_mod_logs(interaction.guild, embed)

    # /vc user - Supervisor+ (move specific users)
    @vc_group.command(name="user", description="Move specific user(s) to a voice channel")
    @app_commands.describe(
        users="Users to move (space-separated mentions, IDs, or names)",
        to="Destination channel",
        reason="Reason for the move"
    )
    @has_role_level(SUPERVISORS + LEADERS + LOCKS + [YOUR_USER_ID])
    async def vc_user(
            self,
            interaction: discord.Interaction,
            users: str,
            to: discord.VoiceChannel,
            reason: Optional[str] = "No reason provided"
    ):
        """Move specific users to a channel"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Moving users...", ephemeral=True)

        # Parse users
        member_list = await self.parse_users(interaction, users)
        if not member_list:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid users found.", ephemeral=True)
            return

        # Perform moves (owner included)
        results = await self.perform_bulk_moves(
            members=member_list,
            dest_channels=[to],
            reason=reason,
            moderator=interaction.user,
            include_me=True
        )

        await self.send_move_summary(interaction, results, "Specific Users", reason)

    @vca_group.command(name="move", description="Move all users from multiple channels with optional trail")
    @app_commands.describe(
        origin="Source channels (space-separated, or 'ALL')",
        to="Destination channels (space-separated, or 'ALL')",
        trail="Enable trail mode (sequential moves through channels) - Owner only",
        reason="Reason for the move"
    )
    @has_role_level(LOCKS + [YOUR_USER_ID])
    async def vca_move(
            self,
            interaction: discord.Interaction,
            origin: str,
            to: str,
            trail: bool = False,
            reason: Optional[str] = "No reason provided"
    ):
        """Advanced move with multi-channel support and trail option"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Moving users...", ephemeral=True)

        # Check trail permission
        if trail and interaction.user.id != YOUR_USER_ID:
            await interaction.followup.send("<:Denied:1426930694633816248> Trail mode is owner only!", ephemeral=True)
            return

        # Parse channels
        source_channels = await self.parse_channels_or_all(interaction, origin, filter_with_users=True)
        dest_channels = await self.parse_channels_or_all(interaction, to, filter_with_users=False)

        if not source_channels:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid source channels found.",
                                            ephemeral=True)
            return

        if not dest_channels:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid destination channels found.",
                                            ephemeral=True)
            return

        # Collect members
        members_to_move = []
        for channel in source_channels:
            members_to_move.extend(channel.members)
        members_to_move = list(set(members_to_move))

        if not members_to_move:
            await interaction.followup.send("<:Denied:1426930694633816248> No users found in source channels.",
                                            ephemeral=True)
            return

        if trail:
            # Trail mode: move through each channel sequentially
            await self.move_trail_users(interaction, members_to_move, dest_channels, reason, include_me=True)
        else:
            # Normal mode: move all to destination channels (distributed)
            results = await self.perform_bulk_moves(
                members=members_to_move,
                dest_channels=dest_channels,
                reason=reason,
                moderator=interaction.user,
                distribute=True,
                include_me=True
            )
            await self.send_move_summary(interaction, results,
                                         f"From {len(source_channels)} to {len(dest_channels)} channels", reason)

    # /vca disperse - Locks+ (trail is owner only)
    @vca_group.command(name="disperse", description="Randomly disperse users across channels with optional trail")
    @app_commands.describe(
        origin="Source channels (space-separated, or 'ALL')",
        to="Destination channels (space-separated, or 'ALL')",
        trail="Enable trail mode (repeated random dispersions) - Owner only",
        repetitions="Number of trail repetitions (only with trail=True)",
        reason="Reason for dispersing"
    )
    @has_role_level(LOCKS + [YOUR_USER_ID])
    async def vca_disperse(
            self,
            interaction: discord.Interaction,
            origin: str,
            to: str,
            trail: bool = False,
            repetitions: Optional[int] = 3,
            reason: Optional[str] = "No reason provided"
    ):
        """Advanced disperse with multi-channel support and trail option"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Dispersing users...", ephemeral=True)

        # Check trail permission
        if trail and interaction.user.id != YOUR_USER_ID:
            await interaction.followup.send("<:Denied:1426930694633816248> Trail mode is owner only!", ephemeral=True)
            return

        # Parse channels
        source_channels = await self.parse_channels_or_all(interaction, origin, filter_with_users=True)
        dest_channels = await self.parse_channels_or_all(interaction, to, filter_with_users=False)

        if not source_channels:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid source channels found.",
                                            ephemeral=True)
            return

        if not dest_channels:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid destination channels found.",
                                            ephemeral=True)
            return

        # Collect members
        members_to_move = []
        for channel in source_channels:
            members_to_move.extend(channel.members)
        members_to_move = list(set(members_to_move))

        if not members_to_move:
            await interaction.followup.send("<:Denied:1426930694633816248> No users found in source channels.",
                                            ephemeral=True)
            return

        if trail:
            # Trail mode: repeated random dispersions
            if repetitions < 1:
                repetitions = 3
            await self.move_disperse_trail_users(interaction, members_to_move, dest_channels, repetitions, reason,
                                                 include_me=True)
        else:
            # Normal mode: single random dispersion
            import random
            shuffled = members_to_move.copy()
            random.shuffle(shuffled)

            results = await self.perform_bulk_moves(
                members=shuffled,
                dest_channels=dest_channels,
                reason=reason,
                moderator=interaction.user,
                distribute=True,
                include_me=True
            )
            await self.send_move_summary(interaction, results, f"Dispersed across {len(dest_channels)} channels",
                                         reason)

    # /vca tour - Owner only (sequential channel tour)
    @vca_group.command(name="tour", description="Move users sequentially through multiple channels")
    @app_commands.describe(
        channels="Channels for the tour (space-separated, or 'ALL')",
        users="Specific users (optional, leave empty for all users in voice)",
        reason="Reason for the tour"
    )
    @is_owner()
    async def vca_tour(
            self,
            interaction: discord.Interaction,
            channels: str,
            users: Optional[str] = None,
            reason: Optional[str] = "No reason provided"
    ):
        """Sequential channel tour - owner only"""
        await interaction.response.send_message("<a:Load:1430912797469970444> Starting tour...", ephemeral=True)

        # Parse channels
        tour_channels = await self.parse_channels_or_all(interaction, channels, filter_with_users=False)

        if not tour_channels:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid channels found.", ephemeral=True)
            return

        if len(tour_channels) < 2:
            await interaction.followup.send("<:Denied:1426930694633816248> Tour requires at least 2 channels.",
                                            ephemeral=True)
            return

        # Get members
        if users:
            member_list = await self.parse_users(interaction, users)
            if not member_list:
                await interaction.followup.send("<:Denied:1426930694633816248> No valid users found.", ephemeral=True)
                return
        else:
            # Get all users currently in voice
            member_list = []
            for channel in interaction.guild.voice_channels:
                member_list.extend(channel.members)
            member_list = list(set(member_list))

            if not member_list:
                await interaction.followup.send("<:Denied:1426930694633816248> No users in voice channels.",
                                                ephemeral=True)
                return

        # Execute tour (owner included)
        await self.move_trail_users(interaction, member_list, tour_channels, reason, include_me=True)

    async def move_swap_channels(self, interaction: discord.Interaction,
                                 source_channels: List[discord.VoiceChannel],
                                 dest_channels: List[discord.VoiceChannel], reason: str):
        """Swap users between two sets of channels"""

        import asyncio

        # Collect members from both sets
        source_members = []
        for channel in source_channels:
            source_members.extend(channel.members)
        source_members = list(set(source_members))

        dest_members = []
        for channel in dest_channels:
            dest_members.extend(channel.members)
        dest_members = list(set(dest_members))

        if not source_members and not dest_members:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No users found in either source or destination channels.",
                ephemeral=True
            )
            return

        # Create a temporary holding area (we'll use None/disconnect then reconnect)
        # Or we can do simultaneous moves which is cleaner

        results = {
            'success': [],
            'failed': [],
            'not_in_voice': []
        }

        # Move source members to destination channels
        if source_members:
            source_results = await self.perform_bulk_moves(
                members=source_members,
                dest_channels=dest_channels,
                reason=f"Swap (1/2): {reason}",
                moderator=interaction.user,
                distribute=True
            )
            # Merge results
            results['success'].extend(source_results['success'])
            results['failed'].extend(source_results['failed'])
            results['not_in_voice'].extend(source_results['not_in_voice'])

        # Small delay to ensure first moves complete
        await asyncio.sleep(0.3)

        # Move destination members to source channels
        if dest_members:
            dest_results = await self.perform_bulk_moves(
                members=dest_members,
                dest_channels=source_channels,
                reason=f"Swap (2/2): {reason}",
                moderator=interaction.user,
                distribute=True
            )
            # Merge results
            results['success'].extend(dest_results['success'])
            results['failed'].extend(dest_results['failed'])
            results['not_in_voice'].extend(dest_results['not_in_voice'])

        # Send summary
        await self.send_move_summary(
            interaction, results,
            f"Swapped {len(source_members)} ↔️ {len(dest_members)} users between channels",
            reason
        )

    async def move_empty_channels(self, interaction: discord.Interaction,
                                  source_channels: List[discord.VoiceChannel], reason: str,
                                  include_me: bool = False):
        """Disconnect all users from specified channel(s)"""

        # Collect members from source channels
        members_to_disconnect = []
        for channel in source_channels:
            members_to_disconnect.extend(channel.members)

        # Remove duplicates
        members_to_disconnect = list(set(members_to_disconnect))

        if not members_to_disconnect:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No users to disconnect.",
                ephemeral=True
            )
            return

        # Perform disconnections
        import asyncio

        results = {
            'success': [],
            'failed': [],
            'not_in_voice': []
        }

        tasks = []
        for member in members_to_disconnect:
            if member.id == YOUR_USER_ID and not include_me:
                continue
            task = self.safe_disconnect_member(member, reason, interaction.user, results)
            tasks.append(task)

        # Execute in batches
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.2)

        # Create summary
        embed = discord.Embed(
            title="Disconnect Operation",
            description=f"**Total Users:** {len(members_to_disconnect)}",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason", value=reason, inline=False)

        if results['success']:
            success_names = ", ".join([m['member'].name for m in results['success'][:15]])
            if len(results['success']) > 15:
                success_names += f" +{len(results['success']) - 15} more"
            embed.add_field(
                name=f"<:Accepted:1426930333789585509> Disconnected ({len(results['success'])})",
                value=success_names,
                inline=False
            )

        if results['failed']:
            failed_text = ""
            for item in results['failed'][:5]:
                failed_text += f"• {item['member'].name}: {item['error']}\n"
            if len(results['failed']) > 5:
                failed_text += f"*... and {len(results['failed']) - 5} more*"
            embed.add_field(
                name=f"<:Denied:1426930694633816248> Failed ({len(results['failed'])})",
                value=failed_text,
                inline=False
            )

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.send_to_mod_logs(interaction.guild, embed)

    async def move_disperse_trail(self, interaction: discord.Interaction,
                                  source_channels: List[discord.VoiceChannel],  # Changed from members
                                  dest_channels: List[discord.VoiceChannel],
                                  repetitions: int, reason: str, include_me: bool = False):
        """Repeatedly disperse users from source channels randomly across destination channels"""

        import asyncio
        import random

        # Collect initial members from source channels
        initial_members = []
        for channel in source_channels:
            initial_members.extend(channel.members)
        initial_members = list(set(initial_members))

        if not initial_members:
            source_desc = "all voice channels" if len(source_channels) == len(
                interaction.guild.voice_channels) else "source channel(s)"
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> No users found in {source_desc}",
                ephemeral=True
            )
            return

        # Track the same users across all repetitions
        tracked_members = initial_members.copy()

        all_results = []

        for i in range(repetitions):
            # Filter out users who are no longer in voice
            current_members = [m for m in tracked_members if m.voice and m.voice.channel]

            if not current_members:
                await interaction.followup.send(
                    f"All users left voice channels after {i} dispersion(s). Stopping trail.",
                    ephemeral=True
                )
                break

            # Randomly shuffle for this iteration
            shuffled = current_members.copy()
            random.shuffle(shuffled)

            # Filter destination channels (avoid current channels if possible)
            current_channel_ids = {m.voice.channel.id for m in current_members if m.voice and m.voice.channel}
            filtered_dest = [c for c in dest_channels if c.id not in current_channel_ids]
            if not filtered_dest and len(dest_channels) > 1:
                filtered_dest = dest_channels
            elif not filtered_dest:
                filtered_dest = dest_channels

            # Perform dispersion
            results = await self.perform_bulk_moves(
                members=shuffled,
                dest_channels=filtered_dest,
                reason=f"Disperse Trail ({i + 1}/{repetitions}): {reason}",
                moderator=interaction.user,
                distribute=True,
                include_me=include_me
            )

            all_results.append({
                'iteration': i + 1,
                'results': results
            })

            # Wait between dispersions
            if i < repetitions - 1:
                await asyncio.sleep(1.5)  # 1.5 second delay between dispersions

        # Create comprehensive summary
        embed = discord.Embed(
            title="Disperse Trail Operation",
            description=f"**Repetitions:** {len(all_results)}/{repetitions}\n"
                        f"**Initial Users:** {len(initial_members)}\n"
                        f"**Source:** {len(source_channels)} channel(s)\n"
                        f"**Destinations:** {len(dest_channels)} channel(s)",
            color=discord.Color.purple(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason", value=reason, inline=False)

        # Summary for each iteration
        for iteration_data in all_results:
            iteration_num = iteration_data['iteration']
            results = iteration_data['results']

            success_count = len(results['success'])
            failed_count = len(results['failed'])

            status = "<:Accepted:1426930333789585509>" if success_count > 0 and failed_count == 0 else "<:Warn:1437771973970104471>" if success_count > 0 else "<:Denied:1426930694633816248>"

            embed.add_field(
                name=f"{status} Dispersion {iteration_num}",
                value=f"Success: {success_count} | Failed: {failed_count}",
                inline=True
            )

        # Final summary
        total_success = sum(len(r['results']['success']) for r in all_results)
        total_failed = sum(len(r['results']['failed']) for r in all_results)

        embed.add_field(
            name="Total Statistics",
            value=f"**Total Moves:** {total_success}\n**Total Failures:** {total_failed}",
            inline=False
        )

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.send_to_mod_logs(interaction.guild, embed)

    async def move_disperse_trail_users(self, interaction: discord.Interaction,
                                        members: List[discord.Member],
                                        dest_channels: List[discord.VoiceChannel],
                                        repetitions: int, reason: str, include_me: bool = False):
        """Repeatedly disperse specific users randomly across destination channels"""

        import asyncio
        import random

        if not members:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No valid users provided",
                ephemeral=True
            )
            return

        # Filter to only users currently in voice
        initial_members = [m for m in members if m.voice and m.voice.channel]

        if not initial_members:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> None of the specified users are in voice channels",
                ephemeral=True
            )
            return

        # Track the same users across all repetitions
        tracked_members = initial_members.copy()

        all_results = []

        for i in range(repetitions):
            # Filter out users who are no longer in voice
            current_members = [m for m in tracked_members if m.voice and m.voice.channel]

            if not current_members:
                await interaction.followup.send(
                    f"All users left voice channels after {i} dispersion(s). Stopping trail.",
                    ephemeral=True
                )
                break

            # Randomly shuffle for this iteration
            shuffled = current_members.copy()
            random.shuffle(shuffled)

            # Filter destination channels (avoid current channels if possible)
            current_channel_ids = {m.voice.channel.id for m in current_members if m.voice and m.voice.channel}
            filtered_dest = [c for c in dest_channels if c.id not in current_channel_ids]
            if not filtered_dest and len(dest_channels) > 1:
                filtered_dest = dest_channels
            elif not filtered_dest:
                filtered_dest = dest_channels

            # Perform dispersion
            results = await self.perform_bulk_moves(
                members=shuffled,
                dest_channels=filtered_dest,
                reason=f"Disperse Trail ({i + 1}/{repetitions}): {reason}",
                moderator=interaction.user,
                distribute=True,
                include_me=include_me
            )

            all_results.append({
                'iteration': i + 1,
                'results': results
            })

            # Wait between dispersions
            if i < repetitions - 1:
                await asyncio.sleep(1.5)

        # Create comprehensive summary
        embed = discord.Embed(
            title="Disperse Trail Operation (Specific Users)",
            description=f"**Repetitions:** {len(all_results)}/{repetitions}\n"
                        f"**Users:** {len(initial_members)}\n"
                        f"**Destinations:** {len(dest_channels)} channel(s)",
            color=discord.Color.purple(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason", value=reason, inline=False)

        # Summary for each iteration
        for iteration_data in all_results:
            iteration_num = iteration_data['iteration']
            results = iteration_data['results']

            success_count = len(results['success'])
            failed_count = len(results['failed'])

            status = "<:Accepted:1426930333789585509>" if success_count > 0 and failed_count == 0 else "<:Warn:1437771973970104471>" if success_count > 0 else "<:Denied:1426930694633816248>"

            embed.add_field(
                name=f"{status} Dispersion {iteration_num}",
                value=f"Success: {success_count} | Failed: {failed_count}",
                inline=True
            )

        # Final summary
        total_success = sum(len(r['results']['success']) for r in all_results)
        total_failed = sum(len(r['results']['failed']) for r in all_results)

        embed.add_field(
            name="Total Statistics",
            value=f"**Total Moves:** {total_success}\n**Total Failures:** {total_failed}",
            inline=False
        )

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.send_to_mod_logs(interaction.guild, embed)

    async def move_disperse_trail_origin(self, interaction: discord.Interaction,
                                                source_channels: List[discord.VoiceChannel],
                                                dest_channels: List[discord.VoiceChannel],
                                                repetitions: int, reason: str, include_me: bool = False):
        """Disperse trail starting from specific channels"""

        # Collect initial members from source channels
        initial_members = []
        for channel in source_channels:
            initial_members.extend(channel.members)
        initial_members = list(set(initial_members))

        if not initial_members:
            source_desc = "all voice channels" if len(source_channels) == len(
                interaction.guild.voice_channels) else "source channel(s)"
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> No users found in {source_desc}",
                ephemeral=True
            )
            return

        # Call the existing disperse_trail with the members list
        await self.move_disperse_trail(interaction, initial_members, dest_channels, repetitions, reason, include_me)

    async def move_trail_users(self, interaction: discord.Interaction,
                               members: List[discord.Member],
                               dest_channels: List[discord.VoiceChannel], reason: str,
                               include_me: bool = False):
        """Move users sequentially through a trail of channels"""

        import asyncio

        if len(dest_channels) < 2:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Move trail requires at least 2 destination channels to create a trail.",
                ephemeral=True
            )
            return

        all_results = []

        for i, channel in enumerate(dest_channels):
            # Filter out users who left voice
            current_members = [m for m in members if m.voice and m.voice.channel]

            if not current_members:
                await interaction.followup.send(
                    f"All users left voice channels after {i} move(s). Stopping trail.",
                    ephemeral=True
                )
                break

            # Move all users to this channel
            results = await self.perform_bulk_moves(
                members=current_members,
                dest_channels=[channel],  # Single channel for this step
                reason=f"Move Trail ({i + 1}/{len(dest_channels)}): {reason}",
                moderator=interaction.user,
                distribute=False
            )

            all_results.append({
                'channel': channel,
                'step': i + 1,
                'results': results
            })

            # Wait between moves (except after last move)
            if i < len(dest_channels) - 1:
                await asyncio.sleep(1.0)  # 1 second between channel moves

        # Create comprehensive summary
        embed = discord.Embed(
            title="Move Trail Operation",
            description=f"**Trail Length:** {len(all_results)}/{len(dest_channels)} channels\n"
                        f"**Users in Trail:** {len(members)}",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason", value=reason, inline=False)

        # Show the trail path
        trail_path = " → ".join([r['channel'].name for r in all_results])
        embed.add_field(
            name="Trail Path",
            value=trail_path if len(trail_path) <= 1024 else trail_path[:1020] + "...",
            inline=False
        )

        # Summary for each step
        for step_data in all_results:
            step_num = step_data['step']
            channel = step_data['channel']
            results = step_data['results']

            success_count = len(results['success'])
            failed_count = len(results['failed'])

            status = "<:Accepted:1426930333789585509>" if success_count > 0 and failed_count == 0 else "<:Warn:1437771973970104471>" if success_count > 0 else "<:Denied:1426930694633816248>"

            embed.add_field(
                name=f"{status} Step {step_num}: {channel.name}",
                value=f"Moved: {success_count} | Failed: {failed_count}",
                inline=True
            )

        # Final statistics
        total_moves = sum(len(r['results']['success']) for r in all_results)
        total_failed = sum(len(r['results']['failed']) for r in all_results)

        embed.add_field(
            name="Total Statistics",
            value=f"**Total Moves:** {total_moves}\n**Total Failures:** {total_failed}",
            inline=False
        )

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.send_to_mod_logs(interaction.guild, embed)

    async def move_swap_channels_repeated(self, interaction: discord.Interaction,
                                          source_channels: List[discord.VoiceChannel],
                                          dest_channels: List[discord.VoiceChannel],
                                          repetitions: int, reason: str,
                                          include_me: bool = False):
        """Swap users between channels multiple times"""

        import asyncio

        all_results = []

        for i in range(repetitions):
            # Get current members (they may have moved or left)
            source_members = []
            for channel in source_channels:
                source_members.extend(channel.members)
            source_members = list(set(source_members))

            dest_members = []
            for channel in dest_channels:
                dest_members.extend(channel.members)
            dest_members = list(set(dest_members))

            if not source_members and not dest_members:
                await interaction.followup.send(
                    f"No users left in any channels after {i} swap(s). Stopping.",
                    ephemeral=True
                )
                break

            iteration_results = {
                'success': [],
                'failed': [],
                'not_in_voice': []
            }

            # Move source to dest
            if source_members:
                source_results = await self.perform_bulk_moves(
                    members=source_members,
                    dest_channels=dest_channels,
                    reason=f"Swap {i + 1}/{repetitions} (A→B): {reason}",
                    moderator=interaction.user,
                    distribute=True
                )
                iteration_results['success'].extend(source_results['success'])
                iteration_results['failed'].extend(source_results['failed'])
                iteration_results['not_in_voice'].extend(source_results['not_in_voice'])

            await asyncio.sleep(0.3)

            # Move dest to source
            if dest_members:
                dest_results = await self.perform_bulk_moves(
                    members=dest_members,
                    dest_channels=source_channels,
                    reason=f"Swap {i + 1}/{repetitions} (B→A): {reason}",
                    moderator=interaction.user,
                    distribute=True
                )
                iteration_results['success'].extend(dest_results['success'])
                iteration_results['failed'].extend(dest_results['failed'])
                iteration_results['not_in_voice'].extend(dest_results['not_in_voice'])

            all_results.append({
                'iteration': i + 1,
                'source_count': len(source_members),
                'dest_count': len(dest_members),
                'results': iteration_results
            })

            # Wait between swaps (except after last swap)
            if i < repetitions - 1:
                await asyncio.sleep(1.5)

        # Create comprehensive summary
        embed = discord.Embed(
            title="Repeated Swap Operation",
            description=f"**Swaps Completed:** {len(all_results)}/{repetitions}\n"
                        f"**Channels A:** {', '.join(c.name for c in source_channels)}\n"
                        f"**Channels B:** {', '.join(c.name for c in dest_channels)}",
            color=discord.Color.gold(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason", value=reason, inline=False)

        # Summary for each swap
        for swap_data in all_results:
            iteration_num = swap_data['iteration']
            source_count = swap_data['source_count']
            dest_count = swap_data['dest_count']
            results = swap_data['results']

            success_count = len(results['success'])
            failed_count = len(results['failed'])

            status = "<:Accepted:1426930333789585509>" if failed_count == 0 else "<:Warn:1437771973970104471>"

            embed.add_field(
                name=f"{status} Swap {iteration_num}",
                value=f"{source_count} ↔️ {dest_count} users\nSuccess: {success_count} | Failed: {failed_count}",
                inline=True
            )

        # Total statistics
        total_success = sum(len(r['results']['success']) for r in all_results)
        total_failed = sum(len(r['results']['failed']) for r in all_results)

        embed.add_field(
            name="Total Statistics",
            value=f"**Total Swaps:** {total_success}\n**Total Failures:** {total_failed}",
            inline=False
        )

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.followup.send(embed=embed, ephemeral=True)
        await self.send_to_mod_logs(interaction.guild, embed)

    async def safe_disconnect_member(self, member: discord.Member, reason: str,
                                     moderator: discord.Member, results: dict):
        """Safely disconnect a member with error handling"""
        try:
            if not member.voice or not member.voice.channel:
                results['not_in_voice'].append(member)
                return

            await member.move_to(None, reason=f"{reason} | By {moderator.name}")

            results['success'].append({
                'member': member,
                'channel': None
            })

        except discord.Forbidden:
            results['failed'].append({
                'member': member,
                'error': 'Missing permissions'
            })
        except discord.HTTPException as e:
            if e.code == 40032:
                results['not_in_voice'].append(member)
            else:
                results['failed'].append({
                    'member': member,
                    'error': f'Error {e.code}: {str(e)[:30]}'
                })
        except Exception as e:
            results['failed'].append({
                'member': member,
                'error': str(e)[:50]
            })

    async def parse_channels_spaces(self, interaction: discord.Interaction, channels_str: str) -> List[
        discord.VoiceChannel]:
        """Parse channel string (space-separated) into list of voice channels"""
        channels = []
        parts = channels_str.split()  # Split by spaces instead of commas

        for part in parts:
            channel = None

            # Try as mention
            if part.startswith('<#') and part.endswith('>'):
                channel_id = part.strip('<#>')
                try:
                    channel = interaction.guild.get_channel(int(channel_id))
                except ValueError:
                    pass

            # Try as ID
            if not channel:
                try:
                    channel = interaction.guild.get_channel(int(part))
                except ValueError:
                    pass

            # Try as name (match full name, spaces handled by quotes in Discord)
            if not channel:
                channel = discord.utils.find(
                    lambda c: isinstance(c, discord.VoiceChannel) and c.name.lower() == part.lower(),
                    interaction.guild.channels
                )

            if channel and isinstance(channel, discord.VoiceChannel) and channel not in channels:
                channels.append(channel)

        return channels

    async def parse_channels_or_all(self, interaction: discord.Interaction, channels_str: Optional[str],
                                    filter_with_users: bool = False) -> List[discord.VoiceChannel]:
        """Parse channel string or 'ALL' keyword into list of voice channels"""
        if not channels_str:
            return []

        # Check for ALL keyword (case-insensitive)
        if channels_str.strip().upper() == "ALL":
            if filter_with_users:
                # Return all channels with users
                return [c for c in interaction.guild.voice_channels if len(c.members) > 0]
            else:
                # Return all voice channels
                return interaction.guild.voice_channels

        # Otherwise parse normally
        return await self.parse_channels_spaces(interaction, channels_str)

    async def move_specific_users(self, interaction: discord.Interaction, users_str: str,
                                  dest_channels: List[discord.VoiceChannel], reason: str):
        """Move specific users to destination channel(s)"""

        # Parse users
        member_list = await self.parse_users(interaction, users_str)
        if not member_list:
            await interaction.followup.send("<:Denied:1426930694633816248> No valid users found.", ephemeral=True)
            return

        # Perform moves with validation and speed optimization
        results = await self.perform_bulk_moves(
            members=member_list,
            dest_channels=dest_channels,
            reason=reason,
            moderator=interaction.user
        )

        # Send summary
        await self.send_move_summary(interaction, results, "Specific Users", reason)

    async def move_from_enhanced(self, interaction: discord.Interaction,
                                 source_channels: List[discord.VoiceChannel],
                                 dest_channels: List[discord.VoiceChannel], reason: str,
                                 include_me: bool = False):
        """Move all users from source channel(s) to destination channel(s)"""

        # Collect all members from source channels
        members_to_move = []
        for channel in source_channels:
            members_to_move.extend(channel.members)

        if not members_to_move:
            channel_names = "all voice channels" if len(source_channels) > 5 else ', '.join(
                c.name for c in source_channels)
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> No users found in {channel_names}",
                ephemeral=True
            )
            return

        # Remove duplicates
        members_to_move = list(set(members_to_move))

        # Perform moves
        results = await self.perform_bulk_moves(
            members=members_to_move,
            dest_channels=dest_channels,
            reason=reason,
            moderator=interaction.user
        )

        # Send summary
        source_desc = "all voice channels" if len(source_channels) > 5 else f"{len(source_channels)} channel(s)"
        await self.send_move_summary(
            interaction, results,
            f"From {source_desc} to {len(dest_channels)} channel(s)",
            reason
        )

    async def move_disperse_enhanced(self, interaction: discord.Interaction,
                                     source_channels: List[discord.VoiceChannel],
                                     dest_channels: List[discord.VoiceChannel], reason: str,
                                     include_me: bool = False):
        """Randomly disperse users from source channel(s) across destination channel(s)"""

        # Collect all members from source channels
        members_to_move = []
        for channel in source_channels:
            members_to_move.extend(channel.members)

        if not members_to_move:
            source_desc = "all voice channels" if len(source_channels) == len(
                interaction.guild.voice_channels) else "source channel(s)"
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> No users found in {source_desc}",
                ephemeral=True
            )
            return

        # Remove duplicates
        members_to_move = list(set(members_to_move))

        # Filter out source channels from destination channels to avoid moving users to their own channel
        # (unless there's only one destination channel specified)
        if len(dest_channels) > 1:
            source_ids = {c.id for c in source_channels}
            filtered_dest = [c for c in dest_channels if c.id not in source_ids]
            if filtered_dest:
                dest_channels = filtered_dest

        # Randomly shuffle the members
        import random
        random.shuffle(members_to_move)

        # Perform moves with random distribution
        results = await self.perform_bulk_moves(
            members=members_to_move,
            dest_channels=dest_channels,
            reason=reason,
            moderator=interaction.user,
            distribute=True
        )

        # Send summary
        source_desc = f"all {len(source_channels)} voice channels" if len(source_channels) == len(
            interaction.guild.voice_channels) else f"{len(source_channels)} channel(s)"
        dest_desc = f"all {len(dest_channels)} voice channels" if len(dest_channels) == len(
            interaction.guild.voice_channels) else f"{len(dest_channels)} channel(s)"

        await self.send_move_summary(
            interaction, results,
            f"Dispersed from {source_desc} across {dest_desc}",
            reason
        )

    async def perform_bulk_moves(self, members: List[discord.Member],
                                 dest_channels: List[discord.VoiceChannel],
                                 reason: str, moderator: discord.Member,
                                 distribute: bool = False, include_me: bool = False) -> dict:
        """
        Perform bulk moves with optimization and validation.
        Returns results dictionary with success/failure counts.
        """
        import asyncio

        results = {
            'success': [],
            'failed': [],
            'not_in_voice': []
        }

        # Create move tasks for concurrent execution (faster moves)
        tasks = []

        for i, member in enumerate(members):
            # Skip bot owner unless include_me is True
            if member.id == YOUR_USER_ID and not include_me:
                continue

            # Check if user is in voice
            if not member.voice or not member.voice.channel:
                results['not_in_voice'].append(member)
                continue

            # Determine destination channel
            if distribute:
                # Distribute evenly across channels
                dest_channel = dest_channels[i % len(dest_channels)]
            else:
                # Cycle through channels (split load)
                dest_channel = dest_channels[i % len(dest_channels)]

            # Create move task
            task = self.safe_move_member(member, dest_channel, reason, moderator, results)
            tasks.append(task)

        # Execute all moves concurrently for speed
        # Limit concurrency to avoid rate limits (Discord allows ~5 moves/sec)
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            # Small delay between batches
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.2)

        return results

    async def safe_move_member(self, member: discord.Member, dest_channel: discord.VoiceChannel,
                               reason: str, moderator: discord.Member, results: dict):
        """Safely move a member with error handling"""
        try:
            # Double-check they're still in voice before moving
            if not member.voice or not member.voice.channel:
                results['not_in_voice'].append(member)
                return

            # Perform the move
            await member.move_to(
                dest_channel,
                reason=f"{reason} | By {moderator.name}"
            )

            results['success'].append({
                'member': member,
                'channel': dest_channel
            })

        except discord.Forbidden:
            results['failed'].append({
                'member': member,
                'error': 'Missing permissions'
            })
        except discord.HTTPException as e:
            # Handle specific error codes
            if e.code == 40032:  # User not in voice
                results['not_in_voice'].append(member)
            else:
                results['failed'].append({
                    'member': member,
                    'error': f'Error {e.code}: {str(e)[:30]}'
                })
        except Exception as e:
            results['failed'].append({
                'member': member,
                'error': str(e)[:50]
            })

    async def send_move_summary(self, interaction: discord.Interaction, results: dict,
                                operation: str, reason: str):
        """Send a summary embed of the move operation"""

        total_attempted = len(results['success']) + len(results['failed']) + len(results['not_in_voice'])

        embed = discord.Embed(
            title="Voice Channel Move Operation",
            description=f"**Operation:** {operation}\n**Total Affected:** {total_attempted} users",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )

        embed.add_field(name="Reason", value=reason, inline=False)

        # Success summary
        if results['success']:
            success_text = f"<:Accepted:1426930333789585509> **{len(results['success'])} moved successfully**\n"
            # Group by destination channel
            by_channel = {}
            for item in results['success']:
                channel_name = item['channel'].name
                if channel_name not in by_channel:
                    by_channel[channel_name] = []
                by_channel[channel_name].append(item['member'])

            for channel_name, members in by_channel.items():
                member_list = ", ".join([m.name for m in members[:5]])
                if len(members) > 5:
                    member_list += f" +{len(members) - 5} more"
                success_text += f"• **{channel_name}**: {member_list}\n"

            embed.add_field(name="<:Accepted:1426930333789585509> Successful Moves", value=success_text, inline=False)

        # Failed moves
        if results['failed']:
            failed_text = f"<:Denied:1426930694633816248> **{len(results['failed'])} failed**\n"
            for item in results['failed'][:5]:
                failed_text += f"• {item['member'].name}: {item['error']}\n"
            if len(results['failed']) > 5:
                failed_text += f"*... and {len(results['failed']) - 5} more*"
            embed.add_field(name="<:Denied:1426930694633816248> Failed Moves", value=failed_text, inline=False)

        # Not in voice
        if results['not_in_voice']:
            niv_text = f"**{len(results['not_in_voice'])} not in voice**\n"
            niv_names = ", ".join([m.name for m in results['not_in_voice'][:5]])
            if len(results['not_in_voice']) > 5:
                niv_names += f" +{len(results['not_in_voice']) - 5} more"
            niv_text += niv_names
            embed.add_field(name="Skipped", value=niv_text, inline=False)

        embed.set_footer(text=f"Executed by {interaction.user.name}")

        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

        # Send to mod logs
        await self.send_to_mod_logs(interaction.guild, embed)


async def setup(bot):
    cog = ModerateCog(bot)
    await bot.add_cog(cog)
