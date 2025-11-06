import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta, time
import pytz
from discord.ext import commands, tasks
from typing import Optional
from database import db, ensure_database_connected

import asyncio
import math
import json

# Role IDs for shift types
FENZ_ROLE_ID = 1412790680991961149
HHSTJ_ROLE_ID = 1414146295974727861
CC_ROLE_ID = 1430108352377262090

QUOTA_BYPASS_ROLE = 1431031672777740298  # QB - Full quota bypass
REDUCED_ACTIVITY_ROLE = 1435755328749699275  # RA - 50% quota requirement
LOA_ROLE = 1415423781161402468  # LOA - Full quota bypass

FENZ_LEADERBOARD_ROLES = [1365536209681514636, 1285474077556998196, 1390867686170300456]
HHSTJ_LEADERBOARD_ROLES = [1389113393511923863, 1389113460687765534, 1414146295974727861]
CC_LEADERBOARD_ROLES = [1430108352377262090, 1430116569077383179]


typeS = {
    FENZ_ROLE_ID: "Shift FENZ",
    HHSTJ_ROLE_ID: "Shift HHStJ",
    CC_ROLE_ID: "Shift CC"
}

# Duty role IDs
FENZ_DUTY_ROLE = 1386898954892873843
FENZ_BREAK_ROLE = 1427639620367286282
HHSTJ_DUTY_ROLE = 1386898954892873843
HHSTJ_BREAK_ROLE = 1427639620367286282
CC_DUTY_ROLE = 1430110532932997152
CC_BREAK_ROLE = 1430110532932997152  # Update this with the actual CC break role if different

# Admin role IDs
ADMIN_ROLES = [
    1389113393511923863,
    1389113460687765534,
    1365536209681514636,
    1389550689113473024,
    1285474077556998196
]

# Senior admin role IDs (for modify/delete operations)
SENIOR_ADMIN_ROLES = [
    1285474077556998196,
    1389113393511923863,
    1389550689113473024
]

# Super admin role IDs (for clear operations)
SUPER_ADMIN_ROLES = [1389550689113473024]

# Quota management role IDs
QUOTA_ADMIN_ROLES = [
    1285474077556998196,
    1389113393511923863,
    1389550689113473024
]

# Leaderboard view role IDs
LEADERBOARD_ROLES = [
    1386898954892873843,
    1430110532932997152,
    1389113393511923863,
    1389113460687765534,
    1365536209681514636,
    1389550689113473024,
    1285474077556998196
]

# Reset command role IDs
RESET_ROLES = [
    1389550689113473024,
    1389157641799991347,
    1389111326571499590
]

# Additional shift type access for specific roles
ADDITIONAL_SHIFT_ACCESS = {
    1389113393511923863: ["Shift HHStJ", "Shift CC"],
    1389113460687765534: ["Shift HHStJ", "Shift CC"],
    1285474077556998196: ["Shift FENZ", "Shift CC"],
    1365536209681514636: ["Shift FENZ", "Shift CC"]
}

NZST = pytz.timezone('Pacific/Auckland')
SHIFT_LOGS_CHANNEL = 1435798856687161467
PING_ROLES = [1285474077556998196, 1389113393511923863, 1389550689113473024]


class WeeklyShiftManager:
    """Manages weekly shift resets and leaderboard generation"""

    def __init__(self, cog):
        self.cog = cog
        self.bot = cog.bot
        self.SHIFT_LOGS_CHANNEL = SHIFT_LOGS_CHANNEL

    @staticmethod
    def get_week_monday(dt: Optional[datetime] = None) -> datetime:
        """Get the Monday (start) of the week for a given datetime in NZST, returned as naive UTC"""
        if dt is None:
            dt = datetime.now(NZST)  # ✅ CORRECT
        elif dt.tzinfo is None:
            dt = NZST.localize(dt)  # ✅ CORRECT
        else:
            dt = dt.astimezone(WeeklyShiftManager.NZST)

        # Get to Monday of this week in NZST
        days_since_monday = dt.weekday()
        monday = dt - timedelta(days=days_since_monday)
        monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)

        # Convert to UTC and return as naive datetime for database storage
        monday_utc = monday.astimezone(pytz.UTC)
        return monday_utc.replace(tzinfo=None)

    @staticmethod
    def get_current_week_monday() -> datetime:
        """Returns Monday of the current week as naive UTC"""
        return WeeklyShiftManager.get_week_monday()

    @staticmethod
    def get_previous_week_monday() -> datetime:
        """Returns Monday of last week as naive UTC"""
        current = WeeklyShiftManager.get_current_week_monday()
        return current - timedelta(days=7)

    @staticmethod
    def get_two_weeks_ago_monday() -> datetime:
        """Returns Monday of two weeks ago as naive UTC"""
        current = WeeklyShiftManager.get_current_week_monday()
        return current - timedelta(days=14)

    async def archive_current_week_to_wave(self):
        """Archive current week shifts to next wave number"""
        async with self.cog.db.pool.acquire() as conn:
            current_week = self.get_current_week_monday()

            # Get next wave number
            max_wave = await conn.fetchval(
                'SELECT MAX(wave_number) FROM shifts WHERE wave_number IS NOT NULL'
            )
            next_wave = (max_wave or 0) + 1

            # Archive all current week shifts to wave number
            archived = await conn.execute(
                '''UPDATE shifts
                   SET wave_number = $1
                   WHERE week_identifier = $2
                     AND wave_number IS NULL''',
                next_wave, current_week
            )

            print(f"Weekly reset: Archived {archived} shifts to wave {next_wave}")

    async def force_end_active_shifts(self, assign_to_wave: int):
        """End all active shifts at the weekly reset and assign to wave"""
        async with self.cog.db.pool.acquire() as conn:
            active_shifts = await conn.fetch(
                'SELECT * FROM shifts WHERE end_time IS NULL'
            )

            for shift in active_shifts:
                # Calculate final pause duration
                pause_duration = shift.get('pause_duration', 0)
                if shift.get('pause_start'):
                    pause_duration += (datetime.utcnow() - shift['pause_start']).total_seconds()

                # End the shift and assign to current wave
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time        = $1,
                           pause_duration  = $2,
                           pause_start     = NULL,
                           week_identifier = $3,
                           wave_number     = $4
                       WHERE id = $5''',
                    datetime.utcnow(),
                    pause_duration,
                    self.get_current_week_monday(),
                    assign_to_wave,
                    shift['id']
                )

                # Clean up roles/nicknames
                member = self.bot.get_guild(shift.get('guild_id')).get_member(shift['discord_user_id'])
                if member:
                    await self.cog.update_nickname_for_shift_status(member, 'off')
                    await self.cog.update_duty_roles(member, shift['type'], 'off')

            print(f"Force-ended {len(active_shifts)} active shifts for wave {assign_to_wave}")

    async def generate_weekly_report(self, wave_number: int):
        """Generate and send weekly leaderboard report for a specific wave"""

        async with self.cog.db.pool.acquire() as conn:
            # Get all users who had shifts in this wave
            shifts = await conn.fetch(
                '''SELECT discord_user_id,
                          type,
                          SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                              COALESCE(pause_duration, 0)) as total_seconds
                   FROM shifts
                   WHERE wave_number = $1
                     AND end_time IS NOT NULL
                   GROUP BY discord_user_id, type
                   ORDER BY type, total_seconds DESC''',
                wave_number
            )

            if not shifts:
                print("No shifts to report for weekly reset")
                return

            # Get all role quotas
            quotas = await conn.fetch(
                'SELECT role_id, quota_seconds, type FROM shift_quotas'
            )
            quota_map = {}
            for q in quotas:
                key = (q['role_id'], q['type'])
                quota_map[key] = q['quota_seconds']

            # Get guild
            guild = self.bot.get_guild(shifts[0]['guild_id']) if 'guild_id' in shifts[0] else None
            if not guild:
                # Try to find guild from bot's guilds
                for g in self.bot.guilds:
                    if g.get_member(shifts[0]['discord_user_id']):
                        guild = g
                        break

            if not guild:
                print("Could not find guild for weekly report")
                return

            # Organize by shift type
            types = {}
            for shift in shifts:
                type = shift['type']
                if type not in types:
                    types[type] = []
                types[type].append(shift)

            # Build report embeds
            embeds = []

            for type, type_shifts in types.items():
                lines = []

                for shift_data in type_shifts:
                    member = guild.get_member(shift_data['discord_user_id'])
                    if not member:
                        continue

                    # Get user's highest quota for this shift type
                    max_quota = 0
                    for role in member.roles:
                        quota = quota_map.get((role.id, type), 0)
                        if quota > max_quota:
                            max_quota = quota

                    # Skip users without quota requirements
                    if max_quota == 0:
                        continue

                    active_seconds = shift_data['total_seconds']

                    # Check bypass roles
                    user_role_ids = {role.id for role in member.roles}
                    bypass_type = None
                    completed = False

                    if QUOTA_BYPASS_ROLE in user_role_ids:
                        bypass_type = 'QB'
                        completed = True
                        status = f"✅ {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **(QB Bypass)**"
                    elif LOA_ROLE in user_role_ids:
                        bypass_type = 'LOA'
                        completed = True
                        status = f"✅ {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **(LOA Exempt)**"
                    elif REDUCED_ACTIVITY_ROLE in user_role_ids:
                        bypass_type = 'RA'
                        modified_quota = max_quota * 0.5
                        percentage = (active_seconds / modified_quota * 100) if modified_quota > 0 else 0
                        completed = percentage >= 100
                        emoji = "✅" if completed else "❌"
                        status = f"{emoji} {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **({percentage:.1f}% - RA 50% Required)**"
                    else:
                        percentage = (active_seconds / max_quota * 100) if max_quota > 0 else 0
                        completed = percentage >= 100
                        emoji = "✅" if completed else "❌"
                        status = f"{emoji} {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **({percentage:.1f}%)**"

                    lines.append(status)

                if lines:
                    embed = discord.Embed(
                        title=f"**{type.replace('Shift ', '')} Weekly Report**",
                        description="\n".join(lines),
                        color=discord.Color(0x000000)
                    )

                    # Get week dates from wave
                    week_start = await conn.fetchval(
                        'SELECT MIN(week_identifier) FROM shifts WHERE wave_number = $1',
                        wave_number
                    )

                    if week_start:
                        week_end = week_start + timedelta(days=6)
                        embed.set_footer(
                            text=f"Wave {wave_number} • {week_start.strftime('%d %b')} - {week_end.strftime('%d %b %Y')}"
                        )
                    else:
                        embed.set_footer(text=f"Wave {wave_number}")

                    embeds.append(embed)

            # Send to channel
            channel = self.bot.get_channel(SHIFT_LOGS_CHANNEL)
            if channel and embeds:
                # Create ping message
                ping_mentions = " ".join([f"<@&{role_id}>" for role_id in self.PING_ROLES])

                await channel.send(
                    content=f"{ping_mentions}\n**Weekly Shift Report**",
                    embeds=embeds
                )

                print(f"Sent weekly report with {len(embeds)} embeds")

class ShiftManagementCog(commands.Cog):
    shift_group = app_commands.Group(name="shift", description="Shift management commands")

    def __init__(self, bot):
        self.bot = bot
        self._role_cache = {}
        self.SHIFT_LOGS_CHANNEL = SHIFT_LOGS_CHANNEL
        self._cache_cleanup_task = None
        self.weekly_manager = WeeklyShiftManager(self)
        bot.loop.create_task(self.on_cog_load())

    async def on_cog_load(self):
        """Run initialization tasks when cog loads"""
        # Wait for bot to be ready
        await self.bot.wait_until_ready()
        await ensure_database_connected()

        # Start weekly reset task
        if not self.weekly_reset_task.is_running():
            self.weekly_reset_task.start()

        # Clean up stale shifts
        await self.cleanup_stale_shifts(self.bot)

    @tasks.loop(time=time(hour=0, minute=0, tzinfo=pytz.timezone('Pacific/Auckland')))
    async def weekly_reset_task(self):
        """Check if it's Monday midnight NZST and run weekly reset"""
        now = datetime.now(WeeklyShiftManager.NZST)

        if now.weekday() == 0:  # Monday
            print(f"Running weekly shift reset at {now}")

            try:
                # Get next wave number ONCE
                async with db.pool.acquire() as conn:
                    max_wave = await conn.fetchval(
                        'SELECT MAX(wave_number) FROM shifts WHERE wave_number IS NOT NULL'
                    )
                    next_wave = (max_wave or 0) + 1

                # 1️⃣ Archive current week's completed shifts
                await self.weekly_manager.archive_current_week_to_wave()

                # 2️⃣ Force-end active shifts (they get archived by step 1)
                await self.weekly_manager.force_end_active_shifts(next_wave)

                # 3️⃣ Generate report for the completed wave
                await self.weekly_manager.generate_weekly_report(next_wave)

                print(f"Weekly reset completed successfully - Wave {next_wave} created")

            except Exception as e:
                print(f"Error during weekly reset: {e}")
                import traceback
                traceback.print_exc()

    @weekly_reset_task.before_loop
    async def before_weekly_reset(self):
        """Wait until bot is ready before starting the task"""
        await self.bot.wait_until_ready()

    def get_user_role_ids(self, member: discord.Member) -> set:
        """Cache user role IDs with TTL"""
        cache_key = (member.guild.id, member.id)
        current_time = datetime.utcnow()

        # Check cache
        if cache_key in self._role_cache:
            cached_data, timestamp = self._role_cache[cache_key]
            # Cache valid for 5 minutes
            if (current_time - timestamp).total_seconds() < 300:
                return cached_data

        # Update cache
        role_ids = {role.id for role in member.roles}
        self._role_cache[cache_key] = (role_ids, current_time)

        # Cleanup old entries periodically
        if len(self._role_cache) > 1000:
            cutoff = current_time - timedelta(minutes=5)
            self._role_cache = {
                k: v for k, v in self._role_cache.items()
                if v[1] > cutoff
            }

        return role_ids

    def has_admin_permission(self, member: discord.Member) -> bool:
        """Check if user has admin permissions (cached)"""
        return bool(self.get_user_role_ids(member) & set(ADMIN_ROLES))

    def has_senior_admin_permission(self, member: discord.Member) -> bool:
        """Check if user has senior admin permissions (cached)"""
        return bool(self.get_user_role_ids(member) & set(SENIOR_ADMIN_ROLES))

    def has_super_admin_permission(self, member: discord.Member) -> bool:
        """Check if user has super admin permissions (cached)"""
        return bool(self.get_user_role_ids(member) & set(SUPER_ADMIN_ROLES))

    async def log_shift_event(self, guild: discord.Guild, event_type: str, member: discord.Member,
                              shift_data: dict, admin: discord.Member = None, details: str = None):
        """
        Send shift event logs to the logging channel

        event_type: 'start', 'end', 'pause', 'resume', 'modify', 'delete', 'clear'
        """
        channel = self.bot.get_channel(SHIFT_LOGS_CHANNEL)  # ← Remove 'self.'
        if not channel:
            print(f"Warning: Shift logs channel {SHIFT_LOGS_CHANNEL} not found")  # ← Remove 'self.'
            return

        try:
            # Get callsign if available
            async with db.pool.acquire() as conn:
                callsign_row = await conn.fetchrow(
                    'SELECT callsign, fenz_prefix FROM callsigns WHERE discord_user_id = $1',
                    member.id
                )

            display_name = member.display_name
            if callsign_row:
                if callsign_row['fenz_prefix']:
                    display_name = f"@{callsign_row['fenz_prefix']}-{callsign_row['callsign']}"
                else:
                    display_name = f"@{callsign_row['callsign']}"

            type = shift_data.get('type', 'Unknown')
            shift_id = shift_data.get('id', 'N/A')

            # Build embed based on event type
            if event_type == 'start':
                embed = discord.Embed(
                    title=f"Shift Started • {type.replace('Shift ', '')}",
                    color=discord.Color(0x57f288)  # Green
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                embed.add_field(
                    name="Started",
                    value=f"<t:{int(shift_data['start_time'].timestamp())}:F> (<t:{int(shift_data['start_time'].timestamp())}:R>)",
                    inline=False
                )
                embed.set_footer(
                    text=f"{'Started by ' + admin.display_name if admin else 'Started by ' + display_name} • Shift ID: {shift_id}")

            elif event_type == 'end':
                duration = shift_data['end_time'] - shift_data['start_time']
                active_duration = duration - timedelta(seconds=shift_data.get('pause_duration', 0))
                break_duration = timedelta(seconds=shift_data.get('pause_duration', 0))

                embed = discord.Embed(
                    title=f"Shift Ended • {type.replace('Shift ', '')}",
                    color=discord.Color(0xed4245)  # Red
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                embed.add_field(
                    name="Total Time",
                    value=self.format_duration(active_duration),
                    inline=True
                )

                if break_duration.total_seconds() > 0:
                    embed.add_field(
                        name="Break Time",
                        value=self.format_duration(break_duration),
                        inline=True
                    )

                embed.set_footer(
                    text=f"{'Ended by ' + admin.display_name if admin else 'Ended by ' + display_name} • Shift ID: {shift_id}")

            elif event_type == 'pause':
                embed = discord.Embed(
                    title=f"Shift Paused • {type.replace('Shift ', '')}",
                    color=discord.Color(0xfee75c)  # Yellow
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                embed.add_field(
                    name="Break Started",
                    value=f"<t:{int(shift_data['pause_start'].timestamp())}:R>",
                    inline=False
                )
                embed.set_footer(
                    text=f"{'Paused by ' + admin.display_name if admin else 'Paused by ' + display_name} • Shift ID: {shift_id}")

            elif event_type == 'resume':
                last_break = timedelta(seconds=details) if details else timedelta(0)

                embed = discord.Embed(
                    title=f"Shift Resumed • {type.replace('Shift ', '')}",
                    color=discord.Color(0x57f288)  # Green
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                if last_break.total_seconds() > 0:
                    embed.add_field(
                        name="Break Duration",
                        value=self.format_duration(last_break),
                        inline=False
                    )
                embed.set_footer(
                    text=f"{'Resumed by ' + admin.display_name if admin else 'Resumed by ' + display_name} • Shift ID: {shift_id}")

            elif event_type == 'modify':
                embed = discord.Embed(
                    title=f"Shift Modified • {type.replace('Shift ', '')}",
                    color=discord.Color(0x5865f2)  # Blurple
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                if details:
                    embed.add_field(
                        name="Modification",
                        value=details,
                        inline=False
                    )
                embed.set_footer(text=f"Modified by {admin.display_name if admin else 'System'} • Shift ID: {shift_id}")

            elif event_type == 'delete':
                duration = shift_data['end_time'] - shift_data['start_time']
                active_duration = duration - timedelta(seconds=shift_data.get('pause_duration', 0))

                embed = discord.Embed(
                    title=f"Shift Deleted • {type.replace('Shift ', '')}",
                    color=discord.Color(0xed4245)  # Red
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                embed.add_field(
                    name="Duration (Deleted)",
                    value=self.format_duration(active_duration),
                    inline=True
                )
                embed.add_field(
                    name="Started",
                    value=f"<t:{int(shift_data['start_time'].timestamp())}:f>",
                    inline=True
                )
                embed.set_footer(text=f"Deleted by {admin.display_name if admin else 'System'} • Shift ID: {shift_id}")

            elif event_type == 'clear':
                embed = discord.Embed(
                    title=f"Shifts Cleared • {type.replace('Shift ', '')}",
                    color=discord.Color(0xed4245)  # Red
                )
                embed.add_field(
                    name="Staff Member",
                    value=f"{member.mention} • {display_name}",
                    inline=False
                )
                if details:
                    embed.add_field(
                        name="Shifts Cleared",
                        value=details,
                        inline=False
                    )
                embed.set_footer(text=f"Cleared by {admin.display_name if admin else 'System'}")

            # Set thumbnail to user avatar
            embed.set_thumbnail(url=member.display_avatar.url)
            embed.timestamp = datetime.utcnow()

            await channel.send(embed=embed)

        except Exception as e:
            print(f"Error logging shift event: {e}")
            import traceback
            traceback.print_exc()

    async def get_duty_roles_for_type(self, type: str) -> tuple:
        """Get duty and break role IDs for a shift type"""
        if type == "Shift FENZ":
            return (FENZ_DUTY_ROLE, FENZ_BREAK_ROLE)
        elif type == "Shift HHStJ":
            return (HHSTJ_DUTY_ROLE, HHSTJ_BREAK_ROLE)
        elif type == "Shift CC":
            return (CC_DUTY_ROLE, CC_BREAK_ROLE)
        return (None, None)

    async def update_duty_roles(self, member: discord.Member, type: str, status: str):
        """Update duty/break roles based on shift status"""
        duty_role_id, break_role_id = await self.get_duty_roles_for_type(type)

        if not duty_role_id:
            return

        guild = member.guild
        duty_role = guild.get_role(duty_role_id)
        break_role = guild.get_role(break_role_id)

        try:
            if status == 'duty':
                # Add duty role, remove break role
                if duty_role and duty_role not in member.roles:
                    await member.add_roles(duty_role)
                if break_role and break_role in member.roles:
                    await member.remove_roles(break_role)
            elif status == 'break':
                # Add break role, remove duty role
                if break_role and break_role not in member.roles:
                    await member.add_roles(break_role)
                if duty_role and duty_role in member.roles:
                    await member.remove_roles(duty_role)
            else:  # 'off'
                # Remove both roles
                roles_to_remove = []
                if duty_role and duty_role in member.roles:
                    roles_to_remove.append(duty_role)
                if break_role and break_role in member.roles:
                    roles_to_remove.append(break_role)
                if roles_to_remove:
                    await member.remove_roles(*roles_to_remove)
        except discord.Forbidden:
            pass
        except Exception as e:
            print(f"Error updating duty roles for {member.display_name}: {e}")

    async def cleanup_stale_shifts(self, bot):
        """Clean up shifts that were active when bot went offline"""
        current_week = WeeklyShiftManager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            # Find all active shifts
            stale_shifts = await conn.fetch('''
                                            SELECT *
                                            FROM shifts
                                            WHERE end_time IS NULL
                                            ''')

            for shift in stale_shifts:
                # Calculate total pause time
                pause_duration = shift.get('pause_duration', 0)
                if shift.get('pause_start'):
                    # Add time from last pause to now
                    pause_duration += (datetime.utcnow() - shift['pause_start']).total_seconds()

                # End the shift at current time
                await conn.execute('''
                                   UPDATE shifts
                                   SET end_time        = $1,
                                       pause_duration  = $2,
                                       pause_start     = NULL,
                                       week_identifier = $3
                                   WHERE id = $4
                                   ''', datetime.utcnow(), pause_duration, current_week, shift['id'])

                # Clean up roles/nicknames
                member = bot.get_guild(shift.get('guild_id')).get_member(shift['discord_user_id'])
                if member:
                    await self.update_nickname_for_shift_status(member, 'off')
                    await self.update_duty_roles(member, shift['type'], 'off')

            if stale_shifts:
                print(f"Cleaned up {len(stale_shifts)} stale shifts on startup")

    async def get_user_types(self, member: discord.Member) -> list:
        """Get all shift types a user is eligible for"""
        types = []
        for role_id, type in typeS.items():
            if any(role.id == role_id for role in member.roles):
                types.append(type)

        # Add additional shift types for specific roles
        for role in member.roles:
            if role.id in ADDITIONAL_SHIFT_ACCESS:
                for type in ADDITIONAL_SHIFT_ACCESS[role.id]:
                    if type not in types:
                        types.append(type)

        return types

    async def get_active_shift(self, user_id: int):
        """Get the user's currently active shift if any"""
        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NULL
                   ORDER BY start_time DESC LIMIT 1''',
                user_id
            )
            return dict(shift) if shift else None

    async def get_shift_statistics(self, user_id: int):
        """Calculate shift statistics for a user (current week only)"""
        current_week = self.weekly_manager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND week_identifier = $2''',  # ✅ Correct - uses week_identifier
                user_id, current_week
            )

            if not shifts:
                return {
                    'count': 0,
                    'total_duration': timedelta(0),
                    'average_duration': timedelta(0)
                }

            total_duration = timedelta(0)
            for shift in shifts:
                duration = shift['end_time'] - shift['start_time']
                if shift.get('pause_duration'):
                    duration -= timedelta(seconds=shift['pause_duration'])
                total_duration += duration

            return {
                'count': len(shifts),
                'total_duration': total_duration,
                'average_duration': total_duration / len(shifts) if len(shifts) > 0 else timedelta(0)
            }

    # Update get_bulk_quota_info to filter by current week
    async def get_bulk_quota_info(self, user_ids: list, guild, type: str = None) -> dict:
        """Fetch quota info for multiple users at once (current week)"""
        current_week = self.weekly_manager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            if type:
                quotas = await conn.fetch(
                    'SELECT role_id, quota_seconds FROM shift_quotas WHERE type = $1',
                    type
                )
            else:
                quotas = await conn.fetch('SELECT role_id, quota_seconds FROM shift_quotas')

            quota_map = {q['role_id']: q['quota_seconds'] for q in quotas}

            if type:
                shifts = await conn.fetch(
                    '''SELECT discord_user_id,
                              SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                  COALESCE(pause_duration, 0)) as total_seconds
                       FROM shifts
                       WHERE discord_user_id = ANY ($1)
                         AND end_time IS NOT NULL
                         AND week_identifier = $2
                         AND type = $3
                       GROUP BY discord_user_id''',
                    user_ids, current_week, type
                )
            else:
                shifts = await conn.fetch(
                    '''SELECT discord_user_id,
                              SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                  COALESCE(pause_duration, 0)) as total_seconds
                       FROM shifts
                       WHERE discord_user_id = ANY ($1)
                         AND end_time IS NOT NULL
                         AND week_identifier = $2
                       GROUP BY discord_user_id''',
                    user_ids, current_week
                )

            user_data = {s['discord_user_id']: s['total_seconds'] for s in shifts}

            results = {}
            for user_id in user_ids:
                member = guild.get_member(user_id)
                if not member:
                    results[user_id] = {'has_quota': False, 'completed': False, 'bypass_type': None}
                    continue

                max_quota = max((quota_map.get(role.id, 0) for role in member.roles), default=0)
                active_seconds = user_data.get(user_id, 0)

                user_role_ids = {role.id for role in member.roles}
                bypass_type = None
                completed = False

                if max_quota > 0:
                    if QUOTA_BYPASS_ROLE in user_role_ids:
                        bypass_type = 'QB'
                        completed = True
                    elif LOA_ROLE in user_role_ids:
                        bypass_type = 'LOA'
                        completed = True
                    elif REDUCED_ACTIVITY_ROLE in user_role_ids:
                        bypass_type = 'RA'
                        modified_quota = max_quota * 0.5
                        completed = (active_seconds / modified_quota >= 1) if modified_quota > 0 else False
                    else:
                        completed = (active_seconds / max_quota >= 1) if max_quota > 0 else False

                results[user_id] = {
                    'has_quota': max_quota > 0,
                    'quota_seconds': max_quota,
                    'active_seconds': active_seconds,
                    'percentage': (active_seconds / max_quota * 100) if max_quota > 0 else 0,
                    'completed': completed,
                    'bypass_type': bypass_type
                }

            return results

    async def get_user_summary(self, user_id: int, member: discord.Member = None):
        """Get all user data in a single database connection"""
        current_week = self.weekly_manager.get_current_week_monday()  # ← ADD THIS

        async with db.pool.acquire() as conn:
            # Get statistics for CURRENT WEEK ONLY
            shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND week_identifier = $2''',  # ✅ Filter by current week
                user_id, current_week
            )

            # Calculate stats
            total_duration = timedelta(0)
            for shift in shifts:
                duration = shift['end_time'] - shift['start_time']
                if shift.get('pause_duration'):
                    duration -= timedelta(seconds=shift['pause_duration'])
                total_duration += duration

            stats = {
                'count': len(shifts),
                'total_duration': total_duration,
                'average_duration': total_duration / len(shifts) if len(shifts) > 0 else timedelta(0)
            }

            # Get last shift (current week only)
            last_shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND week_identifier = $2
                   ORDER BY end_time DESC LIMIT 1''',
                user_id, current_week
            )

            # Get quota info if member provided
            quota_info = None
            if member:
                quota_seconds = 0
                for role in member.roles:
                    result = await conn.fetchrow(
                        'SELECT quota_seconds FROM shift_quotas WHERE role_id = $1',
                        role.id
                    )
                    if result and result['quota_seconds'] > quota_seconds:
                        quota_seconds = result['quota_seconds']

                if quota_seconds > 0:
                    active_seconds = sum(
                        int((shift['end_time'] - shift['start_time']).total_seconds() -
                            shift.get('pause_duration', 0))
                        for shift in shifts
                    )
                    percentage = (active_seconds / quota_seconds) * 100

                    quota_info = {
                        'has_quota': True,
                        'quota_seconds': quota_seconds,
                        'active_seconds': active_seconds,
                        'percentage': percentage,
                        'completed': percentage >= 100
                    }
                else:
                    quota_info = {
                        'has_quota': False,
                        'quota_seconds': 0,
                        'active_seconds': 0,
                        'percentage': 0,
                        'completed': False
                    }

            return {
                'stats': stats,
                'last_shift': dict(last_shift) if last_shift else None,
                'quota_info': quota_info
            }

    async def get_last_shift(self, user_id: int):
        """Get the user's most recent completed shift (current week)"""
        current_week = self.weekly_manager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND week_identifier = $2
                   ORDER BY end_time DESC LIMIT 1''',
                user_id, current_week
            )
            return dict(shift) if shift else None

    async def get_quota_for_role(self, role_id: int, type: str = None) -> int:
        """Get quota in seconds for a role, returns 0 if no quota set"""
        async with db.pool.acquire() as conn:
            if type:
                result = await conn.fetchrow(
                    'SELECT quota_seconds FROM shift_quotas WHERE role_id = $1 AND type = $2',
                    role_id, type
                )
            else:
                result = await conn.fetchrow(
                    'SELECT quota_seconds FROM shift_quotas WHERE role_id = $1',
                    role_id
                )
            return result['quota_seconds'] if result else 0

    async def get_bulk_quota_info(self, user_ids: list, guild, type: str = None) -> dict:
        """Fetch quota info for multiple users at once"""
        async with db.pool.acquire() as conn:
            # Get all role quotas once (with optional type filter)
            if type:
                quotas = await conn.fetch(
                    'SELECT role_id, quota_seconds FROM shift_quotas WHERE type = $1',
                    type
                )
            else:
                quotas = await conn.fetch('SELECT role_id, quota_seconds FROM shift_quotas')

            quota_map = {q['role_id']: q['quota_seconds'] for q in quotas}

            # Get shift data for all users in one query
            if type:
                shifts = await conn.fetch(
                    '''SELECT discord_user_id,
                              SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                  COALESCE(pause_duration, 0)) as total_seconds
                       FROM shifts
                       WHERE discord_user_id = ANY ($1)
                         AND end_time IS NOT NULL
                         AND round_number IS NULL
                         AND type = $2
                       GROUP BY discord_user_id''',
                    user_ids, type
                )
            else:
                shifts = await conn.fetch(
                    '''SELECT discord_user_id,
                              SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                  COALESCE(pause_duration, 0)) as total_seconds
                       FROM shifts
                       WHERE discord_user_id = ANY ($1)
                         AND end_time IS NOT NULL
                         AND round_number IS NULL
                       GROUP BY discord_user_id''',
                    user_ids
                )

            # Build result dict
            user_data = {s['discord_user_id']: s['total_seconds'] for s in shifts}

            results = {}
            for user_id in user_ids:
                member = guild.get_member(user_id)
                if not member:
                    results[user_id] = {'has_quota': False, 'completed': False, 'bypass_type': None}
                    continue

                max_quota = max((quota_map.get(role.id, 0) for role in member.roles), default=0)
                active_seconds = user_data.get(user_id, 0)

                # Check for bypass roles
                user_role_ids = {role.id for role in member.roles}
                bypass_type = None
                completed = False

                if max_quota > 0:
                    if QUOTA_BYPASS_ROLE in user_role_ids:
                        bypass_type = 'QB'
                        completed = True
                    elif LOA_ROLE in user_role_ids:
                        bypass_type = 'LOA'
                        completed = True
                    elif REDUCED_ACTIVITY_ROLE in user_role_ids:
                        bypass_type = 'RA'
                        modified_quota = max_quota * 0.5
                        completed = (active_seconds / modified_quota >= 1) if modified_quota > 0 else False
                    else:
                        completed = (active_seconds / max_quota >= 1) if max_quota > 0 else False

                results[user_id] = {
                    'has_quota': max_quota > 0,
                    'quota_seconds': max_quota,
                    'active_seconds': active_seconds,
                    'percentage': (active_seconds / max_quota * 100) if max_quota > 0 else 0,
                    'completed': completed,
                    'bypass_type': bypass_type
                }

            return results

    async def get_user_quota(self, member: discord.Member, type: str = None) -> int:
        """Get the highest quota from all of a user's roles that have quotas set"""
        max_quota = 0
        for role in member.roles:
            quota = await self.get_quota_for_role(role.id, type)
            if quota > max_quota:
                max_quota = quota
        return max_quota

    async def get_quota_info(self, member: discord.Member, type: str = None) -> dict:
        """Get quota information for a user including percentage and bypass status"""
        quota_seconds = await self.get_user_quota(member, type)

        if quota_seconds == 0:
            return {
                'has_quota': False,
                'quota_seconds': 0,
                'active_seconds': 0,
                'percentage': 0,
                'completed': False,
                'bypass_type': None
            }

        active_seconds = await self.get_total_active_time(member.id, type)

        # Check for quota bypass roles
        user_role_ids = {role.id for role in member.roles}
        bypass_type = None
        modified_quota = quota_seconds

        if QUOTA_BYPASS_ROLE in user_role_ids:
            bypass_type = 'QB'
            completed = True
        elif LOA_ROLE in user_role_ids:
            bypass_type = 'LOA'
            completed = True
        elif REDUCED_ACTIVITY_ROLE in user_role_ids:
            bypass_type = 'RA'
            modified_quota = quota_seconds * 0.5  # 50% of quota
            percentage = (active_seconds / modified_quota) * 100 if modified_quota > 0 else 0
            completed = percentage >= 100
        else:
            percentage = (active_seconds / quota_seconds) * 100
            completed = percentage >= 100

        # Calculate percentage based on original quota for display
        display_percentage = (active_seconds / quota_seconds) * 100 if quota_seconds > 0 else 0

        return {
            'has_quota': True,
            'quota_seconds': quota_seconds,
            'modified_quota_seconds': modified_quota if bypass_type == 'RA' else quota_seconds,
            'active_seconds': active_seconds,
            'percentage': display_percentage,
            'completed': completed,
            'bypass_type': bypass_type
        }

    async def get_total_active_time(self, user_id: int, type: str = None) -> int:
        """Get total active shift time in seconds for current week"""
        current_week = self.weekly_manager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            if type:
                shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND type = $2
                         AND end_time IS NOT NULL
                         AND week_identifier = $3''',
                    user_id, type, current_week
                )
            else:
                shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND end_time IS NOT NULL
                         AND week_identifier = $3''',
                    user_id, current_week
                )

            total_seconds = 0
            for shift in shifts:
                duration = shift['end_time'] - shift['start_time']
                active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))
                total_seconds += active_duration.total_seconds()

            return int(total_seconds)

    async def update_nickname_for_shift_status(self, member: discord.Member, status: str):
        """
        Update member's nickname based on shift status
        status can be: 'duty', 'break', 'off'
        """
        try:
            current_nick = member.nick or member.name

            # Remove any existing prefix
            for prefix in ["DUTY | ", "BRK | "]:
                if current_nick.startswith(prefix):
                    current_nick = current_nick[len(prefix):]
                    break

            # Determine new nickname
            if status == 'duty':
                new_nick = f"DUTY | {current_nick}"
            elif status == 'break':
                new_nick = f"BRK | {current_nick}"
            else:  # 'off'
                new_nick = current_nick

            # Check if it fits Discord's 32 character limit
            if len(new_nick) > 32:
                # If adding prefix makes it too long, keep current nickname
                if status == 'off':
                    # Still try to remove prefix if going off shift
                    new_nick = current_nick
                else:
                    # Don't add prefix if it won't fit
                    return

            # Only update if different
            if member.nick != new_nick:
                await member.edit(nick=new_nick)

        except discord.Forbidden:
            # Can't edit nickname (permissions or higher role)
            pass
        except Exception as e:
            print(f"Error updating nickname for {member.display_name}: {e}")

    def format_duration(self, td: timedelta) -> str:
        """Format a timedelta into a readable string"""
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        parts = []
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if seconds > 0 or not parts:
            parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

        return ", ".join(parts)

    @shift_group.command(name="test_weekly_reset", description="[ADMIN] Test weekly reset (dev only)")
    async def test_weekly_reset(self, interaction: discord.Interaction):
        """Test command to manually trigger weekly reset"""
        await interaction.response.defer(ephemeral=True)

        if not self.has_super_admin_permission(interaction.user):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission for this command.",
                ephemeral=True
            )
            return

        try:
            await self.weekly_manager.generate_weekly_report()
            await self.weekly_manager.force_end_active_shifts()
            await self.weekly_manager.archive_and_cleanup_shifts()

            await interaction.followup.send(
                "<:Accepted:1426930333789585509> Weekly reset test completed!",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @shift_group.command(name="quota", description="View or set shift quotas")
    @app_commands.describe(
        action="View your quota or set quota for roles",
        roles="The role(s) to set quota for (admin only)",
        hours="Hours for the quota",
        minutes="Minutes for the quota",
        type="The shift type to set quota for"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="View My Quota", value="view"),
        app_commands.Choice(name="Set Role Quota", value="set"),
        app_commands.Choice(name="Remove Role Quota", value="remove"),
        app_commands.Choice(name="View All Quotas", value="view_all")
    ],
        type=[
            app_commands.Choice(name="Shift FENZ", value="Shift FENZ"),
            app_commands.Choice(name="Shift HHStJ", value="Shift HHStJ"),
            app_commands.Choice(name="Shift CC", value="Shift CC")
        ]
    )
    async def shift_quota(
            self,
            interaction: discord.Interaction,
            action: app_commands.Choice[str],
            roles: str = None,
            hours: int = 0,
            minutes: int = 0,
            type: app_commands.Choice[str] = None
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            if action.value == "view":
                # View user's own quota
                quota_info = await self.get_quota_info(interaction.user, type.value if type else None)

                if not quota_info['has_quota']:

                    embed = discord.Embed(
                        title="<:Search:1434957367505719457> Your Quota",
                        color=discord.Color(0x000000)
                    )

                    embed.set_author(
                        name=f"{interaction.guild.name}",
                        icon_url=interaction.guild.icon.url if interaction.guild.icon else None
                    )

                    await interaction.followup.send(embed=embed, epheremal=True)
                    return

                embed = discord.Embed(
                    title="<:Search:1434957367505719457> Your Quota",
                    color=discord.Color(0x000000)
                )

                embed.set_author(
                    name="Shift Management",
                    icon_url=interaction.user.display_avatar.url
                )

                if quota_info['bypass_type']:
                    if quota_info['bypass_type'] == 'RA':
                        status_text = f"<:Accepted:1426930333789585509> **{quota_info['percentage']:.1f}%** (RA - 50% Required)"
                    else:
                        status_text = f"<:Accepted:1426930333789585509> **100%** ({quota_info['bypass_type']} Bypass)"
                else:
                    status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                        'completed'] else "<:Denied:1426930694633816248>"
                    status_text = f"{status_emoji} **{quota_info['percentage']:.1f}%**"

                embed.description = (
                    f"{status_text}\n"
                    f"**Required:** {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}\n"
                    f"**Completed:** {self.format_duration(timedelta(seconds=quota_info['active_seconds']))}"
                )

                if type:
                    embed.set_footer(text=f"Shift Type: {type.value}")

                await interaction.followup.send(embed=embed, ephemeral=True)

            elif action.value == "set":
                # Check admin permission
                if not any(role_check.id in QUOTA_ADMIN_ROLES for role_check in interaction.user.roles):
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> You don't have permission to set quotas.",
                        ephemeral=True
                    )
                    return

                if not roles:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Please specify role(s) to set quota for.",
                        ephemeral=True
                    )
                    return

                if not type:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Please specify a shift type.",
                        ephemeral=True
                    )
                    return

                # Parse roles
                role_ids = []
                for role_str in roles.split(','):
                    role_str = role_str.strip().replace('<@&', '').replace('>', '').replace('@', '')
                    if not role_str:
                        continue
                    try:
                        role_id = int(role_str)
                        role = interaction.guild.get_role(role_id)
                        if role:
                            role_ids.append(role_id)
                        else:
                            print(f"Warning: Role ID {role_id} not found in guild")
                    except ValueError:
                        print(f"Warning: Could not parse role ID from: {role_str}")
                        continue

                if not role_ids:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> No valid roles provided.",
                        ephemeral=True
                    )
                    return

                # Calculate total seconds
                total_seconds = (hours * 3600) + (minutes * 60)

                if total_seconds < 0:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Quota cannot be negative.",
                        ephemeral=True
                    )
                    return

                # Allow 0 as a valid quota (means no requirement for this role)
                if total_seconds == 0:
                    # Confirm the user wants to set a 0 quota
                    if not (hours == 0 and minutes == 0):
                        await interaction.followup.send(
                            "<:Denied:1426930694633816248> Invalid time values.",
                            ephemeral=True
                        )
                        return

                # Check for conflicts and save to database
                async with db.pool.acquire() as conn:
                    conflicts = []
                    for role_id in role_ids:
                        existing = await conn.fetchrow(
                            'SELECT quota_seconds FROM shift_quotas WHERE role_id = $1 AND type = $2',
                            role_id, type.value
                        )

                        if existing:
                            role = interaction.guild.get_role(role_id)
                            conflicts.append(
                                f"{role.mention} (currently: {self.format_duration(timedelta(seconds=existing['quota_seconds']))})")

                    if conflicts:
                        # Show confirmation for overwriting
                        view = QuotaConflictView(
                            self, interaction.user, role_ids, total_seconds, type.value
                        )
                        await interaction.followup.send(
                            f"**The following roles already have quotas set for {type.value}:**\n" +
                            "\n".join(conflicts) +
                            f"\n\nOverwrite with **{self.format_duration(timedelta(seconds=total_seconds))}**?",
                            view=view,
                            ephemeral=True
                        )
                        return

                    # No conflicts, set quotas directly
                    role_mentions = []
                    for role_id in role_ids:
                        await conn.execute(
                            '''INSERT INTO shift_quotas (role_id, quota_seconds, type)
                               VALUES ($1, $2, $3) ON CONFLICT (role_id, type) 
                               DO
                            UPDATE SET quota_seconds = $2''',
                            role_id, total_seconds, type.value
                        )
                        role = interaction.guild.get_role(role_id)
                        if role:
                            role_mentions.append(role.mention)

                    await interaction.followup.send(
                        f"<:Accepted:1426930333789585509> Set quota for {', '.join(role_mentions)} to {self.format_duration(timedelta(seconds=total_seconds))} ({type.value})",
                        ephemeral=True
                    )

            elif action.value == "remove":
                # Check admin permission
                if not any(role_check.id in QUOTA_ADMIN_ROLES for role_check in interaction.user.roles):
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> You don't have permission to remove quotas.",
                        ephemeral=True
                    )
                    return

                if not roles:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Please specify role(s) to remove quota from.",
                        ephemeral=True
                    )
                    return

                if not type:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Please specify a shift type.",
                        ephemeral=True
                    )
                    return

                # Parse roles
                role_ids = []
                for role_str in roles.split(','):
                    role_str = role_str.strip().replace('<@&', '').replace('>', '').replace('@', '')
                    if not role_str:
                        continue
                    try:
                        role_id = int(role_str)
                        role = interaction.guild.get_role(role_id)
                        if role:
                            role_ids.append(role_id)
                    except ValueError:
                        continue

                if not role_ids:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> No valid roles provided.",
                        ephemeral=True
                    )
                    return

                # Delete from database
                async with db.pool.acquire() as conn:
                    role_mentions = []
                    removed_count = 0

                    for role_id in role_ids:
                        result = await conn.execute(
                            'DELETE FROM shift_quotas WHERE role_id = $1 AND type = $2',
                            role_id, type.value
                        )

                        if result != "DELETE 0":
                            removed_count += 1
                            role = interaction.guild.get_role(role_id)
                            if role:
                                role_mentions.append(role.mention)

                if removed_count == 0:
                    await interaction.followup.send(
                        f"<:Denied:1426930694633816248> No quotas found for the specified roles in {type.value}.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"<:Accepted:1426930333789585509> Removed quota for {', '.join(role_mentions)} from {type.value}",
                        ephemeral=True
                    )

            elif action.value == "view_all":
                # Check admin permission
                if not any(role_check.id in QUOTA_ADMIN_ROLES for role_check in interaction.user.roles):
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> You don't have permission to view quotas.",
                        ephemeral=True
                    )
                    return

                # Fetch all quotas
                async with db.pool.acquire() as conn:
                    if type:
                        quotas = await conn.fetch(
                            'SELECT role_id, quota_seconds, type FROM shift_quotas WHERE type = $1 ORDER BY type, quota_seconds DESC',
                            type.value
                        )
                    else:
                        quotas = await conn.fetch(
                            'SELECT role_id, quota_seconds, type FROM shift_quotas ORDER BY type, quota_seconds DESC'
                        )

                if not quotas:
                    await interaction.followup.send(
                        "No shift quotas have been set.",
                        ephemeral=True
                    )
                    return

                # Create embed
                embed = discord.Embed(
                    title="<:Search:1434957367505719457> Role Quotas",
                    color=discord.Color(0x000000)
                )

                # Group by shift type
                quotas_by_type = {}
                for quota in quotas:
                    type_name = quota['type']
                    if type_name not in quotas_by_type:
                        quotas_by_type[type_name] = []
                    quotas_by_type[type_name].append(quota)

                # Add fields for each shift type
                for type_name, type_quotas in quotas_by_type.items():
                    quota_lines = []
                    for quota in type_quotas:
                        quota_role = interaction.guild.get_role(quota['role_id'])
                        if quota_role:
                            quota_lines.append(
                                f"{quota_role.mention} • {self.format_duration(timedelta(seconds=quota['quota_seconds']))}"
                            )

                    if quota_lines:
                        embed.add_field(
                            name=f"**{type_name}**",
                            value="\n".join(quota_lines),
                            inline=False
                        )

                if not embed.fields:
                    embed.description = "No valid roles found."

                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            print(f"Error in shift_quota command: {e}")
            import traceback
            traceback.print_exc()

            try:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> An error occurred: {str(e)}",
                    ephemeral=True
                )
            except:
                print("Could not send error message to user")

    @shift_group.command(name="leaderboard", description="View shift leaderboard")
    @app_commands.describe(
        type="Filter by shift type (REQUIRED)",
        wave="View current week or specific wave number (e.g., 1, 2, 3)"  # ← Changed description
    )
    @app_commands.choices(
        type=[
            app_commands.Choice(name="Shift FENZ", value="Shift FENZ"),
            app_commands.Choice(name="Shift HHStJ", value="Shift HHStJ"),
            app_commands.Choice(name="Shift CC", value="Shift CC")
        ]
        # ← REMOVED the week choices completely
    )
    async def shift_leaderboard(
            self,
            interaction: discord.Interaction,
            type: app_commands.Choice[str],
            wave: int = None  # ← Changed to integer parameter
    ):
        await interaction.response.defer()

        # Check permission (keep existing code)
        required_roles = {
            "Shift FENZ": [FENZ_ROLE_ID] + ADMIN_ROLES,
            "Shift HHStJ": [HHSTJ_ROLE_ID] + ADMIN_ROLES,
            "Shift CC": [CC_ROLE_ID] + ADMIN_ROLES
        }

        user_role_ids = {role.id for role in interaction.user.roles}
        if not user_role_ids & set(required_roles.get(type.value, [])):
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> You don't have permission to view the {type.value} leaderboard.",
                ephemeral=True
            )
            return

        try:
            async with db.pool.acquire() as conn:
                if wave is not None:
                    # View specific wave (historical data)
                    query = '''SELECT discord_user_id,
                                      discord_username,
                                      SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                          COALESCE(pause_duration, 0)) as total_seconds
                               FROM shifts
                               WHERE end_time IS NOT NULL
                                 AND wave_number = $1
                                 AND type = $2
                               GROUP BY discord_user_id, discord_username
                               ORDER BY total_seconds DESC LIMIT 25'''
                    results = await conn.fetch(query, wave, type.value)

                    # Get week dates for this wave
                    week_start = await conn.fetchval(
                        'SELECT MIN(week_identifier) FROM shifts WHERE wave_number = $1',
                        wave
                    )

                    if week_start:
                        week_end = week_start + timedelta(days=6)
                        wave_label = f"Wave {wave} ({week_start.strftime('%d %b')} - {week_end.strftime('%d %b %Y')})"
                    else:
                        wave_label = f"Wave {wave}"
                else:
                    # View current week (no wave assigned yet)
                    current_week = self.weekly_manager.get_current_week_monday()
                    query = '''SELECT discord_user_id,
                                      discord_username,
                                      SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                          COALESCE(pause_duration, 0)) as total_seconds
                               FROM shifts
                               WHERE end_time IS NOT NULL
                                 AND week_identifier = $1
                                 AND wave_number IS NULL
                                 AND type = $2
                               GROUP BY discord_user_id, discord_username
                               ORDER BY total_seconds DESC LIMIT 25'''
                    results = await conn.fetch(query, current_week, type.value)
                    wave_label = "Current Wave"

            if not results:
                if wave is not None:
                    embed = discord.Embed(
                        title="Shift Leaderboard",
                        description=f"No shift data is available for {wave_label}",
                        color=discord.Color(0x000000)
                    )
                    embed.set_author(
                        name=f"{interaction.guild.name}: {wave_label}",
                        icon_url=interaction.guild.icon.url if interaction.guild.icon else None
                    )
                    await interaction.edit_original_response(embed=embed)

                if not wave_exists:
                    await interaction.followup.send(
                        f"<:Denied:1426930694633816248> Wave {wave} does not exist.",
                        ephemeral=True
                    )
                    return

            user_ids = [row['discord_user_id'] for row in results]
            quota_infos = await self.get_bulk_quota_info(user_ids, interaction.guild, type.value)

            embed = discord.Embed(
                title="Shift Leaderboard",
                description="",
                color=discord.Color(0x000000)
            )
            embed.set_author(
                name=f"{interaction.guild.name}: {wave_label}",
                icon_url=interaction.guild.icon.url if interaction.guild.icon else None
            )

            leaderboard_lines = []

            for idx, row in enumerate(results, 1):
                member = interaction.guild.get_member(row['discord_user_id'])
                if not member:
                    continue

                quota_info = quota_infos.get(row['discord_user_id'], {'has_quota': False, 'bypass_type': None})
                quota_status = ""
                if quota_info['has_quota']:
                    if quota_info['bypass_type']:
                        quota_status = f" • <:Accepted:1426930333789585509> ({quota_info['bypass_type']})"
                    elif quota_info['completed']:
                        quota_status = f" • <:Accepted:1426930333789585509>"
                    else:
                        quota_status = f" • <:Denied:1426930694633816248>"

                time_str = self.format_duration(timedelta(seconds=int(row['total_seconds'])))
                leaderboard_lines.append(f"`{idx}.` {member.mention} • {time_str}{quota_status}")

            if leaderboard_lines:
                embed.description = "\n".join(leaderboard_lines)

            embed.set_footer(text=f"Shift Type: {type.value}")

            await interaction.edit_original_response(embed=embed)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    '''@shift_group.command(name="reset", description="[ADMIN] Reset shifts for a wave")
    @app_commands.describe(
        roles="Roles to reset shifts for (comma-separated role IDs or @mentions)",
        confirm="Type CONFIRM to proceed"
    )
    async def shift_reset(
            self,
            interaction: discord.Interaction,
            roles: str
    ):
        await interaction.response.defer()

        # Check permission
        if not any(role.id in RESET_ROLES for role in interaction.user.roles):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission to reset shifts.",
                ephemeral=True
            )
            return

        try:
            # Parse roles
            role_ids = []
            for role_str in roles.split(','):
                role_str = role_str.strip().replace('<@&', '').replace('>', '')
                try:
                    role_id = int(role_str)
                    role_ids.append(role_id)
                except ValueError:
                    continue

            if not role_ids:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> No valid roles provided.",
                    ephemeral=True
                )
                return

            # Get affected user count
            affected_users = set()
            role_names = []
            for role_id in role_ids:
                role = interaction.guild.get_role(role_id)
                if role:
                    affected_users.update([member.id for member in role.members])
                    role_names.append(role.name)

            # Show confirmation view
            embed = discord.Embed(
                title="**Reset Shifts**",
                description=f"Are you sure you want to archive all current shifts and create a new wave?\n\n"
                            f"**Affected Roles:** {', '.join(role_names)}\n"
                            f"**Affected Users:** {len(affected_users)}\n\n"
                            f"This cannot be undone.",
                color=discord.Color.red()
            )

            view = ResetConfirmView(self, interaction.user, role_ids, role_names, len(affected_users))
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()'''

    @shift_group.command(name="manage", description="Manage your shifts")
    @app_commands.describe(type="Select your shift type")
    @app_commands.choices(type=[
        app_commands.Choice(name="Shift FENZ", value="Shift FENZ"),
        app_commands.Choice(name="Shift HHStJ", value="Shift HHStJ"),
        app_commands.Choice(name="Shift CC", value="Shift CC")
    ])
    async def shift_manage(self, interaction: discord.Interaction, type: app_commands.Choice[str]):
        """Main shift management command"""
        await interaction.response.defer()

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Check what shift types the user has access to
            types = await self.get_user_types(interaction.user)

            if not types:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You don't have any shift roles (FENZ, HHStJ, or CC).",
                    ephemeral=True
                )
                return

            # Check if user has an active shift
            active_shift = await self.get_active_shift(interaction.user.id)

            if active_shift:
                # User has an active shift - show shift status
                await self.show_active_shift_panel(interaction, active_shift)
            else:
                # No active shift - show statistics and start option
                await self.show_shift_statistics_panel(interaction, types, show_last_shift=False)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @shift_group.command(name="active", description="View all active shifts")
    async def shift_active(self, interaction: discord.Interaction):
        """Show all currently active shifts categorized by type"""
        await interaction.response.defer()

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Get all active shifts
            async with db.pool.acquire() as conn:
                active_shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE end_time IS NULL
                       ORDER BY type, start_time'''
                )

                # Move this INSIDE the context manager
                if active_shifts:
                    user_ids = [shift['discord_user_id'] for shift in active_shifts]
                    callsigns = await conn.fetch(
                        'SELECT discord_user_id, callsign, fenz_prefix FROM callsigns WHERE discord_user_id = ANY($1)',
                        user_ids
                    )
                    callsign_map = {c['discord_user_id']: c for c in callsigns}
                else:
                    callsign_map = {}

            if not active_shifts:

                embed = discord.Embed(
                    title="**<:Clock:1434949269554597978> Active Shifts**",
                    description='No active shfts.',
                    color=discord.Color(0xffffff)
                )

                embed.set_author(
                    name=interaction.guild.name,  # Use guild name instead
                    icon_url=interaction.guild.icon.url if interaction.guild.icon else None
                )

                await interaction.followup.send(embed=embed)


            # Categorize by shift type
            shifts_by_type = {}
            for shift in active_shifts:
                type = shift['type']
                if type not in shifts_by_type:
                    shifts_by_type[type] = []
                shifts_by_type[type].append(dict(shift))

            # Create embed
            embed = discord.Embed(
                title="**<:Clock:1434949269554597978> Active Shifts**",
                description='',
                color=discord.Color(0xffffff)
            )

            embed.set_author(
                name=interaction.guild.name,  # Use guild name instead
                icon_url=interaction.guild.icon.url if interaction.guild.icon else None
            )

            # Add each shift type section
            for type in ["Shift FENZ", "Shift HHStJ", "Shift CC"]:
                if type not in shifts_by_type:
                    continue

                shifts = shifts_by_type[type]
                shift_lines = []

                for idx, shift in enumerate(shifts, 1):
                    member = interaction.guild.get_member(shift['discord_user_id'])
                    if not member:
                        continue

                    # Calculate shift duration
                    shift_duration = datetime.utcnow() - shift['start_time']

                    # Calculate current break duration if on break
                    current_break = timedelta(0)
                    is_on_break = shift.get('pause_start') is not None

                    if is_on_break:
                        current_break = datetime.utcnow() - shift['pause_start']
                        # Subtract total previous pause time from shift duration
                        shift_duration -= timedelta(seconds=shift.get('pause_duration', 0))
                    else:
                        # Subtract all pause time
                        shift_duration -= timedelta(seconds=shift.get('pause_duration', 0))

                    # Format shift duration
                    shift_time = self.format_duration_short(shift_duration)

                    # Get display name or username
                    display_name = member.display_name
                    callsign_row = callsign_map.get(member.id)

                    if callsign_row:
                        if callsign_row['fenz_prefix']:
                            display_name = f"@{callsign_row['fenz_prefix']}-{callsign_row['callsign']}"
                        else:
                            display_name = f"@{callsign_row['callsign']}"

                    # Build line
                    if is_on_break:
                        break_time = self.format_duration_short(current_break)
                        # Bold if break is over 20 minutes
                        if current_break.total_seconds() > 1200:  # 20 minutes = 1200 seconds
                            shift_lines.append(f"`{idx}.` {display_name} | *{break_time}*")
                        else:
                            shift_lines.append(f"`{idx}.` {display_name} | *{break_time}*")
                    else:
                        member = interaction.guild.get_member(shift['discord_user_id'])
                        if member:
                            shift_lines.append(f"`{idx}.` {member.mention} • {shift_time}")

                if shift_lines:
                    embed.add_field(
                        name=f"**{type.replace('Shift ', '')} Type**",
                        value="\n".join(shift_lines),
                        inline=False
                    )

            await interaction.edit_original_response(embed=embed)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    # Admin command
    @shift_group.command(name="admin", description="[ADMIN] Manage shifts for users")
    @app_commands.describe(
        user="The user to manage shifts for",
        type="The shift type (REQUIRED)"
    )
    @app_commands.choices(type=[
        app_commands.Choice(name="Shift FENZ", value="Shift FENZ"),
        app_commands.Choice(name="Shift HHStJ", value="Shift HHStJ"),
        app_commands.Choice(name="Shift CC", value="Shift CC")
    ])
    async def shift_admin(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            type: app_commands.Choice[str]
    ):
        """Admin shift management command"""
        await interaction.response.defer()

        # Check admin permission
        if not self.has_admin_permission(interaction.user):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission to use this command.",
                ephemeral=True
            )
            return

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Get user's shift types
            user_types = await self.get_user_types(user)

            if not user_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} doesn't have any shift roles.",
                    ephemeral=True
                )
                return

            # Validate provided shift type
            if type.value not in user_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} doesn't have access to {type.value}.",
                    ephemeral=True
                )
                return

            # Show admin control panel
            await self.show_admin_shift_panel(interaction, user, type.value)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    def format_duration_short(self, td: timedelta) -> str:
        """Format a timedelta into a short readable string"""
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    async def show_shift_statistics_panel(self, interaction: discord.Interaction, types: list,
                                          show_last_shift: bool = False):
        """Show the all-time statistics panel with start button

        Args:
            show_last_shift: Only show last shift info if True (after ending a shift in current session)
        """
        summary = await self.get_user_summary(interaction.user.id, interaction.user)
        stats = summary['stats']
        last_shift = summary['last_shift']
        quota_info = summary['quota_info']

        embed = discord.Embed(
            title="<:Checklist:1434948670226432171> **All Time Information**",
            description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.format_duration(stats['total_duration'])}\n**Average Duration:** {self.format_duration(stats['average_duration'])}",
            color=discord.Color(0x000000)
        )
        embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)

        # Add quota info if available
        if quota_info and quota_info['has_quota']:
            if quota_info['bypass_type']:
                if quota_info['bypass_type'] == 'RA':
                    status_text = f"<:Accepted:1426930333789585509> **{quota_info['percentage']:.1f}%** (RA - 50% Required)"
                else:
                    status_text = f"<:Accepted:1426930333789585509> **100%** ({quota_info['bypass_type']} Bypass)"
            else:
                status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                    'completed'] else "<:Denied:1426930694633816248>"
                status_text = f"{status_emoji} **{quota_info['percentage']:.1f}%**"

            embed.add_field(
                name="Quota Progress",
                value=f"> {status_text} of {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                inline=False
            )

        # ONLY show last shift if explicitly requested (after ending a shift)
        if show_last_shift and last_shift:
            last_duration = last_shift['end_time'] - last_shift['start_time']
            active_duration = last_duration - timedelta(seconds=last_shift.get('pause_duration', 0))
            pause_duration = last_shift.get('pause_duration', 0)

            embed.add_field(
                name="<:Clock:1434949269554597978> Last Shift",
                value=f"**Status:** <:Offline:1434951694319620197> Ended\n"
                      f"**Ended:** <t:{int(last_shift['end_time'].timestamp())}:R>\n"
                      f"**Total Time:** {self.format_duration(active_duration)}\n"
                      f"**Break Time:** {self.format_duration(timedelta(seconds=pause_duration))}",
                inline=False
            )
            embed.set_footer(text=f"Shift Type: {last_shift['type']}")

        # Create view with only Start button
        view = ShiftStartView(self, interaction.user, types)
        message = await interaction.edit_original_response(embed=embed, view=view)
        view.message = message

    async def show_active_shift_panel(self, interaction: discord.Interaction, shift: dict):
        """Show the active shift panel"""

        is_on_break = shift.get('pause_start') is not None

        if is_on_break:
            # On Break status
            embed = discord.Embed(
                title="Shift Management",
                description="**Break Started**",
                color=discord.Color.gold()
            )

            embed.add_field(
                name="Current Shift",
                value=f"**Status:** <:Idle:1434949872968273940> On Break\n"
                      f"**Started:** <t:{int(shift['start_time'].timestamp())}:R>\n"
                      f"**Break Started:** <t:{int(shift['pause_start'].timestamp())}:R>",
                inline=False
            )

            view = ShiftBreakView(self, interaction.user, shift)
        else:
            # On Shift status
            embed = discord.Embed(
                title="Shift Management",
                description="**Shift Started**",
                color=discord.Color.green()
            )

            embed.add_field(
                name="Current Shift",
                value=f"**Status:** <:Online:1434949591303983194> On Shift\n"
                      f"**Started:** <t:{int(shift['start_time'].timestamp())}:R>",
                inline=False
            )

            view = ShiftActiveView(self, interaction.user, shift)

        embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)
        embed.set_footer(text=f"Shift Type: {shift['type']}")

        await interaction.edit_original_response(embed=embed, view=view)

    async def end_shift_and_show_summary(self, interaction: discord.Interaction, shift: dict):
        """End the shift and show summary with statistics"""
        # If paused, add final pause duration
        pause_duration = shift.get('pause_duration', 0)
        if shift.get('pause_start'):
            pause_duration += (datetime.utcnow() - shift['pause_start']).total_seconds()

        # End the shift
        async with db.pool.acquire() as conn:
            await conn.execute(
                '''UPDATE shifts
                   SET end_time       = $1,
                       pause_duration = $2,
                       pause_start    = NULL
                   WHERE id = $3''',
                datetime.utcnow(), pause_duration, shift['id']
            )

        # Get updated statistics
        stats = await self.get_shift_statistics(interaction.user.id)

        # Calculate this shift's duration
        total_duration = datetime.utcnow() - shift['start_time']
        active_duration = total_duration - timedelta(seconds=pause_duration)

        # Create summary embed
        embed = discord.Embed(
            title="<:Checklist:1434948670226432171> **All Time Information**",
            description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.format_duration(stats['total_duration'])}\n**Average Duration:** {self.format_duration(stats['average_duration'])}",
            color=discord.Color(0x000000)
        )
        embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)

        # Add quota info if available
        member = interaction.guild.get_member(interaction.user.id)
        if member:
            quota_info = await self.get_quota_info(member)
            if quota_info['has_quota']:
                status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                    'completed'] else "<:Denied:1426930694633816248>"
                if quota_info and quota_info['has_quota']:
                    if quota_info['bypass_type']:
                        if quota_info['bypass_type'] == 'RA':
                            status_text = f"<:Accepted:1426930333789585509> **{quota_info['percentage']:.1f}%** (RA - 50% Required)"
                        else:
                            status_text = f"<:Accepted:1426930333789585509> **100%** ({quota_info['bypass_type']} Bypass)"
                    else:
                        status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                            'completed'] else "<:Denied:1426930694633816248>"
                        status_text = f"{status_emoji} **{quota_info['percentage']:.1f}%**"

                    embed.add_field(
                        name="Quota Progress",
                        value=f"> {status_text} of {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                        inline=False
                    )

        embed.set_footer(text=f"Shift Type: {shift['type']}")

        # Create view with Start button for next shift
        types = await self.get_user_types(interaction.user)
        view = ShiftStartView(self, interaction.user, types)
        await interaction.edit_original_response(embed=embed, view=view)

    # Fix for show_admin_shift_panel method (around line 2220-2280)
    # Replace the existing show_admin_shift_panel method with this corrected version:

    async def show_admin_shift_panel(self, interaction: discord.Interaction, user: discord.Member, type: str):
        """Show admin control panel for managing user's shift"""
        # Get active shift for this user
        active_shift = await self.get_active_shift(user.id)

        # Get statistics
        stats = await self.get_shift_statistics(user.id)

        embed = discord.Embed(
            title="<:Checklist:1434948670226432171> **All Time Information**",
            description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.format_duration(stats['total_duration'])}\n**Average Duration:** {self.format_duration(stats['average_duration'])}",
            color=discord.Color(0x000000)
        )
        embed.set_author(
            name=f"Shift Management: {user.display_name}",
            icon_url=user.display_avatar.url
        )

        # Add quota info if available
        member = interaction.guild.get_member(user.id)
        if member:
            quota_info = await self.get_quota_info(user)
            if quota_info['has_quota']:
                if quota_info['bypass_type']:
                    if quota_info['bypass_type'] == 'RA':
                        status_text = f"<:Accepted:1426930333789585509> **{quota_info['percentage']:.1f}%** (RA - 50% Required)"
                    else:
                        status_text = f"<:Accepted:1426930333789585509> **100%** ({quota_info['bypass_type']} Bypass)"
                else:
                    status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                        'completed'] else "<:Denied:1426930694633816248>"
                    status_text = f"{status_emoji} **{quota_info['percentage']:.1f}%**"

                embed.add_field(
                    name="Quota Progress",
                    value=f"> {status_text} of {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                    inline=False
                )

        # Show shift status if active
        if active_shift:
            is_on_break = active_shift.get('pause_start') is not None
            status = "<:Idle:1434949872968273940> On Break" if is_on_break else "<:Online:1434949591303983194> On Shift"

            shift_info = f"**Status:** {status}\n"
            shift_info += f"**Started:** <t:{int(active_shift['start_time'].timestamp())}:R>"

            if is_on_break:
                shift_info += f"\n**Break Started:** <t:{int(active_shift['pause_start'].timestamp())}:R>"

            embed.add_field(
                name="Current Shift",
                value=shift_info,
                inline=False
            )

        embed.set_footer(text=f"Shift Type: {type}")

        view = AdminShiftControlView(self, interaction.user, user, type, active_shift)
        await interaction.edit_original_response(embed=embed, view=view)

class QuotaConflictView(discord.ui.View):
    """Confirmation view for overwriting existing quotas"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, role_ids: list,
                 quota_seconds: int, type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.quota_seconds = quota_seconds
        self.type = type
        self.message = None

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your confirmation!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Overwrite", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            async with db.pool.acquire() as conn:
                role_mentions = []
                for role_id in self.role_ids:
                    await conn.execute(
                        '''INSERT INTO shift_quotas (role_id, quota_seconds, type)
                           VALUES ($1, $2, $3) ON CONFLICT (role_id, type) 
                           DO
                        UPDATE SET quota_seconds = $2''',
                        role_id, self.quota_seconds, self.type
                    )
                    role = interaction.guild.get_role(role_id)
                    if role:
                        role_mentions.append(role.mention)

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Updated quota for {', '.join(role_mentions)} to {self.cog.format_duration(timedelta(seconds=self.quota_seconds))} ({self.type})",
                ephemeral=True
            )

            # Disable buttons
            for item in self.children:
                item.disabled = True
            try:
                await interaction.message.edit(view=self)
            except discord.NotFound:
                pass  # Message was deleted, ignore
            except discord.HTTPException:
                pass
            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("Cancelled.", ephemeral=True)

        for item in self.children:
            item.disabled = True
        try:
            await interaction.message.edit(view=self)
        except discord.NotFound:
            pass  # Message was deleted, ignore
        except discord.HTTPException:
            pass
        self.stop()

class QuotaConflictView(discord.ui.View):
    """Confirmation view for overwriting existing quotas"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, role_ids: list,
                 quota_seconds: int, type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.quota_seconds = quota_seconds
        self.type = type
        self.message = None

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your confirmation!",
                ephemeral=True
            )
            return False
        return True


    @discord.ui.button(label="Confirm Overwrite", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            async with db.pool.acquire() as conn:
                role_mentions = []
                for role_id in self.role_ids:
                    await conn.execute(
                        '''INSERT INTO shift_quotas (role_id, quota_seconds, type)
                           VALUES ($1, $2, $3) ON CONFLICT (role_id, type) 
                               DO
                        UPDATE SET quota_seconds = $2''',
                        role_id, self.quota_seconds, self.type
                    )
                    role = interaction.guild.get_role(role_id)
                    if role:
                        role_mentions.append(role.mention)

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Updated quota for {', '.join(role_mentions)} to {self.cog.format_duration(timedelta(seconds=self.quota_seconds))} ({self.type})",
                ephemeral=True
            )

            # Disable buttons and edit only if the message still exists
            for item in self.children:
                item.disabled = True

            try:
                await interaction.message.edit(view=self)
            except discord.NotFound:
                pass  # Message was deleted, ignore
            except discord.HTTPException:
                pass  # Other HTTP error, ignore

            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("Cancelled.", ephemeral=True)

        for item in self.children:
            item.disabled = True

        try:
            await interaction.message.edit(view=self)
        except discord.NotFound:
            pass  # Message was deleted, ignore
        except discord.HTTPException:
            pass  # Other HTTP error, ignore

        self.stop()

class ShiftStartView(discord.ui.View):
    """View shown when no shift is active - only Start button"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, types: list):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.types = types
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Start", style=discord.ButtonStyle.success, emoji="<:Play:1434957147829047467>")
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        try:
            # Only one shift type, start it directly
            await self.start_shift(interaction, self.types[0])

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def start_shift(self, interaction: discord.Interaction, type: str):
        """Start a new shift"""
        current_week = WeeklyShiftManager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO shifts
                   (discord_user_id, discord_username, type, start_time, pause_duration, week_identifier, guild_id)
                   VALUES ($1, $2, $3, $4, 0, $5, $6)''',
                self.user.id, str(self.user), type, datetime.utcnow(),
                current_week, interaction.guild.id
            )

        await self.cog.update_nickname_for_shift_status(self.user, 'duty')
        await self.cog.update_duty_roles(self.user, type, 'duty')

        shift = await self.cog.get_active_shift(self.user.id)

        # 🆕 LOG THE SHIFT START
        await self.cog.log_shift_event(
            interaction.guild,
            'start',
            self.user,
            shift
        )

        # Build the embed directly instead of calling show_active_shift_panel
        embed = discord.Embed(
            title="**Shift Started**",
            color=discord.Color(0x57f288)
        )

        embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)

        embed.add_field(
            name="<:Clock:1434949269554597978> Current Shift",
            value=f"**Status:** <:Online:1434949591303983194> On Shift\n"
                  f"**Started:** <t:{int(shift['start_time'].timestamp())}:R>",
            inline=False
        )

        embed.set_footer(text=f"Shift Type: {shift['type']}")

        # Create the active view
        view = ShiftActiveView(self.cog, self.user, shift)

        # Edit the original message instead of creating a new one
        await interaction.edit_original_response(embed=embed, view=view)

class ShiftActiveView(discord.ui.View):
    """View shown when shift is active (on shift) - Pause and End buttons"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, shift: dict):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.shift = shift
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Pause", style=discord.ButtonStyle.primary, emoji="<:Pause:1434982402593390632>")
    async def pause_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start = $1
                       WHERE id = $2''',
                    datetime.utcnow(), self.shift['id']
                )

            await self.cog.update_nickname_for_shift_status(self.user, 'break')
            await self.cog.update_duty_roles(self.user, self.shift['type'], 'break')

            updated_shift = await self.cog.get_active_shift(self.user.id)

            # 🆕 LOG THE PAUSE
            await self.cog.log_shift_event(
                interaction.guild,
                'pause',
                self.user,
                updated_shift
            )

            # Build break panel embed directly
            embed = discord.Embed(
                title="**Break Started**",
                color=discord.Color.gold()
            )

            embed.add_field(
                name="<:Clock:1434949269554597978> Current Shift",
                value=f"**Status:** <:Idle:1434949872968273940> On Break\n"
                      f"**Started:** <t:{int(updated_shift['start_time'].timestamp())}:R>\n"
                      f"**Break Started:** <t:{int(updated_shift['pause_start'].timestamp())}:R>",
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {updated_shift['type']}")

            # Create break view
            view = ShiftBreakView(self.cog, self.user, updated_shift)

            # Edit the message
            await interaction.edit_original_response(embed=embed, view=view)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="End", style=discord.ButtonStyle.danger, emoji="<:Reset:1434959478796714074>")
    async def end_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            await asyncio.gather(
                self.cog.update_nickname_for_shift_status(self.user, 'off'),
                self.cog.update_duty_roles(self.user, self.shift['type'], 'off')
            )

            pause_duration = self.shift.get('pause_duration', 0)
            if self.shift.get('pause_start'):
                pause_duration += (datetime.utcnow() - self.shift['pause_start']).total_seconds()

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time       = $1,
                           pause_duration = $2,
                           pause_start    = NULL
                       WHERE id = $3''',
                    datetime.utcnow(), pause_duration, self.shift['id']
                )

            # Get the completed shift for logging
            completed_shift = dict(self.shift)
            completed_shift['end_time'] = datetime.utcnow()
            completed_shift['pause_duration'] = pause_duration

            # ðŸ†• LOG THE SHIFT END
            await self.cog.log_shift_event(
                interaction.guild,
                'end',
                self.user,
                completed_shift
            )

            # **ADD THESE LINES TO CALCULATE active_duration and total_break:**
            total_duration = datetime.utcnow() - self.shift['start_time']
            active_duration = total_duration - timedelta(seconds=pause_duration)
            total_break = timedelta(seconds=pause_duration)

            # Get updated statistics
            stats = await self.cog.get_shift_statistics(self.user.id)

            # Create summary embed
            embed = discord.Embed(
                title="<:Checklist:1434948670226432171> **All Time Information**",
                description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.cog.format_duration(stats['total_duration'])}\n**Average Duration:** {self.cog.format_duration(stats['average_duration'])}",
                color=discord.Color(0x000000)
            )

            embed.set_footer(text=f"Shift Type: {self.shift['type']}")

            # Add quota info if available
            member = interaction.guild.get_member(self.user.id)
            if member:
                quota_info = await self.cog.get_quota_info(member)
                if quota_info['has_quota']:
                    status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                        'completed'] else "<:Denied:1426930694633816248>"
                    embed.add_field(
                        name="Quota Progress",
                        value=f"> {status_emoji} **{quota_info['percentage']:.1f}%** of {self.cog.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                        inline=False
                    )

            value_parts = [
                f"**Status:** <:Offline:1434951694319620197> Ended\n"
                f"**Total Time:** {self.cog.format_duration(active_duration)}"
            ]

            # Only add break info if there was actually break time
            if total_break.total_seconds() > 0:
                value_parts.append(f"**Break Time:** {self.cog.format_duration(total_break)}")

            embed.add_field(
                name="<:Clock:1434949269554597978> Last Shift",
                value="\n".join(value_parts),
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {self.shift['type']}")

            await self.cog.end_shift_and_show_summary(interaction, self.shift)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

class ShiftBreakView(discord.ui.View):
    """View shown when on break - Resume and End buttons"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, shift: dict):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.shift = shift
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Resume", style=discord.ButtonStyle.success, emoji="<:Pause:1434982402593390632>")
    async def resume_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            pause_duration = (datetime.utcnow() - self.shift['pause_start']).total_seconds()
            total_pause = self.shift.get('pause_duration', 0) + pause_duration

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start    = NULL,
                           pause_duration = $1
                       WHERE id = $2''',
                    total_pause, self.shift['id']
                )

            await self.cog.update_nickname_for_shift_status(self.user, 'duty')
            await self.cog.update_duty_roles(self.user, self.shift['type'], 'duty')

            updated_shift = await self.cog.get_active_shift(self.user.id)

            # 🆕 LOG THE RESUME WITH BREAK DURATION
            await self.cog.log_shift_event(
                interaction.guild,
                'resume',
                self.user,
                updated_shift,
                details=pause_duration  # Pass break duration
            )
            total_break = timedelta(seconds=updated_shift.get('pause_duration', 0))
            last_break = timedelta(seconds=pause_duration)  # Use the pause_duration we calculated

            # Build active panel embed directly
            embed = discord.Embed(
                title="**Shift Started**",
                color=discord.Color.green()
            )

            # Build the value string conditionally
            value_parts = [
                f"**Status:** <:Online:1434949591303983194> On Shift",
                f"**Started:** <t:{int(updated_shift['start_time'].timestamp())}:R>"
            ]

            # Only add break info if there was actually break time
            if total_break.total_seconds() > 0:
                value_parts.append(f"**Total Break Time:** {self.cog.format_duration(total_break)}")
                value_parts.append(f"**Last Break Time:** {self.cog.format_duration(last_break)}")

            embed.add_field(
                name="<:Clock:1434949269554597978> Current Shift",
                value="\n".join(value_parts),
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {updated_shift['type']}")

            # Create active view
            view = ShiftActiveView(self.cog, self.user, updated_shift)

            # Edit the message
            await interaction.edit_original_response(embed=embed, view=view)

            # Create active view
            view = ShiftActiveView(self.cog, self.user, updated_shift)

            # Edit the message
            await interaction.edit_original_response(embed=embed, view=view)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="End", style=discord.ButtonStyle.danger, emoji="<:Reset:1434959478796714074>")
    async def end_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            await asyncio.gather(
                self.cog.update_nickname_for_shift_status(self.user, 'off'),
                self.cog.update_duty_roles(self.user, self.shift['type'], 'off')
            )

            pause_duration = self.shift.get('pause_duration', 0)
            if self.shift.get('pause_start'):
                pause_duration += (datetime.utcnow() - self.shift['pause_start']).total_seconds()

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time       = $1,
                           pause_duration = $2,
                           pause_start    = NULL
                       WHERE id = $3''',
                    datetime.utcnow(), pause_duration, self.shift['id']
                )

            # Get the completed shift for logging
            completed_shift = dict(self.shift)
            completed_shift['end_time'] = datetime.utcnow()
            completed_shift['pause_duration'] = pause_duration

            # 🆕 LOG THE SHIFT END
            await self.cog.log_shift_event(
                interaction.guild,
                'end',
                self.user,
                completed_shift
            )
            # Get updated statistics
            stats = await self.cog.get_shift_statistics(self.user.id)

            total_duration = datetime.utcnow() - self.shift['start_time']
            active_duration = total_duration - timedelta(seconds=pause_duration)

            # Create summary embed
            embed = discord.Embed(
                title="<:Checklist:1434948670226432171> **All Time Information**",
                description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.cog.format_duration(stats['total_duration'])}\n**Average Duration:** {self.cog.format_duration(stats['average_duration'])}",
                color=discord.Color(0x000000)
            )

            embed.set_footer(text=f"Shift Type: {self.shift['type']}")

            # Add quota info if available
            member = interaction.guild.get_member(self.user.id)
            if member:
                quota_info = await self.cog.get_quota_info(member)
                if quota_info['has_quota']:
                    status_emoji = "<:Accepted:1426930333789585509>" if quota_info['completed'] else "<:Denied:1426930694633816248>"
                    embed.add_field(
                        name="Quota Progress",
                        value=f"> {status_emoji} **{quota_info['percentage']:.1f}%** of {self.cog.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                        inline=False
                    )

                value_parts = [
                    f"**Status:** <:Offline:1434951694319620197> Ended\n"
                    f"**Total Time:** {self.cog.format_duration(active_duration)}"
                ]

            # Only add break info if there was actually break time
            if total_break.total_seconds() > 0:
                value_parts.append(f"**Break Time:** {self.cog.format_duration(total_break)}")

            embed.add_field(
                name="<:Clock:1434949269554597978> Last Shift",
                value="\n".join(value_parts),
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {self.shift['type']}")

            await self.cog.end_shift_and_show_summary(interaction, self.shift)


        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

class ShiftTypeSelectView(discord.ui.View):
    """View for selecting shift type when user has multiple options"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, types: list):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.message = None

        async def on_timeout(self):
            """Clean up when view times out"""
            for item in self.children:
                item.disabled = True
            if self.message:
                try:
                    await self.message.edit(view=self)
                except:
                    pass

        # Add a button for each shift type
        for type in types:
            button = discord.ui.Button(
                label=type,
                style=discord.ButtonStyle.primary,
                custom_id=f"type_{type}"
            )
            button.callback = self.create_callback(type)
            self.add_item(button)

    def create_callback(self, type: str):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()

            # Verify user still has access to this shift type
            user_types = await self.cog.get_user_types(interaction.user)

            if type not in user_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> You don't have access to **{type}** shifts!",
                    ephemeral=True
                )
                return

            current_week = WeeklyShiftManager.get_current_week_monday()  # ← ADD THIS

            # Start the shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO shifts
                       (discord_user_id, discord_username, type, start_time, pause_duration, week_identifier, guild_id)
                       VALUES ($1, $2, $3, $4, 0, $5, $6)''',
                    self.user.id, str(self.user), type, datetime.utcnow(),
                    current_week, interaction.guild.id  # ← ADD THESE TWO
                )

            # Update nickname to DUTY
            await self.cog.update_nickname_for_shift_status(self.user, 'duty')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, type, 'duty')

            # Get the newly created shift
            shift = await self.cog.get_active_shift(self.user.id)

            # Build embed directly
            embed = discord.Embed(
                title="**Shift Started**",
                color=discord.Color.green()
            )

            embed.add_field(
                name="<:Clock:1434949269554597978> Current Shift",
                value=f"**Status:** <:Online:1434949591303983194> On Shift\n"
                      f"**Started:** <t:{int(shift['start_time'].timestamp())}:R>",
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {shift['type']}")

            # Create the active view
            view = ShiftActiveView(self.cog, self.user, shift)

            # Edit the original message
            await interaction.edit_original_response(embed=embed, view=view)

            # Note: Don't disable buttons or edit the old message since we're replacing it
            self.stop()

        return callback

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift selection!",
                ephemeral=True
            )
            return False
        return True


class AdminShiftTypeSelectView(discord.ui.View):
    """View for admins to select shift type"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, types: list):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.message = None

        async def on_timeout(self):
            """Clean up when view times out"""
            for item in self.children:
                item.disabled = True
            if self.message:
                try:
                    await self.message.edit(view=self)
                except:
                    pass

        for type in types:
            button = discord.ui.Button(
                label=type,
                style=discord.ButtonStyle.primary,
                custom_id=f"admin_type_{type}"
            )
            button.callback = self.create_callback(type)
            self.add_item(button)

    def create_callback(self, type: str):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()
            await self.cog.show_admin_shift_panel(interaction, self.target_user, type)
            self.stop()

        return callback

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True


class AdminShiftControlView(discord.ui.View):
    """Main admin control view with dropdown and action buttons"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str,
                 active_shift: dict):
        super().__init__(timeout=300)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.active_shift = active_shift
        self.message = None

        # Add dropdown for admin actions
        self.add_item(AdminActionsSelect(cog, admin, target_user, type))

        async def on_timeout(self):
            """Clean up when view times out"""
            for item in self.children:
                item.disabled = True
            if self.message:
                try:
                    await self.message.edit(view=self)
                except:
                    pass

        # Add shift control buttons if there's an active shift
        if active_shift:
            is_on_break = active_shift.get('pause_start') is not None

            if is_on_break:
                # Resume button
                resume_btn = discord.ui.Button(label="Resume Shift", style=discord.ButtonStyle.success, emoji="<:Play:1434957147829047467>")
                resume_btn.callback = self.resume_callback
                self.add_item(resume_btn)
            else:
                # Pause button
                pause_btn = discord.ui.Button(label="Pause Shift", style=discord.ButtonStyle.primary, emoji="<:Pause:1434982402593390632>")
                pause_btn.callback = self.pause_callback
                self.add_item(pause_btn)

            # Stop button
            stop_btn = discord.ui.Button(label="Stop Shift", style=discord.ButtonStyle.danger, emoji="<:Reset:1434959478796714074>")
            stop_btn.callback = self.stop_callback
            self.add_item(stop_btn)
        else:
            # Start button
            start_btn = discord.ui.Button(label="Start Shift", style=discord.ButtonStyle.success, emoji="<:Play:1434957147829047467>")
            start_btn.callback = self.start_callback
            self.add_item(start_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    async def start_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            current_week = WeeklyShiftManager.get_current_week_monday()

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO shifts
                       (discord_user_id, discord_username, type, start_time, pause_duration, week_identifier, guild_id)
                       VALUES ($1, $2, $3, $4, 0, $5, $6)''',
                    self.target_user.id, str(self.target_user), self.type, datetime.utcnow(),
                    current_week, interaction.guild.id
                )

            await self.cog.update_nickname_for_shift_status(self.target_user, 'duty')
            await self.cog.update_duty_roles(self.target_user, self.type, 'duty')

            # Get the newly created shift
            shift = await self.cog.get_active_shift(self.target_user.id)

            # 🆕 LOG THE ADMIN START
            await self.cog.log_shift_event(
                interaction.guild,
                'start',
                self.target_user,
                shift,
                admin=self.admin
            )

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Started shift for {self.target_user.mention}",
                ephemeral=True
            )

            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def pause_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start = $1
                       WHERE id = $2''',
                    datetime.utcnow(), self.active_shift['id']
                )

            await self.cog.update_nickname_for_shift_status(self.target_user, 'break')
            await self.cog.update_duty_roles(self.target_user, self.type, 'break')

            # Get updated shift
            updated_shift = await self.cog.get_active_shift(self.target_user.id)

            # 🆕 LOG THE ADMIN PAUSE
            await self.cog.log_shift_event(
                interaction.guild,
                'pause',
                self.target_user,
                updated_shift,
                admin=self.admin
            )

            await interaction.followup.send(
                f"⏸️ Paused shift for {self.target_user.mention}",
                ephemeral=True
            )

            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def resume_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            pause_duration = (datetime.utcnow() - self.active_shift['pause_start']).total_seconds()
            total_pause = self.active_shift.get('pause_duration', 0) + pause_duration

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start    = NULL,
                           pause_duration = $1
                       WHERE id = $2''',
                    total_pause, self.active_shift['id']
                )

            await self.cog.update_nickname_for_shift_status(self.target_user, 'duty')
            await self.cog.update_duty_roles(self.target_user, self.type, 'duty')

            # Get updated shift
            updated_shift = await self.cog.get_active_shift(self.target_user.id)

            # 🆕 LOG THE ADMIN RESUME
            await self.cog.log_shift_event(
                interaction.guild,
                'resume',
                self.target_user,
                updated_shift,
                admin=self.admin,
                details=pause_duration
            )

            await interaction.followup.send(
                f"▶️ Resumed shift for {self.target_user.mention}",
                ephemeral=True
            )

            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def stop_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            pause_duration = self.active_shift.get('pause_duration', 0)
            if self.active_shift.get('pause_start'):
                pause_duration += (datetime.utcnow() - self.active_shift['pause_start']).total_seconds()

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time       = $1,
                           pause_duration = $2,
                           pause_start    = NULL
                       WHERE id = $3''',
                    datetime.utcnow(), pause_duration, self.active_shift['id']
                )

            await self.cog.update_nickname_for_shift_status(self.target_user, 'off')
            await self.cog.update_duty_roles(self.target_user, self.type, 'off')

            # Create completed shift dict for logging
            completed_shift = dict(self.active_shift)
            completed_shift['end_time'] = datetime.utcnow()
            completed_shift['pause_duration'] = pause_duration

            # 🆕 LOG THE ADMIN STOP
            await self.cog.log_shift_event(
                interaction.guild,
                'end',
                self.target_user,
                completed_shift,
                admin=self.admin
            )

            await interaction.followup.send(
                f"⏹️ Stopped shift for {self.target_user.mention}",
                ephemeral=True
            )

            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

class AdminActionsSelect(discord.ui.Select):
    """Dropdown menu for admin actions"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str):
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = type

        options = [
            discord.SelectOption(label="Shift List", description="View shift history", emoji="<:List:1434953240155525201>"),
            discord.SelectOption(label="Modify Shift", description="Modify shift duration", emoji="<:Modify:1434954278362939632>"),
            discord.SelectOption(label="Delete Shift", description="Delete a shift", emoji="<:Reset:1434959478796714074>"),
            discord.SelectOption(label="Clear User Shifts", description="Clear all shifts", emoji="<:Wipe:1434954284851658762>")
        ]

        super().__init__(placeholder="Select an action...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        selection = self.values[0]

        if selection == "Shift List":
            await self.show_shift_list(interaction)
        elif selection == "Modify Shift":
            # Check if user has active shift
            active_shift = await self.cog.get_active_shift(self.target_user.id)
            if active_shift:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Cannot modify shifts while user has an active shift.",
                    ephemeral=True
                )
                return
            await self.show_modify_shift(interaction)
        elif selection == "Delete Shift":
            # Check if user has active shift
            active_shift = await self.cog.get_active_shift(self.target_user.id)
            if active_shift:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Cannot delete shifts while user has an active shift.",
                    ephemeral=True
                )
                return
            await self.show_delete_shift(interaction)

        elif selection == "Clear User Shifts":
            if not self.cog.has_super_admin_permission(interaction.user):
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You don't have permission for this action.",
                    ephemeral=True
                )
                return
            await self.show_clear_shifts(interaction)

        elif selection == "Clear User Shifts":
            if not self.cog.has_super_admin_permission(interaction.user):
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You don't have permission for this action.",
                    ephemeral=True
                )
                return
            await self.show_clear_shifts(interaction)

    async def show_shift_list(self, interaction: discord.Interaction):
        """Show paginated shift list"""
        view = ShiftListView(self.cog, self.admin, self.target_user, self.shift_type)
        await view.show_page(interaction, 0)

    async def show_modify_shift(self, interaction: discord.Interaction):
        """Show modify shift interface"""
        view = ModifyShiftSelectView(self.cog, self.admin, self.target_user, self.shift_type)
        await view.populate_shift_dropdown()  # Add this line
        await interaction.edit_original_response(
            content=f"Select a shift to modify for {self.target_user.mention}:",
            view=view,
        )

    async def show_delete_shift(self, interaction: discord.Interaction):
        """Show delete shift interface"""
        view = DeleteShiftSelectView(self.cog, self.admin, self.target_user, self.shift_type)
        await view.populate_shift_dropdown()  # Add this line
        await interaction.edit_original_response(
            content=f"Select a shift to delete for {self.target_user.mention}:",
            view=view,
        )

    async def show_clear_shifts(self, interaction: discord.Interaction):
        """Show clear all shifts interface"""
        # Get shift count
        async with db.pool.acquire() as conn:
            count = await conn.fetchval(
                '''SELECT COUNT(*)
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2''',
                self.target_user.id, self.shift_type
            )

        embed = discord.Embed(
            title=f"**Clear User Shifts**",
            description=f"Are you sure you want to clear **{count}** shifts for this user under the `**{self.shift_type}**` shift type?\n\nThis cannot be undone.",
            color=discord.Color.red()
        )

        view = ClearShiftsConfirmView(self.cog, self.admin, self.target_user, self.shift_type, count)
        await interaction.followup.send(embed=embed, view=view)


class ShiftListView(discord.ui.View):
    """Paginated shift list view"""
    ITEMS_PER_PAGE = 4

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.current_page = 0
        self.total_pages = 0
        self.shifts = []
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def get_shifts(self):
        """Fetch all completed shifts for current week"""
        current_week = WeeklyShiftManager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            self.shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2
                     AND end_time IS NOT NULL
                     AND week_identifier = $3
                   ORDER BY end_time DESC''',
                self.target_user.id, self.type, current_week
            )
        self.total_pages = max(1, math.ceil(len(self.shifts) / self.ITEMS_PER_PAGE))

    async def show_page(self, interaction: discord.Interaction, page: int):
        await self.get_shifts()

        if not self.shifts:
            embed = discord.Embed(
                title=f"**<:List:1434953240155525201> Shift List**",
                description="No completed shifts found.",
                color=discord.Color(0xffffff)
            )
            await interaction.followup.send(embed=embed)
            return

        self.current_page = max(0, min(page, self.total_pages - 1))

        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_shifts = self.shifts[start_idx:end_idx]

        embed = discord.Embed(
            title=f"**<:List:1434953240155525201> Shift List**",
            color=discord.Color(0xffffff)
        )

        for shift in page_shifts:
            shift_id = str(shift['id'])
            shift_time = shift['end_time'].strftime('%a, %d %b %Y %H:%M:%S GMT UTC')
            shift_name = f"`{shift_id}` | {shift_time}"
            duration = shift['end_time'] - shift['start_time']
            break_duration = timedelta(seconds=shift.get('pause_duration', 0))

            value = f"- **Duration:** {self.cog.format_duration(duration)}\n"
            value += f"- **Started:** <t:{int(shift['start_time'].timestamp())}:f>\n"
            value += f"- **Ended:** <t:{int(shift['end_time'].timestamp())}:f>\n"
            value += f"- **Break:** {self.cog.format_duration(break_duration)}\n"

            embed.add_field(
                name=f"`{shift_id}`",
                value=value,
                inline=False
            )

        embed.set_footer(text=f"Shift Type: {self.type} • Page {self.current_page + 1}/{self.total_pages}")

        # Update buttons
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == "first":
                    item.disabled = self.current_page == 0
                elif item.custom_id == "prev":
                    item.disabled = self.current_page == 0
                elif item.custom_id == "page":
                    item.label = f"{self.current_page + 1}/{self.total_pages}"
                elif item.custom_id == "next":
                    item.disabled = self.current_page >= self.total_pages - 1
                elif item.custom_id == "last":
                    item.disabled = self.current_page >= self.total_pages - 1

        if interaction.response.is_done():
            # Already responded, use followup
            await interaction.followup.send(embed=embed, view=self, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(emoji="<:LeftSkip:1434962162064822343>", style=discord.ButtonStyle.secondary, custom_id="first")
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, 0)

    @discord.ui.button(emoji="<:LeftArrow:1434962165215002777>", style=discord.ButtonStyle.secondary, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, self.current_page - 1)

    @discord.ui.button(label="1/7", style=discord.ButtonStyle.primary, custom_id="page", disabled=True)
    async def page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass

    @discord.ui.button(emoji="<:RightArrow:1434962170147246120>", style=discord.ButtonStyle.secondary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, self.current_page + 1)

    @discord.ui.button(emoji="<:RightSkip:1434962167660281926>", style=discord.ButtonStyle.secondary, custom_id="last")
    async def last_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, self.total_pages - 1)


class ModifyShiftSelectView(discord.ui.View):
    """View for selecting which shift to modify"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Most Recent", emoji="<:Play:1434957147829047467>", style=discord.ButtonStyle.primary)
    async def most_recent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        shift = await self.cog.get_last_shift(self.target_user.id)
        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No completed shifts found.",
                ephemeral=True
            )
            return

        await self.show_modify_panel(interaction, shift)

    @discord.ui.button(label="Search by Shift ID", emoji="<:Search:1434957367505719457>", style=discord.ButtonStyle.secondary)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ShiftIDModal(self.cog, self.admin, self.target_user, "modify")
        await interaction.response.send_modal(modal)

    async def show_modify_panel(self, interaction: discord.Interaction, shift: dict):
        """Show the modify options for a shift"""
        duration = shift['end_time'] - shift['start_time']
        active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

        embed = discord.Embed(
            title=f"**Modify Shift**",
            description=f"**Status:** <:Offline:1434951694319620197> Ended\n**Duration:** {self.cog.format_duration(active_duration)}",
            color=discord.Color(0x00000)
        )
        embed.set_footer(text=f"{shift['id']} • Shift Type: {shift['type']}")

        view = ModifyShiftActionsView(self.cog, self.admin, self.target_user, shift)
        await interaction.followup.send(embed=embed, view=view)


class ModifyShiftActionsView(discord.ui.View):
    """View for modifying a shift"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict):
        super().__init__(timeout=120)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Add Time", emoji="<:Add:1434959063329931396>", style=discord.ButtonStyle.success)
    async def add_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "add")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Remove Time", emoji="<:Remove:1434959215830499470>", style=discord.ButtonStyle.danger)
    async def remove_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "remove")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Time", emoji="<:Set:1434959334273712219>", style=discord.ButtonStyle.primary)
    async def set_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "set")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Reset Time", emoji="<:Reset:1434959478796714074>", style=discord.ButtonStyle.secondary)
    async def reset_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET start_time     = end_time,
                           pause_duration = 0
                       WHERE id = $1''',
                    self.shift['id']
                )

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Reset shift time for {self.target_user.mention} (Shift ID: {self.shift['id']})",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def populate_shift_dropdown(self):
        """Add a dropdown with recent shifts"""
        # Fetch recent shifts
        async with db.pool.acquire() as conn:
            recent_shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2
                     AND end_time IS NOT NULL
                   ORDER BY end_time DESC LIMIT 10''',
                self.target_user.id, self.type
            )

        if not recent_shifts:
            return

        # Create dropdown options
        options = []
        for shift in recent_shifts:
            shift_id = str(shift['id'])
            shift_time = shift['end_time'].strftime('%a, %d %b %Y %H:%M:%S GMT UTC')
            label = f"{shift_id} | {shift_time}"

            # Discord limits option labels to 100 characters
            if len(label) > 100:
                label = label[:97] + "..."

            options.append(discord.SelectOption(
                label=label,
                value=str(shift['id']),
                description=f"Duration: {self.cog.format_duration(shift['end_time'] - shift['start_time'])}"[:100]
            ))

        # Create the select menu
        select = discord.ui.Select(
            placeholder="Or select from recent shifts...",
            options=options,
            custom_id="shift_select"
        )
        select.callback = self.shift_select_callback
        self.add_item(select)

    async def shift_select_callback(self, interaction: discord.Interaction):
        """Handle shift selection from dropdown"""
        await interaction.response.defer()

        shift_id = int(interaction.data['values'][0])

        # Fetch the selected shift
        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE id = $1
                     AND discord_user_id = $2''',
                shift_id, self.target_user.id
            )

        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Shift not found.",
                ephemeral=True
            )
            return

        shift_dict = dict(shift)

        # For ModifyShiftSelectView
        if hasattr(self, 'show_modify_panel'):
            await self.show_modify_panel(interaction, shift_dict)
        # For DeleteShiftSelectView
        elif hasattr(self, 'show_delete_confirm'):
            await self.show_delete_confirm(interaction, shift_dict)

class TimeModifyModal(discord.ui.Modal):
    """Modal for modifying shift time"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict,
                 action: str):
        super().__init__(title=f"{action.capitalize()} Time")
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift
        self.action = action

        if action == "set":
            self.add_item(discord.ui.TextInput(
                label="Hours",
                placeholder="Enter hours",
                required=True
            ))
            self.add_item(discord.ui.TextInput(
                label="Minutes",
                placeholder="Enter minutes",
                required=True
            ))
        else:
            self.add_item(discord.ui.TextInput(
                label="Hours (optional)",
                placeholder="Enter hours",
                required=False
            ))
            self.add_item(discord.ui.TextInput(
                label="Minutes (optional)",
                placeholder="Enter minutes",
                required=False
            ))
            self.add_item(discord.ui.TextInput(
                label="Seconds (optional)",
                placeholder="Enter seconds",
                required=False
            ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            hours = int(self.children[0].value or 0)
            minutes = int(self.children[1].value or 0)
            seconds = int(self.children[2].value or 0) if len(self.children) > 2 else 0

            time_delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)

            if self.action == "set":
                new_start_time = self.shift['end_time'] - time_delta

                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE shifts
                           SET start_time = $1
                           WHERE id = $2''',
                        new_start_time, self.shift['id']
                    )

                # 🆕 LOG THE MODIFICATION
                await self.cog.log_shift_event(
                    interaction.guild,
                    'modify',
                    self.target_user,
                    self.shift,
                    admin=self.admin,
                    details=f"Set duration to {self.cog.format_duration(time_delta)}"
                )

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Set shift duration to {self.cog.format_duration(time_delta)} for {self.target_user.mention}",
                    ephemeral=True
                )


            elif self.action == "add":
                # Subtract time from pause_duration (which increases active time)
                current_pause = self.shift.get('pause_duration', 0)
                new_pause = max(0, current_pause - time_delta.total_seconds())

                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE shifts
                           SET pause_duration = $1
                           WHERE id = $2''',
                        new_pause, self.shift['id']
                    )

                    # 🆕 LOG THE MODIFICATION
                await self.cog.log_shift_event(
                    interaction.guild,
                    'modify',
                    self.target_user,
                    self.shift,
                    admin=self.admin,
                    details=f"Added {self.cog.format_duration(time_delta)} to shift"
                )

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Added {self.cog.format_duration(time_delta)} to shift for {self.target_user.mention}",
                    ephemeral=True
                )

            elif self.action == "remove":
                # Add time to pause_duration (which decreases active time)
                current_pause = self.shift.get('pause_duration', 0)
                new_pause = current_pause + time_delta.total_seconds()

                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE shifts
                           SET pause_duration = $1
                           WHERE id = $2''',
                        new_pause, self.shift['id']
                    )

                # 🆕 LOG THE MODIFICATION
                await self.cog.log_shift_event(
                    interaction.guild,
                    'modify',
                    self.target_user,
                    self.shift,
                    admin=self.admin,
                    details=f"Removed {self.cog.format_duration(time_delta)} from shift"
                )

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Removed {self.cog.format_duration(time_delta)} from shift for {self.target_user.mention}",
                    ephemeral=True
                )

        except ValueError:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please enter valid numbers for time.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )


class DeleteShiftSelectView(discord.ui.View):
    """View for selecting which shift to delete"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Most Recent", emoji="<:Play:1434957147829047467>", style=discord.ButtonStyle.primary)
    async def most_recent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        shift = await self.cog.get_last_shift(self.target_user.id)
        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No completed shifts found.",
                ephemeral=True
            )
            return

        await self.show_delete_confirm(interaction, shift)

    @discord.ui.button(label="Search by Shift ID", emoji="<:Search:1434957367505719457>", style=discord.ButtonStyle.secondary)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ShiftIDModal(self.cog, self.admin, self.target_user, "delete")
        await interaction.response.send_modal(modal)

    async def show_delete_confirm(self, interaction: discord.Interaction, shift: dict):
        """Show delete confirmation"""
        duration = shift['end_time'] - shift['start_time']
        active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

        embed = discord.Embed(
            title=f"**Delete Shift**",
            description=f"Are you sure you want to delete this shift?\nThis cannot be undone\n."
                        f"\n"
                        f"**Status:** <:Offline:1434951694319620197> Ended\n"
                        f"**Started:** <t:{int(shift['start_time'].timestamp())}:t>\n"
                        f"**Duration:** {self.cog.format_duration(active_duration)}",
            color=discord.Color.red()
        )

        embed.set_footer(text=f"{shift['id']} • Shift Type: {shift['type']}")

        view = DeleteShiftConfirmView(self.cog, self.admin, self.target_user, shift)
        await interaction.followup.send(embed=embed, view=view)


class DeleteShiftConfirmView(discord.ui.View):
    """Confirmation view for deleting a shift"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Delete", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            # 🆕 LOG BEFORE DELETING
            await self.cog.log_shift_event(
                interaction.guild,
                'delete',
                self.target_user,
                self.shift,
                admin=self.admin
            )

            async with db.pool.acquire() as conn:
                await conn.execute('DELETE FROM shifts WHERE id = $1', self.shift['id'])

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Deleted shift (ID: {self.shift['id']}) for {self.target_user.mention}",
                ephemeral=True
            )
        except:
            pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await interaction.followup.send("Cancelled.", ephemeral=True)
        self.stop()

    async def populate_shift_dropdown(self):
        """Add a dropdown with recent shifts"""
        # Fetch recent shifts
        async with db.pool.acquire() as conn:
            recent_shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2
                     AND end_time IS NOT NULL
                   ORDER BY end_time DESC LIMIT 10''',
                self.target_user.id, self.type
            )

        if not recent_shifts:
            return

        # Create dropdown options
        options = []
        for shift in recent_shifts:
            shift_id = str(shift['id'])
            shift_time = shift['end_time'].strftime('%a, %d %b %Y %H:%M:%S GMT UTC')
            label = f"{shift_id} | {shift_time}"

            # Discord limits option labels to 100 characters
            if len(label) > 100:
                label = label[:97] + "..."

            options.append(discord.SelectOption(
                label=label,
                value=str(shift['id']),
                description=f"Duration: {self.cog.format_duration(shift['end_time'] - shift['start_time'])}"[:100]
            ))

        # Create the select menu
        select = discord.ui.Select(
            placeholder="Or select from recent shifts...",
            options=options,
            custom_id="shift_select"
        )
        select.callback = self.shift_select_callback
        self.add_item(select)

    async def shift_select_callback(self, interaction: discord.Interaction):
        """Handle shift selection from dropdown"""
        await interaction.response.defer()

        shift_id = int(interaction.data['values'][0])

        # Fetch the selected shift
        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE id = $1
                     AND discord_user_id = $2''',
                shift_id, self.target_user.id
            )

        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Shift not found.",
                ephemeral=True
            )
            return

        shift_dict = dict(shift)

        # For ModifyShiftSelectView
        if hasattr(self, 'show_modify_panel'):
            await self.show_modify_panel(interaction, shift_dict)
        # For DeleteShiftSelectView
        elif hasattr(self, 'show_delete_confirm'):
            await self.show_delete_confirm(interaction, shift_dict)


class ClearShiftsConfirmView(discord.ui.View):
    """Confirmation view for clearing all shifts"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str,
                 count: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.count = count
        self.armed = False
        self.message = None

    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="ARM", emoji="<:ARM:1435117432791633921>", style=discord.ButtonStyle.secondary)
    async def arm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Toggle armed state
        self.armed = not self.armed

        if self.armed:
            # Switch to DISARM
            button.label = "DISARM"
            button.emoji = discord.PartialEmoji(name="DISARM", id=1435117667097772116)  # Replace with your disarm emoji
            button.style = discord.ButtonStyle.secondary

            # Enable the clear button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "clear_shifts":
                    item.disabled = False
                    item.label = f"Clear {self.count} User Shifts"

            await interaction.message.edit(view=self)
        else:
            # Switch back to ARM
            button.label = "ARM"
            button.emoji = discord.PartialEmoji(name="ARM", id=1435117432791633921)
            button.style = discord.ButtonStyle.secondary

            # Disable the clear button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "clear_shifts":
                    item.disabled = True
                    item.label = f"Clear {self.count} User Shifts"

            await interaction.message.edit(view=self)

    @discord.ui.button(label="Clear User Shifts", style=discord.ButtonStyle.secondary, disabled=True,
                       custom_id="clear_shifts")
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        if not self.armed:
            await interaction.followup.send("<:Denied:1426930694633816248> Please ARM first!", ephemeral=True)
            return

        try:
            # 🆕 LOG THE CLEAR
            await self.cog.log_shift_event(
                interaction.guild,
                'clear',
                self.target_user,
                {'type': self.type, 'id': 'N/A'},
                admin=self.admin,
                details=f"{self.count} shifts cleared"
            )

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''DELETE
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND type = $2''',
                    self.target_user.id, self.type
                )

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Cleared {self.count} shifts for {self.target_user.mention} ({self.type})",
                ephemeral=True
            )

            # Disable all buttons after clearing
            for item in self.children:
                item.disabled = True
            await interaction.message.edit(view=self)
            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await interaction.followup.send("Cancelled.", ephemeral=True)

        # Disable all buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        self.stop()


class ShiftIDModal(discord.ui.Modal):
    """Modal for entering a shift ID"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, action: str):
        super().__init__(title="Enter Shift ID")
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.action = action

        self.add_item(discord.ui.TextInput(
            label="Shift ID",
            placeholder="Enter the shift ID",
            required=True
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            shift_id = int(self.children[0].value)

            # Fetch the shift
            async with db.pool.acquire() as conn:
                shift = await conn.fetchrow(
                    '''SELECT *
                       FROM shifts
                       WHERE id = $1
                         AND discord_user_id = $2''',
                    shift_id, self.target_user.id
                )

            if not shift:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Shift not found or doesn't belong to this user.",
                    ephemeral=True
                )
                return

            shift_dict = dict(shift)

            if self.action == "modify":
                # Show modify panel
                view = ModifyShiftSelectView(self.cog, self.admin, self.target_user, shift_dict['type'])
                await view.show_modify_panel(interaction, shift_dict)
            elif self.action == "delete":
                # Show delete confirm
                view = DeleteShiftSelectView(self.cog, self.admin, self.target_user, shift_dict['type'])
                await view.show_delete_confirm(interaction, shift_dict)

        except ValueError:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please enter a valid shift ID number.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )


class QuotaConflictView(discord.ui.View):
    """Confirmation view for overwriting existing quotas"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, role_ids: list,
                 quota_seconds: int, type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.quota_seconds = quota_seconds
        self.type = type
        self.message = None

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your confirmation!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Overwrite", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)  # ← ADD THIS LINE

        try:
            async with db.pool.acquire() as conn:
                role_mentions = []
                for role_id in self.role_ids:
                    await conn.execute(
                        '''INSERT INTO shift_quotas (role_id, quota_seconds, type)
                           VALUES ($1, $2, $3) ON CONFLICT (role_id, type) 
                           DO
                        UPDATE SET quota_seconds = $2''',
                        role_id, self.quota_seconds, self.type
                    )
                    role = interaction.guild.get_role(role_id)
                    if role:
                        role_mentions.append(role.mention)

            await interaction.followup.send(  # ← Already correct
                f"<:Accepted:1426930333789585509> Updated quota for {', '.join(role_mentions)} to {self.cog.format_duration(timedelta(seconds=self.quota_seconds))} ({self.type})",
                ephemeral=True
            )

            # Disable buttons
            for item in self.children:
                item.disabled = True
            await interaction.message.edit(view=self)
            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)  # ← ADD THIS LINE
        await interaction.followup.send("Cancelled.", ephemeral=True)

        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        self.stop()

class ResetConfirmView(discord.ui.View):
    """Confirmation view for resetting shifts"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, role_ids: list,
                 role_names: list, affected_count: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.role_names = role_names
        self.affected_count = affected_count
        self.armed = False
        self.message = None

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your confirmation!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="ARM", emoji="<:ARM:1435117432791633921>", style=discord.ButtonStyle.secondary)
    async def arm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        self.armed = not self.armed

        if self.armed:
            button.label = "DISARM"
            button.emoji = discord.PartialEmoji(name="DISARM", id=1435117667097772116)
            button.style = discord.ButtonStyle.danger

            # Enable reset button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "reset":
                    item.disabled = False
        else:
            button.label = "ARM"
            button.emoji = discord.PartialEmoji(name="ARM", id=1435117432791633921)
            button.style = discord.ButtonStyle.danger

            # Disable reset button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "reset":
                    item.disabled = True

        await interaction.message.edit(view=self)

    @discord.ui.button(label="Execute Reset", style=discord.ButtonStyle.danger, disabled=True, custom_id="reset")
    async def reset_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        if not self.armed:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please ARM first!",
                ephemeral=True
            )
            return

        try:
            # Get next wave number
            async with db.pool.acquire() as conn:
                max_wave = await conn.fetchval(
                    'SELECT MAX(round_number) FROM shifts WHERE round_number IS NOT NULL'
                )
                next_wave = (max_wave or 0) + 1

                # Get all users with these roles and current shifts
                affected_users = set()
                for role_id in self.role_ids:
                    role = interaction.guild.get_role(role_id)
                    if role:
                        affected_users.update([member.id for member in role.members])

                # Archive current shifts to wave
                archived_count = 0
                for user_id in affected_users:
                    result = await conn.execute(
                        '''UPDATE shifts
                           SET round_number = $1
                           WHERE discord_user_id = $2
                             AND round_number IS NULL''',
                        next_wave, user_id
                    )
                    if result != 'UPDATE 0':
                        archived_count += 1

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> **Wave {next_wave} Created**\n"
                f"• Archived shifts for {len(affected_users)} users\n"
                f"• Affected roles: {', '.join(self.role_names)}\n"
                f"• Users can now start fresh shifts for the new wave",
                ephemeral=True
            )

            # Disable all buttons
            for item in self.children:
                item.disabled = True
            await interaction.message.edit(view=self)
            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await interaction.followup.send("Cancelled.", ephemeral=True)

        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        self.stop()

async def setup(bot):
    await bot.add_cog(ShiftManagementCog(bot))