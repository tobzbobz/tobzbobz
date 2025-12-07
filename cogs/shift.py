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

CACHE_TTL_SECONDS = 300
LONG_BREAK_THRESHOLD_SECONDS = 1200
LONG_BREAK_THRESHOLD_MINUTES = LONG_BREAK_THRESHOLD_SECONDS / 60
SHIFT_LIST_ITEMS_PER_PAGE = 4

OWNER_USER_ID = 678475709257089057
WEEKLY_REPORT_CHANNELS = [1413001074440142948, 1436829440729682014]
WEEKLY_REPORT_PING_ROLES = [1285474077556998196, 1389113393511923863]

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
SHIFT_LOGS_CHANNELS = [
    1435798856687161467,  # Primary log channel
    1411662121531609130   # Secondary log channel - CHANGE THIS
]
PING_ROLES = [1285474077556998196, 1389113393511923863, 1389550689113473024]


def validate_time_input(hours: int, minutes: int, seconds: int = 0) -> tuple[bool, str]:
    """Validate time input values"""
    if hours < 0 or minutes < 0 or seconds < 0:
        return False, "Time values cannot be negative"
    if minutes >= 60 or seconds >= 60:
        return False, "Minutes and seconds must be less than 60"
    if hours > 999:
        return False, "Hours cannot exceed 999"
    return True, ""

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
            dt = datetime.now(NZST)  # <:Accepted:1426930333789585509> CORRECT
        elif dt.tzinfo is None:
            dt = NZST.localize(dt)  # <:Accepted:1426930333789585509> CORRECT
        else:
            dt = dt.astimezone(NZST)

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
            # Get current wave shifts
            shifts = await conn.fetch(
                '''SELECT discord_user_id,
                          type,
                          guild_id,
                          SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                              COALESCE(pause_duration, 0)) as total_seconds
                   FROM shifts
                   WHERE wave_number = $1
                     AND end_time IS NOT NULL
                   GROUP BY discord_user_id, type, guild_id
                   ORDER BY type, total_seconds DESC''',
                wave_number
            )

            if not shifts:
                print(f"No shifts found for wave {wave_number}")
                return

            # ✅ FIX: Get all quotas to find max period per shift type
            all_quotas = await conn.fetch(
                'SELECT role_id, quota_seconds, type, quota_period_weeks FROM shift_quotas'
            )

            # Find max period for each shift type (for historical data fetching)
            max_periods = {}
            for q in all_quotas:
                shift_type = q['type']
                period = q.get('quota_period_weeks', 1)
                if shift_type not in max_periods or period > max_periods[shift_type]:
                    max_periods[shift_type] = period

            # ✅ FIX: Fetch historical data based on max period needed for each type
            historical_data = {}

            for shift_type, max_period in max_periods.items():
                # Fetch enough historical data to cover the longest quota period
                historical = await conn.fetch(
                    '''SELECT discord_user_id,
                              SUM(EXTRACT(EPOCH FROM (end_time - start_time)) -
                                  COALESCE(pause_duration, 0)) as total_seconds
                       FROM shifts
                       WHERE wave_number <= $1
                         AND wave_number > $2
                         AND type = $3
                         AND end_time IS NOT NULL
                       GROUP BY discord_user_id''',
                    wave_number,
                    wave_number - max_period,  # ✅ Use longest period for this type
                    shift_type
                )

                historical_data[shift_type] = {
                    row['discord_user_id']: row['total_seconds']
                    for row in historical
                }

            # Get guild
            guild = None
            for g in self.bot.guilds:
                if g.get_member(shifts[0]['discord_user_id']):
                    guild = g
                    break

            if not guild:
                print("Could not find guild for weekly report")
                return

            # Get all role quotas
            quotas = await conn.fetch(
                'SELECT role_id, quota_seconds, type, quota_period_weeks FROM shift_quotas'
            )
            quota_map = {}
            for q in quotas:
                key = (q['role_id'], q['type'])
                quota_map[key] = {
                    'seconds': q['quota_seconds'],
                    'period': q.get('quota_period_weeks', 1)
                }

            # Get ignored roles
            ignored_roles = await conn.fetch(
                'SELECT role_id, type FROM quota_ignored_roles'
            )
            ignored_set = {(r['role_id'], r['type']) for r in ignored_roles}

        # Organize by shift type
        types = {}
        for shift in shifts:
            shift_type = shift['type']
            if shift_type not in types:
                types[shift_type] = []
            types[shift_type].append(shift)

        # Build report embeds
        embeds = []

        for shift_type, type_shifts in types.items():
            user_ids = list(set(s['discord_user_id'] for s in type_shifts))
            members_dict = {m.id: m for m in guild.members if m.id in user_ids}

            lines = []

            for shift_data in type_shifts:
                member = guild.get_member(shift_data['discord_user_id'])
                if not member:
                    continue

                # Get user's highest quota for this shift type
                max_quota = 0
                user_quota_period = 1  # ✅ Track the user's specific quota period
                user_ignored = False

                for role in member.roles:
                    if (role.id, shift_type) in ignored_set:
                        user_ignored = True
                        break

                    quota_info = quota_map.get((role.id, shift_type))
                    if quota_info and quota_info['seconds'] > max_quota:
                        max_quota = quota_info['seconds']
                        user_quota_period = quota_info['period']  # ✅ Use this role's period

                if user_ignored or max_quota == 0:
                    continue

                active_seconds = shift_data['total_seconds']

                # ✅ FIX: Calculate rolling totals based on USER'S quota period
                rolling_seconds = None
                weeks_included = 0

                # Use the user's specific quota period (from their highest quota role)
                if user_quota_period > 1:
                    # Calculate position in THIS user's quota cycle
                    # Wave 1 starts the cycle, so: waves 1,2,3,4 = positions 1,2,3,4 for 4-week
                    cycle_position = ((wave_number - 1) % user_quota_period) + 1

                    # Show rolling total except for first week of user's cycle
                    if cycle_position > 1:
                        rolling_seconds = historical_data.get(shift_type, {}).get(
                            member.id,
                            active_seconds
                        )
                        weeks_included = cycle_position

                # Check bypass roles (rest of the code remains the same)
                user_role_ids = {role.id for role in member.roles}
                bypass_type = None
                completed = False

                if QUOTA_BYPASS_ROLE in user_role_ids:
                    bypass_type = 'QB'
                    completed = True
                    status = f"<:Accepted:1426930333789585509> {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **(QB Bypass)**"
                elif LOA_ROLE in user_role_ids:
                    bypass_type = 'LOA'
                    completed = True
                    status = f"<:Accepted:1426930333789585509> {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **(LOA Exempt)**"
                elif REDUCED_ACTIVITY_ROLE in user_role_ids:
                    bypass_type = 'RA'
                    modified_quota = max_quota * 0.5
                    percentage = (active_seconds / modified_quota * 100) if modified_quota > 0 else 0
                    completed = percentage >= 100
                    emoji = "<:Accepted:1426930333789585509>" if completed else "<:Denied:1426930694633816248>"

                    if rolling_seconds and rolling_seconds > active_seconds:
                        status = f"{emoji} {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} ({self.cog.format_duration(timedelta(seconds=rolling_seconds))}) / {self.cog.format_duration(timedelta(seconds=max_quota))} **({percentage:.1f}% - RA 50% Required)**"
                    else:
                        status = f"{emoji} {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **({percentage:.1f}% - RA 50% Required)**"
                else:
                    bypass_type = None
                    percentage = (active_seconds / max_quota * 100) if max_quota > 0 else 0
                    completed = percentage >= 100
                    emoji = "<:Accepted:1426930333789585509>" if completed else "<:Denied:1426930694633816248>"

                    if rolling_seconds and rolling_seconds > active_seconds:
                        status = f"{emoji} {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} ({self.cog.format_duration(timedelta(seconds=rolling_seconds))}) / {self.cog.format_duration(timedelta(seconds=max_quota))} **({percentage:.1f}%)**"
                    else:
                        status = f"{emoji} {member.mention} - {self.cog.format_duration(timedelta(seconds=active_seconds))} / {self.cog.format_duration(timedelta(seconds=max_quota))} **({percentage:.1f}%)**"

                lines.append(status)

            if lines:
                embed = discord.Embed(
                    title=f"**{shift_type.replace('Shift ', '')} Weekly Report**",
                    description="\n".join(lines),
                    color=discord.Color(0x000000)
                )

                # Get week dates from wave
                async with self.cog.db.pool.acquire() as conn:
                    week_start = await conn.fetchval(
                        'SELECT MIN(week_identifier) FROM shifts WHERE wave_number = $1',
                        wave_number
                    )

                if week_start:
                    week_end = week_start + timedelta(days=6)

                    # ✅ FIX: Dynamic footer based on user's actual quota period
                    footer_text = f"Wave {wave_number} • {week_start.strftime('%d %b')} - {week_end.strftime('%d %b %Y')}"

                    # Note: Footer shows general info, individual users may have different periods
                    # This is intentional - each user's line shows their own rolling total

                    embed.set_footer(text=footer_text)
                else:
                    embed.set_footer(text=f"Wave {wave_number}")

                embeds.append(embed)

        # Send to multiple channels
        if embeds:
            ping_mentions = " ".join([f"<@&{role_id}>" for role_id in WEEKLY_REPORT_PING_ROLES])

            for channel_id in WEEKLY_REPORT_CHANNELS:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    try:
                        # Send main report embeds
                        await channel.send(
                            content=f"||{ping_mentions}||\n**Weekly Shift Report - Wave {wave_number}**",
                            embeds=embeds
                        )
                        print(f"Sent weekly report to channel {channel_id} with {len(embeds)} embeds")

                        # ✅ NEW: Send quota information summary
                        await self.send_quota_summary(channel, wave_number, guild, quota_map, types)

                    except Exception as e:
                        print(f"Failed to send weekly report to channel {channel_id}: {e}")
                else:
                    print(f"Warning: Channel {channel_id} not found")
        else:
            print(f"No embeds generated for wave {wave_number}")

    async def send_quota_summary(self, channel, wave_number: int, guild, quota_map, types):
        """Send detailed quota information after the weekly report"""

        try:
            async with self.cog.db.pool.acquire() as conn:
                # Get next wave start time (Monday at 00:00 NZST)
                current_wave_monday = self.get_week_monday()
                next_wave_monday = current_wave_monday + timedelta(days=7)
                reset_timestamp = int(next_wave_monday.timestamp())

                # Get bypass role members
                qb_role = guild.get_role(QUOTA_BYPASS_ROLE)
                loa_role = guild.get_role(LOA_ROLE)
                ra_role = guild.get_role(REDUCED_ACTIVITY_ROLE)

                # Organize quotas by shift type and period
                quota_info_by_type = {}

                for (role_id, shift_type), quota_data in quota_map.items():
                    if shift_type not in quota_info_by_type:
                        quota_info_by_type[shift_type] = {}

                    period = quota_data['period']
                    quota_seconds = quota_data['seconds']

                    if period not in quota_info_by_type[shift_type]:
                        quota_info_by_type[shift_type][period] = []

                    role = guild.get_role(role_id)
                    if role:
                        # Count members with this role (approximate - counts all role members)
                        member_count = len(role.members)

                        quota_info_by_type[shift_type][period].append({
                            'role': role,
                            'seconds': quota_seconds,
                            'members': member_count
                        })

            # Create summary embed for each shift type that appeared in the report
            for shift_type in types.keys():
                if shift_type not in quota_info_by_type:
                    continue

                embed = discord.Embed(
                    title=f"**{shift_type.replace('Shift ', '')} Quota Information**",
                    color=discord.Color(0x5865f2)
                )

                # Add reset information
                embed.add_field(
                    name="Next Wave Reset",
                    value=f"<t:{reset_timestamp}:F> (<t:{reset_timestamp}:R>)",
                    inline=False
                )

                # Calculate total members with quotas
                total_members = set()
                for period_quotas in quota_info_by_type[shift_type].values():
                    for quota in period_quotas:
                        total_members.update([m.id for m in quota['role'].members])

                embed.add_field(
                    name="Members with Quotas",
                    value=f"{len(total_members)} members have quota requirements",
                    inline=False
                )

                # Add quota details organized by period
                sorted_periods = sorted(quota_info_by_type[shift_type].keys())

                for period in sorted_periods:
                    period_quotas = quota_info_by_type[shift_type][period]

                    # Sort by quota amount (highest first)
                    period_quotas.sort(key=lambda x: x['seconds'], reverse=True)

                    quota_lines = []
                    for quota in period_quotas:
                        duration_str = self.cog.format_duration(timedelta(seconds=quota['seconds']))
                        quota_lines.append(
                            f"{quota['role'].mention} • {duration_str} • {quota['members']} member{'s' if quota['members'] != 1 else ''}"
                        )

                    period_text = f"{period} Week" if period == 1 else f"{period} Weeks"

                    # Add cycle information
                    cycle_position = ((wave_number - 1) % period) + 1

                    waves_until_reset = period - cycle_position
                    quota_reset_date = current_wave_monday + timedelta(weeks=waves_until_reset + 1)
                    quota_reset_timestamp = int(quota_reset_date.timestamp())

                    if period > 1:
                        cycle_info = f" (Week {cycle_position}/{period} of cycle)"
                        reset_info = f"\n**Resets:** <t:{quota_reset_timestamp}:F> (<t:{quota_reset_timestamp}:R>)"
                    else:
                        cycle_info = " (Resets every wave)"
                        reset_info = f"\n**Resets:** <t:{reset_timestamp}:F> (<t:{reset_timestamp}:R>)"

                    field_value = "\n".join(quota_lines) if quota_lines else "No roles configured"
                    field_value += reset_info

                    embed.add_field(
                        name=f"{period_text} Quota Period{cycle_info}",
                        value=field_value,
                        inline=False
                    )

                # ✅ NEW: Add bypass roles section
                bypass_sections = []

                # Get members with shift type access for this specific shift type
                shift_type_members = set()
                for member in guild.members:
                    # Check if member has any role that grants this shift type
                    for role in member.roles:
                        if role.id in typeS and typeS[role.id] == shift_type:
                            shift_type_members.add(member.id)
                            break
                        # Check additional shift access
                        if role.id in ADDITIONAL_SHIFT_ACCESS and shift_type in ADDITIONAL_SHIFT_ACCESS[role.id]:
                            shift_type_members.add(member.id)
                            break

                # QB (Quota Bypass) - Full exemption
                if qb_role and qb_role.members:
                    qb_members = [m.mention for m in qb_role.members if m.id in shift_type_members]
                    if qb_members:
                        qb_text = ", ".join(qb_members[:15])  # Limit to 15 to avoid embed length issues
                        if len(qb_members) > 15:
                            qb_text += f" *+{len(qb_members) - 15} more*"
                        bypass_sections.append(f"**QB (Full Exemption):** {qb_text}")

                # LOA (Leave of Absence) - Full exemption
                if loa_role and loa_role.members:
                    loa_members = [m.mention for m in loa_role.members if m.id in shift_type_members]
                    if loa_members:
                        loa_text = ", ".join(loa_members[:15])
                        if len(loa_members) > 15:
                            loa_text += f" *+{len(loa_members) - 15} more*"
                        bypass_sections.append(f"**LOA (Full Exemption):** {loa_text}")

                # RA (Reduced Activity) - 50% requirement
                if ra_role and ra_role.members:
                    ra_members = [m.mention for m in ra_role.members if m.id in shift_type_members]
                    if ra_members:
                        ra_text = ", ".join(ra_members[:15])
                        if len(ra_members) > 15:
                            ra_text += f" *+{len(ra_members) - 15} more*"
                        bypass_sections.append(f"**RA (50% Required):** {ra_text}")

                if bypass_sections:
                    embed.add_field(
                        name="Quota Modifications",
                        value="\n\n".join(bypass_sections),
                        inline=False
                    )

                # Add footer with helpful info
                embed.set_footer(
                    text=f"Wave {wave_number} • Shifts Reset at the start of each week (but quotas may not)"
                )

                await channel.send(embed=embed)

        except Exception as e:
            print(f"Error sending quota summary: {e}")
            import traceback
            traceback.print_exc()

class QuotaTimeView(discord.ui.View):
    """View with button to trigger time input modal"""

    def __init__(self, cog, admin: discord.Member, role_ids: list, type: str, guild,
                 period_weeks: int = 1):  # Remove : ShiftManagementCog type hint
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.type = type
        self.guild = guild
        self.period_weeks = period_weeks

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
       if interaction.user.id != self.admin.id:
           await interaction.response.send_message(
               "<:Denied:1426930694633816248> This is not your quota panel!",
                ephemeral=True
           )
           return False
       return True

    @discord.ui.button(label="Set Time", style=discord.ButtonStyle.primary, emoji="⏰")
    async def set_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = QuotaTimeModal(self.cog, self.admin, self.role_ids, self.type, self.guild, self.period_weeks)
        await interaction.response.send_modal(modal)

class QuotaTimeModal(discord.ui.Modal):
    """Modal for entering quota time and watch requirement"""

    def __init__(self, cog: "ShiftManagementCog", admin: discord.Member, role_ids: list, type: str, guild,
                 period_weeks: int = 1):  # Use string "ShiftManagementCog" for forward reference
        super().__init__(title=f"Set Quota ({period_weeks}w)")
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.type = type
        self.guild = guild
        self.period_weeks = period_weeks

        self.add_item(discord.ui.TextInput(
            label="Hours",
            placeholder="Enter hours (0-999)",
            required=True,
            max_length=3
        ))

        self.add_item(discord.ui.TextInput(
            label="Minutes",
            placeholder="Enter minutes (0-59)",
            required=True,
            max_length=2
        ))

        # Add watch quota field for FENZ only
        if type == "Shift FENZ":
            self.add_item(discord.ui.TextInput(
                label="Watch Quota (FENZ only)",
                placeholder="Number of watches required (0 for none)",
                required=False,
                default="0",
                max_length=3
            ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            hours = int(self.children[0].value or 0)
            minutes = int(self.children[1].value or 0)
            watch_quota = 0

            # Get watch quota if FENZ
            if self.type == "Shift FENZ" and len(self.children) > 2:
                watch_quota = int(self.children[2].value or 0)

            # Validate
            valid, error_msg = validate_time_input(hours, minutes, 0)
            if not valid:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {error_msg}",
                    ephemeral=True
                )
                return

            if watch_quota < 0:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Watch quota cannot be negative.",
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

            # Save to database
            async with db.pool.acquire() as conn:
                role_mentions = []
                for role_id in self.role_ids:
                    await conn.execute(
                        '''INSERT INTO shift_quotas (role_id, quota_seconds, type, quota_period_weeks, watch_quota)
                           VALUES ($1, $2, $3, $4, $5) ON CONFLICT (role_id, type) 
                           DO UPDATE SET quota_seconds = $2, quota_period_weeks = $4, watch_quota = $5''',
                        role_id, total_seconds, self.type, self.period_weeks, watch_quota
                    )
                    role = self.guild.get_role(role_id)
                    if role:
                        role_mentions.append(role.mention)

                period_text = f"{self.period_weeks} week{'s' if self.period_weeks != 1 else ''}"
                watch_text = f" + {watch_quota} watches" if watch_quota > 0 else ""

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Set quota for {', '.join(role_mentions)} to {self.cog.format_duration(timedelta(seconds=total_seconds))}{watch_text} over {period_text} ({self.type})",
                    ephemeral=True
                )

        except ValueError:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please enter valid numbers.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

class QuotaPeriodSelectView(discord.ui.View):
    """View for selecting quota period before setting time"""

    def __init__(self, cog: "ShiftManagementCog", admin: discord.Member, role_ids: list, type: str, guild):
        # Use string "ShiftManagementCog" for forward reference
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.type = type
        self.guild = guild
        self.message = None

        # Add period selection dropdown
        options = [
            discord.SelectOption(label="1 Week", value="1", description="Quota resets weekly", emoji="📅"),
            discord.SelectOption(label="2 Weeks", value="2", description="Quota resets bi-weekly", emoji="📅"),
            discord.SelectOption(label="3 Weeks", value="3", description="Quota resets every 3 weeks", emoji="📅"),
            discord.SelectOption(label="4 Weeks", value="4", description="Quota resets monthly", emoji="📅"),
        ]

        select = discord.ui.Select(
            placeholder="Select quota period...",
            options=options,
            custom_id="period_select"
        )
        select.callback = self.period_callback
        self.add_item(select)

        # Add cancel button
        cancel_btn = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.secondary,
            emoji="<:Denied:1426930694633816248>"
        )
        cancel_btn.callback = self.cancel_callback
        self.add_item(cancel_btn)

    async def period_callback(self, interaction: discord.Interaction):
        """Handle period selection"""
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your quota panel!",
                ephemeral=True
            )
            return

        period_weeks = int(interaction.data['values'][0])

        # Show time input modal
        modal = QuotaTimeModal(self.cog, self.admin, self.role_ids, self.type, self.guild, period_weeks)
        await interaction.response.send_modal(modal)

    async def cancel_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your quota panel!",
                ephemeral=True
            )
            return

        await interaction.response.send_message("Cancelled.", ephemeral=True)
        if self.message:
            try:
                await self.message.delete()
            except:
                pass
        self.stop()

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass



class ShiftManagementCog(commands.Cog):
    shift_group = app_commands.Group(name="shift", description="Shift management commands")

    def __init__(self, bot):
        self.bot = bot
        self.db = db
        self._role_cache = {}
        self._quota_cache = {}
        self._quota_cache_time = None
        self._modification_cache = {}
        self._modification_timers = {}
        self.SHIFT_LOGS_CHANNEL = SHIFT_LOGS_CHANNEL
        self._cache_cleanup_task = None
        self.weekly_manager = WeeklyShiftManager(self)

        # Set Admin Command usage limit
        self.admin_rate_limiter = AdminCommandRateLimiter(calls=10, period=60)
        bot.loop.create_task(self.on_cog_load())

    async def on_cog_load(self):
        """Run initialization tasks when cog loads"""
        await self.bot.wait_until_ready()

        # Wait for database with better error handling
        max_wait_time = 60
        start_time = asyncio.get_event_loop().time()

        while True:
            if await ensure_database_connected():
                print("✅ Database ready")
                break

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > max_wait_time:
                print("❌ Database failed to connect within timeout")
                # Don't raise - let bot continue but log warning
                print("⚠️ COG LOADED WITHOUT DATABASE - Some features may not work!")
                break

            print(f"⏳ Waiting for database... ({int(elapsed)}s)")
            await asyncio.sleep(5)


        # Start weekly reset task
        if not self.weekly_reset_task.is_running():
            self.weekly_reset_task.start()

        if not self.check_long_breaks.is_running():
            self.check_long_breaks.start()

        # Clean up stale shifts
        await self.cleanup_stale_shifts(self.bot)

    @tasks.loop(time=time(hour=22, minute=0, tzinfo=pytz.timezone('Pacific/Auckland')))
    async def weekly_reset_task(self):
        """Check if it's Sunday 10 PM NZST and run weekly reset"""
        now = datetime.now(NZST)

        if now.weekday() == 6:  # Sunday (0=Monday, 6=Sunday)
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
        Send shift event logs to multiple logging channels

        event_type: 'start', 'end', 'pause', 'resume', 'modify', 'delete', 'clear'
        """
        # ✅ CHANGE: Get ALL log channels instead of just one
        log_channels = []
        for channel_id in [SHIFT_LOGS_CHANNEL, 1411662121531609130]:  # Add your second channel ID here
            channel = self.bot.get_channel(channel_id)
            if channel:
                log_channels.append(channel)
            else:
                print(f"Warning: Log channel {channel_id} not found")

        if not log_channels:
            print(f"Warning: No valid log channels found")
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
                    color=discord.Color(0x57f288)
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
                    color=discord.Color(0xed4245)
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
                        name="Total Break Time",
                        value=self.format_duration(break_duration),
                        inline=True
                    )

                embed.add_field(name='', value='', inline=False)

                break_sessions = shift_data.get('break_sessions')
                if break_sessions:
                    try:
                        sessions = json.loads(break_sessions) if isinstance(break_sessions, str) else break_sessions
                        if sessions:
                            session_lines = []
                            for i, session in enumerate(sessions, 1):
                                if session.get('end'):
                                    start_time = datetime.fromisoformat(session['start'])
                                    end_time = datetime.fromisoformat(session['end'])
                                    session_duration = timedelta(seconds=session['duration'])
                                    session_lines.append(
                                        f"`{i}.` <t:{int(start_time.timestamp())}:t> - <t:{int(end_time.timestamp())}:t> ({self.format_duration(session_duration)})"
                                    )

                            if session_lines:
                                embed.add_field(
                                    name="Break Sessions",
                                    value="\n".join(session_lines),
                                    inline=False
                                )
                    except (json.JSONDecodeError, ValueError):
                        pass

                embed.add_field(
                    name="Started",
                    value=f"<t:{int(shift_data['start_time'].timestamp())}:F>",
                    inline=True
                )
                embed.add_field(
                    name="Ended",
                    value=f"<t:{int(shift_data['end_time'].timestamp())}:F>",
                    inline=True
                )

                embed.set_footer(
                    text=f"{'Ended by ' + admin.display_name if admin else 'Ended by ' + display_name} • Shift ID: {shift_id}"
                )

            elif event_type == 'pause':
                embed = discord.Embed(
                    title=f"Shift Paused • {type.replace('Shift ', '')}",
                    color=discord.Color(0xfee75c)
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
                    color=discord.Color(0x57f288)
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
                    color=discord.Color(0x5865f2)
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
                    color=discord.Color(0xed4245)
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
                    color=discord.Color(0xed4245)
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

            embed.set_thumbnail(url=member.display_avatar.url)
            embed.timestamp = datetime.utcnow()

            # ✅ CHANGE: Send to ALL log channels
            for channel in log_channels:
                try:
                    await channel.send(embed=embed)
                except Exception as e:
                    print(f"Failed to send log to channel {channel.id}: {e}")

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

    async def get_shift_statistics(self, user_id: int, guild_id: int = None):
        """Calculate shift statistics for a user (current week only)"""
        current_week = self.weekly_manager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            query = '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND end_time IS NOT NULL
                         AND week_identifier = $2'''
            params = [user_id, current_week]

            if guild_id:
                query += ' AND guild_id = $3'
                params.append(guild_id)

            shifts = await conn.fetch(query, *params)

            if not shifts:
                return {
                    'count': 0,
                    'total_duration': timedelta(0),
                    'average_duration': timedelta(0)
                }

            total_duration = timedelta(0)
            for shift in shifts:
                if shift['end_time'] and shift['start_time']:  # ADD THIS CHECK
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
                        bypass_type = None  # Add this line
                        completed = (active_seconds / max_quota >= 1) if max_quota > 0 else False
                if max_quota == 0:
                    results[user_id] = {
                        'has_quota': max_quota > 0,
                        'quota_seconds': max_quota,
                        'active_seconds': active_seconds,
                        'percentage': (active_seconds / max_quota * 100) if max_quota > 0 else 0,
                        'completed': completed,
                        'bypass_type': bypass_type
                    }
                    continue

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
                     AND week_identifier = $2''',  # <:Accepted:1426930333789585509> Filter by current week
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

        # Cache quota data for 5 minutes
        cache_key = (role_id, type)
        current_time = datetime.utcnow()

        if self._quota_cache_time and (current_time - self._quota_cache_time).total_seconds() < 300:
            if cache_key in self._quota_cache:
                return self._quota_cache[cache_key]

        if not self._quota_cache_time or (current_time - self._quota_cache_time).total_seconds() >= 300:
            async with db.pool.acquire() as conn:
                all_quotas = await conn.fetch('SELECT role_id, quota_seconds, type FROM shift_quotas')
                self._quota_cache = {(q['role_id'], q['type']): q['quota_seconds'] for q in all_quotas}
                self._quota_cache_time = current_time

        return self._quota_cache.get(cache_key, 0)

    # CHANGE get_user_quota to:
    async def get_user_quota(self, member: discord.Member, type: str = None) -> tuple[int, bool]:
        """
        Get the highest quota from all of a user's roles.
        Returns: (quota_seconds, has_any_quota)
        """
        max_quota = 0
        has_any_quota = False  # Track if user has ANY quota role

        for role in member.roles:
            quota = await self.get_quota_for_role(role.id, type)
            if quota is not None and quota > 0:  # Role has a quota set
                has_any_quota = True
                if quota > max_quota:
                    max_quota = quota

        return (max_quota, has_any_quota)

    async def get_quota_info(self, member: discord.Member, type: str = None) -> dict:
        """Get quota information including percentage, bypass status, and reset time"""

        # Get quota and period
        max_quota = 0
        quota_period_weeks = 1
        watch_quota = 0  # Track watch requirement
        has_any_quota = False

        async with db.pool.acquire() as conn:
            for role in member.roles:
                quota_data = await conn.fetchrow(
                    'SELECT quota_seconds, quota_period_weeks, watch_quota FROM shift_quotas WHERE role_id = $1 AND type = $2',
                    role.id, type
                )
                if quota_data and quota_data['quota_seconds'] > 0:
                    has_any_quota = True
                    if quota_data['quota_seconds'] > max_quota:
                        max_quota = quota_data['quota_seconds']
                        quota_period_weeks = quota_data.get('quota_period_weeks', 1)
                        watch_quota = quota_data.get('watch_quota', 0)

        if not has_any_quota:
            return {
                'has_quota': False,
                'quota_seconds': 0,
                'active_seconds': 0,
                'percentage': 0,
                'completed': False,
                'bypass_type': None,
                'quota_period_weeks': 1,
                'watch_count': 0,
                'watch_quota': 0,
                'reset_timestamp': None
            }

        # ✅ FIX: Get active time AND watch count
        active_seconds, watch_count = await self.get_total_active_time_with_watches(
            member.id, type, quota_period_weeks
        )

        # Calculate when quota resets
        current_week = self.weekly_manager.get_current_week_monday()
        reset_date = current_week + timedelta(weeks=quota_period_weeks)
        reset_timestamp = int(reset_date.timestamp())

        # Check for bypass roles
        user_role_ids = {role.id for role in member.roles}

        if QUOTA_BYPASS_ROLE in user_role_ids:
            return {
                'has_quota': True,
                'quota_seconds': max_quota,
                'active_seconds': active_seconds,
                'percentage': 100,
                'completed': True,
                'bypass_type': 'QB',
                'quota_period_weeks': quota_period_weeks,
                'watch_count': watch_count,
                'watch_quota': watch_quota,
                'reset_timestamp': reset_timestamp
            }
        elif LOA_ROLE in user_role_ids:
            return {
                'has_quota': True,
                'quota_seconds': max_quota,
                'active_seconds': active_seconds,
                'percentage': 100,
                'completed': True,
                'bypass_type': 'LOA',
                'quota_period_weeks': quota_period_weeks,
                'watch_count': watch_count,
                'watch_quota': watch_quota,
                'reset_timestamp': reset_timestamp
            }

        # Special handling for 0-second quotas
        if max_quota == 0:
            return {
                'has_quota': True,
                'quota_seconds': 0,
                'active_seconds': active_seconds,
                'percentage': 100,
                'completed': True,
                'bypass_type': None,
                'quota_period_weeks': quota_period_weeks,
                'watch_count': watch_count,
                'watch_quota': watch_quota,
                'reset_timestamp': reset_timestamp
            }

        # Check for RA (Reduced Activity)
        if REDUCED_ACTIVITY_ROLE in user_role_ids:
            bypass_type = 'RA'
            modified_quota = max_quota * 0.5

            # ✅ FIX: Calculate percentage including watch completion
            watch_completed = (watch_count >= watch_quota) if watch_quota > 0 else True
            time_percentage = (active_seconds / modified_quota * 100) if modified_quota > 0 else 0

            # Must meet BOTH requirements
            completed = time_percentage >= 100 and watch_completed
            percentage = min(time_percentage, 100)
        else:
            bypass_type = None

            # ✅ FIX: Calculate percentage including watch completion
            watch_completed = (watch_count >= watch_quota) if watch_quota > 0 else True
            time_percentage = (active_seconds / max_quota * 100) if max_quota > 0 else 0

            # Must meet BOTH requirements
            completed = time_percentage >= 100 and watch_completed
            percentage = time_percentage

        return {
            'has_quota': True,
            'quota_seconds': max_quota,
            'modified_quota_seconds': modified_quota if bypass_type == 'RA' else max_quota,
            'active_seconds': active_seconds,
            'percentage': percentage,
            'completed': completed,
            'bypass_type': bypass_type,
            'quota_period_weeks': quota_period_weeks,
            'watch_count': watch_count,
            'watch_quota': watch_quota,
            'reset_timestamp': reset_timestamp
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
                         AND week_identifier = $2''',
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

    @shift_group.command(name="test_weekly_reset", description="[ADMIN] Test weekly reset (dev only - dry run)")
    async def test_weekly_reset(self, interaction: discord.Interaction):
        """Test command to simulate weekly reset WITHOUT making permanent changes"""
        await interaction.response.defer()

        if not self.has_super_admin_permission(interaction.user):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission for this command.",
                ephemeral=True
            )
            return

        try:
            # Get next wave number (READ ONLY)
            async with db.pool.acquire() as conn:
                max_wave = await conn.fetchval(
                    'SELECT MAX(wave_number) FROM shifts WHERE wave_number IS NOT NULL'
                )
                next_wave = (max_wave or 0) + 1

                # COUNT shifts that WOULD be archived (READ ONLY)
                current_week = self.weekly_manager.get_current_week_monday()

                completed_shift_count = await conn.fetchval(
                    '''SELECT COUNT(*)
                       FROM shifts
                       WHERE week_identifier = $1
                         AND wave_number IS NULL
                         AND end_time IS NOT NULL''',
                    current_week
                )

                # COUNT active shifts that WOULD be force-ended (READ ONLY)
                active_shift_count = await conn.fetchval(
                    'SELECT COUNT(*) FROM shifts WHERE end_time IS NULL'
                )

                # COUNT users that WOULD appear in report (READ ONLY)
                report_users = await conn.fetch(
                    '''SELECT DISTINCT discord_user_id, type
                       FROM shifts
                       WHERE wave_number IS NULL
                         AND end_time IS NOT NULL
                         AND week_identifier = $1''',
                    current_week
                )

            # Build detailed report
            embed = discord.Embed(
                title="📋 **Weekly Reset Test (Dry Run)**",
                description=f"**Simulating Wave {next_wave} Creation**\n"
                            f"*No permanent changes will be made*",
                color=discord.Color.blue()
            )

            embed.add_field(
                name="1️⃣ Archive Completed Shifts",
                value=f"Would archive **{completed_shift_count}** completed shifts to Wave {next_wave}",
                inline=False
            )

            embed.add_field(
                name="2️⃣ Force-End Active Shifts",
                value=f"Would force-end **{active_shift_count}** active shifts and assign to Wave {next_wave}",
                inline=False
            )

            # Group report users by type
            users_by_type = {}
            for user in report_users:
                type_name = user['type']
                if type_name not in users_by_type:
                    users_by_type[type_name] = []
                users_by_type[type_name].append(user['discord_user_id'])

            report_lines = []
            for type_name, user_ids in users_by_type.items():
                report_lines.append(f"**{type_name}:** {len(user_ids)} users")

            embed.add_field(
                name="3️⃣ Generate Weekly Report",
                value="\n".join(report_lines) if report_lines else "No users to report",
                inline=False
            )

            embed.add_field(
                name="Report Destination",
                value=f"Would send to channels: {', '.join([f'<#{ch}>' for ch in WEEKLY_REPORT_CHANNELS])}",
                inline=False
            )

            embed.set_footer(text="<:Warn:1437771973970104471> DRY RUN - No actual changes made to database")
            embed.timestamp = datetime.utcnow()

            
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error during test: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    # Replace the shift_quota command (around line 1068) with this:

    @shift_group.command(name="quota", description="View your quota (Leadership+ - Set Quota)")
    @app_commands.describe(
        action="What you want to do.",
        roles="The role(s) to set quota for (for set/remove actions)",
        type="The shift type to set quota for (for set/remove actions)"
    )
    @app_commands.choices(
        action=[
            app_commands.Choice(name="View My Quota", value="view"),
            app_commands.Choice(name="Set Quota", value="set"),
            app_commands.Choice(name="Remove Quota", value="remove"),
            app_commands.Choice(name="View All Quotas", value="view_all"),
            app_commands.Choice(name="Toggle Role Visibility in Reports", value="toggle_visibility")
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
            action: str,
            roles: str = None,
            type: app_commands.Choice[str] = None
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            if action == "view":
                # View user's own quota
                quota_info = await self.get_quota_info(interaction.user, type.value if type else None)

                if not quota_info['has_quota']:
                    embed = discord.Embed(
                        title="<:Search:1434957367505719457> Your Quota",
                        description="You don't have any quota requirements set.",
                        color=discord.Color(0x000000)
                    )
                    embed.set_author(
                        name=f"{interaction.guild.name}",
                        icon_url=interaction.guild.icon.url if interaction.guild.icon else None
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return

                embed = discord.Embed(
                    title="<:Search:1434957367505719457> Your Quota",
                    color=discord.Color(0x000000)
                )
                embed.set_author(
                    name="Shift Management",
                    icon_url=interaction.user.display_avatar.url
                )

                # Build status text based on bypass type
                if quota_info['bypass_type']:
                    if quota_info['bypass_type'] == 'RA':
                        status_text = f"<:Accepted:1426930333789585509> **{quota_info['percentage']:.1f}%** (RA - 50% Required)"
                    else:
                        status_text = f"<:Accepted:1426930333789585509> **100%** ({quota_info['bypass_type']} Bypass)"
                else:
                    status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                        'completed'] else "<:Denied:1426930694633816248>"
                    status_text = f"{status_emoji} **{quota_info['percentage']:.1f}%**"

                period_weeks = quota_info.get('quota_period_weeks', 1)
                period_text = f"{period_weeks} week{'s' if period_weeks != 1 else ''}"

                # Build description with shift time
                desc_parts = [
                    status_text,
                    f"**Required:** {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}"
                ]

                # Add watch requirement to the required line if applicable
                if quota_info.get('watch_quota', 0) > 0:
                    desc_parts[1] += f" + {quota_info['watch_quota']} watches per {period_text}"
                else:
                    desc_parts[1] += f" per {period_text}"

                desc_parts.append(
                    f"**Completed:** {self.format_duration(timedelta(seconds=quota_info['active_seconds']))}")

                # Add watch completion if applicable
                if quota_info.get('watch_quota', 0) > 0:
                    watch_emoji = "<:Accepted:1426930333789585509>" if quota_info['watch_count'] >= quota_info[
                        'watch_quota'] else "<:Denied:1426930694633816248>"
                    desc_parts.append(
                        f"**Watches:** {watch_emoji} {quota_info['watch_count']}/{quota_info['watch_quota']} hosted")

                # Add reset time
                if quota_info.get('reset_timestamp'):
                    desc_parts.append(f"\n**Resets:** <t:{quota_info['reset_timestamp']}:R>")

                embed.description = "\n".join(desc_parts)

                if type:
                    footer_text = f"Shift Type: {type.value}"
                    if type.value == "Shift FENZ" and quota_info.get('watch_quota', 0) > 0:
                        footer_text += " • Includes watch hosting requirement"
                    embed.set_footer(text=footer_text)

                await interaction.followup.send(embed=embed, ephemeral=True)

            elif action == "set":
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
                    except ValueError:
                        continue

                if not role_ids:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> No valid roles provided.",
                        ephemeral=True
                    )
                    return

                # Show period selection view
                view = QuotaPeriodSelectView(self, interaction.user, role_ids, type.value, interaction.guild)
                message = await interaction.followup.send(
                    "**Select Quota Period**\nHow often should this quota reset?",
                    view=view,
                    ephemeral=True
                )
                view.message = message

            elif action == "remove":
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

            elif action == "view_all":
                # View all quotas - ANYONE with shift access can use this
                user_types = await self.get_user_types(interaction.user)
                if not user_types:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> You don't have permission to view quotas.",
                        ephemeral=True
                    )
                    return

                # Check if user is admin (to show ignored roles)
                is_admin = any(role_check.id in QUOTA_ADMIN_ROLES for role_check in interaction.user.roles)

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

                    # Fetch ignored roles if admin
                    ignored_roles_data = []
                    if is_admin:
                        if type:
                            ignored_roles_data = await conn.fetch(
                                'SELECT role_id, type FROM quota_ignored_roles WHERE type = $1',
                                type.value
                            )
                        else:
                            ignored_roles_data = await conn.fetch(
                                'SELECT role_id, type FROM quota_ignored_roles'
                            )

                if not quotas:
                    await interaction.followup.send(
                        "No shift quotas have been set.",
                        ephemeral=True
                    )
                    return

                # Create ignored roles set
                ignored_set = {(r['role_id'], r['type']) for r in ignored_roles_data}

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
                    ignored_lines = []

                    for quota in type_quotas:
                        quota_role = interaction.guild.get_role(quota['role_id'])
                        if quota_role:
                            is_ignored = (quota['role_id'], type_name) in ignored_set

                            quota_display = self.cog.format_duration(timedelta(seconds=quota['quota_seconds']))

                            # Add watch requirement for FENZ roles
                            if type_name == "Shift FENZ" and quota.get('watch_quota', 0) > 0:
                                quota_display += f" + {quota['watch_quota']} watches"


                            if is_ignored:
                                if is_admin:
                                    ignored_lines.append(
                                        f"~~{quota_role.mention}~~ • {self.format_duration(timedelta(seconds=quota['quota_seconds']))} **(Hidden)**"
                                    )
                            else:
                                quota_lines.append(
                                    f"{quota_role.mention} • {self.format_duration(timedelta(seconds=quota['quota_seconds']))}"
                                )

                    # Add visible quotas
                    if quota_lines:
                        embed.add_field(
                            name=f"**{type_name}**",
                            value="\n".join(quota_lines),
                            inline=False
                        )

                    # Add ignored quotas (only for admins)
                    if ignored_lines and is_admin:
                        embed.add_field(
                            name=f"**{type_name} - Hidden from Reports**",
                            value="\n".join(ignored_lines),
                            inline=False
                        )

                if not embed.fields:
                    embed.description = "No valid roles found."

                if is_admin:
                    embed.set_footer(
                        text="Use 'Toggle Role Visibility in Reports' to show/hide roles from weekly leaderboards")

                await interaction.followup.send(embed=embed, ephemeral=True)

            elif action == "toggle_visibility":
                # Check admin permission
                if not any(role_check.id in QUOTA_ADMIN_ROLES for role_check in interaction.user.roles):
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> You don't have permission to toggle role visibility.",
                        ephemeral=True
                    )
                    return

                if not roles:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Please specify role(s) to toggle visibility for.",
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

                # Toggle visibility for each role
                async with db.pool.acquire() as conn:
                    toggled_on = []
                    toggled_off = []

                    for role_id in role_ids:
                        has_quota = await conn.fetchval(
                            'SELECT EXISTS(SELECT 1 FROM shift_quotas WHERE role_id = $1 AND type = $2)',
                            role_id, type.value
                        )

                        if not has_quota:
                            continue

                        is_ignored = await conn.fetchval(
                            'SELECT EXISTS(SELECT 1 FROM quota_ignored_roles WHERE role_id = $1 AND type = $2)',
                            role_id, type.value
                        )

                        role = interaction.guild.get_role(role_id)
                        if not role:
                            continue

                        if is_ignored:
                            await conn.execute(
                                'DELETE FROM quota_ignored_roles WHERE role_id = $1 AND type = $2',
                                role_id, type.value
                            )
                            toggled_on.append(role.mention)
                        else:
                            await conn.execute(
                                '''INSERT INTO quota_ignored_roles (role_id, type, added_by_id, added_by_name)
                                   VALUES ($1, $2, $3, $4)''',
                                role_id, type.value, interaction.user.id, interaction.user.display_name
                            )
                            toggled_off.append(role.mention)

                # Build response message
                response_parts = []

                if toggled_on:
                    response_parts.append(
                        f"**<:Accepted:1426930333789585509> Now Visible in {type.value} Reports:**\n" +
                        "\n".join(f"• {role}" for role in toggled_on)
                    )

                if toggled_off:
                    response_parts.append(
                        f"**<:No:1437788507111428228> Now Hidden from {type.value} Reports:**\n" +
                        "\n".join(f"• {role}" for role in toggled_off)
                    )

                if not response_parts:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> No roles with quotas found to toggle.",
                        ephemeral=True
                    )
                    return

                embed = discord.Embed(
                    title="Role Visibility Toggled",
                    description="\n\n".join(response_parts),
                    color=discord.Color(0x5865f2)
                )
                embed.set_footer(text=f"Hidden roles will not appear in weekly leaderboards for {type.value}")

                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @shift_group.command(name="leaderboard", description="View shift leaderboard")
    @app_commands.describe(
        type="Filter by shift type",
        wave="View current week or specific wave number (e.g., 1, 2, 3)"
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
                                 AND (wave_number = $1 OR round_number = $1)
                                 AND type = $2
                               GROUP BY discord_user_id, discord_username
                               ORDER BY total_seconds DESC LIMIT 25'''
                    results = await conn.fetch(query, wave, type.value)

                    week_start = await conn.fetchval(
                        '''SELECT MIN(week_identifier) 
                           FROM shifts 
                           WHERE (wave_number = $1 OR round_number = $1)''',
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
                wave_display = f"Wave {wave}" if wave is not None else "Current Wave"
                embed = discord.Embed(
                    title="Shift Leaderboard",
                    description=f"No shift data available for {wave_display}",
                    color=discord.Color(0x000000)
                )
                embed.set_author(
                    name=f"{interaction.guild.name}",
                    icon_url=interaction.guild.icon.url if interaction.guild.icon else None
                )
                await interaction.edit_original_response(embed=embed)
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

                quota_status = ""

                # ONLY show quota icons for current wave (wave is None)
                if wave is None:  # Current wave
                    quota_info = quota_infos.get(row['discord_user_id'])

                    if quota_info and quota_info.get('has_quota'):
                        if quota_info.get('bypass_type'):
                            quota_status = f" • <:Accepted:1426930333789585509> ({quota_info['bypass_type']})"
                        elif quota_info.get('completed'):
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

    @shift_leaderboard.autocomplete('wave')
    async def wave_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[int]]:
        """Autocomplete to suggest recent waves while allowing custom entry"""

        try:
            async with db.pool.acquire() as conn:
                # Get the last 25 waves (Discord's limit for autocomplete options)
                recent_waves = await conn.fetch(
                    '''SELECT DISTINCT 
                              COALESCE(wave_number, round_number) as wave_num,
                              MIN(week_identifier) as week_start
                       FROM shifts 
                       WHERE wave_number IS NOT NULL OR round_number IS NOT NULL
                       GROUP BY COALESCE(wave_number, round_number)
                       ORDER BY COALESCE(wave_number, round_number) DESC 
                       LIMIT 25'''
                )

            if not recent_waves:
                return []

            choices = []
            for wave in recent_waves:
                wave_num = wave['wave_num']
                week_start = wave['week_start']

                # Format the label with wave number and date
                if week_start:
                    week_end = week_start + timedelta(days=6)
                    label = f"Wave {wave_num} ({week_start.strftime('%d %b')} - {week_end.strftime('%d %b %Y')})"
                else:
                    label = f"Wave {wave_num}"

                # Truncate label if too long (Discord limit is 100 chars)
                if len(label) > 100:
                    label = label[:97] + "..."

                choices.append(app_commands.Choice(name=label, value=wave_num))

            # Filter by current input if provided
            if current:
                try:
                    # If user is typing a number, filter to waves containing that number
                    search_num = int(current)
                    filtered = [
                        choice for choice in choices
                        if str(search_num) in str(choice.value)
                    ]
                    if filtered:
                        return filtered
                except ValueError:
                    # Not a number, do string search on the label
                    pass

                # Fallback to string search
                return [
                    choice for choice in choices
                    if current.lower() in choice.name.lower()
                ]

            return choices

        except Exception as e:
            print(f"Error in wave autocomplete: {e}")
            return []

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
    async def shift_manage(self, interaction: discord.Interaction, type: str):
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

            # Validate the selected type
            if type not in types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> You don't have access to {type}.",
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
                await self.show_shift_statistics_panel(interaction, [type], show_last_shift=False)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @shift_manage.autocomplete('type')
    async def shift_type_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        """Autocomplete to show only shift types the user has access to"""

        # Get user's available shift types
        types = await self.get_user_types(interaction.user)

        # Create choices from available types
        choices = [app_commands.Choice(name=type, value=type) for type in types]

        # Filter by current input if any
        if current:
            return [
                choice for choice in choices
                if current.lower() in choice.name.lower()
            ]

        return choices

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
                    description='No active shifts.',
                    color=discord.Color(0xffffff)
                )

                embed.set_author(
                    name=interaction.guild.name,
                    icon_url=interaction.guild.icon.url if interaction.guild.icon else None
                )

                await interaction.followup.send(embed=embed)
                return

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
    @shift_group.command(name="admin", description="Manage shifts for users")
    @app_commands.describe(
        user="The user to manage shifts for",
        type="The shift type"
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
        # Rate limit check FIRST
        if not self.admin_rate_limiter.check(interaction.user.id):
            await interaction.response.send_message(
                "<:Alert:1437790206462922803> Rate limit exceeded. Please wait before using this command again.",
                ephemeral=True
            )
            return

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
            if quota_info.get('bypass_type'):
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

        embed.set_footer(text=f"Shift Type: {types[0]}")

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
                      f"**Break Started:** <t:{int(shift['pause_start'].timestamp())}:R>\n\n"
                      f"*Breaks over {LONG_BREAK_THRESHOLD_MINUTES} will automatically end your shift.*",  # ADD THIS LINE
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

                value_parts = [
                    f"**Status:** <:Offline:1434951694319620197> Ended",
                    f"**Total Time:** {self.format_duration(active_duration)}"
                ]

                # Only add break info if there was actually break time
                if pause_duration > 0:
                    value_parts.append(f"**Break Time:** {self.format_duration(timedelta(seconds=pause_duration))}")

                embed.add_field(
                    name="<:Clock:1434949269554597978> Last Shift",
                    value="\n".join(value_parts),
                    inline=False
                )

        embed.set_footer(text=f"Shift Type: {shift['type']}")

        # Create view with Start button for next shift
        types = await self.get_user_types(interaction.user)
        view = ShiftStartView(self, interaction.user, types)
        await interaction.edit_original_response(embed=embed, view=view)

    async def show_admin_shift_panel(self, interaction: discord.Interaction, user: discord.Member, type: str,
                                     show_last_shift: bool = False):
        """Show admin control panel for managing user's shift"""

        current_week = self.weekly_manager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            # Get active shift
            active_shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NULL
                   ORDER BY start_time DESC LIMIT 1''',
                user.id
            )

            shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND week_identifier = $2''',
                user.id, current_week
            )

            quota_seconds = 0
            for role in user.roles:
                result = await conn.fetchrow(
                    'SELECT quota_seconds FROM shift_quotas WHERE role_id = $1',
                    role.id
                )
                if result and result['quota_seconds'] > quota_seconds:
                    quota_seconds = result['quota_seconds']

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

        if not active_shift:
            embed = discord.Embed(
                title="<:Checklist:1434948670226432171> **All Time Information**",
                description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.format_duration(stats['total_duration'])}\n**Average Duration:** {self.format_duration(stats['average_duration'])}",
                color=discord.Color(0x000000)
            )
        else:
            is_on_break = active_shift.get('pause_start') is not None
            if is_on_break:
                embed = discord.Embed(
                    title="Shift Management",
                    color=discord.Color.gold()  # Yellow/gold for break
                )
            else:
                embed = discord.Embed(
                    title="Shift Management",
                    color=discord.Color.green()  # Green for active
                )

        embed.set_author(
            name=f"Shift Management: {user.display_name}",
            icon_url=user.display_avatar.url
        )

        # Add quota info if available
        if not active_shift:
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

            if show_last_shift:
                last_shift = await self.get_last_shift(user.id)
                if last_shift:
                    last_duration = last_shift['end_time'] - last_shift['start_time']
                    active_duration = last_duration - timedelta(seconds=last_shift.get('pause_duration', 0))
                    pause_duration = last_shift.get('pause_duration', 0)

                    value_parts = [
                        f"**Status:** <:Offline:1434951694319620197> Ended",
                        f"**Total Time:** {self.format_duration(active_duration)}"
                    ]

                    if pause_duration > 0:
                        value_parts.append(f"**Break Time:** {self.format_duration(timedelta(seconds=pause_duration))}")

                    embed.add_field(
                        name="<:Clock:1434949269554597978> Last Shift",
                        value="\n".join(value_parts),
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

    async def queue_modification_log(self, guild: discord.Guild, admin: discord.Member,
                                     target_user: discord.Member, shift: dict,
                                     modification_detail: str):
        """Queue a modification to be logged in batch after 5 minutes"""
        cache_key = (admin.id, target_user.id, shift['id'])
        current_time = datetime.utcnow()

        # Initialize cache entry if doesn't exist
        if cache_key not in self._modification_cache:
            self._modification_cache[cache_key] = {
                'modifications': [],
                'first_time': current_time,
                'shift': shift,
                'guild': guild,
                'admin': admin,
                'target_user': target_user
            }

        # Add modification to cache
        self._modification_cache[cache_key]['modifications'].append({
            'time': current_time,
            'detail': modification_detail
        })

        # Cancel existing timer if present
        if cache_key in self._modification_timers:
            self._modification_timers[cache_key].cancel()

        # Create new timer
        timer = asyncio.create_task(self._send_batched_modifications(cache_key))
        self._modification_timers[cache_key] = timer

    async def _send_batched_modifications(self, cache_key: tuple):
        """Send batched modifications after 5 minute delay"""
        await asyncio.sleep(300)  # 5 minutes

        if cache_key not in self._modification_cache:
            return

        cache_data = self._modification_cache.pop(cache_key)
        self._modification_timers.pop(cache_key, None)

        # Build modification details
        mod_lines = []
        for mod in cache_data['modifications']:
            mod_lines.append(f"• {mod['detail']}")

        # Create embed
        channel = self.bot.get_channel(SHIFT_LOGS_CHANNEL)
        if not channel:
            return

        # Get display name
        async with db.pool.acquire() as conn:
            callsign_row = await conn.fetchrow(
                'SELECT callsign, fenz_prefix FROM callsigns WHERE discord_user_id = $1',
                cache_data['target_user'].id
            )

        display_name = cache_data['target_user'].display_name
        if callsign_row:
            if callsign_row['fenz_prefix']:
                display_name = f"@{callsign_row['fenz_prefix']}-{callsign_row['callsign']}"
            else:
                display_name = f"@{callsign_row['callsign']}"

        embed = discord.Embed(
            title=f"Shift Modified • {cache_data['shift']['type'].replace('Shift ', '')}",
            color=discord.Color.orange()  # Orange
        )
        embed.add_field(
            name="Staff Member",
            value=f"{cache_data['target_user'].mention} • {display_name}",
            inline=False
        )
        embed.add_field(
            name="Modifications",
            value="\n".join(mod_lines),
            inline=False
        )
        embed.add_field(
            name="First Modification",
            value=f"<t:{int(cache_data['first_time'].timestamp())}:F>",
            inline=False
        )
        embed.set_thumbnail(url=cache_data['target_user'].display_avatar.url)
        embed.set_footer(
            text=f"Modified by {cache_data['admin'].display_name} • Shift ID: {cache_data['shift']['id']}"
        )
        embed.timestamp = datetime.utcnow()

        await channel.send(embed=embed)

    # Add this method to ShiftManagementCog class (around line 800)
    @tasks.loop(minutes=5)  # Check every 5 minutes
    async def check_long_breaks(self):
        """Automatically end shifts with breaks over threshold"""
        try:
            async with db.pool.acquire() as conn:
                # Find all shifts on break for longer than threshold
                threshold_time = datetime.utcnow() - timedelta(seconds=LONG_BREAK_THRESHOLD_SECONDS)

                long_breaks = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE pause_start IS NOT NULL
                         AND pause_start < $1
                         AND end_time IS NULL''',
                    threshold_time
                )

                for shift in long_breaks:
                    # Calculate final pause duration
                    pause_duration = shift.get('pause_duration', 0)
                    pause_duration += (datetime.utcnow() - shift['pause_start']).total_seconds()

                    # Close break session
                    current_sessions = await conn.fetchval(
                        'SELECT break_sessions FROM shifts WHERE id = $1',
                        shift['id']
                    )

                    sessions = json.loads(current_sessions) if current_sessions else []
                    if sessions and sessions[-1]['end'] is None:
                        final_duration = (datetime.utcnow() - shift['pause_start']).total_seconds()
                        sessions[-1]['end'] = datetime.utcnow().isoformat()
                        sessions[-1]['duration'] = final_duration

                    # End the shift
                    await conn.execute(
                        '''UPDATE shifts
                           SET end_time       = $1,
                               pause_duration = $2,
                               pause_start    = NULL,
                               break_sessions = $3
                           WHERE id = $4''',
                        datetime.utcnow(),
                        pause_duration,
                        json.dumps(sessions),
                        shift['id']
                    )

                    # Clean up roles/nicknames
                    guild = self.bot.get_guild(shift.get('guild_id'))
                    if guild:
                        member = guild.get_member(shift['discord_user_id'])
                        if member:
                            await self.update_nickname_for_shift_status(member, 'off')
                            await self.update_duty_roles(member, shift['type'], 'off')

                            # Log the auto-termination
                            completed_shift = dict(shift)
                            completed_shift['end_time'] = datetime.utcnow()
                            completed_shift['pause_duration'] = pause_duration

                            await self.log_shift_event(
                                guild,
                                'end',
                                member,
                                completed_shift,
                                admin=guild.me,  # ADD THIS - passes the bot as the admin
                                details=f"Auto-terminated: Break exceeded {LONG_BREAK_THRESHOLD_SECONDS // 60} minutes"
                            )

                    print(f"Auto-terminated shift {shift['id']} due to long break")

        except Exception as e:
            print(f"Error in check_long_breaks: {e}")
            import traceback
            traceback.print_exc()

    @check_long_breaks.before_loop
    async def before_check_long_breaks(self):
        """Wait until bot is ready before starting the task"""
        await self.bot.wait_until_ready()

    async def get_watch_hosting_count(self, user_id: int, weeks_back: int = 1) -> int:
        """
        Get number of watches hosted by user (for FENZ supervisors)
        Returns count of watches, NOT duration

        Args:
            user_id: Discord user ID
            weeks_back: Number of weeks to look back (for quota period)
        """
        try:
            from watches import load_completed_watches
            completed_watches = await load_completed_watches()

            # Calculate cutoff time
            cutoff_timestamp = int((datetime.utcnow() - timedelta(weeks=weeks_back)).timestamp())

            watch_count = 0

            for watch_id, watch_data in completed_watches.items():
                # Check if this user hosted the watch
                if watch_data.get('user_id') != user_id:
                    continue

                # Check if watch is within time period
                if watch_data.get('started_at', 0) < cutoff_timestamp:
                    continue

                # Only count successful watches
                if watch_data.get('status') == 'failed':
                    continue

                watch_count += 1

            return watch_count

        except Exception as e:
            print(f'Error getting watch hosting count: {e}')
            return 0

    async def get_total_active_time_with_watches(self, user_id: int, type: str, quota_period_weeks: int = 1) -> tuple[
        int, int]:
        """
        Get total active shift time + watch count (for FENZ only)

        Args:
            user_id: Discord user ID
            type: Shift type
            quota_period_weeks: Number of weeks in quota period

        Returns:
            Tuple of (shift_seconds, watch_count)
        """
        current_week = self.weekly_manager.get_current_week_monday()
        cutoff_week = current_week - timedelta(weeks=quota_period_weeks - 1)

        async with db.pool.acquire() as conn:
            if type:
                shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND type = $2
                         AND end_time IS NOT NULL
                         AND week_identifier >= $3''',
                    user_id, type, cutoff_week
                )
            else:
                shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND end_time IS NOT NULL
                         AND week_identifier >= $2''',
                    user_id, cutoff_week
                )

            total_seconds = 0
            for shift in shifts:
                duration = shift['end_time'] - shift['start_time']
                active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))
                total_seconds += active_duration.total_seconds()

        # Get watch count for FENZ only
        watch_count = 0
        if type == "Shift FENZ":
            watch_count = await self.get_watch_hosting_count(user_id, quota_period_weeks)
            print(f"User {user_id} FENZ quota: {int(total_seconds)}s shifts + {watch_count} watches")

        return int(total_seconds), watch_count

class QuotaConflictView(discord.ui.View):
    """Confirmation view for overwriting existing quotas"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, role_ids: list,
                 quota_seconds: int, type: str, period_weeks: int = 1):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.role_ids = role_ids
        self.quota_seconds = quota_seconds
        self.type = type
        self.period_weeks = period_weeks
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
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Overwriting",
                                                ephemeral=True)

        try:
            async with db.pool.acquire() as conn:
                role_mentions = []
                for role_id in self.role_ids:
                    await conn.execute(
                        '''INSERT INTO shift_quotas (role_id, quota_seconds, type, quota_period_weeks)
                           VALUES ($1, $2, $3, $4) ON CONFLICT (role_id, type) 
                           DO UPDATE SET quota_seconds = $2, quota_period_weeks = $4''',
                        role_id, self.quota_seconds, self.type, self.period_weeks
                    )
                    role = interaction.guild.get_role(role_id)
                    if role:
                        role_mentions.append(role.mention)

            period_text = f"{self.period_weeks} week{'s' if self.period_weeks != 1 else ''}"
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Updated quota for {', '.join(role_mentions)} to {self.cog.format_duration(timedelta(seconds=self.quota_seconds))} over {period_text} ({self.type})",
                ephemeral=True
            )

            # Disable buttons and edit only if the message still exists
            for item in self.children:
                item.disabled = True

            try:
                await interaction.message.edit(view=self)
            except discord.NotFound:
                pass
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
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Cancelling",
                                                ephemeral=True)
        await interaction.followup.send("Cancelled.", ephemeral=True)

        for item in self.children:
            item.disabled = True

        try:
            await interaction.message.edit(view=self)
        except discord.NotFound:
            pass
        except discord.HTTPException:
            pass

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
                # Get current break_sessions
                current_sessions = await conn.fetchval(
                    'SELECT break_sessions FROM shifts WHERE id = $1',
                    self.shift['id']
                )

                # Parse existing sessions (handle None case)
                sessions = json.loads(current_sessions) if current_sessions else []

                # Add new break session with start time (no end yet)
                sessions.append({
                    'start': datetime.utcnow().isoformat(),
                    'end': None,
                    'duration': None
                })

                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start    = $1,
                           break_sessions = $2
                       WHERE id = $3''',
                    datetime.utcnow(),
                    json.dumps(sessions),
                    self.shift['id']
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

            async with db.pool.acquire() as conn:
                current_sessions = await conn.fetchval(
                    'SELECT break_sessions FROM shifts WHERE id = $1',
                    self.shift['id']
                )

                sessions = json.loads(current_sessions) if current_sessions else []

                # If there's an open break, close it
                if sessions and sessions[-1]['end'] is None:
                    final_duration = (datetime.utcnow() - self.shift['pause_start']).total_seconds()
                    sessions[-1]['end'] = datetime.utcnow().isoformat()
                    sessions[-1]['duration'] = final_duration

                    # Calculate pause_duration INCLUDING this final break
                    pause_duration = self.shift.get('pause_duration', 0) + final_duration

                    # Update break_sessions AND pause_duration in ONE query
                    await conn.execute(
                        '''UPDATE shifts
                           SET break_sessions = $1,
                               pause_duration = $2
                           WHERE id = $3''',
                        json.dumps(sessions),
                        pause_duration,
                        self.shift['id']
                    )
                else:
                    # No open break, just use existing pause_duration
                    pause_duration = self.shift.get('pause_duration', 0)

            # NOW end the shift with correct pause_duration
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time    = $1,
                           pause_start = NULL
                       WHERE id = $2''',
                    datetime.utcnow(),
                    self.shift['id']
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
                # Get current break_sessions
                current_sessions = await conn.fetchval(
                    'SELECT break_sessions FROM shifts WHERE id = $1',
                    self.shift['id']
                )

                # Parse and update the last session
                sessions = json.loads(current_sessions) if current_sessions else []
                if sessions and sessions[-1]['end'] is None:
                    sessions[-1]['end'] = datetime.utcnow().isoformat()
                    sessions[-1]['duration'] = pause_duration

                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start    = NULL,
                           pause_duration = $1,
                           break_sessions = $2
                       WHERE id = $3''',
                    total_pause,
                    json.dumps(sessions),
                    self.shift['id']
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
            total_break = timedelta(seconds=pause_duration)
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

            async with db.pool.acquire() as conn:
                current_sessions = await conn.fetchval(
                    'SELECT break_sessions FROM shifts WHERE id = $1',
                    self.shift['id']
                )

                sessions = json.loads(current_sessions) if current_sessions else []

                # If there's an open break, close it
                if sessions and sessions[-1]['end'] is None:
                    final_duration = (datetime.utcnow() - self.shift['pause_start']).total_seconds()
                    sessions[-1]['end'] = datetime.utcnow().isoformat()
                    sessions[-1]['duration'] = final_duration

                    # Calculate pause_duration INCLUDING this final break
                    pause_duration = self.shift.get('pause_duration', 0) + final_duration

                    # Update break_sessions AND pause_duration in ONE query
                    await conn.execute(
                        '''UPDATE shifts
                           SET break_sessions = $1,
                               pause_duration = $2
                           WHERE id = $3''',
                        json.dumps(sessions),
                        pause_duration,
                        self.shift['id']
                    )
                else:
                    # No open break, just use existing pause_duration
                    pause_duration = self.shift.get('pause_duration', 0)

            # NOW end the shift with correct pause_duration
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time    = $1,
                           pause_start = NULL
                       WHERE id = $2''',
                    datetime.utcnow(),
                    self.shift['id']
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
            total_break = timedelta(seconds=pause_duration)
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

        # Add a button for each shift type - THIS MUST STAY INSIDE __init__
        for type in types:
            button = discord.ui.Button(
                label=type,
                style=discord.ButtonStyle.primary,
                custom_id=f"type_{type}"
            )
            button.callback = self.create_callback(type)
            self.add_item(button)

    # UNINDENT THESE METHODS - they should be at class level, not inside __init__
    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

    def create_callback(self, type: str):
        async def callback(interaction: discord.Interaction):
            await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Checking Shifts",
                                                    ephemeral=True)

            # Verify user still has access to this shift type
            user_types = await self.cog.get_user_types(interaction.user)

            if type not in user_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> You don't have access to **{type}** shifts!",
                    ephemeral=True
                )
                return

            current_week = WeeklyShiftManager.get_current_week_monday()

            # Start the shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO shifts
                       (discord_user_id, discord_username, type, start_time, pause_duration, week_identifier, guild_id)
                       VALUES ($1, $2, $3, $4, 0, $5, $6)''',
                    self.user.id, str(self.user), type, datetime.utcnow(),
                    current_week, interaction.guild.id
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

        # Add buttons for each type - THIS STAYS INSIDE __init__
        for type in types:
            button = discord.ui.Button(
                label=type,
                style=discord.ButtonStyle.primary,
                custom_id=f"admin_type_{type}"
            )
            button.callback = self.create_callback(type)
            self.add_item(button)

    # UNINDENT THESE - they're class methods, not nested in __init__
    async def on_timeout(self):
        """Clean up when view times out"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass

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
        if cog.has_senior_admin_permission(admin):
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
                resume_btn = discord.ui.Button(label="Resume", style=discord.ButtonStyle.success, emoji="<:Play:1434957147829047467>")
                resume_btn.callback = self.resume_callback
                self.add_item(resume_btn)
            else:
                # Pause button
                pause_btn = discord.ui.Button(label="Pause", style=discord.ButtonStyle.primary, emoji="<:Pause:1434982402593390632>")
                pause_btn.callback = self.pause_callback
                self.add_item(pause_btn)

            # Stop button
            stop_btn = discord.ui.Button(label="Stop", style=discord.ButtonStyle.danger, emoji="<:Reset:1434959478796714074>")
            stop_btn.callback = self.stop_callback
            self.add_item(stop_btn)
        else:
            # Start button
            start_btn = discord.ui.Button(label="Start", style=discord.ButtonStyle.success, emoji="<:Play:1434957147829047467>")
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

            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def pause_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            if not self.active_shift:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> No active shift found.",
                    ephemeral=True
                )
                return

            async with db.pool.acquire() as conn:
                # Get current break_sessions
                current_sessions = await conn.fetchval(
                    'SELECT break_sessions FROM shifts WHERE id = $1',
                    self.active_shift['id']
                )

                # Parse existing sessions
                sessions = json.loads(current_sessions) if current_sessions else []

                # Add new break session
                sessions.append({
                    'start': datetime.utcnow().isoformat(),
                    'end': None,
                    'duration': None
                })

                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start    = $1,
                           break_sessions = $2
                       WHERE id = $3''',
                    datetime.utcnow(),
                    json.dumps(sessions),
                    self.active_shift['id']
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
            if not self.active_shift:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> No active shift found.",
                    ephemeral=True
                )
                return

            pause_duration = (datetime.utcnow() - self.active_shift['pause_start']).total_seconds()
            total_pause = self.active_shift.get('pause_duration', 0) + pause_duration

            async with db.pool.acquire() as conn:
                # Get current break_sessions
                current_sessions = await conn.fetchval(
                    'SELECT break_sessions FROM shifts WHERE id = $1',
                    self.active_shift['id']
                )

                # Parse and update the last session
                sessions = json.loads(current_sessions) if current_sessions else []
                if sessions and sessions[-1]['end'] is None:
                    sessions[-1]['end'] = datetime.utcnow().isoformat()
                    sessions[-1]['duration'] = pause_duration

                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start    = NULL,
                           pause_duration = $1,
                           break_sessions = $2
                       WHERE id = $3''',
                    total_pause,
                    json.dumps(sessions),
                    self.active_shift['id']
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
            # ADD THIS: Send temporary status message
            status_msg = await interaction.followup.send(
                "<a:Load:1430912797469970444> Stopping shift...",
                ephemeral=True
            )
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

            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type, show_last_shift=True)
            try:
                await status_msg.delete()
            except:
                pass

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
            discord.SelectOption(label="View Shift List", description="View all shifts for this user, all time.", emoji="<:List:1434953240155525201>"),
            discord.SelectOption(label="Modify Shift", description="Add, remove, set or reset shift duration.", emoji="<:Modify:1434954278362939632>"),
            discord.SelectOption(label="Delete Shift", description="Delete a shift", emoji="<:Reset:1434959478796714074>"),
            discord.SelectOption(label="Clear User Shifts", description="Clear all shifts in this shift type for this user.", emoji="<:Wipe:1434954284851658762>")
        ]

        super().__init__(placeholder="Shift Actions", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        selection = self.values[0]

        if selection == "View Shift List":
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

        # Create embed for selection panel
        embed = discord.Embed(
            title="**Modify Shift**",
            description="Select a shift to modify.",
            color=discord.Color(0x000000)
        )

        embed.set_author(
            name=f"Shift Management: @{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        embed.set_footer(text=f"Shift Type: {self.shift_type}")

        # Create view and populate dropdown
        view = ModifyShiftSelectView(self.cog, self.admin, self.target_user, self.shift_type)
        await view.populate_shift_dropdown()

        message = await interaction.followup.send(
            embed=embed,
            view=view,
            ephemeral=True
        )
        view.message = message

    async def show_delete_shift(self, interaction: discord.Interaction):
        """Show delete shift interface"""

        # Create embed for selection panel
        embed = discord.Embed(
            title="**Delete Shift**",
            description="Select a shift to delete.",
            color=discord.Color(0x000000)
        )

        embed.set_author(
            name=f"Shift Management: @{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        embed.set_footer(text=f"Shift Type: {self.shift_type}")

        # Create view and populate dropdown
        view = DeleteShiftSelectView(self.cog, self.admin, self.target_user, self.shift_type)
        await view.populate_shift_dropdown()

        message = await interaction.followup.send(
            embed=embed,
            view=view,
            ephemeral=True
        )
        view.message = message

    async def show_clear_shifts(self, interaction: discord.Interaction):
        """Show clear shifts interface with scope selection"""

        # Get counts for each scope
        current_week = WeeklyShiftManager.get_current_week_monday()

        async with db.pool.acquire() as conn:
            # Current wave count (no wave_number assigned yet)
            current_wave_count = await conn.fetchval(
                '''SELECT COUNT(*)
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2
                     AND wave_number IS NULL
                     AND week_identifier = $3''',
                self.target_user.id, self.shift_type, current_week
            )

            # Get max wave number
            max_wave = await conn.fetchval(
                'SELECT MAX(wave_number) FROM shifts WHERE wave_number IS NOT NULL'
            )

            # All time count
            all_time_count = await conn.fetchval(
                '''SELECT COUNT(*)
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2''',
                self.target_user.id, self.shift_type
            )

        embed = discord.Embed(
            title="Clear User Shifts",
            description="Select which shifts to clear:",
            color=discord.Color(0x000000)
        )

        embed.set_author(
            name=f"Shift Management: @{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        embed.add_field(
            name="Current Wave",
            value=f"{current_wave_count} shifts",
            inline=True
        )

        embed.add_field(
            name=f"Previous Waves",
            value=f"Waves 1-{max_wave if max_wave else 0}",
            inline=True
        )

        embed.add_field(
            name="All Time",
            value=f"{all_time_count} shifts",
            inline=True
        )

        embed.set_footer(text=f"Shift Type: {self.shift_type}")

        view = ClearShiftsScopeView(
            self.cog,
            self.admin,
            self.target_user,
            self.shift_type,
            current_wave_count,
            all_time_count,
            max_wave
        )

        # Try to edit original response
        try:
            await interaction.edit_original_response(embed=embed, view=view)
            view.message = interaction.message
        except:
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = message

class PageJumpModal(discord.ui.Modal):
    """Modal for jumping to specific page"""

    def __init__(self, view, total_pages: int):
        super().__init__(title="Jump to Page")
        self.view = view
        self.total_pages = total_pages

        self.add_item(discord.ui.TextInput(
            label=f"Page Number (1-{total_pages})",
            placeholder=f"Enter page number",
            required=True,
            max_length=5
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            page_num = int(self.children[0].value)

            if page_num < 1 or page_num > self.total_pages:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Page must be between 1 and {self.total_pages}.",
                    ephemeral=True
                )
                return

            # Convert to 0-indexed and show page
            await self.view.show_page(interaction, page_num - 1)

        except ValueError:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please enter a valid page number.",
                ephemeral=True
            )

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
        """Fetch all completed shifts (all time)"""
        async with db.pool.acquire() as conn:
            self.shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND type = $2
                     AND end_time IS NOT NULL
                   ORDER BY end_time DESC''',
                self.target_user.id, self.type
            )
        self.total_pages = max(1, math.ceil(len(self.shifts) / self.ITEMS_PER_PAGE))

    def _create_navigation_buttons(self):
        """Create navigation buttons based on current page"""
        # Clear existing buttons
        self.clear_items()

        # First page button - only show if not on first page AND more than one page
        if self.current_page > 0 and self.total_pages > 1:
            first_btn = discord.ui.Button(
                emoji="<:LeftSkip:1434962162064822343>",
                style=discord.ButtonStyle.secondary,
                custom_id="first"
            )
            first_btn.callback = self.first_button_callback
            self.add_item(first_btn)

        # Previous button - only show if not on first page AND more than one page
        if self.current_page > 0 and self.total_pages > 1:
            prev_btn = discord.ui.Button(
                emoji="<:LeftArrow:1434962165215002777>",
                style=discord.ButtonStyle.secondary,
                custom_id="prev"
            )
            prev_btn.callback = self.prev_button_callback
            self.add_item(prev_btn)

        # Page indicator (only show if multiple pages)
        if self.total_pages > 1:
            page_btn = discord.ui.Button(
                label=f"{self.current_page + 1}/{self.total_pages}",
                style=discord.ButtonStyle.primary,
                custom_id="page"
            )
            page_btn.callback = self.page_jump_callback
            self.add_item(page_btn)

        # Next button - only show if not on last page AND more than one page
        if self.current_page < self.total_pages - 1 and self.total_pages > 1:
            next_btn = discord.ui.Button(
                emoji="<:RightArrow:1434962170147246120>",
                style=discord.ButtonStyle.secondary,
                custom_id="next"
            )
            next_btn.callback = self.next_button_callback
            self.add_item(next_btn)

        # Last page button - only show if not on last page AND more than one page
        if self.current_page < self.total_pages - 1 and self.total_pages > 1:
            last_btn = discord.ui.Button(
                emoji="<:RightSkip:1434962167660281926>",
                style=discord.ButtonStyle.secondary,
                custom_id="last"
            )
            last_btn.callback = self.last_button_callback
            self.add_item(last_btn)

        # ALWAYS show return button
        return_btn = discord.ui.Button(
            label="Return",
            emoji="<:Denied:1426930694633816248>",
            style=discord.ButtonStyle.secondary,
            custom_id="return"
        )
        return_btn.callback = self.return_button_callback
        self.add_item(return_btn)

    async def show_page(self, interaction: discord.Interaction, page: int):
        await self.get_shifts()

        if not self.shifts:
            embed = discord.Embed(
                title=f"**<:List:1434953240155525201> Shift List**",
                description="No completed shifts found.",
                color=discord.Color(0xffffff)
            )
            embed.set_author(
                name=f"@{self.target_user.display_name}",
                icon_url=self.target_user.display_avatar.url
            )
            embed.set_footer(text=f"Shift Type: {self.type}")

            # Add return button even when no shifts
            self._create_navigation_buttons()

            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)

            if self.message:
                await self.message.edit(embed=embed, view=self)
            else:
                self.message = await interaction.followup.send(embed=embed, view=self, ephemeral=True)
            return

        self.current_page = page
        start_idx = page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_shifts = self.shifts[start_idx:end_idx]

        embed = discord.Embed(
            title=f"**<:List:1434953240155525201> Shift List**",
            description="",
            color=discord.Color(0xffffff)
        )

        embed.set_author(
            name=f"@{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        for idx, shift in enumerate(page_shifts, start=start_idx + 1):
            duration = shift['end_time'] - shift['start_time']
            active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

            embed.add_field(
                name=f"`{idx}.` Shift ID: {shift['id']}",
                value=f"**Status:** <:Offline:1434951694319620197> Ended\n"
                      f"**Duration:** {self.cog.format_duration(active_duration)}\n"
                      f"**Ended:** <t:{int(shift['end_time'].timestamp())}:R>",
                inline=False
            )

        embed.set_footer(text=f"Page {self.current_page + 1}/{self.total_pages} • Shift Type: {self.type}")

        self._create_navigation_buttons()

        if self.message:
            await self.message.edit(embed=embed, view=self)
        else:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            self.message = await interaction.followup.send(embed=embed, view=self, ephemeral=True)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    # CHANGE ALL BUTTON METHODS TO CALLBACKS
    async def first_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.show_page(interaction, 0)

    async def prev_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.show_page(interaction, self.current_page - 1)

    async def next_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.show_page(interaction, self.current_page + 1)

    async def last_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.show_page(interaction, self.total_pages - 1)

    async def page_jump_callback(self, interaction: discord.Interaction):
        """Show modal for jumping to specific page"""
        modal = PageJumpModal(self, self.total_pages)
        await interaction.response.send_modal(modal)

    async def return_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        # Delete the shift list
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        self.stop()


class ResetTimeConfirmModal(discord.ui.Modal):
    """Modal for confirming reset time action"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict):
        super().__init__(title="Reset Shift Time")
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift

        self.add_item(discord.ui.TextInput(
            label="Type CEEBS to reset shift time to 0",
            placeholder="CEEBS",
            required=True,
            max_length=7
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        confirmation = self.children[0].value.strip().upper()

        if confirmation != "CEEBS":
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You must type CEEBS to reset shift time.",
                ephemeral=True
            )
            return


        try:
            # Set shift duration to 0 by making start_time = end_time
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET start_time     = end_time,
                           pause_duration = 0
                       WHERE id = $1''',
                    self.shift['id']
                )

            # Log the modification
            await self.cog.queue_modification_log(
                interaction.guild,
                self.admin,
                self.target_user,
                self.shift,
                "Reset shift time to 0"
            )

            

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Reset shift time to 0 for {self.target_user.mention} (Shift ID: {self.shift['id']})",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

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

    async def populate_shift_dropdown(self):
        """Add a dropdown with recent shifts, then add buttons below"""
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

        # Add dropdown FIRST if we have shifts
        if recent_shifts:
            options = []
            for shift in recent_shifts:
                shift_id = str(shift['id'])
                shift_time = shift['end_time'].strftime('%a, %d %b %Y %H:%M:%S GMT UTC')
                label = f"{shift_id} | {shift_time}"

                if len(label) > 100:
                    label = label[:97] + "..."

                options.append(discord.SelectOption(
                    label=label,
                    value=str(shift['id']),
                    description=f"Duration: {self.cog.format_duration(shift['end_time'] - shift['start_time'])}"[:100]
                ))

            # Create and add the select menu
            select = discord.ui.Select(
                placeholder="Or select from recent shifts...",
                options=options,
                custom_id="shift_select"
            )
            select.callback = self.shift_select_callback
            self.add_item(select)

        # NOW add buttons BELOW the dropdown
        most_recent_btn = discord.ui.Button(
            label="Most Recent",
            emoji="<:Play:1434957147829047467>",
            style=discord.ButtonStyle.primary,
            custom_id="most_recent"
        )
        most_recent_btn.callback = self.most_recent_callback
        self.add_item(most_recent_btn)

        search_btn = discord.ui.Button(
            label="Search by Shift ID",
            emoji="<:Search:1434957367505719457>",
            style=discord.ButtonStyle.secondary,
            custom_id="search"
        )
        search_btn.callback = self.search_callback
        self.add_item(search_btn)

        # ADD CANCEL BUTTON
        cancel_btn = discord.ui.Button(
            label="Cancel",
            emoji="<:Denied:1426930694633816248>",
            style=discord.ButtonStyle.secondary,
            custom_id="cancel"
        )
        cancel_btn.callback = self.cancel_callback
        self.add_item(cancel_btn)

    async def most_recent_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT * FROM shifts 
                   WHERE discord_user_id = $1 
                     AND type = $2
                     AND end_time IS NOT NULL 
                   ORDER BY end_time DESC LIMIT 1''',
                self.target_user.id, self.type
            )

        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No completed shifts found.",
                ephemeral=True
            )
            return

        shift_dict = dict(shift)
        await self.show_modify_panel(interaction, shift_dict)

    async def search_callback(self, interaction: discord.Interaction):
        modal = ShiftIDModal(self.cog, self.admin, self.target_user, "modify", self)
        await interaction.response.send_modal(modal)

    async def shift_select_callback(self, interaction: discord.Interaction):
        """Handle shift selection from dropdown"""
        await interaction.response.defer()

        shift_id = int(interaction.data['values'][0])

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
        await self.show_modify_panel(interaction, shift_dict)

    async def cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        # Delete the selection panel
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        self.stop()

    async def show_modify_panel(self, interaction: discord.Interaction, shift: dict):
        """Show the modify options for a shift"""
        duration = shift['end_time'] - shift['start_time']
        active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

        embed = discord.Embed(
            title="Modify Shift",
            description=f"**Status:** <:Offline:1434951694319620197> Ended\n**Duration:** {self.cog.format_duration(active_duration)}",
            color=discord.Color(0x000000)
        )

        embed.set_author(
            name=f"Shift Management: @{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        embed.set_footer(text=f"Shift ID: {shift['id']} • Shift Type: {self.type}")

        view = ModifyShiftActionsView(self.cog, self.admin, self.target_user, shift)

        # Edit the original message instead of sending new one
        if self.message:
            await self.message.edit(embed=embed, view=view)
            view.message = self.message
        else:
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = message
            

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
    async def add_time_button(self, interaction, button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "add", view=self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Remove Time", emoji="<:Remove:1434959215830499470>", style=discord.ButtonStyle.danger)
    async def remove_time_button(self, interaction, button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "remove", view=self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Time", emoji="<:Set:1434959334273712219>", style=discord.ButtonStyle.primary)
    async def set_time_button(self, interaction, button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "set", view=self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Reset Time", emoji="<:Reset:1434959478796714074>", style=discord.ButtonStyle.secondary)
    async def reset_time_button(self, interaction, button):
        modal = ResetTimeConfirmModal(self.cog, self.admin, self.target_user, self.shift)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="<:Denied:1426930694633816248>")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Delete the modify panel and return to admin panel
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift['type'])
        self.stop()

class TimeModifyModal(discord.ui.Modal):
    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict,
                 action: str, view=None):  # ADD view parameter with default
        super().__init__(title=f"{action.capitalize()} Time")
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift
        self.action = action
        self.view = view  # STORE the view


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
        await interaction.response.defer(ephemeral=True)

        try:
            hours = int(self.children[0].value or 0)
            minutes = int(self.children[1].value or 0)
            seconds = int(self.children[2].value or 0) if len(self.children) > 2 else 0

            # Validate
            valid, error_msg = validate_time_input(hours, minutes, seconds)
            if not valid:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {error_msg}",
                    ephemeral=True
                )
                return

            time_delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)

            if self.action == "set":
                # Calculate new start time to achieve desired duration
                new_start_time = self.shift['end_time'] - time_delta

                # Make sure start time isn't in the future
                if new_start_time > datetime.utcnow():
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Cannot set duration that would result in future start time.",
                        ephemeral=True
                    )
                    return

                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE shifts
                           SET start_time = $1,
                               pause_duration = 0
                           WHERE id = $2''',
                        new_start_time, self.shift['id']
                    )

                # Log the modification
                await self.cog.queue_modification_log(
                    interaction.guild,
                    self.admin,
                    self.target_user,
                    self.shift,
                    f"Set duration to {self.cog.format_duration(time_delta)}"
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

                await self.cog.queue_modification_log(
                    interaction.guild,
                    self.admin,
                    self.target_user,
                    self.shift,
                    f"Added {self.cog.format_duration(time_delta)} to shift"
                )

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Added {self.cog.format_duration(time_delta)} to shift for {self.target_user.mention}",
                    ephemeral=True
                )

            elif self.action == "remove":
                # Add time to pause_duration (which decreases active time)
                current_pause = self.shift.get('pause_duration', 0)
                total_duration = (self.shift['end_time'] - self.shift['start_time']).total_seconds()
                new_pause = current_pause + time_delta.total_seconds()

                # Make sure we don't remove more time than exists
                if new_pause >= total_duration:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Cannot remove more time than the shift's total duration.",
                        ephemeral=True
                    )
                    return

                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE shifts
                           SET pause_duration = $1
                           WHERE id = $2''',
                        new_pause, self.shift['id']
                    )

                await self.cog.queue_modification_log(
                    interaction.guild,
                    self.admin,
                    self.target_user,
                    self.shift,
                    f"Removed {self.cog.format_duration(time_delta)} from shift"
                )

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Removed {self.cog.format_duration(time_delta)} from shift for {self.target_user.mention}",
                    ephemeral=True
                )

            # UPDATE THE MODIFY PANEL EMBED WITH FRESH DATA
            if self.view and hasattr(self.view, 'message') and self.view.message:
                try:
                    # Get fresh shift data from database
                    async with db.pool.acquire() as conn:
                        fresh_shift = await conn.fetchrow('SELECT * FROM shifts WHERE id = $1', self.shift['id'])

                    if fresh_shift:
                        fresh_shift = dict(fresh_shift)
                        duration = fresh_shift['end_time'] - fresh_shift['start_time']
                        active_duration = duration - timedelta(seconds=fresh_shift.get('pause_duration', 0))

                        # Create updated embed
                        embed = discord.Embed(
                            title="Modify Shift",
                            description=f"**Status:** <:Offline:1434951694319620197> Ended\n**Duration:** {self.cog.format_duration(active_duration)}",
                            color=discord.Color(0x000000)
                        )
                        embed.set_author(
                            name=f"Shift Management: @{self.target_user.display_name}",
                            icon_url=self.target_user.display_avatar.url
                        )
                        embed.set_footer(text=f"Shift ID: {fresh_shift['id']} • Shift Type: {fresh_shift['type']}")

                        # Create new view with updated shift data
                        new_view = ModifyShiftActionsView(self.cog, self.admin, self.target_user, fresh_shift)
                        new_view.message = self.view.message

                        # Edit the original message
                        await self.view.message.edit(embed=embed, view=new_view)

                except discord.NotFound:
                    pass  # Message was deleted
                except discord.HTTPException as e:
                    print(f"Failed to edit message: {e}")

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
            import traceback
            traceback.print_exc()

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

    async def populate_shift_dropdown(self):
        """Add a dropdown with recent shifts, then add buttons below"""
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

        # Add dropdown FIRST if we have shifts
        if recent_shifts:
            options = []
            for shift in recent_shifts:
                shift_id = str(shift['id'])
                shift_time = shift['end_time'].strftime('%a, %d %b %Y %H:%M:%S GMT UTC')
                label = f"{shift_id} | {shift_time}"

                if len(label) > 100:
                    label = label[:97] + "..."

                options.append(discord.SelectOption(
                    label=label,
                    value=str(shift['id']),
                    description=f"Duration: {self.cog.format_duration(shift['end_time'] - shift['start_time'])}"[:100]
                ))

            # Create and add the select menu
            select = discord.ui.Select(
                placeholder="Or select from recent shifts...",
                options=options,
                custom_id="shift_select"
            )
            select.callback = self.shift_select_callback
            self.add_item(select)

        # NOW add buttons BELOW the dropdown
        most_recent_btn = discord.ui.Button(
            label="Most Recent",
            emoji="<:Play:1434957147829047467>",
            style=discord.ButtonStyle.primary,
            custom_id="most_recent"
        )
        most_recent_btn.callback = self.most_recent_callback
        self.add_item(most_recent_btn)

        search_btn = discord.ui.Button(
            label="Search by Shift ID",
            emoji="<:Search:1434957367505719457>",
            style=discord.ButtonStyle.secondary,
            custom_id="search"
        )
        search_btn.callback = self.search_callback
        self.add_item(search_btn)

        # ADD CANCEL BUTTON
        cancel_btn = discord.ui.Button(
            label="Cancel",
            emoji="<:Denied:1426930694633816248>",
            style=discord.ButtonStyle.secondary,
            custom_id="cancel"
        )
        cancel_btn.callback = self.cancel_callback
        self.add_item(cancel_btn)

    async def most_recent_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT * FROM shifts 
                   WHERE discord_user_id = $1 
                     AND type = $2
                     AND end_time IS NOT NULL 
                   ORDER BY end_time DESC LIMIT 1''',
                self.target_user.id, self.type
            )

        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No completed shifts found.",
                ephemeral=True
            )
            return

        shift_dict = dict(shift)
        await self.show_delete_confirm(interaction, shift_dict)

    async def search_callback(self, interaction: discord.Interaction):
        modal = ShiftIDModal(self.cog, self.admin, self.target_user, "delete", self)
        await interaction.response.send_modal(modal)

    async def shift_select_callback(self, interaction: discord.Interaction):
        """Handle shift selection from dropdown"""
        await interaction.response.defer()

        shift_id = int(interaction.data['values'][0])

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
        await self.show_delete_confirm(interaction, shift_dict)

    async def cancel_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        # Delete the selection panel
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        self.stop()

    async def show_delete_confirm(self, interaction: discord.Interaction, shift: dict):
        """Show delete confirmation"""
        duration = shift['end_time'] - shift['start_time']
        active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

        embed = discord.Embed(
            title=f"Delete Shift",
            description=f"Are you sure you want to delete this shift?\n"
                        f"This cannot be undone.\n\n"
                        f"**Status:** <:Offline:1434951694319620197> Ended\n"
                        f"**Started:** <t:{int(shift['start_time'].timestamp())}:t>\n"
                        f"**Duration:** {self.cog.format_duration(active_duration)}",
            color=discord.Color(0x000000)
        )

        embed.set_footer(text=f"{shift['id']} • Shift Type: {shift['type']}")

        view = DeleteShiftConfirmView(self.cog, self.admin, self.target_user, shift)

        # Edit original message
        if self.message:
            await self.message.edit(embed=embed, view=view)
            view.message = self.message
        else:
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = message


class DeleteShiftConfirmView(discord.ui.View):
    """Confirmation view for deleting a shift"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift
        self.message = None
        self.armed = False

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

    @discord.ui.button(label="ARM", emoji="<:ARM:1435117432791633921>", style=discord.ButtonStyle.danger, custom_id="arm")
    async def arm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        self.armed = not self.armed

        if self.armed:
            button.label = "DISARM"
            button.emoji = discord.PartialEmoji(name="DISARM", id=1435117667097772116)
            button.style = discord.ButtonStyle.secondary

            # Enable delete button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "delete":
                    item.disabled = False
                    item.style = discord.ButtonStyle.danger
        else:
            button.label = "ARM"
            button.emoji = discord.PartialEmoji(name="ARM", id=1435117432791633921)
            button.style = discord.ButtonStyle.secondary

            # Disable delete button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "delete":
                    item.disabled = True
                    item.style = discord.ButtonStyle.secondary

        await interaction.edit_original_response(view=self)

    @discord.ui.button(label="Delete Shift", style=discord.ButtonStyle.secondary, disabled=True, custom_id="delete",
                       emoji="<:Reset:1434959478796714074>")
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.armed:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Please ARM first!",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # Create completed shift dict for logging BEFORE deleting
            completed_shift = dict(self.shift)

            # Log before deleting
            await self.cog.log_shift_event(
                interaction.guild,
                'delete',
                self.target_user,
                completed_shift,
                admin=self.admin
            )

            # Delete the shift
            async with db.pool.acquire() as conn:
                await conn.execute('DELETE FROM shifts WHERE id = $1', self.shift['id'])

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Deleted shift (ID: {self.shift['id']}) for {self.target_user.mention}",
                ephemeral=True
            )

            # Delete the confirmation message
            if self.message:
                try:
                    await self.message.delete()
                except:
                    pass

            # Return to admin panel
            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift['type'])
            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="<:Denied:1426930694633816248>")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Delete confirmation and return to admin panel
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift['type'])
        self.stop()

class ClearShiftsScopeView(discord.ui.View):
    """View for selecting which scope of shifts to clear"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member,
                 type: str, current_count: int, all_count: int, max_wave: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.current_count = current_count
        self.all_count = all_count
        self.max_wave = max_wave
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
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Current Wave", style=discord.ButtonStyle.primary, emoji="📅")
    async def current_wave_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Check super admin permission for current wave
        if not self.cog.has_super_admin_permission(interaction.user):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission to clear current wave shifts. Super admin role required.",
                ephemeral=True
            )
            return

        if self.current_count == 0:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No shifts in current wave to clear.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"**Clear User Shifts - Current Wave**",
            description=f"Are you sure you want to clear **{self.current_count}** shifts from current wave?\n\nThis cannot be undone.",
            color=discord.Color.red()
        )

        embed.set_author(
            name=f"@{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        embed.set_footer(text=f"Shift Type: {self.type}")

        view = ClearShiftsConfirmView(
            self.cog,
            self.admin,
            self.target_user,
            self.type,
            self.current_count,
            "current",
            None  # No wave number for current
        )

        # Edit original message
        if self.message:
            await self.message.edit(embed=embed, view=view)
            view.message = self.message
        else:
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = message

    @discord.ui.button(label="All Time", style=discord.ButtonStyle.danger, emoji="<:Wipe:1434954284851658762>")
    async def all_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Check if user is the owner for all-time wipe
        if interaction.user.id != OWNER_USER_ID:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission to clear all-time shifts. Owner only.",
                ephemeral=True
            )
            return

        if self.all_count == 0:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No shifts to clear.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"**Clear User Shifts - All Time**",
            description=f"Are you sure you want to clear **{self.all_count}** shifts from all time?\n\nThis cannot be undone.",
            color=discord.Color.red()
        )

        embed.set_author(
            name=f"@{self.target_user.display_name}",
            icon_url=self.target_user.display_avatar.url
        )

        embed.set_footer(text=f"Shift Type: {self.type}")

        view = ClearShiftsConfirmView(
            self.cog,
            self.admin,
            self.target_user,
            self.type,
            self.all_count,
            "all",
            None  # No wave number for all time
        )

        if self.message:
            await self.message.edit(embed=embed, view=view)
            view.message = self.message
        else:
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            view.message = message

    @discord.ui.button(label="Previous Waves", style=discord.ButtonStyle.primary, emoji="🔙")
    async def previous_waves_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # DON'T defer here - we're showing a modal which requires response
        # await interaction.response.defer()  # REMOVE THIS LINE

        # Check super admin permission for previous waves
        if not self.cog.has_super_admin_permission(interaction.user):
            await interaction.response.send_message(  # Changed from followup
                "<:Denied:1426930694633816248> You don't have permission to clear previous wave shifts. Super admin role required.",
                ephemeral=True
            )
            return

        # Check if any waves exist
        if not self.max_wave or self.max_wave < 1:
            await interaction.response.send_message(  # Changed from followup
                "<:Denied:1426930694633816248> No previous waves exist yet.",
                ephemeral=True
            )
            return

        # Show modal for wave selection
        modal = WaveNumberModal(
            self.cog,
            self.admin,
            self.target_user,
            self.type,
            self.max_wave
        )
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="<:Denied:1426930694633816248>")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Delete the scope selection
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        self.stop()

class ClearShiftsConfirmView(discord.ui.View):
    """Confirmation view for clearing all shifts"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, type: str,
                 count: int, scope: str, wave_number: int):  # CHANGE: max_wave -> wave_number
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.count = count
        self.scope = scope
        self.wave_number = wave_number  # CHANGE: Add this line
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
                    item.style = discord.ButtonStyle.danger
                    item.label = f"Clear {self.count} User Shifts"

            await interaction.edit_original_response(view=self)
        else:
            # Switch back to ARM
            button.label = "ARM"
            button.emoji = discord.PartialEmoji(name="ARM", id=1435117432791633921)
            button.style = discord.ButtonStyle.secondary

            # Disable the clear button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "clear_shifts":
                    item.disabled = True
                    item.style = discord.ButtonStyle.secondary
                    item.label = f"Clear {self.count} User Shifts"

            await interaction.edit_original_response(view=self)

    @discord.ui.button(label=f"Clear User Shifts", style=discord.ButtonStyle.secondary, disabled=True,
                       custom_id="clear_shifts", emoji="<:Reset:1434959478796714074>")
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not self.armed:
            await interaction.response.send_message("<:Denied:1426930694633816248> Please ARM first!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            current_week = WeeklyShiftManager.get_current_week_monday()

            # Build delete query based on scope
            async with db.pool.acquire() as conn:
                if self.scope == "current":
                    await conn.execute(
                        '''DELETE FROM shifts
                           WHERE discord_user_id = $1
                             AND type = $2
                             AND wave_number IS NULL
                             AND week_identifier = $3''',
                        self.target_user.id, self.type, current_week
                    )
                    scope_text = "current wave"

                elif self.scope == "wave":
                    await conn.execute(
                        '''DELETE FROM shifts
                           WHERE discord_user_id = $1
                             AND type = $2
                             AND wave_number = $3''',
                        self.target_user.id, self.type, self.wave_number  # Use wave_number
                    )
                    scope_text = f"Wave {self.wave_number}"

                else:  # "all"
                    await conn.execute(
                        '''DELETE FROM shifts
                           WHERE discord_user_id = $1
                             AND type = $2''',
                        self.target_user.id, self.type
                    )
                    scope_text = "all time"

            # Log the clear
            await self.cog.log_shift_event(
                interaction.guild,
                'clear',
                self.target_user,
                {'type': self.type, 'id': 'N/A'},
                admin=self.admin,
                details=f"{self.count} shifts cleared from {scope_text}"
            )

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Cleared {self.count} shifts from {scope_text} for {self.target_user.mention} ({self.type})",
                ephemeral=True
            )

            # Delete the confirmation message
            if self.message:
                try:
                    await self.message.delete()
                except:
                    pass

            # Return to admin panel
            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Delete confirmation and return to admin panel
        if self.message:
            try:
                await self.message.delete()
            except:
                pass

        await self.cog.show_admin_shift_panel(interaction, self.target_user, self.type)
        self.stop()


class ShiftIDModal(discord.ui.Modal):
    """Modal for entering a shift ID"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, action: str, parent_view=None):
        super().__init__(title="Enter Shift ID")
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.action = action
        self.parent_view = parent_view  # Store parent view reference

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
                # Create or get the view
                if self.parent_view:
                    view = self.parent_view
                else:
                    view = ModifyShiftSelectView(self.cog, self.admin, self.target_user, shift_dict['type'])
                await view.show_modify_panel(interaction, shift_dict)
            elif self.action == "delete":
                # Create or get the view
                if self.parent_view:
                    view = self.parent_view
                else:
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


class WaveNumberModal(discord.ui.Modal):
    """Modal for entering a wave number to clear"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member,
                 type: str, max_wave: int):
        super().__init__(title="Select Wave Number")
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.type = type
        self.max_wave = max_wave

        self.add_item(discord.ui.TextInput(
            label=f"Wave Number (1-{max_wave if max_wave else 0})",
            placeholder=f"Enter wave number to clear",
            required=True,
            max_length=10
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            wave_number = int(self.children[0].value)

            # Validate wave number exists
            if wave_number < 1:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Wave number must be at least 1.",
                    ephemeral=True
                )
                return

            if self.max_wave and wave_number > self.max_wave:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Wave number cannot exceed {self.max_wave}.",
                    ephemeral=True
                )
                return

            # Check if wave has any shifts for this user
            async with db.pool.acquire() as conn:
                wave_count = await conn.fetchval(
                    '''SELECT COUNT(*)
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND type = $2
                         AND wave_number = $3''',
                    self.target_user.id, self.type, wave_number
                )

            if wave_count == 0:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> No shifts found in Wave {wave_number} for {self.target_user.mention}.",
                    ephemeral=True
                )
                return

            # Show confirmation
            embed = discord.Embed(
                title=f"**Clear User Shifts - Wave {wave_number}**",
                description=f"Are you sure you want to clear **{wave_count}** shifts from Wave {wave_number}?\n\nThis cannot be undone.",
                color=discord.Color.red()
            )

            embed.set_author(
                name=f"@{self.target_user.display_name}",
                icon_url=self.target_user.display_avatar.url
            )

            embed.set_footer(text=f"Shift Type: {self.type}")

            view = ClearShiftsConfirmView(
                self.cog,
                self.admin,
                self.target_user,
                self.type,
                wave_count,
                "wave",
                wave_number
            )

            await interaction.edit_original_response(embed=embed, view=view)

        except ValueError:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please enter a valid wave number.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

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

        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.custom_id == "clear_shifts":
                item.label = f"Clear {self.count} User Shifts"

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
                    item.style = discord.ButtonStyle.danger
        else:
            button.label = "ARM"
            button.emoji = discord.PartialEmoji(name="ARM", id=1435117432791633921)
            button.style = discord.ButtonStyle.danger

            # Disable reset button
            for item in self.children:
                if isinstance(item, discord.ui.Button) and item.custom_id == "reset":
                    item.disabled = True
                    item.style = discord.ButtonStyle.secondary

        await interaction.edit_original_response(view=self)

    @discord.ui.button(label="Execute Reset", style=discord.ButtonStyle.danger, disabled=True, custom_id="reset")
    async def reset_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not self.armed:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please ARM first!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Executing Reset",
                                                ephemeral=True)


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

            try:
                await interaction.edit_original_response(view=self)
            except discord.NotFound:
                pass  # Message was already deleted

            self.stop()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Return to home shift management panel
        await self.cog.show_admin_shift_panel(
            interaction,
            self.target_user,
            self.type,
            show_last_shift=False
        )

        self.stop()

class AdminCommandRateLimiter:
    def __init__(self, calls: int, period: int):
        self.calls = calls
        self.period = period
        self.cache = {}

    def check(self, user_id: int) -> bool:
        now = datetime.utcnow()
        if user_id not in self.cache:
            self.cache[user_id] = []

        # Remove old entries
        self.cache[user_id] = [
            t for t in self.cache[user_id]
            if (now - t).total_seconds() < self.period
        ]

        if len(self.cache[user_id]) >= self.calls:
            return False

        self.cache[user_id].append(now)
        return True

async def setup(bot):
    await bot.add_cog(ShiftManagementCog(bot))