import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
from database import db
import asyncio
import math
import json

# Role IDs for shift types
FENZ_ROLE_ID = 1412790680991961149
HHSTJ_ROLE_ID = 1414146295974727861
CC_ROLE_ID = 1430108352377262090

SHIFT_TYPES = {
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


class ShiftManagementCog(commands.Cog):
    shift_group = app_commands.Group(name="shift", description="Shift management commands")

    def __init__(self, bot):
        self.bot = bot
        self._role_cache = {}
        self._cache_cleanup_task = None

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

    def has_admin_permission(self, member: discord.Member) -> bool:
        """Check if user has admin permissions"""
        return any(role.id in ADMIN_ROLES for role in member.roles)

    def has_senior_admin_permission(self, member: discord.Member) -> bool:
        """Check if user has senior admin permissions"""
        return any(role.id in SENIOR_ADMIN_ROLES for role in member.roles)

    def has_super_admin_permission(self, member: discord.Member) -> bool:
        """Check if user has super admin permissions"""
        return any(role.id in SUPER_ADMIN_ROLES for role in member.roles)

    async def get_duty_roles_for_shift_type(self, shift_type: str) -> tuple:
        """Get duty and break role IDs for a shift type"""
        if shift_type == "Shift FENZ":
            return (FENZ_DUTY_ROLE, FENZ_BREAK_ROLE)
        elif shift_type == "Shift HHStJ":
            return (HHSTJ_DUTY_ROLE, HHSTJ_BREAK_ROLE)
        elif shift_type == "Shift CC":
            return (CC_DUTY_ROLE, CC_BREAK_ROLE)
        return (None, None)

    async def update_duty_roles(self, member: discord.Member, shift_type: str, status: str):
        """Update duty/break roles based on shift status"""
        duty_role_id, break_role_id = await self.get_duty_roles_for_shift_type(shift_type)

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

    async def get_user_shift_types(self, member: discord.Member) -> list:
        """Get all shift types a user is eligible for"""
        shift_types = []
        for role_id, shift_type in SHIFT_TYPES.items():
            if any(role.id == role_id for role in member.roles):
                shift_types.append(shift_type)

        # Add additional shift types for specific roles
        for role in member.roles:
            if role.id in ADDITIONAL_SHIFT_ACCESS:
                for shift_type in ADDITIONAL_SHIFT_ACCESS[role.id]:
                    if shift_type not in shift_types:
                        shift_types.append(shift_type)

        return shift_types

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
        """Calculate shift statistics for a user"""
        async with db.pool.acquire() as conn:
            # Get all completed shifts
            shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND round_number IS NULL''',
                user_id
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
                # Subtract pause time if any
                if shift.get('pause_duration'):
                    duration -= timedelta(seconds=shift['pause_duration'])
                total_duration += duration

            return {
                'count': len(shifts),
                'total_duration': total_duration,
                'average_duration': total_duration / len(shifts) if len(shifts) > 0 else timedelta(0)
            }

    async def get_user_summary(self, user_id: int, member: discord.Member = None):
        """Get all user data in a single database connection"""
        async with db.pool.acquire() as conn:
            # Get statistics
            shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND round_number IS NULL''',
                user_id
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

            # Get last shift
            last_shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND round_number IS NULL
                   ORDER BY end_time DESC LIMIT 1''',
                user_id
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
        """Get the user's most recent completed shift"""
        async with db.pool.acquire() as conn:
            shift = await conn.fetchrow(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND end_time IS NOT NULL
                     AND round_number IS NULL
                   ORDER BY end_time DESC LIMIT 1''',
                user_id
            )
            return dict(shift) if shift else None

    async def get_quota_for_role(self, role_id: int) -> int:
        """Get quota in seconds for a role, returns 0 if no quota set"""
        async with db.pool.acquire() as conn:
            result = await conn.fetchrow(
                'SELECT quota_seconds FROM shift_quotas WHERE role_id = $1',
                role_id
            )
            return result['quota_seconds'] if result else 0

    async def get_user_quota(self, member: discord.Member) -> int:
        """Get the highest quota from all of a user's roles"""
        max_quota = 0
        for role in member.roles:
            quota = await self.get_quota_for_role(role.id)
            if quota > max_quota:
                max_quota = quota
        return max_quota

    async def get_bulk_quota_info(self, user_ids: list, guild) -> dict:
        """Fetch quota info for multiple users at once"""
        async with db.pool.acquire() as conn:
            # Get all role quotas once
            quotas = await conn.fetch('SELECT role_id, quota_seconds FROM shift_quotas')
            quota_map = {q['role_id']: q['quota_seconds'] for q in quotas}

            # Get shift data for all users in one query
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
                    results[user_id] = {'has_quota': False, 'completed': False}
                    continue

                max_quota = max((quota_map.get(role.id, 0) for role in member.roles), default=0)
                active_seconds = user_data.get(user_id, 0)

                results[user_id] = {
                    'has_quota': max_quota > 0,
                    'quota_seconds': max_quota,
                    'active_seconds': active_seconds,
                    'percentage': (active_seconds / max_quota * 100) if max_quota > 0 else 0,
                    'completed': (active_seconds / max_quota >= 1) if max_quota > 0 else False
                }

            return results

    async def get_quota_info(self, member: discord.Member) -> dict:
        """Get quota information for a user including percentage"""
        quota_seconds = await self.get_user_quota(member)

        if quota_seconds == 0:
            return {
                'has_quota': False,
                'quota_seconds': 0,
                'active_seconds': 0,
                'percentage': 0,
                'completed': False
            }

        active_seconds = await self.get_total_active_time(member.id)
        percentage = (active_seconds / quota_seconds) * 100

        return {
            'has_quota': True,
            'quota_seconds': quota_seconds,
            'active_seconds': active_seconds,
            'percentage': percentage,
            'completed': percentage >= 100
        }

    async def get_total_active_time(self, user_id: int, shift_type: str = None) -> int:
        """Get total active shift time in seconds for current round"""
        async with db.pool.acquire() as conn:
            if shift_type:
                shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND shift_type = $2
                         AND end_time IS NOT NULL
                         AND round_number IS NULL''',
                    user_id, shift_type
                )
            else:
                shifts = await conn.fetch(
                    '''SELECT *
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND end_time IS NOT NULL
                         AND round_number IS NULL''',
                    user_id
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

    @shift_group.command(name="quota", description="View or set shift quotas")
    @app_commands.describe(
        action="View your quota or set quota for roles",
        role="The role to set quota for (admin only)",
        hours="Hours for the quota",
        minutes="Minutes for the quota"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="View My Quota", value="view"),
        app_commands.Choice(name="Set Role Quota", value="set")
    ])
    async def shift_quota(
            self,
            interaction: discord.Interaction,
            action: app_commands.Choice[str],
            role: discord.Role = None,
            hours: int = 0,
            minutes: int = 0
    ):
        await interaction.response.defer()

        if action.value == "view":
            # Show user's quota
            quota_info = await self.get_quota_info(interaction.user)

            if not quota_info['has_quota']:
                await interaction.edit_original_response(
                    content="📊 You don't have a shift quota assigned."
                )
                return

            embed = discord.Embed(
                title="",
                color=discord.Color.green() if quota_info['completed'] else discord.Color.orange()
            )
            embed.set_author(
                name="<:Search:1434957367505719457> Your Shift Quota",
                icon_url=interaction.user.display_avatar.url
            )

            status_emoji = "<:Accepted:1426930333789585509>" if quota_info['completed'] else "<:Denied:1426930694633816248>"
            embed.add_field(
                name=f"{status_emoji} **{quota_info['percentage']:.1f}%** Complete",
                value=f"**Required:** {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}\n"
                      f"**Completed:** {self.format_duration(timedelta(seconds=quota_info['active_seconds']))}\n"
                      f"**Remaining:** {self.format_duration(timedelta(seconds=remaining))}",
                inline=False
            )
            await interaction.edit_original_response(embed=embed)

        elif action.value == "set":
            # Check admin permission
            if not any(role_check.id in QUOTA_ADMIN_ROLES for role_check in interaction.user.roles):
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You don't have permission to set quotas.",
                    ephemeral=True
                )
                return
            if not role:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Please specify a role to set quota for.",
                    ephemeral=True
                )
                return
            # Calculate total seconds
            total_seconds = (hours * 3600) + (minutes * 60)

            if total_seconds <= 0:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Quota must be greater than 0.",
                    ephemeral=True
                )
                return
            # Save to database
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO shift_quotas (role_id, quota_seconds)
                       VALUES ($1, $2) ON CONFLICT (role_id) DO
                    UPDATE SET quota_seconds = $2''',
                    role.id, total_seconds
                )

            await interaction.edit_original_response(
                content=f"<:Accepted:1426930333789585509> Set quota for {role.mention} to {self.format_duration(timedelta(seconds=total_seconds))}"
            )

    @shift_group.command(name="leaderboard", description="View shift leaderboard")
    @app_commands.describe(
        filter_role="Filter by role (optional)",
        wave="Filter by wave number (optional)"
    )
    async def shift_leaderboard(
            self,
            interaction: discord.Interaction,
            filter_role: discord.Role = None,
            wave: int = None
    ):
        await interaction.response.defer()

        # Check permission
        if not any(role.id in LEADERBOARD_ROLES for role in interaction.user.roles):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission to view the leaderboard.",
                ephemeral=True
            )
            return

        try:
            # Build query based on filters
            async with db.pool.acquire() as conn:
                if wave is not None:
                    # Filter by wave
                    query = '''SELECT discord_user_id, \
                                      discord_username,
                                      SUM(EXTRACT(EPOCH FROM (end_time - start_time)) - \
                                          COALESCE(pause_duration, 0)) as total_seconds
                               FROM shifts
                               WHERE end_time IS NOT NULL \
                                 AND round_number = $1
                               GROUP BY discord_user_id, discord_username
                               ORDER BY total_seconds DESC LIMIT 25'''
                    results = await conn.fetch(query, wave)
                else:
                    # Current round (no wave)
                    query = '''SELECT discord_user_id, \
                                      discord_username,
                                      SUM(EXTRACT(EPOCH FROM (end_time - start_time)) - \
                                          COALESCE(pause_duration, 0)) as total_seconds
                               FROM shifts
                               WHERE end_time IS NOT NULL \
                                 AND round_number IS NULL
                               GROUP BY discord_user_id, discord_username
                               ORDER BY total_seconds DESC LIMIT 25'''
                    results = await conn.fetch(query)

            if not results:
                await interaction.edit_original_response(
                    content="📊 No shift data available."
                )
                return

            user_ids = [row['discord_user_id'] for row in results]
            quota_infos = await self.get_bulk_quota_info(user_ids, interaction.guild)

            embed = discord.Embed(
                title="🏆 Shift Leaderboard",
                description="",  # Remove wave info from description
                color=discord.Color.gold()
            )
            embed.set_author(
                name=f"Leaderboard: Activity Wave {wave if wave else 'Current'}",
                icon_url=interaction.guild.icon.url if interaction.guild.icon else None
            )

            leaderboard_lines = []
            user_position = None

            for idx, row in enumerate(results, 1):
                member = interaction.guild.get_member(row['discord_user_id'])
                if not member:
                    continue

                # Check if filter_role is set and user has it
                if filter_role and filter_role not in member.roles:
                    continue

                # Get quota info
                quota_info = quota_infos.get(row['discord_user_id'], {'has_quota': False})
                quota_status = ""
                if quota_info['has_quota']:
                    quota_status = " <:Accepted:1426930333789585509>" if quota_info['completed'] else " <:Denied:1426930694633816248>"

                # Format time
                time_str = self.format_duration(timedelta(seconds=int(row['total_seconds'])))

                # Check if this is the requesting user
                if row['discord_user_id'] == interaction.user.id:
                    user_position = idx
                    leaderboard_lines.append(f"**`{idx}.` {member.mention} • {time_str} - {quota_status}**")
                else:
                    leaderboard_lines.append(f"`{idx}.` {member.mention} • {time_str} - {quota_status}")

            if leaderboard_lines:
                embed.description += "\n\n" + "\n".join(leaderboard_lines)

            if filter_role:
                embed.set_footer(text=f"Filtered by: {filter_role.name}")

            await interaction.edit_original_response(embed=embed)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @shift_group.command(name="reset", description="[ADMIN] Reset shifts for a wave")
    @app_commands.describe(
        roles="Roles to reset shifts for (comma-separated role IDs or @mentions)",
        confirm="Type CONFIRM to proceed"
    )
    async def shift_reset(
            self,
            interaction: discord.Interaction,
            roles: str,
            confirm: str
    ):
        await interaction.response.defer()

        # Check permission
        if not any(role.id in RESET_ROLES for role in interaction.user.roles):
            await interaction.followup.send(
                "<:Denied:1426930694633816248> You don't have permission to reset shifts.",
                ephemeral=True
            )
            return

        if confirm.upper() != "CONFIRM":
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please type CONFIRM to proceed with reset.",
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

            # Get next wave number
            async with db.pool.acquire() as conn:
                max_wave = await conn.fetchval(
                    'SELECT MAX(round_number) FROM shifts WHERE round_number IS NOT NULL'
                )
                next_wave = (max_wave or 0) + 1

                # Get all users with these roles and current shifts
                affected_users = set()
                for role_id in role_ids:
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

            role_names = [interaction.guild.get_role(rid).name for rid in role_ids if interaction.guild.get_role(rid)]

            await interaction.edit_original_response(
                content=f"<:Accepted:1426930333789585509> **Wave {next_wave} Created**\n"
                        f"• Archived shifts for {len(affected_users)} users\n"
                        f"• Affected roles: {', '.join(role_names)}\n"
                        f"• Users can now start fresh shifts for the new wave"
            )

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @shift_group.command(name="manage", description="Manage your shifts")
    @app_commands.describe(shift_type="Select your shift type")
    @app_commands.choices(shift_type=[
        app_commands.Choice(name="Shift FENZ", value="Shift FENZ"),
        app_commands.Choice(name="Shift HHStJ", value="Shift HHStJ"),
        app_commands.Choice(name="Shift CC", value="Shift CC")
    ])
    async def shift_manage(self, interaction: discord.Interaction, shift_type: app_commands.Choice[str]):
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
            shift_types = await self.get_user_shift_types(interaction.user)

            if not shift_types:
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
                await self.show_shift_statistics_panel(interaction, shift_types)

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
                       ORDER BY shift_type, start_time'''
                )

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
                await interaction.followup.send(
                    "🔭 No active shifts at the moment.",
                    ephemeral=True
                )
                return

            # Categorize by shift type
            shifts_by_type = {}
            for shift in active_shifts:
                shift_type = shift['shift_type']
                if shift_type not in shifts_by_type:
                    shifts_by_type[shift_type] = []
                shifts_by_type[shift_type].append(dict(shift))

            # Create embed
            embed = discord.Embed(
                title="🚨 HNZRP | FENZ & HHStJ",
                description="🕒 **Active Shifts**",
                color=discord.Color(0xffffff)
            )

            # Add each shift type section
            for shift_type in ["Shift FENZ", "Shift HHStJ", "Shift CC"]:
                if shift_type not in shifts_by_type:
                    continue

                shifts = shifts_by_type[shift_type]
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
                    async with db.pool.acquire() as conn:
                        callsign_row = await conn.fetchrow(
                            'SELECT * FROM callsigns WHERE discord_user_id = $1',
                            member.id
                        )

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
                            shift_lines.append(f"{idx}. 🟠 {display_name} | **{break_time}**")
                        else:
                            shift_lines.append(f"{idx}. 🟠 {display_name} | {break_time}")
                    else:
                        shift_lines.append(f"{idx}. 🟢 {display_name} • {shift_time}")

                if shift_lines:
                    embed.add_field(
                        name=f"**{shift_type.replace('Shift ', '')} Type**",
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
        shift_type="The shift type (REQUIRED)"
    )
    @app_commands.choices(shift_type=[
        app_commands.Choice(name="Shift FENZ", value="Shift FENZ"),
        app_commands.Choice(name="Shift HHStJ", value="Shift HHStJ"),
        app_commands.Choice(name="Shift CC", value="Shift CC")
    ])
    async def shift_admin(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            shift_type: app_commands.Choice[str]
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
            user_shift_types = await self.get_user_shift_types(user)

            if not user_shift_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} doesn't have any shift roles.",
                    ephemeral=True
                )
                return

            # Validate provided shift type
            if shift_type.value not in user_shift_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} doesn't have access to {shift_type.value}.",
                    ephemeral=True
                )
                return

            # Show admin control panel
            await self.show_admin_shift_panel(interaction, user, shift_type.value)

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


    async def show_shift_statistics_panel(self, interaction: discord.Interaction, shift_types: list):
        """Show the all-time statistics panel with start button"""
        summary = await self.get_user_summary(interaction.user.id, interaction.user)
        stats = summary['stats']
        last_shift = summary['last_shift']
        quota_info = summary['quota_info']

        embed = discord.Embed(
            title="<:Checklist:1434948670226432171> **All Time Information**",
            description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.format_duration(stats['total_duration'])}\n**Average Duration:** {self.format_duration(stats['average_duration'])}",
            color=discord.Color(0xffffff)
        )
        embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)

        # Add quota info if available
        if quota_info and quota_info['has_quota']:
            status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                'completed'] else "<:Denied:1426930694633816248>"
            embed.add_field(
                name="Quota Progress",
                value=f"> {status_emoji} **{quota_info['percentage']:.1f}%** of {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                inline=False
            )

        if last_shift:
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
            embed.set_footer(text=f"Shift Type: {last_shift['shift_type']}")
        else:
            embed.set_footer(text=f"Available Shift Types: {', '.join(shift_types)}")

        # Create view with only Start button
        view = ShiftStartView(self, interaction.user, shift_types)
        message = await interaction.edit_original_response(embed=embed, view=view)
        view.message = message

    async def show_active_shift_panel(self, interaction: discord.Interaction, shift: dict):
        """Show the active shift panel"""

        is_on_break = shift.get('pause_start') is not None

        if is_on_break:
            # On Break status
            embed = discord.Embed(
                title="📋 Shift Management",
                description="**Break Started**",
                color=discord.Color.gold()
            )

            embed.add_field(
                name="🕒 Current Shift",
                value=f"**Status:** 🟡 On Break\n"
                      f"**Started:** <t:{int(shift['start_time'].timestamp())}:R>\n"
                      f"**Break Started:** <t:{int(shift['pause_start'].timestamp())}:R>",
                inline=False
            )

            view = ShiftBreakView(self, interaction.user, shift)
        else:
            # On Shift status
            embed = discord.Embed(
                title="📋 Shift Management",
                description="**Shift Started**",
                color=discord.Color.green()
            )

            embed.add_field(
                name="🕒 Current Shift",
                value=f"**Status:** 🟢 On Shift\n"
                      f"**Started:** <t:{int(shift['start_time'].timestamp())}:R>",
                inline=False
            )

            view = ShiftActiveView(self, interaction.user, shift)

        embed.set_footer(text=f"Shift Type: {shift['shift_type']}")

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
            color=discord.Color(0xffffff)
        )
        embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)

        # Add quota info if available
        member = interaction.guild.get_member(interaction.user.id)
        if member:
            quota_info = await self.get_quota_info(member)
            if quota_info['has_quota']:
                status_emoji = "<:Accepted:1426930333789585509>" if quota_info[
                    'completed'] else "<:Denied:1426930694633816248>"
                embed.add_field(
                    name="Quota Progress",
                    value=f"> {status_emoji} **{quota_info['percentage']:.1f}%** of {self.format_duration(timedelta(seconds=quota_info['quota_seconds']))}",
                    inline=False
                )

        # Last shift (the one we just ended)
        embed.add_field(
            name="<:Clock:1434949269554597978> Last Shift",
            value=f"**Status:** <:Offline:1434951694319620197> Ended\n"
                  f"**Total Time:** {self.format_duration(active_duration)}\n"
                  f"**Break Time:** {self.format_duration(timedelta(seconds=pause_duration))}",
            inline=False
        )

        embed.set_footer(text=f"Shift Type: {shift['shift_type']}")

        await interaction.edit_original_response(embed=embed)

    async def show_admin_shift_panel(self, interaction: discord.Interaction, user: discord.Member, shift_type: str):
        """Show admin control panel for managing user's shift"""
        # Get active shift for this user
        active_shift = await self.get_active_shift(user.id)

        # Get statistics
        stats = await self.get_shift_statistics(user.id)

        embed = discord.Embed(
            title="<:Checklist:1434948670226432171> **All Time Information**",
            description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.format_duration(stats['total_duration'])}\n**Average Duration:** {self.format_duration(stats['average_duration'])}",
            color=discord.Color(0xffffff)
        )
        embed.set_author(
            name=f"Shift Management: {user.display_name}",
            icon_url=user.display_avatar.url
        )

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

        # Last shift (the one we just ended)
        embed.add_field(
            name="<:Clock:1434949269554597978> Last Shift",
            value=f"**Status:** <:Offline:1434951694319620197> Ended\n"
                  f"**Total Time:** {self.cog.format_duration(active_duration)}\n"
                  f"**Break Time:** {self.cog.format_duration(timedelta(seconds=pause_duration))}",
            inline=False
        )

        embed.set_footer(text=f"Shift Type: {shift_type}")

        # Show shift status
        if active_shift:
            is_on_break = active_shift.get('pause_start') is not None
            status = "🟡 On Break" if is_on_break else "🟢 On Shift"

            shift_info = f"**Status:** {status}\n"
            shift_info += f"**Started:** <t:{int(active_shift['start_time'].timestamp())}:R>"

            if is_on_break:
                shift_info += f"\n**Break Started:** <t:{int(active_shift['pause_start'].timestamp())}:R>"

            embed.add_field(
                name="🕒 Current Shift",
                value=shift_info,
                inline=False
            )

        view = AdminShiftControlView(self, interaction.user, user, shift_type, active_shift)
        await interaction.edit_original_response(embed=embed, view=view)


class ShiftStartView(discord.ui.View):
    """View shown when no shift is active - only Start button"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, shift_types: list):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.shift_types = shift_types
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
            # If multiple shift types, ask which one
            if len(self.shift_types) > 1:
                view = ShiftTypeSelectView(self.cog, self.user, self.shift_types)
                await interaction.followup.send(
                    "You have multiple shift types available. Please select one:",
                    view=view,
                    ephemeral=True
                )
            else:
                # Only one shift type, start it directly
                await self.start_shift(interaction, self.shift_types[0])

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    async def start_shift(self, interaction: discord.Interaction, shift_type: str):
        """Start a new shift"""
        async with db.pool.acquire() as conn:
            await conn.execute(
                '''INSERT INTO shifts
                   (discord_user_id, discord_username, shift_type, start_time, pause_duration)
                   VALUES ($1, $2, $3, $4, 0)''',
                self.user.id, str(self.user), shift_type, datetime.utcnow()
            )

        # Update nickname to DUTY
        await self.cog.update_nickname_for_shift_status(self.user, 'duty')

        # Update duty roles
        await self.cog.update_duty_roles(self.user, shift_type, 'duty')

        # Get the newly created shift
        shift = await self.cog.get_active_shift(self.user.id)

        # Build the embed directly instead of calling show_active_shift_panel
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

        embed.set_footer(text=f"Shift Type: {shift['shift_type']}")

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
            # Pause the shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET pause_start = $1
                       WHERE id = $2''',
                    datetime.utcnow(), self.shift['id']
                )

            # Update nickname to BRK
            await self.cog.update_nickname_for_shift_status(self.user, 'break')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, self.shift['shift_type'], 'break')

            # Get updated shift
            updated_shift = await self.cog.get_active_shift(self.user.id)

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

            embed.set_footer(text=f"Shift Type: {updated_shift['shift_type']}")

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
            # Update nickname back to normal (remove prefix)
            await self.cog.update_nickname_for_shift_status(self.user, 'off')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, self.shift['shift_type'], 'off')

            # Calculate final pause duration
            pause_duration = self.shift.get('pause_duration', 0)
            if self.shift.get('pause_start'):
                pause_duration += (datetime.utcnow() - self.shift['pause_start']).total_seconds()

            # End the shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time       = $1,
                           pause_duration = $2,
                           pause_start    = NULL
                       WHERE id = $3''',
                    datetime.utcnow(), pause_duration, self.shift['id']
                )

            # Get updated statistics
            stats = await self.cog.get_shift_statistics(self.user.id)

            total_duration = datetime.utcnow() - self.shift['start_time']
            active_duration = total_duration - timedelta(seconds=pause_duration)

            # Create summary embed
            embed = discord.Embed(
                title="<:Checklist:1434948670226432171> **All Time Information**",
                description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.cog.format_duration(stats['total_duration'])}\n**Average Duration:** self.cog.format_duration(stats['average_duration'])",
                color=discord.Color(0xffffff)
            )
            embed.set_author(name="Shift Management", icon_url=interaction.user.display_avatar.url)

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

            # Last shift (the one we just ended)
            embed.add_field(
                name="<:Clock:1434949269554597978> Last Shift",
                value=f"**Status:** <:Offline:1434951694319620197> Ended\n"
                      f"**Total Time:** {self.cog.format_duration(active_duration)}\n"
                      f"**Break Time:** {self.cog.format_duration(timedelta(seconds=pause_duration))}",
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {self.shift['shift_type']}")

            shift_types = await self.cog.get_user_shift_types(self.user)
            view = ShiftStartView(self.cog, self.user, shift_types)
            await interaction.edit_original_response(embed=embed, view=view)  # Only this one

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

    @discord.ui.button(label="Resume", style=discord.ButtonStyle.success, emoji="<:Play:1434957147829047467>")
    async def resume_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            # Calculate pause duration and resume
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

            # Update nickname back to DUTY
            await self.cog.update_nickname_for_shift_status(self.user, 'duty')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, self.shift['shift_type'], 'duty')

            # Get updated shift
            updated_shift = await self.cog.get_active_shift(self.user.id)

            # Build active panel embed directly
            embed = discord.Embed(
                title="**Shift Started**",
                color=discord.Color.green()
            )

            embed.add_field(
                name="<:Clock:1434949269554597978> Current Shift",
                value=f"**Status:** <:Online:1434949591303983194> On Shift\n"
                      f"**Started:** <t:{int(updated_shift['start_time'].timestamp())}:R>",
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {updated_shift['shift_type']}")

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
            # Update nickname back to normal (remove prefix)
            await self.cog.update_nickname_for_shift_status(self.user, 'off')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, self.shift['shift_type'], 'off')

            # Calculate final pause duration
            pause_duration = self.shift.get('pause_duration', 0)
            if self.shift.get('pause_start'):
                pause_duration += (datetime.utcnow() - self.shift['pause_start']).total_seconds()

            # End the shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE shifts
                       SET end_time       = $1,
                           pause_duration = $2,
                           pause_start    = NULL
                       WHERE id = $3''',
                    datetime.utcnow(), pause_duration, self.shift['id']
                )

            # Get updated statistics
            stats = await self.cog.get_shift_statistics(self.user.id)

            total_duration = datetime.utcnow() - self.shift['start_time']
            active_duration = total_duration - timedelta(seconds=pause_duration)

            # Create summary embed
            embed = discord.Embed(
                title="<:Checklist:1434948670226432171> **All Time Information**",
                description=f"**Shift Count:** {str(stats['count'])}\n**Total Duration:** {self.cog.format_duration(stats['total_duration'])}\n**Average Duration:** self.cog.format_duration(stats['average_duration'])",
                color=discord.Color(0xffffff)
            )

            embed.set_footer(text=f"Shift Type: {self.shift['shift_type']}")

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

            # Last shift (the one we just ended)
            embed.add_field(
                name="<:Clock:1434949269554597978> Last Shift",
                value=f"**Status:** <:Offline:1434951694319620197> Ended\n"
                      f"**Total Time:** {self.cog.format_duration(active_duration)}\n"
                      f"**Break Time:** {self.cog.format_duration(timedelta(seconds=pause_duration))}",
                inline=False
            )

            embed.set_footer(text=f"Shift Type: {self.shift['shift_type']}")

            shift_types = await self.cog.get_user_shift_types(self.user)
            view = ShiftStartView(self.cog, self.user, shift_types)
            await interaction.edit_original_response(embed=embed, view=view)  # Only this one


        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

class ShiftTypeSelectView(discord.ui.View):
    """View for selecting shift type when user has multiple options"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, shift_types: list):
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
        for shift_type in shift_types:
            button = discord.ui.Button(
                label=shift_type,
                style=discord.ButtonStyle.primary,
                custom_id=f"shift_type_{shift_type}"
            )
            button.callback = self.create_callback(shift_type)
            self.add_item(button)

    def create_callback(self, shift_type: str):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()

            # Verify user still has access to this shift type
            user_shift_types = await self.cog.get_user_shift_types(interaction.user)

            if shift_type not in user_shift_types:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> You don't have access to **{shift_type}** shifts!",
                    ephemeral=True
                )
                return

            # Start the shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO shifts
                       (discord_user_id, discord_username, shift_type, start_time, pause_duration)
                       VALUES ($1, $2, $3, $4, 0)''',
                    self.user.id, str(self.user), shift_type, datetime.utcnow()
                )

            # Update nickname to DUTY
            await self.cog.update_nickname_for_shift_status(self.user, 'duty')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, shift_type, 'duty')

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

            embed.set_footer(text=f"Shift Type: {shift['shift_type']}")

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

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_types: list):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.message = none

        async def on_timeout(self):
            """Clean up when view times out"""
            for item in self.children:
                item.disabled = True
            if self.message:
                try:
                    await self.message.edit(view=self)
                except:
                    pass

        for shift_type in shift_types:
            button = discord.ui.Button(
                label=shift_type,
                style=discord.ButtonStyle.primary,
                custom_id=f"admin_shift_type_{shift_type}"
            )
            button.callback = self.create_callback(shift_type)
            self.add_item(button)

    def create_callback(self, shift_type: str):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()
            await self.cog.show_admin_shift_panel(interaction, self.target_user, shift_type)
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

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str,
                 active_shift: dict):
        super().__init__(timeout=300)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type
        self.active_shift = active_shift
        self.message = None

        # Add dropdown for admin actions
        self.add_item(AdminActionsSelect(cog, admin, target_user, shift_type))

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
            # Start shift
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO shifts
                       (discord_user_id, discord_username, shift_type, start_time, pause_duration)
                       VALUES ($1, $2, $3, $4, 0)''',
                    self.target_user.id, str(self.target_user), self.shift_type, datetime.utcnow()
                )

            # Update nickname and roles
            await self.cog.update_nickname_for_shift_status(self.target_user, 'duty')
            await self.cog.update_duty_roles(self.target_user, self.shift_type, 'duty')

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Started shift for {self.target_user.mention}",
                ephemeral=True
            )

            # Refresh panel
            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift_type)
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
            await self.cog.update_duty_roles(self.target_user, self.shift_type, 'break')

            await interaction.followup.send(
                f"⏸️ Paused shift for {self.target_user.mention}",
                ephemeral=True
            )

            # Refresh panel
            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift_type)
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
            await self.cog.update_duty_roles(self.target_user, self.shift_type, 'duty')

            await interaction.followup.send(
                f"▶️ Resumed shift for {self.target_user.mention}",
                ephemeral=True
            )

            # Refresh panel
            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift_type)
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
            await self.cog.update_duty_roles(self.target_user, self.shift_type, 'off')

            await interaction.followup.send(
                f"⏹️ Stopped shift for {self.target_user.mention}",
                ephemeral=True
            )

            # Refresh panel
            await self.cog.show_admin_shift_panel(interaction, self.target_user, self.shift_type)
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )


class AdminActionsSelect(discord.ui.Select):
    """Dropdown menu for admin actions"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str):
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type

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
                     AND shift_type = $2''',
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

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type
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
        """Fetch all completed shifts"""
        async with db.pool.acquire() as conn:
            self.shifts = await conn.fetch(
                '''SELECT *
                   FROM shifts
                   WHERE discord_user_id = $1
                     AND shift_type = $2
                     AND end_time IS NOT NULL
                   ORDER BY end_time DESC''',
                self.target_user.id, self.shift_type
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
            value += f"- **Ended:** <t:{int(shift['end_time'].timestamp())}:f>"
            value += f"- **Break:** {self.cog.format_duration(break_duration)}\n"

            embed.add_field(
                name=f"`{shift_id}`",
                value=value,
                inline=False
            )

        embed.set_footer(text=f"Shift Type: {self.shift_type} • Page {self.current_page + 1}/{self.total_pages}")

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

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type
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
        embed.set_footer(text=f"{shift['id']} • Shift Type: {shift['shift_type']}")

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
                     AND shift_type = $2
                     AND end_time IS NOT NULL
                   ORDER BY end_time DESC LIMIT 10''',
                self.target_user.id, self.shift_type
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
            # Parse time inputs
            hours = int(self.children[0].value or 0)
            minutes = int(self.children[1].value or 0)
            seconds = int(self.children[2].value or 0) if len(self.children) > 2 else 0

            time_delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)

            if self.action == "set":
                # Set the shift duration by adjusting start time
                new_start_time = self.shift['end_time'] - time_delta

                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE shifts
                           SET start_time = $1
                           WHERE id = $2''',
                        new_start_time, self.shift['id']
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

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type
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

        embed.set_footer(text=f"{shift['id']} • Shift Type: {shift['shift_type']}")

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
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'DELETE FROM shifts WHERE id = $1',
                    self.shift['id']
                )

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Deleted shift (ID: {self.shift['id']}) for {self.target_user.mention}",
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
                     AND shift_type = $2
                     AND end_time IS NOT NULL
                   ORDER BY end_time DESC LIMIT 10''',
                self.target_user.id, self.shift_type
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

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str,
                 count: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type
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
            button.style = discord.ButtonStyle.danger

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

    @discord.ui.button(label="Clear User Shifts", style=discord.ButtonStyle.danger, disabled=True,
                       custom_id="clear_shifts")
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        if not self.armed:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Please ARM first!",
                ephemeral=True
            )
            return

        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''DELETE
                       FROM shifts
                       WHERE discord_user_id = $1
                         AND shift_type = $2''',
                    self.target_user.id, self.shift_type
                )

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Cleared {self.count} shifts for {self.target_user.mention} ({self.shift_type})",
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
                view = ModifyShiftSelectView(self.cog, self.admin, self.target_user, shift_dict['shift_type'])
                await view.show_modify_panel(interaction, shift_dict)
            elif self.action == "delete":
                # Show delete confirm
                view = DeleteShiftSelectView(self.cog, self.admin, self.target_user, shift_dict['shift_type'])
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


async def setup(bot):
    await bot.add_cog(ShiftManagementCog(bot))