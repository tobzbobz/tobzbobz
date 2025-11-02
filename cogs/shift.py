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

    @shift_group.command(name="manage", description="Manage your shifts")
    async def shift_manage(self, interaction: discord.Interaction):
        """Main shift management command"""
        await interaction.response.defer(ephemeral=True)

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
        await interaction.response.defer(ephemeral=True)

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
                color=discord.Color.blue()
            )

            # Add each shift type section
            for shift_type in ["Shift FENZ", "Shift HHStJ", "Shift CC"]:
                if shift_type not in shifts_by_type:
                    continue

                shifts = shifts_by_type[shift_type]
                shift_lines = []

                for idx, shift in enumerate(shifts, 1):
                    # Get member
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
                    # Try to get callsign from database
                    async with db.pool.acquire() as conn:
                        callsign_row = await conn.fetchrow(
                            'SELECT * FROM callsigns WHERE discord_user_id = $1',
                            member.id
                        )

                    if callsign_row:
                        # Format with callsign
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

            await interaction.followup.send(embed=embed, ephemeral=True)

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
        shift_type="The shift type (leave empty for auto-detect)"
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
            shift_type: app_commands.Choice[str] = None
    ):
        """Admin shift management command"""
        await interaction.response.defer(ephemeral=True)

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

            # Determine shift type
            selected_shift_type = None
            if shift_type:
                # Validate provided shift type
                if shift_type.value not in user_shift_types:
                    await interaction.followup.send(
                        f"<:Denied:1426930694633816248> {user.mention} doesn't have access to {shift_type.value}.",
                        ephemeral=True
                    )
                    return
                selected_shift_type = shift_type.value
            elif len(user_shift_types) == 1:
                # Auto-select if only one type
                selected_shift_type = user_shift_types[0]
            else:
                # Multiple types available, need to select
                view = AdminShiftTypeSelectView(self, interaction.user, user, user_shift_types)
                await interaction.followup.send(
                    f"Select a shift type for {user.mention}:",
                    view=view,
                    ephemeral=True
                )
                return

            # Show admin control panel
            await self.show_admin_shift_panel(interaction, user, selected_shift_type)

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
        stats = await self.get_shift_statistics(interaction.user.id)
        last_shift = await self.get_last_shift(interaction.user.id)

        embed = discord.Embed(
            title="📋 Shift Management",
            description="**📊 All Time Information**",
            color=discord.Color.blue()
        )

        embed.add_field(
            name="Shift Count",
            value=str(stats['count']),
            inline=False
        )
        embed.add_field(
            name="Total Duration",
            value=self.format_duration(stats['total_duration']),
            inline=False
        )
        embed.add_field(
            name="Average Duration",
            value=self.format_duration(stats['average_duration']),
            inline=False
        )

        # Show last shift info if available
        if last_shift:
            last_duration = last_shift['end_time'] - last_shift['start_time']
            active_duration = last_duration - timedelta(seconds=last_shift.get('pause_duration', 0))

            embed.add_field(
                name="🕒 Last Shift",
                value=f"**Status:** ⚫ Ended\n"
                      f"**Ended:** <t:{int(last_shift['end_time'].timestamp())}:R>\n"
                      f"**Break Time:** {self.format_duration(timedelta(seconds=last_shift.get('pause_duration', 0)))}",
                inline=False
            )
            embed.set_footer(text=f"Shift Type: {last_shift['shift_type']}")
        else:
            embed.set_footer(text=f"Available Shift Types: {', '.join(shift_types)}")

        # Create view with only Start button
        view = ShiftStartView(self, interaction.user, shift_types)

        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

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

        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

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
            title="📋 Shift Management",
            description="**📊 All Time Information**",
            color=discord.Color.blue()
        )

        embed.add_field(
            name="Shift Count",
            value=str(stats['count']),
            inline=False
        )
        embed.add_field(
            name="Total Duration",
            value=self.format_duration(stats['total_duration']),
            inline=False
        )
        embed.add_field(
            name="Average Duration",
            value=self.format_duration(stats['average_duration']),
            inline=False
        )

        # Check if user has a quota
        member = interaction.guild.get_member(interaction.user.id)
        if member:
            quota_seconds = await self.get_user_quota(member)
            if quota_seconds > 0:
                total_active = await self.get_total_active_time(interaction.user.id)
                quota_percentage = (total_active / quota_seconds) * 100
                quota_td = timedelta(seconds=quota_seconds)

                embed.add_field(
                    name="📈 Quota Progress",
                    value=f"**{quota_percentage:.1f}%** of {self.format_duration(quota_td)} completed",
                    inline=False
                )

        # Last shift (the one we just ended)
        embed.add_field(
            name="🕒 Last Shift",
            value=f"**Status:** ⚫ Ended\n"
                  f"**Ended:** <t:{int(datetime.utcnow().timestamp())}:R>\n"
                  f"**Break Time:** {self.format_duration(timedelta(seconds=pause_duration))}",
            inline=False
        )

        embed.set_footer(text=f"Shift Type: {shift['shift_type']}")

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def show_admin_shift_panel(self, interaction: discord.Interaction, user: discord.Member, shift_type: str):
        """Show admin control panel for managing user's shift"""
        # Get active shift for this user
        active_shift = await self.get_active_shift(user.id)

        # Get statistics
        stats = await self.get_shift_statistics(user.id)

        embed = discord.Embed(
            title=f"Shift Management: {user.display_name}",
            description=f"**All Time Information**\nShift Type: {shift_type}",
            color=discord.Color.blue()
        )

        embed.add_field(
            name="Shift Count",
            value=str(stats['count']),
            inline=False
        )
        embed.add_field(
            name="Total Duration",
            value=self.format_duration(stats['total_duration']),
            inline=False
        )
        embed.add_field(
            name="Average Duration",
            value=self.format_duration(stats['average_duration']),
            inline=False
        )

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
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class ShiftStartView(discord.ui.View):
    """View shown when no shift is active - only Start button"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, shift_types: list):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.shift_types = shift_types

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Start", style=discord.ButtonStyle.success, emoji="🟢")
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

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

        # Show the active shift panel
        await self.cog.show_active_shift_panel(interaction, shift)


class ShiftActiveView(discord.ui.View):
    """View shown when shift is active (on shift) - Pause and End buttons"""

    def __init__(self, cog: ShiftManagementCog, user: discord.Member, shift: dict):
        super().__init__(timeout=300)
        self.cog = cog
        self.user = user
        self.shift = shift

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Pause", style=discord.ButtonStyle.primary, emoji="🟡")
    async def pause_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

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

            # Show break panel
            await self.cog.show_active_shift_panel(interaction, updated_shift)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="End", style=discord.ButtonStyle.danger, emoji="🔴")
    async def end_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            # Update nickname back to normal (remove prefix)
            await self.cog.update_nickname_for_shift_status(self.user, 'off')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, self.shift['shift_type'], 'off')

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

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your shift panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Resume", style=discord.ButtonStyle.success, emoji="🟢")
    async def resume_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

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

            # Show active panel
            await self.cog.show_active_shift_panel(interaction, updated_shift)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="End", style=discord.ButtonStyle.danger, emoji="🔴")
    async def end_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            # Update nickname back to normal (remove prefix)
            await self.cog.update_nickname_for_shift_status(self.user, 'off')

            # Update duty roles
            await self.cog.update_duty_roles(self.user, self.shift['shift_type'], 'off')

            await self.cog.end_shift_and_show_summary(interaction, self.shift)
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
            await interaction.response.defer(ephemeral=True)

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

            # Show the active shift panel
            await self.cog.show_active_shift_panel(interaction, shift)

            # Disable all buttons
            for item in self.children:
                item.disabled = True
            await interaction.message.edit(view=self)
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
            await interaction.response.defer(ephemeral=True)
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

        # Add dropdown for admin actions
        self.add_item(AdminActionsSelect(cog, admin, target_user, shift_type))

        # Add shift control buttons if there's an active shift
        if active_shift:
            is_on_break = active_shift.get('pause_start') is not None

            if is_on_break:
                # Resume button
                resume_btn = discord.ui.Button(label="Resume Shift", style=discord.ButtonStyle.success, emoji="🟢")
                resume_btn.callback = self.resume_callback
                self.add_item(resume_btn)
            else:
                # Pause button
                pause_btn = discord.ui.Button(label="Pause Shift", style=discord.ButtonStyle.primary, emoji="🟡")
                pause_btn.callback = self.pause_callback
                self.add_item(pause_btn)

            # Stop button
            stop_btn = discord.ui.Button(label="Stop Shift", style=discord.ButtonStyle.danger, emoji="🔴")
            stop_btn.callback = self.stop_callback
            self.add_item(stop_btn)
        else:
            # Start button
            start_btn = discord.ui.Button(label="Start Shift", style=discord.ButtonStyle.success, emoji="🟢")
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
        await interaction.response.defer(ephemeral=True)

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
                f"✅ Started shift for {self.target_user.mention}",
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
        await interaction.response.defer(ephemeral=True)

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
        await interaction.response.defer(ephemeral=True)

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
        await interaction.response.defer(ephemeral=True)

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
            discord.SelectOption(label="Shift List", description="View shift history", emoji="📋"),
            discord.SelectOption(label="Modify Shift", description="Modify shift duration", emoji="✏️"),
            discord.SelectOption(label="Delete Shift", description="Delete a shift", emoji="🗑️"),
            discord.SelectOption(label="Clear User Shifts", description="Clear all shifts", emoji="⚠️")
        ]

        super().__init__(placeholder="Select an action...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        selection = self.values[0]

        if selection == "Shift List":
            await self.show_shift_list(interaction)
        elif selection == "Modify Shift":
            if not self.cog.has_senior_admin_permission(interaction.user):
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You don't have permission for this action.",
                    ephemeral=True
                )
                return
            await self.show_modify_shift(interaction)
        elif selection == "Delete Shift":
            if not self.cog.has_senior_admin_permission(interaction.user):
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You don't have permission for this action.",
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

    async def show_shift_list(self, interaction: discord.Interaction):
        """Show paginated shift list"""
        view = ShiftListView(self.cog, self.admin, self.target_user, self.shift_type)
        await view.show_page(interaction, 0)

    async def show_modify_shift(self, interaction: discord.Interaction):
        """Show modify shift interface"""
        view = ModifyShiftSelectView(self.cog, self.admin, self.target_user, self.shift_type)
        await interaction.followup.send(
            f"Select a shift to modify for {self.target_user.mention}:",
            view=view,
            ephemeral=True
        )

    async def show_delete_shift(self, interaction: discord.Interaction):
        """Show delete shift interface"""
        view = DeleteShiftSelectView(self.cog, self.admin, self.target_user, self.shift_type)
        await interaction.followup.send(
            f"Select a shift to delete for {self.target_user.mention}:",
            view=view,
            ephemeral=True
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
            title=f"Shift Management: {self.target_user.display_name}",
            description=f"**Clear User Shifts**",
            color=discord.Color.red()
        )
        embed.add_field(
            name="⚠️ Warning",
            value=f"Are you sure you want to clear **{count} shifts** for this user under the **{self.shift_type}** shift type?\n\nThis cannot be undone.",
            inline=False
        )

        view = ClearShiftsConfirmView(self.cog, self.admin, self.target_user, self.shift_type, count)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class ShiftListView(discord.ui.View):
    """Paginated shift list view"""
    ITEMS_PER_PAGE = 7

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift_type: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift_type = shift_type
        self.current_page = 0
        self.total_pages = 0
        self.shifts = []

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
        """Show a specific page"""
        await self.get_shifts()

        if not self.shifts:
            embed = discord.Embed(
                title=f"Shift Management: {self.target_user.display_name}",
                description="**📋 Shift List**\n\nNo completed shifts found.",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        self.current_page = max(0, min(page, self.total_pages - 1))

        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_shifts = self.shifts[start_idx:end_idx]

        embed = discord.Embed(
            title=f"Shift Management: {self.target_user.display_name}",
            description=f"**📋 Shift List**",
            color=discord.Color.blue()
        )

        for shift in page_shifts:
            shift_id = str(shift['id'])
            duration = shift['end_time'] - shift['start_time']
            active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))
            break_duration = timedelta(seconds=shift.get('pause_duration', 0))

            value = f"• **Duration:** {self.cog.format_duration(duration)}\n"
            value += f"• **Active:** {self.cog.format_duration(active_duration)}\n"
            value += f"• **Break:** {self.cog.format_duration(break_duration)}\n"
            value += f"• **Started:** <t:{int(shift['start_time'].timestamp())}:f>\n"
            value += f"• **Ended:** <t:{int(shift['end_time'].timestamp())}:f>"

            embed.add_field(
                name=f"Shift ID: {shift_id}",
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

        await interaction.followup.send(embed=embed, view=self, ephemeral=True)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary, custom_id="first")
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, 0)

    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.secondary, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, self.current_page - 1)

    @discord.ui.button(label="1/7", style=discord.ButtonStyle.primary, custom_id="page", disabled=True)
    async def page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass

    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.secondary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.show_page(interaction, self.current_page + 1)

    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary, custom_id="last")
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

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Most Recent", style=discord.ButtonStyle.primary)
    async def most_recent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        shift = await self.cog.get_last_shift(self.target_user.id)
        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No completed shifts found.",
                ephemeral=True
            )
            return

        await self.show_modify_panel(interaction, shift)

    @discord.ui.button(label="Search by Shift ID", style=discord.ButtonStyle.secondary)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ShiftIDModal(self.cog, self.admin, self.target_user, "modify")
        await interaction.response.send_modal(modal)

    async def show_modify_panel(self, interaction: discord.Interaction, shift: dict):
        """Show the modify options for a shift"""
        duration = shift['end_time'] - shift['start_time']
        active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

        embed = discord.Embed(
            title=f"Shift Management: {self.target_user.display_name}",
            description="**Modify Shift**",
            color=discord.Color.blue()
        )

        embed.add_field(
            name="Status",
            value="⚫ Ended",
            inline=False
        )
        embed.add_field(
            name="Duration",
            value=self.cog.format_duration(active_duration),
            inline=False
        )
        embed.add_field(
            name="Shift ID",
            value=f"{shift['id']} • Shift Type: {shift['shift_type']}",
            inline=False
        )

        view = ModifyShiftActionsView(self.cog, self.admin, self.target_user, shift)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class ModifyShiftActionsView(discord.ui.View):
    """View for modifying a shift"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict):
        super().__init__(timeout=120)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Add Time", style=discord.ButtonStyle.success)
    async def add_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "add")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Remove Time", style=discord.ButtonStyle.danger)
    async def remove_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "remove")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Set Time", style=discord.ButtonStyle.primary)
    async def set_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TimeModifyModal(self.cog, self.admin, self.target_user, self.shift, "set")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Reset Time", style=discord.ButtonStyle.secondary)
    async def reset_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

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
                f"✅ Reset shift time for {self.target_user.mention} (Shift ID: {self.shift['id']})",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )


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
        await interaction.response.defer(ephemeral=True)

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
                    f"✅ Set shift duration to {self.cog.format_duration(time_delta)} for {self.target_user.mention}",
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
                    f"✅ Added {self.cog.format_duration(time_delta)} to shift for {self.target_user.mention}",
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
                    f"✅ Removed {self.cog.format_duration(time_delta)} from shift for {self.target_user.mention}",
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

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Most Recent", style=discord.ButtonStyle.primary)
    async def most_recent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        shift = await self.cog.get_last_shift(self.target_user.id)
        if not shift:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> No completed shifts found.",
                ephemeral=True
            )
            return

        await self.show_delete_confirm(interaction, shift)

    @discord.ui.button(label="Search by Shift ID", style=discord.ButtonStyle.secondary)
    async def search_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ShiftIDModal(self.cog, self.admin, self.target_user, "delete")
        await interaction.response.send_modal(modal)

    async def show_delete_confirm(self, interaction: discord.Interaction, shift: dict):
        """Show delete confirmation"""
        duration = shift['end_time'] - shift['start_time']
        active_duration = duration - timedelta(seconds=shift.get('pause_duration', 0))

        embed = discord.Embed(
            title=f"Shift Management: {self.target_user.display_name}",
            description="**Delete Shift**",
            color=discord.Color.red()
        )

        embed.add_field(
            name="⚠️ Confirm Delete",
            value=f"Are you sure you want to delete this shift?\n\n"
                  f"**Shift ID:** {shift['id']}\n"
                  f"**Duration:** {self.cog.format_duration(active_duration)}\n"
                  f"**Started:** <t:{int(shift['start_time'].timestamp())}:f>\n"
                  f"**Ended:** <t:{int(shift['end_time'].timestamp())}:f>\n\n"
                  f"This cannot be undone.",
            inline=False
        )

        view = DeleteShiftConfirmView(self.cog, self.admin, self.target_user, shift)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class DeleteShiftConfirmView(discord.ui.View):
    """Confirmation view for deleting a shift"""

    def __init__(self, cog: ShiftManagementCog, admin: discord.Member, target_user: discord.Member, shift: dict):
        super().__init__(timeout=60)
        self.cog = cog
        self.admin = admin
        self.target_user = target_user
        self.shift = shift

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
        await interaction.response.defer(ephemeral=True)

        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'DELETE FROM shifts WHERE id = $1',
                    self.shift['id']
                )

            await interaction.followup.send(
                f"✅ Deleted shift (ID: {self.shift['id']}) for {self.target_user.mention}",
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
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("Cancelled.", ephemeral=True)
        self.stop()


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

        # Update the clear button label with count
        self.children[1].label = f"Clear {count} User Shifts"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your admin panel!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="ARM", style=discord.ButtonStyle.secondary)
    async def arm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        self.armed = True
        button.disabled = True
        button.style = discord.ButtonStyle.success
        button.label = "ARMED"

        # Enable the clear button
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.label.startswith("Clear"):
                item.disabled = False

        await interaction.message.edit(view=self)
        await interaction.followup.send("⚠️ Armed. You can now clear shifts.", ephemeral=True)

    @discord.ui.button(label="Clear User Shifts", style=discord.ButtonStyle.danger, disabled=True)
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

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
                f"✅ Cleared {self.count} shifts for {self.target_user.mention} ({self.shift_type})",
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
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("Cancelled.", ephemeral=True)
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
        await interaction.response.defer(ephemeral=True)

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