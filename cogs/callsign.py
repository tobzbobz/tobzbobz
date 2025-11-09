import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()
import aiohttp
import os
from datetime import datetime
from database import db
import json
from google_sheets_integration import sheets_manager, COMMAND_RANKS, NON_COMMAND_RANKS

BLOXLINK_API_KEY = os.getenv('BLOXLINK_API_KEY')

SYNC_LOG_CHANNEL_ID = 1434770430505390221
CALLSIGN_REQUEST_LOG_CHANNEL_ID = 1435318020619632851

EXCLUDED_GUILDS = {
    1420770769562243083,  # Example: Test Server
    1430002479239532747,
    1425867713183744023   # Example: Development Server
}

NAUGHTY_ROLES = {
    1432540488312950805: "Under Investigation",
    1365536207726973060: "Strike 3!! F",
    1365536206892437545: "Strike 2! F",
    1365536206083067927: "Strike 1 F",
    1389606936856756406: "Strike 1 H",
    1389607171863347220: "Strike 2! H",
    1389607397269569596: "Strike 3!! H",
    1430126585834504314: "Strike 1 C",
    1430126817465077820: "Strike 2! C",
    1430126920447692863: "Strike 3!! C",
    1412792728630202378: "FENZ Termination",
    1389607789088866405: "HHStJ Termination",
    1430127047061274645: "CC Termination",
    1435787285995196468: "Rank Lock",
}

# Configuration: Map Discord Role IDs to FENZ Ranks
FENZ_RANK_MAP = {
    1309020834400047134: ("Recruit Firefighter", "RFF"),
    1309020730561790052: ("Qualified Firefighter", "QFF"),
    1309020647128825867: ("Senior Firefighter", "SFF"),
    1309019405329502238: ("Station Officer", "SO"),
    1309019042765344810: ("Senior Station Officer", "SSO"),
    1365959865381556286: ("Deputy Chief Officer", "DCO"),
    1365959864618188880: ("Chief Officer", "CO"),
    1389158062635487312: ("Assistant Area Commander", "AAC"),
    1365959866363150366: ("Area Commander", "AC"),
    1389157690760232980: ("Assistant National Commander", "ANC"),
    1389157641799991347: ("Deputy National Commander", "DNC"),
    1285113945664917514: ("National Commander", "NC"),
}

# Configuration: Map Discord Role IDs to HHStJ Ranks
HHSTJ_RANK_MAP = {
    1389113026900394064: ("First Responder", "FR"),
    1389112936517079230: ("Emergency Medical Technician", "EMT"),
    1389112844364021871: ("Graduate Paramedic", "GPARA"),
    1389112803712827473: ("Paramedic", "PARA"),
    1389112753142366298: ("Extended Care Paramedic", "ECP"),
    1389112689267314790: ("Critical Care Paramedic", "CCP"),
    1389112601815941240: ("Doctor", "DR"),
    1389112470211264552: ("Watch Operations Manager", "WOM-MIKE30"),
    1403314606839037983: ("Area Operations Manager", "AOM-OSCAR32"),
    1403314387602767932: ("District Operations Support Manager", "DOSM-OSCAR31"),
    1403312277876248626: ("District Operations Manager", "DOM-OSCAR30"),
    1389111474949062726: ("Assistant National Operations Manager", "ANOM-OSCAR3"),
    1389111326571499590: ("Deputy National Operations Manager", "DNOM-OSCAR2"),
    1389110819190472775: ("National Operations Manager", "NOM-OSCAR1"),
}

HIGH_COMMAND_RANKS = {
    1365959865381556286,  # DCO
    1365959864618188880,  # CO
    1389158062635487312,  # AAC
    1365959866363150366,  # AC
    1389157690760232980,  # ANC
    1389157641799991347,  # DNC
    1285113945664917514,  # NC
}

HHSTJ_HIGH_COMMAND_RANKS = {
    1389110819190472775,  # NOM-OSCAR1
    1389111326571499590,  # DNOM-OSCAR2
    1389111474949062726,  # ANOM-OSCAR3
    1403312277876248626,  # DOM-OSCAR30
    1403314387602767932,  # DOSM-OSCAR31
    1403314606839037983,  # AOM-OSCAR32
    1389112470211264552,  # WOM-MIKE30
}

# Rank hierarchy for sorting (highest to lowest)
FENZ_RANK_HIERARCHY = [
    "NC", "DNC", "ANC", "AC", "AAC", "CO", "DCO", "SSO", "SO", "SFF", "QFF", "RFF"
]

HHSTJ_RANK_HIERARCHY = [
    "NOM-OSCAR1", "DNOM-OSCAR2", "ANOM-OSCAR3", "DOM-OSCAR30",
    "DOSM-OSCAR31", "AOM-OSCAR32", "WOM-MIKE30", "DR", "CCP",
    "ECP", "PARA", "GPARA", "EMT", "FR"
]

# Sync role required
SYNC_ROLE_ID = 1389550689113473024

LEAD_ROLES = {
    1389550689113473024,
    1389113393511923863,
    1389113460687765534,
    1285474077556998196,
    1365536209681514636
}

UPPER_LEAD = {
    1389550689113473024,
    1389157641799991347,
    1389111326571499590
}

def get_rank_sort_key(fenz_prefix: str, hhstj_prefix: str) -> tuple:
    """
    Generate a sort key based on rank hierarchy.
    Returns tuple: (fenz_rank_index, hhstj_rank_index)
    Lower index = higher rank
    """
    fenz_index = FENZ_RANK_HIERARCHY.index(fenz_prefix) if fenz_prefix in FENZ_RANK_HIERARCHY else 999
    hhstj_index = HHSTJ_RANK_HIERARCHY.index(hhstj_prefix) if hhstj_prefix in HHSTJ_RANK_HIERARCHY else 999
    return (fenz_index, hhstj_index)


def validate_nickname(nickname: str) -> bool:
    """
    Validate that a nickname meets Discord's requirements
    Returns True if valid, False otherwise
    """
    if not nickname:
        return False

    # Discord nickname requirements:
    # - 1-32 characters
    # - Cannot be only whitespace
    # - Cannot have trailing/leading whitespace
    # - Cannot end with a hyphen followed by nothing

    if len(nickname) > 32 or len(nickname) < 1:
        return False

    if nickname != nickname.strip():
        return False

    # Check for invalid patterns like "DNC-" or "CO- "
    if nickname.endswith('-') or '- ' in nickname or ' -' in nickname:
        return False

    return True


def format_nickname(fenz_prefix: str, callsign: str, hhstj_prefix: str, roblox_username: str,
                    has_fenz_high_command: bool = False, has_hhstj_high_command: bool = False) -> str:
    """
    Format nickname in standard format with strong fallback chain
    Priority: If HHStJ high command WITHOUT FENZ high command, format as:
    {HHStJ prefix} | {FENZ}-{callsign} | {Roblox username}

    Otherwise: {FENZ prefix}-{callsign} | {HHStJ prefix} | {Roblox username}

    GUARANTEED to return a valid nickname under 32 characters
    """

    # Helper function to build and validate
    def try_format(parts: list) -> str:
        """Try to format parts, return None if invalid"""
        if not parts:
            return None
        result = " | ".join(parts)
        if validate_nickname(result) and len(result) <= 32:
            return result
        return None

    nickname_parts = []

    # Determine priority
    hhstj_priority = has_hhstj_high_command and not has_fenz_high_command

    # === ATTEMPT 1: Full nickname with all components ===
    if callsign in ["###", "BLANK"]:
        # Special handling for ### and BLANK
        if hhstj_priority and hhstj_prefix:
            if fenz_prefix and callsign == "BLANK":
                nickname_parts = [hhstj_prefix, fenz_prefix, roblox_username]
            elif fenz_prefix and callsign == "###":
                nickname_parts = [hhstj_prefix, f"{fenz_prefix}-###", roblox_username]
            else:
                nickname_parts = [hhstj_prefix, roblox_username]
        else:
            if fenz_prefix and callsign == "BLANK":
                nickname_parts = [fenz_prefix]
            elif fenz_prefix and callsign == "###":
                nickname_parts = [f"{fenz_prefix}-###"]
            else:
                nickname_parts = ["###"]

            if hhstj_prefix and "-" not in hhstj_prefix:
                nickname_parts.append(hhstj_prefix)
            if roblox_username:
                nickname_parts.append(roblox_username)
    else:
        # Normal callsign
        if hhstj_priority and hhstj_prefix:
            nickname_parts = [hhstj_prefix]
            if fenz_prefix:
                nickname_parts.append(f"{fenz_prefix}-{callsign}")
            else:
                nickname_parts.append(callsign)
            if roblox_username:
                nickname_parts.append(roblox_username)
        else:
            if fenz_prefix:
                nickname_parts.append(f"{fenz_prefix}-{callsign}")
            else:
                nickname_parts.append(callsign)

            if hhstj_prefix and "-" not in hhstj_prefix:
                nickname_parts.append(hhstj_prefix)
            if roblox_username:
                nickname_parts.append(roblox_username)

    # Try full nickname
    result = try_format(nickname_parts)
    if result:
        return result

    # === ATTEMPT 2: Remove middle component (usually HHStJ or less important prefix) ===
    if len(nickname_parts) >= 3:
        fallback_parts = [nickname_parts[0], nickname_parts[-1]]
        result = try_format(fallback_parts)
        if result:
            return result

    # === ATTEMPT 3: Primary identifier only ===
    if hhstj_priority and hhstj_prefix:
        result = try_format([hhstj_prefix])
        if result:
            return result

    if fenz_prefix and callsign and callsign not in ["###", "BLANK"]:
        result = try_format([f"{fenz_prefix}-{callsign}"])
        if result:
            return result

    if fenz_prefix and callsign == "###":
        result = try_format([f"{fenz_prefix}-###"])
        if result:
            return result

    if callsign and callsign not in ["BLANK"]:
        result = try_format([callsign])
        if result:
            return result

    # === ATTEMPT 4: Just roblox username ===
    if roblox_username:
        result = try_format([roblox_username])
        if result:
            return result

    # === ATTEMPT 5: Truncate roblox username if too long ===
    if roblox_username and len(roblox_username) > 32:
        truncated = roblox_username[:32]
        if validate_nickname(truncated):
            return truncated
        # Try removing trailing characters until valid
        for i in range(31, 0, -1):
            truncated = roblox_username[:i]
            if validate_nickname(truncated):
                return truncated

    # === ABSOLUTE LAST RESORT ===
    # This should theoretically never happen, but prevents crashes
    fallback_options = [
        fenz_prefix if fenz_prefix else None,
        hhstj_prefix if hhstj_prefix else None,
        callsign if callsign and callsign != "BLANK" else None,
        "User"  # Ultimate fallback
    ]

    for option in fallback_options:
        if option and validate_nickname(option) and len(option) <= 32:
            return option

    # If literally everything fails (should be impossible)
    return "User"

async def check_callsign_exists(callsign: str, fenz_prefix: str = None) -> dict:
    """Check if a callsign exists in the database with the same prefix"""
    async with db.pool.acquire() as conn:
        # BLANK and ### callsigns are allowed to be non-unique, skip check
        if callsign in ["BLANK", "###"]:
            return None

        # Check for same callsign WITH same prefix
        if fenz_prefix:
            row = await conn.fetchrow(
                'SELECT * FROM callsigns WHERE callsign = $1 AND fenz_prefix = $2',
                callsign, fenz_prefix
            )
        else:
            # If no prefix provided, check for any match
            row = await conn.fetchrow(
                'SELECT * FROM callsigns WHERE callsign = $1',
                callsign
            )

        return dict(row) if row else None

def get_hhstj_prefix_from_roles(roles) -> str:
    """Get HHStJ prefix from roles, prioritizing management over clinical"""
    # First check for management roles (high command)
    for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
        if role_id in HHSTJ_HIGH_COMMAND_RANKS:
            if any(role.id == role_id for role in roles):
                return prefix

    # Then check for clinical roles
    for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
        if role_id not in HHSTJ_HIGH_COMMAND_RANKS:
            if any(role.id == role_id for role in roles):
                return prefix

    return ""

def format_duplicate_callsign_message(callsign: str, existing_data: dict) -> str:
    """Format a user-friendly message when a callsign is already taken"""
    # Build the full callsign display
    if existing_data['fenz_prefix']:
        full_callsign = f"{existing_data['fenz_prefix']}-{callsign}"
    else:
        full_callsign = callsign

    # Base message
    message = f"<:Denied:1426930694633816248> **Sorry, callsign {full_callsign} is already assigned!**\n\n"

    # Who has it
    message += f"**Currently assigned to:** <@{existing_data['discord_user_id']}>\n"
    message += f"**Discord:** {existing_data['discord_username']}\n"
    message += f"**Roblox:** {existing_data['roblox_username']}\n"

    # When assigned
    if existing_data.get('approved_at'):
        message += f"**Assigned:** < t: {int(existing_data['approved_at'].timestamp())}:R >\n"

    # What to do next
    message += f"\n'**What to do:**\n"
    message += f"Try a different callsign number\n"
    message += f"Use `/callsign lookup callsign:{callsign}` to verify\n"
    message += f"Contact an admin if you believe this is an error"

    return message


async def add_callsign_to_database(callsign: str, discord_user_id: int, discord_username: str,
                                   roblox_user_id: str, roblox_username: str, fenz_prefix: str,
                                   hhstj_prefix: str, approved_by_id: int, approved_by_name: str,
                                   is_fenz_high_command: bool = False, is_hhstj_high_command: bool = False):
    """Add a new callsign to the database"""
    async with db.pool.acquire() as conn:
        async with conn.transaction():  # <:Accepted:1426930333789585509> Proper transaction
            # <:Accepted:1426930333789585509> CHECK FOR CONFLICTS FIRST (before any DELETE)
            if callsign not in ["BLANK", "###"]:
                existing = await conn.fetchrow(
                    'SELECT discord_user_id FROM callsigns WHERE callsign = $1 AND fenz_prefix = $2',
                    callsign, fenz_prefix
                )
                if existing and existing['discord_user_id'] != discord_user_id:
                    raise ValueError(
                        f"Callsign {fenz_prefix}-{callsign} is already assigned to user {existing['discord_user_id']}")

            # Check if user already has ANY callsign
            old_callsigns = await conn.fetch(
                'SELECT * FROM callsigns WHERE discord_user_id = $1',
                discord_user_id
            )

            # Store history of previous callsigns
            history = []
            for old_data in old_callsigns:
                history.append({
                    "callsign": old_data.get("callsign"),
                    "fenz_prefix": old_data.get("fenz_prefix"),
                    "hhstj_prefix": old_data.get("hhstj_prefix"),
                    "approved_at": old_data.get("approved_at").isoformat() if old_data.get("approved_at") else None,
                    "replaced_at": int(datetime.utcnow().timestamp())
                })

            # Delete ALL old callsigns for this user
            await conn.execute(
                'DELETE FROM callsigns WHERE discord_user_id = $1',
                discord_user_id
            )

            # Insert the new callsign
            await conn.execute(
                '''INSERT INTO callsigns
                   (callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
                    fenz_prefix, hhstj_prefix, approved_by_id, approved_by_name, callsign_history)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''',
                callsign,
                discord_user_id,
                discord_username,
                roblox_user_id,
                roblox_username,
                fenz_prefix,
                hhstj_prefix,
                approved_by_id,
                approved_by_name,
                json.dumps(history)
            )

class CallsignCog(commands.Cog):
    callsign_group = app_commands.Group(name="callsign", description="Callsign management commands")

    def __init__(self, bot):
        self.bot = bot
        self.sync_interval = 60  # 60 minutes
        # Start auto-sync on bot startup
        self.auto_sync_loop.start()

    def cog_unload(self):
        """Stop the auto-sync loop when cog is unloaded"""
        self.auto_sync_loop.cancel()

    async def reload_data(self):
        async with db.pool.acquire() as conn:
            self.active_watches = await conn.fetch("SELECT * FROM callsigns;")
        print("<:Accepted:1426930333789585509> Reloaded callsigns cache")

    @staticmethod
    def strip_shift_prefixes(nickname: str) -> str:
        """
        Remove shift-related prefixes from nicknames before comparison
        Returns the nickname without DUTY, BRK, or LOA prefixes
        """
        prefixes_to_strip = ["DUTY | ", "BRK | ", "LOA | "]

        for prefix in prefixes_to_strip:
            if nickname.startswith(prefix):
                return nickname[len(prefix):]

        return nickname

    @tasks.loop(minutes=60)
    async def auto_sync_loop(self):
        """Background task for automatic syncing with nickname updates"""
        if db.pool is None:
            print("⚠️ Auto-sync skipped: database not connected")
            return

        for guild in self.bot.guilds:
            # Skip excluded guilds
            if guild.id in EXCLUDED_GUILDS:
                print(f"⏭️ Skipping auto-sync for excluded guild: {guild.name} ({guild.id})")
                continue

            try:
                sync_start_time = datetime.utcnow()

                async with db.pool.acquire() as conn:
                    callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

                # Track detailed statistics with actual changes
                stats = {
                    'total_callsigns': len(callsigns),
                    'members_found': 0,
                    'members_not_found': 0,
                    'nickname_updates': 0,
                    'rank_updates': 0,
                    'callsigns_reset': [],
                    'added_from_sheets': 0,
                    'removed_inactive': 0,
                    'last_seen_updates': 0,
                    'errors': [],
                    'nickname_changes': [],
                    'rank_changes': [],
                    'removed_users': [],
                    'added_users': [],
                    # <:Accepted:1426930333789585509> NEW: Naughty role tracking
                    'naughty_roles_found': 0,
                    'naughty_roles_stored': 0,
                    'naughty_roles_removed': 0,
                    'permission_errors': [],
                }

                # <:Accepted:1426930333789585509> NEW: Track all naughty roles in the server during sync
                naughty_role_data = []

                # Check each callsign in database
                for record in callsigns:
                    member = guild.get_member(record['discord_user_id'])

                    if member:
                        stats['members_found'] += 1

                        # Update last_seen_at to now
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                'UPDATE callsigns SET last_seen_at = NOW() WHERE discord_user_id = $1',
                                member.id
                            )
                        stats['last_seen_updates'] += 1

                        # <:Accepted:1426930333789585509> NEW: Check for naughty roles on this member
                        member_naughty_roles = []
                        for role in member.roles:
                            if role.id in NAUGHTY_ROLES:
                                member_naughty_roles.append({
                                    'discord_user_id': member.id,
                                    'discord_username': str(member),
                                    'role_id': role.id,
                                    'role_name': NAUGHTY_ROLES[role.id]
                                })
                                stats['naughty_roles_found'] += 1

                        if member_naughty_roles:
                            naughty_role_data.extend(member_naughty_roles)

                    else:
                        stats['members_not_found'] += 1
                        # User not in server - check if been gone > 7 days
                        last_seen = record.get('last_seen_at')
                        if last_seen:
                            days_gone = (datetime.utcnow() - last_seen).days
                            if days_gone >= 7:
                                # Store removal details
                                callsign_display = f"{record['fenz_prefix']}-{record['callsign']}" if record[
                                    'fenz_prefix'] else record['callsign']
                                stats['removed_users'].append({
                                    'username': record['discord_username'],
                                    'id': record['discord_user_id'],
                                    'callsign': callsign_display,
                                    'days_gone': days_gone,
                                    'reason': f'Inactive for {days_gone} days'
                                })

                                # Remove from database
                                async with db.pool.acquire() as conn:
                                    await conn.execute(
                                        'DELETE FROM callsigns WHERE discord_user_id = $1',
                                        record['discord_user_id']
                                    )
                                # Remove from sheets
                                await sheets_manager.remove_callsign_from_sheets(record['discord_user_id'])
                                stats['removed_inactive'] += 1
                                continue

                # <:Accepted:1426930333789585509> NEW: Sync naughty roles to database
                if naughty_role_data:
                    async with db.pool.acquire() as conn:
                        # Get all currently stored naughty roles
                        stored_roles = await conn.fetch(
                            'SELECT discord_user_id, role_id FROM naughty_roles WHERE removed_at IS NULL'
                        )
                        stored_set = {(r['discord_user_id'], r['role_id']) for r in stored_roles}

                        # Current roles in server
                        current_set = {(r['discord_user_id'], r['role_id']) for r in naughty_role_data}

                        # Roles to add (in server but not in DB)
                        to_add = current_set - stored_set

                        # Roles to remove (in DB but not in server)
                        to_remove = stored_set - current_set

                        # Add new naughty roles
                        for user_id, role_id in to_add:
                            role_info = next(r for r in naughty_role_data if
                                             r['discord_user_id'] == user_id and r['role_id'] == role_id)
                            await conn.execute(
                                '''INSERT INTO naughty_roles
                                       (discord_user_id, discord_username, role_id, role_name, last_seen_at)
                                   VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (discord_user_id, role_id) 
                                   DO UPDATE SET removed_at = NULL, last_seen_at = NOW(), discord_username = $2''',
                                role_info['discord_user_id'],
                                role_info['discord_username'],
                                role_info['role_id'],
                                role_info['role_name']
                            )
                            stats['naughty_roles_stored'] += 1

                        # Mark removed roles (user no longer has them)
                        for user_id, role_id in to_remove:
                            await conn.execute(
                                '''UPDATE naughty_roles
                                   SET removed_at = NOW()
                                   WHERE discord_user_id = $1
                                     AND role_id = $2
                                     AND removed_at IS NULL''',
                                user_id, role_id
                            )
                            stats['naughty_roles_removed'] += 1

                        # Update last_seen_at for all current naughty roles
                        for role_info in naughty_role_data:
                            await conn.execute(
                                '''UPDATE naughty_roles
                                   SET last_seen_at = NOW()
                                   WHERE discord_user_id = $1
                                     AND role_id = $2''',
                                role_info['discord_user_id'],
                                role_info['role_id']
                            )

                    # âœ… Track individual naughty role changes for detailed logging (OUTSIDE transaction)
                    stats['naughty_role_details'] = {
                        'added': [],
                        'removed': []
                    }

                    # Track who got roles added
                    for user_id, role_id in to_add:
                        role_info = next(r for r in naughty_role_data if
                                       r['discord_user_id'] == user_id and r['role_id'] == role_id)
                        member = guild.get_member(user_id)
                        if member:
                            stats['naughty_role_details']['added'].append({
                                'member': member,
                                'role_name': role_info['role_name']
                            })

                    # Track who got roles removed
                    for user_id, role_id in to_remove:
                        member = guild.get_member(user_id)
                        role_name = NAUGHTY_ROLES.get(role_id, "Unknown")
                        if member:
                            stats['naughty_role_details']['removed'].append({
                                'member': member,
                                'role_name': role_name
                            })

                if callsigns:
                    callsign_data = []

                    # First, check for entries in sheets but not in database
                    sheet_callsigns = await sheets_manager.get_all_callsigns_from_sheets()
                    sheet_map = {cs['discord_user_id']: cs for cs in sheet_callsigns}
                    db_map = {record['discord_user_id']: dict(record) for record in callsigns}

                    # Add missing entries from sheets → database
                    for discord_id, sheet_data in sheet_map.items():
                        if discord_id not in db_map:
                            member = guild.get_member(discord_id)
                            if member:
                                bloxlink_data = await self.get_bloxlink_data(member.id, guild.id)
                                if bloxlink_data:
                                    roblox_id = bloxlink_data['id']
                                    roblox_username = await self.get_roblox_user_from_id(roblox_id)

                                    if roblox_username:
                                        hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)
                                        is_fenz_high_command = any(
                                            role.id in HIGH_COMMAND_RANKS for role in member.roles)
                                        is_hhstj_high_command = any(
                                            role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                                        await add_callsign_to_database(
                                            sheet_data['callsign'],
                                            discord_id,
                                            str(member),
                                            roblox_id,
                                            roblox_username,
                                            sheet_data['fenz_prefix'],
                                            hhstj_prefix or '',
                                            self.bot.user.id,
                                            "Auto-sync",
                                            is_fenz_high_command,
                                            is_hhstj_high_command
                                        )

                                        # Track addition
                                        callsign_display = f"{sheet_data['fenz_prefix']}-{sheet_data['callsign']}" if \
                                            sheet_data['fenz_prefix'] else sheet_data['callsign']
                                        stats['added_users'].append({
                                            'member': member,
                                            'callsign': callsign_display
                                        })
                                        stats['added_from_sheets'] += 1

                    # Re-fetch database if we added entries
                    if stats['added_from_sheets'] > 0:
                        async with db.pool.acquire() as conn:
                            callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')
                        stats['total_callsigns'] = len(callsigns)

                    for record in callsigns:
                        member = guild.get_member(record['discord_user_id'])

                        if not member:
                            # User not in guild, just add to sheets data
                            rank_type, rank_data = sheets_manager.determine_rank_type([])
                            is_command_rank = False

                            callsign_data.append({
                                'fenz_prefix': record['fenz_prefix'] or '',
                                'hhstj_prefix': record['hhstj_prefix'] or '',
                                'callsign': record['callsign'],
                                'discord_user_id': record['discord_user_id'],
                                'discord_username': record['discord_username'],
                                'roblox_user_id': record['roblox_user_id'],
                                'roblox_username': record['roblox_username'],
                                'is_command': is_command_rank,
                                'strikes': None,
                                'qualifications': None
                            })
                            continue

                        # CHECK FOR RANK CHANGES
                        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                        current_fenz_prefix = record['fenz_prefix']
                        current_hhstj_prefix = record['hhstj_prefix']
                        current_callsign = record['callsign']

                        # Get correct FENZ rank from current roles
                        correct_fenz_prefix = None
                        for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                            if any(role.id == role_id for role in member.roles):
                                correct_fenz_prefix = prefix
                                break

                        # Get correct HHStJ rank from current roles
                        correct_hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)

                        # DETECT RANK CHANGES AND RESET CALLSIGN
                        fenz_rank_changed = correct_fenz_prefix and correct_fenz_prefix != current_fenz_prefix

                        # Special case: high command can choose no prefix, so don't count "" as a change
                        if is_fenz_high_command and current_fenz_prefix == "":
                            fenz_rank_changed = False

                        # If FENZ rank changed AND they have a real callsign (not ### or BLANK), reset it
                        if fenz_rank_changed and current_callsign not in ["###", "BLANK"]:
                            old_callsign = f"{current_fenz_prefix}-{current_callsign}" if current_fenz_prefix else current_callsign

                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET callsign = $1, fenz_prefix = $2 WHERE discord_user_id = $3',
                                    "###", correct_fenz_prefix, member.id
                                )

                            stats['callsigns_reset'].append({
                                'member': member,
                                'old_callsign': old_callsign,
                                'new_prefix': correct_fenz_prefix,
                                'reason': f'FENZ rank changed: {current_fenz_prefix} → {correct_fenz_prefix}'
                            })

                            current_fenz_prefix = correct_fenz_prefix
                            current_callsign = "###"
                            stats['rank_updates'] += 1
                        elif fenz_rank_changed:
                            # Rank changed but callsign is already ### or BLANK, just update prefix
                            stats['rank_changes'].append({
                                'member': member,
                                'old_rank': current_fenz_prefix or 'None',
                                'new_rank': correct_fenz_prefix,
                                'type': 'FENZ'
                            })

                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET fenz_prefix = $1 WHERE discord_user_id = $2',
                                    correct_fenz_prefix, member.id
                                )
                            current_fenz_prefix = correct_fenz_prefix
                            stats['rank_updates'] += 1

                        # Update HHStJ prefix if changed
                        if correct_hhstj_prefix != current_hhstj_prefix:
                            stats['rank_changes'].append({
                                'member': member,
                                'old_rank': current_hhstj_prefix or 'None',
                                'new_rank': correct_hhstj_prefix or 'None',
                                'type': 'HHStJ'
                            })

                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET hhstj_prefix = $1 WHERE discord_user_id = $2',
                                    correct_hhstj_prefix or '', member.id
                                )
                            current_hhstj_prefix = correct_hhstj_prefix
                            stats['rank_updates'] += 1

                        # UPDATE NICKNAME
                        if current_callsign == "###":
                            # Reset callsign - nickname should be: PREFIX-### | HHStJ | Roblox
                            nickname_parts = []
                            if current_fenz_prefix:
                                nickname_parts.append(f"{current_fenz_prefix}-###")
                            if current_hhstj_prefix and "-" not in current_hhstj_prefix:
                                nickname_parts.append(current_hhstj_prefix)
                            if record['roblox_username']:
                                nickname_parts.append(record['roblox_username'])
                            expected_nickname = " | ".join(nickname_parts) if nickname_parts else record[
                                'roblox_username']
                        else:
                            expected_nickname = format_nickname(
                                current_fenz_prefix,
                                current_callsign,
                                current_hhstj_prefix,
                                record['roblox_username'],
                                is_fenz_high_command,
                                is_hhstj_high_command
                            )

                        current_nick_stripped = self.strip_shift_prefixes(member.nick) if member.nick else member.name
                        expected_nick_stripped = self.strip_shift_prefixes(expected_nickname)

                        # Only update if the stripped versions don't match
                        if current_nick_stripped != expected_nick_stripped:
                            try:
                                # Store old nickname for logging
                                old_nickname = member.nick or member.name

                                # Preserve shift prefix if it exists
                                shift_prefix = ""
                                if member.nick:
                                    for prefix in ["DUTY | ", "BRK | ", "LOA | "]:
                                        if member.nick.startswith(prefix):
                                            shift_prefix = prefix
                                            break

                                # Apply shift prefix back to new nickname
                                final_nickname = shift_prefix + expected_nickname

                                await member.edit(nick=final_nickname)

                                # Track nickname change
                                stats['nickname_changes'].append({
                                    'member': member,
                                    'old': old_nickname,
                                    'new': final_nickname
                                })
                                stats['nickname_updates'] += 1
                            except discord.Forbidden:
                                # Track permission errors separately for cleaner logging
                                stats['permission_errors'].append(member)
                                stats['errors'].append({
                                    'member': member,
                                    'username': record['discord_username'],
                                    'error': 'Missing permissions'
                                })
                            except Exception as e:
                                stats['errors'].append({
                                    'member': member,
                                    'username': record['discord_username'],
                                    'error': str(e)
                                })

                        # Determine rank type for sheets
                        rank_type, rank_data = sheets_manager.determine_rank_type(member.roles)
                        is_command_rank = (rank_type == 'command')

                        callsign_data.append({
                            'fenz_prefix': current_fenz_prefix or '',
                            'hhstj_prefix': current_hhstj_prefix or '',
                            'callsign': current_callsign,
                            'discord_user_id': record['discord_user_id'],
                            'discord_username': record['discord_username'],
                            'roblox_user_id': record['roblox_user_id'],
                            'roblox_username': record['roblox_username'],
                            'is_command': is_command_rank,
                            'strikes': sheets_manager.determine_strikes_value(member.roles) if member else None,
                            'qualifications': sheets_manager.determine_qualifications(member.roles,
                                                                                      is_command_rank) if member else None
                        })

                    # Sort by rank hierarchy
                    callsign_data.sort(key=lambda x: get_rank_sort_key(x['fenz_prefix'], x['hhstj_prefix']))

                    # Update Google Sheets
                    await sheets_manager.batch_update_callsigns(callsign_data)

                    # Calculate sync duration
                    sync_duration = (datetime.utcnow() - sync_start_time).total_seconds()

                    # Send enhanced log with DETAILED changes
                    await self.send_detailed_sync_log(self.bot, guild.name, stats, sync_duration)

                    print(f"<:Accepted:1426930333789585509> Auto-sync completed for guild {guild.name}:")
                    print(f"    📊 {stats['total_callsigns']} callsigns synced to Google Sheets")
                    print(f"    👥 {stats['members_found']} members found / {stats['members_not_found']} not in server")
                    print(f"    🏷️ {stats['nickname_updates']} nicknames updated")
                    print(f"    🎖️ {stats['rank_updates']} rank changes detected and saved")
                    if stats['added_from_sheets'] > 0:
                        print(f"    ➕ {stats['added_from_sheets']} added from sheets")
                    if stats['removed_inactive'] > 0:
                        print(f"    🗑️ {stats['removed_inactive']} removed (inactive 7+ days)")
                    if stats['callsigns_reset']:
                        print(f"    🔄 {len(stats['callsigns_reset'])} callsigns reset due to rank changes")
                    # <:Accepted:1426930333789585509> NEW: Print naughty role stats
                    if stats['naughty_roles_found'] > 0:
                        print(f"    🚨 {stats['naughty_roles_found']} naughty roles found")
                        print(f"    💾 {stats['naughty_roles_stored']} new naughty roles stored")
                        print(f"    ✂️ {stats['naughty_roles_removed']} naughty roles removed")
                    if stats['permission_errors']:
                        print(f"    ⚠️ {len(stats['permission_errors'])} permission errors")
                    if stats['errors']:
                        non_perm_errors = [e for e in stats['errors'] if e['error'] != 'Missing permissions']
                        if non_perm_errors:
                            print(f"    ⚠️ {len(non_perm_errors)} other errors occurred")
                    print(f"    ⏱️ Completed in {sync_duration:.2f}s")

            except Exception as e:
                print(f"<:Denied:1426930694633816248> Error during auto-sync for {guild.name}: {e}")
                import traceback
                traceback.print_exc()

                # Send error log
                try:
                    await self.send_sync_log(
                        self.bot,
                        "<:Denied:1426930694633816248> Auto-Sync Failed",
                        f"Auto-sync failed for **{guild.name}**",
                        [{'name': 'Error', 'value': f'```{str(e)[:1000]}```', 'inline': False}],
                        discord.Color.red()
                    )
                except:
                    pass

    async def send_detailed_sync_log(self, bot, guild_name: str, stats: dict, sync_duration: float):
        """Send detailed sync logs with specific changes to designated channel"""
        try:
            channel = bot.get_channel(SYNC_LOG_CHANNEL_ID)
            if not channel:
                print(f"⚠️ Could not find sync log channel {SYNC_LOG_CHANNEL_ID}")
                return

            # Main summary embed
            summary_embed = discord.Embed(
                title="Auto-Sync Completed",
                description=f"Automatic sync completed for **{guild_name}**",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )

            summary_embed.add_field(name='Total Callsigns', value=str(stats['total_callsigns']), inline=True)
            summary_embed.add_field(name='Members Found', value=str(stats['members_found']), inline=True)
            summary_embed.add_field(name='Not in Server', value=str(stats['members_not_found']), inline=True)
            summary_embed.add_field(name='Nicknames Updated', value=str(stats['nickname_updates']), inline=True)
            summary_embed.add_field(name='Rank Changes', value=str(stats['rank_updates']), inline=True)
            summary_embed.add_field(name='Duration', value=f'{sync_duration:.2f}s', inline=True)

            if stats['permission_errors']:
                # Create mentions string (limit to avoid embed length issues)
                mentions = ' '.join([member.mention for member in stats['permission_errors'][:25]])
                if len(stats['permission_errors']) > 25:
                    mentions += f"\n... and {len(stats['permission_errors']) - 25} more"

                summary_embed.add_field(
                    name=f'Permission Errors ({len(stats["permission_errors"])})',
                    value=mentions,
                    inline=False
                )

            # <:Accepted:1426930333789585509> Naughty role stats
            if stats.get('naughty_roles_found', 0) > 0:
                summary_embed.add_field(
                    name='Naughty Roles',
                    value=f"Found: {stats['naughty_roles_found']}\n"
                          f"Stored: {stats['naughty_roles_stored']}\n"
                          f"Removed: {stats['naughty_roles_removed']}",
                    inline=True
                )

            await channel.send(embed=summary_embed)

            # 1. Nickname Changes - SHOW WHO AND WHAT CHANGED
            if stats['nickname_changes']:
                for i in range(0, len(stats['nickname_changes']), 5):
                    chunk = stats['nickname_changes'][i:i + 5]

                    embed = discord.Embed(
                        title=f"Nickname Updates ({i + 1}-{min(i + 5, len(stats['nickname_changes']))} of {len(stats['nickname_changes'])})",
                        color=discord.Color.green()
                    )

                    for change in chunk:
                        embed.add_field(
                            name=f"{change['member'].mention} ({change['member'].display_name})",
                            value=f"**Before:** `{change['old']}`\n**After:** `{change['new']}`",
                            inline=False
                        )

                    await channel.send(embed=embed)

            # 2. Rank Changes - SHOW WHO AND WHAT RANK CHANGED
            if stats['rank_changes']:
                for i in range(0, len(stats['rank_changes']), 5):
                    chunk = stats['rank_changes'][i:i + 5]

                    embed = discord.Embed(
                        title=f"Rank Changes ({i + 1}-{min(i + 5, len(stats['rank_changes']))} of {len(stats['rank_changes'])})",
                        color=discord.Color.gold()
                    )

                    for change in chunk:
                        embed.add_field(
                            name=f"{change['member'].mention} ({change['member'].display_name})",
                            value=f"**Type:** {change['type']}\n**{change['old_rank']}** → **{change['new_rank']}**",
                            inline=False
                        )

                    await channel.send(embed=embed)

            # 3. Callsigns Reset - SHOW WHO AND WHY
            if stats['callsigns_reset']:
                for i in range(0, len(stats['callsigns_reset']), 5):
                    chunk = stats['callsigns_reset'][i:i + 5]

                    embed = discord.Embed(
                        title=f"Callsigns Reset ({i + 1}-{min(i + 5, len(stats['callsigns_reset']))} of {len(stats['callsigns_reset'])})",
                        description="These callsigns were reset to ### due to rank changes",
                        color=discord.Color.orange()
                    )

                    for reset in chunk:
                        embed.add_field(
                            name=f"{reset['member'].mention} ({reset['member'].display_name})",
                            value=f"**Old:** {reset['old_callsign']}\n**New:** {reset['new_prefix']}-###\n**Reason:** {reset['reason']}",
                            inline=False
                        )

                    await channel.send(embed=embed)

            # 4. Added from Sheets - SHOW WHO WAS ADDED
            if stats['added_users']:
                for i in range(0, len(stats['added_users']), 10):
                    chunk = stats['added_users'][i:i + 10]

                    embed = discord.Embed(
                        title=f"Added from Sheets ({i + 1}-{min(i + 10, len(stats['added_users']))} of {len(stats['added_users'])})",
                        description="Users found in sheets but not in database (now added)",
                        color=discord.Color.teal()
                    )

                    for added in chunk:
                        embed.add_field(
                            name=f"{added['member'].mention} ({added['member'].display_name})",
                            value=f"**Callsign:** {added['callsign']}",
                            inline=True
                        )

                    await channel.send(embed=embed)

            # 5. Removed Users - SHOW WHO WAS REMOVED AND WHY
            if stats['removed_users']:
                for i in range(0, len(stats['removed_users']), 5):
                    chunk = stats['removed_users'][i:i + 5]

                    embed = discord.Embed(
                        title=f"Removed (Inactive) ({i + 1}-{min(i + 5, len(stats['removed_users']))} of {len(stats['removed_users'])})",
                        description="Users removed from database (not in server for 7+ days)",
                        color=discord.Color.dark_red()
                    )

                    for removed in chunk:
                        embed.add_field(
                            name=f"{removed['username']} (ID: {removed['id']})",
                            value=f"**Callsign:** {removed['callsign']}\n**Days Gone:** {removed['days_gone']}\n**Reason:** {removed['reason']}",
                            inline=False
                        )

                    await channel.send(embed=embed)

            # 6. Naughty Roles Added - SHOW WHO GOT NAUGHTY ROLES
            if stats.get('naughty_role_details', {}).get('added'):
                added_roles = stats['naughty_role_details']['added']
                for i in range(0, len(added_roles), 10):
                    chunk = added_roles[i:i + 10]

                    embed = discord.Embed(
                        title=f"Naughty Roles Added ({i + 1}-{min(i + 10, len(added_roles))} of {len(added_roles)})",
                        description="These users received naughty roles during sync",
                        color=discord.Color.red()
                    )

                    for item in chunk:
                        embed.add_field(
                            name=f"{item['member'].mention} ({item['member'].display_name})",
                            value=f"**Role:** {item['role_name']}",
                            inline=True
                        )

                    await channel.send(embed=embed)

            # 7. Naughty Roles Removed - SHOW WHO LOST NAUGHTY ROLES
            if stats.get('naughty_role_details', {}).get('removed'):
                removed_roles = stats['naughty_role_details']['removed']
                for i in range(0, len(removed_roles), 10):
                    chunk = removed_roles[i:i + 10]

                    embed = discord.Embed(
                        title=f"Naughty Roles Removed ({i + 1}-{min(i + 10, len(removed_roles))} of {len(removed_roles)})",
                        description="These users no longer have naughty roles",
                        color=discord.Color.green()
                    )

                    for item in chunk:
                        embed.add_field(
                            name=f"{item['member'].mention} ({item['member'].display_name})",
                            value=f"**Role:** {item['role_name']}",
                            inline=True
                        )

                    await channel.send(embed=embed)

            # 8. Errors - SHOW WHO HAD NON-PERMISSION ERRORS
            # Filter out permission errors since they're shown in the summary
            non_permission_errors = [e for e in stats['errors'] if e['error'] != 'Missing permissions']

            if non_permission_errors:
                for i in range(0, len(non_permission_errors), 5):
                    chunk = non_permission_errors[i:i + 5]

                    embed = discord.Embed(
                        title=f"<:Denied:1426930694633816248> Other Errors ({i + 1}-{min(i + 5, len(non_permission_errors))} of {len(non_permission_errors)})",
                        description="Non-permission errors that occurred during sync",
                        color=discord.Color.red()
                    )

                    for error in chunk:
                        member_mention = error['member'].mention if error.get('member') else f"`{error['username']}`"
                        embed.add_field(
                            name=member_mention,
                            value=f"**Error:** {error['error']}",
                            inline=False
                        )

                    await channel.send(embed=embed)

        except Exception as e:
            print(f"<:Denied:1426930694633816248> Error sending detailed sync log: {e}")
            import traceback
            traceback.print_exc()

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Restore naughty roles when a user rejoins"""
        # Skip excluded guilds
        if member.guild.id in EXCLUDED_GUILDS:
            return

        try:
            async with db.pool.acquire() as conn:
                # Get all active naughty roles for this user
                stored_roles = await conn.fetch(
                    '''SELECT role_id, role_name
                       FROM naughty_roles
                       WHERE discord_user_id = $1
                         AND removed_at IS NULL''',
                    member.id
                )

            if not stored_roles:
                return

            # Re-assign each naughty role
            restored_roles = []
            failed_roles = []

            for record in stored_roles:
                role_id = record['role_id']
                role_name = record['role_name']

                role = member.guild.get_role(role_id)
                if role:
                    try:
                        await member.add_roles(role, reason="Auto-restored naughty role on rejoin")
                        restored_roles.append(role_name)

                        # Update last_seen_at
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                '''UPDATE naughty_roles
                                   SET last_seen_at = NOW()
                                   WHERE discord_user_id = $1
                                     AND role_id = $2''',
                                member.id, role_id
                            )
                    except discord.Forbidden:
                        failed_roles.append(role_name)
                else:
                    failed_roles.append(f"{role_name} (role not found)")

            # Log the restoration
            if restored_roles or failed_roles:
                channel = self.bot.get_channel(SYNC_LOG_CHANNEL_ID)
                if channel:
                    embed = discord.Embed(
                        title="Naughty Roles Restored on Rejoin",
                        description=f"{member.mention} rejoined the server",
                        color=discord.Color.red(),
                        timestamp=datetime.utcnow()
                    )

                    if restored_roles:
                        embed.add_field(
                            name="<:Accepted:1426930333789585509> Restored Roles",
                            value="\n".join(f"• {name}" for name in restored_roles),
                            inline=False
                        )

                    if failed_roles:
                        embed.add_field(
                            name="<:Denied:1426930694633816248> Failed to Restore",
                            value="\n".join(f"• {name}" for name in failed_roles),
                            inline=False
                        )

                    embed.set_footer(text=f"User ID: {member.id}")
                    await channel.send(embed=embed)

        except Exception as e:
            print(f"Error restoring naughty roles for {member.id}: {e}")
            import traceback
            traceback.print_exc()

    @staticmethod
    async def send_callsign_request_log(bot, user: discord.Member, callsign: str, fenz_prefix: str,
                                        hhstj_prefix: str, roblox_username: str, approved: bool = True):
        """Send callsign request logs to designated channel"""
        try:
            channel = bot.get_channel(CALLSIGN_REQUEST_LOG_CHANNEL_ID)
            if not channel:
                print(f"Could not find callsign request log channel {CALLSIGN_REQUEST_LOG_CHANNEL_ID}")
                return

            embed = discord.Embed(
                title="Callsign Request" + (" - Approved <:Accepted:1426930333789585509>" if approved else " - Failed <:Denied:1426930694633816248>"),
                color=discord.Color.green() if approved else discord.Color.red(),
                timestamp=datetime.utcnow()
            )

            # Format callsign display
            if fenz_prefix:
                full_callsign = f"{fenz_prefix}-{callsign}"
            else:
                full_callsign = callsign

            embed.add_field(name="User", value=f"{user.mention}\n`{user.display_name}`", inline=True)
            embed.add_field(name="Requested Callsign", value=f"`{full_callsign}`", inline=True)
            embed.add_field(name="FENZ Rank", value=f"`{fenz_prefix}`", inline=True)

            if hhstj_prefix:
                embed.add_field(name="HHStJ Rank", value=f"`{hhstj_prefix}`", inline=True)

            embed.add_field(name="Roblox Username", value=f"`{roblox_username}`", inline=True)
            embed.add_field(name="Status", value="Auto-approved <:Accepted:1426930333789585509>" if approved else "Failed <:Denied:1426930694633816248>", inline=True)

            embed.set_thumbnail(url=user.display_avatar.url)
            embed.set_footer(text=f"User ID: {user.id}")

            await channel.send(embed=embed)
        except Exception as e:
            print(f"<:Denied:1426930694633816248> Error sending callsign request log: {e}")

    @auto_sync_loop.before_loop
    async def before_auto_sync(self):
        """Wait for bot AND database to be ready before starting auto-sync"""
        await self.bot.wait_until_ready()

        # Wait for database connection to be established
        import asyncio
        while db.pool is None:
            print("⏳ Auto-sync waiting for database connection...")
            await asyncio.sleep(1)

        print("<:Accepted:1426930333789585509> Auto-sync ready - database connected")

    async def search_callsign_database(self, query: str, search_type: str) -> list:
        async with db.pool.acquire() as conn:
            if search_type == 'discord_id':
                rows = await conn.fetch(
                    'SELECT * FROM callsigns WHERE discord_user_id = $1',
                    int(query)
                )
            elif search_type == 'roblox_username':
                rows = await conn.fetch(
                    'SELECT * FROM callsigns WHERE LOWER(roblox_username) LIKE LOWER($1)',
                    f'%{query}%'
                )
            elif search_type == 'roblox_id':
                rows = await conn.fetch(
                    'SELECT * FROM callsigns WHERE roblox_user_id = $1',
                    query
                )
            else:
                return []

            return [dict(row) for row in rows]

    async def get_bloxlink_data(self, user_id: int, guild_id: int):
        """Get Roblox info from Bloxlink API"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        f'https://api.blox.link/v4/public/guilds/{guild_id}/discord-to-roblox/{user_id}',
                        headers={'Authorization': BLOXLINK_API_KEY}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {'id': str(data['robloxID'])}
            return None
        except Exception as e:
            print(f"Error fetching from Bloxlink: {e}")
            return None

    async def get_roblox_user_from_id(self, user_id: str):
        """Get Roblox username from user ID"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        f'https://users.roblox.com/v1/users/{user_id}'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get('name')
            return None
        except Exception as e:
            print(f"Error fetching Roblox username: {e}")
            return None

    @callsign_group.command(name="sync", description="Sync callsigns between database and Google Sheets")
    @app_commands.checks.has_role(SYNC_ROLE_ID)
    @app_commands.checks.has_permissions(administrator=True)
    async def sync_callsigns(self, interaction: discord.Interaction):
        """Bidirectional sync: database ↔ Google Sheets and update Discord nicknames"""
        await interaction.response.defer(thinking=True)

        # Safety check: ensure database is connected
        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database connection not available. Please try again in a moment.",
                ephemeral=True
            )
            return

        try:
            # Get all callsigns from database
            async with db.pool.acquire() as conn:
                db_callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

            # Get all callsigns from sheets
            sheet_callsigns = await sheets_manager.get_all_callsigns_from_sheets()

            # Create lookup maps
            db_map = {record['discord_user_id']: dict(record) for record in db_callsigns}
            sheet_map = {cs['discord_user_id']: cs for cs in sheet_callsigns}

            # Track additions from sheets → database
            added_from_sheets = 0
            missing_in_sheets = []

            # Find entries in sheets but not in database
            for discord_id, sheet_data in sheet_map.items():
                if discord_id not in db_map:
                    # Entry exists in sheet but not database - need more info to add it
                    member = interaction.guild.get_member(discord_id)
                    if member:
                        # Get Roblox info
                        bloxlink_data = await self.get_bloxlink_data(member.id, interaction.guild.id)
                        if bloxlink_data:
                            roblox_id = bloxlink_data['id']
                            roblox_username = await self.get_roblox_user_from_id(roblox_id)

                            if roblox_username:
                                # Get HHStJ rank from roles
                                hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)

                                # Add to database
                                is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                                is_hhstj_high_command = any(
                                    role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                                await add_callsign_to_database(
                                    sheet_data['callsign'],
                                    discord_id,
                                    str(member),
                                    roblox_id,
                                    roblox_username,
                                    sheet_data['fenz_prefix'],
                                    hhstj_prefix or '',
                                    self.bot.user.id,
                                    "Manual Sync",
                                    is_fenz_high_command,
                                    is_hhstj_high_command
                                )

                                added_from_sheets += 1
                    else:
                        missing_in_sheets.append(
                            f"Discord ID {discord_id} (callsign {sheet_data['callsign']}) - user not in server")

            # Re-fetch database after additions
            if added_from_sheets > 0:
                async with db.pool.acquire() as conn:
                    db_callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

            # Update Discord nicknames for users with callsigns
            updated_count = 0
            failed_updates = []
            callsigns_reset = []

            for record in db_callsigns:
                try:
                    record = dict(record)  # Convert to mutable dict
                    member = interaction.guild.get_member(record['discord_user_id'])

                    if member:
                        # Check if member has high command roles
                        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                        current_fenz_prefix = record['fenz_prefix']
                        current_hhstj_prefix = record['hhstj_prefix']
                        current_callsign = record['callsign']

                        # Get correct FENZ rank from current roles
                        correct_fenz_prefix = None
                        for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                            if any(role.id == role_id for role in member.roles):
                                correct_fenz_prefix = prefix
                                break

                        # Get correct HHStJ rank from current roles
                        correct_hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)

                        # DETECT RANK CHANGES AND RESET CALLSIGN
                        fenz_rank_changed = correct_fenz_prefix and correct_fenz_prefix != current_fenz_prefix

                        # Special case: high command can choose no prefix
                        if is_fenz_high_command and current_fenz_prefix == "":
                            fenz_rank_changed = False

                        # If FENZ rank changed AND they have a real callsign, reset it
                        if fenz_rank_changed and current_callsign not in ["###", "BLANK"]:
                            old_callsign = f"{current_fenz_prefix}-{current_callsign}" if current_fenz_prefix else current_callsign

                            existing = await check_callsign_exists(current_callsign, correct_fenz_prefix)
                            if existing and existing['discord_user_id'] != member.id:
                                # Callsign conflict - reset to ### instead
                                async with db.pool.acquire() as conn:
                                    await conn.execute(
                                        'UPDATE callsigns SET callsign = $1, fenz_prefix = $2 WHERE discord_user_id = $3',
                                        "###", correct_fenz_prefix, member.id
                                    )
                                callsigns_reset.append({
                                    'member': member,
                                    'old_callsign': old_callsign,
                                    'new_prefix': correct_fenz_prefix,
                                    'reason': f'Rank changed but callsign conflicts: {correct_fenz_prefix}-{current_callsign} already exists'
                                })
                                current_fenz_prefix = correct_fenz_prefix
                                current_callsign = "###"
                                stats['rank_updates'] += 1
                                continue

                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET callsign = $1, fenz_prefix = $2 WHERE discord_user_id = $3',
                                    "###", correct_fenz_prefix, member.id
                                )

                            callsigns_reset.append({
                                'member': member,
                                'old_callsign': old_callsign,
                                'new_prefix': correct_fenz_prefix
                            })

                            record['callsign'] = "###"
                            record['fenz_prefix'] = correct_fenz_prefix
                            current_fenz_prefix = correct_fenz_prefix
                            current_callsign = "###"
                        elif fenz_rank_changed:
                            # Just update prefix
                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET fenz_prefix = $1 WHERE discord_user_id = $2',
                                    correct_fenz_prefix, member.id
                                )
                            record['fenz_prefix'] = correct_fenz_prefix
                            current_fenz_prefix = correct_fenz_prefix

                        # Update HHStJ prefix in database if changed
                        if correct_hhstj_prefix != current_hhstj_prefix:
                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET hhstj_prefix = $1 WHERE discord_user_id = $2',
                                    correct_hhstj_prefix or '', member.id
                                )
                            record['hhstj_prefix'] = correct_hhstj_prefix
                            current_hhstj_prefix = correct_hhstj_prefix

                        # Calculate the correct nickname
                        if record['callsign'] == "###":
                            # Reset callsign - nickname should be: PREFIX-### | HHStJ | Roblox
                            nickname_parts = []
                            if current_fenz_prefix:
                                nickname_parts.append(f"{current_fenz_prefix}-###")
                            if current_hhstj_prefix and "-" not in current_hhstj_prefix:
                                nickname_parts.append(current_hhstj_prefix)
                            if record['roblox_username']:
                                nickname_parts.append(record['roblox_username'])
                            new_nickname = " | ".join(nickname_parts) if nickname_parts else record['roblox_username']
                        else:

                            # After fetching user roles
                            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                            new_nickname = format_nickname(
                                current_fenz_prefix,
                                record['callsign'],
                                current_hhstj_prefix,
                                record['roblox_username'],
                                is_fenz_high_command,
                                is_hhstj_high_command
                            )

                        # Only update if nickname is different
                        if member.nick != new_nickname:
                            try:
                                await member.edit(nick=new_nickname)
                                updated_count += 1
                            except discord.Forbidden:
                                failed_updates.append(
                                    f"{record.get('discord_username', 'Unknown')}: Missing permissions")

                except Exception as e:
                    failed_updates.append(f"{record.get('discord_username', 'Unknown')}: {str(e)}")

            # Prepare data for sheets - sort by rank hierarchy
            callsign_data = []
            for record in db_callsigns:
                record = dict(record)  # Convert to mutable dict

                # Determine if command based on FENZ prefix
                member = interaction.guild.get_member(record['discord_user_id'])
                is_command_rank = False
                if member:
                    rank_type, rank_data = sheets_manager.determine_rank_type(member.roles)
                    is_command_rank = (rank_type == 'command')

                callsign_data.append({
                    'fenz_prefix': record['fenz_prefix'] or '',
                    'hhstj_prefix': record['hhstj_prefix'] or '',
                    'callsign': record['callsign'],
                    'discord_user_id': record['discord_user_id'],
                    'discord_username': record['discord_username'],
                    'roblox_user_id': record['roblox_user_id'],
                    'roblox_username': record['roblox_username'],
                    'is_command': is_command_rank,
                    'strikes': sheets_manager.determine_strikes_value(member.roles) if member else None,
                    'qualifications': sheets_manager.determine_qualifications(member.roles,
                                                                              is_command_rank) if member else None
                })

            # Sort by rank hierarchy
            callsign_data.sort(key=lambda x: get_rank_sort_key(x['fenz_prefix'], x['hhstj_prefix']))

            # Update Google Sheets
            success = await sheets_manager.batch_update_callsigns(callsign_data)

            if not success:
                await interaction.followup.send("<:Denied:1426930694633816248> Failed to sync to Google Sheets.")
                return

            # Build response
            response = f"<:Accepted:1426930333789585509> **Bidirectional Sync Complete!**\n"
            response += f"Synced {len(db_callsigns)} callsigns to Google Sheets (sorted by rank hierarchy)\n"
            response += f"Updated {updated_count} Discord nicknames\n"

            if added_from_sheets > 0:
                response += f"Added {added_from_sheets} callsigns from sheets to database\n"

            if callsigns_reset:
                response += f"\nReset {len(callsigns_reset)} callsigns due to rank changes:\n"
                response += "\n".join(
                    f"- {r['member'].mention}: ~~{r['old_callsign']}~~ → {r['new_prefix']}-###" for r in
                    callsigns_reset[:5])
                if len(callsigns_reset) > 5:
                    response += f"\n... and {len(callsigns_reset) - 5} more"

            if missing_in_sheets:
                response += f"\nFound {len(missing_in_sheets)} entries in sheets with missing user data:\n"
                response += "\n".join(f"- {msg}" for msg in missing_in_sheets[:5])
                if len(missing_in_sheets) > 5:
                    response += f"\n... and {len(missing_in_sheets) - 5} more"

            if failed_updates:
                response += f"\nFailed to update {len(failed_updates)} nicknames:\n"
                response += "\n".join(f"- {fail}" for fail in failed_updates[:10])
                if len(failed_updates) > 10:
                    response += f"\n... and {len(failed_updates) - 10} more"

            await interaction.followup.send(response, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error during sync: {str(e)}")
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="assign", description="Assign a callsign to a user")
    @app_commands.check(lambda interaction: any(role.id in UPPER_LEAD for role in interaction.user.roles))
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        user="The user to assign the callsign to",
        callsign="The callsign number (1-3 digits)",
        prefix="Whether to use rank prefix (Supervisor+ only - defaults to True)"
    )
    async def assign_callsign(self, interaction: discord.Interaction, user: discord.Member, callsign: str,
                              prefix: bool = True):
        await interaction.response.defer(thinking=True)

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Check if user is high command
            is_high_command = any(role.id in HIGH_COMMAND_RANKS for role in user.roles)

            # Handle "blank" callsign
            if callsign.lower() == "blank":
                if not is_high_command:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Only High Command ranks can use 'blank' as a callsign.",
                        ephemeral=True
                    )
                    return
                callsign = "BLANK"
            else:
                # Validate callsign format (1-3 digits)
                if not callsign.isdigit() or len(callsign) > 3 or len(callsign) < 1:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Callsign must be a 1-3 digit number (e.g., 1, 42, 001) or 'blank' for High Command",
                        ephemeral=True
                    )
                    return

            # Get user's Roblox info FIRST
            bloxlink_data = await self.get_bloxlink_data(user.id, interaction.guild.id)
            if not bloxlink_data:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Could not find Roblox account for {user.mention}. "
                    "Please verify their Bloxlink connection.",
                    ephemeral=True
                )
                return

            roblox_id = bloxlink_data['id']
            roblox_username = await self.get_roblox_user_from_id(roblox_id)

            if not roblox_username:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Failed to fetch Roblox username.",
                    ephemeral=True
                )
                return

            # Get FENZ prefix from user's roles
            fenz_prefix = None
            fenz_rank_name = None
            for role_id, (rank_name, prefix_abbr) in FENZ_RANK_MAP.items():
                if any(role.id == role_id for role in user.roles):
                    fenz_prefix = prefix_abbr
                    fenz_rank_name = rank_name
                    break

            if not fenz_prefix:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} does not have a valid FENZ rank role.",
                    ephemeral=True
                )
                return

            # Get HHStJ rank from user's roles
            hhstj_prefix = get_hhstj_prefix_from_roles(user.roles)

            # NOW check if callsign already exists (AFTER we have fenz_prefix)
            if callsign not in ["BLANK", "###"]:
                existing = await check_callsign_exists(callsign, fenz_prefix)
                if existing and existing['discord_user_id'] != user.id:
                    error_message = format_duplicate_callsign_message(callsign, existing)
                    await interaction.followup.send(error_message, ephemeral=True)
                    return

            # Determine what to assign based on prefix parameter and rank
            if is_high_command and not prefix:
                # High command without number: Just rank prefix, no callsign number
                final_fenz_prefix = fenz_prefix
                final_callsign = "BLANK"
            else:
                # Normal assignment: Rank prefix + number
                final_fenz_prefix = fenz_prefix
                final_callsign = callsign

            # Add to database
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in user.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in user.roles)

            await add_callsign_to_database(
                final_callsign, user.id, str(user), roblox_id, roblox_username,
                final_fenz_prefix, hhstj_prefix or "",
                interaction.user.id,
                interaction.user.display_name,
                is_fenz_high_command,
                is_hhstj_high_command
            )

            # Format nickname
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in user.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in user.roles)

            if callsign == "BLANK":
                # Special formatting for BLANK callsigns
                nickname_parts = []
                if final_fenz_prefix:
                    nickname_parts.append(final_fenz_prefix)
                if hhstj_prefix and "-" not in hhstj_prefix:
                    nickname_parts.append(hhstj_prefix)
                if roblox_username:
                    nickname_parts.append(roblox_username)
                new_nickname = " | ".join(nickname_parts)
            else:

                new_nickname = format_nickname(
                    final_fenz_prefix,
                    final_callsign,
                    hhstj_prefix or "",
                    roblox_username,
                    is_fenz_high_command,
                    is_hhstj_high_command
                )

            # Update nickname
            try:
                await user.edit(nick=new_nickname)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"Callsign assigned but couldn't update nickname (lacking permissions). "
                    f"Please manually set to: `{new_nickname}`",
                    ephemeral=True
                )
                return

            # Format response message
            if callsign == "BLANK":
                callsign_display = "**BLANK** (no callsign number)"
            elif final_fenz_prefix:
                callsign_display = f"**{final_fenz_prefix}-{final_callsign}**"
            else:
                callsign_display = f"**{final_callsign}** (no prefix)"

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Assigned callsign {callsign_display} to {user.mention}\n"
                f"🏷️ Nickname updated to: `{new_nickname}`\n"
                f"💡 Remember to run `/callsign sync` to update Google Sheets!",
                ephemeral=True
            )

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error assigning callsign: {str(e)}")
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="lookup", description="Look up a callsign")
    @app_commands.check(lambda interaction: any(role.id in LEAD_ROLES for role in interaction.user.roles))
    async def lookup_callsign(self, interaction: discord.Interaction, callsign: str = None,
                              user: discord.Member = None):
        await interaction.response.defer(thinking=True)

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            results = []

            if callsign:
                # Handle BLANK callsign lookups
                if callsign.upper() == "BLANK":
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Cannot look up BLANK callsigns. Please specify a user instead.",
                        ephemeral=True
                    )
                    return
                # Search by callsign
                result = await check_callsign_exists(callsign)
                if result:
                    results = [result]
            elif user:
                # Search by Discord user
                results = await self.search_callsign_database(str(user.id), 'discord_id')
            else:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Please provide either a callsign or a user.")
                return

            if not results:
                await interaction.followup.send("<:Denied:1426930694633816248> No callsign found.")
                return

            # Display results
            for result in results:
                # Format title based on callsign type
                if result['callsign'] == "BLANK":
                    title = f"Callsign: BLANK (No number)"
                elif result['fenz_prefix']:
                    title = f"Callsign: {result['fenz_prefix']}-{result['callsign']}"
                else:
                    title = f"Callsign: {result['callsign']}"

                embed = discord.Embed(
                    title=title,
                    color=discord.Color.blue()
                )

                embed.add_field(
                    name="Discord User",
                    value=f"<@{result['discord_user_id']}> (`{result['discord_username']}`)",
                    inline=False
                )

                embed.add_field(
                    name="Roblox User",
                    value=f"{result['roblox_username']} (ID: {result['roblox_user_id']})",
                    inline=False
                )

                # Show FENZ rank
                if result['callsign'] == "BLANK":
                    embed.add_field(
                        name="FENZ Rank",
                        value=f"{result['fenz_prefix']} (High Command - No Callsign Number)",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="FENZ Rank",
                        value=result['fenz_prefix'] if result['fenz_prefix'] else "None (High Command)",
                        inline=True
                    )

                if result.get('hhstj_prefix'):
                    embed.add_field(
                        name="HHStJ Rank",
                        value=result['hhstj_prefix'],
                        inline=True
                    )

                if result.get('approved_at'):
                    embed.add_field(
                        name="Approved At",
                        value=f"<t:{int(result['approved_at'].timestamp())}:R>",
                        inline=False
                    )

                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error looking up callsign: {str(e)}")
            import traceback
            traceback.print_exc()

    @callsign_group.command(name='nickname-sync', description="Force update all nicknames from database (Owner only)")
    @app_commands.describe(dry_run="Preview changes without applying them")
    async def force_sync_nicknames(self, interaction: discord.Interaction, dry_run: bool = False):
        """Force sync all nicknames from database - Owner only"""

        # Lock to your user ID
        OWNER_ID = 678475709257089057

        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer(thinking=True)

        try:
            # Get all callsigns from database
            async with db.pool.acquire() as conn:
                callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

            if not callsigns:
                await interaction.followup.send("<:Denied:1426930694633816248> No callsigns found in database.")
                return

            # Track results
            updated = []
            skipped = []
            errors = []
            not_found = []

            # Process each callsign
            for record in callsigns:
                try:
                    member = interaction.guild.get_member(record['discord_user_id'])

                    if not member:
                        not_found.append(f"{record['discord_username']} (ID: {record['discord_user_id']})")
                        continue

                    # Check if member has high command roles
                    is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                    is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                    # Calculate what the nickname SHOULD be
                    expected_nickname = format_nickname(
                        record['fenz_prefix'],
                        record['callsign'],
                        record['hhstj_prefix'],
                        record['roblox_username'],
                        is_fenz_high_command,
                        is_hhstj_high_command
                    )

                    current_nick = member.nick or member.name

                    # Check if update is needed
                    if member.nick == expected_nickname:
                        skipped.append(f"{member.mention} - Already correct: `{expected_nickname}`")
                        continue

                    if dry_run:
                        # Just preview the change
                        updated.append(
                            f"{member.mention}\n"
                            f"  Current: `{current_nick}`\n"
                            f"  New: `{expected_nickname}`"
                        )
                    else:
                        # Apply the change
                        await member.edit(nick=expected_nickname)
                        updated.append(
                            f"{member.mention}\n"
                            f"  Old: `{current_nick}`\n"
                            f"  New: `{expected_nickname}`"
                        )

                except discord.Forbidden:
                    errors.append(f"{record['discord_username']} - Missing permissions")
                except Exception as e:
                    errors.append(f"{record['discord_username']} - {str(e)}")

            # Build response
            embed = discord.Embed(
                title="🔄 Nickname Sync Results" + (" (DRY RUN - Preview Only)" if dry_run else ""),
                color=discord.Color.blue() if dry_run else discord.Color.green()
            )

            if updated:
                # Split into multiple fields if too many
                chunk_size = 10
                for i in range(0, len(updated), chunk_size):
                    chunk = updated[i:i + chunk_size]
                    field_name = f"<:Accepted:1426930333789585509> Updated ({i + 1}-{min(i + chunk_size, len(updated))})" if len(
                        updated) > chunk_size else f"<:Accepted:1426930333789585509> Updated ({len(updated)})"
                    embed.add_field(
                        name=field_name,
                        value="\n\n".join(chunk[:10]),  # Limit to 10 to avoid field length issues
                        inline=False
                    )

            if skipped:
                embed.add_field(
                    name=f"Skipped ({len(skipped)})",
                    value=f"{len(skipped)} members already have correct nicknames",
                    inline=False
                )

            if not_found:
                embed.add_field(
                    name=f"Not Found ({len(not_found)})",
                    value="\n".join(not_found[:5]) + (
                        f"\n... and {len(not_found) - 5} more" if len(not_found) > 5 else ""),
                    inline=False
                )

            if errors:
                embed.add_field(
                    name=f"Errors ({len(errors)})",
                    value="\n".join(errors[:5]) + (f"\n... and {len(errors) - 5} more" if len(errors) > 5 else ""),
                    inline=False
                )

            # Summary
            summary = f"**Total Callsigns:** {len(callsigns)}\n"
            summary += f"**Updated:** {len(updated)}\n"
            summary += f"**Skipped:** {len(skipped)}\n"
            summary += f"**Not Found:** {len(not_found)}\n"
            summary += f"**Errors:** {len(errors)}"

            embed.description = summary

            if dry_run:
                embed.set_footer(text="This was a dry run. Run without dry_run=True to apply changes.")

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error during nickname sync: {str(e)}"
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="remove", description="Remove a callsign from a user")
    @app_commands.check(lambda interaction: any(role.id in UPPER_LEAD for role in interaction.user.roles))
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(user="The user whose callsign should be removed")
    async def remove_callsign(self, interaction: discord.Interaction, user: discord.Member):
        """Remove a callsign from a user and reset their nickname"""
        await interaction.response.defer(thinking=True, ephemeral=True)

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Check if user has a callsign
            async with db.pool.acquire() as conn:
                existing_callsign = await conn.fetchrow(
                    'SELECT * FROM callsigns WHERE discord_user_id = $1',
                    user.id
                )

            if not existing_callsign:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} does not have a callsign assigned.",
                    ephemeral=True
                )
                return

            # Store callsign info for confirmation message
            old_callsign = existing_callsign['callsign']
            old_fenz_prefix = existing_callsign['fenz_prefix']
            old_hhstj_prefix = existing_callsign['hhstj_prefix']

            # Delete from database
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'DELETE FROM callsigns WHERE discord_user_id = $1',
                    user.id
                )

            # Reset nickname to just Roblox username (or get from Bloxlink if needed)
            try:
                # Try to get Roblox username from stored data first
                roblox_username = existing_callsign.get('roblox_username')

                # If not available, fetch from Bloxlink
                if not roblox_username:
                    bloxlink_data = await self.get_bloxlink_data(user.id, interaction.guild.id)
                    if bloxlink_data:
                        roblox_id = bloxlink_data['id']
                        roblox_username = await self.get_roblox_user_from_id(roblox_id)

                # Reset nickname
                if roblox_username:
                    await user.edit(nick=roblox_username)
                else:
                    # Fallback: remove nickname entirely
                    await user.edit(nick=None)

                nickname_reset = True
            except discord.Forbidden:
                nickname_reset = False

            # Remove from Google Sheets
            try:
                await sheets_manager.remove_callsign_from_sheets(user.id)
            except Exception as e:
                print(f"Warning: Could not remove from sheets: {e}")

            # Build confirmation message
            embed = discord.Embed(
                title="Callsign Removed",
                description=f"Successfully removed callsign from {user.mention}",
                color=discord.Color.orange()
            )

            # Show removed callsign
            if old_callsign == "BLANK":
                callsign_display = "BLANK (no callsign number)"
            elif old_fenz_prefix:
                callsign_display = f"{old_fenz_prefix}-{old_callsign}"
            else:
                callsign_display = old_callsign

            embed.add_field(
                name="Removed Callsign",
                value=f"**{callsign_display}**",
                inline=True
            )

            if old_hhstj_prefix:
                embed.add_field(
                    name="HHStJ Rank",
                    value=old_hhstj_prefix,
                    inline=True
                )

            # Nickname status
            if nickname_reset:
                embed.add_field(
                    name="<:Accepted:1426930333789585509> Nickname Reset",
                    value=f"Changed to: `{roblox_username if roblox_username else 'Default username'}`",
                    inline=False
                )
            else:
                embed.add_field(
                    name="Nickname",
                    value="Could not reset nickname (missing permissions). Please manually update.",
                    inline=False
                )

            embed.add_field(
                name="Next Steps",
                value="• Callsign removed from database\n"
                      "• Run `/callsign sync` to update Google Sheets\n"
                      "• The callsign is now available for reassignment",
                inline=False
            )

            embed.set_footer(text=f"Removed by {interaction.user.display_name}")
            embed.timestamp = datetime.utcnow()

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error removing callsign: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="request", description="Request a callsign for yourself")
    @app_commands.describe(callsign="The numeric callsign you want (e.g., 1, 42, 123)")
    async def request_callsign(self, interaction: discord.Interaction, callsign: str):
        """Allow users to request a callsign for themselves"""
        await interaction.response.defer(thinking=True, ephemeral=True)

        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Validate callsign format (1-3 digits)
            if not callsign.isdigit() or len(callsign) > 3 or len(callsign) < 1:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Callsign must be a 1-3 digit number (e.g., 1, 42, 001)",
                    ephemeral=True
                )
                return

            # Get user's Roblox info
            bloxlink_data = await self.get_bloxlink_data(interaction.user.id, interaction.guild.id)
            if not bloxlink_data:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Could not find your Roblox account. "
                    "Please verify your Bloxlink connection with `/verify` and try again.",
                    ephemeral=True
                )
                return

            roblox_id = bloxlink_data['id']
            roblox_username = await self.get_roblox_user_from_id(roblox_id)

            if not roblox_username:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Failed to fetch Roblox username.",
                    ephemeral=True
                )
                return

            # Get FENZ rank from user's roles
            fenz_prefix = None
            fenz_rank_name = None
            is_high_command = False

            for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                if any(role.id == role_id for role in interaction.user.roles):
                    fenz_prefix = prefix
                    fenz_rank_name = rank_name
                    if role_id in HIGH_COMMAND_RANKS:
                        is_high_command = True
                    break

            # Get HHStJ rank from user's roles
            hhstj_prefix = get_hhstj_prefix_from_roles(interaction.user.roles)

            if not fenz_prefix:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> You do not have a valid FENZ rank role. "
                    "Please contact an admin.",
                    ephemeral=True
                )
                return

            # Check if user already has a callsign - allow replacement
            async with db.pool.acquire() as conn:
                user_callsign = await conn.fetchrow(
                    'SELECT * FROM callsigns WHERE discord_user_id = $1',
                    interaction.user.id
                )

            # Check if callsign already exists
            existing = await check_callsign_exists(callsign, fenz_prefix)
            if existing:
                # Use the new formatted message
                error_message = format_duplicate_callsign_message(callsign, existing)
                await interaction.followup.send(error_message, ephemeral=True)
                return

            # AUTO-ACCEPT: Callsign is available!
            # FOR HIGH COMMAND - Send choice message
            if is_high_command:
                embed = discord.Embed(
                    title="High Command Callsign Request",
                    description=f"Your callsign request for **{callsign}** is approved!\n\n"
                                f"As a **{fenz_rank_name}**, you can choose whether to use your rank prefix or not.",
                    color=discord.Color.gold()
                )
                embed.add_field(
                    name="Option 1: With Prefix",
                    value=f"Your callsign will be: **{fenz_prefix}-{callsign}**\n"
                          f"Example nickname: `{fenz_prefix}-{callsign} | {roblox_username}`",
                    inline=False
                )
                embed.add_field(
                    name="Option 2: Without Prefix",
                    value=f"Your callsign will be: **{callsign}**\n"
                          f"Example nickname: `{callsign} | {roblox_username}`",
                    inline=False
                )
                embed.add_field(
                    name="Time Limit",
                    value="You have **5 minutes** to make your choice.",
                    inline=False
                )
                embed.set_footer(text="Click one of the buttons below to make your choice")

                # Create the view for high command
                view = HighCommandPrefixChoice(
                    interaction.user.id, self, interaction, interaction.user, callsign,
                    fenz_prefix, hhstj_prefix, roblox_id, roblox_username
                )

                # Send ONLY as ephemeral followup (no channel message)
                await interaction.followup.send(embed=embed, view=view, ephemeral=True)
                return

            # FOR NON-HIGH COMMAND - Auto-accept with prefix
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in interaction.user.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in interaction.user.roles)

            await add_callsign_to_database(
                callsign, interaction.user.id, str(interaction.user),
                roblox_id, roblox_username, fenz_prefix, hhstj_prefix,
                interaction.user.id,
                interaction.user.display_name,
                is_fenz_high_command,
                is_hhstj_high_command
            )

            # Update nickname
            new_nickname = format_nickname(
                fenz_prefix, callsign, hhstj_prefix, roblox_username,
                is_fenz_high_command, is_hhstj_high_command
            )

            await member.edit(nick=new_nickname)
        except discord.HTTPException as e:
            if e.code == 50035:  # Invalid Form Body
                print(f"⚠️ Nickname too long for {member.id}: '{new_nickname}' ({len(new_nickname)} chars)")
                # Try again with just roblox username
                await member.edit(nick=roblox_username[:32])
            else:
                raise

            try:
                await interaction.user.edit(nick=new_nickname)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Callsign **{fenz_prefix}-{callsign}** approved!\n"
                    f"Could not update nickname automatically. Please ask an admin to set it to: `{new_nickname}`",
                    ephemeral=True
                )
                return

            # Update Google Sheets
            await sheets_manager.add_callsign_to_sheets(
                interaction.user, callsign, fenz_prefix, roblox_username, interaction.user.id
            )

            await self.send_callsign_request_log(
                self.bot, interaction.user, callsign, fenz_prefix,
                hhstj_prefix, roblox_username, approved=True
            )

            # Send success message
            success_embed = discord.Embed(
                title="<:Accepted:1426930333789585509> Callsign Approved!",
                description=f"Your callsign request has been automatically approved!",
                color=discord.Color.green()
            )

            # Show old callsign if they had one
            if user_callsign:
                success_embed.add_field(
                    name="Previous Callsign",
                    value=f"~~{user_callsign['fenz_prefix']}-{user_callsign['callsign']}~~",
                    inline=True
                )

            success_embed.add_field(
                name="Your New Callsign",
                value=f"**{fenz_prefix}-{callsign}**",
                inline=True
            )
            success_embed.add_field(
                name="Nickname",
                value=f"`{new_nickname}`",
                inline=True
            )
            success_embed.add_field(
                name="<:Accepted:1426930333789585509> What's Been Done",
                value="• Added to database\n• Updated your Discord nickname\n• Synced to Google Sheets" + (
                    "\n• Previous callsign archived" if user_callsign else ""
                ),
                inline=False
            )
            success_embed.set_footer(text=f"Approved automatically • {interaction.user.display_name}")
            success_embed.timestamp = datetime.utcnow()

            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error processing request: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="audit", description="[OWNER] Find users without callsigns or incomplete data")
    @app_commands.describe(
        show_incomplete="Show users with incomplete data (missing callsign number)",
        show_missing="Show users not in database at all"
    )
    async def audit_callsigns(
            self,
            interaction: discord.Interaction,
            show_incomplete: bool = True,
            show_missing: bool = True
    ):
        """Owner-only: Audit callsign database and find issues"""

        OWNER_ID = 678475709257089057

        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer(thinking=True)

        try:
            # Get all users with FENZ roles
            fenz_members = set()
            for role_id in FENZ_RANK_MAP.keys():
                role = interaction.guild.get_role(role_id)
                if role:
                    fenz_members.update(role.members)

            # Get all callsigns from database
            async with db.pool.acquire() as conn:
                db_callsigns = await conn.fetch('SELECT * FROM callsigns')

            db_user_ids = {record['discord_user_id'] for record in db_callsigns}

            # Track issues
            missing_from_db = []
            incomplete_data = []

            for member in fenz_members:
                # Check if in database
                if member.id not in db_user_ids:
                    # Get Roblox info
                    bloxlink_data = await self.get_bloxlink_data(member.id, interaction.guild.id)

                    if bloxlink_data:
                        roblox_id = bloxlink_data['id']
                        roblox_username = await self.get_roblox_user_from_id(roblox_id)
                    else:
                        roblox_id = None
                        roblox_username = None

                    # Get FENZ rank
                    fenz_prefix = None
                    fenz_rank_name = None
                    for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                        if any(role.id == role_id for role in member.roles):
                            fenz_prefix = prefix
                            fenz_rank_name = rank_name
                            break

                    missing_from_db.append({
                        'member': member,
                        'fenz_rank': fenz_rank_name,
                        'fenz_prefix': fenz_prefix,
                        'roblox_username': roblox_username,
                        'roblox_id': roblox_id,
                        'has_bloxlink': bool(bloxlink_data)
                    })
                else:
                    # Check for incomplete data
                    user_record = next((r for r in db_callsigns if r['discord_user_id'] == member.id), None)
                    if user_record:
                        issues = []
                        if user_record['callsign'] in ['###', 'BLANK', None]:
                            issues.append("No callsign number")
                        if not user_record['roblox_username']:
                            issues.append("Missing Roblox username")
                        if not user_record['roblox_user_id']:
                            issues.append("Missing Roblox ID")

                        if issues:
                            incomplete_data.append({
                                'member': member,
                                'record': dict(user_record),
                                'issues': issues
                            })

            # Build response embeds
            embeds = []

            if show_missing and missing_from_db:
                # Create pages for missing users (10 per page)
                chunk_size = 10
                for i in range(0, len(missing_from_db), chunk_size):
                    chunk = missing_from_db[i:i + chunk_size]

                    embed = discord.Embed(
                        title=f"🔍 Users Missing from Database ({i + 1}-{min(i + chunk_size, len(missing_from_db))} of {len(missing_from_db)})",
                        description="These users have FENZ roles but no callsign in the database",
                        color=discord.Color.red()
                    )

                    for user_data in chunk:
                        member = user_data['member']
                        status_icons = []

                        if user_data['has_bloxlink']:
                            status_icons.append("<:Accepted:1426930333789585509> Bloxlink")
                        else:
                            status_icons.append("<:Denied:1426930694633816248> No Bloxlink")

                        if user_data['roblox_username']:
                            status_icons.append(f"Roblox: {user_data['roblox_username']}")

                        embed.add_field(
                            name=f"{member.display_name} ({member.id})",
                            value=f"**Rank:** {user_data['fenz_rank'] or 'Unknown'}\n"
                                  f"**Status:** {' | '.join(status_icons)}\n"
                                  f"**Action:** Use `/callsign assign` to add them",
                            inline=False
                        )

                    embeds.append(embed)

            if show_incomplete and incomplete_data:
                # Create pages for incomplete data (10 per page)
                chunk_size = 10
                for i in range(0, len(incomplete_data), chunk_size):
                    chunk = incomplete_data[i:i + chunk_size]

                    embed = discord.Embed(
                        title=f"Incomplete Callsign Data ({i + 1}-{min(i + chunk_size, len(incomplete_data))} of {len(incomplete_data)})",
                        description="These users are in the database but have incomplete information",
                        color=discord.Color.orange()
                    )

                    for user_data in chunk:
                        member = user_data['member']
                        record = user_data['record']

                        current_callsign = "None"
                        if record['fenz_prefix'] and record['callsign']:
                            current_callsign = f"{record['fenz_prefix']}-{record['callsign']}"
                        elif record['callsign']:
                            current_callsign = record['callsign']

                        embed.add_field(
                            name=f"{member.display_name} ({member.id})",
                            value=f"**Current:** {current_callsign}\n"
                                  f"**Issues:** {', '.join(user_data['issues'])}\n"
                                  f"**Roblox:** {record.get('roblox_username') or 'Missing'}",
                            inline=False
                        )

                    embeds.append(embed)

            # Summary embed
            summary_embed = discord.Embed(
                title="Callsign Audit Summary",
                color=discord.Color.blue()
            )

            summary_embed.add_field(
                name="Total FENZ Members",
                value=str(len(fenz_members)),
                inline=True
            )
            summary_embed.add_field(
                name="In Database",
                value=str(len(db_user_ids)),
                inline=True
            )
            summary_embed.add_field(
                name="Missing from DB",
                value=str(len(missing_from_db)),
                inline=True
            )
            summary_embed.add_field(
                name="Incomplete Data",
                value=str(len(incomplete_data)),
                inline=True
            )

            if missing_from_db or incomplete_data:
                summary_embed.add_field(
                    name="Next Steps",
                    value="• Use `/callsign assign` to add missing users\n"
                          "• Use `/callsign assign` to fix incomplete data\n"
                          "• Run `/callsign sync` after making changes",
                    inline=False
                )
            else:
                summary_embed.add_field(
                    name="<:Accepted:1426930333789585509> Status",
                    value="All FENZ members have complete callsign data!",
                    inline=False
                )

            embeds.insert(0, summary_embed)

            # Send embeds
            if len(embeds) == 1:
                await interaction.followup.send(embed=embeds[0], ephemeral=True)
            else:
                # Send summary first
                await interaction.followup.send(embed=embeds[0], ephemeral=True)

                # Send other embeds
                for embed in embeds[1:]:
                    await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error during audit: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="bulk-assign", description="[OWNER] Assign callsigns to multiple users at once")
    async def bulk_assign(self, interaction: discord.Interaction):
        """Owner-only: Interactive bulk callsign assignment"""

        OWNER_ID = 678475709257089057

        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer(thinking=True)

        try:
            # Get all users with FENZ roles but no callsign
            fenz_members = set()
            for role_id in FENZ_RANK_MAP.keys():
                role = interaction.guild.get_role(role_id)
                if role:
                    fenz_members.update(role.members)

            # Get all callsigns from database
            async with db.pool.acquire() as conn:
                db_callsigns = await conn.fetch('SELECT discord_user_id FROM callsigns')

            db_user_ids = {record['discord_user_id'] for record in db_callsigns}

            # Find users without callsigns
            members_without_callsigns = [member for member in fenz_members if member.id not in db_user_ids]

            if not members_without_callsigns:
                await interaction.followup.send(
                    "<:Accepted:1426930333789585509> All FENZ members already have callsigns or are being processed!",
                    ephemeral=True
                )
                return

            # Send initial status message
            status_embed = discord.Embed(
                title="<a:Load:1430912797469970444> Fetching callsign data...",
                description=f"Checking Bloxlink data for {len(members_without_callsigns)} members...",
                color=discord.Color.blue()
            )
            await interaction.followup.send(embed=status_embed, ephemeral=True)

            # Process users in batches with progress updates
            users_without_callsigns = []
            processed = 0
            batch_size = 10

            for i in range(0, len(members_without_callsigns), batch_size):
                batch = members_without_callsigns[i:i + batch_size]

                # Process batch
                for member in batch:
                    # Get Bloxlink info
                    bloxlink_data = await self.get_bloxlink_data(member.id, interaction.guild.id)
                    if bloxlink_data:
                        roblox_id = bloxlink_data['id']
                        roblox_username = await self.get_roblox_user_from_id(roblox_id)

                        if roblox_username:
                            # Get FENZ rank
                            fenz_prefix = None
                            for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                                if any(role.id == role_id for role in member.roles):
                                    fenz_prefix = prefix
                                    break

                            if fenz_prefix:
                                users_without_callsigns.append({
                                    'member': member,
                                    'fenz_prefix': fenz_prefix,
                                    'roblox_id': roblox_id,
                                    'roblox_username': roblox_username
                                })

                    processed += 1

                # Update progress every batch
                if i + batch_size < len(members_without_callsigns):
                    progress_embed = discord.Embed(
                        title="<a:Load:1430912797469970444> Fetching callsign data...",
                        description=f"Processed {processed}/{len(members_without_callsigns)} members...\n"
                                    f"Found {len(users_without_callsigns)} eligible for assignment.",
                        color=discord.Color.blue()
                    )
                    try:
                        await interaction.edit_original_response(embed=progress_embed)
                    except:
                        pass

            if not users_without_callsigns:
                await interaction.edit_original_response(
                    content="No members found that are eligible for bulk assignment.\n"
                            "Members need:\n"
                            "• Valid FENZ rank role\n"
                            "• Linked Bloxlink account\n"
                            "• Valid Roblox username",
                    embed=None
                )
                return

            # Create summary embed
            summary_embed = discord.Embed(
                title="<:Accepted:1426930333789585509> Ready for Bulk Assignment",
                description=f"Found **{len(users_without_callsigns)}** members eligible for callsign assignment.",
                color=discord.Color.green()
            )

            # Show breakdown by rank
            rank_breakdown = {}
            for user_data in users_without_callsigns:
                prefix = user_data['fenz_prefix']
                rank_breakdown[prefix] = rank_breakdown.get(prefix, 0) + 1

            breakdown_text = "\n".join([f"**{prefix}**: {count}" for prefix, count in sorted(rank_breakdown.items())])
            summary_embed.add_field(
                name="By Rank",
                value=breakdown_text or "None",
                inline=False
            )

            summary_embed.set_footer(text="Click 'Start Assignment' to begin the interactive process")

            # Create view for bulk assignment
            view = BulkAssignView(self, interaction, users_without_callsigns)
            await interaction.edit_original_response(content=None, embed=summary_embed, view=None)
            await view.start()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

class HighCommandPrefixChoice(discord.ui.View):
    def __init__(self, interaction_user_id: int, cog, original_interaction, user, callsign,
                 fenz_prefix, hhstj_prefix, roblox_id, roblox_username):
        super().__init__(timeout=300)  # 5 minute timeout
        self.interaction_user_id = interaction_user_id
        self.cog = cog
        self.original_interaction = original_interaction
        self.user = user
        self.callsign = callsign
        self.fenz_prefix = fenz_prefix
        self.hhstj_prefix = hhstj_prefix
        self.roblox_id = roblox_id
        self.roblox_username = roblox_username
        self.choice_made = False
        self.message = None  # Will be set after sending

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only allow the high command member to respond
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Only the person being assigned the callsign can make this choice!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label='With Prefix (e.g., CO-001)', style=discord.ButtonStyle.primary, emoji='📋')
    async def with_prefix_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        # Save to database WITH prefix
        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in self.user.roles)
        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in self.user.roles)

        await add_callsign_to_database(
            self.callsign, self.user.id, str(self.user),
            self.roblox_id, self.roblox_username,
            self.fenz_prefix, self.hhstj_prefix,
            self.user.id,
            self.user.display_name,
            is_fenz_high_command,
            is_hhstj_high_command
        )

        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in self.user.roles)
        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in self.user.roles)

        new_nickname = format_nickname(
            self.fenz_prefix,
            self.callsign,
            self.hhstj_prefix,
            self.roblox_username,
            is_fenz_high_command,
            is_hhstj_high_command
        )

        try:
            await self.user.edit(nick=new_nickname)
        except discord.Forbidden:
            pass

        # Update Google Sheets
        await sheets_manager.add_callsign_to_sheets(
            self.user, self.callsign, self.fenz_prefix,
            self.roblox_username, self.user.id
        )

        await self.cog.send_callsign_request_log(
            self.cog.bot, self.user, self.callsign, self.fenz_prefix,
            self.hhstj_prefix, self.roblox_username, approved=True
        )

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        # Send confirmation to high command member
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> You've chosen to use the prefix!\n"
            f"Your callsign is: **{self.fenz_prefix}-{self.callsign}**\n"
            f"Nickname set to: `{new_nickname}`",
            ephemeral=True
        )

        # Send confirmation to admin who assigned it
        await self.original_interaction.followup.send(
            f"<:Accepted:1426930333789585509> {self.user.mention} chose to use prefix: **{self.fenz_prefix}-{self.callsign}**\n"
            f"Nickname updated to: `{new_nickname}`\n"
            f"Callsign synced to database and Google Sheets!",
            ephemeral=True
        )

        self.choice_made = True
        self.stop()

    @discord.ui.button(label='Without Prefix (e.g., 001)', style=discord.ButtonStyle.secondary, emoji='🔢')
    async def without_prefix_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        # Save to database WITHOUT prefix (empty string)
        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in self.user.roles)
        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in self.user.roles)

        await add_callsign_to_database(
            self.callsign, self.user.id, str(self.user),
            self.roblox_id, self.roblox_username,
            self.fenz_prefix, self.hhstj_prefix,
            self.user.id,
            self.user.display_name,
            is_fenz_high_command,
            is_hhstj_high_command
        )

        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in self.user.roles)
        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in self.user.roles)

        new_nickname = format_nickname(
            "",
            self.callsign,
            self.hhstj_prefix,
            self.roblox_username,
            is_fenz_high_command,
            is_hhstj_high_command
        )
        try:
            await self.user.edit(nick=new_nickname)
        except discord.Forbidden:
            pass

        # Update Google Sheets without prefix
        await sheets_manager.add_callsign_to_sheets(
            self.user, self.callsign, "",  # Empty prefix
            self.roblox_username, self.user.id
        )

        await self.cog.send_callsign_request_log(
            self.cog.bot, self.user, self.callsign, "",  # Empty prefix
            self.hhstj_prefix, self.roblox_username, approved=True
        )

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        # Send confirmation to high command member
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> You've chosen NOT to use a prefix!\n"
            f"Your callsign is: **{self.callsign}**\n"
            f"Nickname set to: `{new_nickname}`",
            ephemeral=True
        )

        # Send confirmation to admin who assigned it
        await self.original_interaction.followup.send(
            f"<:Accepted:1426930333789585509> {self.user.mention} chose NO prefix: **{self.callsign}**\n"
            f"Nickname updated to: `{new_nickname}`\n"
            f"Callsign synced to database and Google Sheets!",
            ephemeral=True
        )

        self.choice_made = True
        self.stop()

    @discord.ui.button(label='Cancel', style=discord.ButtonStyle.danger, emoji='<:Denied:1426930694633816248>')
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        await interaction.followup.send(
            "<:Denied:1426930694633816248> Callsign assignment cancelled.",
            ephemeral=True
        )

        await self.original_interaction.followup.send(
            f"<:Denied:1426930694633816248> {self.user.mention} cancelled the callsign assignment.",
            ephemeral=True
        )

        self.choice_made = True
        self.stop()

    async def on_timeout(self):
        # Disable all buttons on timeout
        for item in self.children:
            item.disabled = True

        if not self.choice_made and self.message:
            try:
                # Edit the original message
                await self.message.edit(
                    content=f"{self.user.mention} - Callsign assignment timed out (no response after 5 minutes).",
                    view=self
                )

                # Notify admin
                await self.original_interaction.followup.send(
                    f"Callsign assignment for {self.user.mention} timed out.",
                    ephemeral=True
                )
            except Exception as e:
                print(f"Error in timeout handler: {e}")


class BulkAssignView(discord.ui.View):
    """Interactive view for bulk callsign assignment"""

    def __init__(self, cog, interaction, users_data):
        super().__init__(timeout=600)  # 10 minute timeout
        self.cog = cog
        self.interaction = interaction
        self.users_data = users_data
        self.current_index = 0
        self.assigned_count = 0
        self.skipped_count = 0
        self.nil_count = 0  # Track NIL assignments

    async def start(self):
        """Start the bulk assignment process"""
        if not self.users_data:
            await self.interaction.followup.send("No users to assign!", ephemeral=True)
            return

        await self.show_current_user()

    async def show_current_user(self):
        """Show the current user for assignment"""
        if self.current_index >= len(self.users_data):
            # Finished!
            await self.finish()
            return

        user_data = self.users_data[self.current_index]
        member = user_data['member']

        embed = discord.Embed(
            title=f"Bulk Callsign Assignment ({self.current_index + 1}/{len(self.users_data)})",
            description=f"Assign a callsign to {member.mention}",
            color=discord.Color.blue()
        )

        embed.add_field(
            name="Discord User",
            value=f"{member.display_name} ({member.id})",
            inline=False
        )

        embed.add_field(
            name="Roblox User",
            value=user_data['roblox_username'],
            inline=True
        )

        embed.add_field(
            name="FENZ Rank",
            value=user_data['fenz_prefix'],
            inline=True
        )

        embed.add_field(
            name="Progress",
            value=f"Assigned: {self.assigned_count} | NIL: {self.nil_count} | Skipped: {self.skipped_count}",
            inline=False
        )

        embed.set_footer(
            text="Click 'Assign' to enter a callsign, 'NIL' to set ###, 'Skip' to skip, or 'Finish' to end")

        if self.current_index == 0:
            await self.interaction.followup.send(embed=embed, view=self, ephemeral=True)
        else:
            await self.interaction.edit_original_response(embed=embed, view=self)

    async def finish(self):
        """Finish the bulk assignment process"""
        embed = discord.Embed(
            title="<:Accepted:1426930333789585509> Bulk Assignment Complete!",
            color=discord.Color.green()
        )

        embed.add_field(
            name="Summary",
            value=f"**Assigned:** {self.assigned_count} callsigns\n"
                  f"**Set to NIL (###):** {self.nil_count} users\n"
                  f"**Skipped:** {self.skipped_count} users\n"
                  f"**Total Processed:** {self.current_index} / {len(self.users_data)}",
            inline=False
        )

        if self.assigned_count > 0 or self.nil_count > 0:
            embed.add_field(
                name="Next Steps",
                value="Run `/callsign sync` to update Google Sheets",
                inline=False
            )

        # Disable all buttons
        for item in self.children:
            item.disabled = True

        await self.interaction.edit_original_response(embed=embed, view=self)
        self.stop()

    @discord.ui.button(label="Assign Callsign", style=discord.ButtonStyle.success,
                       emoji="<:Accepted:1426930333789585509>")
    async def assign_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show modal to assign callsign"""
        user_data = self.users_data[self.current_index]
        modal = BulkAssignModal(self, user_data)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="###", style=discord.ButtonStyle.primary, emoji="🚫")
    async def nil_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set callsign to ### (NIL/default)"""
        await interaction.response.defer()

        try:
            user_data = self.users_data[self.current_index]
            member = user_data['member']

            # Get HHStJ prefix
            hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)

            # Add to database with ### as callsign
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

            await add_callsign_to_database(
                "###",
                member.id,
                str(member),
                user_data['roblox_id'],
                user_data['roblox_username'],
                user_data['fenz_prefix'],
                hhstj_prefix or '',
                interaction.user.id,
                interaction.user.display_name,
                is_fenz_high_command,
                is_hhstj_high_command
            )

            # Update nickname to PREFIX-###
            nickname_parts = []
            if user_data['fenz_prefix']:
                nickname_parts.append(f"{user_data['fenz_prefix']}-###")
            if hhstj_prefix and "-" not in hhstj_prefix:
                nickname_parts.append(hhstj_prefix)
            if user_data['roblox_username']:
                nickname_parts.append(user_data['roblox_username'])

            new_nickname = " | ".join(nickname_parts) if nickname_parts else user_data['roblox_username']

            try:
                await member.edit(nick=new_nickname)
            except discord.Forbidden:
                pass

            # Success!
            self.nil_count += 1
            self.current_index += 1

            await interaction.followup.send(
                f"Set {member.mention} to ###, ({user_data['fenz_prefix']}-###)",
                ephemeral=True
            )

            # Show next user
            await self.show_current_user()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @discord.ui.button(label="Skip", style=discord.ButtonStyle.secondary, emoji="<:RightSkip:1434962167660281926>")
    async def skip_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Skip this user"""
        await interaction.response.defer()
        self.skipped_count += 1
        self.current_index += 1
        await self.show_current_user()

    @discord.ui.button(label="Finish", style=discord.ButtonStyle.danger, emoji="🏁")
    async def finish_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """End bulk assignment"""
        await interaction.response.defer()
        await self.finish()

class BulkAssignModal(discord.ui.Modal):
    """Modal for entering callsign during bulk assignment"""

    def __init__(self, view: "BulkAssignView", user_data: dict):
        super().__init__(title="Bulk Assign Callsigns")
        self.view = view
        self.user_data = user_data

        self.callsign_input = discord.ui.TextInput(
            label="Callsign Number",
            placeholder="Enter 1-3 digit number (e.g., 1, 42, 123)",
            required=True,
            max_length=3
        )
        self.add_item(self.callsign_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            callsign = self.callsign_input.value.strip()
            member = self.user_data['member']

            # Validate callsign format
            if not callsign.isdigit() or len(callsign) > 3:
                await interaction.followup.send(
                   "<:Denied:1426930694633816248> Invalid callsign! Must be 1-3 digits.",
                    ephemeral=True
                )
                return

            # Get HHStJ prefix BEFORE checking if callsign exists
            hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)

            # NOW check if callsign exists (with the correct prefix)
            existing = await check_callsign_exists(callsign, self.user_data['fenz_prefix'])
            if existing:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Callsign {self.user_data['fenz_prefix']}-{callsign} is already taken by <@{existing['discord_user_id']}>!",
                    ephemeral=True
                )
                return

            # Assign callsign to database
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

            await add_callsign_to_database(
                callsign,
                member.id,
                str(member),
                self.user_data['roblox_id'],
                self.user_data['roblox_username'],
                self.user_data['fenz_prefix'],
                hhstj_prefix or '',
                interaction.user.id,
                interaction.user.display_name,
                is_fenz_high_command,
                is_hhstj_high_command
            )

            # Update nickname
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

            new_nickname = format_nickname(
                self.user_data['fenz_prefix'],
                callsign,
                hhstj_prefix or '',
                self.user_data['roblox_username'],
                is_fenz_high_command,
                is_hhstj_high_command
            )

            try:
                await member.edit(nick=new_nickname)
            except discord.Forbidden:
                pass

            # Success!
            self.view.assigned_count += 1
            self.view.current_index += 1

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Assigned {self.user_data['fenz_prefix']}-{callsign} to {member.mention}",
                ephemeral=True
            )

            # Show next user
            await self.view.show_current_user()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

async def setup(bot):
    await bot.add_cog(CallsignCog(bot))