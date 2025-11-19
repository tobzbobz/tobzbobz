import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv
import aiohttp
import os
from datetime import datetime, timedelta
from database import db
from dataclasses import dataclass
from typing import Optional, Dict, Set, Tuple
import json
from google_sheets_integration import sheets_manager, COMMAND_RANKS, NON_COMMAND_RANKS
import asyncio
import functools
from asyncpg.exceptions import PostgresError


@dataclass
class BotConfig:
    """Centralized configuration"""
    # Channels
    sync_log_channel_id: int = 1434770430505390221
    callsign_request_log_channel_id: int = 1435318020619632851

    # Excluded guilds
    excluded_guilds: Set[int] = None

    # Role IDs
    sync_role_id: int = 1389550689113473024
    owner_id: int = 678475709257089057

    # Cache settings
    bloxlink_cache_duration: int = 86400  # 24 hours
    sync_interval: int = 3600  # 1 hour

    def __post_init__(self):
        if self.excluded_guilds is None:
            self.excluded_guilds = {
                1420770769562243083,
                1430002479239532747,
                1425867713183744023
            }


# Create global config instance
config = BotConfig()

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

OWNER_ID = 678475709257089057

# Rank hierarchy numbers for priority calculation
FENZ_RANK_PRIORITY = {
    "RFF": 1,
    "QFF": 2,
    "SFF": 3,
    # Supervisor tier
    "SO": 8,
    "SSO": 9,
    "DCO": 10,
    "CO": 11,
    # Leadership tier
    "AAC": 12,
    "AC": 13,
    "ANC": 14,
    "DNC": 15,
    "NC": 16
}

HHSTJ_RANK_PRIORITY = {
    "FR": 1,
    "EMT": 2,
    "GPARA": 3,
    "PARA": 4,
    "ECP": 5,
    "CCP": 6,
    "DR": 7,
    # Supervisor tier
    "WOM-MIKE30": 8,
    "AOM-OSCAR32": 9,
    "DOSM-OSCAR31": 10,
    "DOM-OSCAR30": 11,
    # Leadership tier
    "ANOM-OSCAR3": 14,
    "DNOM-OSCAR2": 15,
    "NOM-OSCAR1": 16
}

# Tier boundaries
SUPERVISOR_THRESHOLD = 8
LEADERSHIP_THRESHOLD = 12

class ProgressTracker:
    """Helper for tracking and displaying progress with rate limiting"""

    def __init__(self, interaction: discord.Interaction, total: int, update_interval: float = 3.0):
        self.interaction = interaction
        self.total = total
        self.update_interval = update_interval
        self.last_update = 0
        self.current = 0

    async def update(self, current: int, status_counts: dict = None, force: bool = False):
        """Update progress, respecting rate limits"""
        self.current = current
        current_time = asyncio.get_event_loop().time()

        # Only update if enough time passed or forced (e.g., completion)
        if not force and (current_time - self.last_update) < self.update_interval:
            return

        progress_percent = int((current / self.total) * 100)
        progress_bar = "█" * (progress_percent // 5) + "░" * (20 - (progress_percent // 5))

        embed = discord.Embed(
            title="🔄 Processing...",
            description=f"**Progress:** {current}/{self.total} ({progress_percent}%)\n"
                        f"`{progress_bar}`",
            color=discord.Color.blue()
        )

        if status_counts:
            status_text = "\n".join([
                f"✅ **{key.replace('_', ' ').title()}:** {value}"
                for key, value in status_counts.items()
                if value > 0
            ])
            embed.add_field(name="Status", value=status_text, inline=False)

        embed.set_footer(text=f"Processing item {current} of {self.total}")

        try:
            await self.interaction.edit_original_response(embed=embed)
            self.last_update = current_time
        except discord.HTTPException:
            pass  # Ignore rate limit errors on progress updates


def db_retry(max_attempts: int = 3, delay: float = 1.0):
    """Decorator for retrying database operations"""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except PostgresError as e:
                    last_exception = e
                    if attempt < max_attempts:
                        print(f"⚠️ DB operation failed (attempt {attempt}/{max_attempts}): {e}")
                        await asyncio.sleep(delay * attempt)
                    else:
                        print(f"❌ DB operation failed after {max_attempts} attempts")
                        raise

            raise last_exception

        return wrapper

    return decorator


def normalize_callsign(callsign: str) -> str:
    """
    Normalize callsign by stripping leading zeros
    Examples: '01' -> '1', '001' -> '1', '10' -> '10', '100' -> '100'
    Returns normalized callsign or original if not numeric
    """
    if not callsign or not callsign.isdigit():
        return callsign

    # Convert to int and back to string to strip leading zeros
    # This preserves '10' and '100' while converting '01' to '1'
    return str(int(callsign))

def get_embed_size(embed: discord.Embed) -> int:
    """Calculate total character count of an embed"""
    size = 0
    size += len(embed.title or "")
    size += len(embed.description or "")
    size += len(embed.footer.text or "") if embed.footer else 0
    size += len(embed.author.name or "") if embed.author else 0

    for field in embed.fields:
        size += len(field.name or "")
        size += len(field.value or "")

    return size


def validate_embed_size(embed: discord.Embed, max_size: int = 5500) -> tuple[bool, int]:
    """Validate embed size and return (is_valid, size)"""
    size = get_embed_size(embed)
    return (size <= max_size, size)


async def safe_edit_nickname(member: discord.Member, nickname: str, max_retries: int = 3) -> tuple[bool, str]:
    """
    Safely edit nickname with automatic fixing and truncation
    Returns: (success: bool, final_nickname: str)
    """

    # Validation and cleaning
    nickname = nickname.strip()

    # Remove trailing separators
    while nickname and nickname[-1] in ['-', '|', ' ']:
        nickname = nickname[:-1].strip()

    # Remove leading separators
    while nickname and nickname[0] in ['-', '|', ' ']:
        nickname = nickname[1:].strip()

    if not validate_nickname(nickname):
        print(f"⚠️ Invalid nickname format: '{nickname}'")
        return (False, nickname)

    # Try progressively shorter versions
    attempts = [
        nickname,  # Full version
        nickname[:32],  # Truncated to Discord limit
    ]

    # Try removing shift prefixes if still too long
    shift_prefixes = ["DUTY | ", "BRK | ", "LOA | "]
    for prefix in shift_prefixes:
        if nickname.startswith(prefix):
            stripped = nickname[len(prefix):]
            if len(stripped) <= 32:
                attempts.insert(1, stripped)

    last_error = None

    for attempt_num, attempt_nick in enumerate(attempts, 1):
        if not attempt_nick or len(attempt_nick) > 32:
            continue

        try:
            await member.edit(nick=attempt_nick)
            if attempt_num > 1:
                print(f"✅ Used fallback nickname: '{attempt_nick}'")
            return (True, attempt_nick)

        except discord.HTTPException as e:
            last_error = e
            if e.code != 50035:  # Not a length error
                print(f"❌ Discord HTTP error {e.code}: {e}")
                break

        except discord.Forbidden:
            print(f"❌ Missing permissions to edit {member.id}")
            return (False, nickname)

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            last_error = e

    print(f"❌ All nickname attempts failed for {member.id}")
    return (False, nickname)

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

    # Check for invalid ending patterns
    if nickname.endswith('-') or nickname.endswith('|'):
        return False

    # Check for patterns like "- " or " -" in the middle
    if '- ' in nickname or ' -' in nickname:
        return False

    # Check for double spaces
    if '  ' in nickname:
        return False

    return True


def calculate_rank_priority(fenz_prefix: str, hhstj_prefix: str) -> tuple[str, int, str]:
    """
    Calculate which rank should display first based on priority rules.

    Returns: (first_prefix, first_priority, second_prefix)

    Priority Rules:
    1. Leadership (12+) always trumps everything
    2. Supervisor (8-11) always trumps regular (1-7)
    3. Within same tier, higher number wins
    4. Ties default to FENZ
    """

    fenz_priority = FENZ_RANK_PRIORITY.get(fenz_prefix, 0)
    hhstj_priority = HHSTJ_RANK_PRIORITY.get(hhstj_prefix, 0)

    # Determine tiers
    fenz_is_leadership = fenz_priority >= LEADERSHIP_THRESHOLD
    fenz_is_supervisor = SUPERVISOR_THRESHOLD <= fenz_priority < LEADERSHIP_THRESHOLD
    fenz_is_regular = 0 < fenz_priority < SUPERVISOR_THRESHOLD

    hhstj_is_leadership = hhstj_priority >= LEADERSHIP_THRESHOLD
    hhstj_is_supervisor = SUPERVISOR_THRESHOLD <= hhstj_priority < LEADERSHIP_THRESHOLD
    hhstj_is_regular = 0 < hhstj_priority < SUPERVISOR_THRESHOLD

    # Rule 1: Leadership always wins
    if fenz_is_leadership and not hhstj_is_leadership:
        return (fenz_prefix, fenz_priority, hhstj_prefix)
    elif hhstj_is_leadership and not fenz_is_leadership:
        return (hhstj_prefix, hhstj_priority, fenz_prefix)
    elif fenz_is_leadership and hhstj_is_leadership:
        # Both leadership - higher number wins, FENZ on tie
        if fenz_priority >= hhstj_priority:
            return (fenz_prefix, fenz_priority, hhstj_prefix)
        else:
            return (hhstj_prefix, hhstj_priority, fenz_prefix)

    # Rule 2: Supervisor trumps regular
    if fenz_is_supervisor and hhstj_is_regular:
        return (fenz_prefix, fenz_priority, hhstj_prefix)
    elif hhstj_is_supervisor and fenz_is_regular:
        return (hhstj_prefix, hhstj_priority, fenz_prefix)
    elif fenz_is_supervisor and hhstj_is_supervisor:
        # Both supervisor - higher number wins, FENZ on tie
        if fenz_priority >= hhstj_priority:
            return (fenz_prefix, fenz_priority, hhstj_prefix)
        else:
            return (hhstj_prefix, hhstj_priority, fenz_prefix)

    # Rule 3: Both regular - higher number wins, FENZ on tie
    if fenz_is_regular and hhstj_is_regular:
        if fenz_priority >= hhstj_priority:
            return (fenz_prefix, fenz_priority, hhstj_prefix)
        else:
            return (hhstj_prefix, hhstj_priority, fenz_prefix)

    # Fallback: FENZ takes precedence if no ranks or only one has a rank
    if fenz_prefix and not hhstj_prefix:
        return (fenz_prefix, fenz_priority, "")
    elif hhstj_prefix and not fenz_prefix:
        return (hhstj_prefix, hhstj_priority, "")
    elif fenz_prefix:
        return (fenz_prefix, fenz_priority, hhstj_prefix)
    else:
        return ("", 0, "")


def format_nickname(fenz_prefix: str, callsign: str, hhstj_prefix: str, roblox_username: str) -> str:
    """
    Format nickname with new priority-based system.
    Automatically determines which prefix should display first.
    """

    # Calculate priority
    first_prefix, first_priority, second_prefix = calculate_rank_priority(fenz_prefix, hhstj_prefix)

    # Determine which is FENZ and which is HHStJ
    if first_prefix == fenz_prefix:
        first_is_fenz = True
    else:
        first_is_fenz = False

    # Build nickname parts in priority order
    nickname_parts = []

    # ✅ SPECIAL CASE: Not Assigned - Just show prefix(es), no callsign number
    if callsign == "Not Assigned":
        # Special sub-case: RFF should just be "RFF" alone
        if fenz_prefix == "RFF":
            nickname_parts = ["RFF"]
            # Add HHStJ if exists and is not a command callsign
            if hhstj_prefix and "-" not in hhstj_prefix:
                nickname_parts.append(hhstj_prefix)
        else:
            # For all other ranks: show prefix(es) in priority order, NO "-Not Assigned"
            if first_is_fenz:
                # FENZ has priority
                if first_prefix:
                    nickname_parts.append(first_prefix)
                if second_prefix and "-" not in second_prefix:
                    nickname_parts.append(second_prefix)
            else:
                # HHStJ has priority
                if first_prefix:
                    nickname_parts.append(first_prefix)
                if second_prefix:
                    nickname_parts.append(second_prefix)

        # Always add Roblox username at the end
        if roblox_username:
            nickname_parts.append(roblox_username)

    # ✅ SPECIAL CASE: BLANK - Just show prefix(es), no callsign
    elif callsign == "BLANK":
        if first_prefix:
            nickname_parts.append(first_prefix)
        if second_prefix and "-" not in second_prefix:
            nickname_parts.append(second_prefix)
        if roblox_username:
            nickname_parts.append(roblox_username)

    # ✅ NORMAL CASE: Has a real callsign number
    else:
        if first_is_fenz:
            # FENZ first: {FENZ-callsign} | {HHStJ} | {roblox}
            nickname_parts.append(f"{first_prefix}-{callsign}")
            if second_prefix and "-" not in second_prefix:
                nickname_parts.append(second_prefix)
        else:
            # HHStJ first: {HHStJ} | {FENZ-callsign} | {roblox}
            nickname_parts.append(first_prefix)
            if second_prefix:
                nickname_parts.append(f"{second_prefix}-{callsign}")

        if roblox_username:
            nickname_parts.append(roblox_username)

    # Attempt 1: Full nickname
    result = try_format(nickname_parts)
    if result:
        return result

    # Attempt 2: Smart truncation - remove LOWER priority prefix
    if len(nickname_parts) >= 3:
        # Remove the second prefix (lower priority)
        truncated_parts = [nickname_parts[0], nickname_parts[-1]]  # Keep first prefix and roblox
        result = try_format(truncated_parts)
        if result:
            return result

    # Attempt 3: Just highest priority prefix
    if first_prefix:
        if callsign not in ["Not Assigned", "BLANK"] and first_is_fenz:
            result = try_format([f"{first_prefix}-{callsign}"])
            if result:
                return result
        else:
            result = try_format([first_prefix])
            if result:
                return result

    # Attempt 4: Just roblox username
    if roblox_username:
        result = try_format([roblox_username[:32]])
        if result:
            return result

    # Ultimate fallback
    return "User"

def try_format(parts: list) -> str:
    """Try to format parts, return None if invalid or too long"""
    if not parts:
        return None
    result = " | ".join(filter(None, parts))
    if validate_nickname(result) and len(result) <= 32:
        return result
    return None


async def smart_update_nickname(member, expected_nickname: str, current_fenz_prefix: str,
                                current_callsign: str, current_hhstj_prefix: str,
                                roblox_username: str) -> bool:
    """
    Smart nickname update that preserves shift prefixes and handles special cases
    Returns True if update was attempted, False if no update needed
    """

    # Get current nickname (strip shift prefixes for comparison)
    current_nick_stripped = strip_shift_prefixes(member.nick) if member.nick else member.name
    expected_nick_stripped = strip_shift_prefixes(expected_nickname)

    # Check if update is needed
    if current_nick_stripped == expected_nick_stripped:
        return False  # No update needed

    # ✅ SPECIAL: Not Assigned cases
    if current_callsign == "Not Assigned":
        if current_fenz_prefix == "RFF":
            # RFF-Not Assigned should be just "RFF"
            expected_nickname = "RFF"
            if current_hhstj_prefix and "-" not in current_hhstj_prefix:
                expected_nickname = f"RFF | {current_hhstj_prefix}"
            if roblox_username:
                expected_nickname = f"{expected_nickname} | {roblox_username}"
        else:
            # All other ranks: just prefix(es), NO "-Not Assigned"
            expected_nickname = format_nickname(
                current_fenz_prefix, "Not Assigned", current_hhstj_prefix, roblox_username
            )

    # Preserve shift prefix if it exists
    shift_prefix = ""
    if member.nick:
        for prefix in ["DUTY | ", "BRK | ", "LOA | "]:
            if member.nick.startswith(prefix):
                shift_prefix = prefix
                break

    final_nickname = shift_prefix + expected_nickname

    try:
        success, final_nick = await safe_edit_nickname(member, final_nickname)

        if success:
            print(f"✅ Updated nickname: {member.display_name} → '{final_nickname}'")
            return True
        else:
            print(f"⚠️ Failed to update nickname for {member.id}")
            return False

    except Exception as e:
        print(f"❌ Error updating nickname for {member.display_name}: {e}")
        return False

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

@db_retry(max_attempts=3, delay=1.0)
async def check_callsign_exists(callsign: str, fenz_prefix: str = None) -> dict:
    """Check if a callsign exists in the database with the same prefix"""
    async with db.pool.acquire() as conn:
        # BLANK and Not Assigned callsigns are allowed to be non-unique, skip check
        if callsign in ["BLANK", "Not Assigned"]:
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


def get_hhstj_prefix_from_roles(roles, stored_hhstj_prefix: str = None) -> str:
    """
    Get HHStJ prefix from roles, prioritizing management over clinical.
    If stored_hhstj_prefix is provided and is a valid shortened version, preserve it.
    """

    # First check for management roles (high command)
    full_prefix = None
    for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
        if role_id in HHSTJ_HIGH_COMMAND_RANKS:
            if any(role.id == role_id for role in roles):
                full_prefix = prefix
                break

    # Then check for clinical roles if no management role found
    if not full_prefix:
        for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
            if role_id not in HHSTJ_HIGH_COMMAND_RANKS:
                if any(role.id == role_id for role in roles):
                    full_prefix = prefix
                    break

    # If no HHStJ role found, return empty
    if not full_prefix:
        return ""

    # ✅ NEW: If user has a stored shorthand preference, validate and preserve it
    if stored_hhstj_prefix and '-' in full_prefix:
        valid_versions = get_hhstj_shortened_versions(full_prefix)

        # If stored prefix is a valid shortened version of current role, keep it
        if stored_hhstj_prefix in valid_versions:
            return stored_hhstj_prefix

    # Otherwise return the full prefix from roles
    return full_prefix


async def send_safe_embeds(self, channel, items: list, title_prefix: str, color, formatter_func,
                           max_per_embed: int = 3):
    """Safely send embeds with automatic size validation"""
    for i in range(0, len(items), max_per_embed):
        chunk = items[i:i + max_per_embed]

        embed = discord.Embed(
            title=f"{title_prefix} ({i + 1}-{min(i + max_per_embed, len(items))} of {len(items)})",
            color=color,
            timestamp=datetime.utcnow()
        )

        for item in chunk:
            field_data = formatter_func(item)
            embed.add_field(**field_data)

        # Validate size
        if get_embed_size(embed) > 5500:
            # Try with smaller chunks
            if max_per_embed > 1:
                await self.send_safe_embeds(channel, chunk, title_prefix, color, formatter_func, max_per_embed // 2)
            else:
                print(f"⚠️ Single item too large, skipping")
            continue

        await channel.send(embed=embed)

def get_hhstj_shortened_versions(hhstj_prefix: str) -> list:
    """
    Get all shortened versions of an HHStJ callsign
    Returns list of versions in order: [full, shortened, prefix-only]
    """
    if not hhstj_prefix or '-' not in hhstj_prefix:
        return [hhstj_prefix] if hhstj_prefix else []

    # Split the callsign (e.g., "WOM-MIKE30" -> "WOM", "MIKE30")
    parts = hhstj_prefix.split('-', 1)
    if len(parts) != 2:
        return [hhstj_prefix]

    prefix = parts[0]  # e.g., "WOM"
    phonetic_number = parts[1]  # e.g., "MIKE30"

    # Extract shortened phonetic: First letter + Last letter before number + numbers
    # Examples: MIKE30 -> MKE30, OSCAR32 -> OSC32, OSCAR3 -> OSC3
    if len(phonetic_number) >= 3:
        # Find where numbers start
        number_start = 0
        for i, char in enumerate(phonetic_number):
            if char.isdigit():
                number_start = i
                break

        if number_start >= 2:  # Need at least 2 letters before numbers
            # Take first letter + last letter before numbers + all numbers
            # MIKE30: M + K (phonetic_number[number_start-1]) + 30
            # OSCAR32: O + C (phonetic_number[number_start-1]) + 32
            first_letter = phonetic_number[0]
            last_letter_before_num = phonetic_number[number_start - 1]
            numbers = phonetic_number[number_start:]
            shortened_phonetic = f"{first_letter}{last_letter_before_num}{numbers}"
            version2 = f"{prefix}-{shortened_phonetic}"
        else:
            version2 = hhstj_prefix  # Can't shorten properly
    else:
        version2 = hhstj_prefix  # Can't shorten

    version1 = hhstj_prefix  # Full version
    version3 = prefix  # Prefix only

    return [version1, version2, version3]


def test_hhstj_versions_fit(fenz_prefix: str, callsign: str, hhstj_versions: list,
                            roblox_username: str, is_fenz_hc: bool, is_hhstj_hc: bool) -> list:
    """
    Test which HHStJ versions will fit in a nickname
    Returns list of dicts with {'version': str, 'nickname': str, 'fits': bool}
    """
    results = []

    for version in hhstj_versions:
        # Build the full nickname with this version
        test_nickname = format_nickname(
            fenz_prefix, callsign, version, roblox_username,
        )

        fits = len(test_nickname) <= 32 and validate_nickname(test_nickname)

        results.append({
            'version': version,
            'nickname': test_nickname,
            'fits': fits
        })

    return results

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

@db_retry(max_attempts=3, delay=1.0)
async def add_callsign_to_database(callsign: str, discord_user_id: int, discord_username: str,
                                   roblox_user_id: str, roblox_username: str, fenz_prefix: str,
                                   hhstj_prefix: str, approved_by_id: int, approved_by_name: str,
                                   is_fenz_high_command: bool = False, is_hhstj_high_command: bool = False):
    """Add a new callsign to the database"""
    async with db.pool.acquire() as conn:
        async with conn.transaction():  # <:Accepted:1426930333789585509> Proper transaction
            # <:Accepted:1426930333789585509> CHECK FOR CONFLICTS FIRST (before any DELETE)
            if callsign not in ["BLANK", "Not Assigned"]:
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

class BloxlinkAPI:
    """Enhanced Bloxlink API handler with PostgreSQL-backed 24-hour caching"""

    # ✅ Keep class-level tracking variables
    _api_calls_made = 0
    _quota_limit = 500
    _quota_reset_time = None
    _cache_duration = 86400  # 24 hours in seconds
    _quota_exhausted = False

    def __init__(self):
        self.base_url = "https://api.blox.link/v4/public"
        self.rate_limit_delay = 0.75
        self.last_request_time = 0
        self.max_retries = 3
        self.timeout = 15
        self.requests_this_minute = 0
        self.minute_start = asyncio.get_event_loop().time()
        self.max_requests_per_minute = 50

    async def _check_daily_quota(self) -> bool:
        """
        Check if we have API quota remaining. Returns True if we can make calls.
        Automatically resets quota tracking after 24 hours.
        """
        # Check if quota reset time has passed
        if BloxlinkAPI._quota_reset_time:
            current_time = asyncio.get_event_loop().time()
            if current_time >= BloxlinkAPI._quota_reset_time:
                print("✅ 24-hour quota period expired - resetting counters")
                BloxlinkAPI._api_calls_made = 0
                BloxlinkAPI._quota_exhausted = False
                BloxlinkAPI._quota_reset_time = None

        # Check if we're at or over the limit
        if BloxlinkAPI._api_calls_made >= BloxlinkAPI._quota_limit:
            if not BloxlinkAPI._quota_exhausted:
                BloxlinkAPI._quota_exhausted = True

                # Set reset time to 24 hours from now if not already set
                if not BloxlinkAPI._quota_reset_time:
                    BloxlinkAPI._quota_reset_time = asyncio.get_event_loop().time() + 86400
                    reset_datetime = datetime.utcnow() + timedelta(seconds=86400)
                    print(
                        f"🚫 Daily API quota exhausted ({BloxlinkAPI._api_calls_made}/{BloxlinkAPI._quota_limit} calls used)")
                    print(f"⏰ Quota will reset at: {reset_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC")

            return False

        return True

    async def _get_cached_data(self, discord_user_id: int) -> Optional[Tuple[str, int, str]]:
        """
        Get cached Bloxlink data from database if not expired
        Returns (username, user_id, status) or None if not cached/expired
        """
        async with db.pool.acquire() as conn:
            result = await conn.fetchrow(
                '''SELECT roblox_username, roblox_user_id, status, expires_at
                   FROM bloxlink_cache
                   WHERE discord_user_id = $1
                     AND expires_at > NOW()''',
                discord_user_id
            )

            if result:
                # Only log on failure or first few hits for debugging
                return (
                    result['roblox_username'],
                    int(result['roblox_user_id']) if result['roblox_user_id'] and result[
                        'roblox_user_id'] != 'None' else None,
                    result['status']
                )

            return None

    async def _cache_data(self, discord_user_id: int, roblox_username: Optional[str],
                          roblox_user_id: Optional[int], status: str):
        """
        Store Bloxlink data in database cache with 24-hour expiry
        Only overwrites existing cache if new data is successful
        """
        # Check if this is an error status
        error_statuses = ['rate_limited', 'timeout', 'quota_exhausted', 'api_error',
                          'max_retries_exceeded', 'no_guild_id', 'no_api_key']
        is_error = status in error_statuses or status.startswith('api_error_') or status.startswith('error_')

        async with db.pool.acquire() as conn:
            if is_error:
                # Check if we have existing valid cache
                existing = await conn.fetchrow(
                    '''SELECT roblox_username, roblox_user_id, status, expires_at
                       FROM bloxlink_cache
                       WHERE discord_user_id = $1''',
                    discord_user_id
                )

                if existing and existing['status'] in ['success', 'not_linked']:
                    # We have valid cached data, don't overwrite with error
                    print(f"⚠️ Preserving existing cache for {discord_user_id} - API returned {status}")
                    return

            # Either no existing cache, or new data is successful - update normally
            await conn.execute(
                '''INSERT INTO bloxlink_cache
                   (discord_user_id, roblox_username, roblox_user_id, status, cached_at, expires_at)
                   VALUES ($1, $2, $3, $4, NOW(), NOW() + INTERVAL '24 hours') ON CONFLICT (discord_user_id) 
                   DO
                UPDATE SET
                    roblox_username = EXCLUDED.roblox_username,
                    roblox_user_id = EXCLUDED.roblox_user_id,
                    status = EXCLUDED.status,
                    cached_at = NOW(),
                    expires_at = NOW() + INTERVAL '24 hours' ''',
                discord_user_id,
                roblox_username,
                str(roblox_user_id) if roblox_user_id else None,
                status
            )

        # Only log cache failures
        if is_error:
            print(f"❌ Failed to cache data for {discord_user_id}: {status}")

    async def get_bloxlink_data(
            self,
            discord_user_id: int,
            guild_id: int
    ) -> Tuple[Optional[str], Optional[int], str]:
        """
        Get Bloxlink data with database-backed 24-hour caching

        Returns:
            Tuple of (roblox_username, roblox_user_id, status_message)
        """

        if not guild_id:
            return (None, None, "no_guild_id")

        if not BLOXLINK_API_KEY:
            print("❌ CRITICAL: BLOXLINK_API_KEY is not set!")
            return (None, None, "no_api_key")

        # ✅ CHECK DATABASE CACHE FIRST
        cached = await self._get_cached_data(discord_user_id)
        if cached:
            return cached

        # Cache miss - fetch from API
        print(f"🌐 Cache MISS for {discord_user_id}, fetching from API...")

        # 🚫 NEW: Check if we have quota remaining before making API call
        if not await self._check_daily_quota():
            print(f"🚫 Quota exhausted - cannot fetch data for {discord_user_id}")
            return (None, None, "quota_exhausted")

        for attempt in range(1, self.max_retries + 1):
            try:
                await self._enforce_rate_limit()

                urls_to_try = [
                    f"{self.base_url}/guilds/{guild_id}/discord-to-roblox/{discord_user_id}",
                    f"{self.base_url}/discord-to-roblox/{discord_user_id}"
                ]

                timeout = aiohttp.ClientTimeout(total=self.timeout)
                headers = {
                    'Authorization': BLOXLINK_API_KEY,
                    'User-Agent': 'HNZRP-Callsign-Bot/1.0'
                }

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    for url_index, url in enumerate(urls_to_try):
                        async with session.get(url, headers=headers) as response:

                            if 'X-RateLimit-Remaining' in response.headers:
                                remaining = int(response.headers['X-RateLimit-Remaining'])
                                print(f"📊 API Quota: {remaining} calls remaining")

                            if 'X-RateLimit-Reset' in response.headers:
                                BloxlinkAPI._quota_reset_time = int(response.headers['X-RateLimit-Reset'])

                            if response.status == 200:
                                data = await response.json()
                                roblox_id = data.get('robloxID')

                                if roblox_id:
                                    username = await self._get_roblox_username(roblox_id)
                                    result = (username, int(roblox_id), "success")

                                    # ✅ CACHE THE RESULT in database
                                    await self._cache_data(discord_user_id, username, int(roblox_id), "success")

                                    return result
                                else:
                                    result = (None, None, "not_linked")
                                    await self._cache_data(discord_user_id, None, None, "not_linked")
                                    return result

                            elif response.status == 429:
                                if attempt < self.max_retries:
                                    wait_time = (2 ** attempt) * 2
                                    print(
                                        f"⏳ Rate limited, waiting {wait_time}s (attempt {attempt}/{self.max_retries})")
                                    await asyncio.sleep(wait_time)
                                    continue
                                return (None, None, "rate_limited")

                            elif response.status == 404:
                                result = (None, None, "not_linked")
                                await self._cache_data(discord_user_id, None, None, "not_linked")
                                return result

                            elif 400 <= response.status < 500:
                                return (None, None, "not_linked")

                            else:
                                if attempt < self.max_retries:
                                    await asyncio.sleep(1 * attempt)
                                    continue
                                return (None, None, f"api_error_{response.status}")

            except asyncio.TimeoutError:
                if attempt < self.max_retries:
                    await asyncio.sleep(2 * attempt)
                    continue
                return (None, None, "timeout")

            except Exception as e:
                if attempt < self.max_retries:
                    await asyncio.sleep(1 * attempt)
                    continue
                return (None, None, f"error_{type(e).__name__}")

        return (None, None, "max_retries_exceeded")

    async def _get_roblox_username(self, roblox_id: int) -> Optional[str]:
        """Fetch Roblox username from Roblox API (does NOT count against Bloxlink quota)"""
        url = f"https://users.roblox.com/v1/users/{roblox_id}"

        for attempt in range(1, 3):
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data.get('name', 'Unknown')
                        elif attempt < 2:
                            await asyncio.sleep(1)
                            continue
                        return 'Unknown'
            except:
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                return 'Unknown'

        return 'Unknown'

    async def _enforce_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time

        if current_time - self.minute_start >= 60:
            self.requests_this_minute = 0
            self.minute_start = current_time

        if self.requests_this_minute >= self.max_requests_per_minute:
            wait_time = 60 - (current_time - self.minute_start)
            if wait_time > 0:
                print(f"⏸️ Hit rate limit ceiling, waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                self.requests_this_minute = 0
                self.minute_start = asyncio.get_event_loop().time()

        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last)

        self.last_request_time = asyncio.get_event_loop().time()
        self.requests_this_minute += 1
        BloxlinkAPI._api_calls_made += 1

    async def get_cache_stats(self) -> dict:
        """Get cache statistics from database"""
        async with db.pool.acquire() as conn:
            # Count valid (non-expired) entries
            valid_count = await conn.fetchval(
                'SELECT COUNT(*) FROM bloxlink_cache WHERE expires_at > NOW()'
            )

            # Count expired entries
            expired_count = await conn.fetchval(
                'SELECT COUNT(*) FROM bloxlink_cache WHERE expires_at <= NOW()'
            )

            # Get oldest cache entry
            oldest = await conn.fetchval(
                'SELECT MIN(cached_at) FROM bloxlink_cache WHERE expires_at > NOW()'
            )

            # Get newest cache entry
            newest = await conn.fetchval(
                'SELECT MAX(cached_at) FROM bloxlink_cache WHERE expires_at > NOW()'
            )

        return {
            'total_cached': valid_count + expired_count,
            'valid_entries': valid_count,
            'expired_entries': expired_count,
            'api_calls_made': BloxlinkAPI._api_calls_made,
            'quota_remaining': max(0, BloxlinkAPI._quota_limit - BloxlinkAPI._api_calls_made),
            'quota_reset_time': BloxlinkAPI._quota_reset_time,
            'oldest_cache': oldest,
            'newest_cache': newest
        }

    async def bulk_check_bloxlink(
            self,
            discord_user_ids: list,
            guild_id: int,
            progress_callback=None
    ) -> Dict[int, Dict]:
        """
        Bulk check with database-backed caching
        ✅ Uses database cache across restarts
        """
        results = {}
        total = len(discord_user_ids)

        status_counts = {
            'success': 0,
            'not_linked': 0,
            'rate_limited': 0,
            'timeout': 0,
            'api_error': 0,
            'cached': 0,
            'other': 0
        }

        consecutive_failures = 0
        max_consecutive_failures = 10
        max_total_failures = 30

        print(f"🔍 Starting bulk Bloxlink check for {total} users...")

        # ✅ BATCH 1: Get cached results from DATABASE
        cached_ids = []
        uncached_ids = []

        for discord_id in discord_user_ids:
            cached = await self._get_cached_data(discord_id)

            if cached:
                username, roblox_id, status = cached
                results[discord_id] = {
                    'roblox_username': username,
                    'roblox_user_id': roblox_id,
                    'status': status
                }
                cached_ids.append(discord_id)
                status_counts['cached'] += 1

                if status == 'success':
                    status_counts['success'] += 1
                elif status == 'not_linked':
                    status_counts['not_linked'] += 1
            else:
                uncached_ids.append(discord_id)

        if cached_ids:
            print(f"📦 Retrieved {len(cached_ids)} entries from cache")
        if uncached_ids:
            print(f"🌐 Need to fetch {len(uncached_ids)} from API...")

        # ✅ BATCH 2: Fetch only uncached users
        for i, discord_id in enumerate(uncached_ids, 1):
            username, roblox_id, status = await self.get_bloxlink_data(discord_id, guild_id)

            results[discord_id] = {
                'roblox_username': username,
                'roblox_user_id': roblox_id,
                'status': status
            }

            # 🚫 NEW: Stop if quota is exhausted
            if status == 'quota_exhausted':
                print(f"\n🚫 STOPPING: Daily API quota exhausted")
                print(f"   Processed {i}/{len(uncached_ids)} uncached users before hitting limit")
                print(f"   Cached users: {len(cached_ids)}")
                print(f"   ⏰ Quota will reset in ~24 hours")
                # Return results with what we have so far
                return results

            # Track status
            if status in ['rate_limited', 'timeout', 'max_retries_exceeded'] or 'api_error' in status:
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            if status == 'rate_limited':
                status_counts['rate_limited'] += 1
            elif status == 'timeout':
                status_counts['timeout'] += 1
            elif 'api_error' in status:
                status_counts['api_error'] += 1
            elif status == 'success':
                status_counts['success'] += 1
            elif status == 'not_linked':
                status_counts['not_linked'] += 1
            else:
                status_counts['other'] += 1

            total_failures = status_counts['rate_limited'] + status_counts['timeout'] + status_counts['api_error']

            # Safety checks
            if consecutive_failures >= max_consecutive_failures:
                print(f"\n❌ TERMINATING: Hit {consecutive_failures} consecutive failures!")
                return None

            if total_failures >= max_total_failures:
                print(f"\n❌ TERMINATING: Hit {total_failures} total failures!")
                return None

            # Progress callback
            if progress_callback:
                await progress_callback(len(cached_ids) + i, total, status_counts)

            if i % 10 == 0 or i == len(uncached_ids):
                print(f"Progress: {len(cached_ids) + i}/{total} | "
                      f"📦 Cached: {status_counts['cached']} | "
                      f"✅ Success: {status_counts['success']} | "
                      f"❌ Not Linked: {status_counts['not_linked']}")

        print(f"\n✅ Bulk check complete!")
        if cached_ids:
            print(f"   📦 Used cache: {len(cached_ids)} entries (0 API calls)")
        if uncached_ids:
            print(f"   🌐 Fetched fresh: {len(uncached_ids)} entries ({len(uncached_ids)} API calls)")
        print(f"   ✅ Total Success: {status_counts['success']}")
        if status_counts['cached'] > 0:
            print(f"   💾 Cache Efficiency: {(status_counts['cached'] / total) * 100:.1f}%")

        return results
    async def cleanup_expired_cache(self):
        """
        Remove expired cache entries from database
        Run this periodically (e.g., daily) to keep database clean
        """
        async with db.pool.acquire() as conn:
            deleted = await conn.execute(
                'DELETE FROM bloxlink_cache WHERE expires_at <= NOW()'
            )
            count = int(deleted.split()[-1])  # Extract count from "DELETE X"

        print(f"🧹 Cleaned up {count} expired cache entries")
        return count

class PaginatedEmbedView(discord.ui.View):
    """View for paginating through multiple embeds"""

    def __init__(self, embeds: list[discord.Embed], timeout=300):
        super().__init__(timeout=timeout)
        self.embeds = embeds
        self.current_page = 0
        self.max_pages = len(embeds)

        # Update button states
        self.update_buttons()

    def update_buttons(self):
        """Enable/disable buttons based on current page"""
        self.first_page.disabled = (self.current_page == 0)
        self.prev_page.disabled = (self.current_page == 0)
        self.next_page.disabled = (self.current_page >= self.max_pages - 1)
        self.last_page.disabled = (self.current_page >= self.max_pages - 1)

    @discord.ui.button(emoji="<:LeftSkip:1434962162064822343>", style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

    @discord.ui.button(emoji="<:LeftArrow:1434962165215002777>", style=discord.ButtonStyle.primary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

    @discord.ui.button(emoji="<:RightArrow:1434962170147246120>", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(self.max_pages - 1, self.current_page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

    @discord.ui.button(emoji="<:RightSkip:1434962167660281926>", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = self.max_pages - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)

    @discord.ui.button(emoji="<:Wipe:1434954284851658762>", style=discord.ButtonStyle.danger)
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await interaction.delete_original_response()
        self.stop()

class CallsignCog(commands.Cog):
    callsign_group = app_commands.Group(name="callsign", description="Callsign management commands")

    def __init__(self, bot):
        self.bot = bot
        self.sync_interval = 60  # 60 minutes
        # Start auto-sync on bot startup
        self.bloxlink_api = BloxlinkAPI()
        self.last_bloxlink_sync = None
        self.bloxlink_sync_interval = 86400  # 1 hour in seconds
        self.auto_sync_loop.start()
        self.cleanup_cache_loop.start()


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

    async def _process_sync_batch(self, guild: discord.Guild, callsigns: list, stats: dict,
                                  naughty_role_data: list) -> tuple[dict, list]:
        """Process a batch of callsigns for syncing - shared logic"""

        for record in callsigns:
            member = guild.get_member(record['discord_user_id'])

            if member:
                stats['members_found'] += 1

                # Update Discord username if changed
                current_discord_name = str(member)
                if current_discord_name != record.get('discord_username'):
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE callsigns SET discord_username = $1 WHERE discord_user_id = $2',
                            current_discord_name, member.id
                        )

                # Update last_seen_at
                async with db.pool.acquire() as conn:
                    await conn.execute(
                        'UPDATE callsigns SET last_seen_at = NOW() WHERE discord_user_id = $1',
                        member.id
                    )
                stats['last_seen_updates'] += 1

                # Check for naughty roles
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
                last_seen = record.get('last_seen_at')
                if last_seen:
                    days_gone = (datetime.utcnow() - last_seen).days
                    if days_gone >= 7:
                        callsign_display = f"{record['fenz_prefix']}-{record['callsign']}" if record['fenz_prefix'] else \
                        record['callsign']
                        stats['removed_users'].append({
                            'username': record['discord_username'],
                            'id': record['discord_user_id'],
                            'callsign': callsign_display,
                            'days_gone': days_gone,
                            'reason': f'Inactive for {days_gone} days'
                        })

                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                'DELETE FROM callsigns WHERE discord_user_id = $1',
                                record['discord_user_id']
                            )
                        await sheets_manager.remove_callsign_from_sheets(record['discord_user_id'])
                        stats['removed_inactive'] += 1

        return stats, naughty_role_data

    async def _check_bloxlink_sync_needed(self) -> bool:
        """
        Check if Bloxlink sync is needed (persists across bot restarts)
        Returns True if sync needed, False if cache is still fresh
        """
        try:
            async with db.pool.acquire() as conn:
                # Get last sync time from database
                result = await conn.fetchrow(
                    '''SELECT last_sync_at
                       FROM bot_config
                       WHERE config_key = 'bloxlink_sync' '''
                )

                if not result or not result['last_sync_at']:
                    print("ℹ️ No previous Bloxlink sync found - will perform initial sync")
                    return True

                last_sync = result['last_sync_at']
                time_since_sync = (datetime.utcnow() - last_sync).total_seconds()

                # Sync every 24 hours (86400 seconds)
                if time_since_sync >= 86400:
                    hours_since = int(time_since_sync / 3600)
                    print(f"🔄 Bloxlink cache expired ({hours_since}h old) - refresh needed")
                    return True
                else:
                    return False

        except Exception as e:
            print(f"⚠️ Error checking sync status: {e}")
            # If error, assume sync is needed to be safe
            return True

    async def _record_bloxlink_sync_time(self):
        """Record the current time as the last Bloxlink sync time"""
        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO bot_config (config_key, last_sync_at)
                       VALUES ('bloxlink_sync', NOW()) ON CONFLICT (config_key)
                       DO
                    UPDATE SET last_sync_at = NOW()'''
                )
            print("✅ Recorded Bloxlink sync time to database")
        except Exception as e:
            print(f"⚠️ Error recording sync time: {e}")

    async def _get_sync_schedule_info(self) -> tuple[int, int]:
        """
        Get information about the sync schedule
        Returns: (hours_since_last_sync, hours_until_next_sync)
        """
        try:
            async with db.pool.acquire() as conn:
                result = await conn.fetchrow(
                    '''SELECT last_sync_at
                       FROM bot_config
                       WHERE config_key = 'bloxlink_sync' '''
                )

                if not result or not result['last_sync_at']:
                    return (0, 0)

                last_sync = result['last_sync_at']
                time_since_sync = (datetime.utcnow() - last_sync).total_seconds()
                hours_since = int(time_since_sync / 3600)
                hours_until_next = max(0, int((86400 - time_since_sync) / 3600))

                return (hours_since, hours_until_next)
        except:
            return (0, 0)

    @tasks.loop(hours=1)  # ✅ Runs every hour, but only syncs Bloxlink every 24h
    async def auto_sync_loop(self):
        """Enhanced background task with intelligent Bloxlink caching"""
        if db.pool is None:
            print("⚠️ Auto-sync skipped: database not connected")
            return

        print(f"\n{'=' * 60}")
        print(f"🔄 Auto-sync started at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"{'=' * 60}")

        # ✅ NEW: Determine if we need a full Bloxlink sync (once every 24 hours)
        needs_bloxlink_sync = await self._check_bloxlink_sync_needed()

        for guild in self.bot.guilds:
            if guild.id in EXCLUDED_GUILDS:
                print(f"⭐️ Skipping auto-sync for excluded guild: {guild.name} ({guild.id})")
                continue

            try:
                sync_start_time = datetime.utcnow()

                # ✅ NEW: Phase 1 - Bloxlink Cache Refresh (ONLY if 24 hours have passed)
                if needs_bloxlink_sync:
                    print(f"🔄 Starting 24-hour Bloxlink cache refresh for {guild.name}...")
                    await self._refresh_bloxlink_cache(guild)
                    await self._record_bloxlink_sync_time()
                    print(f"✅ Bloxlink cache refreshed - valid for next 24 hours")

                    # Check if quota was exhausted during refresh
                    if BloxlinkAPI._quota_exhausted:
                        print(f"⚠️ Warning: API quota exhausted during cache refresh")
                        print(f"   Some users may not have fresh data")
                        print(f"   Will retry in next sync cycle (1 hour)")

                else:
                    hours_since, hours_until = await self._get_sync_schedule_info()
                    print(
                        f"📦 Using cached Bloxlink data (refreshed {hours_since}h ago, next refresh in {hours_until}h)")

                # ✅ Phase 2: Regular hourly sync using CACHED Bloxlink data (NO API CALLS)
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
                    'naughty_roles_found': 0,
                    'naughty_roles_stored': 0,
                    'naughty_roles_removed': 0,
                    'permission_errors': [],
                    'bloxlink_cache_hits': 0,
                    'bloxlink_api_calls': 0,
                }

                naughty_role_data = []

                # ✅ MODIFIED: Use cached Bloxlink API
                cache_stats_before = await self.bloxlink_api.get_cache_stats()

                # Detect and fix database mismatches using cached data
                mismatches = await self.detect_database_mismatches(
                    guild=guild,
                    progress_callback=None,
                    bloxlink_cache={}  # ✅ Use cog-level cache
                )

                mismatch_fixes = 0
                fixed_users = set()

                # Fix all database mismatches (KEEP ALL EXISTING CODE)
                for item in mismatches['discord_username_mismatch']:
                    if item['member'].id not in fixed_users:
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                'UPDATE callsigns SET discord_username = $1 WHERE discord_user_id = $2',
                                item['new'], item['member'].id
                            )
                        mismatch_fixes += 1
                        fixed_users.add(item['member'].id)

                for item in mismatches['missing_discord_username']:
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE callsigns SET discord_username = $1 WHERE discord_user_id = $2',
                            str(item['member']), item['member'].id
                        )
                    mismatch_fixes += 1
                    fixed_users.add(item['member'].id)

                for item in mismatches['roblox_username_mismatch']:
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE callsigns SET roblox_username = $1 WHERE discord_user_id = $2',
                            item['new'], item['member'].id
                        )
                    mismatch_fixes += 1
                    fixed_users.add(item['member'].id)

                for item in mismatches['roblox_id_mismatch']:
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE callsigns SET roblox_user_id = $1, roblox_username = $2 WHERE discord_user_id = $3',
                            item['new_id'], item['current_username'], item['member'].id
                        )
                    mismatch_fixes += 1
                    fixed_users.add(item['member'].id)

                for item in mismatches['missing_roblox_id']:
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE callsigns SET roblox_user_id = $1, roblox_username = $2 WHERE discord_user_id = $3',
                            item['current_id'], item['current_username'], item['member'].id
                        )
                    mismatch_fixes += 1
                    fixed_users.add(item['member'].id)

                stats['database_mismatches_fixed'] = mismatch_fixes

                # Process each callsign for nickname updates
                # Process each callsign for nickname updates
                for record in callsigns:
                    member = guild.get_member(record['discord_user_id'])
                    if not member:
                        continue

                    # Get current rank information from ROLES (not database)
                    current_fenz_prefix = None
                    for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                        if any(role.id == role_id for role in member.roles):
                            current_fenz_prefix = prefix
                            break

                    # ✅ CRITICAL: Pass stored shorthand to preserve user's choice
                    stored_hhstj_prefix = record['hhstj_prefix']
                    current_hhstj_prefix = get_hhstj_prefix_from_roles(member.roles, stored_hhstj_prefix)

                    current_callsign = record['callsign']
                    roblox_username = record['roblox_username']

                    # ✅ Calculate expected nickname using priority system
                    expected_nickname = format_nickname(
                        current_fenz_prefix or record['fenz_prefix'],
                        current_callsign,
                        current_hhstj_prefix,  # Already has shorthand preserved
                        roblox_username
                    )

                    # Get current nickname (strip shift prefixes)
                    current_nick = strip_shift_prefixes(member.nick) if member.nick else member.name

                    # Update if different
                    if current_nick != expected_nickname:
                        try:
                            # Preserve shift prefix if exists
                            shift_prefix = ""
                            if member.nick:
                                for prefix in ["DUTY | ", "BRK | ", "LOA | "]:
                                    if member.nick.startswith(prefix):
                                        shift_prefix = prefix
                                        break

                            final_nickname = shift_prefix + expected_nickname

                            success, final_nick = await safe_edit_nickname(member, final_nickname)

                            if success:
                                stats['nickname_changes'].append({
                                    'member': member,
                                    'old': current_nick,
                                    'new': expected_nickname
                                })
                                stats['nickname_updates'] += 1
                        except discord.Forbidden:
                            stats['permission_errors'].append(member)
                        except Exception as e:
                            stats['errors'].append({
                                'member': member,
                                'username': str(member),
                                'error': str(e)
                            })

                # Check each callsign in database (KEEP ALL EXISTING CODE)
                # ✅ Process all callsigns using shared batch logic (error recovery path)
                stats, naughty_role_data = await self._process_sync_batch(
                    guild, callsigns, stats, naughty_role_data
                )

                # Sync naughty roles to database (KEEP ALL EXISTING CODE)
                if naughty_role_data:
                    async with db.pool.acquire() as conn:
                        stored_roles = await conn.fetch(
                            'SELECT discord_user_id, role_id FROM naughty_roles WHERE removed_at IS NULL'
                        )
                        stored_set = {(r['discord_user_id'], r['role_id']) for r in stored_roles}
                        current_set = {(r['discord_user_id'], r['role_id']) for r in naughty_role_data}
                        to_add = current_set - stored_set
                        to_remove = stored_set - current_set

                        for user_id, role_id in to_add:
                            role_info = next(r for r in naughty_role_data if
                                             r['discord_user_id'] == user_id and r['role_id'] == role_id)
                            await conn.execute(
                                '''INSERT INTO naughty_roles (discord_user_id, discord_username, role_id, role_name, last_seen_at)
                                   VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (discord_user_id, role_id) 
                                   DO
                                UPDATE SET removed_at = NULL, last_seen_at = NOW(), discord_username = $2''',
                                role_info['discord_user_id'], role_info['discord_username'], role_info['role_id'],
                                role_info['role_name']
                            )
                            stats['naughty_roles_stored'] += 1

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

                        for role_info in naughty_role_data:
                            await conn.execute(
                                '''UPDATE naughty_roles
                                   SET last_seen_at = NOW()
                                   WHERE discord_user_id = $1
                                     AND role_id = $2''',
                                role_info['discord_user_id'], role_info['role_id']
                            )

                    stats['naughty_role_details'] = {'added': [], 'removed': []}
                    for user_id, role_id in to_add:
                        role_info = next(
                            r for r in naughty_role_data if r['discord_user_id'] == user_id and r['role_id'] == role_id)
                        member = guild.get_member(user_id)
                        if member:
                            stats['naughty_role_details']['added'].append(
                                {'member': member, 'role_name': role_info['role_name']})

                    for user_id, role_id in to_remove:
                        member = guild.get_member(user_id)
                        role_name = NAUGHTY_ROLES.get(role_id, "Unknown")
                        if member:
                            stats['naughty_role_details']['removed'].append({'member': member, 'role_name': role_name})

                # Sync to Google Sheets (KEEP ALL EXISTING CODE)
                if callsigns:
                    callsign_data = []
                    sheet_callsigns = await sheets_manager.get_all_callsigns_from_sheets()
                    sheet_map = {cs['discord_user_id']: cs for cs in sheet_callsigns}
                    db_map = {record['discord_user_id']: dict(record) for record in callsigns}

                    for discord_id, sheet_data in sheet_map.items():
                        if discord_id not in db_map:
                            member = guild.get_member(discord_id)
                            if member:
                                # ✅ Use cog-level cached API
                                # ✅ Use cog-level cached API
                                roblox_username, roblox_id, status = await self.bloxlink_api.get_bloxlink_data(
                                    member.id, guild.id)

                                if status == 'success' and roblox_id and roblox_username:
                                    # ✅ Get HHStJ prefix WITH preservation of any existing shorthand from sheets
                                    stored_hhstj = sheet_data.get('hhstj_prefix', '')
                                    hhstj_prefix = get_hhstj_prefix_from_roles(member.roles, stored_hhstj)

                                    is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                                    is_hhstj_high_command = any(
                                        role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                                    await add_callsign_to_database(
                                        sheet_data['callsign'], discord_id, str(member), str(roblox_id),
                                        roblox_username,
                                        sheet_data['fenz_prefix'], hhstj_prefix or '', self.bot.user.id, "Auto-sync",
                                        is_fenz_high_command, is_hhstj_high_command
                                    )

                                    callsign_display = f"{sheet_data['fenz_prefix']}-{sheet_data['callsign']}" if \
                                    sheet_data['fenz_prefix'] else sheet_data['callsign']
                                    stats['added_users'].append({'member': member, 'callsign': callsign_display})
                                    stats['added_from_sheets'] += 1
                                else:
                                    print(f"⚠️ Auto-sync: Could not get Bloxlink for {member.id}: {status}")

                    if stats['added_from_sheets'] > 0:
                        async with db.pool.acquire() as conn:
                            callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')
                        stats['total_callsigns'] = len(callsigns)

                    for record in callsigns:
                        member = guild.get_member(record['discord_user_id'])
                        if not member:
                            continue

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
                            'strikes': sheets_manager.determine_strikes_value(member.roles),
                            'qualifications': sheets_manager.determine_qualifications(member.roles, is_command_rank)
                        })

                    callsign_data.sort(key=lambda x: get_rank_sort_key(x['fenz_prefix'], x['hhstj_prefix']))
                    await sheets_manager.batch_update_callsigns(callsign_data)

                    # ✅ NEW: Track cache efficiency
                    cache_stats_after = await self.bloxlink_api.get_cache_stats()
                    stats['bloxlink_cache_hits'] = stats['members_found']  # All found members used cache
                    stats['bloxlink_api_calls'] = cache_stats_after['api_calls_made'] - cache_stats_before[
                        'api_calls_made']

                    sync_duration = (datetime.utcnow() - sync_start_time).total_seconds()

                    # ✅ MODIFIED: Enhanced logging with cache stats
                    await self.send_detailed_sync_log(self.bot, guild.name, stats, sync_duration)

                    print(f"✅ Auto-sync completed for guild {guild.name}:")
                    print(f"    📊 {stats['total_callsigns']} callsigns synced")
                    print(f"    👥 {stats['members_found']} members found / {stats['members_not_found']} not in server")
                    print(
                        f"    📦 Bloxlink cache hits: {stats['bloxlink_cache_hits']} (API calls: {stats['bloxlink_api_calls']})")
                    if stats['bloxlink_api_calls'] == 0:
                        print(f"    ✅ Zero API calls - using 100% cached data!")
                        # Show quota status
                    if BloxlinkAPI._quota_exhausted:
                        print(f"    🚫 API QUOTA EXHAUSTED - waiting for reset")
                        if BloxlinkAPI._quota_reset_time:
                            reset_time = datetime.utcfromtimestamp(BloxlinkAPI._quota_reset_time)
                            print(f"    ⏰ Resets at: {reset_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                    else:
                        remaining = max(0, BloxlinkAPI._quota_limit - BloxlinkAPI._api_calls_made)
                        print(f"    📊 API Quota remaining: {remaining}/{BloxlinkAPI._quota_limit}")
                    if stats['nickname_updates'] > 0:
                        print(f"    🏷️ {stats['nickname_updates']} nicknames updated")
                    if stats['added_from_sheets'] > 0:
                        print(f"    ➕ {stats['added_from_sheets']} added from sheets")
                    if stats['removed_inactive'] > 0:
                        print(f"    🗑️ {stats['removed_inactive']} removed (inactive 7+ days)")
                    if stats['callsigns_reset']:
                        print(f"    🔄 {len(stats['callsigns_reset'])} callsigns reset due to rank changes")
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
                    if stats.get('database_mismatches_fixed', 0) > 0:
                        print(f"    🔧 {stats['database_mismatches_fixed']} database mismatches fixed")
                    print(f"    ⏱️ Completed in {sync_duration:.2f}s")

            except Exception as e:
                print(f"❌ Error during auto-sync for {guild.name}: {e}")
                import traceback
                traceback.print_exc()

                try:
                    await self.send_sync_log(
                        self.bot,
                        "❌ Auto-Sync Failed",
                        f"Auto-sync failed for **{guild.name}**",
                        [{'name': 'Error', 'value': f'```{str(e)[:1000]}```', 'inline': False}],
                        discord.Color.red()
                    )
                except:
                    pass

                try:
                    stats, naughty_role_data = await self._process_sync_batch(
                        guild, callsigns, stats, naughty_role_data
                    )
                except Exception as recovery_error:
                    print(f"❌ Error during error recovery: {recovery_error}")

    @auto_sync_loop.before_loop
    async def before_auto_sync(self):
        """Wait for bot AND database to be ready before starting auto-sync"""
        await self.bot.wait_until_ready()

        # Wait for database connection with timeout
        max_wait = 60  # Maximum 60 seconds wait
        waited = 0
        while db.pool is None and waited < max_wait:
            print(f"⏳ Auto-sync waiting for database connection... ({waited}s)")
            await asyncio.sleep(5)
            waited += 5

        if db.pool is None:
            print("❌ Auto-sync failed to start: Database not connected after 60s")
        else:
            print("✅ Auto-sync ready - database connected")

    @tasks.loop(hours=24)
    async def cleanup_cache_loop(self):
        """Clean up expired cache entries daily"""
        await self.bloxlink_api.cleanup_expired_cache()

    @cleanup_cache_loop.before_loop
    async def before_cleanup_cache(self):
        """Wait for bot to be ready before cleanup"""
        await self.bot.wait_until_ready()
        print("✅ Cache cleanup loop ready")


    async def _refresh_bloxlink_cache(self, guild: discord.Guild):
        """
        Refresh Bloxlink cache for all users in the database
        This runs once every 24 HOURS to keep cache fresh
        """
        try:
            # Get all Discord user IDs from database
            async with db.pool.acquire() as conn:
                user_ids = await conn.fetch('SELECT DISTINCT discord_user_id FROM callsigns')

            discord_ids = [record['discord_user_id'] for record in user_ids]

            if not discord_ids:
                print("ℹ️ No users in database to cache")
                return

            print(f"🔄 Starting 24-hour Bloxlink cache refresh for {len(discord_ids)} users...")
            print(f"⏰ This will take a few minutes but ensures fresh data for the next 24 hours")

            # Use bulk check with progress tracking
            async def cache_progress(current, total, status_counts):
                if current % 25 == 0 or current == total:
                    print(f"   📦 Caching progress: {current}/{total} "
                          f"(✅ {status_counts['success']} | ❌ {status_counts['not_linked']} | 📦 {status_counts.get('cached', 0)} from cache)")

            results = await self.bloxlink_api.bulk_check_bloxlink(
                discord_ids,
                guild_id=guild.id,
                progress_callback=cache_progress
            )

            if results is None:
                print("⚠️ Bloxlink cache refresh failed - API issues detected")
                print("   Will retry in next auto-sync cycle")
                return

            # Cache is automatically updated by bulk_check_bloxlink
            cache_stats = await self.bloxlink_api.get_cache_stats()
            print(f"✅ 24-hour cache refresh complete:")
            print(f"   📦 Total cached: {cache_stats['total_cached']}")
            print(f"   ✅ Valid entries: {cache_stats['valid_entries']}")
            print(f"   🌐 API calls made: {cache_stats['api_calls_made']}")
            print(f"   ⏰ Next refresh: 24 hours from now")
            print(f"   💡 All hourly syncs will use this cached data (0 API calls)")

        except Exception as e:
            print(f"❌ Error refreshing Bloxlink cache: {e}")
            import traceback
            traceback.print_exc()

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
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )

            summary_embed.add_field(name='Total Callsigns', value=str(stats['total_callsigns']), inline=True)
            summary_embed.add_field(name='Members Found', value=str(stats['members_found']), inline=True)
            summary_embed.add_field(name='Not in Server', value=str(stats['members_not_found']), inline=True)
            summary_embed.add_field(name='Nicknames Updated', value=str(stats['nickname_updates']), inline=True)
            summary_embed.add_field(name='Rank Changes', value=str(stats['rank_updates']), inline=True)
            summary_embed.add_field(name='Duration', value=f'{sync_duration:.2f}s', inline=True)

            # Cache efficiency stats
            if stats.get('bloxlink_cache_hits') is not None:
                cache_efficiency = 0
                if stats['members_found'] > 0:
                    cache_efficiency = (stats['bloxlink_cache_hits'] / stats['members_found']) * 100

                summary_embed.add_field(
                    name='Cache Efficiency',
                    value=f"Hits: {stats['bloxlink_cache_hits']}\n"
                          f"API Calls: {stats['bloxlink_api_calls']}\n"
                          f"Efficiency: {cache_efficiency:.1f}%",
                    inline=True
                )

                # Quota status warning if exhausted
                if BloxlinkAPI._quota_exhausted:
                    quota_text = "<:No:1437788507111428228> **API QUOTA EXHAUSTED**\n"
                    if BloxlinkAPI._quota_reset_time:
                        quota_text += f"Resets: <t:{int(BloxlinkAPI._quota_reset_time)}:R>"
                    else:
                        quota_text += "Will reset in ~24 hours"

                    summary_embed.add_field(
                        name='<:Warn:1437771973970104471>️ Quota Status',
                        value=quota_text,
                        inline=True
                    )

            # Permission errors - separate admin from non-admin
            if stats.get('permission_errors'):
                non_admin_errors = [m for m in stats['permission_errors']
                                    if not m.guild_permissions.administrator]

                if non_admin_errors:
                    mentions = ' '.join([member.mention for member in non_admin_errors[:25]])
                    if len(non_admin_errors) > 25:
                        mentions += f"\n... and {len(non_admin_errors) - 25} more"

                    summary_embed.add_field(
                        name=f'<:Warn:1437771973970104471>️ Permission Errors ({len(non_admin_errors)})',
                        value=mentions,
                        inline=False
                    )

                admin_count = len(stats['permission_errors']) - len(non_admin_errors)
                if admin_count > 0:
                    summary_embed.add_field(
                        name='Admin Permission Skips',
                        value=f"{admin_count} administrators (cannot edit their nicknames)",
                        inline=False
                    )

            # Naughty role stats
            if stats.get('naughty_roles_found', 0) > 0:
                summary_embed.add_field(
                    name='Naughty Roles',
                    value=f"Found: {stats['naughty_roles_found']}\n"
                          f"Stored: {stats['naughty_roles_stored']}\n"
                          f"Removed: {stats['naughty_roles_removed']}",
                    inline=True
                )

            # Database fixes
            if stats.get('database_mismatches_fixed', 0) > 0:
                summary_embed.add_field(
                    name='Database Fixes',
                    value=str(stats['database_mismatches_fixed']),
                    inline=True
                )

            # Added from sheets
            if stats.get('added_from_sheets', 0) > 0:
                summary_embed.add_field(
                    name='Added from Sheets',
                    value=str(stats['added_from_sheets']),
                    inline=True
                )

            # Removed inactive
            if stats.get('removed_inactive', 0) > 0:
                summary_embed.add_field(
                    name='<:Wipe:1434954284851658762> Removed (Inactive 7+ days)',
                    value=str(stats['removed_inactive']),
                    inline=True
                )

            await channel.send(embed=summary_embed)

            # ========== DETAILED CHANGE LOGS ==========

            # 1. Nickname Changes - SHOW WHO AND WHAT CHANGED
            # 1. Nickname Changes - SHOW WHO AND WHAT CHANGED
            if stats.get('nickname_changes'):
                for i in range(0, len(stats['nickname_changes']), 3):
                    chunk = stats['nickname_changes'][i:i + 3]

                    embed = discord.Embed(
                        title=f"Nickname Updates ({i + 1}-{min(i + 3, len(stats['nickname_changes']))} of {len(stats['nickname_changes'])})",
                        color=discord.Color.green()
                    )

                    for change in chunk:
                        old_nick = change['old'][:100] if len(change['old']) > 100 else change['old']
                        new_nick = change['new'][:100] if len(change['new']) > 100 else change['new']

                        embed.add_field(
                            name=f"{change['member'].display_name[:50]}",
                            value=f"{change['member'].mention}\n**Before:** `{old_nick}`\n**After:** `{new_nick}`",
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    await channel.send(embed=embed)

            # 2. Rank Changes - SHOW WHO AND WHAT RANK CHANGED
            if stats.get('rank_changes'):
                for i in range(0, len(stats['rank_changes']), 3):
                    chunk = stats['rank_changes'][i:i + 3]

                    embed = discord.Embed(
                        title=f"Rank Changes ({i + 1}-{min(i + 3, len(stats['rank_changes']))} of {len(stats['rank_changes'])})",
                        color=discord.Color.gold()
                    )

                    for change in chunk:
                        rank_type = change.get('type', 'Unknown')
                        old_rank = change.get('old_rank', 'Unknown')
                        new_rank = change.get('new_rank', 'Unknown')
                        member = change.get('member')

                        if not member:
                            continue

                        embed.add_field(
                            name=f"{change['member'].display_name[:50]}",
                            value=f"{change['member'].mention}\n**Type:** {rank_type}\n**{old_rank}** → **{new_rank}**",
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    await channel.send(embed=embed)

            # 3. Callsigns Reset - SHOW WHO AND WHY
            if stats.get('callsigns_reset'):
                for i in range(0, len(stats['callsigns_reset']), 3):
                    chunk = stats['callsigns_reset'][i:i + 3]

                    embed = discord.Embed(
                        title=f"Callsigns Reset ({i + 1}-{min(i + 3, len(stats['callsigns_reset']))} of {len(stats['callsigns_reset'])})",
                        description="These callsigns were reset to Not Assigned due to rank changes",
                        color=discord.Color.orange()
                    )

                    for reset in chunk:
                        value_parts = [reset['member'].mention]  # ✅ Start with mention
                        if reset.get('type'):
                            value_parts.append(f"**Type:** {reset['type']}")
                        if reset.get('old_rank') and reset.get('new_rank'):
                            value_parts.append(f"**{reset['old_rank']}** → **{reset['new_rank']}**")
                        if reset.get('old_callsign'):
                            value_parts.append(f"**Old Callsign:** {reset['old_callsign']}")
                        if reset.get('new_prefix'):
                            value_parts.append(f"**New Prefix:** {reset['new_prefix']}")
                        if reset.get('reason'):
                            value_parts.append(f"**Reason:** {reset['reason']}")

                        embed.add_field(
                            name=f"{reset['member'].display_name[:50]}",
                            value="\n".join(value_parts) if value_parts else "Rank changed",
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    await channel.send(embed=embed)

            # 4. Added from Sheets - SHOW WHO WAS ADDED
            if stats.get('added_users'):
                for i in range(0, len(stats['added_users']), 10):
                    chunk = stats['added_users'][i:i + 10]

                    embed = discord.Embed(
                        title=f"Added from Sheets ({i + 1}-{min(i + 10, len(stats['added_users']))} of {len(stats['added_users'])})",
                        description="Users found in sheets but not in database (now added)",
                        color=discord.Color.teal()
                    )

                    for added in chunk:
                        embed.add_field(
                            name=f"{added['member'].mention}",
                            value=f"**Callsign:** {added['callsign']}",
                            inline=True
                        )

                    await channel.send(embed=embed)

            # 5. Removed Users - SHOW WHO WAS REMOVED AND WHY
            if stats.get('removed_users'):
                for i in range(0, len(stats['removed_users']), 10):
                    chunk = stats['removed_users'][i:i + 10]

                    embed = discord.Embed(
                        title=f"<:Wipe:1434954284851658762> Removed (Inactive) ({i + 1}-{min(i + 10, len(stats['removed_users']))} of {len(stats['removed_users'])})",
                        description="Users removed from database (not in server for 7+ days)",
                        color=discord.Color.dark_red()
                    )

                    for removed in chunk:
                        value_parts = [
                            f"<@{removed['id']}>",  # ✅ Added mention using user ID
                            f"**Callsign:** {removed['callsign']}",
                            f"**Days Gone:** {removed['days_gone']}",
                            f"**Reason:** {removed['reason']}"
                        ]

                        embed.add_field(
                            name=f"{removed['username']} (ID: {removed['id']})",
                            value="\n".join(value_parts),
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    await channel.send(embed=embed)

            # 6. Naughty Roles Added - SHOW WHO GOT NAUGHTY ROLES
            if stats.get('naughty_role_details', {}).get('added'):
                added_roles = stats['naughty_role_details']['added']
                for i in range(0, len(added_roles), 5):
                    chunk = added_roles[i:i + 5]

                    embed = discord.Embed(
                        title=f"Naughty Roles Added ({i + 1}-{min(i + 5, len(added_roles))} of {len(added_roles)})",
                        description="These users received naughty roles during sync",
                        color=discord.Color.red()
                    )

                    for item in chunk:
                        embed.add_field(
                            name=f"{item['member'].mention}",
                            value=f"**Role:** {item['role_name']}",
                            inline=True
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    await channel.send(embed=embed)

            # 7. Naughty Roles Removed - SHOW WHO LOST NAUGHTY ROLES
            if stats.get('naughty_role_details', {}).get('removed'):
                removed_roles = stats['naughty_role_details']['removed']
                for i in range(0, len(removed_roles), 5):
                    chunk = removed_roles[i:i + 5]

                    embed = discord.Embed(
                        title=f"<:Accepted:1426930333789585509> Naughty Roles Removed ({i + 1}-{min(i + 5, len(removed_roles))} of {len(removed_roles)})",
                        description="These users no longer have naughty roles",
                        color=discord.Color.green()
                    )

                    for item in chunk:
                        embed.add_field(
                            name=f"{item['member'].mention}",
                            value=f"**Role:** {item['role_name']}",
                            inline=True
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    await channel.send(embed=embed)

            # 8. Other Errors - SHOW WHO HAD NON-PERMISSION ERRORS
            non_permission_errors = [e for e in stats.get('errors', []) if e.get('error') != 'Missing permissions']

            if non_permission_errors:
                for i in range(0, len(non_permission_errors), 5):
                    chunk = non_permission_errors[i:i + 5]

                    embed = discord.Embed(
                        title=f"<:Denied:1426930694633816248> Other Errors ({i + 1}-{min(i + 5, len(non_permission_errors))} of {len(non_permission_errors)})",
                        description="Non-permission errors that occurred during sync",
                        color=discord.Color.red()
                    )

                    for error in chunk:
                        member_mention = error['member'].mention if error.get(
                            'member') else f"<@{error.get('user_id', 'Unknown')}>"  # ✅ Added fallback mention
                        embed.add_field(
                            name=f"User: {error.get('username', 'Unknown')}",
                            value=f"{member_mention}\n**Error:** {error['error']}",  # ✅ Added mention
                            inline=False
                        )

                    await channel.send(embed=embed)

        except Exception as e:
            print(f"❌ Error sending detailed sync log: {e}")
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

    @callsign_group.command(name="sync", description="Sync callsigns to Google Sheets and update Discord nicknames")
    @app_commands.describe(
        dry_run="Preview changes without applying them (default: False)",
        update_nicknames="Update Discord nicknames (default: True)",
        update_sheets="Update Google Sheets (default: True)"
    )
    @app_commands.checks.has_role(SYNC_ROLE_ID)
    async def sync_callsigns(
            self,
            interaction: discord.Interaction,
            dry_run: bool = False,
            update_nicknames: bool = True,
            update_sheets: bool = True
    ):
        """
        Unified sync command that:
        - Syncs database ↔ Google Sheets
        - Updates Discord nicknames to match database
        - Can do dry-run preview
        - Preserves HHStJ shortened prefixes
        """

        if dry_run:
            await interaction.response.send_message(
                content=f"<a:Load:1430912797469970444> Testing Sync (Dry Run)",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                content=f"<a:Load:1430912797469970444> Syncing Callsigns",
                ephemeral=True
            )

        # Safety check
        if db.pool is None:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Database connection not available.",
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

            # Track changes
            stats = {
                'total_callsigns': len(db_callsigns),
                'added_from_sheets': 0,
                'nickname_updates': 0,
                'callsigns_reset': [],
                'rank_updates': 0,
                'failed_updates': [],
                'missing_in_sheets': [],
                'nickname_changes': [],
                'rank_changes': [],
                'hhstj_prefix_preserved': 0
            }

            # Find entries in sheets but not in database
            for discord_id, sheet_data in sheet_map.items():
                if discord_id not in db_map:
                    member = interaction.guild.get_member(discord_id)
                    if member:
                        bloxlink_api = BloxlinkAPI()
                        roblox_username, roblox_id, status = await bloxlink_api.get_bloxlink_data(member.id,
                                                                                                  interaction.guild.id)

                        if status == 'success' and roblox_id and roblox_username:
                            hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)
                            is_fenz_hc = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                            is_hhstj_hc = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

                            if not dry_run:
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
                                    is_fenz_hc,
                                    is_hhstj_hc
                                )

                            stats['added_from_sheets'] += 1
                        else:
                            stats['missing_in_sheets'].append(
                                f"Discord ID {discord_id} - Bloxlink error: {status}"
                            )

            # Re-fetch if we added entries
            if stats['added_from_sheets'] > 0 and not dry_run:
                async with db.pool.acquire() as conn:
                    db_callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

            # Process each callsign for rank changes and nickname updates
            for record in db_callsigns:
                try:
                    record = dict(record)
                    member = interaction.guild.get_member(record['discord_user_id'])

                    if not member:
                        continue

                    # Check high command status
                    is_fenz_hc = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                    is_hhstj_hc = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

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

                    # DETECT FENZ RANK CHANGES
                    fenz_rank_changed = correct_fenz_prefix and correct_fenz_prefix != current_fenz_prefix

                    # Special case: high command can choose no prefix
                    if is_fenz_hc and current_fenz_prefix == "":
                        fenz_rank_changed = False

                    # Handle FENZ rank changes
                    if fenz_rank_changed and current_callsign not in ["Not Assigned", "BLANK"]:
                        old_callsign = f"{current_fenz_prefix}-{current_callsign}" if current_fenz_prefix else current_callsign

                        existing = await check_callsign_exists(current_callsign, correct_fenz_prefix)
                        if existing and existing['discord_user_id'] != member.id:
                            # Callsign conflict - reset to Not Assigned
                            if not dry_run:
                                async with db.pool.acquire() as conn:
                                    await conn.execute(
                                        'UPDATE callsigns SET callsign = $1, fenz_prefix = $2 WHERE discord_user_id = $3',
                                        "Not Assigned", correct_fenz_prefix, member.id
                                    )

                            stats['callsigns_reset'].append({
                                'member': member,
                                'old_callsign': old_callsign,
                                'new_prefix': correct_fenz_prefix,
                                'reason': f'Rank changed but callsign conflicts: {correct_fenz_prefix}-{current_callsign} already exists'
                            })
                            current_fenz_prefix = correct_fenz_prefix
                            current_callsign = "Not Assigned"
                            stats['rank_updates'] += 1

                            stats['rank_changes'].append({
                                'member': member,
                                'type': 'FENZ',
                                'old_rank': current_fenz_prefix,
                                'new_rank': correct_fenz_prefix
                            })

                        else:
                            # No conflict, just reset callsign
                            if not dry_run:
                                async with db.pool.acquire() as conn:
                                    await conn.execute(
                                        'UPDATE callsigns SET callsign = $1, fenz_prefix = $2 WHERE discord_user_id = $3',
                                        "Not Assigned", correct_fenz_prefix, member.id
                                    )

                            stats['callsigns_reset'].append({
                                'member': member,
                                'old_callsign': old_callsign,
                                'new_prefix': correct_fenz_prefix
                            })

                            record['callsign'] = "Not Assigned"
                            record['fenz_prefix'] = correct_fenz_prefix
                            current_fenz_prefix = correct_fenz_prefix
                            current_callsign = "Not Assigned"
                            stats['rank_updates'] += 1

                            stats['rank_changes'].append({
                                'member': member,
                                'type': 'FENZ',
                                'old_rank': current_fenz_prefix,
                                'new_rank': correct_fenz_prefix
                            })

                    elif fenz_rank_changed:
                        # Just update prefix
                        if not dry_run:
                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET fenz_prefix = $1 WHERE discord_user_id = $2',
                                    correct_fenz_prefix, member.id
                                )

                        stats['rank_changes'].append({
                            'member': member,
                            'type': 'FENZ',
                            'old_rank': current_fenz_prefix,
                            'new_rank': correct_fenz_prefix
                        })

                        record['fenz_prefix'] = correct_fenz_prefix
                        current_fenz_prefix = correct_fenz_prefix
                        stats['rank_updates'] += 1

                    # ✅ UPDATE HHSTJ PREFIX WITH PRESERVATION LOGIC
                    stored_hhstj_prefix = record['hhstj_prefix']
                    correct_hhstj_prefix = get_hhstj_prefix_from_roles(member.roles, stored_hhstj_prefix)

                    if correct_hhstj_prefix != stored_hhstj_prefix:
                        # Check if this is just a shorthand preference vs actual rank change
                        needs_update = True

                        if correct_hhstj_prefix and '-' in correct_hhstj_prefix:
                            valid_versions = get_hhstj_shortened_versions(correct_hhstj_prefix)

                            # If current stored prefix is a valid shortened version, preserve it
                            if stored_hhstj_prefix in valid_versions:
                                needs_update = False
                                stats['hhstj_prefix_preserved'] += 1
                                print(f"✅ Preserved HHStJ shorthand '{stored_hhstj_prefix}' for {member.display_name}")

                        if needs_update:
                            # Actual rank change detected, update database
                            if not dry_run:
                                async with db.pool.acquire() as conn:
                                    await conn.execute(
                                        'UPDATE callsigns SET hhstj_prefix = $1 WHERE discord_user_id = $2',
                                        correct_hhstj_prefix or '', member.id
                                    )

                            stats['rank_changes'].append({
                                'member': member,
                                'type': 'HHStJ',
                                'old_rank': stored_hhstj_prefix,
                                'new_rank': correct_hhstj_prefix
                            })

                            record['hhstj_prefix'] = correct_hhstj_prefix
                            current_hhstj_prefix = correct_hhstj_prefix
                            stats['rank_updates'] += 1
                        else:
                            # Keep the stored shorthand
                            current_hhstj_prefix = stored_hhstj_prefix

                    # UPDATE NICKNAME IF REQUESTED
                    if update_nicknames:
                        # Calculate expected nickname
                        if record['callsign'] == "Not Assigned":
                            if current_fenz_prefix == "RFF":
                                expected_nickname = "RFF"
                            else:
                                nickname_parts = []
                                if current_fenz_prefix:
                                    nickname_parts.append(f"{current_fenz_prefix}-Not Assigned")
                                if current_hhstj_prefix and "-" not in current_hhstj_prefix:
                                    nickname_parts.append(current_hhstj_prefix)
                                if record['roblox_username']:
                                    nickname_parts.append(record['roblox_username'])
                                expected_nickname = " | ".join(nickname_parts) if nickname_parts else record[
                                    'roblox_username']
                        else:
                            expected_nickname = format_nickname(
                                current_fenz_prefix,
                                record['callsign'],
                                current_hhstj_prefix,
                                record['roblox_username']
                            )

                        current_nick = member.nick or member.name

                        if current_nick != expected_nickname:
                            if dry_run:
                                # Just preview
                                stats['nickname_changes'].append({
                                    'member': member,
                                    'old': current_nick,
                                    'new': expected_nickname
                                })
                                stats['nickname_updates'] += 1
                            else:
                                # Apply change
                                try:
                                    await member.edit(nick=expected_nickname)
                                    stats['nickname_changes'].append({
                                        'member': member,
                                        'old': current_nick,
                                        'new': expected_nickname
                                    })
                                    stats['nickname_updates'] += 1
                                except discord.Forbidden:
                                    stats['failed_updates'].append(
                                        f"{record.get('discord_username', 'Unknown')}: Missing permissions"
                                    )

                except Exception as e:
                    stats['failed_updates'].append(
                        f"{record.get('discord_username', 'Unknown')}: {str(e)}"
                    )

            # UPDATE GOOGLE SHEETS IF REQUESTED
            if update_sheets and not dry_run:
                callsign_data = []
                for record in db_callsigns:
                    record = dict(record)
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
                        'qualifications': sheets_manager.determine_qualifications(
                            member.roles, is_command_rank
                        ) if member else None
                    })

                # Sort by rank hierarchy
                callsign_data.sort(key=lambda x: get_rank_sort_key(x['fenz_prefix'], x['hhstj_prefix']))

                # Update Google Sheets
                success = await sheets_manager.batch_update_callsigns(callsign_data)

                if not success:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> Failed to sync to Google Sheets.",
                        ephemeral=True
                    )
                    return

            # BUILD RESPONSE WITH LARGE EMBED SUPPORT
            embeds = []

            # Summary embed
            summary_embed = discord.Embed(
                title="Sync Results" + (" (DRY RUN - Preview Only)" if dry_run else ""),
                color=discord.Color.blue() if dry_run else discord.Color.green()
            )

            summary_text = f"**Total Callsigns:** {stats['total_callsigns']}\n"
            if update_sheets:
                summary_text += f"**Synced to Sheets:** {stats['total_callsigns']}\n"
            if update_nicknames:
                summary_text += f"**Nickname Updates:** {stats['nickname_updates']}\n"
            summary_text += f"**Rank Changes:** {stats['rank_updates']}\n"
            if stats['added_from_sheets'] > 0:
                summary_text += f"**Added from Sheets:** {stats['added_from_sheets']}\n"
            if stats['hhstj_prefix_preserved'] > 0:
                summary_text += f"**HHStJ Prefixes Preserved:** {stats['hhstj_prefix_preserved']}\n"
            if stats['failed_updates']:
                summary_text += f"**Failed Updates:** {len(stats['failed_updates'])}\n"

            summary_embed.description = summary_text

            if dry_run:
                summary_embed.set_footer(text="This was a dry run. Run without dry_run=True to apply changes.")

            embeds.append(summary_embed)

            # Nickname changes embed(s)
            if stats['nickname_changes']:
                for i in range(0, len(stats['nickname_changes']), 10):
                    chunk = stats['nickname_changes'][i:i + 10]

                    embed = discord.Embed(
                        title=f"Nickname Updates ({i + 1}-{min(i + 10, len(stats['nickname_changes']))} of {len(stats['nickname_changes'])})",
                        color=discord.Color.green()
                    )

                    for change in chunk:
                        old_nick = change['old'][:100] if len(change['old']) > 100 else change['old']
                        new_nick = change['new'][:100] if len(change['new']) > 100 else change['new']

                        embed.add_field(
                            name=f"{change['member'].display_name[:50]}",
                            value=f"{change['member'].mention}\n**Before:** `{old_nick}`\n**After:** `{new_nick}`",
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    embeds.append(embed)

            # Rank changes embed(s)
            if stats['rank_changes']:
                for i in range(0, len(stats['rank_changes']), 3):
                    chunk = stats['rank_changes'][i:i + 3]

                    embed = discord.Embed(
                        title=f"Rank Changes ({i + 1}-{min(i + 3, len(stats['rank_changes']))} of {len(stats['rank_changes'])})",
                        color=discord.Color.gold()
                    )

                    for change in chunk:
                        rank_type = change.get('type', 'Unknown')  # ✅ This is correct
                        old_rank = change.get('old_rank', 'Unknown')
                        new_rank = change.get('new_rank', 'Unknown')

                        embed.add_field(
                            name=f"{change['member'].display_name[:50]}",
                            value=f"**Type:** {rank_type}\n**{old_rank}** → **{new_rank}**",
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    embeds.append(embed)

            # Callsigns reset embed(s)
            if stats['callsigns_reset']:
                for i in range(0, len(stats['callsigns_reset']), 10):
                    chunk = stats['callsigns_reset'][i:i + 10]

                    embed = discord.Embed(
                        title=f"Callsigns Reset ({i + 1}-{min(i + 10, len(stats['callsigns_reset']))} of {len(stats['callsigns_reset'])})",
                        description="Reset to Not Assigned due to rank changes",
                        color=discord.Color.orange()
                    )

                    for change in chunk:
                        # ✅ Correct: callsigns_reset has different structure
                        value_parts = []
                        if change.get('old_callsign'):
                            value_parts.append(f"**Old Callsign:** {change['old_callsign']}")
                        if change.get('new_prefix'):
                            value_parts.append(f"**New Prefix:** {change['new_prefix']}")
                        if change.get('reason'):
                            value_parts.append(f"**Reason:** {change['reason']}")

                        embed.add_field(
                            name=f"{change['member'].display_name[:50]}",
                            value="\n".join(value_parts) if value_parts else "Rank changed",
                            inline=False
                        )

                    if get_embed_size(embed) > 5500:
                        print(f"⚠️ Embed too large, skipping")
                        continue

                    embeds.append(embed)

            # Failed updates embed (if any)
            if stats['failed_updates']:
                embed = discord.Embed(
                    title=f"<:Warn:1437771973970104471> Failed Updates ({len(stats['failed_updates'])})",
                    color=discord.Color.red()
                )

                failed_text = "\n".join(stats['failed_updates'][:10])
                if len(stats['failed_updates']) > 10:
                    failed_text += f"\n... and {len(stats['failed_updates']) - 10} more"

                embed.description = failed_text
                embeds.append(embed)

            # Send response
            await interaction.delete_original_response()

            if len(embeds) == 1:
                await interaction.followup.send(embed=embeds[0], ephemeral=True)
            else:
                # Use pagination for multiple embeds
                view = PaginatedEmbedView(embeds)
                await interaction.followup.send(embed=embeds[0], view=view, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error during sync: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="assign", description="Assign a callsign to a user")
    @app_commands.check(lambda interaction: any(role.id in UPPER_LEAD for role in interaction.user.roles))
    @app_commands.describe(
        user="The user to assign the callsign to",
        callsign="The callsign number (1-3 digits)",
        prefix="Whether to use rank affix (FENZ Supervisor+ only - refers to whether you want to be just 'DNC' or 'DNC-1')"
    )
    async def assign_callsign(self, interaction: discord.Interaction, user: discord.Member, callsign: str,
                              prefix: bool = True):

        # ✅ FIRST: Always defer or respond immediately
        await interaction.response.defer(ephemeral=True)

        if db.pool is None:
            await interaction.followup.send(
                "❌ Database not connected. Please try again.",
                ephemeral=True
            )
            return

        try:
            # Validate callsign format first
            is_high_command = any(role.id in HIGH_COMMAND_RANKS for role in user.roles)

            if callsign.lower() == "blank":
                if not is_high_command:
                    await interaction.followup.send(
                        "❌ Only High Command ranks can use 'blank' as a callsign.",
                        ephemeral=True
                    )
                    return
                callsign = "BLANK"
            else:
                if not callsign.isdigit() or len(callsign) > 3 or len(callsign) < 1:
                    await interaction.followup.send(
                        "❌ Callsign must be a 1-3 digit number (e.g., 1, 42, 001) or 'blank' for High Command",
                        ephemeral=True
                    )
                    return
                callsign = normalize_callsign(callsign)

            # ✅ NOW fetch all required data
            bloxlink_api = BloxlinkAPI()
            roblox_username, roblox_id, status = await bloxlink_api.get_bloxlink_data(user.id, interaction.guild.id)

            if status != 'success' or not roblox_id:
                await interaction.followup.send(
                    f"❌ Could not find Roblox account for {user.mention}. "
                    f"Please verify their Bloxlink connection.\n"
                    f"Status: {status}",
                    ephemeral=True
                )
                return

            roblox_id = str(roblox_id)

            if not roblox_username:
                await interaction.followup.send(
                    "❌ Failed to fetch Roblox username.",
                    ephemeral=True
                )
                return

            # Get FENZ prefix
            fenz_prefix = None
            fenz_rank_name = None
            for role_id, (rank_name, prefix_abbr) in FENZ_RANK_MAP.items():
                if any(role.id == role_id for role in user.roles):
                    fenz_prefix = prefix_abbr
                    fenz_rank_name = rank_name
                    break

            if not fenz_prefix:
                await interaction.followup.send(
                    f"❌ {user.mention} does not have a valid FENZ rank role.",
                    ephemeral=True
                )
                return

            # Check if callsign exists
            if callsign not in ["BLANK", "Not Assigned"]:
                existing = await check_callsign_exists(callsign, fenz_prefix)
                if existing and existing['discord_user_id'] != user.id:
                    error_message = format_duplicate_callsign_message(callsign, existing)
                    await interaction.followup.send(error_message, ephemeral=True)
                    return

            # Get HHStJ prefix
            hhstj_prefix = get_hhstj_prefix_from_roles(user.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in user.roles)

            # ✅ NOW check if HHStJ version selection is needed (all variables are defined)
            if is_hhstj_high_command and hhstj_prefix and '-' in hhstj_prefix:
                hhstj_versions = get_hhstj_shortened_versions(hhstj_prefix)
                version_tests = test_hhstj_versions_fit(
                    fenz_prefix, callsign, hhstj_versions,
                    roblox_username, is_high_command, is_hhstj_high_command
                )
                valid_versions = [v['version'] for v in version_tests if v['fits']]

                if len(valid_versions) > 1:
                    # Show modal for version selection
                    modal = HHStJVersionModal(
                        self, interaction, user, callsign,
                        fenz_prefix, valid_versions, roblox_id, roblox_username,
                        is_self_request=False
                    )
                    await interaction.followup.send(
                        "🔄 HHStJ High Command detected - showing version selector...",
                        ephemeral=True
                    )
                    # Send modal as a separate interaction
                    # Note: Modals can't be sent via followup, need fresh interaction
                    # This is a Discord limitation - we'll handle it differently
                    # For now, auto-select shortest version
                    hhstj_prefix = valid_versions[-1]  # Shortest version
                    await interaction.followup.send(
                        f"ℹ️ Auto-selected HHStJ version: **{hhstj_prefix}** (shortest that fits)\n"
                        f"User can request a different version later.",
                        ephemeral=True
                    )
                elif len(valid_versions) == 1:
                    hhstj_prefix = valid_versions[0]
                else:
                    hhstj_prefix = hhstj_versions[-1]

            # Handle prefix selection for high command
            FENZ_LEADERSHIP_ROLE_ID = 1285474077556998196

            if is_high_command and not prefix:
                if not any(role.id == FENZ_LEADERSHIP_ROLE_ID for role in interaction.user.roles):
                    await interaction.followup.send(
                        "❌ Only users with the Owner role can assign callsigns without prefix!\n"
                        "The prefix will be included automatically.",
                        ephemeral=True
                    )
                    prefix = True

            if not prefix:
                final_fenz_prefix = fenz_prefix
                final_callsign = "BLANK"
            else:
                final_fenz_prefix = fenz_prefix
                final_callsign = callsign

            # Add to database
            await add_callsign_to_database(
                final_callsign, user.id, str(user), roblox_id, roblox_username,
                final_fenz_prefix, hhstj_prefix or "",
                interaction.user.id,
                interaction.user.display_name,
                is_high_command,
                is_hhstj_high_command
            )

            # Format and update nickname
            if callsign == "BLANK":
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
                    roblox_username
                )

            try:
                await user.edit(nick=new_nickname)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"Callsign assigned but couldn't update nickname (lacking permissions). "
                    f"Please manually set to: `{new_nickname}`",
                    ephemeral=True
                )
                return

            # Format response
            if callsign == "BLANK":
                callsign_display = "**BLANK** (no callsign number)"
            elif final_fenz_prefix:
                callsign_display = f"**{final_fenz_prefix}-{final_callsign}**"
            else:
                callsign_display = f"**{final_callsign}** (no prefix)"

            await interaction.followup.send(
                f"✅ Assigned callsign {callsign_display} to {user.mention}\n"
                f"Nickname updated to: `{new_nickname}`\n",
                ephemeral=True,
            )

            # Log to channel
            log_channel = self.bot.get_channel(CALLSIGN_REQUEST_LOG_CHANNEL_ID)
            if log_channel:
                log_embed = discord.Embed(
                    title="Callsign Assigned (Manual)",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                log_embed.add_field(name="User", value=f"{user.mention}", inline=True)
                log_embed.add_field(name="Callsign", value=f"`{callsign_display}`", inline=True)
                log_embed.add_field(name="Assigned By", value=interaction.user.mention, inline=True)
                log_embed.add_field(name="Nickname", value=f"`{new_nickname}`", inline=False)
                log_embed.set_footer(text=f"User ID: {user.id}")
                await log_channel.send(embed=log_embed)

        except Exception as e:
            await interaction.followup.send(f"❌ Error assigning callsign: {str(e)}")
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="lookup", description="Look up a callsign")
    @app_commands.describe(user="The user to lookup the callsign for")
    @app_commands.describe(callsign="The callsign to look up")
    @app_commands.check(lambda interaction: any(role.id in LEAD_ROLES for role in interaction.user.roles))
    async def lookup_callsign(self, interaction: discord.Interaction, callsign: str = None,
                              user: discord.Member = None):

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Looking Up Callsign", ephemeral=True)

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

                callsign = normalize_callsign(callsign)

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
                # <:Accepted:1426930333789585509> FIX: Format title based on callsign type
                if result['callsign'] == "Not Assigned":
                    title = f"Callsign: Not Assigned (Not Assigned)"
                    callsign_status = "<:Warn:1437771973970104471> Awaiting Assignment"
                elif result['callsign'] == "BLANK":
                    title = f"Callsign: BLANK (No number)"
                    callsign_status = "<:Accepted:1426930333789585509> High Command (No Callsign)"
                elif result['fenz_prefix']:
                    title = f"Callsign: {result['fenz_prefix']}-{result['callsign']}"
                    callsign_status = "<:Accepted:1426930333789585509> Active"
                else:
                    title = f"Callsign: {result['callsign']}"
                    callsign_status = "<:Accepted:1426930333789585509> Active"

                embed = discord.Embed(
                    title=title,
                    color=discord.Color.orange() if result['callsign'] == "Not Assigned" else discord.Color.blue()
                )

                embed.add_field(
                    name="Status",
                    value=callsign_status,
                    inline=True
                )

                embed.add_field(
                    name="Discord User",
                    value=f"<@{result['discord_user_id']}>",
                    inline=False
                )

                embed.add_field(
                    name="Roblox User",
                    value=f"[{result['roblox_username']}](https://www.roblox.com/users/{result['roblox_user_id']}/profile)",
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

                await interaction.delete_original_response()
                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error looking up callsign: {str(e)}")
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="remove", description="Remove a callsign from a user")
    @app_commands.check(lambda interaction: any(role.id in UPPER_LEAD for role in interaction.user.roles))
    @app_commands.describe(user="The user whose callsign should be removed")
    async def remove_callsign(self, interaction: discord.Interaction, user: discord.Member):
        """Remove a callsign from a user and reset their nickname"""

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Removing Callsign",
                                                ephemeral=True)

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
                bloxlink_api = BloxlinkAPI()
                roblox_username, roblox_id, status = await bloxlink_api.get_bloxlink_data(user.id, interaction.guild.id)

                if status != 'success' or not roblox_id:
                    await interaction.followup.send(
                        f"<:Denied:1426930694633816248> Could not find Roblox account for {user.mention}. "
                        f"Please verify their Bloxlink connection.\n"
                        f"Status: {status}",
                        ephemeral=True
                    )
                    return

                # Convert to string for database
                roblox_id = str(roblox_id)

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

            # Log to designated channel
            log_channel = self.bot.get_channel(CALLSIGN_REQUEST_LOG_CHANNEL_ID)
            if log_channel:
                log_embed = discord.Embed(
                    title="Callsign Removed",
                    color=discord.Color.orange(),
                    timestamp=datetime.utcnow()
                )
                log_embed.add_field(name="User", value=f"{user.mention}\n`{user.display_name}`", inline=True)
                log_embed.add_field(name="Removed Callsign", value=f"`{callsign_display}`", inline=True)
                log_embed.add_field(name="Removed By", value=interaction.user.mention, inline=True)
                log_embed.set_footer(text=f"User ID: {user.id}")

                await log_channel.send(embed=log_embed)

            await interaction.delete_original_response()
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

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Processing Callsign Request", ephemeral=True)

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

            callsign = normalize_callsign(callsign)

            # Get user's Roblox info
            bloxlink_api = BloxlinkAPI()
            roblox_username, roblox_id, status = await bloxlink_api.get_bloxlink_data(
                interaction.user.id,
                interaction.guild.id
            )

            if status != 'success' or not roblox_id:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Could not find your Roblox account. "
                    "Please verify your Bloxlink connection with </verify:1114974748624027711> and try again.",
                    ephemeral=True
                )
                return

            # Convert to string for database
            roblox_id = str(roblox_id)

            if not roblox_username or roblox_username == 'Unknown':
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Failed to fetch Roblox username or your account is invalid.",
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

            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in interaction.user.roles)

            # Check if HHStJ high command with multiple version options
            if is_hhstj_high_command and hhstj_prefix and '-' in hhstj_prefix:
                # Get all shortened versions
                hhstj_versions = get_hhstj_shortened_versions(hhstj_prefix)

                # Test which versions fit
                is_fenz_hc = any(role.id in HIGH_COMMAND_RANKS for role in interaction.user.roles)
                version_tests = test_hhstj_versions_fit(
                    fenz_prefix, callsign, hhstj_versions,
                    roblox_username, is_fenz_hc, is_hhstj_high_command
                )

                # Filter to only valid versions
                valid_versions = [v['version'] for v in version_tests if v['fits']]

                if len(valid_versions) > 1:
                    # Show modal to the REQUESTER to choose their version
                    modal = HHStJVersionModal(
                        self, interaction, interaction.user, callsign,
                        fenz_prefix, valid_versions, roblox_id, roblox_username,
                        is_self_request=True
                    )
                    await interaction.response.send_modal(modal)
                    return  # Exit here - modal handles the rest
                elif len(valid_versions) == 1:
                    # Only one version fits, use it automatically
                    hhstj_prefix = valid_versions[0]
                else:
                    # No versions fit (shouldn't happen, but fallback)
                    hhstj_prefix = hhstj_versions[-1]  # Use shortest version

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
                fenz_prefix, callsign, hhstj_prefix, roblox_username)

            success, final_nick = await safe_edit_nickname(interaction.user, new_nickname)
            if not success:
                print(f"⚠️ Failed to set nickname for {member.id}")

        except discord.HTTPException as e:
            if e.code == 50035:  # Invalid Form Body
                print(f"⚠️ Nickname too long for {member.id}: '{new_nickname}' ({len(new_nickname)} chars)")
                # Try again with just roblox username
                success, final_nick = await safe_edit_nickname(interaction.user, new_nickname)
                if not success:
                    print(f"⚠️ Failed to set nickname for {member.id}")
            else:
                raise

            try:
                await interaction.user.edit(nick=new_nickname)
            except discord.HTTPException as e:
                if e.code == 50035:  # Invalid Form Body
                    print(
                        f"<:Warn:1437771973970104471> Nickname too long for {interaction.user.id}: '{new_nickname}' ({len(new_nickname)} chars)")
                    # Try with just roblox username
                    try:
                        await interaction.user.edit(nick=roblox_username[:32])
                    except:
                        pass
                else:
                    raise
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

            success_embed.set_footer(text=f"Approved automatically • {interaction.user.display_name}")
            success_embed.timestamp = datetime.utcnow()

            await interaction.delete_original_response()
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error processing request: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="audit",
                            description="Check all users to identify missing or inconsistent data in the Bot's Database")
    @app_commands.describe(
        show_incomplete="Show users with incomplete data",
        show_missing="Show users not in database at all",
        show_all_members="Show ALL FENZ members (not just those missing callsigns)"
    )
    async def audit_callsigns(
            self,
            interaction: discord.Interaction,
            show_incomplete: bool = True,
            show_missing: bool = True,
            show_all_members: bool = False
    ):
        """Owner-only: Audit callsign database and find issues"""

        OWNER_ID = 678475709257089057

        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Auditing Callsigns",
                                                ephemeral=True)

        try:
            # Get ALL callsigns from database
            async with db.pool.acquire() as conn:
                db_callsigns = await conn.fetch('SELECT * FROM callsigns')

            db_user_ids = {record['discord_user_id'] for record in db_callsigns}

            # Get ALL users with FENZ roles
            all_fenz_members = []
            for role_id in FENZ_RANK_MAP.keys():
                role = interaction.guild.get_role(role_id)
                if role:
                    all_fenz_members.extend(role.members)

            # Remove duplicates while preserving order
            seen = set()
            fenz_members = []
            for member in all_fenz_members:
                if member.id not in seen:
                    seen.add(member.id)
                    fenz_members.append(member)

            # Calculate missing and incomplete data
            missing_from_db = []
            incomplete_data = []

            for member in fenz_members:
                if member.id not in db_user_ids:
                    # NOT in database - this is a missing user
                    bloxlink_api = BloxlinkAPI()
                    roblox_username, roblox_id, status = await bloxlink_api.get_bloxlink_data(
                        member.id,
                        interaction.guild.id
                    )

                    # Convert ID to string if successful
                    if status == 'success' and roblox_id:
                        roblox_id = str(roblox_id)
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
                        'has_bloxlink': bool(roblox_id and roblox_username)
                    })
                else:
                    # IN database - check for incomplete data
                    user_record = next((r for r in db_callsigns if r['discord_user_id'] == member.id), None)
                    if user_record:
                        issues = []
                        if user_record['callsign'] in ['Not Assigned', 'BLANK', None]:
                            issues.append("No callsign number")
                        if not user_record['roblox_username']:
                            issues.append("Missing Roblox username")
                        if not user_record['roblox_user_id']:
                            issues.append("Missing Roblox ID")
                        if not user_record['discord_username']:
                            issues.append("Missing Discord username")

                        if issues:
                            incomplete_data.append({
                                'member': member,
                                'record': dict(user_record),
                                'issues': issues
                            })

            # Build response embeds
            embeds = []

            # Summary embed FIRST
            summary_embed = discord.Embed(
                title="Callsign Audit Summary",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
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
                name="<:Denied:1426930694633816248> Missing from DB",
                value=str(len(missing_from_db)),
                inline=True
            )
            summary_embed.add_field(
                name="<:Warn:1437771973970104471> Incomplete Data",
                value=str(len(incomplete_data)),
                inline=True
            )

            if missing_from_db or incomplete_data:
                summary_embed.add_field(
                    name="Next Steps",
                    value="• Use `/callsign bulk-assign` to fix missing users\n"
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

            embeds.append(summary_embed)

            # Helper function to split items into embeds safely
            def split_into_embeds(items, title_prefix, color, formatter_func, items_per_embed=8):
                """Split items into multiple embeds to avoid character limits"""
                result_embeds = []

                for i in range(0, len(items), items_per_embed):
                    chunk = items[i:i + items_per_embed]

                    embed = discord.Embed(
                        title=f"{title_prefix} ({i + 1}-{min(i + items_per_embed, len(items))} of {len(items)})",
                        color=color,
                        timestamp=datetime.utcnow()
                    )

                    # Calculate total embed size as we add fields
                    total_size = len(embed.title) + len(embed.description or "")

                    for item in chunk:
                        field_data = formatter_func(item)
                        field_size = len(field_data['name']) + len(field_data['value'])

                        # Check if adding this field would exceed limits
                        # Discord limits: 6000 chars total, 25 fields max, 1024 chars per field value
                        if (total_size + field_size > 5500 or  # Leave buffer
                                len(embed.fields) >= 25 or
                                len(field_data['value']) > 1024):
                            # Start a new embed
                            if embed.fields:  # Only add if we have fields
                                result_embeds.append(embed)

                            # Create new embed for remaining items
                            embed = discord.Embed(
                                title=f"{title_prefix} (continued)",
                                color=color,
                                timestamp=datetime.utcnow()
                            )
                            total_size = len(embed.title)

                        # Add the field
                        embed.add_field(**field_data)
                        total_size += field_size

                    # Add the last embed if it has fields
                    if embed.fields:
                        result_embeds.append(embed)

                return result_embeds

            # Generate embeds for missing users
            if show_missing and missing_from_db:
                def format_missing(item):
                    value_parts = []
                    value_parts.append(f"**Rank:** {item['fenz_rank'] or 'Unknown'}")

                    if item['roblox_username']:
                        value_parts.append(f"**Roblox:** {item['roblox_username']}")
                    else:
                        if not item['has_bloxlink']:
                            value_parts.append("**Roblox:** <:Denied:1426930694633816248> Not linked via Bloxlink")
                        else:
                            value_parts.append("**Roblox:** <:Denied:1426930694633816248> Invalid/deleted account")

                    value_parts.append(f"**User ID:** `{item['member'].id}`")

                    return {
                        'name': f"{item['member'].mention} ({item['member'].display_name})",
                        'value': "\n".join(value_parts),
                        'inline': False
                    }

                missing_embeds = split_into_embeds(
                    missing_from_db,
                    "<:Denied:1426930694633816248> Missing from Database",
                    discord.Color.red(),
                    format_missing,
                    items_per_embed=8  # Conservative to stay under limits
                )
                embeds.extend(missing_embeds)

            # Generate embeds for incomplete data
            if show_incomplete and incomplete_data:
                def format_incomplete(item):
                    value_parts = []

                    # Show issues
                    issues_text = "\n".join([f"• {issue}" for issue in item['issues']])
                    value_parts.append(f"**Issues:**\n{issues_text}")

                    # Show current callsign if exists
                    if item['record'].get('callsign'):
                        callsign_display = item['record']['callsign']
                        if item['record'].get('fenz_prefix'):
                            callsign_display = f"{item['record']['fenz_prefix']}-{callsign_display}"
                        value_parts.append(f"**Current Callsign:** {callsign_display}")

                    value_parts.append(f"**User ID:** `{item['member'].id}`")

                    return {
                        'name': f"{item['member'].mention} ({item['member'].display_name})",
                        'value': "\n".join(value_parts),
                        'inline': False
                    }

                incomplete_embeds = split_into_embeds(
                    incomplete_data,
                    "<:Warn:1437771973970104471> Incomplete Data",
                    discord.Color.orange(),
                    format_incomplete,
                    items_per_embed=8
                )
                embeds.extend(incomplete_embeds)

            # Generate embeds for all members (if requested)
            if show_all_members and fenz_members:
                def format_member(member):
                    # Get their callsign if they have one
                    user_record = next((r for r in db_callsigns if r['discord_user_id'] == member.id), None)

                    if user_record:
                        callsign = user_record.get('callsign', 'Unknown')
                        prefix = user_record.get('fenz_prefix', '')

                        if callsign == "Not Assigned":
                            value = "**Callsign:** <:Warn:1437771973970104471> Not Assigned"
                        elif callsign == "BLANK":
                            value = f"**Callsign:** {prefix} (No number)"
                        elif prefix:
                            value = f"**Callsign:** {prefix}-{callsign}"
                        else:
                            value = f"**Callsign:** {callsign}"
                    else:
                        value = "**Callsign:** <:Denied:1426930694633816248> Not in database"

                    return {
                        'name': f"{member.mention}",
                        'value': value,
                        'inline': True  # Inline for more compact display
                    }

                member_embeds = split_into_embeds(
                    fenz_members,
                    "All FENZ Members",
                    discord.Color.blue(),
                    format_member,
                    items_per_embed=15  # More per embed since inline fields are smaller
                )
                embeds.extend(member_embeds)

            # Add footer with page numbers
            for i, embed in enumerate(embeds, 1):
                current_footer = embed.footer.text if embed.footer and embed.footer.text else ""
                if current_footer:
                    embed.set_footer(text=f"Page {i}/{len(embeds)} • {current_footer}")
                else:
                    embed.set_footer(text=f"Page {i}/{len(embeds)}")

            await interaction.delete_original_response()

            # Send embeds
            if len(embeds) == 1:
                await interaction.followup.send(embed=embeds[0], ephemeral=True)
            else:
                # Send with pagination view
                view = PaginatedEmbedView(embeds)
                await interaction.followup.send(embed=embeds[0], view=view, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error during audit: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    async def detect_database_mismatches(self, guild: discord.Guild, progress_callback=None,
                                         bloxlink_cache: dict = None):
        """Detect mismatches with optimized bulk Bloxlink checking"""

        if bloxlink_cache is None:
            bloxlink_cache = {}

        async with db.pool.acquire() as conn:
            db_callsigns = await conn.fetch('SELECT * FROM callsigns')

        # Collect all uncached IDs upfront
        uncached_ids = [
            record['discord_user_id']
            for record in db_callsigns
            if record['discord_user_id'] not in bloxlink_cache
               and guild.get_member(record['discord_user_id'])
        ]

        # Bulk fetch if needed
        if uncached_ids:
            bulk_results = await self.bloxlink_api.bulk_check_bloxlink(
                uncached_ids,
                guild.id,
                progress_callback
            )
            if bulk_results:
                for discord_id, result in bulk_results.items():
                    bloxlink_cache[discord_id] = (
                        result['roblox_username'],
                        result['roblox_user_id'],
                        result['status']
                    )

        # Now process all records using cache
        mismatches = {
            'discord_username_mismatch': [],
            'roblox_username_mismatch': [],
            'roblox_id_mismatch': [],
            'missing_discord_username': [],
            'missing_roblox_id': [],
        }

        for index, record in enumerate(db_callsigns, 1):
            member = guild.get_member(record['discord_user_id'])
            if not member:
                continue

            if progress_callback:
                await progress_callback(index, len(db_callsigns))

            # Check Discord username
            current_discord_name = str(member)
            if not record.get('discord_username'):
                mismatches['missing_discord_username'].append({
                    'member': member,
                    'record': dict(record)
                })
            elif current_discord_name != record['discord_username']:
                mismatches['discord_username_mismatch'].append({
                    'member': member,
                    'old': record['discord_username'],
                    'new': current_discord_name,
                    'record': dict(record)
                })

            # Check Roblox data using cache
            if member.id in bloxlink_cache:
                roblox_username, roblox_id, status = bloxlink_cache[member.id]

                if status == 'success' and roblox_id:
                    current_roblox_id = str(roblox_id)
                    stored_roblox_id = record.get('roblox_user_id')

                    if not stored_roblox_id:
                        mismatches['missing_roblox_id'].append({
                            'member': member,
                            'current_id': current_roblox_id,
                            'current_username': roblox_username,
                            'record': dict(record)
                        })
                    elif current_roblox_id != stored_roblox_id:
                        mismatches['roblox_id_mismatch'].append({
                            'member': member,
                            'old_id': stored_roblox_id,
                            'new_id': current_roblox_id,
                            'current_username': roblox_username,
                            'record': dict(record)
                        })

                    # Check username mismatch
                    if roblox_username and record.get('roblox_username'):
                        if roblox_username != record['roblox_username']:
                            mismatches['roblox_username_mismatch'].append({
                                'member': member,
                                'old': record['roblox_username'],
                                'new': roblox_username,
                                'record': dict(record)
                            })

        return mismatches

    @callsign_group.command(name="cachestats", description="View Bloxlink API cache statistics")
    async def cache_stats(self, interaction: discord.Interaction):
        """Show detailed cache statistics and API usage"""

        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # ✅ MODIFIED: Use cog-level BloxlinkAPI instance
            stats = await self.bloxlink_api.get_cache_stats()

            embed = discord.Embed(
                title="Bloxlink API Cache Statistics",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )

            # Cache efficiency
            total_cached = stats['total_cached']
            valid_entries = stats['valid_entries']
            expired_entries = stats['expired_entries']

            if total_cached > 0:
                efficiency = (valid_entries / total_cached) * 100
            else:
                efficiency = 0

            embed.add_field(
                name="Cache Status",
                value=f"**Total Entries:** {total_cached}\n"
                      f"**Valid (Fresh):** {valid_entries}\n"
                      f"**Expired:** {expired_entries}\n"
                      f"**Efficiency:** {efficiency:.1f}%",
                inline=True
            )

            # API quota tracking
            api_calls = stats['api_calls_made']
            quota_remaining = stats['quota_remaining']

            # Format quota reset time if available
            quota_reset_text = ""
            if BloxlinkAPI._quota_exhausted and BloxlinkAPI._quota_reset_time:
                reset_timestamp = int(BloxlinkAPI._quota_reset_time)
                quota_reset_text = f"\n**Resets:** <t:{reset_timestamp}:R>"

            # ✅ NEW: Show last sync time and schedule
            if self.last_bloxlink_sync:
                time_since_sync = asyncio.get_event_loop().time() - self.last_bloxlink_sync
                hours_since = int(time_since_sync / 3600)
                hours_until_next = int((self.bloxlink_sync_interval - time_since_sync) / 3600)

                sync_status = "🟢 Fresh" if hours_since < 12 else "🟡 Aging" if hours_since < 20 else "🟠 Due Soon"

                embed.add_field(
                    name="<a:Load:1430912797469970444> Sync Schedule",
                    value=f"**Status:** {sync_status}\n"
                          f"**Last Full Sync:** {hours_since}h ago\n"
                          f"**Next Full Sync:** in {hours_until_next}h\n"
                          f"**Hourly Syncs:** Use cached data\n"
                          f"**Full Refresh:** Every 24 hours",
                    inline=True
                )
            else:
                embed.add_field(
                    name="<a:Load:1430912797469970444> Sync Schedule",
                    value="**Status:** No sync yet\n"
                          f"**First Sync:** Will occur on next hourly run\n"
                          f"**Frequency:** Every 24 hours",
                    inline=True
                )

            if BloxlinkAPI._quota_exhausted:
                quota_status = "<:No:1437788507111428228> EXHAUSTED - Waiting for reset"
            else:
                quota_status = "🟢 Healthy" if quota_remaining > 250 else "🟡 Moderate" if quota_remaining > 100 else "🔴 Critical"

            embed.add_field(
                name="API Usage",
                value=f"**Calls Made:** {api_calls}\n"
                      f"**Quota Remaining:** {quota_remaining}/500\n"
                      f"**Status:** {quota_status}{quota_reset_text}",
                inline=True
            )

            # Cache settings
            embed.add_field(
                name="Cache Settings",
                value=f"**Expiration:** 24 hours\n"
                      f"**Full Refresh:** Every 1 hour\n"
                      f"**Rate Limit:** 50 req/min",
                inline=True
            )

            if BloxlinkAPI._quota_exhausted:
                embed.add_field(
                    name="<:No:1437788507111428228> Quota Exhausted",
                    value="• All API calls are blocked until reset\n"
                          "• Bot will use cached data only\n"
                          "• Hourly syncs will continue with cache\n"
                          "• Wait for automatic 24-hour reset",
                    inline=False
                )

            # Recommendations
            if quota_remaining < 100:
                embed.add_field(
                    name="<:Warn:1437771973970104471>️ Recommendations",
                    value="• Quota running low - cache will prevent overuse\n"
                          "• Avoid manual bulk operations\n"
                          "• Quota resets in ~24 hours",
                    inline=False
                )
            else:
                embed.add_field(
                    name="<:Accepted:1426930333789585509> Status",
                    value=f"Cache is healthy! Efficiency at {efficiency:.1f}%\n"
                          f"Auto-sync runs every hour using cached data.",
                    inline=False
                )

            # Check if quota is already exhausted before starting
            if not await self.bloxlink_api._check_daily_quota():
                print("🚫 Cannot refresh cache - API quota exhausted")
                print("   Will use existing cache and retry in next cycle")
                return

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(
                f"❌ Error fetching cache stats: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="bulk-assign", description="Assign callsigns to all unassigned users")
    @app_commands.describe(database_scan="Check and fix database mismatches before assigning (default: False)")
    async def bulk_assign(self, interaction: discord.Interaction, database_scan: bool = False):
        """Owner-only: Interactive bulk callsign assignment"""

        OWNER_ID = 678475709257089057

        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        try:
            bloxlink_cache = {}

            # ✅ FIX: Always run database scan when database_scan=True
            if database_scan:
                # Step 1: Detect database mismatches with progress
                status_embed = discord.Embed(
                    title="<a:Load:1430912797469970444> Scanning for database issues...",
                    description="Preparing to check database...",
                    color=discord.Color.blue()
                )
                await interaction.edit_original_response(embed=status_embed)

                # Create progress callback for database scan
                last_db_update = 0

                async def db_progress_callback(current, total):
                    nonlocal last_db_update
                    current_time = asyncio.get_event_loop().time()

                    # Update every 10 users or every 2 seconds
                    if current % 10 == 0 or (current_time - last_db_update) >= 2 or current == total:
                        progress_percent = int((current / total) * 100)
                        progress_bar = "█" * (progress_percent // 5) + "░" * (20 - (progress_percent // 5))

                        progress_embed = discord.Embed(
                            title="<a:Load:1430912797469970444> Scanning Database",
                            description=f"**Progress:** {current}/{total} ({progress_percent}%)\n"
                                        f"`{progress_bar}`\n\n"
                                        f"Checking for mismatches...",
                            color=discord.Color.blue()
                        )
                        progress_embed.set_footer(text=f"Checking user {current} of {total}")

                        try:
                            await interaction.edit_original_response(embed=progress_embed)
                            last_db_update = current_time
                        except:
                            pass

                # ✅ Actually call the function with progress callback
                mismatches = await self.detect_database_mismatches(
                    interaction.guild,
                    db_progress_callback,
                    bloxlink_cache
                )

                total_issues = sum(len(v) for v in mismatches.values())

                if total_issues > 0:
                    # Show mismatch summary
                    mismatch_embed = discord.Embed(
                        title="<:Warn:1437771973970104471> Database Mismatches Found",
                        description=f"Found **{total_issues}** database issues that should be fixed first:",
                        color=discord.Color.orange()
                    )

                    if mismatches['discord_username_mismatch']:
                        mismatch_embed.add_field(
                            name=f"Discord Username Changes ({len(mismatches['discord_username_mismatch'])})",
                            value=f"Users whose Discord username changed",
                            inline=False
                        )

                    if mismatches['roblox_username_mismatch']:
                        mismatch_embed.add_field(
                            name=f"Roblox Username Changes ({len(mismatches['roblox_username_mismatch'])})",
                            value=f"Users whose Roblox username changed",
                            inline=False
                        )

                    if mismatches['roblox_id_mismatch']:
                        mismatch_embed.add_field(
                            name=f"Roblox ID Changes ({len(mismatches['roblox_id_mismatch'])})",
                            value=f"Users whose Roblox account changed",
                            inline=False
                        )

                    if mismatches['missing_discord_username']:
                        mismatch_embed.add_field(
                            name=f"Missing Discord Username ({len(mismatches['missing_discord_username'])})",
                            value=f"Database entries missing Discord username",
                            inline=False
                        )

                    if mismatches['missing_roblox_id']:
                        mismatch_embed.add_field(
                            name=f"Missing Roblox ID ({len(mismatches['missing_roblox_id'])})",
                            value=f"Database entries missing Roblox user ID",
                            inline=False
                        )

                    mismatch_embed.set_footer(
                        text="Click 'Fix Mismatches' to update all database entries, or 'Skip to Bulk Assign'")

                    # Create view with buttons
                    view = DatabaseMismatchView(self, interaction, mismatches, bloxlink_cache)
                    await interaction.edit_original_response(embed=mismatch_embed, view=view)
                    return
                else:
                    # ✅ Show "no issues found" message before proceeding
                    status_embed.title = "✅ Database Scan Complete"
                    status_embed.description = "No database issues found! Proceeding to bulk assign..."
                    status_embed.color = discord.Color.green()
                    await interaction.edit_original_response(embed=status_embed)
                    await asyncio.sleep(2)  # Give user time to see the message

            # No mismatches OR database_scan=False, proceed to bulk assign
            await self.start_bulk_assign(interaction, bloxlink_cache)

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    async def start_bulk_assign(self, interaction: discord.Interaction, bloxlink_cache: dict = None):
        """Streamlined bulk assign with proper Bloxlink handling"""

        # Initialize cache if not provided
        if bloxlink_cache is None:
            bloxlink_cache = {}

        try:
            # Progress Update 1: Starting scan
            status_embed = discord.Embed(
                title="<a:Load:1430912797469970444> Starting Bulk Assignment",
                description="Fetching FENZ members...",
                color=discord.Color.blue()
            )
            await interaction.edit_original_response(embed=status_embed)

            # Get all users with FENZ roles
            fenz_members = set()
            for role_id in FENZ_RANK_MAP.keys():
                role = interaction.guild.get_role(role_id)
                if role:
                    fenz_members.update(role.members)

            # Progress Update 2: Checking database
            status_embed.description = f"Found {len(fenz_members)} FENZ members\nChecking database..."
            await interaction.edit_original_response(embed=status_embed)

            # Get all callsigns from database
            async with db.pool.acquire() as conn:
                db_callsigns = await conn.fetch('SELECT * FROM callsigns')

            db_user_ids = {record['discord_user_id'] for record in db_callsigns}

            # Find users without callsigns
            members_without_callsigns = [member for member in fenz_members if member.id not in db_user_ids]

            if not members_without_callsigns:
                await interaction.edit_original_response(
                    content="<:Accepted:1426930333789585509> All FENZ members already have callsigns!",
                    embed=None
                )
                return

            # Progress Update 3: Starting Bloxlink checks
            status_embed.title = "<a:Load:1430912797469970444> Checking Bloxlink Connections"
            status_embed.description = f"Scanning {len(members_without_callsigns)} members...\nThis may take a few minutes."

            # Check how many are already cached
            cached_count = sum(1 for m in members_without_callsigns if m.id in bloxlink_cache)
            if cached_count > 0:
                status_embed.description += f"\n✅ {cached_count} already cached from database scan"

            await interaction.edit_original_response(embed=status_embed)

            # Track eligible users
            users_without_callsigns = []

            # Progress callback for real-time updates
            last_update_time = 0

            async def progress_callback(current, total, status_counts):
                """Update progress embed every 5 members or 3 seconds"""
                nonlocal last_update_time
                current_time = asyncio.get_event_loop().time()

                print(f"📊 Progress callback called: {current}/{total}")

                # Only update every 5 members OR every 3 seconds to avoid rate limits
                if current % 5 == 0 or (current_time - last_update_time) >= 3 or current == total:
                    progress_percent = int((current / total) * 100)
                    progress_bar = "█" * (progress_percent // 5) + "░" * (20 - (progress_percent // 5))

                    progress_embed = discord.Embed(
                        title="<a:Load:1430912797469970444> Checking Bloxlink Connections",
                        description=f"**Progress:** {current}/{total} ({progress_percent}%)\n"
                                    f"`{progress_bar}`\n\n"
                                    f"✅ **Linked:** {status_counts['success']}\n"
                                    f"❌ **Not Linked:** {status_counts['not_linked']}\n"
                                    f"⚠️ **API Issues:** {status_counts['rate_limited'] + status_counts['timeout'] + status_counts['api_error']}",
                        color=discord.Color.blue()
                    )

                    progress_embed.set_footer(text=f"Checking member {current} of {total}")

                    try:
                        await interaction.edit_original_response(embed=progress_embed)
                        last_update_time = current_time
                    except:
                        pass

            # Get Discord IDs for all members (only those NOT in cache)
            uncached_ids = [m.id for m in members_without_callsigns if m.id not in bloxlink_cache]

            if uncached_ids:
                tracker = ProgressTracker(interaction, len(uncached_ids), update_interval=3.0)

                async def progress_callback(current, total, status_counts):
                    await tracker.update(current, status_counts, force=(current == total))

                new_results = await self.bloxlink_api.bulk_check_bloxlink(
                    uncached_ids,
                    guild_id=interaction.guild.id,
                    progress_callback=progress_callback
                )

                if new_results is None:
                    error_embed = discord.Embed(
                        title="<:Denied:1426930694633816248> Bulk Check Terminated",
                        description="The Bloxlink API check was terminated due to excessive failures.\n\n"
                                    "**Possible causes:**\n"
                                    "• Bloxlink API is experiencing issues\n"
                                    "• Rate limits are being hit too frequently\n"
                                    "• Network connectivity problems\n\n"
                                    "**What to do:**\n"
                                    "• Wait 10-15 minutes and try again\n"
                                    "• Check if Bloxlink is operational\n"
                                    "• Contact support if the issue persists",
                        color=discord.Color.red()
                    )

                    await interaction.edit_original_response(embed=error_embed)
                    await interaction.edit_original_response(embed=error_embed)
                    return

                # ✅ Add new results to cache
                for discord_id, result in new_results.items():
                    bloxlink_cache[discord_id] = (
                        result['roblox_username'],
                        result['roblox_user_id'],
                        result['status']
                    )

            else:
                progress_embed = discord.Embed(
                    title="✅ Bloxlink Check Complete",
                    description=f"All {len(members_without_callsigns)} members already cached!\nProceeding to assignment...",
                    color=discord.Color.green()
                )
                await interaction.edit_original_response(embed=progress_embed)
                await asyncio.sleep(1)  # Brief pause so user can see the message

            # ✅ FIX: Process results using CACHE instead of undefined 'result' variable
            for member in members_without_callsigns:
                # Get from cache (guaranteed to exist now)
                roblox_username, roblox_id, status = bloxlink_cache[member.id]

                print(f"Bloxlink Result for {member.display_name} ({member.id}): {status}")

                # Initialize data
                has_issues = []

                # ✅ FIX: Validate Roblox data properly
                if status == 'success' and roblox_id and roblox_username:
                    # Check if username is valid (not Unknown or empty)
                    if roblox_username == 'Unknown' or not roblox_username:
                        print(f"   ⚠️ Invalid Roblox username for {member.display_name}")
                        roblox_username = 'MISSING'
                        roblox_id = 'MISSING'
                        has_issues.append('Invalid/deleted Roblox account')
                    else:
                        print(f"   ✅ Roblox: {roblox_username} (ID: {roblox_id})")
                elif status == 'not_linked':
                    print(f"   ❌ No Bloxlink for {member.display_name}")
                    roblox_username = 'MISSING'
                    roblox_id = 'MISSING'
                    has_issues.append('Not linked to Bloxlink')
                else:
                    # API failure
                    print(f"   🔴 API failure for {member.display_name}: {status}")
                    roblox_username = 'MISSING'
                    roblox_id = 'MISSING'
                    has_issues.append(f'API Error: {status}')

                # Get FENZ rank
                fenz_prefix = None
                for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                    if any(role.id == role_id for role in member.roles):
                        fenz_prefix = prefix
                        break

                if not fenz_prefix:
                    print(f"   ⚠️ No FENZ role for {member.display_name}")
                    fenz_prefix = 'MISSING'
                    has_issues.append('No valid FENZ rank role')
                else:
                    print(f"   ✅ FENZ Rank: {fenz_prefix}")

                # Add everyone (even with issues for reporting)
                if has_issues:
                    print(f"   ⚠️ Adding with issues: {', '.join(has_issues)}")
                else:
                    print(f"   ✅ PERFECT: No issues found")

                users_without_callsigns.append({
                    'member': member,
                    'fenz_prefix': fenz_prefix,
                    'roblox_id': str(roblox_id) if roblox_id and roblox_id != 'MISSING' else 'MISSING',
                    'roblox_username': roblox_username,
                    'has_issues': has_issues
                })

            # Progress Update 4: Scan complete
            status_embed.title = "<:Accepted:1426930333789585509> Scan Complete"
            status_embed.description = f"Found **{len(users_without_callsigns)}** members\nPreparing assignment interface..."
            await interaction.edit_original_response(embed=status_embed)

            # Print summary
            print("\n" + "=" * 60)
            print("BLOXLINK CHECK SUMMARY:")
            print("=" * 60)
            print(f"Total scanned: {len(members_without_callsigns)}")

            eligible = [u for u in users_without_callsigns if not u['has_issues']]
            with_issues = [u for u in users_without_callsigns if u['has_issues']]

            print(f"✅ Eligible (no issues): {len(eligible)}")
            print(f"⚠️ With issues: {len(with_issues)}")
            print("=" * 60)

            if with_issues:
                print("\nUsers with issues:")
                for user_data in with_issues[:10]:
                    print(f"  - {user_data['member'].display_name}: {', '.join(user_data['has_issues'])}")
                if len(with_issues) > 10:
                    print(f"  ... and {len(with_issues) - 10} more")

            # Show summary and start interactive assignment
            summary_embed = discord.Embed(
                title="Bulk Assignment Ready",
                description=f"Ready to assign callsigns to **{len(users_without_callsigns)}** members",
                color=discord.Color.blue()
            )

            summary_embed.add_field(
                name="<:Accepted:1426930333789585509> Ready to Assign",
                value=f"{len(eligible)} members with complete data",
                inline=True
            )

            if with_issues:
                summary_embed.add_field(
                    name="<:Warn:1437771973970104471> Members with Issues",
                    value=f"{len(with_issues)} members have missing data\n(Can still assign, but will need manual fixes)",
                    inline=True
                )

            summary_embed.set_footer(text="Click 'Assign' for each member, or 'Not Assigned' to skip")

            await interaction.edit_original_response(embed=summary_embed)

            # Start the interactive assignment
            view = BulkAssignView(self, interaction, users_without_callsigns)
            await view.start()

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()


class HHStJVersionModal(discord.ui.Modal):
    """Modal for selecting HHStJ callsign version"""

    def __init__(self, cog, interaction, user, callsign, fenz_prefix,
                 hhstj_versions, roblox_id, roblox_username, is_self_request=True):
        super().__init__(title="Select HHStJ Callsign Version")
        self.cog = cog
        self.original_interaction = interaction
        self.user = user  # The person GETTING the callsign
        self.callsign = callsign
        self.fenz_prefix = fenz_prefix
        self.hhstj_versions = hhstj_versions
        self.roblox_id = roblox_id
        self.roblox_username = roblox_username
        self.is_self_request = is_self_request  # True for /request, False for /assign

        # Add text input showing the versions
        version_list = "\n".join([f"• {v}" for v in hhstj_versions])
        self.version_input = discord.ui.TextInput(
            label="Enter your preferred version",
            placeholder=f"Choose one: {', '.join(hhstj_versions)}",
            style=discord.TextStyle.short,
            required=True,
            max_length=20
        )
        self.add_item(self.version_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            content=f"<a:Load:1430912797469970444> Processing callsign assignment...",
            ephemeral=True
        )

        try:
            # Get chosen version and validate
            chosen_version = self.version_input.value.strip()

            if chosen_version not in self.hhstj_versions:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Invalid version! Please choose one of: {', '.join(self.hhstj_versions)}",
                    ephemeral=True
                )
                return

            # Add to database
            is_fenz_hc = any(role.id in HIGH_COMMAND_RANKS for role in self.user.roles)
            is_hhstj_hc = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in self.user.roles)

            await add_callsign_to_database(
                self.callsign,
                self.user.id,
                str(self.user),
                self.roblox_id,
                self.roblox_username,
                self.fenz_prefix,
                chosen_version,
                interaction.user.id,
                interaction.user.display_name,
                is_fenz_hc,
                is_hhstj_hc
            )

            # Format nickname
            new_nickname = format_nickname(
                self.fenz_prefix,
                self.callsign,
                chosen_version,
                self.roblox_username
            )

            # Update nickname
            try:
                success, final_nick = await safe_edit_nickname(self.user, new_nickname)
                if not success:
                    print(f"⚠️ Failed to set nickname for {self.user.id}")
            except discord.Forbidden:
                pass

            # Update Google Sheets
            await sheets_manager.add_callsign_to_sheets(
                self.user, self.callsign, self.fenz_prefix,
                self.roblox_username, self.user.id
            )

            # Log to channel
            await self.cog.send_callsign_request_log(
                self.cog.bot, self.user, self.callsign, self.fenz_prefix,
                chosen_version, self.roblox_username, approved=True
            )

            # Send success message
            if self.is_self_request:
                # For /callsign request - notify the requester
                success_embed = discord.Embed(
                    title="<:Accepted:1426930333789585509> Callsign Approved!",
                    description=f"Your callsign request has been automatically approved!",
                    color=discord.Color.green()
                )
                success_embed.add_field(
                    name="Your Callsign",
                    value=f"**{self.fenz_prefix}-{self.callsign}**",
                    inline=True
                )
                success_embed.add_field(
                    name="HHStJ Version",
                    value=f"**{chosen_version}**",
                    inline=True
                )
                success_embed.add_field(
                    name="Nickname",
                    value=f"`{new_nickname}`",
                    inline=True
                )
                success_embed.set_footer(text=f"Approved automatically • {self.user.display_name}")
                success_embed.timestamp = datetime.utcnow()

                await interaction.followup.send(embed=success_embed, ephemeral=True)
            else:
                # For /callsign assign - notify the admin
                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Assigned **{self.fenz_prefix}-{self.callsign}** to {self.user.mention}\n"
                    f"HHStJ Version: **{chosen_version}**\n"
                    f"Nickname updated to: `{new_nickname}`",
                    ephemeral=True
                )

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

    @discord.ui.button(label='With Prefix (e.g., CO-001)', style=discord.ButtonStyle.primary)
    async def with_prefix_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Keeping Prefix",
                                                ephemeral=True)

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
            self.roblox_username
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

        await interaction.delete_original_response()

        # Send confirmation to high command member
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> You've chosen to use the prefix!\n"
            f"Your callsign is: **{self.fenz_prefix}-{self.callsign}**\n"
            f"Nickname set to: `{new_nickname}`",
            ephemeral=True,
            
        )

        # Send confirmation to admin who assigned it
        await self.original_interaction.followup.send(
            f"<:Accepted:1426930333789585509> {self.user.mention} chose to use prefix: **{self.fenz_prefix}-{self.callsign}**\n"
            f"Nickname updated to: `{new_nickname}`\n"
            f"Callsign synced to database and Google Sheets!",
            ephemeral=True,
            
        )

        self.choice_made = True
        self.stop()

    @discord.ui.button(label='Without Prefix (e.g., 001)', style=discord.ButtonStyle.secondary)
    async def without_prefix_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Removing Prefix",
                                                ephemeral=True)

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
            self.roblox_username
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

        await interaction.delete_original_response()


        # Send confirmation to high command member
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> You've chosen NOT to use a prefix!\n"
            f"Your callsign is: **{self.callsign}**\n"
            f"Nickname set to: `{new_nickname}`",
            ephemeral=True,
            
        )

        # Send confirmation to admin who assigned it
        await self.original_interaction.followup.send(
            f"<:Accepted:1426930333789585509> {self.user.mention} chose NO prefix: **{self.callsign}**\n"
            f"Nickname updated to: `{new_nickname}`\n"
            f"Callsign synced to database and Google Sheets!",
            ephemeral=True,
            
        )

        self.choice_made = True
        self.stop()

    @discord.ui.button(label='Cancel', style=discord.ButtonStyle.danger, emoji='<:Denied:1426930694633816248>')
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Cancelling",
                                                ephemeral=True)

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        await interaction.delete_original_response()

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


class DatabaseMismatchView(discord.ui.View):
    """View for handling database mismatches before bulk assign"""

    def __init__(self, cog, interaction, mismatches, bloxlink_cache: dict = None):
        super().__init__(timeout=600)
        self.cog = cog
        self.interaction = interaction
        self.mismatches = mismatches
        # ✅ FIX: Initialize cache AFTER super().__init__
        self.bloxlink_cache = bloxlink_cache if bloxlink_cache is not None else {}


    @discord.ui.button(label="Fix All Mismatches", style=discord.ButtonStyle.success, emoji="🔧")
    async def fix_button(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Fixing Inconsistencies",
                                                ephemeral=True)

        fixed_count = 0

        # Fix Discord username mismatches
        for item in self.mismatches['discord_username_mismatch']:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'UPDATE callsigns SET discord_username = $1 WHERE discord_user_id = $2',
                    item['new'], item['member'].id
                )
            fixed_count += 1

        # Fix missing Discord usernames
        for item in self.mismatches['missing_discord_username']:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'UPDATE callsigns SET discord_username = $1 WHERE discord_user_id = $2',
                    str(item['member']), item['member'].id
                )
            fixed_count += 1

        # Fix Roblox username mismatches
        for item in self.mismatches['roblox_username_mismatch']:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'UPDATE callsigns SET roblox_username = $1 WHERE discord_user_id = $2',
                    item['new'], item['member'].id
                )
            fixed_count += 1

        # Fix Roblox ID mismatches
        for item in self.mismatches['roblox_id_mismatch']:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'UPDATE callsigns SET roblox_user_id = $1, roblox_username = $2 WHERE discord_user_id = $3',
                    item['new_id'], item['current_username'], item['member'].id
                )
            fixed_count += 1

        # Fix missing Roblox IDs
        for item in self.mismatches['missing_roblox_id']:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'UPDATE callsigns SET roblox_user_id = $1, roblox_username = $2 WHERE discord_user_id = $3',
                    item['current_id'], item['current_username'], item['member'].id
                )
            fixed_count += 1

        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> Fixed {fixed_count} database mismatches!",
            ephemeral=True
        )
        await interaction.delete_original_response()

        # PASS THE CACHE when proceeding to bulk assign
        await self.cog.start_bulk_assign(self.interaction, self.bloxlink_cache)
        self.stop()

    @discord.ui.button(label="Skip to Bulk Assign", style=discord.ButtonStyle.secondary, emoji="<:RightSkip:1434962167660281926>")
    async def skip_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Skipping to Bulk Assign",
                                                ephemeral=True)

        # PASS THE CACHE when skipping to bulk assign
        await self.cog.start_bulk_assign(self.interaction, self.bloxlink_cache)
        await interaction.delete_original_response()
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, emoji="<:Denied:1426930694633816248>")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Cancelling Bulk Assign",
                                                ephemeral=True)
        await interaction.followup.send("<:Denied:1426930694633816248> Cancelled.", ephemeral=True)
        await interaction.delete_original_response()
        self.stop()

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
        self.nil_count = 0
        self.assignment_log = []

    async def start(self):
        """Start the bulk assignment process"""
        if not self.users_data:
            await self.interaction.followup.send("No users to assign!", ephemeral=True)
            return

        # Send initial message - this will be edited for all subsequent users
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

        roblox_display = user_data['roblox_username']
        if roblox_display == 'MISSING':
            roblox_display = "<:Warn:1437771973970104471>️ **MISSING** (No Bloxlink)"

        fenz_display = user_data['fenz_prefix']
        if fenz_display == 'MISSING':
            fenz_display = "<:Warn:1437771973970104471>️ **MISSING** (No FENZ role)"

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

        # ✅ Show issues if any
        if user_data.get('has_issues'):
            issues_text = "\n".join([f"• {issue}" for issue in user_data['has_issues']])
            embed.add_field(
                name="<:Warn:1437771973970104471>️ Issues Detected",
                value=issues_text,
                inline=False
            )

        embed.add_field(
            name="Progress",
            value=f"Assigned: {self.assigned_count} | NIL: {self.nil_count} | Skipped: {self.skipped_count}",
            inline=False
        )

        embed.set_footer(
            text="Click 'Assign' to enter a callsign, 'NIL' to set Not Assigned, 'Skip' to skip, or 'Finish' to end")

        # Always edit the original response (works for all users after the first followup)
        try:
            await self.interaction.edit_original_response(embed=embed, view=self)
        except discord.NotFound:
            # If original response doesn't exist yet (shouldn't happen), send as followup
            await self.interaction.followup.send(embed=embed, view=self, ephemeral=True)

    async def finish(self):
        """Finish the bulk assignment process"""

        if self.assignment_log:
            log_channel = self.cog.bot.get_channel(CALLSIGN_REQUEST_LOG_CHANNEL_ID)
            if log_channel:
                embed = discord.Embed(
                    title="Bulk Callsign Assignment",
                    description=f"Bulk assignment completed by {self.interaction.user.mention}",
                    color=discord.Color.blue(),
                    timestamp=datetime.utcnow()
                )

                # Group by type
                assigned = [a for a in self.assignment_log if a['type'] == 'assigned']
                nils = [a for a in self.assignment_log if a['type'] == 'nil']

                if assigned:
                    assigned_text = "\n".join([
                        f"• {a['member'].mention}: **{a['callsign']}** → `{a['nickname']}`"
                        for a in assigned[:10]  # Limit to 10 to avoid embed limits
                    ])
                    if len(assigned) > 10:
                        assigned_text += f"\n... and {len(assigned) - 10} more"

                    embed.add_field(
                        name=f"<:Accepted:1426930333789585509> Assigned ({len(assigned)})",
                        value=assigned_text,
                        inline=False
                    )

                if nils:
                    nil_text = "\n".join([
                        f"• {a['member'].mention}: **Not Assigned**"
                        for a in nils[:10]
                    ])
                    if len(nils) > 10:
                        nil_text += f"\n... and {len(nils) - 10} more"

                    embed.add_field(
                        name=f"Callsign Not Assigned",
                        value=nil_text,
                        inline=False
                    )

                embed.set_footer(text=f"Total: {len(self.assignment_log)} callsigns processed")

                await log_channel.send(embed=embed)

        embed = discord.Embed(
            title="<:Accepted:1426930333789585509> Bulk Assignment Complete!",
            color=discord.Color.green()
        )

        embed.add_field(
            name="Summary",
            value=f"**Assigned:** {self.assigned_count} callsigns\n"
                  f"**Set to NIL (Not Assigned):** {self.nil_count} users\n"
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

    @discord.ui.button(label="Not Assigned", style=discord.ButtonStyle.primary, emoji="❎")
    async def nil_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Set callsign to Not Assigned (NIL/default)"""

        await interaction.response.send_message(
            content=f"<a:Load:1430912797469970444> Setting Callsign to `Not Assigned`",
            ephemeral=True
        )

        try:
            user_data = self.users_data[self.current_index]
            member = user_data['member']

            # Get HHStJ prefix (preserving any stored shorthand)
            stored_hhstj = user_data.get('hhstj_prefix', '')
            hhstj_prefix = get_hhstj_prefix_from_roles(member.roles, stored_hhstj)

            # Add to database with Not Assigned as callsign
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

            await add_callsign_to_database(
                "Not Assigned",
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

            # ✅ Use format_nickname for consistent formatting
            new_nickname = format_nickname(
                user_data['fenz_prefix'],
                "Not Assigned",
                hhstj_prefix or '',
                user_data['roblox_username']
            )

            try:
                success, final_nick = await safe_edit_nickname(member, new_nickname)
                if not success:
                    print(f"⚠️ Failed to set nickname for {member.id}")
            except discord.Forbidden:
                pass

            # Success!
            self.nil_count += 1
            self.current_index += 1

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Set {member.mention} to Not Assigned\n"
                f"Nickname: `{new_nickname}`",
                ephemeral=True
            )

            # Show next user
            await interaction.delete_original_response()
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
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Skipping Callsign Assigment for this User",
                                                ephemeral=True)
        self.skipped_count += 1
        self.current_index += 1
        await interaction.delete_original_response()
        await self.show_current_user()

    @discord.ui.button(label="Finish", style=discord.ButtonStyle.danger, emoji="🏁")
    async def finish_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """End bulk assignment"""
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Finishing Bulk Assigning",
                                                ephemeral=True)
        await interaction.delete_original_response()
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
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Assigning Callsign",
                                                ephemeral=True)

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

            callsign = normalize_callsign(callsign)

            # Get HHStJ prefix BEFORE checking if callsign exists
            hhstj_prefix = get_hhstj_prefix_from_roles(member.roles)

            fenz_prefix = self.user_data['fenz_prefix']
            if fenz_prefix == 'MISSING':
                await interaction.followup.send(
                    "<:Warn:1437771973970104471>️ **Warning:** User has no FENZ rank role!\n"
                    "Cannot assign callsign without a valid FENZ rank.\n"
                    "Please assign them a rank first, then try again.",
                    ephemeral=True
                )
                return

            roblox_username = self.user_data['roblox_username']
            roblox_id = self.user_data['roblox_id']

            if roblox_username == 'MISSING' or roblox_id == 'MISSING':
                await interaction.followup.send(
                    "<:Warn:1437771973970104471>️ **Warning:** User has no Bloxlink connection!\n"
                    "Cannot assign callsign without a valid Roblox account.\n"
                    "Please ask them to run </verify:1114974748624027711> first.",
                    ephemeral=True
                )
                return

            # NOW check if callsign exists (with the correct prefix)
            existing = await check_callsign_exists(callsign, self.user_data['fenz_prefix'])
            if existing:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Callsign {self.user_data['fenz_prefix']}-{callsign} is already taken by <@{existing['discord_user_id']}>!",
                    ephemeral=True
                )
                return

            # Check if HHStJ high command needs version selection
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)

            if is_hhstj_high_command and hhstj_prefix and '-' in hhstj_prefix:
                # Get all shortened versions
                hhstj_versions = get_hhstj_shortened_versions(hhstj_prefix)

                # Test which versions fit
                is_fenz_hc = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                version_tests = test_hhstj_versions_fit(
                    self.user_data['fenz_prefix'], callsign, hhstj_versions,
                    roblox_username, is_fenz_hc, is_hhstj_high_command
                )

                # Filter to only valid versions
                valid_versions = [v['version'] for v in version_tests if v['fits']]

                if len(valid_versions) > 1:
                    # Show modal to the ADMIN doing bulk assign
                    modal = HHStJVersionModal(
                        self.view.cog, interaction, member, callsign,
                        self.user_data['fenz_prefix'], valid_versions,
                        roblox_id, roblox_username,
                        is_self_request=False  # Admin is choosing
                    )

                    # IMPORTANT: Track this assignment in the view
                    self.view.assigned_count += 1
                    self.view.assignment_log.append({
                        'type': 'assigned',
                        'member': member,
                        'callsign': f"{self.user_data['fenz_prefix']}-{callsign}",
                        'nickname': 'Pending HHStJ version selection'
                    })
                    self.view.current_index += 1

                    await interaction.followup.send(
                        f"⚠️ {member.mention} is HHStJ High Command - showing version selector...",
                        ephemeral=True
                    )

                    # Delete the bulk assign message to avoid confusion
                    await interaction.delete_original_response()

                    # Show the HHStJ version modal (this will handle the rest)
                    await interaction.response.send_modal(modal)

                    # Show next user in bulk assign
                    await self.view.show_current_user()
                    return
                elif len(valid_versions) == 1:
                    # Only one version fits, use it automatically
                    hhstj_prefix = valid_versions[0]
                else:
                    # No versions fit (shouldn't happen, but fallback)
                    hhstj_prefix = hhstj_versions[-1]  # Use shortest version

            # Assign callsign to database
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)

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

            shift_prefix = ""
            if member.nick:
                for prefix in ["DUTY | ", "BRK | ", "LOA | "]:
                    if member.nick.startswith(prefix):
                        shift_prefix = prefix
                        break

            # Update nickname
            new_nickname = format_nickname(
                self.user_data['fenz_prefix'],
                callsign,
                hhstj_prefix or '',
                self.user_data['roblox_username']
            )

            final_nickname = shift_prefix + new_nickname

            try:
                success, final_nick = await safe_edit_nickname(member, new_nickname)
                if not success:
                    print(f"⚠️ Failed to set nickname for {member.id}")
            except discord.Forbidden:
                pass

            # Success!
            self.view.assigned_count += 1

            self.view.assignment_log.append({
                'type': 'assigned',
                'member': member,
                'callsign': f"{self.user_data['fenz_prefix']}-{callsign}",
                'nickname': final_nickname
            })

            self.view.current_index += 1

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Assigned {self.user_data['fenz_prefix']}-{callsign} to {member.mention}",
                ephemeral=True
            )

            # Show next user
            await interaction.delete_original_response()
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