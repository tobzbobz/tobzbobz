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


def get_rank_sort_key(fenz_prefix: str, hhstj_prefix: str) -> tuple:
    """
    Generate a sort key based on rank hierarchy.
    Returns tuple: (fenz_rank_index, hhstj_rank_index)
    Lower index = higher rank
    """
    fenz_index = FENZ_RANK_HIERARCHY.index(fenz_prefix) if fenz_prefix in FENZ_RANK_HIERARCHY else 999
    hhstj_index = HHSTJ_RANK_HIERARCHY.index(hhstj_prefix) if hhstj_prefix in HHSTJ_RANK_HIERARCHY else 999
    return (fenz_index, hhstj_index)


async def check_callsign_exists(callsign: str) -> dict:
    """Check if a callsign exists in the database"""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM callsigns WHERE callsign = $1',
            callsign
        )
        return dict(row) if row else None


async def add_callsign_to_database(callsign: str, discord_user_id: int, discord_username: str,
                                   roblox_user_id: str, roblox_username: str, fenz_prefix: str,
                                   hhstj_prefix: str):
    """Add a new callsign to the database"""
    async with db.pool.acquire() as conn:
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

        # Delete ALL old callsigns for this user (not just different ones)
        await conn.execute(
            'DELETE FROM callsigns WHERE discord_user_id = $1',
            discord_user_id
        )

        # Insert new callsign
        await conn.execute(
            '''INSERT INTO callsigns
               (callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
                fenz_prefix, hhstj_prefix, approved_by_id, approved_by_name, callsign_history)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (callsign) DO UPDATE SET
                   discord_user_id = EXCLUDED.discord_user_id,
                   discord_username = EXCLUDED.discord_username,
                   roblox_user_id = EXCLUDED.roblox_user_id,
                   roblox_username = EXCLUDED.roblox_username,
                   fenz_prefix = EXCLUDED.fenz_prefix,
                   hhstj_prefix = EXCLUDED.hhstj_prefix,
                   approved_by_id = EXCLUDED.approved_by_id,
                   approved_by_name = EXCLUDED.approved_by_name,
                   callsign_history = EXCLUDED.callsign_history,
                   approved_at = NOW()''',
            callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
            fenz_prefix, hhstj_prefix, discord_user_id, discord_username,
            json.dumps(history)
        )


def format_nickname(fenz_prefix: str, callsign: str, hhstj_prefix: str, roblox_username: str,
                    has_fenz_high_command: bool = False, has_hhstj_high_command: bool = False) -> str:
    """
    Format nickname in standard format
    Priority: If HHStJ high command WITHOUT FENZ high command, format as:
    {HHStJ prefix} | {FENZ}-{callsign} | {Roblox username}

    Otherwise: {FENZ prefix}-{callsign} | {HHStJ prefix} | {Roblox username}
    """
    nickname_parts = []

    # Check if HHStJ high command takes priority (has HHStJ HC but NOT FENZ HC)
    hhstj_priority = has_hhstj_high_command and not has_fenz_high_command

    if hhstj_priority and hhstj_prefix:
        # HHStJ high command priority format
        nickname_parts.append(hhstj_prefix)

        # Add FENZ callsign
        if fenz_prefix and fenz_prefix != "":
            nickname_parts.append(f"{fenz_prefix}-{callsign}")
        elif callsign:
            nickname_parts.append(callsign)

        # Add Roblox username
        if roblox_username:
            nickname_parts.append(roblox_username)
    else:
        # Standard format: FENZ first
        # Add FENZ callsign
        if fenz_prefix and fenz_prefix != "":
            nickname_parts.append(f"{fenz_prefix}-{callsign}")
        elif callsign:
            nickname_parts.append(callsign)

        # Add HHStJ prefix if available (only if it doesn't already contain a dash)
        if hhstj_prefix and "-" not in hhstj_prefix:
            nickname_parts.append(hhstj_prefix)

        # Add Roblox username
        if roblox_username:
            nickname_parts.append(roblox_username)

    # Standard format with pipes
    new_nickname = " | ".join(nickname_parts)

    # Check length (Discord max is 32 characters)
    if len(new_nickname) <= 32:
        return new_nickname

    # Fallback 1: Remove one prefix based on priority
    if hhstj_priority:
        # Remove FENZ prefix if too long
        fallback_parts = [hhstj_prefix] if hhstj_prefix else []
        if callsign:
            fallback_parts.append(callsign)
        if roblox_username:
            fallback_parts.append(roblox_username)
    else:
        # Remove HHStJ prefix if too long (standard behavior)
        fallback_parts = []
        if fenz_prefix:
            fallback_parts.append(f"{fenz_prefix}-{callsign}")
        elif callsign:
            fallback_parts.append(callsign)
        if roblox_username:
            fallback_parts.append(roblox_username)

    fallback_nickname = " | ".join(fallback_parts)
    if len(fallback_nickname) <= 32:
        return fallback_nickname

    # Fallback 2: Just primary callsign
    if hhstj_priority and hhstj_prefix:
        return hhstj_prefix
    elif fenz_prefix:
        return f"{fenz_prefix}-{callsign}"
    elif callsign:
        return callsign

    # Last resort: truncate
    return new_nickname[:32]

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

    @tasks.loop(minutes=60)
    async def auto_sync_loop(self):
        """Background task for automatic syncing"""
        for guild in self.bot.guilds:
            try:
                async with db.pool.acquire() as conn:
                    callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

                if callsigns:
                    callsign_data = []
                    for record in callsigns:
                        member = guild.get_member(record['discord_user_id'])  # Use 'guild' not 'interaction.guild'
                        is_command_rank = False
                        if member:
                            rank_type, rank_data = sheets_manager.determine_rank_type(member.roles)
                            is_command_rank = (rank_type == 'command')

                        callsign_data.append({
                            'fenz_prefix': record['fenz_prefix'] or '',  # Ensure not None
                            'hhstj_prefix': record['hhstj_prefix'] or '',
                            'callsign': record['callsign'],
                            'discord_user_id': record['discord_user_id'],
                            'discord_username': record['discord_username'],
                            'roblox_user_id': record['roblox_user_id'],
                            'roblox_username': record['roblox_username'],
                            'is_command': is_command_rank
                        })

                    callsign_data.sort(key=lambda x: get_rank_sort_key(x['fenz_prefix'], x['hhstj_prefix']))
                    await sheets_manager.batch_update_callsigns(callsign_data)

                    print(f"Auto-sync completed for guild {guild.name}: {len(callsigns)} callsigns synced")
            except Exception as e:
                print(f'Error in auto-sync for guild {guild.name}: {e}')

    @auto_sync_loop.before_loop
    async def before_auto_sync(self):
        await self.bot.wait_until_ready()

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
                                hhstj_prefix = None
                                for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
                                    if any(role.id == role_id for role in member.roles):
                                        hhstj_prefix = prefix
                                        break

                                # Add to database
                                await add_callsign_to_database(
                                    sheet_data['callsign'],
                                    discord_id,
                                    str(member),
                                    roblox_id,
                                    roblox_username,
                                    sheet_data['fenz_prefix'],
                                    hhstj_prefix or ''
                                )
                                added_from_sheets += 1
                    else:
                        missing_in_sheets.append(
                            f"Discord ID {discord_id} (callsign {sheet_data['callsign']}) - user not in server")

            # Re-fetch database after additions
            if added_from_sheets > 0:
                async with db.pool.acquire() as conn:
                    db_callsigns = await conn.fetch('SELECT * FROM callsigns ORDER BY callsign')

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
                    'is_command': is_command_rank
                })

            # Sort by rank hierarchy
            callsign_data.sort(key=lambda x: get_rank_sort_key(x['fenz_prefix'], x['hhstj_prefix']))

            # Update Google Sheets
            success = await sheets_manager.batch_update_callsigns(callsign_data)

            if not success:
                await interaction.followup.send("<:Denied:1426930694633816248> Failed to sync to Google Sheets.")
                return

            # Update Discord nicknames for users with callsigns
            updated_count = 0
            failed_updates = []

            for record in db_callsigns:
                try:
                    record = dict(record)  # Convert to mutable dict
                    member = interaction.guild.get_member(record['discord_user_id'])

                    if member:
                        # Check if member has high command roles
                        is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)
                        is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in member.roles)
                        is_high_command = any(role.id in HIGH_COMMAND_RANKS for role in member.roles)

                        current_fenz_prefix = record['fenz_prefix']
                        correct_fenz_prefix = None

                        # Get correct FENZ rank from current roles
                        for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                            if any(role.id == role_id for role in member.roles):
                                correct_fenz_prefix = prefix
                                break

                        # Get correct HHStJ rank from current roles
                        current_hhstj_prefix = None
                        for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
                            if any(role.id == role_id for role in member.roles):
                                current_hhstj_prefix = prefix
                                break

                        # Update HHStJ prefix in database if changed
                        if current_hhstj_prefix != record['hhstj_prefix']:
                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET hhstj_prefix = $1 WHERE discord_user_id = $2',
                                    current_hhstj_prefix, member.id
                                )
                            record['hhstj_prefix'] = current_hhstj_prefix

                        # Determine if any ranks changed
                        fenz_changed = correct_fenz_prefix and correct_fenz_prefix != current_fenz_prefix
                        hhstj_changed = current_hhstj_prefix != record['hhstj_prefix']

                        # Update FENZ prefix if changed (unless high command chose no prefix)
                        if fenz_changed and not (is_high_command and current_fenz_prefix == ""):
                            async with db.pool.acquire() as conn:
                                await conn.execute(
                                    'UPDATE callsigns SET fenz_prefix = $1 WHERE discord_user_id = $2',
                                    correct_fenz_prefix, member.id
                                )
                            current_fenz_prefix = correct_fenz_prefix

                        # Calculate the correct nickname with updated prefixes
                        new_nickname = format_nickname(
                            current_fenz_prefix,
                            record['callsign'],
                            record['hhstj_prefix'],
                            record['roblox_username'],
                            is_fenz_high_command,
                            is_hhstj_high_command
                        )

                        # Only update if nickname is different from current
                        if member.nick != new_nickname:
                            try:
                                await member.edit(nick=new_nickname)
                                updated_count += 1

                                # Update sheets if ranks changed
                                if fenz_changed or hhstj_changed:
                                    await sheets_manager.add_callsign_to_sheets(
                                        member, record['callsign'], current_fenz_prefix,
                                        record['roblox_username'], member.id
                                    )
                            except discord.Forbidden:
                                failed_updates.append(
                                    f"{record.get('discord_username', 'Unknown')}: Missing permissions")

                except Exception as e:
                    failed_updates.append(f"{record.get('discord_username', 'Unknown')}: {str(e)}")

            # Build response
            response = f"<:Accepted:1426930333789585509> **Bidirectional Sync Complete!**\n"
            response += f"📊 Synced {len(db_callsigns)} callsigns to Google Sheets (sorted by rank hierarchy)\n"
            response += f"📖 Updated {updated_count} Discord nicknames\n"

            if added_from_sheets > 0:
                response += f"➕ Added {added_from_sheets} callsigns from sheets to database\n"

            if missing_in_sheets:
                response += f"\n⚠️ Found {len(missing_in_sheets)} entries in sheets with missing user data:\n"
                response += "\n".join(f"- {msg}" for msg in missing_in_sheets[:5])
                if len(missing_in_sheets) > 5:
                    response += f"\n... and {len(missing_in_sheets) - 5} more"

            if failed_updates:
                response += f"\n⚠️ Failed to update {len(failed_updates)} nicknames:\n"
                response += "\n".join(f"- {fail}" for fail in failed_updates[:10])
                if len(failed_updates) > 10:
                    response += f"\n... and {len(failed_updates) - 10} more"

            await interaction.followup.send(response, ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error during sync: {str(e)}")
            import traceback
            traceback.print_exc()

    @callsign_group.command(name="assign", description="Assign a callsign to a user")
    @app_commands.checks.has_role(SYNC_ROLE_ID)
    @app_commands.checks.has_permissions(administrator=True)
    async def assign_callsign(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            callsign: str,
            use_prefix: bool = True  # New parameter for high command
    ):
        """Assign a callsign to a Discord user"""
        await interaction.response.defer(thinking=True)

        try:
            # Validate callsign format (should be just the number)
            if not callsign.isdigit() or len(callsign) != 3:
                await interaction.followup.send("<:Denied:1426930694633816248> Callsign must be a 3-digit number (e.g., 001, 142)")
                return

            # Check if callsign already exists
            existing = await check_callsign_exists(callsign)
            if existing and existing['discord_user_id'] != user.id:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Callsign {callsign} is already assigned to <@{existing['discord_user_id']}>"
                )
                return

            # Get user's Roblox info
            bloxlink_data = await self.get_bloxlink_data(user.id, interaction.guild.id)
            if not bloxlink_data:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> Could not find Roblox account for {user.mention}. "
                    "Please verify their Bloxlink connection."
                )
                return

            roblox_id = bloxlink_data['id']
            roblox_username = await self.get_roblox_user_from_id(roblox_id)

            if not roblox_username:
                await interaction.followup.send("<:Denied:1426930694633816248> Failed to fetch Roblox username.")
                return

            # Get FENZ rank from user's roles
            fenz_prefix = None
            is_high_command = False

            for role_id, (rank_name, prefix) in FENZ_RANK_MAP.items():
                if any(role.id == role_id for role in user.roles):
                    fenz_prefix = prefix
                    # Check if this is a high command rank
                    if role_id in HIGH_COMMAND_RANKS:
                        is_high_command = True
                    break

            # Get HHStJ rank from user's roles
            hhstj_prefix = None
            for role_id, (rank_name, prefix) in HHSTJ_RANK_MAP.items():
                if any(role.id == role_id for role in user.roles):
                    hhstj_prefix = prefix
                    break

            if not fenz_prefix:
                await interaction.followup.send(
                    f"<:Denied:1426930694633816248> {user.mention} does not have a valid FENZ rank role."
                )
                return

            # FOR HIGH COMMAND - Send choice message
            if is_high_command:
                rank_name = [name for role_id, (name, _) in FENZ_RANK_MAP.items()
                             if any(r.id == role_id for r in user.roles)][0]

                embed = discord.Embed(
                    title="🎖️ High Command Callsign Assignment",
                    description=f"{user.mention}, you are being assigned callsign **{callsign}**.\n\n"
                                f"As a **{rank_name}**, "
                                f"you can choose whether to use your rank prefix or not.",
                    color=discord.Color.gold()
                )
                embed.add_field(
                    name="📋 Option 1: With Prefix",
                    value=f"Your callsign will be: **{fenz_prefix}-{callsign}**\n"
                          f"Example nickname: `{fenz_prefix}-{callsign} | {roblox_username}`",
                    inline=False
                )
                embed.add_field(
                    name="🔢 Option 2: Without Prefix",
                    value=f"Your callsign will be: **{callsign}**\n"
                          f"Example nickname: `{callsign} | {roblox_username}`",
                    inline=False
                )
                embed.add_field(
                    name="⏰ Time Limit",
                    value="You have **5 minutes** to make your choice.",
                    inline=False
                )
                embed.set_footer(text="Click one of the buttons below to make your choice")

                # Create the view
                view = HighCommandPrefixChoice(
                    interaction.user.id, self, interaction, user, callsign,
                    fenz_prefix, hhstj_prefix, roblox_id, roblox_username
                )

                # Send ephemeral message to HIGH COMMAND member
                try:
                    dm_message = await user.send(embed=embed, view=view)
                    view.message = dm_message  # Store message reference
                    dm_sent = True
                except discord.Forbidden:
                    dm_sent = False

                # Send message in channel if DM fails
                if dm_sent:
                    await interaction.followup.send(
                        f"📩 Sent a DM to {user.mention} to choose their callsign format!",
                        ephemeral=True
                    )
                else:
                    # Post in channel with ephemeral-style ping
                    channel_message = await interaction.channel.send(
                        content=f"{user.mention}",
                        embed=embed,
                        view=view
                    )
                    view.message = channel_message  # Store message reference

                    await interaction.followup.send(
                        f"📌 Posted callsign choice for {user.mention} in channel (DMs are disabled).",
                        ephemeral=True
                    )

                return  # Exit here - the view handles the rest

            # FOR NON-HIGH COMMAND - Proceed normally with prefix
            await add_callsign_to_database(
                callsign, user.id, str(user), roblox_id, roblox_username,
                fenz_prefix, hhstj_prefix
            )

            # Check for high command
            is_fenz_high_command = any(role.id in HIGH_COMMAND_RANKS for role in user.roles)
            is_hhstj_high_command = any(role.id in HHSTJ_HIGH_COMMAND_RANKS for role in user.roles)

            # Update Discord nickname
            new_nickname = format_nickname(
                fenz_prefix,
                callsign,
                hhstj_prefix,
                roblox_username,
                is_fenz_high_command,
                is_hhstj_high_command
            )
            try:
                await user.edit(nick=new_nickname)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"⚠️ Callsign assigned but couldn't update nickname (lacking permissions). "
                    f"Please manually set to: `{new_nickname}`"
                )
                return

            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Assigned callsign **{fenz_prefix}-{callsign}** to {user.mention}\n"
                f"🏷️ Nickname updated to: `{new_nickname}`\n"
                f"💡 Remember to run `/callsign sync` to update Google Sheets!",
                ephemeral=True
            )

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error assigning callsign: {str(e)}")

    @callsign_group.command(name="lookup", description="Look up a callsign")
    @app_commands.checks.has_role(1309021002675654700)
    async def lookup_callsign(
            self,
            interaction: discord.Interaction,
            callsign: str = None,
            user: discord.Member = None
    ):
        """Look up information about a callsign or user"""
        await interaction.response.defer(thinking=True)

        try:
            results = []

            if callsign:
                # Search by callsign
                result = await check_callsign_exists(callsign)
                if result:
                    results = [result]
            elif user:
                # Search by Discord user
                results = await self.search_callsign_database(str(user.id), 'discord_id')
            else:
                await interaction.followup.send("<:Denied:1426930694633816248> Please provide either a callsign or a user.")
                return

            if not results:
                await interaction.followup.send("<:Denied:1426930694633816248> No callsign found.")
                return

            # Display results
            for result in results:
                embed = discord.Embed(
                    title=f"Callsign: {result['fenz_prefix']}-{result['callsign']}" if result[
                        'fenz_prefix'] else f"Callsign: {result['callsign']}",
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

    @callsign_group.command(name="remove", description="Remove a callsign assignment")
    @app_commands.checks.has_role(SYNC_ROLE_ID)
    @app_commands.checks.has_permissions(administrator=True)
    async def remove_callsign(
            self,
            interaction: discord.Interaction,
            callsign: str = None,
            user: discord.Member = None,
            user_id: str = None
    ):
        """Remove a callsign from the database"""
        await interaction.response.defer(thinking=True)

        try:
            # Check if user_id is provided - requires special role
            if user_id:
                if not any(role.id == 1389550689113473024 for role in interaction.user.roles):
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> You need the sync role to use user_id!",
                        ephemeral=True
                    )
                    return

            if not callsign and not user and not user_id:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Please provide either a callsign, user, or user_id.",
                    ephemeral=True
                )
                return

            async with db.pool.acquire() as conn:
                if user_id:
                    result = await conn.fetchrow(
                        'DELETE FROM callsigns WHERE discord_user_id = $1 RETURNING *',
                        int(user_id)
                    )
                elif callsign:
                    result = await conn.fetchrow(
                        'DELETE FROM callsigns WHERE callsign = $1 RETURNING *',
                        callsign
                    )
                else:  # user
                    result = await conn.fetchrow(
                        'DELETE FROM callsigns WHERE discord_user_id = $1 RETURNING *',
                        user.id
                    )

                if not result:
                    await interaction.followup.send(
                        "<:Denied:1426930694633816248> No callsign found to remove.",
                        ephemeral=True
                    )
                    return

                removed_callsign = f"{result['fenz_prefix']}-{result['callsign']}" if result['fenz_prefix'] else result[
                    'callsign']

                # Try to reset nickname if it's a member
                if user:
                    try:
                        await user.edit(nick=result['roblox_username'])
                    except discord.Forbidden:
                        pass

                await interaction.followup.send(
                    f"<:Accepted:1426930333789585509> Removed callsign **{removed_callsign}**\n"
                    f"💡 Remember to run `/callsign sync` to update Google Sheets!",
                    ephemeral=True
                )

        except Exception as e:
            await interaction.followup.send(f"<:Denied:1426930694633816248> Error removing callsign: {str(e)}")


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
        await add_callsign_to_database(
            self.callsign, self.user.id, str(self.user),
            self.roblox_id, self.roblox_username,
            self.fenz_prefix, self.hhstj_prefix
        )

        # In with_prefix_button:
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

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        # Send confirmation to high command member
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> You've chosen to use the prefix!\n"
            f"🏷️ Your callsign is: **{self.fenz_prefix}-{self.callsign}**\n"
            f"📌 Nickname set to: `{new_nickname}`",
            ephemeral=True
        )

        # Send confirmation to admin who assigned it
        await self.original_interaction.followup.send(
            f"<:Accepted:1426930333789585509> {self.user.mention} chose to use prefix: **{self.fenz_prefix}-{self.callsign}**\n"
            f"🏷️ Nickname updated to: `{new_nickname}`\n"
            f"💡 Callsign synced to database and Google Sheets!",
            ephemeral=True
        )

        self.choice_made = True
        self.stop()

    @discord.ui.button(label='Without Prefix (e.g., 001)', style=discord.ButtonStyle.secondary, emoji='🔢')
    async def without_prefix_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        # Save to database WITHOUT prefix (empty string)
        await add_callsign_to_database(
            self.callsign, self.user.id, str(self.user),
            self.roblox_id, self.roblox_username,
            "", self.hhstj_prefix  # Empty prefix
        )

        # Update Discord nickname WITHOUT prefix
        # In without_prefix_button:
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

        # Disable buttons
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

        # Send confirmation to high command member
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> You've chosen NOT to use a prefix!\n"
            f"🔢 Your callsign is: **{self.callsign}**\n"
            f"📌 Nickname set to: `{new_nickname}`",
            ephemeral=True
        )

        # Send confirmation to admin who assigned it
        await self.original_interaction.followup.send(
            f"<:Accepted:1426930333789585509> {self.user.mention} chose NO prefix: **{self.callsign}**\n"
            f"🏷️ Nickname updated to: `{new_nickname}`\n"
            f"💡 Callsign synced to database and Google Sheets!",
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
                    content=f"⏰ {self.user.mention} - Callsign assignment timed out (no response after 5 minutes).",
                    view=self
                )

                # Notify admin
                await self.original_interaction.followup.send(
                    f"⏰ Callsign assignment for {self.user.mention} timed out.",
                    ephemeral=True
                )
            except Exception as e:
                print(f"Error in timeout handler: {e}")


async def setup(bot):
    await bot.add_cog(CallsignCog(bot))