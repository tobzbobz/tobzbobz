import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
load_dotenv()
import aiohttp
import re
import traceback
import sys
import os
from datetime import datetime, timedelta
import asyncio
from database import db
import json  # Still needed for JSON serialization in database
from google_sheets_integration import sheets_manager  # ADD THIS LINE
from google_sheets_integration import COMMAND_RANK_PRIORITY

BLOXLINK_API_KEY = os.getenv('BLOXLINK_API_KEY')
# Your Discord User ID
YOUR_USER_ID = 678475709257089057

# Role ID that can approve/deny requests
STAFF_ROLE_ID = 1285474077556998196

# Channel ID where callsign requests are sent
CALLSIGN_REQUEST_CHANNEL_ID = 1389064400043774022  # REPLACE WITH YOUR ACTUAL CHANNEL ID

# Bot owner ID for error DMs
OWNER_ID = 678475709257089057

# Configuration: Map Discord Role IDs to FENZ Ranks
FENZ_RANK_MAP = {
    # Format: role_id: ("Rank Name", "PREFIX")
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
    # Format: role_id: ("Rank Name", "PREFIX")
    1389113026900394064: ("First Responder", "FR"),
    1389112936517079230: ("Emergency Medical Technician", "EMT"),
    1389112844364021871: ("Graduate Paramedic", "GPARA"),
    1389112803712827473: ("Paramedic", "PARA"),
    1389112753142366298: ("Extended Care Paramedic", "ECP"),
    1389112689267314790: ("Critical Care Paramedic", "CCP"),
    1389112601815941240: ("Doctor", "DR"),

# Operational Management Ranks (these override medical ranks)
    1389112470211264552: ("Watch Operations Manager", "WOM-MIKE30"),
    1403314606839037983: ("Area Operations Manager", "AOM-OSCAR32"),
    1403314387602767932: ("District Operations Support Manager", "DOSM-OSCAR31"),
    1403312277876248626: ("District Operations Manager", "DOM-OSCAR30"),
    1389111474949062726: ("Assistant National Operations Manager", "ANOM-OSCAR3"),
    1389111326571499590: ("Deputy National Operations Manager", "DNOM-OSCAR2"),
    1389110819190472775: ("National Operations Manager", "NOM-OSCAR1"),
}


# ✅ NEW DATABASE FUNCTIONS
async def check_callsign_exists(callsign: str) -> dict:
    """Check if a callsign exists in the database"""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM callsigns WHERE callsign = $1',
            callsign
        )
        return dict(row) if row else None


async def search_callsign_database(query: str, search_type: str) -> list:
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



async def add_callsign_to_database(callsign: str, discord_user_id: int, discord_username: str,
                                   roblox_user_id: str, roblox_username: str, fenz_prefix: str,
                                   hhstj_prefix: str, approved_by_id: int, approved_by_name: str):
    """Add a new approved callsign to the database"""
    async with db.pool.acquire() as conn:
        # Check if user already has a callsign
        old_callsigns = await search_callsign_database(str(discord_user_id), 'discord_id')

        # Store history of previous callsigns
        history = []
        for old_data in old_callsigns:
            history.append({
                "callsign": old_data.get("callsign"),
                "fenz_prefix": old_data.get("fenz_prefix"),
                "hhstj_prefix": old_data.get("hhstj_prefix"),
                "approved_at": old_data.get("approved_at").isoformat() if old_data.get("approved_at") else None,
                "approved_by_id": old_data.get("approved_by_id"),
                "approved_by_name": old_data.get("approved_by_name"),
                "replaced_at": int(datetime.utcnow().timestamp())
            })

            # Delete old callsign
            await conn.execute(
                'DELETE FROM callsigns WHERE callsign = $1',
                old_data['callsign']
            )

        # Insert new callsign
        await conn.execute(
            '''INSERT INTO callsigns
               (callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
                fenz_prefix, hhstj_prefix, approved_by_id, approved_by_name, callsign_history)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''',
            callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
            fenz_prefix, hhstj_prefix, approved_by_id, approved_by_name,
            json.dumps(history)
        )


async def send_error_to_owner(bot, title: str, error: Exception, interaction: discord.Interaction = None):
    """Send error details to bot owner"""
    try:
        owner = await bot.fetch_user(OWNER_ID)
        error_embed = discord.Embed(
            title=f"⚠️ {title}",
            description=f"```{str(error)}```",
            color=discord.Color.red()
        )
        if interaction:
            error_embed.add_field(name="User", value=f"{interaction.user.mention}", inline=True)
            error_embed.add_field(name="Guild", value=f"{interaction.guild.name}", inline=True)
        error_embed.timestamp = discord.utils.utcnow()
        await owner.send(embed=error_embed)
    except Exception as e:
        print(f"Failed to send error to owner: {e}")


class DenyReasonModal(discord.ui.Modal, title="Deny Counter Offer"):
    def __init__(self, view, original_message, thread):
        super().__init__()
        self.view = view
        self.original_message = original_message
        self.thread = thread

    reason = discord.ui.TextInput(
        label="Reason for Denial",
        placeholder="Enter the reason for denying this counter offer",
        required=True,
        max_length=500,
        style=discord.TextStyle.paragraph
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()

            # Update original embed with reason
            embed = discord.Embed(
                title="Callsign Request",
                colour=discord.Colour(0xf24d4d)
            )

            embed.add_field(
                name="Requested Callsign",
                value=f"`{self.view.fenz_prefix}-{self.view.original_callsign}`" if self.view.fenz_prefix else f"`{self.view.original_callsign}`",
                inline=True
            )

            embed.add_field(
                name="User",
                value=f"{self.view.requester.mention}",
                inline=True
            )

            if self.view.fenz_prefix:
                # Get the rank name from the map
                fenz_rank_name = None
                for role in self.view.requester.roles:
                    if role.id in FENZ_RANK_MAP:
                        fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                        break

                embed.add_field(
                    name="FENZ Rank",
                    value=f"{fenz_rank_name}" if fenz_rank_name else f"`{self.view.fenz_prefix}`",
                    inline=True
                )

            embed.add_field(name='Status:', value=f'Denied <:Denied:1426930694633816248>', inline=True)
            embed.add_field(name='Denied at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}', inline=True)

            embed.add_field(
                name="Reason:",
                value=self.reason.value,
                inline=False
            )

            embed.timestamp = discord.utils.utcnow()
            embed.set_footer(text=f"Denied by {interaction.user.display_name}")

            # Update original message
            await self.original_message.edit(embed=embed, view=None)

            try:
                dm_embed = discord.Embed(
                    title="Callsign Request Denied <:Denied:1426930694633816248>",
                    description=f"Your callsign request has been denied.\n\n**Reason:** {self.reason.value}",
                    color=discord.Color.red()
                )
                dm_embed.add_field(
                    name="Next Steps",
                    value="Please submit a new request using `/callsign request` command.",
                    inline=False
                )

                await self.view.requester.send(embed=dm_embed)
                await interaction.followup.send("Denial message sent via DM <:Accepted:1426930333789585509>",
                                                ephemeral=True)

            except discord.Forbidden:
                await interaction.followup.send(
                    "Could not DM the requester – they may have DMs disabled <:Denied:1426930694633816248>",
                    ephemeral=True
                )
            except Exception as e:
                print(f"Error sending DM to requester: {e}")

            # Delete thread after a short delay
            if hasattr(self.view, 'thread') and self.view.thread:
                await asyncio.sleep(5)
                try:
                    await self.thread.delete()
                except Exception as e:
                    print(f"Failed to delete thread: {e}")

        except Exception as e:
            await send_error_to_owner(interaction.client, "Deny reason error", e, interaction)
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

class OverrideModal(discord.ui.Modal, title="Override with New Callsign"):
    def __init__(self, view, thread=None):
        super().__init__()
        self.view = view
        self.thread = thread

    new_callsign = discord.ui.TextInput(
        label="New Callsign",
        placeholder="Enter the callsign to approve instead",
        required=True,
        max_length=10,
        style=discord.TextStyle.short
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()

            # Validate callsign is numeric
            if not self.new_callsign.value.isdigit():
                await interaction.followup.send(
                    f"Callsign must be numeric only <:Denied:1426930694633816248> Invalid: `{self.new_callsign.value}`",
                    ephemeral=True
                )
                return

            # Validate callsign is not over 999
            if int(self.new_callsign.value) > 999:
                await interaction.followup.send(
                    f"Callsign must be 999 or below <:Denied:1426930694633816248> Invalid: `{self.new_callsign.value}`",
                    ephemeral=True
                )
                return

            # Add to database with the NEW callsign
            await add_callsign_to_database(
                callsign=self.new_callsign.value,
                discord_user_id=self.view.requester.id,
                discord_username=self.view.requester.display_name,
                roblox_user_id=self.view.roblox_user_id,
                roblox_username=self.view.roblox_username,
                fenz_prefix=self.view.fenz_prefix,
                hhstj_prefix=self.view.hhstj_prefix,
                approved_by_id=interaction.user.id,
                approved_by_name=interaction.user.display_name
            )

            async with db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT id FROM callsigns WHERE discord_user_id = $1',
                    self.requester.id
                )
                callsign_id = row['id'] if row else None

            # Also add to Google Sheets
            try:
                await sheets_manager.add_callsign_to_sheets(
                    member=self.view.requester,
                    callsign=self.new_callsign.value,
                    fenz_prefix=self.view.fenz_prefix if self.view.fenz_prefix else '',
                    roblox_username=self.view.roblox_username,
                    discord_id=self.view.requester.id
                )
                print(f"✅ Synced callsign to Google Sheets")
            except Exception as e:
                print(f"⚠️ Failed to sync to Google Sheets: {e}")

            # Update nickname with NEW callsign
            nickname_parts = []
            if self.view.fenz_prefix:
                nickname_parts.append(f"{self.view.fenz_prefix}-{self.new_callsign.value}")
            if self.view.hhstj_prefix:
                if "-" not in self.view.hhstj_prefix:
                    nickname_parts.append(self.view.hhstj_prefix)
            if self.view.roblox_username:
                nickname_parts.append(self.view.roblox_username)

            new_nickname = " | ".join(nickname_parts)
            nickname_success = False
            nickname_error = None

            if len(new_nickname) <= 32:
                try:
                    await self.view.requester.edit(nick=new_nickname)
                    nickname_success = True
                except Exception as e:
                    nickname_error = str(e)
            else:
                # [INSERT THE FULL FALLBACK LOGIC FROM accept_button HERE]
                # Copy the entire fallback logic block from the accept_button method
                pass

            # Delete thread after approval
            if self.thread:
                await asyncio.sleep(3)
                try:
                    await self.thread.delete()
                except Exception as e:
                    print(f"Failed to delete thread: {e}")

            # Update embed to show override approval with NEW callsign
            embed = discord.Embed(
                title="Callsign Request",
                colour=discord.Colour(0x2ecc71)
            )

            embed.add_field(
                name="Approved Callsign",
                value=f"`{self.view.fenz_prefix}-{self.new_callsign.value}`" if self.view.fenz_prefix else f"`{self.new_callsign.value}`",
                inline=True
            )

            embed.add_field(
                name="User",
                value=f"{self.view.requester.mention}",
                inline=True
            )

            if self.view.fenz_prefix:
                fenz_rank_name = None
                for role in self.view.requester.roles:
                    if role.id in FENZ_RANK_MAP:
                        fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                        break

                embed.add_field(
                    name="FENZ Rank",
                    value=f"{fenz_rank_name}" if fenz_rank_name else f"`{self.view.fenz_prefix}`",
                    inline=True
                )

            embed.add_field(name='Status:', value=f'Approved (Override) <:Accepted:1426930333789585509>', inline=True)
            embed.add_field(name='Approved at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}', inline=True)

            embed.timestamp = discord.utils.utcnow()
            embed.set_footer(text=f"Overridden by {interaction.user.display_name} • {callsign_id}")

            # Delete the original message
            await interaction.message.delete()

            # Send new message with ping
            await interaction.channel.send(
                content=f"-# ||{self.view.requester.mention}||",
                embed=embed
            )

            await interaction.followup.send("Callsign approved via override <:Accepted:1426930333789585509>", ephemeral=True)

        except Exception as e:
            await send_error_to_owner(interaction.client, "Override approval error", e, interaction)
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

class CounterOfferModal(discord.ui.Modal, title="Counter Offer Callsigns"):
    def __init__(self, view, thread, original_message):
        super().__init__()
        self.view = view
        self.thread = thread
        self.original_message = original_message

    offer1 = discord.ui.TextInput(
        label="Offer 1 (Required)",
        placeholder="Enter the first callsign offer",
        required=True,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer2 = discord.ui.TextInput(
        label="Offer 2 (Optional)",
        placeholder="Enter the second callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer3 = discord.ui.TextInput(
        label="Offer 3 (Optional)",
        placeholder="Enter the third callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer4 = discord.ui.TextInput(
        label="Offer 4 (Optional)",
        placeholder="Enter the fourth callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer5 = discord.ui.TextInput(
        label="Offer 5 (Optional)",
        placeholder="Enter the fifth callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    async def on_submit(self, interaction: discord.Interaction):  # ← Column 4
        try:  # ← Column 8
            await interaction.response.defer()  # ← Column 12
            channel = interaction.guild.get_channel(CALLSIGN_REQUEST_CHANNEL_ID)

            # Collect all offers
            offers = []
            if self.offer1.value:
                offers.append(self.offer1.value)
            if self.offer2.value:
                offers.append(self.offer2.value)
            if self.offer3.value:
                offers.append(self.offer3.value)
            if self.offer4.value:
                offers.append(self.offer4.value)
            if self.offer5.value:
                offers.append(self.offer5.value)

            # Validate all offers are numeric
            for offer in offers:
                if not offer.isdigit():
                    await interaction.followup.send(
                        f"All callsign offers must be numeric only <:Denied:1426930694633816248> Invalid: `{offer}`",
                        ephemeral=True
                    )
                    return

                # Validate all offers are numeric
                for offer in offers:
                    if not offer.isdigit():
                        await interaction.followup.send(
                            f"All callsign offers must be numeric only <:Denied:1426930694633816248> Invalid: `{offer}`",
                            ephemeral=True
                        )
                        return

                    if int(offer) > 999:
                        await interaction.followup.send(
                            f"Callsign offers must be 999 or below <:Denied:1426930694633816248> Invalid: `{offer}`",
                            ephemeral=True
                        )
                        return

                # ✅ No need to send to channel here — we’ll build and send the counter embed next

            # Create counter offer embed
            counter_embed = discord.Embed(
                title="Counter Offer",
                description=f"{self.view.requester.mention} has provided alternative callsign options:",
                color=discord.Color(0xffffff)
            )

            for i, offer in enumerate(offers, 1):
                counter_embed.add_field(
                    name=f"‎",
                    value=f"`{self.view.fenz_prefix}-{offer}`" if self.view.fenz_prefix else f"`{offer}`",
                    inline=True
                )

            counter_embed.set_footer(text="FENZ | Leadership can approve or deny these options")

            # Create view with counter offer buttons
            counter_view = CounterOfferApprovalView(
                self.view.requester,
                offers,
                self.view.fenz_prefix,
                self.view.hhstj_prefix,
                self.view.roblox_username,
                self.view.roblox_user_id,
                self.view.original_callsign,
                self.original_message,
                self.thread
            )

            await self.thread.send(
                embed=counter_embed,
                view=counter_view
            )

            await interaction.message.delete()

            # Edit the original message to show counter offer was submitted
            edit_embed = discord.Embed(
                title="Counter Offer Submitted",
                description=f"{interaction.user.mention} has submitted alternative callsign options.",
                color=discord.Color(0xffffff)
            )
            edit_embed.timestamp = discord.utils.utcnow()
            edit_embed.set_footer(text=f"{interaction.user.display_name} has submitted a counter offer.")

            await self.original_message.edit(embed=edit_embed, view=None)
            await interaction.followup.send("Counter offer submitted <:Accepted:1426930333789585509>", ephemeral=True)

        except Exception as e:
            await send_error_to_owner(interaction.client, "Counter offer error", e, interaction)
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)


class CounterOfferApprovalView(discord.ui.View):
    def __init__(self, requester: discord.Member, offers: list, fenz_prefix: str, hhstj_prefix: str,
                 roblox_username: str, roblox_user_id: str, original_callsign: str,
                 original_message: discord.Message, thread: discord.Thread):
        super().__init__(timeout=None)
        self.requester = requester
        self.offers = offers
        self.fenz_prefix = fenz_prefix
        self.hhstj_prefix = hhstj_prefix
        self.roblox_username = roblox_username
        self.roblox_user_id = roblox_user_id
        self.original_callsign = original_callsign
        self.original_message = original_message
        self.thread = thread

        # Add buttons for each offer
        for i, offer in enumerate(offers, 1):
            button = discord.ui.Button(
                label=f"{self.fenz_prefix}-{offer}",
                style=discord.ButtonStyle.primary,
                custom_id=f"counter_offer_{i}_{offer}"
            )
            button.callback = self.create_offer_callback(offer)
            self.add_item(button)

        # Add deny button
        deny_button = discord.ui.Button(
            label="Deny",
            style=discord.ButtonStyle.danger,
            emoji="<:Denied:1426930694633816248>"
        )
        deny_button.callback = self.deny_callback
        self.add_item(deny_button)

    def create_offer_callback(self, callsign: str):
        async def callback(interaction: discord.Interaction):
            # Check if user has the staff role
            if not any(role.id == STAFF_ROLE_ID for role in interaction.user.roles):
                await interaction.response.send_message(
                    "You don't have permission to approve callsigns <:Denied:1426930694633816248>",
                    ephemeral=True
                )
                return

            await interaction.response.defer()

            # Check if callsign already exists
            existing = await check_callsign_exists(self.callsign)
            if existing and existing['discord_user_id'] != self.requester.id:
                await interaction.followup.send(
                    f"❌ Callsign `{self.fenz_prefix}-{self.callsign}` is already occupied by <@{existing['discord_user_id']}>. Please click deny on this request and offer an alternative callsign.",
                    ephemeral=True
                )
                return

            try:
                # Add to database
                await add_callsign_to_database(
                    callsign=callsign,
                    discord_user_id=self.requester.id,
                    discord_username=self.requester.display_name,
                    roblox_user_id=self.roblox_user_id,
                    roblox_username=self.roblox_username,
                    fenz_prefix=self.fenz_prefix,
                    hhstj_prefix=self.hhstj_prefix,
                    approved_by_id=interaction.user.id,
                    approved_by_name=interaction.user.display_name
                )

                async with db.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        'SELECT id FROM callsigns WHERE discord_user_id = $1',
                        self.requester.id
                    )
                    callsign_id = row['id'] if row else None

                # Also add to Google Sheets
                try:
                    await sheets_manager.add_callsign_to_sheets(
                        member=self.requester,
                        callsign=callsign,
                        fenz_prefix=self.fenz_prefix if self.fenz_prefix else '',
                        roblox_username=self.roblox_username,
                        discord_id=self.requester.id
                    )
                    print(f"✅ Synced callsign to Google Sheets")
                except Exception as e:
                    print(f"⚠️ Failed to sync to Google Sheets: {e}")

                # Update nickname
                nickname_parts = []
                if self.fenz_prefix:
                    nickname_parts.append(f"{self.fenz_prefix}-{callsign}")
                if self.hhstj_prefix:
                    # If operational management (has hyphen), use as-is, otherwise add to parts
                    if "-" not in self.hhstj_prefix:
                        nickname_parts.append(self.hhstj_prefix)
                if self.roblox_username:
                    nickname_parts.append(self.roblox_username)

                new_nickname = " | ".join(nickname_parts)
                nickname_success = False
                nickname_error = None

                if len(new_nickname) <= 32:
                    try:
                        await self.requester.edit(nick=new_nickname)
                        nickname_success = True
                    except Exception as e:
                        nickname_error = str(e)
                else:
                    # Fallback logic for operational management
                    if self.hhstj_prefix and "-" in self.hhstj_prefix:
                        # Try shortening the OM callsign (e.g., MIKE30 → MKE30)
                        om_parts = self.hhstj_prefix.split("-")
                        if len(om_parts) == 2:
                            base, callsign_num = om_parts
                            # Try shortened version (first 3 letters)
                            shortened_prefix = f"{base[:3]}-{callsign_num}"

                            fallback_parts = []
                            if self.fenz_prefix:
                                fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                            fallback_parts.append(shortened_prefix)
                            if self.roblox_username:
                                fallback_parts.append(self.roblox_username)
                            fallback_nickname = " | ".join(fallback_parts)

                            if len(fallback_nickname) <= 32:
                                try:
                                    await self.requester.edit(nick=fallback_nickname)
                                    nickname_success = True
                                    new_nickname = fallback_nickname
                                except Exception as e:
                                    nickname_error = str(e)
                            else:
                                # Try just the base OM rank (e.g., WOM)
                                fallback_parts = []
                                if self.fenz_prefix:
                                    fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                                fallback_parts.append(base)
                                if self.roblox_username:
                                    fallback_parts.append(self.roblox_username)
                                fallback_nickname = " | ".join(fallback_parts)

                                if len(fallback_nickname) <= 32:
                                    try:
                                        await self.requester.edit(nick=fallback_nickname)
                                        nickname_success = True
                                        new_nickname = fallback_nickname
                                    except Exception as e:
                                        nickname_error = str(e)
                                else:
                                    # Last resort: Drop OM entirely
                                    fallback_parts = []
                                    if self.fenz_prefix:
                                        fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                                    if self.roblox_username:
                                        fallback_parts.append(self.roblox_username)
                                    fallback_nickname = " | ".join(fallback_parts)

                                    if len(fallback_nickname) <= 32:
                                        try:
                                            await self.requester.edit(nick=fallback_nickname)
                                            nickname_success = True
                                            new_nickname = fallback_nickname
                                        except Exception as e:
                                            nickname_error = str(e)
                    else:
                        # Non-OM: just drop HHStJ clinical rank
                        fallback_parts = []
                        if self.fenz_prefix:
                            fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                        if self.roblox_username:
                            fallback_parts.append(self.roblox_username)
                        fallback_nickname = " | ".join(fallback_parts)

                        if len(fallback_nickname) <= 32:
                            try:
                                await self.requester.edit(nick=fallback_nickname)
                                nickname_success = True
                                new_nickname = fallback_nickname
                            except Exception as e:
                                nickname_error = str(e)

                # Update original embed
                embed = discord.Embed(
                    title="Callsign Request",
                    colour=discord.Colour(0x2ecc71)
                )

                embed.add_field(
                    name="Requested Callsign",
                    value=f"`{self.fenz_prefix}-{callsign}`" if self.fenz_prefix else f"`{callsign}`",
                    inline=True
                )

                embed.add_field(
                    name="User",
                    value=f"{self.requester.mention}",
                    inline=True
                )

                if self.fenz_prefix:
                    # Get the rank name from the map
                    fenz_rank_name = None
                    for role in self.requester.roles:
                        if role.id in FENZ_RANK_MAP:
                            fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                            break

                    embed.add_field(
                        name="FENZ Rank",
                        value=f"{fenz_rank_name}" if fenz_rank_name else f"`{self.fenz_prefix}`",
                        inline=True
                    )

                embed.add_field(name='Status:', value=f'Approved <:Accepted:1426930333789585509>', inline=True)
                embed.add_field(name='Approved at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}',
                                inline=True)

                embed.timestamp = discord.utils.utcnow()
                embed.set_footer(text=f"Approved by {interaction.user.display_name} • {callsign_id}")

                embed.timestamp = discord.utils.utcnow()

                # Delete thread after a short delay
                await interaction.followup.send("Callsign approved <:Accepted:1426930333789585509>, Deleting thread <a:Load:1430912797469970444>", ephemeral=True)

                # Send new message with ping in the callsign channel
                channel = interaction.guild.get_channel(CALLSIGN_REQUEST_CHANNEL_ID)
                if channel:
                    await channel.send(
                        content=f"-# ||{self.requester.mention}||",
                        embed=embed
                    )

                # Delete the original message
                await self.original_message.delete()
                await asyncio.sleep(3)
                await self.thread.delete()

            except Exception as e:
                await send_error_to_owner(interaction.client, "Counter offer approval error", e, interaction)
                error_embed = discord.Embed(
                    title="Error <:Denied:1426930694633816248>",
                    description=f"An error occurred: {str(e)}",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)

        return callback

    async def deny_callback(self, interaction: discord.Interaction):
        # Check if user has the staff role
        if not any(role.id == STAFF_ROLE_ID for role in interaction.user.roles):
            await interaction.response.send_message(
                "You don't have permission to deny callsigns <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Show modal for denial reason
        modal = DenyReasonModal(self, self.original_message, self.thread)
        await interaction.response.send_modal(modal)

class DenyModal(discord.ui.Modal, title="Deny Callsign Request"):
    def __init__(self, view):
        super().__init__()
        self.view = view

    offer1 = discord.ui.TextInput(
        label="Offer 1 (Required)",
        placeholder="Enter the first callsign offer",
        required=True,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer2 = discord.ui.TextInput(
        label="Offer 2 (Optional)",
        placeholder="Enter the second callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer3 = discord.ui.TextInput(
        label="Offer 3 (Optional)",
        placeholder="Enter the third callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer4 = discord.ui.TextInput(
        label="Offer 4 (Optional)",
        placeholder="Enter the fourth callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    offer5 = discord.ui.TextInput(
        label="Offer 5 (Optional)",
        placeholder="Enter the fifth callsign offer",
        required=False,
        max_length=10,
        style=discord.TextStyle.short
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()

            # Collect all offers
            offers = []
            if self.offer1.value:
                offers.append(self.offer1.value)
            if self.offer2.value:
                offers.append(self.offer2.value)
            if self.offer3.value:
                offers.append(self.offer3.value)
            if self.offer4.value:
                offers.append(self.offer4.value)
            if self.offer5.value:
                offers.append(self.offer5.value)

            # Validate all offers are numeric
            for offer in offers:
                if not offer.isdigit():
                    await interaction.followup.send(
                        f"All callsign offers must be numeric only <:Denied:1426930694633816248> Invalid: `{offer}`",
                        ephemeral=True
                    )
                    return

                # Validate callsign is not over 999
                if int(offer) > 999:
                    await interaction.followup.send(
                        f"Callsign offers must be 999 or below <:Denied:1426930694633816248> Invalid: `{offer}`",
                        ephemeral=True
                    )
                    return

            embed = discord.Embed(
                title="Callsign Request",
                colour=discord.Colour(0xf24d4d)
            )

            embed.add_field(
                name="Requested Callsign",
                value=f"`{self.view.fenz_prefix}-{self.view.callsign}`" if self.view.fenz_prefix else f"`{self.view.callsign}`",
                inline=True
            )

            embed.add_field(
                name="User",
                value=f"{self.view.requester.mention}",
                inline=True
            )

            if self.view.fenz_prefix:
                # Get the rank name from the map
                fenz_rank_name = None
                for role in self.view.requester.roles:
                    if role.id in FENZ_RANK_MAP:
                        fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                        break

                embed.add_field(
                    name="FENZ Rank",
                    value=f"{fenz_rank_name}" if fenz_rank_name else f"`{self.view.fenz_prefix}`",
                    inline=True
                )

            embed.add_field(name='Status:', value=f'Denied <:Denied:1426930694633816248>', inline=True)
            embed.add_field(name='Denied at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}', inline=True)

            embed.timestamp = discord.utils.utcnow()
            embed.set_footer(text=f"Denied by {interaction.user.display_name}")

            # Disable Accept/Deny buttons, add Override button
            for item in self.view.children[:]:
                if hasattr(item, 'label') and item.label in ["Accept", "Deny"]:
                    self.view.remove_item(item)

            # Add override button
            override_button = discord.ui.Button(
                label="Override",
                style=discord.ButtonStyle.danger,
                emoji="🔓"
            )

            # Create a callback that shows modal without deferring
            async def override_with_modal(interaction: discord.Interaction):
                # Check if user has the staff role
                if not any(role.id == STAFF_ROLE_ID for role in interaction.user.roles):
                    await interaction.response.send_message(
                        "You don't have permission to override <:Denied:1426930694633816248>",
                        ephemeral=True
                    )
                    return

                # Show modal directly (don't defer!)
                modal = OverrideModal(self.view, thread)  # Pass the thread
                await interaction.response.send_modal(modal)

            override_button.callback = override_with_modal
            self.view.add_item(override_button)
            self.view.override_button = override_button

            # Delete the original message
            original_channel = interaction.message.channel
            await interaction.message.delete()

            # Send new message with user ping
            new_message = await original_channel.send(
                content=f"-# ||{self.view.requester.mention}||",
                embed=embed,
                view=self.view
            )

            # Update the message reference for the thread
            interaction.message = new_message

            # Create thread
            thread_name = f"{self.view.roblox_username}'s Callsign Request" if self.view.roblox_username else f"Callsign Request - {self.view.requester.display_name}"
            thread = await new_message.create_thread(
                name=thread_name[:100],
                auto_archive_duration=1440
            )

            # Create offers embed
            offers_embed = discord.Embed(
                title="Alternative Callsign Offers",
                description=f"The following callsigns have been provided as options for you to choose from:",
                color=discord.Color(0xffffff)
            )

            for i, offer in enumerate(offers, 1):
                offers_embed.add_field(
                    name=f"‎",
                    value=f"`{self.view.fenz_prefix}-{offer}`" if self.view.fenz_prefix else f"`{offer}`",
                    inline=True
                )

            offers_embed.set_footer(text="Select one of the offers below or provide your own alternative.")

            # Create view with offer buttons
            offers_view = CallsignOffersView(
                self.view.requester,
                offers,
                self.view.fenz_prefix,
                self.view.hhstj_prefix,
                self.view.roblox_username,
                self.view.roblox_user_id,
                interaction.user.id,
                interaction.user.display_name,
                self.view.callsign,
                new_message,  # Changed from interaction.message
                thread
            )

            # Ping user OUTSIDE the embed
            await thread.send(
                content=f"{self.view.requester.mention}",
                embed=offers_embed,
                view=offers_view
            )

            await interaction.followup.send("Alternative offers provided successfully <:Accepted:1426930333789585509>", ephemeral=True)

        except Exception as e:
            await send_error_to_owner(interaction.client, "Deny callsign error", e, interaction)
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

class CallsignOffersView(discord.ui.View):
    def __init__(self, requester: discord.Member, offers: list, fenz_prefix: str, hhstj_prefix: str,
                 roblox_username: str, roblox_user_id: str, staff_id: int, staff_name: str,
                 original_callsign: str, original_message: discord.Message, thread: discord.Thread):
        super().__init__(timeout=None)
        self.requester = requester
        self.offers = offers
        self.fenz_prefix = fenz_prefix
        self.hhstj_prefix = hhstj_prefix
        self.roblox_username = roblox_username
        self.roblox_user_id = roblox_user_id
        self.staff_id = staff_id
        self.staff_name = staff_name
        self.original_callsign = original_callsign
        self.original_message = original_message
        self.thread = thread

        # Add buttons for each offer
        for i, offer in enumerate(offers, 1):
            button = discord.ui.Button(
                label=f"{self.fenz_prefix}-{offer}"  if self.fenz_prefix else f"{offer}",
                style=discord.ButtonStyle.primary,
                custom_id=f"offer_{i}_{offer}"
            )
            button.callback = self.create_offer_callback(offer)
            self.add_item(button)

        # Add "Offer something else" button
        other_button = discord.ui.Button(
            label="Offer Something Else",
            style=discord.ButtonStyle.red,
            emoji="<:Denied:1426930694633816248>"
        )
        other_button.callback = self.offer_other_callback
        self.add_item(other_button)

    def create_offer_callback(self, callsign: str):
        async def callback(interaction: discord.Interaction):
            # Only the requester can select an offer
            if interaction.user.id != self.requester.id:
                await interaction.response.send_message(
                    "Only the person who requested the callsign can select an offer <:Denied:1426930694633816248>",
                    ephemeral=True
                )
                return

            await interaction.response.defer()

            # Check if callsign already exists
            existing = await check_callsign_exists(callsign)
            if existing and existing['discord_user_id'] != self.requester.id:
                await interaction.followup.send(
                    f"❌ Callsign `{self.fenz_prefix}-{callsign}` is already occupied by <@{existing['discord_user_id']}>. Please click deny on this request and offer an alternative callsign.",
                    ephemeral=True
                )
                return

            try:
                # Add to database
                await add_callsign_to_database(
                    callsign=callsign,
                    discord_user_id=self.requester.id,
                    discord_username=self.requester.display_name,
                    roblox_user_id=self.roblox_user_id,
                    roblox_username=self.roblox_username,
                    fenz_prefix=self.fenz_prefix,
                    hhstj_prefix=self.hhstj_prefix,
                    approved_by_id=self.staff_id,
                    approved_by_name=self.staff_name
                )

                async with db.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        'SELECT id FROM callsigns WHERE discord_user_id = $1',
                        self.requester.id
                    )
                    callsign_id = row['id'] if row else None

                # Also add to Google Sheets
                try:
                    await sheets_manager.add_callsign_to_sheets(
                        member=self.requester,
                        callsign=callsign,
                        fenz_prefix=self.fenz_prefix if self.fenz_prefix else '',
                        roblox_username=self.roblox_username,
                        discord_id=self.requester.id
                    )
                    print(f"✅ Synced callsign to Google Sheets")
                except Exception as e:
                    print(f"⚠️ Failed to sync to Google Sheets: {e}")

                # Update nickname
                nickname_parts = []
                if self.fenz_prefix:
                    nickname_parts.append(f"{self.fenz_prefix}-{callsign}")
                if self.hhstj_prefix:
                    # If operational management (has hyphen), use as-is, otherwise add to parts
                    if "-" not in self.hhstj_prefix:
                        nickname_parts.append(self.hhstj_prefix)
                if self.roblox_username:
                    nickname_parts.append(self.roblox_username)

                new_nickname = " | ".join(nickname_parts)
                nickname_success = False
                nickname_error = None

                if len(new_nickname) <= 32:
                    try:
                        await self.requester.edit(nick=new_nickname)
                        nickname_success = True
                    except Exception as e:
                        nickname_error = str(e)
                else:
                    # Fallback logic for operational management
                    if self.hhstj_prefix and "-" in self.hhstj_prefix:
                        # Try shortening the OM callsign (e.g., MIKE30 → MKE30)
                        om_parts = self.hhstj_prefix.split("-")
                        if len(om_parts) == 2:
                            base, callsign_num = om_parts
                            # Try shortened version (first 3 letters)
                            shortened_prefix = f"{base[:3]}-{callsign_num}"

                            fallback_parts = []
                            if self.fenz_prefix:
                                fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                            fallback_parts.append(shortened_prefix)
                            if self.roblox_username:
                                fallback_parts.append(self.roblox_username)
                            fallback_nickname = " | ".join(fallback_parts)

                            if len(fallback_nickname) <= 32:
                                try:
                                    await self.requester.edit(nick=fallback_nickname)
                                    nickname_success = True
                                    new_nickname = fallback_nickname
                                except Exception as e:
                                    nickname_error = str(e)
                            else:
                                # Try just the base OM rank (e.g., WOM)
                                fallback_parts = []
                                if self.fenz_prefix:
                                    fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                                fallback_parts.append(base)
                                if self.roblox_username:
                                    fallback_parts.append(self.roblox_username)
                                fallback_nickname = " | ".join(fallback_parts)

                                if len(fallback_nickname) <= 32:
                                    try:
                                        await self.requester.edit(nick=fallback_nickname)
                                        nickname_success = True
                                        new_nickname = fallback_nickname
                                    except Exception as e:
                                        nickname_error = str(e)
                                else:
                                    # Last resort: Drop OM entirely
                                    fallback_parts = []
                                    if self.fenz_prefix:
                                        fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                                    if self.roblox_username:
                                        fallback_parts.append(self.roblox_username)
                                    fallback_nickname = " | ".join(fallback_parts)

                                    if len(fallback_nickname) <= 32:
                                        try:
                                            await self.requester.edit(nick=fallback_nickname)
                                            nickname_success = True
                                            new_nickname = fallback_nickname
                                        except Exception as e:
                                            nickname_error = str(e)
                    else:
                        # Non-OM: just drop HHStJ clinical rank
                        fallback_parts = []
                        if self.fenz_prefix:
                            fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                        if self.roblox_username:
                            fallback_parts.append(self.roblox_username)
                        fallback_nickname = " | ".join(fallback_parts)

                        if len(fallback_nickname) <= 32:
                            try:
                                await self.requester.edit(nick=fallback_nickname)
                                nickname_success = True
                                new_nickname = fallback_nickname
                            except Exception as e:
                                nickname_error = str(e)

                # Update original embed to approved
                embed = discord.Embed(
                    title="Callsign Request",
                    description="This callsign request has been approved.",
                    color=discord.Color.green()
                )

                embed = discord.Embed(
                    title="Callsign Request",
                    colour=discord.Colour(0x2ecc71)
                )

                embed.add_field(
                    name="Requested Callsign",
                    value=f"`{self.fenz_prefix}-{callsign}`" if self.fenz_prefix else f"`{self.callsign}`",
                    inline=True
                )

                embed.add_field(
                    name="User",
                    value=f"{self.requester.mention}",
                    inline=True
                )

                if self.fenz_prefix:
                    # Get the rank name from the map
                    fenz_rank_name = None
                    for role in self.requester.roles:
                        if role.id in FENZ_RANK_MAP:
                            fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                            break

                    embed.add_field(
                        name="FENZ Rank",
                        value=f"{fenz_rank_name}" if fenz_rank_name else f"`{self.fenz_prefix}`",
                        inline=True
                    )

                embed.add_field(name='Status:', value=f'Approved <:Accepted:1426930333789585509>', inline=True)
                embed.add_field(name='Approved at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}',
                                inline=True)

                embed.timestamp = discord.utils.utcnow()
                embed.set_footer(text=f"Approved by {interaction.user.display_name} • {callsign_id}")

                embed.timestamp = discord.utils.utcnow()

                # Delete thread after a short delay
                await interaction.followup.send("Callsign approved <:Accepted:1426930333789585509>, Deleting thread <a:Load:1430912797469970444>", ephemeral=True)

                # Send new message with ping in the callsign channel
                channel = interaction.guild.get_channel(CALLSIGN_REQUEST_CHANNEL_ID)
                if channel:
                    await channel.send(
                        content=f"-# ||{self.requester.mention}||",
                        embed=embed
                    )

                # Delete the original message
                await self.original_message.delete()
                await asyncio.sleep(3)
                await self.thread.delete()

            except Exception as e:
                await send_error_to_owner(interaction.client, "Offer acceptance error", e, interaction)
                error_embed = discord.Embed(
                    title="Error <:Denied:1426930694633816248>",
                    description=f"An error occurred: {str(e)}",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)

        return callback

    async def offer_other_callback(self, interaction: discord.Interaction):
        # Only the requester can offer something else
        if interaction.user.id != self.requester.id:
            await interaction.response.send_message(
                "Only the person who requested the callsign can offer alternatives <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Show modal for counter offer
        modal = CounterOfferModal(self, self.thread, self.original_message)
        await interaction.response.send_modal(modal)

class CallsignRequestView(discord.ui.View):
    def __init__(self, requester: discord.Member, callsign: str, fenz_prefix: str, hhstj_prefix: str,
            roblox_username: str, roblox_user_id: str):
        super().__init__(timeout=None)
        self.requester = requester
        self.callsign = callsign
        self.fenz_prefix = fenz_prefix
        self.hhstj_prefix = hhstj_prefix
        self.roblox_username = roblox_username
        self.roblox_user_id = roblox_user_id

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success, emoji="<:Accepted:1426930333789585509>")
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user has the staff role
        if not any(role.id == STAFF_ROLE_ID for role in interaction.user.roles):
            await interaction.response.send_message(
                "You don't have permission to approve callsigns <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        # Check if callsign already exists
        existing = await check_callsign_exists(callsign)
        if existing and existing['discord_user_id'] != self.requester.id:
            await interaction.followup.send(
                f"❌ Callsign `{self.fenz_prefix}-{callsign}` is already occupied by <@{existing['discord_user_id']}>. Please click deny on this request and offer an alternative callsign.",
                ephemeral=True
            )
            return

        try:
            # Add to database
            await add_callsign_to_database(
                callsign=self.callsign,
                discord_user_id=self.requester.id,
                discord_username=self.requester.display_name,
                roblox_user_id=self.roblox_user_id,
                roblox_username=self.roblox_username,
                fenz_prefix=self.fenz_prefix,
                hhstj_prefix=self.hhstj_prefix,
                approved_by_id=interaction.user.id,
                approved_by_name=interaction.user.display_name
            )

            async with db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT id FROM callsigns WHERE discord_user_id = $1',
                    self.requester.id
                )
                callsign_id = row['id'] if row else None

            try:
                await sheets_manager.add_callsign_to_sheets(
                    member=self.requester,
                    callsign=self.callsign,
                    fenz_prefix=self.fenz_prefix if self.fenz_prefix else '',
                    roblox_username=self.roblox_username,
                    discord_id=self.requester.id
                )
                print(f"✅ Synced callsign to Google Sheets")
            except Exception as e:
                print(f"⚠️ Failed to sync to Google Sheets: {e}")

                # Update nickname
            nickname_parts = []
            if self.fenz_prefix:
                nickname_parts.append(f"{self.fenz_prefix}-{self.callsign}")
            if self.hhstj_prefix:
                # If operational management (has hyphen), use as-is, otherwise add to parts
                if "-" not in self.hhstj_prefix:
                    nickname_parts.append(self.hhstj_prefix)
            if self.roblox_username:
                nickname_parts.append(self.roblox_username)
            new_nickname = " | ".join(nickname_parts)
            nickname_success = False
            nickname_error = None

            if len(new_nickname) <= 32:
                try:
                    await self.requester.edit(nick=new_nickname)
                    nickname_success = True
                except Exception as e:
                    nickname_error = str(e)
            else:
                # Fallback logic for operational management
                if self.hhstj_prefix and "-" in self.hhstj_prefix:
                    # Try shortening the OM callsign (e.g., MIKE30 → MKE30)
                    om_parts = self.hhstj_prefix.split("-")
                    if len(om_parts) == 2:
                        base, callsign_num = om_parts
                        # Try shortened version (first 3 letters)
                        shortened_prefix = f"{base[:3]}-{callsign_num}"

                        fallback_parts = []
                        if self.fenz_prefix:
                            fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                        fallback_parts.append(shortened_prefix)
                        if self.roblox_username:
                            fallback_parts.append(self.roblox_username)
                        fallback_nickname = " | ".join(fallback_parts)

                        if len(fallback_nickname) <= 32:
                            try:
                                await self.requester.edit(nick=fallback_nickname)
                                nickname_success = True
                                new_nickname = fallback_nickname
                            except Exception as e:
                                nickname_error = str(e)
                        else:
                            # Try just the base OM rank (e.g., WOM)
                            fallback_parts = []
                            if self.fenz_prefix:
                                fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                            fallback_parts.append(base)
                            if self.roblox_username:
                                fallback_parts.append(self.roblox_username)
                            fallback_nickname = " | ".join(fallback_parts)

                            if len(fallback_nickname) <= 32:
                                try:
                                    await self.requester.edit(nick=fallback_nickname)
                                    nickname_success = True
                                    new_nickname = fallback_nickname
                                except Exception as e:
                                    nickname_error = str(e)
                            else:
                                # Last resort: Drop OM entirely
                                fallback_parts = []
                                if self.fenz_prefix:
                                    fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                                if self.roblox_username:
                                    fallback_parts.append(self.roblox_username)
                                fallback_nickname = " | ".join(fallback_parts)

                                if len(fallback_nickname) <= 32:
                                    try:
                                        await self.requester.edit(nick=fallback_nickname)
                                        nickname_success = True
                                        new_nickname = fallback_nickname
                                    except Exception as e:
                                        nickname_error = str(e)
                else:
                    # Non-OM: just drop HHStJ clinical rank
                    fallback_parts = []
                    if self.fenz_prefix:
                        fallback_parts.append(f"{self.fenz_prefix}-{callsign}")
                    if self.roblox_username:
                        fallback_parts.append(self.roblox_username)
                    fallback_nickname = " | ".join(fallback_parts)

                    if len(fallback_nickname) <= 32:
                        try:
                            await self.requester.edit(nick=fallback_nickname)
                            nickname_success = True
                            new_nickname = fallback_nickname
                        except Exception as e:
                            nickname_error = str(e)

            # Update embed
            embed = discord.Embed(
                title="Callsign Request",
                colour=discord.Colour(0x2ecc71)
            )

            embed.add_field(
                name="Requested Callsign",
                value=f"`{self.fenz_prefix}-{self.callsign}`" if self.fenz_prefix else f"`{self.callsign}`",
                inline=True
            )

            embed.add_field(
                name="User",
                value=f"{self.requester.mention}",
                inline=True
            )

            if self.fenz_prefix:
                # Get the rank name from the map
                fenz_rank_name = None
                for role in self.requester.roles:
                    if role.id in FENZ_RANK_MAP:
                        fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                        break

                embed.add_field(
                    name="FENZ Rank",
                    value=f"{fenz_rank_name}" if fenz_rank_name else f"`{fenz_prefix}`",
                    inline=True
                )

            embed.add_field(name='Status:', value=f'Approved <:Accepted:1426930333789585509>', inline=True)
            embed.add_field(name='Approved at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}',
                            inline=True)

            embed.timestamp = discord.utils.utcnow()
            embed.set_footer(text=f"Approved by {interaction.user.display_name} • {callsign_id}")

            embed.timestamp = discord.utils.utcnow()

            # Delete the original message
            await interaction.message.delete()

            # Send new message with ping
            await interaction.channel.send(
                content=f"-# ||{self.requester.mention}||",
                embed=embed
            )

            await interaction.followup.send("Callsign approved <:Accepted:1426930333789585509>", ephemeral=True)
        except Exception as e:
            await send_error_to_owner(interaction.client, "Callsign approval error", e, interaction)
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, emoji="<:Denied:1426930694633816248>")
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user has the staff role
        if not any(role.id == STAFF_ROLE_ID for role in interaction.user.roles):
            await interaction.response.send_message(
                "You don't have permission to deny callsigns <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Show modal for deny with offers
        modal = DenyModal(self)
        await interaction.response.send_modal(modal)

    async def override_callback(self, interaction: discord.Interaction):
        """Show modal to enter a new callsign for override approval"""
        # Check if user has the staff role
        if not any(role.id == STAFF_ROLE_ID for role in interaction.user.roles):
            await interaction.response.send_message(
                "You don't have permission to override <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Show modal for new callsign
        modal = OverrideModal(self)
        await interaction.response.send_modal(modal)

        try:
            # Add to database
            await add_callsign_to_database(
                callsign=self.callsign,
                discord_user_id=self.requester.id,
                discord_username=self.requester.display_name,
                roblox_user_id=self.roblox_user_id,
                roblox_username=self.roblox_username,
                fenz_prefix=self.fenz_prefix,
                hhstj_prefix=self.hhstj_prefix,
                approved_by_id=interaction.user.id,
                approved_by_name=interaction.user.display_name
            )

            async with db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT id FROM callsigns WHERE discord_user_id = $1',
                    self.view.requester.id
                )
                callsign_id = row['id'] if row else None

            # Also add to Google Sheets
            try:
                await sheets_manager.add_callsign_to_sheets(
                    member=self.requester,
                    callsign=self.callsign,
                    fenz_prefix=self.fenz_prefix if self.fenz_prefix else '',
                    roblox_username=self.roblox_username,
                    discord_id=self.requester.id
                )
                print(f"✅ Synced callsign to Google Sheets")
            except Exception as e:
                print(f"⚠️ Failed to sync to Google Sheets: {e}")

            # Update nickname (same logic as accept button)
            nickname_parts = []
            if self.fenz_prefix:
                nickname_parts.append(f"{self.fenz_prefix}-{self.callsign}")
            if self.hhstj_prefix:
                if "-" not in self.hhstj_prefix:
                    nickname_parts.append(self.hhstj_prefix)
            if self.roblox_username:
                nickname_parts.append(self.roblox_username)

            new_nickname = " | ".join(nickname_parts)
            nickname_success = False
            nickname_error = None

            if len(new_nickname) <= 32:
                try:
                    await self.requester.edit(nick=new_nickname)
                    nickname_success = True
                except Exception as e:
                    nickname_error = str(e)
            else:
                # Fallback logic for operational management
                if self.hhstj_prefix and "-" in self.hhstj_prefix:
                    om_parts = self.hhstj_prefix.split("-")
                    if len(om_parts) == 2:
                        base, callsign_num = om_parts
                        shortened_prefix = f"{base[:3]}-{callsign_num}"

                        fallback_parts = []
                        if self.fenz_prefix:
                            fallback_parts.append(f"{self.fenz_prefix}-{self.callsign}")
                        fallback_parts.append(shortened_prefix)
                        if self.roblox_username:
                            fallback_parts.append(self.roblox_username)
                        fallback_nickname = " | ".join(fallback_parts)

                        if len(fallback_nickname) <= 32:
                            try:
                                await self.requester.edit(nick=fallback_nickname)
                                nickname_success = True
                                new_nickname = fallback_nickname
                            except Exception as e:
                                nickname_error = str(e)
                        else:
                            fallback_parts = []
                            if self.fenz_prefix:
                                fallback_parts.append(f"{self.fenz_prefix}-{self.callsign}")
                            fallback_parts.append(base)
                            if self.roblox_username:
                                fallback_parts.append(self.roblox_username)
                            fallback_nickname = " | ".join(fallback_parts)

                            if len(fallback_nickname) <= 32:
                                try:
                                    await self.requester.edit(nick=fallback_nickname)
                                    nickname_success = True
                                    new_nickname = fallback_nickname
                                except Exception as e:
                                    nickname_error = str(e)
                            else:
                                fallback_parts = []
                                if self.fenz_prefix:
                                    fallback_parts.append(f"{self.fenz_prefix}-{self.callsign}")
                                if self.roblox_username:
                                    fallback_parts.append(self.roblox_username)
                                fallback_nickname = " | ".join(fallback_parts)

                                if len(fallback_nickname) <= 32:
                                    try:
                                        await self.requester.edit(nick=fallback_nickname)
                                        nickname_success = True
                                        new_nickname = fallback_nickname
                                    except Exception as e:
                                        nickname_error = str(e)
                else:
                    fallback_parts = []
                    if self.fenz_prefix:
                        fallback_parts.append(f"{self.fenz_prefix}-{self.callsign}")
                    if self.roblox_username:
                        fallback_parts.append(self.roblox_username)
                    fallback_nickname = " | ".join(fallback_parts)

                    if len(fallback_nickname) <= 32:
                        try:
                            await self.requester.edit(nick=fallback_nickname)
                            nickname_success = True
                            new_nickname = fallback_nickname
                        except Exception as e:
                            nickname_error = str(e)

            # Update embed to show override approval
            embed = discord.Embed(
                title="Callsign Request",
                colour=discord.Colour(0x2ecc71)
            )

            embed.add_field(
                name="Requested Callsign",
                value=f"`{self.fenz_prefix}-{self.callsign}`" if self.fenz_prefix else f"`{self.callsign}`",
                inline=True
            )

            embed.add_field(
                name="User",
                value=f"{self.requester.mention}",
                inline=True
            )

            if self.fenz_prefix:
                fenz_rank_name = None
                for role in self.requester.roles:
                    if role.id in FENZ_RANK_MAP:
                        fenz_rank_name = FENZ_RANK_MAP[role.id][0]
                        break

                embed.add_field(
                    name="FENZ Rank",
                    value=f"{fenz_rank_name}" if fenz_rank_name else f"`{self.fenz_prefix}`",
                    inline=True
                )

            embed.add_field(name='Status:', value=f'Approved (Override) <:Accepted:1426930333789585509>', inline=True)
            embed.add_field(name='Approved at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}',
                            inline=True)

            embed.timestamp = discord.utils.utcnow()
            embed.set_footer(text=f"Overridden by {interaction.user.display_name} • {callsign_id}")

            # Delete the original message
            await interaction.message.delete()

            # Send new message with ping
            await interaction.channel.send(
                content=f"-# ||{self.requester.mention}||",
                embed=embed
            )

            await interaction.followup.send("Callsign approved via override <:Accepted:1426930333789585509>",
                                            ephemeral=True)

        except Exception as e:
            await send_error_to_owner(interaction.client, "Override approval error", e, interaction)
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

class CallsignCog(commands.Cog):
    # Define command group as CLASS ATTRIBUTE (before __init__)
    callsign_group = app_commands.Group(name="callsign", description="Callsign management commands")

    def __init__(self, bot):
        self.bot=bot

    async def get_roblox_user_info(self, username: str):
        """Get Roblox user ID from username"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        'https://users.roblox.com/v1/usernames/users',
                        json={"usernames": [username], "excludeBannedUsers": True}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('data') and len(data['data']) > 0:
                            user_data = data['data'][0]
                            return {
                                'id': str(user_data['id']),
                                'username': user_data['name'],
                                'display_name': user_data['displayName']
                            }
            return None
        except Exception as e:
            print(f"Error fetching Roblox user: {e}")
            return None

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
                        print(f"Bloxlink API Response: {data}")  # Debug line
                        return {
                            'id': str(data['robloxID'])
                        }
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
                        return {
                            'id': str(data['id']),
                            'username': data['name'],
                            'display_name': data['displayName']
                        }
            return None
        except Exception as e:
            print(f"Error fetching Roblox user from ID: {e}")
            return None

    @callsign_group.command(name="request", description="Request a callsign.")
    @app_commands.describe(callsign="The numeric callsign you want e.g.20 (must be under 1000).")
    async def request_callsign(self, interaction: discord.Interaction, callsign: str):
        await interaction.response.send_message("Processing <a:Load:1430912797469970444>", ephemeral=True)
        # Get Roblox data from Bloxlink
        bloxlink_data = await self.get_bloxlink_data(interaction.user.id, interaction.guild.id)

        if not bloxlink_data:
            error_embed = discord.Embed(
                title="Not Linked <:Denied:1426930694633816248>",
                description="You must link your Roblox account with Bloxlink first!\nUse `/verify` to link your account.",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(embed=error_embed, ephemeral=True)
            return

        roblox_user_id = bloxlink_data['id']
        roblox_data = await self.get_roblox_user_from_id(roblox_user_id)

        if not roblox_data:
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description="Could not fetch your Roblox profile. Please try again later.",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return

        roblox_username = roblox_data['username']

        # Validate callsign is numeric
        if not callsign.isdigit():
            error_embed = discord.Embed(
                title="Invalid Callsign <:Denied:1426930694633816248>",
                description="Callsign must be numeric only (e.g., 101, 202, 303)",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return
            # ADD THIS: Validate callsign is not over 999
        if int(callsign) > 999:
            error_embed = discord.Embed(
                title="Invalid Callsign <:Denied:1426930694633816248>",
                description="Callsign must be 999 or below (e.g., 101, 202, 999)",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return

        # Find FENZ prefix
        fenz_prefix = None
        for role in interaction.user.roles:
            if role.id in FENZ_RANK_MAP:
                fenz_prefix = FENZ_RANK_MAP[role.id][1]
                break

        # Find HHStJ prefix
        hhstj_prefix = None
        for role in interaction.user.roles:
            if role.id in HHSTJ_RANK_MAP:
                hhstj_prefix = HHSTJ_RANK_MAP[role.id][1]
                break

        # Verify user has at least one rank
        if not fenz_prefix and not hhstj_prefix:
            error_embed = discord.Embed(
                title="No Rank Role <:Denied:1426930694633816248>",
                description="You must have a FENZ or HHStJ rank role to request a callsign!",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return

        # Build the request embed
        request_embed = discord.Embed(
            title="Callsign Request",
            color=discord.Color(0xffffff)
        )

        request_embed.add_field(
            name="Requested Callsign",
            value=f"`{fenz_prefix}-{callsign}`" if fenz_prefix else f"`{callsign}`",
            inline=True
        )

        request_embed.add_field(
            name="User",
            value=f"{interaction.user.mention}",
            inline=True
        )

        if fenz_prefix:
            # Get the rank name from the map
            fenz_rank_name = None
            for role in interaction.user.roles:
                if role.id in FENZ_RANK_MAP:
                    fenz_rank_name = FENZ_RANK_MAP[role.id][0]

        request_embed.add_field(
            name="FENZ Rank",
            value=f"{fenz_rank_name}" if fenz_rank_name else f"`{fenz_prefix}`",
            inline=True
        )

        request_embed.add_field(name='Status:', value=f'Awaiting Review <a:Load:1430912797469970444>', inline=True)
        request_embed.add_field(name='Requested at:', value=f'{discord.utils.format_dt(discord.utils.utcnow())}',
                                inline=True)

        request_embed.timestamp = discord.utils.utcnow()
        request_embed.set_footer(text=f"Requested by {interaction.user.display_name}")

        # Create the view
        view = CallsignRequestView(
            interaction.user,
            callsign,
            fenz_prefix,
            hhstj_prefix,
            roblox_username,
            roblox_user_id
        )

        # Get the request channel
        channel = interaction.guild.get_channel(CALLSIGN_REQUEST_CHANNEL_ID)

        if not channel:
            error_embed = discord.Embed(
                title="Configuration Error <:Denied:1426930694633816248>",
                description="Callsign request channel not found. Please contact an administrator.",
                color=discord.Color.red()
            )

            await interaction.followup.send(embed=error_embed, ephemeral=True)


        # Send to channel
        await channel.send(
            content=f"-# ||<@&{STAFF_ROLE_ID}>||",
            embed=request_embed,
            view=view
        )

        success_embed = discord.Embed(
            title="Request Submitted <:Accepted:1426930333789585509>",
            description=f"Your callsign request for `{fenz_prefix}-{callsign}` has been submitted!" if fenz_prefix else f"Your callsign request for `{callsign}` has been submitted!",
            color=discord.Color.green()
        )

        success_embed.add_field(
            name="What's Next?",
            value="<@&1285474077556998196> will review your request. You'll be notified once it's been processed.",
            inline=False
        )
        await interaction.edit_original_response(content=None, embed=success_embed)
        return

    @callsign_group.command(name="sync", description="Sync callsigns between Google Sheets and database (Admin only)")
    async def sync_callsigns(self, interaction: discord.Interaction):
        """Sync callsigns between Google Sheets and database, treating sheets as source of truth"""

        # Check if user has the specific sync role
        SYNC_ROLE_ID = 1389550689113473024
        if not any(role.id == SYNC_ROLE_ID for role in interaction.user.roles):
            await interaction.response.send_message(
                "You don't have permission to use this command <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            "Starting sync between Google Sheets and database... <a:Load:1430912797469970444>",
            ephemeral=True
        )

        try:
            # Authenticate with Google Sheets
            if not sheets_manager.client:
                auth_success = sheets_manager.authenticate()
                if not auth_success:
                    await interaction.edit_original_response(
                        content="❌ Failed to authenticate with Google Sheets"
                    )
                    return

            # Get both worksheets
            non_command_sheet = sheets_manager.get_worksheet("Non-Command")
            command_sheet = sheets_manager.get_worksheet("Command")

            if not non_command_sheet or not command_sheet:
                await interaction.edit_original_response(
                    content="❌ Could not access worksheets"
                )
                return

            # Track sync statistics
            stats = {
                'sheets_total': 0,
                'db_total': 0,
                'added_to_db': 0,
                'updated_in_db': 0,
                'missing_from_sheets': 0,
                'errors': []
            }

            # Get all database entries
            async with db.pool.acquire() as conn:
                db_rows = await conn.fetch('SELECT * FROM callsigns')
                db_entries = {row['discord_user_id']: dict(row) for row in db_rows}
                stats['db_total'] = len(db_entries)

            # Process Non-Command sheet
            non_command_data = non_command_sheet.get_all_values()
            for i, row in enumerate(non_command_data[1:], start=2):  # Skip header
                if not row or all(cell == '' for cell in row):
                    continue

                try:
                    # Non-Command columns: A=Full callsign, B=FENZ Prefix, C=Callsign, D=Roblox, F=Strikes, G=Discord ID, H=Rank#, I=Quals
                    if len(row) < 7:  # Need at least up to column G
                        continue

                    full_callsign = row[0] if len(row) > 0 else ''
                    fenz_prefix = row[1] if len(row) > 1 else ''
                    callsign = row[2] if len(row) > 2 else ''
                    roblox_username = row[3] if len(row) > 3 else ''
                    strikes = row[5] if len(row) > 5 else ''  # ← ADD THIS: Column F (index 5)
                    discord_id_str = row[6] if len(row) > 6 else ''
                    rank_number = row[7] if len(row) > 7 else ''  # ← ADD THIS: Column H (index 7)
                    qualifications = row[8] if len(row) > 8 else ''  # ← ADD THIS: Column I (index 8)

                    # Skip if missing essential data
                    if not callsign or not discord_id_str:
                        continue

                    try:
                        discord_id = int(discord_id_str)
                    except ValueError:
                        stats['errors'].append(f"Invalid Discord ID in Non-Command row {i}: {discord_id_str}")
                        continue

                    stats['sheets_total'] += 1

                    # Check if exists in database
                    if discord_id in db_entries:
                        db_entry = db_entries[discord_id]

                        # Check if needs updating (only update fields from sheets)
                        needs_update = False
                        updates = {}

                        if db_entry['callsign'] != callsign:
                            updates['callsign'] = callsign
                            needs_update = True

                        if db_entry['fenz_prefix'] != fenz_prefix:
                            updates['fenz_prefix'] = fenz_prefix
                            needs_update = True

                        if roblox_username and db_entry['roblox_username'] != roblox_username:
                            updates['roblox_username'] = roblox_username
                            needs_update = True

                        if needs_update:
                            # Update database
                            async with db.pool.acquire() as conn:
                                set_clauses = []
                                values = []
                                param_num = 1

                                for key, value in updates.items():
                                    set_clauses.append(f"{key} = ${param_num}")
                                    values.append(value)
                                    param_num += 1

                                values.append(discord_id)
                                query = f"UPDATE callsigns SET {', '.join(set_clauses)} WHERE discord_user_id = ${param_num}"
                                await conn.execute(query, *values)

                            stats['updated_in_db'] += 1

                        # Remove from tracking dict (so we know what's left)
                        del db_entries[discord_id]

                    else:
                        # Add new entry to database
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                '''INSERT INTO callsigns
                                   (callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
                                    fenz_prefix, hhstj_prefix, approved_by_id, approved_by_name, callsign_history)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                                           $10) ON CONFLICT (callsign) DO NOTHING''',
                                callsign,
                                discord_id,
                                '',  # Leave blank - will be filled when user interacts
                                '',  # Leave blank
                                roblox_username if roblox_username else '',
                                fenz_prefix if fenz_prefix else '',
                                '',  # Leave blank - not in sheets
                                interaction.user.id,
                                f"Synced from Sheets by {interaction.user.display_name}",
                                '[]'
                            )

                        stats['added_to_db'] += 1

                except Exception as e:
                    stats['errors'].append(f"Error processing Non-Command row {i}: {str(e)}")

            # Process Command sheet
            command_data = command_sheet.get_all_values()
            for i, row in enumerate(command_data[1:], start=2):  # Skip header
                if not row or all(cell == '' for cell in row):
                    continue

                try:
                    # Command columns: A=Full callsign, B=Roblox, C=Quals, D=Strikes, E=Discord ID
                    if len(row) < 5:  # Need at least up to column E
                        continue

                    full_callsign = row[0] if len(row) > 0 else ''
                    roblox_username = row[1] if len(row) > 1 else ''
                    qualifications = row[2] if len(row) > 2 else ''  # ← ADD THIS: Column C (index 2)
                    strikes = row[3] if len(row) > 3 else ''  # ← ADD THIS: Column D (index 3)
                    discord_id_str = row[4] if len(row) > 4 else ''

                    # Skip if missing essential data
                    if not full_callsign or not discord_id_str:
                        continue

                    # Extract callsign from full callsign (e.g., "SO-123" -> "123" OR just "DNC" -> "DNC")
                    callsign_parts = full_callsign.split('-')

                    # Check if it's a standalone prefix (DNC, ANC, NC) or prefix-number format
                    if len(callsign_parts) == 1:
                        # Standalone prefix (e.g., "DNC", "ANC", "NC")
                        fenz_prefix = callsign_parts[0]
                        callsign = ''  # No callsign number for top ranks
                    elif len(callsign_parts) == 2:
                        # Normal format (e.g., "SO-123")
                        fenz_prefix = callsign_parts[0]
                        callsign = callsign_parts[1]

                        # Validate callsign is numeric
                        if not callsign.isdigit():
                            stats['errors'].append(f"Skipping non-numeric callsign in Command row {i}: {full_callsign}")
                            continue
                    else:
                        # Invalid format
                        stats['errors'].append(
                            f"Skipping invalid format in Command row {i}: {full_callsign}")
                        continue

                    rank_priority = COMMAND_RANK_PRIORITY.get(fenz_prefix, 99)

                    try:
                        discord_id = int(discord_id_str)
                    except ValueError:
                        stats['errors'].append(f"Invalid Discord ID in Command row {i}: {discord_id_str}")
                        continue

                    stats['sheets_total'] += 1

                    # Check if exists in database
                    if discord_id in db_entries:
                        db_entry = db_entries[discord_id]

                        # Check if needs updating
                        needs_update = False
                        updates = {}

                        if db_entry['callsign'] != callsign:
                            updates['callsign'] = callsign
                            needs_update = True

                        if db_entry['fenz_prefix'] != fenz_prefix:
                            updates['fenz_prefix'] = fenz_prefix
                            needs_update = True

                        if roblox_username and db_entry['roblox_username'] != roblox_username:
                            updates['roblox_username'] = roblox_username
                            needs_update = True

                        if needs_update:
                            # Update database
                            async with db.pool.acquire() as conn:
                                set_clauses = []
                                values = []
                                param_num = 1

                                for key, value in updates.items():
                                    set_clauses.append(f"{key} = ${param_num}")
                                    values.append(value)
                                    param_num += 1

                                values.append(discord_id)
                                query = f"UPDATE callsigns SET {', '.join(set_clauses)} WHERE discord_user_id = ${param_num}"
                                await conn.execute(query, *values)

                            stats['updated_in_db'] += 1

                        # Remove from tracking dict
                        del db_entries[discord_id]

                    else:
                        # Add new entry to database
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                '''INSERT INTO callsigns
                                   (callsign, discord_user_id, discord_username, roblox_user_id, roblox_username,
                                    fenz_prefix, hhstj_prefix, approved_by_id, approved_by_name, callsign_history)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                                           $10) ON CONFLICT (callsign) DO NOTHING''',
                                callsign,
                                discord_id,
                                '',  # Leave blank
                                '',  # Leave blank
                                roblox_username if roblox_username else '',
                                fenz_prefix if fenz_prefix else '',
                                '',  # Leave blank
                                interaction.user.id,
                                f"Synced from Sheets by {interaction.user.display_name}",
                                '[]'
                            )

                        stats['added_to_db'] += 1

                except Exception as e:
                    stats['errors'].append(f"Error processing Command row {i}: {str(e)}")

            # After processing both sheets, add missing entries back to sheets
            for discord_id, entry in db_entries.items():
                try:
                    # Fetch the member to get their roles
                    member = await interaction.guild.fetch_member(discord_id)

                    if member:
                        # Use the sheets_manager to add them
                        await sheets_manager.add_callsign_to_sheets(
                            member=member,
                            callsign=entry['callsign'],
                            fenz_prefix=entry['fenz_prefix'] if entry['fenz_prefix'] else '',
                            roblox_username=entry['roblox_username'],
                            discord_id=discord_id
                        )
                        stats['added_to_sheets'] = stats.get('added_to_sheets', 0) + 1
                    else:
                        stats['errors'].append(f"Could not fetch member {discord_id}")
                except Exception as e:
                    stats['errors'].append(f"Error adding {discord_id} to sheets: {str(e)}")

            # Remaining entries in db_entries are missing from sheets
            stats['missing_from_sheets'] = len(db_entries)

            # Check for rank mismatches and fix them
            print("Checking for rank mismatches...")
            stats['rank_mismatches_fixed'] = 0

            # Re-fetch all data to check for mismatches
            non_command_data = non_command_sheet.get_all_values()
            command_data = command_sheet.get_all_values()

            # Check Non-Command sheet
            for i, row in enumerate(non_command_data[1:], start=2):
                if not row or all(cell == '' for cell in row):
                    continue

                try:
                    discord_id_str = row[6] if len(row) > 6 else ''
                    if not discord_id_str:
                        continue

                    discord_id = int(discord_id_str)
                    member = await interaction.guild.fetch_member(discord_id)

                    if not member:
                        continue

                    current_fenz_prefix = row[1] if len(row) > 1 else ''
                    has_mismatch, correct_prefix, correct_rank_type = sheets_manager.detect_rank_mismatch(
                        member.roles, current_fenz_prefix
                    )

                    if has_mismatch:
                        # Update database
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                'UPDATE callsigns SET fenz_prefix = $1, callsign = $2 WHERE discord_user_id = $3',
                                correct_prefix, '###', discord_id
                            )

                        # If they need to move to Command sheet
                        if correct_rank_type == 'command':
                            # Delete from Non-Command
                            sheets_manager.delete_row(non_command_sheet, i)

                            # Add to Command sheet
                            await sheets_manager.add_callsign_to_sheets(
                                member=member,
                                callsign='###',
                                fenz_prefix=correct_prefix,
                                roblox_username=row[3] if len(row) > 3 else '',
                                discord_id=discord_id
                            )
                        else:
                            # Update in Non-Command sheet
                            non_command_sheet.update_cell(i, 2, correct_prefix)  # Update prefix
                            non_command_sheet.update_cell(i, 3, '###')  # Set callsign to ###
                            non_command_sheet.update_cell(i, 1, f"{correct_prefix}-###")  # Update full callsign

                        stats['rank_mismatches_fixed'] += 1
                        stats['errors'].append(
                            f"Fixed rank mismatch for <@{discord_id}>: {current_fenz_prefix} → {correct_prefix}")

                except Exception as e:
                    stats['errors'].append(f"Error checking Non-Command mismatch row {i}: {str(e)}")

            # Check Command sheet
            for i, row in enumerate(command_data[1:], start=2):
                if not row or all(cell == '' for cell in row):
                    continue

                try:
                    discord_id_str = row[4] if len(row) > 4 else ''
                    if not discord_id_str:
                        continue

                    discord_id = int(discord_id_str)
                    member = await interaction.guild.fetch_member(discord_id)

                    if not member:
                        continue

                    full_callsign = row[0] if len(row) > 0 else ''
                    callsign_parts = full_callsign.split('-')
                    current_fenz_prefix = callsign_parts[0] if callsign_parts else ''

                    has_mismatch, correct_prefix, correct_rank_type = sheets_manager.detect_rank_mismatch(
                        member.roles, current_fenz_prefix
                    )

                    if has_mismatch:
                        # Update database
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                'UPDATE callsigns SET fenz_prefix = $1, callsign = $2 WHERE discord_user_id = $3',
                                correct_prefix, '###', discord_id
                            )

                        # If they need to move to Non-Command sheet
                        if correct_rank_type == 'non-command':
                            # Delete from Command
                            sheets_manager.delete_row(command_sheet, i)

                            # Add to Non-Command sheet
                            await sheets_manager.add_callsign_to_sheets(
                                member=member,
                                callsign='###',
                                fenz_prefix=correct_prefix,
                                roblox_username=row[1] if len(row) > 1 else '',
                                discord_id=discord_id
                            )
                        else:
                            # Update in Command sheet
                            command_sheet.update_cell(i, 1, f"{correct_prefix}-###")  # Update full callsign
                            # Update rank priority
                            rank_priority = COMMAND_RANK_PRIORITY.get(correct_prefix, 99)
                            command_sheet.update_cell(i, 6, rank_priority)

                        stats['rank_mismatches_fixed'] += 1
                        stats['errors'].append(
                            f"Fixed rank mismatch for <@{discord_id}>: {current_fenz_prefix} → {correct_prefix}")

                except Exception as e:
                    stats['errors'].append(f"Error checking Command mismatch row {i}: {str(e)}")

            # Sort the worksheets
            print("Sorting worksheets...")

            # Non-Command: Sort by Rank# (Column H), then Callsign (Column C)
            sheets_manager.sort_worksheet_multi(non_command_sheet, [
                {'column': 8, 'order': 'ASCENDING'},  # Rank# first (SFF=1 at top)
                {'column': 3, 'order': 'ASCENDING'}  # Then callsign (-1 at top)
            ])

            # Command: Sort by Rank Priority (Column F), then Callsign (Column C if exists)
            sheets_manager.sort_worksheet_multi(command_sheet, [
                {'column': 6, 'order': 'ASCENDING'},  # Rank priority (NC=1 at top)
                {'column': 1, 'order': 'ASCENDING'}  # Then full callsign for ties
            ])

            # Build result embed
            result_embed = discord.Embed(
                title="Sync Complete ✅",
                description="Two-way sync between Google Sheets and database completed.",
                color=discord.Color.green()
            )

            result_embed.add_field(
                name="📊 Statistics",
                value=f"**Sheets Total:** {stats['sheets_total']}\n"
                      f"**Database Total (before):** {stats['db_total']}\n"
                      f"**Added to DB:** {stats['added_to_db']}\n"
                      f"**Updated in DB:** {stats['updated_in_db']}\n"
                      f"**Rank Mismatches Fixed:** {stats['rank_mismatches_fixed']}\n"  # ← ADD THIS
                      f"**Missing from Sheets:** {stats['missing_from_sheets']}",
                inline=False
            )

            if stats['missing_from_sheets'] > 0:
                missing_list = []
                for discord_id, entry in list(db_entries.items())[:10]:  # Show first 10
                    missing_list.append(f"• {entry['fenz_prefix']}-{entry['callsign']} (<@{discord_id}>)")

                result_embed.add_field(
                    name="⚠️ In Database but Not in Sheets",
                    value='\n'.join(missing_list) +
                          (f"\n*...and {stats['missing_from_sheets'] - 10} more*" if stats[
                                                                                         'missing_from_sheets'] > 10 else ""),
                    inline=False
                )

            if stats['errors']:
                error_list = '\n'.join(stats['errors'][:5])  # Show first 5 errors
                result_embed.add_field(
                    name="❌ Errors",
                    value=error_list +
                          (f"\n*...and {len(stats['errors']) - 5} more errors*" if len(stats['errors']) > 5 else ""),
                    inline=False
                )

            result_embed.timestamp = discord.utils.utcnow()
            result_embed.set_footer(text=f"Synced by {interaction.user.display_name}")

            await interaction.edit_original_response(content=None, embed=result_embed)

        except Exception as e:
            error_embed = discord.Embed(
                title="Sync Failed ❌",
                description=f"An error occurred during sync:\n```{str(e)}```",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(content=None, embed=error_embed)
            print(f"Sync error: {e}")
            import traceback
            traceback.print_exc()


async def setup(bot):
    """This function is called when the cog is loaded"""
    print("🔄 Loading CallsignCog...")
    try:
        cog = CallsignCog(bot)
        await bot.add_cog(cog)
        print(f"✅ CallsignCog loaded successfully with {len(cog.callsign_group.commands)} commands")

        # List the commands
        for cmd in cog.callsign_group.commands:
            print(f"   • /callsign {cmd.name}")
    except Exception as e:
        print(f"❌ Failed to load CallsignCog: {e}")
        import traceback
        traceback.print_exc()
        raise
