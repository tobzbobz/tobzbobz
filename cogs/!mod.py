import discord
from discord.ext import commands
from datetime import datetime
from database import db  # Import database instance

MODERATOR_ROLES = {
    1282916959062851634: 1389550689113473024,  # guild_id: role_id (replace with actual IDs)
    1425867713183744023: None
    # Add more guilds as needed
}

IGNORED_WATCH_IDS = {1, 2, 3, 4, 5, 6, 7}

OWNER_ID = 678475709257089057

class ModActionSelect(discord.ui.Select):

    def __init__(self, cog):
        self.cog = cog
        options = [
            discord.SelectOption(label="Kick", description="Kick a member from the server", emoji="👢"),
            discord.SelectOption(label="Ban", description="Ban a member from the server", emoji="🔨"),
            discord.SelectOption(label="Educational Note", description="Give an educational note", emoji="📝"),
        ]
        super().__init__(
            placeholder="Select a moderation action...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.view.command_user_id:
            await interaction.response.send_message("This menu is not for you!", ephemeral=True)
            return

        selected_action = self.values[0]
        self.view.selected_action = selected_action

        if selected_action == "Kick":
            offence_view = KickOffenceSelectView(selected_action, interaction.user, self.cog)
        elif selected_action == "Ban":
            offence_view = BanOffenceSelectView(selected_action, interaction.user, self.cog)
        else:
            offence_view = OffenceSelectView(selected_action, interaction.user, self.cog)

        embed = discord.Embed(
            title=f"{selected_action} - Select Offence",
            description="Choose the reason for this action:",
            color=discord.Color.orange()
        )

        # Delete the original message and send the new one
        await interaction.response.send_message(embed=embed, view=offence_view, ephemeral=True)
        await interaction.message.delete()

class BanOffenceSelect(discord.ui.Select):
    def __init__(self, action_type, moderator, cog):
        self.action_type = action_type
        self.moderator = moderator
        self.cog = cog
        options = [
            discord.SelectOption(label="SRDM (Staff RDM)", emoji="<:Warn:1437771973970104471>"),
            discord.SelectOption(label="NSFW", emoji="🔞"),
            discord.SelectOption(label="Glitching", emoji="🐛"),
            discord.SelectOption(label="LTAP (Leaving to Avoid Punishment)", emoji="🚪"),
            discord.SelectOption(label="Breaking Roblox TOS", emoji="📜"),
            discord.SelectOption(label="Underage (Discord)", emoji="🔞"),
            discord.SelectOption(label="Mass Vehicle Deathmatch (VDM)", emoji="🚗"),
            discord.SelectOption(label="Mass Random Deathmatch (RDM)", emoji="💀"),
            discord.SelectOption(label="Racism/Homophobia", emoji="🚫"),
            discord.SelectOption(label="Rejoining After Kick", emoji="🔄"),
        ]
        super().__init__(
            placeholder="Select an offence...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_offence = self.values[0]
        self.view.selected_offence = selected_offence
        user_view = BanUserSelectView(self.action_type, selected_offence, self.moderator, self.cog)

        embed = discord.Embed(
            title="Select User to Ban",
            description="Choose the user to ban:",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed, view=user_view, ephemeral=True)


class BanUserSelect(discord.ui.UserSelect):
    def __init__(self, action_type, offence, moderator, cog):
        self.action_type = action_type
        self.offence = offence
        self.moderator = moderator
        self.cog = cog
        super().__init__(placeholder="Select a user to ban...", min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        selected_user = self.values[0]

        try:
            modal = BanDetailsModal(self.action_type, self.offence, selected_user, self.moderator, self.view, self.cog)
            await interaction.response.send_modal(modal)
        except discord.InteractionResponded:
            # User already responded to the interaction or cancelled, do nothing
            pass
        except Exception as e:
            # Handle any other errors gracefully
            try:
                await interaction.response.send_message("An error occurred.", ephemeral=True)
            except:
                pass

class BanUserSelectView(discord.ui.View):
    def __init__(self, action_type, offence, moderator, cog):  # ADD cog parameter
        super().__init__(timeout=300)
        self.action_type = action_type
        self.offence = offence
        self.moderator = moderator
        self.cog = cog  # ADD THIS LINE
        self.add_item(BanUserSelect(action_type, offence, moderator, cog))  # ADD cog here

class BanDetailsModal(discord.ui.Modal):
    def __init__(self, action_type, offence, user, moderator, parent_view, cog):
        super().__init__(title=f"{action_type} Details")
        self.action_type = action_type
        self.offence = offence
        self.user = user
        self.moderator = moderator
        self.parent_view = parent_view
        self.cog = cog

        self.additional_info = discord.ui.TextInput(
            label="Additional Information (Optional)",
            placeholder="Add any extra details...",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=500
        )
        self.add_item(self.additional_info)

    async def on_submit(self, interaction: discord.Interaction):
        # Send the ban DM here, after form is submitted
        try:
            embed = discord.Embed(
                title="Ban Notification",
                description=f"You are being banned from Hamilton New Zealand Roleplay for {self.offence}.",
                color=discord.Color.red()
            )
            embed.add_field(
                name="Appeal Information",
                value="The ban appeal link can be found in the support channel.",
                inline=False
            )
            embed.add_field(
                name="Communications Server Code",
                value="hnzrp",
                inline=False
            )
            await self.user.send(embed=embed)
            dm_status = "<:Accepted:1426930333789585509> Banned successfully"
        except discord.Forbidden:
            dm_status = "<:Denied:1426930694633816248> Ban failed (DMs closed)"
        except Exception as e:
            dm_status = "<:Denied:1426930694633816248> Ban failed"

        await db.log_action(
            guild_id=interaction.guild.id,
            user_id=self.user.id,
            action='ban',
            details={
                'user': str(self.user),
                'reason': self.offence,
                'duration': 'Permanent',
                'additional_info': self.additional_info.value or 'None',
                'moderator': str(self.moderator),
                'moderator_id': self.moderator.id,
                'dm_status': dm_status
            }
        )

        await self.cog.reset_user_infractions(interaction.guild.id, self.user.id)

        embed = discord.Embed(
            title="Ban Action Summary",
            color=discord.Color.red()
        )

        value_text = f"**User:** {self.user.mention}\n**Offence:** {self.offence}\n**Duration:** Permanent\n**DM Status:** {dm_status}"
        if self.additional_info.value:
            value_text += f"\n**Additional Info:** {self.additional_info.value}"

        embed.add_field(
            name=f"{self.action_type}",
            value=value_text,
            inline=False
        )

        embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        await interaction.response.send_message(embed=embed, ephemeral=True)

class KickOffenceSelect(discord.ui.Select):
    def __init__(self, action_type, moderator, cog):  # ADD cog parameter
        self.action_type = action_type
        self.moderator = moderator
        self.cog = cog  # ADD THIS LINE
        options = [
            discord.SelectOption(label="RDM (Random Deathmatch)", emoji="💀"),
            discord.SelectOption(label="SZRDM (Safezone)", emoji="🛡️"),
            discord.SelectOption(label="VDM (Vehicle Deathmatch)", emoji="🚗"),
            discord.SelectOption(label="SVDM (Staff)", emoji="<:Warn:1437771973970104471>"),
            discord.SelectOption(label="GTAD (GTA Driving)", emoji="🏎️"),
            discord.SelectOption(label="Staff Evasion", emoji="🏃"),
            discord.SelectOption(label="Trolling", emoji="🤡"),
            discord.SelectOption(label="NITRP (No Intention to Roleplay)", emoji="<:Denied:1426930694633816248>"),
            discord.SelectOption(label="RTAP (Respawning to Avoid Punishment)", emoji="🔄"),
            discord.SelectOption(label="Staff Impersonation", emoji="👮"),
            discord.SelectOption(label="Banned RP", emoji="🚫"),
        ]
        super().__init__(
            placeholder="Select an offence...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_offence = self.values[0]
        user_view = KickUserSelectView(self.action_type, selected_offence, self.moderator, self.cog)
        embed = discord.Embed(
            title="Select User to Kick",
            description="Choose the user to kick:",
            color=discord.Color.orange()
        )
        await interaction.response.send_message(embed=embed, view=user_view, ephemeral=True)


class KickUserSelect(discord.ui.UserSelect):
    def __init__(self, action_type, offence, moderator, cog):
        self.action_type = action_type
        self.offence = offence
        self.moderator = moderator
        self.cog = cog
        super().__init__(placeholder="Select a user to kick...", min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        selected_user = self.values[0]

        try:
            modal = KickTimeModal(self.action_type, self.offence, selected_user, self.moderator, self.view, self.cog)
            await interaction.response.send_modal(modal)
        except discord.InteractionResponded:
            pass
        except Exception as e:
            try:
                await interaction.response.send_message("An error occurred.", ephemeral=True)
            except:
                pass

class KickUserSelectView(discord.ui.View):
    def __init__(self, action_type, offence, moderator, cog):  # ADD cog parameter
        super().__init__(timeout=300)
        self.action_type = action_type
        self.offence = offence
        self.moderator = moderator
        self.cog = cog  # ADD THIS LINE
        self.add_item(KickUserSelect(action_type, offence, moderator, cog))  # ADD cog here


class KickTimeModal(discord.ui.Modal):
    def __init__(self, action_type, offence, user, moderator, parent_view, cog):
        super().__init__(title="Kick Time")
        self.action_type = action_type
        self.offence = offence
        self.user = user
        self.moderator = moderator
        self.parent_view = parent_view
        self.cog = cog

        self.kick_time = discord.ui.TextInput(
            label="Kick Duration",
            placeholder="E.g., 30 minutes, 2 hours, full session",
            style=discord.TextStyle.short,
            required=True,
            max_length=100
        )
        self.add_item(self.kick_time)

    async def on_submit(self, interaction: discord.Interaction):
        # <:Accepted:1426930333789585509> Get recent kicks from database (last 24 hours)
        recent_kicks = await db.get_recent_logs(
            guild_id=interaction.guild.id,
            action_type='kick',
            user_id=self.user.id,
            hours=24
        )

        # Filter by specific reason
        recent_kicks = [
            log for log in recent_kicks
            if log.get('details', {}).get('reason') == self.offence
        ]

        # Get all recent warnings
        all_recent_warnings = await db.get_recent_logs(
            guild_id=interaction.guild.id,
            action_type='educational_note',
            user_id=self.user.id,
            hours=24
        )

        # Filter to only warnings (not notes)
        all_recent_warnings = [
            log for log in all_recent_warnings
            if log.get('details', {}).get('infraction_type') == 'warning'
        ]

        kick_count = len(recent_kicks)
        total_warnings = len(all_recent_warnings)

        special_kicks = ["Banned RP", "Staff Impersonation", "RTAP (Respawning to Avoid Punishment)"]
        extended_kicks = ["RDM (Random Deathmatch)", "VDM (Vehicle Deathmatch)"]

        is_special_kick = self.offence in special_kicks
        is_extended_kick = self.offence in extended_kicks

        # 2nd offense for special kicks = ban
        if is_special_kick and kick_count >= 1:
            try:
                embed = discord.Embed(
                    title="Ban Notification",
                    description=f"You are being banned from Hamilton New Zealand Roleplay for 2 counts of {self.offence}.",
                    color=discord.Color.red()
                )
                embed.add_field(
                    name="Appeal Information",
                    value="The ban appeal link can be found in the support channel.",
                    inline=False
                )
                embed.add_field(
                    name="Communications Server Code",
                    value="hnzrp",
                    inline=False
                )
                await self.user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Banned successfully (2nd offense)"
            except:
                dm_status = "<:Denied:1426930694633816248> Ban failed"

            # <:Accepted:1426930333789585509> Save to database
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=self.user.id,
                action='ban',
                details={
                    'user': str(self.user),
                    'reason': f"2 counts of {self.offence}",
                    'duration': 'Permanent (2nd offense)',
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'infraction_type': 'ban'
                }
            )

            # Reset infractions
            if self.cog:
                await self.cog.reset_user_infractions(interaction.guild.id, self.user.id)

            embed = discord.Embed(
                title="Ban Action Summary (2nd Offense)",
                color=discord.Color.dark_red()
            )
            value_text = f"**User:** {self.user.mention}\n**Offence:** 2 counts of {self.offence}\n**Duration:** Permanent\n**DM Status:** {dm_status}\n**Note:** User was kicked for same offense within 24h"
            embed.add_field(name="Ban", value=value_text, inline=False)
            embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # 4th offense for extended kicks = ban
        elif is_extended_kick and kick_count >= 3:
            try:
                embed = discord.Embed(
                    title="Ban Notification",
                    description=f"You are being banned from Hamilton New Zealand Roleplay for {self.offence}.",
                    color=discord.Color.red()
                )
                embed.add_field(
                    name="Appeal Information",
                    value="The ban appeal link can be found in the support channel.",
                    inline=False
                )
                embed.add_field(
                    name="Communications Server Code",
                    value="hnzrp",
                    inline=False
                )
                await self.user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Banned successfully (4th offense)"
            except:
                dm_status = "<:Denied:1426930694633816248> Ban failed"

            # <:Accepted:1426930333789585509> Save to database
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=self.user.id,
                action='ban',
                details={
                    'user': str(self.user),
                    'reason': self.offence,
                    'duration': 'Permanent (4th offense)',
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'infraction_type': 'ban'
                }
            )

            if self.cog:
                await self.cog.reset_user_infractions(interaction.guild.id, self.user.id)

            embed = discord.Embed(
                title="Ban Action Summary (4th Offense)",
                color=discord.Color.dark_red()
            )
            value_text = f"**User:** {self.user.mention}\n**Offence:** {self.offence}\n**Duration:** Permanent\n**DM Status:** {dm_status}\n**Note:** User was kicked 3 times for same offense within 24h"
            embed.add_field(name="Ban", value=value_text, inline=False)
            embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # 3rd offense for normal kicks = ban
        elif not is_special_kick and not is_extended_kick and kick_count >= 2:
            try:
                embed = discord.Embed(
                    title="Ban Notification",
                    description=f"You are being banned from Hamilton New Zealand Roleplay for 3 counts of {self.offence}.",
                    color=discord.Color.red()
                )
                embed.add_field(
                    name="Appeal Information",
                    value="The ban appeal link can be found in the support channel.",
                    inline=False
                )
                embed.add_field(
                    name="Communications Server Code",
                    value="hnzrp",
                    inline=False
                )
                await self.user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Banned successfully (3rd offense)"
            except:
                dm_status = "<:Denied:1426930694633816248> Ban failed"

            # <:Accepted:1426930333789585509> Save to database
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=self.user.id,
                action='ban',
                details={
                    'user': str(self.user),
                    'reason': f"3 counts of {self.offence}",
                    'duration': 'Permanent (3rd offense)',
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'infraction_type': 'ban'
                }
            )

            if self.cog:
                await self.cog.reset_user_infractions(interaction.guild.id, self.user.id)

            embed = discord.Embed(
                title="Ban Action Summary (3rd Offense)",
                color=discord.Color.dark_red()
            )
            value_text = f"**User:** {self.user.mention}\n**Offence:** 3 counts of {self.offence}\n**Duration:** Permanent\n**DM Status:** {dm_status}\n**Note:** User was kicked twice for same offense within 24h"
            embed.add_field(name="Ban", value=value_text, inline=False)
            embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await interaction.response.send_message(embed=embed, ephemeral=True)

        else:
            # Normal kick
            kick_number = kick_count + 1

            try:
                embed = discord.Embed(
                    title="Kick Notification",
                    color=discord.Color.orange()
                )

                if kick_number == 1:
                    description = f"You are being kicked from Hamilton New Zealand Roleplay for {self.offence}."
                else:
                    description = f"You are being kicked from Hamilton New Zealand Roleplay for {kick_number} counts of {self.offence}."

                embed.description = description
                embed.add_field(
                    name="Important",
                    value=f"Do not rejoin until **{self.kick_time.value}** have passed, or you will be banned.",
                    inline=False
                )
                await self.user.send(embed=embed)

                if kick_number == 1:
                    suffix = "st"
                elif kick_number == 2:
                    suffix = "nd"
                elif kick_number == 3:
                    suffix = "rd"
                else:
                    suffix = "th"

                dm_status = f"<:Accepted:1426930333789585509> Kicked successfully ({kick_number}{suffix} offense)"
            except:
                dm_status = "<:Denied:1426930694633816248> Kick failed"

            # <:Accepted:1426930333789585509> Save to database
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=self.user.id,
                action='kick',
                details={
                    'user': str(self.user),
                    'reason': self.offence,
                    'duration': self.kick_time.value,
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'offense_number': kick_number,
                    'infraction_type': 'kick'
                }
            )

            if kick_number == 1:
                suffix = "st"
            elif kick_number == 2:
                suffix = "nd"
            elif kick_number == 3:
                suffix = "rd"
            else:
                suffix = "th"

            embed = discord.Embed(
                title=f"Kick Action Summary ({kick_number}{suffix} Offense)",
                color=discord.Color.orange()
            )

            value_text = f"**User:** {self.user.mention}\n**Offence:** {self.offence}\n**Duration:** {self.kick_time.value}\n**DM Status:** {dm_status}"
            if kick_number > 1:
                value_text += f"\n**Warning:** This is offense #{kick_number} within 24h"
                if is_extended_kick and kick_number == 3:
                    value_text += "\n**<:Warn:1437771973970104471> Next offense will result in a BAN**"

            embed.add_field(
                name=f"{self.action_type}",
                value=value_text,
                inline=False
            )

            embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await interaction.response.send_message(embed=embed, ephemeral=True)

class OffenceSelect(discord.ui.Select):
    def __init__(self, action_type, moderator, cog):
        self.action_type = action_type
        self.moderator = moderator
        self.cog = cog
        options = [
            discord.SelectOption(label="No Livery", emoji="🚗"),
            discord.SelectOption(label="No Uniform", emoji="👔"),
            discord.SelectOption(label="Cop Baiting", emoji="🚓"),
            discord.SelectOption(label="Breaking Fear Roleplay", emoji="😨"),
            discord.SelectOption(label="Using a restricted weapon", emoji="🔫"),
            discord.SelectOption(label="Using a booster vehicle", emoji="🏎️"),
            discord.SelectOption(label="Using a whitelisted team livery", emoji="🎨"),
            discord.SelectOption(label="Unrealistic avatar", emoji="👤"),
            discord.SelectOption(label="Interfering with a mod scene", emoji="🎬"),
            discord.SelectOption(label="Lying to staff", emoji="🤥"),
            discord.SelectOption(label="FRP (Fail Roleplay)", emoji="<:Denied:1426930694633816248>"),
            discord.SelectOption(label="Meta Gaming", emoji="🎮"),
            discord.SelectOption(label="NLR (New Life Rule)", emoji="🔄"),
            discord.SelectOption(label="Breaking Priority", emoji="⚡"),
            discord.SelectOption(label="No Roadworks perms", emoji="🚧"),
        ]
        super().__init__(
            placeholder="Select an offence...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_offence = self.values[0]
        user_view = NoteUserSelectView(self.action_type, selected_offence, self.moderator, self.cog)
        embed = discord.Embed(
            title="Select User for Educational Note",
            description="Choose the user to give a note:",
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed, view=user_view, ephemeral=True)


class NoteUserSelect(discord.ui.UserSelect):
    def __init__(self, action_type, offence, moderator, cog):
        self.action_type = action_type
        self.offence = offence
        self.moderator = moderator
        self.cog = cog
        super().__init__(placeholder="Select a user...", min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        selected_user = self.values[0]

        kickable_offenses = [
            "Cop Baiting", "Breaking Fear Roleplay", "Using a restricted weapon",
            "Using a booster vehicle", "Using a whitelisted team livery",
            "Interfering with a mod scene", "FRP (Fail Roleplay)",
            "Meta Gaming", "NLR (New Life Rule)", "Breaking Priority"
        ]

        is_kickable = self.offence in kickable_offenses

        # If kickable, show modal for duration input
        if is_kickable:
            try:
                modal = NoteTimeModal(self.action_type, self.offence, selected_user, self.moderator, is_kickable, self.view, self.cog)
                await interaction.response.send_modal(modal)
            except discord.InteractionResponded:
                pass
            except Exception as e:
                try:
                    await interaction.response.send_message("An error occurred.", ephemeral=True)
                except:
                    pass
        else:
            # For non-kickable, process immediately without modal
            await self.process_note(interaction, selected_user)

    async def process_note(self, interaction: discord.Interaction, selected_user):
        # Get recent notes from database
        recent_notes = await db.get_recent_logs(
            guild_id=interaction.guild.id,
            action_type='educational_note',
            user_id=selected_user.id,
            hours=24
        )
        recent_notes = [
            log for log in recent_notes
            if log.get('details', {}).get('infraction_type') == 'note'
               and log.get('details', {}).get('reason') == self.offence
        ]

        all_recent_warnings = await db.get_recent_logs(
            guild_id=interaction.guild.id,
            action_type='educational_note',
            user_id=selected_user.id,
            hours=24
        )
        all_recent_warnings = [
            log for log in all_recent_warnings
            if log.get('details', {}).get('infraction_type') == 'warning'
        ]

        note_count = len(recent_notes)
        total_warnings = len(all_recent_warnings)
        offense_number = note_count + 1
        action_taken = "note"
        dm_status = ""

        # Check for auto-kick on 3 warnings
        if offense_number == 2 and total_warnings >= 2:
            try:
                embed = discord.Embed(
                    title="Kick Notification",
                    description="You are being kicked from Hamilton New Zealand Roleplay for reaching three warnings.",
                    color=discord.Color.orange()
                )
                embed.add_field(
                    name="Important",
                    value="Do not rejoin until **30 minutes** have passed, or you will be banned.",
                    inline=False
                )
                await selected_user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Kicked successfully (3 warnings)"
                action_taken = "kick"
            except:
                dm_status = "<:Denied:1426930694633816248> Kick failed"
                action_taken = "kick"

            # <:Accepted:1426930333789585509> FIXED: Correct infraction_type and duration
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=selected_user.id,
                action='educational_note',
                details={
                    'infraction_type': 'kick',  # <:Accepted:1426930333789585509> Correct - this IS a kick
                    'reason': 'Reaching three warnings',
                    'duration': '30 minutes',
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'offense_number': 3
                }
            )

            embed = discord.Embed(
                title="Auto-Kick Summary (3 Warnings)",
                color=discord.Color.dark_orange()
            )
            value_text = f"**User:** {selected_user.mention}\n**Offence:** Reaching three warnings\n**Action:** Automatic kick\n**Duration:** 30 minutes\n**DM Status:** {dm_status}\n**Note:** User reached 3 warnings within 24h"
            embed.add_field(name="Educational Note Action", value=value_text, inline=False)
            embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        # Handle based on offense number
        if offense_number == 1:
            try:
                embed = discord.Embed(
                    title="Educational Note",
                    description=f"This is a reminder that doing {self.offence} is not allowed. Please don't do it again.",
                    color=discord.Color.blue()
                )
                await selected_user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Note sent successfully"
                action_taken = "note"
            except:
                dm_status = "<:Denied:1426930694633816248> Note failed"
                action_taken = "note"

            # <:Accepted:1426930333789585509> FIXED: Correct infraction_type, no duration for notes
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=selected_user.id,
                action='educational_note',
                details={
                    'infraction_type': 'note',  # <:Accepted:1426930333789585509> Correct
                    'reason': self.offence,
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'offense_number': offense_number
                }
            )

        elif offense_number == 2:
            warnings_left = max(1, 2 - total_warnings)
            try:
                embed = discord.Embed(
                    title="Warning",
                    description=f"You are being warned for 2 counts of {self.offence}. If you receive {warnings_left} more warning{'s' if warnings_left != 1 else ''}, you will be kicked.",
                    color=discord.Color.gold()
                )
                await selected_user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Warning sent successfully"
                action_taken = "warning"
            except:
                dm_status = "<:Denied:1426930694633816248> Warning failed"
                action_taken = "warning"

            # <:Accepted:1426930333789585509> FIXED: Correct infraction_type
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=selected_user.id,
                action='educational_note',
                details={
                    'infraction_type': 'warning',  # <:Accepted:1426930333789585509> Correct
                    'reason': self.offence,
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'offense_number': offense_number
                }
            )

        else:  # offense 3+
            try:
                embed = discord.Embed(
                    title="Ban Notification",
                    description=f"You are being banned from Hamilton New Zealand Roleplay for {self.offence}.",
                    color=discord.Color.red()
                )
                embed.add_field(
                    name="Appeal Information",
                    value="The ban appeal link can be found in the support channel.",
                    inline=False
                )
                embed.add_field(
                    name="Communications Server Code",
                    value="hnzrp",
                    inline=False
                )
                await selected_user.send(embed=embed)
                dm_status = "<:Accepted:1426930333789585509> Banned successfully"
                action_taken = "ban"
            except:
                dm_status = "<:Denied:1426930694633816248> Ban failed"
                action_taken = "ban"

            # <:Accepted:1426930333789585509> FIXED: Correct infraction_type
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=selected_user.id,
                action='educational_note',
                details={
                    'infraction_type': 'ban',  # <:Accepted:1426930333789585509> Correct
                    'reason': self.offence,
                    'duration': 'Permanent',
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'offense_number': offense_number
                }
            )

        # Create summary embed
        if offense_number == 1:
            title = "Educational Note Summary"
            color = discord.Color.blue()
        elif offense_number == 2:
            title = "Warning Summary"
            color = discord.Color.gold()
        else:
            title = f"Ban Summary ({offense_number}th Offense)"
            color = discord.Color.red()

        embed = discord.Embed(title=title, color=color)
        value_text = f"**User:** {selected_user.mention}\n**Offence:** {self.offence}\n**Action:** {action_taken.capitalize()}\n**DM Status:** {dm_status}"
        if offense_number > 1:
            value_text += f"\n**Note:** This is offense #{offense_number} within 24h"
        if offense_number == 2 and total_warnings >= 1:
            value_text += f"\n**Total Warnings:** {total_warnings + 1}/3"

        embed.add_field(name="Educational Note Action", value=value_text, inline=False)
        embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        await interaction.response.send_message(embed=embed, ephemeral=True)

class NoteUserSelectView(discord.ui.View):
    def __init__(self, action_type, offence, moderator, cog):  # ADD cog parameter
        super().__init__(timeout=300)
        self.action_type = action_type
        self.offence = offence
        self.moderator = moderator
        self.cog = cog  # ADD THIS LINE
        self.add_item(NoteUserSelect(action_type, offence, moderator, cog))  # ADD cog here

class NoteTimeModal(discord.ui.Modal):
    def __init__(self, action_type, offence, user, moderator, is_kickable, parent_view, cog):
        super().__init__(title="Kick Duration")
        self.action_type = action_type
        self.offence = offence
        self.user = user
        self.moderator = moderator
        self.is_kickable = is_kickable
        self.parent_view = parent_view
        self.cog = cog

        self.time_input = discord.ui.TextInput(
            label="Kick Duration",
            placeholder="E.g., 30 minutes, 1 hour",
            style=discord.TextStyle.short,
            required=True,
            max_length=100
        )
        self.add_item(self.time_input)

    async def on_submit(self, interaction: discord.Interaction):
        recent_notes = await db.get_recent_logs(
            guild_id=interaction.guild.id,
            action_type='educational_note',
            user_id=self.user.id,  # <:Accepted:1426930333789585509> FIXED: was selected_user.id
            hours=24
        )
        recent_notes = [
            log for log in recent_notes
            if log.get('details', {}).get('infraction_type') == 'note'
               and log.get('details', {}).get('reason') == self.offence
        ]

        all_recent_warnings = await db.get_recent_logs(
            guild_id=interaction.guild.id,
            action_type='educational_note',
            user_id=self.user.id,  # <:Accepted:1426930333789585509> FIXED
            hours=24
        )
        all_recent_warnings = [
            log for log in all_recent_warnings
            if log.get('details', {}).get('infraction_type') == 'warning'
        ]

        note_count = len(recent_notes)
        total_warnings = len(all_recent_warnings)
        offense_number = note_count + 1

        # For kickable offenses on 3rd strike, escalate to kick with custom duration
        if offense_number >= 3:
            try:
                embed = discord.Embed(
                    title="Kick Notification",
                    description=f"You are being kicked from Hamilton New Zealand Roleplay for {offense_number} counts of {self.offence}.",
                    color=discord.Color.orange()
                )
                embed.add_field(
                    name="Important",
                    value=f"Do not rejoin until **{self.time_input.value}** have passed, or you will be banned.",
                    inline=False
                )
                await self.user.send(embed=embed)
                dm_status = f"<:Accepted:1426930333789585509> Kicked successfully ({offense_number}rd+ offense)"
                action_taken = "kick"
            except:
                dm_status = "<:Denied:1426930694633816248> Kick failed"
                action_taken = "kick"

            # <:Accepted:1426930333789585509> FIXED: Correct variable name and infraction_type
            await db.log_action(
                guild_id=interaction.guild.id,
                user_id=self.user.id,  # <:Accepted:1426930333789585509> FIXED: was selected_user.id
                action='educational_note',
                details={
                    'infraction_type': 'kick',  # <:Accepted:1426930333789585509> Correct - this IS a kick
                    'reason': self.offence,
                    'duration': self.time_input.value,
                    'moderator': str(self.moderator),
                    'moderator_id': self.moderator.id,
                    'dm_status': dm_status,
                    'offense_number': offense_number
                }
            )

            embed = discord.Embed(
                title=f"Kick Summary ({offense_number}rd+ Offense)",
                color=discord.Color.dark_orange()
            )
            value_text = f"**User:** {self.user.mention}\n**Offence:** {self.offence}\n**Action:** {action_taken.capitalize()}\n**Duration:** {self.time_input.value}\n**DM Status:** {dm_status}"
            embed.add_field(name="Educational Note Action", value=value_text, inline=False)
            embed.set_footer(text=f"Logged at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            # For 1st and 2nd offense, treat as regular note/warning
            await interaction.response.send_message(
                "This offense doesn't require kick duration yet. Processing as regular note.",
                ephemeral=True
            )

class DeleteLogSelect(discord.ui.Select):
    def __init__(self, command_user_id, logs, filter_user):
        self.command_user_id = command_user_id
        self.logs = logs
        self.filter_user = filter_user

        options = []
        for i, log in enumerate(logs):
            timestamp = datetime.fromisoformat(log['timestamp']).strftime('%Y-%m-%d %H:%M')
            user_name = log.get('user', 'Unknown')[:20]  # Truncate if too long
            log_type = log.get('type', 'unknown').upper()
            reason = log.get('reason', 'No reason')[:30]  # Truncate if too long

            label = f"{log_type} - {user_name}"[:100]  # Discord limit
            description = f"{reason} | {timestamp}"[:100]  # Discord limit

            # Create a unique value that combines index with timestamp to ensure uniqueness
            value = f"{i}_{log['timestamp']}"

            options.append(
                discord.SelectOption(
                    label=label,
                    description=description,
                    value=value,
                    emoji="<:Wipe:1434954284851658762>"
                )
            )

        super().__init__(
            placeholder="Select a log to delete...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.command_user_id:
            await interaction.response.send_message("This menu is not for you!", ephemeral=True)
            return

        # Parse the selected value to get the index
        selected_value = self.values[0]
        selected_index = int(selected_value.split('_')[0])
        selected_log = self.logs[selected_index]

        # <:Accepted:1426930333789585509> FIXED: Delete from database instead of mod_logs list
        try:
            # Delete from database using the log's ID
            result = await db.execute(
                'DELETE FROM audit_logs WHERE id = $1 AND guild_id = $2',
                selected_log['id'],
                interaction.guild.id
            )

            if result == 'DELETE 0':
                await interaction.response.send_message("<:Denied:1426930694633816248> Error: Log not found in database.", ephemeral=True)
                return

            # Create confirmation embed
            embed = discord.Embed(
                title="<:Accepted:1426930333789585509> Log Deleted Successfully",
                color=discord.Color.green()
            )

            details = selected_log.get('details', {})
            timestamp = selected_log['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            field_value = f"**User:** <@{selected_log['user_id']}>\n**Reason:** {details.get('reason', 'N/A')}\n**Moderator:** <@{details.get('moderator_id', 'N/A')}>\n**Time:** {timestamp}"
            if 'duration' in details:
                field_value += f"\n**Duration:** {details['duration']}"

            embed.add_field(
                name=f"{selected_log['action'].upper()} - {details.get('dm_status', 'N/A')}",
                value=field_value,
                inline=False
            )

            embed.set_footer(
                text=f"Log deleted by {interaction.user.name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Delete the select menu message
            try:
                await interaction.message.delete()
            except:
                pass

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.response.send_message(f"<:Denied:1426930694633816248> Error deleting log: {str(e)}", ephemeral=True)

class DeleteLogSelectView(discord.ui.View):
    def __init__(self, command_user_id, logs, filter_user):
        super().__init__(timeout=300)
        self.command_user_id = command_user_id
        self.add_item(DeleteLogSelect(command_user_id, logs, filter_user))

    async def on_timeout(self):
        # Disable all items when the view times out
        for item in self.children:
            item.disabled = True


class BanOffenceSelectView(discord.ui.View):
    def __init__(self, action_type, moderator, cog):
        super().__init__(timeout=300)
        self.action_type = action_type
        self.moderator = moderator
        self.add_item(BanOffenceSelect(action_type, moderator, cog))
        self.cog = cog


class KickOffenceSelectView(discord.ui.View):
    def __init__(self, action_type, moderator, cog):
        super().__init__(timeout=300)
        self.action_type = action_type
        self.moderator = moderator
        self.add_item(KickOffenceSelect(action_type, moderator, cog))
        self.cog = cog


class OffenceSelectView(discord.ui.View):
    def __init__(self, action_type, moderator, cog):
        super().__init__(timeout=300)
        self.action_type = action_type
        self.moderator = moderator
        self.add_item(OffenceSelect(action_type, moderator, cog))
        self.cog = cog


class ModActionView(discord.ui.View):
    def __init__(self, command_user_id, cog):
        super().__init__(timeout=300)
        self.command_user_id = command_user_id
        self.add_item(ModActionSelect(cog))
        self.cog = cog


# Replace the entire has_mod_role function and ModCog class with these fixes:

def has_mod_role():
    async def predicate(ctx):
        if ctx.guild.id not in MODERATOR_ROLES:
            await ctx.send("This command is not configured for this server.")
            return False

        role_id = MODERATOR_ROLES[ctx.guild.id]

        # Handle None role_id (allows everyone)
        if role_id is None:
            return True

        role = ctx.guild.get_role(role_id)
        if role is None:
            await ctx.send("Moderator role not found.")
            return False

        has_role = role in ctx.author.roles
        if not has_role:
            await ctx.send("You don't have permission to use this command!")

        return has_role

    return commands.check(predicate)


class ModCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_ready = False

    @commands.Cog.listener()
    async def on_ready(self):
        """Verify database connection when bot is ready"""
        if self.db_ready:
            return

        # Wait for database with short timeout
        max_wait = 30
        start_time = asyncio.get_event_loop().time()

        while True:
            if db.pool is not None:
                try:
                    async with db.pool.acquire() as conn:
                        await conn.fetchval('SELECT 1')
                    print("✅ ModCog: Database connection verified")
                    self.db_ready = True
                    break
                except Exception as e:
                    print(f"⚠️ ModCog: Database test failed: {e}")

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > max_wait:
                print("⚠️ ModCog: Database not ready after 30s")
                break

            await asyncio.sleep(2)

    async def reset_user_infractions(self, guild_id: int, user_id: int):
        """Reset all warning/kick counters for a user when they get banned"""
        if not self.db_ready:
            print("⚠️ Database not ready - cannot reset infractions")
            return

    @commands.command(name="mod")
    @has_mod_role()
    async def mod_command(self, ctx):
        """Displays a moderation action selection menu"""
        # Delete the command message
        try:
            await ctx.message.delete()
        except:
            pass

        embed = discord.Embed(
            title="Moderation Panel",
            description="Select a moderation action to perform:",
            color=discord.Color.blue()
        )
        view = ModActionView(ctx.author.id, self)
        # Send as regular message (ephemeral doesn't work with prefix commands)
        await ctx.send(embed=embed, view=view, delete_after=300)

    @mod_command.error
    async def mod_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Error message already sent in check function
            pass
        elif isinstance(error, commands.MissingPermissions):
            await ctx.send("You don't have permission to use this command!")

    @commands.command(name="modlogsclear")
    async def modlogsclear_command(self, ctx):
        """Clear all moderation logs (Owner only)"""
        if ctx.author.id != OWNER_ID:
            await ctx.send("You don't have permission to use this command!")
            return

        # <:Accepted:1426930333789585509> Clear from database
        log_count_result = await db.fetch(
            'SELECT COUNT(*) FROM audit_logs WHERE guild_id = $1',
            ctx.guild.id
        )
        log_count = log_count_result[0]['count']

        await db.execute(
            'DELETE FROM audit_logs WHERE guild_id = $1',
            ctx.guild.id
        )

        await ctx.send(f"Cleared {log_count} moderation log entries.")

    @commands.command(name="modlogs")
    @has_mod_role()
    async def modlogs_command(self, ctx, limit: int = None, skip: int = 0, user: discord.User = None):
        """View moderation logs with pagination support"""
        # Delete the command message
        try:
            await ctx.message.delete()
        except:
            pass

        loading_msg = await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Scanning ???",
                                                ephemeral=True)

        if limit is None:
            embed = discord.Embed(
                title="Moderation Logs - Help Menu",
                description="View and navigate through moderation logs",
                color=discord.Color.blue()
            )
            embed.add_field(
                name="Basic Usage",
                value="```\n!modlogs <limit> [skip] [@user]\n```",
                inline=False
            )
            embed.add_field(
                name="Examples",
                value="**`!modlogs 10`** - View the last 10 logs\n"
                      "**`!modlogs 20`** - View the last 20 logs\n"
                      "**`!modlogs 10 5`** - View 10 logs, skipping the 5 most recent\n"
                      "**`!modlogs 15 20`** - View 15 logs, skipping the 20 most recent (older logs)\n"
                      "**`!modlogs 10 0 @user`** - View last 10 logs for a specific user\n"
                      "**`!modlogs 20 5 @user`** - View 20 logs for a user, skipping 5 most recent",
                inline=False
            )
            embed.add_field(
                name="Parameters",
                value="**limit** - Number of logs to display (required)\n"
                      "**skip** - Number of most recent logs to skip (optional, default: 0)\n"
                      "**@user** - Filter logs by specific user (optional)",
                inline=False
            )
            embed.add_field(
                name="Tips",
                value="Use the skip parameter to navigate backwards through older logs.\n"
                      "Use @user to filter logs for a specific user.\n"
                      "Total log count is shown in the footer of results.",
                inline=False
            )
            embed.set_footer(text="This message will auto-delete in 1 minute")

            help_message = await ctx.send(embed=embed)
            await help_message.delete(delay=60)
            return

        # <:Accepted:1426930333789585509> Get logs from database
        if user:
            filtered_logs = await db.get_recent_logs(
                guild_id=ctx.guild.id,
                user_id=user.id,
                limit=1000
            )
        else:
            filtered_logs = await db.get_recent_logs(
                guild_id=ctx.guild.id,
                limit=1000
            )

        if not filtered_logs:
            await ctx.send("No moderation logs found.", delete_after=10)
            return

        total_logs = len(filtered_logs)
        start_index = max(0, total_logs - skip - limit)
        end_index = total_logs - skip

        if end_index <= 0 or start_index >= total_logs:
            await ctx.send(f"Invalid skip amount. Total logs: {total_logs}", delete_after=10)
            return

        displayed_logs = filtered_logs[start_index:end_index]

        user_filter_text = f" for {user.mention}" if user else ""
        embed = discord.Embed(
            title=f"Moderation Logs{user_filter_text}",
            description=f"Showing {len(displayed_logs)} of {total_logs} actions (Skip: {skip}, Limit: {limit})",
            color=discord.Color.blue()
        )

        for log in displayed_logs:
            details = log.get('details', {})
            timestamp = log['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            field_value = f"**User:** <@{log['user_id']}>\n**Reason:** {details.get('reason', 'N/A')}\n**Moderator:** <@{details.get('moderator_id', 'N/A')}>\n**Time:** {timestamp}"
            if 'duration' in details:
                field_value += f"\n**Duration:** {details['duration']}"

            embed.add_field(
                name=f"{log['action'].upper()} - {details.get('dm_status', 'N/A')}",
                value=field_value,
                inline=False
            )

        embed.set_footer(text=f"Showing logs {start_index + 1}-{end_index} of {total_logs}{user_filter_text}")
        await loading_msg.delete()
        await ctx.send(embed=embed, delete_after=300)

    @commands.command(name="deletelog")
    @has_mod_role()
    async def deletelog_command(self, ctx, limit: int = None, skip: int = 0, user: discord.User = None):
        """Delete specific moderation logs with pagination support"""
        # Delete the command message
        try:
            await ctx.message.delete()
        except:
            pass

        if limit is None:
            embed = discord.Embed(
                title="Delete Log - Help Menu",
                description="Select and delete specific moderation logs",
                color=discord.Color.red()
            )
            embed.add_field(
                name="Basic Usage",
                value="```\n!deletelog <limit> [skip] [@user]\n```",
                inline=False
            )
            embed.add_field(
                name="Examples",
                value="**`!deletelog 10`** - View the last 10 logs to delete\n"
                      "**`!deletelog 20`** - View the last 20 logs to delete\n"
                      "**`!deletelog 10 5`** - View 10 logs, skipping the 5 most recent\n"
                      "**`!deletelog 15 20`** - View 15 logs, skipping the 20 most recent (older logs)\n"
                      "**`!deletelog 10 0 @user`** - View last 10 logs for a specific user to delete\n"
                      "**`!deletelog 20 5 @user`** - View 20 logs for a user, skipping 5 most recent",
                inline=False
            )
            embed.add_field(
                name="Parameters",
                value="**limit** - Number of logs to display (required)\n"
                      "**skip** - Number of most recent logs to skip (optional, default: 0)\n"
                      "**@user** - Filter logs by specific user (optional)",
                inline=False
            )
            embed.add_field(
                name="<:Warn:1437771973970104471> Warning",
                value="Deleted logs cannot be recovered. Use with caution!",
                inline=False
            )
            embed.set_footer(text="This message will auto-delete in 1 minute")

            help_message = await ctx.send(embed=embed)
            await help_message.delete(delay=60)
            return

        # <:Accepted:1426930333789585509> Get logs from database
        if user:
            filtered_logs = await db.get_recent_logs(
                guild_id=ctx.guild.id,
                user_id=user.id,
                limit=1000
            )
        else:
            filtered_logs = await db.get_recent_logs(
                guild_id=ctx.guild.id,
                limit=1000
            )

        if not filtered_logs:
            await ctx.send("No moderation logs found.", delete_after=10)
            return

        total_logs = len(filtered_logs)
        start_index = max(0, total_logs - skip - limit)
        end_index = total_logs - skip

        if end_index <= 0 or start_index >= total_logs:
            await ctx.send(f"Invalid skip amount. Total logs: {total_logs}", delete_after=10)
            return

        displayed_logs = filtered_logs[start_index:end_index]

        view = DeleteLogSelectView(ctx.author.id, displayed_logs, user)

        user_filter_text = f" for {user.mention}" if user else ""
        embed = discord.Embed(
            title=f"🗑️ Delete Moderation Log{user_filter_text}",
            description=f"Select a log to delete from the dropdown below.\nShowing {len(displayed_logs)} of {total_logs} actions (Skip: {skip}, Limit: {limit})\n\n<:Warn:1437771973970104471> **Warning:** This action cannot be undone!",
            color=discord.Color.red()
        )

        embed.set_footer(text=f"Showing logs {start_index + 1}-{end_index} of {total_logs}{user_filter_text}")

        await ctx.send(embed=embed, view=view, delete_after=300)


async def setup(bot):
    await bot.add_cog(ModCog(bot))