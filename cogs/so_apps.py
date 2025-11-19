import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
from datetime import datetime, timezone
import json
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

LEADERSHIP = [1389113393511923863, 1285474077556998196, 1389550689113473024]
HIGHER_LEADERSHIP = [1389550689113473024, 1389157641799991347, 1389111326571499590]  # Add specific higher-up role IDs here

# Role ID to Role Name mapping
ROLE_NAMES = {
    1285113945664917514: "𝗙𝗘𝗡𝗭 | National Commander",
    1389157641799991347: "𝗙𝗘𝗡𝗭 | Deputy National Commander",
    1389157690760232980: "𝗙𝗘𝗡𝗭 | Assistant National Commander",
    1365959866363150366: "𝗙𝗘𝗡𝗭 | Area Commander",
    1389158062635487312: "𝗙𝗘𝗡𝗭 | Assistant Area Commander"
    # Add more role IDs and their corresponding names here
}


class StationOfficerCog(commands.Cog):
    """Station Officer application management commands"""

    def __init__(self, bot):
        self.bot = bot
        self.db_pool = None

    async def cog_load(self):
        self.db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)

    async def load_timestamp(self, org: str):
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT timestamp FROM app_timestamps WHERE org=$1", org)
            if row:
                return row['timestamp'].isoformat()
            return None

    async def save_timestamp(self, org: str, timestamp_dt: Optional[datetime]):
        async with self.db_pool.acquire() as conn:
            if timestamp_dt is None:
                # Remove the timestamp entry
                await conn.execute(
                    "DELETE FROM app_timestamps WHERE org=$1",
                    org
                )
            else:
                await conn.execute(
                    "INSERT INTO app_timestamps (org, timestamp) VALUES ($1, $2) "
                    "ON CONFLICT (org) DO UPDATE SET timestamp = EXCLUDED.timestamp",
                    org, timestamp_dt
                )

    async def check_and_reset_timestamp(self, org: str):
        timestamp_str = await self.load_timestamp(org)
        if timestamp_str:
            timestamp_dt = datetime.fromisoformat(timestamp_str)
            if datetime.now(timezone.utc) > timestamp_dt:
                await self.save_timestamp(org, None)

    so_group = app_commands.Group(name="so", description="Station Officer application commands")

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Check if user has leadership role"""
        if not any(role.id in LEADERSHIP for role in interaction.user.roles):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to Leadership only!",
                ephemeral=True
            )
            return False
        return True

    @so_group.command(name="result", description="Send SO application results to a user")
    @app_commands.describe(
        result="Score out of 50",
        user="User to send results to",
        override="Override the pass/fail result",
        notes="Additional notes (optional - will open a modal if not provided)"
    )
    @app_commands.choices(override=[
        app_commands.Choice(name="Pass", value="pass"),
        app_commands.Choice(name="Fail", value="fail")
    ])
    async def so_result(
            self,
            interaction: discord.Interaction,
            result: int,
            user: discord.Member,
            override: Optional[app_commands.Choice[str]] = None,
            notes: Optional[str] = None
    ):
        """Send SO application results"""

        # Validate result range
        if result < 0 or result > 50:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Result must be between 0 and 50.",
                ephemeral=True
            )
            return

        # If notes not provided, show modal
        if notes is None:
            modal = SOResultModal(self, result, user, override.value if override else None)
            await interaction.response.send_modal(modal)
        else:
            # Process directly if notes provided
            await interaction.response.defer(ephemeral=True)
            await self.send_so_result(interaction, result, user, override.value if override else None, notes)

    async def send_so_result(
            self,
            interaction: discord.Interaction,
            result: int,
            user: discord.Member,
            override: Optional[str],
            notes: str
    ):
        """Send the actual SO result message"""

        try:
            # Determine pass/fail first (for DM)
            if override == "pass":
                passed = True
            elif override == "fail":
                passed = False
            else:
                passed = result >= 30

            # Send DM notification FIRST
            dm_sent = False
            try:
                dm_embed = discord.Embed(
                    title="Station Officer Application Result",
                    description=f"Your SO application result has been posted in {interaction.channel.mention}.\n\nPlease check the channel for your full results{'!' if passed else '.'}",
                    color=discord.Color(0x2ecc71) if passed else discord.Color(0xfb4441)
                )
                dm_embed.set_author(name='FENZ | Leadership',
                                    icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691a3aff&is=6918e97f&hm=311a99a24f20e90e24190639c29c9365252b839a169d3b22a97814824c3401db&')
                dm_embed.set_footer(text=f"{interaction.guild.name}",
                                    icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439356945906667706/image.png?ex=691a391d&is=6918e79d&hm=911c91689aa548fe65cc741be61a9826ea23e6efb6ff4c9bcfa2861e8633f4e6&')

                await user.send(embed=dm_embed)
                dm_sent = True
            except discord.Forbidden:
                pass
            except Exception as e:
                print(f"Failed to send DM to {user.name}: {e}")

            # Give user send message permissions in current channel
            channel = interaction.channel
            await channel.set_permissions(
                user,
                send_messages=True,
                reason=f"SO Application Result - By {interaction.user.name}"
            )

            # Calculate percentage
            percentage = (result / 50) * 100

            # Build the message
            message = f"Hello {user.mention},\n\n"

            # Pass/Fail message
            if passed:
                message += "I am proud to inform you that you have **passed** the Station Officer Application. Your results are posted below. You are not a SO yet, as you still must attend at least 1 RA/training session with a member of Leadership. Further information will be released to you next week. Congratulations!\n\n"
            else:
                message += "I regret to inform you that you have **failed** the Station Officer Application. Your results are posted below. Do not hesitate to ask any further queries in regards to your results. Applications will re-open at a similar time next month - Do not fear as applying for the second time on average has resulted in a better SO theory score.\n\n"

            # Results line
            result_status = "PASS" if passed else "FAIL"
            message += f"**Results:** {result}/50 | {percentage:.1f}% | **{result_status}**\n\n"

            # Notes
            if notes and notes.strip():
                message += f"**Notes:** {notes}\n\n"

            # Special case: score < 30 but overridden to pass
            if result < 30 and override == "pass":
                message += "*As it is general SO theory that is taught in your RA, as well as the complexity of some questions, we decided to pass your application. Your RA will be more extensive/there may be more than average to cater for this.*\n\n"

            # Send the message
            await channel.send(message)

            # Confirm to moderator
            dm_status = "<:Accepted:1426930333789585509> DM sent" if dm_sent else "<:Warn:1437771973970104471> DM failed (user has DMs disabled)"
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> SO results sent to {user.mention} in {channel.mention}\n"
                f"**Result:** {result}/50 ({percentage:.1f}%) - {result_status}\n"
                f"**Override:** {override.upper() if override else 'None'}\n"
                f"**DM Status:** {dm_status}",
                ephemeral=True
            )

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Missing permissions to modify channel permissions or send messages.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @so_group.command(name="stj-result", description="Send StJ application results to a user")
    @app_commands.describe(
        result="Score out of 59",
        user="User to send results to",
        override="Override the pass/fail result",
        notes="Additional notes (optional - will open a modal if not provided)"
    )
    @app_commands.choices(override=[
        app_commands.Choice(name="Pass", value="pass"),
        app_commands.Choice(name="Fail", value="fail")
    ])
    async def stj_result(
            self,
            interaction: discord.Interaction,
            result: int,
            user: discord.Member,
            override: Optional[app_commands.Choice[str]] = None,
            notes: Optional[str] = None
    ):
        """Send StJ application results"""

        # Validate result range
        if result < 0 or result > 59:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Result must be between 0 and 59.",
                ephemeral=True
            )
            return

        # If notes not provided, show modal
        if notes is None:
            modal = StJResultModal(self, result, user, override.value if override else None)
            await interaction.response.send_modal(modal)
        else:
            # Process directly if notes provided
            await interaction.response.defer(ephemeral=True)
            await self.send_stj_result(interaction, result, user, override.value if override else None, notes)

    async def send_stj_result(
            self,
            interaction: discord.Interaction,
            result: int,
            user: discord.Member,
            override: Optional[str],
            notes: str
    ):
        """Send the actual StJ result message"""

        try:
            # Determine pass/fail first (for DM) - 60% of 59 = 35.4, so >= 36 to pass
            if override == "pass":
                passed = True
            elif override == "fail":
                passed = False
            else:
                passed = result >= 36

            # Send DM notification FIRST
            dm_sent = False
            try:
                dm_embed = discord.Embed(
                    title="HHStJ Supervisory Application Result",
                    description=f"Your HHStJ application result has been posted in {interaction.channel.mention}.\n\nPlease check the channel for your full results{'!' if passed else '.'}",
                    color=discord.Color(0x2ecc71) if passed else discord.Color(0xfb4441)
                )
                dm_embed.set_author(name='HHStJ | Leadership',
                                    icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691cddff&is=691b8c7f&hm=966e1387ed7fa00060b573263596831b1ef9a99e95d686968159e8e05d745aab&')
                dm_embed.set_footer(text=f"{interaction.guild.name}",
                                    icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1440140372218085556/image.png?ex=691d12bd&is=691bc13d&hm=1201990871e11de88023ee426b584bf43e9a9ec5fcb04b5064f787d35676f932&')

                await user.send(embed=dm_embed)
                dm_sent = True
            except discord.Forbidden:
                pass
            except Exception as e:
                print(f"Failed to send DM to {user.name}: {e}")

            # Give user send message permissions in current channel
            channel = interaction.channel
            await channel.set_permissions(
                user,
                send_messages=True,
                reason=f"HHStJ Application Result - By {interaction.user.name}"
            )

            # Calculate percentage
            percentage = (result / 59) * 100

            # Build the message
            message = f"Hello {user.mention},\n\n"

            # Pass/Fail message
            if passed:
                message += "I am proud to inform you that you have **passed** the Supervisory Application. Your results are posted below. Welcome to the Supervisory team, you will be on Trial for 1 week, and will be assessed on your activity and in-game actions, passing of this will keep your position. Congratulations!\n\n"
            else:
                message += "I regret to inform you that you have **failed** the Supervisory Application. Your results are posted below. Do not hesitate to ask any further queries in regards to your results. Applications will ideally re-open at a similar time next month - Do not fear as applying for the second time on average has resulted in a better score.\n\n"

            # Results line
            result_status = "PASS" if passed else "FAIL"
            message += f"**Results:** {result}/59 | {percentage:.1f}% | **{result_status}**\n\n"

            # Notes
            if notes and notes.strip():
                message += f"**Notes:** {notes}\n\n"

            # Special case: score < 36 but overridden to pass
            if result < 36 and override == "pass":
                message += "*Although on paper you failed your application, we have decided to pass you as the answers and knowledge you showed impressed us in some capacity. Although the process will not change, please consider reaching out to another Supervisory or Leadership member to help improve your understanding of concepts you may have had diffuculty with.*\n\n"

            # Send the message
            await channel.send(message)

            # Confirm to moderator
            dm_status = "<:Accepted:1426930333789585509> DM sent" if dm_sent else "<:Warn:1437771973970104471> DM failed (user has DMs disabled)"
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> HHStJ results sent to {user.mention} in {channel.mention}\n"
                f"**Result:** {result}/59 ({percentage:.1f}%) - {result_status}\n"
                f"**Override:** {override.upper() if override else 'None'}\n"
                f"**DM Status:** {dm_status}",
                ephemeral=True
            )

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Missing permissions to modify channel permissions or send messages.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @so_group.command(name="applied", description="Thank user for their application and remove chat permissions")
    @app_commands.describe(
        user="User who applied",
        org="Organization (FENZ or HHStJ)",
        override_timestamp="Override and set a new timestamp (Higher Leadership only)"
    )
    @app_commands.choices(org=[
        app_commands.Choice(name="FENZ", value="fenz"),
        app_commands.Choice(name="HHStJ", value="hhstj")
    ])
    async def so_applied(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            org: app_commands.Choice[str],
            override_timestamp: Optional[bool] = False
    ):
        """Acknowledge application and remove send permissions"""

        # Check if user wants to override timestamp
        if override_timestamp:
            # Check if user has higher leadership role
            if not any(role.id in HIGHER_LEADERSHIP for role in interaction.user.roles):
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Only Higher Leadership can override timestamps!",
                    ephemeral=True
                )
                return

            # Show modal to set new timestamp
            modal = TimestampModal(self, org.value, user)
            await interaction.response.send_modal(modal)
            return

        # Check and reset timestamp if needed
        self.check_and_reset_timestamp(org.value)

        # If no timestamp set, show modal to set one
        if self.timestamps[org.value] is None:
            modal = TimestampModal(self, org.value, user)
            await interaction.response.send_modal(modal)
            return

        # Process normally with existing timestamp
        await interaction.response.defer(ephemeral=True)
        await self.process_applied(interaction, user, org.value)

    async def process_applied(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            org: str
    ):
        """Process the application acknowledgment"""

        try:
            # Determine org-specific details
            if org == "hhstj":
                org_name = "HHStJ | Leadership"
                org_icon = "https://cdn.discordapp.com/attachments/1425358898831036507/1440140372218085556/image.png?ex=691d12bd&is=691bc13d&hm=1201990871e11de88023ee426b584bf43e9a9ec5fcb04b5064f787d35676f932&"
                app_type = "Supervisory"
            else:  # fenz
                org_name = "FENZ | Leadership"
                org_icon = "https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691a3aff&is=6918e97f&hm=311a99a24f20e90e24190639c29c9365252b839a169d3b22a97814824c3401db&"
                app_type = "Station Officer"

            # Get timestamp
            timestamp = self.timestamps[org]
            timestamp_unix = int(datetime.fromisoformat(timestamp).timestamp())

            # Send DM notification FIRST
            dm_sent = False
            try:
                description = f"\nYour {app_type} application has been received!\n\n"
                description += f"Please wait while we review your application. You will be contacted with results in the days after the application period lapses. Application period closure: <t:{timestamp_unix}:F>."

                dm_embed = discord.Embed(
                    title=f"{app_type} Application Received",
                    description=description,
                    color=discord.Color(0x000000)
                )
                dm_embed.set_author(name=org_name,
                                    icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691cddff&is=691b8c7f&hm=966e1387ed7fa00060b573263596831b1ef9a99e95d686968159e8e05d745aab&')
                dm_embed.set_footer(
                    text=f"{interaction.guild.name}",
                    icon_url=org_icon
                )

                await user.send(embed=dm_embed)
                dm_sent = True
            except discord.Forbidden:
                pass
            except Exception as e:
                print(f"Failed to send DM to {user.name}: {e}")

            # Remove send message permissions
            channel = interaction.channel
            await channel.set_permissions(
                user,
                send_messages=False,
                reason=f"{app_type} Application Acknowledged - By {interaction.user.name}"
            )

            # Build channel message
            message = (
                f"Thanks for your application {user.mention}!\n"
                f"If you have any queries about anything else, please make a thread or create a new ticket. "
                f"Good luck, and I'll be in contact in the days after the application period closes – <t:{timestamp_unix}:R> (removing chat perms now, you will still be able to see the ticket)."
            )

            await channel.send(message)

            # Confirm to moderator
            dm_status = "<:Accepted:1426930333789585509> DM sent" if dm_sent else "<:Warn:1437771973970104471> DM failed (user has DMs disabled)"
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Sent acknowledgment to {user.mention} and removed send permissions in {channel.mention}\n"
                f"**Organization:** {org.upper()}\n"
                f"**Closure Date:** <t:{timestamp_unix}:F>\n"
                f"**DM Status:** {dm_status}",
                ephemeral=True
            )

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Missing permissions to modify channel permissions or send messages.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @so_group.command(name="theory", description="Send QFF theory test results to a user")
    @app_commands.describe(
        user="User to send results to",
        passed="Did the user pass the theory test?",
        notes="Additional notes (optional - will open a modal if not provided)"
    )
    async def theory(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            passed: bool,
            notes: Optional[str] = None
    ):
        """Send QFF theory test results"""

        # If notes not provided, show modal
        if notes is None:
            modal = TheoryResultModal(self, user, passed, interaction.user)
            await interaction.response.send_modal(modal)
        else:
            # Process directly if notes provided
            await interaction.response.defer(ephemeral=True)
            await self.send_theory_result(interaction, user, passed, notes, interaction.user)

    async def send_theory_result(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            passed: bool,
            notes: str,
            sender: discord.Member
    ):
        """Send the actual theory result message"""

        try:
            channel = interaction.channel

            # Get role object by ID
            role_id = 1408256806417072188
            role = discord.utils.get(interaction.guild.roles, id=role_id)

            # Get role name from ROLE_NAMES dict
            role_name = None
            for role in sender.roles:
                if role.id in ROLE_NAMES:
                    role_name = ROLE_NAMES[role.id]
                    break

            if role_name is None:
                role_name = "Leadership Member"

            # Build the message
            message = f"Hello {user.mention}, I'm {interaction.user.mention} the {role_title} from {interaction.guild.name}.\n"
            message += f"I've marked your QFF theory test, "

            if passed:
                message += "**I'm pleased to say you've passed.**\n\n"
                message += "To get promoted to QFF you'll need to partake in a ride-along, you can request a date and time and I'll advise the trainers and see if anyone is available administer your ride-along.\n\n"
            else:
                message += "**I'm sorry to say that you've failed.**\n\n"
                message += "To get promoted to QFF you'll need to reattempt the theory test, this can be found in https://discord.com/channels/1282916959062851634/1389062651375779870. We encourage you to have a re-read of portions of the FENZ Handbook (found in the same place) before you reattempt this.\n\n"

            # Add notes if provided
            if notes and notes.strip():
                message += f"**Notes:** {notes}\n\n"

            message += "*Cheers, FENZ Leadership.*"

            # Send the message
            await channel.send(message)

            # Assign the role to the user if the role exists
            if role:
                await user.add_roles(role, reason="QFF Theory Passed")

            # Confirm to moderator
            status = "PASSED" if passed else "FAILED"
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> QFF theory results sent to {user.mention} in {channel.mention}\n"
                f"**Result:** {status}\n"
                f"**Grader:** {sender.mention} ({role_name})",
                f"**Role:** <@&1408256806417072188> given",
                ephemeral=True
            )

        except discord.Forbidden:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Missing permissions to send messages.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )


class TimestampModal(discord.ui.Modal, title="Set Application Closure Date"):
    """Modal for setting application closure timestamp"""

    def __init__(self, cog: StationOfficerCog, org: str, user: discord.Member):
        super().__init__()
        self.cog = cog
        self.org = org
        self.user = user

    timestamp_input = discord.ui.TextInput(
        label="Closure Date & Time",
        placeholder="Format: YYYY-MM-DD HH:MM (e.g., 2025-01-15 18:00)",
        required=True,
        max_length=16,
        style=discord.TextStyle.short
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            # Parse the timestamp
            timestamp_str = self.timestamp_input.value.strip()
            timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
            timestamp_dt = timestamp_dt.replace(tzinfo=timezone.utc)

            # Check if timestamp is in the future
            if timestamp_dt <= datetime.now(timezone.utc):
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Timestamp must be in the future!",
                    ephemeral=True
                )
                return

            # Save the timestamp
            await self.cog.save_timestamp(self.org, timestamp_dt)

            # Process the application
            await self.cog.process_applied(interaction, self.user, self.org)

        except ValueError:
            await interaction.followup.send(
                "<:Denied:1426930694633816248> Invalid date format! Please use: YYYY-MM-DD HH:MM (e.g., 2025-01-15 18:00)",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )


class SOResultModal(discord.ui.Modal, title="SO Application Notes"):
    """Modal for entering SO application notes"""

    def __init__(self, cog: StationOfficerCog, result: int, user: discord.Member, override: Optional[str]):
        super().__init__()
        self.cog = cog
        self.result = result
        self.user = user
        self.override = override

    notes = discord.ui.TextInput(
        label="Notes",
        placeholder="Enter any additional notes or feedback for the applicant...",
        required=False,
        max_length=1000,
        style=discord.TextStyle.paragraph
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        await self.cog.send_so_result(
            interaction,
            self.result,
            self.user,
            self.override,
            self.notes.value if self.notes.value else ""
        )


class StJResultModal(discord.ui.Modal, title="StJ Application Notes"):
    """Modal for entering StJ application notes"""

    def __init__(self, cog: StationOfficerCog, result: int, user: discord.Member, override: Optional[str]):
        super().__init__()
        self.cog = cog
        self.result = result
        self.user = user
        self.override = override

    notes = discord.ui.TextInput(
        label="Notes",
        placeholder="Enter any additional notes or feedback for the applicant...",
        required=False,
        max_length=1000,
        style=discord.TextStyle.paragraph
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        await self.cog.send_stj_result(
            interaction,
            self.result,
            self.user,
            self.override,
            self.notes.value if self.notes.value else ""
        )


class TheoryResultModal(discord.ui.Modal, title="Theory Test Notes"):
    """Modal for entering theory test notes"""

    def __init__(self, cog: StationOfficerCog, user: discord.Member, roblox_username: str, role_title: str,
                 passed: bool):
        super().__init__()
        self.cog = cog
        self.user = user
        self.roblox_username = roblox_username
        self.role_title = role_title
        self.passed = passed

    notes = discord.ui.TextInput(
        label="Notes",
        placeholder="Enter any additional notes or feedback for the applicant...",
        required=False,
        max_length=1000,
        style=discord.TextStyle.paragraph
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        await self.cog.send_theory_result(
            interaction,
            self.user,
            self.roblox_username,
            self.role_title,
            self.passed,
            self.notes.value if self.notes.value else ""
        )


async def setup(bot):
    cog = StationOfficerCog(bot)
    await cog.cog_load()
    await bot.add_cog(cog)