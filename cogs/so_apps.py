import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional

LEADERSHIP = [1285474077556998196, 1389550689113473024]

class StationOfficerCog(commands.Cog):
    """Station Officer application management commands"""

    def __init__(self, bot):
        self.bot = bot

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
                dm_embed.set_author(name='FENZ | Leadership', icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691a3aff&is=6918e97f&hm=311a99a24f20e90e24190639c29c9365252b839a169d3b22a97814824c3401db&')
                dm_embed.set_footer(text=f"{interaction.guild.name}", icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439356945906667706/image.png?ex=691a391d&is=6918e79d&hm=911c91689aa548fe65cc741be61a9826ea23e6efb6ff4c9bcfa2861e8633f4e6&')

                await user.send(embed=dm_embed)
                dm_sent = True
            except discord.Forbidden:
                # User has DMs disabled
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
                # User has DMs disabled
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

            # Pass/Fail message (you can edit these messages as needed)
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

    @so_group.command(name="applied", description="Thank user for their SO application and remove chat permissions")
    @app_commands.describe(
        user="User who applied",
        org="Organization (FENZ or HHStJ)",
        time="Estimated review time (e.g., 7d, 1h, 3d)"
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
            time: Optional[str] = None
    ):
        """Acknowledge SO application and remove send permissions"""

        await interaction.response.defer(ephemeral=True)

        try:
            # Determine org-specific details
            if org.value == "hhstj":
                org_name = "HHStJ | Leadership"
                org_icon = "https://cdn.discordapp.com/attachments/1425358898831036507/1440140372218085556/image.png?ex=691d12bd&is=691bc13d&hm=1201990871e11de88023ee426b584bf43e9a9ec5fcb04b5064f787d35676f932&"
                app_type = "Supervisory"
            else:  # fenz
                org_name = "FENZ | Leadership"
                org_icon = "https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691a3aff&is=6918e97f&hm=311a99a24f20e90e24190639c29c9365252b839a169d3b22a97814824c3401db&"
                app_type = "Station Officer"

            # Send DM notification FIRST
            dm_sent = False
            try:
                # Build description first
                description = f"\nYour {app_type} application has been received!\n\n"
                if time:
                    description += f"Please wait while we review your application. You will be contacted with results in the days after the application period lapses. Application period closure: **{time}**."
                else:
                    description += "Please wait while we review your application. You will be contacted with results in the days after the application period lapses."

                # Create embed with the built description
                dm_embed = discord.Embed(
                    title=f"{app_type} Application Received",
                    description=description,
                    color=discord.Color(0x000000)
                )
                dm_embed.set_author(name=org_name, icon_url='https://cdn.discordapp.com/attachments/1425358898831036507/1439358965770092666/cropped_circle_image_1.png?ex=691cddff&is=691b8c7f&hm=966e1387ed7fa00060b573263596831b1ef9a99e95d686968159e8e05d745aab&')
                dm_embed.set_footer(
                    text=f"{interaction.guild.name}",
                    icon_url=org_icon
                )

                await user.send(embed=dm_embed)
                dm_sent = True
            except discord.Forbidden:
                # User has DMs disabled
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
            if time:
                message = (
                    f"Thanks for your application {user.mention}!\n"
                    f"If you have any queries about anything else, please make a thread or create a new ticket. "
                    f"Good luck, and I'll be in contact in the days after the application period closes — in **{time}** (removing chat perms now, you will still be able to see the ticket)."
                )
            else:
                message = (
                    f"Thanks for your application {user.mention}!\n"
                    f"If you have any queries about anything else, please make a thread or create a new ticket. "
                    f"Good luck, and I'll be in contact soon (removing chat perms now, you will still be able to see the ticket)."
                )

            await channel.send(message)

            # Confirm to moderator
            dm_status = "<:Accepted:1426930333789585509> DM sent" if dm_sent else "<:Warn:1437771973970104471> DM failed (user has DMs disabled)"
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> Sent acknowledgment to {user.mention} and removed send permissions in {channel.mention}\n"
                f"**Organization:** {org.name}\n"
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

        # Send the SO result with the notes from the modal
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

        # Send the StJ result with the notes from the modal
        await self.cog.send_stj_result(
            interaction,
            self.result,
            self.user,
            self.override,
            self.notes.value if self.notes.value else ""
        )

async def setup(bot):
    await bot.add_cog(StationOfficerCog(bot))