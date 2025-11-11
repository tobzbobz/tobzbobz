import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
from datetime import datetime

# Punishment type choices
PUNISHMENT_TYPES = [
    "𝗛𝗛𝗦t𝗝 | Strike",
    "𝗙𝗘𝗡𝗭 | Strike",
    "𝗛𝗛𝗦t𝗝 | Resignation",
    "𝗙𝗘𝗡𝗭 | Resignation",
    "𝗛𝗛𝗦t𝗝 | Termination",
    "𝗙𝗘𝗡𝗭 | Termination",
    "𝗛𝗛𝗦t𝗝 | Warning",
    "𝗙𝗘𝗡𝗭 | Warning",
    "𝗖𝗖 | Demotion",
    "𝗖𝗖 | Warning",
    "𝗖𝗖 | Strike",
    "𝗖𝗖 | Termination",
    "𝗖𝗖 | Resignation"
]

CASE_LOG_ROLES = [
    1285474077556998196,  # Role 1
    1389113393511923863,  # Role 2
    1389550689113473024,  # Role 3
]

CASE_LOG_CHANNEL_ID = 1432957952695861248  # Replace with your actual channel ID

def has_case_log_permission():
    async def predicate(interaction: discord.Interaction) -> bool:
        # Check if user has any of the required roles
        user_role_ids = {role.id for role in interaction.user.roles}
        if not user_role_ids.intersection(CASE_LOG_ROLES):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use this command.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)

class CaseLogCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    case_group = app_commands.Group(name="case", description="Case management commands")

    @case_group.command(name="log", description="Log a case with punishment details")
    @has_case_log_permission()
    @app_commands.describe(
        user="The user being punished",
        punishment="Type of punishment",
        reason="Reason for the punishment",
        attachment="Optional proof attachment",
        proof_urls="Optional proof URLs (separated by commas)"
    )
    @app_commands.choices(punishment=[
        app_commands.Choice(name="𝗛𝗛𝗦t𝗝 | Strike", value="𝗛𝗛𝗦t𝗝 | Strike"),
        app_commands.Choice(name="𝗙𝗘𝗡𝗭 | Strike", value="𝗙𝗘𝗡𝗭 | Strike"),
        app_commands.Choice(name="𝗛𝗛𝗦t𝗝 | Resignation", value="𝗛𝗛𝗦t𝗝 | Resignation"),
        app_commands.Choice(name="𝗙𝗘𝗡𝗭 | Resignation", value="𝗙𝗘𝗡𝗭 | Resignation"),
        app_commands.Choice(name="𝗛𝗛𝗦t𝗝 | Termination", value="𝗛𝗛𝗦t𝗝 | Termination"),
        app_commands.Choice(name="𝗙𝗘𝗡𝗭 | Termination", value="𝗙𝗘𝗡𝗭 | Termination"),
        app_commands.Choice(name="𝗛𝗛𝗦t𝗝 | Warning", value="𝗛𝗛𝗦t𝗝 | Warning"),
        app_commands.Choice(name="𝗙𝗘𝗡𝗭 | Warning", value="𝗙𝗘𝗡𝗭 | Warning"),
        app_commands.Choice(name="𝗖𝗖 | Demotion", value="𝗖𝗖 | Demotion"),
        app_commands.Choice(name="𝗖𝗖 | Warning", value="𝗖𝗖 | Warning"),
        app_commands.Choice(name="𝗖𝗖 | Strike", value="𝗖𝗖 | Strike"),
        app_commands.Choice(name="𝗖𝗖 | Termination", value="𝗖𝗖 | Termination"),
        app_commands.Choice(name="𝗖𝗖 | Resignation", value="𝗖𝗖 | Resignation"),
    ])
    async def log_case(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            punishment: app_commands.Choice[str],
            reason: str,
            attachment: Optional[discord.Attachment] = None,
            proof_urls: Optional[str] = None
    ):
        """Log a case with punishment details"""

        await interaction.response.defer(ephemeral=False)

        # Create the case log embed
        embed = discord.Embed(
            title="Case Log",
            color=discord.Color(0x000000),
            timestamp=datetime.utcnow()
        )

        embed.add_field(
            name="User:",
            value=f"{user.mention}",
            inline=True
        )

        embed.add_field(
            name="Punishment:",
            value=punishment.value,
            inline=True
        )

        embed.add_field(
            name="Reason:",
            value=reason,
            inline=False
        )

        embed.add_field(
            name="‎",
            value=f"*Logged by {interaction.user.mention}*",  # <:Accepted:1426930333789585509> Changed to mention instead of display_name
            inline=False
        )

        # Get the target channel
        target_channel = self.bot.get_channel(CASE_LOG_CHANNEL_ID)
        if not target_channel:
            await interaction.followup.send("<:Denied:1426930694633816248> Case log channel not found!", ephemeral=True)
            return

        # Send the embed to the target channel
        message = await target_channel.send(embed=embed)

        # Confirm to the user
        await interaction.followup.send(
            f"<:Accepted:1426930333789585509> Case logged successfully in {target_channel.mention}",
            ephemeral=True
        )

        # Create thread for proof
        thread = await message.create_thread(
            name=f"Case Proof - {user.display_name}",
            auto_archive_duration=1440  # 24 hours
        )

        # Prepare proof message
        proof_content = "**Proof:**\n"
        has_proof = False

        # Add URLs if provided
        if proof_urls:
            urls = [url.strip() for url in proof_urls.split(',') if url.strip()]
            if urls:
                proof_content += "\n".join(f"{url}" for url in urls)  # <:Accepted:1426930333789585509> Fixed: use "\n".join() instead of join()
                has_proof = True

        # Add attachment if provided
        if attachment:
            try:
                file = await attachment.to_file()
                if has_proof:
                    await thread.send(proof_content)
                await thread.send(
                    f"**Proof:**",
                    file=file
                )
                has_proof = True
            except Exception as e:
                print(f"Error uploading attachment to thread: {e}")
                await thread.send(f"<:Warn:1437771973970104471> Error uploading attachment: {e}")
        elif has_proof:
            await thread.send(proof_content)

        if not has_proof:
            await thread.send("**Proof:**\nNo proof provided.")


async def setup(bot):
    await bot.add_cog(CaseLogCog(bot))