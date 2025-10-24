import discord
from discord.ext import commands
from discord import app_commands
import json
from datetime import datetime
from dateutil import parser

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'report_channel_id': 1413783738596331580,  # Replace with your channel ID for this guild
        'allowed_role_ids': [1309021002675654700, 1389550689113473024]  # Role IDs that can use the command
    },
    1425867713183744023: {
        'report_channel_id': None,  # Replace with your channel ID for this guild
        'allowed_role_ids': None  # Role IDs that can use the command
    }
}


def get_guild_config(guild_id: int):
    """Get configuration for a specific guild"""
    return GUILD_CONFIGS.get(guild_id, {})


class WentWrongCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Create command group for "went"
    went_group = app_commands.Group(name='went', description='Incident reporting commands')

    @went_group.command(name='time-entry', description='View examples of valid date and time formats.')
    async def time_entry(self, interaction: discord.Interaction):
        """Shows examples of valid date/time formats for the went wrong command"""
        try:
            embed = discord.Embed(
                title='Valid Date & Time Formats',
                description='Here are examples of date and time formats you can use when submitting a report:',
                colour=discord.Colour.blue()
            )

            embed.add_field(
                name='Standard Formats',
                value='• `Jan 20, 2025 3:30 PM`\n• `January 20, 2025 3:30 PM`\n• `01/20/2025 3:30 PM`\n• `2025-01-20 15:30`',
                inline=False
            )

            embed.add_field(
                name='Date Only (assumes midnight)',
                value='• `Jan 20, 2025`\n• `01/20/2025`\n• `2025-01-20`',
                inline=False
            )

            embed.add_field(
                name='With Seconds',
                value='• `Jan 20, 2025 3:30:45 PM`\n• `2025-01-20 15:30:45`',
                inline=False
            )

            embed.add_field(
                name='Relative Dates',
                value='• `today at 3:30 PM`\n• `yesterday 3:30 PM`\n• `tomorrow 3:30 PM`',
                inline=False
            )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            print(f'Error in time_entry command: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Time entry command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour.red()
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)

    @went_group.command(name='wrong',
                        description='Funny snapshots of things that have gone wrong (accidents) on the job!.')
    @app_commands.describe(
        vehicle_callsigns='The callsigns of the vehicles involved separated by a comma (e.g. HAM415. HAM411 and HAM4118).',
        driver_usernames='The username(s) of the drivers involved (e.g. MajorKarlsruhe, dancingskulls and 123eye_sonme).',
        date_and_time='The date and time of incident (e.g., "Jan 20, 2025 3:30 PM")',
        location='The location of the incident (e.g. Sandstone Road, Postal Code 210).',
        approx_speed='The approximate speed before the incident (e.g. 67km/h).',
        proof='Images or videos of the occurance (image/video/link). If you want to add more, add them after you send the command.'
    )
    async def went_wrong(
            self,
            interaction: discord.Interaction,
            vehicle_callsigns: str,
            driver_usernames: str,
            date_and_time: str,
            location: str,
            approx_speed: str,
            proof: discord.Attachment
    ):
        try:
            await interaction.response.defer(ephemeral=True)

            # Get guild-specific configuration
            guild_config = get_guild_config(interaction.guild.id)
            report_channel_id = guild_config.get('report_channel_id')
            allowed_role_ids = guild_config.get('allowed_role_ids', [])

            # Check if user has required role
            if allowed_role_ids:
                user_role_ids = [role.id for role in interaction.user.roles]
                has_permission = any(role_id in user_role_ids for role_id in allowed_role_ids)

                if not has_permission:
                    no_permission_embed = discord.Embed(
                        description='❌ You do not have permission to use this command!',
                        colour=discord.Colour.red()
                    )
                    await interaction.followup.send(embed=no_permission_embed, ephemeral=True)
                    return

            # Parse the date and time string into a datetime object
            try:
                parsed_datetime = parser.parse(date_and_time)
                # Convert to Discord timestamp format
                discord_timestamp = discord.utils.format_dt(parsed_datetime, style="F")
            except Exception as date_error:
                # If parsing fails, use the original string
                discord_timestamp = date_and_time

            # Determine where to send the report
            if report_channel_id:
                report_channel = interaction.guild.get_channel(report_channel_id)
                if not report_channel:
                    error_embed = discord.Embed(
                        description='❌ Report channel not found!',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
                    return
            else:
                # If no report channel configured, use the current channel
                report_channel = interaction.channel

            # Create the report embed
            embed = discord.Embed(
                title='🚨 When tings go wong on da job!',
                colour=discord.Colour(0xf24d4d),
                timestamp=discord.utils.utcnow()
            )

            # Add fields
            embed.add_field(name='🚗 **Vehicle Callsign(s)**', value=vehicle_callsigns, inline=True)
            embed.add_field(name='👤 **Driver Username(s)**', value=driver_usernames, inline=True)
            embed.add_field(name='📅 **Date & Time**', value=discord_timestamp, inline=True)
            embed.add_field(name='📍 **Location**', value=location, inline=True)
            embed.add_field(name='⚡ **Approx Speed**', value=approx_speed, inline=True)
            embed.add_field(name='‎ ',
                            value=f'-# *Submitted at {discord.utils.format_dt(discord.utils.utcnow(), style="F")}*', inline=False)

            embed.set_author(
                name=f'Submitted by {interaction.user.display_name}',
                icon_url=interaction.user.display_avatar.url
            )

            # Send the embed and create thread
            report_message = await report_channel.send(embed=embed)

            # Create thread name from the parsed datetime
            try:
                parsed_datetime = parser.parse(date_and_time)
                thread_name = f"Case {parsed_datetime.strftime('%d/%m/%Y, %-I:%M %p')}"
            except:
                # Fallback to original input if parsing fails
                thread_name = f"Case {date_and_time}"

            # Create a thread for this report
            thread = await report_message.create_thread(
                name=thread_name,
                auto_archive_duration=10080  # 7 days
            )

            # Handle proof attachment in the thread
            await thread.send(content='**Proof:**', file=await proof.to_file())

            # Send confirmation to user
            success_embed = discord.Embed(
                description=f'✅ Report submitted successfully in {report_channel.mention}!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error in went_wrong command: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Went wrong command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)


# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(WentWrongCog(bot))