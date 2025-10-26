import discord
from discord.ext import commands
from discord import app_commands
from discord.ext import tasks
import asyncio
import json
import datetime

# Bot owner ID
OWNER_ID = 678475709257089057

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'watch_channel_id': 1390867914462203914,
        'watch_role_id': 1390867686170300456
    },
    1425867713183744023: {
        'watch_channel_id': 1426190071115284502,  # Replace with your channel ID
        'watch_role_id': 1426185588930777199  # Replace with your role ID
    }
}

def get_guild_config(guild_id: int):
    """Get configuration for a specific guild"""
    return GUILD_CONFIGS.get(guild_id, {})

# Load/save watch data
from database import (
    load_watches,
    save_watches,
    load_scheduled_votes,
    save_scheduled_votes,
    ensure_json_files
)
active_watches = load_watches()


# Vote button and view
class VoteButton(discord.ui.View):
    def __init__(self, message_id: int, required_votes: int, colour: str, station: str, time_minutes: int = None,
                 guild=None, channel=None, cog=None):
        super().__init__(timeout=None)
        self.message_id = message_id
        self.required_votes = required_votes
        self.voted_users = set()  # Store user IDs who have voted
        self.vote_count = 0
        self.colour = colour
        self.station = station
        self.time_minutes = time_minutes
        self.guild = guild
        self.channel = channel
        self.cog = cog
        self.cancelled = False

    @discord.ui.button(label='0 ✅', style=discord.ButtonStyle.green, custom_id='vote_button')
    async def vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check if user has already voted
            if interaction.user.id in self.voted_users:
                already_voted_embed = discord.Embed(
                    description='❌ You have already voted!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=already_voted_embed, ephemeral=True)
                return

            # Add user to voted list
            self.voted_users.add(interaction.user.id)
            self.vote_count += 1

            # Update button label
            button.label = f'{self.vote_count} ✅'

            # Check if vote goal reached
            if self.vote_count >= self.required_votes:
                # Map colour names to Discord colours
                colour_map = {
                    'Yellow': discord.Colour.gold(),
                    'Blue': discord.Colour.blue(),
                    'Brown': discord.Colour(0x8B4513),
                    'Red': discord.Colour.red()
                }
                embed_colour = colour_map.get(self.colour, discord.Colour.orange())

                # Create the Watch Start embed
                start_embed = discord.Embed(title=f'🚨 {self.colour} Watch Announcement 🚨', colour=embed_colour)
                start_embed.add_field(name='Station', value=f'`{self.station}`', inline=True)

                # Calculate time field
                if self.time_minutes:
                    watch_time = discord.utils.utcnow() + datetime.timedelta(minutes=self.time_minutes)
                    start_embed.add_field(name='Time', value=discord.utils.format_dt(watch_time, style='R'),
                                          inline=True)
                else:
                    start_embed.add_field(name='Time', value=discord.utils.format_dt(discord.utils.utcnow(), style='R'),
                                          inline=True)

                start_embed.add_field(name='Watch Leader', value=interaction.user.mention, inline=True)
                start_embed.add_field(name='‎',
                                      value='No need to vote just hop in!!\nIf you are joining, please join Fenz RTO 🙌',
                                      inline=False)
                start_embed.add_field(name='‎',
                                      value='**Select the below reaction role to be notified of any future watches!**',
                                      inline=False)
                start_embed.set_image(
                    url='https://cdn.discordapp.com/attachments/1425867714160758896/1426932258694238258/image.png?ex=68f4eeb9&is=68f39d39&hm=b69f7f8bad7dcd7c7bde4dab731ca7e23e27d32d864cad9fc7224dcbb0648840')
                start_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')
                start_embed.set_author(name=f'Vote passed - Started by vote',
                                       icon_url=interaction.user.display_avatar.url)

                # Create voters embed
                voters_embed = discord.Embed(title='Voters', colour=embed_colour)
                voter_mentions = []
                for user_id in self.voted_users:
                    user = interaction.guild.get_member(user_id)
                    if user:
                        voter_mentions.append(user.mention)
                voters_embed.description = '\n'.join(voter_mentions)

                # Update message with watch start embed and voters embed, add watch ping button
                watch_view = WatchRoleButton(self.message_id)
                guild_config = get_guild_config(interaction.guild.id)
                watch_role_id = guild_config.get('watch_role_id')

                await interaction.response.edit_message(content=f'-# ||<@&{watch_role_id}> {interaction.user.mention} <@&1309021002675654700> <@&1365536209681514636>||' if watch_role_id else '',
                                                        embeds=[start_embed, voters_embed], view=watch_view)

                # Store watch data
                active_watches[str(self.message_id)] = {
                    'user_id': interaction.user.id,
                    'user_name': interaction.user.display_name,
                    'channel_id': interaction.channel.id,
                    'colour': self.colour,
                    'station': self.station,
                    'started_at': int(interaction.created_at.timestamp()),
                    'has_voters_embed': True  # Flag to indicate this watch has a voters embed
                }
                save_watches(active_watches)

                # Cancel the timeout task since vote passed
                if self.cog and str(self.message_id) in self.cog.vote_timeout_tasks:
                    self.cog.vote_timeout_tasks[str(self.message_id)].cancel()
                    del self.cog.vote_timeout_tasks[str(self.message_id)]

            else:
                # Update the message with new vote count
                await interaction.response.edit_message(view=self)

                # Send confirmation
                voted_embed = discord.Embed(
                    description=f'✅ Vote recorded! ({self.vote_count}/{self.required_votes})',
                    colour=discord.Colour(0x2ecc71)
                )
                await interaction.followup.send(embed=voted_embed, ephemeral=True)

        except Exception as e:
            # Send error DM to owner
            await interaction.client.send_error_dm('Vote button error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            print(f'Error processing vote: {e}')

    @discord.ui.button(label='Remove Vote', emoji='🗑️', style=discord.ButtonStyle.red, custom_id='remove_vote_button')
    async def remove_vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check if user has voted
            if interaction.user.id not in self.voted_users:
                not_voted_embed = discord.Embed(
                    description='❌ You have not voted yet!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=not_voted_embed, ephemeral=True)
                return

            # Remove user from voted list
            self.voted_users.remove(interaction.user.id)
            self.vote_count -= 1

            # Update the vote button label
            for item in self.children:
                if item.custom_id == 'vote_button':
                    item.label = f'{self.vote_count} ✅'
                    break

            # Update the message with new vote count
            await interaction.response.edit_message(view=self)

            # Send confirmation
            removed_embed = discord.Embed(
                description=f'✅ Vote removed! ({self.vote_count}/{self.required_votes})',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=removed_embed, ephemeral=True)

        except Exception as e:
            # Send error DM to owner
            await interaction.client.send_error_dm('Remove vote button error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            print(f'Error removing vote: {e}')

# Watch role toggle button
class WatchRoleButton(discord.ui.View):
    def __init__(self, message_id: int):
        super().__init__(timeout=None)
        self.message_id = message_id

    @discord.ui.button(label='Watch Ping', emoji='📢', style=discord.ButtonStyle.gray, custom_id='watch_ping_toggle')
    async def toggle_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            guild_config = get_guild_config(interaction.guild.id)
            role_id = guild_config.get('watch_role_id')

            if not role_id:
                error_embed = discord.Embed(
                    description='❌ Watch role not configured for this server!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
                return

            role = interaction.guild.get_role(role_id)

            if not role:
                error_embed = discord.Embed(
                    description='❌ Role not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
                return

            # Toggle role for ANY user who clicks (no role restrictions)
            if role in interaction.user.roles:
                await interaction.user.remove_roles(role)
                embed = discord.Embed(
                    description=f'Removed {role.mention} role!',
                    colour=discord.Colour(0xf24d4d)  # Red
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await interaction.user.add_roles(role)
                embed = discord.Embed(
                    description=f'Added {role.mention} role!',
                    colour=discord.Colour(0x2ecc71)  # Green
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            # Send error DM to owner
            await interaction.client.send_error_dm('Vote button error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            print(f'Error toggling role: {e}')


# Add this pagination view class before the WatchCog class

class LogsPaginationView(discord.ui.View):
    def __init__(self, pages: list, user_id: int):
        super().__init__(timeout=180)  # 3 minute timeout
        self.pages = pages
        self.current_page = 0
        self.user_id = user_id
        self.update_buttons()

    def update_buttons(self):
        # Disable/enable buttons based on current page
        self.first_button.disabled = self.current_page == 0
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= len(self.pages) - 1
        self.last_button.disabled = self.current_page >= len(self.pages) - 1

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only allow the user who invoked the command to use buttons
        if interaction.user.id != self.user_id:
            await interaction.response.send_message('❌ This is not your logs view!', ephemeral=True)
            return False
        return True

    @discord.ui.button(label='⏮️', style=discord.ButtonStyle.gray)
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 0
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    @discord.ui.button(label='◀️', style=discord.ButtonStyle.blurple)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    @discord.ui.button(label='▶️', style=discord.ButtonStyle.blurple)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    @discord.ui.button(label='⏭️', style=discord.ButtonStyle.gray)
    async def last_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = len(self.pages) - 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)


class MissedVoteConfirmationView(discord.ui.View):
    def __init__(self, vote_id: str, vote_data: dict, cog):
        super().__init__(timeout=None)  # No timeout for missed vote confirmations
        self.vote_id = vote_id
        self.vote_data = vote_data
        self.cog = cog

    @discord.ui.button(label='Send Now', emoji='✅', style=discord.ButtonStyle.green, custom_id='send_missed_vote')
    async def send_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Only allow the bot owner to click
            if interaction.user.id != OWNER_ID:
                await interaction.response.send_message('❌ Only the bot owner can use this!', ephemeral=True)
                return

            await interaction.response.defer()

            # Send the vote
            await self.cog.send_scheduled_vote(self.vote_data)

            # Remove from scheduled votes
            scheduled_votes = load_scheduled_votes()
            if self.vote_id in scheduled_votes:
                del scheduled_votes[self.vote_id]
                save_scheduled_votes(scheduled_votes)

            # Update the message
            embed = interaction.message.embeds[0]
            embed.colour = discord.Colour(0x2ecc71)
            embed.title = '✅ Missed Vote - SENT'

            # Disable buttons
            for item in self.children:
                item.disabled = True

            await interaction.message.edit(embed=embed, view=self)
            await interaction.followup.send('✅ Vote sent successfully!', ephemeral=True)

        except Exception as e:
            print(f'Error sending missed vote: {e}')
            await interaction.followup.send(f'❌ Error sending vote: {e}', ephemeral=True)

    @discord.ui.button(label='Cancel', emoji='❌', style=discord.ButtonStyle.red, custom_id='cancel_missed_vote')
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Only allow the bot owner to click
            if interaction.user.id != OWNER_ID:
                await interaction.response.send_message('❌ Only the bot owner can use this!', ephemeral=True)
                return

            await interaction.response.defer()

            # Remove from scheduled votes
            scheduled_votes = load_scheduled_votes()
            if self.vote_id in scheduled_votes:
                del scheduled_votes[self.vote_id]
                save_scheduled_votes(scheduled_votes)

            # Update the message
            embed = interaction.message.embeds[0]
            embed.colour = discord.Colour(0xf24d4d)
            embed.title = '❌ Missed Vote - CANCELLED'

            # Disable buttons
            for item in self.children:
                item.disabled = True

            await interaction.message.edit(embed=embed, view=self)
            await interaction.followup.send('✅ Vote cancelled and removed from schedule.', ephemeral=True)

        except Exception as e:
            print(f'Error cancelling missed vote: {e}')
            await interaction.followup.send(f'❌ Error: {e}', ephemeral=True)

# Watch Cog
class WatchCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_scheduled_votes.start()  # Start the background task
        self.vote_timeout_tasks = {}  # Add this line to track timeout tasks

    # Watch command group
    watch_group = app_commands.Group(name='watch', description='Watch management commands')

    @watch_group.command(name='start', description='Declares the start of a FENZ watch without a vote.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        colour='The colour watch you want to start.',
        station='The station you are declaring the watch colour for.'
    )
    async def watch_start(
            self,
            interaction: discord.Interaction,
            colour: str,
            station: str
    ):
        try:
            # Role lock - CHECK FIRST before deferring
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='❌ You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            # Check for duplicate watch (same colour and station)
            for watch_data in active_watches.values():
                if watch_data['colour'] == colour and watch_data['station'] == station:
                    # Map colour names to Discord colours
                    colour_map = {
                        'Yellow': discord.Colour.gold(),
                        'Blue': discord.Colour.blue(),
                        'Brown': discord.Colour(0x8B4513),
                        'Red': discord.Colour.red()
                    }
                    embed_colour = colour_map.get(colour, discord.Colour.orange())

                    decline_embed = discord.Embed(
                        description=f'❌ A {colour} Watch for `{station}` is already active! End it first before starting a new one.',
                        colour=embed_colour
                    )
                    await interaction.response.send_message(embed=decline_embed, ephemeral=True)
                    return

            # NOW defer after permission check passes
            await interaction.response.defer()

            # Get guild-specific configuration
            guild_config = get_guild_config(interaction.guild.id)
            watch_channel_id = guild_config.get('watch_channel_id')
            watch_role_id = guild_config.get('watch_role_id')

            if not watch_channel_id:
                error_embed = discord.Embed(
                    description='❌ Watch channel not configured for this server!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # Get the specific watch channel
            watch_channel = interaction.guild.get_channel(watch_channel_id)

            if not watch_channel:
                error_embed = discord.Embed(
                    description='❌ Watch channel not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # Delete all bot messages in the watch channel
            try:
                async for message in watch_channel.history(limit=100):
                    if message.author == self.bot.user:
                        try:
                            await message.delete()
                        except Exception:
                            pass  # Ignore errors if message can't be deleted
            except Exception as e:
                print(f'Error deleting bot messages: {e}')

            # Map colour names to Discord colours
            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }

            # Get the embed colour based on selection
            embed_colour = colour_map.get(colour, discord.Colour.orange())

            # Create embed with matching colour
            embed = discord.Embed(title=f'🚨 {colour} Watch Announcement 🚨', colour=embed_colour)
            embed.add_field(name='Station', value=f'`{station}`', inline=True)
            embed.add_field(name='Time', value=f'<t:{int(interaction.created_at.timestamp())}:R>', inline=True)
            embed.add_field(name='Watch Leader', value=interaction.user.mention, inline=True)
            embed.add_field(name='‎', value='No need to vote just hop in!!\nIf you are joining, please join Fenz RTO 🙌',
                            inline=False)
            embed.add_field(name='‎', value='**Select the below reaction role to be notified of any future watches!**',
                            inline=False)
            embed.set_image(
                url='https://cdn.discordapp.com/attachments/1425867714160758896/1426932258694238258/image.png?ex=68f4eeb9&is=68f39d39&hm=b69f7f8bad7dcd7c7bde4dab731ca7e23e27d32d864cad9fc7224dcbb0648840')
            embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')
            embed.set_author(name=f'Requested by {interaction.user.display_name}',
                             icon_url=interaction.user.display_avatar.url)

            # Create button view
            view = WatchRoleButton(0)
            msg = await watch_channel.send(content=f'-# ||<@&{watch_role_id}> {interaction.user.mention} <@&1309021002675654700> <@&1365536209681514636>||' if watch_role_id else '', embed=embed, view=view)

            # Update view with actual message ID
            view.message_id = msg.id

            # Store watch data by message ID (allows multiple simultaneous watches)
            active_watches[str(msg.id)] = {
                'user_id': interaction.user.id,
                'user_name': interaction.user.display_name,
                'channel_id': watch_channel.id,
                'colour': colour,
                'station': station,
                'started_at': int(interaction.created_at.timestamp())
            }
            save_watches(active_watches)

            # Send confirmation to user
            success_embed = discord.Embed(
                description=f'✅ Watch started in {watch_channel.mention}!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error starting watch: {e}')
            # Send error DM to owner
            await interaction.client.send_error_dm('Vote button error', e, interaction)

            if not interaction.response.is_done():
                error_embed = discord.Embed(
                    description=f'❌ Error: {e}',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=error_embed, ephemeral=True)

            else:
                error_embed = discord.Embed(
                    description=f'❌ Error: {e}',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)

    # Autocomplete for colour
    @watch_start.autocomplete('colour')
    async def colour_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        colours = ['Yellow', 'Blue', 'Brown', 'Red']
        return [
            app_commands.Choice(name=colour, value=colour)
            for colour in colours if current.lower() in colour.lower()
        ]

    # Autocomplete for station
    @watch_start.autocomplete('station')
    async def station_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        stations = ['Station 1', 'Station 2']
        return [
            app_commands.Choice(name=station, value=station)
            for station in stations if current.lower() in station.lower()
        ]

    @watch_group.command(name='vote', description='Start a vote for a FENZ watch.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        colour='The colour watch you want to vote for.',
        station='The station you are voting for.',
        time='Time in minutes from now (optional).',
        votes='Required number of votes to pass.'
    )
    async def watch_vote(
            self,
            interaction: discord.Interaction,
            colour: str,
            station: str,
            votes: int,
            time: int = None
    ):
        try:
            # Role lock
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='❌ You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer()

            # Get guild-specific configuration
            guild_config = get_guild_config(interaction.guild.id)
            watch_channel_id = guild_config.get('watch_channel_id')
            watch_role_id = guild_config.get('watch_role_id')

            if not watch_channel_id:
                error_embed = discord.Embed(
                    description='❌ Watch channel not configured for this server!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            watch_channel = interaction.guild.get_channel(watch_channel_id)
            if not watch_channel:
                error_embed = discord.Embed(
                    description='❌ Watch channel not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # Calculate scheduled time
            current_time = int(discord.utils.utcnow().timestamp())
            scheduled_time = current_time + (time * 60) if time else current_time

            # Store scheduled vote
            scheduled_votes = load_scheduled_votes()
            vote_id = f"{interaction.guild.id}_{current_time}"

            scheduled_votes[vote_id] = {
                'guild_id': interaction.guild.id,
                'channel_id': watch_channel_id,
                'watch_role_id': watch_role_id,
                'user_id': interaction.user.id,
                'colour': colour,
                'station': station,
                'votes': votes,
                'time_minutes': time,
                'scheduled_time': scheduled_time,
                'created_at': current_time
            }

            save_scheduled_votes(scheduled_votes)

            # Send confirmation
            if time:
                scheduled_dt = datetime.datetime.fromtimestamp(scheduled_time, tz=datetime.timezone.utc)
                success_embed = discord.Embed(
                    description=f'✅ Vote scheduled for {discord.utils.format_dt(scheduled_dt, style="F")} ({discord.utils.format_dt(scheduled_dt, style="R")})',
                    colour=discord.Colour(0x2ecc71)
                )
            else:
                success_embed = discord.Embed(
                    description=f'✅ Vote will be sent immediately!',
                    colour=discord.Colour(0x2ecc71)
                )

            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error scheduling vote: {e}')
            await self.bot.send_error_dm('Watch vote command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    # Autocomplete for colour in vote command
    @watch_vote.autocomplete('colour')
    async def vote_colour_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        colours = ['Yellow', 'Blue', 'Brown', 'Red']
        return [
            app_commands.Choice(name=colour, value=colour)
            for colour in colours if current.lower() in colour.lower()
        ]

    # Autocomplete for station in vote command
    @watch_vote.autocomplete('station')
    async def vote_station_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        stations = ['Station 1', 'Station 2']
        return [
            app_commands.Choice(name=station, value=station)
            for station in stations if current.lower() in station.lower()
        ]

    @watch_group.command(name='logs', description='View the history of completed watches.')
    @app_commands.describe(
        limit='Number of recent watches to display (default: 50, max: 500)',
        per_page='Number of logs per page (default: 5, max: 10)'
    )
    async def watch_logs(
            self,
            interaction: discord.Interaction,
            limit: int = 50,
            per_page: int = 5
    ):
        try:
            # Check permissions
            allowed_role_ids = [1389550689113473024, 1333197141920710718]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='❌ You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # Load completed watches
            try:
                from database import load_completed_watches, save_completed_watches
                completed_watches = load_completed_watches()
            except FileNotFoundError:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            if not completed_watches:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            # Limit the number of logs and per_page
            limit = min(max(1, limit), 500)  # Between 1 and 500
            per_page = min(max(1, per_page), 10)  # Between 1 and 10

            # Sort by ended_at timestamp (most recent first)
            sorted_watches = sorted(
                completed_watches.items(),
                key=lambda x: x[1].get('ended_at', 0),
                reverse=True
            )[:limit]

            if not sorted_watches:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            # Create pages
            pages = []
            total_watches = len(sorted_watches)
            total_pages = (total_watches + per_page - 1) // per_page  # Ceiling division

            for page_num in range(total_pages):
                start_idx = page_num * per_page
                end_idx = min(start_idx + per_page, total_watches)
                page_watches = sorted_watches[start_idx:end_idx]

                # Create embed for this page
                page_embed = discord.Embed(
                    title='📋 Watch History',
                    description=f'Showing watches {start_idx + 1}-{end_idx} of {total_watches}',
                    colour=discord.Colour.blue()
                )
                page_embed.set_footer(text=f'Page {page_num + 1}/{total_pages}')

                for watch_id, watch_data in page_watches:
                    # Get user objects
                    started_by = interaction.guild.get_member(watch_data.get('user_id'))
                    ended_by = interaction.guild.get_member(watch_data.get('ended_by'))

                    started_by_name = started_by.display_name if started_by else watch_data.get('user_name', 'Unknown')
                    ended_by_name = ended_by.display_name if ended_by else 'Unknown'

                    # Format timestamps
                    started_at = watch_data.get('started_at', 0)
                    ended_at = watch_data.get('ended_at', 0)

                    # Calculate duration
                    duration_seconds = ended_at - started_at
                    duration_minutes = duration_seconds // 60
                    duration_hours = duration_minutes // 60
                    duration_minutes_remainder = duration_minutes % 60

                    if duration_hours > 0:
                        duration_str = f"{duration_hours}h {duration_minutes_remainder}m"
                    else:
                        duration_str = f"{duration_minutes}m"

                    # Check if it's a failed vote
                    if watch_data.get('status') == 'failed':
                        field_value = (
                            f"**Status:** ❌ FAILED\n"
                            f"**Reason:** {watch_data.get('reason', 'Unknown')}\n"
                            f"**Colour:** {watch_data.get('colour', 'Unknown')}\n"
                            f"**Station:** {watch_data.get('station', 'Unknown')}\n"
                            f"**Votes:** {watch_data.get('votes_received', 0)}/{watch_data.get('votes_required', 0)}\n"
                            f"**Started:** <t:{started_at}:f> (<t:{started_at}:R>)\n"
                            f"**Terminated:** <t:{ended_at}:f> (<t:{ended_at}:R>)"
                        )
                    # Your existing field_value code for successful watches

                    # Add field with colour emoji
                    colour_emoji = {
                        'Yellow': '🟡',
                        'Blue': '🔵',
                        'Brown': '🟤',
                        'Red': '🔴'
                    }.get(watch_data.get('colour', ''), '⚪')

                    page_embed.add_field(
                        name=f"{colour_emoji} {watch_data.get('colour', 'Unknown')} Watch - {watch_data.get('station', 'Unknown')}",
                        value=field_value,
                        inline=False
                    )

                pages.append(page_embed)

            # If only one page, send without pagination
            if len(pages) == 1:
                await interaction.followup.send(embed=pages[0], ephemeral=True)
            else:
                # Send with pagination
                view = LogsPaginationView(pages, interaction.user.id)
                await interaction.followup.send(embed=pages[0], view=view, ephemeral=True)

        except Exception as e:
            print(f'Error fetching watch logs: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Watch logs command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    @watch_group.command(name='delete-log', description='Delete a specific watch log.')
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        log='The watch log to delete (search by colour, station, or date).',
    )
    async def watch_delete_log(
            self,
            interaction: discord.Interaction,
            log: str,
    ):
        try:
            # Role check for specific role
            allowed_role_id = 1389550689113473024
            user_roles = [role.id for role in interaction.user.roles]

            if allowed_role_id not in user_roles:
                permission_embed = discord.Embed(
                    description='❌ You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # Load completed watches
            try:
                with open('completed_watches.json', 'r') as f:
                    completed_watches = json.load(f)
            except FileNotFoundError:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            # Check if log exists
            if log not in completed_watches:
                not_found_embed = discord.Embed(
                    description='❌ Watch log not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=not_found_embed, ephemeral=True)
                return

            # Get log details for confirmation message
            log_data = completed_watches[log]
            colour = log_data.get('colour', 'Unknown')
            station = log_data.get('station', 'Unknown')
            ended_at = log_data.get('ended_at', 0)

            # Format the ended time
            ended_datetime = datetime.datetime.fromtimestamp(ended_at, tz=datetime.timezone.utc)
            formatted_time = ended_datetime.strftime('%b %d, %Y at %I:%M %p UTC')

            # Delete the log
            del completed_watches[log]

            # Save updated logs
            save_completed_watches(completed_watches)

            success_embed = discord.Embed(
                description=f'✅ Deleted watch log:\n**{colour} Watch at {station}**\nEnded: {formatted_time}',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error deleting watch log: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Watch delete log command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    @watch_delete_log.autocomplete('log')
    async def delete_log_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        try:
            with open('completed_watches.json', 'r') as f:
                completed_watches = json.load(f)
        except FileNotFoundError:
            return []

        if not completed_watches:
            return []

        # Sort all watches by most recent first
        sorted_watches = sorted(
            completed_watches.items(),
            key=lambda x: x[1].get('ended_at', 0),
            reverse=True
        )

        choices = []
        for watch_id, data in sorted_watches:
            ended_at = data.get('ended_at', 0)
            ended_datetime = datetime.datetime.fromtimestamp(ended_at, tz=datetime.timezone.utc)
            formatted_time = ended_datetime.strftime('%b %d, %Y at %I:%M %p')

            # More detailed label for better searching
            colour = data.get('colour', 'Unknown')
            station = data.get('station', 'Unknown')
            attendees = data.get('attendees', 'N/A')

            label = f"{colour} Watch - {station} | {attendees} attendees | {formatted_time}"
            choices.append(app_commands.Choice(name=label[:100], value=watch_id))  # Discord has 100 char limit

        # Filter based on what user is typing
        if current:
            filtered = [
                choice for choice in choices
                if current.lower() in choice.name.lower()
            ]
        else:
            filtered = choices

        # Return only first 25 (Discord's limit)
        # User can search by colour, station, date, or attendee count to narrow down
        return filtered[:25]

    @watch_group.command(name='delete-all-logs', description='Delete all watch logs (Owner only).')
    @app_commands.default_permissions(administrator=True)
    async def watch_delete_all_logs(self, interaction: discord.Interaction):
        try:
            # Owner-only check
            if interaction.user.id != 678475709257089057:
                permission_embed = discord.Embed(
                    description='❌ This command is restricted to the bot owner only!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # Load completed watches
            try:
                with open('completed_watches.json', 'r') as f:
                    completed_watches = json.load(f)
            except FileNotFoundError:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            if not completed_watches:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            # Count logs
            log_count = len(completed_watches)

            # Clear all logs
            with open('completed_watches.json', 'w') as f:
                json.dump({}, f, indent=4)

            success_embed = discord.Embed(
                description=f'✅ Successfully deleted all {log_count} watch log(s)!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error deleting all watch logs: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Watch delete log command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    @watch_group.command(name='end', description='End an active watch.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        watch='The active watch to end.',
        attendees='Number of people who attended the watch.'
    )
    async def watch_end(
            self,
            interaction: discord.Interaction,
            watch: str,
            attendees: int
    ):
        try:
            # Check permissions
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='❌ You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # Check if the watch exists
            if watch not in active_watches:
                not_found_embed = discord.Embed(
                    description='❌ Watch not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=not_found_embed, ephemeral=True)
                return

            # Get watch data
            watch_data = active_watches[watch]
            channel = interaction.guild.get_channel(watch_data['channel_id'])

            if channel is None:
                error_embed = discord.Embed(
                    description='❌ Watch channel not found! The channel may have been deleted.',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)

                # Clean up the orphaned watch data
                del active_watches[watch]
                save_watches(active_watches)
                return

            try:
                message = await channel.fetch_message(int(watch))
            except discord.NotFound:
                error_embed = discord.Embed(
                    description='❌ Watch message not found! It may have been deleted.',
                    colour=discord.Colour(0xf24d4d)
                )

                await interaction.followup.send(embed=error_embed, ephemeral=True)
                del active_watches[watch]
                save_watches(active_watches)
                return

            # Get the original embed
            if not message.embeds:
                no_embed_error = discord.Embed(
                    description='❌ No embed found in that message!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_embed_error, ephemeral=True)
                return

            embed = message.embeds[0]

            # Keep the original watch color (don't change it)
            embed.title = f'🚨 {watch_data["colour"]} Watch - ENDED 🚨'

            # Clear all fields and add new ones
            embed.clear_fields()
            embed.add_field(
                name='‎',
                value=f'The {watch_data["colour"]} watch has now concluded. Thank you for attending this watch, and we hope to see you back with FENZ for another one!',
                inline=False
            )
            embed.add_field(
                name='Attendees',
                value=f'`{attendees}` people attended this watch',
                inline=False
            )
            embed.add_field(
                name='‎',
                value='**Select the below reaction role to be notified of any future watches!**',
                inline=False
            )
            embed.add_field(
                name='‎',
                value=f'-# Watch Ended {discord.utils.format_dt(discord.utils.utcnow(), style="F")}',
                inline=True
            )

            # Update author
            ended_by = interaction.user
            embed.set_author(name=f'Ended by {ended_by.display_name}', icon_url=ended_by.display_avatar.url)

            # Keep the button and send role ping (remove voters embed if it exists)
            guild_config = get_guild_config(interaction.guild.id)
            watch_role_id = guild_config.get('watch_role_id')

            await message.edit(content=f'-# ||<@&{watch_role_id}> {interaction.user.mention} <@&1309021002675654700> <@&1365536209681514636>||' if watch_role_id else '', embed=embed,
                               view=WatchRoleButton(int(watch)))

            # Store attendee data before removing from active watches
            ended_watch_data = active_watches[watch].copy()
            ended_watch_data['attendees'] = attendees
            ended_watch_data['ended_at'] = int(discord.utils.utcnow().timestamp())
            ended_watch_data['ended_by'] = interaction.user.id

            # Load completed watches
            try:
                with open('completed_watches.json', 'r') as f:
                    completed_watches = json.load(f)
            except FileNotFoundError:
                completed_watches = {}

            # Store completed watch data
            completed_watches[watch] = ended_watch_data

            # Save completed watches
            with open('completed_watches.json', 'w') as f:
                json.dump(completed_watches, f, indent=4)

            # Remove from storage
            del active_watches[watch]
            save_watches(active_watches)

            success_embed = discord.Embed(
                description=f'✅ Watch ended successfully with {attendees} attendees!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error ending watch: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Watch delete log command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    # Autocomplete for active watches
    @watch_end.autocomplete('watch')
    async def watch_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        choices = []
        for msg_id, data in active_watches.items():
            label = f"{data['colour']} Watch - {data['station']} (by {data.get('user_name', 'Unknown')})"
            choices.append(app_commands.Choice(name=label, value=msg_id))

        # Filter based on what user is typing
        return [
            choice for choice in choices
            if current.lower() in choice.name.lower()
        ][:25]  # Discord limit of 25 choices

    @watch_group.command(name='end-all', description='End and delete all active watches and votes (Owner only).')
    @app_commands.default_permissions(administrator=True)
    async def watch_end_all(self, interaction: discord.Interaction):
        try:
            # Owner-only check
            if interaction.user.id != 678475709257089057:
                permission_embed = discord.Embed(
                    description='❌ This command is restricted to the bot owner only!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # Check if there are any active watches
            if not active_watches:
                no_watches_embed = discord.Embed(
                    description='❌ No active watches to end!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_watches_embed, ephemeral=True)
                return

            # End all watches
            deleted_count = 0
            failed_count = 0

            # Create a copy of keys to avoid modifying dict during iteration
            watch_ids = list(active_watches.keys())

            for message_id in watch_ids:
                try:
                    watch_data = active_watches[message_id]
                    channel = interaction.guild.get_channel(watch_data['channel_id'])
                    message = await channel.fetch_message(int(message_id))

                    # Delete the message entirely
                    await message.delete()

                    # Remove from storage
                    del active_watches[message_id]
                    deleted_count += 1

                except Exception as e:
                    print(f'Error deleting watch {message_id}: {e}')
                    failed_count += 1

            # Save updated watch data
            save_watches(active_watches)

            # Send summary
            summary_embed = discord.Embed(
                description=f'✅ Successfully deleted {deleted_count} watch(es) and vote(s)!' +
                            (f'\n⚠️ Failed to delete {failed_count} watch(es)/vote(s).' if failed_count > 0 else ''),
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=summary_embed, ephemeral=True)

        except Exception as e:
            print(f'Error in end all watches: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Watch delete log command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    # BONUS: Add a new command to browse all logs for deletion
    @watch_group.command(name='browse-logs', description='Browse all watch logs.')
    @app_commands.describe(
        page='Page number to view (25 logs per page)',
        search='Filter by colour, station, or other text'
    )
    async def watch_browse_logs(
            self,
            interaction: discord.Interaction,
            page: int = 1,
            search: str = None
    ):
        try:
            # Role check
            allowed_role_id = 1389550689113473024, 1333197141920710718
            user_roles = [role.id for role in interaction.user.roles]

            if allowed_role_id not in user_roles:
                permission_embed = discord.Embed(
                    description='❌ You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # Load completed watches
            try:
                with open('completed_watches.json', 'r') as f:
                    completed_watches = json.load(f)
            except FileNotFoundError:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            if not completed_watches:
                no_logs_embed = discord.Embed(
                    description='❌ No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            # Sort by most recent
            sorted_watches = sorted(
                completed_watches.items(),
                key=lambda x: x[1].get('ended_at', 0),
                reverse=True
            )

            # Filter if search provided
            if search:
                filtered_watches = []
                search_lower = search.lower()
                for watch_id, data in sorted_watches:
                    searchable = f"{data.get('colour', '')} {data.get('station', '')} {data.get('user_name', '')}".lower()
                    if search_lower in searchable:
                        filtered_watches.append((watch_id, data))
                sorted_watches = filtered_watches

            if not sorted_watches:
                no_results_embed = discord.Embed(
                    description=f'❌ No logs found matching "{search}"!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_results_embed, ephemeral=True)
                return

            # Pagination
            per_page = 25
            total_logs = len(sorted_watches)
            total_pages = (total_logs + per_page - 1) // per_page
            page = max(1, min(page, total_pages))

            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, total_logs)
            page_logs = sorted_watches[start_idx:end_idx]

            # Create embed
            embed = discord.Embed(
                title='🗑️ Browse Logs for Deletion',
                description=f'Showing logs {start_idx + 1}-{end_idx} of {total_logs}' +
                            (f' (filtered by: "{search}")' if search else ''),
                colour=discord.Colour.orange()
            )
            embed.set_footer(text=f'Page {page}/{total_pages} | Use the Log ID with /watch deletelog')

            log_list = []
            for watch_id, data in page_logs:
                ended_at = data.get('ended_at', 0)
                ended_datetime = datetime.datetime.fromtimestamp(ended_at, tz=datetime.timezone.utc)
                formatted_time = ended_datetime.strftime('%b %d, %Y %I:%M %p')

                colour_emoji = {
                    'Yellow': '🟡',
                    'Blue': '🔵',
                    'Brown': '🟤',
                    'Red': '🔴'
                }.get(data.get('colour', ''), '⚪')

                log_entry = (
                    f"{colour_emoji} **{data.get('colour')} - {data.get('station')}**\n"
                    f"└ Ended: {formatted_time} | Attendees: {data.get('attendees', 'N/A')}\n"
                    f"└ Log ID: `{watch_id}`\n"
                )
                log_list.append(log_entry)

            embed.description += '\n\n' + '\n'.join(log_list)

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            print(f'Error browsing logs: {e}')
            # Send error DM to owner
            await self.bot.send_error_dm('Watch delete log command error', e, interaction)

            error_embed = discord.Embed(
                description=f'❌ Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)

    @tasks.loop(minutes=1)
    async def check_scheduled_votes(self):
        """Check every minute for votes that should be sent"""
        try:
            scheduled_votes = load_scheduled_votes()
            current_time = int(discord.utils.utcnow().timestamp())

            votes_to_send = []
            for vote_id, vote_data in list(scheduled_votes.items()):
                if vote_data['scheduled_time'] <= current_time:
                    votes_to_send.append((vote_id, vote_data))

            # Send all due votes
            for vote_id, vote_data in votes_to_send:
                await self.send_scheduled_vote(vote_data)
                del scheduled_votes[vote_id]

            if votes_to_send:
                save_scheduled_votes(scheduled_votes)

        except Exception as e:
            print(f'Error in check_scheduled_votes: {e}')

    @check_scheduled_votes.before_loop
    async def before_check_scheduled_votes(self):
        """Wait until bot is ready, then check for missed votes"""
        await self.bot.wait_until_ready()

        # Check for missed votes while bot was offline
        await self.check_missed_votes()

    async def check_missed_votes(self):
        """Check for votes that should have been sent while bot was offline and DM owner for confirmation"""
        try:
            scheduled_votes = load_scheduled_votes()
            current_time = int(discord.utils.utcnow().timestamp())

            missed_votes = []
            for vote_id, vote_data in scheduled_votes.items():
                if vote_data['scheduled_time'] < current_time:
                    missed_votes.append((vote_id, vote_data))

            if not missed_votes:
                return

            # DM the owner for each missed vote with confirmation buttons
            owner = await self.bot.fetch_user(OWNER_ID)

            for vote_id, vote_data in missed_votes:
                scheduled_dt = datetime.datetime.fromtimestamp(vote_data['scheduled_time'], tz=datetime.timezone.utc)
                missed_by_minutes = (current_time - vote_data['scheduled_time']) // 60

                embed = discord.Embed(
                    title='⚠️ Missed Scheduled Vote',
                    description=f"A **{vote_data['colour']} Watch Vote** for **{vote_data['station']}** was scheduled but not sent because the bot was offline.",
                    colour=discord.Colour.orange()
                )
                embed.add_field(name='Was Scheduled For',
                                value=f"<t:{vote_data['scheduled_time']}:F> (<t:{vote_data['scheduled_time']}:R>)",
                                inline=False)
                embed.add_field(name='Missed By', value=f"{missed_by_minutes} minutes", inline=True)
                embed.add_field(name='Required Votes', value=f"{vote_data['votes']}", inline=True)
                embed.add_field(name='Started By', value=f"<@{vote_data['user_id']}>", inline=True)

                if vote_data.get('time_minutes'):
                    embed.add_field(name='Watch Time', value=f"{vote_data['time_minutes']} minutes from vote send",
                                    inline=False)

                embed.set_footer(text=f"Vote ID: {vote_id}")

                # Create confirmation view
                view = MissedVoteConfirmationView(vote_id, vote_data, self)
                await owner.send(embed=embed, view=view)

        except Exception as e:
            print(f'Error checking missed votes: {e}')

    async def send_scheduled_vote(self, vote_data):
        """Send a scheduled vote to the channel"""
        try:
            guild = self.bot.get_guild(vote_data['guild_id'])
            if not guild:
                print(f"Guild {vote_data['guild_id']} not found")
                return

            watch_channel = guild.get_channel(vote_data['channel_id'])
            if not watch_channel:
                print(f"Channel {vote_data['channel_id']} not found")
                return

            # Recreate the embed
            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }
            embed_colour = colour_map.get(vote_data['colour'], discord.Colour.orange())

            embed = discord.Embed(title=f"🗳️ {vote_data['colour']} Watch Vote 🗳️", colour=embed_colour)
            embed.add_field(name='Station', value=f"`{vote_data['station']}`", inline=True)
            embed.add_field(name='Required Votes', value=f"`{vote_data['votes']}`", inline=True)

            # Calculate display time - use the ORIGINAL scheduled time from when command was entered
            original_scheduled_time = vote_data['scheduled_time']

            if vote_data.get('time_minutes'):
                # Watch time is time_minutes from the ORIGINAL scheduled send time
                watch_time_timestamp = original_scheduled_time + (vote_data['time_minutes'] * 60)
                embed.add_field(name='Time', value=f"<t:{watch_time_timestamp}:R>", inline=True)
            else:
                # If no time specified, show the original scheduled time
                embed.add_field(name='Time', value=f"<t:{original_scheduled_time}:R>", inline=True)

            embed.add_field(name='‎',
                            value=f"Vote up to participate in the {vote_data['colour']} watch at {vote_data['station']}!",
                            inline=False)
            embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')

            user = guild.get_member(vote_data['user_id'])
            if user:
                embed.set_author(name=f"Started by {user.display_name}", icon_url=user.display_avatar.url)

            # Create vote button view - pass the time_minutes from original schedule
            view = VoteButton(
                message_id=0,
                required_votes=vote_data['votes'],
                colour=vote_data['colour'],
                station=vote_data['station'],
                time_minutes=vote_data.get('time_minutes'),
                guild=guild,
                channel=watch_channel,
                cog=self  # Add this line
            )

            msg = await watch_channel.send(
                content=f"||<@&{vote_data['watch_role_id']}> <@{vote_data['user_id']}> <@&1309021002675654700> <@&1365536209681514636>||" if vote_data.get(
                    'watch_role_id') else '',
                embed=embed,
                view=view
            )

            view.message_id = msg.id
            # Start timeout task
            timeout_task = asyncio.create_task(
                self.handle_vote_timeout(msg.id, view, vote_data, watch_channel, guild)
            )
            self.vote_timeout_tasks[str(msg.id)] = timeout_task

        except Exception as e:
            print(f'Error sending scheduled vote: {e}')

    def cog_unload(self):
        """Clean up when cog is unloaded"""
        self.check_scheduled_votes.cancel()

    async def handle_vote_timeout(self, message_id: int, view: VoteButton, vote_data: dict, channel, guild):
        """Handle vote timeout when insufficient votes"""
        try:
            # Wait for the timeout duration
            timeout_duration = vote_data.get('time_minutes', 10) * 60
            await asyncio.sleep(timeout_duration)

            # Check if vote was cancelled (vote passed)
            if view.cancelled or view.vote_count >= view.required_votes:
                return

            # Fetch the message
            message = await channel.fetch_message(message_id)

            # Create failure embed (red color)
            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }

            failed_embed = discord.Embed(
                title=f"❌ {vote_data['colour']} Watch Vote - TERMINATED ❌",
                description="Insufficient votes received. Watch has been cancelled.",
                colour=discord.Colour(0xf24d4d)
            )
            failed_embed.add_field(name='Station', value=f"`{vote_data['station']}`", inline=True)
            failed_embed.add_field(name='Votes Received', value=f"`{view.vote_count}/{view.required_votes}`",
                                   inline=True)
            failed_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')

            # Disable all buttons
            for item in view.children:
                item.disabled = True

            # Re-ping roles
            guild_config = get_guild_config(guild.id)
            watch_role_id = guild_config.get('watch_role_id')

            await message.edit(
                content=f"||<@&{watch_role_id}> <@{vote_data['user_id']}> <@&1309021002675654700> <@&1365536209681514636>||" if watch_role_id else '',
                embed=failed_embed,
                view=view
            )

            # Log the failed vote
            try:
                with open('completed_watches.json', 'r') as f:
                    completed_watches = json.load(f)
            except FileNotFoundError:
                completed_watches = {}

            completed_watches[str(message_id)] = {
                'user_id': vote_data['user_id'],
                'colour': vote_data['colour'],
                'station': vote_data['station'],
                'started_at': vote_data['created_at'],
                'ended_at': int(discord.utils.utcnow().timestamp()),
                'status': 'failed',
                'reason': 'insufficient_votes',
                'votes_received': view.vote_count,
                'votes_required': view.required_votes,
                'ended_by': self.bot.user.id  # Bot ended it
            }

            with open('completed_watches.json', 'w') as f:
                json.dump(completed_watches, f, indent=4)

            # Clean up task tracking
            if str(message_id) in self.vote_timeout_tasks:
                del self.vote_timeout_tasks[str(message_id)]

        except asyncio.CancelledError:
            # Task was cancelled because vote succeeded
            pass
        except Exception as e:
            print(f'Error in vote timeout handler: {e}')

# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(WatchCog(bot))