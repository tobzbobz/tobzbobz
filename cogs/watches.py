from dotenv import load_dotenv

load_dotenv()
import discord
from discord.ext import commands
from discord import app_commands
from discord.ext import tasks
import asyncio
import datetime
import json

# Import database
from database import db, load_watches, load_scheduled_votes, load_completed_watches

# Bot owner ID
OWNER_ID = 678475709257089057

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'watch_channel_id': 1390867914462203914,
        'watch_role_id': 1390867686170300456
    },
    1425867713183744023: {
        'watch_channel_id': 1426190071115284502,
        'watch_role_id': 1426185588930777199
    }
}

CHANNEL_NAME_RULES = {
    "Station 1": {
        "Red": "「🔴1️⃣」active-watch",
        "Blue": "「🔵1️⃣」active-watch",
        "Yellow": "「🟡1️⃣」active-watch",
        "Brown": "「🟤1️⃣」active-watch"
    },
    "Station 2": {
        "Red": "「🔴2️⃣」active-watch",
        "Blue": "「🔵2️⃣」active-watch",
        "Yellow": "「🟡2️⃣」active-watch",
        "Brown": "「🟤2️⃣」active-watch"
    },
}


def get_guild_config(guild_id: int):
    """Get configuration for a specific guild"""
    return GUILD_CONFIGS.get(guild_id, {})


# Initialize as empty dict - will be loaded in cog __init__
active_watches = {}


# Vote button and view
class VoteButton(discord.ui.View):
    def __init__(self, message_id: int, required_votes: int, colour: str, station: str, time_minutes: int = None,
                 guild=None, channel=None, cog=None):
        super().__init__(timeout=None)
        self.message_id = message_id
        self.required_votes = required_votes
        self.voted_users = set()
        self.vote_count = 0
        self.colour = colour
        self.station = station
        self.time_minutes = time_minutes
        self.guild = guild
        self.channel = channel
        self.cog = cog
        self.cancelled = False

    @discord.ui.button(label='0', style=discord.ButtonStyle.green,
                       custom_id='vote_button', emoji='<:FENZ:1389200656090533970>')
    async def vote_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Toggle vote - if already voted, remove vote; otherwise add vote
            if interaction.user.id in self.voted_users:
                # Remove vote
                self.voted_users.remove(interaction.user.id)
                self.vote_count -= 1
                button.label = f'{self.vote_count}'

                await interaction.response.edit_message(view=self)
                removed_embed = discord.Embed(
                    description=f'<:Accepted:1426930333789585509> Vote removed! ({self.vote_count}/{self.required_votes})',
                    colour=discord.Colour(0x2ecc71)
                )
                await interaction.followup.send(embed=removed_embed, ephemeral=True)
                return

            # Add vote
            self.voted_users.add(interaction.user.id)
            self.vote_count += 1
            button.label = f'{self.vote_count}'

            if self.vote_count >= self.required_votes:
                # Cancel auto-cancel task if it exists
                if self.cog and f"auto_cancel_{self.message_id}" in self.cog.vote_timeout_tasks:
                    self.cog.vote_timeout_tasks[f"auto_cancel_{self.message_id}"].cancel()
                    del self.cog.vote_timeout_tasks[f"auto_cancel_{self.message_id}"]

                colour_map = {
                    'Yellow': discord.Colour.gold(),
                    'Blue': discord.Colour.blue(),
                    'Brown': discord.Colour(0x8B4513),
                    'Red': discord.Colour.red()
                }
                embed_colour = colour_map.get(self.colour, discord.Colour.orange())

                # Calculate watch start time
                if self.time_minutes:
                    watch_start_time = discord.utils.utcnow() + datetime.timedelta(minutes=self.time_minutes)
                    watch_start_timestamp = int(watch_start_time.timestamp())
                else:
                    watch_start_time = discord.utils.utcnow()
                    watch_start_timestamp = int(watch_start_time.timestamp())

                # Create watch start embed (skip the "Vote Passed" intermediate step)
                start_embed = discord.Embed(title=f'🚨 {self.colour} Watch Announcement 🚨', colour=embed_colour)
                start_embed.add_field(name='Station', value=f'`{self.station}`', inline=True)

                if self.time_minutes:
                    start_embed.add_field(name='Time', value=f'<t:{watch_start_timestamp}:R>', inline=True)
                else:
                    start_embed.add_field(name='Time', value=discord.utils.format_dt(discord.utils.utcnow(), style='R'),
                                          inline=True)

                start_embed.add_field(name='Watch Leader', value=interaction.user.mention, inline=True)
                start_embed.add_field(name='​',
                                      value='No need to vote just hop in!!\nIf you are joining, please join Fenz RTO 🙌',
                                      inline=False)
                start_embed.add_field(name='​',
                                      value='**Select the below reaction role to be notified of any future watches!**',
                                      inline=False)
                start_embed.set_image(
                    url='https://cdn.discordapp.com/attachments/1425867714160758896/1426932258694238258/image.png?ex=68f4eeb9&is=68f39d39&hm=b69f7f8bad7dcd7c7bde4dab731ca7e23e27d32d864cad9fc7224dcbb0648840')
                start_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')
                start_embed.set_author(name=f'Requested by {interaction.user.display_name}',
                                       icon_url=interaction.user.display_avatar.url)

                # Add voters embed
                voters_embed = discord.Embed(title='Voters:', colour=embed_colour)
                voter_mentions = []
                for user_id in self.voted_users:
                    user = interaction.guild.get_member(user_id)
                    if user:
                        voter_mentions.append(user.mention)
                voters_embed.description = '\n'.join(voter_mentions)

                watch_view = WatchRoleButton(self.message_id)
                guild_config = get_guild_config(interaction.guild.id)
                watch_role_id = guild_config.get('watch_role_id')

                # SINGLE response - edit message to watch state
                await interaction.response.edit_message(
                    content=f'-# ||<@&{watch_role_id}> {interaction.user.mention} <@&1285474077556998196> <@&1365536209681514636>||' if watch_role_id else '',
                    embeds=[start_embed, voters_embed],
                    view=watch_view
                )

                # Save to database
                await db.add_active_watch(
                    message_id=self.message_id,
                    guild_id=interaction.guild.id,
                    channel_id=interaction.channel.id,
                    user_id=interaction.user.id,
                    user_name=interaction.user.display_name,
                    colour=self.colour,
                    station=self.station,
                    started_at=interaction.created_at,
                    has_voters_embed=True
                )

                # Update in-memory cache
                active_watches[str(self.message_id)] = {
                    'user_id': interaction.user.id,
                    'user_name': interaction.user.display_name,
                    'channel_id': interaction.channel.id,
                    'colour': self.colour,
                    'station': self.station,
                    'started_at': int(interaction.created_at.timestamp()),
                    'has_voters_embed': True
                }

                # Cancel vote timeout task since vote passed
                if self.cog and str(self.message_id) in self.cog.vote_timeout_tasks:
                    self.cog.vote_timeout_tasks[str(self.message_id)].cancel()
                    del self.cog.vote_timeout_tasks[str(self.message_id)]

                # Schedule the actual watch start (if delayed)
                if self.cog and self.time_minutes:
                    delay_seconds = self.time_minutes * 60
                    start_task = asyncio.create_task(
                        self.cog.start_watch_after_vote(
                            channel=self.channel,
                            message_id=self.message_id,
                            user_id=interaction.user.id,
                            user_name=interaction.user.display_name,
                            colour=self.colour,
                            station=self.station,
                            watch_role_id=watch_role_id,
                            voters=list(self.voted_users),
                            delay_seconds=delay_seconds
                        )
                    )
                    self.cog.vote_timeout_tasks[f"start_{self.message_id}"] = start_task

            else:
                # Vote not yet passed - just update the button
                await interaction.response.edit_message(view=self)
                voted_embed = discord.Embed(
                    description=f'<:Accepted:1426930333789585509> Vote recorded! ({self.vote_count}/{self.required_votes})',
                    colour=discord.Colour(0x2ecc71)
                )
                await interaction.followup.send(embed=voted_embed, ephemeral=True)

        except Exception as e:
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            print(f'Error processing vote: {e}')
            raise

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
                    description='<:Denied:1426930694633816248> Watch role not configured for this server!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
                return

            role = interaction.guild.get_role(role_id)
            if not role:
                error_embed = discord.Embed(description='<:Denied:1426930694633816248> Role not found!',
                                            colour=discord.Colour(0xf24d4d))
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
                return

            if role in interaction.user.roles:
                await interaction.user.remove_roles(role)
                embed = discord.Embed(description=f'Removed {role.mention} role!', colour=discord.Colour(0xf24d4d))
                await interaction.response.send_message(embed=embed, ephemeral=True)
            else:
                await interaction.user.add_roles(role)
                embed = discord.Embed(description=f'Added {role.mention} role!', colour=discord.Colour(0x2ecc71))
                await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            await interaction.response.send_message(embed=error_embed, ephemeral=True)
            print(f'Error toggling role: {e}')
            raise


class LogsPaginationView(discord.ui.View):
    def __init__(self, pages: list, user_id: int):
        super().__init__(timeout=180)
        self.pages = pages
        self.current_page = 0
        self.user_id = user_id
        self.update_buttons()

    def update_buttons(self):
        self.first_button.disabled = self.current_page == 0
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= len(self.pages) - 1
        self.last_button.disabled = self.current_page >= len(self.pages) - 1

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message('<:Denied:1426930694633816248> This is not your logs view!',
                                                    ephemeral=True)
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
        super().__init__(timeout=None)
        self.vote_id = vote_id
        self.vote_data = vote_data
        self.cog = cog

    @discord.ui.button(label='Send Now', emoji='<:Accepted:1426930333789585509>', style=discord.ButtonStyle.green,
                       custom_id='send_missed_vote')
    async def send_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != OWNER_ID:
                await interaction.response.send_message(
                    '<:Denied:1426930694633816248> Only the bot owner can use this!', ephemeral=True)
                return

            await interaction.response.defer()
            await self.cog.send_scheduled_vote(self.vote_data)

            # Remove from database
            await db.remove_scheduled_vote(self.vote_id)

            embed = interaction.message.embeds[0]
            embed.colour = discord.Colour(0x2ecc71)
            embed.title = '<:Accepted:1426930333789585509> Missed Vote - SENT'

            for item in self.children:
                item.disabled = True

            await interaction.message.edit(embed=embed, view=self)
            await interaction.followup.send('<:Accepted:1426930333789585509> Vote sent successfully!', ephemeral=True)

        except Exception as e:
            print(f'Error sending missed vote: {e}')
            await interaction.followup.send(f'<:Denied:1426930694633816248> Error sending vote: {e}', ephemeral=True)

    @discord.ui.button(label='Cancel', emoji='<:Denied:1426930694633816248>', style=discord.ButtonStyle.red,
                       custom_id='cancel_missed_vote')
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != OWNER_ID:
                await interaction.response.send_message(
                    '<:Denied:1426930694633816248> Only the bot owner can use this!', ephemeral=True)
                return

            await interaction.response.defer()

            # Remove from database
            await db.remove_scheduled_vote(self.vote_id)

            embed = interaction.message.embeds[0]
            embed.colour = discord.Colour(0xf24d4d)
            embed.title = '<:Denied:1426930694633816248> Missed Vote - CANCELLED'

            for item in self.children:
                item.disabled = True

            await interaction.message.edit(embed=embed, view=self)
            await interaction.followup.send('<:Accepted:1426930333789585509> Vote cancelled and removed from schedule.',
                                            ephemeral=True)

        except Exception as e:
            print(f'Error cancelling missed vote: {e}')
            await interaction.followup.send(f'<:Denied:1426930694633816248> Error: {e}', ephemeral=True)


class WatchCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.vote_timeout_tasks = {}
        # Don't load immediately - wait for database
        self.bot.loop.create_task(self.initialize_cog())

    async def check_database_ready(self, interaction: discord.Interaction) -> bool:
        """Check if database is ready, send error message if not"""
        if db.pool is None:
            error_embed = discord.Embed(
                description='<:Denied:1426930694633816248> Database connection not ready. Please try again in a moment.',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            return False
        return True


    async def initialize_cog(self):
        """Initialize the cog after database is ready"""
        # Wait for bot to be ready
        await self.bot.wait_until_ready()

        # Wait for database connection
        max_wait = 30  # Maximum 30 seconds
        waited = 0
        while db.pool is None and waited < max_wait:
            print("⏳ WatchCog waiting for database connection...")
            await asyncio.sleep(1)
            waited += 1

        if db.pool is None:
            print("❌ WatchCog initialization failed: database connection timeout")
            return

        # Now load initial data
        await self.load_initial_data()

        # Start the scheduled votes checker
        self.check_scheduled_votes.start()

        print("✅ WatchCog initialized successfully")

    async def load_initial_data(self):
        """Load active watches from database on startup"""
        global active_watches
        try:
            active_watches = await load_watches()
            print(f'✅ Loaded {len(active_watches)} active watches from database')
        except Exception as e:
            print(f'❌ Error loading watches: {e}')
            active_watches = {}

    # Also update cog_unload to handle the case where check_scheduled_votes might not be started
    def cog_unload(self):
        """Clean up when cog is unloaded"""
        if self.check_scheduled_votes.is_running():
            self.check_scheduled_votes.cancel()

    watch_group = app_commands.Group(name='watch', description='Watch management commands')

    @watch_group.command(name='start', description='Declares the start of a FENZ watch without a vote.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        colour='The colour watch you want to start.',
        station='The station you are declaring the watch colour for.'
    )
    async def watch_start(self, interaction: discord.Interaction, colour: str, station: str):
        try:
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            for watch_data in active_watches.values():
                if watch_data['colour'] == colour and watch_data['station'] == station:
                    colour_map = {
                        'Yellow': discord.Colour.gold(),
                        'Blue': discord.Colour.blue(),
                        'Brown': discord.Colour(0x8B4513),
                        'Red': discord.Colour.red()
                    }
                    embed_colour = colour_map.get(colour, discord.Colour.orange())
                    decline_embed = discord.Embed(
                        description=f'<:Denied:1426930694633816248> A {colour} Watch for `{station}` is already active! End it first before starting a new one.',
                        colour=embed_colour
                    )
                    await interaction.response.send_message(embed=decline_embed, ephemeral=True)
                    return

            await interaction.response.defer()

            guild_config = get_guild_config(interaction.guild.id)
            watch_channel_id = guild_config.get('watch_channel_id')
            watch_role_id = guild_config.get('watch_role_id')

            if not watch_channel_id:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not configured for this server!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            watch_channel = interaction.guild.get_channel(watch_channel_id)
            if not watch_channel:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            try:
                deleted_count = 0
                async for message in watch_channel.history(limit=100):
                    try:
                        await message.delete()
                        deleted_count += 1
                    except (discord.Forbidden, discord.NotFound):
                        pass
                print(f'Cleaned {deleted_count} messages before watch start')
            except Exception as e:
                print(f'Error cleaning channel: {e}')

            # Delete only previous watches for the SAME station
            try:
                watches_to_delete = []
                # Find all active watches for this station in the channel
                for msg_id, watch_data in list(active_watches.items()):
                    if (watch_data.get('channel_id') == watch_channel_id and
                            watch_data.get('station') == station):
                        watches_to_delete.append(msg_id)

                # Delete those messages
                for msg_id in watches_to_delete:
                    try:
                        message = await watch_channel.fetch_message(int(msg_id))
                        await message.delete()
                        # Remove from database and memory
                        await db.remove_active_watch(int(msg_id))
                        if msg_id in active_watches:
                            del active_watches[msg_id]
                    except discord.NotFound:
                        # Message already deleted, just clean up data
                        await db.remove_active_watch(int(msg_id))
                        if msg_id in active_watches:
                            del active_watches[msg_id]
                    except Exception as e:
                        print(f'Error deleting previous watch {msg_id}: {e}')
            except Exception as e:
                print(f'Error cleaning up previous watches: {e}')

            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }
            embed_colour = colour_map.get(colour, discord.Colour.orange())

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

            view = WatchRoleButton(0)
            msg = await watch_channel.send(
                content=f'-# ||<@&{watch_role_id}> {interaction.user.mention} <@&1285474077556998196> <@&1365536209681514636>||' if watch_role_id else '',
                embed=embed,
                view=view
            )

            view.message_id = msg.id

            # Save to database
            # Save to database
            # Save to database
            try:
                await db.add_active_watch(
                    message_id=msg.id,
                    guild_id=interaction.guild.id,
                    channel_id=watch_channel.id,
                    user_id=interaction.user.id,
                    user_name=interaction.user.display_name,
                    colour=colour,
                    station=station,
                    started_at=interaction.created_at,  # ✅ Pass the datetime object directly
                    has_voters_embed=False,
                    related_messages=[msg.id]
                )


                # Update in-memory cache
                active_watches[str(msg.id)] = {
                    'user_id': interaction.user.id,
                    'user_name': interaction.user.display_name,
                    'channel_id': watch_channel.id,
                    'colour': colour,
                    'station': station,
                    'started_at': int(interaction.created_at.timestamp()),
                    'has_voters_embed': False,
                    'related_messages': [msg.id]
                }

            except Exception as e:
                import traceback
                traceback.print_exc()
                # Still let the watch message stay posted, just warn the user

            success_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Watch started in {watch_channel.mention}!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error starting watch: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_start.autocomplete('colour')
    async def colour_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        colours = ['Yellow', 'Blue', 'Brown', 'Red']
        return [app_commands.Choice(name=colour, value=colour) for colour in colours if
                current.lower() in colour.lower()]

    @watch_start.autocomplete('station')
    async def station_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        stations = ['Station 1', 'Station 2']
        return [app_commands.Choice(name=station, value=station) for station in stations if
                current.lower() in station.lower()]

    @watch_group.command(name='vote', description='Start a vote for a FENZ watch.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        colour='The colour watch you want to vote for.',
        station='The station you are voting for.',
        time='Time in minutes from now (optional).',
        votes='Required number of votes to pass.'
    )
    async def watch_vote(self, interaction: discord.Interaction, colour: str, station: str, votes: int,
                         time: int = None):
        try:
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer()

            guild_config = get_guild_config(interaction.guild.id)
            watch_channel_id = guild_config.get('watch_channel_id')
            watch_role_id = guild_config.get('watch_role_id')

            if not watch_channel_id:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not configured for this server!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            watch_channel = interaction.guild.get_channel(watch_channel_id)
            if not watch_channel:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            current_time = int(discord.utils.utcnow().timestamp())
            scheduled_time = current_time + (time * 60) if time else current_time
            vote_id = f"{interaction.guild.id}_{current_time}"

            # Save to database
            await db.add_scheduled_vote(
                vote_id=vote_id,
                guild_id=interaction.guild.id,
                channel_id=watch_channel_id,
                watch_role_id=watch_role_id,
                user_id=interaction.user.id,
                colour=colour,
                station=station,
                votes=votes,
                time_minutes=time,
                scheduled_time=scheduled_time,
                created_at=current_time
            )

            if not time:
                await self.send_scheduled_vote({
                    "guild_id": interaction.guild.id,
                    "channel_id": watch_channel_id,
                    "watch_role_id": watch_role_id,
                    "user_id": interaction.user.id,
                    "colour": colour,
                    "station": station,
                    "votes": votes,
                    "time_minutes": None,
                    "scheduled_time": scheduled_time,
                    "created_at": current_time
                })
                await db.remove_scheduled_vote(vote_id)

            if time:
                scheduled_dt = datetime.datetime.fromtimestamp(scheduled_time, tz=timezone.utc)
                success_embed = discord.Embed(
                    description=f'<:Accepted:1426930333789585509> Vote scheduled for {discord.utils.format_dt(scheduled_dt, style="F")} ({discord.utils.format_dt(scheduled_dt, style="R")})',
                    colour=discord.Colour(0x2ecc71)
                )
            else:
                success_embed = discord.Embed(
                    description=f'<:Accepted:1426930333789585509> Vote will be sent immediately!',
                    colour=discord.Colour(0x2ecc71)
                )

            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error scheduling vote: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_vote.autocomplete('colour')
    async def vote_colour_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        colours = ['Yellow', 'Blue', 'Brown', 'Red']
        return [app_commands.Choice(name=colour, value=colour) for colour in colours if
                current.lower() in colour.lower()]

    @watch_vote.autocomplete('station')
    async def vote_station_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        stations = ['Station 1', 'Station 2']
        return [app_commands.Choice(name=station, value=station) for station in stations if
                current.lower() in station.lower()]

    @watch_group.command(name='end', description='End an active watch.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        watch='The active watch to end.',
        attendees='Number of people who attended the watch.'
    )
    async def watch_end(self, interaction: discord.Interaction, watch: str, attendees: int):
        try:
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            if watch not in active_watches:
                not_found_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=not_found_embed, ephemeral=True)
                return

            watch_data = active_watches[watch]
            channel = interaction.guild.get_channel(watch_data['channel_id'])

            if channel is None:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not found! The channel may have been deleted.',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                await db.remove_active_watch(int(watch))
                del active_watches[watch]
                return

            # Get the original message
            try:
                original_message = await channel.fetch_message(int(watch))
            except discord.NotFound:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch message not found! It may have been deleted.',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                await db.remove_active_watch(int(watch))
                del active_watches[watch]
                return

            # DELETE ALL RELATED MESSAGES (boosts, etc.)
            related_messages = watch_data.get('related_messages', [int(watch)])
            for msg_id in related_messages:
                try:
                    msg_to_delete = await channel.fetch_message(msg_id)
                    await msg_to_delete.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
                except Exception as e:
                    print(f'Error deleting related message {msg_id}: {e}')

            # DELETE ALL USER MESSAGES (keep bot messages) since watch started
            try:
                deleted_count = 0
                async for message in channel.history(after=discord.Object(id=int(watch)), limit=None):
                    # Delete if it's a user message (not from a bot)
                    if not message.author.bot:
                        try:
                            await message.delete()
                            deleted_count += 1
                        except (discord.Forbidden, discord.NotFound):
                            pass
                print(f'Deleted {deleted_count} user messages from watch')
            except Exception as e:
                print(f'Error deleting user messages: {e}')

            # CREATE NEW ENDED EMBED
            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }
            embed_colour = colour_map.get(watch_data["colour"], discord.Colour.orange())

            embed = discord.Embed(
                title=f'🚨 {watch_data["colour"]} Watch - ENDED 🚨',
                colour=embed_colour
            )
            embed.add_field(
                name='​',
                value=f'The {watch_data["colour"]} watch has now concluded. Thank you for attending this watch, and we hope to see you back with FENZ for another one!',
                inline=False
            )
            embed.add_field(
                name='Attendees',
                value=f'`{attendees}` people attended this watch',
                inline=False
            )
            embed.add_field(
                name='​',
                value='**Select the below reaction role to be notified of any future watches!**',
                inline=False
            )
            embed.add_field(
                name='​',
                value=f'-# Watch Ended {discord.utils.format_dt(discord.utils.utcnow(), style="F")}',
                inline=True
            )
            embed.set_image(
                url='https://cdn.discordapp.com/attachments/1425867714160758896/1426932258694238258/image.png?ex=68f4eeb9&is=68f39d39&hm=b69f7f8bad7dcd7c7bde4dab731ca7e23e27d32d864cad9fc7224dcbb0648840')
            embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')

            ended_by = interaction.user
            embed.set_author(name=f'Ended by {ended_by.display_name}', icon_url=ended_by.display_avatar.url)

            # SEND NEW MESSAGE (no ping)
            await channel.send(embed=embed, view=WatchRoleButton(0))

            # Save to completed watches database
            await db.add_completed_watch(
                message_id=int(watch),
                guild_id=interaction.guild.id,
                channel_id=watch_data['channel_id'],
                user_id=watch_data['user_id'],
                user_name=watch_data['user_name'],
                colour=watch_data['colour'],
                station=watch_data['station'],
                started_at=watch_data['started_at'],
                ended_at=int(discord.utils.utcnow().timestamp()),
                ended_by=interaction.user.id,
                attendees=attendees,
                status='completed',
                has_voters_embed=watch_data.get('has_voters_embed', False),
                original_colour=watch_data.get('original_colour'),
                original_station=watch_data.get('original_station'),
                switch_history=json.dumps(watch_data.get('switch_history', []))
            )

            # Remove from active watches
            await db.remove_active_watch(int(watch))
            del active_watches[watch]

            success_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Watch ended successfully with {attendees} attendees!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error ending watch: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_end.autocomplete('watch')
    async def watch_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        choices = []
        for msg_id, data in active_watches.items():
            label = f"{data['colour']} Watch - {data['station']} (by {data.get('user_name', 'Unknown')})"
            choices.append(app_commands.Choice(name=label, value=msg_id))
        return [choice for choice in choices if current.lower() in choice.name.lower()][:25]

    @watch_group.command(name='logs', description='View the history of completed watches.')
    @app_commands.describe(
        limit='Number of recent watches to display (default: 50, max: 500)',
        per_page='Number of logs per page (default: 5, max: 10)'
    )
    async def watch_logs(self, interaction: discord.Interaction, limit: int = 50, per_page: int = 5):
        try:
            # ✅ Check database first
            if not await self.check_database_ready(interaction):
                return

            # Permission check
            allowed_role_ids = [1389550689113473024, 1333197141920710718]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            # ✅ Now safe to call database
            completed_watches = await load_completed_watches()

            if not completed_watches:
                no_logs_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            limit = min(max(1, limit), 500)
            per_page = min(max(1, per_page), 10)

            sorted_watches = sorted(
                completed_watches.items(),
                key=lambda x: x[1].get('ended_at', 0),
                reverse=True
            )[:limit]

            if not sorted_watches:
                no_logs_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> No watch logs found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_logs_embed, ephemeral=True)
                return

            pages = []
            total_watches = len(sorted_watches)
            total_pages = (total_watches + per_page - 1) // per_page

            for page_num in range(total_pages):
                start_idx = page_num * per_page
                end_idx = min(start_idx + per_page, total_watches)
                page_watches = sorted_watches[start_idx:end_idx]

                page_embed = discord.Embed(
                    title='📋 Watch History',
                    description=f'Showing watches {start_idx + 1}-{end_idx} of {total_watches}',
                    colour=discord.Colour.blue()
                )
                page_embed.set_footer(text=f'Page {page_num + 1}/{total_pages}')

                for watch_id, watch_data in page_watches:
                    started_by = interaction.guild.get_member(watch_data.get('user_id'))
                    ended_by = interaction.guild.get_member(watch_data.get('ended_by'))

                    started_by_name = started_by.display_name if started_by else watch_data.get('user_name', 'Unknown')
                    ended_by_name = ended_by.display_name if ended_by else 'Unknown'

                    started_at = watch_data.get('started_at', 0)
                    ended_at = watch_data.get('ended_at', 0)

                    duration_seconds = ended_at - started_at
                    duration_minutes = duration_seconds // 60
                    duration_hours = duration_minutes // 60
                    duration_minutes_remainder = duration_minutes % 60

                    if duration_hours > 0:
                        duration_str = f"{duration_hours}h {duration_minutes_remainder}m"
                    else:
                        duration_str = f"{duration_minutes}m"

                    if watch_data.get('status') == 'failed':
                        field_value = (
                            f"**Status:** <:Denied:1426930694633816248> FAILED\n"
                            f"**Reason:** {watch_data.get('reason', 'Unknown')}\n"
                            f"**Colour:** {watch_data.get('colour', 'Unknown')}\n"
                            f"**Station:** {watch_data.get('station', 'Unknown')}\n"
                            f"**Votes:** {watch_data.get('votes_received', 0)}/{watch_data.get('votes_required', 0)}\n"
                            f"**Started:** <t:{started_at}:f> (<t:{started_at}:R>)\n"
                            f"**Terminated:** <t:{ended_at}:f> (<t:{ended_at}:R>)"
                        )
                    else:
                        field_value = (
                            f"**Started by:** {started_by_name}\n"
                            f"**Ended by:** {ended_by_name}\n"
                            f"**Duration:** {duration_str}\n"
                            f"**Attendees:** {watch_data.get('attendees', 'N/A')}\n"
                            f"**Started:** <t:{started_at}:f> (<t:{started_at}:R>)\n"
                            f"**Ended:** <t:{ended_at}:f> (<t:{ended_at}:R>)"
                        )

                    # Add switch history if exists
                    switch_history = watch_data.get('switch_history', [])
                    if switch_history:
                        switches = []
                        for switch in switch_history:
                            switch_time = f"<t:{switch['timestamp']}:t>"
                            changes = []
                            if 'from_colour' in switch:
                                changes.append(f"{switch['from_colour']}→{switch['to_colour']}")
                            if 'from_station' in switch:
                                changes.append(f"{switch['from_station']}→{switch['to_station']}")
                            switches.append(f"{' & '.join(changes)} at {switch_time}")

                        field_value += f"\n**Switches:** {', '.join(switches)}"

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

            if len(pages) == 1:
                await interaction.followup.send(embed=pages[0], ephemeral=True)
            else:
                view = LogsPaginationView(pages, interaction.user.id)
                await interaction.followup.send(embed=pages[0], view=view, ephemeral=True)

        except Exception as e:
            print(f'Error fetching watch logs: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))

            # Handle if response not sent yet
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_group.command(name='delete-log', description='Delete a specific watch log.')
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(log='The watch log to delete (search by colour, station, or date).')
    async def watch_delete_log(self, interaction: discord.Interaction, log: str):
        try:
            allowed_role_id = 1389550689113473024
            user_roles = [role.id for role in interaction.user.roles]

            if allowed_role_id not in user_roles:
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            completed_watches = await load_completed_watches()

            if log not in completed_watches:
                not_found_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch log not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=not_found_embed, ephemeral=True)
                return

            log_data = completed_watches[log]
            colour = log_data.get('colour', 'Unknown')
            station = log_data.get('station', 'Unknown')
            ended_at = log_data.get('ended_at', 0)

            ended_datetime = datetime.datetime.datetime.fromtimestamp(ended_at, tz=datetime.timezone.utc)
            formatted_time = ended_datetime.strftime('%b %d, %Y at %I:%M %p UTC')

            # Delete from database
            await db.delete_completed_watch(int(log))

            success_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Deleted watch log:\n**{colour} Watch at {station}**\nEnded: {formatted_time}',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error deleting watch log: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_delete_log.autocomplete('log')
    async def delete_log_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        try:
            completed_watches = await load_completed_watches()
        except:
            return []

        if not completed_watches:
            return []

        sorted_watches = sorted(
            completed_watches.items(),
            key=lambda x: x[1].get('ended_at', 0),
            reverse=True
        )

        choices = []
        for watch_id, data in sorted_watches:
            ended_at = data.get('ended_at', 0)
            ended_datetime = datetime.datetime.datetime.fromtimestamp(ended_at, tz=datetime.timezone.utc)
            formatted_time = ended_datetime.strftime('%b %d, %Y at %I:%M %p')

            colour = data.get('colour', 'Unknown')
            station = data.get('station', 'Unknown')
            attendees = data.get('attendees', 'N/A')

            label = f"{colour} Watch - {station} | {attendees} attendees | {formatted_time}"
            choices.append(app_commands.Choice(name=label[:100], value=watch_id))

        if current:
            filtered = [choice for choice in choices if current.lower() in choice.name.lower()]
        else:
            filtered = choices

        return filtered[:25]

    @watch_group.command(name='end-all', description='End and delete all active watches and votes (Owner only).')
    @app_commands.default_permissions(administrator=True)
    async def watch_end_all(self, interaction: discord.Interaction):
        try:
            if interaction.user.id != OWNER_ID:
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> This command is restricted to the bot owner only!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            if not active_watches:
                no_watches_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> No active watches to end!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=no_watches_embed, ephemeral=True)
                return

            deleted_count = 0
            failed_count = 0

            watch_ids = list(active_watches.keys())

            for message_id in watch_ids:
                try:
                    watch_data = active_watches[message_id]
                    channel = interaction.guild.get_channel(watch_data['channel_id'])
                    message = await channel.fetch_message(int(message_id))
                    await message.delete()

                    # Remove from database
                    await db.remove_active_watch(int(message_id))
                    del active_watches[message_id]
                    deleted_count += 1

                except Exception as e:
                    print(f'Error deleting watch {message_id}: {e}')
                    failed_count += 1

            summary_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Successfully deleted {deleted_count} watch(es) and vote(s)!' +
                            (f'\n⚠️ Failed to delete {failed_count} watch(es)/vote(s).' if failed_count > 0 else ''),
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=summary_embed, ephemeral=True)

        except Exception as e:
            print(f'Error in end all watches: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @tasks.loop(minutes=1)
    async def check_scheduled_votes(self):
        """Check every minute for votes that should be sent"""
        try:
            scheduled_votes = await load_scheduled_votes()
            current_time = int(discord.utils.utcnow().timestamp())

            votes_to_send = []
            for vote_id, vote_data in list(scheduled_votes.items()):
                if vote_data['scheduled_time'] <= current_time:
                    votes_to_send.append((vote_id, vote_data))

            for vote_id, vote_data in votes_to_send:
                await self.send_scheduled_vote(vote_data)
                await db.remove_scheduled_vote(vote_id)

        except Exception as e:
            print(f'Error in check_scheduled_votes: {e}')

    @check_scheduled_votes.before_loop
    async def before_check_scheduled_votes(self):
        """Wait until bot is ready, then check for missed votes"""
        await self.bot.wait_until_ready()
        await self.check_missed_votes()

    async def check_missed_votes(self):
        """Check for votes that should have been sent while bot was offline"""
        try:
            scheduled_votes = await load_scheduled_votes()
            current_time = int(discord.utils.utcnow().timestamp())

            missed_votes = []
            for vote_id, vote_data in scheduled_votes.items():
                if vote_data['scheduled_time'] < current_time:
                    missed_votes.append((vote_id, vote_data))

            if not missed_votes:
                return

            owner = await self.bot.fetch_user(OWNER_ID)

            for vote_id, vote_data in missed_votes:
                scheduled_dt = datetime.datetime.datetime.fromtimestamp(vote_data['scheduled_time'], tz=datetime.timezone.utc)
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

            # CLEAN UP CHANNEL BEFORE SENDING VOTE
            try:
                deleted_count = 0
                async for message in watch_channel.history(limit=100):
                    try:
                        await message.delete()
                        deleted_count += 1
                    except (discord.Forbidden, discord.NotFound):
                        pass
                print(f'Cleaned {deleted_count} messages before vote')
            except Exception as e:
                print(f'Error cleaning channel: {e}')

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

            original_scheduled_time = vote_data['scheduled_time']

            if vote_data.get('time_minutes'):
                watch_time_timestamp = original_scheduled_time + (vote_data['time_minutes'] * 60)
                embed.add_field(name='Time', value=f"<t:{watch_time_timestamp}:R>", inline=True)
            else:
                embed.add_field(name='Time', value=f"<t:{original_scheduled_time}:R>", inline=True)

            embed.add_field(name='‎',
                            value=f"Vote up to participate in the {vote_data['colour']} watch at {vote_data['station']}!",
                            inline=False)
            embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')

            user = guild.get_member(vote_data['user_id'])
            if user:
                embed.set_author(name=f"Started by {user.display_name}", icon_url=user.display_avatar.url)

            view = VoteButton(
                message_id=0,
                required_votes=vote_data['votes'],
                colour=vote_data['colour'],
                station=vote_data['station'],
                time_minutes=vote_data.get('time_minutes'),
                guild=guild,
                channel=watch_channel,
                cog=self
            )

            msg = await watch_channel.send(
                content=f"||<@&{vote_data['watch_role_id']}><@{vote_data['user_id']}><@&1285474077556998196><@&1365536209681514636>||" if vote_data.get(
                    'watch_role_id') else '',
                embed=embed,
                view=view
            )

            view.message_id = msg.id

            # Schedule auto-cancel ONLY if time_minutes is set (not immediate)
            if vote_data.get('time_minutes'):
                cancel_task = asyncio.create_task(
                    self.auto_cancel_vote(msg.id, view, vote_data, watch_channel, guild)
                )
                self.vote_timeout_tasks[f"auto_cancel_{msg.id}"] = cancel_task

        except Exception as e:
            print(f'Error sending scheduled vote: {e}')

    async def auto_cancel_vote(self, message_id: int, view: VoteButton, vote_data: dict, channel, guild):
        """Auto-cancel vote when scheduled time arrives if insufficient votes"""
        try:
            # Wait until the scheduled watch time
            timeout_duration = vote_data.get('time_minutes', 10) * 60
            await asyncio.sleep(timeout_duration)

            # Check if vote passed or was cancelled
            if view.cancelled or view.vote_count >= view.required_votes:
                return

            # Vote did not pass - cancel it
            message = await channel.fetch_message(message_id)

            cancelled_embed = discord.Embed(
                title=f"<:Denied:1426930694633816248> {vote_data['colour']} Watch Vote - CANCELLED <:Denied:1426930694633816248>",
                description="Insufficient votes received by scheduled time. Watch has been cancelled.",
                colour=discord.Colour(0xf24d4d)
            )
            cancelled_embed.add_field(name='Station', value=f"`{vote_data['station']}`", inline=True)
            cancelled_embed.add_field(name='Votes Received', value=f"`{view.vote_count}/{view.required_votes}`",
                                      inline=True)
            cancelled_embed.add_field(name='Scheduled Time',
                                      value=f"<t:{vote_data['scheduled_time'] + (vote_data.get('time_minutes', 0) * 60)}:F>",
                                      inline=True)
            cancelled_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')

            for item in view.children:
                item.disabled = True

            guild_config = get_guild_config(guild.id)
            watch_role_id = guild_config.get('watch_role_id')

            await message.edit(
                content=f"||<@&{watch_role_id}> <@{vote_data['user_id']}> <@&1285474077556998196> <@&1365536209681514636>||" if watch_role_id else '',
                embed=cancelled_embed,
                view=view
            )

            # Log failed vote to database
            await db.add_completed_watch(
                message_id=message_id,
                guild_id=guild.id,
                channel_id=channel.id,
                user_id=vote_data['user_id'],
                user_name='',
                colour=vote_data['colour'],
                station=vote_data['station'],
                started_at=vote_data['created_at'],
                ended_at=int(discord.utils.utcnow().timestamp()),
                ended_by=self.bot.user.id,
                status='failed',
                reason='insufficient_votes_at_scheduled_time',
                votes_received=view.vote_count,
                votes_required=view.required_votes
            )

            if f"auto_cancel_{message_id}" in self.vote_timeout_tasks:
                del self.vote_timeout_tasks[f"auto_cancel_{message_id}"]

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f'Error in auto-cancel handler: {e}')

    async def handle_vote_timeout(self, message_id: int, view: VoteButton, vote_data: dict, channel, guild):
        """Handle vote timeout when insufficient votes"""
        try:
            timeout_duration = vote_data.get('time_minutes', 10) * 60
            await asyncio.sleep(timeout_duration)

            if view.cancelled or view.vote_count >= view.required_votes:
                return

            message = await channel.fetch_message(message_id)

            failed_embed = discord.Embed(
                title=f"<:Denied:1426930694633816248> {vote_data['colour']} Watch Vote - TERMINATED <:Denied:1426930694633816248>",
                description="Insufficient votes received. Watch has been cancelled.",
                colour=discord.Colour(0xf24d4d)
            )
            failed_embed.add_field(name='Station', value=f"`{vote_data['station']}`", inline=True)
            failed_embed.add_field(name='Votes Received', value=f"`{view.vote_count}/{view.required_votes}`",
                                   inline=True)
            failed_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')

            for item in view.children:
                item.disabled = True

            guild_config = get_guild_config(guild.id)
            watch_role_id = guild_config.get('watch_role_id')

            await message.edit(
                content=f"||<@&{watch_role_id}><@{vote_data['user_id']}><@&1285474077556998196><@&1365536209681514636>||" if watch_role_id else '',
                embed=failed_embed,
                view=view
            )

            # Log failed vote to database
            await db.add_completed_watch(
                message_id=message_id,
                guild_id=guild.id,
                channel_id=channel.id,
                user_id=vote_data['user_id'],
                user_name='',
                colour=vote_data['colour'],
                station=vote_data['station'],
                started_at=vote_data['created_at'],
                ended_at=int(discord.utils.utcnow().timestamp()),
                ended_by=self.bot.user.id,
                status='failed',
                reason='insufficient_votes',
                votes_received=view.vote_count,
                votes_required=view.required_votes
            )

            if str(message_id) in self.vote_timeout_tasks:
                del self.vote_timeout_tasks[str(message_id)]

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f'Error in vote timeout handler: {e}')

    def cog_unload(self):
        """Clean up when cog is unloaded"""
        self.check_scheduled_votes.cancel()

    async def start_watch_after_vote(self, channel, message_id: int, user_id: int, user_name: str,
                                     colour: str, station: str, watch_role_id: int, voters: list,
                                     delay_seconds: int):
        """Start a watch after the vote delay period"""
        try:
            # Wait for the delay period
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)

            # Delete the vote passed message
            try:
                vote_message = await channel.fetch_message(message_id)
                await vote_message.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                print(f'Error deleting vote message: {e}')

            # Create the actual watch start embed
            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }
            embed_colour = colour_map.get(colour, discord.Colour.orange())

            start_embed = discord.Embed(title=f'🚨 {colour} Watch Announcement 🚨', colour=embed_colour)
            start_embed.add_field(name='Station', value=f'`{station}`', inline=True)
            start_embed.add_field(name='Time', value=discord.utils.format_dt(discord.utils.utcnow(), style='R'),
                                  inline=True)
            start_embed.add_field(name='Watch Leader', value=f'<@{user_id}>', inline=True)
            start_embed.add_field(name='​',
                                  value='No need to vote just hop in!!\nIf you are joining, please join Fenz RTO 🙌',
                                  inline=False)
            start_embed.add_field(name='​',
                                  value='**Select the below reaction role to be notified of any future watches!**',
                                  inline=False)
            start_embed.set_image(
                url='https://cdn.discordapp.com/attachments/1425867714160758896/1426932258694238258/image.png?ex=68f4eeb9&is=68f39d39&hm=b69f7f8bad7dcd7c7bde4dab731ca7e23e27d32d864cad9fc7224dcbb0648840')
            start_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')
            start_embed.set_author(name=f'Requested by {interaction.user.display_name}', icon_url=interaction.user.display_avatar.url)

            # Add voters embed
            voters_embed = discord.Embed(title='Voters:', colour=embed_colour)
            voter_mentions = []
            for voter_id in voters:
                voter_mentions.append(f'<@{voter_id}>')
            voters_embed.description = '\n'.join(voter_mentions)

            view = WatchRoleButton(0)

            # Send new watch message
            msg = await channel.send(
                content=f'-# ||<@&{watch_role_id}><@{user_id}><@&1285474077556998196><@&1365536209681514636>||' if watch_role_id else '',
                embeds=[start_embed, voters_embed],
                view=view
            )

            view.message_id = msg.id

            # Save to database as active watch
            await db.add_active_watch(
                message_id=msg.id,
                guild_id=channel.guild.id,
                channel_id=channel.id,
                user_id=user_id,
                user_name=user_name,
                colour=colour,
                station=station,
                started_at=discord.utils.utcnow(),
                has_voters_embed=True,
                related_messages=[msg.id]
            )

            # Update in-memory cache
            active_watches[str(msg.id)] = {
                'user_id': user_id,
                'user_name': user_name,
                'channel_id': channel.id,
                'colour': colour,
                'station': station,
                'started_at': int(discord.utils.utcnow().timestamp()),
                'has_voters_embed': True,
                'related_messages': [msg.id]
            }

            # Clean up the task from tracking
            if f"start_{message_id}" in self.vote_timeout_tasks:
                del self.vote_timeout_tasks[f"start_{message_id}"]

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f'Error starting watch after vote: {e}')
            import traceback
            traceback.print_exc()

    @watch_group.command(name='switch', description='Switch an active watch to a different colour/station.')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        watch='The active watch to switch.',
        new_colour='New colour for the watch (optional).',
        new_station='New station for the watch (optional).'
    )
    async def watch_switch(self, interaction: discord.Interaction, watch: str,
                           new_colour: str = None, new_station: str = None):
        try:
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            if new_colour is None and new_station is None:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You must specify at least one parameter to switch (colour or station)!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            if watch not in active_watches:
                not_found_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=not_found_embed, ephemeral=True)
                return

            watch_data = active_watches[watch]
            old_colour = watch_data['colour']
            old_station = watch_data['station']

            # Use old values if new ones not provided
            final_colour = new_colour if new_colour else old_colour
            final_station = new_station if new_station else old_station

            # Check if switching to the EXACT SAME watch (no changes at all)
            if final_colour == old_colour and final_station == old_station:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> This watch is already that colour and station! No changes to make.',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # Check if only switching colour (station stays the same) but that colour already exists at this station
            if final_station == old_station and final_colour != old_colour:
                for msg_id, other_watch in active_watches.items():
                    if (msg_id != watch and
                            other_watch.get('colour') == final_colour and
                            other_watch.get('station') == final_station):
                        colour_map = {
                            'Yellow': discord.Colour.gold(),
                            'Blue': discord.Colour.blue(),
                            'Brown': discord.Colour(0x8B4513),
                            'Red': discord.Colour.red()
                        }
                        embed_colour = colour_map.get(final_colour, discord.Colour.orange())

                        conflict_embed = discord.Embed(
                            description=f'<:Denied:1426930694633816248> A {final_colour} Watch for `{final_station}` is already active! End it first before switching to this colour.',
                            colour=embed_colour
                        )
                        await interaction.followup.send(embed=conflict_embed, ephemeral=True)
                        return

            # Check if only switching station (colour stays the same) but that station already has this colour
            if final_colour == old_colour and final_station != old_station:
                for msg_id, other_watch in active_watches.items():
                    if (msg_id != watch and
                            other_watch.get('colour') == final_colour and
                            other_watch.get('station') == final_station):
                        colour_map = {
                            'Yellow': discord.Colour.gold(),
                            'Blue': discord.Colour.blue(),
                            'Brown': discord.Colour(0x8B4513),
                            'Red': discord.Colour.red()
                        }
                        embed_colour = colour_map.get(final_colour, discord.Colour.orange())

                        conflict_embed = discord.Embed(
                            description=f'<:Denied:1426930694633816248> A {final_colour} Watch for `{final_station}` is already active! End it first before switching to this station.',
                            colour=embed_colour
                        )
                        await interaction.followup.send(embed=conflict_embed, ephemeral=True)
                        return

            # Check if switching BOTH colour AND station - validate the new combination doesn't exist
            if final_colour != old_colour and final_station != old_station:
                for msg_id, other_watch in active_watches.items():
                    if (msg_id != watch and
                            other_watch.get('colour') == final_colour and
                            other_watch.get('station') == final_station):
                        colour_map = {
                            'Yellow': discord.Colour.gold(),
                            'Blue': discord.Colour.blue(),
                            'Brown': discord.Colour(0x8B4513),
                            'Red': discord.Colour.red()
                        }
                        embed_colour = colour_map.get(final_colour, discord.Colour.orange())

                        conflict_embed = discord.Embed(
                            description=f'<:Denied:1426930694633816248> A {final_colour} Watch for `{final_station}` is already active! End it first before switching to this combination.',
                            colour=embed_colour
                        )
                        await interaction.followup.send(embed=conflict_embed, ephemeral=True)
                        return

            channel = interaction.guild.get_channel(watch_data['channel_id'])
            if channel is None:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            guild_config = get_guild_config(interaction.guild.id)
            watch_channel_id = guild_config.get('watch_channel_id')

            # Delete the original watch message
            try:
                original_message = await channel.fetch_message(int(watch))
                await original_message.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                print(f'Error deleting original watch message: {e}')

            # Create new watch announcement
            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }
            embed_colour = colour_map.get(final_colour, discord.Colour.orange())

            embed = discord.Embed(title=f'🚨 {final_colour} Watch Announcement 🚨', colour=embed_colour)
            embed.add_field(name='Station', value=f'`{final_station}`', inline=True)
            embed.add_field(name='Time', value=f'<t:{watch_data["started_at"]}:R>', inline=True)
            embed.add_field(name='Watch Leader', value=f'<@{watch_data["user_id"]}>', inline=True)
            embed.add_field(name='‎', value='No need to vote just hop in!!\nIf you are joining, please join Fenz RTO 🙌',
                            inline=False)

            # Add switch information
            switch_info = []
            if new_colour and new_colour != old_colour:
                switch_info.append(f'**Colour changed:** {old_colour} → {final_colour}')
            if new_station and new_station != old_station:
                switch_info.append(f'**Station changed:** {old_station} → {final_station}')

            embed.add_field(name='‎', value='**Select the below reaction role to be notified of any future watches!**',
                            inline=False)
            embed.set_image(
                url='https://cdn.discordapp.com/attachments/1425867714160758896/1426932258694238258/image.png?ex=68f4eeb9&is=68f39d39&hm=b69f7f8bad7dcd7c7bde4dab731ca7e23e27d32d864cad9fc7224dcbb0648840')
            embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')
            embed.set_author(name=f'Switched by {interaction.user.display_name}',
                             icon_url=interaction.user.display_avatar.url)

            view = WatchRoleButton(0)

            # Only ping specific roles for switch
            msg = await channel.send(
                content=f'-# ||<@&1285474077556998196><@&1365536209681514636><@&1390867686170300456><@{watch_data["user_id"]}><@{interaction.user.id}>||',
                embed=embed,
                view=view
            )

            view.message_id = msg.id

            # Update database with switch information
            switch_timestamp = int(discord.utils.utcnow().timestamp())
            import json

            # Get existing switch history
            switch_history = watch_data.get('switch_history', [])
            if isinstance(switch_history, str):
                switch_history = json.loads(switch_history) if switch_history else []

            # Add new switch entry
            switch_entry = {
                'timestamp': switch_timestamp,
                'switched_by': interaction.user.id,
                'switched_by_name': interaction.user.display_name
            }
            if new_colour:
                switch_entry['from_colour'] = old_colour
                switch_entry['to_colour'] = final_colour
            if new_station:
                switch_entry['from_station'] = old_station
                switch_entry['to_station'] = final_station

            switch_history.append(switch_entry)

            # Update active watch with new message ID and switch data
            await db.remove_active_watch(int(watch))
            # Update active watch with new message ID and switch data
            await db.remove_active_watch(int(watch))

            try:
                await db.add_active_watch(
                    message_id=msg.id,
                    guild_id=interaction.guild.id,
                    channel_id=channel.id,
                    user_id=watch_data['user_id'],
                    user_name=watch_data['user_name'],
                    colour=final_colour,
                    station=final_station,
                    started_at=watch_data['started_at'],
                    has_voters_embed=watch_data.get('has_voters_embed', False),
                    original_colour=watch_data.get('original_colour', old_colour),
                    original_station=watch_data.get('original_station', old_station),
                    switch_history=json.dumps(switch_history)
                )

                # Update in-memory cache
                del active_watches[watch]
                active_watches[str(msg.id)] = {
                    'user_id': watch_data['user_id'],
                    'user_name': watch_data['user_name'],
                    'channel_id': channel.id,
                    'colour': final_colour,
                    'station': final_station,
                    'started_at': watch_data['started_at'],
                    'has_voters_embed': watch_data.get('has_voters_embed', False),
                    'original_colour': watch_data.get('original_colour', old_colour),
                    'original_station': watch_data.get('original_station', old_station),
                    'switch_history': switch_history
                }

                print(f"✅ Successfully saved switched watch {msg.id}")

            except Exception as e:
                # Database save failed - critical error
                import traceback
                traceback.print_exc()
                print(f"❌ CRITICAL: Failed to save switched watch {msg.id}: {e}")

                # Delete the watch message since we couldn't save it
                try:
                    await msg.delete()
                except:
                    pass

                error_embed = discord.Embed(
                    description=f'<:Denied:1426930694633816248> Failed to save watch to database. Please try again.',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return  # Exit early

                # THIS IS OUTSIDE THE TRY-EXCEPT - runs if everything succeeded
            success_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Watch switched successfully!\n' + '\n'.join(switch_info),
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error switching watch: {e}')
            error_embed = discord.Embed(
                description=f'<:Denied:1426930694633816248> Error: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_switch.autocomplete('watch')
    async def switch_watch_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        choices = []
        for msg_id, data in active_watches.items():
            label = f"{data['colour']} Watch - {data['station']} (by {data.get('user_name', 'Unknown')})"
            choices.append(app_commands.Choice(name=label, value=msg_id))
        return [choice for choice in choices if current.lower() in choice.name.lower()][:25]

    @watch_switch.autocomplete('new_colour')
    async def switch_colour_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        colours = ['Yellow', 'Blue', 'Brown', 'Red']
        return [app_commands.Choice(name=colour, value=colour) for colour in colours if
                current.lower() in colour.lower()]

    @watch_switch.autocomplete('new_station')
    async def switch_station_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        stations = ['Station 1', 'Station 2']
        return [app_commands.Choice(name=station, value=station) for station in stations if
                current.lower() in station.lower()]

    @watch_group.command(name='low', description='Boost an active watch to encourage more people to join!')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(watch='The active watch to boost.')
    async def watch_boost(self, interaction: discord.Interaction, watch: str):
        try:
            allowed_role_ids = [1285474077556998196, 1389550689113473024, 1365536209681514636]
            user_roles = [role.id for role in interaction.user.roles]

            if not any(role_id in user_roles for role_id in allowed_role_ids):
                permission_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> You do not have permission to use this command!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=permission_embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            if watch not in active_watches:
                not_found_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=not_found_embed, ephemeral=True)
                return

            watch_data = active_watches[watch]
            channel = interaction.guild.get_channel(watch_data['channel_id'])

            if channel is None:
                error_embed = discord.Embed(
                    description='<:Denied:1426930694633816248> Watch channel not found!',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # DELETE ALL PREVIOUS BOOST MESSAGES for this watch
            related_messages = watch_data.get('related_messages', [int(watch)])
            # Keep only the original watch message
            for msg_id in related_messages:
                if msg_id != int(watch):  # Don't delete the main watch message
                    try:
                        msg_to_delete = await channel.fetch_message(msg_id)
                        await msg_to_delete.delete()
                    except (discord.NotFound, discord.Forbidden):
                        pass
                    except Exception as e:
                        print(f'Error deleting boost message {msg_id}: {e}')

            guild_config = get_guild_config(interaction.guild.id)
            watch_role_id = guild_config.get('watch_role_id')

            colour_map = {
                'Yellow': discord.Colour.gold(),
                'Blue': discord.Colour.blue(),
                'Brown': discord.Colour(0x8B4513),
                'Red': discord.Colour.red()
            }
            embed_colour = colour_map.get(watch_data['colour'], discord.Colour.orange())

            boost_embed = discord.Embed(
                title=f"🚨 {watch_data['colour']} Watch Boost - Join Now! 🚨",
                description=f"A **{watch_data['colour']} Watch** is currently active at **{watch_data['station']}**!\n\nWe need more people to join! If you're available, hop in now!",
                colour=embed_colour
            )
            boost_embed.add_field(name='Station', value=f"`{watch_data['station']}`", inline=True)
            boost_embed.add_field(name='Watch Leader', value=f"<@{watch_data['user_id']}>", inline=True)
            boost_embed.add_field(name='Started', value=f"<t:{watch_data['started_at']}:R>", inline=True)
            boost_embed.add_field(name='​', value='Join Fenz RTO and help out! 🙌', inline=False)
            boost_embed.set_thumbnail(url='https://cdn.discordapp.com/emojis/1389200656090533970.webp?size=128')
            boost_embed.set_footer(text=f'Boosted by {interaction.user.display_name}',
                                   icon_url=interaction.user.display_avatar.url)

            boost_msg = await channel.send(
                content=f'||<@&{watch_role_id}> <@&1285474077556998196> <@&1365536209681514636>||' if watch_role_id else '',
                embed=boost_embed
            )

            # ADD NEW BOOST MESSAGE to tracked messages
            watch_data['related_messages'] = [int(watch), boost_msg.id]
            active_watches[watch] = watch_data

            # Update database
            await db.update_watch_related_messages(int(watch), watch_data['related_messages'])

            success_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Watch boosted successfully in {channel.mention}!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error boosting watch: {e}')
            error_embed = discord.Embed(description=f'<:Denied:1426930694633816248> Error: {e}',
                                        colour=discord.Colour(0xf24d4d))
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            raise

    @watch_boost.autocomplete('watch')
    async def boost_watch_autocomplete(self, interaction: discord.Interaction, current: str) -> list[
        app_commands.Choice[str]]:
        choices = []
        for msg_id, data in active_watches.items():
            label = f"{data['colour']} Watch - {data['station']} (by {data.get('user_name', 'Unknown')})"
            choices.append(app_commands.Choice(name=label, value=msg_id))
        return [choice for choice in choices if current.lower() in choice.name.lower()][:25]

    async def update_watch_channel_name(self, channel: discord.TextChannel, colour: str, station: str, state: str):

        try:
            # Simplify station to short form
            station_clean = station.strip()
            colour_clean = colour.strip()

            base_name = CHANNEL_NAME_RULES.get(station_clean, {}).get(colour_clean)

            if not base_name:
                station_simple = station_clean.lower().replace(" ", "-")
                colour_simple = colour_clean.lower()
                base_name = f"「⚫」watches"

            if state == 'voting':
                new_name = f"「🗳️」watch-voting"
            elif state == 'waiting':
                new_name = f"「🟠」watch-soon"
            elif state == 'ended':
                new_name = f"「⚫」watches"
            else:
                new_name = f"「⚫」watches"

            await channel.edit(name=new_name)
            print(f"✅ Channel renamed to {new_name}")

        except discord.Forbidden:
            print("⚠️ Missing permissions to rename channel.")
        except Exception as e:
            print(f"⚠️ Error renaming channel: {e}")

async def setup(bot):
    await bot.add_cog(WatchCog(bot))
