import discord
from discord.ext import commands
from discord import app_commands
import wavelink
from typing import Optional
import asyncio
from database import db

OWNER_ID = 678475709257089057


class MusicCog(commands.Cog):
    """Music player cog using free public Lavalink nodes"""

    def __init__(self, bot):
        self.bot = bot

    async def is_whitelisted(self, user_id: int) -> bool:
        """Check if a user is whitelisted for music commands"""
        if user_id == OWNER_ID:
            return True

        try:
            result = await db.fetchrow(
                'SELECT * FROM music_access WHERE user_id = $1 AND is_whitelisted = TRUE',
                str(user_id)
            )
            return result is not None
        except Exception as e:
            print(f"Error checking whitelist: {e}")
            return False

    async def cog_load(self):
        """Setup Wavelink node when cog loads"""
        await self.bot.wait_until_ready()

        # List of free public Lavalink nodes
        # If one is down, it'll try the next one
        public_nodes = [
            {
                'uri': 'https://lavalink.devz.cloud',
                'password': 'mathiaslavalink'
            },
            {
                'uri': 'https://lavalink.oops.wtf',
                'password': 'www.freelavalink.ga'
            },
            {
                'uri': 'https://lavalink.clmc.host',
                'password': 'LAVA'
            }
        ]

        # Try to connect to nodes
        nodes_to_connect = []
        for node_info in public_nodes:
            try:
                node = wavelink.Node(
                    uri=node_info['uri'],
                    password=node_info['password']
                )
                nodes_to_connect.append(node)
            except Exception as e:
                print(f"⚠️ Failed to create node {node_info['uri']}: {e}")

        if not nodes_to_connect:
            print("<:Denied:1426930694633816248> No Lavalink nodes available!")
            return

        try:
            await wavelink.Pool.connect(client=self.bot, nodes=nodes_to_connect)
            print(f"<:Accepted:1426930333789585509> Connected to {len(nodes_to_connect)} Lavalink node(s)")
        except Exception as e:
            print(f"<:Denied:1426930694633816248> Failed to connect to Lavalink: {e}")

    @commands.Cog.listener()
    async def on_wavelink_node_ready(self, payload: wavelink.NodeReadyEventPayload):
        """Event fired when a Wavelink node is ready"""
        print(f"🎵 Wavelink node {payload.node.identifier} is ready!")

    @commands.Cog.listener()
    async def on_wavelink_track_start(self, payload: wavelink.TrackStartEventPayload):
        """Event fired when a track starts playing"""
        player: wavelink.Player = payload.player
        track: wavelink.Playable = payload.track

        embed = discord.Embed(
            title="🎵 Now Playing",
            description=f"**[{track.title}]({track.uri})**",
            color=discord.Color.green()
        )

        if track.artwork:
            embed.set_thumbnail(url=track.artwork)

        embed.add_field(name="Artist", value=track.author, inline=True)
        embed.add_field(name="Duration", value=self.format_duration(track.length), inline=True)

        if hasattr(player, 'text_channel') and player.text_channel:
            try:
                await player.text_channel.send(embed=embed)
            except:
                pass

    @commands.Cog.listener()
    async def on_wavelink_track_end(self, payload: wavelink.TrackEndEventPayload):
        """Event fired when a track ends"""
        player: wavelink.Player = payload.player

        # Auto-disconnect if queue is empty
        if player.queue.is_empty and not player.playing:
            await asyncio.sleep(180)  # Wait 3 minutes
            if player.queue.is_empty and not player.playing:
                await player.disconnect()
                if hasattr(player, 'text_channel') and player.text_channel:
                    try:
                        await player.text_channel.send("ℹ️ Queue finished. Disconnected due to inactivity.")
                    except:
                        pass

    def format_duration(self, milliseconds: int) -> str:
        """Format duration from milliseconds to readable format"""
        seconds = milliseconds // 1000
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    async def ensure_voice(self, interaction: discord.Interaction) -> Optional[wavelink.Player]:
        """Ensure user is in voice channel and bot can connect"""
        if not interaction.user.voice:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You must be in a voice channel to use music commands!",
                ephemeral=True
            )
            return None

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            # Check if any nodes are available
            if not wavelink.Pool.nodes:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Music service is currently unavailable. No Lavalink nodes connected.",
                    ephemeral=True
                )
                return None

            # Join the user's voice channel
            try:
                player = await interaction.user.voice.channel.connect(cls=wavelink.Player)
                player.text_channel = interaction.channel
                player.autoplay = wavelink.AutoPlayMode.enabled
            except Exception as e:
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> Failed to connect to voice channel: {str(e)}",
                    ephemeral=True
                )
                return None

        return player

    m_group = app_commands.Group(name="m", description="Music player commands")

    @m_group.command(name="play", description="Play a song or add it to queue")
    @app_commands.describe(query="Song name or URL (YouTube, SoundCloud, etc.)")
    async def play(self, interaction: discord.Interaction, query: str):
        """Play a song"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        await interaction.response.defer()

        player = await self.ensure_voice(interaction)
        if not player:
            return

        try:
            # Search for tracks
            tracks: wavelink.Search = await wavelink.Playable.search(query)

            if not tracks:
                await interaction.followup.send("<:Denied:1426930694633816248> No tracks found!")
                return

            # If playlist, add all tracks
            if isinstance(tracks, wavelink.Playlist):
                added: int = await player.queue.put_wait(tracks)
                embed = discord.Embed(
                    title="📚 Playlist Added to Queue",
                    description=f"Added **{added}** tracks from **{tracks.name}**",
                    color=discord.Color.blue()
                )
                await interaction.followup.send(embed=embed)
            else:
                # Add first track to queue
                track: wavelink.Playable = tracks[0]
                await player.queue.put_wait(track)

                embed = discord.Embed(
                    title="➕ Added to Queue",
                    description=f"**[{track.title}]({track.uri})**",
                    color=discord.Color.blue()
                )

                if track.artwork:
                    embed.set_thumbnail(url=track.artwork)

                embed.add_field(name="Artist", value=track.author, inline=True)
                embed.add_field(name="Duration", value=self.format_duration(track.length), inline=True)
                embed.add_field(name="Position in Queue", value=str(len(player.queue)), inline=True)

                await interaction.followup.send(embed=embed)

            # Start playing if not already
            if not player.playing:
                await player.play(player.queue.get())

        except wavelink.LavalinkException as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Lavalink error: {str(e)}\n\n💡 The public node might be down. Try again in a moment.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    @m_group.command(name="pause", description="Pause or resume playback")
    async def pause(self, interaction: discord.Interaction):
        """Toggle pause/resume playback"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!", ephemeral=True)
            return

        if not player.current:
            await interaction.response.send_message("<:Denied:1426930694633816248> Nothing is playing!", ephemeral=True)
            return

        # Toggle pause state
        await player.pause(not player.paused)

        if player.paused:
            await interaction.response.send_message("⏸️ Paused playback")
        else:
            await interaction.response.send_message("▶️ Resumed playback")

    @m_group.command(name="skip", description="Skip the current song")
    async def skip(self, interaction: discord.Interaction):
        """Skip current track"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player or not player.playing:
            await interaction.response.send_message("<:Denied:1426930694633816248> Nothing is playing!", ephemeral=True)
            return

        await player.skip(force=True)
        await interaction.response.send_message("⏭️ Skipped track")

    @m_group.command(name="stop", description="Stop playback and clear queue")
    async def stop(self, interaction: discord.Interaction):
        """Stop playback and clear queue"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!", ephemeral=True)
            return

        player.queue.clear()
        await player.stop()
        await interaction.response.send_message("⏹️ Stopped playback and cleared queue")

    @m_group.command(name="leave", description="Disconnect the bot from voice")
    async def disconnect(self, interaction: discord.Interaction):
        """Disconnect from voice channel"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!", ephemeral=True)
            return

        await player.disconnect()
        await interaction.response.send_message("👋 Disconnected from voice channel")

    @m_group.command(name="queue", description="View the current queue")
    async def queue(self, interaction: discord.Interaction):
        """Display the current queue"""
        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!", ephemeral=True)
            return

        if player.queue.is_empty and not player.current:
            await interaction.response.send_message("<:Denied:1426930694633816248> Queue is empty!", ephemeral=True)
            return

        embed = discord.Embed(
            title="📋 Current Queue",
            color=discord.Color.blue()
        )

        # Now Playing
        if player.current:
            embed.add_field(
                name="🎵 Now Playing",
                value=f"**[{player.current.title}]({player.current.uri})**\n"
                      f"By {player.current.author} | {self.format_duration(player.current.length)}",
                inline=False
            )

        # Queue
        if not player.queue.is_empty:
            queue_list = []
            for i, track in enumerate(player.queue[:10], start=1):
                queue_list.append(f"`{i}.` **{track.title}** | {self.format_duration(track.length)}")

            if len(player.queue) > 10:
                queue_list.append(f"*...and {len(player.queue) - 10} more tracks*")

            embed.add_field(
                name="🔜 Up Next",
                value="\n".join(queue_list),
                inline=False
            )

        embed.set_footer(text=f"Total tracks in queue: {len(player.queue)} | Using free public nodes")

        await interaction.response.send_message(embed=embed)

    @m_group.command(name="playing", description="Show currently playing song")
    async def nowplaying(self, interaction: discord.Interaction):
        """Display current track"""
        player: wavelink.Player = interaction.guild.voice_client

        if not player or not player.current:
            await interaction.response.send_message("<:Denied:1426930694633816248> Nothing is playing!", ephemeral=True)
            return

        track = player.current
        position = player.position

        embed = discord.Embed(
            title="🎵 Now Playing",
            description=f"**[{track.title}]({track.uri})**",
            color=discord.Color.green()
        )

        if track.artwork:
            embed.set_thumbnail(url=track.artwork)

        embed.add_field(name="Artist", value=track.author, inline=True)
        embed.add_field(name="Duration", value=self.format_duration(track.length), inline=True)
        embed.add_field(
            name="Progress",
            value=f"{self.format_duration(position)} / {self.format_duration(track.length)}",
            inline=True
        )

        # Progress bar
        progress = int((position / track.length) * 20)
        bar = "▬" * progress + "🔘" + "▬" * (20 - progress)
        embed.add_field(name="⏳", value=bar, inline=False)

        await interaction.response.send_message(embed=embed)

    @m_group.command(name="volume", description="Set player volume (Owner only)")
    @app_commands.describe(volume="Volume level (0-100)")
    async def volume(self, interaction: discord.Interaction, volume: int):
        """Set player volume"""
        # Owner only check
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("<:Denied:1426930694633816248> Only the bot owner can change volume!", ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!", ephemeral=True)
            return

        if not 0 <= volume <= 100:
            await interaction.response.send_message("<:Denied:1426930694633816248> Volume must be between 0 and 100!", ephemeral=True)
            return

        await player.set_volume(volume)
        await interaction.response.send_message(f"🔊 Set volume to **{volume}%**")

    @m_group.command(name="shuffle", description="Shuffle the queue")
    async def shuffle(self, interaction: discord.Interaction):
        """Shuffle the queue"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player or player.queue.is_empty:
            await interaction.response.send_message("<:Denied:1426930694633816248> Queue is empty!", ephemeral=True)
            return

        player.queue.shuffle()
        await interaction.response.send_message("🔀 Shuffled the queue")

    @m_group.command(name="loop", description="Toggle loop mode")
    @app_commands.describe(mode="Loop mode")
    @app_commands.choices(mode=[
        app_commands.Choice(name="Off", value="off"),
        app_commands.Choice(name="Track", value="track"),
        app_commands.Choice(name="Queue", value="queue")
    ])
    async def loop(self, interaction: discord.Interaction, mode: str):
        """Set loop mode"""
        # Check whitelist
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message("<:Denied:1426930694633816248> You don't have permission to use music commands!",
                                                    ephemeral=True)
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!", ephemeral=True)
            return

        if mode == "off":
            player.queue.mode = wavelink.QueueMode.normal
            await interaction.response.send_message("🔁 Loop mode: **Off**")
        elif mode == "track":
            player.queue.mode = wavelink.QueueMode.loop
            await interaction.response.send_message("🔂 Loop mode: **Track**")
        elif mode == "queue":
            player.queue.mode = wavelink.QueueMode.loop_all
            await interaction.response.send_message("🔁 Loop mode: **Queue**")

    @m_group.command(name="list", description="Manage music access whitelist (Owner only)")
    @app_commands.describe(
        action="Add or remove user from whitelist",
        user="User to whitelist/blacklist"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="Whitelist", value="whitelist"),
        app_commands.Choice(name="Blacklist", value="blacklist"),
        app_commands.Choice(name="View", value="view")
    ])
    async def access_list(self, interaction: discord.Interaction, action: str, user: Optional[discord.User] = None):
        """Manage music access whitelist"""
        # Owner only check
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("<:Denied:1426930694633816248> Only the bot owner can manage the access list!", ephemeral=True)
            return

        if action == "view":
            try:
                result = await db.fetch('SELECT * FROM music_access')

                if not result:
                    await interaction.response.send_message("📋 No users in the access list yet.", ephemeral=True)
                    return

                embed = discord.Embed(
                    title="🎵 Music Access List",
                    color=discord.Color.blue()
                )

                whitelisted = []
                blacklisted = []

                for row in result:
                    try:
                        discord_user = await self.bot.fetch_user(int(row['user_id']))
                        user_str = f"• {discord_user.mention} ({discord_user.name})"
                    except:
                        user_str = f"• User ID: {row['user_id']}"

                    if row['is_whitelisted']:
                        whitelisted.append(user_str)
                    else:
                        blacklisted.append(user_str)

                if whitelisted:
                    embed.add_field(
                        name="<:Accepted:1426930333789585509> Whitelisted Users",
                        value="\n".join(whitelisted),
                        inline=False
                    )

                if blacklisted:
                    embed.add_field(
                        name="<:Denied:1426930694633816248> Blacklisted Users",
                        value="\n".join(blacklisted),
                        inline=False
                    )

                if not whitelisted and not blacklisted:
                    embed.description = "No users in the access list"

                await interaction.response.send_message(embed=embed, ephemeral=True)

            except Exception as e:
                await interaction.response.send_message(f"<:Denied:1426930694633816248> Error fetching access list: {str(e)}", ephemeral=True)
            return

        if not user:
            await interaction.response.send_message("<:Denied:1426930694633816248> You must specify a user for whitelist/blacklist actions!",
                                                    ephemeral=True)
            return

        if user.id == OWNER_ID:
            await interaction.response.send_message("<:Denied:1426930694633816248> Cannot modify access for the bot owner!", ephemeral=True)
            return

        try:
            if action == "whitelist":
                # Check if user already exists
                result = await db.fetchrow(
                    'SELECT * FROM music_access WHERE user_id = $1',
                    str(user.id)
                )

                if result:
                    # Update existing entry
                    await db.execute(
                        'UPDATE music_access SET is_whitelisted = TRUE WHERE user_id = $1',
                        str(user.id)
                    )
                else:
                    # Insert new entry
                    await db.execute(
                        'INSERT INTO music_access (user_id, is_whitelisted) VALUES ($1, TRUE)',
                        str(user.id)
                    )

                await interaction.response.send_message(f"<:Accepted:1426930333789585509> {user.mention} has been whitelisted for music commands!",
                                                        ephemeral=True)

            elif action == "blacklist":
                # Remove or set to false
                result = await db.fetchrow(
                    'SELECT * FROM music_access WHERE user_id = $1',
                    str(user.id)
                )

                if result:
                    await db.execute(
                        'UPDATE music_access SET is_whitelisted = FALSE WHERE user_id = $1',
                        str(user.id)
                    )
                    await interaction.response.send_message(
                        f"<:Accepted:1426930333789585509> {user.mention} has been blacklisted from music commands!", ephemeral=True)
                else:
                    await interaction.response.send_message(f"ℹ️ {user.mention} was not on the whitelist.",
                                                            ephemeral=True)

        except Exception as e:
            await interaction.response.send_message(f"<:Denied:1426930694633816248> Error managing access: {str(e)}", ephemeral=True)


async def setup(bot):
    await bot.add_cog(MusicCog(bot))