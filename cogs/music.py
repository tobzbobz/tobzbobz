import discord
from discord.ext import commands
from discord import app_commands
import wavelink
from typing import Optional
import asyncio
from database import db
import os
import json
import traceback
import aiohttp

OWNER_ID = 678475709257089057


class MusicCog(commands.Cog):
    """Music player cog with custom Lavalink node"""

    def __init__(self, bot):
        self.bot = bot
        self.node_connected = False

    async def is_whitelisted(self, user_id: int) -> bool:
        """Check if a user is whitelisted for music commands"""
        if user_id == OWNER_ID:
            return True

        try:
            if not hasattr(db, 'pool') or db.pool is None:
                print("Warning: Database pool not initialized")
                return False

            async with db.pool.acquire() as conn:
                result = await conn.fetchrow(
                    'SELECT * FROM music_access WHERE user_id = $1 AND is_whitelisted = TRUE',
                    str(user_id)
                )
            return result is not None
        except Exception as e:
            print(f"Error checking whitelist: {e}")
            return False

    def get_lavalink_nodes(self):
        """Get Lavalink node configuration from environment or file"""

        # Try environment variables first (for Render deployment)
        lavalink_host = os.getenv('LAVALINK_HOST')
        lavalink_password = os.getenv('LAVALINK_PASSWORD')
        lavalink_port = os.getenv('LAVALINK_PORT', '2333')
        lavalink_secure = os.getenv('LAVALINK_SECURE', 'false').lower() == 'true'

        if lavalink_host and lavalink_password:
            print(f"✅ Using custom Lavalink node from environment: {lavalink_host}")
            return [
                {
                    "identifier": "Custom-Node",
                    "host": lavalink_host,
                    "port": int(lavalink_port),
                    "password": lavalink_password,
                    "secure": lavalink_secure
                }
            ]

        # Try loading from JSON file
        if os.path.exists('lavalink_nodes.json'):
            try:
                with open('lavalink_nodes.json', 'r') as f:
                    custom_nodes = json.load(f)
                    print(f"✅ Loaded {len(custom_nodes)} node(s) from lavalink_nodes.json")
                    return custom_nodes
            except Exception as e:
                print(f"⚠️ Error loading lavalink_nodes.json: {e}")

        # Try environment variable JSON array
        nodes_json = os.getenv('LAVALINK_NODES')
        if nodes_json:
            try:
                custom_nodes = json.loads(nodes_json)
                print(f"✅ Loaded {len(custom_nodes)} node(s) from LAVALINK_NODES env")
                return custom_nodes
            except json.JSONDecodeError as e:
                print(f"⚠️ Invalid LAVALINK_NODES JSON: {e}")

        # Fallback to public nodes
        print("📡 Using fallback public Lavalink nodes")
        return [
            {
                "identifier": "Serenetia-LDP-NonSSL",
                "host": "lavalink.serenetia.com",
                "port": 80,
                "password": "public",
                "secure": False
            },
            {
                "identifier": "AjieDev-LDP-NonSSL",
                "host": "lava-all.ajieblogs.eu.org",
                "port": 80,
                "password": "public",
                "secure": False
            }
        ]

    async def cog_load(self):
        """Setup Wavelink node when cog loads"""
        print("🎵 MusicCog loading...")
        self._connection_task = self.bot.loop.create_task(self._connect_nodes())

    async def _connect_nodes(self):
        """Background task to connect to Wavelink nodes"""
        try:
            await self.bot.wait_until_ready()
            await asyncio.sleep(2)

            if wavelink.Pool.nodes:
                print("✅ Wavelink nodes already connected")
                self.node_connected = True
                return

            public_nodes = self.get_lavalink_nodes()
            nodes_to_connect = []

            for node_info in public_nodes:
                try:
                    protocol = "https" if node_info.get("secure", False) else "http"
                    uri = f"{protocol}://{node_info['host']}:{node_info['port']}"

                    print(f"🔗 Preparing to connect to {node_info['identifier']} ({uri})...")
                    print(f"🔑 Using password: {node_info['password'][:3]}***")  # Debug log

                    node = wavelink.Node(
                        uri=uri,
                        password=node_info['password'],
                        identifier=node_info['identifier'],
                        heartbeat=30.0,  # Add heartbeat
                        retries=5  # Add retry logic
                    )

                    nodes_to_connect.append(node)

                except Exception as e:
                    print(f"❌ Failed to create node {node_info['identifier']}: {e}")
                    traceback.print_exc()
                    continue

            if nodes_to_connect:
                try:
                    await wavelink.Pool.connect(client=self.bot, nodes=nodes_to_connect)
                    print(f"✅ Connected to {len(nodes_to_connect)} Lavalink node(s)")
                    self.node_connected = True
                except Exception as e:
                    print(f"❌ Failed to connect to Lavalink nodes: {e}")
                    traceback.print_exc()
            else:
                print("❌ No nodes available to connect")

            if not self.node_connected:
                print("⚠️ Warning: Failed to connect to any Lavalink nodes!")
            else:
                print("✅ MusicCog ready!")

        except Exception as e:
            print(f"❌ Critical error in node connection: {e}")
            traceback.print_exc()

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        try:
            print("🛑 MusicCog unloading...")

            # Cancel keep-alive task
            if hasattr(self, '_keep_alive_task'):
                self._keep_alive_task.cancel()

            # Cancel connection task
            if hasattr(self, '_connection_task'):
                self._connection_task.cancel()

            # Disconnect from all voice channels
            for guild in self.bot.guilds:
                if guild.voice_client:
                    try:
                        await guild.voice_client.disconnect(force=True)
                    except:
                        pass
        except Exception as e:
            print(f"Error during cog unload: {e}")


    @commands.Cog.listener()
    async def on_wavelink_node_ready(self, payload: wavelink.NodeReadyEventPayload):
        """Event fired when a Wavelink node is ready"""
        print(f"🎵 Wavelink node {payload.node.identifier} is ready!")
        self.node_connected = True

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

        if player.queue.is_empty and not player.playing:
            await asyncio.sleep(180)
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
            if not wavelink.Pool.nodes:
                await interaction.response.send_message(
                    "<:Denied:1426930694633816248> Music service is currently unavailable. No Lavalink nodes connected.",
                    ephemeral=True
                )
                return None

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

    async def play_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> list[app_commands.Choice[str]]:
        """Autocomplete for play command"""
        if not current or len(current) < 2:
            return []

        try:
            if not wavelink.Pool.nodes:
                return []

            search_query = current.strip()

            if not any(search_query.startswith(prefix) for prefix in
                       ['http://', 'https://', 'ytsearch:', 'ytmsearch:', 'scsearch:', 'spotify:']):
                search_query = f"ytsearch:{search_query}"

            tracks = await asyncio.wait_for(
                wavelink.Playable.search(search_query),
                timeout=5.0
            )

            if not tracks:
                return []

            if isinstance(tracks, wavelink.Playlist):
                return [
                    app_commands.Choice(
                        name=f"📚 Playlist: {tracks.name[:90]}",
                        value=current
                    )
                ]

            choices = []
            for track in tracks[:25]:
                duration = self.format_duration(track.length)
                choice_name = f"{track.author} - {track.title}"
                if len(choice_name) > 85:
                    choice_name = choice_name[:82] + "..."
                choice_name += f" ({duration})"

                choice_value = track.uri if track.uri else current

                choices.append(
                    app_commands.Choice(name=choice_name, value=choice_value)
                )

            return choices

        except asyncio.TimeoutError:
            return []
        except Exception as e:
            print(f"❌ Autocomplete error: {e}")
            return []

    async def _keep_lavalink_alive(self):
        """Background task to keep Lavalink server awake with HTTP pings and Discord notifications"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)  # Wait for initial connection

        lavalink_host = os.getenv('LAVALINK_HOST')
        lavalink_password = os.getenv('LAVALINK_PASSWORD')
        lavalink_port = os.getenv('LAVALINK_PORT', '443')
        lavalink_secure = os.getenv('LAVALINK_SECURE', 'true').lower() == 'true'

        # Channel ID where you want status updates (set this in your .env or here)
        status_channel_id = int(os.getenv('LAVALINK_STATUS_CHANNEL_ID', '0'))

        if not lavalink_host or not lavalink_password:
            print("⚠️ Lavalink credentials not found, keep-alive disabled")
            return

        protocol = "https" if lavalink_secure else "http"
        url = f"{protocol}://{lavalink_host}:{lavalink_port}/version"
        headers = {"Authorization": lavalink_password}

        print(f"💚 Starting keep-alive pings to {url}")

        ping_count = 0
        consecutive_failures = 0

        while not self.bot.is_closed():
            try:
                await asyncio.sleep(300)  # Wait 5 minutes between pings

                ping_count += 1
                status_channel = None

                # Get the status channel
                if status_channel_id:
                    try:
                        status_channel = self.bot.get_channel(status_channel_id)
                        if not status_channel:
                            status_channel = await self.bot.fetch_channel(status_channel_id)
                    except:
                        pass

                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.get(url, headers=headers, timeout=10) as response:
                            if response.status == 200:
                                data = await response.json()
                                version = data.get('version', 'unknown')
                                consecutive_failures = 0

                                print(f"💚 Keep-alive ping #{ping_count} successful - Lavalink v{version}")

                                # Send success message to Discord
                                if status_channel:
                                    embed = discord.Embed(
                                        title="🎵 Lavalink Status",
                                        description=f"✅ Keep-alive ping successful",
                                        color=discord.Color.green(),
                                        timestamp=discord.utils.utcnow()
                                    )
                                    embed.add_field(name="Version", value=version, inline=True)
                                    embed.add_field(name="Ping #", value=str(ping_count), inline=True)
                                    embed.add_field(name="Host", value=lavalink_host, inline=False)

                                    # Only send every 6th ping (every 30 minutes) for success
                                    if ping_count % 6 == 0:
                                        await status_channel.send(embed=embed)
                            else:
                                consecutive_failures += 1
                                print(f"⚠️ Keep-alive ping returned status {response.status}")

                                # Send warning to Discord
                                if status_channel:
                                    embed = discord.Embed(
                                        title="⚠️ Lavalink Warning",
                                        description=f"Ping returned status code {response.status}",
                                        color=discord.Color.orange(),
                                        timestamp=discord.utils.utcnow()
                                    )
                                    embed.add_field(name="Consecutive Failures", value=str(consecutive_failures),
                                                    inline=True)
                                    embed.add_field(name="Ping #", value=str(ping_count), inline=True)
                                    await status_channel.send(embed=embed)

                    except asyncio.TimeoutError:
                        consecutive_failures += 1
                        print(f"⚠️ Keep-alive ping #{ping_count} timed out")

                        # Send timeout warning to Discord
                        if status_channel:
                            embed = discord.Embed(
                                title="⏰ Lavalink Timeout",
                                description="Keep-alive ping timed out after 10 seconds",
                                color=discord.Color.orange(),
                                timestamp=discord.utils.utcnow()
                            )
                            embed.add_field(name="Consecutive Failures", value=str(consecutive_failures), inline=True)
                            embed.add_field(name="Ping #", value=str(ping_count), inline=True)
                            await status_channel.send(embed=embed)

            except Exception as e:
                consecutive_failures += 1
                print(f"❌ Keep-alive ping error: {e}")

                # Send error to Discord
                if status_channel_id:
                    try:
                        status_channel = self.bot.get_channel(status_channel_id) or await self.bot.fetch_channel(
                            status_channel_id)
                        if status_channel:
                            embed = discord.Embed(
                                title="❌ Lavalink Error",
                                description=f"Keep-alive ping failed",
                                color=discord.Color.red(),
                                timestamp=discord.utils.utcnow()
                            )
                            embed.add_field(name="Error", value=str(e)[:1024], inline=False)
                            embed.add_field(name="Consecutive Failures", value=str(consecutive_failures), inline=True)
                            embed.add_field(name="Ping #", value=str(ping_count), inline=True)
                            await status_channel.send(embed=embed)
                    except:
                        pass

                await asyncio.sleep(60)  # Wait 1 minute before retrying on error


    @m_group.command(name="play", description="Play a song or add it to queue")
    @app_commands.describe(query="Song name, YouTube/Spotify/SoundCloud URL")
    @app_commands.autocomplete(query=play_autocomplete)
    async def play(self, interaction: discord.Interaction, query: str):
        """Play a song"""
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        player = await self.ensure_voice(interaction)
        if not player:
            return

        try:
            search_query = query.strip()

            # Spotify URLs are handled directly by lavasrc plugin
            if not self.is_spotify_url(search_query):
                # Only add search prefix if it's not already a URL
                if not any(search_query.startswith(prefix) for prefix in
                           ['http://', 'https://', 'ytsearch:', 'ytmsearch:', 'scsearch:']):
                    search_query = f"ytsearch:{search_query}"

            tracks: wavelink.Search = await wavelink.Playable.search(search_query)

            if not tracks:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> No tracks found! Try a different search term or URL."
                )
                return

            if isinstance(tracks, wavelink.Playlist):
                added: int = await player.queue.put_wait(tracks)
                embed = discord.Embed(
                    title="🎵 Playlist Added to Queue" if not self.is_spotify_url(query) else "🎵 Spotify Playlist Added",
                    description=f"Added **{added}** tracks from **{tracks.name}**",
                    color=discord.Color.green() if self.is_spotify_url(query) else discord.Color.blue()
                )
                await interaction.followup.send(embed=embed)
            else:
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

                # Add source indicator
                if self.is_spotify_url(query):
                    embed.set_footer(text="🟢 Source: Spotify")

                await interaction.followup.send(embed=embed)

            if not player.playing:
                await player.play(player.queue.get())

        except wavelink.LavalinkException as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Lavalink error: {str(e)}",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error: {str(e)}",
                ephemeral=True
            )

    def is_spotify_url(self, url: str) -> bool:
        """Check if URL is a Spotify URL"""
        return 'spotify.com' in url or url.startswith('spotify:')

    @m_group.command(name="pause", description="Pause or resume playback")
    async def pause(self, interaction: discord.Interaction):
        """Toggle pause/resume playback"""
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!",
                                                    ephemeral=True)
            return

        if not player.current:
            await interaction.response.send_message("<:Denied:1426930694633816248> Nothing is playing!", ephemeral=True)
            return

        await player.pause(not player.paused)

        if player.paused:
            await interaction.response.send_message("⏸️ Paused playback")
        else:
            await interaction.response.send_message("▶️ Resumed playback")

    @m_group.command(name="skip", description="Skip the current song")
    async def skip(self, interaction: discord.Interaction):
        """Skip current track"""
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
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
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!",
                                                    ephemeral=True)
            return

        player.queue.clear()
        await player.stop()
        await interaction.response.send_message("⏹️ Stopped playback and cleared queue")

    @m_group.command(name="leave", description="Disconnect the bot from voice")
    async def disconnect(self, interaction: discord.Interaction):
        """Disconnect from voice channel"""
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!",
                                                    ephemeral=True)
            return

        await player.disconnect()
        await interaction.response.send_message("👋 Disconnected from voice channel")

    @m_group.command(name="queue", description="View the current queue")
    async def queue(self, interaction: discord.Interaction):
        """Display the current queue"""
        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!",
                                                    ephemeral=True)
            return

        if player.queue.is_empty and not player.current:
            await interaction.response.send_message("<:Denied:1426930694633816248> Queue is empty!", ephemeral=True)
            return

        embed = discord.Embed(
            title="📋 Current Queue",
            color=discord.Color.blue()
        )

        if player.current:
            embed.add_field(
                name="🎵 Now Playing",
                value=f"**[{player.current.title}]({player.current.uri})**\n"
                      f"By {player.current.author} | {self.format_duration(player.current.length)}",
                inline=False
            )

        if not player.queue.is_empty:
            queue_list = []
            for i, track in enumerate(player.queue[:10], start=1):
                queue_list.append(f"`{i}.` **{track.title}** | {self.format_duration(track.length)}")

            if len(player.queue) > 10:
                queue_list.append(f"*...and {len(player.queue) - 10} more tracks*")

            embed.add_field(
                name="📜 Up Next",
                value="\n".join(queue_list),
                inline=False
            )

        embed.set_footer(text=f"Total tracks in queue: {len(player.queue)}")

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

        progress = int((position / track.length) * 20)
        bar = "▬" * progress + "🔘" + "▬" * (20 - progress)
        embed.add_field(name="⏳", value=bar, inline=False)

        await interaction.response.send_message(embed=embed)

    @m_group.command(name="volume", description="Set player volume (Owner only)")
    @app_commands.describe(volume="Volume level (0-100)")
    async def volume(self, interaction: discord.Interaction, volume: int):
        """Set player volume"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Only the bot owner can change volume!",
                ephemeral=True
            )
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!",
                                                    ephemeral=True)
            return

        if not 0 <= volume <= 100:
            await interaction.response.send_message("<:Denied:1426930694633816248> Volume must be between 0 and 100!",
                                                    ephemeral=True)
            return

        await player.set_volume(volume)
        await interaction.response.send_message(f"🔊 Set volume to **{volume}%**")

    @m_group.command(name="shuffle", description="Shuffle the queue")
    async def shuffle(self, interaction: discord.Interaction):
        """Shuffle the queue"""
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
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
        if not await self.is_whitelisted(interaction.user.id):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to use music commands!",
                ephemeral=True
            )
            return

        player: wavelink.Player = interaction.guild.voice_client

        if not player:
            await interaction.response.send_message("<:Denied:1426930694633816248> Not connected to voice!",
                                                    ephemeral=True)
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
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Only the bot owner can manage the access list!",
                ephemeral=True
            )
            return

        if action == "view":
            try:
                async with db.pool.acquire() as conn:
                    result = await conn.fetch('SELECT * FROM music_access')

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
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> Error fetching access list: {str(e)}",
                    ephemeral=True
                )
            return

        if not user:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You must specify a user for whitelist/blacklist actions!",
                ephemeral=True
            )
            return

        if user.id == OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Cannot modify access for the bot owner!",
                ephemeral=True
            )
            return

        try:
            async with db.pool.acquire() as conn:
                if action == "whitelist":
                    result = await conn.fetchrow(
                        'SELECT * FROM music_access WHERE user_id = $1',
                        str(user.id)
                    )

                    if result:
                        await conn.execute(
                            'UPDATE music_access SET is_whitelisted = TRUE WHERE user_id = $1',
                            str(user.id)
                        )
                    else:
                        await conn.execute(
                            'INSERT INTO music_access (user_id, is_whitelisted) VALUES ($1, TRUE)',
                            str(user.id)
                        )

                    await interaction.response.send_message(
                        f"<:Accepted:1426930333789585509> {user.mention} has been whitelisted for music commands!",
                        ephemeral=True
                    )

                elif action == "blacklist":
                    result = await conn.fetchrow(
                        'SELECT * FROM music_access WHERE user_id = $1',
                        str(user.id)
                    )

                    if result:
                        await conn.execute(
                            'UPDATE music_access SET is_whitelisted = FALSE WHERE user_id = $1',
                            str(user.id)
                        )
                        await interaction.response.send_message(
                            f"<:Accepted:1426930333789585509> {user.mention} has been blacklisted from music commands!",
                            ephemeral=True
                        )
                    else:
                        await interaction.response.send_message(
                            f"ℹ️ {user.mention} was not on the whitelist.",
                            ephemeral=True
                        )

        except Exception as e:
            await interaction.response.send_message(
                f"<:Denied:1426930694633816248> Error managing access: {str(e)}",
                ephemeral=True
            )


async def setup(bot):
    try:
        await bot.add_cog(MusicCog(bot))
        print("✅ MusicCog setup completed successfully")
    except Exception as e:
        print(f"❌ Failed to setup MusicCog: {e}")
        traceback.print_exc()
        raise