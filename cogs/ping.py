import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
from collections import deque
import json
from database import db

# Your Discord User ID
YOUR_USER_ID = 678475709257089057


class PingLoggerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.message_cache = {}  # Cache messages for delete detection

    @commands.Cog.listener()
    async def on_message(self, message):
        """Log whenever you get pinged"""

        # Ignore bot messages
        if message.author.bot:
            return

        # Check if you were mentioned
        if self.bot.user.id == YOUR_USER_ID and self.bot.user in message.mentions:
            # Cache the message for delete detection
            self.message_cache[message.id] = {
                'message': message,
                'logged': True
            }
            await self.log_ping(message)

    @commands.Cog.listener()
    async def on_message_delete(self, message):
        """Detect when a message that pinged you is deleted"""

        # Check if this message pinged you
        if message.id in self.message_cache:
            try:
                async with db.pool.acquire() as conn:
                    # Update the log to mark as deleted
                    await conn.execute(
                        '''UPDATE audit_logs
                           SET details = jsonb_set(
                                   jsonb_set(details, '{deleted}', 'true'::jsonb),
                                   '{deleted_at}', to_jsonb($1::text)
                                         )
                           WHERE details ->>'message_id' = $2
                             AND action = 'ping_received' ''',
                        datetime.utcnow().isoformat(),
                        str(message.id)
                    )

                print(f"[PING DELETED] {message.author.name} deleted a ping (ID: {message.id})")
            except Exception as e:
                print(f"Error updating deleted ping: {e}")
            finally:
                del self.message_cache[message.id]

    async def log_ping(self, message):
        """Log ping to database"""

        try:
            # Create ping record
            ping_data = {
                'message_id': str(message.id),
                'timestamp': datetime.utcnow().isoformat(),
                'server': message.guild.name if message.guild else "DM",
                'server_id': message.guild.id if message.guild else None,
                'channel': message.channel.name if hasattr(message.channel, 'name') else "Direct Message",
                'channel_id': message.channel.id,
                'channel_mention': message.channel.mention if message.guild else "DM",
                'author': str(message.author),
                'author_id': message.author.id,
                'author_mention': message.author.mention,
                'content': message.content[:500],  # Truncate long messages
                'jump_url': message.jump_url,
                'deleted': False,
                'deleted_at': None
            }

            # Save to database
            await db.log_action(
                guild_id=message.guild.id if message.guild else 0,
                user_id=message.author.id,
                action='ping_received',
                details=ping_data
            )

            print(f"[PING LOG] {ping_data['author']} pinged you in {ping_data['server']}")

        except Exception as e:
            print(f"Error logging ping: {e}")
            import traceback
            traceback.print_exc()

    # Prefix command version - only works in DMs
    @commands.command(name='pings')
    async def pings_prefix(self, ctx):
        """Show the 10 most recent pings (prefix command - DM only)"""

        # Check if the user is you
        if ctx.author.id != YOUR_USER_ID:
            return  # Silently ignore

        # Check if in DMs
        if ctx.guild is not None:
            return  # Silently ignore if not in DMs

        try:
            # Get from database
            logs = await db.get_recent_logs(
                guild_id=None,  # All guilds
                action_type='ping_received',
                limit=10
            )

            if not logs:
                await ctx.send("<:Denied:1426930694633816248> No pings logged yet!")
                return

            # Create embed
            embed = discord.Embed(
            title="Your Recent Pings",
            description = f"Showing {len(logs)} most recent pings",
            color = discord.Color.gold(),
            timestamp = datetime.utcnow()
            )

            for i, log in enumerate(logs, 1):
                details = log.get('details', {})

                # Get values from details
                message_id = details.get('message_id', 'Unknown')
                server = details.get('server', 'Unknown')
                channel_mention = details.get('channel_mention', 'Unknown')
                author_mention = details.get('author_mention', 'Unknown')
                author = details.get('author', 'Unknown')
                content = details.get('content', '')
                jump_url = details.get('jump_url', '')
                deleted = details.get('deleted', False)
                deleted_at = details.get('deleted_at')

                # Format timestamp
                timestamp = log.get('timestamp')
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp)
                time_str = timestamp.strftime('%H:%M:%S %d/%m/%Y')

                # Truncate content for display
                content_preview = content[:100]
                if len(content) > 100:
                    content_preview += "..."

                # Check if deleted
                status_emoji = "<:Accepted:1426930333789585509>" if deleted else "<:Denied:1426930694633816248>"
                deleted_info = ""
                if deleted:
                    if deleted_at:
                        if isinstance(deleted_at, str):
                            deleted_dt = datetime.fromisoformat(deleted_at)
                        else:
                            deleted_dt = deleted_at
                        deleted_time = deleted_dt.strftime('%H:%M:%S %d/%m/%Y')
                    else:
                        deleted_time = "Unknown"
                    deleted_info = f"\nâš ï¸ **DELETED** at {deleted_time}"

                field_value = (
                    f"{status_emoji} **Server:** {server}\n"
                    f"**Channel:** {channel_mention}\n"
                    f"**From:** {author_mention} ({author})\n"
                    f"**When:** {time_str}{deleted_info}\n"
                    f"**Message:** {content_preview}\n"
                )

                # Only add jump link if not deleted
                if not deleted and jump_url:
                    field_value += f"[Jump to Message]({jump_url})"

                embed.add_field(
                    name=f"#{i}",
                    value=field_value,
                    inline=False
                )

            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"<:Denied:1426930694633816248> Error retrieving pings: {str(e)}")
            print(f"Error in pings command: {e}")
            import traceback
            traceback.print_exc()


async def setup(bot):
    await bot.add_cog(PingLoggerCog(bot))