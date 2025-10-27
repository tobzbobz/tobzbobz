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
        self.ping_history = deque(maxlen=100)  # Store last 100 pings
        self.message_cache = {}  # Cache messages for delete detection

    @commands.Cog.listener()
    async def on_message(self, message):
        """Log whenever you get pinged"""

        # Ignore bot messages
        if message.author.bot:
            return

        # Check if you were mentioned
        if self.bot.get_user(YOUR_USER_ID) in message.mentions:
            # Cache the message for delete detection
            self.message_cache[message.id] = {
                'message': message,
                'logged': False
            }
            await self.log_ping(message, deleted=False)

    @commands.Cog.listener()
    async def on_message_delete(self, message):
        """Detect when a message that pinged you is deleted"""

        # Check if this message pinged you
        if message.id in self.message_cache:
            cached = self.message_cache[message.id]

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE audit_logs
                       SET details = details || '{"deleted": true, "deleted_at": $1}'::jsonb
                       WHERE details->>'message_id' = $2''',
                    datetime.utcnow().isoformat(),
                    str(message.id)
                )

            print(f"[PING DELETED] {message.author.name} deleted a ping")
            del self.message_cache[message.id]

    async def log_ping(self, message, deleted=False):
        """Log ping to history silently"""

        # Create ping record
        ping_data = {
            'message_id': message.id,
            'timestamp': datetime.utcnow(),
            'server': message.guild.name if message.guild else "DM",
            'server_id': message.guild.id if message.guild else None,
            'channel': message.channel.name if message.guild else "Direct Message",
            'channel_id': message.channel.id,
            'channel_mention': message.channel.mention if message.guild else "DM",
            'author': message.author.name,
            'author_id': message.author.id,
            'author_mention': message.author.mention,
            'content': message.content[:500],  # Truncate long messages
            'jump_url': message.jump_url,
            'deleted': deleted,
            'deleted_at': None
        }

        # UPDATED: Save to database
        await db.log_action(
            guild_id=message.guild.id if message.guild else 0,
            user_id=message.author.id,
            action='ping_received',
            details=ping_data
        )

        if message.id in self.message_cache:
            self.message_cache[message.id]['logged'] = True

        print(f"[PING LOG] {ping_data['author']} pinged you in {ping_data['server']}")

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

        # UPDATED: Get from database
        logs = await db.get_recent_logs(
            guild_id=0,  # All guilds
            action_type='ping_received',
            limit=10
        )

        if not logs:
            await ctx.send("📭 No pings logged yet!")
            return

        # Create embed
        embed = discord.Embed(
            title="📌 Your Recent Pings",
            description=f"Showing {len(recent_pings)} most recent pings",
            color=discord.Color.gold(),
            timestamp=datetime.utcnow()
        )

        for i, ping in enumerate(logs, 1):
            details = log.get('details', {})
            # Format timestamp
            time_str = ping['timestamp'].strftime('%H:%M:%S %d/%m/%Y')

            # Truncate content for display
            content_preview = ping['content'][:100]
            if len(ping['content']) > 100:
                content_preview += "..."

            # Check if deleted
            status_emoji = "🗑️" if ping['deleted'] else "✅"
            deleted_info = ""
            if ping['deleted']:
                deleted_time = ping['deleted_at'].strftime('%H:%M:%S %d/%m/%Y') if ping['deleted_at'] else "Unknown"
                deleted_info = f"\n⚠️ **DELETED** at {deleted_time}"

            field_value = (
                f"{status_emoji} **Server:** {ping['server']}\n"
                f"**Channel:** {ping['channel_mention']}\n"
                f"**From:** {ping['author_mention']} ({ping['author']})\n"
                f"**When:** {time_str}{deleted_info}\n"
                f"**Message:** {content_preview}\n"
            )

            # Only add jump link if not deleted
            if not ping['deleted']:
                field_value += f"[Jump to Message]({ping['jump_url']})"

            embed.add_field(
                name=f"#{i}",
                value=field_value,
                inline=False
            )

        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(PingLoggerCog(bot))