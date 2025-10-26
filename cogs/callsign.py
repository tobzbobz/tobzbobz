import discord
from discord.ext import commands
from discord import app_commands
from collections import deque
import json
from database import load_ping_history as load_ping_history_db, save_ping_history as save_ping_history_db
from datetime import datetime
import os

# Your Discord User ID
YOUR_USER_ID = 678475709257089057

# JSON file for storing ping history
PING_LOG_FILE = "ping_history.json"


def load_ping_history():
    """Load ping history from JSON file"""
    data = load_ping_history_db()
    # Convert timestamp strings back to datetime objects
    for ping in data:
        ping['timestamp'] = datetime.fromisoformat(ping['timestamp'])
        if ping.get('deleted_at'):
            ping['deleted_at'] = datetime.fromisoformat(ping['deleted_at'])
    from collections import deque
    return deque(data, maxlen=100)

def save_ping_history(ping_history):
    """Save ping history to JSON file"""
    data = []
    for ping in ping_history:
        ping_copy = ping.copy()
        ping_copy['timestamp'] = ping['timestamp'].isoformat()
        if ping_copy.get('deleted_at'):
            ping_copy['deleted_at'] = ping_copy['deleted_at'].isoformat()
        data.append(ping_copy)
    save_ping_history_db(data)

class PingLoggerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ping_history = load_ping_history()  # Load from file on startup
        self.message_cache = {}  # Cache messages for delete detection

        # Print how many pings were loaded
        print(f"[PING LOGGER] Loaded {len(self.ping_history)} pings from history")

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

            # If we already logged it, mark as deleted
            for ping in self.ping_history:
                if ping.get('message_id') == message.id:
                    ping['deleted'] = True
                    ping['deleted_at'] = datetime.utcnow()
                    break

            # Save updated history
            save_ping_history(self.ping_history)

            # Log to console
            print(
                f"[PING DELETED] {message.author.name} deleted a ping in {message.guild.name if message.guild else 'DM'}")

            # Remove from cache
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

        # Add to history
        self.ping_history.append(ping_data)

        # Save to file
        save_ping_history(self.ping_history)

        # Mark as logged in cache
        if message.id in self.message_cache:
            self.message_cache[message.id]['logged'] = True

        # Log to console only
        print(f"[PING LOG] {ping_data['author']} pinged you in {ping_data['server']} - #{ping_data['channel']}")

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

        if len(self.ping_history) == 0:
            await ctx.send("📭 No pings logged yet!")
            return

        # Get last 10 pings (most recent first)
        recent_pings = list(self.ping_history)[-10:]
        recent_pings.reverse()

        # Create embed
        embed = discord.Embed(
            title="📌 Your Recent Pings",
            description=f"Showing {len(recent_pings)} most recent pings",
            color=discord.Color.gold(),
            timestamp=datetime.utcnow()
        )

        for i, ping in enumerate(recent_pings, 1):
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

        embed.set_footer(text=f"Total pings logged: {len(self.ping_history)}")
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(PingLoggerCog(bot))