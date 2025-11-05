import discord
from discord.ext import commands
from discord import app_commands
import asyncio

# ============================
# CONFIGURATION
# ============================

# Replace with your Discord User ID
AUTHORIZED_USER_ID = 678475709257089057  # <-- PUT YOUR USER ID HERE

# Log channels to monitor for auto-publishing
LOG_CHANNELS = {
    1434770430505390221,  # SYNC_LOG_CHANNEL_ID
    1435318020619632851,
    1435489971342409809,
    1435499104221532231   # CALLSIGN_REQUEST_LOG_CHANNEL_ID
}

# Error indicators to check for (messages containing these will NOT be published)
ERROR_INDICATORS = [
    "⚠️",  # Warning emoji
    "error",
    "failed",
    "Error",
    "Failed",
    "ERROR",
    "FAILED",
    "exception",
    "Exception",
    "Auto-Sync Failed",
    "Sync Failed",
]

# Success indicators (optional - for extra confidence)
SUCCESS_INDICATORS = [
    "✅",
    "<:Accepted:",
    "Auto-Sync Completed",
    "Sync Complete",
    "Approved",
]


# ============================
# CUSTOM CHECK FOR UID LOCK
# ============================

def is_authorized_user():
    """Custom check to ensure only the authorized user can run commands"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.id != AUTHORIZED_USER_ID:
            await interaction.response.send_message(
                "❌ You are not authorized to use this command.",
                ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)


class AutoPublishCog(commands.Cog):
    """Automatically publishes non-error messages from log channels"""

    def __init__(self, bot):
        self.bot = bot
        self.publish_queue = asyncio.Queue()
        self.processing_task = None

    async def cog_load(self):
        """Start the background task when cog loads"""
        self.processing_task = asyncio.create_task(self.process_publish_queue())
        print("✅ Auto-Publish: Started background processing task")

    def cog_unload(self):
        """Stop the background task when cog unloads"""
        if self.processing_task:
            self.processing_task.cancel()
        print("🛑 Auto-Publish: Stopped background processing task")

    async def process_publish_queue(self):
        """Background task to process the publish queue with rate limiting"""
        while True:
            try:
                message = await self.publish_queue.get()

                try:
                    await message.publish()
                    print(f"📢 Auto-Published message in #{message.channel.name} (ID: {message.id})")
                except discord.HTTPException as e:
                    if e.code == 50033:  # Invalid Form Body (already published)
                        print(f"ℹ️ Message {message.id} already published")
                    else:
                        print(f"❌ Failed to publish message {message.id}: {e}")
                except Exception as e:
                    print(f"❌ Unexpected error publishing message {message.id}: {e}")

                # Rate limit: wait 1 second between publishes to avoid hitting Discord limits
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Error in publish queue processor: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    def is_error_message(self, message: discord.Message) -> bool:
        """
        Check if a message is an error message.
        Returns True if it's a RED error (should NOT be published).
        Returns False if it's not a red error (should be published).
        """
        # Check embeds for RED color - this is the key indicator
        if message.embeds:
            for embed in message.embeds:
                # Check embed color - RED = error (do not publish)
                if embed.color and embed.color == discord.Color.red():
                    return True

                # Also check for "Failed" or "Error" in title of RED embeds
                if embed.title:
                    title_lower = embed.title.lower()
                    if ("failed" in title_lower or "error" in title_lower) and embed.color == discord.Color.red():
                        return True

        # If we get here, it's not a red error message (blue messages with errors are fine)
        return False

    def should_publish(self, message: discord.Message) -> bool:
        """
        Determine if a message should be auto-published.
        """
        # Must be in an announcement channel
        if not isinstance(message.channel, discord.TextChannel):
            return False

        if not message.channel.is_news():
            return False

        # Must be in one of our log channels
        if message.channel.id not in LOG_CHANNELS:
            return False

        # Must be from the bot itself
        if message.author.id != self.bot.user.id:
            return False

        # Must not be an error message
        if self.is_error_message(message):
            return False

        return True

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for new messages in log channels"""
        if self.should_publish(message):
            await self.publish_queue.put(message)
            print(f"📝 Queued message for publishing in #{message.channel.name}")

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """
        Handle message edits - useful if a message is edited after being sent.
        This won't re-publish, but will attempt to publish if it wasn't published before.
        """
        # Only process if the message wasn't already published
        if not before.flags.crossposted and self.should_publish(after):
            await self.publish_queue.put(after)
            print(f"📝 Queued edited message for publishing in #{after.channel.name}")

    # Admin commands for managing auto-publish

    publish_group = app_commands.Group(name="autopublish", description="Manage auto-publish settings")

    @publish_group.command(name="status", description="Check auto-publish status")
    @is_authorized_user()
    async def autopublish_status(self, interaction: discord.Interaction):
        """Show current auto-publish status"""
        embed = discord.Embed(
            title="📢 Auto-Publish Status",
            color=discord.Color.blue()
        )

        # Check which channels are monitored and if they're announcement channels
        channel_status = []
        for channel_id in LOG_CHANNELS:
            channel = self.bot.get_channel(channel_id)
            if channel:
                is_news = isinstance(channel, discord.TextChannel) and channel.is_news()
                status_emoji = "✅" if is_news else "❌"
                channel_status.append(
                    f"{status_emoji} {channel.mention} - {'Announcement' if is_news else 'Not Announcement'}")
            else:
                channel_status.append(f"❌ Channel ID {channel_id} - Not Found")

        embed.add_field(
            name="Monitored Channels",
            value="\n".join(channel_status) if channel_status else "None",
            inline=False
        )

        embed.add_field(
            name="Processing Task",
            value="✅ Running" if self.processing_task and not self.processing_task.done() else "❌ Not Running",
            inline=True
        )

        embed.add_field(
            name="Queue Size",
            value=str(self.publish_queue.qsize()),
            inline=True
        )

        embed.add_field(
            name="Error Indicators",
            value=f"{len(ERROR_INDICATORS)} patterns",
            inline=True
        )

        embed.set_footer(text="Messages containing error indicators will NOT be published")

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @publish_group.command(name="test", description="Test if a message would be auto-published")
    @is_authorized_user()
    @app_commands.describe(message_id="The message ID to test", channel="The channel containing the message")
    async def autopublish_test(self, interaction: discord.Interaction, message_id: str,
                               channel: discord.TextChannel = None):
        """Test if a specific message would be auto-published"""
        await interaction.response.defer(ephemeral=True)

        try:
            # Use provided channel or interaction channel
            target_channel = channel or interaction.channel

            # Fetch the message
            message = await target_channel.fetch_message(int(message_id))

            # Check if it would be published
            would_publish = self.should_publish(message)
            is_error = self.is_error_message(message)

            embed = discord.Embed(
                title="🧪 Auto-Publish Test Result",
                color=discord.Color.green() if would_publish else discord.Color.orange()
            )

            embed.add_field(name="Message ID", value=message_id, inline=True)
            embed.add_field(name="Channel", value=target_channel.mention, inline=True)
            embed.add_field(name="Author", value=message.author.mention, inline=True)

            # Detailed checks
            checks = []
            checks.append(
                f"{'✅' if isinstance(target_channel, discord.TextChannel) and target_channel.is_news() else '❌'} Is announcement channel")
            checks.append(f"{'✅' if target_channel.id in LOG_CHANNELS else '❌'} Is monitored log channel")
            checks.append(f"{'✅' if message.author.id == self.bot.user.id else '❌'} Is from bot")
            checks.append(f"{'✅' if not is_error else '❌'} Is not an error message")
            checks.append(f"{'✅' if not message.flags.crossposted else '⚠️'} Not already published")

            embed.add_field(
                name="Checks",
                value="\n".join(checks),
                inline=False
            )

            embed.add_field(
                name="Result",
                value=f"**{'✅ WOULD BE PUBLISHED' if would_publish else '❌ WOULD NOT BE PUBLISHED'}**",
                inline=False
            )

            if is_error:
                embed.add_field(
                    name="⚠️ Error Detected",
                    value="This message contains error indicators and will not be auto-published.",
                    inline=False
                )

            await interaction.followup.send(embed=embed, ephemeral=True)

        except discord.NotFound:
            await interaction.followup.send(
                "❌ Message not found. Make sure the message ID is correct and exists in the specified channel.",
                ephemeral=True
            )
        except ValueError:
            await interaction.followup.send(
                "❌ Invalid message ID. Please provide a valid numeric message ID.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error testing message: {str(e)}",
                ephemeral=True
            )

    @publish_group.command(name="manual", description="Manually publish a message")
    @is_authorized_user()
    @app_commands.describe(message_id="The message ID to publish", channel="The channel containing the message")
    async def autopublish_manual(self, interaction: discord.Interaction, message_id: str,
                                 channel: discord.TextChannel = None):
        """Manually publish a specific message"""
        await interaction.response.defer(ephemeral=True)

        try:
            target_channel = channel or interaction.channel
            message = await target_channel.fetch_message(int(message_id))

            # Check if it's an announcement channel
            if not isinstance(target_channel, discord.TextChannel) or not target_channel.is_news():
                await interaction.followup.send(
                    "❌ This channel is not an announcement channel. Messages can only be published in announcement channels.",
                    ephemeral=True
                )
                return

            # Publish the message
            await message.publish()

            await interaction.followup.send(
                f"✅ Successfully published message {message_id} in {target_channel.mention}",
                ephemeral=True
            )

        except discord.HTTPException as e:
            if e.code == 50033:
                await interaction.followup.send(
                    "⚠️ This message is already published!",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"❌ Failed to publish message: {e}",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )


async def setup(bot):
    await bot.add_cog(AutoPublishCog(bot))