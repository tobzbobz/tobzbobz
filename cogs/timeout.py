import discord
from discord.ext import commands
import logging
import asyncio
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


class TimeoutMonitor(commands.Cog):
    """Monitor timeouts and notify staff when no reason is provided."""

    def __init__(self, bot):
        self.bot = bot

        # Configuration
        self.NOTIFICATION_CHANNEL_ID = 1429625530277171271
        self.PING_ROLE_IDS = [1285474077556998196, 1389113393511923863]
        self.TIMEOUT_THRESHOLD = timedelta(minutes=4, seconds=50)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Listen for member updates to detect timeouts."""

        # Check if a timeout was applied
        if before.timed_out_until is None and after.timed_out_until is not None:
            # Calculate timeout duration
            timeout_duration = after.timed_out_until - datetime.now(timezone.utc)

            # Check if timeout exceeds threshold (4 minutes 50 seconds)
            if timeout_duration >= self.TIMEOUT_THRESHOLD:
                # Fetch audit log to check for reason
                try:
                    async for entry in after.guild.audit_logs(
                            limit=5,
                            action=discord.AuditLogAction.member_update,
                            after=datetime.now(timezone.utc) - timedelta(seconds=5)
                    ):
                        # Check if this entry is for the timed out member
                        if entry.target.id == after.id:
                            # Check if timeout was applied and has no reason
                            if entry.changes.before.timed_out_until is None and entry.changes.after.timed_out_until is not None:
                                moderator = entry.user

                                await self.send_notification(
                                    guild=after.guild,
                                    member=after,
                                    moderator=moderator,
                                    timeout_until=after.timed_out_until,
                                    duration=timeout_duration
                                )
                                break

                except discord.Forbidden:
                    logger.error("Missing permissions to view audit log")
                except Exception as e:
                    logger.error(f"Error checking timeout reason: {e}")

    async def send_notification(
            self,
            guild: discord.Guild,
            member: discord.Member,
            moderator: discord.Member,
            timeout_until: datetime,
            duration: timedelta
    ):
        """Send notification to the designated channel after a 10 second delay."""
        try:
            # Wait 10 seconds before sending to avoid conflicts with other logging bots
            await asyncio.sleep(10)

            channel = self.bot.get_channel(self.NOTIFICATION_CHANNEL_ID)
            if not channel:
                logger.error(f"Notification channel {self.NOTIFICATION_CHANNEL_ID} not found")
                return

            # Create role mentions
            role_mentions = " ".join([f"<@&{role_id}>" for role_id in self.PING_ROLE_IDS])

            # Format duration
            total_minutes = int(duration.total_seconds() // 60)
            hours = total_minutes // 60
            minutes = total_minutes % 60

            if hours > 0:
                duration_str = f"{hours}h {minutes}m"
            else:
                duration_str = f"{minutes}m"

            # Create embed
            embed = discord.Embed(
                title="Timeout Without Reason",
                description=f"A member was timed out for over 5 minutes without a reason provided.",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="Member",
                value=f"{member.mention} ({member.name})\nID: `{member.id}`",
                inline=True
            )

            embed.add_field(
                name="Moderator",
                value=f"{moderator.mention} ({moderator.name})\nID: `{moderator.id}`",
                inline=True
            )

            embed.add_field(
                name="Duration",
                value=duration_str,
                inline=True
            )

            embed.add_field(
                name="Timeout Until",
                value=f"<t:{int(timeout_until.timestamp())}:F>\n(<t:{int(timeout_until.timestamp())}:R>)",
                inline=False
            )

            embed.set_footer(text=f"Server: {guild.name}")

            if member.avatar:
                embed.set_thumbnail(url=member.avatar.url)

            # Send notification with role pings
            await channel.send(
                content=f"||{role_mentions}||",
                embed=embed
            )

            logger.info(f"Sent timeout notification for {member.name} (ID: {member.id}) in {guild.name}")

        except discord.Forbidden:
            logger.error(f"Missing permissions to send message in channel {self.NOTIFICATION_CHANNEL_ID}")
        except Exception as e:
            logger.error(f"Error sending timeout notification: {e}")


async def setup(bot):
    """Setup function to add the cog to the bot."""
    await bot.add_cog(TimeoutMonitor(bot))