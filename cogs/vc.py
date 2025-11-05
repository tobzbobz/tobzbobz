import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
import asyncio
from database import db  # Your existing database connection

# Configuration
VC_REQUEST_LOG_CHANNEL_ID = 1234567890  # Replace with your channel ID
STAFF_ROLES = {  # Roles allowed to use /vc request
    1389550689113473024,
    1389113393511923863,
    1389113460687765534,
    1285474077556998196,
    1365536209681514636
}


class VCRequestCog(commands.Cog):
    vc_group = app_commands.Group(name="vc", description="Voice channel management commands")

    def __init__(self, bot):
        self.bot = bot
        # Start checking for expired tracking on bot startup
        self.bot.loop.create_task(self.check_expired_tracking())

    @vc_group.command(name="request", description="Request a user to join a voice channel")
    @app_commands.describe(
        user="The user being requested to join voice",
        voice_channel="The voice channel they should join",
        reason="Reason for the request (optional)"
    )
    async def vc_request(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            voice_channel: discord.VoiceChannel,
            reason: str = "Need to speak."
    ):
        """Request a user to join a voice channel"""

        # Check if user has required role
        if not any(role.id in STAFF_ROLES for role in interaction.user.roles):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to make voice channel requests!",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # Get the log channel
            log_channel = self.bot.get_channel(VC_REQUEST_LOG_CHANNEL_ID)
            if not log_channel:
                await interaction.followup.send(
                    "<:Denied:1426930694633816248> Could not find the log channel. Please contact an admin.",
                    ephemeral=True
                )
                return

            # Create the initial embed
            embed = discord.Embed(
                title="VC Request",
                description=f"**Hello!**\n\n"
                            f"You have been requested to join {voice_channel.mention} for **{reason}**\n"
                            f"Please join within the next **10 minutes**.\n\n"
                            f"*Failure to comply may result in moderation or disciplinary actions. "
                            f"If you are not able to join, please DM the user requesting you or reply to this message, thank you!*",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )

            embed.add_field(
                name="Requested by:",
                value=f"{interaction.user.mention} `{interaction.user.display_name}`",
                inline=False
            )

            if interaction.guild.icon:
                embed.set_thumbnail(url=interaction.guild.icon.url)

            # Send to log channel with ping
            log_message = await log_channel.send(
                content=f"{user.mention}",
                embed=embed
            )

            # Store tracking data in database
            end_time = datetime.utcnow() + timedelta(hours=1)

            async with db.pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO vc_requests
                       (message_id, channel_id, user_id, requested_channel_id, requester_id,
                        start_time, end_time, guild_id)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)''',
                    log_message.id,
                    log_channel.id,
                    user.id,
                    voice_channel.id,
                    interaction.user.id,
                    datetime.utcnow(),
                    end_time,
                    interaction.guild.id
                )

            # Confirm to requester
            await interaction.followup.send(
                f"<:Accepted:1426930333789585509> **VC Request Submitted!**\n\n"
                f"**User:** {user.mention}\n"
                f"**Channel:** {voice_channel.mention}\n"
                f"**Reason:** {reason}\n\n"
                f"🔍 Now tracking {user.display_name}'s voice activity for the next **1 hour**.\n"
                f"Results will be posted in <#{VC_REQUEST_LOG_CHANNEL_ID}>",
                ephemeral=True
            )

        except Exception as e:
            await interaction.followup.send(
                f"<:Denied:1426930694633816248> Error submitting request: {str(e)}",
                ephemeral=True
            )
            import traceback
            traceback.print_exc()

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState,
                                    after: discord.VoiceState):
        """Track voice channel joins/leaves for requested users"""

        try:
            # Check if this user is being tracked
            async with db.pool.acquire() as conn:
                tracking = await conn.fetchrow(
                    '''SELECT *
                       FROM vc_requests
                       WHERE user_id = $1
                         AND end_time > $2
                         AND completed = FALSE''',
                    member.id,
                    datetime.utcnow()
                )

            if not tracking:
                return

            # Determine what happened
            activity_type = None
            from_channel = None
            to_channel = None

            if before.channel is None and after.channel is not None:
                # User joined a voice channel
                activity_type = 'join'
                to_channel = after.channel.id

            elif before.channel is not None and after.channel is None:
                # User left a voice channel
                activity_type = 'leave'
                from_channel = before.channel.id

            elif before.channel != after.channel:
                # User switched channels
                activity_type = 'switch'
                from_channel = before.channel.id
                to_channel = after.channel.id

            if activity_type:
                # Store activity in database
                async with db.pool.acquire() as conn:
                    await conn.execute(
                        '''INSERT INTO vc_activity
                               (request_id, activity_type, from_channel_id, to_channel_id, timestamp)
                           VALUES ($1, $2, $3, $4, $5)''',
                        tracking['id'],
                        activity_type,
                        from_channel,
                        to_channel,
                        datetime.utcnow()
                    )

        except Exception as e:
            print(f"Error tracking voice activity: {e}")
            import traceback
            traceback.print_exc()

    async def check_expired_tracking(self):
        """Background task to check for expired tracking and post results"""
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                async with db.pool.acquire() as conn:
                    # Get all expired tracking that hasn't been completed
                    expired = await conn.fetch(
                        '''SELECT *
                           FROM vc_requests
                           WHERE end_time <= $1
                             AND completed = FALSE''',
                        datetime.utcnow()
                    )

                for tracking in expired:
                    await self.post_tracking_results(tracking)

            except Exception as e:
                print(f"Error checking expired tracking: {e}")

            # Check every 30 seconds
            await asyncio.sleep(30)

    async def post_tracking_results(self, tracking):
        """Post the tracking results as a reply to the original message"""
        try:
            # Get the channel and message
            channel = self.bot.get_channel(tracking['channel_id'])
            if not channel:
                return

            try:
                original_message = await channel.fetch_message(tracking['message_id'])
            except discord.NotFound:
                # Message was deleted, mark as completed
                async with db.pool.acquire() as conn:
                    await conn.execute(
                        'UPDATE vc_requests SET completed = TRUE WHERE id = $1',
                        tracking['id']
                    )
                return

            # Get all activity for this tracking
            async with db.pool.acquire() as conn:
                activities = await conn.fetch(
                    '''SELECT *
                       FROM vc_activity
                       WHERE request_id = $1
                       ORDER BY timestamp ASC''',
                    tracking['id']
                )

            # Get guild to fetch channel names
            guild = self.bot.get_guild(tracking['guild_id'])
            if not guild:
                return

            # Build activity log
            activity_lines = []
            joined_requested_channel = False

            for activity in activities:
                timestamp = int(activity['timestamp'].timestamp())

                if activity['activity_type'] == 'join':
                    channel_obj = guild.get_channel(activity['to_channel_id'])
                    channel_mention = channel_obj.mention if channel_obj else f"<#{activity['to_channel_id']}>"

                    is_requested = activity['to_channel_id'] == tracking['requested_channel_id']
                    emoji = "✅" if is_requested else "🔊"

                    line = f"{emoji} <t:{timestamp}:T> - Joined {channel_mention}"
                    if is_requested:
                        line += " **(Requested Channel)**"
                        joined_requested_channel = True

                    activity_lines.append(line)

                elif activity['activity_type'] == 'leave':
                    channel_obj = guild.get_channel(activity['from_channel_id'])
                    channel_mention = channel_obj.mention if channel_obj else f"<#{activity['from_channel_id']}>"
                    activity_lines.append(f"🔇 <t:{timestamp}:T> - Left {channel_mention}")

                elif activity['activity_type'] == 'switch':
                    from_channel_obj = guild.get_channel(activity['from_channel_id'])
                    to_channel_obj = guild.get_channel(activity['to_channel_id'])
                    from_mention = from_channel_obj.mention if from_channel_obj else f"<#{activity['from_channel_id']}>"
                    to_mention = to_channel_obj.mention if to_channel_obj else f"<#{activity['to_channel_id']}>"

                    is_requested = activity['to_channel_id'] == tracking['requested_channel_id']
                    emoji = "✅" if is_requested else "🔄"

                    line = f"{emoji} <t:{timestamp}:T> - Moved from {from_mention} to {to_mention}"
                    if is_requested:
                        line += " **(Requested Channel)**"
                        joined_requested_channel = True

                    activity_lines.append(line)

            # Create results embed
            user = guild.get_member(tracking['user_id'])
            requester = guild.get_member(tracking['requester_id'])
            requested_channel = guild.get_channel(tracking['requested_channel_id'])

            result_embed = discord.Embed(
                title="📊 VC Request - 1 Hour Tracking Results",
                color=discord.Color.green() if joined_requested_channel else discord.Color.orange(),
                timestamp=datetime.utcnow()
            )

            result_embed.add_field(
                name="Tracked User",
                value=user.mention if user else f"<@{tracking['user_id']}>",
                inline=True
            )

            result_embed.add_field(
                name="Requested Channel",
                value=requested_channel.mention if requested_channel else f"<#{tracking['requested_channel_id']}>",
                inline=True
            )

            result_embed.add_field(
                name="Compliance Status",
                value="✅ Joined Requested Channel" if joined_requested_channel else "❌ Did Not Join",
                inline=True
            )

            result_embed.add_field(
                name="Requested by",
                value=requester.mention if requester else f"<@{tracking['requester_id']}>",
                inline=False
            )

            # Add activity log
            if activity_lines:
                # Split into chunks if too long
                activity_text = "\n".join(activity_lines)
                if len(activity_text) > 1024:
                    # Take first 10 and last 10
                    first_activities = "\n".join(activity_lines[:10])
                    last_activities = "\n".join(activity_lines[-10:])
                    activity_text = f"{first_activities}\n\n*... {len(activity_lines) - 20} more activities ...*\n\n{last_activities}"

                result_embed.add_field(
                    name=f"📋 Activity Log ({len(activities)} total)",
                    value=activity_text,
                    inline=False
                )
            else:
                result_embed.add_field(
                    name="📋 Activity Log",
                    value="*No voice activity detected during tracking period*",
                    inline=False
                )

            result_embed.add_field(
                name="⏱️ Tracking Duration",
                value=f"Started: <t:{int(tracking['start_time'].timestamp())}:R>\n"
                      f"Ended: <t:{int(tracking['end_time'].timestamp())}:R>",
                inline=False
            )

            if guild.icon:
                result_embed.set_thumbnail(url=guild.icon.url)

            result_embed.set_footer(text=f"Request ID: {tracking['id']}")

            # Reply to original message
            await original_message.reply(embed=result_embed)

            # Mark as completed
            async with db.pool.acquire() as conn:
                await conn.execute(
                    'UPDATE vc_requests SET completed = TRUE WHERE id = $1',
                    tracking['id']
                )

        except Exception as e:
            print(f"Error posting tracking results: {e}")
            import traceback
            traceback.print_exc()


async def setup(bot):
    await bot.add_cog(VCRequestCog(bot))