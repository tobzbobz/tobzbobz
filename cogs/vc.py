import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
import asyncio
from database import db

# Configuration
VC_REQUEST_LOG_CHANNEL_ID = 1435489971342409809  # Replace with your log channel ID

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
        reason="Reason for the request"
    )
    async def vc_request(
            self,
            interaction: discord.Interaction,
            user: discord.Member,
            voice_channel: discord.VoiceChannel,
            reason: str
    ):
        """Request a user to join a voice channel"""

        # Check if user has required role
        if not any(role.id in STAFF_ROLES for role in interaction.user.roles):
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You don't have permission to make voice channel requests!",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=False)  # Show publicly in the channel

        try:
            # Create the request embed
            embed = discord.Embed(
                title="VC Request",
                description=f"**Hello, {user.mention}!**\n\n"
                            f"You have been requested to join {voice_channel.mention} for `{reason}`.\n"
                            f"Please join within the next **10 minutes**.\n\n"
                            f"*Failure to comply may result in moderation or disciplinary actions.*"
                            f"*If you are not able to join, please DM the user requesting you or reply to this message, thank you!*\n\n"
                            f"*Requested by {interaction.user.mention}*",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )

            if interaction.guild.icon:
                embed.set_thumbnail(url=interaction.guild.icon.url)

            # Send in CURRENT channel with ping
            request_message = await interaction.followup.send(
                content=f"{user.mention}",
                embed=embed
            )

            # Store in database
            end_time = datetime.utcnow() + timedelta(minutes=30)
            await db.add_vc_request(
                message_id=request_message.id,
                channel_id=interaction.channel.id,
                user_id=user.id,
                requested_channel_id=voice_channel.id,
                requester_id=interaction.user.id,
                reason=reason,
                start_time=datetime.utcnow(),
                end_time=end_time,
                guild_id=interaction.guild.id
            )

            # Send log to logging channel
            log_channel = self.bot.get_channel(VC_REQUEST_LOG_CHANNEL_ID)
            if log_channel:
                log_embed = discord.Embed(
                    title="VC Request Created",
                    color=discord.Color.blue(),
                    timestamp=datetime.utcnow()
                )

                log_embed.add_field(name="Requested User", value=f"{user.mention} `{user.display_name}`", inline=True)
                log_embed.add_field(name="Voice Channel", value=voice_channel.mention, inline=True)
                log_embed.add_field(name="Reason", value=reason, inline=False)
                log_embed.add_field(name="Requested By",
                                    value=f"{interaction.user.mention} `{interaction.user.display_name}`", inline=True)
                log_embed.add_field(name="Request Channel", value=interaction.channel.mention, inline=True)

                # Current voice state
                if user.voice and user.voice.channel:
                    log_embed.add_field(name="Current Voice", value=f"🔊 {user.voice.channel.mention}", inline=False)
                else:
                    log_embed.add_field(name="Current Voice", value="❌ Not in voice", inline=False)

                log_embed.set_footer(text=f"Request ID: {request_message.id}")

                await log_channel.send(embed=log_embed)

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
            tracking = await db.get_active_vc_request(member.id)

            if not tracking:
                return

            # Check if tracking has expired
            if datetime.utcnow() > tracking['end_time']:
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
                await db.add_vc_activity(
                    request_id=tracking['id'],
                    activity_type=activity_type,
                    from_channel_id=from_channel,
                    to_channel_id=to_channel,
                    timestamp=datetime.utcnow()
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
                # Get all expired tracking that hasn't been completed
                expired = await db.get_expired_vc_requests()

                for tracking in expired:
                    await self.post_tracking_results(tracking)

            except Exception as e:
                print(f"Error checking expired tracking: {e}")

            # Check every 30 seconds
            await asyncio.sleep(30)

    async def post_tracking_results(self, tracking):
        """Post the tracking results to the log channel"""
        try:
            # Get the channel where request was made
            request_channel = self.bot.get_channel(tracking['channel_id'])
            if not request_channel:
                await db.mark_vc_request_completed(tracking['id'])
                return

            # Try to get the original message
            try:
                original_message = await request_channel.fetch_message(tracking['message_id'])
            except discord.NotFound:
                # Message was deleted, mark as completed
                await db.mark_vc_request_completed(tracking['id'])
                return

            # Get all activity for this tracking
            activities = await db.get_vc_activities(tracking['id'])

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
                title="VC Request Results",
                color=discord.Color.green() if joined_requested_channel else discord.Color.orange(),
                timestamp=datetime.utcnow()
            )

            result_embed.add_field(
                name="User",
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
                name="Reason",
                value=tracking.get('reason', 'Need to speak.'),
                inline=False
            )

            result_embed.add_field(
                name="Requested by",
                value=requester.mention if requester else f"<@{tracking['requester_id']}>",
                inline=False
            )

            # Add activity log
            if activity_lines:
                activity_text = "\n".join(activity_lines)
                if len(activity_text) > 1024:
                    # Take first 10 and last 10
                    first_activities = "\n".join(activity_lines[:15])
                    last_activities = "\n".join(activity_lines[-15:])
                    activity_text = f"{first_activities}\n\n*... {len(activity_lines) - 30} more activities ...*\n\n{last_activities}"

                result_embed.add_field(
                    name=f"Activity Log ({len(activities)} total)",
                    value=activity_text,
                    inline=False
                )
            else:
                result_embed.add_field(
                    name="Activity Log",
                    value="*No voice activity detected during tracking period*",
                    inline=False
                )

            result_embed.add_field(
                name="Check Duration",
                value=f"Started: <t:{int(tracking['start_time'].timestamp())}:R>\n"
                      f"Ended: <t:{int(tracking['end_time'].timestamp())}:R>",
                inline=False
            )

            if guild.icon:
                result_embed.set_thumbnail(url=guild.icon.url)

            result_embed.set_footer(text=f"Request ID: {tracking['id']}")

            # Send to LOG channel (not the original request channel)
            log_channel = self.bot.get_channel(VC_REQUEST_LOG_CHANNEL_ID)
            if log_channel:
                await log_channel.send(embed=result_embed)

            # Mark as completed
            await db.mark_vc_request_completed(tracking['id'])

        except Exception as e:
            print(f"Error posting results: {e}")
            import traceback
            traceback.print_exc()


async def setup(bot):
    await bot.add_cog(VCRequestCog(bot))