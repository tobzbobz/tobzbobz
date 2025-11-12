import discord
from discord.ext import commands, tasks
from discord import app_commands
import random
from datetime import timedelta
from database import db

# Your Discord User ID for approval permissions
OWNER_ID = 678475709257089057


class StatusCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.status_list = [
            "With fire trucks 🚒",
            "With ambulances 🚑",
            "Emergency services",
            "Fire COMMS",
            "Ambulance COMMS",
            "K99",
            "Cardiac arrest",
            "CPR",
            "Firefighting",
            "Tourniquet",
            "HAZMAT",
            "Sirens",
            "Ambulance TV shows",
            "Ambulance movies",
            "Fire station TV shows",
            "Fire station movies",
            "Using an Ariel",
            "Moving ambulance",
            "Inserting IV",
            "Extinguishing fire",
            "Finding fire",
            "Finding vein",
            "Firefighter games",
            "Medical games",
            "Emergency services games",
            "Don't do /help for commands",
            "Do /help for no commands",
            "Keeping NZ safe",
            "Monitoring emergencies"
        ]
        self.current_status_index = 0
        self.pending_statuses = []

    async def load_submissions(self):
        """Load pending submissions from file"""
        pending = await db.get_setting(0, 'pending_statuses', [])  # guild_id=0 for bot-wide
        self.pending_statuses = pending

        # Get approved submissions
        approved = await db.get_setting(0, 'approved_statuses', [])
        for status in approved:
            if status not in self.status_list:
                self.status_list.append(status)

    async def save_submissions(self):
        """Save pending submissions to file"""
        await db.set_setting(0, 'pending_statuses', self.pending_statuses)
        default_statuses = [
            "With fire trucks 🚒",
            "With ambulances 🚑",
            "Emergency services",
            "Fire COMMS",
            "Ambulance COMMS",
            "K99",
            "Cardiac arrest",
            "CPR",
            "Firefighting",
            "Tourniquet",
            "HAZMAT",
            "Sirens",
            "Ambulance TV shows",
            "Ambulance movies",
            "Fire station TV shows",
            "Fire station movies",
            "Using an Ariel",
            "Moving ambulance",
            "Inserting IV",
            "Extinguishing fire",
            "Finding fire",
            "Finding vein",
            "Firefighter games",
            "Medical games",
            "Emergency services games",
            "Don't do /help for commands",
            "Do /help for no commands",
            "Keeping NZ safe",
            "Monitoring emergencies"
        ]
        approved = [s for s in self.status_list if s not in default_statuses]
        await db.set_setting(0, 'approved_statuses', approved)


    def is_duplicate_status(self, status_text: str) -> bool:
        """Check if status is duplicate"""
        normalized_new = status_text.strip().lower()

        for existing in self.status_list:
            if existing.strip().lower() == normalized_new:
                return True

        for pending in self.pending_statuses:
            if pending['status'].strip().lower() == normalized_new:
                return True

        return False

    async def cog_load(self):
        """Called when the cog is loaded - start tasks here"""
        print(f'Status cog loaded!')

        await self.load_submissions()

        # Start the status rotation task
        if not self.change_status.is_running():
            self.change_status.start()
        if not self.daily_submission_summary.is_running():
            self.daily_submission_summary.start()

    @commands.Cog.listener()
    async def on_ready(self):
        """Set status when bot is ready"""
        print(f'{self.bot.user} is ready!')

    @tasks.loop(hours=24)
    async def daily_submission_summary(self):
        """Send daily summary of pending submissions at 8 PM NZST"""
        if not self.pending_statuses:
            return  # Don't send if no pending submissions

        try:
            owner = await self.bot.fetch_user(OWNER_ID)

            embed = discord.Embed(
                title=f"📝 Daily Status Submission Summary",
                description=f"You have {len(self.pending_statuses)} pending status submissions.",
                color=discord.Color.blue()
            )

            # Add each pending status
            for idx, pending in enumerate(self.pending_statuses, 1):
                embed.add_field(
                    name=f"{idx}. {pending['status'][:100]}",
                    value=f"By: <@{pending['user_id']}> • <t:{int(discord.utils.parse_time(pending['submitted_at']).timestamp())}:R>",
                    inline=False
                )

            # Add the dropdown view to the daily message
            view = StatusBulkReviewView(self)
            await owner.send(embed=embed, view=view)  # CHANGE THIS LINE - add view parameter
        except Exception as e:
            print(f"Failed to send daily summary: {e}")

    @daily_submission_summary.before_loop
    async def before_daily_summary(self):
        """Wait until bot is ready and schedule for 8 PM NZST"""
        await self.bot.wait_until_ready()

        # Get current time in NZST (UTC+13 in summer, UTC+12 in winter)
        # For simplicity, using UTC+12 (adjust if needed)
        from datetime import datetime, time
        import pytz

        nz_tz = pytz.timezone('Pacific/Auckland')
        now = datetime.now(nz_tz)
        target_time = now.replace(hour=20, minute=0, second=0, microsecond=0)

        # If it's past 8 PM today, schedule for tomorrow
        if now.time() >= time(20, 0):
            target_time += timedelta(days=1)

    @tasks.loop(minutes=1)
    async def change_status(self):
        """Automatically change bot status with random type"""
        # Pick a random status message
        status = random.choice(self.status_list)

        # Pick a random activity type (1=Playing, 2=Streaming, 3=Listening, 4=Watching)
        activity_type = random.randint(1, 4)

        if activity_type == 1:
            # Playing
            activity = discord.Game(name=status)
        elif activity_type == 2:
            # Streaming (requires a valid URL)
            activity = discord.Streaming(name=status, url="https://twitch.tv/discord")
        elif activity_type == 3:
            # Listening
            activity = discord.Activity(type=discord.ActivityType.listening, name=status)
        else:
            # Watching
            activity = discord.Activity(type=discord.ActivityType.watching, name=status)

        await self.bot.change_presence(activity=activity)

    @change_status.before_loop
    async def before_change_status(self):
        """Wait until bot is ready before starting the loop"""
        await self.bot.wait_until_ready()

    @app_commands.command(name="status", description="Submit a new status suggestion for the bot")
    @app_commands.describe(suggestion="Your status suggestion (e.g., 'Emergency response')")
    async def submit_status(self, interaction: discord.Interaction, suggestion: str):
        """Submit a status suggestion"""

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Submitting Status",
                                                ephemeral=True)

        # Check if status is too long
        if len(suggestion) > 128:
            await interaction.followup.send(
                "Status suggestion is too long! Must be 128 characters or less <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Check for duplicates (case-insensitive)
        if self.is_duplicate_status(suggestion):
            await interaction.followup.send(
                "This status (or a very similar one) already exists or is pending review <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Add to pending list
        self.pending_statuses.append({
            'status': suggestion,
            'user_id': interaction.user.id,
            'user_name': str(interaction.user),
            'submitted_at': discord.utils.utcnow().isoformat()
        })

        await self.save_submissions()
        await interaction.delete_original_response()

        # Confirm submission
        embed = discord.Embed(
            title="<:Accepted:1426930333789585509> Status Submitted",
            description=f"Your status suggestion has been submitted for review!\n\n**Suggestion:** {suggestion}",
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="status-view", description="Review pending status submissions")
    @app_commands.default_permissions(administrator=True)
    async def view_status(self, interaction: discord.Interaction):
        """View and approve/deny status submissions"""
        # Check if user is owner
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "You don't have permission to use this command <:Denied:1426930694633816248>",
                ephemeral=True
            )
            return

        # Check if there are pending submissions
        if not self.pending_statuses:
            await interaction.response.send_message(
                "<:Accepted:1426930333789585509> No pending status submissions!",
                ephemeral=True
            )
            return

        # Create embed with all pending statuses
        embed = discord.Embed(
            title=f"📝 Pending Status Submissions ({len(self.pending_statuses)})",
            description="Select a status to approve or deny using the dropdown menu below.",
            color=discord.Color.blue()
        )

        # Add each pending status as a field
        for idx, pending in enumerate(self.pending_statuses, 1):
            user_mention = f"<@{pending['user_id']}>"
            time_ago = f"<t:{int(discord.utils.parse_time(pending['submitted_at']).timestamp())}:R>"
            embed.add_field(
                name=f"{idx}. {pending['status'][:50]}{'...' if len(pending['status']) > 50 else ''}",
                value=f"👤 {user_mention} • 🕒 {time_ago}",
                inline=False
            )

        # Create view with dropdown
        view = StatusBulkReviewView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def approve_status_with_reason(self, interaction: discord.Interaction, index: int, reason: str = None):
        """Approve a status submission by index with optional reason"""
        if index >= len(self.pending_statuses):
            await interaction.response.send_message("Invalid submission index <:Denied:1426930694633816248>", ephemeral=True)
            return

        pending = self.pending_statuses[index]
        status_text = pending['status']
        user_id = pending['user_id']

        # Add to status list
        self.status_list.append(status_text)

        # Remove from pending
        self.pending_statuses.pop(index)

        # UPDATED: Save to database
        await self.save_submissions()

        # DM the user
        try:
            user = await self.bot.fetch_user(user_id)
            approve_embed = discord.Embed(
                title="<:Accepted:1426930333789585509> Status Approved!",
                description=f"Your status suggestion has been approved and added to the rotation!\n\n**Status:** {status_text}",
                color=discord.Color.green()
            )

            if reason:
                approve_embed.add_field(name="Reason", value=reason, inline=False)

            await user.send(embed=approve_embed)
        except:
            pass

        # Update the original interaction
        await interaction.response.send_message("<:Accepted:1426930333789585509> Status approved!", ephemeral=True)

        # Try to update the review view if possible
        try:
            # Find and update the original message
            await self.refresh_view_from_modal(interaction, index)
        except:
            pass

    # Keep the old method for backward compatibility
    async def approve_status_by_index(self, interaction: discord.Interaction, index: int):
        await self.approve_status_with_reason(interaction, index, None)

    async def deny_status_with_reason(self, interaction: discord.Interaction, index: int, reason: str = None):
        """Deny a status submission by index with optional reason"""
        if index >= len(self.pending_statuses):
            await interaction.response.send_message("Invalid submission index <:Denied:1426930694633816248>", ephemeral=True)
            return

        pending = self.pending_statuses[index]
        status_text = pending['status']
        user_id = pending['user_id']

        # Remove from pending
        self.pending_statuses.pop(index)
        await self.save_submissions()

        # DM the user
        try:
            user = await self.bot.fetch_user(user_id)
            deny_embed = discord.Embed(
                title="Status Denied <:Denied:1426930694633816248>",
                description=f"Your status suggestion was not approved.\n\n**Status:** {status_text}",
                color=discord.Color.red()
            )

            if reason:
                deny_embed.add_field(name="Reason", value=reason, inline=False)

            await user.send(embed=deny_embed)
        except:
            pass

        # Update the original interaction
        await interaction.response.send_message("Status denied <:Denied:1426930694633816248>", ephemeral=True)

    # Keep the old method for backward compatibility
    async def deny_status_by_index(self, interaction: discord.Interaction, index: int):
        await self.deny_status_with_reason(interaction, index, None)

    async def refresh_view(self, interaction: discord.Interaction):
        """Refresh the status view after approval/denial"""
        if not self.pending_statuses:
            await interaction.response.edit_message(
                content="<:Accepted:1426930333789585509> All status submissions reviewed!",
                embed=None,
                view=None
            )
            return

        # Create updated embed
        embed = discord.Embed(
            title=f"📝 Pending Status Submissions ({len(self.pending_statuses)})",
            description="Select a status to approve or deny using the dropdown menu below.",
            color=discord.Color.blue()
        )

        # Add each pending status as a field
        for idx, pending in enumerate(self.pending_statuses, 1):
            user_mention = f"<@{pending['user_id']}>"
            time_ago = f"<t:{int(discord.utils.parse_time(pending['submitted_at']).timestamp())}:R>"
            embed.add_field(
                name=f"{idx}. {pending['status'][:50]}{'...' if len(pending['status']) > 50 else ''}",
                value=f"👤 {user_mention} • 🕒 {time_ago}",
                inline=False
            )

        # Create new view
        view = StatusBulkReviewView(self)
        await interaction.response.edit_message(embed=embed, view=view)

    async def cog_unload(self):
        """Cancel the task when cog is unloaded"""
        self.change_status.cancel()
        self.daily_submission_summary.cancel()

class StatusBulkReviewView(discord.ui.View):
    def __init__(self, cog: StatusCog):
        super().__init__(timeout=300)  # 5 minute timeout
        self.cog = cog
        self.add_item(StatusSelectDropdown(cog))


class StatusSelectDropdown(discord.ui.Select):
    def __init__(self, cog: StatusCog):
        self.cog = cog

        # Create options for each pending status (max 25)
        options = []
        for idx, pending in enumerate(self.cog.pending_statuses[:25]):
            # Truncate status if too long for select menu
            label = pending['status'][:100]
            description = f"By: {pending['user_name'][:50]}"

            options.append(
                discord.SelectOption(
                    label=label,
                    description=description,
                    value=str(idx)
                )
            )

        super().__init__(
            placeholder="Select a status to review...",
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        """Handle dropdown selection"""
        index = int(self.values[0])
        pending = self.cog.pending_statuses[index]

        # Create detailed view for this submission
        embed = discord.Embed(
            title="📝 Review Status Submission",
            description=f"**Status:** {pending['status']}",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Submitted by",
            value=f"<@{pending['user_id']}> ({pending['user_name']})",
            inline=True
        )
        embed.add_field(
            name="Submitted",
            value=f"<t:{int(discord.utils.parse_time(pending['submitted_at']).timestamp())}:R>",
            inline=True
        )

        # Create approve/deny buttons for this specific submission
        view = StatusIndividualReviewView(self.cog, index)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class StatusIndividualReviewView(discord.ui.View):
    def __init__(self, cog: StatusCog, index: int):
        super().__init__(timeout=180)  # 3 minute timeout
        self.cog = cog
        self.index = index

    @discord.ui.button(label="<:Accepted:1426930333789585509> Approve", style=discord.ButtonStyle.green)
    async def approve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Show modal for optional reason
        modal = ReasonModal(self.cog, self.index, is_approval=True)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="<:Denied:1426930694633816248> Deny", style=discord.ButtonStyle.red)
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Show modal for optional reason
        modal = ReasonModal(self.cog, self.index, is_approval=False)
        await interaction.response.send_modal(modal)


class ReasonModal(discord.ui.Modal, title="Add Reason (Optional)"):
    def __init__(self, cog: StatusCog, index: int, is_approval: bool):
        super().__init__()
        self.cog = cog
        self.index = index
        self.is_approval = is_approval

    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="Enter a reason (optional, leave blank for none)",
        required=False,
        max_length=500,
        style=discord.TextStyle.paragraph
    )

    async def on_submit(self, interaction: discord.Interaction):
        reason_text = self.reason.value.strip() if self.reason.value else None

        if self.is_approval:
            await self.cog.approve_status_with_reason(interaction, self.index, reason_text)
        else:
            await self.cog.deny_status_with_reason(interaction, self.index, reason_text)

# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(StatusCog(bot))