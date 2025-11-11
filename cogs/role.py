import discord
from discord import app_commands
from discord.ext import commands
import asyncio


class RolesPaginator(discord.ui.View):
    def __init__(self, pages, author_id):
        super().__init__(timeout=300)  # 5 minute timeout
        self.pages = pages
        self.current_page = 0
        self.author_id = author_id
        self.message = None
        self.update_buttons()

    def update_buttons(self):
        # Disable buttons based on current page
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == len(self.pages) - 1

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these buttons <:Denied:1426930694633816248>", ephemeral=True)
            return

        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command user can use these buttons <:Denied:1426930694633816248>", ephemeral=True)
            return

        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    async def on_timeout(self):
        # Disable all buttons when timeout occurs
        for item in self.children:
            item.disabled = True

        try:
            if self.message:
                await self.message.edit(view=self)
        except:
            pass


class RoleCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        print("<:Accepted:1426930333789585509> RoleCog initialized")

    @app_commands.command(name="roles", description="View server roles; Get detailed info on specific server roles")
    @app_commands.describe(role="Optional: Specify a role name or ID to view detailed info")
    async def roles(self, interaction: discord.Interaction, role: str = None):
        """Display all server roles or detailed info about a specific role"""
        try:
            print(f"🔍 Roles command triggered by {interaction.user} in {interaction.guild.name}")

            # Defer the response to prevent timeout
            await interaction.response.defer()
            print("<:Accepted:1426930333789585509> Response deferred")

            # Check if user has one of the allowed roles
            allowed_role_ids = [
                1285474077556998196,
                1389550689113473024,
                1389113460687765534,
                1365536209681514636,
                1389113393511923863
            ]

            user_role_ids = [role.id for role in interaction.user.roles]
            has_permission = any(role_id in user_role_ids for role_id in allowed_role_ids)

            print(f"🔐 Permission check: {has_permission}")

            if not has_permission:
                await interaction.followup.send(
                    "You don't have permission to use this command <:Denied:1426930694633816248>",
                    ephemeral=True
                )
                return

            # If a specific role is requested
            if role:
                print(f"🔍 Looking for specific role: {role}")
                target_role = None

                if role.isdigit():
                    target_role = interaction.guild.get_role(int(role))

                if not target_role:
                    target_role = discord.utils.get(interaction.guild.roles, name=role)

                if not target_role:
                    await interaction.followup.send(
                        f"Could not find a role with the name or ID <:Denied:1426930694633816248>: `{role}`",
                        ephemeral=True
                    )
                    return

                print(f"<:Accepted:1426930333789585509> Found role: {target_role.name}, building embed...")

                # Get members with this role
                members_with_role = target_role.members
                member_count = len(members_with_role)

                # Create pages if there are many members
                pages = []
                members_per_page = 20

                # First page with role info
                embed = discord.Embed(
                    title=f"Role Information: {target_role.name}",
                    color=target_role.color if target_role.color != discord.Color.default() else discord.Color.blue()
                )

                embed.add_field(name="Role ID", value=f"`{target_role.id}`", inline=True)
                embed.add_field(name="Color", value=f"{str(target_role.color)}", inline=True)
                embed.add_field(name="Position", value=f"{target_role.position}", inline=True)
                embed.add_field(name="Members", value=f"{member_count}", inline=True)
                embed.add_field(name="Mentionable", value="<:Accepted:1426930333789585509>" if target_role.mentionable else "<:Denied:1426930694633816248>", inline=True)
                embed.add_field(name="Hoisted", value="<:Accepted:1426930333789585509>" if target_role.hoist else "<:Denied:1426930694633816248>", inline=True)

                perms = target_role.permissions
                key_perms = []
                if perms.administrator:
                    key_perms.append("Administrator")
                if perms.manage_guild:
                    key_perms.append("Manage Server")
                if perms.manage_roles:
                    key_perms.append("Manage Roles")
                if perms.manage_channels:
                    key_perms.append("Manage Channels")
                if perms.kick_members:
                    key_perms.append("Kick Members")
                if perms.ban_members:
                    key_perms.append("Ban Members")

                if key_perms:
                    embed.add_field(name="Key Permissions", value=", ".join(key_perms), inline=False)

                # Add members list if there are any
                if member_count > 0:
                    # Sort members by display name
                    sorted_members = sorted(members_with_role, key=lambda m: m.display_name.lower())

                    # First page members (up to 20)
                    first_page_members = sorted_members[:members_per_page]
                    member_list = "\n".join([f"• {member.mention}" for member in first_page_members])

                    embed.add_field(
                        name=f"Members ({member_count} total)" + (
                            f" - Page 1/{(member_count + members_per_page - 1) // members_per_page}" if member_count > members_per_page else ""),
                        value=member_list,
                        inline=False
                    )
                else:
                    embed.add_field(name="Members", value="No members have this role", inline=False)

                embed.set_footer(text=f"Page 1 | Requested by {interaction.user.display_name}")
                pages.append(embed)

                # Create additional pages if needed
                if member_count > members_per_page:
                    total_pages = (member_count + members_per_page - 1) // members_per_page

                    for page_num in range(1, total_pages):
                        start_idx = page_num * members_per_page
                        end_idx = min(start_idx + members_per_page, member_count)
                        page_members = sorted_members[start_idx:end_idx]

                        page_embed = discord.Embed(
                            title=f"Role Information: {target_role.name} (continued)",
                            color=target_role.color if target_role.color != discord.Color.default() else discord.Color.blue()
                        )

                        member_list = "\n".join([f"• {member.mention}" for member in page_members])
                        page_embed.add_field(
                            name=f"Members - Page {page_num + 1}/{total_pages}",
                            value=member_list,
                            inline=False
                        )

                        page_embed.set_footer(
                            text=f"Page {page_num + 1} | Requested by {interaction.user.display_name}")
                        pages.append(page_embed)

                print(f"📤 Sending role info with {len(pages)} page(s)")

                # Send with pagination if multiple pages
                if len(pages) > 1:
                    view = RolesPaginator(pages, interaction.user.id)
                    msg = await interaction.followup.send(embed=pages[0], view=view)
                    view.message = msg
                    print("<:Accepted:1426930333789585509> Sent with pagination")
                else:
                    msg = await interaction.followup.send(embed=pages[0])
                    print("<:Accepted:1426930333789585509> Sent successfully")

                # Auto-delete after 5 minutes
                await asyncio.sleep(300)
                try:
                    await msg.delete()
                    print("🗑️ Auto-deleted role info message")
                except:
                    pass

                return

            print("📋 Fetching all roles")
            roles = sorted(interaction.guild.roles, key=lambda r: r.position, reverse=True)
            print(f"Found {len(roles)} roles")

            # Build pages
            pages = []
            current_embed = discord.Embed(
                title=f"Roles in {interaction.guild.name}",
                description=f"Total roles: {len(roles)}",
                color=discord.Color.blue()
            )
            current_char_count = len(current_embed.title) + len(current_embed.description)
            current_field_count = 0
            role_buffer = []

            for r in roles:
                member_count = len(r.members)
                role_line = f"{r.mention} - {member_count} members\n"

                # Check if adding this role would exceed limits
                if (current_char_count + len(role_line) > 5000 or
                        current_field_count >= 24):  # Leave room for 1 more field

                    # Add current buffer as a field
                    if role_buffer:
                        field_value = "".join(role_buffer)
                        if len(field_value) > 1024:
                            field_value = field_value[:1020] + "..."
                        current_embed.add_field(
                            name="Roles",
                            value=field_value,
                            inline=False
                        )

                    # Set footer and save page
                    current_embed.set_footer(
                        text=f"Page {len(pages) + 1} | Requested by {interaction.user.display_name}")
                    pages.append(current_embed)

                    # Start new page
                    current_embed = discord.Embed(
                        title=f"Roles in {interaction.guild.name} (continued)",
                        color=discord.Color.blue()
                    )
                    current_char_count = len(current_embed.title)
                    current_field_count = 0
                    role_buffer = []

                role_buffer.append(role_line)
                current_char_count += len(role_line)

                # Add field every 20 roles
                if len(role_buffer) >= 20:
                    field_value = "".join(role_buffer)
                    if len(field_value) > 1024:
                        field_value = field_value[:1020] + "..."
                    current_embed.add_field(
                        name="Roles",
                        value=field_value,
                        inline=False
                    )
                    current_field_count += 1
                    role_buffer = []

            # Add remaining roles
            if role_buffer:
                field_value = "".join(role_buffer)
                if len(field_value) > 1024:
                    field_value = field_value[:1020] + "..."
                current_embed.add_field(
                    name="Roles",
                    value=field_value,
                    inline=False
                )

            # Add last page
            current_embed.set_footer(text=f"Page {len(pages) + 1} | Requested by {interaction.user.display_name}")
            pages.append(current_embed)

            print(f"📄 Created {len(pages)} pages")

            # Send with pagination if multiple pages
            if len(pages) > 1:
                view = RolesPaginator(pages, interaction.user.id)
                msg = await interaction.followup.send(embed=pages[0], view=view)
                view.message = msg
                print("<:Accepted:1426930333789585509> Sent with pagination")
            else:
                msg = await interaction.followup.send(embed=pages[0])
                print("<:Accepted:1426930333789585509> Sent single page")

            # Auto-delete after 5 minutes
            await asyncio.sleep(300)
            try:
                await msg.delete()
                print("<:Wipe:1434954284851658762> Auto-deleted roles message")
            except:
                print("<:Warn:1437771973970104471> Could not delete message (may already be deleted)")

        except Exception as e:
            print(f"ERROR in roles command <:Denied:1426930694633816248>: {e}")
            import traceback
            traceback.print_exc()
            try:
                await interaction.followup.send(
                    f"An error occurred <:Denied:1426930694633816248>: {str(e)}",
                    ephemeral=True
                )
            except:
                pass


async def setup(bot):
    await bot.add_cog(RoleCog(bot))
    print("<:Accepted:1426930333789585509> RoleCog setup complete")