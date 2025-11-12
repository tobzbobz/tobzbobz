import discord
from discord.ext import commands
from discord import app_commands

# Bot owner ID
OWNER_ID = 678475709257089057

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'allowed_role_ids': [1365536209681514636, 1389113393511923863, 1285474077556998196, 1389113460687765534, 1389550689113473024]  # Role IDs that can use the command
    },
    1425867713183744023: {
        'allowed_role_ids': None  # Role IDs that can use the command
    },
    1420770769562243083: {
        'allowed_role_ids': 1424394267291549787
    }
}


def get_guild_config(guild_id: int):
    """Get configuration for a specific guild"""
    return GUILD_CONFIGS.get(guild_id, {})


class InactiveTicketCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def check_permission(self, interaction: discord.Interaction) -> bool:
        """Check if user has permission to use inactive ticket command"""
        # Owner bypass
        if interaction.user.id == OWNER_ID:
            return True

        # Get guild-specific configuration
        guild_config = get_guild_config(interaction.guild.id)
        allowed_role_ids = guild_config.get('allowed_role_ids', [])

        # Check if user has required role
        if allowed_role_ids:
            user_role_ids = [role.id for role in interaction.user.roles]
            return any(role_id in user_role_ids for role_id in allowed_role_ids)

        return True  # No restrictions if no roles configured

    # Create command group
    inactive_group = app_commands.Group(name="inactive", description="Inactive ticket commands")

    @inactive_group.command(name='ticket', description='Sends an inactive ticket notice')
    @app_commands.default_permissions(manage_nicknames=True)
    @app_commands.describe(
        user='The user the notice is for'
    )
    async def inactive_ticket(
            self,
            interaction: discord.Interaction,
            user: discord.User
    ):
        try:
            # Check permissions
            if not self.check_permission(interaction):
                no_permission_embed = discord.Embed(
                    description='You do not have permission to use this command <:Denied:1426930694633816248>',
                    colour=discord.Colour(0xf24d4d)
                )

                await interaction.response.send_message(embed=no_permission_embed, ephemeral=True)
                return

            await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Sending Inactivity Notice",
                                                    ephemeral=True)

            # Create the inactive ticket embed
            # Create the inactive ticket embed
            embed = discord.Embed(
                title='Inactive Ticket Alert',
                description=f'''Kia Ora {user.mention},\n\nWe've noticed that this ticket has been inactive for a bit of time. To ensure we can assist you to the fullest, please respond within 12 hours of this message and let us know what still needs to be done or how else we can help! If you don't respond to us it's ok, the ticket will be automatically closed to help keep things organised for us.\n\nIf the ticket does get closed and you still need assistance, don't worry you can always open a new ticket at any time.\n\nWe're always here to help, so please let us know how we can assist you further or what else we can assist you with!\n\nThank you for your patience and understanding.\n\n*Cheers, {interaction.user.mention}.*''',
                colour=discord.Colour(0xFFFFFF)
            )

            embed.set_thumbnail(url='https://cdn.discordapp.com/avatars/1426248543706026026/84e798cc0f8fabb5260fc0b715d07242.png?size=512'),
            embed.set_image(url='https://message.style/cdn/images/465c6cd9daa50d83686fc37e82c6e223ea8f4f5330365012db441087fc1a69d6.png')

            # Send the embed in the current channel
            await interaction.channel.send(content=f'||{user.mention}||', embed=embed)

            # Confirm to user
            success_embed = discord.Embed(
                description=f'<:Accepted:1426930333789585509> Inactive ticket warning sent to {user.mention}!',
                colour=discord.Colour(0x2ecc71)
            )

            await interaction.followup.send(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error in inactive ticket command: {e}')
            # Send error DM to owner

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )

            await interaction.delete_original_response()
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True, delete_after=60)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True, delete_after=60)

            raise


# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(InactiveTicketCog(bot))