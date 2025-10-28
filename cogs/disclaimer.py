import discord
from discord.ext import commands
from discord import app_commands

# Bot owner ID
OWNER_ID = 678475709257089057

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'allowed_role_ids': [1333197141920710718]  # Role IDs that can use the command
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


class DisclaimerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def check_permission(self, interaction: discord.Interaction) -> bool:
        """Check if user has permission to use disclaimer command"""
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

    @app_commands.command(name='disclaimer', description='Display important disclaimer information.')
    async def disclaimer(self, interaction: discord.Interaction):
        try:
            # Check permissions (optional - remove this block if you want everyone to use it)
            if not self.check_permission(interaction):
                no_permission_embed = discord.Embed(
                    description='You do not have permission to use this command <:Denied:1426930694633816248>',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=no_permission_embed, ephemeral=True)
                return

            # Defer the response
            await interaction.response.defer(ephemeral=False)  # Set to True if you want only the user to see it

            # First Embed
            embed2 = discord.Embed(
                title='⚠️ Disclaimer',
                description='''The ***New Zealand Copyright Act of 1994*** protects all original materials produced and owned by **FENZ and HHStJ Utilities**, including but not limited to source code, documentation, branding, and Discord assets.\n\nIt is strictly forbidden for any person, server, or organisation to use, reproduce, resell, or claim ownership of these materials without permission. The **FENZ and HHStJ Development Team** reserves the right to pursue legal action against any infringement, including submitting a DMCA notice.\n\nSee if it applies to your country: [NZ Copyright Act 1994](https://www.legislation.govt.nz/act/public/1994/0143/latest/DLM345634.html)\n\n~~‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎‎ ‎ ‎ ‎ ‎‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎ ‎  ‎ ~~\n‎\n📜 Legal Links:\n- [**Discord Support Server**](https://discord.gg/9ntthSkhhS).\n- [**Terms of Service**](https://discord.com/channels/1430002479239532747/1430003436316921897) (In Support Server).\n- [**Privacy Policy**](https://discord.com/channels/1430002479239532747/1430002586584612946) (In Support Server).\n- Contact **<@678475709257089057>** directly through discord for any other inquiries.''',
                colour=discord.Colour(0x000000)
            )
            embed2.set_image(
                url='https://cdn.discordapp.com/attachments/1430002480523247618/1430025254578819163/image.png?ex=68f8464d&is=68f6f4cd&hm=64929e9f5a0f72680548018751c3b1194c4ec1caa623d07708eaa315ee6bcc43&')
            embed2.set_footer(text='Last Updated | October 21, 2025')

            # Second Embed
            embed1 = discord.Embed(
                colour=discord.Colour(0x000000)
            )
            embed1.set_image(
                url='https://cdn.discordapp.com/attachments/1430002480523247618/1430300284269105222/TERMS_5.png?ex=68f94671&is=68f7f4f1&hm=45e909cb0953756a4b1fd9cec9da2420495e9cca5decfc0410b0fd1e9a0d1874&')

            # Send both embeds
            await interaction.followup.send(embeds=[embed1, embed2])

        except Exception as e:
            print(f'Error in disclaimer command: {e}')

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)

            raise


# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(DisclaimerCog(bot))