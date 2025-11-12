import discord
from discord.ext import commands
from discord import app_commands
import re

# Bot owner ID
OWNER_ID = 678475709257089057

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'allowed_role_ids': [1389550689113473024]  # Role IDs that can use the command
    },
    1425867713183744023: {
        'allowed_role_ids': None  # Role IDs that can use the command
    }
}


def get_guild_config(guild_id: int):
    """Get configuration for a specific guild"""
    return GUILD_CONFIGS.get(guild_id, {})


class ReactCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def check_permission(self, interaction: discord.Interaction) -> bool:
        """Check if user has permission to use react command"""
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

    @app_commands.command(name='react', description='Add reactions to a message as the bot')
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        message_id='The ID of the message to react to',
        emojis='The emojis to react with, separated by spaces (e.g., 😀 or :custom: or Emote ID)',
        channel='The channel where the message is (defaults to current channel)',
        remove='Remove the reactions instead of adding them'
    )
    async def react(
            self,
            interaction: discord.Interaction,
            message_id: str,
            emojis: str,
            channel: discord.TextChannel = None,
            remove: str = None
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

            if remove == 'yes':
                await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Removing Reactions",
                                                    ephemeral=True)
            else:
                await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Adding Reactions",
                                                        ephemeral=True)

            # Determine target channel
            target_channel = channel if channel else interaction.channel

            # Fetch the message
            try:
                message = await target_channel.fetch_message(int(message_id))
            except discord.NotFound:
                error_embed = discord.Embed(
                    description='Message not found! Make sure the message ID is correct and exists in the target channel <:Denied:1426930694633816248>',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return
            except ValueError:
                error_embed = discord.Embed(
                    description='Invalid message ID! Please provide a valid number <:Denied:1426930694633816248>',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return
            except discord.Forbidden:
                error_embed = discord.Embed(
                    description='I don\'t have permission to access messages in that channel <:Denied:1426930694633816248>',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # Parse the emojis (split by spaces)
            emoji_list = emojis.strip().split()

            if not emoji_list:
                error_embed = discord.Embed(
                    description='Please provide at least one emoji <:Denied:1426930694633816248>',
                    colour=discord.Colour(0xf24d4d)
                )
                await interaction.followup.send(embed=error_embed, ephemeral=True)
                return

            # Track results
            successful_reactions = []
            failed_reactions = []

            # Process each emoji
            for emoji in emoji_list:
                emoji_to_use = None

                # Check if it's a custom emoji format like <:name:id> or <a:name:id>
                custom_emoji_match = re.match(r'<(a)?:(\w+):(\d+)>', emoji)
                if custom_emoji_match:
                    # Extract the emoji ID
                    emoji_id = int(custom_emoji_match.group(3))
                    emoji_to_use = discord.utils.get(self.bot.emojis, id=emoji_id)

                    if not emoji_to_use:
                        failed_reactions.append(f'{emoji} (not found)')
                        continue

                # Check if it's just an emoji ID (numbers only)
                elif emoji.isdigit():
                    emoji_id = int(emoji)
                    emoji_to_use = discord.utils.get(self.bot.emojis, id=emoji_id)

                    if not emoji_to_use:
                        failed_reactions.append(f'{emoji} (not found)')
                        continue

                # Check if it's a custom emoji name like :emoji_name:
                elif emoji.startswith(':') and emoji.endswith(':'):
                    emoji_name = emoji.strip(':')
                    emoji_to_use = discord.utils.get(self.bot.emojis, name=emoji_name)

                    if not emoji_to_use:
                        failed_reactions.append(f':{emoji_name}: (not found)')
                        continue

                # Otherwise, treat it as a standard Unicode emoji
                else:
                    emoji_to_use = emoji

                # Add or remove the reaction based on the remove parameter
                try:
                    if remove and remove.lower() == 'yes':
                        await message.remove_reaction(emoji_to_use, self.bot.user)
                    else:
                        await message.add_reaction(emoji_to_use)
                    successful_reactions.append(str(emoji_to_use))

                except discord.HTTPException as e:
                    if e.code == 10014:  # Unknown Emoji
                        failed_reactions.append(f'{emoji} (invalid)')
                    elif e.code == 90001:  # Reaction Blocked
                        failed_reactions.append(f'{emoji} (blocked)')
                    else:
                        failed_reactions.append(f'{emoji} (error: {e.code})')
                except discord.Forbidden:
                    failed_reactions.append(f'{emoji} (no permission)')
                except Exception as e:
                    failed_reactions.append(f'{emoji} (error)')

            # Build response message
            response_parts = []

            if successful_reactions:
                action = "removed reactions from" if remove and remove.lower() == 'yes' else "reacted to"
                success_text = f'<:Accepted:1426930333789585509> Successfully {action} [message]({message.jump_url}) in {target_channel.mention} with: {" ".join(successful_reactions)}'
                response_parts.append(success_text)

            if failed_reactions:
                failed_text = f'Failed to add <:Denied:1426930694633816248>: {", ".join(failed_reactions)}'
                response_parts.append(failed_text)

            # Determine embed color based on results
            if successful_reactions and not failed_reactions:
                embed_color = discord.Colour(0x2ecc71)
            elif successful_reactions and failed_reactions:
                embed_color = discord.Colour(0xFFA756)
            else:
                embed_color = discord.Colour(0xf24d4d)

            result_embed = discord.Embed(
                description='\n\n'.join(response_parts),
                colour=embed_color
            )
            await interaction.followup.send(embed=result_embed, ephemeral=True)

        except Exception as e:
            print(f'Error in react command: {e}')
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

    @react.autocomplete('remove')
    async def remove_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        return [
            app_commands.Choice(name='Yes', value='yes')
        ]

# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(ReactCog(bot))