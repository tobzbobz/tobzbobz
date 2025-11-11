import discord
from discord.ext import commands
from discord import app_commands

# Bot owner ID
OWNER_ID = 678475709257089057

# Configuration for multiple guilds
GUILD_CONFIGS = {
    1282916959062851634: {
        'allowed_role_ids': [1389550689113473024]  # Role IDs that can use the command
    },
    1425867713183744023: {
        'allowed_role_ids': [1426185588930777199]  # Role IDs that can use the command
    }
}


def get_guild_config(guild_id: int):
    """Get configuration for a specific guild"""
    return GUILD_CONFIGS.get(guild_id, {})


class EditTextModal(discord.ui.Modal, title='Edit Message'):
    message_text = discord.ui.TextInput(
        label='Message Text',
        placeholder='Enter the new message text',
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000
    )

    def __init__(self, bot, message):
        super().__init__()
        self.bot = bot
        self.message = message

        # Pre-fill with existing message content
        if message.content:
            self.message_text.default = message.content

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Edit the message
            await self.message.edit(content=self.message_text.value)

            # Confirm to user
            success_embed = discord.Embed(
                description='<:Accepted:1426930333789585509> Message edited successfully!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.response.send_message(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error editing message: {e}')

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            try:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            except:
                pass

            raise

class EmbedModal(discord.ui.Modal, title='Create Embed'):
    embed_title = discord.ui.TextInput(
        label='Title',
        placeholder='Enter embed title (optional)',
        required=False,
        max_length=256
    )

    description = discord.ui.TextInput(
        label='Description',
        placeholder='Enter embed description (optional)',
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=4000
    )

    color = discord.ui.TextInput(
        label='Color (Hex)',
        placeholder='Enter color hex code (e.g., #FF5733 or FF5733) (optional)',
        required=False,
        max_length=7
    )

    footer = discord.ui.TextInput(
        label='Footer',
        placeholder='Enter footer text (optional)',
        required=False,
        max_length=2048
    )

    def __init__(self, bot, channel, reply_to=None, image_url=None, thumbnail_url=None):
        super().__init__()
        self.bot = bot
        self.channel = channel
        self.reply_to = reply_to
        self.image_url = image_url
        self.thumbnail_url = thumbnail_url

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Create embed
            embed = discord.Embed()

            # Add title if provided
            if self.embed_title.value:
                embed.title = self.embed_title.value

            # Add description if provided
            if self.description.value:
                embed.description = self.description.value

            # Add color if provided
            if self.color.value:
                # Remove # if present and convert to hex
                color_hex = self.color.value.strip().lstrip('#')
                try:
                    embed.colour = discord.Colour(int(color_hex, 16))
                except ValueError:
                    pass  # Invalid color, skip it

            # Add footer if provided
            if self.footer.value:
                embed.set_footer(text=self.footer.value)

            # Add image if provided
            if self.image_url:
                embed.set_image(url=self.image_url)

            # Add thumbnail if provided
            if self.thumbnail_url:
                embed.set_thumbnail(url=self.thumbnail_url)

            # Send the embed
            if self.reply_to:
                await self.reply_to.reply(embed=embed)
            else:
                await self.channel.send(embed=embed)

            # Confirm to user
            success_embed = discord.Embed(
                description='<:Accepted:1426930333789585509> Embed sent successfully!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.response.send_message(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error sending embed: {e}')
            # Send error DM to owner

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            try:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            except:
                pass  # Response might already be sent

            raise


class EditEmbedModal(discord.ui.Modal, title='Edit Embed'):
    embed_title = discord.ui.TextInput(
        label='Title',
        placeholder='Enter embed title (optional)',
        required=False,
        max_length=256
    )

    description = discord.ui.TextInput(
        label='Description',
        placeholder='Enter embed description (optional)',
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=4000
    )

    color = discord.ui.TextInput(
        label='Color (Hex)',
        placeholder='Enter color hex code (e.g., #FF5733 or FF5733) (optional)',
        required=False,
        max_length=7
    )

    footer = discord.ui.TextInput(
        label='Footer',
        placeholder='Enter footer text (optional)',
        required=False,
        max_length=2048
    )

    def __init__(self, bot, message, image_url=None, thumbnail_url=None):
        super().__init__()
        self.bot = bot
        self.message = message
        self.image_url = image_url
        self.thumbnail_url = thumbnail_url

        # Pre-fill with existing embed data
        if message.embeds:
            existing_embed = message.embeds[0]

            if existing_embed.title:
                self.embed_title.default = existing_embed.title

            if existing_embed.description:
                self.description.default = existing_embed.description

            if existing_embed.color:
                # Convert color to hex
                hex_color = format(existing_embed.color.value, '06x')
                self.color.default = f'#{hex_color}'

            if existing_embed.footer:
                self.footer.default = existing_embed.footer.text

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Create embed
            embed = discord.Embed()

            # Add title if provided
            if self.embed_title.value:
                embed.title = self.embed_title.value

            # Add description if provided
            if self.description.value:
                embed.description = self.description.value

            # Add color if provided
            if self.color.value:
                color_hex = self.color.value.strip().lstrip('#')
                try:
                    embed.colour = discord.Colour(int(color_hex, 16))
                except ValueError:
                    pass

            # Add footer if provided
            if self.footer.value:
                embed.set_footer(text=self.footer.value)

            # Add image - use new if provided, otherwise keep existing
            if self.image_url:
                embed.set_image(url=self.image_url)
            elif self.message.embeds and self.message.embeds[0].image:
                embed.set_image(url=self.message.embeds[0].image.url)

            # Add thumbnail - use new if provided, otherwise keep existing
            if self.thumbnail_url:
                embed.set_thumbnail(url=self.thumbnail_url)
            elif self.message.embeds and self.message.embeds[0].thumbnail:
                embed.set_thumbnail(url=self.message.embeds[0].thumbnail.url)

            # Edit the message
            await self.message.edit(embed=embed)

            # Confirm to user
            success_embed = discord.Embed(
                description='<:Accepted:1426930333789585509> Embed edited successfully!',
                colour=discord.Colour(0x2ecc71)
            )
            await interaction.response.send_message(embed=success_embed, ephemeral=True)

        except Exception as e:
            print(f'Error editing embed: {e}')

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            try:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            except:
                pass

            raise


class SayCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def check_permission(self, interaction: discord.Interaction) -> bool:
        """Check if user has permission to use say commands"""
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
    say_group = app_commands.Group(name="say", description="Make the bot say something")

    @say_group.command(name='text', description='Send a message (or edit a sent message) as the bot')
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        text='What you want the bot to say',
        channel='Channel ID to send the message in',
        reply='Message ID to reply to (or edit)',
        edit='Edit an existing message by the bot instead of sending a new one'
    )
    async def say_text(
            self,
            interaction: discord.Interaction,
            text: str = None,
            channel: discord.TextChannel = None,
            reply: str = None,
            edit: str = None
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

            # Determine target channel
            target_channel = channel if channel else interaction.channel

            # Handle edit mode FIRST (before any defer)
            if edit and edit.lower() == 'yes':
                # Check if text parameter was provided with edit
                if text:
                    error_embed = discord.Embed(
                        description='You cannot provide text when using edit mode! The edit form will let you modify the message content <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

                if not reply:
                    error_embed = discord.Embed(
                        description='You must provide a message ID in the `reply` field to edit a message <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

                try:
                    # Fetch the message to edit
                    message_to_edit = await target_channel.fetch_message(int(reply))

                    # Check if bot owns the message
                    if message_to_edit.author.id != self.bot.user.id:
                        error_embed = discord.Embed(
                            description='I can only edit my own messages <:Denied:1426930694633816248>',
                            colour=discord.Colour(0xf24d4d)
                        )
                        await interaction.response.send_message(embed=error_embed, ephemeral=True)
                        return

                    # Show edit modal with existing message content
                    modal = EditTextModal(self.bot, message_to_edit)
                    await interaction.response.send_modal(modal)
                    return

                except discord.NotFound:
                    error_embed = discord.Embed(
                        description='Message not found! Make sure the message ID is correct <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return
                except ValueError:
                    error_embed = discord.Embed(
                        description='Invalid message ID! Please provide a valid number <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return
                except discord.Forbidden:
                    error_embed = discord.Embed(
                        description='I don\'t have permission to edit that message <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

            # Normal send mode - defer now
            await interaction.response.defer(ephemeral=True)

            # Check if reply parameter is provided
            if reply:
                try:
                    # Fetch the message to reply to
                    message_to_reply = await target_channel.fetch_message(int(reply))
                    # Send the message as a reply
                    await message_to_reply.reply(content=text)

                    # Confirm to user
                    success_embed = discord.Embed(
                        description=f'<:Accepted:1426930333789585509> Message sent as a reply in {target_channel.mention}!',
                        colour=discord.Colour(0x2ecc71)
                    )
                    await interaction.followup.send(embed=success_embed, ephemeral=True)

                except discord.NotFound:
                    error_embed = discord.Embed(
                        description='Message not found! Make sure the message ID is correct and exists in the target channel <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
                except ValueError:
                    error_embed = discord.Embed(
                        description='Invalid message ID! Please provide a valid number <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
                except discord.Forbidden:
                    error_embed = discord.Embed(
                        description='I don\'t have permission to send messages in that channel <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
            else:
                # Send the message normally
                try:
                    await target_channel.send(content=text)

                    # Confirm to user
                    success_embed = discord.Embed(
                        description=f'<:Accepted:1426930333789585509> Message sent in {target_channel.mention}!',
                        colour=discord.Colour(0x2ecc71)
                    )
                    await interaction.followup.send(embed=success_embed, ephemeral=True)
                except discord.Forbidden:
                    error_embed = discord.Embed(
                        description='I don\'t have permission to send messages in that channel <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.followup.send(embed=error_embed, ephemeral=True)

        except Exception as e:
            print(f'Error in say text command: {e}')
            # Send error DM to owner

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)

            raise

    @say_text.autocomplete('edit')
    async def edit_text_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        return [
            app_commands.Choice(name='Yes', value='yes')
        ]

    @say_group.command(name='embed', description='Send an embed (or edit a sent embed) as the bot')
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        image='Upload an image',
        thumbnail='Upload a thumbnail',
        channel='Channel ID to send the embed in',
        reply='Message ID to reply to (or to edit)',
        edit='Edit an existing embed by the Bot instead of sending a new one'
    )
    async def say_embed(
            self,
            interaction: discord.Interaction,
            image: discord.Attachment = None,
            thumbnail: discord.Attachment = None,
            channel: discord.TextChannel = None,
            reply: str = None,
            edit: str = None
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

            # Determine target channel
            target_channel = channel if channel else interaction.channel

            # Handle edit mode FIRST (before any defer)
            if edit and edit.lower() == 'yes':
                if not reply:
                    error_embed = discord.Embed(
                        description='You must provide a message ID in the `reply` field to edit an embed <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

                try:
                    message_to_edit = await target_channel.fetch_message(int(reply))

                    # Check if bot owns the message
                    if message_to_edit.author.id != self.bot.user.id:
                        error_embed = discord.Embed(
                            description='I can only edit my own messages <:Denied:1426930694633816248>',
                            colour=discord.Colour(0xf24d4d)
                        )
                        await interaction.response.send_message(embed=error_embed, ephemeral=True)
                        return

                    # Check if message has embeds
                    if not message_to_edit.embeds:
                        error_embed = discord.Embed(
                            description='That message doesn\'t have an embed to edit <:Denied:1426930694633816248>',
                            colour=discord.Colour(0xf24d4d)
                        )
                        await interaction.response.send_message(embed=error_embed, ephemeral=True)
                        return

                    # Convert attachments to URLs if provided
                    image_url = image.url if image else None
                    thumbnail_url = thumbnail.url if thumbnail else None

                    # Show edit modal with existing embed data
                    modal = EditEmbedModal(self.bot, message_to_edit, image_url, thumbnail_url)
                    await interaction.response.send_modal(modal)
                    return

                except discord.NotFound:
                    error_embed = discord.Embed(
                        description='Message not found! Make sure the message ID is correct <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return
                except ValueError:
                    error_embed = discord.Embed(
                        description='Invalid message ID! Please provide a valid number <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return
                except discord.Forbidden:
                    error_embed = discord.Embed(
                        description='I don\'t have permission to access that message <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

            # Normal create mode - fetch reply message if needed
            reply_message = None
            if reply:
                try:
                    reply_message = await target_channel.fetch_message(int(reply))
                except discord.NotFound:
                    error_embed = discord.Embed(
                        description='Message not found! Make sure the message ID is correct and exists in the target channel <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return
                except ValueError:
                    error_embed = discord.Embed(
                        description='Invalid message ID! Please provide a valid number <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return
                except discord.Forbidden:
                    error_embed = discord.Embed(
                        description='I don\'t have permission to access messages in that channel <:Denied:1426930694633816248>',
                        colour=discord.Colour(0xf24d4d)
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

            # Show the modal
            # Convert attachments to URLs if provided
            image_url = image.url if image else None
            thumbnail_url = thumbnail.url if thumbnail else None

            modal = EmbedModal(self.bot, target_channel, reply_message, image_url, thumbnail_url)
            await interaction.response.send_modal(modal)

        except Exception as e:
            print(f'Error in say embed command: {e}')
            # Send error DM to owner

            error_embed = discord.Embed(
                description=f'Error <:Denied:1426930694633816248>: {e}',
                colour=discord.Colour(0xf24d4d)
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)

            raise

    @say_embed.autocomplete('edit')
    async def edit_embed_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str
    ) -> list[app_commands.Choice[str]]:
        return [
            app_commands.Choice(name='Yes', value='yes')
        ]


# Setup function (required for cogs)
async def setup(bot):
    await bot.add_cog(SayCog(bot))