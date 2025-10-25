import discord
from discord.ext import commands
import asyncio

# Guild ID for FENZ (replace with your actual guild ID)
FENZ_GUILD_ID = 1282916959062851634  # Replace with your FENZ server ID

# Configuration: Add multiple emoji-user pairs here
# Format: {"emoji": user_id_to_ping} or {"!emoji": user_id_to_ping} for prefix requirement
EMOJI_CONFIG = {
    "🪖": 587515628190040076,  # Military helmet
    "!👻": 590274454563717147,  # Ghost emoji with ! prefix required
    # Add more emoji-user pairs here:
    # "🔥": 123456789012345678,
    # "!⚡": 987654321098765432,  # Lightning with ! prefix required
}


class NumberInputModal(discord.ui.Modal):
    def __init__(self, emoji: str, target_user_id: int, channel):
        super().__init__(title=f"{emoji} - Enter Number")

        self.emoji = emoji
        self.target_user_id = target_user_id
        self.channel = channel

        self.number_input = discord.ui.TextInput(
            label="Enter a number",
            placeholder="Type a number here...",
            style=discord.TextStyle.short,
            required=True,
            max_length=10
        )

        self.add_item(self.number_input)

    async def on_submit(self, interaction: discord.Interaction):
        # Validate that input is a number
        try:
            number = int(self.number_input.value)

            # Validate number is reasonable (1-50 to prevent spam)
            if number < 1:
                embed = discord.Embed(
                    title="Invalid Number <:Denied:1426930694633816248>",
                    description="Please enter a number greater than 0.",
                    color=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return

            if number > 50:
                embed = discord.Embed(
                    title="Number Too Large <:Denied:1426930694633816248>",
                    description="Please enter a number between 1 and 50.",
                    color=discord.Colour(0xf24d4d)
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return

            # Create confirmation embed
            embed = discord.Embed(
                title="✅ Processing...",
                description=f"Sending {number} ghost ping(s)...",
                color=discord.Color(0x2ecc71)
            )

            await interaction.response.send_message(embed=embed, ephemeral=True)

            # Ghost ping the target user the specified number of times
            for i in range(number):
                # Send the emoji
                emoji_msg = await self.channel.send(self.emoji)

                # Send user ping
                ping_msg = await self.channel.send(f"<@{self.target_user_id}>")

                # Delete the ping message immediately (ghost ping)
                await ping_msg.delete()

                # Optional: small delay between pings to avoid rate limits
                # await asyncio.sleep(0.5)

            # Send completion message
            completion_embed = discord.Embed(
                title="✅ Complete",
                description=f"Successfully sent {number} ghost ping(s) with {self.emoji}!",
                color=discord.Color(0x2ecc71)
            )

            # Try to edit the original response
            try:
                await interaction.edit_original_response(embed=completion_embed)
            except:
                await interaction.followup.send(embed=completion_embed, ephemeral=True)

        except ValueError:
            # If input is not a valid number
            embed = discord.Embed(
                title="Invalid Input <:Denied:1426930694633816248>",
                description="Please enter a valid number.",
                color=discord.Colour(0xf24d4d)
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            # Handle any other errors
            error_embed = discord.Embed(
                title="Error <:Denied:1426930694633816248>",
                description=f"An error occurred: {str(e)}",
                color=discord.Colour(0xf24d4d)
            )
            try:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            except:
                await interaction.followup.send(embed=error_embed, ephemeral=True)


class InvisibleButtonView(discord.ui.View):
    def __init__(self, emoji: str, target_user_id: int, channel, author):
        super().__init__(timeout=5)  # 5 second timeout
        self.emoji = emoji
        self.target_user_id = target_user_id
        self.channel = channel
        self.author = author

    @discord.ui.button(label="Open Form", style=discord.ButtonStyle.primary, emoji="📝")
    async def open_modal_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only allow the original author to click
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                "This is not for you!",
                ephemeral=True
            )
            return

        # Show the modal
        modal = NumberInputModal(self.emoji, self.target_user_id, self.channel)
        await interaction.response.send_modal(modal)

        # Stop the view and try to delete the message
        self.stop()
        try:
            await interaction.message.delete()
        except:
            pass


class EmojiTriggerCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message):
        # Ignore bot messages
        if message.author.bot:
            return

        # Only process messages in FENZ guild
        if message.guild is None or message.guild.id != FENZ_GUILD_ID:
            return

        # Check if message contains any configured emoji
        triggered_emoji = None
        for emoji_key in EMOJI_CONFIG.keys():
            # Check if emoji requires a prefix (starts with !)
            if emoji_key.startswith("!"):
                # Remove the ! to get the actual emoji
                actual_emoji = emoji_key[1:]
                # Check if message contains !emoji pattern
                if f"!{actual_emoji}" in message.content:
                    triggered_emoji = emoji_key
                    break
            else:
                # Normal emoji without prefix requirement
                if emoji_key in message.content:
                    triggered_emoji = emoji_key
                    break

        if triggered_emoji:
            try:
                # Delete the message first
                await message.delete()

                # Get the target user ID for this emoji
                target_user_id = EMOJI_CONFIG[triggered_emoji]

                # Get display emoji (remove ! prefix if present)
                display_emoji = triggered_emoji[1:] if triggered_emoji.startswith("!") else triggered_emoji

                # Send an ephemeral message with a button to the user
                view = InvisibleButtonView(display_emoji, target_user_id, message.channel, message.author)

                # Send a button that only the author can see
                # We send it to the channel but make it ephemeral-like by deleting quickly
                button_msg = await message.channel.send(
                    f"{message.author.mention}",
                    view=view,
                    delete_after=5  # Auto-delete after 5 seconds
                )

            except discord.Forbidden:
                # Bot doesn't have permission to delete messages
                print(f"Missing permissions to delete message in {message.channel.name}")
            except Exception as e:
                print(f"Error handling emoji trigger: {e}")


async def setup(bot):
    await bot.add_cog(EmojiTriggerCog(bot))