import discord
from discord import app_commands
from discord.ext import commands
import aiohttp
from dotenv import load_dotenv
import os
load_dotenv()

class ImageFetcher(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.owner_id = 678475709257089057  # Your Discord user ID
        self.user_id = os.getenv("USER_ID")
        self.api_key = os.getenv("API_KEY")

    @app_commands.command(name="other", description="Fetches any number of random images. Locked to the bot owner.")
    async def fetch_image(self, interaction: discord.Interaction, number: int = 1):
        # Check if user is allowed
        if interaction.user.id != self.owner_id:
            return await interaction.response.send_message("You don’t have permission to use this command <:Denied:1426930694633816248>", ephemeral=True)

        await interaction.response.defer(thinking=True)

        async with aiohttp.ClientSession() as session:
            for _ in range(number):
                async with session.get(f"https://api.rule34.xxx/=&api_key={self.api_key}&user_id={self.user_id}") as response:
                    if response.status != 200:
                        return await interaction.followup.send("Failed to fetch image.")
                    data = await response.json()
                    image_url = data.get("message")

                    async with session.get(url) as response:
                        print(f"Status: {response.status}")
                        text = await response.text()
                        print(f"Response text: {text}")  # 👈 Add this temporarily

                        if response.status != 200:
                            return await interaction.followup.send(
                                "Failed to fetch image. Check the console for details <:Denied:1426930694633816248>")

                        data = await response.json()

                    embed = discord.Embed(title="Random Image", color=discord.Color.blurple())
                    embed.set_image(url=image_url)
                    await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(ImageFetcher(bot))
