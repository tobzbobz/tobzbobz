import discord
from discord.ext import commands
from dotenv import load_dotenv

intents = discord.Intents.all()

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Bot is ready! Logged in as {bot.user}")

bot.run('MTQyNjI0ODU0MzcwNjAyNjAyNg.GKoTQS.XNHUH09EPCOQJEHSfIrA0IJ4-8bp7PAccEXdD8')