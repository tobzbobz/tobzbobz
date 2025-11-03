import discord
from discord.ext import commands
from discord import app_commands
import io, contextlib, traceback, textwrap
from database import db  # ✅ automatically available
import sys

OWNER_ID = 678475709257089057


class CodeInput(discord.ui.Modal, title="Execute Python Code"):
    code = discord.ui.TextInput(
        label="Python Code",
        style=discord.TextStyle.paragraph,
        placeholder="Enter your Python code here...",
        required=True,
        max_length=4000
    )

    def __init__(self, cog, interaction):
        super().__init__()
        self.cog = cog
        self.interaction = interaction

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.run_code(interaction, str(self.code))


class ExecCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._last_result = None

    @app_commands.command(name="x", description="Execute Python code (Owner only, multiline supported)")
    async def execute_code(self, interaction: discord.Interaction):
        """Open a modal to execute arbitrary Python code"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        modal = CodeInput(self, interaction)
        await interaction.response.send_modal(modal)

    async def run_code(self, interaction: discord.Interaction, code: str):
        await interaction.response.defer(ephemeral=True)

        # ✅ Preload useful globals automatically
        env = {
            'bot': self.bot,
            'interaction': interaction,
            'guild': interaction.guild,
            'channel': interaction.channel,
            'user': interaction.user,
            'discord': discord,
            'commands': commands,
            'db': db,  # <-- now included automatically
            '_': self._last_result,
        }

        stdout = io.StringIO()

        # Clean up code (remove ``` wrappers, strip spaces)
        if code.startswith("```") and code.endswith("```"):
            code = "\n".join(code.split("\n")[1:-1])
        code = code.strip()

        # Wrap in async def for await support
        to_compile = f"async def func():\n{textwrap.indent(code, '    ')}"

        try:
            exec(to_compile, env)
        except Exception as e:
            embed = discord.Embed(
                title="❌ Compilation Error",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        func = env["func"]

        try:
            with contextlib.redirect_stdout(stdout):
                ret = await func()
        except Exception as e:
            output = stdout.getvalue()
            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))

            embed = discord.Embed(title="❌ Execution Error", color=discord.Color.red())
            if output:
                embed.add_field(name="Output", value=f"```\n{output[:1000]}\n```", inline=False)
            embed.add_field(name="Error", value=f"```py\n{tb[:1000]}\n```", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        output = stdout.getvalue()
        self._last_result = ret

        embed = discord.Embed(title="✅ Execution Successful", color=discord.Color.green())
        if output:
            embed.add_field(name="Output", value=f"```\n{output[:1000]}\n```", inline=False)
        if ret is not None:
            embed.add_field(name="Return Value", value=f"```py\n{repr(ret)[:1000]}\n```", inline=False)
        if not output and ret is None:
            embed.description = "*No output or return value*"

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="xsync", description="Sync database changes to the bot without restarting (Owner only)")
    async def reload_database(self, interaction: discord.Interaction):
        """Reload bot data and resync caches from the database."""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        import inspect
        reloaded = []
        failed = []

        # ✅ Iterate through all cogs and call reload/load/sync functions if present
        for name, cog in self.bot.cogs.items():
            for method_name in ("reload_data", "load_data", "refresh_cache", "sync_db"):
                method = getattr(cog, method_name, None)
                if method and inspect.iscoroutinefunction(method):
                    try:
                        await method()
                        reloaded.append(f"{name}.{method_name}()")
                    except Exception as e:
                        failed.append(f"{name}.{method_name}() → {e.__class__.__name__}: {e}")

        # ✅ Optional: reload global bot-level data
        if hasattr(self.bot, "reload_data") and inspect.iscoroutinefunction(self.bot.reload_data):
            try:
                await self.bot.reload_data()
                reloaded.append("bot.reload_data()")
            except Exception as e:
                failed.append(f"bot.reload_data() → {e.__class__.__name__}: {e}")

        embed = discord.Embed(
            title="🔄 Database Sync Complete",
            color=discord.Color.green() if not failed else discord.Color.orange()
        )

        if reloaded:
            embed.add_field(name="✅ Reloaded", value="\n".join(reloaded)[:1000], inline=False)
        if failed:
            embed.add_field(name="⚠️ Failed", value="\n".join(failed)[:1000], inline=False)
        if not reloaded and not failed:
            embed.description = "No reloadable methods found."

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(ExecCog(bot))