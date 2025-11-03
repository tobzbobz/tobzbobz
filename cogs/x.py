import discord
from discord.ext import commands
from discord import app_commands
import io
import contextlib
import traceback
import textwrap

# Bot owner ID - LOCKED TO YOU
OWNER_ID = 678475709257089057


class ExecCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._last_result = None

    @app_commands.command(name="x", description="Execute Python code (Owner only)")
    @app_commands.describe(code="The Python code to execute")
    async def execute_code(self, interaction: discord.Interaction, code: str):
        """Execute arbitrary Python code - OWNER ONLY"""

        # Lock to your user ID
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        # Create environment with useful variables
        env = {
            'bot': self.bot,
            'interaction': interaction,
            'guild': interaction.guild,
            'channel': interaction.channel,
            'user': interaction.user,
            'discord': discord,
            'commands': commands,
            '_': self._last_result,
        }

        # Clean up code block if present
        if code.startswith('```') and code.endswith('```'):
            code = '\n'.join(code.split('\n')[1:-1])
        else:
            code = code.strip('` \n')

        # Prepare code for execution
        stdout = io.StringIO()

        to_compile = f'async def func():\n{textwrap.indent(code, "  ")}'

        try:
            # Compile the code
            exec(to_compile, env)
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Compilation Error",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return

        func = env['func']

        try:
            # Execute the code and capture output
            with contextlib.redirect_stdout(stdout):
                ret = await func()

        except Exception as e:
            # Get full traceback
            value = stdout.getvalue()
            error_trace = ''.join(traceback.format_exception(type(e), e, e.__traceback__))

            error_embed = discord.Embed(
                title="❌ Execution Error",
                color=discord.Color.red()
            )

            if value:
                error_embed.add_field(
                    name="Output",
                    value=f"```\n{value[:1000]}\n```",
                    inline=False
                )

            error_embed.add_field(
                name="Error",
                value=f"```py\n{error_trace[:1000]}\n```",
                inline=False
            )

            await interaction.followup.send(embed=error_embed, ephemeral=True)
            return

        else:
            # Success - show output
            value = stdout.getvalue()

            # Store result for future use with '_'
            if ret is not None:
                self._last_result = ret

            success_embed = discord.Embed(
                title="✅ Execution Successful",
                color=discord.Color.green()
            )

            # Add stdout output if any
            if value:
                success_embed.add_field(
                    name="Output",
                    value=f"```\n{value[:1000]}\n```",
                    inline=False
                )

            # Add return value if any
            if ret is not None:
                ret_str = repr(ret)
                success_embed.add_field(
                    name="Return Value",
                    value=f"```py\n{ret_str[:1000]}\n```",
                    inline=False
                )

            # If nothing to show
            if not value and ret is None:
                success_embed.description = "*No output or return value*"

            await interaction.followup.send(embed=success_embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(ExecCog(bot))