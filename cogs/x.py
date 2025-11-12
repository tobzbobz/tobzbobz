import discord
from discord.ext import commands
from discord import app_commands
import io
import contextlib
import traceback
import textwrap
import subprocess
import asyncio
import time
import psutil
import sys
import gc
from database import db
from datetime import datetime, timedelta

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


class ShellInput(discord.ui.Modal, title="Execute Shell Command"):
    command = discord.ui.TextInput(
        label="Shell Command",
        style=discord.TextStyle.short,
        placeholder="curl http://https://tobzbobz.onrender.com:8080/logs?page=2&size=50",
        required=True,
        max_length=500
    )

    def __init__(self, cog, interaction):
        super().__init__()
        self.cog = cog
        self.interaction = interaction

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.run_shell(interaction, str(self.command))


class JishakuCog(commands.Cog):
    """Jishaku-style debug and diagnostic commands"""

    py_group = app_commands.Group(name="py", description="Debug and diagnostic commands (Owner only)")

    def __init__(self, bot):
        self.bot = bot
        self._last_result = None
        self.start_time = time.time()

    def format_uptime(self) -> str:
        """Format bot uptime"""
        uptime_seconds = time.time() - self.start_time
        uptime_delta = timedelta(seconds=int(uptime_seconds))

        days = uptime_delta.days
        hours, remainder = divmod(uptime_delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{seconds}s")

        return " ".join(parts)

    @py_group.command(name="eval", description="Execute arbitrary Python code")
    async def py_eval(self, interaction: discord.Interaction):
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
        """Execute Python code with bot context"""
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Running Code",
                                                ephemeral=True)

        # Preload useful globals
        env = {
            'bot': self.bot,
            'interaction': interaction,
            'guild': interaction.guild,
            'channel': interaction.channel,
            'user': interaction.user,
            'discord': discord,
            'commands': commands,
            'db': db,
            'asyncio': asyncio,
            'subprocess': subprocess,
            'psutil': psutil,
            '_': self._last_result,
        }

        stdout = io.StringIO()

        # Clean up code
        if code.startswith("```") and code.endswith("```"):
            code = "\n".join(code.split("\n")[1:-1])
        code = code.strip()

        # Wrap in async def
        to_compile = f"async def func():\n{textwrap.indent(code, '    ')}"

        try:
            exec(to_compile, env)
        except Exception as e:
            embed = discord.Embed(
                title="<:Warn:1437771973970104471> Compilation Error",
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

            embed = discord.Embed(title="<:Denied:1426930694633816248> Execution Error", color=discord.Color.red())
            if output:
                embed.add_field(name="Output", value=f"```\n{output[:1000]}\n```", inline=False)
            embed.add_field(name="Error", value=f"```py\n{tb[:1000]}\n```", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        output = stdout.getvalue()
        self._last_result = ret

        embed = discord.Embed(title="<:Accepted:1426930333789585509> Execution Successful", color=discord.Color.green())
        if output:
            embed.add_field(name="Output", value=f"```\n{output[:1000]}\n```", inline=False)
        if ret is not None:
            embed.add_field(name="Return Value", value=f"```py\n{repr(ret)[:1000]}\n```", inline=False)
        if not output and ret is None:
            embed.description = "*No output or return value*"

        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @py_group.command(name="shell", description="Execute a shell command on the bot host")
    async def py_shell(self, interaction: discord.Interaction):
        """Open a modal to execute shell commands"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        modal = ShellInput(self, interaction)
        await interaction.response.send_modal(modal)

    async def run_shell(self, interaction: discord.Interaction, command: str):
        """Execute a shell command"""
        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Running Shell",
                                                ephemeral=True)

        try:
            # Run command with timeout
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30.0)
            except asyncio.TimeoutError:
                process.kill()
                await interaction.followup.send(
                    embed=discord.Embed(
                        title="<:Alert:1437790206462922803> Command Timeout",
                        description="Command execution exceeded 30 seconds and was killed.",
                        color=discord.Color.red()
                    ),
                    ephemeral=True
                )
                return

            stdout_text = stdout.decode('utf-8') if stdout else ""
            stderr_text = stderr.decode('utf-8') if stderr else ""

            embed = discord.Embed(
                title=f"🖥️ Shell Command: `{command}`",
                color=discord.Color.green() if process.returncode == 0 else discord.Color.red()
            )

            embed.add_field(name="Return Code", value=f"`{process.returncode}`", inline=True)

            if stdout_text:
                embed.add_field(
                    name="📤 stdout",
                    value=f"```\n{stdout_text[:1000]}\n```",
                    inline=False
                )

            if stderr_text:
                embed.add_field(
                    name="📛 stderr",
                    value=f"```\n{stderr_text[:1000]}\n```",
                    inline=False
                )

            if not stdout_text and not stderr_text:
                embed.description = "*No output*"

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Shell Execution Error",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )
            await interaction.delete_original_response()
            await interaction.followup.send(embed=embed, ephemeral=True)

    @py_group.command(name="stats", description="Show runtime diagnostics and cache information")
    async def py_stats(self, interaction: discord.Interaction):
        """Display bot statistics and system information"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Collecting Stats",
                                                ephemeral=True)

        # System stats
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024

        # CPU usage (non-blocking)
        cpu_percent = process.cpu_percent(interval=0.1)

        # Discord stats
        total_members = sum(guild.member_count for guild in self.bot.guilds)

        # Cache stats
        cached_messages = len(self.bot.cached_messages) if hasattr(self.bot, 'cached_messages') else 0

        # Database connection info
        db_status = "🟢 Connected" if db.pool else "🔴 Disconnected"
        db_pool_size = len(db.pool._holders) if db.pool and hasattr(db.pool, '_holders') else 0

        embed = discord.Embed(
            title="📊 Bot Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )

        # Bot info
        embed.add_field(
            name="🤖 Bot Info",
            value=f"**Uptime:** {self.format_uptime()}\n"
                  f"**Latency:** {round(self.bot.latency * 1000)}ms\n"
                  f"**Python:** {sys.version.split()[0]}",
            inline=True
        )

        # Guild/User stats
        embed.add_field(
            name="🌐 Discord Stats",
            value=f"**Guilds:** {len(self.bot.guilds)}\n"
                  f"**Users:** {total_members:,}\n"
                  f"**Cached Messages:** {cached_messages}",
            inline=True
        )

        # System resources
        embed.add_field(
            name="💻 System Resources",
            value=f"**Memory:** {memory_mb:.2f} MB\n"
                  f"**CPU:** {cpu_percent:.1f}%\n"
                  f"**Threads:** {process.num_threads()}",
            inline=True
        )

        # Cache info
        embed.add_field(
            name="🗂️ Cache Stats",
            value=f"**Guilds:** {len(self.bot.guilds)}\n"
                  f"**Users:** {len(self.bot.users)}\n"
                  f"**Emojis:** {len(self.bot.emojis)}",
            inline=True
        )

        # Database info
        embed.add_field(
            name="🗄️ Database",
            value=f"**Status:** {db_status}\n"
                  f"**Pool Size:** {db_pool_size}\n"
                  f"**Type:** PostgreSQL",
            inline=True
        )

        # Cog info
        embed.add_field(
            name="🧩 Loaded Cogs",
            value=f"**Count:** {len(self.bot.cogs)}\n"
                  f"**Commands:** {len(self.bot.tree.get_commands())}",
            inline=True
        )

        embed.set_footer(text=f"Process ID: {process.pid}")
        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @py_group.command(name="tasks", description="List active event loop handles and requests")
    async def py_tasks(self, interaction: discord.Interaction):
        """Display all active asyncio tasks"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Collecting Tasks",
                                                ephemeral=True)

        # Get all tasks
        all_tasks = asyncio.all_tasks()
        current_task = asyncio.current_task()

        embed = discord.Embed(
            title="🔁 Active Tasks",
            description=f"**Total Tasks:** {len(all_tasks)}\n"
                        f"**Current Task:** `{current_task.get_name() if current_task else 'Unknown'}`",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )

        # Categorize tasks
        running_tasks = []
        pending_tasks = []
        done_tasks = []

        for task in all_tasks:
            task_name = task.get_name()
            coro_name = task.get_coro().__qualname__ if hasattr(task.get_coro(), '__qualname__') else str(
                task.get_coro())

            if task.done():
                done_tasks.append(f"<:Accepted:1426930333789585509> `{task_name}` - {coro_name}")
            elif task == current_task:
                running_tasks.append(f"▶️ `{task_name}` - {coro_name} (current)")
            else:
                pending_tasks.append(f"⏳ `{task_name}` - {coro_name}")

        # Show running tasks (limit to 10)
        if running_tasks or pending_tasks:
            active_list = (running_tasks + pending_tasks)[:10]
            embed.add_field(
                name=f"🏃 Active Tasks ({len(running_tasks) + len(pending_tasks)})",
                value="\n".join(active_list) if active_list else "*None*",
                inline=False
            )
            if len(running_tasks) + len(pending_tasks) > 10:
                embed.add_field(
                    name="➕ More",
                    value=f"*...and {len(running_tasks) + len(pending_tasks) - 10} more tasks*",
                    inline=False
                )

        # Show completed tasks (limit to 5)
        if done_tasks:
            embed.add_field(
                name=f"<:Accepted:1426930333789585509> Completed Tasks ({len(done_tasks)})",
                value="\n".join(done_tasks[:5]) if done_tasks else "*None*",
                inline=False
            )
            if len(done_tasks) > 5:
                embed.add_field(
                    name="➕ More",
                    value=f"*...and {len(done_tasks) - 5} more completed tasks*",
                    inline=False
                )

        # Event loop info
        loop = asyncio.get_event_loop()
        embed.add_field(
            name="🔄 Event Loop",
            value=f"**Running:** {'Yes' if loop.is_running() else 'No'}\n"
                  f"**Closed:** {'Yes' if loop.is_closed() else 'No'}",
            inline=True
        )
        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @py_group.command(name="sync", description="Sync database changes to the bot without restarting")
    async def py_sync(self, interaction: discord.Interaction):
        """Reload bot data and resync caches from the database"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Sycning",
                                                ephemeral=True)

        import inspect
        reloaded = []
        failed = []

        # Iterate through all cogs and call reload/load/sync functions if present
        for name, cog in self.bot.cogs.items():
            for method_name in ("reload_data", "load_data", "refresh_cache", "sync_db"):
                method = getattr(cog, method_name, None)
                if method and inspect.iscoroutinefunction(method):
                    try:
                        await method()
                        reloaded.append(f"<:Accepted:1426930333789585509> {name}.{method_name}()")
                    except Exception as e:
                        failed.append(f"<:Denied:1426930694633816248> {name}.{method_name}() → {e.__class__.__name__}: {e}")

        # Optional: reload global bot-level data
        if hasattr(self.bot, "reload_data") and inspect.iscoroutinefunction(self.bot.reload_data):
            try:
                await self.bot.reload_data()
                reloaded.append("<:Accepted:1426930333789585509> bot.reload_data()")
            except Exception as e:
                failed.append(f"<:Denied:1426930694633816248> bot.reload_data() → {e.__class__.__name__}: {e}")

        # Force garbage collection
        collected = gc.collect()

        embed = discord.Embed(
            title="🔄 Database Sync Complete",
            color=discord.Color.green() if not failed else discord.Color.orange(),
            timestamp=datetime.utcnow()
        )

        if reloaded:
            embed.add_field(name="<:Accepted:1426930333789585509> Reloaded", value="\n".join(reloaded)[:1000], inline=False)
        if failed:
            embed.add_field(name="<:Warn:1437771973970104471> Failed", value="\n".join(failed)[:1000], inline=False)
        if not reloaded and not failed:
            embed.description = "No reloadable methods found."

        embed.set_footer(text=f"Garbage collected: {collected} objects")
        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @py_group.command(name="reload", description="Reload a specific cog")
    @app_commands.describe(cog="The name of the cog to reload")
    async def py_reload(self, interaction: discord.Interaction, cog: str):
        """Reload a specific cog"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(content=f"<a:Load:1430912797469970444> Reloading",
                                                ephemeral=True)

        try:
            # Reload the cog
            await self.bot.reload_extension(f"cogs.{cog}" if not cog.startswith("cogs.") else cog)

            embed = discord.Embed(
                title="<:Accepted:1426930333789585509> Cog Reloaded",
                description=f"Successfully reloaded `{cog}`",
                color=discord.Color.green()
            )
        except commands.ExtensionNotLoaded:
            embed = discord.Embed(
                title="<:Warn:1437771973970104471> Not Loaded",
                description=f"Cog `{cog}` is not currently loaded. Use `/py load` instead.",
                color=discord.Color.orange()
            )
        except commands.ExtensionNotFound:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Not Found",
                description=f"Cog `{cog}` does not exist.",
                color=discord.Color.red()
            )
        except Exception as e:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Reload Failed",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )
        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(JishakuCog(bot))