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
import logging  # Add this line
from database import db
from datetime import datetime, timedelta
from typing import Any, Dict, List


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

    @py_group.command(name="cog", description="Manage bot cogs (load, unload, reload)")
    @app_commands.describe(
        mode="Choose the operation to perform",
        cog="The name of the cog (or select from dropdown)"
    )
    @app_commands.choices(mode=[
        app_commands.Choice(name="Toggle Cog (Load/Unload)", value="toggle"),
        app_commands.Choice(name="Reload Cog", value="reload"),
    ])
    async def py_cog(self, interaction: discord.Interaction, mode: app_commands.Choice[str], cog: str):
        """Manage cogs - load, unload, or reload"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            content=f"<a:Load:1430912797469970444> Processing {mode.name}",
            ephemeral=True
        )

        # Normalize cog name
        cog_name = f"cogs.{cog}" if not cog.startswith("cogs.") else cog

        try:
            if mode.value == "toggle":
                # Check if cog is loaded
                if cog_name in self.bot.extensions:
                    # Unload the cog
                    await self.bot.unload_extension(cog_name)
                    embed = discord.Embed(
                        title="<:Accepted:1426930333789585509> Cog Unloaded",
                        description=f"Successfully unloaded `{cog}`",
                        color=discord.Color.orange()
                    )
                else:
                    # Load the cog
                    await self.bot.load_extension(cog_name)
                    embed = discord.Embed(
                        title="<:Accepted:1426930333789585509> Cog Loaded",
                        description=f"Successfully loaded `{cog}`",
                        color=discord.Color.green()
                    )

            elif mode.value == "reload":
                # Reload the cog
                await self.bot.reload_extension(cog_name)
                embed = discord.Embed(
                    title="<:Accepted:1426930333789585509> Cog Reloaded",
                    description=f"Successfully reloaded `{cog}`",
                    color=discord.Color.green()
                )

        except commands.ExtensionNotLoaded:
            embed = discord.Embed(
                title="<:Warn:1437771973970104471> Not Loaded",
                description=f"Cog `{cog}` is not currently loaded.",
                color=discord.Color.orange()
            )
        except commands.ExtensionNotFound:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Not Found",
                description=f"Cog `{cog}` does not exist in the cogs directory.",
                color=discord.Color.red()
            )
        except commands.ExtensionAlreadyLoaded:
            embed = discord.Embed(
                title="<:Warn:1437771973970104471> Already Loaded",
                description=f"Cog `{cog}` is already loaded. Use 'Reload Cog' mode instead.",
                color=discord.Color.orange()
            )
        except Exception as e:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Operation Failed",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )

        # Add current status footer
        status = "Loaded" if cog_name in self.bot.extensions else "Unloaded"
        embed.set_footer(text=f"Current Status: {status} | Total Cogs: {len(self.bot.extensions)}")

        await interaction.delete_original_response()
        await interaction.followup.send(embed=embed, ephemeral=True)

    @py_cog.autocomplete('cog')
    async def cog_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> list[app_commands.Choice[str]]:
        """Autocomplete for cog names"""
        # Get all available cogs from the cogs directory
        import os
        cog_files = []

        # Get loaded cogs
        loaded_cogs = [ext.replace("cogs.", "") for ext in self.bot.extensions.keys()]

        # Try to get all cog files from directory
        try:
            cogs_path = "cogs"
            if os.path.exists(cogs_path):
                for file in os.listdir(cogs_path):
                    if file.endswith(".py") and not file.startswith("_"):
                        cog_name = file[:-3]  # Remove .py extension
                        if cog_name not in cog_files:
                            cog_files.append(cog_name)
        except Exception:
            pass

        # Combine loaded cogs with discovered files
        all_cogs = list(set(loaded_cogs + cog_files))

        # Filter based on current input
        filtered = [
            app_commands.Choice(
                name=f"{cog} {'✅' if cog in loaded_cogs else '❌'}",
                value=cog
            )
            for cog in all_cogs
            if current.lower() in cog.lower()
        ]

        # Return up to 25 choices (Discord limit)
        return filtered[:25]

    @py_group.command(name="logs", description="View bot logs with pagination")
    async def py_logs(self, interaction: discord.Interaction):
        """Display bot logs with navigation controls"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            content=f"<a:Load:1430912797469970444> Loading Logs",
            ephemeral=True
        )

        try:
            # Get logs from the logging handler
            # Assuming you have a log file or memory handler
            logs = []

            # Option 1: Read from log file
            try:
                with open('bot.log', 'r', encoding='utf-8') as f:
                    logs = f.readlines()
                    # Get last 100 logs
                    logs = logs[-100:]
            except FileNotFoundError:
                # Option 2: Get from root logger handlers
                for handler in logging.getLogger().handlers:
                    if isinstance(handler, logging.FileHandler):
                        try:
                            with open(handler.baseFilename, 'r', encoding='utf-8') as f:
                                logs = f.readlines()
                                logs = logs[-100:]
                            break
                        except Exception:
                            pass

            if not logs:
                # No logs found, create sample message
                logs = ["No logs available. Configure logging to view logs here."]

            # Clean up logs (remove trailing newlines)
            logs = [log.rstrip() for log in logs if log.strip()]

            view = LogsView(logs, interaction.user.id)

            embed = discord.Embed(
                title="Bot Logs",
                description=f"```\n{view.get_page_logs()}\n```",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            embed.set_footer(text=f"Page 1/{view.total_pages} • {len(logs)} total log entries")

            await interaction.delete_original_response()
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Error Loading Logs",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )
            await interaction.delete_original_response()
            await interaction.followup.send(embed=embed, ephemeral=True)

    async def get_all_caches(self) -> Dict[str, Any]:
        """Collect all caches from the bot and cogs"""
        caches = {}

        # Bot-level caches
        if hasattr(self.bot, 'cached_messages'):
            caches['bot.cached_messages'] = self.bot.cached_messages

        # Iterate through all cogs and find cache attributes
        for cog_name, cog in self.bot.cogs.items():
            for attr_name in dir(cog):
                # Skip private/magic attributes and methods
                if attr_name.startswith('_'):
                    continue

                attr = getattr(cog, attr_name, None)

                # Check if it's a cache-like attribute
                if attr_name.endswith('_cache') or attr_name.endswith('_caches'):
                    cache_key = f"{cog_name}.{attr_name}"
                    caches[cache_key] = attr
                elif isinstance(attr, dict) and 'cache' in attr_name.lower():
                    cache_key = f"{cog_name}.{attr_name}"
                    caches[cache_key] = attr

        return caches

    async def clear_cache(self, cache_name: str) -> tuple[bool, str]:
        """Clear a specific cache"""
        try:
            parts = cache_name.split('.', 1)

            if len(parts) == 1:
                # Bot-level cache
                if hasattr(self.bot, cache_name):
                    cache = getattr(self.bot, cache_name)
                    if isinstance(cache, dict):
                        cache.clear()
                    elif isinstance(cache, list):
                        cache.clear()
                    elif isinstance(cache, set):
                        cache.clear()
                    return True, f"Cleared bot cache: `{cache_name}`"
                return False, f"Cache not found: `{cache_name}`"

            elif len(parts) == 2:
                # Cog cache
                cog_name, attr_name = parts

                if cog_name not in self.bot.cogs:
                    return False, f"Cog not found: `{cog_name}`"

                cog = self.bot.cogs[cog_name]

                if not hasattr(cog, attr_name):
                    return False, f"Cache not found: `{cache_name}`"

                cache = getattr(cog, attr_name)

                if isinstance(cache, dict):
                    cache.clear()
                elif isinstance(cache, list):
                    cache.clear()
                elif isinstance(cache, set):
                    cache.clear()
                else:
                    return False, f"Cache type `{type(cache).__name__}` cannot be cleared"

                return True, f"Cleared cache: `{cache_name}`"

        except Exception as e:
            return False, f"Error clearing cache: `{e.__class__.__name__}: {e}`"

        return False, "Unknown error occurred"

    @py_group.command(name="cache", description="View and manage bot caches")
    @app_commands.describe(
        cache="Select a cache to view"
    )
    async def py_cache(self, interaction: discord.Interaction, cache: str):
        """View and manage bot caches"""
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This command is restricted to the bot owner only!",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            content=f"<a:Load:1430912797469970444> Loading Cache",
            ephemeral=True
        )

        try:
            # Get all caches
            all_caches = await self.get_all_caches()

            if cache not in all_caches:
                embed = discord.Embed(
                    title="<:Denied:1426930694633816248> Cache Not Found",
                    description=f"Cache `{cache}` does not exist.\n\n"
                                f"Available caches:\n" + "\n".join([f"• `{c}`" for c in list(all_caches.keys())[:10]]),
                    color=discord.Color.red()
                )
                await interaction.edit_original_response(content=None, embed=embed)
                return

            cache_data = all_caches[cache]
            view = CacheManagementView(self, cache, cache_data, interaction.user.id)
            embed = view.create_embed()

            await interaction.edit_original_response(content=None, embed=embed, view=view)

        except Exception as e:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Error Loading Cache",
                description=f"```py\n{e.__class__.__name__}: {e}\n```",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(content=None, embed=embed)

    @py_cache.autocomplete('cache')
    async def cache_autocomplete(
            self,
            interaction: discord.Interaction,
            current: str,
    ) -> list[app_commands.Choice[str]]:
        """Autocomplete for cache names"""
        try:
            all_caches = await self.get_all_caches()

            # Create choices with size info
            choices = []
            for cache_name, cache_data in all_caches.items():
                # Get size
                if isinstance(cache_data, (dict, list, set, tuple)):
                    size = len(cache_data)
                else:
                    size = 1

                # Get type
                cache_type = type(cache_data).__name__

                display_name = f"{cache_name} ({cache_type}, {size} items)"

                choices.append(
                    app_commands.Choice(
                        name=display_name[:100],  # Discord limit
                        value=cache_name
                    )
                )

            # Filter by current input
            if current:
                choices = [
                    choice for choice in choices
                    if current.lower() in choice.value.lower()
                ]

            # Sort by name
            choices.sort(key=lambda c: c.value)

            return choices[:25]  # Discord limit

        except Exception as e:
            print(f"Cache autocomplete error: {e}")
            return []


class PageInput(discord.ui.Modal, title="Go to Page"):
    page = discord.ui.TextInput(
        label="Page Number",
        style=discord.TextStyle.short,
        placeholder="Enter page number...",
        required=True,
        max_length=10
    )

    def __init__(self, view):
        super().__init__()
        self.view = view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            page_num = int(str(self.page))
            if 1 <= page_num <= self.view.total_pages:
                self.view.current_page = page_num
                await self.view.update_message(interaction)
            else:
                await interaction.response.send_message(
                    f"<:Denied:1426930694633816248> Invalid page number. Please enter a number between 1 and {self.view.total_pages}",
                    ephemeral=True
                )
        except ValueError:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> Please enter a valid number",
                ephemeral=True
            )


class LogsView(discord.ui.View):
    def __init__(self, logs: list[str], user_id: int):
        super().__init__(timeout=300)
        self.logs = logs
        self.user_id = user_id
        self.logs_per_page = 10
        self.total_pages = max(1, (len(logs) + self.logs_per_page - 1) // self.logs_per_page)
        self.current_page = 1
        self.update_buttons()

    def update_buttons(self):
        # Update button states
        self.first_button.disabled = self.current_page == 1
        self.prev_button.disabled = self.current_page == 1
        self.next_button.disabled = self.current_page == self.total_pages
        self.last_button.disabled = self.current_page == self.total_pages

        # Update page button label
        self.page_button.label = f"{self.current_page}/{self.total_pages}"

    def get_page_logs(self) -> str:
        start_idx = (self.current_page - 1) * self.logs_per_page
        end_idx = start_idx + self.logs_per_page
        page_logs = self.logs[start_idx:end_idx]

        if not page_logs:
            return "No logs available."

        return "\n".join(page_logs)

    async def update_message(self, interaction: discord.Interaction):
        self.update_buttons()
        embed = discord.Embed(
            title="Bot Logs",
            description=f"```\n{self.get_page_logs()}\n```",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Page {self.current_page}/{self.total_pages} • {len(self.logs)} total log entries")

        await interaction.response.edit_message(embed=embed, view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You cannot control this navigation menu.",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(emoji="<:LeftSkip:1434962162064822343>️", style=discord.ButtonStyle.primary, custom_id="first")
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 1
        await self.update_message(interaction)

    @discord.ui.button(emoji="<:LeftArrow:1434962165215002777>️", style=discord.ButtonStyle.primary, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(1, self.current_page - 1)
        await self.update_message(interaction)

    @discord.ui.button(label="1/1", style=discord.ButtonStyle.secondary, custom_id="page")
    async def page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = PageInput(self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(emoji="<:RightSkip:1434962167660281926>", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(self.total_pages, self.current_page + 1)
        await self.update_message(interaction)

    @discord.ui.button(emoji="<:RightArrow:1434962170147246120>️", style=discord.ButtonStyle.primary, custom_id="last")
    async def last_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = self.total_pages
        await self.update_message(interaction)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class CacheViewModal(discord.ui.Modal, title="View Cache Contents"):
    def __init__(self, cache_name: str, cache_data: Any):
        super().__init__()
        self.cache_name = cache_name
        self.cache_data = cache_data

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()


class CacheManagementView(discord.ui.View):
    def __init__(self, cog, cache_name: str, cache_data: Any, user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.cache_name = cache_name
        self.cache_data = cache_data
        self.user_id = user_id
        self.current_page = 0
        self.items_per_page = 10

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> You cannot control this cache menu.",
                ephemeral=True
            )
            return False
        return True

    def get_cache_items(self) -> List[tuple]:
        """Convert cache data to list of items"""
        if isinstance(self.cache_data, dict):
            return list(self.cache_data.items())
        elif isinstance(self.cache_data, (list, set, tuple)):
            return list(enumerate(self.cache_data))
        else:
            return [("value", self.cache_data)]

    def get_total_pages(self) -> int:
        items = self.get_cache_items()
        return max(1, (len(items) + self.items_per_page - 1) // self.items_per_page)

    def format_value(self, value: Any, max_length: int = 100) -> str:
        """Format a value for display"""
        if isinstance(value, (dict, list, set, tuple)):
            formatted = repr(value)
            if len(formatted) > max_length:
                return formatted[:max_length] + "..."
            return formatted
        elif isinstance(value, (datetime, timedelta)):
            return str(value)
        else:
            formatted = str(value)
            if len(formatted) > max_length:
                return formatted[:max_length] + "..."
            return formatted

    def create_embed(self) -> discord.Embed:
        """Create the cache display embed"""
        items = self.get_cache_items()
        total_pages = self.get_total_pages()

        start_idx = self.current_page * self.items_per_page
        end_idx = min(start_idx + self.items_per_page, len(items))
        page_items = items[start_idx:end_idx]

        embed = discord.Embed(
            title=f"Cache: {self.cache_name}",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )

        # Cache overview
        cache_type = type(self.cache_data).__name__
        cache_size = len(items)

        embed.description = f"**Type:** `{cache_type}`\n**Total Items:** `{cache_size}`"

        # Display items
        if page_items:
            items_text = []
            for key, value in page_items:
                key_str = self.format_value(key, 50)
                value_str = self.format_value(value, 80)
                items_text.append(f"**{key_str}**\n└─ `{value_str}`")

            embed.add_field(
                name=f"Items ({start_idx + 1}-{end_idx} of {cache_size})",
                value="\n\n".join(items_text)[:1024],
                inline=False
            )
        else:
            embed.add_field(
                name="Items",
                value="*Cache is empty*",
                inline=False
            )

        embed.set_footer(text=f"Page {self.current_page + 1}/{total_pages}")

        return embed

    async def update_display(self, interaction: discord.Interaction):
        """Update the message with current page"""
        embed = self.create_embed()

        # Update button states
        total_pages = self.get_total_pages()
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == "first" or item.custom_id == "prev":
                    item.disabled = self.current_page == 0
                elif item.custom_id == "next" or item.custom_id == "last":
                    item.disabled = self.current_page >= total_pages - 1

        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(emoji="<:LeftSkip:1434962162064822343>", style=discord.ButtonStyle.primary, custom_id="first")
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 0
        await self.update_display(interaction)

    @discord.ui.button(emoji="<:LeftArrow:1434962165215002777>", style=discord.ButtonStyle.primary, custom_id="prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        await self.update_display(interaction)

    @discord.ui.button(label="Clear Cache", emoji="<:Wipe:1434954284851658762>", style=discord.ButtonStyle.danger, custom_id="clear")
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Show confirmation
        confirm_view = CacheClearConfirmView(self.cog, self.cache_name, self.user_id)

        embed = discord.Embed(
            title="<:Warn:1437771973970104471>️ Confirm Cache Clear",
            description=f"Are you sure you want to clear the **{self.cache_name}** cache?\n\n"
                        f"This will remove all {len(self.get_cache_items())} items.",
            color=discord.Color.red()
        )

        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)

    @discord.ui.button(emoji="<:RightArrow:1434962170147246120>", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        total_pages = self.get_total_pages()
        self.current_page = min(total_pages - 1, self.current_page + 1)
        await self.update_display(interaction)

    @discord.ui.button(emoji="<:RightSkip:1434962167660281926>", style=discord.ButtonStyle.primary, custom_id="last")
    async def last_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = self.get_total_pages() - 1
        await self.update_display(interaction)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class CacheClearConfirmView(discord.ui.View):
    def __init__(self, cog, cache_name: str, user_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.cache_name = cache_name
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "<:Denied:1426930694633816248> This is not your confirmation!",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Confirm Clear", style=discord.ButtonStyle.danger, emoji="<:Accepted:1426930333789585509>")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        success, message = await self.cog.clear_cache(self.cache_name)

        if success:
            embed = discord.Embed(
                title="<:Accepted:1426930333789585509> Cache Cleared",
                description=message,
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="<:Denied:1426930694633816248> Clear Failed",
                description=message,
                color=discord.Color.red()
            )

        # Disable buttons
        for item in self.children:
            item.disabled = True

        await interaction.edit_original_response(embed=embed, view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="<:Denied:1426930694633816248>")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="Cancelled",
            description="Cache clear operation cancelled.",
            color=discord.Color.orange()
        )

        for item in self.children:
            item.disabled = True

        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


async def setup(bot):
    await bot.add_cog(JishakuCog(bot))