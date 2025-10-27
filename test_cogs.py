import asyncio
import os
from dotenv import load_dotenv

# ✅ CRITICAL: Load .env BEFORE importing anything that uses it
load_dotenv()

# Verify it loaded
db_url = os.getenv('DATABASE_URL')
if not db_url:
    print("❌ FATAL: DATABASE_URL not found after load_dotenv()")
    exit(1)

print(f"✅ DATABASE_URL loaded: {db_url[:30]}...{db_url[-20:]}\n")

# NOW import everything else
import discord
from discord.ext import commands
from database import db


async def test_cog(cog_name: str):
    """Test a single cog"""
    print(f"\n{'=' * 50}")
    print(f"Testing {cog_name}")
    print(f"{'=' * 50}\n")

    # Create a minimal bot
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True
    bot = commands.Bot(command_prefix='!', intents=intents)

    # Connect to database
    print("🔌 Connecting to database...")
    connected = await db.connect()

    if not connected:
        print("❌ Database connection failed!")
        return False

    print("✅ Connected to database")
    print(f"✅ Pool status: {db.pool is not None}")
    print()

    # Try to load the cog
    try:
        await bot.load_extension(f'cogs.{cog_name}')
        print(f"✅ Successfully loaded {cog_name}")

        # Unload it
        await bot.unload_extension(f'cogs.{cog_name}')
        print(f"✅ Successfully unloaded {cog_name}")

        return True

    except Exception as e:
        print(f"❌ Error with {cog_name}:")
        print(f"   {type(e).__name__}: {e}")

        # Show detailed traceback
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()

        return False

    finally:
        await db.close()


async def test_all_cogs():
    """Test all cogs"""
    cogs_to_test = [
        'status',
        'ping',
        'callsign',
        '!mod',
        # Add other cog names here
    ]

    results = {}

    print("=" * 50)
    print("STARTING COG TESTS")
    print("=" * 50)

    for cog_name in cogs_to_test:
        results[cog_name] = await test_cog(cog_name)
        await asyncio.sleep(1)  # Small delay between tests

    # Print summary
    print(f"\n{'=' * 50}")
    print("TEST SUMMARY")
    print(f"{'=' * 50}\n")

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for cog_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {cog_name}")

    print(f"\n{passed}/{total} cogs passed")

    if passed == total:
        print("\n🎉 All tests passed!")
        return True
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_all_cogs())
    exit(0 if success else 1)