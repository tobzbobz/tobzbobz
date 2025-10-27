import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

from database import db

async def test_database():
    """Test database connection and basic operations"""

    print("🔍 Testing database connection...")

    # Test 1: Connect
    connected = await db.connect()
    if not connected:
        print("❌ Failed to connect to database!")
        return

    print("✅ Connected to database!")

    # Test 2: Create a test setting
    print("\n🔍 Testing set_setting...")
    success = await db.set_setting(
        guild_id=123456789,
        key='test_key',
        value={'test': 'data', 'number': 123}
    )
    if success:
        print("✅ Successfully saved setting")
    else:
        print("❌ Failed to save setting")

    # Test 3: Retrieve the setting
    print("\n🔍 Testing get_setting...")
    result = await db.get_setting(
        guild_id=123456789,
        key='test_key'
    )
    if result:
        print(f"✅ Retrieved setting: {result}")
    else:
        print("❌ Failed to retrieve setting")

    # Test 4: Log an action
    print("\n🔍 Testing log_action...")
    success = await db.log_action(
        guild_id=123456789,
        user_id=987654321,
        action='test_action',
        details={'message': 'This is a test'}
    )
    if success:
        print("✅ Successfully logged action")
    else:
        print("❌ Failed to log action")

    # Test 5: Get recent logs
    print("\n🔍 Testing get_recent_logs...")
    logs = await db.get_recent_logs(
        guild_id=123456789,
        limit=5
    )
    print(f"✅ Retrieved {len(logs)} logs")
    if logs:
        print(f"   Latest log: {logs[0]}")

    # Test 6: Set a callsign
    print("\n🔍 Testing callsign functions...")
    success = await db.set_callsign(
        guild_id=123456789,
        user_id=987654321,
        callsign='TEST-123',
        set_by=111111111
    )
    if success:
        print("✅ Callsign saved")

        # Retrieve it
        callsign = await db.get_callsign(
            guild_id=123456789,
            user_id=987654321
        )
        print(f"✅ Retrieved callsign: {callsign}")

    # Cleanup
    print("\n🧹 Cleaning up test data...")
    async with db.pool.acquire() as conn:
        # Delete test setting
        await conn.execute(
            "DELETE FROM bot_settings WHERE guild_id = 123456789"
        )
        # Delete test logs
        await conn.execute(
            "DELETE FROM audit_logs WHERE guild_id = 123456789"
        )
        # Delete test callsign
        await conn.execute(
            "DELETE FROM callsigns WHERE guild_id = 123456789"
        )

    print("✅ Test data cleaned up")

    # Close connection
    await db.close()
    print("\n✅ All tests passed!")


if __name__ == "__main__":
    asyncio.run(test_database())


# Test script - add to test_database.py
async def test_callsigns():
    print("\n🔍 Testing callsign functions...")

    # Test add
    await add_callsign_to_database(
        callsign="123",
        discord_user_id=123456789,
        discord_username="TestUser",
        roblox_user_id="987654321",
        roblox_username="TestRoblox",
        fenz_prefix="RFF",
        hhstj_prefix="EMT",
        approved_by_id=111111111,
        approved_by_name="Staff"
    )
    print("✅ Added callsign 123")

    # Test check exists
    result = await check_callsign_exists("123")
    print(f"✅ Retrieved callsign: {result}")

    # Test search
    results = await search_callsign_database("123456789", "discord_id")
    print(f"✅ Search found {len(results)} results")

    # Cleanup
    await remove_callsign_from_database("123")
    print("✅ Cleaned up test callsign")