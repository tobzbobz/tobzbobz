# Sending Messages
from discord import MessageSnapshot
/x code:
channel = bot.get_channel(1234567890)
await channel.send("Hello from /x!")


# Fixing Database Data
/x code:
from database import db
async with db.pool.acquire() as conn:
    # Fix a specific record
    await conn.execute(
        'UPDATE callsigns SET callsign = $1 WHERE discord_user_id = $2',
        "123", 678475709257089057
    )
    print("✅ Database updated!")

/x code:
from database import db
# Check what's in the database
async with db.pool.acquire() as conn:
    rows = await conn.fetch('SELECT * FROM active_watches')
    for row in rows:
        print(dict(row))


# Update Channel Names
/x code:
channel = bot.get_channel(1390867914462203914)
await channel.edit(name="「🔴1️⃣」active-watch")
print(f"✅ Renamed channel to: {channel.name}")

/x code:
# Update multiple channels at once
channels = [1234, 5678, 9012]  # channel IDs
for channel_id in channels:
    ch = bot.get_channel(channel_id)
    if ch:
        await ch.edit(name="new-name")
        print(f"✅ Updated {ch.name}")


# Delete Message
/x code:
channel = interaction.channel
async for msg in channel.history(limit=10):
    await msg.delete()
print("Deleted 10 messages")


# Give/Remove Roles
/x code:
member = guild.get_member(USER_ID)
role = guild.get_role(ROLE_ID)
await member.add_roles(role)
print(f"✅ Added role to {member.name}")



#Manually trigger watch embed update
/x code:
from cogs.watches import WatchCog
cog = bot.get_cog('WatchCog')
channel = bot.get_channel(1390867914462203914)
await cog.update_stats_embed(channel)
print("✅ Stats embed updated!")


# Check Active Watches
/x code:
from cogs.watches import active_watches
print(f"Active watches: {len(active_watches)}")
for msg_id, data in active_watches.items():
    print(f"{data['colour']} Watch at {data['station']} - by {data['user_name']}")


