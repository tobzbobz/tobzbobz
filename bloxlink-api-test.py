import aiohttp
import asyncio
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()

BLOXLINK_API_KEY = os.getenv('BLOXLINK_API_KEY')
TEST_DISCORD_ID = 678475709257089057
TEST_GUILD_ID = 1282916959062851634


async def test_bloxlink():
    print("=" * 60)
    print("BLOXLINK API TEST & QUOTA TRACKER")
    print("=" * 60)

    if not BLOXLINK_API_KEY:
        print("❌ BLOXLINK_API_KEY not found in .env file!")
        return

    print(f"✅ API Key loaded: {len(BLOXLINK_API_KEY)} characters")
    print(f"   First 10 chars: {BLOXLINK_API_KEY[:10]}...")
    print(f"   Last 4 chars: ...{BLOXLINK_API_KEY[-4:]}")

    url = f"https://api.blox.link/v4/public/guilds/{TEST_GUILD_ID}/discord-to-roblox/{TEST_DISCORD_ID}"

    print(f"\n🔡 Testing API endpoint:")
    print(f"   URL: {url}")

    headers = {
        'Authorization': BLOXLINK_API_KEY,
        'User-Agent': 'HNZRP-Test/1.0'
    }

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                print(f"\n📊 Response:")
                print(f"   Status: {response.status}")

                # ✅ ENHANCED: Extract ALL rate limit headers
                rate_limit_remaining = response.headers.get('X-RateLimit-Remaining')
                rate_limit_reset = response.headers.get('X-RateLimit-Reset')
                rate_limit_limit = response.headers.get('X-RateLimit-Limit')

                quota_info = {}

                if rate_limit_remaining:
                    quota_info['remaining'] = int(rate_limit_remaining)
                    print(f"   📈 Rate Limit Remaining: {rate_limit_remaining}")

                if rate_limit_limit:
                    quota_info['limit'] = int(rate_limit_limit)
                    print(f"   📊 Rate Limit Max: {rate_limit_limit}")

                if rate_limit_reset:
                    try:
                        reset_timestamp = int(rate_limit_reset)
                        reset_time = datetime.fromtimestamp(reset_timestamp)
                        time_until_reset = reset_time - datetime.now()

                        quota_info['reset_time'] = reset_time
                        quota_info['time_until_reset'] = time_until_reset

                        hours = int(time_until_reset.total_seconds() // 3600)
                        minutes = int((time_until_reset.total_seconds() % 3600) // 60)
                        print(f"   ⏰ Rate Limit Resets In: {hours}h {minutes}m")
                        print(f"   📅 Reset Time: {reset_time.strftime('%Y-%m-%d %I:%M:%S %p')}")
                    except:
                        print(f"   ⏰ Rate Limit Reset: {rate_limit_reset}")

                # Get response body
                try:
                    data = await response.json()
                    print(f"\n📦 Response Data:")

                    if 'error' in data:
                        print(f"   ❌ Error: {data['error']}")
                    elif 'robloxID' in data:
                        print(f"   ✅ Roblox ID: {data['robloxID']}")
                        if 'resolved' in data:
                            print(f"   👤 Username: {data['resolved'].get('username', 'N/A')}")
                    else:
                        print(f"   {data}")
                except:
                    text = await response.text()
                    print(f"   Text Data: {text[:200]}")

                # ✅ ENHANCED SUMMARY with quota calculations
                print(f"\n{'=' * 60}")
                print("DETAILED QUOTA SUMMARY:")
                print(f"{'=' * 60}")

                if response.status == 200:
                    print("✅ SUCCESS! API is working correctly\n")

                    if quota_info.get('remaining') is not None and quota_info.get('limit'):
                        remaining = quota_info['remaining']
                        limit = quota_info['limit']
                        used = limit - remaining
                        usage_percent = (used / limit) * 100

                        print(f"📊 QUOTA STATUS:")
                        print(f"   Limit: {limit} calls/day")
                        print(f"   Used: {used} calls ({usage_percent:.1f}%)")
                        print(f"   Remaining: {remaining} calls ({100 - usage_percent:.1f}%)")

                        # ✅ Visual progress bar
                        bar_length = 40
                        filled = int(bar_length * (used / limit))
                        bar = '█' * filled + '░' * (bar_length - filled)
                        print(f"   [{bar}]")

                        # ✅ Warnings based on usage
                        if usage_percent > 90:
                            print(f"\n   🚨 CRITICAL: You've used {usage_percent:.1f}% of your quota!")
                            print(f"   ⚠️  Only {remaining} calls remaining!")
                        elif usage_percent > 70:
                            print(f"\n   ⚠️  WARNING: You've used {usage_percent:.1f}% of your quota")
                            print(f"   💡 Consider upgrading or implementing caching")
                        elif usage_percent > 50:
                            print(f"\n   ℹ️  INFO: You've used {usage_percent:.1f}% of your quota")
                        else:
                            print(f"\n   ✅ HEALTHY: Plenty of quota remaining ({remaining} calls)")

                        # ✅ Reset time info
                        if quota_info.get('reset_time'):
                            reset_dt = quota_info['reset_time']
                            time_left = quota_info['time_until_reset']

                            hours = int(time_left.total_seconds() // 3600)
                            minutes = int((time_left.total_seconds() % 3600) // 60)

                            print(f"\n⏰ RESET INFORMATION:")
                            print(f"   Resets in: {hours} hours, {minutes} minutes")
                            print(f"   Reset time: {reset_dt.strftime('%I:%M %p on %B %d, %Y')}")
                            print(f"   Timestamp: {int(reset_dt.timestamp())}")
                    else:
                        print("⚠️  Could not determine quota information from headers")

                elif response.status == 401:
                    print("❌ UNAUTHORIZED - Your API key is invalid!")
                    print("   Go to https://blox.link/dashboard/user/developer")
                    print("   and regenerate your API key")

                elif response.status == 429:
                    print("⚠️  RATE LIMITED - You've hit your quota!")
                    if quota_info.get('limit'):
                        print(f"   Daily limit: {quota_info['limit']} requests")
                    if quota_info.get('reset_time'):
                        print(f"   Quota resets at: {quota_info['reset_time'].strftime('%I:%M %p on %B %d, %Y')}")
                    print("\n💡 Options:")
                    print("   • Wait for quota reset (shown above)")
                    print("   • Upgrade to Bloxlink Premium ($5-10/month)")
                    print("   • Implement 24-hour caching (see updated code)")

                elif response.status == 404:
                    print("⚠️  NOT FOUND - User not linked or wrong endpoint")

                else:
                    print(f"⚠️  UNEXPECTED STATUS: {response.status}")

    except asyncio.TimeoutError:
        print("\n❌ REQUEST TIMEOUT - API took too long to respond")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_bloxlink())