import os
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv('DATABASE_URL')

print("=" * 50)
print("ENV FILE DEBUG")
print("=" * 50)
print(f"DATABASE_URL exists: {db_url is not None}")
print(f"DATABASE_URL is empty: {db_url == ''}")
print(f"DATABASE_URL length: {len(db_url) if db_url else 0}")

if db_url:
    print(f"\nFirst 50 chars: {db_url[:50]}")
    print(f"Last 30 chars: {db_url[-30:]}")
    print(f"\nFull URL (be careful - contains password):")
    print(db_url)
else:
    print("\n❌ DATABASE_URL is None or empty!")

print("\n" + "=" * 50)
print("ALL ENVIRONMENT VARIABLES:")
print("=" * 50)
for key, value in os.environ.items():
    if 'DATABASE' in key or 'DISCORD' in key:
        masked = value[:20] + "..." if len(value) > 20 else value
        print(f"{key} = {masked}")