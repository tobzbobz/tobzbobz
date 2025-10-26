import os
import asyncpg
from typing import Optional, Dict, List, Any
import json


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.database_url = os.getenv('DATABASE_URL')

        if not self.database_url:
            print('⚠️  DATABASE_URL not set! Bot will not be able to save data.')

    async def connect(self):
        """Connect to the database"""
        if not self.database_url:
            print('❌ Cannot connect: DATABASE_URL not set')
            return False

        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            print('✅ Connected to Supabase database')
            return True
        except Exception as e:
            print(f'❌ Failed to connect to database: {e}')
            return False

    async def close(self):
        """Close database connection"""
        if self.pool:
            await self.pool.close()
            print('📊 Database connection closed')

    # === WATCHES ===
    async def add_watch(self, guild_id: int, user_id: int, reason: str, added_by: int):
        """Add a user to the watch list"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO watches (guild_id, user_id, reason, added_by)
                       VALUES ($1, $2, $3, $4) ON CONFLICT (guild_id, user_id) 
                       DO
                    UPDATE SET reason = $3, added_by = $4, added_at = NOW()''',
                    guild_id, user_id, reason, added_by
                )
                return True
            except Exception as e:
                print(f'❌ Error adding watch: {e}')
                return False

    async def remove_watch(self, guild_id: int, user_id: int):
        """Remove a user from the watch list"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM watches WHERE guild_id = $1 AND user_id = $2',
                guild_id, user_id
            )
            return result != 'DELETE 0'

    async def get_watches(self, guild_id: int) -> List[Dict]:
        """Get all watches for a guild"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM watches WHERE guild_id = $1 ORDER BY added_at DESC',
                guild_id
            )
            return [dict(row) for row in rows]

    async def is_watched(self, guild_id: int, user_id: int) -> bool:
        """Check if a user is being watched"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(
                'SELECT COUNT(*) FROM watches WHERE guild_id = $1 AND user_id = $2',
                guild_id, user_id
            )
            return result > 0

    # === TICKETS ===
    async def create_ticket(self, guild_id: int, channel_id: int, user_id: int):
        """Create a new ticket"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO tickets (guild_id, channel_id, user_id, status)
                       VALUES ($1, $2, $3, 'open')''',
                    guild_id, channel_id, user_id
                )
                return True
            except Exception as e:
                print(f'❌ Error creating ticket: {e}')
                return False

    async def close_ticket(self, guild_id: int, channel_id: int):
        """Close a ticket"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                '''UPDATE tickets
                   SET status    = 'closed',
                       closed_at = NOW()
                   WHERE guild_id = $1
                     AND channel_id = $2''',
                guild_id, channel_id
            )
            return result != 'UPDATE 0'

    async def get_ticket(self, guild_id: int, channel_id: int) -> Optional[Dict]:
        """Get ticket info"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT * FROM tickets WHERE guild_id = $1 AND channel_id = $2',
                guild_id, channel_id
            )
            return dict(row) if row else None

    async def get_open_tickets(self, guild_id: int) -> List[Dict]:
        """Get all open tickets for a guild"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                '''SELECT *
                   FROM tickets
                   WHERE guild_id = $1
                     AND status = 'open'
                   ORDER BY created_at DESC''',
                guild_id
            )
            return [dict(row) for row in rows]

    # === CALLSIGNS ===
    async def set_callsign(self, guild_id: int, user_id: int, callsign: str, set_by: int):
        """Set a user's callsign"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO callsigns (guild_id, user_id, callsign, set_by)
                       VALUES ($1, $2, $3, $4) ON CONFLICT (guild_id, user_id)
                       DO
                    UPDATE SET callsign = $3, set_by = $4, set_at = NOW()''',
                    guild_id, user_id, callsign, set_by
                )
                return True
            except Exception as e:
                print(f'❌ Error setting callsign: {e}')
                return False

    async def get_callsign(self, guild_id: int, user_id: int) -> Optional[str]:
        """Get a user's callsign"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(
                'SELECT callsign FROM callsigns WHERE guild_id = $1 AND user_id = $2',
                guild_id, user_id
            )
            return result

    async def remove_callsign(self, guild_id: int, user_id: int):
        """Remove a user's callsign"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM callsigns WHERE guild_id = $1 AND user_id = $2',
                guild_id, user_id
            )
            return result != 'DELETE 0'

    # === SETTINGS ===
    async def set_setting(self, guild_id: int, key: str, value: Any):
        """Set a guild setting"""
        async with self.pool.acquire() as conn:
            try:
                # Convert value to JSON
                json_value = json.dumps(value)
                await conn.execute(
                    '''INSERT INTO bot_settings (guild_id, setting_key, setting_value)
                       VALUES ($1, $2, $3::jsonb) ON CONFLICT (guild_id, setting_key)
                       DO
                    UPDATE SET setting_value = $3::jsonb, updated_at = NOW()''',
                    guild_id, key, json_value
                )
                return True
            except Exception as e:
                print(f'❌ Error setting config: {e}')
                return False

    async def get_setting(self, guild_id: int, key: str, default=None):
        """Get a guild setting"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(
                'SELECT setting_value FROM bot_settings WHERE guild_id = $1 AND setting_key = $2',
                guild_id, key
            )
            if result:
                return json.loads(result)
            return default

    # === AUDIT LOGS ===
    async def log_action(self, guild_id: int, user_id: int, action: str, details: Dict = None):
        """Log an action to audit log"""
        async with self.pool.acquire() as conn:
            try:
                json_details = json.dumps(details) if details else None
                await conn.execute(
                    '''INSERT INTO audit_logs (guild_id, user_id, action, details)
                       VALUES ($1, $2, $3, $4::jsonb)''',
                    guild_id, user_id, action, json_details
                )
                return True
            except Exception as e:
                print(f'❌ Error logging action: {e}')
                return False

    async def get_recent_logs(self, guild_id: int, limit: int = 50) -> List[Dict]:
        """Get recent audit logs"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                '''SELECT *
                   FROM audit_logs
                   WHERE guild_id = $1
                   ORDER BY timestamp DESC
                       LIMIT $2''',
                guild_id, limit
            )
            return [dict(row) for row in rows]


# Global database instance
db = Database()


async def ensure_database_connected():
    """Ensure database is connected (call this on bot startup)"""
    if not db.pool:
        await db.connect()
    return db.pool is not None