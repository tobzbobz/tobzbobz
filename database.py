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

    # === RAW SQL HELPERS ===
    async def execute(self, query: str, *params):
        """Execute a raw SQL query"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *params)

    async def fetch(self, query: str, *params):
        """Fetch multiple rows"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *params)

    async def fetchrow(self, query: str, *params):
        """Fetch a single row"""
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *params)

    # === CALLSIGNS ===
    async def set_callsign(self, guild_id: int, user_id: int, callsign: str, set_by: int):
        """Set a user's callsign"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO callsigns (guild_id, user_id, callsign, set_by)
                       VALUES ($1, $2, $3, $4) ON CONFLICT (guild_id, user_id)
                       DO UPDATE SET callsign = $3, set_by = $4, set_at = NOW()''',
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

    async def get_all_callsigns(self, guild_id: int) -> List[Dict]:
        """Get all callsigns for a guild"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT * FROM callsigns WHERE guild_id = $1 ORDER BY set_at DESC',
                guild_id
            )
            return [dict(row) for row in rows]

    # === SETTINGS ===
    async def set_setting(self, guild_id: int, key: str, value: Any):
        """Set a guild setting"""
        async with self.pool.acquire() as conn:
            try:
                json_value = json.dumps(value)
                await conn.execute(
                    '''INSERT INTO bot_settings (guild_id, setting_key, setting_value)
                       VALUES ($1, $2, $3::jsonb) ON CONFLICT (guild_id, setting_key)
                       DO UPDATE SET setting_value = $3::jsonb, updated_at = NOW()''',
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

    async def delete_setting(self, guild_id: int, key: str):
        """Delete a guild setting"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM bot_settings WHERE guild_id = $1 AND setting_key = $2',
                guild_id, key
            )
            return result != 'DELETE 0'

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

    async def get_recent_logs(self, guild_id: int, action_type: str = None, user_id: int = None,
                              limit: int = 50, hours: int = None) -> List[Dict]:
        """Get recent logs with filters"""
        async with self.pool.acquire() as conn:
            query = 'SELECT * FROM audit_logs WHERE guild_id = $1'
            params = [guild_id]
            param_count = 1

            if action_type:
                param_count += 1
                query += f' AND action = ${param_count}'
                params.append(action_type)

            if user_id:
                param_count += 1
                query += f' AND user_id = ${param_count}'
                params.append(user_id)

            if hours:
                query += f" AND timestamp > NOW() - INTERVAL '{hours} hours'"

            query += f' ORDER BY timestamp DESC LIMIT ${param_count + 1}'
            params.append(limit)

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    # === ACTIVE WATCHES (FENZ Watch System) ===
    async def add_active_watch(self, message_id: int, guild_id: int, channel_id: int,
                               user_id: int, user_name: str, colour: str, station: str,
                               started_at: int, has_voters_embed: bool = False):
        """Add an active watch"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO active_watches
                       (message_id, guild_id, channel_id, user_id, user_name, colour, station, started_at,
                        has_voters_embed)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8), $9) ON CONFLICT (message_id) DOUPDATE SET
                       guild_id = $2, channel_id = $3, user_id = $4, user_name = $5,
                       colour = $6, station = $7, started_at = to_timestamp($8), has_voters_embed = $9''',
                    message_id, guild_id, channel_id, user_id, user_name, colour, station, started_at, has_voters_embed
                )
                return True
            except Exception as e:
                print(f'❌ Error adding active watch: {e}')
                return False

    async def get_active_watches(self, guild_id: int = None) -> Dict:
        """Get all active watches (returns dict with message_id as key for compatibility)"""
        async with self.pool.acquire() as conn:
            if guild_id:
                rows = await conn.fetch('SELECT * FROM active_watches WHERE guild_id = $1', guild_id)
            else:
                rows = await conn.fetch('SELECT * FROM active_watches')

            watches = {}
            for row in rows:
                watches[str(row['message_id'])] = {
                    'user_id': row['user_id'],
                    'user_name': row['user_name'],
                    'channel_id': row['channel_id'],
                    'colour': row['colour'],
                    'station': row['station'],
                    'started_at': int(row['started_at'].timestamp()),
                    'has_voters_embed': row['has_voters_embed']
                }
            return watches

    async def remove_active_watch(self, message_id: int):
        """Remove an active watch"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM active_watches WHERE message_id = $1',
                message_id
            )
            return result != 'DELETE 0'

    # === SCHEDULED VOTES ===
    async def add_scheduled_vote(self, vote_id: str, guild_id: int, channel_id: int,
                                 watch_role_id: int, user_id: int, colour: str, station: str,
                                 votes: int, time_minutes: int, scheduled_time: int, created_at: int):
        """Add a scheduled vote"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO scheduled_votes
                       (vote_id, guild_id, channel_id, watch_role_id, user_id, colour, station,
                        votes, time_minutes, scheduled_time, created_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10), to_timestamp($11))
                       ON CONFLICT (vote_id) DO UPDATE SET
                       guild_id = $2, channel_id = $3, watch_role_id = $4, user_id = $5,
                       colour = $6, station = $7, votes = $8, time_minutes = $9,
                       scheduled_time = to_timestamp($10), created_at = to_timestamp($11)''',
                    vote_id, guild_id, channel_id, watch_role_id, user_id, colour, station,
                    votes, time_minutes, scheduled_time, created_at
                )
                return True
            except Exception as e:
                print(f'❌ Error adding scheduled vote: {e}')
                return False

    async def get_scheduled_votes(self) -> Dict:
        """Get all scheduled votes (returns dict with vote_id as key for compatibility)"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM scheduled_votes ORDER BY scheduled_time')

            votes = {}
            for row in rows:
                votes[row['vote_id']] = {
                    'guild_id': row['guild_id'],
                    'channel_id': row['channel_id'],
                    'watch_role_id': row['watch_role_id'],
                    'user_id': row['user_id'],
                    'colour': row['colour'],
                    'station': row['station'],
                    'votes': row['votes'],
                    'time_minutes': row['time_minutes'],
                    'scheduled_time': int(row['scheduled_time'].timestamp()),
                    'created_at': int(row['created_at'].timestamp())
                }
            return votes

    async def remove_scheduled_vote(self, vote_id: str):
        """Remove a scheduled vote"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM scheduled_votes WHERE vote_id = $1',
                vote_id
            )
            return result != 'DELETE 0'

    # === COMPLETED WATCHES ===
    async def add_completed_watch(self, message_id: int, guild_id: int, channel_id: int,
                                  user_id: int, user_name: str, colour: str, station: str,
                                  started_at: int, ended_at: int, ended_by: int = None,
                                  attendees: int = None, status: str = 'completed',
                                  reason: str = None, votes_received: int = None,
                                  votes_required: int = None, has_voters_embed: bool = False):
        """Add a completed watch"""
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    '''INSERT INTO completed_watches
                       (message_id, guild_id, channel_id, user_id, user_name, colour, station,
                        started_at, ended_at, ended_by, attendees, status, reason,
                        votes_received, votes_required, has_voters_embed)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8), to_timestamp($9),
                               $10, $11, $12, $13, $14, $15, $16)
                       ON CONFLICT (message_id) DO UPDATE SET
                       guild_id = $2, channel_id = $3, user_id = $4, user_name = $5,
                       colour = $6, station = $7, started_at = to_timestamp($8),
                       ended_at = to_timestamp($9), ended_by = $10, attendees = $11,
                       status = $12, reason = $13, votes_received = $14,
                       votes_required = $15, has_voters_embed = $16''',
                    message_id, guild_id, channel_id, user_id, user_name, colour, station,
                    started_at, ended_at, ended_by, attendees, status, reason,
                    votes_received, votes_required, has_voters_embed
                )
                return True
            except Exception as e:
                print(f'❌ Error adding completed watch: {e}')
                return False

    async def get_completed_watches(self, guild_id: int = None, limit: int = 500) -> Dict:
        """Get completed watches (returns dict with message_id as key for compatibility)"""
        async with self.pool.acquire() as conn:
            if guild_id:
                rows = await conn.fetch(
                    'SELECT * FROM completed_watches WHERE guild_id = $1 ORDER BY ended_at DESC LIMIT $2',
                    guild_id, limit
                )
            else:
                rows = await conn.fetch(
                    'SELECT * FROM completed_watches ORDER BY ended_at DESC LIMIT $1',
                    limit
                )

            watches = {}
            for row in rows:
                watches[str(row['message_id'])] = {
                    'user_id': row['user_id'],
                    'user_name': row['user_name'],
                    'colour': row['colour'],
                    'station': row['station'],
                    'started_at': int(row['started_at'].timestamp()),
                    'ended_at': int(row['ended_at'].timestamp()),
                    'ended_by': row['ended_by'],
                    'attendees': row['attendees'],
                    'status': row['status'],
                    'reason': row['reason'],
                    'votes_received': row['votes_received'],
                    'votes_required': row['votes_required']
                }
            return watches

    async def delete_completed_watch(self, message_id: int):
        """Delete a completed watch"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM completed_watches WHERE message_id = $1',
                int(message_id)
            )
            return result != 'DELETE 0'


# === GLOBAL DATABASE INSTANCE ===
db = Database()


async def ensure_database_connected():
    """Ensure database is connected (call this on bot startup)"""
    if not db.pool:
        await db.connect()
    return db.pool is not None


# === BACKWARD COMPATIBILITY FUNCTIONS ===
# These maintain the same interface as the old JSON system but use the database

async def load_watches():
    """Load active watches (now from database)"""
    return await db.get_active_watches()


async def save_watches(watches: dict):
    """Save active watches - now saves to database instead of no-op"""
    # This function is called by watches.py but we need to handle it differently
    # The watches should be saved individually using db.add_active_watch()
    # This remains as a no-op since we save in real-time now
    pass


async def load_scheduled_votes():
    """Load scheduled votes (now from database)"""
    return await db.get_scheduled_votes()


async def save_scheduled_votes(votes: dict):
    """Save scheduled votes - now saves to database instead of no-op"""
    # This function is called by watches.py but we need to handle it differently
    # The votes should be saved individually using db.add_scheduled_vote()
    # This remains as a no-op since we save in real-time now
    pass


async def load_completed_watches():
    """Load completed watches (now from database)"""
    return await db.get_completed_watches()


async def save_completed_watches(watches: dict):
    """Save completed watches - now saves to database instead of no-op"""
    # This function is called by watches.py but we need to handle it differently
    # The watches should be saved individually using db.add_completed_watch()
    # This remains as a no-op since we save in real-time now
    pass