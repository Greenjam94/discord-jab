"""Database manager for Torn API data storage."""

import aiosqlite
import os
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import json


class TornDatabase:
    """Manages SQLite database for Torn API data."""
    
    CURRENT_SCHEMA_VERSION = 6
    
    def __init__(self, db_path: str = "data/torn_data.db"):
        """Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Connect to database and initialize schema."""
        self.connection = await aiosqlite.connect(str(self.db_path))
        
        # Enable foreign keys and WAL mode
        await self.connection.execute("PRAGMA foreign_keys = ON")
        await self.connection.execute("PRAGMA journal_mode = WAL")
        await self.connection.execute("PRAGMA synchronous = NORMAL")
        await self.connection.execute("PRAGMA cache_size = -64000")  # 64MB cache
        await self.connection.execute("PRAGMA temp_store = MEMORY")
        
        await self.connection.commit()
        
        # Initialize schema
        await self._initialize_schema()
    
    async def close(self):
        """Close database connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None
    
    async def _ensure_connected(self):
        """Ensure database connection is established."""
        if self.connection is None:
            await self.connect()
    
    async def _initialize_schema(self):
        """Create database schema if it doesn't exist."""
        current_version = await self._get_schema_version()
        
        if current_version < 1:
            await self._create_schema_v1()
            await self._set_schema_version(1, "Initial schema")
        
        if current_version < 2:
            await self._create_schema_v2()
            await self._set_schema_version(2, "Added competition tracking tables")
        
        if current_version < 3:
            await self._create_schema_v3()
            await self._set_schema_version(3, "Added contributor stats support and stat_source column")
        
        if current_version < 4:
            await self._create_schema_v4()
            await self._set_schema_version(4, "Added bot instances and command permissions tables")
        
        if current_version < 5:
            # Check if tables already exist (in case migration was partially applied)
            tables_exist = (
                await self._check_table_exists("organized_crimes_current") and
                await self._check_table_exists("organized_crimes_history") and
                await self._check_table_exists("organized_crimes_config") and
                await self._check_table_exists("organized_crimes_participant_stats")
            )
            
            if not tables_exist:
                await self._create_schema_v5()
                await self._set_schema_version(5, "Added organized crime tracking tables")
            else:
                # Tables exist but version wasn't set - just set the version
                await self._set_schema_version(5, "Added organized crime tracking tables")
        
        if current_version < 6:
            await self._create_schema_v6()
            await self._set_schema_version(6, "Added missing item reminders, items cache, and discord_id to players")
    
    async def _get_schema_version(self) -> int:
        """Get current schema version."""
        try:
            async with self.connection.execute(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except aiosqlite.OperationalError:
            return 0
    
    async def _set_schema_version(self, version: int, description: str = ""):
        """Set schema version."""
        await self.connection.execute(
            "INSERT INTO schema_version (version, description) VALUES (?, ?)",
            (version, description)
        )
        await self.connection.commit()
    
    async def _check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            async with self.connection.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,)) as cursor:
                row = await cursor.fetchone()
                return row is not None
        except Exception:
            return False
    
    async def reset_schema_version(self, target_version: int):
        """Reset schema version by deleting versions greater than target.
        
        WARNING: This should only be used for development/testing.
        It will cause migrations to re-run on next connection.
        """
        await self._ensure_connected()
        await self.connection.execute("""
            DELETE FROM schema_version WHERE version > ?
        """, (target_version,))
        await self.connection.commit()
    
    async def _create_schema_v1(self):
        """Create initial database schema."""
        # Schema version table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                description TEXT
            )
        """)
        
        # Upsert tables
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                level INTEGER CHECK (level >= 1),
                rank TEXT,
                faction_id INTEGER,
                status_state TEXT,
                status_description TEXT,
                life_current INTEGER CHECK (life_current >= 0),
                life_maximum INTEGER CHECK (life_maximum > 0),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS factions (
                faction_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tag TEXT,
                leader_id INTEGER,
                co_leader_id INTEGER,
                respect INTEGER CHECK (respect >= 0),
                age INTEGER CHECK (age >= 0),
                best_chain INTEGER CHECK (best_chain >= 0),
                member_count INTEGER CHECK (member_count >= 0),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        # Append-only tables
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS player_stats_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                strength INTEGER CHECK (strength >= 0),
                defense INTEGER CHECK (defense >= 0),
                speed INTEGER CHECK (speed >= 0),
                dexterity INTEGER CHECK (dexterity >= 0),
                total_stats INTEGER CHECK (total_stats >= 0),
                level INTEGER CHECK (level >= 1),
                life_maximum INTEGER CHECK (life_maximum > 0),
                networth INTEGER,
                data_source TEXT,  -- Masked API key identifier
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS faction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                respect INTEGER CHECK (respect >= 0),
                member_count INTEGER CHECK (member_count >= 0),
                best_chain INTEGER CHECK (best_chain >= 0),
                data_source TEXT,  -- Masked API key identifier
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS war_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                war_id INTEGER NOT NULL,
                war_type TEXT NOT NULL CHECK (war_type IN ('territory', 'ranked')),
                territory_id INTEGER,
                attacking_faction_id INTEGER,
                defending_faction_id INTEGER,
                attacking_score INTEGER CHECK (attacking_score >= 0),
                defending_score INTEGER CHECK (defending_score >= 0),
                required_score INTEGER CHECK (required_score >= 0),
                status TEXT CHECK (status IN ('ongoing', 'completed', 'cancelled')),
                started_at INTEGER,
                ends_at INTEGER,
                recorded_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                data_source TEXT,  -- Masked API key identifier
                FOREIGN KEY (attacking_faction_id) REFERENCES factions(faction_id),
                FOREIGN KEY (defending_faction_id) REFERENCES factions(faction_id)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS territory_ownership_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                territory_id INTEGER NOT NULL,
                faction_id INTEGER,
                sector TEXT,
                coordinates TEXT,
                racket_level INTEGER CHECK (racket_level >= 0),
                racket_type TEXT,
                war_status TEXT CHECK (war_status IN ('none', 'under_attack', 'defending')),
                recorded_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                data_source TEXT,  -- Masked API key identifier
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id)
            )
        """)
        
        # Summary tables
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS player_stats_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                period_type TEXT NOT NULL DEFAULT 'monthly',
                strength_start INTEGER,
                strength_end INTEGER,
                strength_change INTEGER,
                defense_start INTEGER,
                defense_end INTEGER,
                defense_change INTEGER,
                speed_start INTEGER,
                speed_end INTEGER,
                speed_change INTEGER,
                dexterity_start INTEGER,
                dexterity_end INTEGER,
                dexterity_change INTEGER,
                total_stats_start INTEGER,
                total_stats_end INTEGER,
                total_stats_change INTEGER,
                level_start INTEGER,
                level_end INTEGER,
                level_change INTEGER,
                life_maximum_start INTEGER,
                life_maximum_end INTEGER,
                networth_start INTEGER,
                networth_end INTEGER,
                networth_change INTEGER,
                record_count INTEGER CHECK (record_count >= 0),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (player_id) REFERENCES players(player_id),
                UNIQUE(player_id, period_start, period_end, period_type)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS faction_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                period_type TEXT NOT NULL DEFAULT 'monthly',
                respect_start INTEGER,
                respect_end INTEGER,
                respect_change INTEGER,
                member_count_start INTEGER,
                member_count_end INTEGER,
                member_count_change INTEGER,
                best_chain_start INTEGER,
                best_chain_end INTEGER,
                best_chain_change INTEGER,
                record_count INTEGER CHECK (record_count >= 0),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id),
                UNIQUE(faction_id, period_start, period_end, period_type)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS war_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                war_id INTEGER NOT NULL,
                war_type TEXT NOT NULL CHECK (war_type IN ('territory', 'ranked')),
                territory_id INTEGER,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                period_type TEXT NOT NULL DEFAULT 'monthly',
                attacking_faction_id INTEGER,
                defending_faction_id INTEGER,
                final_attacking_score INTEGER,
                final_defending_score INTEGER,
                winner_faction_id INTEGER,
                duration_seconds INTEGER CHECK (duration_seconds >= 0),
                record_count INTEGER CHECK (record_count >= 0),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (attacking_faction_id) REFERENCES factions(faction_id),
                FOREIGN KEY (defending_faction_id) REFERENCES factions(faction_id),
                FOREIGN KEY (winner_faction_id) REFERENCES factions(faction_id),
                UNIQUE(war_id, period_start, period_end, period_type)
            )
        """)
        
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS territory_ownership_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                territory_id INTEGER NOT NULL,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                period_type TEXT NOT NULL DEFAULT 'monthly',
                faction_id_start INTEGER,
                faction_id_end INTEGER,
                ownership_changes INTEGER CHECK (ownership_changes >= 0),
                days_owned INTEGER CHECK (days_owned >= 0),
                record_count INTEGER CHECK (record_count >= 0),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (faction_id_start) REFERENCES factions(faction_id),
                FOREIGN KEY (faction_id_end) REFERENCES factions(faction_id),
                UNIQUE(territory_id, period_start, period_end, period_type)
            )
        """)
        
        # Create indexes
        await self._create_indexes()
        
        await self.connection.commit()
    
    async def _create_indexes(self):
        """Create database indexes."""
        indexes = [
            # Players
            "CREATE INDEX IF NOT EXISTS idx_players_faction ON players(faction_id)",
            "CREATE INDEX IF NOT EXISTS idx_players_last_updated ON players(last_updated)",
            
            # Factions
            "CREATE INDEX IF NOT EXISTS idx_factions_last_updated ON factions(last_updated)",
            
            # Player stats history
            "CREATE INDEX IF NOT EXISTS idx_player_stats_player_time ON player_stats_history(player_id, timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_player_stats_timestamp ON player_stats_history(timestamp)",
            
            # Faction history
            "CREATE INDEX IF NOT EXISTS idx_faction_history_faction_time ON faction_history(faction_id, timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_faction_history_timestamp ON faction_history(timestamp)",
            
            # War status history
            "CREATE INDEX IF NOT EXISTS idx_war_status_war_time ON war_status_history(war_id, recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_war_status_territory ON war_status_history(territory_id, recorded_at)",
            "CREATE INDEX IF NOT EXISTS idx_war_status_timestamp ON war_status_history(recorded_at)",
            
            # Territory ownership history
            "CREATE INDEX IF NOT EXISTS idx_territory_ownership_territory_time ON territory_ownership_history(territory_id, recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_territory_ownership_faction ON territory_ownership_history(faction_id, recorded_at)",
            "CREATE INDEX IF NOT EXISTS idx_territory_ownership_timestamp ON territory_ownership_history(recorded_at)",
            
            # Summary tables
            "CREATE INDEX IF NOT EXISTS idx_player_stats_summary_player_period ON player_stats_summary(player_id, period_start, period_end)",
            "CREATE INDEX IF NOT EXISTS idx_player_stats_summary_period_type ON player_stats_summary(period_type, period_start)",
            "CREATE INDEX IF NOT EXISTS idx_faction_summary_faction_period ON faction_summary(faction_id, period_start, period_end)",
            "CREATE INDEX IF NOT EXISTS idx_faction_summary_period_type ON faction_summary(period_type, period_start)",
            "CREATE INDEX IF NOT EXISTS idx_war_summary_war_period ON war_summary(war_id, period_start, period_end)",
            "CREATE INDEX IF NOT EXISTS idx_war_summary_territory ON war_summary(territory_id, period_start)",
            "CREATE INDEX IF NOT EXISTS idx_territory_summary_territory_period ON territory_ownership_summary(territory_id, period_start, period_end)",
            "CREATE INDEX IF NOT EXISTS idx_territory_summary_faction ON territory_ownership_summary(faction_id_end, period_start)",
        ]
        
        for index_sql in indexes:
            await self.connection.execute(index_sql)
    
    # Upsert methods
    async def upsert_player(
        self,
        player_id: int,
        name: str,
        level: Optional[int] = None,
        rank: Optional[str] = None,
        faction_id: Optional[int] = None,
        status_state: Optional[str] = None,
        status_description: Optional[str] = None,
        life_current: Optional[int] = None,
        life_maximum: Optional[int] = None
    ):
        """Upsert player data."""
        now = int(datetime.utcnow().timestamp())
        
        # If faction_id is provided, check if faction exists
        # If not, create a minimal faction entry to satisfy foreign key constraint
        if faction_id is not None:
            async with self.connection.execute(
                "SELECT faction_id FROM factions WHERE faction_id = ?", (faction_id,)
            ) as cursor:
                faction_exists = await cursor.fetchone()
            
            if not faction_exists:
                # Faction doesn't exist, create minimal entry to satisfy FK constraint
                # We'll update it with full data when faction info is fetched
                await self.connection.execute("""
                    INSERT OR IGNORE INTO factions (
                        faction_id, name, created_at, last_updated
                    ) VALUES (?, ?, ?, ?)
                """, (faction_id, f"Faction {faction_id}", now, now))
        
        # Check if player exists
        async with self.connection.execute(
            "SELECT player_id FROM players WHERE player_id = ?", (player_id,)
        ) as cursor:
            exists = await cursor.fetchone()
        
        if exists:
            # Update existing
            await self.connection.execute("""
                UPDATE players SET
                    name = ?,
                    level = COALESCE(?, level),
                    rank = COALESCE(?, rank),
                    faction_id = COALESCE(?, faction_id),
                    status_state = COALESCE(?, status_state),
                    status_description = COALESCE(?, status_description),
                    life_current = COALESCE(?, life_current),
                    life_maximum = COALESCE(?, life_maximum),
                    last_updated = ?
                WHERE player_id = ?
            """, (name, level, rank, faction_id, status_state, status_description,
                  life_current, life_maximum, now, player_id))
        else:
            # Insert new
            await self.connection.execute("""
                INSERT INTO players (
                    player_id, name, level, rank, faction_id, status_state,
                    status_description, life_current, life_maximum, created_at, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (player_id, name, level, rank, faction_id, status_state,
                  status_description, life_current, life_maximum, now, now))
        
        await self.connection.commit()
    
    async def upsert_faction(
        self,
        faction_id: int,
        name: str,
        tag: Optional[str] = None,
        leader_id: Optional[int] = None,
        co_leader_id: Optional[int] = None,
        respect: Optional[int] = None,
        age: Optional[int] = None,
        best_chain: Optional[int] = None,
        member_count: Optional[int] = None
    ):
        """Upsert faction data."""
        now = int(datetime.utcnow().timestamp())
        
        # Check if faction exists
        async with self.connection.execute(
            "SELECT faction_id FROM factions WHERE faction_id = ?", (faction_id,)
        ) as cursor:
            exists = await cursor.fetchone()
        
        if exists:
            # Update existing
            await self.connection.execute("""
                UPDATE factions SET
                    name = ?,
                    tag = COALESCE(?, tag),
                    leader_id = COALESCE(?, leader_id),
                    co_leader_id = COALESCE(?, co_leader_id),
                    respect = COALESCE(?, respect),
                    age = COALESCE(?, age),
                    best_chain = COALESCE(?, best_chain),
                    member_count = COALESCE(?, member_count),
                    last_updated = ?
                WHERE faction_id = ?
            """, (name, tag, leader_id, co_leader_id, respect, age,
                  best_chain, member_count, now, faction_id))
        else:
            # Insert new
            await self.connection.execute("""
                INSERT INTO factions (
                    faction_id, name, tag, leader_id, co_leader_id,
                    respect, age, best_chain, member_count, created_at, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (faction_id, name, tag, leader_id, co_leader_id,
                  respect, age, best_chain, member_count, now, now))
        
        await self.connection.commit()
    
    # Append methods
    async def append_player_stats(
        self,
        player_id: int,
        strength: Optional[int] = None,
        defense: Optional[int] = None,
        speed: Optional[int] = None,
        dexterity: Optional[int] = None,
        total_stats: Optional[int] = None,
        level: Optional[int] = None,
        life_maximum: Optional[int] = None,
        networth: Optional[int] = None,
        data_source: Optional[str] = None
    ):
        """Append player stats to history."""
        await self.connection.execute("""
            INSERT INTO player_stats_history (
                player_id, timestamp, strength, defense, speed, dexterity,
                total_stats, level, life_maximum, networth, data_source
            ) VALUES (?, strftime('%s', 'now'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (player_id, strength, defense, speed, dexterity,
              total_stats, level, life_maximum, networth, data_source))
        await self.connection.commit()
    
    async def append_player_contributor_history(
        self,
        player_id: int,
        stat_name: str,
        value: float,
        faction_id: Optional[int] = None,
        data_source: Optional[str] = None,
        timestamp: Optional[int] = None
    ):
        """Append player contributor stat to history.
        
        Args:
            player_id: Torn player ID
            stat_name: Name of the contributor stat (e.g., 'gymstrength', 'gym_e_spent')
            value: The contributor value
            faction_id: Faction ID this was collected from
            data_source: Masked API key used
            timestamp: Optional timestamp (defaults to current time)
        """
        if timestamp is None:
            timestamp = int(datetime.utcnow().timestamp())
        
        await self.connection.execute("""
            INSERT OR REPLACE INTO player_contributor_history
            (player_id, stat_name, timestamp, value, faction_id, data_source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (player_id, stat_name, timestamp, value, faction_id, data_source))
        await self.connection.commit()
    
    async def append_faction_history(
        self,
        faction_id: int,
        respect: Optional[int] = None,
        member_count: Optional[int] = None,
        best_chain: Optional[int] = None,
        data_source: Optional[str] = None
    ):
        """Append faction history."""
        await self.connection.execute("""
            INSERT INTO faction_history (
                faction_id, timestamp, respect, member_count, best_chain, data_source
            ) VALUES (?, strftime('%s', 'now'), ?, ?, ?, ?)
        """, (faction_id, respect, member_count, best_chain, data_source))
        await self.connection.commit()
    
    async def append_war_status(
        self,
        war_id: int,
        war_type: str,
        territory_id: Optional[int] = None,
        attacking_faction_id: Optional[int] = None,
        defending_faction_id: Optional[int] = None,
        attacking_score: Optional[int] = None,
        defending_score: Optional[int] = None,
        required_score: Optional[int] = None,
        status: Optional[str] = None,
        started_at: Optional[int] = None,
        ends_at: Optional[int] = None,
        data_source: Optional[str] = None
    ):
        """Append war status snapshot."""
        await self.connection.execute("""
            INSERT INTO war_status_history (
                war_id, war_type, territory_id, attacking_faction_id,
                defending_faction_id, attacking_score, defending_score,
                required_score, status, started_at, ends_at, recorded_at, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'), ?)
        """, (war_id, war_type, territory_id, attacking_faction_id,
              defending_faction_id, attacking_score, defending_score,
              required_score, status, started_at, ends_at, data_source))
        await self.connection.commit()
    
    async def append_territory_ownership(
        self,
        territory_id: int,
        faction_id: Optional[int] = None,
        sector: Optional[str] = None,
        coordinates: Optional[str] = None,
        racket_level: Optional[int] = None,
        racket_type: Optional[str] = None,
        war_status: Optional[str] = None,
        data_source: Optional[str] = None
    ):
        """Append territory ownership snapshot."""
        await self.connection.execute("""
            INSERT INTO territory_ownership_history (
                territory_id, faction_id, sector, coordinates,
                racket_level, racket_type, war_status, recorded_at, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'), ?)
        """, (territory_id, faction_id, sector, coordinates,
              racket_level, racket_type, war_status, data_source))
        await self.connection.commit()
    
    # Health metrics
    async def get_health_metrics(self) -> Dict[str, Any]:
        """Get database health metrics."""
        metrics = {}
        
        # Table sizes
        table_names = [
            'players', 'factions',
            'player_stats_history', 'faction_history',
            'war_status_history', 'territory_ownership_history',
            'player_stats_summary', 'faction_summary',
            'war_summary', 'territory_ownership_summary'
        ]
        
        for table in table_names:
            async with self.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ) as cursor:
                row = await cursor.fetchone()
                metrics[f"{table}_count"] = row[0] if row else 0
        
        # Database file size
        if self.db_path.exists():
            metrics["database_size_mb"] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
        else:
            metrics["database_size_mb"] = 0
        
        # Oldest and newest records in append-only tables
        for table in ['player_stats_history', 'faction_history', 'war_status_history', 'territory_ownership_history']:
            timestamp_col = 'timestamp' if table != 'war_status_history' and table != 'territory_ownership_history' else 'recorded_at'
            
            async with self.connection.execute(
                f"SELECT MIN({timestamp_col}), MAX({timestamp_col}) FROM {table}"
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    metrics[f"{table}_oldest"] = datetime.fromtimestamp(row[0]).isoformat()
                    metrics[f"{table}_newest"] = datetime.fromtimestamp(row[1]).isoformat()
                else:
                    metrics[f"{table}_oldest"] = None
                    metrics[f"{table}_newest"] = None
        
        # Records older than 2 months (should be pruned)
        two_months_ago = int((datetime.utcnow() - timedelta(days=60)).timestamp())
        
        for table in ['player_stats_history', 'faction_history', 'war_status_history', 'territory_ownership_history']:
            timestamp_col = 'timestamp' if table != 'war_status_history' and table != 'territory_ownership_history' else 'recorded_at'
            
            async with self.connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {timestamp_col} < ?",
                (two_months_ago,)
            ) as cursor:
                row = await cursor.fetchone()
                metrics[f"{table}_old_records"] = row[0] if row else 0
        
        # Schema version
        metrics["schema_version"] = await self._get_schema_version()
        
        return metrics
    
    # Backup
    async def backup(self, backup_dir: str = "data/backups") -> str:
        """Create a backup of the database.
        
        Returns:
            Path to backup file
        """
        # Ensure all pending writes are committed
        await self.connection.commit()
        
        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_path / f"torn_data_backup_{timestamp}.db"
        
        # Copy database file (SQLite is file-based, so this is safe after commit)
        # This approach is simpler and works reliably with WAL mode
        if self.db_path.exists():
            shutil.copy2(str(self.db_path), str(backup_file))
            # Also copy WAL file if it exists
            wal_file = Path(str(self.db_path) + "-wal")
            if wal_file.exists():
                shutil.copy2(str(wal_file), str(backup_file) + "-wal")
        
        return str(backup_file)
    
    # Summarization (to be implemented in next phase)
    async def summarize_player_stats_monthly(self, year: int, month: int, force: bool = False) -> int:
        """Summarize player stats for a specific month.
        
        Returns:
            Number of summaries created
        """
        # Calculate period
        period_start = datetime(year, month, 1)
        if month == 12:
            period_end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            period_end = datetime(year, month + 1, 1) - timedelta(seconds=1)
        
        period_start_ts = int(period_start.timestamp())
        period_end_ts = int(period_end.timestamp())
        
        # Check if already summarized
        if not force:
            async with self.connection.execute("""
                SELECT COUNT(*) FROM player_stats_summary
                WHERE period_start = ? AND period_end = ? AND period_type = 'monthly'
            """, (period_start_ts, period_end_ts)) as cursor:
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    return 0  # Already summarized
        
        # Get all players with data in this period
        async with self.connection.execute("""
            SELECT DISTINCT player_id FROM player_stats_history
            WHERE timestamp >= ? AND timestamp <= ?
        """, (period_start_ts, period_end_ts)) as cursor:
            player_rows = await cursor.fetchall()
        
        summaries_created = 0
        
        async with self.connection.execute("BEGIN TRANSACTION"):
            try:
                for (player_id,) in player_rows:
                    # Get first and last records for this period
                    async with self.connection.execute("""
                        SELECT * FROM player_stats_history
                        WHERE player_id = ? AND timestamp >= ? AND timestamp <= ?
                        ORDER BY timestamp ASC
                        LIMIT 1
                    """, (player_id, period_start_ts, period_end_ts)) as cursor:
                        first_row = await cursor.fetchone()
                    
                    async with self.connection.execute("""
                        SELECT * FROM player_stats_history
                        WHERE player_id = ? AND timestamp >= ? AND timestamp <= ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (player_id, period_start_ts, period_end_ts)) as cursor:
                        last_row = await cursor.fetchone()
                    
                    if not first_row or not last_row:
                        continue
                    
                    # Count records
                    async with self.connection.execute("""
                        SELECT COUNT(*) FROM player_stats_history
                        WHERE player_id = ? AND timestamp >= ? AND timestamp <= ?
                    """, (player_id, period_start_ts, period_end_ts)) as cursor:
                        count_row = await cursor.fetchone()
                        record_count = count_row[0] if count_row else 0
                    
                    # Calculate changes
                    # Column order: id, player_id, timestamp, strength, defense, speed, dexterity,
                    # total_stats, level, life_maximum, networth, data_source
                    strength_start = first_row[3] if first_row[3] is not None else 0
                    strength_end = last_row[3] if last_row[3] is not None else 0
                    strength_change = strength_end - strength_start
                    
                    defense_start = first_row[4] if first_row[4] is not None else 0
                    defense_end = last_row[4] if last_row[4] is not None else 0
                    defense_change = defense_end - defense_start
                    
                    speed_start = first_row[5] if first_row[5] is not None else 0
                    speed_end = last_row[5] if last_row[5] is not None else 0
                    speed_change = speed_end - speed_start
                    
                    dexterity_start = first_row[6] if first_row[6] is not None else 0
                    dexterity_end = last_row[6] if last_row[6] is not None else 0
                    dexterity_change = dexterity_end - dexterity_start
                    
                    total_stats_start = first_row[7] if first_row[7] is not None else 0
                    total_stats_end = last_row[7] if last_row[7] is not None else 0
                    total_stats_change = total_stats_end - total_stats_start
                    
                    level_start = first_row[8] if first_row[8] is not None else 0
                    level_end = last_row[8] if last_row[8] is not None else 0
                    level_change = level_end - level_start
                    
                    life_maximum_start = first_row[9] if first_row[9] is not None else 0
                    life_maximum_end = last_row[9] if last_row[9] is not None else 0
                    
                    networth_start = first_row[10] if first_row[10] is not None else 0
                    networth_end = last_row[10] if last_row[10] is not None else 0
                    networth_change = networth_end - networth_start
                    
                    # Insert summary
                    await self.connection.execute("""
                        INSERT OR REPLACE INTO player_stats_summary (
                            player_id, period_start, period_end, period_type,
                            strength_start, strength_end, strength_change,
                            defense_start, defense_end, defense_change,
                            speed_start, speed_end, speed_change,
                            dexterity_start, dexterity_end, dexterity_change,
                            total_stats_start, total_stats_end, total_stats_change,
                            level_start, level_end, level_change,
                            life_maximum_start, life_maximum_end,
                            networth_start, networth_end, networth_change,
                            record_count
                        ) VALUES (?, ?, ?, 'monthly', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (player_id, period_start_ts, period_end_ts,
                          strength_start, strength_end, strength_change,
                          defense_start, defense_end, defense_change,
                          speed_start, speed_end, speed_change,
                          dexterity_start, dexterity_end, dexterity_change,
                          total_stats_start, total_stats_end, total_stats_change,
                          level_start, level_end, level_change,
                          life_maximum_start, life_maximum_end,
                          networth_start, networth_end, networth_change,
                          record_count))
                    
                    summaries_created += 1
                
                await self.connection.commit()
            except Exception as e:
                await self.connection.rollback()
                raise
        
        return summaries_created
    
    async def _prune_history_table(
        self,
        table_name: str,
        timestamp_column: str,
        older_than_days: int
    ) -> int:
        """Generic helper to prune history tables.
        
        Args:
            table_name: Name of the table to prune
            timestamp_column: Name of the timestamp column
            older_than_days: Days threshold for pruning
            
        Returns:
            Number of records deleted
        """
        cutoff_timestamp = int((datetime.utcnow() - timedelta(days=older_than_days)).timestamp())
        
        async with self.connection.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {timestamp_column} < ?",
            (cutoff_timestamp,)
        ) as cursor:
            count_row = await cursor.fetchone()
            count = count_row[0] if count_row else 0
        
        await self.connection.execute(
            f"DELETE FROM {table_name} WHERE {timestamp_column} < ?",
            (cutoff_timestamp,)
        )
        await self.connection.commit()
        
        return count
    
    async def prune_player_stats_history(self, older_than_days: int = 60) -> int:
        """Prune player stats history older than specified days.
        
        Returns:
            Number of records deleted
        """
        return await self._prune_history_table("player_stats_history", "timestamp", older_than_days)
    
    async def summarize_faction_history_monthly(self, year: int, month: int, force: bool = False) -> int:
        """Summarize faction history for a specific month.
        
        Returns:
            Number of summaries created
        """
        # Calculate period
        period_start = datetime(year, month, 1)
        if month == 12:
            period_end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            period_end = datetime(year, month + 1, 1) - timedelta(seconds=1)
        
        period_start_ts = int(period_start.timestamp())
        period_end_ts = int(period_end.timestamp())
        
        # Check if already summarized
        if not force:
            async with self.connection.execute("""
                SELECT COUNT(*) FROM faction_summary
                WHERE period_start = ? AND period_end = ? AND period_type = 'monthly'
            """, (period_start_ts, period_end_ts)) as cursor:
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    return 0  # Already summarized
        
        # Get all factions with data in this period
        async with self.connection.execute("""
            SELECT DISTINCT faction_id FROM faction_history
            WHERE timestamp >= ? AND timestamp <= ?
        """, (period_start_ts, period_end_ts)) as cursor:
            faction_rows = await cursor.fetchall()
        
        summaries_created = 0
        
        async with self.connection.execute("BEGIN TRANSACTION"):
            try:
                for (faction_id,) in faction_rows:
                    # Get first and last records for this period
                    async with self.connection.execute("""
                        SELECT * FROM faction_history
                        WHERE faction_id = ? AND timestamp >= ? AND timestamp <= ?
                        ORDER BY timestamp ASC
                        LIMIT 1
                    """, (faction_id, period_start_ts, period_end_ts)) as cursor:
                        first_row = await cursor.fetchone()
                    
                    async with self.connection.execute("""
                        SELECT * FROM faction_history
                        WHERE faction_id = ? AND timestamp >= ? AND timestamp <= ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (faction_id, period_start_ts, period_end_ts)) as cursor:
                        last_row = await cursor.fetchone()
                    
                    if not first_row or not last_row:
                        continue
                    
                    # Count records
                    async with self.connection.execute("""
                        SELECT COUNT(*) FROM faction_history
                        WHERE faction_id = ? AND timestamp >= ? AND timestamp <= ?
                    """, (faction_id, period_start_ts, period_end_ts)) as cursor:
                        count_row = await cursor.fetchone()
                        record_count = count_row[0] if count_row else 0
                    
                    # Calculate changes
                    # Column order: id, faction_id, timestamp, respect, member_count, best_chain, data_source
                    respect_start = first_row[3] if first_row[3] is not None else 0
                    respect_end = last_row[3] if last_row[3] is not None else 0
                    respect_change = respect_end - respect_start
                    
                    member_count_start = first_row[4] if first_row[4] is not None else 0
                    member_count_end = last_row[4] if last_row[4] is not None else 0
                    member_count_change = member_count_end - member_count_start
                    
                    best_chain_start = first_row[5] if first_row[5] is not None else 0
                    best_chain_end = last_row[5] if last_row[5] is not None else 0
                    best_chain_change = best_chain_end - best_chain_start
                    
                    # Insert summary
                    await self.connection.execute("""
                        INSERT OR REPLACE INTO faction_summary (
                            faction_id, period_start, period_end, period_type,
                            respect_start, respect_end, respect_change,
                            member_count_start, member_count_end, member_count_change,
                            best_chain_start, best_chain_end, best_chain_change,
                            record_count
                        ) VALUES (?, ?, ?, 'monthly', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (faction_id, period_start_ts, period_end_ts,
                          respect_start, respect_end, respect_change,
                          member_count_start, member_count_end, member_count_change,
                          best_chain_start, best_chain_end, best_chain_change,
                          record_count))
                    
                    summaries_created += 1
                
                await self.connection.commit()
            except Exception as e:
                await self.connection.rollback()
                raise
        
        return summaries_created
    
    async def summarize_territory_ownership_monthly(self, year: int, month: int, force: bool = False) -> int:
        """Summarize territory ownership for a specific month.
        
        Returns:
            Number of summaries created
        """
        # Calculate period
        period_start = datetime(year, month, 1)
        if month == 12:
            period_end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            period_end = datetime(year, month + 1, 1) - timedelta(seconds=1)
        
        period_start_ts = int(period_start.timestamp())
        period_end_ts = int(period_end.timestamp())
        
        # Check if already summarized
        if not force:
            async with self.connection.execute("""
                SELECT COUNT(*) FROM territory_ownership_summary
                WHERE period_start = ? AND period_end = ? AND period_type = 'monthly'
            """, (period_start_ts, period_end_ts)) as cursor:
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    return 0  # Already summarized
        
        # Get all territories with data in this period
        async with self.connection.execute("""
            SELECT DISTINCT territory_id FROM territory_ownership_history
            WHERE recorded_at >= ? AND recorded_at <= ?
        """, (period_start_ts, period_end_ts)) as cursor:
            territory_rows = await cursor.fetchall()
        
        summaries_created = 0
        
        async with self.connection.execute("BEGIN TRANSACTION"):
            try:
                for (territory_id,) in territory_rows:
                    # Get first and last records for this period
                    async with self.connection.execute("""
                        SELECT * FROM territory_ownership_history
                        WHERE territory_id = ? AND recorded_at >= ? AND recorded_at <= ?
                        ORDER BY recorded_at ASC
                        LIMIT 1
                    """, (territory_id, period_start_ts, period_end_ts)) as cursor:
                        first_row = await cursor.fetchone()
                    
                    async with self.connection.execute("""
                        SELECT * FROM territory_ownership_history
                        WHERE territory_id = ? AND recorded_at >= ? AND recorded_at <= ?
                        ORDER BY recorded_at DESC
                        LIMIT 1
                    """, (territory_id, period_start_ts, period_end_ts)) as cursor:
                        last_row = await cursor.fetchone()
                    
                    if not first_row or not last_row:
                        continue
                    
                    # Count records and ownership changes
                    async with self.connection.execute("""
                        SELECT COUNT(*) FROM territory_ownership_history
                        WHERE territory_id = ? AND recorded_at >= ? AND recorded_at <= ?
                    """, (territory_id, period_start_ts, period_end_ts)) as cursor:
                        count_row = await cursor.fetchone()
                        record_count = count_row[0] if count_row else 0
                    
                    # Count ownership changes (where faction_id changed between consecutive records)
                    # Get all records in order and count changes
                    async with self.connection.execute("""
                        SELECT faction_id FROM territory_ownership_history
                        WHERE territory_id = ? AND recorded_at >= ? AND recorded_at <= ?
                        ORDER BY recorded_at ASC
                    """, (territory_id, period_start_ts, period_end_ts)) as cursor:
                        all_rows = await cursor.fetchall()
                    
                    ownership_changes = 0
                    prev_faction_id = None
                    for (faction_id,) in all_rows:
                        if prev_faction_id is not None and prev_faction_id != faction_id:
                            ownership_changes += 1
                        prev_faction_id = faction_id
                    
                    # Calculate days owned (simplified - assumes end faction owned for the period)
                    # Column order: id, territory_id, faction_id, sector, coordinates,
                    # racket_level, racket_type, war_status, recorded_at, data_source
                    faction_id_start = first_row[2]
                    faction_id_end = last_row[2]
                    
                    # Calculate days owned by end faction
                    days_in_period = (period_end - period_start).days
                    days_owned = days_in_period if faction_id_end else 0
                    
                    # Insert summary
                    await self.connection.execute("""
                        INSERT OR REPLACE INTO territory_ownership_summary (
                            territory_id, period_start, period_end, period_type,
                            faction_id_start, faction_id_end,
                            ownership_changes, days_owned, record_count
                        ) VALUES (?, ?, ?, 'monthly', ?, ?, ?, ?, ?)
                    """, (territory_id, period_start_ts, period_end_ts,
                          faction_id_start, faction_id_end,
                          ownership_changes, days_owned, record_count))
                    
                    summaries_created += 1
                
                await self.connection.commit()
            except Exception as e:
                await self.connection.rollback()
                raise
        
        return summaries_created
    
    async def prune_faction_history(self, older_than_days: int = 60) -> int:
        """Prune faction history older than specified days.
        
        Returns:
            Number of records deleted
        """
        return await self._prune_history_table("faction_history", "timestamp", older_than_days)
    
    async def prune_territory_ownership_history(self, older_than_days: int = 60) -> int:
        """Prune territory ownership history older than specified days.
        
        Returns:
            Number of records deleted
        """
        return await self._prune_history_table("territory_ownership_history", "recorded_at", older_than_days)
    
    async def _create_schema_v2(self):
        """Create competition tracking schema (version 2)."""
        # Competitions table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS competitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                tracked_stat TEXT NOT NULL,
                start_date INTEGER NOT NULL,
                end_date INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'cancelled', 'completed')),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                created_by TEXT NOT NULL,
                UNIQUE(name)
            )
        """)
        
        # Competition teams table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS competition_teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competition_id INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                captain_discord_id_1 TEXT NOT NULL,
                captain_discord_id_2 TEXT,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (competition_id) REFERENCES competitions(id) ON DELETE CASCADE,
                UNIQUE(competition_id, team_name)
            )
        """)
        
        # Competition participants table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS competition_participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competition_id INTEGER NOT NULL,
                team_id INTEGER,
                player_id INTEGER NOT NULL,
                discord_user_id TEXT,
                assigned_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (competition_id) REFERENCES competitions(id) ON DELETE CASCADE,
                FOREIGN KEY (team_id) REFERENCES competition_teams(id) ON DELETE SET NULL,
                FOREIGN KEY (player_id) REFERENCES players(player_id),
                UNIQUE(competition_id, player_id)
            )
        """)
        
        # Competition start stats cache table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS competition_start_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                competition_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                stat_value REAL,
                recorded_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (competition_id) REFERENCES competitions(id) ON DELETE CASCADE,
                FOREIGN KEY (player_id) REFERENCES players(player_id),
                UNIQUE(competition_id, player_id)
            )
        """)
        
        # Create indexes for competitions
        competition_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_competitions_status ON competitions(status, start_date)",
            "CREATE INDEX IF NOT EXISTS idx_competitions_dates ON competitions(start_date, end_date)",
            "CREATE INDEX IF NOT EXISTS idx_competition_teams_competition ON competition_teams(competition_id)",
            "CREATE INDEX IF NOT EXISTS idx_competition_teams_captains ON competition_teams(captain_discord_id_1, captain_discord_id_2)",
            "CREATE INDEX IF NOT EXISTS idx_competition_participants_competition ON competition_participants(competition_id)",
            "CREATE INDEX IF NOT EXISTS idx_competition_participants_team ON competition_participants(team_id)",
            "CREATE INDEX IF NOT EXISTS idx_competition_participants_player ON competition_participants(player_id)",
            "CREATE INDEX IF NOT EXISTS idx_competition_start_stats_competition ON competition_start_stats(competition_id, player_id)",
        ]
        
        for index_sql in competition_indexes:
            await self.connection.execute(index_sql)
        
        await self.connection.commit()
    
    async def _create_schema_v3(self):
        """Create schema v3: Add contributor stats support."""
        # Add stat_source column to competition_start_stats
        try:
            await self.connection.execute("""
                ALTER TABLE competition_start_stats 
                ADD COLUMN stat_source TEXT DEFAULT 'contributors'
            """)
        except aiosqlite.OperationalError:
            # Column might already exist, ignore
            pass
        
        # Create player_contributor_history table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS player_contributor_history (
                player_id INTEGER NOT NULL,
                stat_name TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                value REAL NOT NULL,
                faction_id INTEGER,
                data_source TEXT,
                PRIMARY KEY (player_id, stat_name, timestamp),
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            )
        """)
        
        # Create indexes for contributor history
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_player_contributor_history_player_stat 
            ON player_contributor_history(player_id, stat_name, timestamp DESC)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_player_contributor_history_faction 
            ON player_contributor_history(faction_id)
        """)
        
        await self.connection.commit()
    
    async def _create_schema_v4(self):
        """Create schema v4: Add bot instances and command permissions."""
        # Bot instances table (tracks Discord guilds/servers where bot is deployed)
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS bot_instances (
                guild_id TEXT PRIMARY KEY,
                guild_name TEXT,
                status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'unknown')),
                last_seen INTEGER,
                member_count INTEGER,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        # Command permissions table (maps commands to allowed users/roles per instance)
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS command_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT NOT NULL,
                command_name TEXT NOT NULL,
                permission_type TEXT NOT NULL CHECK (permission_type IN ('admin', 'role', 'user')),
                permission_value TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (guild_id) REFERENCES bot_instances(guild_id) ON DELETE CASCADE,
                UNIQUE(guild_id, command_name, permission_type, permission_value)
            )
        """)
        
        # Create indexes
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_command_permissions_guild_command 
            ON command_permissions(guild_id, command_name)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_command_permissions_type_value 
            ON command_permissions(permission_type, permission_value)
        """)
        
        await self.connection.commit()
    
    # Bot instance and permission methods
    async def get_bot_instances(self) -> List[Dict[str, Any]]:
        """Get all bot instances."""
        instances = []
        async with self.connection.execute("""
            SELECT guild_id, guild_name, status, last_seen, member_count, created_at, updated_at
            FROM bot_instances
            ORDER BY guild_name, guild_id
        """) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                instances.append({
                    'guild_id': row[0],
                    'guild_name': row[1],
                    'status': row[2],
                    'last_seen': row[3],
                    'member_count': row[4],
                    'created_at': row[5],
                    'updated_at': row[6]
                })
        return instances
    
    async def upsert_bot_instance(
        self,
        guild_id: str,
        guild_name: Optional[str] = None,
        status: str = 'active',
        member_count: Optional[int] = None,
        bot_online: bool = True
    ):
        """Create or update a bot instance."""
        import time
        current_time = int(time.time())
        
        # If bot_online is False, mark as inactive
        if not bot_online:
            status = 'inactive'
        
        await self.connection.execute("""
            INSERT INTO bot_instances (guild_id, guild_name, status, last_seen, member_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                guild_name = COALESCE(excluded.guild_name, bot_instances.guild_name),
                status = excluded.status,
                last_seen = excluded.last_seen,
                member_count = COALESCE(excluded.member_count, bot_instances.member_count),
                updated_at = excluded.updated_at
        """, (guild_id, guild_name, status, current_time, member_count, current_time))
        await self.connection.commit()
    
    async def update_bot_heartbeat(self, guild_ids: List[str] = None):
        """Update heartbeat timestamp for specified guilds or all active guilds.
        
        Args:
            guild_ids: Optional list of guild IDs to update. If None, updates all active guilds.
        """
        # Ensure connection is established
        if self.connection is None:
            return  # Silently skip if connection not available
        
        import time
        current_time = int(time.time())
        
        if guild_ids:
            # Update specified guilds
            for guild_id in guild_ids:
                await self.connection.execute("""
                    UPDATE bot_instances 
                    SET last_seen = ?, updated_at = ?, status = 'active'
                    WHERE guild_id = ?
                """, (current_time, current_time, guild_id))
        else:
            # Update all active guilds
            async with self.connection.execute("""
                SELECT guild_id FROM bot_instances WHERE status != 'inactive'
            """) as cursor:
                guilds = await cursor.fetchall()
                for (guild_id,) in guilds:
                    await self.connection.execute("""
                        UPDATE bot_instances 
                        SET last_seen = ?, updated_at = ?, status = 'active'
                        WHERE guild_id = ?
                    """, (current_time, current_time, guild_id))
        await self.connection.commit()
    
    async def get_command_permissions(self, guild_id: str) -> Dict[str, List[Dict[str, str]]]:
        """Get all command permissions for a guild, organized by command."""
        # Ensure connection is established
        await self._ensure_connected()
        
        permissions = {}
        async with self.connection.execute("""
            SELECT command_name, permission_type, permission_value
            FROM command_permissions
            WHERE guild_id = ?
            ORDER BY command_name, permission_type, permission_value
        """, (guild_id,)) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                command_name, perm_type, perm_value = row
                if command_name not in permissions:
                    permissions[command_name] = []
                permissions[command_name].append({
                    'type': perm_type,
                    'value': perm_value
                })
        return permissions
    
    async def set_command_permission(
        self,
        guild_id: str,
        command_name: str,
        permission_type: str,
        permission_value: str
    ):
        """Add a permission for a command."""
        await self.connection.execute("""
            INSERT OR IGNORE INTO command_permissions (guild_id, command_name, permission_type, permission_value)
            VALUES (?, ?, ?, ?)
        """, (guild_id, command_name, permission_type, permission_value))
        await self.connection.commit()
    
    async def remove_command_permission(
        self,
        guild_id: str,
        command_name: str,
        permission_type: str,
        permission_value: str
    ):
        """Remove a permission for a command."""
        await self.connection.execute("""
            DELETE FROM command_permissions
            WHERE guild_id = ? AND command_name = ? AND permission_type = ? AND permission_value = ?
        """, (guild_id, command_name, permission_type, permission_value))
        await self.connection.commit()
    
    async def clear_command_permissions(self, guild_id: str, command_name: str):
        """Clear all permissions for a command."""
        await self.connection.execute("""
            DELETE FROM command_permissions
            WHERE guild_id = ? AND command_name = ?
        """, (guild_id, command_name))
        await self.connection.commit()
    
    async def _create_schema_v5(self):
        """Create schema v5: Add organized crime tracking tables."""
        # Current organized crimes table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS organized_crimes_current (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                crime_id INTEGER NOT NULL,
                crime_name TEXT NOT NULL,
                crime_type TEXT,
                participants TEXT NOT NULL,
                participant_count INTEGER NOT NULL,
                required_participants INTEGER,
                time_started INTEGER,
                time_completed INTEGER,
                status TEXT NOT NULL CHECK (status IN ('planning', 'ready', 'in_progress', 'completed', 'failed', 'cancelled')),
                reward_money INTEGER,
                reward_respect INTEGER,
                reward_other TEXT,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                data_source TEXT,
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id),
                UNIQUE(faction_id, crime_id)
            )
        """)
        
        # Organized crimes history table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS organized_crimes_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                crime_id INTEGER NOT NULL,
                event_type TEXT NOT NULL CHECK (event_type IN ('created', 'participant_joined', 'participant_left', 'status_changed', 'completed', 'failed', 'cancelled')),
                event_timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                player_id INTEGER,
                old_status TEXT,
                new_status TEXT,
                old_participants TEXT,
                new_participants TEXT,
                reward_money INTEGER,
                reward_respect INTEGER,
                reward_other TEXT,
                metadata TEXT,
                data_source TEXT,
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            )
        """)
        
        # Organized crimes configuration table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS organized_crimes_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                guild_id TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT 1,
                notification_channel_id TEXT,
                frequent_leaver_threshold INTEGER NOT NULL DEFAULT 2,
                tracking_window_days INTEGER NOT NULL DEFAULT 30,
                faction_lead_discord_ids TEXT,
                auto_sync_enabled BOOLEAN NOT NULL DEFAULT 1,
                last_sync INTEGER,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id),
                UNIQUE(faction_id, guild_id)
            )
        """)
        
        # Organized crimes participant stats table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS organized_crimes_participant_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                faction_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                crime_type TEXT,
                crimes_started INTEGER NOT NULL DEFAULT 0,
                crimes_completed INTEGER NOT NULL DEFAULT 0,
                crimes_failed INTEGER NOT NULL DEFAULT 0,
                crimes_left INTEGER NOT NULL DEFAULT 0,
                total_reward_money INTEGER NOT NULL DEFAULT 0,
                total_reward_respect INTEGER NOT NULL DEFAULT 0,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (faction_id) REFERENCES factions(faction_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id),
                UNIQUE(faction_id, player_id, crime_type, period_start, period_end)
            )
        """)
        
        # Create indexes
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_current_faction 
            ON organized_crimes_current(faction_id)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_current_status 
            ON organized_crimes_current(status)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_history_faction_crime 
            ON organized_crimes_history(faction_id, crime_id)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_history_player 
            ON organized_crimes_history(player_id)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_history_event_type 
            ON organized_crimes_history(event_type, event_timestamp)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_config_faction 
            ON organized_crimes_config(faction_id)
        """)
        
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_oc_stats_faction_player 
            ON organized_crimes_participant_stats(faction_id, player_id)
        """)
        
        await self.connection.commit()
    
    async def _create_schema_v6(self):
        """Create schema version 6: Add missing item reminders, items cache, and discord_id to players."""
        # Add missing_item_reminder_channel_id to organized_crimes_config
        try:
            await self.connection.execute("""
                ALTER TABLE organized_crimes_config
                ADD COLUMN missing_item_reminder_channel_id TEXT
            """)
        except aiosqlite.OperationalError as e:
            # Column might already exist
            if "duplicate column" not in str(e).lower():
                raise
        
        # Create items cache table
        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                type TEXT,
                market_value INTEGER,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        # Add discord_id column to players table
        try:
            await self.connection.execute("""
                ALTER TABLE players
                ADD COLUMN discord_id TEXT
            """)
        except aiosqlite.OperationalError as e:
            # Column might already exist
            if "duplicate column" not in str(e).lower():
                raise
        
        # Create index on items table
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_items_item_id 
            ON items(item_id)
        """)
        
        # Create index on players discord_id
        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_players_discord_id 
            ON players(discord_id)
        """)
        
        await self.connection.commit()
    
    # Competition methods
    async def create_competition(
        self,
        name: str,
        tracked_stat: str,
        start_date: int,
        end_date: int,
        created_by: str
    ) -> int:
        """Create a new competition.
        
        Returns:
            Competition ID
        """
        cursor = await self.connection.execute("""
            INSERT INTO competitions (name, tracked_stat, start_date, end_date, created_by)
            VALUES (?, ?, ?, ?, ?)
        """, (name, tracked_stat, start_date, end_date, created_by))
        await self.connection.commit()
        return cursor.lastrowid
    
    async def create_competition_team(
        self,
        competition_id: int,
        team_name: str,
        captain_discord_id_1: str,
        captain_discord_id_2: Optional[str] = None
    ) -> int:
        """Create a team for a competition.
        
        Returns:
            Team ID
        """
        cursor = await self.connection.execute("""
            INSERT INTO competition_teams (competition_id, team_name, captain_discord_id_1, captain_discord_id_2)
            VALUES (?, ?, ?, ?)
        """, (competition_id, team_name, captain_discord_id_1, captain_discord_id_2))
        await self.connection.commit()
        return cursor.lastrowid
    
    async def add_competition_participant(
        self,
        competition_id: int,
        player_id: int,
        team_id: Optional[int] = None,
        discord_user_id: Optional[str] = None
    ):
        """Add a participant to a competition."""
        await self.connection.execute("""
            INSERT OR REPLACE INTO competition_participants
            (competition_id, team_id, player_id, discord_user_id)
            VALUES (?, ?, ?, ?)
        """, (competition_id, team_id, player_id, discord_user_id))
        await self.connection.commit()
    
    async def set_competition_start_stat(
        self,
        competition_id: int,
        player_id: int,
        stat_value: Optional[float],
        stat_source: str = "contributors"
    ):
        """Set or update the starting stat value for a competition participant."""
        await self.connection.execute("""
            INSERT OR REPLACE INTO competition_start_stats
            (competition_id, player_id, stat_value, stat_source)
            VALUES (?, ?, ?, ?)
        """, (competition_id, player_id, stat_value, stat_source))
        await self.connection.commit()
    
    async def get_competition(self, competition_id: int) -> Optional[Dict[str, Any]]:
        """Get competition details."""
        async with self.connection.execute("""
            SELECT id, name, tracked_stat, start_date, end_date, status, created_at, created_by
            FROM competitions WHERE id = ?
        """, (competition_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "tracked_stat": row[2],
                "start_date": row[3],
                "end_date": row[4],
                "status": row[5],
                "created_at": row[6],
                "created_by": row[7]
            }
    
    async def get_competition_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get competition by name."""
        async with self.connection.execute("""
            SELECT id, name, tracked_stat, start_date, end_date, status, created_at, created_by
            FROM competitions WHERE name = ?
        """, (name,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "tracked_stat": row[2],
                "start_date": row[3],
                "end_date": row[4],
                "status": row[5],
                "created_at": row[6],
                "created_by": row[7]
            }
    
    async def list_competitions(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List competitions, optionally filtered by status."""
        if status:
            async with self.connection.execute("""
                SELECT id, name, tracked_stat, start_date, end_date, status, created_at, created_by
                FROM competitions WHERE status = ? ORDER BY start_date DESC
            """, (status,)) as cursor:
                rows = await cursor.fetchall()
        else:
            async with self.connection.execute("""
                SELECT id, name, tracked_stat, start_date, end_date, status, created_at, created_by
                FROM competitions ORDER BY start_date DESC
            """) as cursor:
                rows = await cursor.fetchall()
        
        return [
            {
                "id": row[0],
                "name": row[1],
                "tracked_stat": row[2],
                "start_date": row[3],
                "end_date": row[4],
                "status": row[5],
                "created_at": row[6],
                "created_by": row[7]
            }
            for row in rows
        ]
    
    async def cancel_competition(self, competition_id: int):
        """Cancel a competition."""
        await self.connection.execute("""
            UPDATE competitions SET status = 'cancelled' WHERE id = ?
        """, (competition_id,))
        await self.connection.commit()
    
    async def get_competition_teams(self, competition_id: int) -> List[Dict[str, Any]]:
        """Get all teams for a competition."""
        async with self.connection.execute("""
            SELECT id, competition_id, team_name, captain_discord_id_1, captain_discord_id_2, created_at
            FROM competition_teams WHERE competition_id = ?
        """, (competition_id,)) as cursor:
            rows = await cursor.fetchall()
        
        return [
            {
                "id": row[0],
                "competition_id": row[1],
                "team_name": row[2],
                "captain_discord_id_1": row[3],
                "captain_discord_id_2": row[4],
                "created_at": row[5]
            }
            for row in rows
        ]
    
    async def get_competition_participants(self, competition_id: int) -> List[Dict[str, Any]]:
        """Get all participants for a competition."""
        async with self.connection.execute("""
            SELECT cp.id, cp.competition_id, cp.team_id, cp.player_id, cp.discord_user_id, cp.assigned_at,
                   p.name as player_name
            FROM competition_participants cp
            LEFT JOIN players p ON cp.player_id = p.player_id
            WHERE cp.competition_id = ?
        """, (competition_id,)) as cursor:
            rows = await cursor.fetchall()
        
        return [
            {
                "id": row[0],
                "competition_id": row[1],
                "team_id": row[2],
                "player_id": row[3],
                "discord_user_id": row[4],
                "assigned_at": row[5],
                "player_name": row[6]
            }
            for row in rows
        ]
    
    async def get_player_current_stat_value(
        self,
        player_id: int,
        stat_name: str
    ) -> Optional[float]:
        """Get the most recent value for a stat from contributor history or player_stats_history."""
        # First try contributor history (for contributor-based stats)
        async with self.connection.execute("""
            SELECT value FROM player_contributor_history
            WHERE player_id = ? AND stat_name = ?
            ORDER BY timestamp DESC LIMIT 1
        """, (player_id, stat_name)) as cursor:
            row = await cursor.fetchone()
            if row and row[0] is not None:
                return float(row[0])
        
        # Fallback to old player_stats_history for backward compatibility
        # (though we're removing user endpoint stats, keeping this for now)
        stat_column_map = {
            "strength": "strength",
            "defense": "defense",
            "speed": "speed",
            "dexterity": "dexterity",
            "total_stats": "total_stats",
            "level": "level",
            "life_maximum": "life_maximum",
            "networth": "networth"
        }
        
        if stat_name in stat_column_map:
            column = stat_column_map[stat_name]
            async with self.connection.execute(f"""
                SELECT {column} FROM player_stats_history
                WHERE player_id = ? AND {column} IS NOT NULL
                ORDER BY timestamp DESC LIMIT 1
            """, (player_id,)) as cursor:
                row = await cursor.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
        
        # Fallback to players table for level
        if stat_name == "level":
            async with self.connection.execute("""
                SELECT level FROM players WHERE player_id = ?
            """, (player_id,)) as cursor:
                row = await cursor.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
        
        return None
    
    async def get_competition_start_stat(
        self,
        competition_id: int,
        player_id: int
    ) -> Optional[float]:
        """Get the starting stat value for a participant."""
        async with self.connection.execute("""
            SELECT stat_value FROM competition_start_stats
            WHERE competition_id = ? AND player_id = ?
        """, (competition_id, player_id)) as cursor:
            row = await cursor.fetchone()
            if row:
                return row[0] if row[0] is not None else None
            return None
    
    async def update_participant_team(
        self,
        competition_id: int,
        player_id: int,
        team_id: Optional[int]
    ):
        """Update a participant's team assignment."""
        await self.connection.execute("""
            UPDATE competition_participants SET team_id = ?
            WHERE competition_id = ? AND player_id = ?
        """, (team_id, competition_id, player_id))
        await self.connection.commit()
    
    async def update_team_captains(
        self,
        team_id: int,
        captain_discord_id_1: str,
        captain_discord_id_2: Optional[str] = None
    ):
        """Update team captains."""
        await self.connection.execute("""
            UPDATE competition_teams
            SET captain_discord_id_1 = ?, captain_discord_id_2 = ?
            WHERE id = ?
        """, (captain_discord_id_1, captain_discord_id_2, team_id))
        await self.connection.commit()
    
    async def get_table_info(self) -> Dict[str, List[str]]:
        """Get list of available tables and their columns.
        
        Returns:
            Dict mapping table names to list of column names
        """
        table_info = {}
        async with self.connection.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """) as cursor:
            tables = await cursor.fetchall()
            
            for (table_name,) in tables:
                async with self.connection.execute(f"PRAGMA table_info({table_name})") as col_cursor:
                    columns = await col_cursor.fetchall()
                    table_info[table_name] = [col[1] for col in columns]  # col[1] is column name
        
        return table_info
    
    async def query_table(
        self,
        table_name: str,
        limit: int = 100,
        offset: int = 0,
        order_by: Optional[str] = None,
        where_clause: Optional[str] = None,
        where_params: Optional[tuple] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Safely query a table with pagination.
        
        Args:
            table_name: Name of the table to query (must be a valid table name)
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            order_by: Column name to order by (optional)
            where_clause: WHERE clause (without WHERE keyword, optional)
            where_params: Parameters for WHERE clause (optional)
            
        Returns:
            Tuple of (list of row dicts, total count)
        """
        # Whitelist of allowed table names for safety
        allowed_tables = {
            'players', 'factions', 'player_stats_history', 'faction_history',
            'war_status_history', 'territory_ownership_history',
            'player_stats_summary', 'faction_summary', 'war_summary',
            'territory_ownership_summary', 'competitions', 'competition_participants',
            'competition_teams', 'competition_start_stats', 'competition_stats'
        }
        
        if table_name not in allowed_tables:
            raise ValueError(f"Table '{table_name}' is not allowed or doesn't exist")
        
        # Build query safely
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        data_query = f"SELECT * FROM {table_name}"
        
        if where_clause:
            count_query += f" WHERE {where_clause}"
            data_query += f" WHERE {where_clause}"
        
        # Get total count
        params = where_params or ()
        async with self.connection.execute(count_query, params) as cursor:
            total_count = (await cursor.fetchone())[0]
        
        # Add ordering
        if order_by:
            # Validate order_by column (basic check)
            if not order_by.replace('_', '').replace('-', '').replace(' ', '').isalnum():
                raise ValueError("Invalid order_by column name")
            data_query += f" ORDER BY {order_by}"
        
        # Add pagination
        data_query += f" LIMIT ? OFFSET ?"
        
        params = (where_params or ()) + (limit, offset)
        
        # Execute query
        async with self.connection.execute(data_query, params) as cursor:
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            result = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Convert timestamps to readable format
                    if col in ['timestamp', 'recorded_at', 'created_at', 'last_updated', 
                              'started_at', 'ends_at', 'start_date', 'end_date', 'period_start', 'period_end']:
                        if value:
                            try:
                                value = datetime.fromtimestamp(value).isoformat()
                            except (ValueError, TypeError):
                                pass
                    row_dict[col] = value
                result.append(row_dict)
        
        return result, total_count
    
    # Organized crime methods
    async def upsert_organized_crime_current(
        self,
        faction_id: int,
        crime_id: int,
        crime_name: str,
        crime_type: Optional[str] = None,
        participants: List[int] = None,
        participant_count: Optional[int] = None,
        required_participants: Optional[int] = None,
        time_started: Optional[int] = None,
        time_completed: Optional[int] = None,
        status: str = 'planning',
        reward_money: Optional[int] = None,
        reward_respect: Optional[int] = None,
        reward_other: Optional[str] = None,
        data_source: Optional[str] = None
    ):
        """Upsert current organized crime state."""
        await self._ensure_connected()
        now = int(datetime.utcnow().timestamp())
        
        if participants is None:
            participants = []
        if participant_count is None:
            participant_count = len(participants)
        
        participants_json = json.dumps(participants)
        
        await self.connection.execute("""
            INSERT INTO organized_crimes_current (
                faction_id, crime_id, crime_name, crime_type, participants, participant_count,
                required_participants, time_started, time_completed, status,
                reward_money, reward_respect, reward_other, last_updated, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(faction_id, crime_id) DO UPDATE SET
                crime_name = excluded.crime_name,
                crime_type = COALESCE(excluded.crime_type, organized_crimes_current.crime_type),
                participants = excluded.participants,
                participant_count = excluded.participant_count,
                required_participants = COALESCE(excluded.required_participants, organized_crimes_current.required_participants),
                time_started = COALESCE(excluded.time_started, organized_crimes_current.time_started),
                time_completed = COALESCE(excluded.time_completed, organized_crimes_current.time_completed),
                status = excluded.status,
                reward_money = COALESCE(excluded.reward_money, organized_crimes_current.reward_money),
                reward_respect = COALESCE(excluded.reward_respect, organized_crimes_current.reward_respect),
                reward_other = COALESCE(excluded.reward_other, organized_crimes_current.reward_other),
                last_updated = excluded.last_updated,
                data_source = COALESCE(excluded.data_source, organized_crimes_current.data_source)
        """, (faction_id, crime_id, crime_name, crime_type, participants_json, participant_count,
              required_participants, time_started, time_completed, status,
              reward_money, reward_respect, reward_other, now, data_source))
        await self.connection.commit()
    
    async def get_organized_crimes_current(self, faction_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get current organized crimes, optionally filtered by faction."""
        await self._ensure_connected()
        
        if faction_id:
            query = """
                SELECT id, faction_id, crime_id, crime_name, crime_type, participants, participant_count,
                       required_participants, time_started, time_completed, status,
                       reward_money, reward_respect, reward_other, created_at, last_updated, data_source
                FROM organized_crimes_current
                WHERE faction_id = ?
                ORDER BY time_started DESC, crime_id DESC
            """
            params = (faction_id,)
        else:
            query = """
                SELECT id, faction_id, crime_id, crime_name, crime_type, participants, participant_count,
                       required_participants, time_started, time_completed, status,
                       reward_money, reward_respect, reward_other, created_at, last_updated, data_source
                FROM organized_crimes_current
                ORDER BY faction_id, time_started DESC, crime_id DESC
            """
            params = ()
        
        crimes = []
        async with self.connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                participants_json = row[5]
                try:
                    participants = json.loads(participants_json) if participants_json else []
                except (json.JSONDecodeError, TypeError):
                    participants = []
                
                crimes.append({
                    'id': row[0],
                    'faction_id': row[1],
                    'crime_id': row[2],
                    'crime_name': row[3],
                    'crime_type': row[4],
                    'participants': participants,
                    'participant_count': row[6],
                    'required_participants': row[7],
                    'time_started': row[8],
                    'time_completed': row[9],
                    'status': row[10],
                    'reward_money': row[11],
                    'reward_respect': row[12],
                    'reward_other': row[13],
                    'created_at': row[14],
                    'last_updated': row[15],
                    'data_source': row[16]
                })
        return crimes
    
    async def delete_organized_crime_current(self, faction_id: int, crime_id: int):
        """Remove a crime from current table (after completion/failure)."""
        await self._ensure_connected()
        await self.connection.execute("""
            DELETE FROM organized_crimes_current
            WHERE faction_id = ? AND crime_id = ?
        """, (faction_id, crime_id))
        await self.connection.commit()
    
    async def append_organized_crime_history(
        self,
        faction_id: int,
        crime_id: int,
        event_type: str,
        player_id: Optional[int] = None,
        old_status: Optional[str] = None,
        new_status: Optional[str] = None,
        old_participants: Optional[List[int]] = None,
        new_participants: Optional[List[int]] = None,
        reward_money: Optional[int] = None,
        reward_respect: Optional[int] = None,
        reward_other: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        data_source: Optional[str] = None,
        event_timestamp: Optional[int] = None
    ):
        """Append organized crime history event."""
        await self._ensure_connected()
        
        if event_timestamp is None:
            event_timestamp = int(datetime.utcnow().timestamp())
        
        old_participants_json = json.dumps(old_participants) if old_participants else None
        new_participants_json = json.dumps(new_participants) if new_participants else None
        metadata_json = json.dumps(metadata) if metadata else None
        
        await self.connection.execute("""
            INSERT INTO organized_crimes_history (
                faction_id, crime_id, event_type, event_timestamp, player_id,
                old_status, new_status, old_participants, new_participants,
                reward_money, reward_respect, reward_other, metadata, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (faction_id, crime_id, event_type, event_timestamp, player_id,
              old_status, new_status, old_participants_json, new_participants_json,
              reward_money, reward_respect, reward_other, metadata_json, data_source))
        await self.connection.commit()
    
    async def get_organized_crime_history(
        self,
        faction_id: int,
        crime_id: Optional[int] = None,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get organized crime history."""
        await self._ensure_connected()
        
        conditions = ["faction_id = ?"]
        params = [faction_id]
        
        if crime_id:
            conditions.append("crime_id = ?")
            params.append(crime_id)
        
        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT id, faction_id, crime_id, event_type, event_timestamp, player_id,
                   old_status, new_status, old_participants, new_participants,
                   reward_money, reward_respect, reward_other, metadata, data_source
            FROM organized_crimes_history
            WHERE {where_clause}
            ORDER BY event_timestamp DESC
            LIMIT ?
        """
        params.append(limit)
        
        history = []
        async with self.connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                old_participants = None
                new_participants = None
                metadata = None
                
                if row[8]:
                    try:
                        old_participants = json.loads(row[8])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                if row[9]:
                    try:
                        new_participants = json.loads(row[9])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                if row[13]:
                    try:
                        metadata = json.loads(row[13])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                history.append({
                    'id': row[0],
                    'faction_id': row[1],
                    'crime_id': row[2],
                    'event_type': row[3],
                    'event_timestamp': row[4],
                    'player_id': row[5],
                    'old_status': row[6],
                    'new_status': row[7],
                    'old_participants': old_participants,
                    'new_participants': new_participants,
                    'reward_money': row[10],
                    'reward_respect': row[11],
                    'reward_other': row[12],
                    'metadata': metadata,
                    'data_source': row[14]
                })
        return history
    
    async def get_participant_crime_leaves(
        self,
        faction_id: int,
        player_id: int,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get crimes a participant left within the time window."""
        await self._ensure_connected()
        cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        
        query = """
            SELECT id, faction_id, crime_id, event_type, event_timestamp, player_id,
                   old_status, new_status, old_participants, new_participants, metadata
            FROM organized_crimes_history
            WHERE faction_id = ? AND player_id = ? AND event_type = 'participant_left'
            AND event_timestamp >= ?
            ORDER BY event_timestamp DESC
        """
        
        leaves = []
        async with self.connection.execute(query, (faction_id, player_id, cutoff_timestamp)) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                old_participants = None
                new_participants = None
                metadata = None
                
                if row[8]:
                    try:
                        old_participants = json.loads(row[8])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                if row[9]:
                    try:
                        new_participants = json.loads(row[9])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                if row[10]:
                    try:
                        metadata = json.loads(row[10])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                leaves.append({
                    'id': row[0],
                    'faction_id': row[1],
                    'crime_id': row[2],
                    'event_type': row[3],
                    'event_timestamp': row[4],
                    'player_id': row[5],
                    'old_status': row[6],
                    'new_status': row[7],
                    'old_participants': old_participants,
                    'new_participants': new_participants,
                    'metadata': metadata
                })
        return leaves
    
    async def get_organized_crime_config(
        self,
        faction_id: int,
        guild_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get organized crime configuration for a faction/guild."""
        await self._ensure_connected()
        
        async with self.connection.execute("""
            SELECT id, faction_id, guild_id, enabled, notification_channel_id,
                   frequent_leaver_threshold, tracking_window_days, faction_lead_discord_ids,
                   auto_sync_enabled, last_sync, missing_item_reminder_channel_id,
                   created_at, last_updated
            FROM organized_crimes_config
            WHERE faction_id = ? AND guild_id = ?
        """, (faction_id, guild_id)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            
            lead_ids = None
            if row[7]:
                try:
                    lead_ids = json.loads(row[7])
                except (json.JSONDecodeError, TypeError):
                    pass
            
            return {
                'id': row[0],
                'faction_id': row[1],
                'guild_id': row[2],
                'enabled': bool(row[3]),
                'notification_channel_id': row[4],
                'frequent_leaver_threshold': row[5],
                'tracking_window_days': row[6],
                'faction_lead_discord_ids': lead_ids or [],
                'auto_sync_enabled': bool(row[8]),
                'last_sync': row[9],
                'missing_item_reminder_channel_id': row[10],
                'created_at': row[11],
                'last_updated': row[12]
            }
    
    async def upsert_organized_crime_config(
        self,
        faction_id: int,
        guild_id: str,
        enabled: Optional[bool] = None,
        notification_channel_id: Optional[str] = None,
        frequent_leaver_threshold: Optional[int] = None,
        tracking_window_days: Optional[int] = None,
        faction_lead_discord_ids: Optional[List[str]] = None,
        auto_sync_enabled: Optional[bool] = None,
        missing_item_reminder_channel_id: Optional[str] = None
    ):
        """Create or update organized crime configuration."""
        await self._ensure_connected()
        now = int(datetime.utcnow().timestamp())
        
        lead_ids_json = json.dumps(faction_lead_discord_ids) if faction_lead_discord_ids else None
        
        # Get existing config to preserve values
        existing = await self.get_organized_crime_config(faction_id, guild_id)
        
        if existing:
            # Update existing
            final_enabled = enabled if enabled is not None else existing['enabled']
            final_channel = notification_channel_id if notification_channel_id is not None else existing['notification_channel_id']
            final_threshold = frequent_leaver_threshold if frequent_leaver_threshold is not None else existing['frequent_leaver_threshold']
            final_window = tracking_window_days if tracking_window_days is not None else existing['tracking_window_days']
            final_leads = lead_ids_json if faction_lead_discord_ids is not None else json.dumps(existing['faction_lead_discord_ids'])
            final_auto_sync = auto_sync_enabled if auto_sync_enabled is not None else existing['auto_sync_enabled']
            final_missing_item_channel = missing_item_reminder_channel_id if missing_item_reminder_channel_id is not None else existing.get('missing_item_reminder_channel_id')
            
            await self.connection.execute("""
                UPDATE organized_crimes_config SET
                    enabled = ?,
                    notification_channel_id = ?,
                    frequent_leaver_threshold = ?,
                    tracking_window_days = ?,
                    faction_lead_discord_ids = ?,
                    auto_sync_enabled = ?,
                    missing_item_reminder_channel_id = ?,
                    last_updated = ?
                WHERE faction_id = ? AND guild_id = ?
            """, (final_enabled, final_channel, final_threshold, final_window, final_leads,
                  final_auto_sync, final_missing_item_channel, now, faction_id, guild_id))
        else:
            # Insert new
            final_enabled = enabled if enabled is not None else True
            final_channel = notification_channel_id
            final_threshold = frequent_leaver_threshold if frequent_leaver_threshold is not None else 2
            final_window = tracking_window_days if tracking_window_days is not None else 30
            final_leads = lead_ids_json
            final_auto_sync = auto_sync_enabled if auto_sync_enabled is not None else True
            final_missing_item_channel = missing_item_reminder_channel_id
            
            await self.connection.execute("""
                INSERT INTO organized_crimes_config (
                    faction_id, guild_id, enabled, notification_channel_id,
                    frequent_leaver_threshold, tracking_window_days, faction_lead_discord_ids,
                    auto_sync_enabled, missing_item_reminder_channel_id, created_at, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (faction_id, guild_id, final_enabled, final_channel, final_threshold,
                  final_window, final_leads, final_auto_sync, final_missing_item_channel, now, now))
        
        await self.connection.commit()
    
    async def update_organized_crime_config_sync_time(
        self,
        faction_id: int,
        guild_id: str,
        sync_timestamp: int
    ):
        """Update last sync timestamp for a config."""
        await self._ensure_connected()
        await self.connection.execute("""
            UPDATE organized_crimes_config
            SET last_sync = ?, last_updated = ?
            WHERE faction_id = ? AND guild_id = ?
        """, (sync_timestamp, sync_timestamp, faction_id, guild_id))
        await self.connection.commit()
    
    async def get_all_tracked_factions(self) -> List[Dict[str, Any]]:
        """Get all factions with organized crime tracking enabled."""
        await self._ensure_connected()
        
        factions = []
        async with self.connection.execute("""
            SELECT id, faction_id, guild_id, enabled, notification_channel_id,
                   frequent_leaver_threshold, tracking_window_days, faction_lead_discord_ids,
                   auto_sync_enabled, last_sync, missing_item_reminder_channel_id,
                   created_at, last_updated
            FROM organized_crimes_config
            WHERE enabled = 1 AND auto_sync_enabled = 1
            ORDER BY faction_id
        """) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                lead_ids = None
                if row[7]:
                    try:
                        lead_ids = json.loads(row[7])
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                factions.append({
                    'id': row[0],
                    'faction_id': row[1],
                    'guild_id': row[2],
                    'enabled': bool(row[3]),
                    'notification_channel_id': row[4],
                    'frequent_leaver_threshold': row[5],
                    'tracking_window_days': row[6],
                    'faction_lead_discord_ids': lead_ids or [],
                    'auto_sync_enabled': bool(row[8]),
                    'last_sync': row[9],
                    'missing_item_reminder_channel_id': row[10],
                    'created_at': row[11],
                    'last_updated': row[12]
                })
        return factions
    
    async def update_participant_crime_stats(
        self,
        faction_id: int,
        player_id: int,
        crime_type: Optional[str],
        crimes_started: int = 0,
        crimes_completed: int = 0,
        crimes_failed: int = 0,
        crimes_left: int = 0,
        total_reward_money: int = 0,
        total_reward_respect: int = 0,
        period_start: Optional[int] = None,
        period_end: Optional[int] = None
    ):
        """Update participant crime statistics."""
        await self._ensure_connected()
        
        # Default to current month if not specified
        if period_start is None or period_end is None:
            now = datetime.utcnow()
            period_start = int(datetime(now.year, now.month, 1).timestamp())
            if now.month == 12:
                period_end = int(datetime(now.year + 1, 1, 1).timestamp()) - 1
            else:
                period_end = int(datetime(now.year, now.month + 1, 1).timestamp()) - 1
        
        await self.connection.execute("""
            INSERT INTO organized_crimes_participant_stats (
                faction_id, player_id, crime_type, crimes_started, crimes_completed,
                crimes_failed, crimes_left, total_reward_money, total_reward_respect,
                period_start, period_end, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
            ON CONFLICT(faction_id, player_id, crime_type, period_start, period_end) DO UPDATE SET
                crimes_started = crimes_started + excluded.crimes_started,
                crimes_completed = crimes_completed + excluded.crimes_completed,
                crimes_failed = crimes_failed + excluded.crimes_failed,
                crimes_left = crimes_left + excluded.crimes_left,
                total_reward_money = total_reward_money + excluded.total_reward_money,
                total_reward_respect = total_reward_respect + excluded.total_reward_respect,
                last_updated = excluded.last_updated
        """, (faction_id, player_id, crime_type, crimes_started, crimes_completed,
              crimes_failed, crimes_left, total_reward_money, total_reward_respect,
              period_start, period_end))
        await self.connection.commit()
    
    async def get_frequent_leavers(
        self,
        faction_id: int,
        threshold: int = 2,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get players who have left more than threshold crimes in the time window."""
        await self._ensure_connected()
        cutoff_timestamp = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        
        query = """
            SELECT player_id, COUNT(*) as leave_count
            FROM organized_crimes_history
            WHERE faction_id = ? AND event_type = 'participant_left'
            AND event_timestamp >= ?
            GROUP BY player_id
            HAVING leave_count > ?
            ORDER BY leave_count DESC
        """
        
        leavers = []
        async with self.connection.execute(query, (faction_id, cutoff_timestamp, threshold)) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                leavers.append({
                    'player_id': row[0],
                    'leave_count': row[1]
                })
        return leavers
    
    # Item cache methods
    async def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Get item from cache by item_id."""
        await self._ensure_connected()
        
        async with self.connection.execute("""
            SELECT item_id, name, description, type, market_value, created_at, last_updated
            FROM items
            WHERE item_id = ?
        """, (item_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            
            return {
                'item_id': row[0],
                'name': row[1],
                'description': row[2],
                'type': row[3],
                'market_value': row[4],
                'created_at': row[5],
                'last_updated': row[6]
            }
    
    async def upsert_item(
        self,
        item_id: int,
        name: str,
        description: Optional[str] = None,
        item_type: Optional[str] = None,
        market_value: Optional[int] = None
    ):
        """Insert or update item in cache."""
        await self._ensure_connected()
        now = int(datetime.utcnow().timestamp())
        
        await self.connection.execute("""
            INSERT INTO items (item_id, name, description, type, market_value, created_at, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                type = excluded.type,
                market_value = excluded.market_value,
                last_updated = excluded.last_updated
        """, (item_id, name, description, item_type, market_value, now, now))
        await self.connection.commit()
    
    # Player discord_id methods
    async def get_player_discord_id(self, player_id: int) -> Optional[str]:
        """Get discord_id for a player from players table."""
        await self._ensure_connected()
        
        async with self.connection.execute("""
            SELECT discord_id FROM players WHERE player_id = ?
        """, (player_id,)) as cursor:
            row = await cursor.fetchone()
            if not row or not row[0]:
                return None
            return row[0]
    
    async def update_player_discord_id(self, player_id: int, discord_id: Optional[str]):
        """Update discord_id for a player."""
        await self._ensure_connected()
        now = int(datetime.utcnow().timestamp())
        
        # First check if player exists
        async with self.connection.execute("""
            SELECT player_id FROM players WHERE player_id = ?
        """, (player_id,)) as cursor:
            exists = await cursor.fetchone()
        
        if exists:
            # Update existing player
            await self.connection.execute("""
                UPDATE players SET discord_id = ?, last_updated = ?
                WHERE player_id = ?
            """, (discord_id, now, player_id))
        else:
            # Insert new player with minimal data (name required)
            await self.connection.execute("""
                INSERT INTO players (player_id, name, discord_id, created_at, last_updated)
                VALUES (?, ?, ?, ?, ?)
            """, (player_id, f"Player {player_id}", discord_id, now, now))
        
        await self.connection.commit()
