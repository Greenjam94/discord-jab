"""Database manager for Torn API data storage."""

import aiosqlite
import os
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json


class TornDatabase:
    """Manages SQLite database for Torn API data."""
    
    CURRENT_SCHEMA_VERSION = 1
    
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
    
    async def _initialize_schema(self):
        """Create database schema if it doesn't exist."""
        current_version = await self._get_schema_version()
        
        if current_version < 1:
            await self._create_schema_v1()
            await self._set_schema_version(1)
    
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
    
    async def prune_player_stats_history(self, older_than_days: int = 60) -> int:
        """Prune player stats history older than specified days.
        
        Returns:
            Number of records deleted
        """
        cutoff_timestamp = int((datetime.utcnow() - timedelta(days=older_than_days)).timestamp())
        
        async with self.connection.execute("""
            SELECT COUNT(*) FROM player_stats_history WHERE timestamp < ?
        """, (cutoff_timestamp,)) as cursor:
            count_row = await cursor.fetchone()
            count = count_row[0] if count_row else 0
        
        await self.connection.execute("""
            DELETE FROM player_stats_history WHERE timestamp < ?
        """, (cutoff_timestamp,))
        await self.connection.commit()
        
        return count
    
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
        cutoff_timestamp = int((datetime.utcnow() - timedelta(days=older_than_days)).timestamp())
        
        async with self.connection.execute("""
            SELECT COUNT(*) FROM faction_history WHERE timestamp < ?
        """, (cutoff_timestamp,)) as cursor:
            count_row = await cursor.fetchone()
            count = count_row[0] if count_row else 0
        
        await self.connection.execute("""
            DELETE FROM faction_history WHERE timestamp < ?
        """, (cutoff_timestamp,))
        await self.connection.commit()
        
        return count
    
    async def prune_territory_ownership_history(self, older_than_days: int = 60) -> int:
        """Prune territory ownership history older than specified days.
        
        Returns:
            Number of records deleted
        """
        cutoff_timestamp = int((datetime.utcnow() - timedelta(days=older_than_days)).timestamp())
        
        async with self.connection.execute("""
            SELECT COUNT(*) FROM territory_ownership_history WHERE recorded_at < ?
        """, (cutoff_timestamp,)) as cursor:
            count_row = await cursor.fetchone()
            count = count_row[0] if count_row else 0
        
        await self.connection.execute("""
            DELETE FROM territory_ownership_history WHERE recorded_at < ?
        """, (cutoff_timestamp,))
        await self.connection.commit()
        
        return count
