# SQLite Database Plan for Torn API Data Storage

## Overview

This plan outlines a SQLite database schema to store Torn API data with three storage patterns:
1. **Upsert Tables** - Current state data (players, factions)
2. **Append-Only Tables** - Historical change tracking (stats, wars, territories)
3. **Summary Tables** - Aggregated historical data with automatic pruning of Append-Only tables

---

## Design Questions

Before finalizing the schema, please clarify:

### 1. Data Collection Frequency
- How often will you poll the API? it depends on the function, could be as much as 60 requests per minute per API key. However this is not likely to be required, one update every 4 minutes is more likely.
- This affects how much data accumulates in append-only tables

### 2. Retention & Summarization Policy
- How long should detailed records be kept before summarizing? A month, summarize on the first of each month.
- What summarization intervals? Monthly
- Should summaries be progressive? yes, keep append-only tables for 2 months. The historical summaries tables should remain indefintely.

### 3. Player Stats to Track
- Which stats need historical tracking? strength, defense, speed, dexterity, total stats, level, life, networth
- Should we track all stats or only specific ones? specific ones to start.

### 4. War Data Structure
- What war types to track? both territory wars, ranked wars
- What fields per war? faction IDs, scores, start/end times, status, territory ID if applicable
- Should we track individual war events or just war status snapshots? snapshots to start.

### 5. Territory Data
- What territory fields to track? ownership changes, war status, racket info, sector/coordinates
- Should we track all territories or only specific ones? specifc ones above

### 6. Faction Data
- What faction fields need historical tracking? (respect, member count, best chain)

---

## Proposed Schema Structure

### Schema Management

#### `schema_version` Table
```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    description TEXT
);

-- Insert initial version
INSERT INTO schema_version (version, description) VALUES (1, 'Initial schema');
```

### Type 1: Upsert Tables (Current State)

#### `players` Table
```sql
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    level INTEGER CHECK (level >= 1),
    rank TEXT,
    faction_id INTEGER,
    status_state TEXT,
    status_description TEXT,
    life_current INTEGER CHECK (life_current >= 0),
    life_maximum INTEGER CHECK (life_maximum > 0),
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (faction_id) REFERENCES factions(faction_id)
);

CREATE INDEX idx_players_faction ON players(faction_id);
CREATE INDEX idx_players_last_updated ON players(last_updated);
```

#### `factions` Table
```sql
CREATE TABLE factions (
    faction_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    tag TEXT,
    leader_id INTEGER,
    co_leader_id INTEGER,
    respect INTEGER CHECK (respect >= 0),
    age INTEGER CHECK (age >= 0),
    best_chain INTEGER CHECK (best_chain >= 0),
    member_count INTEGER CHECK (member_count >= 0),
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    last_updated INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))  -- Unix timestamp
);

CREATE INDEX idx_factions_last_updated ON factions(last_updated);
```

---

### Type 2: Append-Only Tables (Historical Changes)

#### `player_stats_history` Table
```sql
CREATE TABLE player_stats_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    strength INTEGER CHECK (strength >= 0),
    defense INTEGER CHECK (defense >= 0),
    speed INTEGER CHECK (speed >= 0),
    dexterity INTEGER CHECK (dexterity >= 0),
    total_stats INTEGER CHECK (total_stats >= 0),
    level INTEGER CHECK (level >= 1),
    life_maximum INTEGER CHECK (life_maximum > 0),
    networth INTEGER,
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

CREATE INDEX idx_player_stats_player_time ON player_stats_history(player_id, timestamp DESC);
CREATE INDEX idx_player_stats_timestamp ON player_stats_history(timestamp);
```

#### `war_status_history` Table
```sql
CREATE TABLE war_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    war_id INTEGER NOT NULL,
    war_type TEXT NOT NULL CHECK (war_type IN ('territory', 'ranked')),
    territory_id INTEGER,  -- NULL for ranked wars
    attacking_faction_id INTEGER,
    defending_faction_id INTEGER,
    attacking_score INTEGER CHECK (attacking_score >= 0),
    defending_score INTEGER CHECK (defending_score >= 0),
    required_score INTEGER CHECK (required_score >= 0),
    status TEXT CHECK (status IN ('ongoing', 'completed', 'cancelled')),
    started_at INTEGER,  -- Unix timestamp
    ends_at INTEGER,  -- Unix timestamp
    recorded_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (attacking_faction_id) REFERENCES factions(faction_id),
    FOREIGN KEY (defending_faction_id) REFERENCES factions(faction_id)
);

CREATE INDEX idx_war_status_war_time ON war_status_history(war_id, recorded_at DESC);
CREATE INDEX idx_war_status_territory ON war_status_history(territory_id, recorded_at);
CREATE INDEX idx_war_status_timestamp ON war_status_history(recorded_at);
```

#### `territory_ownership_history` Table
```sql
CREATE TABLE territory_ownership_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    territory_id INTEGER NOT NULL,
    faction_id INTEGER,  -- NULL if unowned
    sector TEXT,
    coordinates TEXT,
    racket_level INTEGER CHECK (racket_level >= 0),
    racket_type TEXT,
    war_status TEXT CHECK (war_status IN ('none', 'under_attack', 'defending')),
    recorded_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (faction_id) REFERENCES factions(faction_id)
);

CREATE INDEX idx_territory_ownership_territory_time ON territory_ownership_history(territory_id, recorded_at DESC);
CREATE INDEX idx_territory_ownership_faction ON territory_ownership_history(faction_id, recorded_at);
CREATE INDEX idx_territory_ownership_timestamp ON territory_ownership_history(recorded_at);
```

#### `faction_history` Table
```sql
CREATE TABLE faction_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    faction_id INTEGER NOT NULL,
    timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    respect INTEGER CHECK (respect >= 0),
    member_count INTEGER CHECK (member_count >= 0),
    best_chain INTEGER CHECK (best_chain >= 0),
    FOREIGN KEY (faction_id) REFERENCES factions(faction_id)
);

CREATE INDEX idx_faction_history_faction_time ON faction_history(faction_id, timestamp DESC);
CREATE INDEX idx_faction_history_timestamp ON faction_history(timestamp);
```

---

### Type 3: Summary Tables (Aggregated Historical Data)

#### `player_stats_summary` Table
```sql
CREATE TABLE player_stats_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    period_start INTEGER NOT NULL,  -- Unix timestamp
    period_end INTEGER NOT NULL,  -- Unix timestamp
    period_type TEXT NOT NULL DEFAULT 'monthly',  -- Monthly summaries only
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
    record_count INTEGER CHECK (record_count >= 0),  -- Number of detailed records summarized
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    UNIQUE(player_id, period_start, period_end, period_type)
);

CREATE INDEX idx_player_stats_summary_player_period ON player_stats_summary(player_id, period_start, period_end);
CREATE INDEX idx_player_stats_summary_period_type ON player_stats_summary(period_type, period_start);
```

#### `war_summary` Table
```sql
CREATE TABLE war_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    war_id INTEGER NOT NULL,
    war_type TEXT NOT NULL CHECK (war_type IN ('territory', 'ranked')),  -- 'territory' or 'ranked'
    territory_id INTEGER,  -- NULL for ranked wars
    period_start INTEGER NOT NULL,  -- Unix timestamp
    period_end INTEGER NOT NULL,  -- Unix timestamp
    period_type TEXT NOT NULL DEFAULT 'monthly',  -- Monthly summaries only
    attacking_faction_id INTEGER,
    defending_faction_id INTEGER,
    final_attacking_score INTEGER,
    final_defending_score INTEGER,
    winner_faction_id INTEGER,  -- NULL if ongoing/unresolved
    duration_seconds INTEGER CHECK (duration_seconds >= 0),
    record_count INTEGER CHECK (record_count >= 0),
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (attacking_faction_id) REFERENCES factions(faction_id),
    FOREIGN KEY (defending_faction_id) REFERENCES factions(faction_id),
    FOREIGN KEY (winner_faction_id) REFERENCES factions(faction_id),
    UNIQUE(war_id, period_start, period_end, period_type)
);

CREATE INDEX idx_war_summary_war_period ON war_summary(war_id, period_start, period_end);
CREATE INDEX idx_war_summary_territory ON war_summary(territory_id, period_start);
```

#### `territory_ownership_summary` Table
```sql
CREATE TABLE territory_ownership_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    territory_id INTEGER NOT NULL,
    period_start INTEGER NOT NULL,  -- Unix timestamp
    period_end INTEGER NOT NULL,  -- Unix timestamp
    period_type TEXT NOT NULL DEFAULT 'monthly',  -- Monthly summaries only
    faction_id_start INTEGER,  -- Owner at start of period
    faction_id_end INTEGER,  -- Owner at end of period
    ownership_changes INTEGER CHECK (ownership_changes >= 0),  -- Count of ownership changes in period
    days_owned INTEGER CHECK (days_owned >= 0),  -- Days owned by end faction (if same throughout)
    record_count INTEGER CHECK (record_count >= 0),
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (faction_id_start) REFERENCES factions(faction_id),
    FOREIGN KEY (faction_id_end) REFERENCES factions(faction_id),
    UNIQUE(territory_id, period_start, period_end, period_type)
);

CREATE INDEX idx_territory_summary_territory_period ON territory_ownership_summary(territory_id, period_start, period_end);
CREATE INDEX idx_territory_summary_faction ON territory_ownership_summary(faction_id_end, period_start);
```

#### `faction_summary` Table
```sql
CREATE TABLE faction_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    faction_id INTEGER NOT NULL,
    period_start INTEGER NOT NULL,  -- Unix timestamp
    period_end INTEGER NOT NULL,  -- Unix timestamp
    period_type TEXT NOT NULL DEFAULT 'monthly',  -- Monthly summaries only
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
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    FOREIGN KEY (faction_id) REFERENCES factions(faction_id),
    UNIQUE(faction_id, period_start, period_end, period_type)
);

CREATE INDEX idx_faction_summary_faction_period ON faction_summary(faction_id, period_start, period_end);
CREATE INDEX idx_faction_summary_period_type ON faction_summary(period_type, period_start);
```

---

## Database Management Functions

### Pruning & Summarization Strategy

The system will need functions to:

1. **Summarize Append-Only Data**
   - Aggregate records by time period (daily/weekly/monthly)
   - Calculate start/end values and changes
   - Insert into summary tables
   - Delete summarized records from append-only tables

2. **Retention Policy**
   - Keep append-only detailed records for 2 months
   - On the 1st of each month, summarize the previous month's data to monthly summaries
   - After summarizing, delete detailed records older than 2 months from append-only tables
   - Keep summary tables indefinitely (no automatic deletion)
   - Summarization runs monthly (not daily/weekly)

3. **Upsert Operations**
   - Use SQLite's `INSERT OR REPLACE` or `ON CONFLICT` for upserts
   - Update `last_updated` timestamp on changes

---

## Implementation Plan

### Phase 1: Database Setup
- Create database file and connection manager
- Implement schema creation/migration system
- Add helper functions for upserts

### Phase 2: Data Collection
- Integrate database writes into API call handlers
- Implement upsert logic for players/factions
- Implement append logic for historical data

### Phase 3: Summarization System
- Create summarization functions
- Implement scheduled pruning jobs
- Add configuration for retention policies

### Phase 4: Query Helpers
- Create helper functions for common queries
- Add functions to retrieve current state
- Add functions to retrieve historical trends

---

## Example Usage Patterns

### Upsert Example
```python
# Update player current state
db.upsert_player(player_id=12345, name="PlayerName", level=50, ...)
```

### Append Example
```python
# Record player stats change
db.append_player_stats(player_id=12345, strength=1000, defense=800, ...)
```

### Summarization Example
```python
# Summarize last month's player stats to monthly summary (run on 1st of month)
db.summarize_player_stats_monthly(
    period_start=datetime(2024, 1, 1),
    period_end=datetime(2024, 1, 31)
)
# Then prune detailed records older than 2 months
db.prune_player_stats_history(older_than_days=60)

# Similar for other tables
db.summarize_faction_history_monthly(...)
db.summarize_war_status_monthly(...)
db.summarize_territory_ownership_monthly(...)
```

---

## Finalized Schema Summary

Based on your answers:
- **Collection**: Up to 60 req/min per key, typically every 4 minutes
- **Retention**: Append-only tables keep 2 months of data, summarize monthly on 1st of month
- **Summaries**: Monthly only, kept indefinitely
- **Player Stats Tracked**: strength, defense, speed, dexterity, total_stats, level, life (current/max), networth
- **Faction History Tracked**: respect, member_count, best_chain
- **War Types**: Both territory and ranked wars (snapshots)
- **Territory Fields**: ownership, war_status, racket_info, sector, coordinates

## Next Steps

1. ✅ **Design questions answered** - Schema finalized
2. **Review and approve** this plan
3. **Implement database module** with schema creation
4. **Integrate with existing API client** for data collection
5. **Add summarization/pruning logic** (monthly job for 1st of month)

---

## Notes

- SQLite supports `INSERT OR REPLACE` for upserts
- Use `ON CONFLICT` clauses for more complex upsert logic
- Consider using WAL mode for better concurrency
- Indexes are crucial for query performance on historical data
- Consider partitioning large tables if they grow very large (though SQLite doesn't support native partitioning)

---

## ⚠️ Important Implementation Considerations

**See `DATABASE_REVIEW.md` for detailed best practices review.**

### Critical Requirements:

1. **Use `aiosqlite`** for async database operations (required for Discord.py async bot)
2. **Enable foreign keys**: `PRAGMA foreign_keys = ON` (SQLite doesn't enforce by default)
3. **Enable WAL mode**: `PRAGMA journal_mode = WAL` (better concurrency)
4. **Standardize timestamps**: Use INTEGER (Unix epoch) for consistency and performance
5. **Transaction management**: Use transactions for multi-step operations (upserts, summarization)
6. **Schema versioning**: Implement migration system for future schema changes

### Recommended Additions:

- CHECK constraints for data validation
- Schema version tracking table
- Backup strategy before summarization
- Idempotent summarization (handle re-runs safely)
- Error handling and data validation layer

### Dependencies to Add:

```txt
aiosqlite>=0.19.0  # Async SQLite for Discord.py compatibility
```

---
# Database Review
# Database Plan Review - Best Practices & Improvements

## ✅ What's Good

1. **Clear separation of concerns** - Upsert, append-only, and summary tables are well-defined
2. **Appropriate indexing** - Good coverage for common query patterns
3. **Foreign key relationships** - Properly defined
4. **Unique constraints** - Prevent duplicate summaries

## 🔧 Recommended Improvements

### 1. SQLite Configuration & Best Practices

#### Enable Foreign Keys
SQLite doesn't enforce foreign keys by default. Must be enabled per connection:
```python
# In database initialization
conn.execute("PRAGMA foreign_keys = ON")
```

#### Use WAL Mode for Better Concurrency
WAL (Write-Ahead Logging) mode allows concurrent reads during writes:
```sql
PRAGMA journal_mode = WAL;
```

#### Enable Query Optimizer
```sql
PRAGMA optimize;
```

#### Recommended PRAGMA Settings
```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;  -- Good balance for performance
PRAGMA cache_size = -64000;   -- 64MB cache (adjust based on available RAM)
PRAGMA temp_store = MEMORY;   -- Store temp tables in memory
PRAGMA mmap_size = 268435456; -- 256MB memory-mapped I/O
```

### 2. Timestamp Handling

**Issue**: SQLite's `TIMESTAMP` is actually stored as TEXT, INTEGER, or REAL. For consistency and performance:

**Recommendation**: Use INTEGER (Unix timestamp) or TEXT (ISO 8601) consistently.

**Current**: Mixed use of `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` and manual timestamps.

**Better approach**:
```sql
-- Use INTEGER for timestamps (Unix epoch seconds)
timestamp INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))

-- Or use TEXT with ISO 8601 format
timestamp TEXT NOT NULL DEFAULT (datetime('now'))
```

**Recommendation**: Use INTEGER (Unix timestamps) for:
- Better performance in queries
- Easier date arithmetic
- Smaller storage size

### 3. Data Type Consistency

**Issues Found**:
- `coordinates` stored as TEXT - consider if this should be structured (e.g., separate lat/long columns)
- Some INTEGER fields might need to handle NULLs better
- TEXT fields without length constraints

**Recommendations**:
```sql
-- For coordinates, consider:
sector TEXT,
coordinate_x INTEGER,  -- If coordinates are numeric
coordinate_y INTEGER,
-- OR keep as TEXT if format is "x,y" or similar

-- Add CHECK constraints where appropriate
life_current INTEGER CHECK (life_current >= 0),
life_maximum INTEGER CHECK (life_maximum > 0),
level INTEGER CHECK (level >= 1),
```

### 4. Missing Constraints

**Add CHECK constraints** for data validation:
```sql
-- In player_stats_history
strength INTEGER CHECK (strength >= 0),
defense INTEGER CHECK (defense >= 0),
speed INTEGER CHECK (speed >= 0),
dexterity INTEGER CHECK (dexterity >= 0),
level INTEGER CHECK (level >= 1),

-- In war_status_history
attacking_score INTEGER CHECK (attacking_score >= 0),
defending_score INTEGER CHECK (defending_score >= 0),
status TEXT CHECK (status IN ('ongoing', 'completed', 'cancelled')),

-- In territory_ownership_history
war_status TEXT CHECK (war_status IN ('none', 'under_attack', 'defending')),
racket_level INTEGER CHECK (racket_level >= 0),
```

### 5. Index Optimization

**Current indexes are good**, but consider:

**Composite indexes for common query patterns**:
```sql
-- If you often query by player_id AND date range
CREATE INDEX idx_player_stats_player_timestamp ON player_stats_history(player_id, timestamp DESC);

-- For faction queries with time
CREATE INDEX idx_faction_history_faction_timestamp ON faction_history(faction_id, timestamp DESC);
```

**Covering indexes** (if queries only need indexed columns):
```sql
-- If you often query just player_id and timestamp
-- The existing index already covers this well
```

### 6. Schema Versioning & Migrations

**Missing**: Schema version tracking and migration system.

**Recommendation**: Add a `schema_version` table:
```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);
```

**Migration pattern**:
```python
# Check current version
current_version = get_schema_version()

# Apply migrations in order
if current_version < 1:
    apply_migration_1()
if current_version < 2:
    apply_migration_2()
# etc.
```

### 7. Connection Management

**For async Discord bot**, use `aiosqlite`:
```python
# Add to requirements.txt
aiosqlite>=0.19.0

# Connection pooling pattern
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.pool = None
    
    async def connect(self):
        self.pool = await aiosqlite.connect(self.db_path)
        await self.pool.execute("PRAGMA foreign_keys = ON")
        await self.pool.execute("PRAGMA journal_mode = WAL")
        # ... other PRAGMAs
    
    async def close(self):
        if self.pool:
            await self.pool.close()
```

### 8. Transaction Management

**Important**: Use transactions for multi-step operations:

```python
async def upsert_player(self, **kwargs):
    async with self.pool.cursor() as cursor:
        await cursor.execute("BEGIN TRANSACTION")
        try:
            # Check if exists
            # Insert or update
            await cursor.execute("COMMIT")
        except Exception as e:
            await cursor.execute("ROLLBACK")
            raise
```

**For bulk operations** (summarization):
```python
async def summarize_monthly(self, period_start, period_end):
    async with self.pool.cursor() as cursor:
        await cursor.execute("BEGIN TRANSACTION")
        try:
            # Aggregate data
            # Insert summaries
            # Delete old records
            await cursor.execute("COMMIT")
        except Exception as e:
            await cursor.execute("ROLLBACK")
            raise
```

### 9. Error Handling & Data Validation

**Add validation layer**:
```python
def validate_player_data(data: dict) -> dict:
    """Validate and sanitize player data before insert."""
    validated = {}
    # Type checking, range validation, etc.
    return validated
```

**Handle API data inconsistencies**:
- Some fields might be missing
- Some might be strings instead of integers
- Handle NULL values appropriately

### 10. Backup Strategy

**Recommendation**: Implement periodic backups:
```python
async def backup_database(self, backup_path: str):
    """Create a backup of the database."""
    # SQLite backup API
    async with aiosqlite.connect(backup_path) as backup:
        await self.pool.backup(backup)
```

**Consider**:
- Daily backups before summarization
- Keep last N backups
- Backup before major operations

### 11. Query Performance Considerations

**For large append-only tables**:

**Vacuum periodically** (after pruning):
```sql
VACUUM;
```

**Analyze tables** for query optimizer:
```sql
ANALYZE;
```

**Consider**:
- Batch inserts for better performance
- Use executemany() for bulk operations
- Consider connection timeout settings

### 12. Missing Fields/Considerations

**Consider adding**:
- `created_at` to upsert tables (when first seen, not just last_updated)
- `data_source` or `api_key_used` for tracking which key collected data
- `is_active` flag for players/factions (if they can be deleted/deactivated)

**For war tracking**:
- Consider tracking war participants (individual players) if needed later
- Consider tracking war events (not just snapshots) if requirements expand

### 13. Summarization Logic Improvements

**Current plan**: Summarize on 1st of month.

**Consider**:
- Idempotency: What if summarization runs twice?
- Partial months: How to handle current month?
- Error recovery: What if summarization fails partway through?

**Recommendation**:
```python
async def summarize_monthly(self, year: int, month: int, force: bool = False):
    """Summarize a specific month. Idempotent if force=False."""
    period_start = datetime(year, month, 1)
    period_end = (period_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    # Check if already summarized
    if not force:
        existing = await self.check_summary_exists('player_stats_summary', period_start, period_end)
        if existing:
            return  # Already done
    
    # Perform summarization in transaction
    # ...
```

### 14. Monitoring & Logging

**Add**:
- Logging for database operations (errors, slow queries)
- Metrics: table sizes, record counts
- Health checks: database file size, last backup time

### 15. Testing Considerations

**Recommendation**: 
- Use in-memory database for tests
- Test migration scripts
- Test summarization logic with sample data
- Test concurrent access patterns

## 📋 Implementation Checklist

- [ ] Add `aiosqlite` to requirements.txt
- [ ] Implement connection management with WAL mode
- [ ] Enable foreign keys
- [ ] Standardize timestamp format (INTEGER Unix timestamps recommended)
- [ ] Add CHECK constraints for data validation
- [ ] Implement schema versioning table
- [ ] Create migration system
- [ ] Add transaction management
- [ ] Implement backup strategy
- [ ] Add error handling and validation
- [ ] Create monitoring/logging
- [ ] Add `created_at` to upsert tables
- [ ] Make summarization idempotent
- [ ] Add database health checks

## 🎯 Priority Recommendations

**High Priority**:
1. Use `aiosqlite` for async operations
2. Enable foreign keys and WAL mode
3. Standardize timestamp format
4. Add transaction management
5. Implement schema versioning

**Medium Priority**:
6. Add CHECK constraints
7. Implement backup strategy
8. Make summarization idempotent
9. Add error handling/validation

**Low Priority**:
10. Add monitoring/logging
11. Optimize indexes further
12. Add created_at to upsert tables

## Questions to Consider

1. **Concurrent access**: Will multiple processes access the database? (If yes, need connection pooling)
2. **Backup location**: Where should backups be stored?
3. **Monitoring**: Do you want database metrics exposed via Discord commands?
4. **Data retention**: Should there be a way to manually purge old summary data if needed?
5. **API key tracking**: Do you want to track which API key collected which data?
