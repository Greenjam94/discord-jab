# Database Review Options

This project now supports two methods for reviewing database contents:

## Option 1: Discord Commands (Recommended for quick queries)

### Features
- Query any database table directly from Discord
- Pagination with interactive buttons
- Filter and sorting options
- Admin-only access for security

### Commands

#### `/db-tables`
Lists all available database tables with column counts.

#### `/db-query`
Query a database table with pagination.

**Parameters:**
- `table` (required): Table name to query
- `page` (optional, default: 1): Page number
- `limit` (optional, default: 20, max: 50): Rows per page
- `order_by` (optional): Column name to sort by
- `filter` (optional): WHERE clause filter (e.g., `player_id = 12345`)

**Examples:**
```
/db-query table:players limit:10
/db-query table:player_stats_history order_by:timestamp filter:player_id=12345
/db-query table:competitions page:2 limit:25
```

**Usage:**
1. Use `/db-tables` to see available tables
2. Use `/db-query` with the table name
3. Navigate pages using the buttons (First, Prev, Next, Last)
4. Values are automatically truncated for Discord's limits

**Note:** Only administrators can use these commands.

## Option 2: Web Interface (Recommended for extensive browsing)

### Features
- Full-screen table browsing
- Click column headers to sort
- Filter with WHERE clauses
- Adjustable page sizes (25-200 rows)
- Dark theme UI
- Better for large result sets

### Setup

1. **Install Flask** (if not already installed):
   ```bash
   pip install flask>=2.3.0
   ```

2. **Set environment variables** (optional):
   ```bash
   export DATABASE_PATH=data/torn_data.db  # Default if not set
   export WEB_PORT=5000                    # Default if not set
   export FLASK_DEBUG=True                 # Optional, for development
   ```

3. **Run the web server**:
   ```bash
   python web_app.py
   ```

4. **Open in browser**:
   Navigate to `http://localhost:5000`

### Usage

1. **Select a table** from the dropdown
2. **Choose page size** (25, 50, 100, or 200 rows)
3. **Optional filters**:
   - Order by: Column name (e.g., `timestamp DESC`)
   - Filter: WHERE clause (e.g., `player_id = 12345`)
4. **Click Query** to fetch results
5. **Navigate** using pagination buttons
6. **Click column headers** to sort by that column

### Security Note

The web interface has no authentication by default. For production use:
- Run behind a reverse proxy (nginx/Apache) with authentication
- Use Flask-Login or similar for user authentication
- Restrict access via firewall rules
- Consider using environment variables for sensitive configuration

## Comparison

| Feature | Discord Commands | Web Interface |
|---------|-----------------|---------------|
| Setup | No additional setup | Requires Flask installation |
| Access | Discord only (admin) | Browser (no auth by default) |
| Best for | Quick queries, mobile | Extensive browsing, analysis |
| Page size | Max 50 rows | Up to 200 rows |
| Sorting | Via parameter | Click headers |
| Mobile friendly | Yes | Responsive design |

## Available Tables

- **Current State**: `players`, `factions`
- **History**: `player_stats_history`, `faction_history`, `war_status_history`, `territory_ownership_history`
- **Summaries**: `player_stats_summary`, `faction_summary`, `war_summary`, `territory_ownership_summary`
- **Competitions**: `competitions`, `competition_participants`, `competition_teams`, `competition_start_stats`, `competition_stats`

## Tips

1. **For timestamp columns**, values are automatically converted to ISO format for readability
2. **Long text values** are truncated with "..." in both interfaces
3. **NULL values** are displayed as "NULL" (italicized in web interface)
4. **Filter syntax**: Use SQL WHERE clause syntax, e.g., `player_id = 12345` or `level > 50`

## Troubleshooting

### Discord Commands Not Working
- Ensure you have administrator permissions
- Check that the bot has the database initialized
- Verify table names are correct (use `/db-tables` to list them)

### Web Interface Issues
- Ensure database file exists at the configured path
- Check that Flask is installed: `pip install flask`
- Verify database is not locked by another process
- Check console for error messages

### Performance
- Large tables may be slow - use filters to narrow results
- Historical tables can be very large - consider using date filters
- For best performance, add appropriate indexes (already included in schema)
