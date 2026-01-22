"""Simple Flask web interface for database browsing."""

from flask import Flask, render_template_string, request, jsonify, redirect, url_for
import aiosqlite
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import os
import sys
import json

app = Flask(__name__)

# Database path (default from db_manager)
DB_PATH = os.getenv("DATABASE_PATH", "data/torn_data.db")

# HTML template for the web interface
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Browser - Torn API Data</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 20px;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        h1 {
            color: #4ec9b0;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #858585;
            margin-bottom: 30px;
        }
        
        .controls {
            background: #252526;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #3e3e42;
        }
        
        .control-group {
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: flex-end;
            margin-bottom: 15px;
        }
        
        .control-item {
            flex: 1;
            min-width: 200px;
        }
        
        label {
            display: block;
            color: #cccccc;
            margin-bottom: 5px;
            font-size: 14px;
        }
        
        select, input {
            width: 100%;
            padding: 8px 12px;
            background: #1e1e1e;
            border: 1px solid #3e3e42;
            border-radius: 4px;
            color: #d4d4d4;
            font-size: 14px;
        }
        
        select:focus, input:focus {
            outline: none;
            border-color: #007acc;
        }
        
        button {
            padding: 10px 20px;
            background: #007acc;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            transition: background 0.2s;
        }
        
        button:hover {
            background: #005a9e;
        }
        
        button:disabled {
            background: #3e3e42;
            cursor: not-allowed;
        }
        
        .info-bar {
            background: #252526;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #3e3e42;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
        
        .pagination {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        
        .pagination button {
            padding: 6px 12px;
            font-size: 13px;
        }
        
        .table-container {
            background: #252526;
            border-radius: 8px;
            border: 1px solid #3e3e42;
            overflow-x: auto;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        
        thead {
            background: #2d2d30;
            position: sticky;
            top: 0;
        }
        
        th {
            padding: 12px;
            text-align: left;
            color: #4ec9b0;
            font-weight: 600;
            border-bottom: 2px solid #3e3e42;
            white-space: nowrap;
        }
        
        th.sortable {
            cursor: pointer;
            user-select: none;
        }
        
        th.sortable:hover {
            background: #3e3e42;
        }
        
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #3e3e42;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        
        tr:hover {
            background: #2d2d30;
        }
        
        .null-value {
            color: #858585;
            font-style: italic;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #858585;
        }
        
        .error {
            background: #5a1d1d;
            border: 1px solid #be1100;
            color: #f48771;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        
        .timestamp {
            color: #9cdcfe;
        }
        
        .number {
            color: #b5cea8;
            text-align: right;
        }
        
        @media (max-width: 768px) {
            .control-group {
                flex-direction: column;
            }
            
            .control-item {
                width: 100%;
            }
            
            table {
                font-size: 11px;
            }
            
            th, td {
                padding: 8px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗄️ Database Browser</h1>
        <p class="subtitle">Browse Torn API data stored in SQLite database</p>
        
        <div class="controls">
            <form id="queryForm" onsubmit="queryDatabase(event)">
                <div class="control-group">
                    <div class="control-item">
                        <label for="table">Table:</label>
                        <select id="table" name="table" required>
                            <option value="">Select a table...</option>
                        </select>
                    </div>
                    <div class="control-item">
                        <label for="pageSize">Rows per page:</label>
                        <select id="pageSize" name="pageSize">
                            <option value="25">25</option>
                            <option value="50" selected>50</option>
                            <option value="100">100</option>
                            <option value="200">200</option>
                        </select>
                    </div>
                    <div class="control-item">
                        <label for="orderBy">Order by:</label>
                        <input type="text" id="orderBy" name="orderBy" placeholder="column_name">
                    </div>
                    <div class="control-item">
                        <label for="filter">Filter (WHERE clause):</label>
                        <input type="text" id="filter" name="filter" placeholder="player_id = 12345">
                    </div>
                    <div class="control-item">
                        <button type="submit">🔍 Query</button>
                    </div>
                </div>
            </form>
        </div>
        
        <div id="errorContainer"></div>
        
        <div id="infoBar" class="info-bar" style="display: none;">
            <div>
                <strong id="rowInfo">No data</strong>
            </div>
            <div class="pagination">
                <button id="firstBtn" onclick="changePage(1)">⏮ First</button>
                <button id="prevBtn" onclick="changePage(-1)">◀ Prev</button>
                <span id="pageInfo">Page 1 / 1</span>
                <button id="nextBtn" onclick="changePage(1)">Next ▶</button>
                <button id="lastBtn" onclick="changePage('last')">Last ⏭</button>
            </div>
        </div>
        
        <div class="table-container">
            <div id="loading" class="loading" style="display: none;">Loading...</div>
            <div id="tableContainer"></div>
        </div>
    </div>
    
    <script>
        let currentPage = 1;
        let currentTable = '';
        let currentParams = {};
        let totalPages = 1;
        
        // Load table list on page load
        window.onload = function() {
            fetch('/api/tables')
                .then(r => r.json())
                .then(data => {
                    const select = document.getElementById('table');
                    data.tables.forEach(table => {
                        const option = document.createElement('option');
                        option.value = table;
                        option.textContent = table;
                        select.appendChild(option);
                    });
                })
                .catch(err => showError('Failed to load tables: ' + err));
        };
        
        function showError(message) {
            const container = document.getElementById('errorContainer');
            container.innerHTML = `<div class="error">${escapeHtml(message)}</div>`;
        }
        
        function clearError() {
            document.getElementById('errorContainer').innerHTML = '';
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function formatValue(value, columnName) {
            if (value === null || value === undefined) {
                return '<span class="null-value">NULL</span>';
            }
            
            const valueStr = String(value);
            
            // Check if it's a timestamp (ISO format)
            if (columnName.includes('timestamp') || columnName.includes('_at') || 
                columnName.includes('_date') || columnName.includes('start') || 
                columnName.includes('end')) {
                if (valueStr.match(/^\d{4}-\d{2}-\d{2}T/)) {
                    return `<span class="timestamp">${valueStr.replace('T', ' ').substring(0, 19)}</span>`;
                }
            }
            
            // Check if it's a number
            if (!isNaN(value) && valueStr.trim() !== '') {
                return `<span class="number">${parseFloat(value).toLocaleString()}</span>`;
            }
            
            // Truncate long strings
            if (valueStr.length > 100) {
                return escapeHtml(valueStr.substring(0, 97)) + '...';
            }
            
            return escapeHtml(valueStr);
        }
        
        function queryDatabase(event) {
            if (event) event.preventDefault();
            
            const table = document.getElementById('table').value;
            const pageSize = parseInt(document.getElementById('pageSize').value);
            const orderBy = document.getElementById('orderBy').value.trim();
            const filter = document.getElementById('filter').value.trim();
            
            if (!table) {
                showError('Please select a table');
                return;
            }
            
            currentTable = table;
            currentPage = 1;
            currentParams = {
                table: table,
                page: 1,
                limit: pageSize,
                order_by: orderBy || null,
                filter: filter || null
            };
            
            fetchData();
        }
        
        function changePage(delta) {
            if (delta === 'last') {
                currentPage = totalPages;
            } else if (delta === -1) {
                if (currentPage > 1) currentPage--;
            } else if (delta === 1) {
                if (currentPage < totalPages) currentPage++;
            } else if (typeof delta === 'number') {
                currentPage = delta;
            }
            
            currentParams.page = currentPage;
            fetchData();
        }
        
        function fetchData() {
            document.getElementById('loading').style.display = 'block';
            document.getElementById('tableContainer').innerHTML = '';
            document.getElementById('infoBar').style.display = 'none';
            clearError();
            
            const queryParams = new URLSearchParams({
                table: currentParams.table,
                page: currentParams.page,
                limit: currentParams.limit
            });
            
            if (currentParams.order_by) {
                queryParams.append('order_by', currentParams.order_by);
            }
            if (currentParams.filter) {
                queryParams.append('filter', currentParams.filter);
            }
            
            fetch(`/api/query?${queryParams}`)
                .then(r => r.json())
                .then(data => {
                    document.getElementById('loading').style.display = 'none';
                    
                    if (data.error) {
                        showError(data.error);
                        return;
                    }
                    
                    totalPages = data.total_pages;
                    displayTable(data.rows, data.columns, data.total_count);
                    updatePagination(data.total_count, data.offset);
                })
                .catch(err => {
                    document.getElementById('loading').style.display = 'none';
                    showError('Failed to fetch data: ' + err);
                });
        }
        
        function displayTable(rows, columns, totalCount) {
            const container = document.getElementById('tableContainer');
            
            if (rows.length === 0) {
                container.innerHTML = '<div class="loading">No rows found</div>';
                return;
            }
            
            let html = '<table><thead><tr>';
            
            columns.forEach(col => {
                html += `<th class="sortable" onclick="sortBy('${col}')" title="Click to sort">${escapeHtml(col)}</th>`;
            });
            
            html += '</tr></thead><tbody>';
            
            rows.forEach(row => {
                html += '<tr>';
                columns.forEach(col => {
                    html += `<td>${formatValue(row[col], col)}</td>`;
                });
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            
            container.innerHTML = html;
        }
        
        function sortBy(column) {
            const currentOrder = document.getElementById('orderBy').value;
            if (currentOrder === column || currentOrder === column + ' DESC') {
                document.getElementById('orderBy').value = column + ' DESC';
            } else {
                document.getElementById('orderBy').value = column;
            }
            currentParams.order_by = document.getElementById('orderBy').value;
            currentPage = 1;
            currentParams.page = 1;
            fetchData();
        }
        
        function updatePagination(totalCount, offset) {
            const infoBar = document.getElementById('infoBar');
            infoBar.style.display = 'flex';
            
            const endRow = Math.min(offset + currentParams.limit, totalCount);
            document.getElementById('rowInfo').textContent = 
                `Showing rows ${offset + 1}-${endRow} of ${totalCount.toLocaleString()}`;
            
            document.getElementById('pageInfo').textContent = 
                `Page ${currentPage} / ${totalPages}`;
            
            document.getElementById('firstBtn').disabled = currentPage === 1;
            document.getElementById('prevBtn').disabled = currentPage === 1;
            document.getElementById('nextBtn').disabled = currentPage >= totalPages;
            document.getElementById('lastBtn').disabled = currentPage >= totalPages;
        }
    </script>
</body>
</html>
"""


async def get_db_connection():
    """Get async database connection."""
    return await aiosqlite.connect(DB_PATH)


def get_table_info_sync() -> Dict[str, List[str]]:
    """Get table info synchronously."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_get_table_info_async())
    finally:
        loop.close()


async def _get_table_info_async() -> Dict[str, List[str]]:
    """Get table info asynchronously."""
    table_info = {}
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """) as cursor:
            tables = await cursor.fetchall()
            
            for (table_name,) in tables:
                async with conn.execute(f"PRAGMA table_info({table_name})") as col_cursor:
                    columns = await col_cursor.fetchall()
                    table_info[table_name] = [col[1] for col in columns]
    
    return table_info


async def query_table_async(
    table_name: str,
    limit: int = 50,
    offset: int = 0,
    order_by: Optional[str] = None,
    filter_clause: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """Query a table asynchronously."""
    allowed_tables = {
        'players', 'factions', 'player_stats_history', 'faction_history',
        'war_status_history', 'territory_ownership_history',
        'player_stats_summary', 'faction_summary', 'war_summary',
        'territory_ownership_summary', 'competitions', 'competition_participants',
        'competition_teams', 'competition_start_stats', 'competition_stats'
    }
    
    if table_name not in allowed_tables:
        raise ValueError(f"Table '{table_name}' is not allowed")
    
    count_query = f"SELECT COUNT(*) FROM {table_name}"
    data_query = f"SELECT * FROM {table_name}"
    
    if filter_clause:
        count_query += f" WHERE {filter_clause}"
        data_query += f" WHERE {filter_clause}"
    
    async with aiosqlite.connect(DB_PATH) as conn:
        # Get total count
        async with conn.execute(count_query) as cursor:
            total_count = (await cursor.fetchone())[0]
        
        # Add ordering
        if order_by:
            data_query += f" ORDER BY {order_by}"
        
        # Add pagination
        data_query += f" LIMIT ? OFFSET ?"
        
        # Execute query
        async with conn.execute(data_query, (limit, offset)) as cursor:
            rows = await cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            result = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Convert timestamps
                    if col in ['timestamp', 'recorded_at', 'created_at', 'last_updated', 
                              'started_at', 'ends_at', 'start_date', 'end_date', 
                              'period_start', 'period_end']:
                        if value:
                            try:
                                value = datetime.fromtimestamp(value).isoformat()
                            except (ValueError, TypeError):
                                pass
                    row_dict[col] = value
                result.append(row_dict)
    
    return result, total_count


def query_table_sync(
    table_name: str,
    limit: int = 50,
    offset: int = 0,
    order_by: Optional[str] = None,
    filter_clause: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """Query a table synchronously."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            query_table_async(table_name, limit, offset, order_by, filter_clause)
        )
    finally:
        loop.close()


@app.route('/')
def index():
    """Main page."""
    # Inject navigation into the template
    nav_html = """
    <div style="margin-bottom: 20px;">
        <a href="/instances" style="color: #4ec9b0; text-decoration: none; margin-right: 20px; font-weight: 600;">🤖 Bot Instances</a>
        <a href="/competitions" style="color: #4ec9b0; text-decoration: none; margin-right: 20px; font-weight: 600;">📊 Competitions</a>
        <a href="/" style="color: #4ec9b0; text-decoration: none; font-weight: 600;">🗄️ Database Browser</a>
    </div>
    """
    # Insert navigation after the subtitle
    template_with_nav = HTML_TEMPLATE.replace(
        '<p class="subtitle">Browse Torn API data stored in SQLite database</p>',
        '<p class="subtitle">Browse Torn API data stored in SQLite database</p>' + nav_html
    )
    return render_template_string(template_with_nav)


@app.route('/api/tables')
def api_tables():
    """API endpoint to get list of tables."""
    try:
        table_info = get_table_info_sync()
        return jsonify({
            'tables': list(table_info.keys()),
            'table_info': {k: len(v) for k, v in table_info.items()}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/query')
def api_query():
    """API endpoint to query a table."""
    try:
        table = request.args.get('table', '')
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        order_by = request.args.get('order_by', '')
        filter_clause = request.args.get('filter', '')
        
        if not table:
            return jsonify({'error': 'Table name required'}), 400
        
        limit = max(1, min(200, limit))
        offset = (page - 1) * limit
        
        rows, total_count = query_table_sync(
            table_name=table,
            limit=limit,
            offset=offset,
            order_by=order_by if order_by else None,
            filter_clause=filter_clause if filter_clause else None
        )
        
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        columns = list(rows[0].keys()) if rows else []
        
        return jsonify({
            'rows': rows,
            'columns': columns,
            'total_count': total_count,
            'total_pages': total_pages,
            'page': page,
            'offset': offset,
            'limit': limit
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Global bot reference (set when bot is running)
_bot_instance = None


def set_bot_instance(bot):
    """Set the Discord bot instance for status checks."""
    global _bot_instance
    _bot_instance = bot


def get_all_commands() -> List[str]:
    """Get list of all bot commands."""
    commands = [
        # Admin commands
        "ping", "warn", "sync-commands", "db-health", "db-query", "db-tables",
        # Competition commands
        "competition-list", "competition-create", "competition-cancel", 
        "competition-status", "competition-team-status", "competition-faction-overview",
        "competition-team-set-captains", "competition-add-participants",
        "competition-update-assignment", "competition-update-stats",
        # Torn commands
        "torn-key-add", "torn-key-remove", "torn-key-list", "torn-key-check",
        "torn-key-validate", "torn-user", "torn-faction",
        # Games commands
        "diceroll", "parrot"
    ]
    return sorted(commands)


def get_bot_status(guild_id: str) -> Dict[str, Any]:
    """Get bot status for a guild.
    
    First tries to use the direct bot instance if available.
    Falls back to checking the database for heartbeat information.
    """
    # Try direct bot instance first (if web app and bot are in same process)
    if _bot_instance:
        try:
            if not _bot_instance.is_ready():
                return {'status': 'unknown', 'bot_running': True, 'bot_online': False, 'message': 'Bot is not ready yet'}
            
            guild = _bot_instance.get_guild(int(guild_id))
            if not guild:
                return {'status': 'not_in_guild', 'bot_running': True, 'bot_online': True, 'message': 'Bot is not in this guild'}
            
            bot_member = guild.get_member(_bot_instance.user.id)
            if not bot_member:
                return {'status': 'not_member', 'bot_running': True, 'bot_online': True, 'message': 'Bot member not found in guild'}
            
            return {
                'status': 'active',
                'bot_running': True,
                'bot_online': True,
                'guild_name': guild.name,
                'member_count': guild.member_count
            }
        except (ValueError, AttributeError) as e:
            # Fall through to database check
            pass
        except Exception as e:
            # Fall through to database check
            pass
    
    # Fallback: Check database for heartbeat (for separate processes)
    try:
        import time
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            from database import TornDatabase
            db = TornDatabase(DB_PATH)
            loop.run_until_complete(db.connect())
            try:
                instances = loop.run_until_complete(db.get_bot_instances())
                instance = next((i for i in instances if i['guild_id'] == guild_id), None)
                
                if instance:
                    last_seen = instance.get('last_seen', 0)
                    current_time = int(time.time())
                    time_since_last_seen = current_time - last_seen
                    
                    # If last_seen is within last 5 minutes, consider bot online
                    bot_online = time_since_last_seen < 300 if last_seen else False
                    
                    return {
                        'status': instance.get('status', 'unknown'),
                        'bot_running': bot_online,
                        'bot_online': bot_online,
                        'guild_name': instance.get('guild_name'),
                        'member_count': instance.get('member_count'),
                        'message': f'Last seen {time_since_last_seen // 60} minutes ago' if last_seen else 'Never seen'
                    }
            finally:
                loop.run_until_complete(db.close())
        finally:
            loop.close()
    except Exception as e:
        pass
    
    # Final fallback
    return {'status': 'unknown', 'bot_running': False, 'message': 'Bot status unknown - check if bot is running'}


async def get_instances_with_status_async() -> List[Dict[str, Any]]:
    """Get all bot instances with their current status."""
    from database import TornDatabase
    import time
    
    db = TornDatabase(DB_PATH)
    await db.connect()
    try:
        instances = await db.get_bot_instances()
        
        # Add real-time status for each instance
        for instance in instances:
            # Check database heartbeat first
            last_seen = instance.get('last_seen', 0)
            current_time = int(time.time())
            time_since_last_seen = current_time - last_seen if last_seen else 999999
            
            # If last_seen is within last 5 minutes, consider bot online
            bot_online_db = time_since_last_seen < 300 if last_seen else False
            
            # Try to get direct status (if bot is in same process)
            status_info = get_bot_status(instance['guild_id'])
            
            # Prefer direct bot status if available, otherwise use database heartbeat
            if status_info.get('bot_online') is not None and _bot_instance:
                instance['bot_status'] = status_info['status']
                instance['bot_running'] = status_info.get('bot_running', False)
                instance['bot_online'] = status_info.get('bot_online', False)
            else:
                # Use database heartbeat
                instance['bot_status'] = 'active' if bot_online_db else 'inactive'
                instance['bot_running'] = bot_online_db
                instance['bot_online'] = bot_online_db
            
            # Update with real-time data if available
            if status_info.get('guild_name'):
                instance['guild_name'] = status_info['guild_name']
            if status_info.get('member_count'):
                instance['member_count'] = status_info['member_count']
        
        return instances
    finally:
        await db.close()


def get_instances_with_status_sync() -> List[Dict[str, Any]]:
    """Get all bot instances with their current status (sync wrapper)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(get_instances_with_status_async())
    finally:
        loop.close()


@app.route('/instances')
def instances():
    """List all bot instances."""
    instances_list = get_instances_with_status_sync()
    commands_list = get_all_commands()
    
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot Instances - Discord Bot Management</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: #1e1e1e;
                color: #d4d4d4;
                padding: 20px;
                line-height: 1.6;
            }
            
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            
            h1 {
                color: #4ec9b0;
                margin-bottom: 10px;
            }
            
            .subtitle {
                color: #858585;
                margin-bottom: 30px;
            }
            
            .nav-links {
                margin-bottom: 20px;
            }
            
            .nav-links a {
                color: #4ec9b0;
                text-decoration: none;
                margin-right: 20px;
            }
            
            .nav-links a:hover {
                text-decoration: underline;
            }
            
            .instance-card {
                background: #252526;
                border: 1px solid #3e3e42;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 20px;
                transition: border-color 0.2s;
            }
            
            .instance-card:hover {
                border-color: #007acc;
            }
            
            .instance-header {
                display: flex;
                justify-content: space-between;
                align-items: start;
                margin-bottom: 15px;
                flex-wrap: wrap;
                gap: 10px;
            }
            
            .instance-title {
                font-size: 1.3em;
                font-weight: 600;
                color: #4ec9b0;
            }
            
            .status-badge {
                display: inline-block;
                padding: 4px 12px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: 600;
                text-transform: uppercase;
            }
            
            .status-active { background: #1e3a1e; color: #4ec9b0; }
            .status-inactive { background: #3a1e1e; color: #f48771; }
            .status-unknown { background: #2d2d30; color: #858585; }
            .status-not_in_guild { background: #3a3a1e; color: #dcdcaa; }
            
            .instance-info {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 15px;
            }
            
            .info-item {
                display: flex;
                flex-direction: column;
            }
            
            .info-label {
                font-size: 0.85em;
                color: #858585;
                margin-bottom: 4px;
            }
            
            .info-value {
                color: #d4d4d4;
                font-weight: 500;
            }
            
            .instance-actions {
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
            }
            
            .btn {
                padding: 8px 16px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 500;
                text-decoration: none;
                display: inline-block;
                transition: background 0.2s;
            }
            
            .btn-primary {
                background: #007acc;
                color: white;
            }
            
            .btn-primary:hover {
                background: #005a9e;
            }
            
            .btn-secondary {
                background: #3e3e42;
                color: #d4d4d4;
            }
            
            .btn-secondary:hover {
                background: #505050;
            }
            
            .empty-state {
                text-align: center;
                padding: 60px 20px;
                color: #858585;
            }
            
            .empty-state h2 {
                color: #d4d4d4;
                margin-bottom: 10px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Bot Instances</h1>
            <p class="subtitle">Manage Discord bot instances and their command permissions</p>
            
            <div class="nav-links">
                <a href="/">Database Browser</a>
                <a href="/instances">Instances</a>
            </div>
    """
    
    if not instances_list:
        html += """
            <div class="empty-state">
                <h2>No Bot Instances Found</h2>
                <p>No bot instances have been registered yet. Instances are automatically created when the bot joins a server.</p>
            </div>
        """
    else:
        for instance in instances_list:
            status = instance.get('bot_status', instance.get('status', 'unknown'))
            guild_name = instance.get('guild_name', 'Unknown Server')
            guild_id = instance['guild_id']
            
            status_class = {
                'active': 'status-active',
                'inactive': 'status-inactive',
                'not_in_guild': 'status-not_in_guild',
                'unknown': 'status-unknown'
            }.get(status, 'status-unknown')
            
            member_count = instance.get('member_count', 'N/A')
            last_seen = instance.get('last_seen')
            if last_seen:
                last_seen_str = datetime.fromtimestamp(last_seen).strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_seen_str = 'Never'
            
            html += f"""
            <div class="instance-card">
                <div class="instance-header">
                    <div>
                        <div class="instance-title">{guild_name}</div>
                        <div style="color: #858585; font-size: 0.9em; margin-top: 4px;">Guild ID: {guild_id}</div>
                    </div>
                    <span class="status-badge {status_class}">{status.replace('_', ' ')}</span>
                </div>
                
                <div class="instance-info">
                    <div class="info-item">
                        <span class="info-label">Status</span>
                        <span class="info-value">{status.replace('_', ' ').title()}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Members</span>
                        <span class="info-value">{member_count}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Last Seen</span>
                        <span class="info-value">{last_seen_str}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Bot Running</span>
                        <span class="info-value">{'✅ Yes' if instance.get('bot_running') else '❌ No'}</span>
                    </div>
                </div>
                
                <div class="instance-actions">
                    <a href="/instances/{guild_id}/permissions" class="btn btn-primary">Manage Permissions</a>
                </div>
            </div>
            """
    
    html += """
        </div>
    </body>
    </html>
    """
    
    return render_template_string(html)


@app.route('/instances/<guild_id>/permissions')
def instance_permissions(guild_id):
    """Manage command permissions for a specific instance."""
    # Get instance info
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        from database import TornDatabase
        db = TornDatabase(DB_PATH)
        loop.run_until_complete(db.connect())
        
        try:
            instances = loop.run_until_complete(db.get_bot_instances())
            instance = next((i for i in instances if i['guild_id'] == guild_id), None)
            
            if not instance:
                # Create instance if it doesn't exist
                status_info = get_bot_status(guild_id)
                guild_name = status_info.get('guild_name', 'Unknown Server')
                member_count = status_info.get('member_count')
                status = 'active' if status_info.get('status') == 'active' else 'unknown'
                
                loop.run_until_complete(db.upsert_bot_instance(
                    guild_id, guild_name, status, member_count
                ))
                instance = {'guild_id': guild_id, 'guild_name': guild_name, 'status': status}
            
            # Get current permissions
            permissions = loop.run_until_complete(db.get_command_permissions(guild_id))
            
            commands_list = get_all_commands()
            
        finally:
            loop.run_until_complete(db.close())
    finally:
        loop.close()
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Permissions - {instance.get('guild_name', guild_id)}</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: #1e1e1e;
                color: #d4d4d4;
                padding: 20px;
                line-height: 1.6;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            
            h1 {{
                color: #4ec9b0;
                margin-bottom: 10px;
            }}
            
            .subtitle {{
                color: #858585;
                margin-bottom: 30px;
            }}
            
            .nav-links {{
                margin-bottom: 20px;
            }}
            
            .nav-links a {{
                color: #4ec9b0;
                text-decoration: none;
                margin-right: 20px;
            }}
            
            .nav-links a:hover {{
                text-decoration: underline;
            }}
            
            .command-section {{
                background: #252526;
                border: 1px solid #3e3e42;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 20px;
            }}
            
            .command-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 15px;
                flex-wrap: wrap;
                gap: 10px;
            }}
            
            .command-name {{
                font-size: 1.2em;
                font-weight: 600;
                color: #4ec9b0;
                font-family: 'Courier New', monospace;
            }}
            
            .permissions-list {{
                margin-bottom: 15px;
            }}
            
            .permission-item {{
                background: #2d2d30;
                padding: 10px 15px;
                border-radius: 4px;
                margin-bottom: 8px;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            
            .permission-badge {{
                display: inline-block;
                padding: 4px 10px;
                border-radius: 4px;
                font-size: 0.85em;
                font-weight: 600;
                margin-right: 10px;
            }}
            
            .badge-admin {{ background: #1e3a1e; color: #4ec9b0; }}
            .badge-role {{ background: #3a3a1e; color: #dcdcaa; }}
            .badge-user {{ background: #3a1e3a; color: #ce9178; }}
            
            .permission-value {{
                flex: 1;
                color: #d4d4d4;
            }}
            
            .add-permission-form {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                align-items: flex-end;
                padding: 15px;
                background: #2d2d30;
                border-radius: 4px;
            }}
            
            .form-group {{
                flex: 1;
                min-width: 150px;
            }}
            
            label {{
                display: block;
                color: #cccccc;
                margin-bottom: 5px;
                font-size: 14px;
            }}
            
            select, input {{
                width: 100%;
                padding: 8px 12px;
                background: #1e1e1e;
                border: 1px solid #3e3e42;
                border-radius: 4px;
                color: #d4d4d4;
                font-size: 14px;
            }}
            
            select:focus, input:focus {{
                outline: none;
                border-color: #007acc;
            }}
            
            .btn {{
                padding: 8px 16px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 500;
                transition: background 0.2s;
            }}
            
            .btn-primary {{
                background: #007acc;
                color: white;
            }}
            
            .btn-primary:hover {{
                background: #005a9e;
            }}
            
            .btn-danger {{
                background: #be1100;
                color: white;
            }}
            
            .btn-danger:hover {{
                background: #8b0d00;
            }}
            
            .btn-small {{
                padding: 4px 8px;
                font-size: 12px;
            }}
            
            .empty-permissions {{
                color: #858585;
                font-style: italic;
                padding: 15px;
                text-align: center;
            }}
            
            .note {{
                background: #2d2d30;
                border-left: 3px solid #007acc;
                padding: 12px;
                margin-bottom: 20px;
                border-radius: 4px;
                font-size: 0.9em;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔐 Command Permissions</h1>
            <p class="subtitle">Guild: {instance.get('guild_name', guild_id)} (ID: {guild_id})</p>
            
            <div class="nav-links">
                <a href="/instances">← Back to Instances</a>
            </div>
            
            <div class="note">
                <strong>Note:</strong> Permissions are checked in order: Admin users → Roles → Specific users. 
                If a user has the "Admin" permission type, they can use the command. Roles and users are checked next.
                <br><br>
                <strong>Admin:</strong> No value needed - any server administrator can use the command.
                <br>
                <strong>Role:</strong> Requires a Role ID (right-click role → Copy ID).
                <br>
                <strong>User:</strong> Requires a Discord User ID.
            </div>
    """
    
    for command_name in commands_list:
        command_perms = permissions.get(command_name, [])
        
        html += f"""
            <div class="command-section" data-command="{command_name}">
                <div class="command-header">
                    <span class="command-name">/{command_name}</span>
                </div>
                
                <div class="permissions-list">
        """
        
        if command_perms:
            for perm in command_perms:
                perm_type = perm['type']
                perm_value = perm['value']
                display_value = "Any server admin" if perm_type == 'admin' else perm_value
                html += f"""
                    <div class="permission-item">
                        <div>
                            <span class="permission-badge badge-{perm_type}">{perm_type.title()}</span>
                            <span class="permission-value">{display_value}</span>
                        </div>
                        <button class="btn btn-danger btn-small" onclick="removePermission('{command_name}', '{perm_type}', '{perm_value}')">Remove</button>
                    </div>
                """
        else:
            html += '<div class="empty-permissions">No permissions set (command is publicly accessible)</div>'
        
        html += f"""
                </div>
                
                <form class="add-permission-form" onsubmit="addPermission(event, '{command_name}')">
                    <div class="form-group">
                        <label for="perm_type_{command_name}">Permission Type:</label>
                        <select id="perm_type_{command_name}" name="permission_type" required onchange="updatePermissionValueField('{command_name}')">
                            <option value="admin">Admin (any server admin)</option>
                            <option value="role">Role (role ID)</option>
                            <option value="user">User (user ID)</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="perm_value_{command_name}" id="perm_value_label_{command_name}">Value:</label>
                        <input type="text" id="perm_value_{command_name}" name="permission_value" 
                               placeholder="Role ID or User ID">
                    </div>
                    <button type="submit" class="btn btn-primary">Add Permission</button>
                </form>
            </div>
        """
    
    html += f"""
        </div>
        
        <script>
            // Initialize form fields on page load
            document.addEventListener('DOMContentLoaded', function() {{
                const commandSections = document.querySelectorAll('[data-command]');
                commandSections.forEach(section => {{
                    const commandName = section.getAttribute('data-command');
                    updatePermissionValueField(commandName);
                }});
            }});
            
            function updatePermissionValueField(commandName) {{
                const select = document.getElementById(`perm_type_${{commandName}}`);
                const input = document.getElementById(`perm_value_${{commandName}}`);
                const label = document.getElementById(`perm_value_label_${{commandName}}`);
                
                if (!select || !input || !label) return;
                
                const permType = select.value;
                
                if (permType === 'admin') {{
                    input.required = false;
                    input.placeholder = 'Not required for admin';
                    input.value = '';
                    label.textContent = 'Value: (not required)';
                }} else if (permType === 'role') {{
                    input.required = true;
                    input.placeholder = 'Role ID (right-click role → Copy ID)';
                    label.textContent = 'Value:';
                }} else {{
                    input.required = true;
                    input.placeholder = 'User ID';
                    label.textContent = 'Value:';
                }}
            }}
            
            function addPermission(event, commandName) {{
                event.preventDefault();
                
                const form = event.target;
                const permType = form.querySelector('[name="permission_type"]').value;
                let permValue = form.querySelector('[name="permission_value"]').value.trim();
                
                // For admin, use placeholder value; for role/user, require value
                if (permType === 'admin') {{
                    permValue = 'any';  // Placeholder value for admin
                }} else if (!permValue) {{
                    alert('Please enter a permission value (Role ID or User ID)');
                    return;
                }}
                
                fetch(`/api/instances/{guild_id}/permissions`, {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json',
                    }},
                    body: JSON.stringify({{
                        command_name: commandName,
                        permission_type: permType,
                        permission_value: permValue
                    }})
                }})
                .then(response => response.json())
                .then(data => {{
                    if (data.error) {{
                        alert('Error: ' + data.error);
                    }} else {{
                        location.reload();
                    }}
                }})
                .catch(error => {{
                    alert('Error: ' + error);
                }});
            }}
            
            function removePermission(commandName, permType, permValue) {{
                const displayValue = permType === 'admin' ? 'Any server admin' : permValue;
                if (!confirm(`Remove ${{permType}} permission "${{displayValue}}" from /${{commandName}}?`)) {{
                    return;
                }}
                
                fetch(`/api/instances/{guild_id}/permissions`, {{
                    method: 'DELETE',
                    headers: {{
                        'Content-Type': 'application/json',
                    }},
                    body: JSON.stringify({{
                        command_name: commandName,
                        permission_type: permType,
                        permission_value: permValue
                    }})
                }})
                .then(response => response.json())
                .then(data => {{
                    if (data.error) {{
                        alert('Error: ' + data.error);
                    }} else {{
                        location.reload();
                    }}
                }})
                .catch(error => {{
                    alert('Error: ' + error);
                }});
            }}
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html)


@app.route('/api/instances/<guild_id>/permissions', methods=['POST'])
def api_add_permission(guild_id):
    """API endpoint to add a command permission."""
    try:
        data = request.get_json()
        command_name = data.get('command_name')
        permission_type = data.get('permission_type')
        permission_value = data.get('permission_value', '')
        
        if not command_name or not permission_type:
            return jsonify({'error': 'Missing required fields'}), 400
        
        if permission_type not in ['admin', 'role', 'user']:
            return jsonify({'error': 'Invalid permission type'}), 400
        
        # For admin, use 'any' as placeholder if no value provided
        # For role/user, require a value
        if permission_type == 'admin':
            if not permission_value or permission_value == 'any':
                permission_value = 'any'
        elif not permission_value:
            return jsonify({'error': 'Permission value is required for role and user types'}), 400
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            from database import TornDatabase
            db = TornDatabase(DB_PATH)
            loop.run_until_complete(db.connect())
            
            try:
                # Ensure instance exists
                loop.run_until_complete(db.upsert_bot_instance(guild_id))
                
                # Add permission
                loop.run_until_complete(db.set_command_permission(
                    guild_id, command_name, permission_type, permission_value
                ))
            finally:
                loop.run_until_complete(db.close())
        finally:
            loop.close()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/instances/<guild_id>/permissions', methods=['DELETE'])
def api_remove_permission(guild_id):
    """API endpoint to remove a command permission."""
    try:
        data = request.get_json()
        command_name = data.get('command_name')
        permission_type = data.get('permission_type')
        permission_value = data.get('permission_value')
        
        if not all([command_name, permission_type, permission_value]):
            return jsonify({'error': 'Missing required fields'}), 400
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            from database import TornDatabase
            db = TornDatabase(DB_PATH)
            loop.run_until_complete(db.connect())
            
            try:
                loop.run_until_complete(db.remove_command_permission(
                    guild_id, command_name, permission_type, permission_value
                ))
            finally:
                loop.run_until_complete(db.close())
        finally:
            loop.close()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


async def get_competition_progress_data_async(
    competition_id: int,
    view_type: str = 'individual',
    team_id: Optional[int] = None,
    player_ids: Optional[List[int]] = None
) -> Dict[str, Any]:
    """Get competition progress data for graphing.
    
    Args:
        competition_id: Competition ID
        view_type: 'individual' or 'team'
        team_id: Optional team ID to filter
        player_ids: Optional list of player IDs to filter
    
    Returns:
        Dict with chart data and table data
    """
    from database import TornDatabase
    
    db = TornDatabase(DB_PATH)
    await db.connect()
    try:
        # Get competition info
        comp = await db.get_competition(competition_id)
        if not comp:
            return {'error': 'Competition not found'}
        
        tracked_stat = comp['tracked_stat']
        start_date = comp['start_date']
        end_date = comp['end_date']
        
        # Get participants
        participants = await db.get_competition_participants(competition_id)
        
        # Filter participants if needed
        if team_id:
            participants = [p for p in participants if p.get('team_id') == team_id]
        if player_ids:
            participants = [p for p in participants if p['player_id'] in player_ids]
        
        if not participants:
            return {'error': 'No participants found'}
        
        # Get teams for team view
        teams = await db.get_competition_teams(competition_id)
        team_map = {t['id']: t['team_name'] for t in teams}
        
        # Get start stats for all participants
        start_stats = {}
        for participant in participants:
            player_id = participant['player_id']
            start_stat = await db.get_competition_start_stat(competition_id, player_id)
            start_stats[player_id] = start_stat if start_stat is not None else 0.0
        
        # Get historical data from player_contributor_history
        # For gym_e_spent, we need to sum multiple stats
        if tracked_stat == "gym_e_spent":
            gym_stats = ["gymstrength", "gymdefense", "gymspeed", "gymdexterity"]
            stat_names = gym_stats
        else:
            stat_names = [tracked_stat]
        
        # Collect all timestamps and values
        history_data = {}  # {player_id: [(timestamp, value), ...]}
        
        participant_ids = [p['player_id'] for p in participants]
        if not participant_ids:
            return {'error': 'No participants found'}
        
        # Build query with proper parameter binding
        placeholders_p = ','.join('?' * len(participant_ids))
        placeholders_s = ','.join('?' * len(stat_names))
        params = participant_ids + stat_names + [start_date, end_date]
        
        async with db.connection.execute(f"""
            SELECT player_id, timestamp, value
            FROM player_contributor_history
            WHERE player_id IN ({placeholders_p})
            AND stat_name IN ({placeholders_s})
            AND timestamp >= ? AND timestamp <= ?
            ORDER BY player_id, timestamp ASC
        """, params) as cursor:
            rows = await cursor.fetchall()
            
            for row in rows:
                player_id, timestamp, value = row
                if player_id not in history_data:
                    history_data[player_id] = []
                history_data[player_id].append((timestamp, value))
        
        # Process data for gym_e_spent (sum multiple stats)
        if tracked_stat == "gym_e_spent":
            # Group by timestamp and sum values
            processed_history = {}
            for player_id, data_points in history_data.items():
                # Group by timestamp
                timestamp_values = {}
                for timestamp, value in data_points:
                    if timestamp not in timestamp_values:
                        timestamp_values[timestamp] = 0.0
                    timestamp_values[timestamp] += value
                processed_history[player_id] = sorted(timestamp_values.items())
            history_data = processed_history
        
        # Build chart data
        if view_type == 'team':
            # Aggregate by team
            team_data = {}  # {team_id: {timestamp: total_progress}}
            
            for participant in participants:
                player_id = participant['player_id']
                team_id_p = participant.get('team_id')
                start_stat = start_stats.get(player_id, 0.0)
                
                if team_id_p not in team_data:
                    team_data[team_id_p] = {}
                
                # Get cumulative values over time
                if player_id in history_data:
                    cumulative = start_stat
                    for timestamp, value in history_data[player_id]:
                        cumulative = value  # Current value at this timestamp
                        progress = cumulative - start_stat
                        
                        if timestamp not in team_data[team_id_p]:
                            team_data[team_id_p][timestamp] = 0.0
                        team_data[team_id_p][timestamp] += progress
            
            # Get all unique timestamps across all teams
            all_timestamps = set()
            for timestamps_dict in team_data.values():
                all_timestamps.update(timestamps_dict.keys())
            all_timestamps = sorted(all_timestamps)
            
            # Convert to chart format with aligned timestamps
            chart_datasets = []
            table_data = []
            labels = [datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M') for ts in all_timestamps] if all_timestamps else []
            
            for team_id_p in sorted(team_data.keys()):
                team_name = team_map.get(team_id_p, f"Team {team_id_p}") if team_id_p else "No Team"
                timestamps_dict = team_data[team_id_p]
                
                # Build values array aligned to all_timestamps
                values = []
                last_value = 0.0
                for ts in all_timestamps:
                    if ts in timestamps_dict:
                        last_value = timestamps_dict[ts]
                    values.append(last_value)
                
                if values:
                    chart_datasets.append({
                        'label': team_name,
                        'data': values,
                        'borderColor': f'hsl({hash(team_name) % 360}, 70%, 50%)',
                        'backgroundColor': f'hsla({hash(team_name) % 360}, 70%, 50%, 0.1)',
                        'fill': False
                    })
                    
                    # Table data: latest value
                    latest_value = values[-1] if values else 0.0
                    table_data.append({
                        'name': team_name,
                        'latest_progress': latest_value,
                        'data_points': len([v for v in values if v > 0])
                    })
            
            return {
                'competition': comp,
                'view_type': 'team',
                'labels': labels,
                'datasets': chart_datasets,
                'table_data': sorted(table_data, key=lambda x: x['latest_progress'], reverse=True)
            }
        
        else:
            # Individual view
            # Get all unique timestamps across all players
            all_timestamps = set()
            for player_id in history_data:
                all_timestamps.update([ts for ts, _ in history_data[player_id]])
            all_timestamps = sorted(all_timestamps)
            labels = [datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M') for ts in all_timestamps] if all_timestamps else []
            
            chart_datasets = []
            table_data = []
            
            for participant in participants:
                player_id = participant['player_id']
                player_name = participant.get('player_name') or f"Player {player_id}"
                start_stat = start_stats.get(player_id, 0.0)
                
                if player_id in history_data:
                    # Build a dict for quick lookup
                    data_dict = {ts: val for ts, val in history_data[player_id]}
                    
                    # Build values array aligned to all_timestamps
                    values = []
                    last_value = start_stat
                    for ts in all_timestamps:
                        if ts in data_dict:
                            last_value = data_dict[ts]
                        # Progress = current - start
                        progress = last_value - start_stat
                        values.append(progress)
                    
                    if values:
                        chart_datasets.append({
                            'label': player_name,
                            'data': values,
                            'borderColor': f'hsl({hash(player_name) % 360}, 70%, 50%)',
                            'backgroundColor': f'hsla({hash(player_name) % 360}, 70%, 50%, 0.1)',
                            'fill': False
                        })
                        
                        # Table data
                        latest_value = values[-1] if values else 0.0
                        team_name = team_map.get(participant.get('team_id'), 'No Team') if participant.get('team_id') else 'No Team'
                        table_data.append({
                            'name': player_name,
                            'player_id': player_id,
                            'team': team_name,
                            'latest_progress': latest_value,
                            'data_points': len([v for v in values if v != 0])
                        })
            
            return {
                'competition': comp,
                'view_type': 'individual',
                'labels': labels,
                'datasets': chart_datasets,
                'table_data': sorted(table_data, key=lambda x: x['latest_progress'], reverse=True)
            }
    
    finally:
        await db.close()


def get_competition_progress_data_sync(
    competition_id: int,
    view_type: str = 'individual',
    team_id: Optional[int] = None,
    player_ids: Optional[List[int]] = None
) -> Dict[str, Any]:
    """Sync wrapper for get_competition_progress_data_async."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            get_competition_progress_data_async(competition_id, view_type, team_id, player_ids)
        )
    finally:
        loop.close()


@app.route('/competitions/<int:competition_id>/progress')
def competition_progress(competition_id: int):
    """Competition progress graph page."""
    view_type = request.args.get('view', 'individual')  # 'individual' or 'team'
    team_id = request.args.get('team_id', type=int)
    player_ids_str = request.args.get('player_ids', '')
    player_ids = [int(pid) for pid in player_ids_str.split(',') if pid.strip()] if player_ids_str else None
    
    # Get competition data
    progress_data = get_competition_progress_data_sync(competition_id, view_type, team_id, player_ids)
    
    if 'error' in progress_data:
        return f"<h1>Error</h1><p>{progress_data['error']}</p>", 404
    
    comp = progress_data['competition']
    
    # Get teams and participants for filters
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        from database import TornDatabase
        db = TornDatabase(DB_PATH)
        loop.run_until_complete(db.connect())
        try:
            teams = loop.run_until_complete(db.get_competition_teams(competition_id))
            participants = loop.run_until_complete(db.get_competition_participants(competition_id))
        finally:
            loop.run_until_complete(db.close())
    finally:
        loop.close()
    
    # Prepare variables to avoid backslashes in f-string expressions
    individual_selected = 'selected' if view_type == 'individual' else ''
    team_selected = 'selected' if view_type == 'team' else ''
    
    # Build team filter HTML
    team_filter_html = ''
    if teams and view_type == 'individual':
        team_options = []
        for t in teams:
            team_selected_attr = 'selected' if team_id == t['id'] else ''
            team_options.append(f'<option value="{t["id"]}" {team_selected_attr}>{t["team_name"]}</option>')
        team_filter_html = f'''
                        <div class="filter-item">
                            <label for="team_id">Filter by Team:</label>
                            <select id="team_id" name="team_id" onchange="this.form.submit()">
                                <option value="">All Teams</option>
                                {''.join(team_options)}
                            </select>
                        </div>
                        '''
    
    # Build table header
    if view_type == 'team':
        table_header = '<th>Team</th>'
    else:
        table_header = '<th>Player</th><th>Player ID</th><th>Team</th>'
    
    # Build table rows
    table_rows = []
    for row in progress_data['table_data']:
        if view_type == 'team':
            row_cells = f'<td>{row["name"]}</td>'
        else:
            row_cells = f'<td>{row["name"]}</td><td>{row["player_id"]}</td><td>{row["team"]}</td>'
        table_rows.append(f'''
                            <tr>
                                {row_cells}
                                <td class="number">{row["latest_progress"]:,.2f}</td>
                                <td class="number">{row["data_points"]}</td>
                            </tr>
                            ''')
    
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Competition Progress - {comp['name']}</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: #1e1e1e;
                color: #d4d4d4;
                padding: 20px;
                line-height: 1.6;
            }}
            
            .container {{
                max-width: 1600px;
                margin: 0 auto;
            }}
            
            h1 {{
                color: #4ec9b0;
                margin-bottom: 10px;
            }}
            
            .subtitle {{
                color: #858585;
                margin-bottom: 30px;
            }}
            
            .nav-links {{
                margin-bottom: 20px;
            }}
            
            .nav-links a {{
                color: #4ec9b0;
                text-decoration: none;
                margin-right: 20px;
            }}
            
            .nav-links a:hover {{
                text-decoration: underline;
            }}
            
            .filters {{
                background: #252526;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
                border: 1px solid #3e3e42;
            }}
            
            .filter-group {{
                display: flex;
                gap: 15px;
                flex-wrap: wrap;
                align-items: flex-end;
                margin-bottom: 15px;
            }}
            
            .filter-item {{
                flex: 1;
                min-width: 200px;
            }}
            
            label {{
                display: block;
                color: #cccccc;
                margin-bottom: 5px;
                font-size: 14px;
            }}
            
            select, input {{
                width: 100%;
                padding: 8px 12px;
                background: #1e1e1e;
                border: 1px solid #3e3e42;
                border-radius: 4px;
                color: #d4d4d4;
                font-size: 14px;
            }}
            
            select:focus, input:focus {{
                outline: none;
                border-color: #007acc;
            }}
            
            button {{
                padding: 10px 20px;
                background: #007acc;
                color: white;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 500;
                transition: background 0.2s;
            }}
            
            button:hover {{
                background: #005a9e;
            }}
            
            .chart-container {{
                background: #252526;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
                border: 1px solid #3e3e42;
            }}
            
            .chart-wrapper {{
                position: relative;
                height: 500px;
            }}
            
            .table-container {{
                background: #252526;
                border-radius: 8px;
                border: 1px solid #3e3e42;
                overflow-x: auto;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }}
            
            thead {{
                background: #2d2d30;
                position: sticky;
                top: 0;
            }}
            
            th {{
                padding: 12px;
                text-align: left;
                color: #4ec9b0;
                font-weight: 600;
                border-bottom: 2px solid #3e3e42;
            }}
            
            td {{
                padding: 10px 12px;
                border-bottom: 1px solid #3e3e42;
            }}
            
            tr:hover {{
                background: #2d2d30;
            }}
            
            .number {{
                text-align: right;
                color: #b5cea8;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📈 Competition Progress: {comp['name']}</h1>
            <p class="subtitle">Tracked Stat: {comp['tracked_stat']}</p>
            
            <div class="nav-links">
                <a href="/">Database Browser</a>
                <a href="/instances">Instances</a>
                <a href="/competitions">← Back to Competitions</a>
            </div>
            
            <div class="filters">
                <form method="GET" action="">
                    <div class="filter-group">
                        <div class="filter-item">
                            <label for="view">View Type:</label>
                            <select id="view" name="view" onchange="this.form.submit()">
                                <option value="individual" {individual_selected}>Individual</option>
                                <option value="team" {team_selected}>Team</option>
                            </select>
                        </div>
                        {team_filter_html}
                        <div class="filter-item">
                            <button type="submit">Apply Filters</button>
                        </div>
                    </div>
                </form>
            </div>
            
            <div class="chart-container">
                <div class="chart-wrapper">
                    <canvas id="progressChart"></canvas>
                </div>
            </div>
            
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            {table_header}
                            <th class="number">Latest Progress</th>
                            <th class="number">Data Points</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(table_rows)}
                    </tbody>
                </table>
            </div>
        </div>
        
        <script>
            const ctx = document.getElementById('progressChart').getContext('2d');
            const chartData = {json.dumps({
                'labels': progress_data['labels'],
                'datasets': progress_data['datasets']
            })};
            
            new Chart(ctx, {{
                type: 'line',
                data: chartData,
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                color: '#d4d4d4',
                                font: {{
                                    size: 12
                                }}
                            }}
                        }},
                        title: {{
                            display: true,
                            text: 'Competition Progress Over Time',
                            color: '#4ec9b0',
                            font: {{
                                size: 16,
                                weight: 'bold'
                            }}
                        }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            backgroundColor: '#252526',
                            titleColor: '#4ec9b0',
                            bodyColor: '#d4d4d4',
                            borderColor: '#3e3e42',
                            borderWidth: 1
                        }}
                    }},
                    scales: {{
                        x: {{
                            ticks: {{
                                color: '#858585',
                                maxRotation: 45,
                                minRotation: 45
                            }},
                            grid: {{
                                color: '#3e3e42'
                            }}
                        }},
                        y: {{
                            ticks: {{
                                color: '#858585'
                            }},
                            grid: {{
                                color: '#3e3e42'
                            }},
                            title: {{
                                display: true,
                                text: 'Progress ({comp["tracked_stat"]})',
                                color: '#858585'
                            }}
                        }}
                    }},
                    interaction: {{
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html)


@app.route('/competitions')
def competitions_list():
    """List all competitions."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        from database import TornDatabase
        db = TornDatabase(DB_PATH)
        loop.run_until_complete(db.connect())
        try:
            competitions = loop.run_until_complete(db.list_competitions())
        finally:
            loop.run_until_complete(db.close())
    finally:
        loop.close()
    
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Competitions</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: #1e1e1e;
                color: #d4d4d4;
                padding: 20px;
                line-height: 1.6;
            }
            
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            
            h1 {
                color: #4ec9b0;
                margin-bottom: 10px;
            }
            
            .subtitle {
                color: #858585;
                margin-bottom: 30px;
            }
            
            .nav-links {
                margin-bottom: 20px;
            }
            
            .nav-links a {
                color: #4ec9b0;
                text-decoration: none;
                margin-right: 20px;
            }
            
            .nav-links a:hover {
                text-decoration: underline;
            }
            
            .competition-card {
                background: #252526;
                border: 1px solid #3e3e42;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 20px;
                transition: border-color 0.2s;
            }
            
            .competition-card:hover {
                border-color: #007acc;
            }
            
            .competition-header {
                display: flex;
                justify-content: space-between;
                align-items: start;
                margin-bottom: 15px;
                flex-wrap: wrap;
                gap: 10px;
            }
            
            .competition-title {
                font-size: 1.3em;
                font-weight: 600;
                color: #4ec9b0;
            }
            
            .status-badge {
                display: inline-block;
                padding: 4px 12px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: 600;
                text-transform: uppercase;
            }
            
            .status-active { background: #1e3a1e; color: #4ec9b0; }
            .status-cancelled { background: #3a1e1e; color: #f48771; }
            .status-completed { background: #3a3a1e; color: #dcdcaa; }
            
            .competition-info {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 15px;
            }
            
            .info-item {
                display: flex;
                flex-direction: column;
            }
            
            .info-label {
                font-size: 0.85em;
                color: #858585;
                margin-bottom: 4px;
            }
            
            .info-value {
                color: #d4d4d4;
                font-weight: 500;
            }
            
            .btn {
                padding: 8px 16px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 500;
                text-decoration: none;
                display: inline-block;
                transition: background 0.2s;
                background: #007acc;
                color: white;
            }
            
            .btn:hover {
                background: #005a9e;
            }
            
            .empty-state {
                text-align: center;
                padding: 60px 20px;
                color: #858585;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Competitions</h1>
            <p class="subtitle">View and track competition progress</p>
            
            <div class="nav-links">
                <a href="/">Database Browser</a>
                <a href="/instances">Instances</a>
                <a href="/competitions">Competitions</a>
            </div>
    """
    
    if not competitions:
        html += """
            <div class="empty-state">
                <h2>No Competitions Found</h2>
                <p>No competitions have been created yet.</p>
            </div>
        """
    else:
        for comp in competitions:
            status = comp.get('status', 'active')
            status_class = f'status-{status}'
            start_date_str = datetime.fromtimestamp(comp['start_date']).strftime('%Y-%m-%d') if comp.get('start_date') else 'N/A'
            end_date_str = datetime.fromtimestamp(comp['end_date']).strftime('%Y-%m-%d') if comp.get('end_date') else 'N/A'
            
            html += f"""
            <div class="competition-card">
                <div class="competition-header">
                    <div>
                        <div class="competition-title">{comp['name']}</div>
                        <div style="color: #858585; font-size: 0.9em; margin-top: 4px;">ID: {comp['id']}</div>
                    </div>
                    <span class="status-badge {status_class}">{status}</span>
                </div>
                
                <div class="competition-info">
                    <div class="info-item">
                        <span class="info-label">Tracked Stat</span>
                        <span class="info-value">{comp['tracked_stat']}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Start Date</span>
                        <span class="info-value">{start_date_str}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">End Date</span>
                        <span class="info-value">{end_date_str}</span>
                    </div>
                </div>
                
                <a href="/competitions/{comp['id']}/progress" class="btn">View Progress Graph</a>
            </div>
            """
    
    html += """
        </div>
    </body>
    </html>
    """
    
    return render_template_string(html)


@app.route('/api/competitions')
def api_competitions():
    """API endpoint to get list of competitions."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        from database import TornDatabase
        db = TornDatabase(DB_PATH)
        loop.run_until_complete(db.connect())
        try:
            competitions = loop.run_until_complete(db.list_competitions())
        finally:
            loop.run_until_complete(db.close())
    finally:
        loop.close()
    
    return jsonify({'competitions': competitions})


if __name__ == '__main__':
    # Check if database exists
    if not Path(DB_PATH).exists():
        print(f"Warning: Database file not found at {DB_PATH}")
        print("Set DATABASE_PATH environment variable if database is elsewhere")
    
    port = int(os.getenv('WEB_PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    print(f"Starting web interface on http://localhost:{port}")
    print(f"Database: {DB_PATH}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)