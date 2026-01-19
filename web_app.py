"""Simple Flask web interface for database browsing."""

from flask import Flask, render_template_string, request, jsonify
import aiosqlite
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import os

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
    return render_template_string(HTML_TEMPLATE)


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