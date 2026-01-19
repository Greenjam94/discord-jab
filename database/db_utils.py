"""Utility functions for database operations."""

import aiosqlite
from typing import Optional, Dict, Any
from datetime import datetime


def competition_row_to_dict(row: Optional[aiosqlite.Row]) -> Optional[Dict[str, Any]]:
    """Convert a competition database row to a dictionary.
    
    Args:
        row: Database row (id, name, tracked_stat, start_date, end_date, status, created_at, created_by)
        
    Returns:
        Competition dict if row exists, None otherwise
    """
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


def competition_row_to_dict_list(rows: list) -> list:
    """Convert a list of competition database rows to dictionaries.
    
    Args:
        rows: List of database rows
        
    Returns:
        List of competition dicts
    """
    return [competition_row_to_dict(row) for row in rows]
