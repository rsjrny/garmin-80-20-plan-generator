from __future__ import annotations
import sqlite3
from datetime import datetime
from typing import Any
import pandas as pd


def list_recent_activities(
    conn: sqlite3.Connection, limit: int = 200
) -> list[dict[str, Any]]:
    # Delegate to central DB queries helper
    try:
        from garmin_data_hub.db import queries as db_queries

        return db_queries.list_recent_activities(conn, limit=int(limit))
    except Exception:
        return []


def get_activity_records(conn: sqlite3.Connection, activity_id: int) -> pd.DataFrame:
    """
    Fetches time-series records for a specific activity.
    """
    # We need to join through session to get records for this activity
    try:
        from garmin_data_hub.db import queries as db_queries

        return db_queries.get_activity_records(conn, activity_id)
    except Exception:
        return pd.DataFrame()


def problems_summary(
    conn: sqlite3.Connection, limit: int = 200
) -> list[dict[str, Any]]:
    # Read legacy `problems` rows via central helper if available.
    try:
        from garmin_data_hub.db import queries as db_queries

        return db_queries.get_problems(conn, limit=int(limit))
    except Exception:
        return []
