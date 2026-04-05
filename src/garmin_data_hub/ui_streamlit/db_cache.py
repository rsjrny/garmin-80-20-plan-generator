"""Streamlit utilities for caching database connections and queries."""
from __future__ import annotations

import streamlit as st
from pathlib import Path
import sqlite3

from garmin_data_hub.paths import default_db_path
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema


@st.cache_resource
def get_db_connection():
    """
    Get a cached database connection that persists across Streamlit reruns.
    This avoids creating a new connection on every interaction.
    """
    db_path = default_db_path()
    conn = connect_sqlite(db_path)
    
    # Ensure schema is applied
    schema_path = Path(__file__).resolve().parents[1] / "db" / "schema.sql"
    apply_schema(conn, schema_path)
    
    return conn


def close_connection():
    """Close the cached database connection if needed (called on app exit)."""
    # Note: Streamlit will handle cleanup automatically, but this is here
    # in case manual cleanup is needed in the future.
    pass
