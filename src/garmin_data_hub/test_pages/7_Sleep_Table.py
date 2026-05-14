from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import default_db_path, ensure_app_dirs
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar

st.set_page_config(page_title="Sleep Table", layout="wide")

RANGE_OPTIONS = {
    "7": 7,
    "30": 30,
    "60": 60,
    "90": 90,
    "year": 365,
}


def _sleep_table_exists(conn) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sleep'"
    ).fetchone()
    return row is not None


def _sleep_columns(conn) -> list[str]:
    rows = conn.execute("PRAGMA table_info(sleep)").fetchall()
    return [str(row[1]) for row in rows]


def _detect_date_column(columns: list[str]) -> str | None:
    candidates = [
        "calendar_date",
        "date",
        "sleep_start_timestamp_local",
        "sleep_start_timestamp_gmt",
        "sleep_start_timestamp_utc",
    ]
    lowered = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def _load_sleep_dataframe(conn, days: int) -> tuple[pd.DataFrame, str | None]:
    columns = _sleep_columns(conn)
    date_col = _detect_date_column(columns)

    if date_col:
        escaped_date_col = date_col.replace('"', '""')
        query = (
            f'SELECT * FROM sleep WHERE date("{escaped_date_col}") >= '
            "date('now', ?) ORDER BY date(\""
            f"{escaped_date_col}"
            "\") DESC"
        )
        df = pd.read_sql_query(query, conn, params=(f"-{days} day",))
    else:
        # Fallback for unexpected schemas that do not expose a date-like column.
        df = pd.read_sql_query("SELECT * FROM sleep ORDER BY rowid DESC", conn)

    return df, date_col


ensure_app_dirs()
conn = connect_sqlite(default_db_path())
schema_path = Path(__file__).resolve().parents[2] / "db" / "schema.sql"
apply_schema(conn, schema_path)

try:
    render_sidebar(conn)

    st.header("Sleep Table")
    st.caption("Click any column header to sort.")

    selected_window = st.radio(
        "Window",
        options=list(RANGE_OPTIONS.keys()),
        horizontal=True,
        index=1,
    )
    selected_days = RANGE_OPTIONS[selected_window]

    if not _sleep_table_exists(conn):
        st.error("Table 'sleep' was not found in the database.")
        st.stop()

    sleep_df, date_col = _load_sleep_dataframe(conn, selected_days)

    if date_col is None:
        st.warning(
            "No date column found on sleep table; showing all rows without time filter."
        )

    col1, col2 = st.columns(2)
    col1.metric("Rows", f"{len(sleep_df):,}")
    col2.metric("Columns", len(sleep_df.columns))

    if sleep_df.empty:
        st.info("No sleep rows found for the selected window.")
    else:
        st.dataframe(sleep_df, use_container_width=True, hide_index=True)
finally:
    conn.close()
