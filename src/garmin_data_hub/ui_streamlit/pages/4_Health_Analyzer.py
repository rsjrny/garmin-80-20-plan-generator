from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import default_db_path, ensure_app_dirs
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar

st.set_page_config(page_title="Health Analyzer", layout="wide")

APP_INTERNAL_TABLES = {
    "planned_workout",
    "activity_metrics",
    "athlete_profile",
    "app_settings",
    "activity_trackpoints",
    "schema_migrations",
}

HEALTH_TABLE_KEYWORDS = (
    "sleep",
    "stress",
    "body",
    "battery",
    "wellness",
    "hrv",
    "heart",
    "pulse",
    "respiration",
    "oxygen",
    "spo2",
    "weight",
    "hydration",
    "daily",
)

READINESS_POSITIVE_KEYWORDS = (
    "hrv",
    "body_battery",
    "sleep",
    "spo2",
    "oxygen",
    "recovery",
)

READINESS_NEGATIVE_KEYWORDS = (
    "stress",
    "resting_hr",
    "resting_heart",
    "strain",
    "fatigue",
)


@st.cache_data(ttl=600, show_spinner=False)
def get_table_list(db_path: str, db_mtime: float) -> list[str]:
    conn = connect_sqlite(Path(db_path))
    try:
        rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        return [r[0] for r in rows if r and r[0]]
    finally:
        conn.close()


@st.cache_data(ttl=600, show_spinner=False)
def get_table_columns(db_path: str, db_mtime: float, table_name: str) -> pd.DataFrame:
    conn = connect_sqlite(Path(db_path))
    try:
        df = pd.read_sql_query(
            f'PRAGMA table_info("{table_name.replace(chr(34), chr(34) * 2)}")', conn
        )
        return df
    finally:
        conn.close()


@st.cache_data(ttl=600, show_spinner=False)
def get_table_row_count(db_path: str, db_mtime: float, table_name: str) -> int:
    conn = connect_sqlite(Path(db_path))
    try:
        row = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name.replace(chr(34), chr(34) * 2)}"'
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()


@st.cache_data(ttl=600, show_spinner=False)
def load_health_data(
    db_path: str,
    db_mtime: float,
    table_name: str,
    date_col: str,
    metric_cols: tuple[str, ...],
    row_limit: int,
) -> pd.DataFrame:
    conn = connect_sqlite(Path(db_path))
    try:
        quoted_table = f'"{table_name.replace(chr(34), chr(34) * 2)}"'
        quoted_date = f'"{date_col.replace(chr(34), chr(34) * 2)}"'
        quoted_metrics = [f'"{c.replace(chr(34), chr(34) * 2)}"' for c in metric_cols]
        select_cols = ", ".join([quoted_date] + quoted_metrics)

        sql = f"""
            SELECT {select_cols}
            FROM {quoted_table}
            WHERE {quoted_date} IS NOT NULL
            ORDER BY {quoted_date} DESC
            LIMIT ?
        """
        return pd.read_sql_query(sql, conn, params=[int(row_limit)])
    finally:
        conn.close()


def pick_health_tables(all_tables: list[str]) -> list[str]:
    filtered = []
    for table_name in all_tables:
        if table_name in APP_INTERNAL_TABLES:
            continue
        lower_name = table_name.lower()
        if any(keyword in lower_name for keyword in HEALTH_TABLE_KEYWORDS):
            filtered.append(table_name)
    return filtered


def detect_date_columns(columns_df: pd.DataFrame) -> list[str]:
    if columns_df.empty:
        return []

    candidates: list[str] = []
    priority_tokens = (
        "date",
        "day",
        "time",
        "timestamp",
        "utc",
        "gmt",
        "local",
    )

    for _, row in columns_df.iterrows():
        name = str(row.get("name", ""))
        col_type = str(row.get("type", "")).lower()
        name_lower = name.lower()
        if not name:
            continue

        if any(token in name_lower for token in priority_tokens):
            candidates.append(name)
            continue

        if "date" in col_type or "time" in col_type:
            candidates.append(name)

    return candidates


def detect_numeric_columns(columns_df: pd.DataFrame) -> list[str]:
    if columns_df.empty:
        return []

    numeric_tokens = ("int", "real", "float", "double", "numeric", "decimal")
    cols: list[str] = []

    for _, row in columns_df.iterrows():
        name = str(row.get("name", ""))
        col_type = str(row.get("type", "")).lower()
        if not name:
            continue
        if any(token in col_type for token in numeric_tokens):
            cols.append(name)

    return cols


def render_metric_kpis(df: pd.DataFrame, date_col: str, metric_cols: list[str]) -> None:
    if df.empty:
        return

    kpi_cols = st.columns(min(4, max(1, len(metric_cols))))
    for idx, metric in enumerate(metric_cols[:4]):
        series = pd.to_numeric(df[metric], errors="coerce").dropna()
        if series.empty:
            value = "n/a"
            delta = None
        else:
            value = f"{series.iloc[-1]:.2f}"
            delta = None
            if len(series) > 1:
                delta = f"{series.iloc[-1] - series.iloc[-2]:+.2f}"

        with kpi_cols[idx]:
            st.metric(label=f"Latest {metric}", value=value, delta=delta)


def infer_metric_inversion(metric_name: str) -> bool:
    name = metric_name.lower()
    if any(token in name for token in READINESS_NEGATIVE_KEYWORDS):
        return True
    if any(token in name for token in READINESS_POSITIVE_KEYWORDS):
        return False
    return False


def build_readiness_frame(
    source_df: pd.DataFrame,
    date_col: str,
    metric_cols: list[str],
    inverted_metrics: set[str],
) -> pd.DataFrame:
    work = source_df[[date_col] + metric_cols].copy()
    work["readiness_date"] = pd.to_datetime(work[date_col], errors="coerce").dt.date
    work = work.dropna(subset=["readiness_date"])
    if work.empty:
        return pd.DataFrame()

    daily = (
        work.groupby("readiness_date", as_index=False)[metric_cols]
        .mean(numeric_only=True)
        .rename(columns={"readiness_date": date_col})
        .sort_values(date_col)
    )

    component_cols: list[str] = []
    for metric in metric_cols:
        series = pd.to_numeric(daily[metric], errors="coerce")
        std = float(series.std(skipna=True))
        if std == 0 or np.isnan(std):
            z_score = pd.Series(0.0, index=series.index)
        else:
            z_score = (series - series.mean(skipna=True)) / std

        direction = -1.0 if metric in inverted_metrics else 1.0
        component_col = f"{metric}__score"
        daily[component_col] = z_score * direction
        component_cols.append(component_col)

    daily["readiness_z"] = daily[component_cols].mean(axis=1, skipna=True)
    daily["readiness_score"] = (50.0 + (daily["readiness_z"] * 15.0)).clip(0, 100)
    daily["readiness_baseline_7d"] = (
        daily["readiness_score"].rolling(window=7, min_periods=1).mean()
    )
    daily["readiness_delta_vs_7d"] = (
        daily["readiness_score"] - daily["readiness_baseline_7d"]
    )
    return daily


ensure_app_dirs()
db_path = default_db_path()
db_mtime = os.path.getmtime(db_path) if db_path.exists() else 0.0

conn = connect_sqlite(db_path)
schema_path = Path(__file__).resolve().parents[2] / "db" / "schema.sql"
apply_schema(conn, schema_path)
render_sidebar(conn)

st.header("Garmin Health Analyzer")
st.caption("Explore health-oriented Garmin tables stored in your database.")

if not db_path.exists():
    st.error("Database file not found.")
    st.stop()

all_tables = get_table_list(str(db_path), db_mtime)
health_tables = pick_health_tables(all_tables)

if not health_tables:
    st.warning(
        "No obvious health tables were detected. Use 'Show all tables' to inspect your DB."
    )

show_all_tables = st.checkbox("Show all tables", value=False)
table_options = all_tables if show_all_tables else health_tables

if not table_options:
    st.info("No tables available to analyze with current filters.")
    st.stop()

selected_table = st.selectbox("Health table", table_options)
columns_df = get_table_columns(str(db_path), db_mtime, selected_table)

if columns_df.empty:
    st.error("Could not inspect table columns.")
    st.stop()

date_candidates = detect_date_columns(columns_df)
if not date_candidates:
    st.error("No date/time-like column detected in this table.")
    st.dataframe(columns_df, width="stretch")
    st.stop()

numeric_candidates = detect_numeric_columns(columns_df)
if not numeric_candidates:
    st.error("No numeric metric columns detected in this table.")
    st.dataframe(columns_df, width="stretch")
    st.stop()

c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    selected_date_col = st.selectbox("Date column", date_candidates)
with c2:
    default_metrics = numeric_candidates[: min(3, len(numeric_candidates))]
    selected_metrics = st.multiselect(
        "Metrics",
        options=numeric_candidates,
        default=default_metrics,
    )
with c3:
    row_limit = st.selectbox("Rows", [2000, 5000, 10000, 20000], index=1)

if not selected_metrics:
    st.info("Select at least one metric to continue.")
    st.stop()

raw_df = load_health_data(
    str(db_path),
    db_mtime,
    selected_table,
    selected_date_col,
    tuple(selected_metrics),
    row_limit,
)

if raw_df.empty:
    st.info("No data found in the selected table for current settings.")
    st.stop()

raw_df[selected_date_col] = pd.to_datetime(raw_df[selected_date_col], errors="coerce")
raw_df = raw_df.dropna(subset=[selected_date_col]).sort_values(selected_date_col)

for metric in selected_metrics:
    raw_df[metric] = pd.to_numeric(raw_df[metric], errors="coerce")

if raw_df.empty:
    st.info("No valid datetime rows found after parsing the selected date column.")
    st.stop()

now = datetime.now(timezone.utc)
time_window = st.selectbox(
    "Time window",
    ["Last 30 Days", "Last 90 Days", "Last 180 Days", "Last Year", "All Time"],
    index=1,
)

if time_window == "Last 30 Days":
    cutoff = now - timedelta(days=30)
elif time_window == "Last 90 Days":
    cutoff = now - timedelta(days=90)
elif time_window == "Last 180 Days":
    cutoff = now - timedelta(days=180)
elif time_window == "Last Year":
    cutoff = now - timedelta(days=365)
else:
    cutoff = pd.Timestamp("1900-01-01", tz="UTC").to_pydatetime()

if raw_df[selected_date_col].dt.tz is None:
    raw_df[selected_date_col] = raw_df[selected_date_col].dt.tz_localize("UTC")

df = raw_df[raw_df[selected_date_col] >= cutoff].copy()

if df.empty:
    st.info("No data points in the selected time window.")
    st.stop()

aggregation_mode = st.radio("Aggregation", ["Raw", "Daily Mean"], horizontal=True)
if aggregation_mode == "Daily Mean":
    df["chart_date"] = df[selected_date_col].dt.date
    df = (
        df.groupby("chart_date", as_index=False)[selected_metrics]
        .mean(numeric_only=True)
        .rename(columns={"chart_date": selected_date_col})
    )

analysis_mode = st.radio(
    "Analysis Mode",
    ["Metric Explorer", "Readiness Analyzer"],
    horizontal=True,
)

st.subheader("Overview")
overview_col1, overview_col2, overview_col3 = st.columns(3)
with overview_col1:
    st.metric("Rows in table", f"{get_table_row_count(str(db_path), db_mtime, selected_table):,}")
with overview_col2:
    st.metric("Rows in window", f"{len(df):,}")
with overview_col3:
    st.metric("Metrics selected", str(len(selected_metrics)))

if analysis_mode == "Metric Explorer":
    render_metric_kpis(df, selected_date_col, selected_metrics)

    plot_df = df.melt(
        id_vars=[selected_date_col],
        value_vars=selected_metrics,
        var_name="metric",
        value_name="value",
    ).dropna(subset=["value"])

    if plot_df.empty:
        st.info("No numeric values available for charting in the selected window.")
        st.stop()

    st.subheader("Trend Chart")
    chart = (
        alt.Chart(plot_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{selected_date_col}:T", title="Date"),
            y=alt.Y("value:Q", title="Value"),
            color=alt.Color("metric:N", title="Metric"),
            tooltip=[selected_date_col, "metric", alt.Tooltip("value", format=".2f")],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")

    st.subheader("Data Table")
    st.dataframe(
        df[[selected_date_col] + selected_metrics].sort_values(
            selected_date_col, ascending=False
        ),
        width="stretch",
    )
else:
    st.subheader("Readiness Configuration")
    default_inverted = [m for m in selected_metrics if infer_metric_inversion(m)]
    inverted_metrics = st.multiselect(
        "Invert metrics where higher means more strain",
        options=selected_metrics,
        default=default_inverted,
        help="Inverted metrics lower readiness when values increase.",
    )

    readiness_df = build_readiness_frame(
        source_df=df,
        date_col=selected_date_col,
        metric_cols=selected_metrics,
        inverted_metrics=set(inverted_metrics),
    )
    if readiness_df.empty:
        st.info("Not enough usable data to compute readiness.")
        st.stop()

    latest = readiness_df.dropna(subset=["readiness_score"]).tail(1)
    if latest.empty:
        st.info("Readiness score could not be calculated from selected metrics.")
        st.stop()

    latest_row = latest.iloc[0]
    readiness_cols = st.columns(3)
    with readiness_cols[0]:
        st.metric("Current Readiness", f"{latest_row['readiness_score']:.1f}")
    with readiness_cols[1]:
        st.metric("7-day Baseline", f"{latest_row['readiness_baseline_7d']:.1f}")
    with readiness_cols[2]:
        st.metric(
            "Delta vs 7-day",
            f"{latest_row['readiness_delta_vs_7d']:+.1f}",
        )

    st.subheader("Readiness Trend")
    readiness_plot = readiness_df[
        [selected_date_col, "readiness_score", "readiness_baseline_7d"]
    ].melt(
        id_vars=[selected_date_col],
        value_vars=["readiness_score", "readiness_baseline_7d"],
        var_name="series",
        value_name="score",
    )
    readiness_chart = (
        alt.Chart(readiness_plot)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{selected_date_col}:T", title="Date"),
            y=alt.Y("score:Q", title="Readiness (0-100)", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                "series:N",
                scale=alt.Scale(
                    domain=["readiness_score", "readiness_baseline_7d"],
                    range=["#1f77b4", "#ff7f0e"],
                ),
                title="Series",
            ),
            tooltip=[selected_date_col, "series", alt.Tooltip("score", format=".2f")],
        )
        .interactive()
    )
    st.altair_chart(readiness_chart, width="stretch")

    direction_df = pd.DataFrame(
        {
            "Metric": selected_metrics,
            "Direction": [
                "Inverted (higher lowers readiness)"
                if metric in set(inverted_metrics)
                else "Positive (higher raises readiness)"
                for metric in selected_metrics
            ],
        }
    )

    st.subheader("Metric Directions")
    st.dataframe(direction_df, width="stretch", hide_index=True)

    st.subheader("Readiness Data Table")
    display_cols = [
        selected_date_col,
        "readiness_score",
        "readiness_baseline_7d",
        "readiness_delta_vs_7d",
    ] + selected_metrics
    st.dataframe(
        readiness_df[display_cols].sort_values(selected_date_col, ascending=False),
        width="stretch",
    )

conn.close()
