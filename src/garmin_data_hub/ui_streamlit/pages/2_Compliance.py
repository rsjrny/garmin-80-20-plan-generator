from __future__ import annotations
import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json

from garmin_data_hub.paths import default_db_path, ensure_app_dirs
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar
from garmin_data_hub.db import queries

# Opt-in to new pandas behavior to silence downcasting warnings
pd.set_option("future.no_silent_downcasting", True)

st.set_page_config(page_title="Compliance", layout="wide")

# Initialize session state for selections
if "compliance_metric" not in st.session_state:
    st.session_state.compliance_metric = "Duration"
if "compliance_view" not in st.session_state:
    st.session_state.compliance_view = "Weekly"

ensure_app_dirs()
db_path = default_db_path()

# Connect to DB
conn = connect_sqlite(db_path)
schema_path = Path(__file__).resolve().parents[2] / "db" / "schema.sql"
apply_schema(conn, schema_path)

# Render Sidebar
unit_system = render_sidebar(conn)

st.header("Plan Compliance")

# Fetch current athlete profile
athlete_profile = queries.get_athlete_profile(conn)

if athlete_profile:
    hrmax = athlete_profile["hrmax_override"] or athlete_profile["hrmax_calc"]
    lthr = athlete_profile["lthr_override"] or athlete_profile["lthr_calc"]
    calc_time = athlete_profile["calc_updated_utc"]

    # Display current profile info
    col_info_1, col_info_2, col_info_3 = st.columns(3)
    col_info_1.metric(
        "HRmax",
        f"{hrmax} bpm" if hrmax else "—",
        delta="Override" if athlete_profile["hrmax_override"] else "Calculated",
    )
    col_info_2.metric(
        "LTHR",
        f"{lthr} bpm" if lthr else "—",
        delta="Override" if athlete_profile["lthr_override"] else "Calculated",
    )
    col_info_3.caption(f"Updated: {calc_time}")
else:
    hrmax = lthr = None
    st.warning("No athlete profile found. Run ingest to calculate HRmax/LTHR.")

# --- Data Fetching ---

# 1. Get Planned Workouts
planned_df = pd.read_sql_query(
    """
    SELECT 
        scheduled_date, 
        workout_name, 
        planned_distance_m, 
        planned_duration_s
    FROM planned_workout
    ORDER BY scheduled_date ASC
    """,
    conn,
)

if not planned_df.empty:
    plan_start_ts_iso = pd.to_datetime(planned_df["scheduled_date"]).min().isoformat()
else:
    plan_start_ts_iso = "1970-01-01T00:00:00Z"

# 2. Get Actual Activities (persisted metrics only; no per-page temp tables)
try:
    actual_raw_df = queries.get_activities_dataframe(
        conn,
        start_ts_iso=plan_start_ts_iso,
        lthr=lthr,
        use_temp_zone_metrics=False,
    )
except TypeError:
    # Old signature fallback
    actual_raw_df = queries.get_activities_dataframe(conn, start_ts_iso=plan_start_ts_iso)

if actual_raw_df.empty:
    actual_df = pd.DataFrame(
        columns=[
            "activity_date",
            "sport",
            "actual_distance_m",
            "actual_duration_s",
            "zone_1_s",
            "zone_2_s",
            "zone_3_s",
            "zone_4_s",
            "zone_5_s",
        ]
    )
else:
    actual_raw_df["activity_date"] = pd.to_datetime(
        actual_raw_df["start_time_utc"], errors="coerce"
    ).dt.normalize()

    for zone_col in ["zone_1_s", "zone_2_s", "zone_3_s", "zone_4_s", "zone_5_s"]:
        if zone_col not in actual_raw_df.columns:
            actual_raw_df[zone_col] = 0

    actual_df = (
        actual_raw_df.dropna(subset=["activity_date"])
        .groupby(["activity_date", "sport"], as_index=False)
        .agg(
            {
                "total_distance_m": "sum",
                "total_elapsed_s": "sum",
                "zone_1_s": "sum",
                "zone_2_s": "sum",
                "zone_3_s": "sum",
                "zone_4_s": "sum",
                "zone_5_s": "sum",
            }
        )
        .rename(
            columns={
                "total_distance_m": "actual_distance_m",
                "total_elapsed_s": "actual_duration_s",
            }
        )
    )

conn.close()

if planned_df.empty:
    st.info("No training plan found. Go to 'Build Plan' to generate one.")
    st.stop()

# --- Data Processing ---

# Convert dates
planned_df["date"] = pd.to_datetime(planned_df["scheduled_date"])
actual_df["date"] = pd.to_datetime(actual_df["activity_date"])

# Get plan date range
plan_start = planned_df["date"].min()
plan_end = planned_df["date"].max()

# Filter Actuals by Sport (Optional) - Only show sports used within plan dates
actual_df_in_range = actual_df[
    (actual_df["date"] >= plan_start) & (actual_df["date"] <= plan_end)
]
sports = actual_df_in_range["sport"].unique().tolist()
if sports:
    selected_sport = st.multiselect(
        "Filter Actual Activities by Sport", options=sports, default=sports
    )
    if selected_sport:
        actual_df = actual_df[actual_df["sport"].isin(selected_sport)]

# Re-aggregate actuals by date after filtering (in case multiple sports on same day)
actual_agg = (
    actual_df.groupby("date")
    .agg(
        {
            "actual_distance_m": "sum",
            "actual_duration_s": "sum",
            "zone_1_s": "sum",
            "zone_2_s": "sum",
            "zone_3_s": "sum",
            "zone_4_s": "sum",
            "zone_5_s": "sum",
        }
    )
    .reset_index()
)

# Merge Dataframes
merged_df = pd.merge(planned_df, actual_agg, on="date", how="outer")

# Fill numeric columns with 0 and string columns with placeholder
# Use astype to avoid downcasting warnings
numeric_cols = {
    "planned_distance_m": 0,
    "planned_duration_s": 0,
    "actual_distance_m": 0,
    "actual_duration_s": 0,
    "zone_1_s": 0,
    "zone_2_s": 0,
    "zone_3_s": 0,
    "zone_4_s": 0,
    "zone_5_s": 0,
}
string_cols = {"workout_name": "Unplanned"}

merged_df = merged_df.fillna(numeric_cols)
# Explicitly convert numeric columns to float to avoid downcasting
for col in numeric_cols.keys():
    if col in merged_df.columns:
        merged_df[col] = merged_df[col].astype("float64")
merged_df = merged_df.fillna(string_cols)
# Ensure all object columns are properly inferred to avoid FutureWarning
merged_df = merged_df.infer_objects(copy=False)

# --- Filtering ---
# 1. Filter by Plan Range
if not planned_df.empty:
    plan_start = planned_df["date"].min()
    plan_end = planned_df["date"].max()
    merged_df = merged_df[
        (merged_df["date"] >= plan_start) & (merged_df["date"] <= plan_end)
    ]

# 2. Filter Future Dates (Show only up to today)
today = pd.Timestamp.now().normalize()
merged_df = merged_df[merged_df["date"] <= today]

if merged_df.empty:
    st.info("No data available for the current plan period (up to today).")
    st.stop()

# Calculate Weekly Aggregates
merged_df["week_start"] = merged_df["date"] - pd.to_timedelta(
    merged_df["date"].dt.weekday, unit="D"
)

weekly_df = (
    merged_df.groupby("week_start")
    .agg(
        {
            "planned_distance_m": "sum",
            "actual_distance_m": "sum",
            "planned_duration_s": "sum",
            "actual_duration_s": "sum",
        }
    )
    .reset_index()
)

# Create daily dataframe for display
daily_df = merged_df[
    [
        "date",
        "planned_distance_m",
        "actual_distance_m",
        "planned_duration_s",
        "actual_duration_s",
    ]
].copy()

# Unit Conversion
if unit_system == "Imperial":
    weekly_df["Planned Distance"] = (
        weekly_df["planned_distance_m"] * 0.000621371
    )  # miles
    weekly_df["Actual Distance"] = weekly_df["actual_distance_m"] * 0.000621371  # miles
    daily_df["Planned Distance"] = daily_df["planned_distance_m"] * 0.000621371  # miles
    daily_df["Actual Distance"] = daily_df["actual_distance_m"] * 0.000621371  # miles
    dist_unit = "mi"
else:
    weekly_df["Planned Distance"] = weekly_df["planned_distance_m"] / 1000.0  # km
    weekly_df["Actual Distance"] = weekly_df["actual_distance_m"] / 1000.0  # km
    daily_df["Planned Distance"] = daily_df["planned_distance_m"] / 1000.0  # km
    daily_df["Actual Distance"] = daily_df["actual_distance_m"] / 1000.0  # km
    dist_unit = "km"

weekly_df["Planned Duration"] = weekly_df["planned_duration_s"] / 3600.0  # hours
weekly_df["Actual Duration"] = weekly_df["actual_duration_s"] / 3600.0  # hours
daily_df["Planned Duration"] = daily_df["planned_duration_s"] / 3600.0  # hours
daily_df["Actual Duration"] = daily_df["actual_duration_s"] / 3600.0  # hours

# --- Visualizations ---

# 1. Volume Chart
st.subheader("Training Volume: Planned vs Actual")

col_metric, col_view = st.columns([2, 1])
with col_metric:
    metric = st.radio(
        "Metric",
        ["Distance", "Duration"],
        horizontal=True,
        index=["Distance", "Duration"].index(st.session_state.compliance_metric),
        key="metric_selector",
        on_change=lambda: st.session_state.update(
            {"compliance_metric": st.session_state.metric_selector}
        ),
    )
with col_view:
    view = st.radio(
        "View",
        ["Weekly", "Daily"],
        horizontal=True,
        index=["Weekly", "Daily"].index(st.session_state.compliance_view),
        key="view_selector",
        on_change=lambda: st.session_state.update(
            {"compliance_view": st.session_state.view_selector}
        ),
    )

if metric == "Distance":
    y_planned = "Planned Distance"
    y_actual = "Actual Distance"
    y_title = f"Distance ({dist_unit})"
else:
    y_planned = "Planned Duration"
    y_actual = "Actual Duration"
    y_title = "Duration (hours)"

# Select dataframe based on view
if view == "Weekly":
    df_for_chart = weekly_df.copy()
    date_col = "week_start"
    date_label = "Week"
else:
    df_for_chart = daily_df.copy()
    date_col = "date"
    date_label = "Date"

# Melt for grouped bar chart
chart_df = df_for_chart.melt(
    date_col, value_vars=[y_planned, y_actual], var_name="Type", value_name="Value"
)

# Convert date/week to string to ensure proper grouping
chart_df["date_str"] = chart_df[date_col].astype(str)

# Create grouped bar chart using column encoding for proper side-by-side display
chart = (
    alt.Chart(chart_df)
    .mark_bar()
    .encode(
        x=alt.X("date_str:N", title=date_label, axis=alt.Axis(labelAngle=45)),
        y=alt.Y("Value:Q", title=y_title),
        xOffset="Type:N",
        color=alt.Color(
            "Type:N",
            scale=alt.Scale(domain=[y_planned, y_actual], range=["#1f77b4", "#ff7f0e"]),
            legend=alt.Legend(title="Type"),
        ),
        tooltip=["date_str:N", "Type:N", alt.Tooltip("Value:Q", format=".1f")],
    )
    .properties(width=1200, height=400)
    .interactive()
)

st.altair_chart(chart, width="stretch")

# 2. Compliance Score
st.subheader("Overall Compliance")

# Calculate totals for Duration (Hours)
total_planned_dur = weekly_df["Planned Duration"].sum()
total_actual_dur = weekly_df["Actual Duration"].sum()

# Calculate percentage of plan left to complete
remaining_dur = max(0, total_planned_dur - total_actual_dur)
if total_planned_dur > 0:
    plan_remaining = (remaining_dur / total_planned_dur) * 100
    compliance_pct = (total_actual_dur / total_planned_dur) * 100
else:
    plan_remaining = 0
    compliance_pct = 0

# Calculate days remaining until plan end
days_remaining = max(0, (plan_end - today).days)

col1, col2, col3 = st.columns(3)
col1.metric("Plan Completion (Duration)", f"{plan_remaining:.1f}%")
col2.metric("Days to Completion", f"{days_remaining}")
col3.metric("Plan Compliance %", f"{compliance_pct:.1f}%")

# 3. Daily Adherence (Simple Table for now)
st.subheader("Daily Adherence")

# Show date range info to help debug
min_plan = planned_df["date"].min().date() if not planned_df.empty else "N/A"
max_plan = planned_df["date"].max().date() if not planned_df.empty else "N/A"
min_act = actual_df["date"].min().date() if not actual_df.empty else "N/A"
max_act = actual_df["date"].max().date() if not actual_df.empty else "N/A"

st.caption(
    f"Plan Range: {min_plan} to {max_plan} | Activity Range: {min_act} to {max_act}"
)

if lthr:
    st.caption(
        f"💡 HR Zones calculated using LTHR = {lthr} bpm: Z1 (<50%), Z2 (50-70%), Z3 (70-85%), Z4 (85-100%), Z5 (>100%)"
    )
else:
    st.warning(
        "⚠️ No LTHR found. HR zones cannot be calculated. Run ingest to refresh athlete profile."
    )

# Prepare display dataframe with Duration in Hours and HR zones
display_df = merged_df[
    ["date", "workout_name", "planned_duration_s", "actual_duration_s"]
].copy()
display_df["planned_duration_h"] = display_df["planned_duration_s"] / 3600.0
display_df["actual_duration_h"] = display_df["actual_duration_s"] / 3600.0

# Add HR zone data (convert from seconds to minutes)
for zone in [1, 2, 3, 4, 5]:
    col_name = f"zone_{zone}_s"
    if col_name in merged_df.columns:
        display_df[f"Z{zone} (min)"] = merged_df[col_name] / 60.0
    else:
        display_df[f"Z{zone} (min)"] = 0.0

display_df = display_df.sort_values("date", ascending=False)
display_df = display_df[
    [
        "date",
        "workout_name",
        "planned_duration_h",
        "actual_duration_h",
        "Z1 (min)",
        "Z2 (min)",
        "Z3 (min)",
        "Z4 (min)",
        "Z5 (min)",
    ]
]
display_df = display_df.rename(
    columns={
        "date": "Date",
        "workout_name": "Planned Workout",
        "planned_duration_h": "Planned (h)",
        "actual_duration_h": "Actual (h)",
    }
)

# Format date to string
display_df["Date"] = display_df["Date"].dt.strftime("%Y-%m-%d")

st.dataframe(
    display_df,
    width="stretch",
    column_config={
        "Planned (h)": st.column_config.NumberColumn(format="%.2f"),
        "Actual (h)": st.column_config.NumberColumn(format="%.2f"),
        "Z1 (min)": st.column_config.NumberColumn(format="%.2f"),
        "Z2 (min)": st.column_config.NumberColumn(format="%.2f"),
        "Z3 (min)": st.column_config.NumberColumn(format="%.2f"),
        "Z4 (min)": st.column_config.NumberColumn(format="%.2f"),
        "Z5 (min)": st.column_config.NumberColumn(format="%.2f"),
    },
)
