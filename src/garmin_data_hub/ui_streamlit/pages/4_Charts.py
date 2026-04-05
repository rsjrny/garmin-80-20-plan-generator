from __future__ import annotations
import os
import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from datetime import datetime, timedelta, timezone

from garmin_data_hub.paths import default_db_path, ensure_app_dirs
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar
from garmin_data_hub.db import queries

st.set_page_config(page_title="Charts", layout="wide")

# Initialize session state for selections
if "charts_time_range" not in st.session_state:
    st.session_state.charts_time_range = "Last 30 Days"
if "charts_vol_metric" not in st.session_state:
    st.session_state.charts_vol_metric = "Duration (h)"
if "charts_speed_mode" not in st.session_state:
    st.session_state.charts_speed_mode = "Speed (mph)"


# --- Cached Query Functions ---
@st.cache_data(ttl=3600, show_spinner=False)
def get_sports_list(db_path: str, db_mtime: float, start_ts_iso: str) -> list[str]:
    """Fetch distinct sports for the given time range, invalidating when the DB changes."""
    conn = connect_sqlite(Path(db_path))
    try:
        return queries.get_sports_list(conn, start_ts_iso)
    finally:
        conn.close()


@st.cache_data(ttl=3600, show_spinner=False)
def get_activities_dataframe(
    db_path: str,
    db_mtime: float,
    start_ts_iso: str,
    sports_list: tuple,
) -> pd.DataFrame:
    """Fetch activities for the given filters, invalidating when the DB changes."""
    conn = connect_sqlite(Path(db_path))
    try:
        return queries.get_activities_dataframe(conn, start_ts_iso, sports_list)
    finally:
        conn.close()


@st.cache_data(ttl=3600, show_spinner=False)
def get_plan_date_range(db_path: str, db_mtime: float) -> tuple[str | None, str | None]:
    """Fetch the cached current planned-workout date range."""
    conn = connect_sqlite(Path(db_path))
    try:
        return queries.get_planned_workout_date_range(conn)
    finally:
        conn.close()


ensure_app_dirs()
db_path = default_db_path()
db_mtime = os.path.getmtime(db_path) if db_path.exists() else 0.0

# Connect to DB (create fresh connection for this session)
conn = connect_sqlite(db_path)
schema_path = Path(__file__).resolve().parents[2] / "db" / "schema.sql"
apply_schema(conn, schema_path)

# Render Sidebar and get global units
unit_system = render_sidebar(conn)

st.header("Training Trends")

# --- Controls ---
col1, col2 = st.columns(2)
with col1:
    time_range = st.selectbox(
        "Time Range",
        options=[
            "Last 30 Days",
            "Last 90 Days",
            "Last Year",
            "Current Plan",
            "All Time",
        ],
        index=[
            "Last 30 Days",
            "Last 90 Days",
            "Last Year",
            "Current Plan",
            "All Time",
        ].index(st.session_state.charts_time_range),
        key="time_range_selector",
    )
    # Update session state after widget is created
    st.session_state.charts_time_range = time_range

# Determine date filter FIRST so we can filter sports
now = datetime.now(timezone.utc)
plan_end_date = None  # Used for filtering if Current Plan is selected

if time_range == "Last 30 Days":
    start_date = now - timedelta(days=30)
elif time_range == "Last 90 Days":
    start_date = now - timedelta(days=90)
elif time_range == "Last Year":
    start_date = now - timedelta(days=365)
elif time_range == "Current Plan":
    # Fetch plan start/end date via cached helper
    min_date, max_date = get_plan_date_range(str(db_path), db_mtime)
    if min_date:
        try:
            start_date = datetime.fromisoformat(min_date).replace(tzinfo=timezone.utc)
            if max_date:
                plan_end_date = datetime.fromisoformat(max_date).replace(
                    tzinfo=timezone.utc
                )
        except ValueError:
            start_date = now - timedelta(days=90)
            st.warning("Could not determine plan start date. Defaulting to 90 days.")
    else:
        st.info("No active plan found. Defaulting to 90 days.")
        start_date = now - timedelta(days=90)
else:
    # Use a valid pandas timestamp for "All Time"
    start_date = pd.Timestamp("1900-01-01").to_pydatetime().replace(tzinfo=timezone.utc)

start_ts_iso = start_date.isoformat()

with col2:
    # Fetch available sports for filtering, RESTRICTED by time range (cached)
    all_sports = get_sports_list(str(db_path), db_mtime, start_ts_iso)

    # If no sports found (e.g. empty range), handle gracefully
    if not all_sports:
        selected_sports = []
        st.warning("No activities found in this time range.")
    else:
        selected_sports = st.multiselect(
            "Filter by Sport", options=all_sports, default=all_sports
        )

# --- Data Fetching ---
# We need activity summary data joined with activity info
# Fetch using cached function (with sports as tuple for cache key)
df = get_activities_dataframe(
    str(db_path),
    db_mtime,
    start_ts_iso,
    tuple(selected_sports) if selected_sports else (),
)

if df.empty:
    if not selected_sports and all_sports:
        st.info("Please select at least one sport.")
    elif not all_sports:
        pass  # Already warned above
    else:
        st.info("No data found for the selected criteria.")
    st.stop()

# --- Data Processing ---
# Convert timestamp to datetime
# SQLite stores ISO strings now
df["date"] = pd.to_datetime(df["start_time_utc"])

# If Current Plan, filter end date if it exists (though usually we only have past activities)
if time_range == "Current Plan" and plan_end_date:
    # We only want to show activities that happened during the plan
    # But usually we want to see "how am I doing so far".
    # Let's just keep it simple: start date forward.
    pass

# Base Metric Calculations
df["distance_km"] = df["total_distance_m"] / 1000.0
df["duration_min"] = df["total_elapsed_s"] / 60.0
df["moving_duration_min"] = df["moving_time_s"] / 60.0
df["speed_kmh"] = df["avg_speed_mps"] * 3.6
df["pace_min_km"] = df["speed_kmh"].apply(lambda x: 60.0 / x if x > 0 else None)

# Imperial Calculations
df["distance_mi"] = df["total_distance_m"] * 0.000621371
df["speed_mph"] = df["avg_speed_mps"] * 2.23694
df["pace_min_mi"] = df["speed_mph"].apply(lambda x: 60.0 / x if x > 0 else None)
df["elev_gain_ft"] = df["total_ascent_m"] * 3.28084
df["elev_gain_m"] = df["total_ascent_m"]

# --- Chart Functions ---


def render_activity_distribution(df):
    st.subheader("Activity Distribution")
    sport_counts = df["sport"].value_counts().reset_index()
    sport_counts.columns = ["sport", "count"]
    chart = (
        alt.Chart(sport_counts)
        .mark_arc(outerRadius=100)
        .encode(
            theta=alt.Theta("count", stack=True),
            color=alt.Color("sport"),
            tooltip=["sport", "count", alt.Tooltip("count", format=".0f")],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def render_distance_over_time(df):
    st.subheader("Distance Over Time")
    if unit_system == "Imperial":
        y_val = "distance_mi"
        y_title = "Distance (mi)"
    else:
        y_val = "distance_km"
        y_title = "Distance (km)"

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y(f"{y_val}:Q", title=y_title),
            color=alt.Color("sport:N", title="Sport"),
            tooltip=["date", "sport", alt.Tooltip(y_val, format=".2f")],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def render_duration_over_time(df):
    st.subheader("Duration Over Time")
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("duration_min:Q", title="Duration (min)"),
            color=alt.Color("sport:N", title="Sport"),
            tooltip=["date", "sport", alt.Tooltip("duration_min", format=".1f")],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def render_weekly_volume(df):
    st.subheader("Weekly Volume")

    # Pre-calculate weekly sums for all metrics
    df_weekly = (
        df.set_index("date")
        .resample("W-MON")
        .agg({"distance_km": "sum", "distance_mi": "sum", "duration_min": "sum"})
        .reset_index()
    )

    if unit_system == "Imperial":
        dist_label = "Distance (mi)"
    else:
        dist_label = "Distance (km)"

    vol_metric = st.radio(
        "Metric",
        [dist_label, "Duration (h)"],
        horizontal=True,
        index=[dist_label, "Duration (h)"].index(
            st.session_state.charts_vol_metric
            if st.session_state.charts_vol_metric in [dist_label, "Duration (h)"]
            else "Duration (h)"
        ),
        key="vol_metric_selector",
        on_change=lambda: st.session_state.update(
            {"charts_vol_metric": st.session_state.vol_metric_selector}
        ),
    )

    if vol_metric == dist_label:
        if unit_system == "Imperial":
            y_val = "distance_mi"
            y_title = "Total Distance (mi)"
        else:
            y_val = "distance_km"
            y_title = "Total Distance (km)"
        tooltip_fmt = ".1f"
    else:
        df_weekly["duration_hours"] = df_weekly["duration_min"] / 60.0
        y_val = "duration_hours"
        y_title = "Total Duration (hours)"
        tooltip_fmt = ".1f"

    chart = (
        alt.Chart(df_weekly)
        .mark_bar()
        .encode(
            x=alt.X("date:T", title="Week Starting"),
            y=alt.Y(f"{y_val}:Q", title=y_title),
            tooltip=["date", alt.Tooltip(y_val, format=tooltip_fmt)],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def render_speed_pace(df):
    st.subheader("Speed & Pace")

    if unit_system == "Imperial":
        speed_label = "Speed (mph)"
        pace_label = "Pace (min/mi)"
    else:
        speed_label = "Speed (km/h)"
        pace_label = "Pace (min/km)"

    speed_mode = st.radio(
        "Metric",
        [speed_label, pace_label],
        horizontal=True,
        index=[speed_label, pace_label].index(
            st.session_state.charts_speed_mode
            if st.session_state.charts_speed_mode in [speed_label, pace_label]
            else speed_label
        ),
        key="speed_mode_selector",
        on_change=lambda: st.session_state.update(
            {"charts_speed_mode": st.session_state.speed_mode_selector}
        ),
    )

    if speed_mode == speed_label:
        if unit_system == "Imperial":
            y_val = "speed_mph"
            title = "Avg Speed (mph)"
        else:
            y_val = "speed_kmh"
            title = "Avg Speed (km/h)"
        df_plot = df[df[y_val] > 0]
    else:
        if unit_system == "Imperial":
            y_val = "pace_min_mi"
            title = "Avg Pace (min/mi)"
        else:
            y_val = "pace_min_km"
            title = "Avg Pace (min/km)"

        df_plot = df[df[y_val] > 0]
        # Filter outliers (e.g., > 30 min/mile is likely walking/stopped)
        df_plot = df_plot[df_plot[y_val] < 30]

    if not df_plot.empty:
        base = alt.Chart(df_plot).encode(x=alt.X("date:T", title="Date"))
        points = base.mark_circle(opacity=0.6).encode(
            y=alt.Y(f"{y_val}:Q", title=title, scale=alt.Scale(zero=False)),
            color=alt.Color("sport:N"),
            tooltip=["date", "sport", alt.Tooltip(y_val, format=".2f")],
        )
        line = (
            base.transform_window(rolling_mean=f"mean({y_val})", frame=[-5, 5])
            .mark_line(color="red")
            .encode(y="rolling_mean:Q")
        )
        st.altair_chart((points + line).interactive(), width="stretch")
    else:
        st.info("No speed data available.")


def render_heart_rate(df):
    st.subheader("Heart Rate")
    df_hr = df[df["avg_hr_bpm"] > 0]
    if not df_hr.empty:
        base = alt.Chart(df_hr).encode(x=alt.X("date:T", title="Date"))
        points_avg = base.mark_circle(opacity=0.6).encode(
            y=alt.Y("avg_hr_bpm:Q", title="HR (bpm)", scale=alt.Scale(zero=False)),
            color=alt.value("blue"),
            tooltip=["date", "sport", "avg_hr_bpm"],
        )
        points_max = base.mark_circle(opacity=0.6).encode(
            y=alt.Y("max_hr_bpm:Q", title="HR (bpm)"),
            color=alt.value("red"),
            tooltip=["date", "sport", "max_hr_bpm"],
        )
        line = (
            base.transform_window(rolling_mean="mean(avg_hr_bpm)", frame=[-5, 5])
            .mark_line(color="orange")
            .encode(y="rolling_mean:Q")
        )
        st.altair_chart((points_avg + points_max + line).interactive(), width="stretch")
        st.caption("Blue: Avg, Red: Max, Orange: Rolling Avg")
    else:
        st.info("No HR data.")


def render_elevation(df):
    st.subheader("Elevation Gain")

    if unit_system == "Imperial":
        y_val = "elev_gain_ft"
        y_title = "Elevation (ft)"
    else:
        y_val = "elev_gain_m"
        y_title = "Elevation (m)"

    if df[y_val].sum() > 0:
        chart = (
            alt.Chart(df)
            .mark_bar()
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y(f"{y_val}:Q", title=y_title),
                color="sport:N",
                tooltip=["date", "sport", alt.Tooltip(y_val, format=".0f")],
            )
            .interactive()
        )
        st.altair_chart(chart, width="stretch")
    else:
        st.info("No elevation data.")


def render_cadence(df):
    st.subheader("Cadence")
    df_cad = df[df["avg_cadence_spm"] > 0]
    if not df_cad.empty:
        base = alt.Chart(df_cad).encode(x=alt.X("date:T", title="Date"))
        points = base.mark_circle(opacity=0.6).encode(
            y=alt.Y(
                "avg_cadence_spm:Q", title="Cadence (spm)", scale=alt.Scale(zero=False)
            ),
            color=alt.Color("sport:N"),
            tooltip=["date", "sport", "avg_cadence_spm"],
        )
        line = (
            base.transform_window(rolling_mean="mean(avg_cadence_spm)", frame=[-5, 5])
            .mark_line(color="green")
            .encode(y="rolling_mean:Q")
        )
        st.altair_chart((points + line).interactive(), width="stretch")
    else:
        st.info("No cadence data.")


def render_power(df):
    st.subheader("Power")
    df_pow = df[df["avg_power_w"] > 0]
    if not df_pow.empty:
        base = alt.Chart(df_pow).encode(x=alt.X("date:T", title="Date"))
        points = base.mark_circle(opacity=0.6).encode(
            y=alt.Y("avg_power_w:Q", title="Power (W)", scale=alt.Scale(zero=False)),
            color=alt.Color("sport:N"),
            tooltip=["date", "sport", "avg_power_w"],
        )
        line = (
            base.transform_window(rolling_mean="mean(avg_power_w)", frame=[-5, 5])
            .mark_line(color="purple")
            .encode(y="rolling_mean:Q")
        )
        st.altair_chart((points + line).interactive(), width="stretch")
    else:
        st.info("No power data.")


def render_cycling_power_analysis(df):
    st.subheader("Cycling Power Analysis")
    # Filter for cycling with power
    df_cycle = df[(df["sport"] == "cycling") & (df["normalized_power_w"] > 0)]

    if not df_cycle.empty:
        base = alt.Chart(df_cycle).encode(x=alt.X("date:T", title="Date"))

        # NP vs Avg Power
        points_np = base.mark_circle(color="purple").encode(
            y=alt.Y("normalized_power_w:Q", title="Power (W)"),
            tooltip=["date", "normalized_power_w", "avg_power_w"],
        )

        points_avg = base.mark_circle(color="gray", opacity=0.5).encode(
            y="avg_power_w:Q", tooltip=["date", "avg_power_w"]
        )

        st.altair_chart((points_np + points_avg).interactive(), width="stretch")
        st.caption("Purple: Normalized Power, Gray: Avg Power")

        # TSS if available
        if df_cycle["training_stress_score"].sum() > 0:
            st.markdown("**Training Stress Score (TSS)**")
            chart_tss = (
                base.mark_bar()
                .encode(
                    y=alt.Y("training_stress_score:Q", title="TSS"),
                    tooltip=["date", "training_stress_score", "intensity_factor"],
                )
                .interactive()
            )
            st.altair_chart(chart_tss, width="stretch")

    else:
        st.info("No Cycling Power data.")


def render_running_training_status(df):
    st.subheader("Running Training Status")

    # Filter for running activities only
    df_running = df[df["sport"] == "running"].copy()

    if df_running.empty:
        st.info("No running activities in selected time range.")
        return

    # Use appropriate distance unit
    if unit_system == "Imperial":
        dist_col = "distance_mi"
        dist_label = "Weekly Mileage (mi)"
    else:
        dist_col = "distance_km"
        dist_label = "Weekly Mileage (km)"

    # Aggregate by week
    df_weekly = (
        df_running.set_index("date")
        .resample("W-MON")
        .agg({dist_col: "sum", "sport": "count"})  # Count activities per week
        .reset_index()
    )
    df_weekly = df_weekly.rename(columns={"sport": "activity_count"})

    # Calculate 4-week rolling average
    df_weekly["rolling_avg"] = (
        df_weekly[dist_col].rolling(window=4, min_periods=1).mean()
    )

    # Calculate percentage change from rolling average
    df_weekly["pct_change"] = (
        (df_weekly[dist_col] - df_weekly["rolling_avg"])
        / df_weekly["rolling_avg"]
        * 100
    )

    # Determine training status
    df_weekly["status"] = "Normal"
    df_weekly.loc[df_weekly["pct_change"] > 10, "status"] = "High Risk (>10% increase)"
    df_weekly.loc[df_weekly["pct_change"] < -30, "status"] = "Low Volume"

    # Create chart with weekly mileage and rolling average
    base = alt.Chart(df_weekly).encode(x=alt.X("date:T", title="Week Starting"))

    # Define color scale based on status
    color_scale = alt.Scale(
        domain=["Normal", "High Risk (>10% increase)", "Low Volume"],
        range=["steelblue", "orange", "lightblue"],
    )

    bars = base.mark_bar().encode(
        y=alt.Y(f"{dist_col}:Q", title=dist_label),
        color=alt.Color("status:N", scale=color_scale, legend=None),
        tooltip=[
            alt.Tooltip("date:T", title="Week"),
            alt.Tooltip(f"{dist_col}:Q", title="Distance", format=".1f"),
            alt.Tooltip("activity_count:Q", title="Activities"),
            alt.Tooltip("pct_change:Q", title="% Change", format=".1f"),
            alt.Tooltip("status:N", title="Status"),
        ],
    )

    line = base.mark_line(color="red", strokeDash=[5, 5]).encode(
        y=alt.Y("rolling_avg:Q"),
        tooltip=[alt.Tooltip("rolling_avg:Q", title="4-week Avg", format=".1f")],
    )

    chart = (bars + line).interactive()
    st.altair_chart(chart, width="stretch")

    # Show current status
    if len(df_weekly) > 0:
        latest = df_weekly.iloc[-1]
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Current Week",
                f"{latest[dist_col]:.1f} {'mi' if unit_system == 'Imperial' else 'km'}",
            )

        with col2:
            st.metric(
                "4-Week Avg",
                f"{latest['rolling_avg']:.1f} {'mi' if unit_system == 'Imperial' else 'km'}",
            )

        with col3:
            change_val = latest["pct_change"]
            if pd.notna(change_val):
                st.metric(
                    "Change",
                    f"{change_val:+.1f}%",
                    delta_color="inverse" if change_val > 10 else "normal",
                )
            else:
                st.metric("Change", "N/A")

        # Training status message
        if pd.notna(change_val):
            if change_val > 10:
                st.warning(
                    "⚠️ High overtraining risk: Weekly mileage increased >10% from 4-week average"
                )
            elif change_val < -30:
                st.info("📉 Low training volume: Consider maintaining consistency")
            else:
                st.success("✅ Training load is within safe progression range")

    st.caption(
        "🔵 Normal progression | 🟠 High risk (>10% increase) | 🔵 Low volume (<30% decrease) | Red dashed line: 4-week rolling average"
    )


CHART_FUNCS = {
    "Activity Distribution": render_activity_distribution,
    "Running Training Status": render_running_training_status,
    "Distance Over Time": render_distance_over_time,
    "Duration Over Time": render_duration_over_time,
    "Weekly Volume": render_weekly_volume,
    "Speed/Pace Trends": render_speed_pace,
    "Heart Rate Trends": render_heart_rate,
    "Elevation Gain": render_elevation,
    "Cadence Trends": render_cadence,
    "Power Trends": render_power,
    "Cycling Power Analysis": render_cycling_power_analysis,
}

# --- Chart Selection ---
st.divider()
st.subheader("Charts")

# Quick reset for saved chart selection stored in DB settings
reset_cols = st.columns([1, 6])
with reset_cols[0]:
    if st.button("Reset chart layout"):
        try:
            queries.delete_setting(conn, "default_charts")
            st.success("Chart layout reset. Reloading…")
            st.rerun()
        except Exception as e:
            st.error(f"Reset failed: {e}")

available_charts = list(CHART_FUNCS.keys())
default_charts = queries.get_setting(
    conn,
    "default_charts",
    [
        "Running Training Status",
        "Distance Over Time",
        "Weekly Volume",
        "Activity Distribution",
    ],
)

# Filter default_charts to only include charts that exist in available_charts
# This handles cases where saved settings reference old/renamed charts
default_charts = [c for c in default_charts if c in available_charts]

# If no valid defaults, use a sensible fallback
if not default_charts:
    default_charts = [
        "Running Training Status",
        "Distance Over Time",
        "Weekly Volume",
        "Activity Distribution",
    ]

# Add a "Select All" checkbox
select_all = st.checkbox("Select All Charts")

if select_all:
    selected_charts = st.multiselect(
        "Select Charts to Display", options=available_charts, default=available_charts
    )
else:
    selected_charts = st.multiselect(
        "Select Charts to Display", options=available_charts, default=default_charts
    )

# Reorder selected_charts to match the default_charts order (intended display order)
# This ensures charts appear in the specified order, not alphabetical
selected_charts_ordered = [c for c in default_charts if c in selected_charts] + [
    c for c in selected_charts if c not in default_charts
]
selected_charts = selected_charts_ordered

# Save the current selection for next time
queries.set_setting(conn, "default_charts", selected_charts)

# --- Charts Rendering (Grid Layout) ---

# Iterate in steps of 2 to create rows
for i in range(0, len(selected_charts), 2):
    cols = st.columns(2)

    # First chart in the row
    chart_name_1 = selected_charts[i]
    with cols[0]:
        CHART_FUNCS[chart_name_1](df)

    # Second chart in the row (if exists)
    if i + 1 < len(selected_charts):
        chart_name_2 = selected_charts[i + 1]
        with cols[1]:
            CHART_FUNCS[chart_name_2](df)

# Close the connection at the end of the script
conn.close()
