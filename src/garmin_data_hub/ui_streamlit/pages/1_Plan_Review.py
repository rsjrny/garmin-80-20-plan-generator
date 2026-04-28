from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from garmin_data_hub.paths import ensure_app_dirs, default_db_path, schema_sql_path
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar
from garmin_data_hub.services.plan_persistence import (
    load_generated_plan,
    load_plan_settings,
)
from garmin_data_hub.exports.forever.content_library import (
    forever_manifesto,
    nutrition_sections,
    workout_library,
)
from garmin_data_hub.exports.forever.training_rules import (
    calculate_weekly_intensity_distribution,
    validate_week_structure,
)


def init_db(db_path: Path) -> None:
    conn = connect_sqlite(db_path)
    apply_schema(conn, schema_sql_path())
    conn.close()


@st.cache_data(show_spinner=False)
def get_cached_plan_settings(
    db_path_str: str, db_mtime: float, refresh_token: str
) -> dict:
    return load_plan_settings(Path(db_path_str))


@st.cache_data(show_spinner=False)
def get_cached_generated_plan(db_path_str: str, db_mtime: float, refresh_token: str):
    return load_generated_plan(Path(db_path_str))


st.set_page_config(page_title="Plan Review", layout="wide")

ensure_app_dirs()
db_path = default_db_path()
db_mtime = os.path.getmtime(db_path) if db_path.exists() else 0.0
init_db(db_path)

if "plan_refresh_token" not in st.session_state:
    st.session_state.plan_refresh_token = ""
refresh_token = str(st.session_state.plan_refresh_token)

conn = connect_sqlite(db_path)
render_sidebar(conn)
conn.close()

st.header("Plan Review (Upcoming Activities)")

settings = get_cached_plan_settings(str(db_path), db_mtime, refresh_token)
inputs, analysis, day_plans, weekly_rows = get_cached_generated_plan(
    str(db_path), db_mtime, refresh_token
)

if not (inputs and analysis and day_plans):
    st.info("No generated plan found yet. Open 'Build Plan' and generate a plan first.")
    st.stop()


# Helper to access dict or object attributes safely


def get_val(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


# Use current settings when available; fall back to generated plan payload.
age = int(settings.get("plan_age") or 0)
distance = str(settings.get("plan_distance") or "")

if age <= 0:
    if isinstance(inputs, dict):
        age = int(inputs.get("athlete", {}).get("age") or 0)
    else:
        age = int(getattr(inputs.athlete, "age", 0) or 0)

if not distance:
    if isinstance(inputs, dict):
        distance = str(inputs.get("event", {}).get("distance") or "")
    else:
        distance = str(getattr(inputs.event, "distance", "") or "")

col_refresh, col_spacer = st.columns([0.5, 9.5])
with col_refresh:
    if st.button("🔄", help="Refresh compliance data", key="refresh_compliance_review"):
        st.cache_data.clear()
        st.session_state.plan_refresh_token = datetime.now(timezone.utc).isoformat()
        st.rerun()

# ===== Show 80/20 metrics in header row =====
# Get a sample week (e.g., week 1 workouts)
sample_week_workouts = [
    (p.workout if hasattr(p, "workout") else p.get("workout"))
    for p in day_plans
    if (p.week if hasattr(p, "week") else p.get("week")) == 1
]

if sample_week_workouts:
    intensity_dist = calculate_weekly_intensity_distribution(
        day_workouts=sample_week_workouts,
        phase="Build",
        distance=distance,
        age=int(age),
    )
else:
    intensity_dist = None

header_col1, header_col2, header_col3, header_col4 = st.columns([2, 1.5, 1.5, 1.5])

with header_col1:
    st.subheader("Plan Preview (Today Forward)")

if intensity_dist:
    with header_col2:
        z2_color = (
            "🟢"
            if intensity_dist.z2_percent >= 75
            else "🟡" if intensity_dist.z2_percent >= 65 else "🔴"
        )
        st.metric(
            "Easy/Aerobic",
            f"{intensity_dist.z2_percent:.0f}%",
            delta=z2_color,
            delta_color="off",
        )

    with header_col3:
        hard_pct = 100 - intensity_dist.z2_percent
        hard_color = "🟢" if hard_pct <= 25 else "🟡" if hard_pct <= 35 else "🔴"
        st.metric(
            "Hard (Z3-Z6)",
            f"{hard_pct:.0f}%",
            delta=hard_color,
            delta_color="off",
        )

    with header_col4:
        compliance = "✓ 80/20" if intensity_dist.is_compliant else "⚠️ Check"
        st.metric("Compliance", compliance, delta_color="off")

if intensity_dist and intensity_dist.warnings:
    st.warning("⚠️ " + " | ".join(intensity_dist.warnings))

(
    tab_cal,
    tab_metrics,
    tab_analysis,
    tab_validation,
    tab_forever,
    tab_workouts,
    tab_nutrition,
) = st.tabs(
    [
        "Calendar",
        "Metrics",
        "Garmin Analysis",
        "Plan Validation",
        "Forever Plan",
        "Workout Library",
        "Nutrition",
    ]
)

with tab_cal:
    today_iso = date.today().isoformat()
    future_plans = [dp for dp in day_plans if get_val(dp, "iso_date") >= today_iso]

    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Date": get_val(dp, "iso_date"),
                    "Day": get_val(dp, "day"),
                    "Week#": get_val(dp, "week"),
                    "Phase": get_val(dp, "phase"),
                    "Flags": get_val(dp, "flags"),
                    "Workout": get_val(dp, "workout"),
                    "Notes": get_val(dp, "notes"),
                }
                for dp in future_plans
            ]
        ),
        width="stretch",
    )

with tab_metrics:
    if weekly_rows:
        st.dataframe(pd.DataFrame(weekly_rows), width="stretch")
    else:
        st.write("No metrics available.")

with tab_analysis:
    notes = get_val(analysis, "notes")
    st.markdown(f"**Notes:** {notes}")

    def get_an(k):
        return str(get_val(analysis, k) or "")

    def get_inp_ath(k):
        if isinstance(inputs, dict):
            return str(inputs.get("athlete", {}).get(k) or "")
        return str(getattr(inputs.athlete, k, "") or "")

    z2_val = get_val(analysis, "z2_fraction")
    z2_str = f"{z2_val:.0%}" if z2_val is not None else ""

    data = {
        "Metric": [
            "Observed HRmax",
            "Robust HRmax (99.5th pct)",
            "Suggested LTHR (conservative)",
            "Avg weekly hours",
            "Avg weekly miles",
            "Z2 fraction",
        ],
        "Calculated": [
            get_an("hrmax_observed"),
            get_an("hrmax_robust"),
            get_an("lthr_suggested"),
            get_an("avg_weekly_hours"),
            get_an("avg_weekly_miles"),
            z2_str,
        ],
        "Used in Plan": [
            get_inp_ath("hrmax"),
            get_inp_ath("hrmax"),
            get_inp_ath("lthr"),
            "",
            "",
            "",
        ],
    }
    st.table(data)

with tab_validation:
    st.markdown("### Plan Structure Validation")
    st.markdown("Checks hard/easy separation and recovery principles.")

    day_intensity_map = {}
    for dp in day_plans:
        workout = get_val(dp, "workout") or ""
        if "Recovery" in workout or "Z1" in workout:
            intensity = "Recovery"
        elif "Easy" in workout or "Z2" in workout:
            intensity = "Easy"
        elif "Threshold" in workout or "LTHR" in workout:
            intensity = "Threshold"
        elif "VO2" in workout or "VO2max" in workout:
            intensity = "VO2max"
        elif "Hard" in workout:
            intensity = "Hard"
        else:
            intensity = "Easy"

        iso_date = get_val(dp, "iso_date")
        day_intensity_map[iso_date] = intensity

    week_issues = []
    current_week = None
    week_days = []
    week_start_date = None

    for iso_date in sorted(day_intensity_map.keys()):
        year_week = iso_date[:7]
        if current_week != year_week:
            if week_days:
                val = validate_week_structure(week_days)
                if not val.is_valid:
                    week_issues.append((current_week, week_start_date, val.issues))
            current_week = year_week
            week_start_date = iso_date
            week_days = []

        week_days.append(day_intensity_map[iso_date])

    if week_days:
        val = validate_week_structure(week_days)
        if not val.is_valid:
            week_issues.append((current_week, week_start_date, val.issues))

    if week_issues:
        st.warning("⚠️ Plan has hard/easy separation issues:")
        for week_id, week_date, issues in week_issues:
            st.markdown(f"**Week of {week_date}:**")
            for issue in issues:
                st.markdown(f"  {issue}")
    else:
        st.success("✓ Plan follows hard/easy training principles")

    st.markdown("---")
    st.markdown("### Weekly Distribution Summary")

    hard_count = sum(
        1
        for v in day_intensity_map.values()
        if v in ["Hard", "Threshold", "VO2max", "Anaerobic"]
    )
    easy_count = sum(1 for v in day_intensity_map.values() if v in ["Easy", "Recovery"])

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Workouts", len(day_intensity_map))
    with col2:
        st.metric("Hard Days", hard_count)
    with col3:
        st.metric("Easy/Recovery Days", easy_count)

with tab_forever:
    st.markdown("### Forever Training System – Core")

    if isinstance(inputs, dict):
        ath_name = inputs["athlete"]["athlete_name"]
        ath_age = inputs["athlete"]["age"]
        evt_name = inputs["event"]["event_name"]
        evt_date = inputs["event"]["event_date"]
    else:
        ath_name = inputs.athlete.athlete_name
        ath_age = inputs.athlete.age
        evt_name = inputs.event.event_name
        evt_date = inputs.event.event_date

    st.write(f"Athlete: {ath_name} | Age: {ath_age} | Event: {evt_name} ({evt_date})")
    st.markdown("#### Manifesto")
    for line in forever_manifesto():
        st.markdown(f"• {line}")

with tab_workouts:
    st.markdown("### Workout Library (Intervals.icu → Garmin)")
    st.write("Minimal set of reusable workouts with targets.")

    if isinstance(inputs, dict):
        lthr_val = inputs["athlete"]["lthr"]
    else:
        lthr_val = inputs.athlete.lthr

    for name, bullets in workout_library(lthr_val):
        st.markdown(f"**{name}**")
        for b in bullets:
            st.markdown(f"• {b}")

with tab_nutrition:
    st.markdown("### Nutrition & Hydration")

    if isinstance(inputs, dict):
        sod_val = inputs["athlete"]["sodium"]
        dist_val = inputs["event"]["distance"]
    else:
        sod_val = inputs.athlete.sodium_mg_per_hr_hot
        dist_val = inputs.event.distance

    for title, text in nutrition_sections(sod_val, dist_val):
        st.markdown(f"**{title}**")
        st.write(text)
