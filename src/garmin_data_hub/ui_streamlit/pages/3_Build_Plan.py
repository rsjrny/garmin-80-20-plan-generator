from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import importlib
import pandas as pd

import streamlit as st

from garmin_data_hub.paths import ensure_app_dirs, default_db_path, schema_sql_path
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar
from garmin_data_hub.db import queries as db_queries
from garmin_data_hub.services.athlete_metrics_service import (
    calculate_metrics_from_db_sources,
    clear_override_metrics,
    ensure_athlete_metrics_table,
    get_athlete_metrics,
    set_calculated_metrics,
    set_override_metrics,
)
from garmin_data_hub.services.plan_persistence import (
    load_generated_plan,
    save_generated_plan,
)

# Your existing export function (already in your app)
import garmin_data_hub.exports.master_export as master_export
from garmin_data_hub.exports.master_export import generate_master_workbook
import garmin_data_hub.exports.forever.garmin_ingest as garmin_ingest
from garmin_data_hub.exports.forever.garmin_ingest import analyze_garmin
from garmin_data_hub.exports.forever.models import (
    AnalysisSummary,
    Inputs,
    AthleteProfile,
    EventProfile,
)
from garmin_data_hub.exports.forever.content_library import (
    forever_manifesto,
    workout_library,
    nutrition_sections,
)

# NEW: Import training rules
from garmin_data_hub.exports.forever.training_rules import (
    build_phase_structure,
    validate_week_structure,
    get_recovery_multiplier,
    get_intensity_cap,
    build_weekly_schedule,
    calculate_weekly_intensity_distribution,
)


# ---------------------------
# DB init (safe every run)
# ---------------------------
def init_db(db_path: Path) -> None:
    conn = connect_sqlite(db_path)
    apply_schema(
        conn, schema_sql_path()
    )  # schema.sql must be at src/garmin_data_hub/db/schema.sql
    conn.close()


# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="Build Plan", layout="wide")

# Force refresh of compliance data when page is visited
# This ensures latest data is always displayed
if "last_build_plan_visit" not in st.session_state:
    st.session_state.last_build_plan_visit = None

current_visit = datetime.now(timezone.utc).isoformat()
st.session_state.last_build_plan_visit = current_visit

ensure_app_dirs()
db_path = default_db_path()

# Always ensure schema exists so pages don't die on missing tables
init_db(db_path)

# Render Sidebar
conn = sqlite3.connect(str(db_path))
unit_system = render_sidebar(conn)
conn.close()

st.header("Build Plan: Master Workbook")

try:
    # Ensure athlete_metrics table exists
    ensure_athlete_metrics_table(db_path)

    # Load metrics
    m = get_athlete_metrics(db_path)

    # If calculated values missing, try to calculate (best-effort, non-fatal)
    if m["hrmax_calc"] is None or m["lthr_calc"] is None:
        hrmax_calc, lthr_calc, err = calculate_metrics_from_db_sources(db_path)
        if err:
            st.info(f"Auto-calc not available yet: {err}")
        else:
            set_calculated_metrics(db_path, hrmax_calc, lthr_calc)
            m = get_athlete_metrics(db_path)

    # Effective defaults
    default_hrmax = int(m["hrmax_effective"] or 0)
    default_lthr = int(m["lthr_effective"] or 0)

    # # Sidebar debug (handy; safe to keep)
    # with st.sidebar:
    #     st.markdown("### Build Plan Debug")
    #     st.write("DB:", db_path)
    #     p = schema_sql_path()
    #     st.write("Schema:", p)
    #     st.write("Schema exists:", p.exists())

    st.subheader("Athlete HR Settings")
    st.caption(
        f"Calculated: HRmax={m['hrmax_calc'] or 'n/a'} | LTHR={m['lthr_calc'] or 'n/a'} "
        f"(updated {m['calc_updated_at'] or 'n/a'})\n"
        f"Override: HRmax={m['hrmax_override'] or 'none'} | LTHR={m['lthr_override'] or 'none'} "
        f"(updated {m['override_updated_at'] or 'n/a'})"
    )

    hr_col1, hr_col2, hr_col3 = st.columns([1, 1, 2])
    with hr_col1:
        hrmax_ui = st.number_input(
            "HRmax (bpm)", min_value=0, max_value=250, value=default_hrmax, step=1
        )
    with hr_col2:
        lthr_ui = st.number_input(
            "LTHR (bpm)", min_value=0, max_value=220, value=default_lthr, step=1
        )
    with hr_col3:
        years_back = st.number_input("Years back", min_value=1, max_value=50, value=5)
        b_save, b_clear, b_recalc = st.columns(3)
        with b_save:
            if st.button("Save override"):
                new_hrmax = int(hrmax_ui) if int(hrmax_ui) > 0 else None
                new_lthr = int(lthr_ui) if int(lthr_ui) > 0 else None
                set_override_metrics(db_path, new_hrmax, new_lthr)
                st.success("Saved overrides to SQLite.")
                st.rerun()
        with b_clear:
            if st.button("Clear override"):
                clear_override_metrics(db_path)
                st.info("Cleared overrides (now using calculated).")
                st.rerun()
        with b_recalc:
            if st.button("Recalculate"):
                hrmax_calc, lthr_calc, err = calculate_metrics_from_db_sources(
                    db_path, years_back=years_back
                )
                if err:
                    st.warning(err)
                else:
                    set_calculated_metrics(db_path, hrmax_calc, lthr_calc)
                    st.success("Updated calculated values.")
                    st.rerun()

    # Reload metrics so Garmin setup shows current effective values
    m2 = get_athlete_metrics(db_path)
    eff_hrmax = m2["hrmax_effective"]
    eff_lthr = m2["lthr_effective"]

    # Show Garmin HR Zone Setup Instructions
    with st.expander("🎯 How to Set HR Zones in Garmin Connect"):
        st.markdown(
            f"""
        ### Setting Up Heart Rate Zones on Your Garmin Device
        
        **Your Calculated Values:**
        - **Max HR**: {eff_hrmax or 'Not calculated'} bpm
        - **Lactate Threshold**: {eff_lthr or 'Not calculated'} bpm
        
        #### On Garmin Connect (Web or Mobile App):
        
        1. **Open Garmin Connect** (web: connect.garmin.com or mobile app)
        2. Go to **Settings** → **User Settings** → **Heart Rate Zones**
        3. Select **Based on % of Max**
        4. Enter your **Max HR**: `{eff_hrmax or '___'}` bpm
        5. Set **Lactate Threshold**: `{eff_lthr or '___'}` bpm
        6. Click **Save**
        7. Sync your device
        
        #### Recommended Zone Setup (% of Max HR):
        - **Zone 1 (Recovery)**: 50-60% ({int(eff_hrmax * 0.50) if eff_hrmax else '___'}-{int(eff_hrmax * 0.60) if eff_hrmax else '___'} bpm)
        - **Zone 2 (Easy)**: 60-70% ({int(eff_hrmax * 0.60) if eff_hrmax else '___'}-{int(eff_hrmax * 0.70) if eff_hrmax else '___'} bpm)
        - **Zone 3 (Aerobic)**: 70-80% ({int(eff_hrmax * 0.70) if eff_hrmax else '___'}-{int(eff_hrmax * 0.80) if eff_hrmax else '___'} bpm)
        - **Zone 4 (Threshold)**: 80-90% ({int(eff_hrmax * 0.80) if eff_hrmax else '___'}-{int(eff_hrmax * 0.90) if eff_hrmax else '___'} bpm)
        - **Zone 5 (Max)**: 90-100% ({int(eff_hrmax * 0.90) if eff_hrmax else '___'}-{eff_hrmax or '___'} bpm)
        
        #### Why These Values?
        - **Max HR**: 99.5th percentile of your recorded heart rates (avoids spikes)
        - **LTHR**: Conservative estimate at 86% of max HR
        - These zones will make your Garmin workouts more accurate
        
        💡 **Tip**: After updating, do a test workout to verify the zones feel right!
        """
        )

    st.divider()
    st.subheader("Plan Inputs")
    submitted = st.button("Generate Plan / Save Workbook", type="primary")
    # Load persisted settings
    conn = sqlite3.connect(str(db_path))
    s_name = db_queries.get_setting(conn, "plan_athlete_name", "Runner")
    s_age = db_queries.get_setting(conn, "plan_age", 50)
    s_run_days = db_queries.get_setting(conn, "plan_run_days", 5)
    s_sodium = db_queries.get_setting(conn, "plan_sodium", 900)
    s_distance = db_queries.get_setting(conn, "plan_distance", "50K")
    s_event_name = db_queries.get_setting(
        conn, "plan_event_name", f"{s_distance} Training Plan"
    )
    s_long_run_day = db_queries.get_setting(conn, "plan_long_run_day", "Saturday")

    today = date.today()

    def parse_date(s, default):
        try:
            return date.fromisoformat(s)
        except:
            return default

    s_event_date = parse_date(
        db_queries.get_setting(conn, "plan_event_date", today.isoformat()), today
    )
    s_start_date = parse_date(
        db_queries.get_setting(conn, "plan_start_date", today.isoformat()), today
    )

    s_out_dir = db_queries.get_setting(
        conn, "plan_out_dir", str(Path.home() / "Documents")
    )
    s_out_name = db_queries.get_setting(
        conn, "plan_out_name", f"{s_name}_master_workbook.xlsx"
    )
    conn.close()

    # No form, so we can save on change
    c1, c2, c3 = st.columns(3)

    with c1:
        athlete_name = st.text_input("Runner name", value=s_name)
        if athlete_name != s_name:
            conn = sqlite3.connect(str(db_path))
            db_queries.set_setting(conn, "plan_athlete_name", athlete_name)
            conn.close()

        age_col, long_run_col, run_days_col = st.columns([1, 2, 1])
        with age_col:
            st.write("")  # Empty space to align all fields
            age = st.number_input(
                "Age", min_value=10, max_value=100, value=s_age, step=1
            )
            if age != s_age:
                conn = sqlite3.connect(str(db_path))
                db_queries.set_setting(conn, "plan_age", age)
                conn.close()
        with long_run_col:
            st.write("")  # Empty space to align all fields
            day_opts = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
            try:
                day_idx = day_opts.index(s_long_run_day)
            except:
                day_idx = 5
            long_run_day = st.selectbox("Long run day", day_opts, index=day_idx)
            if long_run_day != s_long_run_day:
                conn = sqlite3.connect(str(db_path))
                db_queries.set_setting(conn, "plan_long_run_day", long_run_day)
                conn.close()
        with run_days_col:
            st.write("")  # Empty space to align all fields
            run_days = st.number_input(
                "Days/week", min_value=3, max_value=7, value=s_run_days, step=1
            )
            if run_days != s_run_days:
                conn = sqlite3.connect(str(db_path))
                db_queries.set_setting(conn, "plan_run_days", run_days)
                conn.close()

    with c2:
        sodium = st.number_input(
            "Sodium mg/hr hot (optional)",
            min_value=0,
            max_value=3000,
            value=s_sodium,
            step=50,
        )
        if sodium != s_sodium:
            conn = sqlite3.connect(str(db_path))
            db_queries.set_setting(conn, "plan_sodium", sodium)
            conn.close()

        date_col1, date_col2 = st.columns(2)
        with date_col1:
            st.write("")  # Empty space to align all fields
            start_date = st.date_input("Plan start date", value=s_start_date)
            if start_date != s_start_date:
                conn = sqlite3.connect(str(db_path))
                db_queries.set_setting(conn, "plan_start_date", start_date.isoformat())
                conn.close()
        with date_col2:
            st.write("")  # Empty space to align all fields
            event_date = st.date_input("Race date", value=s_event_date)
            if event_date != s_event_date:
                conn = sqlite3.connect(str(db_path))
                db_queries.set_setting(conn, "plan_event_date", event_date.isoformat())
                conn.close()

    with c3:
        dist_opts = ["5K", "10K", "HM", "MAR", "50K", "50M", "100K", "100M"]
        try:
            dist_idx = dist_opts.index(s_distance)
        except:
            dist_idx = 4
        distance = st.selectbox("Race type / distance", dist_opts, index=dist_idx)
        if distance != s_distance:
            conn = sqlite3.connect(str(db_path))
            db_queries.set_setting(conn, "plan_distance", distance)
            conn.close()

        st.write("")  # Empty space to align all fields
        event_name = st.text_input("Event name", value=s_event_name)
        if event_name != s_event_name:
            conn = sqlite3.connect(str(db_path))
            db_queries.set_setting(conn, "plan_event_name", event_name)
            conn.close()

    out_dir = st.text_input("Output folder", value=s_out_dir)
    if out_dir != s_out_dir:
        conn = sqlite3.connect(str(db_path))
        db_queries.set_setting(conn, "plan_out_dir", out_dir)
        conn.close()

    out_name = st.text_input("Filename", value=s_out_name)
    if out_name != s_out_name:
        conn = sqlite3.connect(str(db_path))
        db_queries.set_setting(conn, "plan_out_name", out_name)
        conn.close()

    # ===== NEW: Plan Analysis Summary =====
    st.divider()
    st.subheader("Plan Configuration Analysis")

    col_info1, col_info2, col_info3, col_info4 = st.columns(4)

    with col_info1:
        recovery_mult = get_recovery_multiplier(int(age))
        st.metric(
            "Recovery Multiplier",
            f"{recovery_mult:.1f}x",
            delta="Older = more recovery" if int(age) >= 50 else "Standard",
        )

    with col_info2:
        intensity_cap = get_intensity_cap(int(age))
        st.metric(
            "Max Hard Days/Week",
            intensity_cap,
            delta="Age-adjusted cap" if int(age) >= 45 else "Standard",
        )

    with col_info3:
        weeks_available = (event_date - start_date).days // 7
        phase_struct = build_phase_structure(distance, int(age))
        min_weeks = sum(p.weeks for p in phase_struct.values())
        status = "⚠️ Tight" if weeks_available < min_weeks else "✓ OK"
        st.metric(
            "Weeks Available", weeks_available, delta=f"Need {min_weeks} min – {status}"
        )

    with col_info4:
        z2_target = phase_struct["Build"].z2_target
        st.metric(
            "Build Phase Z2 Target",
            f"{z2_target:.0%}",
            delta=f"Distance-based for {distance}",
        )

    # Show phase breakdown
    with st.expander("📊 Phase Breakdown (Age & Distance Aware)", expanded=False):
        phase_data = []
        for phase_name, config in phase_struct.items():
            phase_data.append(
                {
                    "Phase": phase_name,
                    "Weeks": config.weeks,
                    "Z2 Target": f"{config.z2_target:.0%}",
                    "Hard Days": config.intensity_days,
                    "Long Run Cap (km)": f"{config.long_run_cap_km:.1f}",
                    "Recovery Mult": f"{config.recovery_multiplier:.2f}x",
                }
            )
        st.dataframe(pd.DataFrame(phase_data), width="stretch")

    # Show weekly schedule preview
    with st.expander("📅 Weekly Schedule Preview", expanded=False):
        st.markdown("**Preview your training week structure:**")

        # Get the weekly schedule
        preview_schedule = build_weekly_schedule(
            run_days_per_week=int(run_days), long_run_day=long_run_day
        )

        # Create a nice table display
        days_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]

        # Display as columns for better visualization
        cols = st.columns(7)
        for idx, day in enumerate(days_order):
            with cols[idx]:
                day_type = preview_schedule[day]

                # Style based on day type
                if day_type == "rest":
                    st.markdown(f"**{day[:3]}**")
                    st.markdown("🛌 **REST**")
                elif day_type == "long_run":
                    st.markdown(f"**{day[:3]}**")
                    st.markdown("🏃‍♂️ **LONG**")
                else:  # run
                    st.markdown(f"**{day[:3]}**")
                    st.markdown("🏃 **RUN**")

        # Add summary
        st.divider()
        run_count = sum(
            1 for v in preview_schedule.values() if v in ["run", "long_run"]
        )
        rest_count = sum(1 for v in preview_schedule.values() if v == "rest")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Run Days", run_count)
        with col2:
            st.metric("Rest Days", rest_count)
        with col3:
            rest_days_list = [day for day, v in preview_schedule.items() if v == "rest"]
            st.info(f"**Rest on:** {', '.join(rest_days_list)}")

        # Add explanation
        st.markdown("---")
        st.markdown(
            """
        **Schedule Rules:**
        - **5 days/week**: Monday & Friday rest (before/after long run)
        - **4 days/week**: Monday, Wednesday, Friday rest
        - **3 days/week**: Monday, Wednesday, Thursday, Friday rest
        - **6 days/week**: Monday rest only
        - **7 days/week**: No rest days (not recommended!)
        
        *Long run day can be customized above.*
        """
        )

    st.divider()

    # submitted = st.button("Generate Plan / Save Workbook", type="primary")

    # Variables to hold plan data for display
    display_inputs = None
    display_analysis = None
    display_day_plans = None
    display_weekly_rows = None

    if submitted:
        out_path = Path(out_dir).expanduser().resolve() / out_name

        # The garmin_files list is now always empty as the UI has been removed.
        garmin_files = []

        p = generate_master_workbook(
            out_path=out_path,
            athlete_name=athlete_name.strip() or "Runner",
            age=int(age),
            lthr=int(eff_lthr) if eff_lthr is not None else None,
            hrmax=int(eff_hrmax) if eff_hrmax is not None else None,
            sodium_mg_per_hr_hot=int(sodium) if int(sodium) > 0 else None,
            event_name=event_name.strip() or f"{distance} Training Plan",
            distance=distance,
            start_date_iso=start_date.isoformat(),
            event_date_iso=event_date.isoformat(),
            run_days_per_week=int(run_days),
            long_run_day=long_run_day,
            garmin_files=garmin_files,
        )

        st.success(f"Saved: {p}")

        # Force reload to pick up new function if needed
        importlib.reload(master_export)

        # Generate data for display and persistence
        inputs, analysis, day_plans, weekly_rows = master_export.generate_plan_data(
            athlete_name=athlete_name.strip() or "Runner",
            age=int(age),
            lthr=int(eff_lthr) if eff_lthr is not None else None,
            hrmax=int(eff_hrmax) if eff_hrmax is not None else None,
            sodium_mg_per_hr_hot=int(sodium) if int(sodium) > 0 else None,
            event_name=event_name.strip() or f"{distance} Training Plan",
            distance=distance,
            start_date_iso=start_date.isoformat(),
            event_date_iso=event_date.isoformat(),
            run_days_per_week=int(run_days),
            long_run_day=long_run_day,
            garmin_files=garmin_files,
            out_dir=out_path.parent,
        )

        # Save to DB for persistence
        save_generated_plan(db_path, inputs, analysis, day_plans, weekly_rows)

        # Set for display
        display_inputs = inputs
        display_analysis = analysis
        display_day_plans = day_plans
        display_weekly_rows = weekly_rows

        # After generating plan, add this check:
        # Get a sample week (e.g., week 1 workouts)
        sample_week_workouts = [p.workout for p in day_plans if p.week == 1]

        intensity_dist = calculate_weekly_intensity_distribution(
            day_workouts=sample_week_workouts,
            phase="Build",
            distance=distance,
            age=int(age),
        )

        with st.expander("📊 **Intensity Distribution (80/20 Check)**"):
            col1, col2 = st.columns(2)

            with col1:
                st.metric(
                    "Easy/Aerobic (Z1-Z2)",
                    f"{intensity_dist.z2_percent:.0f}%",
                    delta=f"Target: 80%" if intensity_dist.z2_percent < 80 else "✓",
                )

            with col2:
                hard_pct = 100 - intensity_dist.z2_percent
                st.metric(
                    "Hard (Z3-Z6)",
                    f"{hard_pct:.0f}%",
                    delta=f"Target: 20%" if hard_pct > 20 else "✓",
                )

            if intensity_dist.warnings:
                for warning in intensity_dist.warnings:
                    st.warning(warning)
            else:
                st.success("✓ Compliant with 80/20 training principle!")

    else:
        # Try to load last generated plan
        l_inputs, l_analysis, l_day_plans, l_weekly_rows = load_generated_plan(db_path)
        if l_inputs and l_analysis and l_day_plans:
            # Convert dicts back to objects/lists where needed for display logic
            # (Actually, our display logic below works mostly with dicts or simple access)
            display_inputs = l_inputs  # dict
            display_analysis = l_analysis  # dict
            display_day_plans = l_day_plans  # list of dicts
            display_weekly_rows = l_weekly_rows  # list of dicts

    # Display Plan Preview if data is available
    if display_day_plans:
        st.divider()

        # Add refresh button in top right to update compliance data
        col_refresh, col_spacer = st.columns([0.5, 9.5])
        with col_refresh:
            if st.button(
                "🔄", help="Refresh compliance data", key="refresh_compliance"
            ):
                st.rerun()

        # ===== NEW: Show 80/20 metrics in header row =====
        # Get a sample week (e.g., week 1 workouts)
        # Handle both DayPlan objects (fresh) and dicts (loaded from DB)
        sample_week_workouts = [
            (p.workout if hasattr(p, "workout") else p.get("workout"))
            for p in display_day_plans
            if (p.week if hasattr(p, "week") else p.get("week")) == 1
        ]

        if sample_week_workouts:
            # Force recalculation of intensity distribution each page load
            intensity_dist = calculate_weekly_intensity_distribution(
                day_workouts=sample_week_workouts,
                phase="Build",
                distance=distance,
                age=int(age),
            )
        else:
            intensity_dist = None

        # Header row with title + 80/20 metrics
        header_col1, header_col2, header_col3, header_col4 = st.columns(
            [2, 1.5, 1.5, 1.5]
        )

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
                hard_color = (
                    "🟢" if hard_pct <= 25 else "🟡" if hard_pct <= 35 else "🔴"
                )
                st.metric(
                    "Hard (Z3-Z6)",
                    f"{hard_pct:.0f}%",
                    delta=hard_color,
                    delta_color="off",
                )

            with header_col4:
                compliance = "✓ 80/20" if intensity_dist.is_compliant else "⚠️ Check"
                st.metric("Compliance", compliance, delta_color="off")

        # Show warnings if any
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

        # Helper to access dict or object attributes safely
        def get_val(obj, key):
            if isinstance(obj, dict):
                return obj.get(key)
            return getattr(obj, key, None)

        with tab_cal:
            # Filter for current date onwards
            today_iso = date.today().isoformat()
            # Handle both object list (fresh) and dict list (loaded)
            future_plans = [
                dp for dp in display_day_plans if get_val(dp, "iso_date") >= today_iso
            ]

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
            if display_weekly_rows:
                st.dataframe(pd.DataFrame(display_weekly_rows), width="stretch")
            else:
                st.write("No metrics available.")

        with tab_analysis:
            notes = get_val(display_analysis, "notes")
            st.markdown(f"**Notes:** {notes}")

            # Extract values safely
            def get_an(k):
                return str(get_val(display_analysis, k) or "")

            def get_inp_ath(k):
                # inputs might be object or dict
                if isinstance(display_inputs, dict):
                    return str(display_inputs.get("athlete", {}).get(k) or "")
                return str(getattr(display_inputs.athlete, k, "") or "")

            z2_val = get_val(display_analysis, "z2_fraction")
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

        # ===== NEW: Plan Validation Tab =====
        with tab_validation:
            st.markdown("### Plan Structure Validation")
            st.markdown("Checks hard/easy separation and recovery principles.")

            # Extract day intensities for validation
            day_intensity_map = {}
            for dp in display_day_plans:
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

            # Group by week and validate
            week_issues = []
            current_week = None
            week_days = []
            week_start_date = None

            for iso_date in sorted(day_intensity_map.keys()):
                year_week = iso_date[:7]  # YYYY-MM format grouping
                if current_week != year_week:
                    if week_days:
                        val = validate_week_structure(week_days)
                        if not val.is_valid:
                            week_issues.append(
                                (current_week, week_start_date, val.issues)
                            )
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

            # Show summary stats
            st.markdown("---")
            st.markdown("### Weekly Distribution Summary")

            hard_count = sum(
                1
                for v in day_intensity_map.values()
                if v in ["Hard", "Threshold", "VO2max", "Anaerobic"]
            )
            easy_count = sum(
                1 for v in day_intensity_map.values() if v in ["Easy", "Recovery"]
            )

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Workouts", len(day_intensity_map))
            with col2:
                st.metric("Hard Days", hard_count)
            with col3:
                st.metric("Easy/Recovery Days", easy_count)

        with tab_forever:
            st.markdown("### Forever Training System – Core")

            # Extract names safely
            if isinstance(display_inputs, dict):
                ath_name = display_inputs["athlete"]["athlete_name"]
                ath_age = display_inputs["athlete"]["age"]
                evt_name = display_inputs["event"]["event_name"]
                evt_date = display_inputs["event"]["event_date"]
            else:
                ath_name = display_inputs.athlete.athlete_name
                ath_age = display_inputs.athlete.age
                evt_name = display_inputs.event.event_name
                evt_date = display_inputs.event.event_date

            st.write(
                f"Athlete: {ath_name} | Age: {ath_age} | Event: {evt_name} ({evt_date})"
            )
            st.markdown("#### Manifesto")
            for line in forever_manifesto():
                st.markdown(f"• {line}")

        with tab_workouts:
            st.markdown("### Workout Library (Intervals.icu → Garmin)")
            st.write("Minimal set of reusable workouts with targets.")

            # Need LTHR for dynamic descriptions
            if isinstance(display_inputs, dict):
                lthr_val = display_inputs["athlete"]["lthr"]
            else:
                lthr_val = display_inputs.athlete.lthr

            for name, bullets in workout_library(lthr_val):
                st.markdown(f"**{name}**")
                for b in bullets:
                    st.markdown(f"• {b}")

        with tab_nutrition:
            st.markdown("### Nutrition & Hydration")

            if isinstance(display_inputs, dict):
                sod_val = display_inputs["athlete"]["sodium"]
                dist_val = display_inputs["event"]["distance"]
            else:
                sod_val = display_inputs.athlete.sodium_mg_per_hr_hot
                dist_val = display_inputs.event.distance

            for title, text in nutrition_sections(sod_val, dist_val):
                st.markdown(f"**{title}**")
                st.write(text)

except Exception as e:
    st.error("Export page crashed:")
    st.exception(e)
    st.stop()
