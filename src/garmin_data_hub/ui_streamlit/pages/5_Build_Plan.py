from __future__ import annotations

import os
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import pandas as pd

import streamlit as st

from garmin_data_hub.paths import ensure_app_dirs, default_db_path, schema_sql_path
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ui_streamlit.sidebar import render_sidebar
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
    load_plan_settings,
    save_generated_plan,
    save_plan_setting,
)
from garmin_data_hub.services.training_policy import evaluate_training_policy

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


@st.cache_data(show_spinner=False)
def get_cached_plan_settings(db_path_str: str, db_mtime: float) -> dict:
    return load_plan_settings(Path(db_path_str))


@st.cache_data(show_spinner=False)
def get_cached_generated_plan(db_path_str: str, db_mtime: float):
    return load_generated_plan(Path(db_path_str))


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
db_mtime = os.path.getmtime(db_path) if db_path.exists() else 0.0

# Always ensure schema exists so pages don't die on missing tables
init_db(db_path)

# Render Sidebar
conn = connect_sqlite(db_path)
unit_system = render_sidebar(conn)
conn.close()

st.header("Build Plan: Master Workbook")
st.info(
    "Recommended: use Codex Plan Workspace for a history-aware personalized plan. "
    "The rule-based builder below remains available as a deterministic baseline "
    "and offline fallback; both paths use local policy checks before saving."
)
st.page_link(
    "pages/8_Codex_Plan_Workspace.py",
    label="Open recommended Codex Plan Workspace",
    icon="💬",
)

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
    st.subheader("Rule-based baseline inputs")
    submitted = st.button(
        "Generate rule-based baseline + workbook",
        help="Creates a predictable fallback plan without calling an AI model.",
    )
    settings = get_cached_plan_settings(str(db_path), db_mtime)
    s_name = settings["plan_athlete_name"]
    s_age = settings["plan_age"]
    s_run_days = settings["plan_run_days"]
    s_sodium = settings["plan_sodium"]
    s_distance = settings["plan_distance"]
    s_event_name = settings["plan_event_name"]
    s_long_run_day = settings["plan_long_run_day"]

    today = date.today()

    def parse_date(s, default):
        try:
            return date.fromisoformat(str(s))
        except (TypeError, ValueError):
            return default

    s_event_date = parse_date(settings["plan_event_date"], today)
    s_start_date = parse_date(settings["plan_start_date"], today)
    s_out_dir = settings["plan_out_dir"]
    s_out_name = settings["plan_out_name"]

    # No form, so we can save on change
    c1, c2, c3 = st.columns(3)

    with c1:
        athlete_name = st.text_input("Runner name", value=s_name)
        if athlete_name != s_name:
            save_plan_setting(db_path, "plan_athlete_name", athlete_name)

        age_col, long_run_col, run_days_col = st.columns([1, 2, 1])
        with age_col:
            st.write("")  # Empty space to align all fields
            age = st.number_input(
                "Age", min_value=10, max_value=100, value=s_age, step=1
            )
            if age != s_age:
                save_plan_setting(db_path, "plan_age", age)
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
                save_plan_setting(db_path, "plan_long_run_day", long_run_day)
        with run_days_col:
            st.write("")  # Empty space to align all fields
            run_days = st.number_input(
                "Days/week", min_value=3, max_value=7, value=s_run_days, step=1
            )
            if run_days != s_run_days:
                save_plan_setting(db_path, "plan_run_days", run_days)

    with c2:
        sodium = st.number_input(
            "Sodium mg/hr hot (optional)",
            min_value=0,
            max_value=3000,
            value=s_sodium,
            step=50,
        )
        if sodium != s_sodium:
            save_plan_setting(db_path, "plan_sodium", sodium)

        date_col1, date_col2 = st.columns(2)
        with date_col1:
            st.write("")  # Empty space to align all fields
            start_date = st.date_input("Plan start date", value=s_start_date)
            if start_date != s_start_date:
                save_plan_setting(db_path, "plan_start_date", start_date.isoformat())
        with date_col2:
            st.write("")  # Empty space to align all fields
            event_date = st.date_input("Race date", value=s_event_date)
            if event_date != s_event_date:
                save_plan_setting(db_path, "plan_event_date", event_date.isoformat())

    with c3:
        dist_opts = ["5K", "10K", "10M", "HM", "20M", "MAR", "50K", "50M", "100K", "100M"]
        try:
            dist_idx = dist_opts.index(s_distance)
        except ValueError:
            dist_idx = dist_opts.index("50K")
        distance = st.selectbox(
            "Race type / distance",
            dist_opts,
            index=dist_idx,
            format_func=lambda value: {
                "10M": "10M (10 miler)",
                "20M": "20M (20 miler)",
            }.get(value, value),
        )
        if distance != s_distance:
            save_plan_setting(db_path, "plan_distance", distance)

        st.write("")  # Empty space to align all fields
        event_name = st.text_input("Event name", value=s_event_name)
        if event_name != s_event_name:
            save_plan_setting(db_path, "plan_event_name", event_name)

    out_dir = st.text_input("Output folder", value=s_out_dir)
    if out_dir != s_out_dir:
        save_plan_setting(db_path, "plan_out_dir", out_dir)

    out_name = st.text_input("Filename", value=s_out_name)
    if out_name != s_out_name:
        save_plan_setting(db_path, "plan_out_name", out_name)

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

        # Generate and validate before writing a workbook or changing SQLite.
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

        policy_report = evaluate_training_policy(
            day_plans,
            start=start_date,
            event_date=event_date,
            age=int(age),
            run_days_per_week=int(run_days),
            preferred_long_session_day=long_run_day,
            minimum_strength_sessions_per_week=1,
        )
        if policy_report.errors:
            st.error("The baseline failed local training-policy validation:")
            for issue in policy_report.errors:
                st.write(f"- {issue.message}")
            st.stop()
        if policy_report.warnings:
            st.warning("Review these baseline policy warnings:")
            for issue in policy_report.warnings:
                st.write(f"- {issue.message}")

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

        # Save to DB for persistence
        save_generated_plan(db_path, inputs, analysis, day_plans, weekly_rows)
        st.success(f"Validated baseline saved to SQLite and workbook: {p}")

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
        l_inputs, l_analysis, l_day_plans, l_weekly_rows = get_cached_generated_plan(
            str(db_path), db_mtime
        )
        if l_inputs and l_analysis and l_day_plans:
            # Convert dicts back to objects/lists where needed for display logic
            # (Actually, our display logic below works mostly with dicts or simple access)
            display_inputs = l_inputs  # dict
            display_analysis = l_analysis  # dict
            display_day_plans = l_day_plans  # list of dicts
            display_weekly_rows = l_weekly_rows  # list of dicts

    st.divider()
    st.subheader("Personalize this baseline with Codex")
    st.write(
        "Use the dedicated workspace to generate a proposal from a privacy-minimized "
        "coaching context and review it before saving."
    )
    st.page_link(
        "pages/8_Codex_Plan_Workspace.py",
        label="Open Codex Plan Workspace",
        icon="💬",
    )


except Exception as e:
    st.error("Export page crashed:")
    st.exception(e)
    st.stop()
