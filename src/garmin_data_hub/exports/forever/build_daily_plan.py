from __future__ import annotations

import importlib
import sqlite3
from garmin_data_hub.db.sqlite import connect_sqlite
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import json
import re

import streamlit as st

from garmin_data_hub.db import queries as db_queries
import garmin_data_hub.exports.master_export as master_export


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_athlete_metrics_table(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(db_path)
    try:
        db_queries.ensure_athlete_profile_table(conn)
    finally:
        conn.close()


def get_athlete_metrics(db_path: Path) -> dict:
    ensure_athlete_metrics_table(db_path)
    conn = connect_sqlite(db_path)
    try:
        return db_queries.get_athlete_metrics(conn)
    finally:
        conn.close()


def set_calculated_metrics(db_path: Path, hrmax: int | None, lthr: int | None) -> None:
    ensure_athlete_metrics_table(db_path)
    conn = connect_sqlite(db_path)
    try:
        db_queries.set_calculated_metrics(conn, hrmax, lthr)
    finally:
        conn.close()


def set_override_metrics(db_path: Path, hrmax: int | None, lthr: int | None) -> None:
    ensure_athlete_metrics_table(db_path)
    conn = connect_sqlite(db_path)
    try:
        db_queries.set_override_metrics(conn, hrmax, lthr)
    finally:
        conn.close()


def clear_override_metrics(db_path: Path) -> None:
    ensure_athlete_metrics_table(db_path)
    conn = connect_sqlite(db_path)
    try:
        db_queries.clear_override_metrics(conn)
    finally:
        conn.close()


def calculate_metrics_from_db_sources(
    db_path: Path, years_back: int = 5
) -> tuple[int | None, int | None, str | None]:
    """
    Uses DB activity_summary table for fast calculation if available.
    Fallbacks to file-based analysis if DB is empty or incomplete.
    """
    cutoff_date = date.today() - timedelta(days=years_back * 365)
    cutoff_iso = cutoff_date.isoformat()

    # Try fast DB path first
    if db_path.exists():
        conn = connect_sqlite(db_path)
        try:
            try:
                hrmax_robust, lthr_suggested = db_queries.get_hrmax_robust_and_lthr(
                    conn, cutoff_iso, percentile=0.995
                )
                if hrmax_robust:
                    conn.close()
                    return hrmax_robust, lthr_suggested, None
            except sqlite3.OperationalError:
                # Table might not exist; fall back to file-based path
                pass
        finally:
            conn.close()

    return None, None, "No activities found in DB (run Import first)."


def save_generated_plan(db_path: Path, inputs, analysis, day_plans, weekly_rows):
    """Saves the generated plan data to the database as a JSON blob AND inserts into planned_workout table."""

    # 1. Save JSON blob for UI state
    day_plans_data = [
        {
            "iso_date": dp.iso_date,
            "day": dp.day,
            "week": dp.week,
            "phase": dp.phase,
            "flags": dp.flags,
            "workout": dp.workout,
            "notes": dp.notes,
        }
        for dp in day_plans
    ]

    analysis_data = {
        "hrmax_observed": analysis.hrmax_observed,
        "hrmax_robust": analysis.hrmax_robust,
        "lthr_suggested": analysis.lthr_suggested,
        "avg_weekly_hours": analysis.avg_weekly_hours,
        "avg_weekly_miles": analysis.avg_weekly_miles,
        "z2_fraction": analysis.z2_fraction,
        "notes": analysis.notes,
    }

    inputs_data = {
        "athlete": {
            "athlete_name": inputs.athlete.athlete_name,
            "age": inputs.athlete.age,
            "hrmax": inputs.athlete.hrmax,
            "lthr": inputs.athlete.lthr,
            "sodium": inputs.athlete.sodium_mg_per_hr_hot,
        },
        "event": {
            "event_name": inputs.event.event_name,
            "event_date": inputs.event.event_date,
            "distance": inputs.event.distance,
        },
    }

    plan_blob = json.dumps(
        {
            "day_plans": day_plans_data,
            "weekly_rows": weekly_rows,
            "analysis": analysis_data,
            "inputs": inputs_data,
            "generated_at": _utc_now_iso(),
        }
    )

    conn = connect_sqlite(db_path)
    db_queries.set_setting(conn, "last_generated_plan", plan_blob)

    # 2. Insert into planned_workout table
    # First, clear the entire range of the new plan to avoid duplicates
    plan_dates = [dp.iso_date for dp in day_plans]
    if plan_dates:
        min_date = min(plan_dates)
        max_date = max(plan_dates)
        db_queries.delete_planned_workouts_in_range(conn, min_date, max_date)

    # Insert new workouts
    for dp in day_plans:
        # Insert ALL workouts, even past ones
        if dp.workout:
            planned_dist = None
            planned_dur = None

            text_to_parse = (dp.workout + " " + (dp.notes or "")).lower()

            try:
                # Look for duration first
                hour_match = re.search(r"(\d+(?:\.\d+)?)\s*hour", text_to_parse)
                min_range_match = re.search(r"(\d+)-(\d+)\s*min", text_to_parse)
                min_match = re.search(r"(\d+)\s*min", text_to_parse)

                if hour_match:
                    planned_dur = float(hour_match.group(1)) * 3600  # seconds
                elif min_range_match:
                    avg_mins = (
                        int(min_range_match.group(1)) + int(min_range_match.group(2))
                    ) / 2
                    planned_dur = avg_mins * 60
                elif min_match:
                    planned_dur = int(min_match.group(1)) * 60  # seconds

                # Then look for distance
                mile_match = re.search(r"(\d+(?:\.\d+)?)\s*mi", text_to_parse)
                km_match = re.search(r"(\d+(?:\.\d+)?)\s*km", text_to_parse)

                if mile_match:
                    planned_dist = float(mile_match.group(1)) * 1609.34  # meters
                elif km_match:
                    planned_dist = float(km_match.group(1)) * 1000  # meters

            except Exception as e:
                st.warning(f"Could not parse workout string: {dp.workout} - {e}")

            db_queries.insert_planned_workout(
                conn,
                dp.iso_date,
                dp.workout,
                dp.notes,
                planned_dist,
                planned_dur,
                None,
            )

    conn.commit()
    conn.close()


def load_generated_plan(db_path: Path):
    """Loads the last generated plan from the database."""
    conn = connect_sqlite(db_path)
    blob = db_queries.get_setting(conn, "last_generated_plan", "")
    conn.close()

    if not blob:
        return None, None, None, None

    try:
        data = json.loads(blob)

        # Reconstruct objects (simplified for display purposes)
        # We don't need full class reconstruction just for display
        return (
            data.get("inputs"),
            data.get("analysis"),
            data.get("day_plans"),
            data.get("weekly_rows"),
        )
    except Exception:
        return None, None, None, None


def build_and_store_plan(
    out_path: Path,
    athlete_name: str,
    age: int,
    lthr: int | None,
    hrmax: int | None,
    sodium_mg_per_hr_hot: int | None,
    event_name: str,
    distance: str,
    start_date_iso: str,
    event_date_iso: str,
    run_days_per_week: int = 5,
    long_run_day: str = "Saturday",
    garmin_files: list[Path] | None = None,
    db_path: Path | None = None,
):
    """Wrapper that generates workbook, plan data, and persists the plan to DB."""

    # Generate Excel workbook
    output_path = master_export.generate_master_workbook(
        out_path=out_path,
        athlete_name=athlete_name,
        age=age,
        lthr=lthr,
        hrmax=hrmax,
        sodium_mg_per_hr_hot=sodium_mg_per_hr_hot,
        event_name=event_name,
        distance=distance,
        start_date_iso=start_date_iso,
        event_date_iso=event_date_iso,
        run_days_per_week=run_days_per_week,
        long_run_day=long_run_day,
        garmin_files=garmin_files,
    )

    # Force reload then generate plan data (mirrors previous behavior)
    importlib.reload(master_export)
    inputs, analysis, day_plans, weekly_rows = master_export.generate_plan_data(
        athlete_name=athlete_name,
        age=age,
        lthr=lthr,
        hrmax=hrmax,
        sodium_mg_per_hr_hot=sodium_mg_per_hr_hot,
        event_name=event_name,
        distance=distance,
        start_date_iso=start_date_iso,
        event_date_iso=event_date_iso,
        run_days_per_week=run_days_per_week,
        long_run_day=long_run_day,
        garmin_files=garmin_files,
        out_dir=out_path.parent if out_path else None,
    )

    # Persist plan if DB path provided
    if db_path:
        save_generated_plan(db_path, inputs, analysis, day_plans, weekly_rows)

    return output_path, inputs, analysis, day_plans, weekly_rows
