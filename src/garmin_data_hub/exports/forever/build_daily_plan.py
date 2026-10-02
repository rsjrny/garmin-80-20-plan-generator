from __future__ import annotations

import importlib
import logging
import sqlite3
from garmin_data_hub.db.sqlite import connect_sqlite
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
import json
import re

from garmin_data_hub.db import queries as db_queries
import garmin_data_hub.exports.master_export as master_export


logger = logging.getLogger(__name__)


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
    """Compatibility entry using the atomic, season-aware plan writer."""
    from garmin_data_hub.services.plan_persistence import save_generated_plan as save

    return save(db_path, inputs, analysis, day_plans, weekly_rows)


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
    training_method: object = "eighty_twenty",
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
        training_method=training_method,
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
        training_method=training_method,
        garmin_files=garmin_files,
        out_dir=out_path.parent if out_path else None,
    )

    # Persist plan if DB path provided
    if db_path:
        save_generated_plan(db_path, inputs, analysis, day_plans, weekly_rows)

    return output_path, inputs, analysis, day_plans, weekly_rows
