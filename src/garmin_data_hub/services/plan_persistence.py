from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from garmin_data_hub.db import queries as db_queries
from garmin_data_hub.db.sqlite import connect_sqlite

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def save_generated_plan(
    db_path: Path,
    inputs: Any,
    analysis: Any,
    day_plans: list[Any],
    weekly_rows: list[dict[str, Any]],
) -> None:
    """Persist the most recent generated plan for UI reloads and compliance views."""
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
    try:
        db_queries.set_setting(conn, "last_generated_plan", plan_blob)

        plan_dates = [dp.iso_date for dp in day_plans]
        if plan_dates:
            db_queries.delete_planned_workouts_in_range(
                conn, min(plan_dates), max(plan_dates)
            )

        for dp in day_plans:
            if not dp.workout:
                continue

            planned_dist = None
            planned_dur = None
            text_to_parse = (dp.workout + " " + (dp.notes or "")).lower()

            try:
                hour_match = re.search(r"(\d+(?:\.\d+)?)\s*hour", text_to_parse)
                min_range_match = re.search(r"(\d+)-(\d+)\s*min", text_to_parse)
                min_match = re.search(r"(\d+)\s*min", text_to_parse)

                if hour_match:
                    planned_dur = float(hour_match.group(1)) * 3600
                elif min_range_match:
                    avg_mins = (
                        int(min_range_match.group(1)) + int(min_range_match.group(2))
                    ) / 2
                    planned_dur = avg_mins * 60
                elif min_match:
                    planned_dur = int(min_match.group(1)) * 60

                mile_match = re.search(r"(\d+(?:\.\d+)?)\s*mi", text_to_parse)
                km_match = re.search(r"(\d+(?:\.\d+)?)\s*km", text_to_parse)

                if mile_match:
                    planned_dist = float(mile_match.group(1)) * 1609.34
                elif km_match:
                    planned_dist = float(km_match.group(1)) * 1000

            except (TypeError, ValueError):
                logger.warning("Could not parse workout string for %s", dp.iso_date)

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
    finally:
        conn.close()


def load_generated_plan(db_path: Path):
    """Load the most recently generated plan from the database."""
    conn = connect_sqlite(db_path)
    try:
        blob = db_queries.get_setting(conn, "last_generated_plan", "")
    finally:
        conn.close()

    if not blob:
        return None, None, None, None

    try:
        data = json.loads(blob)
        return (
            data.get("inputs"),
            data.get("analysis"),
            data.get("day_plans"),
            data.get("weekly_rows"),
        )
    except (TypeError, json.JSONDecodeError):
        logger.exception("Failed to decode persisted generated plan from %s", db_path)
        return None, None, None, None


def load_plan_settings(db_path: Path) -> dict[str, Any]:
    """Load persisted Build Plan UI settings in one round-trip."""
    conn = connect_sqlite(db_path)
    try:
        athlete_name = db_queries.get_setting(conn, "plan_athlete_name", "Runner")
        distance = db_queries.get_setting(conn, "plan_distance", "50K")
        today_iso = datetime.now(timezone.utc).date().isoformat()
        return {
            "plan_athlete_name": athlete_name,
            "plan_age": db_queries.get_setting(conn, "plan_age", 50),
            "plan_run_days": db_queries.get_setting(conn, "plan_run_days", 5),
            "plan_sodium": db_queries.get_setting(conn, "plan_sodium", 900),
            "plan_distance": distance,
            "plan_event_name": db_queries.get_setting(
                conn, "plan_event_name", f"{distance} Training Plan"
            ),
            "plan_long_run_day": db_queries.get_setting(
                conn, "plan_long_run_day", "Saturday"
            ),
            "plan_event_date": db_queries.get_setting(conn, "plan_event_date", today_iso),
            "plan_start_date": db_queries.get_setting(conn, "plan_start_date", today_iso),
            "plan_out_dir": db_queries.get_setting(
                conn, "plan_out_dir", str(Path.home() / "Documents")
            ),
            "plan_out_name": db_queries.get_setting(
                conn, "plan_out_name", f"{athlete_name}_master_workbook.xlsx"
            ),
        }
    finally:
        conn.close()


def save_plan_setting(db_path: Path, key: str, value: Any) -> None:
    """Persist a single Build Plan UI setting."""
    conn = connect_sqlite(db_path)
    try:
        db_queries.set_setting(conn, key, value)
    finally:
        conn.close()
