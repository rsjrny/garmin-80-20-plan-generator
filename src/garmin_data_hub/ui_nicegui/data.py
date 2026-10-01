"""Framework-neutral data access for the primary NiceGUI interface."""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar

import pandas as pd

from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables
from garmin_data_hub.db import queries
from garmin_data_hub.db.activity_dates import activity_calendar_day_sql
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.garmin_auth_session import (
    BrowserSessionResetRequired,
    BrowserSessionResetResult,
    garmin_browser_session_exists,
    reset_garmin_browser_session,
)
from garmin_data_hub.services.athlete_metrics_service import get_athlete_metrics
from garmin_data_hub.services.garmin_credentials import (
    GarminCredentials,
    build_sync_environment,
)
from garmin_data_hub.services.plan_persistence import (
    load_generated_plan,
    load_plan_settings,
)
from garmin_data_hub.services.sync_status import progress_from_log
from garmin_data_hub.services.training_policy import normalize_training_method
from garmin_data_hub.ui_nicegui.process_tree import (
    ProcessTree,
    attach_process_tree,
    fallback_process_tree,
    popen_process_tree_kwargs,
)


READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH|EXPLAIN)\b", re.IGNORECASE)
_LoginUpdateResult = TypeVar("_LoginUpdateResult")
FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|ATTACH|DETACH|"
    r"VACUUM|REINDEX|PRAGMA|TRIGGER)\b",
    re.IGNORECASE,
)

MILES_PER_KILOMETRE = 0.621371192237334
METRES_PER_MILE = 1609.344
INTERFACE_SETTING_DEFAULTS: dict[str, Any] = {
    "unit_system": "Imperial",
    "activity_velocity_display": "Pace",
    "activity_lookback_days": 365,
    "activity_row_limit": 1000,
    "activity_default_sport": "All",
    "chart_lookback_days": 365,
    "sync_lookback_days": 0,
    "dashboard_item_limit": 8,
}
INTERFACE_SETTING_KEYS = {
    "unit_system": "unit_system",  # shared with the legacy interface
    "activity_velocity_display": "nicegui_activity_velocity_display",
    "activity_lookback_days": "nicegui_activity_lookback_days",
    "activity_row_limit": "nicegui_activity_row_limit",
    "activity_default_sport": "nicegui_activity_default_sport",
    "chart_lookback_days": "nicegui_chart_lookback_days",
    "sync_lookback_days": "nicegui_sync_lookback_days",
    "dashboard_item_limit": "nicegui_dashboard_item_limit",
}


def ensure_database(db_path: Path) -> Path:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(db_path, timeout=30.0)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return db_path


def _finite(value: Any, digits: int = 2) -> float | int | str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, digits) if math.isfinite(value) else None
    return str(value)


def _rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    return [
        {key: _finite(value) for key, value in dict(row).items()}
        for row in cursor.fetchall()
    ]


def interface_settings(db_path: Path) -> dict[str, Any]:
    """Load validated display defaults from the local application database."""
    conn = connect_sqlite(db_path)
    try:
        values = {
            friendly: queries.get_setting(conn, key, default)
            for friendly, key in INTERFACE_SETTING_KEYS.items()
            for default in (INTERFACE_SETTING_DEFAULTS[friendly],)
        }
    finally:
        conn.close()
    try:
        return _validated_interface_settings(values)
    except (TypeError, ValueError):
        return dict(INTERFACE_SETTING_DEFAULTS)


def _validated_interface_settings(values: Mapping[str, Any]) -> dict[str, Any]:
    unit_system = str(values.get("unit_system") or "")
    if unit_system not in {"Imperial", "Metric"}:
        raise ValueError("Distance units must be Imperial or Metric")
    result = {
        "unit_system": unit_system,
        "activity_velocity_display": str(
            values.get("activity_velocity_display") or "Pace"
        ),
        "activity_lookback_days": int(values.get("activity_lookback_days", 365)),
        "activity_row_limit": int(values.get("activity_row_limit", 1000)),
        "activity_default_sport": str(values.get("activity_default_sport") or "All"),
        "chart_lookback_days": int(values.get("chart_lookback_days", 365)),
        "sync_lookback_days": int(values.get("sync_lookback_days", 0)),
        "dashboard_item_limit": int(values.get("dashboard_item_limit", 8)),
    }
    if result["activity_velocity_display"] not in {"Pace", "Speed"}:
        raise ValueError("Activity velocity display must be Pace or Speed")
    if not 7 <= result["activity_lookback_days"] <= 3650:
        raise ValueError("Activity history must be between 7 and 3,650 days")
    if not 25 <= result["activity_row_limit"] <= 5000:
        raise ValueError("Activity table size must be between 25 and 5,000")
    if not 7 <= result["chart_lookback_days"] <= 3650:
        raise ValueError("Chart history must be between 7 and 3,650 days")
    if not 0 <= result["sync_lookback_days"] <= 3650:
        raise ValueError("Sync history must be between 0 and 3,650 days")
    if not 4 <= result["dashboard_item_limit"] <= 20:
        raise ValueError("Dashboard item count must be between 4 and 20")
    return result


def save_interface_settings(db_path: Path, values: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and persist NiceGUI display preferences."""
    normalized = _validated_interface_settings(values)
    conn = connect_sqlite(db_path)
    try:
        queries.set_settings(
            conn,
            {
                key: normalized[friendly]
                for friendly, key in INTERFACE_SETTING_KEYS.items()
            },
        )
    finally:
        conn.close()
    return normalized


def distance_unit(unit_system: str) -> str:
    return "mi" if unit_system == "Imperial" else "km"


def distance_from_km(value: Any, unit_system: str, *, digits: int = 2) -> float | None:
    if value is None:
        return None
    converted = float(value)
    if unit_system == "Imperial":
        converted *= MILES_PER_KILOMETRE
    return round(converted, digits)


def speed_unit(unit_system: str) -> str:
    return "mph" if unit_system == "Imperial" else "km/h"


def pace_unit(unit_system: str) -> str:
    return "min/mi" if unit_system == "Imperial" else "min/km"


def speed_from_mps(value: Any, unit_system: str, *, digits: int = 2) -> float | None:
    if value is None:
        return None
    converted = float(value) * (2.2369362920544 if unit_system == "Imperial" else 3.6)
    if not math.isfinite(converted):
        return None
    return round(converted, digits)


def pace_minutes_from_mps(value: Any, unit_system: str, *, digits: int = 2) -> float | None:
    if value is None:
        return None
    speed_mps = float(value)
    if not math.isfinite(speed_mps) or speed_mps <= 0:
        return None
    metres = METRES_PER_MILE if unit_system == "Imperial" else 1000.0
    return round((metres / speed_mps) / 60.0, digits)


def pace_text_from_mps(value: Any, unit_system: str) -> str | None:
    pace_minutes = pace_minutes_from_mps(value, unit_system, digits=4)
    if pace_minutes is None:
        return None
    total_seconds = int(round(pace_minutes * 60))
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}:{seconds:02d} {pace_unit(unit_system)}"


def dashboard_data(db_path: Path, *, item_limit: int = 8) -> dict[str, Any]:
    item_limit = max(4, min(int(item_limit), 20))
    conn = connect_sqlite(db_path)
    try:
        stats = queries.get_activity_stats(conn)
        metrics = queries.get_activity_metrics_diagnostics(conn)
        athlete = queries.get_athlete_metrics(conn)
        recent = queries.list_recent_activities(conn, limit=item_limit)
        plan_row = conn.execute(
            """
            SELECT COUNT(*) AS sessions, MIN(scheduled_date) AS start_date,
                   MAX(scheduled_date) AS end_date,
                   SUM(COALESCE(planned_duration_s, 0)) AS duration_s
            FROM active_planned_workout
            """
        ).fetchone()
        upcoming = _rows(
            conn.execute(
                """
                SELECT scheduled_date AS date, workout_name AS workout,
                       planned_duration_s / 60.0 AS duration_min,
                       planned_distance_m / 1000.0 AS distance_km,
                       planned_tss AS tss
                FROM active_planned_workout
                WHERE scheduled_date >= ?
                ORDER BY scheduled_date, planned_workout_id
                LIMIT ?
                """,
                (date.today().isoformat(), item_limit),
            )
        )
        return {
            "stats": stats,
            "diagnostics": metrics,
            "athlete": athlete,
            "recent": recent,
            "plan": dict(plan_row) if plan_row else {},
            "upcoming": upcoming,
        }
    finally:
        conn.close()


def activity_sports(db_path: Path) -> list[str]:
    conn = connect_sqlite(db_path)
    try:
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT activity_type FROM activity
                WHERE activity_type IS NOT NULL AND TRIM(activity_type) <> ''
                ORDER BY activity_type
                """
            ).fetchall()
        except sqlite3.Error:
            return []
        return [str(row[0]) for row in rows]
    finally:
        conn.close()


def list_activities(
    db_path: Path,
    *,
    sport: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    conn = connect_sqlite(db_path)
    try:
        try:
            activity_day = activity_calendar_day_sql(conn)
            clauses: list[str] = []
            parameters: list[Any] = []
            if sport:
                clauses.append("activity_type = ?")
                parameters.append(sport)
            if start_date:
                clauses.append(f"{activity_day} >= date(?)")
                parameters.append(start_date)
            if end_date:
                clauses.append(f"{activity_day} <= date(?)")
                parameters.append(end_date)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            parameters.append(max(1, min(int(limit), 5000)))
            cursor = conn.execute(
                f"""
                SELECT activity_id AS id, {activity_day} AS date,
                       activity_type AS sport,
                       ROUND(distance_meters / 1000.0, 2) AS distance_km,
                       ROUND(elapsed_duration_seconds / 60.0, 1) AS duration_min,
                       average_hr AS avg_hr, max_hr,
                       ROUND(elevation_gain, 1) AS ascent_m,
                       ROUND(average_speed, 2) AS speed_mps,
                       ROUND(training_stress_score, 1) AS garmin_tss
                FROM activity
                {where}
                ORDER BY start_time_gmt DESC
                LIMIT ?
                """,
                parameters,
            )
        except sqlite3.Error:
            return []
        return _rows(cursor)
    finally:
        conn.close()


def activity_detail(db_path: Path, activity_id: int) -> dict[str, Any] | None:
    conn = connect_sqlite(db_path)
    try:
        try:
            activity = conn.execute(
                "SELECT * FROM activity WHERE activity_id = ?", (int(activity_id),)
            ).fetchone()
        except sqlite3.Error:
            return None
        if activity is None:
            return None
        splits = queries.get_activity_records(conn, int(activity_id))
        trackpoints = queries.get_activity_trackpoints(conn, int(activity_id))
        metrics = queries.get_activity_metrics(conn, int(activity_id))
        return {
            "activity": {
                key: _finite(value) for key, value in dict(activity).items()
            },
            "metrics": (
                {key: _finite(value) for key, value in dict(metrics).items()}
                if metrics
                else {}
            ),
            "splits": json.loads(splits.to_json(orient="records"))
            if not splits.empty
            else [],
            "trackpoints": json.loads(trackpoints.to_json(orient="records"))
            if not trackpoints.empty
            else [],
        }
    finally:
        conn.close()


def activity_export_json(db_path: Path, activity_id: int) -> bytes:
    detail = activity_detail(db_path, activity_id)
    if detail is None:
        raise ValueError(f"Activity {activity_id} was not found")
    return (json.dumps(detail, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def chart_dataframe(
    db_path: Path,
    *,
    start_date: str,
    sports: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    conn = connect_sqlite(db_path)
    try:
        metrics = queries.get_athlete_metrics(conn)
        return queries.get_activities_dataframe(
            conn,
            start_ts_iso=f"{start_date}T00:00:00",
            sports_list=sports,
            lthr=metrics.get("lthr_effective"),
            use_temp_zone_metrics=False,
        )
    finally:
        conn.close()


def plan_rows(db_path: Path) -> list[dict[str, Any]]:
    conn = connect_sqlite(db_path)
    try:
        raw = _rows(
            conn.execute(
                """
                SELECT scheduled_date AS date, workout_name AS workout,
                       description AS notes,
                       planned_duration_s / 60.0 AS duration_min,
                       planned_distance_m / 1000.0 AS distance_km,
                       planned_tss AS tss, structure_json
                FROM active_planned_workout
                ORDER BY scheduled_date, planned_workout_id
                """
            )
        )
    finally:
        conn.close()
    for row in raw:
        structure: dict[str, Any] = {}
        try:
            parsed = json.loads(str(row.pop("structure_json") or "{}"))
            if isinstance(parsed, dict) and isinstance(parsed.get("workout"), dict):
                structure = parsed["workout"]
        except (TypeError, json.JSONDecodeError):
            row.pop("structure_json", None)
        row["sport"] = structure.get("sport")
        row["phase"] = structure.get("phase")
        row["intensity"] = structure.get("intensity")
    return raw


def nutrition_rows(db_path: Path) -> list[dict[str, Any]]:
    """Return the accepted date-linked macro schedule from the plan cache."""
    _, _, day_plans, _ = load_generated_plan(db_path)
    if not isinstance(day_plans, list):
        return []
    rows: list[dict[str, Any]] = []
    for day in day_plans:
        if not isinstance(day, dict) or not isinstance(day.get("nutrition"), dict):
            continue
        nutrition = day["nutrition"]
        rows.append(
            {
                "date": day.get("iso_date"),
                "day_type": nutrition.get("day_type"),
                "carbohydrate_g_per_kg": _range_text(
                    nutrition.get("carbohydrate_g_per_kg_min"),
                    nutrition.get("carbohydrate_g_per_kg_max"),
                ),
                "protein_g_per_kg": _range_text(
                    nutrition.get("protein_g_per_kg_min"),
                    nutrition.get("protein_g_per_kg_max"),
                ),
                "fat_g_per_kg": _range_text(
                    nutrition.get("fat_g_per_kg_min"),
                    nutrition.get("fat_g_per_kg_max"),
                ),
                "during_training_carbohydrate_g_per_hour": _range_text(
                    nutrition.get("during_training_carbohydrate_g_per_hour_min"),
                    nutrition.get("during_training_carbohydrate_g_per_hour_max"),
                ),
                "notes": nutrition.get("notes") or "",
            }
        )
    return rows


def _range_text(minimum: Any, maximum: Any) -> str | None:
    if minimum is None and maximum is None:
        return None
    if minimum == maximum:
        return str(minimum)
    return f"{minimum}-{maximum}"


def compliance_data(db_path: Path) -> dict[str, Any]:
    conn = connect_sqlite(db_path)
    try:
        try:
            activity_day = activity_calendar_day_sql(conn)
            cursor = conn.execute(
                f"""
                WITH planned AS (
                    SELECT scheduled_date AS day,
                           SUM(COALESCE(planned_distance_m, 0)) AS planned_distance_m,
                           SUM(COALESCE(planned_duration_s, 0)) AS planned_duration_s
                    FROM active_planned_workout GROUP BY scheduled_date
                ), actual AS (
                    SELECT {activity_day} AS day,
                           SUM(COALESCE(distance_meters, 0)) AS actual_distance_m,
                           SUM(COALESCE(elapsed_duration_seconds, 0)) AS actual_duration_s
                    FROM activity GROUP BY {activity_day}
                ), days AS (
                    SELECT day FROM planned UNION SELECT day FROM actual
                )
                SELECT days.day AS date,
                       ROUND(COALESCE(planned.planned_distance_m, 0) / 1000.0, 2) AS planned_km,
                       ROUND(COALESCE(actual.actual_distance_m, 0) / 1000.0, 2) AS actual_km,
                       ROUND(COALESCE(planned.planned_duration_s, 0) / 3600.0, 2) AS planned_hours,
                       ROUND(COALESCE(actual.actual_duration_s, 0) / 3600.0, 2) AS actual_hours
                FROM days
                LEFT JOIN planned ON planned.day = days.day
                LEFT JOIN actual ON actual.day = days.day
                WHERE days.day <= ?
                  AND days.day BETWEEN
                      COALESCE((SELECT MIN(scheduled_date) FROM active_planned_workout), days.day)
                      AND COALESCE((SELECT MAX(scheduled_date) FROM active_planned_workout), days.day)
                ORDER BY days.day
                """,
                (date.today().isoformat(),),
            )
        except sqlite3.Error:
            rows = []
        else:
            rows = _rows(cursor)
    finally:
        conn.close()
    totals = {
        key: round(sum(float(row.get(key) or 0) for row in rows), 2)
        for key in ("planned_km", "actual_km", "planned_hours", "actual_hours")
    }
    totals["duration_compliance_pct"] = (
        round(100 * totals["actual_hours"] / totals["planned_hours"], 1)
        if totals["planned_hours"]
        else 0.0
    )
    totals["distance_compliance_pct"] = (
        round(100 * totals["actual_km"] / totals["planned_km"], 1)
        if totals["planned_km"]
        else 0.0
    )
    return {"rows": rows, "totals": totals}


PLAN_SETTING_KEYS = {
    "athlete_name": "plan_athlete_name",
    "age": "plan_age",
    "distance": "plan_distance",
    "event_name": "plan_event_name",
    "run_days_per_week": "plan_run_days",
    "long_run_day": "plan_long_run_day",
    "sodium_mg_per_hour": "plan_sodium",
    "plan_start": "plan_start_date",
    "event_date": "plan_event_date",
    "training_method": "plan_training_method",
}

PLAN_OUTPUT_SETTING_KEYS = {
    "output_directory": "plan_out_dir",
    "output_filename": "plan_out_name",
}


def planning_settings(db_path: Path) -> dict[str, Any]:
    raw = load_plan_settings(db_path)
    result = {
        friendly: raw.get(setting)
        for friendly, setting in PLAN_SETTING_KEYS.items()
    }
    result.update(
        {
            friendly: raw.get(setting)
            for friendly, setting in PLAN_OUTPUT_SETTING_KEYS.items()
        }
    )
    try:
        result["training_method"] = normalize_training_method(
            result.get("training_method")
        )
    except ValueError:
        result["training_method"] = "eighty_twenty"
    result["metrics"] = get_athlete_metrics(db_path)
    return result


def _prepared_planning_settings(
    values: Mapping[str, Any],
) -> list[tuple[str, str]]:
    start = date.fromisoformat(str(values["plan_start"]))
    event = date.fromisoformat(str(values["event_date"]))
    if event < start:
        raise ValueError("Event date must be on or after plan start")
    age = int(values["age"])
    run_days = int(values["run_days_per_week"])
    sodium = int(values.get("sodium_mg_per_hour") or 0)
    if not 10 <= age <= 100:
        raise ValueError("Age must be between 10 and 100")
    if not 1 <= run_days <= 7:
        raise ValueError("Run days must be between 1 and 7")
    if not 0 <= sodium <= 3000:
        raise ValueError("Sodium must be between 0 and 3,000 mg/hour")
    if not str(values["distance"]).strip():
        raise ValueError("Race distance cannot be blank")
    training_method = normalize_training_method(values.get("training_method"))
    output_directory: str | None = None
    if "output_directory" in values:
        output_directory = str(values["output_directory"] or "").strip()
        if not output_directory:
            raise ValueError("Workbook folder cannot be blank")
    output_filename: str | None = None
    if "output_filename" in values:
        output_filename = str(values["output_filename"] or "").strip()
        if (
            not output_filename
            or "/" in output_filename
            or "\\" in output_filename
            or Path(output_filename).suffix.casefold() != ".xlsx"
        ):
            raise ValueError(
                "Workbook filename must be a file name ending with .xlsx"
            )
    persisted_values = {
        setting: values[friendly]
        for friendly, setting in PLAN_SETTING_KEYS.items()
        if friendly != "training_method"
    }
    persisted_values["plan_training_method"] = training_method
    if output_directory is not None:
        persisted_values[PLAN_OUTPUT_SETTING_KEYS["output_directory"]] = (
            output_directory
        )
    if output_filename is not None:
        persisted_values[PLAN_OUTPUT_SETTING_KEYS["output_filename"]] = (
            output_filename
        )
    return [
        (setting, json.dumps(value))
        for setting, value in persisted_values.items()
    ]


def validate_planning_settings(values: Mapping[str, Any]) -> None:
    """Validate Plan-page settings without changing the database."""

    _prepared_planning_settings(values)


def save_planning_settings(db_path: Path, values: Mapping[str, Any]) -> None:
    """Validate and atomically persist Plan-page settings."""

    rows = _prepared_planning_settings(values)
    conn = connect_sqlite(db_path, timeout=30.0)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT OR REPLACE INTO app_settings(key, value) VALUES (?, ?)",
            rows,
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def repair_derived_metrics(db_path: Path) -> dict[str, Any]:
    conn = connect_sqlite(db_path, timeout=30.0)
    try:
        return refresh_post_sync_tables(conn)
    finally:
        conn.close()


def validate_read_only_sql(sql: str) -> None:
    text = str(sql or "").strip()
    if not text:
        raise ValueError("Enter a query")
    if not READ_ONLY_SQL.match(text):
        raise ValueError("Only SELECT, WITH, and EXPLAIN queries are allowed")
    if FORBIDDEN_SQL.search(text):
        raise ValueError("The query contains a write or schema-changing keyword")
    statements = [item for item in text.split(";") if item.strip()]
    if len(statements) != 1:
        raise ValueError("Run one read-only statement at a time")


def run_read_only_query(db_path: Path, sql: str, *, limit: int = 1000) -> list[dict[str, Any]]:
    validate_read_only_sql(sql)
    uri = f"file:{Path(db_path).resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(sql)
        rows = cursor.fetchmany(max(1, min(int(limit), 5000)))
        return [
            {key: _finite(value) for key, value in dict(row).items()}
            for row in rows
        ]
    finally:
        conn.close()


@dataclass(frozen=True)
class SyncSnapshot:
    state: str
    progress: float
    elapsed_seconds: float
    return_code: int | None
    log: str
    error: str | None


class SyncJob:
    """One cancellable Garmin sync process with persisted local logging."""

    _ACTIVE_STATES = frozenset({"running", "cancelling"})
    _STOP_TIMEOUT_SECONDS = 8.0

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.log_path = self.db_path.parent / "logs" / "garmin_sync_latest.log"
        self._lock = threading.Lock()
        self._login_operation_lock = threading.RLock()
        self._process: subprocess.Popen[str] | None = None
        self._process_tree: ProcessTree | None = None
        self._monitor_thread: threading.Thread | None = None
        self._cancel_thread: threading.Thread | None = None
        self._maintenance_state: str | None = None
        self._state = "idle"
        self._started: float | None = None
        self._finished: float | None = None
        self._return_code: int | None = None
        self._error: str | None = None

    def _command(self, days: int) -> list[str]:
        if getattr(sys, "frozen", False):
            executable_candidates = (
                Path(sys.executable).parent / "cli_backup_ingest.exe",
                Path(sys.executable).parent.parent
                / "cli_backup_ingest"
                / "cli_backup_ingest.exe",
            )
            executable = next(
                (candidate for candidate in executable_candidates if candidate.is_file()),
                executable_candidates[0],
            )
            if not executable.is_file():
                raise FileNotFoundError(
                    "Sync helper not found. Checked: "
                    + ", ".join(str(item) for item in executable_candidates)
                )
            command = [str(executable)]
        else:
            command = [sys.executable, "-u", "-m", "garmin_data_hub.cli_backup_ingest"]
        command.extend(["--db", str(self.db_path)])
        if days > 0:
            command.extend(["--days", str(days)])
        command.append("--visible")
        return command

    def start(
        self,
        *,
        days: int = 0,
        credentials: GarminCredentials | None = None,
    ) -> None:
        days = int(days)
        if not 0 <= days <= 3650:
            raise ValueError("Sync days must be between 0 and 3,650")
        with self._lock:
            if self._maintenance_state is not None:
                raise RuntimeError("Garmin browser login is being reset")
        with self._login_operation_lock, self._lock:
            self._refresh_process_locked()
            if self._maintenance_state is not None:
                raise RuntimeError("Garmin browser login is being reset")
            process = self._process
            process_is_live = process is not None and process.poll() is None
            monitor_is_finishing = (
                self._monitor_thread is not None
                and self._monitor_thread.is_alive()
            )
            if (
                self._state in self._ACTIVE_STATES
                or process_is_live
                or monitor_is_finishing
            ):
                raise RuntimeError("Garmin sync is already running")
            command = self._command(days)
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self.log_path.write_text(
                "Started: " + " ".join(command) + "\n", encoding="utf-8"
            )
            environment = (
                build_sync_environment(credentials)
                if credentials is not None
                else os.environ.copy()
            )
            environment["PYTHONUNBUFFERED"] = "1"
            self._started = time.monotonic()
            self._finished = None
            self._return_code = None
            self._error = None
            try:
                with self.log_path.open("a", encoding="utf-8") as log_file:
                    process = subprocess.Popen(
                        command,
                        stdin=subprocess.DEVNULL,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        env=environment,
                        text=True,
                        shell=False,
                        **popen_process_tree_kwargs(),
                    )
            except OSError as exc:
                self._state = "failed"
                self._finished = time.monotonic()
                self._error = str(exc)
                raise
            try:
                process_tree = attach_process_tree(process)
            except Exception as attach_error:
                process_tree = fallback_process_tree(process)
                self._process = process
                self._process_tree = process_tree
                attachment_message = (
                    "Could not start Garmin sync with safe descendant cleanup: "
                    f"{attach_error}"
                )
                try:
                    code = process_tree.terminate(
                        process,
                        timeout=self._STOP_TIMEOUT_SECONDS,
                    )
                    process_tree.close_after_exit()
                except Exception as stop_error:
                    self._state = "running"
                    self._error = f"{attachment_message}. Emergency stop failed: {stop_error}"
                    self._start_monitor_locked(process, process_tree)
                else:
                    self._state = "failed"
                    self._finished = time.monotonic()
                    self._return_code = int(code)
                    self._error = attachment_message
                raise RuntimeError(self._error) from attach_error
            self._process = process
            self._process_tree = process_tree
            self._state = "running"
            if process_tree.warning:
                try:
                    with self.log_path.open("a", encoding="utf-8") as log_file:
                        log_file.write(f"Warning: {process_tree.warning}\n")
                except OSError:
                    pass
            self._start_monitor_locked(process, process_tree)

    def apply_browser_login_update(
        self,
        operation: Callable[[], _LoginUpdateResult],
        *,
        reset_confirmed: bool,
    ) -> tuple[BrowserSessionResetResult | None, _LoginUpdateResult]:
        """Apply a login change without allowing another sync to race it.

        The caller obtains user confirmation before setting ``reset_confirmed``.
        If no reset was confirmed, a newly appeared browser session aborts the
        operation so the UI can request confirmation instead of removing it.
        """

        with self._login_operation_lock:
            reset_result = None
            if reset_confirmed:
                reset_result = self.reset_browser_session()
            elif garmin_browser_session_exists(self.db_path.parent):
                raise BrowserSessionResetRequired(
                    "The Garmin browser session changed. Confirm its reset "
                    "before updating the login."
                )
            return reset_result, operation()

    def reset_browser_session(self) -> BrowserSessionResetResult:
        """Exclusively reset persisted Garmin browser authentication state."""

        with self._login_operation_lock:
            return self._reset_browser_session_exclusive()

    def _reset_browser_session_exclusive(self) -> BrowserSessionResetResult:
        with self._lock:
            self._refresh_process_locked()
            process = self._process
            process_is_live = process is not None and process.poll() is None
            monitor_is_finishing = (
                self._monitor_thread is not None
                and self._monitor_thread.is_alive()
            )
            if self._maintenance_state is not None:
                raise RuntimeError("Garmin browser login is already being reset")
            if (
                self._state in self._ACTIVE_STATES
                or process_is_live
                or monitor_is_finishing
            ):
                raise RuntimeError(
                    "Stop Garmin sync before resetting its browser login"
                )
            self._maintenance_state = "resetting_login"
        try:
            return reset_garmin_browser_session(self.db_path.parent)
        finally:
            with self._lock:
                self._maintenance_state = None

    def _start_monitor_locked(
        self,
        process: subprocess.Popen[str],
        process_tree: ProcessTree,
    ) -> None:
        monitor = threading.Thread(
            target=self._monitor_process,
            args=(process, process_tree),
            name=f"garmin-sync-monitor-{process.pid}",
            daemon=True,
        )
        self._monitor_thread = monitor
        try:
            monitor.start()
        except RuntimeError as thread_error:
            self._monitor_thread = None
            try:
                code = process_tree.terminate(
                    process,
                    timeout=self._STOP_TIMEOUT_SECONDS,
                )
                process_tree.close_after_exit()
            except Exception as stop_error:
                self._state = "running"
                self._error = (
                    f"Could not start the sync monitor ({thread_error}); "
                    f"emergency stop failed: {stop_error}"
                )
            else:
                self._return_code = int(code)
                self._finished = self._finished or time.monotonic()
                self._state = "failed"
                self._error = f"Could not start the sync monitor: {thread_error}"
            raise RuntimeError(self._error) from thread_error

    def cancel(self, *, wait: bool = False) -> bool:
        existing_cancel: threading.Thread | None = None
        process_tree: ProcessTree | None = None
        already_cancelling = False
        with self._lock:
            self._refresh_process_locked()
            process = self._process
            if process is None or process.poll() is not None:
                return False
            if self._state == "cancelling":
                already_cancelling = True
                existing_cancel = self._cancel_thread
            else:
                process_tree = self._process_tree
                if process_tree is None:
                    raise RuntimeError("Sync process tree controller is unavailable")
                self._state = "cancelling"
                self._error = None
                if not wait:
                    cancel_thread = threading.Thread(
                        target=self._finish_cancel,
                        args=(process, process_tree),
                        name=f"garmin-sync-cancel-{process.pid}",
                        daemon=True,
                    )
                    self._cancel_thread = cancel_thread
                    try:
                        cancel_thread.start()
                    except RuntimeError:
                        self._cancel_thread = None
                        wait = True

        if already_cancelling:
            if wait:
                if existing_cancel is not None:
                    existing_cancel.join(self._STOP_TIMEOUT_SECONDS + 2.0)
            return False
        if process_tree is None:
            return False
        if wait:
            self._finish_cancel(process, process_tree)
        return True

    def _finish_cancel(
        self,
        process: subprocess.Popen[str],
        process_tree: ProcessTree,
    ) -> None:
        try:
            code = process_tree.terminate(
                process,
                timeout=self._STOP_TIMEOUT_SECONDS,
            )
            process_tree.close_after_exit()
        except Exception as exc:
            code = process.poll()
            with self._lock:
                if self._process is process:
                    if code is None:
                        self._state = "running"
                        self._error = f"Could not stop Garmin sync: {exc}"
                    else:
                        if self._state == "cancelling":
                            self._return_code = int(code)
                            self._finished = self._finished or time.monotonic()
                            self._state = "failed"
                            self._error = (
                                "Sync stopped, but descendant cleanup could not be "
                                f"confirmed: {exc}"
                            )
                    if self._cancel_thread is threading.current_thread():
                        self._cancel_thread = None
            return
        with self._lock:
            if self._process is process:
                self._return_code = int(code)
                self._finished = self._finished or time.monotonic()
                if self._state == "cancelling":
                    self._state = "cancelled"
                    self._error = None
                elif self._state == "cancelled":
                    self._error = None
                if self._cancel_thread is threading.current_thread():
                    self._cancel_thread = None

    def _monitor_process(
        self,
        process: subprocess.Popen[str],
        process_tree: ProcessTree,
    ) -> None:
        while True:
            try:
                code = int(process.wait())
                break
            except Exception as exc:
                polled_code = process.poll()
                if polled_code is not None:
                    code = int(polled_code)
                    break
                with self._lock:
                    if self._process is process:
                        self._error = f"Could not monitor Garmin sync: {exc}"
                time.sleep(0.25)

        cleanup_error: Exception | None = None
        try:
            process_tree.close_after_exit()
        except Exception as exc:
            cleanup_error = exc

        with self._lock:
            if self._process is process:
                self._finalize_exit_locked(process, code)
                if cleanup_error is not None:
                    self._state = "failed"
                    self._error = (
                        "Sync exited, but descendant cleanup failed: "
                        f"{cleanup_error}"
                    )

    def _finalize_exit_locked(
        self,
        process: subprocess.Popen[str],
        code: int,
    ) -> None:
        if self._process is not process:
            return
        self._return_code = int(code)
        self._finished = self._finished or time.monotonic()
        if self._state == "cancelling":
            self._state = "cancelled"
            self._error = None
        elif self._state == "running":
            self._state = "completed" if code == 0 else "failed"
            self._error = None

    def _refresh_process_locked(self) -> None:
        process = self._process
        if self._state in self._ACTIVE_STATES and process is not None:
            code = process.poll()
            monitor_is_cleaning_up = (
                self._monitor_thread is not None
                and self._monitor_thread.is_alive()
            )
            if code is not None and not monitor_is_cleaning_up:
                cleanup_error: Exception | None = None
                if self._process_tree is not None:
                    try:
                        self._process_tree.close_after_exit()
                    except Exception as exc:
                        cleanup_error = exc
                self._finalize_exit_locked(process, int(code))
                if cleanup_error is not None:
                    self._state = "failed"
                    self._error = (
                        "Sync exited, but descendant cleanup failed: "
                        f"{cleanup_error}"
                    )

    def shutdown(self) -> None:
        """Synchronously stop an active tree during application shutdown."""
        self.cancel(wait=True)
        with self._lock:
            monitor = self._monitor_thread
        if monitor is not None and monitor.is_alive():
            monitor.join(self._STOP_TIMEOUT_SECONDS + 2.0)
        with self._lock:
            process = self._process
            process_tree = self._process_tree
            retry = process is not None and process.poll() is None
            if retry:
                self._state = "cancelling"
                self._error = None
        if retry and process is not None and process_tree is not None:
            self._finish_cancel(process, process_tree)

    def clear_log(self) -> None:
        with self._lock:
            self._refresh_process_locked()
            process = self._process
            if (
                self._state in self._ACTIVE_STATES
                or (process is not None and process.poll() is None)
            ):
                raise RuntimeError("Cannot clear the log while sync is running")
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self.log_path.write_text("", encoding="utf-8")

    def snapshot(self) -> SyncSnapshot:
        with self._lock:
            self._refresh_process_locked()
            try:
                log = self.log_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                log = ""
            log = log[-30000:]
            elapsed_until = self._finished or time.monotonic()
            elapsed = (
                elapsed_until - self._started
                if self._started is not None
                else 0.0
            )
            return SyncSnapshot(
                state=self._maintenance_state or self._state,
                progress=progress_from_log(log),
                elapsed_seconds=max(0.0, elapsed),
                return_code=self._return_code,
                log=log,
                error=self._error,
            )


_SYNC_JOBS: dict[Path, SyncJob] = {}
_SYNC_JOBS_LOCK = threading.Lock()


def get_sync_job(db_path: Path) -> SyncJob:
    """Return the process controller shared across page navigation and clients."""
    key = Path(db_path).resolve()
    with _SYNC_JOBS_LOCK:
        if key not in _SYNC_JOBS:
            _SYNC_JOBS[key] = SyncJob(key)
        return _SYNC_JOBS[key]


def cancel_all_sync_jobs() -> None:
    """Bounded shutdown cleanup for every database-scoped sync process."""
    with _SYNC_JOBS_LOCK:
        jobs = tuple(_SYNC_JOBS.values())
    for job in jobs:
        job.shutdown()
