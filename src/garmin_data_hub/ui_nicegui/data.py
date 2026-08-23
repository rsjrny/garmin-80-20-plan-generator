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
from typing import Any, Mapping

import pandas as pd

from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.athlete_metrics_service import get_athlete_metrics
from garmin_data_hub.services.plan_persistence import (
    load_generated_plan,
    load_plan_settings,
    save_plan_setting,
)
from garmin_data_hub.services.sync_status import progress_from_log


READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH|EXPLAIN)\b", re.IGNORECASE)
FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|ATTACH|DETACH|"
    r"VACUUM|REINDEX|PRAGMA|TRIGGER)\b",
    re.IGNORECASE,
)

MILES_PER_KILOMETRE = 0.621371192237334
INTERFACE_SETTING_DEFAULTS: dict[str, Any] = {
    "unit_system": "Imperial",
    "activity_lookback_days": 365,
    "activity_row_limit": 1000,
    "activity_default_sport": "All",
    "chart_lookback_days": 365,
    "sync_lookback_days": 0,
    "dashboard_item_limit": 8,
}
INTERFACE_SETTING_KEYS = {
    "unit_system": "unit_system",  # shared with the legacy interface
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
        "activity_lookback_days": int(values.get("activity_lookback_days", 365)),
        "activity_row_limit": int(values.get("activity_row_limit", 1000)),
        "activity_default_sport": str(values.get("activity_default_sport") or "All"),
        "chart_lookback_days": int(values.get("chart_lookback_days", 365)),
        "sync_lookback_days": int(values.get("sync_lookback_days", 0)),
        "dashboard_item_limit": int(values.get("dashboard_item_limit", 8)),
    }
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
        for friendly, key in INTERFACE_SETTING_KEYS.items():
            queries.set_setting(conn, key, normalized[friendly])
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
            FROM planned_workout
            """
        ).fetchone()
        upcoming = _rows(
            conn.execute(
                """
                SELECT scheduled_date AS date, workout_name AS workout,
                       planned_duration_s / 60.0 AS duration_min,
                       planned_distance_m / 1000.0 AS distance_km,
                       planned_tss AS tss
                FROM planned_workout
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
    clauses: list[str] = []
    parameters: list[Any] = []
    if sport:
        clauses.append("activity_type = ?")
        parameters.append(sport)
    if start_date:
        clauses.append("date(start_time_gmt) >= date(?)")
        parameters.append(start_date)
    if end_date:
        clauses.append("date(start_time_gmt) <= date(?)")
        parameters.append(end_date)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    parameters.append(max(1, min(int(limit), 5000)))
    conn = connect_sqlite(db_path)
    try:
        try:
            cursor = conn.execute(
                f"""
                SELECT activity_id AS id, date(start_time_gmt) AS date,
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
                FROM planned_workout
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
            cursor = conn.execute(
                """
                WITH planned AS (
                    SELECT scheduled_date AS day,
                           SUM(COALESCE(planned_distance_m, 0)) AS planned_distance_m,
                           SUM(COALESCE(planned_duration_s, 0)) AS planned_duration_s
                    FROM planned_workout GROUP BY scheduled_date
                ), actual AS (
                    SELECT date(start_time_gmt) AS day,
                           SUM(COALESCE(distance_meters, 0)) AS actual_distance_m,
                           SUM(COALESCE(elapsed_duration_seconds, 0)) AS actual_duration_s
                    FROM activity GROUP BY date(start_time_gmt)
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
                      COALESCE((SELECT MIN(scheduled_date) FROM planned_workout), days.day)
                      AND COALESCE((SELECT MAX(scheduled_date) FROM planned_workout), days.day)
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
}


def planning_settings(db_path: Path) -> dict[str, Any]:
    raw = load_plan_settings(db_path)
    result = {
        friendly: raw.get(setting)
        for friendly, setting in PLAN_SETTING_KEYS.items()
    }
    result["metrics"] = get_athlete_metrics(db_path)
    return result


def save_planning_settings(db_path: Path, values: Mapping[str, Any]) -> None:
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
    for friendly, setting in PLAN_SETTING_KEYS.items():
        save_plan_setting(db_path, setting, values[friendly])


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

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.log_path = self.db_path.parent / "logs" / "garmin_sync_latest.log"
        self._lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None
        self._state = "idle"
        self._started: float | None = None
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
        command.extend(["--visible", "--chrome"])
        return command

    def start(self, *, days: int = 0) -> None:
        days = int(days)
        if not 0 <= days <= 3650:
            raise ValueError("Sync days must be between 0 and 3,650")
        with self._lock:
            if self._state == "running":
                raise RuntimeError("Garmin sync is already running")
            command = self._command(days)
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self.log_path.write_text(
                "Started: " + " ".join(command) + "\n", encoding="utf-8"
            )
            environment = os.environ.copy()
            environment["PYTHONUNBUFFERED"] = "1"
            try:
                log_file = self.log_path.open("a", encoding="utf-8")
                process = subprocess.Popen(
                    command,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    text=True,
                    shell=False,
                )
                log_file.close()
            except OSError as exc:
                self._state = "failed"
                self._error = str(exc)
                raise
            self._process = process
            self._state = "running"
            self._started = time.monotonic()
            self._return_code = None
            self._error = None

    def cancel(self) -> bool:
        with self._lock:
            process = self._process
            if self._state != "running" or process is None:
                return False
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True,
                    check=False,
                    timeout=5,
                )
            else:
                process.terminate()
            self._state = "cancelled"
            self._return_code = process.poll()
            return True

    def clear_log(self) -> None:
        with self._lock:
            if self._state == "running":
                raise RuntimeError("Cannot clear the log while sync is running")
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self.log_path.write_text("", encoding="utf-8")

    def snapshot(self) -> SyncSnapshot:
        with self._lock:
            process = self._process
            if self._state == "running" and process is not None:
                code = process.poll()
                if code is not None:
                    self._return_code = code
                    self._state = "completed" if code == 0 else "failed"
            try:
                log = self.log_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                log = ""
            log = log[-30000:]
            elapsed = (
                time.monotonic() - self._started if self._started is not None else 0.0
            )
            return SyncSnapshot(
                state=self._state,
                progress=progress_from_log(log),
                elapsed_seconds=elapsed,
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
