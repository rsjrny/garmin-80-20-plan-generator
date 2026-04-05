from __future__ import annotations
from pathlib import Path
import logging
import sqlite3

logger = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 4


def _ensure_schema_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )
        """
    )


def _get_current_schema_version(conn: sqlite3.Connection) -> int:
    _ensure_schema_migrations_table(conn)
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
    ).fetchone()
    return int(row[0] or 0) if row else 0


def get_current_schema_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied app schema migration version."""
    return _get_current_schema_version(conn)


def _record_migration(conn: sqlite3.Connection, version: int, name: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO schema_migrations(version, name) VALUES (?, ?)",
        (version, name),
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    existing = _get_table_columns(conn, table_name)
    if column_name in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def _load_schema_sql(schema_path: Path | None) -> str:
    if schema_path is not None:
        candidate = Path(schema_path)
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
        logger.warning(
            "Schema path %s was not found; falling back to packaged schema resource",
            candidate,
        )

    from garmin_data_hub.paths import read_schema_sql

    return read_schema_sql()


def _migration_1_apply_baseline_schema(
    conn: sqlite3.Connection, schema_sql: str
) -> None:
    conn.executescript(schema_sql)


def _migration_2_upgrade_athlete_profile(conn: sqlite3.Connection) -> None:
    columns = {
        "ftp_calc": "INTEGER",
        "ftp_override": "INTEGER",
        "resting_hr": "INTEGER",
    }
    for column_name, column_sql in columns.items():
        _add_column_if_missing(conn, "athlete_profile", column_name, column_sql)


def _migration_3_upgrade_app_tables(conn: sqlite3.Connection) -> None:
    activity_metric_columns = {
        "hr_drift_pct": "REAL",
        "hr_recovery_60s_bpm": "REAL",
        "avg_hr_to_max_pct": "REAL",
        "variability_index": "REAL",
        "avg_power_w": "REAL",
        "max_power_w": "REAL",
        "peak_power_5s_w": "REAL",
        "peak_power_30s_w": "REAL",
        "peak_power_60s_w": "REAL",
        "peak_power_300s_w": "REAL",
        "peak_power_1200s_w": "REAL",
        "power_zone_1_s": "REAL",
        "power_zone_2_s": "REAL",
        "power_zone_3_s": "REAL",
        "power_zone_4_s": "REAL",
        "power_zone_5_s": "REAL",
        "power_zone_6_s": "REAL",
        "power_zone_7_s": "REAL",
        "efficiency_factor": "REAL",
        "pace_decoupling_pct": "REAL",
        "avg_cadence_spm": "REAL",
        "avg_stride_length_m": "REAL",
        "avg_vertical_osc_cm": "REAL",
        "avg_ground_contact_ms": "REAL",
        "avg_vertical_ratio": "REAL",
        "gct_balance_avg_pct": "REAL",
        "avg_temperature_c": "REAL",
        "min_temperature_c": "REAL",
        "max_temperature_c": "REAL",
        "total_ascent_m": "REAL",
        "total_descent_m": "REAL",
        "max_altitude_m": "REAL",
        "min_altitude_m": "REAL",
        "training_effect_aerobic": "REAL",
        "training_effect_anaerobic": "REAL",
        "performance_condition_start": "REAL",
        "performance_condition_end": "REAL",
    }
    for column_name, column_sql in activity_metric_columns.items():
        _add_column_if_missing(conn, "activity_metrics", column_name, column_sql)

    _add_column_if_missing(conn, "planned_workout", "structure_json", "TEXT")
    _add_column_if_missing(
        conn,
        "app_settings",
        "updated_at",
        "TEXT",
    )


def _fix_trackpoint_cascade(conn: sqlite3.Connection) -> None:
    """Remove ON DELETE CASCADE from activity_trackpoint if present.

    garmin-givemydata uses INSERT OR REPLACE on the activity table, which
    internally does DELETE + INSERT and fires the cascade, wiping all
    trackpoints on every sync.  This migration recreates the table without
    the cascade so existing trackpoint data is preserved.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity_trackpoint'"
    ).fetchone()
    if row is None:
        return  # table doesn't exist yet; schema.sql will create it correctly
    ddl: str = row[0] or ""
    if "ON DELETE CASCADE" not in ddl.upper():
        return  # already fixed

    existing_columns = _get_table_columns(conn, "activity_trackpoint")
    required_columns = {"activity_id", "seq", "timestamp_utc"}
    if not required_columns.issubset(existing_columns):
        raise sqlite3.OperationalError(
            "Legacy activity_trackpoint table is missing required key columns"
        )

    ordered_columns = [
        "activity_id",
        "seq",
        "timestamp_utc",
        "latitude",
        "longitude",
        "altitude_m",
        "distance_m",
        "speed_mps",
        "heart_rate_bpm",
        "cadence",
        "power_w",
        "temperature_c",
    ]
    select_columns = [
        column if column in existing_columns else f"NULL AS {column}"
        for column in ordered_columns
    ]

    conn.executescript(
        """
        PRAGMA foreign_keys=OFF;

        CREATE TABLE activity_trackpoint_new (
          activity_id    INTEGER NOT NULL,
          seq            INTEGER NOT NULL,
          timestamp_utc  TEXT NOT NULL,
          latitude       REAL,
          longitude      REAL,
          altitude_m     REAL,
          distance_m     REAL,
          speed_mps      REAL,
          heart_rate_bpm INTEGER,
          cadence        INTEGER,
          power_w        INTEGER,
          temperature_c  REAL,
          PRIMARY KEY (activity_id, seq),
          FOREIGN KEY (activity_id) REFERENCES activity(activity_id)
        );
        """
    )
    conn.execute(
        """
        INSERT INTO activity_trackpoint_new (
          activity_id, seq, timestamp_utc, latitude, longitude,
          altitude_m, distance_m, speed_mps, heart_rate_bpm,
          cadence, power_w, temperature_c
        )
        SELECT """
        + ", ".join(select_columns)
        + " FROM activity_trackpoint"
    )
    conn.executescript(
        """
        DROP TABLE activity_trackpoint;
        ALTER TABLE activity_trackpoint_new RENAME TO activity_trackpoint;

        CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_activity_time
          ON activity_trackpoint(activity_id, timestamp_utc);

        CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_latlon
          ON activity_trackpoint(latitude, longitude);

        PRAGMA foreign_keys=ON;
        """
    )
    conn.commit()


def apply_schema(conn: sqlite3.Connection, schema_path: Path | None = None) -> None:
    """Apply app schema and run one-time versioned migrations for older databases."""
    _ensure_schema_migrations_table(conn)
    schema_sql = _load_schema_sql(schema_path)

    # Preflight older trackpoint tables before replaying schema.sql, otherwise
    # legacy `ON DELETE CASCADE` DDL can make the later index creation fail.
    _fix_trackpoint_cascade(conn)

    migrations = [
        (
            1,
            "baseline app schema",
            lambda: _migration_1_apply_baseline_schema(conn, schema_sql),
        ),
        (
            2,
            "upgrade athlete_profile columns",
            lambda: _migration_2_upgrade_athlete_profile(conn),
        ),
        (
            3,
            "upgrade app-owned table columns",
            lambda: _migration_3_upgrade_app_tables(conn),
        ),
        (
            4,
            "fix activity_trackpoint cascade behavior",
            lambda: _fix_trackpoint_cascade(conn),
        ),
    ]

    current_version = _get_current_schema_version(conn)
    for version, name, migration in migrations:
        if current_version >= version:
            continue
        migration()
        _record_migration(conn, version, name)
        current_version = version
        logger.info("Applied schema migration v%s: %s", version, name)

    # Keep baseline DDL idempotent so new installs and reruns remain safe.
    conn.executescript(schema_sql)
    conn.commit()
