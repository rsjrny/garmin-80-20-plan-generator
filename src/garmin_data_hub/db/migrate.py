from __future__ import annotations
from pathlib import Path
import logging
import sqlite3

logger = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 8


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


def _migration_5_add_plan_import_history(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS plan_import_history (
          plan_import_id          INTEGER PRIMARY KEY,
          source                  TEXT NOT NULL DEFAULT 'chatgpt_manual_upload',
          schema_version          TEXT NOT NULL,
          content_sha256          TEXT NOT NULL UNIQUE,
          plan_name               TEXT NOT NULL,
          replace_start_date      TEXT NOT NULL,
          replace_end_date        TEXT NOT NULL,
          workout_count           INTEGER NOT NULL,
          payload_json            TEXT NOT NULL,
          previous_plan_json      TEXT,
          validation_warnings_json TEXT NOT NULL DEFAULT '[]',
          applied_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE INDEX IF NOT EXISTS idx_plan_import_history_applied_at
          ON plan_import_history(applied_at);
        """
    )


def _migration_6_add_metric_refresh_provenance(conn: sqlite3.Connection) -> None:
    """Add nullable provenance without claiming legacy metric rows are current."""
    columns = {
        "refresh_provenance_version": "INTEGER",
        "threshold_lthr_bpm": "INTEGER",
        "threshold_ftp_w": "INTEGER",
        "threshold_resting_hr_bpm": "INTEGER",
    }
    savepoint_name = "migration_6_metric_refresh_provenance"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        for column_name, column_sql in columns.items():
            _add_column_if_missing(conn, "activity_metrics", column_name, column_sql)
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        raise
    conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")


def _migration_7_add_threshold_calculation_provenance(
    conn: sqlite3.Connection,
) -> None:
    """Add narrow append-only threshold provenance and preserve legacy values."""
    savepoint_name = "migration_7_threshold_calculation_provenance"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS threshold_calculation (
                threshold_calculation_id INTEGER PRIMARY KEY,
                threshold_type TEXT NOT NULL,
                calculated_value INTEGER NOT NULL,
                algorithm_version TEXT NOT NULL,
                calculated_at_utc TEXT,
                evidence_cutoff_utc TEXT,
                evidence_at_utc TEXT,
                source_kind TEXT NOT NULL,
                source_activity_id INTEGER,
                source_activity_timestamp_utc TEXT,
                source_sport TEXT,
                evidence_value REAL,
                evidence_duration_s REAL,
                candidate_count INTEGER,
                aggregate_evidence_json TEXT,
                parent_calculation_id INTEGER,
                FOREIGN KEY (parent_calculation_id)
                  REFERENCES threshold_calculation(threshold_calculation_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_threshold_calculation_type_id
            ON threshold_calculation(threshold_type, threshold_calculation_id)
            """
        )
        if not _table_exists(conn, "athlete_profile"):
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            return

        reference_columns = {
            "hrmax_calculation_id": "INTEGER REFERENCES threshold_calculation(threshold_calculation_id)",
            "lthr_calculation_id": "INTEGER REFERENCES threshold_calculation(threshold_calculation_id)",
            "ftp_calculation_id": "INTEGER REFERENCES threshold_calculation(threshold_calculation_id)",
            "resting_hr_calculation_id": "INTEGER REFERENCES threshold_calculation(threshold_calculation_id)",
        }
        for column_name, column_sql in reference_columns.items():
            _add_column_if_missing(conn, "athlete_profile", column_name, column_sql)

        row = conn.execute(
            """
            SELECT hrmax_calc, lthr_calc, ftp_calc, resting_hr,
                   calc_updated_utc,
                   hrmax_calculation_id, lthr_calculation_id,
                   ftp_calculation_id, resting_hr_calculation_id
            FROM athlete_profile WHERE profile_id=1
            """
        ).fetchone()
        if row is not None:
            legacy = (
                ("hrmax", "hrmax_calculation_id", row[0], row[5]),
                ("estimated_lthr", "lthr_calculation_id", row[1], row[6]),
                ("running_ftp", "ftp_calculation_id", row[2], row[7]),
                ("resting_hr", "resting_hr_calculation_id", row[3], row[8]),
            )
            for threshold_type, reference_column, raw_value, current_id in legacy:
                try:
                    value = int(raw_value) if raw_value is not None else None
                except (TypeError, ValueError, OverflowError):
                    value = None
                if value is None or value <= 0 or current_id is not None:
                    continue
                cursor = conn.execute(
                    """
                    INSERT INTO threshold_calculation(
                        threshold_type, calculated_value, algorithm_version,
                        calculated_at_utc, source_kind
                    ) VALUES (?, ?, 'legacy_unknown_v1', ?, 'unknown')
                    """,
                    (threshold_type, value, row[4]),
                )
                conn.execute(
                    f"UPDATE athlete_profile SET {reference_column}=? WHERE profile_id=1",
                    (int(cursor.lastrowid),),
                )
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        raise
    conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")


def _migration_8_add_archive_reconciliation(conn: sqlite3.Connection) -> None:
    """Add the app-owned historical archive reconciliation ledger."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS archive_reconciliation (
          reconciliation_id      INTEGER PRIMARY KEY,
          archive_identity       TEXT NOT NULL,
          target_activity_id     INTEGER,
          archive_format         TEXT NOT NULL
            CHECK (archive_format IN ('fit', 'gpx', 'tcx', 'unknown')),
          source_size            INTEGER NOT NULL CHECK (source_size >= 0),
          source_mtime_ns        INTEGER NOT NULL CHECK (source_mtime_ns >= 0),
          source_sha256          TEXT NOT NULL
            CHECK (
              length(source_sha256) = 64
              AND source_sha256 = lower(source_sha256)
              AND source_sha256 NOT GLOB '*[^0-9a-f]*'
            ),
          algorithm_version      TEXT NOT NULL,
          status                 TEXT NOT NULL
            CHECK (status IN (
              'ingested', 'resolved_no_records', 'unsupported', 'malformed',
              'mismatch', 'ambiguous', 'parser_error', 'write_error'
            )),
          reason_code            TEXT,
          identity_evidence_json TEXT NOT NULL DEFAULT '{}',
          trackpoint_count       INTEGER NOT NULL DEFAULT 0
            CHECK (trackpoint_count >= 0),
          attempted_at_utc       TEXT NOT NULL,
          completed_at_utc       TEXT,
          FOREIGN KEY (target_activity_id) REFERENCES activity(activity_id),
          UNIQUE (
            archive_identity, target_activity_id, source_size, source_mtime_ns,
            source_sha256, algorithm_version
          )
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_archive_reconciliation_fingerprint
        ON archive_reconciliation(
          archive_identity, target_activity_id, source_size, source_mtime_ns,
          source_sha256, algorithm_version
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_archive_reconciliation_status
        ON archive_reconciliation(status, target_activity_id)
        """
    )


def _fix_trackpoint_cascade(conn: sqlite3.Connection) -> None:
    """Remove ON DELETE CASCADE from activity_trackpoints if present.

    garmin-givemydata uses INSERT OR REPLACE on the activity table, which
    internally does DELETE + INSERT and fires the cascade, wiping all
    trackpoints on every sync.  This migration recreates the table without
    the cascade so existing trackpoint data is preserved.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity_trackpoints'"
    ).fetchone()
    if row is None:
        return  # table doesn't exist yet; schema.sql will create it correctly
    ddl: str = row[0] or ""
    if "ON DELETE CASCADE" not in ddl.upper():
        return  # already fixed

    existing_columns = _get_table_columns(conn, "activity_trackpoints")
    required_columns = {"activity_id", "seq", "timestamp_utc"}
    if not required_columns.issubset(existing_columns):
        raise sqlite3.OperationalError(
            "Legacy activity_trackpoints table is missing required key columns"
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
        + " FROM activity_trackpoints"
    )
    conn.executescript(
        """
        DROP TABLE activity_trackpoints;
        ALTER TABLE activity_trackpoint_new RENAME TO activity_trackpoints;

        CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_activity_time
          ON activity_trackpoints(activity_id, timestamp_utc);

        CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_latlon
          ON activity_trackpoints(latitude, longitude);

        PRAGMA foreign_keys=ON;
        """
    )
    conn.commit()


def _rename_legacy_trackpoint_table(conn: sqlite3.Connection) -> None:
    """Rename legacy singular trackpoint table to upstream plural naming."""
    has_plural = _table_exists(conn, "activity_trackpoints")
    has_singular = _table_exists(conn, "activity_trackpoint")
    if has_plural or not has_singular:
        return

    conn.execute("ALTER TABLE activity_trackpoint RENAME TO activity_trackpoints")
    conn.execute(
        """
                CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_activity_time
                    ON activity_trackpoints(activity_id, timestamp_utc)
                """
    )
    conn.execute(
        """
                CREATE INDEX IF NOT EXISTS idx_activity_trackpoint_latlon
                    ON activity_trackpoints(latitude, longitude)
                """
    )
    conn.commit()


def apply_schema(conn: sqlite3.Connection, schema_path: Path | None = None) -> None:
    """Apply app schema and run one-time versioned migrations for older databases."""
    _ensure_schema_migrations_table(conn)
    schema_sql = _load_schema_sql(schema_path)

    # Preflight older trackpoint tables before replaying schema.sql, otherwise
    # legacy `ON DELETE CASCADE` DDL can make the later index creation fail.
    _rename_legacy_trackpoint_table(conn)
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
            "fix activity_trackpoints cascade behavior",
            lambda: _fix_trackpoint_cascade(conn),
        ),
        (
            5,
            "add manual plan import history",
            lambda: _migration_5_add_plan_import_history(conn),
        ),
        (
            6,
            "add activity metric refresh provenance",
            lambda: _migration_6_add_metric_refresh_provenance(conn),
        ),
        (
            7,
            "add threshold calculation provenance",
            lambda: _migration_7_add_threshold_calculation_provenance(conn),
        ),
        (
            8,
            "add historical archive reconciliation ledger",
            lambda: _migration_8_add_archive_reconciliation(conn),
        ),
    ]

    current_version = _get_current_schema_version(conn)
    recorded_version = current_version
    for version, name, migration in migrations:
        if current_version >= version:
            continue
        if version >= 7:
            boundary = f"migration_{version}_with_version_record"
            conn.execute(f"SAVEPOINT {boundary}")
            try:
                migration()
                _record_migration(conn, version, name)
            except Exception:
                try:
                    conn.execute(f"ROLLBACK TO SAVEPOINT {boundary}")
                finally:
                    conn.execute(f"RELEASE SAVEPOINT {boundary}")
                raise
            conn.execute(f"RELEASE SAVEPOINT {boundary}")
        else:
            migration()
            _record_migration(conn, version, name)
        current_version = version
        logger.info("Applied schema migration v%s: %s", version, name)

    # A copied or manually altered database can claim v6 while missing one of
    # its nullable columns. Repair that state idempotently instead of trusting
    # the version record alone.
    if recorded_version >= 6:
        _migration_6_add_metric_refresh_provenance(conn)
    if recorded_version >= 7:
        _migration_7_add_threshold_calculation_provenance(conn)
    if recorded_version >= 8:
        _migration_8_add_archive_reconciliation(conn)

    # Keep baseline DDL idempotent so new installs and reruns remain safe.
    conn.executescript(schema_sql)
    conn.commit()
