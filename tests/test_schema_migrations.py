from __future__ import annotations

import sqlite3

from garmin_data_hub.db.migrate import (
    CURRENT_SCHEMA_VERSION,
    apply_schema,
    get_current_schema_version,
)
from garmin_data_hub.paths import schema_sql_path


def test_apply_schema_records_current_version(tmp_path):
    db_path = tmp_path / "migration_version.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        apply_schema(conn, schema_sql_path())

        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION

        rows = conn.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert len(rows) == CURRENT_SCHEMA_VERSION
        assert rows[-1][0] == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_apply_schema_upgrades_legacy_tables(tmp_path):
    db_path = tmp_path / "legacy_upgrade.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        conn.execute(
            """
            CREATE TABLE athlete_profile (
                profile_id INTEGER PRIMARY KEY DEFAULT 1,
                hrmax_calc INTEGER,
                lthr_calc INTEGER,
                calc_updated_utc TEXT,
                hrmax_override INTEGER,
                lthr_override INTEGER,
                override_updated_utc TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE activity_metrics (
                activity_id INTEGER PRIMARY KEY,
                moving_time_s REAL,
                stopped_time_s REAL,
                avg_moving_speed_mps REAL,
                hr_max_est_bpm REAL,
                lthr_est_bpm REAL,
                trimp REAL,
                aerobic_decoupling_pct REAL,
                zone_1_s REAL,
                zone_2_s REAL,
                zone_3_s REAL,
                zone_4_s REAL,
                zone_5_s REAL,
                np_w REAL,
                if_val REAL,
                tss REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE activity_trackpoint (
              activity_id    INTEGER NOT NULL,
              seq            INTEGER NOT NULL,
              timestamp_utc  TEXT NOT NULL,
              heart_rate_bpm INTEGER,
              PRIMARY KEY (activity_id, seq),
              FOREIGN KEY (activity_id) REFERENCES activity(activity_id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()

        apply_schema(conn, schema_sql_path())

        athlete_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(athlete_profile)").fetchall()
        }
        assert {"ftp_calc", "ftp_override", "resting_hr"}.issubset(athlete_cols)

        metric_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(activity_metrics)").fetchall()
        }
        assert {"power_zone_7_s", "performance_condition_end", "avg_power_w"}.issubset(
            metric_cols
        )

        settings_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(app_settings)").fetchall()
        }
        assert "updated_at" in settings_cols

        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity_trackpoint'"
        ).fetchone()[0]
        assert "ON DELETE CASCADE" not in ddl.upper()

        apply_schema(conn, schema_sql_path())
        migration_rows = conn.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0]
        assert migration_rows == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()
