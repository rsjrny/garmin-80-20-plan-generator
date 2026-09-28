from __future__ import annotations

import sqlite3

import pytest

from garmin_data_hub.db import migrate
from garmin_data_hub.db.migrate import (
    CURRENT_SCHEMA_VERSION,
    apply_schema,
    get_current_schema_version,
)
from garmin_data_hub.paths import schema_sql_path


PROVENANCE_COLUMNS = {
    "refresh_provenance_version",
    "threshold_lthr_bpm",
    "threshold_ftp_w",
    "threshold_resting_hr_bpm",
}


def _record_versions_through_five(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.executemany(
        "INSERT INTO schema_migrations(version, name) VALUES (?, ?)",
        [(version, f"migration {version}") for version in range(1, 6)],
    )
    conn.commit()


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
            "INSERT INTO activity_metrics(activity_id, lthr_est_bpm, zone_1_s) VALUES (7, 155, 321)"
        )
        conn.execute(
            """
            CREATE TABLE activity_trackpoints (
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
        assert {
            "power_zone_7_s",
            "performance_condition_end",
            "avg_power_w",
            "refresh_provenance_version",
            "threshold_lthr_bpm",
            "threshold_ftp_w",
            "threshold_resting_hr_bpm",
        }.issubset(metric_cols)
        legacy_metric = conn.execute(
            """
            SELECT lthr_est_bpm, zone_1_s, refresh_provenance_version,
                   threshold_lthr_bpm, threshold_ftp_w
            FROM activity_metrics WHERE activity_id=7
            """
        ).fetchone()
        assert tuple(legacy_metric) == (155, 321, None, None, None)

        settings_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(app_settings)").fetchall()
        }
        assert "updated_at" in settings_cols

        import_history_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(plan_import_history)").fetchall()
        }
        assert {
            "content_sha256",
            "payload_json",
            "replace_start_date",
            "replace_end_date",
        }.issubset(import_history_cols)

        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity_trackpoints'"
        ).fetchone()[0]
        assert "ON DELETE CASCADE" not in ddl.upper()

        apply_schema(conn, schema_sql_path())
        migration_rows = conn.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0]
        assert migration_rows == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_apply_schema_falls_back_when_given_path_is_missing(tmp_path):
    db_path = tmp_path / "missing_schema_path.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )

        apply_schema(conn, tmp_path / "does_not_exist.sql")

        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "activity_metrics" in tables
    finally:
        conn.close()


def test_v6_repairs_missing_column_even_when_version_is_already_recorded(tmp_path):
    db_path = tmp_path / "recorded_v6_missing_column.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        conn.execute(
            """
            CREATE TABLE activity_metrics (
                activity_id INTEGER PRIMARY KEY,
                refresh_provenance_version INTEGER,
                threshold_lthr_bpm INTEGER,
                threshold_resting_hr_bpm INTEGER
            )
            """
        )
        _record_versions_through_five(conn)
        conn.execute(
            "INSERT INTO schema_migrations(version, name) VALUES (6, 'incomplete v6')"
        )
        conn.commit()

        apply_schema(conn, schema_sql_path())

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(activity_metrics)")
        }
        assert PROVENANCE_COLUMNS.issubset(columns)
        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_partial_v6_failure_rolls_back_columns_and_version(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "failed_v6.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        conn.execute("CREATE TABLE activity_metrics (activity_id INTEGER PRIMARY KEY)")
        _record_versions_through_five(conn)
        original_add_column = migrate._add_column_if_missing
        calls = 0

        def fail_during_v6(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise sqlite3.OperationalError("injected v6 migration failure")
            return original_add_column(*args, **kwargs)

        monkeypatch.setattr(migrate, "_add_column_if_missing", fail_during_v6)

        with pytest.raises(sqlite3.OperationalError, match="injected v6"):
            apply_schema(conn, schema_sql_path())

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(activity_metrics)")
        }
        assert PROVENANCE_COLUMNS.isdisjoint(columns)
        assert get_current_schema_version(conn) == 5
    finally:
        conn.close()


def test_partially_completed_v6_is_repaired_idempotently(tmp_path):
    db_path = tmp_path / "partial_v6.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        conn.execute(
            """
            CREATE TABLE activity_metrics (
                activity_id INTEGER PRIMARY KEY,
                refresh_provenance_version INTEGER
            )
            """
        )
        _record_versions_through_five(conn)

        apply_schema(conn, schema_sql_path())
        apply_schema(conn, schema_sql_path())

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(activity_metrics)")
        }
        assert PROVENANCE_COLUMNS.issubset(columns)
        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_fresh_and_migrated_provenance_column_definitions_match(tmp_path):
    fresh = sqlite3.connect(tmp_path / "fresh.db")
    migrated = sqlite3.connect(tmp_path / "migrated.db")
    try:
        fresh.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        apply_schema(fresh, schema_sql_path())

        migrated.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        migrated.execute(
            "CREATE TABLE activity_metrics (activity_id INTEGER PRIMARY KEY)"
        )
        _record_versions_through_five(migrated)
        apply_schema(migrated, schema_sql_path())

        def definitions(conn):
            return {
                row[1]: (row[2], row[3], row[4], row[5])
                for row in conn.execute("PRAGMA table_info(activity_metrics)")
                if row[1] in PROVENANCE_COLUMNS
            }

        assert definitions(migrated) == definitions(fresh)
        assert set(definitions(fresh)) == PROVENANCE_COLUMNS
    finally:
        fresh.close()
        migrated.close()
