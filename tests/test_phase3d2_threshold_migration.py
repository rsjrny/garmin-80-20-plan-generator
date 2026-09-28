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


REFERENCE_COLUMNS = {
    "hrmax_calculation_id",
    "lthr_calculation_id",
    "ftp_calculation_id",
    "resting_hr_calculation_id",
}


def _legacy_v6(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT);
        CREATE TABLE activity_metrics (
            activity_id INTEGER PRIMARY KEY,
            refresh_provenance_version INTEGER,
            threshold_lthr_bpm INTEGER,
            threshold_ftp_w INTEGER,
            threshold_resting_hr_bpm INTEGER
        );
        CREATE TABLE athlete_profile (
            profile_id INTEGER PRIMARY KEY DEFAULT 1,
            hrmax_calc INTEGER,
            lthr_calc INTEGER,
            ftp_calc INTEGER,
            calc_updated_utc TEXT,
            hrmax_override INTEGER,
            lthr_override INTEGER,
            ftp_override INTEGER,
            resting_hr INTEGER,
            override_updated_utc TEXT
        );
        INSERT INTO athlete_profile VALUES (
            1, 184, 158, 251, '2025-01-01T00:00:00Z',
            190, 170, 275, 49, '2026-01-01T00:00:00Z'
        );
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.executemany(
        "INSERT INTO schema_migrations(version, name) VALUES (?, ?)",
        [(version, f"migration {version}") for version in range(1, 7)],
    )
    conn.commit()


def test_v7_preserves_values_and_records_honest_unknown_provenance(tmp_path):
    conn = sqlite3.connect(tmp_path / "legacy-v6.db")
    try:
        _legacy_v6(conn)
        apply_schema(conn, schema_sql_path())

        profile = conn.execute(
            """
            SELECT hrmax_calc, lthr_calc, ftp_calc, resting_hr,
                   hrmax_override, lthr_override, ftp_override,
                   hrmax_calculation_id, lthr_calculation_id,
                   ftp_calculation_id, resting_hr_calculation_id
            FROM athlete_profile WHERE profile_id=1
            """
        ).fetchone()
        assert profile[:7] == (184, 158, 251, 49, 190, 170, 275)
        assert all(value is not None for value in profile[7:])

        rows = conn.execute(
            """
            SELECT threshold_type, calculated_value, source_kind,
                   source_activity_id, source_activity_timestamp_utc,
                   source_sport, evidence_value, evidence_duration_s
            FROM threshold_calculation ORDER BY threshold_calculation_id
            """
        ).fetchall()
        assert [(row[0], row[1], row[2]) for row in rows] == [
            ("hrmax", 184, "unknown"),
            ("estimated_lthr", 158, "unknown"),
            ("running_ftp", 251, "unknown"),
            ("resting_hr", 49, "unknown"),
        ]
        assert all(all(value is None for value in row[3:]) for row in rows)
        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_v7_rerun_and_recorded_repair_are_idempotent(tmp_path):
    conn = sqlite3.connect(tmp_path / "repair-v7.db")
    try:
        _legacy_v6(conn)
        apply_schema(conn, schema_sql_path())
        apply_schema(conn, schema_sql_path())
        assert conn.execute(
            "SELECT COUNT(*) FROM threshold_calculation"
        ).fetchone()[0] == 4
        columns = {row[1] for row in conn.execute("PRAGMA table_info(athlete_profile)")}
        assert REFERENCE_COLUMNS <= columns
    finally:
        conn.close()


def test_v7_failure_rolls_back_schema_data_and_version(tmp_path, monkeypatch):
    conn = sqlite3.connect(tmp_path / "failed-v7.db")
    try:
        _legacy_v6(conn)
        original = migrate._add_column_if_missing
        calls = 0

        def fail_second_reference(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise sqlite3.OperationalError("injected v7 migration failure")
            return original(*args, **kwargs)

        monkeypatch.setattr(migrate, "_add_column_if_missing", fail_second_reference)
        with pytest.raises(sqlite3.OperationalError, match="injected v7"):
            apply_schema(conn, schema_sql_path())

        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        columns = {row[1] for row in conn.execute("PRAGMA table_info(athlete_profile)")}
        assert "threshold_calculation" not in tables
        assert REFERENCE_COLUMNS.isdisjoint(columns)
        assert conn.execute(
            "SELECT hrmax_calc, ftp_calc, ftp_override FROM athlete_profile"
        ).fetchone() == (184, 251, 275)
        assert get_current_schema_version(conn) == 6
    finally:
        conn.close()
