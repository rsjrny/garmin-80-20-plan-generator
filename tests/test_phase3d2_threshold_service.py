from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3

import pytest

from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import athlete_metrics_service
from garmin_data_hub.services import thresholds


def _database(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "threshold-service.db")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE activity (
            activity_id INTEGER PRIMARY KEY,
            start_time_gmt TEXT,
            elapsed_duration_seconds REAL,
            moving_duration_seconds REAL,
            max_hr REAL,
            average_hr REAL,
            activity_type TEXT,
            avg_power REAL,
            max_power REAL,
            norm_power REAL
        );
        CREATE TABLE daily_summary (
            calendar_date TEXT PRIMARY KEY,
            resting_heart_rate INTEGER
        );
        CREATE TABLE sleep (
            calendar_date TEXT PRIMARY KEY,
            resting_heart_rate INTEGER
        );
        """
    )
    apply_schema(conn, schema_sql_path())
    return conn


def test_value_and_provenance_roll_back_together_on_profile_failure(tmp_path):
    conn = _database(tmp_path)
    try:
        queries.set_calculated_metrics(conn, 180, 155)
        before_profile = tuple(
            conn.execute(
                """
                SELECT hrmax_calc, lthr_calc,
                       hrmax_calculation_id, lthr_calculation_id
                FROM athlete_profile WHERE profile_id=1
                """
            ).fetchone()
        )
        before_count = conn.execute(
            "SELECT COUNT(*) FROM threshold_calculation"
        ).fetchone()[0]
        started = datetime.now(timezone.utc) - timedelta(days=1)
        conn.execute(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, elapsed_duration_seconds,
                moving_duration_seconds, max_hr, activity_type
            ) VALUES (1, ?, 1200, 1200, 190, 'running')
            """,
            (started.isoformat(),),
        )
        conn.executemany(
            """
            INSERT INTO activity_trackpoints(
                activity_id, seq, timestamp_utc, heart_rate_bpm
            ) VALUES (1, ?, ?, ?)
            """,
            [
                (1, started.isoformat(), 188),
                (2, (started + timedelta(seconds=5)).isoformat(), 190),
            ],
        )
        conn.execute(
            """
            CREATE TEMP TRIGGER reject_hrmax_threshold_update
            BEFORE UPDATE OF hrmax_calc ON athlete_profile
            BEGIN
                SELECT RAISE(ABORT, 'injected profile pointer failure');
            END
            """
        )

        with pytest.raises(sqlite3.IntegrityError, match="pointer failure"):
            thresholds.refresh_thresholds(
                conn,
                activity_metric_provenance_version=(
                    queries.ACTIVITY_METRICS_PROVENANCE_VERSION
                ),
                commit=False,
            )

        after_profile = tuple(
            conn.execute(
                """
                SELECT hrmax_calc, lthr_calc,
                       hrmax_calculation_id, lthr_calculation_id
                FROM athlete_profile WHERE profile_id=1
                """
            ).fetchone()
        )
        assert after_profile == before_profile
        assert conn.execute(
            "SELECT COUNT(*) FROM threshold_calculation"
        ).fetchone()[0] == before_count
    finally:
        conn.close()


def test_no_evidence_refresh_keeps_values_references_and_ages(tmp_path):
    conn = _database(tmp_path)
    try:
        queries.set_calculated_metrics(conn, 184, 158)
        queries.set_calculated_ftp(conn, 250)
        before = tuple(
            conn.execute(
                """
                SELECT hrmax_calc, lthr_calc, ftp_calc,
                       hrmax_calculation_id, lthr_calculation_id,
                       ftp_calculation_id
                FROM athlete_profile WHERE profile_id=1
                """
            ).fetchone()
        )
        before_metrics = queries.get_athlete_metrics(conn)
        before_count = conn.execute(
            "SELECT COUNT(*) FROM threshold_calculation"
        ).fetchone()[0]

        thresholds.refresh_thresholds(
            conn,
            activity_metric_provenance_version=(
                queries.ACTIVITY_METRICS_PROVENANCE_VERSION
            ),
            as_of_utc=datetime.now(timezone.utc) + timedelta(days=365),
            commit=False,
        )

        after = tuple(
            conn.execute(
                """
                SELECT hrmax_calc, lthr_calc, ftp_calc,
                       hrmax_calculation_id, lthr_calculation_id,
                       ftp_calculation_id
                FROM athlete_profile WHERE profile_id=1
                """
            ).fetchone()
        )
        after_metrics = queries.get_athlete_metrics(conn)
        assert after == before
        assert conn.execute(
            "SELECT COUNT(*) FROM threshold_calculation"
        ).fetchone()[0] == before_count
        assert after_metrics["hrmax_calculated_at"] == before_metrics[
            "hrmax_calculated_at"
        ]
        assert after_metrics["ftp_calculated_at"] == before_metrics[
            "ftp_calculated_at"
        ]
    finally:
        conn.close()


def test_stale_retained_threshold_can_have_current_metric_snapshot(tmp_path):
    conn = _database(tmp_path)
    try:
        now = datetime.now(timezone.utc)
        evidence_at = now - timedelta(days=181)
        calculation_id = thresholds.insert_calculation(
            conn,
            {
                "threshold_type": "running_ftp",
                "calculated_value": 250,
                "algorithm_version": thresholds.RUNNING_FTP_ALGORITHM_VERSION,
                "calculated_at_utc": evidence_at.isoformat(),
                "evidence_at_utc": evidence_at.isoformat(),
                "source_kind": "running_ftp",
                "source_activity_id": 99,
                "source_activity_timestamp_utc": evidence_at.isoformat(),
                "source_sport": "running",
                "evidence_value": 263.157894,
                "evidence_duration_s": 1200,
            },
        )
        thresholds.point_profile_at_calculation(
            conn,
            threshold_type="running_ftp",
            value=250,
            calculation_id=calculation_id,
        )
        conn.execute(
            "INSERT INTO activity(activity_id, start_time_gmt, activity_type) VALUES (1, ?, 'running')",
            (now.isoformat(),),
        )
        conn.execute(
            """
            INSERT INTO activity_metrics(
                activity_id, refresh_provenance_version,
                threshold_lthr_bpm, threshold_ftp_w,
                threshold_resting_hr_bpm
            ) VALUES (1, ?, NULL, 250, 60)
            """,
            (queries.ACTIVITY_METRICS_PROVENANCE_VERSION,),
        )
        conn.commit()

        metrics = queries.get_athlete_metrics(conn)
        assert metrics["ftp_status"]["source_age_status"] == "stale"
        assert metrics["ftp_effective"] == 250
        assert 1 not in queries.list_activities_needing_metrics(conn)
    finally:
        conn.close()


def test_manual_recalculation_persists_validated_and_derived_provenance(tmp_path):
    conn = _database(tmp_path)
    db_path = tmp_path / "threshold-service.db"
    started = datetime.now(timezone.utc) - timedelta(days=1)
    try:
        conn.execute(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, elapsed_duration_seconds,
                moving_duration_seconds, max_hr, activity_type
            ) VALUES (7, ?, 1200, 1200, 190, 'running')
            """,
            (started.isoformat(),),
        )
        conn.executemany(
            """
            INSERT INTO activity_trackpoints(
                activity_id, seq, timestamp_utc, heart_rate_bpm
            ) VALUES (7, ?, ?, ?)
            """,
            [
                (1, started.isoformat(), 188),
                (2, (started + timedelta(seconds=5)).isoformat(), 190),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    assert athlete_metrics_service.calculate_metrics_from_db_sources(db_path) == (
        190,
        163,
        None,
    )
    conn = sqlite3.connect(db_path)
    try:
        metrics = queries.get_athlete_metrics(conn)
        assert metrics["hrmax_provenance"]["source_kind"] == "validated_hrmax"
        assert metrics["lthr_provenance"]["source_kind"] == (
            "estimated_from_validated_hrmax"
        )
        assert metrics["lthr_provenance"]["source_hrmax_provenance_id"] == (
            metrics["hrmax_provenance"]["threshold_calculation_id"]
        )
    finally:
        conn.close()
