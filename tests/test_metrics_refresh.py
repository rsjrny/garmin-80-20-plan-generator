from __future__ import annotations

import pytest

from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables
from garmin_data_hub.db import queries


def _insert_activity(
    conn,
    activity_id: int,
    *,
    start_time_gmt: str = "2026-04-01T06:00:00Z",
    elapsed_duration_seconds: int = 3600,
    average_speed: float = 3.2,
    max_hr: int = 180,
    training_stress_score: float = 55.0,
    average_hr: int = 150,
    activity_type: str = "running",
) -> None:
    conn.execute(
        """
        INSERT INTO activity (
            activity_id, start_time_gmt, elapsed_duration_seconds,
            average_speed, max_hr, training_stress_score, average_hr, activity_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            activity_id,
            start_time_gmt,
            elapsed_duration_seconds,
            average_speed,
            max_hr,
            training_stress_score,
            average_hr,
            activity_type,
        ),
    )
    conn.commit()


def _insert_trackpoints(conn, activity_id: int) -> None:
    rows = [
        (activity_id, 1, "2026-04-01T06:00:00Z", 60),
        (activity_id, 2, "2026-04-01T06:00:10Z", 90),
        (activity_id, 3, "2026-04-01T06:00:20Z", 120),
        (activity_id, 4, "2026-04-01T06:00:30Z", 140),
        (activity_id, 5, "2026-04-01T06:00:40Z", 155),
        (activity_id, 6, "2026-04-01T06:00:50Z", 155),
    ]
    conn.executemany(
        """
        INSERT INTO activity_trackpoint (activity_id, seq, timestamp_utc, heart_rate_bpm)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def test_refresh_persisted_activity_metrics_updates_zone_totals(db_conn):
    _insert_activity(db_conn, 1, max_hr=182, training_stress_score=63.5)
    _insert_trackpoints(db_conn, 1)

    summary = queries.refresh_persisted_activity_metrics(
        db_conn,
        activity_ids=[1],
        lthr=150,
    )

    assert summary["errors"] == 0
    assert summary["target_activities"] == 1
    assert summary["rows_upserted"] == 1
    assert summary["zones_updated"] == 1

    row = db_conn.execute(
        """
        SELECT zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s, lthr_est_bpm, tss
        FROM activity_metrics
        WHERE activity_id = 1
        """
    ).fetchone()

    assert row is not None
    assert row[0] == pytest.approx(10.0, abs=0.2)
    assert row[1] == pytest.approx(10.0, abs=0.2)
    assert row[2] == pytest.approx(10.0, abs=0.2)
    assert row[3] == pytest.approx(10.0, abs=0.2)
    assert row[4] == pytest.approx(10.0, abs=0.2)
    assert row[5] == 150
    assert row[6] == pytest.approx(63.5)

    diagnostics = queries.get_activity_metrics_diagnostics(db_conn)
    assert diagnostics["total_activities"] == 1
    assert diagnostics["total_metrics_rows"] == 1
    assert diagnostics["missing_metrics_count"] == 0
    assert diagnostics["last_refresh_utc"] is not None


def test_refresh_post_sync_tables_updates_athlete_profile(db_conn):
    _insert_activity(db_conn, 22, max_hr=188, average_hr=158)
    _insert_trackpoints(db_conn, 22)

    summary = refresh_post_sync_tables(db_conn, activity_ids=[22])

    assert summary["errors"] == 0
    assert summary["target_activities"] == 1
    assert summary["rows_upserted"] == 1

    profile = queries.get_athlete_profile(db_conn)
    assert profile is not None
    assert profile["hrmax_calc"] == 188
    assert profile["lthr_calc"] == int(round(188 * 0.86))
