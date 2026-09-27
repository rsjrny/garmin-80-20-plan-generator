from __future__ import annotations

import pytest

from garmin_data_hub.db import queries


TEMPORAL_VALUES = {
    "aerobic_decoupling_pct": 11.0,
    "pace_decoupling_pct": 12.0,
    "hr_drift_pct": 13.0,
    "peak_power_5s_w": 105.0,
    "peak_power_30s_w": 130.0,
    "peak_power_60s_w": 160.0,
    "peak_power_300s_w": 300.0,
    "peak_power_1200s_w": 1200.0,
}


def _prepare_activity_reader(conn, activity_id: int) -> None:
    conn.execute("ALTER TABLE activity ADD COLUMN distance_meters REAL")
    conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, average_speed, average_hr, max_hr,
            activity_type, distance_meters, avg_power, norm_power,
            training_stress_score
        ) VALUES (?, '2026-04-01T06:00:00Z', 600, 600, 3.0, 140, 180,
                  'cycling', 3000, 200, 210, 42)
        """,
        (activity_id,),
    )


def _insert_temporal_metrics(conn, activity_id: int, version: int | None) -> None:
    columns = ", ".join(TEMPORAL_VALUES)
    placeholders = ", ".join("?" for _ in TEMPORAL_VALUES)
    conn.execute(
        f"""
        INSERT INTO activity_metrics(
            activity_id, refresh_provenance_version, {columns},
            efficiency_factor
        ) VALUES (?, ?, {placeholders}, 1.234)
        """,
        (activity_id, version, *TEMPORAL_VALUES.values()),
    )
    conn.commit()


def _activity_frame_row(conn, activity_id: int):
    frame = queries.get_activities_dataframe(
        conn, "2026-01-01T00:00:00Z", use_temp_zone_metrics=False
    )
    assert len(frame.index) == 1
    return frame.iloc[0]


@pytest.mark.parametrize(
    ("stored_version", "visible"),
    [
        (queries.ACTIVITY_METRICS_PROVENANCE_VERSION, True),
        (queries.ACTIVITY_METRICS_PROVENANCE_VERSION - 1, False),
        (None, False),
        (queries.ACTIVITY_METRICS_PROVENANCE_VERSION + 1, False),
    ],
)
def test_canonical_temporal_projection_requires_exact_current_provenance(
    db_conn, stored_version, visible
):
    _prepare_activity_reader(db_conn, 301)
    _insert_temporal_metrics(db_conn, 301, stored_version)
    stored_before = tuple(
        db_conn.execute(
            f"""
            SELECT refresh_provenance_version, {', '.join(TEMPORAL_VALUES)}
            FROM activity_metrics WHERE activity_id=301
            """
        ).fetchone()
    )

    projection = queries.current_temporal_metric_projection_sql("am")
    projected = db_conn.execute(
        f"SELECT {projection} FROM activity_metrics am WHERE activity_id=301"
    ).fetchone()
    expected = tuple(TEMPORAL_VALUES.values()) if visible else (None,) * 8
    assert tuple(projected) == expected

    frame_row = _activity_frame_row(db_conn, 301)
    exposed_frame_columns = set(TEMPORAL_VALUES) - {"pace_decoupling_pct"}
    for column_name in exposed_frame_columns:
        if visible:
            assert frame_row[column_name] == pytest.approx(
                TEMPORAL_VALUES[column_name]
            )
        else:
            assert frame_row[column_name] is None
    assert frame_row["efficiency_factor"] == pytest.approx(1.234)

    stored_after = tuple(
        db_conn.execute(
            f"""
            SELECT refresh_provenance_version, {', '.join(TEMPORAL_VALUES)}
            FROM activity_metrics WHERE activity_id=301
            """
        ).fetchone()
    )
    assert stored_after == stored_before


def test_failed_refresh_keeps_old_temporal_values_suppressed_until_success(db_conn):
    _prepare_activity_reader(db_conn, 302)
    db_conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps,
            heart_rate_bpm, power_w
        ) VALUES (302, ?, ?, ?, 100, ?)
        """,
        [
            (1, "2026-04-01T06:00:00Z", 2.0, 200),
            (2, "2026-04-01T06:00:05Z", 1.0, 100),
            (3, "2026-04-01T06:00:10Z", None, None),
        ],
    )
    _insert_temporal_metrics(db_conn, 302, 1)
    db_conn.execute(
        """
        CREATE TRIGGER fail_phase2d1_refresh
        BEFORE UPDATE ON activity_metrics
        WHEN NEW.activity_id=302 AND NEW.aerobic_decoupling_pct IS NULL
        BEGIN
            SELECT RAISE(ABORT, 'injected temporal refresh failure');
        END
        """
    )
    db_conn.commit()

    assert _activity_frame_row(db_conn, 302)["aerobic_decoupling_pct"] is None
    failed = queries.refresh_persisted_activity_metrics(db_conn, [302])
    assert failed["errors"] == 1
    physical_after_failure = db_conn.execute(
        """
        SELECT refresh_provenance_version, aerobic_decoupling_pct
        FROM activity_metrics WHERE activity_id=302
        """
    ).fetchone()
    assert tuple(physical_after_failure) == (1, TEMPORAL_VALUES["aerobic_decoupling_pct"])
    assert _activity_frame_row(db_conn, 302)["aerobic_decoupling_pct"] is None

    db_conn.execute("DROP TRIGGER fail_phase2d1_refresh")
    db_conn.commit()
    succeeded = queries.refresh_persisted_activity_metrics(db_conn, [302])
    assert succeeded["errors"] == 0
    physical_after_success = db_conn.execute(
        """
        SELECT refresh_provenance_version, aerobic_decoupling_pct
        FROM activity_metrics WHERE activity_id=302
        """
    ).fetchone()
    assert tuple(physical_after_success) == (
        queries.ACTIVITY_METRICS_PROVENANCE_VERSION,
        pytest.approx(50.0),
    )
    assert _activity_frame_row(db_conn, 302)[
        "aerobic_decoupling_pct"
    ] == pytest.approx(50.0)
