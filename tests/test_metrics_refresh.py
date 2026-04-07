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
    moving_duration_seconds: int | None = 3300,
    average_speed: float = 3.2,
    max_hr: int = 180,
    training_stress_score: float | None = 55.0,
    average_hr: int = 150,
    activity_type: str = "running",
    avg_power: float | None = None,
    max_power: float | None = None,
    norm_power: float | None = None,
    intensity_factor: float | None = None,
    avg_cadence: float | None = 86.0,
    elevation_gain: float | None = 120.0,
    elevation_loss: float | None = 118.0,
    min_elevation: float | None = 12.0,
    max_elevation: float | None = 66.0,
    aerobic_training_effect: float | None = 2.8,
    anaerobic_training_effect: float | None = 0.4,
    min_temperature: float | None = 8.0,
    max_temperature: float | None = 14.0,
) -> None:
    conn.execute(
        """
        INSERT INTO activity (
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, average_speed, max_hr,
            training_stress_score, average_hr, activity_type,
            avg_power, max_power, norm_power, intensity_factor,
            avg_cadence, elevation_gain, elevation_loss,
            min_elevation, max_elevation,
            aerobic_training_effect, anaerobic_training_effect,
            min_temperature, max_temperature
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            activity_id,
            start_time_gmt,
            elapsed_duration_seconds,
            moving_duration_seconds,
            average_speed,
            max_hr,
            training_stress_score,
            average_hr,
            activity_type,
            avg_power,
            max_power,
            norm_power,
            intensity_factor,
            avg_cadence,
            elevation_gain,
            elevation_loss,
            min_elevation,
            max_elevation,
            aerobic_training_effect,
            anaerobic_training_effect,
            min_temperature,
            max_temperature,
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


def _insert_power_trackpoints(conn, activity_id: int) -> None:
    rows = [
        (activity_id, 1, "2026-04-01T07:00:00Z", 120, 80, 90),
        (activity_id, 2, "2026-04-01T07:00:10Z", 125, 82, 115),
        (activity_id, 3, "2026-04-01T07:00:20Z", 130, 84, 145),
        (activity_id, 4, "2026-04-01T07:00:30Z", 135, 86, 170),
        (activity_id, 5, "2026-04-01T07:00:40Z", 140, 88, 205),
        (activity_id, 6, "2026-04-01T07:00:50Z", 145, 90, 235),
        (activity_id, 7, "2026-04-01T07:01:00Z", 150, 92, 280),
        (activity_id, 8, "2026-04-01T07:01:10Z", 155, 94, 320),
    ]
    conn.executemany(
        """
        INSERT INTO activity_trackpoint (
            activity_id, seq, timestamp_utc, heart_rate_bpm, cadence, power_w
        ) VALUES (?, ?, ?, ?, ?, ?)
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


def test_refresh_persisted_activity_metrics_empty_activity_ids_falls_back_to_all(
    db_conn,
):
    _insert_activity(db_conn, 7, max_hr=181, training_stress_score=42.0)
    _insert_trackpoints(db_conn, 7)

    summary = queries.refresh_persisted_activity_metrics(
        db_conn,
        activity_ids=[],
        lthr=150,
    )

    assert summary["errors"] == 0
    assert summary["target_activities"] == 1
    assert summary["rows_upserted"] == 1
    assert summary["zones_updated"] == 1


def test_list_activities_needing_metrics_ignores_optional_trimp_tss_gaps(db_conn):
    _insert_activity(
        db_conn,
        8,
        activity_type="indoor_cardio",
        training_stress_score=None,
    )
    db_conn.execute(
        """
        INSERT INTO activity_metrics(
            activity_id, lthr_est_bpm, zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s,
            trimp, tss
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (8, 150, 60.0, 120.0, 180.0, 240.0, 0.0, None, None),
    )
    db_conn.commit()

    missing_rows = queries.list_activities_needing_metrics(db_conn)

    assert 8 not in missing_rows


def test_refresh_post_sync_tables_tops_off_remaining_missing_rows(db_conn):
    _insert_activity(db_conn, 21, max_hr=182, training_stress_score=63.5)
    _insert_trackpoints(db_conn, 21)
    _insert_activity(
        db_conn,
        22,
        start_time_gmt=None,
        moving_duration_seconds=None,
        average_speed=None,
        max_hr=None,
        training_stress_score=None,
        average_hr=None,
        activity_type=None,
        avg_power=None,
        max_power=None,
        norm_power=None,
        intensity_factor=None,
        avg_cadence=None,
        elevation_gain=None,
        elevation_loss=None,
        min_elevation=None,
        max_elevation=None,
        aerobic_training_effect=None,
        anaerobic_training_effect=None,
        min_temperature=None,
        max_temperature=None,
    )

    summary = refresh_post_sync_tables(db_conn, activity_ids=[21])
    diagnostics = queries.get_activity_metrics_diagnostics(db_conn)

    assert summary["errors"] == 0
    assert summary["target_activities"] == 2
    assert summary["rows_upserted"] == 2
    assert diagnostics["total_activities"] == 2
    assert diagnostics["total_metrics_rows"] == 2
    assert diagnostics["missing_metrics_count"] == 0


def test_refresh_persisted_activity_metrics_populates_scalar_fields(db_conn):
    _insert_activity(
        db_conn,
        11,
        elapsed_duration_seconds=5400,
        moving_duration_seconds=5000,
        average_speed=4.1,
        average_hr=152,
        max_hr=190,
        training_stress_score=None,
        avg_power=210,
        max_power=460,
        norm_power=235,
        intensity_factor=0.91,
        avg_cadence=88,
        elevation_gain=310,
        elevation_loss=305,
        min_elevation=14,
        max_elevation=102,
        aerobic_training_effect=3.4,
        anaerobic_training_effect=0.8,
        min_temperature=6,
        max_temperature=16,
    )
    _insert_trackpoints(db_conn, 11)

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[11], lthr=160
    )

    assert summary["errors"] == 0

    row = db_conn.execute(
        """
        SELECT moving_time_s, stopped_time_s, avg_moving_speed_mps,
               avg_hr_to_max_pct, np_w, if_val, tss, variability_index,
               avg_power_w, max_power_w, avg_cadence_spm,
               total_ascent_m, total_descent_m,
               min_altitude_m, max_altitude_m,
               training_effect_aerobic, training_effect_anaerobic,
               avg_temperature_c, min_temperature_c, max_temperature_c
        FROM activity_metrics
        WHERE activity_id = 11
        """
    ).fetchone()

    assert row is not None
    assert row[0] == pytest.approx(5000)
    assert row[1] == pytest.approx(400)
    assert row[2] == pytest.approx(4.1)
    assert row[3] == pytest.approx(80.0)
    assert row[4] == pytest.approx(235)
    assert row[5] == pytest.approx(0.91)
    assert row[6] is not None
    assert row[7] == pytest.approx(235 / 210, rel=1e-3)
    assert row[8] == pytest.approx(210)
    assert row[9] == pytest.approx(460)
    assert row[10] == pytest.approx(88)
    assert row[11] == pytest.approx(310)
    assert row[12] == pytest.approx(305)
    assert row[13] == pytest.approx(14)
    assert row[14] == pytest.approx(102)
    assert row[15] == pytest.approx(3.4)
    assert row[16] == pytest.approx(0.8)
    assert row[17] == pytest.approx(11)
    assert row[18] == pytest.approx(6)
    assert row[19] == pytest.approx(16)


def test_upsert_activity_metrics_preserves_existing_extended_columns(db_conn):
    _insert_activity(db_conn, 13)
    db_conn.execute(
        """
        INSERT INTO activity_metrics(activity_id, avg_power_w, training_effect_aerobic)
        VALUES (13, 222, 3.1)
        """
    )
    db_conn.commit()

    queries.upsert_activity_metrics(
        db_conn,
        13,
        3200,
        120,
        3.5,
        185,
        160,
        70.0,
        1.2,
        240,
        0.95,
        82.0,
        100,
        200,
        300,
        400,
        500,
    )

    row = db_conn.execute(
        "SELECT avg_power_w, training_effect_aerobic, tss FROM activity_metrics WHERE activity_id = 13"
    ).fetchone()

    assert row is not None
    assert row[0] == pytest.approx(222)
    assert row[1] == pytest.approx(3.1)
    assert row[2] == pytest.approx(82.0)


def test_get_effective_ftp_estimates_from_power_activities(db_conn):
    _insert_activity(
        db_conn,
        21,
        activity_type="cycling",
        elapsed_duration_seconds=3600,
        moving_duration_seconds=3500,
        avg_power=180,
        max_power=320,
        norm_power=210,
        intensity_factor=None,
        training_stress_score=None,
    )
    _insert_power_trackpoints(db_conn, 21)

    ftp = queries.get_effective_ftp(db_conn)

    assert ftp == 200
    profile = queries.get_athlete_profile(db_conn)
    assert profile is not None
    assert profile["ftp_calc"] == 200


def test_refresh_persisted_activity_metrics_populates_power_zones_with_estimated_ftp(
    db_conn,
):
    _insert_activity(
        db_conn,
        23,
        activity_type="cycling",
        elapsed_duration_seconds=3600,
        moving_duration_seconds=3400,
        avg_power=180,
        max_power=320,
        norm_power=210,
        intensity_factor=None,
        training_stress_score=None,
    )
    _insert_power_trackpoints(db_conn, 23)

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[23], lthr=160
    )

    assert summary["errors"] == 0

    row = db_conn.execute(
        """
        SELECT power_zone_1_s, power_zone_2_s, power_zone_3_s, power_zone_4_s,
               power_zone_5_s, power_zone_6_s, power_zone_7_s, peak_power_5s_w
        FROM activity_metrics
        WHERE activity_id = 23
        """
    ).fetchone()

    assert row is not None
    assert row[0] == pytest.approx(10.0, abs=0.2)
    assert row[1] == pytest.approx(20.0, abs=0.2)
    assert row[2] == pytest.approx(10.0, abs=0.2)
    assert row[3] == pytest.approx(10.0, abs=0.2)
    assert row[4] == pytest.approx(10.0, abs=0.2)
    assert row[5] == pytest.approx(10.0, abs=0.2)
    assert row[6] == pytest.approx(0.0, abs=0.2)
    assert sum(float(v or 0) for v in row[:7]) == pytest.approx(70.0, abs=0.5)
    assert row[7] > 200.0


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
