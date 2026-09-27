from __future__ import annotations

import sqlite3

import pytest

from garmin_data_hub.db import queries


def _insert_activity(
    conn,
    activity_id: int,
    *,
    average_hr: int | None = 145,
    max_hr: int | None = 180,
    avg_power: float | None = None,
    max_power: float | None = None,
    norm_power: float | None = None,
    intensity_factor: float | None = None,
    training_stress_score: float | None = None,
    avg_cadence: float | None = None,
    min_temperature: float | None = None,
    max_temperature: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, average_speed, average_hr, max_hr,
            activity_type, avg_power, max_power, norm_power, intensity_factor,
            training_stress_score, avg_cadence, min_temperature, max_temperature
        ) VALUES (?, '2026-04-01T06:00:00Z', 3600, 3300, 3.0, ?, ?,
                  'cycling', ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            activity_id,
            average_hr,
            max_hr,
            avg_power,
            max_power,
            norm_power,
            intensity_factor,
            training_stress_score,
            avg_cadence,
            min_temperature,
            max_temperature,
        ),
    )
    conn.commit()


def _insert_trackpoints(conn, activity_id: int) -> None:
    conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps,
            heart_rate_bpm, cadence, power_w, temperature_c
        ) VALUES (?, ?, ?, 3.0, ?, ?, ?, ?)
        """,
        [
            (activity_id, 1, "2026-04-01T06:00:00Z", 120, 0, 0, 0.0),
            (activity_id, 2, "2026-04-01T06:00:10Z", 150, 90, 300, 20.0),
            (activity_id, 3, "2026-04-01T06:00:20Z", 170, 95, 350, 22.0),
        ],
    )
    conn.commit()


def _set_thresholds(conn, *, hrmax=190, lthr=160, ftp=250) -> None:
    queries.set_override_metrics(conn, hrmax=hrmax, lthr=lthr, ftp=ftp)


def test_lthr_change_marks_refreshed_metrics_stale(db_conn):
    _insert_activity(db_conn, 201)
    _insert_trackpoints(db_conn, 201)
    _set_thresholds(db_conn, lthr=150)
    assert queries.refresh_persisted_activity_metrics(db_conn, [201])["errors"] == 0
    assert 201 not in queries.list_activities_needing_metrics(db_conn)

    _set_thresholds(db_conn, lthr=170)

    assert 201 in queries.list_activities_needing_metrics(db_conn)


def test_lthr_override_clear_uses_calculated_value_for_invalidation(db_conn):
    _insert_activity(db_conn, 202)
    _insert_trackpoints(db_conn, 202)
    queries.set_calculated_metrics(db_conn, hrmax=185, lthr=158)
    _set_thresholds(db_conn, lthr=170)
    assert queries.refresh_persisted_activity_metrics(db_conn, [202])["errors"] == 0

    queries.clear_override_metrics(db_conn)

    assert queries.get_effective_lthr(db_conn) == 158
    assert 202 in queries.list_activities_needing_metrics(db_conn)


def test_override_clear_with_unchanged_effective_lthr_does_not_invalidate(db_conn):
    _insert_activity(db_conn, 216)
    _insert_trackpoints(db_conn, 216)
    queries.set_calculated_metrics(db_conn, hrmax=185, lthr=160)
    queries.set_calculated_ftp(db_conn, ftp=250)
    _set_thresholds(db_conn, lthr=160)
    assert queries.refresh_persisted_activity_metrics(db_conn, [216])["errors"] == 0

    queries.clear_override_metrics(db_conn)

    assert queries.get_effective_lthr(db_conn) == 160
    assert 216 not in queries.list_activities_needing_metrics(db_conn)


def test_zero_overrides_use_positive_calculated_thresholds_consistently(db_conn):
    _insert_activity(db_conn, 233)
    queries.set_calculated_metrics(db_conn, hrmax=185, lthr=160)
    queries.set_calculated_ftp(db_conn, ftp=250)
    queries.set_override_metrics(db_conn, hrmax=0, lthr=0, ftp=0)

    metrics = queries.get_athlete_metrics(db_conn)
    assert metrics["hrmax_effective"] == 185
    assert metrics["lthr_effective"] == queries.get_effective_lthr(db_conn) == 160
    assert metrics["ftp_effective"] == queries.get_effective_ftp(db_conn) == 250
    assert queries.refresh_persisted_activity_metrics(db_conn, [233])["errors"] == 0
    provenance = db_conn.execute(
        """
        SELECT threshold_lthr_bpm, threshold_ftp_w
        FROM activity_metrics WHERE activity_id=233
        """
    ).fetchone()
    assert tuple(provenance) == (160, 250)
    assert 233 not in queries.list_activities_needing_metrics(db_conn)


def test_hrmax_recalculation_that_changes_lthr_invalidates_metrics(db_conn):
    _insert_activity(db_conn, 203)
    _insert_trackpoints(db_conn, 203)
    queries.set_calculated_metrics(db_conn, hrmax=180, lthr=155)
    assert queries.refresh_persisted_activity_metrics(db_conn, [203])["errors"] == 0

    queries.set_calculated_metrics(db_conn, hrmax=190, lthr=163)

    assert 203 in queries.list_activities_needing_metrics(db_conn)


def test_ftp_change_invalidates_power_zone_metrics(db_conn):
    _insert_activity(db_conn, 204, avg_power=220, max_power=410, norm_power=245)
    _insert_trackpoints(db_conn, 204)
    _set_thresholds(db_conn, ftp=250)
    assert queries.refresh_persisted_activity_metrics(db_conn, [204])["errors"] == 0

    _set_thresholds(db_conn, ftp=275)

    assert 204 in queries.list_activities_needing_metrics(db_conn)


def test_unchanged_threshold_values_do_not_invalidate_metrics(db_conn):
    _insert_activity(db_conn, 205)
    _insert_trackpoints(db_conn, 205)
    _set_thresholds(db_conn, hrmax=190, lthr=160, ftp=250)
    assert queries.refresh_persisted_activity_metrics(db_conn, [205])["errors"] == 0

    _set_thresholds(db_conn, hrmax=190, lthr=160, ftp=250)

    assert 205 not in queries.list_activities_needing_metrics(db_conn)


def test_failed_threshold_change_preserves_outer_transaction_and_freshness(db_conn):
    _insert_activity(db_conn, 206)
    _insert_trackpoints(db_conn, 206)
    _set_thresholds(db_conn, lthr=150)
    assert queries.refresh_persisted_activity_metrics(db_conn, [206])["errors"] == 0
    db_conn.execute(
        """
        CREATE TRIGGER fail_lthr_change
        BEFORE UPDATE ON athlete_profile
        WHEN NEW.lthr_override = 170
        BEGIN
            SELECT RAISE(ABORT, 'injected threshold failure');
        END
        """
    )
    db_conn.commit()
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('outer-work', 'true')"
    )

    with pytest.raises(sqlite3.IntegrityError, match="injected threshold failure"):
        queries.set_override_metrics(db_conn, hrmax=190, lthr=170, ftp=250)

    assert db_conn.in_transaction
    assert db_conn.execute(
        "SELECT value FROM app_settings WHERE key='outer-work'"
    ).fetchone()[0] == "true"
    assert queries.get_effective_lthr(db_conn) == 150
    assert 206 not in queries.list_activities_needing_metrics(db_conn)
    db_conn.rollback()


def test_successful_threshold_change_does_not_commit_caller_transaction(db_conn):
    _set_thresholds(db_conn, lthr=150)
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('outer-success', 'true')"
    )

    queries.set_override_metrics(db_conn, hrmax=190, lthr=170, ftp=250)

    assert db_conn.in_transaction
    assert queries.get_effective_lthr(db_conn) == 170
    db_conn.rollback()
    assert queries.get_effective_lthr(db_conn) == 150
    assert db_conn.execute(
        "SELECT value FROM app_settings WHERE key='outer-success'"
    ).fetchone() is None


def test_fallback_threshold_recalculation_does_not_commit_caller_transaction(
    db_conn,
):
    _insert_activity(db_conn, 218, max_hr=180)
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('outer-fallback', 'true')"
    )

    calculated_lthr = queries.get_effective_lthr(db_conn)

    assert calculated_lthr == int(round(180 * 0.86))
    assert db_conn.in_transaction
    db_conn.rollback()
    profile = queries.get_athlete_profile(db_conn)
    assert profile is not None and profile["lthr_calc"] is None
    assert db_conn.execute(
        "SELECT value FROM app_settings WHERE key='outer-fallback'"
    ).fetchone() is None


def test_legacy_metric_row_is_stale_when_known_lthr_differs(db_conn):
    _insert_activity(db_conn, 207)
    _insert_trackpoints(db_conn, 207)
    _set_thresholds(db_conn, lthr=170)
    db_conn.execute(
        """
        INSERT INTO activity_metrics(
            activity_id, lthr_est_bpm,
            zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s
        ) VALUES (207, 150, 10, 10, 0, 0, 0)
        """
    )
    db_conn.commit()

    assert 207 in queries.list_activities_needing_metrics(db_conn)


def test_refresh_changes_numeric_source_to_null(db_conn):
    _insert_activity(
        db_conn,
        208,
        avg_power=220,
        max_power=410,
        norm_power=245,
        avg_cadence=88,
    )
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [208])["errors"] == 0
    db_conn.execute(
        """
        UPDATE activity
        SET avg_power=NULL, max_power=NULL, norm_power=NULL, avg_cadence=NULL
        WHERE activity_id=208
        """
    )
    db_conn.commit()

    assert queries.refresh_persisted_activity_metrics(db_conn, [208])["errors"] == 0
    row = db_conn.execute(
        "SELECT avg_power_w, max_power_w, np_w, avg_cadence_spm FROM activity_metrics WHERE activity_id=208"
    ).fetchone()
    assert tuple(row) == (None, None, None, None)


def test_refresh_replaces_numeric_source_with_new_value(db_conn):
    _insert_activity(db_conn, 209, avg_power=220, max_power=410, norm_power=245)
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [209])["errors"] == 0
    db_conn.execute(
        "UPDATE activity SET avg_power=180, max_power=360, norm_power=205 WHERE activity_id=209"
    )
    db_conn.commit()

    assert queries.refresh_persisted_activity_metrics(db_conn, [209])["errors"] == 0
    row = db_conn.execute(
        "SELECT avg_power_w, max_power_w, np_w FROM activity_metrics WHERE activity_id=209"
    ).fetchone()
    assert tuple(row) == (180, 360, 205)


def test_trackpoint_metric_disappears_with_trackpoints(db_conn):
    _insert_activity(db_conn, 210)
    _insert_trackpoints(db_conn, 210)
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [210])["errors"] == 0
    before = db_conn.execute(
        "SELECT peak_power_5s_w, avg_temperature_c FROM activity_metrics WHERE activity_id=210"
    ).fetchone()
    assert before[0] is not None and before[1] is not None
    db_conn.execute("DELETE FROM activity_trackpoints WHERE activity_id=210")
    db_conn.commit()

    assert queries.refresh_persisted_activity_metrics(db_conn, [210])["errors"] == 0
    after = db_conn.execute(
        "SELECT peak_power_5s_w, avg_temperature_c FROM activity_metrics WHERE activity_id=210"
    ).fetchone()
    assert tuple(after) == (None, None)


def test_legitimate_zero_source_values_remain_zero(db_conn):
    _insert_activity(
        db_conn,
        211,
        avg_power=0,
        max_power=0,
        norm_power=0,
        intensity_factor=0,
        training_stress_score=0,
        avg_cadence=0,
        min_temperature=0,
        max_temperature=0,
    )
    _set_thresholds(db_conn)

    assert queries.refresh_persisted_activity_metrics(db_conn, [211])["errors"] == 0
    row = db_conn.execute(
        """
        SELECT avg_power_w, max_power_w, np_w, if_val, tss,
               avg_cadence_spm, avg_temperature_c
        FROM activity_metrics WHERE activity_id=211
        """
    ).fetchone()
    assert tuple(row) == (0, 0, 0, 0, 0, 0, 0)


def test_legitimate_zero_trackpoints_remain_zero(db_conn):
    _insert_activity(db_conn, 217)
    db_conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, cadence, power_w, temperature_c
        ) VALUES (217, ?, ?, 0, 0, 0)
        """,
        [
            (1, "2026-04-01T06:00:00Z"),
            (2, "2026-04-01T06:00:10Z"),
        ],
    )
    db_conn.commit()
    _set_thresholds(db_conn)

    assert queries.refresh_persisted_activity_metrics(db_conn, [217])["errors"] == 0
    row = db_conn.execute(
        """
        SELECT avg_power_w, max_power_w, avg_cadence_spm, avg_temperature_c,
               peak_power_5s_w, power_zone_1_s
        FROM activity_metrics WHERE activity_id=217
        """
    ).fetchone()
    assert tuple(row) == pytest.approx((0, 0, 0, 0, 0, 10), abs=0.2)


def test_missing_source_remains_distinct_from_measured_zero(db_conn):
    _insert_activity(db_conn, 212, avg_power=None, min_temperature=None)
    _insert_activity(db_conn, 213, avg_power=0, min_temperature=0, max_temperature=0)
    _set_thresholds(db_conn)

    assert queries.refresh_persisted_activity_metrics(db_conn, [212, 213])["errors"] == 0
    rows = db_conn.execute(
        """
        SELECT activity_id, avg_power_w, avg_temperature_c
        FROM activity_metrics WHERE activity_id IN (212, 213) ORDER BY activity_id
        """
    ).fetchall()
    assert tuple(rows[0]) == (212, None, None)
    assert tuple(rows[1]) == (213, 0, 0)


def test_failed_refresh_rolls_back_stale_value_clearing(db_conn):
    _insert_activity(db_conn, 214, avg_power=220, max_power=410, norm_power=245)
    _insert_trackpoints(db_conn, 214)
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [214])["errors"] == 0
    expected = db_conn.execute(
        "SELECT avg_power_w, peak_power_5s_w FROM activity_metrics WHERE activity_id=214"
    ).fetchone()
    db_conn.execute(
        "UPDATE activity SET avg_power=NULL, max_power=NULL, norm_power=NULL WHERE activity_id=214"
    )
    db_conn.execute("DELETE FROM activity_trackpoints WHERE activity_id=214")
    db_conn.execute(
        """
        CREATE TRIGGER fail_metric_refresh
        BEFORE UPDATE ON activity_metrics
        WHEN NEW.activity_id=214
        BEGIN
            SELECT RAISE(ABORT, 'injected refresh failure');
        END
        """
    )
    db_conn.commit()

    summary = queries.refresh_persisted_activity_metrics(db_conn, [214])

    assert summary["errors"] == 1
    actual = db_conn.execute(
        "SELECT avg_power_w, peak_power_5s_w FROM activity_metrics WHERE activity_id=214"
    ).fetchone()
    assert tuple(actual) == tuple(expected)


def test_refresh_does_not_clear_schema_only_unowned_columns(db_conn):
    _insert_activity(db_conn, 215)
    db_conn.execute(
        "INSERT INTO activity_metrics(activity_id, performance_condition_start) VALUES (215, 4.5)"
    )
    db_conn.commit()
    _set_thresholds(db_conn)

    assert queries.refresh_persisted_activity_metrics(db_conn, [215])["errors"] == 0
    value = db_conn.execute(
        "SELECT performance_condition_start FROM activity_metrics WHERE activity_id=215"
    ).fetchone()[0]
    assert value == pytest.approx(4.5)


@pytest.mark.parametrize(
    "stored_version",
    [
        0,
        queries.ACTIVITY_METRICS_PROVENANCE_VERSION - 1,
        queries.ACTIVITY_METRICS_PROVENANCE_VERSION + 1,
    ],
)
def test_noncurrent_provenance_versions_are_always_stale(db_conn, stored_version):
    _insert_activity(db_conn, 219)
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [219])["errors"] == 0
    db_conn.execute(
        "UPDATE activity_metrics SET refresh_provenance_version=? WHERE activity_id=219",
        (stored_version,),
    )
    db_conn.commit()

    assert 219 in queries.list_activities_needing_metrics(db_conn)


def test_freshness_query_handles_current_legacy_and_old_version_rows_together(
    db_conn,
):
    for activity_id in (230, 231, 232):
        _insert_activity(db_conn, activity_id, average_hr=None, max_hr=None)
    _set_thresholds(db_conn, lthr=160, ftp=250)
    assert queries.refresh_persisted_activity_metrics(
        db_conn, [230, 231, 232]
    )["errors"] == 0
    db_conn.execute(
        "UPDATE activity_metrics SET refresh_provenance_version=NULL "
        "WHERE activity_id=231"
    )
    db_conn.execute(
        "UPDATE activity_metrics SET refresh_provenance_version=0 "
        "WHERE activity_id=232"
    )
    db_conn.commit()

    stale_ids = queries.list_activities_needing_metrics(db_conn)

    assert 230 not in stale_ids
    assert 231 not in stale_ids
    assert 232 in stale_ids


@pytest.mark.parametrize(
    ("stored_lthr", "current_lthr"),
    [(None, 160), (160, None)],
)
def test_current_provenance_detects_lthr_null_transitions(
    db_conn, stored_lthr, current_lthr
):
    _insert_activity(db_conn, 220, average_hr=None, max_hr=None)
    queries.set_calculated_metrics(db_conn, hrmax=None, lthr=current_lthr)
    db_conn.execute(
        """
        INSERT INTO activity_metrics(
            activity_id, refresh_provenance_version, threshold_lthr_bpm,
            threshold_ftp_w, threshold_resting_hr_bpm
        ) VALUES (220, ?, ?, NULL, 60)
        """,
        (queries.ACTIVITY_METRICS_PROVENANCE_VERSION, stored_lthr),
    )
    db_conn.commit()

    assert 220 in queries.list_activities_needing_metrics(db_conn)


def test_calculated_lthr_to_override_transition_is_stale(db_conn):
    _insert_activity(db_conn, 221)
    queries.set_calculated_metrics(db_conn, hrmax=185, lthr=158)
    assert queries.refresh_persisted_activity_metrics(db_conn, [221])["errors"] == 0

    queries.set_override_metrics(db_conn, hrmax=190, lthr=170, ftp=None)

    assert 221 in queries.list_activities_needing_metrics(db_conn)


@pytest.mark.parametrize(
    ("stored_ftp", "current_ftp"),
    [(None, 250), (250, None)],
)
def test_current_provenance_detects_ftp_null_transitions(
    db_conn, stored_ftp, current_ftp
):
    _insert_activity(db_conn, 222, average_hr=None, max_hr=None)
    queries.set_calculated_ftp(db_conn, current_ftp)
    db_conn.execute(
        """
        INSERT INTO activity_metrics(
            activity_id, refresh_provenance_version, threshold_lthr_bpm,
            threshold_ftp_w, threshold_resting_hr_bpm
        ) VALUES (222, ?, NULL, ?, 60)
        """,
        (queries.ACTIVITY_METRICS_PROVENANCE_VERSION, stored_ftp),
    )
    db_conn.commit()

    assert 222 in queries.list_activities_needing_metrics(db_conn)


def test_resting_hr_change_invalidates_current_metrics(db_conn):
    _insert_activity(db_conn, 223)
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [223])["errors"] == 0

    db_conn.execute("UPDATE athlete_profile SET resting_hr=48 WHERE profile_id=1")
    db_conn.commit()

    assert 223 in queries.list_activities_needing_metrics(db_conn)


def test_legacy_ftp_if_without_power_zones_is_stale_when_ftp_disappears(db_conn):
    _insert_activity(db_conn, 224, norm_power=225)
    queries.set_calculated_ftp(db_conn, None)
    db_conn.execute(
        "INSERT INTO activity_metrics(activity_id, if_val) VALUES (224, 0.9)"
    )
    db_conn.commit()

    assert 224 in queries.list_activities_needing_metrics(db_conn)


def test_legacy_hr_load_is_stale_when_resting_hr_differs_from_default(db_conn):
    _insert_activity(db_conn, 225)
    db_conn.execute("UPDATE athlete_profile SET resting_hr=48 WHERE profile_id=1")
    db_conn.execute(
        "INSERT INTO activity_metrics(activity_id, trimp, if_val, tss) "
        "VALUES (225, 75, 0.8, 64)"
    )
    db_conn.commit()

    assert 225 in queries.list_activities_needing_metrics(db_conn)


def test_compatible_legacy_metrics_remain_readable_and_do_not_need_refresh(db_conn):
    _insert_activity(db_conn, 234, average_hr=None, max_hr=None)
    queries.set_calculated_metrics(db_conn, hrmax=185, lthr=160)
    db_conn.execute(
        """
        INSERT INTO activity_metrics(
            activity_id, lthr_est_bpm, zone_1_s, trimp, tss
        ) VALUES (234, 160, 120, 75, 64)
        """
    )
    db_conn.commit()

    assert 234 not in queries.list_activities_needing_metrics(db_conn)
    assert tuple(queries.get_activity_metrics(db_conn, 234)) == (120, 75, 64)


def test_regular_metric_readers_hide_threshold_stale_values(db_conn):
    db_conn.execute("ALTER TABLE activity ADD COLUMN distance_meters REAL")
    _insert_activity(db_conn, 226, training_stress_score=42)
    _insert_trackpoints(db_conn, 226)
    _set_thresholds(db_conn, lthr=150, ftp=250)
    assert queries.refresh_persisted_activity_metrics(db_conn, [226])["errors"] == 0

    _set_thresholds(db_conn, lthr=170, ftp=275)

    frame = queries.get_activities_dataframe(
        db_conn, "2026-01-01T00:00:00Z", use_temp_zone_metrics=False
    )
    row = frame.loc[frame.index[frame["sport"] == "cycling"][0]]
    assert sum(row[f"zone_{index}_s"] for index in range(1, 6)) == 0
    assert sum(row[f"power_zone_{index}_s"] for index in range(1, 8)) == 0
    assert row["tss"] == pytest.approx(42)
    assert row["trimp"] is None
    assert row["hr_drift_pct"] == pytest.approx(25)
    assert row["peak_power_5s_w"] == pytest.approx(300)

    detail_metrics = queries.get_activity_metrics(db_conn, 226)
    assert tuple(detail_metrics) == (None, None, None)


def test_trackpoint_disappearance_uses_current_activity_summary_fallback(db_conn):
    _insert_activity(
        db_conn,
        227,
        avg_power=220,
        max_power=410,
        min_temperature=10,
        max_temperature=20,
    )
    _insert_trackpoints(db_conn, 227)
    _set_thresholds(db_conn)
    assert queries.refresh_persisted_activity_metrics(db_conn, [227])["errors"] == 0
    db_conn.execute("DELETE FROM activity_trackpoints WHERE activity_id=227")
    db_conn.commit()

    assert queries.refresh_persisted_activity_metrics(db_conn, [227])["errors"] == 0
    row = db_conn.execute(
        """
        SELECT avg_power_w, max_power_w, avg_temperature_c
        FROM activity_metrics WHERE activity_id=227
        """
    ).fetchone()
    assert tuple(row) == pytest.approx((220, 410, 15))


def test_failed_repopulation_rolls_back_metrics_and_provenance(db_conn):
    _insert_activity(db_conn, 228, avg_power=220, max_power=410, norm_power=245)
    _set_thresholds(db_conn, lthr=150)
    assert queries.refresh_persisted_activity_metrics(db_conn, [228])["errors"] == 0
    expected = db_conn.execute(
        """
        SELECT avg_power_w, refresh_provenance_version, threshold_lthr_bpm
        FROM activity_metrics WHERE activity_id=228
        """
    ).fetchone()
    db_conn.execute("UPDATE activity SET avg_power=180 WHERE activity_id=228")
    _set_thresholds(db_conn, lthr=170)
    db_conn.execute(
        """
        CREATE TRIGGER fail_metric_repopulation
        BEFORE UPDATE ON activity_metrics
        WHEN NEW.activity_id=228 AND NEW.avg_power_w=180
        BEGIN
            SELECT RAISE(ABORT, 'injected repopulation failure');
        END
        """
    )
    db_conn.commit()

    summary = queries.refresh_persisted_activity_metrics(db_conn, [228])

    assert summary["errors"] == 1
    actual = db_conn.execute(
        """
        SELECT avg_power_w, refresh_provenance_version, threshold_lthr_bpm
        FROM activity_metrics WHERE activity_id=228
        """
    ).fetchone()
    assert tuple(actual) == tuple(expected)


def test_zero_derived_variability_and_efficiency_are_preserved(db_conn):
    _insert_activity(db_conn, 229, avg_power=200, norm_power=0)
    _set_thresholds(db_conn)

    assert queries.refresh_persisted_activity_metrics(db_conn, [229])["errors"] == 0
    row = db_conn.execute(
        """
        SELECT variability_index, efficiency_factor, if_val
        FROM activity_metrics WHERE activity_id=229
        """
    ).fetchone()
    assert tuple(row) == (0, 0, 0)
