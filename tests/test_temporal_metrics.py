from __future__ import annotations

import pytest

from garmin_data_hub.analytics.temporal_metrics import (
    POWER_PEAK_DURATIONS_SECONDS,
    TemporalSample,
    build_temporal_intervals,
    calculate_temporal_metrics,
)
from garmin_data_hub.db import queries
from garmin_data_hub.ingest import writer as ingest_writer
from garmin_data_hub.ingest.writer import calculate_aerobic_decoupling


def sample(
    timestamp_s: float,
    *,
    power: float | None = None,
    speed: float | None = None,
    hr: float | None = None,
    seq: int = 0,
) -> TemporalSample:
    return TemporalSample(timestamp_s, speed, hr, power, seq)


def peak(samples: list[TemporalSample], duration_s: int = 5) -> float | None:
    return calculate_temporal_metrics(samples).power_peaks_w[duration_s]


def test_regular_one_hz_constant_power_has_time_weighted_peak():
    samples = [sample(second, power=250) for second in range(11)]
    assert peak(samples) == pytest.approx(250)


def test_half_second_audit_signal_has_five_second_peak_of_300_watts():
    powers = [100] * 5 + [500] * 5 + [100]
    samples = [sample(index / 2, power=power) for index, power in enumerate(powers)]
    assert peak(samples) == pytest.approx(300)


def test_equivalent_power_signal_is_sampling_density_independent():
    sparse = [sample(0, power=100), sample(2.5, power=500), sample(5, power=100)]
    dense = [
        sample(timestamp, power=100 if timestamp < 2.5 else 500)
        for timestamp in [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5]
    ]
    assert peak(sparse) == pytest.approx(peak(dense)) == pytest.approx(300)


def test_measured_zero_watts_are_included():
    assert peak([sample(0, power=0), sample(5, power=999)]) == pytest.approx(0)


def test_null_power_creates_unsupported_coverage():
    samples = [
        sample(0, power=100),
        sample(2, power=None),
        sample(7, power=100),
        sample(9, power=100),
    ]
    assert peak(samples) is None


def test_complete_window_adjacent_to_null_power_remains_eligible():
    samples = [
        sample(0, power=None),
        sample(1, power=200),
        sample(6, power=None),
    ]
    assert peak(samples) == pytest.approx(200)


def test_activity_shorter_than_peak_duration_is_ineligible():
    assert peak([sample(0, power=500), sample(4.999, power=500)]) is None


def test_exactly_requested_power_support_is_eligible():
    assert peak([sample(0, power=175), sample(5, power=None)]) == pytest.approx(175)


def test_partial_high_power_prefix_cannot_qualify_as_complete_window():
    samples = [
        sample(0, power=1000),
        sample(2, power=None),
        sample(3, power=100),
        sample(8, power=None),
    ]
    assert peak(samples) == pytest.approx(100)


def test_gap_longer_than_30_seconds_cannot_be_bridged_by_power_window():
    samples = [
        sample(0, power=200),
        sample(3, power=200),
        sample(34, power=200),
        sample(37, power=200),
    ]
    assert peak(samples) is None


def test_exactly_30_second_interval_is_supported():
    assert peak([sample(0, power=210), sample(30, power=None)], 30) == pytest.approx(
        210
    )


def test_power_window_boundary_inside_segment_is_integrated_correctly():
    samples = [
        sample(0, power=0),
        sample(3, power=500),
        sample(10, power=0),
    ]
    assert peak(samples) == pytest.approx(500)


def test_fractional_timestamps_are_preserved():
    intervals = build_temporal_intervals(
        [sample(0.25, power=100), sample(2.75, power=300), sample(5.25, power=0)]
    )
    assert [interval.duration_s for interval in intervals] == pytest.approx([2.5, 2.5])
    assert peak(
        [sample(0.25, power=100), sample(2.75, power=300), sample(5.25, power=0)]
    ) == pytest.approx(200)


def test_duplicate_timestamps_use_highest_sequence_observation():
    samples = [
        sample(0, power=100, seq=1),
        sample(0, power=200, seq=2),
        sample(5, power=None, seq=3),
    ]
    assert peak(samples) == pytest.approx(200)


def test_out_of_order_sequences_cannot_create_negative_duration():
    samples = [
        sample(5, power=200, seq=1),
        sample(0, power=200, seq=2),
        sample(10, power=None, seq=3),
    ]
    assert peak(samples) == pytest.approx(200)


def test_no_terminal_power_duration_is_invented():
    assert peak([sample(0, power=100), sample(4, power=1000)]) is None


def test_all_persisted_peak_durations_share_exact_elapsed_semantics():
    samples = [sample(second, power=150) for second in range(0, 1201, 30)]
    result = calculate_temporal_metrics(samples)
    assert result.power_peaks_w == {
        duration: pytest.approx(150) for duration in POWER_PEAK_DURATIONS_SECONDS
    }


def test_all_peak_fields_are_persisted_from_the_same_temporal_algorithm(db_conn):
    db_conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, activity_type
        ) VALUES (900, '2026-04-01T06:00:00Z', 1200, 1200, 'cycling')
        """
    )
    db_conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, power_w
        ) VALUES (900, ?, ?, 150)
        """,
        [
            (index + 1, f"2026-04-01T06:{second // 60:02d}:{second % 60:02d}Z")
            for index, second in enumerate(range(0, 1201, 30))
        ],
    )
    db_conn.commit()

    assert queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[900], lthr=160
    )["errors"] == 0
    row = db_conn.execute(
        """
        SELECT peak_power_5s_w, peak_power_30s_w, peak_power_60s_w,
               peak_power_300s_w, peak_power_1200s_w
        FROM activity_metrics WHERE activity_id=900
        """
    ).fetchone()
    assert tuple(row) == pytest.approx((150, 150, 150, 150, 150))


def test_irregular_decoupling_fixture_time_weights_to_25_percent():
    samples = [
        sample(0, speed=4, hr=100),
        sample(1, speed=2, hr=100),
        sample(9, speed=1, hr=100),
        sample(10, speed=1, hr=100),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.aerobic_decoupling_pct == pytest.approx(25)
    assert result.pace_decoupling_pct == pytest.approx(25)


def test_interval_crossing_midpoint_is_split_between_halves():
    result = calculate_temporal_metrics(
        [sample(0, speed=4, hr=100), sample(8, speed=2, hr=100), sample(10)]
    )
    assert result.pace_decoupling_pct == pytest.approx(20)


def test_decoupling_is_sampling_density_independent():
    sparse = [
        sample(0, speed=4, hr=100),
        sample(5, speed=2, hr=100),
        sample(10),
    ]
    dense = [
        sample(second, speed=4 if second < 5 else 2, hr=100)
        for second in range(11)
    ]
    assert calculate_temporal_metrics(sparse).pace_decoupling_pct == pytest.approx(
        calculate_temporal_metrics(dense).pace_decoupling_pct
    )


def test_intermittent_power_never_mixes_watts_with_speed():
    samples = [
        sample(0, power=200, speed=2, hr=100),
        sample(1, power=None, speed=2, hr=100),
        sample(2, power=200, speed=1, hr=100),
        sample(3, power=None, speed=1, hr=100),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.aerobic_workload_signal == "speed"
    assert result.paired_power_coverage == pytest.approx(2 / 3)
    assert result.aerobic_decoupling_pct == pytest.approx(33.33)
    assert result.aerobic_decoupling_pct == result.pace_decoupling_pct


def test_complete_power_coverage_uses_power_for_aerobic_decoupling():
    samples = [
        sample(0, power=200, speed=3, hr=100),
        sample(5, power=100, speed=3, hr=100),
        sample(10),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.aerobic_workload_signal == "power"
    assert result.aerobic_decoupling_pct == pytest.approx(50)
    assert result.pace_decoupling_pct == pytest.approx(0)


def coverage_fixture(power_coverage_s: float) -> list[TemporalSample]:
    return [
        sample(0, power=200, speed=4, hr=100),
        sample(25, power=200, speed=4, hr=100),
        sample(50, power=100, speed=2, hr=100),
        sample(75, power=100, speed=2, hr=100),
        sample(power_coverage_s, power=None, speed=2, hr=100),
        sample(100, power=None, speed=2, hr=100),
    ]


def test_exactly_95_percent_time_coverage_selects_power():
    result = calculate_temporal_metrics(coverage_fixture(95))
    assert result.paired_power_coverage == pytest.approx(0.95)
    assert result.aerobic_workload_signal == "power"


def test_just_below_95_percent_time_coverage_selects_speed():
    result = calculate_temporal_metrics(coverage_fixture(94.999))
    assert result.paired_power_coverage == pytest.approx(0.94999)
    assert result.aerobic_workload_signal == "speed"


def test_power_coverage_threshold_is_time_based_not_row_based():
    samples = [sample(index / 100, power=None, speed=4, hr=100) for index in range(100)]
    samples.extend(
        [
            sample(1, power=200, speed=4, hr=100),
            sample(25, power=200, speed=4, hr=100),
            sample(50, power=100, speed=2, hr=100),
            sample(75, power=100, speed=2, hr=100),
            sample(100, power=None, speed=2, hr=100),
        ]
    )
    result = calculate_temporal_metrics(samples)
    assert result.paired_power_coverage == pytest.approx(0.99)
    assert result.aerobic_workload_signal == "power"


def test_null_power_is_missing_but_zero_power_is_measured():
    missing = calculate_temporal_metrics(
        [sample(0, power=None, speed=2, hr=100), sample(10)]
    )
    zero = calculate_temporal_metrics(
        [sample(0, power=0, speed=2, hr=100), sample(10)]
    )
    assert missing.paired_power_coverage == pytest.approx(0)
    assert missing.aerobic_workload_signal == "speed"
    assert zero.paired_power_coverage == pytest.approx(1)
    assert zero.aerobic_workload_signal == "power"


def test_null_speed_is_missing_and_can_leave_insufficient_support():
    samples = [
        sample(0, speed=2, hr=100),
        sample(5, speed=None, hr=100),
        sample(10),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct is None


def test_zero_speed_is_measured_data():
    samples = [
        sample(0, speed=2, hr=100),
        sample(5, speed=0, hr=100),
        sample(10),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct == pytest.approx(100)


def test_workload_and_hr_are_averaged_over_identical_paired_support():
    samples = [
        sample(0, speed=4, hr=100),
        sample(2, speed=None, hr=200),
        sample(5, speed=2, hr=100),
        sample(10),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct == pytest.approx(50)


def test_missing_hr_excludes_the_same_interval_from_workload_and_coverage():
    samples = [
        sample(0, power=200, speed=4, hr=100),
        sample(5, power=None, speed=100, hr=None),
        sample(7, power=100, speed=2, hr=100),
        sample(10),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.paired_power_coverage == pytest.approx(1)
    assert result.aerobic_workload_signal == "power"
    assert result.aerobic_decoupling_pct == pytest.approx(50)
    assert result.pace_decoupling_pct == pytest.approx(50)


def test_power_present_while_hr_is_missing_does_not_enter_coverage_or_pairing():
    samples = [
        sample(0, power=200, speed=4, hr=100),
        sample(5, power=999, speed=100, hr=None),
        sample(10, power=100, speed=2, hr=100),
        sample(15),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.paired_power_coverage == pytest.approx(1)
    assert result.aerobic_workload_signal == "power"
    assert result.aerobic_decoupling_pct == pytest.approx(50)


def test_sparse_hr_islands_define_power_coverage_domain():
    samples = [
        sample(0, power=200, speed=4, hr=100),
        sample(1, power=None, speed=None, hr=None),
        sample(26, power=None, speed=None, hr=None),
        sample(51, power=None, speed=None, hr=None),
        sample(76, power=None, speed=None, hr=None),
        sample(99, power=100, speed=2, hr=100),
        sample(100),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.paired_power_coverage == pytest.approx(1)
    assert result.aerobic_workload_signal == "power"
    assert result.aerobic_decoupling_pct == pytest.approx(50)


def test_broken_gap_contributes_to_neither_side_of_power_coverage():
    samples = [
        sample(0, power=200, speed=4, hr=100),
        sample(10, power=200, speed=4, hr=100),
        sample(50, power=None, speed=2, hr=100),
        sample(60),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.paired_power_coverage == pytest.approx(0.5)
    assert result.aerobic_workload_signal == "speed"


def test_gap_longer_than_30_seconds_is_not_carried_into_decoupling():
    samples = [
        sample(0, speed=4, hr=100),
        sample(10, speed=4, hr=100),
        sample(41, speed=2, hr=100),
        sample(51),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct == pytest.approx(50)


def test_exactly_30_second_gap_is_eligible_for_decoupling():
    samples = [
        sample(0, speed=4, hr=100),
        sample(30, speed=2, hr=100),
        sample(60),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct == pytest.approx(50)


def test_multiple_broken_segments_follow_wall_clock_halves():
    samples = [
        sample(0, speed=4, hr=100),
        sample(10, speed=4, hr=100),
        sample(50, speed=2, hr=100),
        sample(60, speed=2, hr=100),
        sample(100, speed=1, hr=100),
        sample(110),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct == pytest.approx(60)


def test_asymmetric_broken_segments_keep_wall_clock_midpoint_semantics():
    samples = [
        sample(0, speed=4, hr=100),
        sample(10, speed=4, hr=100),
        sample(41, speed=2, hr=100),
        sample(46),
    ]
    assert calculate_temporal_metrics(samples).pace_decoupling_pct == pytest.approx(50)


def test_missing_one_half_returns_null_decoupling():
    samples = [
        sample(0, speed=4, hr=100),
        sample(5, speed=None, hr=100),
        sample(10),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.aerobic_decoupling_pct is None
    assert result.pace_decoupling_pct is None


def test_pace_decoupling_is_always_speed_based():
    samples = [
        sample(0, power=200, speed=4, hr=100),
        sample(5, power=200, speed=2, hr=100),
        sample(10),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.aerobic_decoupling_pct == pytest.approx(0)
    assert result.pace_decoupling_pct == pytest.approx(50)


def test_hr_drift_preserves_positive_second_half_orientation():
    samples = [
        sample(0, speed=2, hr=100),
        sample(5, speed=2, hr=110),
        sample(10),
    ]
    assert calculate_temporal_metrics(samples).hr_drift_pct == pytest.approx(10)


def test_hr_drift_is_sampling_density_independent():
    sparse = [sample(0, hr=100), sample(5, hr=110), sample(10)]
    dense = [
        sample(second, hr=100 if second < 5 else 110)
        for second in range(11)
    ]
    assert calculate_temporal_metrics(sparse).hr_drift_pct == pytest.approx(
        calculate_temporal_metrics(dense).hr_drift_pct
    ) == pytest.approx(10)


def test_hr_drift_splits_an_irregular_interval_at_the_midpoint():
    samples = [sample(0, hr=100), sample(8, hr=200), sample(10)]
    assert calculate_temporal_metrics(samples).hr_drift_pct == pytest.approx(40)


def test_zero_first_half_workload_returns_null_without_nonfinite_result():
    samples = [
        sample(0, speed=0, hr=100),
        sample(5, speed=2, hr=100),
        sample(10),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.aerobic_decoupling_pct is None
    assert result.pace_decoupling_pct is None


def test_no_terminal_interval_is_invented_for_decoupling_or_hr_drift():
    samples = [
        sample(0, speed=2, hr=100),
        sample(5, speed=0, hr=200),
    ]
    result = calculate_temporal_metrics(samples)
    assert result.pace_decoupling_pct == pytest.approx(0)
    assert result.hr_drift_pct == pytest.approx(0)


def test_single_point_stream_has_no_temporal_metrics():
    result = calculate_temporal_metrics([sample(0, power=200, speed=3, hr=140)])
    assert all(value is None for value in result.power_peaks_w.values())
    assert result.aerobic_decoupling_pct is None
    assert result.pace_decoupling_pct is None
    assert result.hr_drift_pct is None


def test_legacy_writer_delegates_to_canonical_temporal_semantics(db_conn):
    db_conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, activity_type
        ) VALUES (901, '2026-04-01T06:00:00Z', 3, 3, 'cycling')
        """
    )
    db_conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps,
            heart_rate_bpm, power_w
        ) VALUES (901, ?, ?, ?, 100, ?)
        """,
        [
            (1, "2026-04-01T06:00:00Z", 2, 200),
            (2, "2026-04-01T06:00:01Z", 2, None),
            (3, "2026-04-01T06:00:02Z", 1, 200),
            (4, "2026-04-01T06:00:03Z", 1, None),
        ],
    )
    db_conn.commit()

    value = calculate_aerobic_decoupling(db_conn, 901)
    assert isinstance(value, float)
    assert value == pytest.approx(33.33)

    db_conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, activity_type
        ) VALUES (902, '2026-04-01T07:00:00Z', 0, 0, 'cycling')
        """
    )
    db_conn.execute(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps,
            heart_rate_bpm, power_w
        ) VALUES (902, 1, '2026-04-01T07:00:00Z', 2, 100, 200)
        """
    )
    assert calculate_aerobic_decoupling(db_conn, 902) is None


def test_legacy_persistence_leaves_unversioned_temporal_value_suppressed(
    db_conn, monkeypatch
):
    db_conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, activity_type
        ) VALUES (903, '2026-04-01T08:00:00Z', 3, 3, 'cycling')
        """
    )
    db_conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps,
            heart_rate_bpm, power_w
        ) VALUES (903, ?, ?, ?, 100, ?)
        """,
        [
            (1, "2026-04-01T08:00:00Z", 2, 200),
            (2, "2026-04-01T08:00:01Z", 2, None),
            (3, "2026-04-01T08:00:02Z", 1, 200),
            (4, "2026-04-01T08:00:03Z", 1, None),
        ],
    )
    monkeypatch.setattr(
        ingest_writer.db_queries,
        "get_session_first_row",
        lambda _conn, _activity_id: (3, 3, 1.5, None, None, None, 100, None),
        raising=False,
    )

    ingest_writer.calculate_activity_metrics(db_conn, 903)

    stored = db_conn.execute(
        """
        SELECT refresh_provenance_version, aerobic_decoupling_pct
        FROM activity_metrics WHERE activity_id=903
        """
    ).fetchone()
    assert tuple(stored) == (None, pytest.approx(33.33))
    projection = queries.current_temporal_metric_projection_sql(
        "am", ("aerobic_decoupling_pct",)
    )
    exposed = db_conn.execute(
        f"SELECT {projection} FROM activity_metrics am WHERE activity_id=903"
    ).fetchone()[0]
    assert exposed is None
