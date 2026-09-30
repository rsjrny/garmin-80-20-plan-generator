from __future__ import annotations

from decimal import Decimal

import pytest

from garmin_data_hub.analytics.temporal_metrics import TemporalSample
from garmin_data_hub.plan_methodology.compliance import classify_missing_data
from garmin_data_hub.plan_methodology.domain import DomainError
from garmin_data_hub.plan_methodology.fitzgerald_compliance import evaluate as evaluate_fitzgerald
from garmin_data_hub.plan_methodology.maffetone_compliance import (
    evaluate as evaluate_maffetone,
    evaluate_test_observation,
    evaluate_test_trend,
)
from garmin_data_hub.plan_methodology.fitzgerald_policy import aggregate_runtime_results


def _sample(second: float, *, speed=None, hr=None) -> TemporalSample:
    return TemporalSample(second, speed_mps=speed, heart_rate_bpm=hr)


def test_shared_evidence_states_never_invent_noncompliance():
    for observations, quality in (
        (None, "UNAVAILABLE"),
        ("INADEQUATE_COVERAGE", "INSUFFICIENT"),
        ("INCOMPLETE_COVERAGE", "PARTIAL"),
    ):
        result = classify_missing_data(observations=observations, quality=quality)
        assert result == {
            "status": "UNKNOWN",
            "quality": quality,
            "noncompliant": False,
        }


def test_fitzgerald_uses_left_held_time_all_zones_and_partial_evidence():
    # A 31 second gap is unsupported; missing speed in a supported interval is
    # also unsupported. The final observation has no implied duration.
    samples = [
        _sample(0, speed=2.0),
        _sample(10, speed=2.5),
        _sample(20, speed=2.7),
        _sample(30, speed=2.9),
        _sample(40, speed=3.0),
        _sample(50, speed=3.1),
        _sample(60, speed=3.5),
        _sample(70, speed=None),
        _sample(80, speed=3.0),
        _sample(111, speed=3.0),
    ]
    result = evaluate_fitzgerald(
        prescription={
            "methodology_id": "FITZGERALD_80_20_RUNNING_V1",
            "primary_metric": "SPEED",
            "native_target": "F80.ZONE_2",
            "threshold": "3.0",
        },
        samples=samples,
    )

    assert result["quality"] == "PARTIAL"
    assert result["coverage"]["metric_supported_seconds"] == Decimal("70")
    assert result["coverage"]["unsupported_seconds"] == Decimal("41")
    assert result["native_zone_seconds"] == {
        "ZONE_1": Decimal("10"),
        "ZONE_2": Decimal("10"),
        "ZONE_X": Decimal("10"),
        "ZONE_3": Decimal("10"),
        "ZONE_Y": Decimal("10"),
        "ZONE_4": Decimal("10"),
        "ZONE_5": Decimal("10"),
    }
    assert result["category_seconds"] == {
        "LOW": Decimal("20"),
        "MODERATE": Decimal("20"),
        "HIGH": Decimal("30"),
    }
    assert result["status"] == "UNKNOWN"
    assert result["composite_score"] is None


def test_maffetone_boundaries_excursions_gaps_and_invalid_hr():
    samples = [
        _sample(0, hr=134),
        _sample(10, hr=135),
        _sample(20, hr=145),
        _sample(30, hr=146),
        _sample(40, hr=150),
        _sample(50, hr=None),
        _sample(60, hr=146),
        _sample(91, hr=146),
        _sample(101, hr=221),
        _sample(111, hr=145),
    ]
    result = evaluate_maffetone(
        ceiling_bpm=145,
        lower_bpm=135,
        samples=samples,
    )

    assert result["quality"] == "PARTIAL"
    assert result["below_seconds"] == Decimal("10")
    assert result["in_range_seconds"] == Decimal("20")
    assert result["above_seconds"] == Decimal("30")
    assert [item["duration_seconds"] for item in result["excursions"]] == [
        Decimal("20"),
        Decimal("10"),
    ]
    assert result["excursion_summary"]["count"] == 2
    assert result["noncompliant"] is False


def test_objective_maf_trend_is_unrated_and_has_no_authorization_effect():
    state = {"higher_intensity_authorized": False}
    result = evaluate_test_trend(
        tests=[
            {"date": "2026-08-01", "pace_seconds_per_km": 360, "quality": "VALID"},
            {"date": "2026-09-01", "pace_seconds_per_km": 368, "quality": "VALID"},
        ],
        configured_tolerance=None,
        authorization_state=state,
    )
    assert result["raw_delta_seconds_per_km"] == 8
    assert result["status"] == "UNRATED"
    assert result["diagnosis"] is None
    assert state == {"higher_intensity_authorized": False}


def test_exactly_thirty_seconds_is_supported_but_any_larger_gap_is_not():
    exact = evaluate_maffetone(
        ceiling_bpm=145,
        lower_bpm=135,
        samples=[_sample(0, hr=140), _sample(30, hr=140)],
    )
    over = evaluate_maffetone(
        ceiling_bpm=145,
        lower_bpm=135,
        samples=[_sample(0, hr=140), _sample(30.000001, hr=140)],
    )
    assert exact["in_range_seconds"] == Decimal("30.0")
    assert over["in_range_seconds"] is None
    assert over["quality"] == "INSUFFICIENT"


def test_maf_test_observation_uses_typed_measurement_window_and_no_fake_laps():
    samples = [_sample(second, speed=2.5, hr=140) for second in range(0, 2341, 30)]
    result = evaluate_test_observation(
        samples=samples,
        benchmark_identity="MAF_TEST",
        ceiling_bpm=145,
        lower_bpm=135,
        protocol_version="MAF-GPS-30MIN-V1",
        warmup_seconds=720,
        measurement_seconds=900,
        cooldown_seconds=720,
        conditions={"course": "track"},
        course_key="track-a",
        test_date="2026-09-01",
    )
    assert result["quality"] == "VALID"
    assert result["distance_metres"] == Decimal("2250.0")
    assert result["pace_seconds_per_km"] == Decimal("400")
    assert result["per_lap_fade"] is None
    assert result["per_lap_reason"] == "NO_EXPLICIT_LAP_DEFINITION"


def test_fitzgerald_weekly_and_context_windows_remain_unrated():
    results = [
        {
            "local_activity_date": f"2026-09-{day:02d}",
            "phase": "BASE",
            "quality": "VALID",
            "coverage": {
                "metric_supported_seconds": Decimal("75.5"),
                "unsupported_seconds": Decimal(0),
            },
            "category_seconds": {
                "LOW": Decimal("60.5"),
                "MODERATE": Decimal("10"),
                "HIGH": Decimal("5"),
            },
        }
        for day in (1, 8, 15, 22)
    ]
    aggregate = aggregate_runtime_results(
        activity_results=results,
        completed_weeks=("2026-W36", "2026-W37", "2026-W38", "2026-W39"),
    )
    rolling = aggregate["context"]["ROLLING_FOUR_COMPLETED_WEEKS"]
    assert rolling["coverage"] == "FULL_WINDOW"
    assert rolling["low_seconds"] == Decimal("242.0")
    assert rolling["status"] == "UNRATED"
    assert aggregate["context"]["PHASE"] == {
        "coverage": "FULL_WINDOW",
        "phases": {
            "BASE": {
                "low_seconds": Decimal("242.0"),
                "moderate_seconds": Decimal("40"),
                "high_seconds": Decimal("20"),
                "evidence_quality": "VALID",
                "evidence_status": "UNRATED",
                "metric_supported_seconds": Decimal("302.0"),
                "unsupported_seconds": Decimal(0),
            }
        },
        "unassigned_activity_count": 0,
        "status": "UNRATED",
    }


def test_fitzgerald_aggregate_keeps_mixed_evidence_explicit_without_a_verdict():
    aggregate = aggregate_runtime_results(
        activity_results=[
            {
                "local_activity_date": "2026-09-01",
                "phase": "BASE",
                "quality": "VALID",
                "coverage": {
                    "metric_supported_seconds": Decimal(60),
                    "unsupported_seconds": Decimal(0),
                },
                "category_seconds": {
                    "LOW": Decimal(60),
                    "MODERATE": Decimal(0),
                    "HIGH": Decimal(0),
                },
            },
            {
                "local_activity_date": "2026-09-02",
                "phase": "BASE",
                "quality": "UNAVAILABLE",
                "coverage": {
                    "metric_supported_seconds": Decimal(0),
                    "unsupported_seconds": None,
                },
                "category_seconds": {
                    "LOW": Decimal(0),
                    "MODERATE": Decimal(0),
                    "HIGH": Decimal(0),
                },
            },
        ]
    )

    week = aggregate["2026-W36"]
    assert week["evidence_quality"] == "PARTIAL"
    assert week["evidence_status"] == "UNKNOWN"
    assert week["metric_supported_seconds"] == Decimal(60)
    assert week["unsupported_seconds"] is None


def test_fitzgerald_secondary_hr_is_diagnostic_and_never_replaces_pace():
    result = evaluate_fitzgerald(
        prescription={
            "methodology_id": "FITZGERALD_80_20_RUNNING_V1",
            "primary_metric": "SPEED",
            "secondary_metric": "HEART_RATE",
            "native_target": "F80.ZONE_2",
            "threshold": "4",
            "secondary_threshold": "170",
        },
        samples=[_sample(0, speed=None, hr=145), _sample(30, speed=None, hr=145)],
    )
    assert result["quality"] == "INSUFFICIENT"
    assert result["status"] == "UNKNOWN"
    assert result["secondary_evidence"]["coverage"]["quality"] == "VALID"
    assert result["secondary_evidence"]["diagnostic_only"] is True
    assert result["secondary_evidence"]["replaces_primary"] is False


def test_missing_mapping_timestamps_cannot_fabricate_temporal_support():
    result = evaluate_maffetone(
        ceiling_bpm=145,
        lower_bpm=135,
        samples=[{"hr": 140}, {"hr": 140}],
    )

    assert result["quality"] == "UNAVAILABLE"
    assert result["coverage"]["metric_supported_seconds"] == Decimal(0)
    assert result["in_range_seconds"] is None


def test_fitzgerald_target_adherence_is_unknown_without_metric_support():
    result = evaluate_fitzgerald(
        prescription={
            "methodology_id": "FITZGERALD_80_20_RUNNING_V1",
            "primary_metric": "SPEED",
            "native_target": "F80.ZONE_2",
            "threshold": "4",
        },
        samples=[_sample(0, speed=None), _sample(30, speed=None)],
    )

    assert result["quality"] == "INSUFFICIENT"
    assert result["target_adherence"] == {
        "status": "UNKNOWN",
        "inside_target_seconds": None,
        "outside_target_seconds": None,
        "reason": "INSUFFICIENT_PRIMARY_EVIDENCE",
    }


def test_fitzgerald_missing_metric_evidence_does_not_require_a_threshold():
    result = evaluate_fitzgerald(
        prescription={
            "methodology_id": "FITZGERALD_80_20_RUNNING_V1",
            "primary_metric": "SPEED",
            "native_target": "F80.ZONE_2",
        },
        samples=[_sample(0, speed=None), _sample(30, speed=None)],
    )

    assert result["quality"] == "INSUFFICIENT"
    assert result["status"] == "UNKNOWN"


def test_fitzgerald_rejects_foreign_native_target_even_when_numeric_evidence_matches():
    with pytest.raises(DomainError, match="foreign native target"):
        evaluate_fitzgerald(
            prescription={
                "methodology_id": "FITZGERALD_80_20_RUNNING_V1",
                "primary_metric": "SPEED",
                "native_target": "MAF.MAF_AEROBIC_RANGE",
                "threshold": "4",
            },
            samples=[_sample(0, speed=4), _sample(30, speed=4)],
        )


def test_maf_test_window_is_anchored_to_first_parseable_observation():
    samples = [_sample(0, speed=2.5, hr=140)]
    samples.extend(_sample(second, speed=2.5, hr=140) for second in range(1000, 1901, 30))

    result = evaluate_test_observation(
        samples=samples,
        benchmark_identity="MAF_TEST",
        ceiling_bpm=145,
        lower_bpm=135,
        protocol_version="MAF-GPS-30MIN-V1",
        warmup_seconds=720,
        measurement_seconds=900,
        cooldown_seconds=720,
        conditions={"course": "track"},
        course_key="track-a",
        test_date="2026-09-01",
    )

    assert result["quality"] == "PARTIAL"
    assert result["supported_seconds"] == Decimal("620.0")
    assert result["unsupported_seconds"] == Decimal("280.0")


def test_maf_test_requires_joint_hr_and_speed_support_and_hides_insufficient_result():
    result = evaluate_test_observation(
        samples=[
            _sample(0, speed=2.5, hr=140),
            _sample(720, speed=None, hr=140),
            _sample(750, speed=2.5, hr=None),
            _sample(780, speed=None, hr=None),
        ],
        benchmark_identity="MAF_TEST",
        ceiling_bpm=145,
        lower_bpm=135,
        protocol_version="MAF-GPS-30MIN-V1",
        warmup_seconds=720,
        measurement_seconds=900,
        cooldown_seconds=720,
        conditions={"course": "track"},
        course_key="track-a",
        test_date="2026-09-01",
    )

    assert result["quality"] == "INSUFFICIENT"
    assert result["supported_seconds"] == Decimal(0)
    assert result["distance_metres"] is None
    assert result["speed_mps"] is None
    assert result["pace_seconds_per_km"] is None


def test_ordinary_run_identity_cannot_produce_a_maf_test_result():
    samples = [_sample(second, speed=2.5, hr=140) for second in range(0, 2341, 30)]
    result = evaluate_test_observation(
        samples=samples,
        benchmark_identity="AEROBIC_RUN",
        ceiling_bpm=145,
        lower_bpm=135,
        protocol_version="MAF-GPS-30MIN-V1",
        warmup_seconds=720,
        measurement_seconds=900,
        cooldown_seconds=720,
        conditions={"course": "track"},
        test_date="2026-09-01",
    )

    assert result["quality"] == "INSUFFICIENT"
    assert result["status"] == "UNKNOWN"
    assert result["distance_metres"] is None
    assert result["pace_seconds_per_km"] is None
    assert "EXPLICIT_MAF_TEST_IDENTITY_REQUIRED" in result["protocol_validation"]["reasons"]


def test_maf_trend_does_not_treat_missing_comparability_key_as_wildcard():
    result = evaluate_test_trend(
        tests=[
            {
                "date": "2026-08-01",
                "pace_seconds_per_km": 360,
                "quality": "VALID",
                "protocol_version": "MAF-GPS-30MIN-V1",
                "course_key": "course-a",
            },
            {
                "date": "2026-09-01",
                "pace_seconds_per_km": 368,
                "quality": "VALID",
                "protocol_version": "MAF-GPS-30MIN-V1",
            },
        ]
    )

    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["raw_delta_seconds_per_km"] is None
