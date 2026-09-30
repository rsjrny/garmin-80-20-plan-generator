"""Maffetone V1 runtime range, excursion, test, and trend calculations."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from garmin_data_hub.analytics.temporal_metrics import TemporalInterval, TemporalSample, build_temporal_evidence

from .compliance import EVALUATOR_VERSION, build_coverage, coerce_samples, decimal_seconds
from .domain import DataQuality, DomainError, MethodologyId, Metric
from .maffetone_policy import validate_maf_test


def _exact_number(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        # Runtime sensor floats are converted elsewhere through str; frozen
        # methodology values themselves must not originate as binary floats.
        if isinstance(value, float):
            return Decimal(str(value))
        raise DomainError(f"invalid {field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DomainError(f"invalid {field}") from exc
    if not result.is_finite():
        raise DomainError(f"invalid {field}")
    return result


def _finish_excursion(current: dict[str, Any] | None, excursions: list[dict[str, Any]]) -> None:
    if current is not None:
        excursions.append(current)


def evaluate(
    *,
    ceiling_bpm: int | str,
    lower_bpm: int | str,
    samples: Iterable[TemporalSample | Mapping[str, Any]] | None,
    hr_quality: str | None = None,
    methodology_id: str = MethodologyId.MAFFETONE_RUNNING_V1.value,
    native_target: str | None = None,
    methodology_state: str | None = None,
    event_context: Mapping[str, Any] | None = None,
    **_: Any,
) -> dict[str, Any]:
    if methodology_id != MethodologyId.MAFFETONE_RUNNING_V1.value:
        raise DomainError("Maffetone evaluator rejects foreign methodology state")
    ceiling = _exact_number(ceiling_bpm, "ceiling_bpm")
    lower = _exact_number(lower_bpm, "lower_bpm")
    if lower > ceiling:
        raise DomainError("lower_bpm cannot exceed ceiling_bpm")

    materialized = coerce_samples(samples)
    evidence = build_temporal_evidence(materialized)
    below = Decimal(0)
    in_range = Decimal(0)
    above = Decimal(0)
    supported = Decimal(0)
    excursions: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    previous_end: float | None = None

    for interval in evidence.intervals:
        hr = interval.heart_rate_bpm
        contiguous = previous_end is not None and interval.start_s == previous_end
        if hr is None:
            _finish_excursion(current, excursions)
            current = None
            previous_end = interval.end_s
            continue
        duration = decimal_seconds(interval.duration_s) or Decimal(0)
        supported += duration
        value = Decimal(str(hr))
        if value < lower:
            below += duration
            _finish_excursion(current, excursions)
            current = None
        elif value <= ceiling:
            in_range += duration
            _finish_excursion(current, excursions)
            current = None
        else:
            above += duration
            if current is None or not contiguous:
                _finish_excursion(current, excursions)
                current = {
                    "start_timestamp": interval.start_s,
                    "end_timestamp": interval.end_s,
                    "duration_seconds": duration,
                    "maximum_hr_bpm": value,
                }
            else:
                current["end_timestamp"] = interval.end_s
                current["duration_seconds"] += duration
                current["maximum_hr_bpm"] = max(current["maximum_hr_bpm"], value)
        previous_end = interval.end_s
    _finish_excursion(current, excursions)

    coverage = build_coverage(
        metric=Metric.HEART_RATE,
        observations_received=evidence.observations_received,
        parseable_observations=evidence.parseable_observations,
        observed_window_seconds=evidence.observed_window_seconds,
        temporal_supported_seconds=evidence.temporal_supported_seconds,
        metric_supported_seconds=supported,
        explicit_quality=hr_quality,
    )
    if supported:
        percentages = {
            "BELOW_RANGE": below / supported * Decimal(100),
            "IN_RANGE": in_range / supported * Decimal(100),
            "ABOVE_CEILING": above / supported * Decimal(100),
        }
    else:
        percentages = {key: None for key in ("BELOW_RANGE", "IN_RANGE", "ABOVE_CEILING")}
    status = "UNRATED" if coverage.quality is DataQuality.VALID else "UNKNOWN"
    short_target = None if native_target is None else native_target.split(".", 1)[-1]
    if short_target == "SUB_MAF":
        inside = below
    elif short_target == "MAF_AEROBIC_RANGE":
        inside = in_range
    elif short_target == "MAF_CEILING":
        inside = below + in_range
    elif short_target is None:
        inside = None
    else:
        raise DomainError("Maffetone evaluator rejects foreign native targets")
    return {
        "methodology_id": MethodologyId.MAFFETONE_RUNNING_V1.value,
        "evaluator_version": EVALUATOR_VERSION,
        "status": status,
        "quality": coverage.quality.value,
        "noncompliant": False,
        "below_seconds": below if supported else None,
        "in_range_seconds": in_range if supported else None,
        "above_seconds": above if supported else None,
        "percentages": percentages,
        "excursions": excursions,
        "excursion_summary": {
            "count": len(excursions),
            "cumulative_seconds": sum((item["duration_seconds"] for item in excursions), Decimal(0)),
            "longest_seconds": max((item["duration_seconds"] for item in excursions), default=Decimal(0)),
            "maximum_hr_bpm": max((item["maximum_hr_bpm"] for item in excursions), default=None),
        },
        "coverage": coverage.as_dict(),
        "target_adherence": {
            "status": "UNKNOWN" if inside is None or not supported else "UNRATED",
            "inside_target_seconds": inside if inside is not None and supported else None,
            "outside_target_seconds": supported - inside if inside is not None and supported else None,
        },
        "methodology_state": methodology_state,
        "event_context": None if event_context is None else dict(event_context),
        "ceiling_changed": False,
        "state_changed": False,
        "authorization_changed": False,
        "composite_score": None,
        "severity": "INFORMATIONAL",
    }


def evaluate_test_trend(
    *,
    tests: Iterable[Mapping[str, Any]],
    configured_tolerance: Any = None,
    **_: Any,
) -> dict[str, Any]:
    # configured_tolerance is intentionally not interpreted here. A future
    # versioned trend policy may consume it; V1 only reports objective deltas.
    del configured_tolerance
    usable = [
        item
        for item in tests
        if item.get("quality") == DataQuality.VALID.value
        and item.get("pace_seconds_per_km") is not None
    ]
    usable.sort(key=lambda item: (str(item.get("date", "")), str(item.get("observation_id", ""))))
    pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for earlier_index, earlier in enumerate(usable):
        for later in usable[earlier_index + 1 :]:
            keys = ("protocol_version", "course_key", "lap_definition_key")
            if all(earlier.get(key) == later.get(key) for key in keys):
                pairs.append((earlier, later))
    if not pairs:
        return {
            "status": "INSUFFICIENT_DATA",
            "raw_delta_seconds_per_km": None,
            "diagnosis": None,
            "severity": "INFORMATIONAL",
            "authorization_changed": False,
        }
    comparisons: list[dict[str, Any]] = []
    for earlier_item, later_item in pairs:
        delta = _exact_number(later_item["pace_seconds_per_km"], "pace") - _exact_number(
            earlier_item["pace_seconds_per_km"], "pace"
        )
        comparisons.append(
            {
                "earlier_date": earlier_item.get("date"),
                "later_date": later_item.get("date"),
                "raw_delta_seconds_per_km": int(delta) if delta == delta.to_integral() else delta,
                "conditions_differ": bool(
                    earlier_item.get("conditions_differ") or later_item.get("conditions_differ")
                ),
            }
        )
    earlier, later = pairs[-1]
    pace_delta = _exact_number(later["pace_seconds_per_km"], "pace") - _exact_number(
        earlier["pace_seconds_per_km"], "pace"
    )
    speed_delta = None
    if earlier.get("speed_mps") is not None and later.get("speed_mps") is not None:
        speed_delta = _exact_number(later["speed_mps"], "speed") - _exact_number(
            earlier["speed_mps"], "speed"
        )
    return {
        "earlier_date": earlier.get("date"),
        "later_date": later.get("date"),
        "raw_delta_seconds_per_km": int(pace_delta) if pace_delta == pace_delta.to_integral() else pace_delta,
        "raw_speed_delta_mps": speed_delta,
        "conditions_differ": bool(earlier.get("conditions_differ") or later.get("conditions_differ")),
        "comparisons": comparisons,
        "status": "UNRATED",
        "diagnosis": None,
        "severity": "INFORMATIONAL",
        "authorization_changed": False,
    }


def evaluate_test_observation(
    *,
    samples: Iterable[TemporalSample | Mapping[str, Any]],
    benchmark_identity: str,
    ceiling_bpm: int,
    lower_bpm: int,
    protocol_version: str,
    warmup_seconds: int,
    measurement_seconds: int,
    cooldown_seconds: int,
    conditions: Mapping[str, Any],
    test_date: str,
    segments: Iterable[Mapping[str, Any]] | None = None,
    speed_integration_version: str | None = "left-held-speed.v1",
    course_key: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    """Calculate one structured GPS MAF Test observation without inventing laps."""
    materialized = coerce_samples(samples)
    evidence = build_temporal_evidence(materialized)
    if not evidence.intervals:
        quality = (
            DataQuality.UNAVAILABLE
            if evidence.parseable_observations == 0
            else DataQuality.INSUFFICIENT
        )
        validation = validate_maf_test(
            benchmark_identity=benchmark_identity,
            protocol_version=protocol_version,
            ceiling_bpm=ceiling_bpm,
            warmup_seconds=warmup_seconds,
            measurement_seconds=measurement_seconds,
            cooldown_seconds=cooldown_seconds,
            hr_coverage=quality.value,
            conditions=conditions,
            segments=None if segments is None else tuple(segments),
        )
        return {
            "date": test_date,
            "protocol_version": protocol_version,
            "quality": quality.value,
            "status": "UNKNOWN",
            "pace_seconds_per_km": None,
            "per_lap_fade": None,
            "reason": "NO_USABLE_MEASUREMENT_EVIDENCE",
            "protocol_validation": validation,
        }
    if evidence.observed_start_seconds is None:
        raise DomainError("MAF Test requires a parseable observation timestamp")
    window_start = evidence.observed_start_seconds + warmup_seconds
    window_end = window_start + measurement_seconds
    hr_supported = Decimal(0)
    jointly_supported = Decimal(0)
    distance_metres = Decimal(0)
    above_seconds = Decimal(0)
    ceiling = Decimal(ceiling_bpm)
    for interval in evidence.intervals:
        overlap = max(0.0, min(interval.end_s, window_end) - max(interval.start_s, window_start))
        if overlap <= 0:
            continue
        seconds = Decimal(str(overlap))
        valid_hr = interval.heart_rate_bpm is not None
        valid_speed = (
            interval.speed_mps is not None
            and speed_integration_version == "left-held-speed.v1"
        )
        if valid_hr:
            hr_supported += seconds
            if Decimal(str(interval.heart_rate_bpm)) > ceiling:
                above_seconds += seconds
        if valid_hr and valid_speed:
            jointly_supported += seconds
            distance_metres += Decimal(str(interval.speed_mps)) * seconds
    expected = Decimal(measurement_seconds)
    supported = jointly_supported
    if supported == 0:
        quality = DataQuality.INSUFFICIENT
    elif supported < expected:
        quality = DataQuality.PARTIAL
    else:
        quality = DataQuality.VALID
    validation = validate_maf_test(
        benchmark_identity=benchmark_identity,
        protocol_version=protocol_version,
        ceiling_bpm=ceiling_bpm,
        warmup_seconds=warmup_seconds,
        measurement_seconds=measurement_seconds,
        cooldown_seconds=cooldown_seconds,
        hr_coverage=(
            DataQuality.VALID.value
            if hr_supported >= expected
            else DataQuality.PARTIAL.value
            if hr_supported > 0
            else DataQuality.INSUFFICIENT.value
        ),
        conditions=conditions,
        segments=None if segments is None else tuple(segments),
    )
    average_speed = distance_metres / supported if supported > 0 else None
    pace = Decimal(1000) / average_speed if average_speed is not None and average_speed > 0 else None
    final_quality = (
        quality
        if validation["valid"]
        else quality
        if quality in {
            DataQuality.PARTIAL,
            DataQuality.INSUFFICIENT,
            DataQuality.UNAVAILABLE,
        }
        else DataQuality.INSUFFICIENT
    )
    result_available = final_quality in {DataQuality.VALID, DataQuality.PARTIAL}
    return {
        "date": test_date,
        "protocol_version": protocol_version,
        "course_key": course_key,
        "quality": final_quality.value,
        "status": "UNRATED" if final_quality is DataQuality.VALID else "UNKNOWN",
        "measurement_seconds": expected,
        "supported_seconds": supported,
        "unsupported_seconds": max(Decimal(0), expected - supported),
        "distance_metres": distance_metres if result_available and supported else None,
        "speed_mps": average_speed if result_available else None,
        "pace_seconds_per_km": pace if result_available else None,
        "above_ceiling_seconds": above_seconds,
        "conditions": dict(conditions),
        "per_lap_fade": None,
        "per_lap_reason": "NO_EXPLICIT_LAP_DEFINITION",
        "protocol_validation": validation,
    }
