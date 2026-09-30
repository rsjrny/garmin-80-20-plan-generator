"""Fitzgerald 80/20 V1 runtime compliance from frozen revision facts."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable, Mapping

from garmin_data_hub.analytics.temporal_metrics import TemporalSample, build_temporal_evidence

from .compliance import EVALUATOR_VERSION, build_coverage, coerce_samples, decimal_seconds
from .domain import DataQuality, DomainError, MethodologyId, Metric
from .fitzgerald_parameters import TARGET_ORDER, classify_sample
from .fitzgerald_policy import TARGET_CATEGORIES
from .prescriptions import IntensityPrescription


def _prescription_value(prescription: Mapping[str, Any] | IntensityPrescription, name: str) -> Any:
    if isinstance(prescription, Mapping):
        return prescription.get(name)
    if name == "methodology_id":
        return prescription.methodology_id.value
    if name == "primary_metric":
        return prescription.primary.metric.value
    if name == "native_target":
        return prescription.native_target.value
    if name == "secondary_metric":
        return None if prescription.secondary is None else prescription.secondary.metric.value
    return None


def _threshold(prescription: Mapping[str, Any] | IntensityPrescription, supplied: Any) -> str:
    value = supplied
    if value is None and isinstance(prescription, Mapping):
        value = prescription.get("threshold") or prescription.get("threshold_value")
    if value is None:
        raise DomainError("frozen Fitzgerald threshold is required when observations exist")
    return str(value)


def evaluate(
    *,
    prescription: Mapping[str, Any] | IntensityPrescription,
    samples: Iterable[TemporalSample | Mapping[str, Any]] | None,
    sensor_quality: str | None = None,
    threshold: str | Decimal | int | None = None,
    secondary_threshold: str | Decimal | int | None = None,
    target_alignment_available: bool = True,
    **_: Any,
) -> dict[str, Any]:
    methodology = _prescription_value(prescription, "methodology_id")
    if methodology not in (None, MethodologyId.FITZGERALD_80_20_RUNNING_V1.value):
        raise DomainError("Fitzgerald evaluator rejects foreign methodology prescriptions")
    raw_metric = str(_prescription_value(prescription, "primary_metric") or "SPEED")
    metric = Metric.SPEED if raw_metric == "PACE" else Metric(raw_metric)
    if metric not in {Metric.SPEED, Metric.HEART_RATE}:
        raise DomainError("Fitzgerald V1 runtime metric must be frozen SPEED/PACE or HEART_RATE")

    materialized = coerce_samples(samples)
    evidence = build_temporal_evidence(materialized)
    zone_seconds = {zone: Decimal(0) for zone in TARGET_ORDER}
    unaccounted = Decimal(0)
    metric_supported = Decimal(0)

    frozen_threshold = None
    for interval in evidence.intervals:
        signal = interval.speed_mps if metric is Metric.SPEED else interval.heart_rate_bpm
        if signal is None:
            continue
        if frozen_threshold is None:
            frozen_threshold = _threshold(prescription, threshold)
        duration = decimal_seconds(interval.duration_s) or Decimal(0)
        metric_supported += duration
        classified = classify_sample(
            metric=metric.value,
            threshold=frozen_threshold,
            sample=str(signal),
        )["native_target"]
        if classified is None:
            unaccounted += duration
        else:
            zone_seconds[classified] += duration

    coverage = build_coverage(
        metric=metric,
        observations_received=evidence.observations_received,
        parseable_observations=evidence.parseable_observations,
        observed_window_seconds=evidence.observed_window_seconds,
        temporal_supported_seconds=evidence.temporal_supported_seconds,
        metric_supported_seconds=metric_supported,
        explicit_quality=sensor_quality,
    )
    category_seconds = {"LOW": Decimal(0), "MODERATE": Decimal(0), "HIGH": Decimal(0)}
    for zone, seconds in zone_seconds.items():
        category = TARGET_CATEGORIES[zone].value
        category_seconds[category] += seconds
    denominator = sum(zone_seconds.values(), Decimal(0))
    percentages = {
        key: (value / denominator * Decimal(100) if denominator else None)
        for key, value in category_seconds.items()
    }
    raw_target = str(_prescription_value(prescription, "native_target") or "")
    if "." in raw_target and not raw_target.startswith("F80."):
        raise DomainError("Fitzgerald evaluator rejects foreign native target")
    target = raw_target.split(".", 1)[-1]
    if target not in zone_seconds:
        raise DomainError("Fitzgerald evaluator rejects foreign native target")
    target_seconds = zone_seconds.get(target)
    aligned_target = target_seconds is not None and target_alignment_available
    if metric_supported == 0:
        target_adherence = {
            "status": "UNKNOWN",
            "inside_target_seconds": None,
            "outside_target_seconds": None,
            "reason": "INSUFFICIENT_PRIMARY_EVIDENCE",
        }
    elif not aligned_target:
        target_adherence = {
            "status": "UNKNOWN",
            "inside_target_seconds": None,
            "outside_target_seconds": None,
            "reason": "INSUFFICIENT_EXECUTION_ALIGNMENT",
        }
    else:
        target_adherence = {
            "status": "UNRATED" if coverage.quality is DataQuality.VALID else "UNKNOWN",
            "inside_target_seconds": target_seconds,
            "outside_target_seconds": metric_supported - target_seconds,
            "reason": None
            if coverage.quality is DataQuality.VALID
            else "INCOMPLETE_PRIMARY_EVIDENCE",
        }
    secondary_result = None
    raw_secondary = _prescription_value(prescription, "secondary_metric")
    if raw_secondary is None and isinstance(prescription, Mapping):
        secondary_payload = prescription.get("secondary")
        if isinstance(secondary_payload, Mapping):
            raw_secondary = secondary_payload.get("metric")
        raw_secondary = raw_secondary or prescription.get("secondary_metric")
    if raw_secondary is not None:
        secondary_metric = Metric.SPEED if str(raw_secondary) == "PACE" else Metric(str(raw_secondary))
        if secondary_metric not in {Metric.SPEED, Metric.HEART_RATE}:
            raise DomainError("Fitzgerald secondary runtime metric must be SPEED/PACE or HEART_RATE")
        frozen_secondary_threshold = secondary_threshold
        if frozen_secondary_threshold is None and isinstance(prescription, Mapping):
            frozen_secondary_threshold = prescription.get("secondary_threshold")
        secondary_zones = {zone: Decimal(0) for zone in TARGET_ORDER}
        secondary_unaccounted = Decimal(0)
        secondary_supported = Decimal(0)
        if frozen_secondary_threshold is not None:
            for interval in evidence.intervals:
                signal = (
                    interval.speed_mps
                    if secondary_metric is Metric.SPEED
                    else interval.heart_rate_bpm
                )
                if signal is None:
                    continue
                duration = decimal_seconds(interval.duration_s) or Decimal(0)
                secondary_supported += duration
                classified = classify_sample(
                    metric=secondary_metric.value,
                    threshold=str(frozen_secondary_threshold),
                    sample=str(signal),
                )["native_target"]
                if classified is None:
                    secondary_unaccounted += duration
                else:
                    secondary_zones[classified] += duration
        secondary_coverage = build_coverage(
            metric=secondary_metric,
            observations_received=evidence.observations_received,
            parseable_observations=evidence.parseable_observations,
            observed_window_seconds=evidence.observed_window_seconds,
            temporal_supported_seconds=evidence.temporal_supported_seconds,
            metric_supported_seconds=secondary_supported,
            explicit_quality=sensor_quality,
        )
        secondary_result = {
            "metric": secondary_metric.value,
            "native_zone_seconds": secondary_zones,
            "below_zone_seconds": secondary_unaccounted,
            "coverage": secondary_coverage.as_dict(),
            "diagnostic_only": True,
            "replaces_primary": False,
            "reason": None
            if frozen_secondary_threshold is not None
            else "FROZEN_SECONDARY_THRESHOLD_UNAVAILABLE",
        }
    status = "UNRATED" if coverage.quality is DataQuality.VALID else "UNKNOWN"
    return {
        "methodology_id": MethodologyId.FITZGERALD_80_20_RUNNING_V1.value,
        "evaluator_version": EVALUATOR_VERSION,
        "status": status,
        "quality": coverage.quality.value,
        "noncompliant": False,
        "primary_metric": metric.value,
        "native_zone_seconds": zone_seconds,
        "category_seconds": category_seconds,
        "category_percentages": percentages,
        "distribution_denominator_seconds": denominator,
        "below_zone_seconds": unaccounted,
        "unsupported_seconds": coverage.unsupported_seconds,
        "target_adherence": target_adherence,
        "coverage": coverage.as_dict(),
        "secondary_evidence": secondary_result,
        "composite_score": None,
        "severity": "INFORMATIONAL",
    }
