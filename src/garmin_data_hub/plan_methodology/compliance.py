"""Shared deterministic runtime evidence semantics."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from garmin_data_hub.analytics.temporal_metrics import TemporalSample

from .domain import DataQuality, DomainError, Metric, canonical_decimal, exact_decimal, parse_enum


QUALITY_POLICY_VERSION = "runtime-evidence.v1"
EVALUATOR_VERSION = "runtime-compliance.v1"


@dataclass(frozen=True, slots=True)
class MetricCoverage:
    metric: Metric
    observed_window_seconds: Decimal | None
    temporal_supported_seconds: Decimal
    metric_supported_seconds: Decimal
    unsupported_seconds: Decimal | None
    coverage_ratio: Decimal | None
    quality: DataQuality
    quality_policy_version: str
    reason_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric.value,
            "observed_window_seconds": self.observed_window_seconds,
            "temporal_supported_seconds": self.temporal_supported_seconds,
            "metric_supported_seconds": self.metric_supported_seconds,
            "unsupported_seconds": self.unsupported_seconds,
            "coverage_ratio": self.coverage_ratio,
            "quality": self.quality.value,
            "quality_policy_version": self.quality_policy_version,
            "reason_codes": list(self.reason_codes),
        }


def describe_outputs(*, methodology_id: str, **_: Any) -> dict[str, Any]:
    if methodology_id == "FITZGERALD_80_20_RUNNING_V1":
        outputs = [
            "native_zone_time",
            "low_moderate_high_time",
            "target_adherence",
            "distribution_context",
            "coverage",
            "quality",
        ]
    elif methodology_id == "MAFFETONE_RUNNING_V1":
        outputs = [
            "below_range_time",
            "in_range_time",
            "above_ceiling_time",
            "excursions",
            "maf_test_result_trend",
            "coverage",
            "quality",
        ]
    else:
        raise DomainError(f"unsupported runtime methodology: {methodology_id!r}")
    return {"outputs": outputs, "universal_composite_score": False}


def classify_missing_data(
    *, observations: Any, quality: str | DataQuality, **_: Any
) -> dict[str, Any]:
    parsed = parse_enum(DataQuality, quality, "quality")
    if parsed is DataQuality.VALID:
        raise DomainError("VALID evidence is not missing-data evidence")
    return {
        "status": "UNKNOWN",
        "quality": parsed.value,
        "noncompliant": False,
    }


def coerce_samples(samples: Iterable[TemporalSample | Mapping[str, Any]] | None) -> tuple[TemporalSample, ...]:
    result: list[TemporalSample] = []
    for index, sample in enumerate(samples or ()):
        if isinstance(sample, TemporalSample):
            result.append(sample)
        elif isinstance(sample, Mapping):
            result.append(
                TemporalSample(
                    timestamp_utc=sample.get("timestamp_utc", sample.get("timestamp")),
                    speed_mps=sample.get("speed_mps", sample.get("speed")),
                    heart_rate_bpm=sample.get("heart_rate_bpm", sample.get("hr")),
                    power_w=sample.get("power_w", sample.get("power")),
                    seq=sample.get("seq", index),
                )
            )
        else:
            raise DomainError("runtime samples must be TemporalSample values or mappings")
    return tuple(result)


def decimal_seconds(value: float | int | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def build_coverage(
    *,
    metric: Metric | str,
    observations_received: int,
    parseable_observations: int,
    observed_window_seconds: float | Decimal | None,
    temporal_supported_seconds: float | Decimal,
    metric_supported_seconds: float | Decimal,
    explicit_quality: DataQuality | str | None = None,
) -> MetricCoverage:
    metric_value = parse_enum(Metric, metric, "metric")
    observed = decimal_seconds(observed_window_seconds)
    temporal = decimal_seconds(temporal_supported_seconds) or Decimal(0)
    supported = decimal_seconds(metric_supported_seconds) or Decimal(0)
    unsupported = None if observed is None else max(Decimal(0), observed - supported)

    reasons: list[str] = []
    if observations_received == 0 or parseable_observations == 0:
        structural = DataQuality.UNAVAILABLE
        reasons.append("NO_OBSERVATIONS")
    elif supported == 0:
        structural = DataQuality.INSUFFICIENT
        reasons.append("NO_VALID_SUPPORTED_DURATION")
    elif unsupported is not None and unsupported > 0:
        structural = DataQuality.PARTIAL
        reasons.append("INCOMPLETE_COVERAGE")
    else:
        structural = DataQuality.VALID

    quality = structural
    if explicit_quality is not None:
        supplied = parse_enum(DataQuality, explicit_quality, "explicit_quality")
        ranks = {
            DataQuality.VALID: 0,
            DataQuality.PARTIAL: 1,
            DataQuality.INSUFFICIENT: 2,
            DataQuality.UNAVAILABLE: 3,
        }
        if ranks[supplied] > ranks[quality]:
            quality = supplied
            reasons.append("EXPLICIT_QUALITY_DOWNGRADE")

    ratio = None
    if observed is not None and observed > 0:
        ratio = supported / observed
    return MetricCoverage(
        metric=metric_value,
        observed_window_seconds=observed,
        temporal_supported_seconds=temporal,
        metric_supported_seconds=supported,
        unsupported_seconds=unsupported,
        coverage_ratio=ratio,
        quality=quality,
        quality_policy_version=QUALITY_POLICY_VERSION,
        reason_codes=tuple(reasons),
    )


def decimal_mapping(values: Mapping[str, Decimal]) -> dict[str, str]:
    """JSON-safe helper used by report/API adapters without changing domain math."""
    return {key: canonical_decimal(value) for key, value in values.items()}
