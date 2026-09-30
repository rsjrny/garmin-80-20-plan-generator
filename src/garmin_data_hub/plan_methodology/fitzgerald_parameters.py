"""Deterministic Fitzgerald V1 threshold and zone primitives."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Mapping

from .domain import (
    DataQuality,
    DomainError,
    EvidenceClass,
    Metric,
    Sport,
    canonical_decimal,
    exact_decimal,
    parse_enum,
)
from .snapshots import ThresholdSnapshot


TARGET_ORDER = (
    "ZONE_1",
    "ZONE_2",
    "ZONE_X",
    "ZONE_3",
    "ZONE_Y",
    "ZONE_4",
    "ZONE_5",
)

SPEED_FACTORS: dict[str, tuple[Decimal, Decimal | None]] = {
    "ZONE_1": (Decimal("0.60"), Decimal("0.76")),
    "ZONE_2": (Decimal("0.76"), Decimal("0.87")),
    "ZONE_X": (Decimal("0.87"), Decimal("0.93")),
    "ZONE_3": (Decimal("0.93"), Decimal("1.00")),
    "ZONE_Y": (Decimal("1.00"), Decimal("1.02")),
    "ZONE_4": (Decimal("1.02"), Decimal("1.15")),
    "ZONE_5": (Decimal("1.15"), None),
}

HR_FACTORS: dict[str, tuple[Decimal, Decimal | None]] = {
    "ZONE_1": (Decimal("0.72"), Decimal("0.81")),
    "ZONE_2": (Decimal("0.81"), Decimal("0.90")),
    "ZONE_X": (Decimal("0.90"), Decimal("0.95")),
    "ZONE_3": (Decimal("0.95"), Decimal("1.00")),
    "ZONE_Y": (Decimal("1.00"), Decimal("1.02")),
    "ZONE_4": (Decimal("1.02"), Decimal("1.05")),
    "ZONE_5": (Decimal("1.05"), None),
}


def _iso_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DomainError(f"invalid evidence date: {value!r}") from exc


def classify_threshold(
    *,
    metric: str,
    value: str | None,
    unit: str,
    sport: str,
    method_or_protocol: str | None,
    source_record: str | None,
    measured_at: str | None,
    evidence_class: str,
    quality: str,
    derivation_approved: bool = False,
    derivation_version: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    metric_value = Metric.SPEED if metric == "PACE" else parse_enum(Metric, metric, "metric")
    evidence = parse_enum(EvidenceClass, evidence_class, "evidence_class")
    snapshot = ThresholdSnapshot(
        metric=metric_value,
        value=None if value is None else exact_decimal(value, "threshold value"),
        unit=unit,
        sport=parse_enum(Sport, sport, "sport"),
        method_or_protocol=method_or_protocol,
        source_record=source_record,
        observed_at=_iso_date(measured_at),
        evidence_class=evidence,
        quality=parse_enum(DataQuality, quality, "quality"),
        derivation_version=derivation_version,
        derivation_approved=derivation_approved or evidence is EvidenceClass.MEASURED,
    )
    return {
        "evidence_class": snapshot.evidence_class.value,
        "prescription_grade": snapshot.prescription_grade,
    }


def _derive_bounds(
    threshold: Decimal, factors: Mapping[str, tuple[Decimal, Decimal | None]]
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for target in TARGET_ORDER:
        lower_factor, upper_factor = factors[target]
        result[target] = {
            "lower": canonical_decimal(threshold * lower_factor),
            "upper": None
            if upper_factor is None
            else canonical_decimal(threshold * upper_factor),
            "lower_inclusive": True,
            "upper_inclusive": False,
        }
    return result


def derive_zones(*, threshold_speed_mps: str) -> dict[str, Any]:
    threshold = exact_decimal(threshold_speed_mps, "threshold_speed_mps")
    if threshold <= 0:
        raise DomainError("threshold_speed_mps must be positive")
    return {"native_targets": list(TARGET_ORDER), **_derive_bounds(threshold, SPEED_FACTORS)}


def _is_grade(evidence: Mapping[str, Any] | None) -> bool:
    if not evidence or evidence.get("value") is None:
        return False
    if evidence.get("quality", "VALID") != "VALID":
        return False
    evidence_class = evidence.get("evidence_class")
    return evidence_class == "MEASURED" or (
        evidence_class == "DERIVED" and evidence.get("derivation_approved") is True
    )


def select_primary_metric(
    *, pace: Mapping[str, Any], lthr: Mapping[str, Any], context: str, **_: Any
) -> dict[str, Any]:
    pace_suitable = context not in {"TRAIL_TECHNICAL", "EXTREME_HEAT", "PACE_UNSUITABLE"}
    if _is_grade(pace) and pace_suitable:
        return {
            "primary_metric": "PACE",
            "secondary_metric": "HEART_RATE" if _is_grade(lthr) else None,
            "confidence": "HIGH",
            "pace_target": pace.get("value"),
            "blocked": False,
        }
    if _is_grade(lthr):
        return {
            "primary_metric": "HEART_RATE",
            "confidence": "REDUCED",
            "pace_target": None,
            "blocked": False,
        }
    return {
        "primary_metric": None,
        "confidence": "UNKNOWN",
        "pace_target": None,
        "blocked": True,
        "reason": "INSUFFICIENT_THRESHOLD_EVIDENCE",
    }


def classify_sample(
    *, metric: str, threshold: str, sample: str, display_bpm: int | None = None, **_: Any
) -> dict[str, Any]:
    metric_value = parse_enum(Metric, metric, "metric")
    factors = HR_FACTORS if metric_value is Metric.HEART_RATE else SPEED_FACTORS
    threshold_value = exact_decimal(threshold, "threshold")
    sample_value = exact_decimal(sample, "sample")
    for target in TARGET_ORDER:
        lower_factor, upper_factor = factors[target]
        lower = threshold_value * lower_factor
        upper = None if upper_factor is None else threshold_value * upper_factor
        if sample_value >= lower and (upper is None or sample_value < upper):
            return {
                "native_target": target,
                "used_unrounded_value": True,
                "boundary_semantics": "LOWER_INCLUSIVE_UPPER_EXCLUSIVE",
            }
    return {
        "native_target": None,
        "used_unrounded_value": True,
        "boundary_semantics": "LOWER_INCLUSIVE_UPPER_EXCLUSIVE",
    }
