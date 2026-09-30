"""Typed methodology-native intensity prescriptions."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from .domain import (
    Confidence,
    DomainError,
    FitzgeraldTarget,
    MaffetoneTarget,
    MethodologyId,
    Metric,
    canonical_decimal,
    exact_decimal,
    parse_enum,
)


CANONICAL_UNITS = {
    Metric.PACE: "m/s",
    Metric.SPEED: "m/s",
    Metric.HEART_RATE: "bpm",
    Metric.POWER: "W",
}


@dataclass(frozen=True, slots=True)
class MetricRange:
    metric: Metric
    unit: str
    lower: Decimal | None
    upper: Decimal | None
    lower_inclusive: bool
    upper_inclusive: bool

    def __post_init__(self) -> None:
        metric = parse_enum(Metric, self.metric, "metric")
        object.__setattr__(self, "metric", metric)
        expected_unit = CANONICAL_UNITS[metric]
        if self.unit != expected_unit:
            raise DomainError(f"{metric.value} requires canonical unit {expected_unit!r}")
        if self.lower is not None:
            object.__setattr__(self, "lower", exact_decimal(self.lower, "lower bound"))
        if self.upper is not None:
            object.__setattr__(self, "upper", exact_decimal(self.upper, "upper bound"))
        if self.lower is None and self.upper is None:
            raise DomainError("a metric range requires at least one numeric bound")
        if self.lower is not None and self.upper is not None and self.lower >= self.upper:
            raise DomainError("lower bound must be less than upper bound")


@dataclass(frozen=True, slots=True)
class MetricCeiling:
    value: Decimal
    inclusive: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", exact_decimal(self.value, "ceiling"))


@dataclass(frozen=True, slots=True)
class IntensityPrescription:
    methodology_id: MethodologyId
    native_target: FitzgeraldTarget | MaffetoneTarget
    primary: MetricRange
    derivation_ref: str
    confidence: Confidence
    data_quality_requirement: str
    secondary: MetricRange | None = None
    ceiling: MetricCeiling | None = None
    parameter_snapshot_ref: str | None = None
    confidence_reason: str | None = None

    def __post_init__(self) -> None:
        method = parse_enum(MethodologyId, self.methodology_id, "methodology_id")
        confidence = parse_enum(Confidence, self.confidence, "confidence")
        object.__setattr__(self, "methodology_id", method)
        object.__setattr__(self, "confidence", confidence)
        if method is MethodologyId.FITZGERALD_80_20_RUNNING_V1:
            native_target = parse_enum(FitzgeraldTarget, self.native_target, "native_target")
            object.__setattr__(self, "native_target", native_target)
            if self.primary.metric not in {Metric.SPEED, Metric.HEART_RATE, Metric.POWER}:
                raise DomainError("Fitzgerald primary metric is not supported")
        elif method is MethodologyId.MAFFETONE_RUNNING_V1:
            native_target = parse_enum(MaffetoneTarget, self.native_target, "native_target")
            object.__setattr__(self, "native_target", native_target)
            if self.primary.metric is not Metric.HEART_RATE:
                raise DomainError("Maffetone V1 prescriptions are heart-rate primary")
            if self.native_target is MaffetoneTarget.MAF_AEROBIC_RANGE:
                if self.ceiling is None or not self.ceiling.inclusive:
                    raise DomainError("MAF aerobic range requires an inclusive ceiling")
                if self.primary.upper != self.ceiling.value or not self.primary.upper_inclusive:
                    raise DomainError("MAF range upper bound must equal its inclusive ceiling")
        else:
            raise DomainError("legacy methodology cannot create a named numeric prescription")
        if not self.derivation_ref:
            raise DomainError("derivation_ref is required")
        if not self.data_quality_requirement:
            raise DomainError("data_quality_requirement is required")


def _range_from_payload(payload: Mapping[str, Any]) -> MetricRange:
    return MetricRange(
        metric=parse_enum(Metric, payload["metric"], "metric"),
        unit=str(payload["unit"]),
        lower=None if payload.get("lower") is None else exact_decimal(payload["lower"], "lower"),
        upper=None if payload.get("upper") is None else exact_decimal(payload["upper"], "upper"),
        lower_inclusive=bool(payload.get("lower_inclusive", False)),
        upper_inclusive=bool(payload.get("upper_inclusive", False)),
    )


def _native_target(method: MethodologyId, target: str) -> FitzgeraldTarget | MaffetoneTarget:
    target_value = target
    if method is MethodologyId.FITZGERALD_80_20_RUNNING_V1:
        if not target_value.startswith("F80."):
            target_value = f"F80.{target_value}"
        return parse_enum(FitzgeraldTarget, target_value, "native_target")
    if method is MethodologyId.MAFFETONE_RUNNING_V1:
        if not target_value.startswith("MAF."):
            target_value = f"MAF.{target_value}"
        return parse_enum(MaffetoneTarget, target_value, "native_target")
    raise DomainError("legacy methodology cannot own a named native target")


def create(
    *,
    methodology_id: str,
    native_target: str,
    primary: Mapping[str, Any],
    derivation_ref: str,
    confidence: str,
    data_quality_requirement: str,
    secondary: Mapping[str, Any] | None = None,
    ceiling: Mapping[str, Any] | None = None,
    parameter_snapshot_ref: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    method = parse_enum(MethodologyId, methodology_id, "methodology_id")
    prescription = IntensityPrescription(
        methodology_id=method,
        native_target=_native_target(method, native_target),
        primary=_range_from_payload(primary),
        secondary=None if secondary is None else _range_from_payload(secondary),
        ceiling=None
        if ceiling is None
        else MetricCeiling(
            value=exact_decimal(ceiling["value"], "ceiling"),
            inclusive=bool(ceiling.get("inclusive", False)),
        ),
        parameter_snapshot_ref=parameter_snapshot_ref,
        derivation_ref=derivation_ref,
        confidence=parse_enum(Confidence, confidence, "confidence"),
        data_quality_requirement=data_quality_requirement,
    )
    result: dict[str, Any] = {
        "valid": True,
        "native_target": prescription.native_target.value,
        "canonical_units": True,
    }
    if prescription.ceiling is not None:
        result["ceiling"] = {
            "value": canonical_decimal(prescription.ceiling.value),
            "inclusive": prescription.ceiling.inclusive,
        }
    return result


def compare_semantics(*, left: str, right: str) -> dict[str, Any]:
    if left == right:
        return {"equivalent": True, "reason": "IDENTICAL_NATIVE_TARGET"}
    left_namespace = left.split(".", 1)[0]
    right_namespace = right.split(".", 1)[0]
    reason = (
        "NAMESPACED_NATIVE_TARGETS_DIFFER"
        if left_namespace != right_namespace
        else "NATIVE_TARGETS_DIFFER"
    )
    return {"equivalent": False, "reason": reason}
