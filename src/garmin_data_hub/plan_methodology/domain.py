"""Shared enums and exact value helpers for the planning domain."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import TypeVar


class DomainError(ValueError):
    """Raised when a pure-domain invariant is violated."""


class _StableStringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class Sport(_StableStringEnum):
    RUNNING = "RUNNING"
    STRENGTH = "STRENGTH"
    MOBILITY = "MOBILITY"
    REST = "REST"


class GoalIntent(_StableStringEnum):
    COMPLETION = "COMPLETION"
    PERFORMANCE = "PERFORMANCE"


class MethodologyId(_StableStringEnum):
    FITZGERALD_80_20_RUNNING_V1 = "FITZGERALD_80_20_RUNNING_V1"
    MAFFETONE_RUNNING_V1 = "MAFFETONE_RUNNING_V1"
    LEGACY_UNSPECIFIED = "legacy_unspecified"


class Metric(_StableStringEnum):
    # PACE is retained as a vocabulary value; numeric pace prescriptions are
    # canonically represented as SPEED in metres/second.
    PACE = "PACE"
    SPEED = "SPEED"
    HEART_RATE = "HEART_RATE"
    POWER = "POWER"


class EvidenceClass(_StableStringEnum):
    MEASURED = "MEASURED"
    DERIVED = "DERIVED"
    ESTIMATED = "ESTIMATED"
    UNAVAILABLE = "UNAVAILABLE"


class DataQuality(_StableStringEnum):
    VALID = "VALID"
    PARTIAL = "PARTIAL"
    INSUFFICIENT = "INSUFFICIENT"
    UNAVAILABLE = "UNAVAILABLE"


class Confidence(_StableStringEnum):
    HIGH = "HIGH"
    REDUCED = "REDUCED"
    UNKNOWN = "UNKNOWN"


class RevisionReason(_StableStringEnum):
    INITIAL_GENERATION = "INITIAL_GENERATION"
    LEGACY_CONVERSION = "LEGACY_CONVERSION"
    METHODOLOGY_CHANGE = "METHODOLOGY_CHANGE"
    GOAL_CHANGE = "GOAL_CHANGE"
    ATHLETE_STATE_CHANGE = "ATHLETE_STATE_CHANGE"
    PRESCRIPTION_EDIT = "PRESCRIPTION_EDIT"
    ADAPTATION = "ADAPTATION"
    REGENERATION = "REGENERATION"


class ApprovalState(_StableStringEnum):
    CANDIDATE = "CANDIDATE"
    VALIDATED = "VALIDATED"
    APPROVED = "APPROVED"


class FitzgeraldTarget(_StableStringEnum):
    ZONE_1 = "F80.ZONE_1"
    ZONE_2 = "F80.ZONE_2"
    ZONE_X = "F80.ZONE_X"
    ZONE_3 = "F80.ZONE_3"
    ZONE_Y = "F80.ZONE_Y"
    ZONE_4 = "F80.ZONE_4"
    ZONE_5 = "F80.ZONE_5"


class FitzgeraldCategory(_StableStringEnum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    UNACCOUNTED = "UNACCOUNTED"


class MaffetoneTarget(_StableStringEnum):
    MAF_AEROBIC_RANGE = "MAF.MAF_AEROBIC_RANGE"
    MAF_CEILING = "MAF.MAF_CEILING"
    SUB_MAF = "MAF.SUB_MAF"


class SegmentKind(_StableStringEnum):
    WARMUP = "WARMUP"
    WORK = "WORK"
    RECOVERY = "RECOVERY"
    STRIDE = "STRIDE"
    COOLDOWN = "COOLDOWN"
    FREE_RUN = "FREE_RUN"
    EVENT = "EVENT"
    NON_TRAINING = "NON_TRAINING"


class LoadMode(_StableStringEnum):
    DURATION = "DURATION"
    DISTANCE = "DISTANCE"
    OPEN = "OPEN"


class MeasureRole(_StableStringEnum):
    AUTHORITATIVE = "AUTHORITATIVE"
    ESTIMATED = "ESTIMATED"


EnumT = TypeVar("EnumT", bound=Enum)


def parse_enum(enum_type: type[EnumT], value: EnumT | str, field_name: str) -> EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise DomainError(f"invalid {field_name}: {value!r}") from exc


def exact_decimal(value: Decimal | int | str, field_name: str) -> Decimal:
    """Create an exact finite decimal without accepting binary floats."""
    if isinstance(value, bool) or isinstance(value, float):
        raise DomainError(f"{field_name} must be an exact decimal or integer")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DomainError(f"invalid {field_name}: {value!r}") from exc
    if not result.is_finite():
        raise DomainError(f"{field_name} must be finite")
    return result


def canonical_decimal(value: Decimal | int | str) -> str:
    decimal_value = exact_decimal(value, "decimal")
    if decimal_value == 0:
        return "0"
    rendered = format(decimal_value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered
