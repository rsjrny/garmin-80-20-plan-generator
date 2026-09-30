"""Immutable, privacy-minimized generation input snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Mapping

from .domain import (
    DataQuality,
    DomainError,
    EvidenceClass,
    GoalIntent,
    MethodologyId,
    Metric,
    Sport,
    exact_decimal,
    parse_enum,
)


def _parse_date(value: date | str | None, field_name: str, *, required: bool = False) -> date | None:
    if value is None:
        if required:
            raise DomainError(f"{field_name} is required")
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DomainError(f"invalid {field_name}: {value!r}") from exc


@dataclass(frozen=True, slots=True)
class GoalSnapshot:
    sport: Sport
    goal_intent: GoalIntent
    event_type: str | None = None
    event_distance_m: int | None = None
    event_date: date | None = None
    plan_start_date: date | None = None
    original_event_label: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sport", parse_enum(Sport, self.sport, "sport"))
        object.__setattr__(
            self, "goal_intent", parse_enum(GoalIntent, self.goal_intent, "goal_intent")
        )
        if self.event_distance_m is not None and self.event_distance_m <= 0:
            raise DomainError("event_distance_m must be positive")
        if self.event_date and self.plan_start_date and self.event_date < self.plan_start_date:
            raise DomainError("event_date cannot precede plan_start_date")


@dataclass(frozen=True, slots=True)
class SnapshotFact:
    name: str
    value: str | int
    unit: str | None = None
    as_of: date | None = None
    provenance: str | None = None
    quality: DataQuality | None = None
    derivation_version: str | None = None


@dataclass(frozen=True, slots=True)
class AthleteStateSnapshot:
    facts: tuple[SnapshotFact, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "facts", tuple(self.facts))
        if any(not isinstance(fact, SnapshotFact) for fact in self.facts):
            raise DomainError("athlete snapshot facts must be SnapshotFact values")
        names = [fact.name for fact in self.facts]
        if len(names) != len(set(names)):
            raise DomainError("athlete snapshot fact names must be unique")

    def get(self, name: str) -> SnapshotFact | None:
        return next((fact for fact in self.facts if fact.name == name), None)


@dataclass(frozen=True, slots=True)
class ThresholdSnapshot:
    metric: Metric
    value: Decimal | None
    unit: str
    sport: Sport
    method_or_protocol: str | None
    source_record: str | None
    observed_at: date | None
    evidence_class: EvidenceClass
    quality: DataQuality
    derivation_version: str | None = None
    derivation_approved: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "metric", parse_enum(Metric, self.metric, "metric"))
        object.__setattr__(self, "sport", parse_enum(Sport, self.sport, "sport"))
        object.__setattr__(
            self,
            "evidence_class",
            parse_enum(EvidenceClass, self.evidence_class, "evidence_class"),
        )
        object.__setattr__(self, "quality", parse_enum(DataQuality, self.quality, "quality"))
        if self.value is not None:
            parsed = exact_decimal(self.value, "threshold value")
            if parsed <= 0:
                raise DomainError("threshold value must be positive")
            object.__setattr__(self, "value", parsed)
        expected_unit = {
            Metric.PACE: "m/s",
            Metric.SPEED: "m/s",
            Metric.HEART_RATE: "bpm",
            Metric.POWER: "W",
        }[self.metric]
        if self.unit != expected_unit:
            raise DomainError(f"{self.metric.value} threshold requires unit {expected_unit!r}")
        if self.evidence_class is EvidenceClass.UNAVAILABLE and self.value is not None:
            raise DomainError("unavailable threshold cannot carry a numeric value")

    @property
    def prescription_grade(self) -> bool:
        if (
            self.value is None
            or self.sport is not Sport.RUNNING
            or self.quality is not DataQuality.VALID
            or self.observed_at is None
        ):
            return False
        if self.evidence_class is EvidenceClass.MEASURED:
            return True
        return self.evidence_class is EvidenceClass.DERIVED and self.derivation_approved


ALLOWED_MAF_ADJUSTMENTS = frozenset({-10, -5, 0, 5})


@dataclass(frozen=True, slots=True)
class MaffetoneParameterSnapshot:
    formula_version: str
    calculation_date: date
    completed_age: int
    selected_adjustment: int
    confirmed: bool
    ceiling_bpm: int
    lower_bpm: int
    provenance: str
    age_provenance: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.completed_age, int) or isinstance(self.completed_age, bool):
            raise DomainError("completed_age must be an integer")
        if self.completed_age < 0:
            raise DomainError("completed_age cannot be negative")
        if self.completed_age <= 16:
            raise DomainError("ordinary MAF snapshot is unavailable for age <= 16")
        if (
            not isinstance(self.selected_adjustment, int)
            or isinstance(self.selected_adjustment, bool)
            or self.selected_adjustment not in ALLOWED_MAF_ADJUSTMENTS
        ):
            raise DomainError("selected_adjustment must be one of -10, -5, 0, +5")
        if not self.confirmed:
            raise DomainError("MAF adjustment requires explicit confirmation")
        if self.provenance != "USER_SELECTED":
            raise DomainError("ordinary MAF adjustment provenance must be USER_SELECTED")
        expected_ceiling = 180 - self.completed_age + self.selected_adjustment
        if self.ceiling_bpm != expected_ceiling or self.lower_bpm != expected_ceiling - 10:
            raise DomainError("MAF bounds do not match the frozen formula inputs")


def freeze_goal(
    *,
    sport: str,
    goal_intent: str,
    event_type: str | None = None,
    event_distance_m: int | None = None,
    event_date: str | None = None,
    plan_start_date: str | None = None,
    methodology_id: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    try:
        parsed_sport = parse_enum(Sport, sport, "sport")
    except DomainError:
        if methodology_id in {
            MethodologyId.FITZGERALD_80_20_RUNNING_V1.value,
            MethodologyId.MAFFETONE_RUNNING_V1.value,
        }:
            return {"accepted": False, "reason": "UNSUPPORTED_SPORT_FOR_METHODOLOGY"}
        raise
    if methodology_id in {
        MethodologyId.FITZGERALD_80_20_RUNNING_V1.value,
        MethodologyId.MAFFETONE_RUNNING_V1.value,
    } and parsed_sport is not Sport.RUNNING:
        return {"accepted": False, "reason": "UNSUPPORTED_SPORT_FOR_METHODOLOGY"}
    snapshot = GoalSnapshot(
        sport=parsed_sport,
        goal_intent=parse_enum(GoalIntent, goal_intent, "goal_intent"),
        event_type=event_type,
        event_distance_m=event_distance_m,
        event_date=_parse_date(event_date, "event_date"),
        plan_start_date=_parse_date(plan_start_date, "plan_start_date"),
    )
    return {
        "sport": snapshot.sport.value,
        "goal_intent": snapshot.goal_intent.value,
        "immutable": True,
    }


def compare_goal_intents(*, methodology_id: str, intents: list[str]) -> dict[str, Any]:
    parse_enum(MethodologyId, methodology_id, "methodology_id")
    for intent in intents:
        parse_enum(GoalIntent, intent, "goal_intent")
    return {"methodology_id_unchanged": True}


def freeze_athlete_state(
    *, profile: Mapping[str, Any], consumed_fields: list[str]
) -> dict[str, Any]:
    facts: list[SnapshotFact] = []
    result: dict[str, Any] = {}
    for field_name in consumed_fields:
        if field_name not in profile:
            raise DomainError(f"consumed athlete field is missing: {field_name}")
        raw_value = profile[field_name]
        # Preserve the copied fact's exact textual representation. Decimal
        # normalization happens only at the canonical serialization boundary.
        value = str(raw_value)
        facts.append(SnapshotFact(name=field_name, value=value))
        result[field_name] = value
    AthleteStateSnapshot(tuple(facts))
    result.update({"contains_live_profile_reference": False, "immutable": True})
    return result


def freeze_threshold(*, pace: Mapping[str, Any], lthr: Mapping[str, Any]) -> dict[str, Any]:
    def prescription_grade(item: Mapping[str, Any]) -> bool:
        evidence = item.get("evidence_class")
        reliable_class = evidence == "MEASURED" or (
            evidence == "DERIVED" and item.get("derivation_approved", True)
        )
        quality_valid = item.get("quality", "VALID") == "VALID"
        return reliable_class and quality_valid and item.get("value") is not None

    allowed = any(prescription_grade(item) for item in (pace, lthr))
    return {
        "named_numeric_prescription_allowed": allowed,
        "invented_targets": False,
    }


def freeze_maffetone_parameters(
    *,
    formula_version: str,
    calculation_date: str,
    completed_age: int,
    selected_adjustment: int,
    confirmed: bool,
    ceiling: int,
    lower_bound: int,
    provenance: str,
    **_: Any,
) -> dict[str, Any]:
    snapshot = MaffetoneParameterSnapshot(
        formula_version=formula_version,
        calculation_date=_parse_date(calculation_date, "calculation_date", required=True),  # type: ignore[arg-type]
        completed_age=completed_age,
        selected_adjustment=selected_adjustment,
        confirmed=confirmed,
        ceiling_bpm=ceiling,
        lower_bpm=lower_bound,
        provenance=provenance,
    )
    return {
        "completed_age": snapshot.completed_age,
        "ceiling": snapshot.ceiling_bpm,
        "lower_bound": snapshot.lower_bpm,
        "raw_health_answers_persisted": False,
        "immutable": True,
    }
