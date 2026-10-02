"""Immutable workout leaves and bounded deterministic repeat expansion."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .domain import DomainError, LoadMode, MeasureRole, SegmentKind, Sport, parse_enum
from .prescriptions import IntensityPrescription


def _freeze_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_metadata(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_metadata(item) for item in value)
    if isinstance(value, (set, frozenset)):
        raise DomainError("unordered workout metadata is not supported")
    return value


@dataclass(frozen=True, slots=True)
class WorkoutSegment:
    kind: SegmentKind
    load_mode: LoadMode
    duration_seconds: int | None = None
    distance_metres: int | None = None
    duration_role: MeasureRole | None = None
    distance_role: MeasureRole | None = None
    purpose: str | None = None
    prescription: IntensityPrescription | None = None
    repeat_group: str | None = None
    repeat_iteration: int | None = None
    duration_conversion_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", parse_enum(SegmentKind, self.kind, "segment kind"))
        object.__setattr__(self, "load_mode", parse_enum(LoadMode, self.load_mode, "load mode"))
        if self.duration_role is not None:
            object.__setattr__(
                self,
                "duration_role",
                parse_enum(MeasureRole, self.duration_role, "duration role"),
            )
        if self.distance_role is not None:
            object.__setattr__(
                self,
                "distance_role",
                parse_enum(MeasureRole, self.distance_role, "distance role"),
            )
        if self.duration_seconds is not None and (
            not isinstance(self.duration_seconds, int)
            or isinstance(self.duration_seconds, bool)
            or self.duration_seconds <= 0
        ):
            raise DomainError("duration_seconds must be positive")
        if self.distance_metres is not None and (
            not isinstance(self.distance_metres, int)
            or isinstance(self.distance_metres, bool)
            or self.distance_metres <= 0
        ):
            raise DomainError("distance_metres must be positive")
        if self.kind is SegmentKind.NON_TRAINING and (
            self.load_mode is not LoadMode.OPEN or self.duration_seconds is not None
            or self.distance_metres is not None or self.prescription is not None
            or self.duration_role is not None or self.distance_role is not None
        ):
            raise DomainError("non-training leaves must be OPEN without measures or prescriptions")
        authoritative = sum(
            role is MeasureRole.AUTHORITATIVE
            for role in (self.duration_role, self.distance_role)
        )
        if self.load_mode is LoadMode.OPEN:
            if self.kind not in {SegmentKind.FREE_RUN, SegmentKind.EVENT, SegmentKind.NON_TRAINING}:
                raise DomainError("OPEN load is allowed only for free-run, event or non-training leaves")
            if authoritative:
                raise DomainError("OPEN load cannot have an authoritative duration or distance")
        else:
            if authoritative != 1:
                raise DomainError("a structured leaf requires exactly one authoritative measure")
            if self.load_mode is LoadMode.DURATION and self.duration_role is not MeasureRole.AUTHORITATIVE:
                raise DomainError("DURATION load requires authoritative duration")
            if self.load_mode is LoadMode.DURATION and self.duration_seconds is None:
                raise DomainError("DURATION load requires duration_seconds")
            if self.load_mode is LoadMode.DISTANCE and self.distance_role is not MeasureRole.AUTHORITATIVE:
                raise DomainError("DISTANCE load requires authoritative distance")
            if self.load_mode is LoadMode.DISTANCE and self.distance_metres is None:
                raise DomainError("DISTANCE load requires distance_metres")


@dataclass(frozen=True, slots=True)
class RepeatBlock:
    count: int
    children: tuple[WorkoutSegment, ...]
    group_id: str = "repeat-1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "children", tuple(self.children))
        if not isinstance(self.count, int) or isinstance(self.count, bool) or self.count <= 0:
            raise DomainError("repeat count must be positive")
        if not self.children:
            raise DomainError("repeat must contain at least one leaf")
        if any(not isinstance(child, WorkoutSegment) for child in self.children):
            raise DomainError("repeat children must be workout segment leaves")

    def expand(self) -> tuple[WorkoutSegment, ...]:
        return tuple(
            replace(child, repeat_group=self.group_id, repeat_iteration=iteration)
            for iteration in range(1, self.count + 1)
            for child in self.children
        )


@dataclass(frozen=True, slots=True)
class PlannedWorkout:
    scheduled_date: date
    sport: Sport
    family: str
    purpose: str
    description: str
    segments: tuple[WorkoutSegment, ...]
    event_flag: bool = False
    workout_id: str | None = None
    ordinal: int | None = None
    title: str | None = None
    phase: str | None = None
    quality_flag: bool = False
    long_run_flag: bool = False
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sport", parse_enum(Sport, self.sport, "sport"))
        object.__setattr__(self, "segments", tuple(self.segments))
        if not self.family or not self.purpose:
            raise DomainError("workout family and purpose are required")
        if not self.segments:
            raise DomainError("planned workout requires ordered leaf segments")
        if any(not isinstance(segment, WorkoutSegment) for segment in self.segments):
            raise DomainError("planned workout segments must be workout segment leaves")
        auxiliary = {Sport.STRENGTH: "STRENGTH", Sport.MOBILITY: "MOBILITY", Sport.REST: "REST"}
        if self.sport in auxiliary:
            if self.family != auxiliary[self.sport] or self.event_flag or self.quality_flag or self.long_run_flag:
                raise DomainError("auxiliary sport/family and flags must agree")
            if any(s.prescription is not None or s.distance_metres is not None for s in self.segments):
                raise DomainError("auxiliary sessions cannot have endurance prescriptions or distance")
            if self.sport is Sport.REST and any(s.kind is not SegmentKind.NON_TRAINING for s in self.segments):
                raise DomainError("rest requires explicit non-training leaves")
            if self.sport is not Sport.REST and any(s.kind is SegmentKind.NON_TRAINING or s.load_mode is not LoadMode.DURATION for s in self.segments):
                raise DomainError("strength and mobility require duration leaves")
        elif self.family in auxiliary.values() or any(s.kind is SegmentKind.NON_TRAINING for s in self.segments):
            raise DomainError("running cannot contain auxiliary families or non-training leaves")
        if self.ordinal is not None and (
            not isinstance(self.ordinal, int) or isinstance(self.ordinal, bool) or self.ordinal < 0
        ):
            raise DomainError("workout ordinal must be a non-negative integer")
        if self.metadata is not None:
            object.__setattr__(self, "metadata", _freeze_metadata(self.metadata))


def _role(value: Any) -> MeasureRole | None:
    return None if value is None else parse_enum(MeasureRole, value, "measure role")


def _leaf_from_mapping(payload: Mapping[str, Any]) -> WorkoutSegment:
    kind = parse_enum(SegmentKind, payload["kind"], "segment kind")
    if payload.get("load_mode") is not None:
        mode = parse_enum(LoadMode, payload["load_mode"], "load mode")
    elif payload.get("duration_seconds") is not None:
        mode = LoadMode.DURATION
    elif payload.get("distance_metres") is not None:
        mode = LoadMode.DISTANCE
    else:
        mode = LoadMode.OPEN
    duration_role = _role(payload.get("duration_role"))
    distance_role = _role(payload.get("distance_role"))
    if mode is LoadMode.DURATION and duration_role is None:
        duration_role = MeasureRole.AUTHORITATIVE
    if mode is LoadMode.DISTANCE and distance_role is None:
        distance_role = MeasureRole.AUTHORITATIVE
    return WorkoutSegment(
        kind=kind,
        load_mode=mode,
        duration_seconds=payload.get("duration_seconds"),
        distance_metres=payload.get("distance_metres"),
        duration_role=duration_role,
        distance_role=distance_role,
        purpose=payload.get("purpose"),
        duration_conversion_ref=(payload.get("duration_conversion") or {}).get("version")
        if isinstance(payload.get("duration_conversion"), Mapping)
        else payload.get("duration_conversion"),
    )


def expand_repeats(*, authoring: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not authoring:
        raise DomainError("segment authoring requires at least one leaf or repeat")
    leaves: list[WorkoutSegment] = []
    for ordinal, item in enumerate(authoring, start=1):
        if "repeat" not in item:
            leaves.append(_leaf_from_mapping(item))
            continue
        if any("repeat" in child for child in item.get("children", [])):
            raise DomainError("nested repetition is not supported")
        block = RepeatBlock(
            count=int(item["repeat"]),
            children=tuple(_leaf_from_mapping(child) for child in item.get("children", [])),
            group_id=f"repeat-{ordinal}",
        )
        leaves.extend(block.expand())
    return {
        "leaf_kinds": [leaf.kind.value for leaf in leaves],
        "repeat_iterations": [leaf.repeat_iteration for leaf in leaves],
        "authoritative": "EXPANDED_LEAVES",
    }


def validate_leaf(**payload: Any) -> dict[str, Any]:
    roles = (payload.get("duration_role"), payload.get("distance_role"))
    if roles.count("AUTHORITATIVE") > 1:
        return {"valid": False, "reason": "BOTH_MEASURES_AUTHORITATIVE"}
    leaf = _leaf_from_mapping(payload)
    result: dict[str, Any] = {"valid": True}
    if leaf.load_mode is LoadMode.DURATION:
        result["authoritative_measure"] = "DURATION"
    elif leaf.load_mode is LoadMode.DISTANCE:
        result["authoritative_measure"] = "DISTANCE"
        if (
            payload.get("methodology_id") == "FITZGERALD_80_20_RUNNING_V1"
            and leaf.duration_seconds is None
            and leaf.duration_conversion_ref is None
        ):
            result.update({"accounting_status": "UNACCOUNTED", "invented_duration": False})
    else:
        result["authoritative_measure"] = "OPEN"
    return result
