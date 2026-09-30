"""Strict V2 AI composition boundary for plan revision candidates.

AI output is treated as untrusted authoring input.  This module accepts only
product-owned workout families and application-provided prescription template
identifiers, expands bounded repeats, and constructs the existing immutable
domain values.  Methodology calculations and approval remain outside the AI
contract.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime
import json
import re
from types import MappingProxyType
from typing import Any, Mapping, Sequence
import unicodedata

from .domain import (
    ApprovalState,
    DomainError,
    LoadMode,
    MeasureRole,
    MethodologyId,
    RevisionReason,
    SegmentKind,
    Sport,
    parse_enum,
)
from .prescriptions import IntensityPrescription
from .revisions import PlanRevisionCandidate
from .segments import PlannedWorkout, WorkoutSegment


AI_PROPOSAL_SCHEMA = "garmin-data-hub.plan-proposal"
AI_PROPOSAL_VERSION = 2
AI_PROPOSAL_PARSER_VERSION = "plan-proposal-parser.v2.0.0"
WORKOUT_VOCABULARY_VERSION = "data-hub-workout-families.v1"

MAX_PAYLOAD_BYTES = 1_000_000
MAX_WORKOUTS = 366 * 3
MAX_AUTHORED_SEGMENTS_PER_WORKOUT = 64
MAX_EXPANDED_SEGMENTS_PER_WORKOUT = 256
MAX_TEXT_LENGTH = 4_000
MAX_ID_LENGTH = 128
MAX_JSON_NESTING = 8

WORKOUT_FAMILIES = frozenset(
    {
        "RECOVERY",
        "AEROBIC",
        "LONG_AEROBIC",
        "MODERATE_DEVELOPMENT",
        "HIGH_INTENSITY_INTERVALS",
        "RACE_SPECIFIC",
        "EVENT_DAY",
        "STRENGTH",
        "MOBILITY",
        "REST",
        "CUSTOM_STRUCTURED",
    }
)
RUNNING_WORKOUT_FAMILIES = WORKOUT_FAMILIES - {"STRENGTH", "MOBILITY", "REST"}

# These names are rejected anywhere in model-authored structure.  Closed-key
# validation catches all other unknown fields, but this list gives authority
# violations a stable, explicit diagnostic instead of silently dropping them.
FORBIDDEN_AUTHORITY_FIELDS = frozenset(
    {
        "methodology_id",
        "methodology_id_override",
        "methodology_version",
        "threshold_pace",
        "threshold_speed_mps",
        "lthr",
        "lthr_bpm",
        "ftp",
        "ftp_w",
        "zone_bounds",
        "raw_lower_bound",
        "raw_upper_bound",
        "maf_adjustment",
        "selected_adjustment",
        "maf_ceiling",
        "maf_ceiling_bpm",
        "ceiling_bpm",
        "maf_lower_range",
        "lower_bound",
        "higher_intensity_state",
        "state",
        "state_transition",
        "safety_override",
        "safety_waiver",
        "validation_outcome",
        "validation_status",
        "compliance_result",
        "compliance_score",
        "approval",
        "approval_flag",
        "approval_state",
        "active_revision_id",
        "current_revision_id",
        "content_hash",
        "revision_hash",
        "expected_hash",
        "max_repeat_count",
        "max_duration_seconds",
        "max_distance_metres",
        "max_workouts",
        "max_segments",
        "allowed_template_ids",
        "prescription",
        "provider",
        "model",
        "generator_version",
        "parser_version",
        "validation_version",
        "timestamp",
        "approved_by",
    }
)

NUMERIC_AUTHORITY_FIELDS = frozenset(
    {
        "threshold_pace",
        "threshold_speed_mps",
        "lthr",
        "lthr_bpm",
        "ftp",
        "ftp_w",
        "zone_bounds",
        "raw_lower_bound",
        "raw_upper_bound",
        "maf_ceiling",
        "maf_ceiling_bpm",
        "ceiling_bpm",
        "maf_lower_range",
        "lower_bound",
    }
)

METHODOLOGY_AUTHORITY_FIELDS = FORBIDDEN_AUTHORITY_FIELDS - NUMERIC_AUTHORITY_FIELDS

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema",
        "version",
        "candidate_id",
        "parent_revision_id",
        "parent_content_hash",
        "workouts",
        "rationale",
        "warnings",
    }
)
_TOP_LEVEL_REQUIRED = frozenset(
    {
        "schema",
        "version",
        "candidate_id",
        "parent_revision_id",
        "parent_content_hash",
        "workouts",
    }
)
_WORKOUT_KEYS = frozenset(
    {
        "id",
        "ordinal",
        "date",
        "sport",
        "family",
        "purpose",
        "description",
        "segments",
        "title",
        "phase",
        "event_flag",
        "quality_flag",
        "long_run_flag",
    }
)
_WORKOUT_REQUIRED = frozenset(
    {"id", "ordinal", "date", "sport", "family", "purpose", "description", "segments"}
)
_LEAF_KEYS = frozenset(
    {
        "id",
        "ordinal",
        "kind",
        "load_mode",
        "duration_seconds",
        "distance_metres",
        "purpose",
        "template_id",
    }
)
_LEAF_REQUIRED = frozenset(
    {"id", "ordinal", "kind", "load_mode", "purpose", "template_id"}
)
_REPEAT_KEYS = frozenset({"id", "ordinal", "repeat_count", "children"})
_REPEAT_REQUIRED = _REPEAT_KEYS
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class ProposalValidationError(DomainError):
    """Raised when untrusted proposal structure cannot become a candidate."""

    def __init__(self, code: str, message: str, *, path: str = "$") -> None:
        super().__init__(f"{code} at {path}: {message}")
        self.code = code
        self.path = path
        self.detail = message


@dataclass(frozen=True, slots=True)
class IntegerBounds:
    """Application-owned inclusive bounds for one compositional integer."""

    minimum: int
    maximum: int

    def __post_init__(self) -> None:
        if any(isinstance(value, bool) or not isinstance(value, int) for value in (self.minimum, self.maximum)):
            raise DomainError("integer bounds require exact integers")
        if self.minimum < 1 or self.maximum < self.minimum:
            raise DomainError("integer bounds must be positive and ordered")

    def require(self, value: Any, field_name: str, path: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ProposalValidationError(
                "WRONG_TYPE", f"{field_name} must be an exact integer", path=path
            )
        if not self.minimum <= value <= self.maximum:
            raise ProposalValidationError(
                "VALUE_OUT_OF_BOUNDS",
                f"{field_name} must be between {self.minimum} and {self.maximum}",
                path=path,
            )
        return value


@dataclass(frozen=True, slots=True)
class ProposalBounds:
    """Frozen deterministic scheduling and compositional limits sent to AI."""

    window_start: date
    window_end: date
    allowed_families: frozenset[str]
    allowed_template_ids: frozenset[str]
    duration_seconds: IntegerBounds
    distance_metres: IntegerBounds
    repeat_count: IntegerBounds
    max_workouts: int = MAX_WORKOUTS
    max_authored_segments_per_workout: int = MAX_AUTHORED_SEGMENTS_PER_WORKOUT
    max_expanded_segments_per_workout: int = MAX_EXPANDED_SEGMENTS_PER_WORKOUT

    def __post_init__(self) -> None:
        object.__setattr__(self, "allowed_families", frozenset(self.allowed_families))
        object.__setattr__(self, "allowed_template_ids", frozenset(self.allowed_template_ids))
        if self.window_end < self.window_start:
            raise DomainError("proposal window end cannot precede its start")
        if not self.allowed_families or not self.allowed_families <= WORKOUT_FAMILIES:
            raise DomainError("allowed families must be a non-empty product vocabulary subset")
        if not self.allowed_template_ids:
            raise DomainError("at least one prescription template must be allowed")
        if any(
            not isinstance(template_id, str) or not _ID_RE.fullmatch(template_id)
            for template_id in self.allowed_template_ids
        ):
            raise DomainError("allowed prescription template IDs must be canonical IDs")
        for value, hard_limit, name in (
            (self.max_workouts, MAX_WORKOUTS, "max_workouts"),
            (
                self.max_authored_segments_per_workout,
                MAX_AUTHORED_SEGMENTS_PER_WORKOUT,
                "max_authored_segments_per_workout",
            ),
            (
                self.max_expanded_segments_per_workout,
                MAX_EXPANDED_SEGMENTS_PER_WORKOUT,
                "max_expanded_segments_per_workout",
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= hard_limit:
                raise DomainError(f"{name} exceeds the application resource limit")
        if self.repeat_count.maximum > self.max_expanded_segments_per_workout:
            raise DomainError("repeat bounds exceed the expanded-segment resource limit")


@dataclass(frozen=True, slots=True)
class PrescriptionTemplateCatalog:
    """Application-owned mapping from opaque IDs to typed prescriptions."""

    methodology_id: MethodologyId | str
    templates: Mapping[str, IntensityPrescription]

    def __post_init__(self) -> None:
        method = parse_enum(MethodologyId, self.methodology_id, "methodology_id")
        if not isinstance(self.templates, Mapping):
            raise DomainError("prescription templates must be a mapping")
        frozen: dict[str, IntensityPrescription] = {}
        for template_id, prescription in self.templates.items():
            if not isinstance(template_id, str) or not _ID_RE.fullmatch(template_id):
                raise DomainError(f"invalid prescription template ID: {template_id!r}")
            if template_id in frozen:
                raise DomainError(f"duplicate prescription template ID: {template_id!r}")
            if not isinstance(prescription, IntensityPrescription):
                raise DomainError("prescription templates must contain typed prescriptions")
            if prescription.methodology_id is not method:
                raise DomainError("prescription template belongs to a foreign methodology")
            frozen[template_id] = deepcopy(prescription)
        if not frozen:
            raise DomainError("prescription template catalog cannot be empty")
        object.__setattr__(self, "methodology_id", method)
        object.__setattr__(self, "templates", MappingProxyType(frozen))

    def resolve(self, template_id: str, allowed_ids: frozenset[str], path: str) -> IntensityPrescription:
        if template_id not in allowed_ids:
            raise ProposalValidationError(
                "UNKNOWN_PRESCRIPTION_TEMPLATE",
                f"template {template_id!r} was not supplied in the deterministic envelope",
                path=path,
            )
        try:
            return self.templates[template_id]
        except KeyError as exc:
            raise ProposalValidationError(
                "UNKNOWN_PRESCRIPTION_TEMPLATE",
                f"application template {template_id!r} is unavailable",
                path=path,
            ) from exc


@dataclass(frozen=True, slots=True)
class AIProposalV2:
    candidate_id: str
    parent_revision_id: str | None
    parent_content_hash: str | None
    workouts: tuple[PlannedWorkout, ...]
    rationale: str | None = None
    warnings: tuple[str, ...] = ()
    schema: str = AI_PROPOSAL_SCHEMA
    version: int = AI_PROPOSAL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "workouts", tuple(self.workouts))
        object.__setattr__(self, "warnings", tuple(self.warnings))


@dataclass(frozen=True, slots=True)
class AIProvenance:
    """Non-sensitive application-observed generation metadata."""

    generator_version: str
    prompt_template_version: str
    generated_at: datetime
    provider: str | None = None
    tool: str | None = None
    model: str | None = None
    request_id: str | None = None

    def __post_init__(self) -> None:
        if not self.generator_version or not self.prompt_template_version:
            raise DomainError("generator and prompt template versions are required")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise DomainError("AI generation timestamp must include a timezone")

    def to_revision_value(self) -> dict[str, Any]:
        return {
            "proposal_schema": AI_PROPOSAL_SCHEMA,
            "proposal_version": AI_PROPOSAL_VERSION,
            "generator_version": self.generator_version,
            "provider": self.provider,
            "tool": self.tool,
            "model": self.model,
            "request_id": self.request_id,
            "generated_at": self.generated_at,
            "prompt_template_version": self.prompt_template_version,
            "parser_validator_version": AI_PROPOSAL_PARSER_VERSION,
            "workout_vocabulary_version": WORKOUT_VOCABULARY_VERSION,
        }


def _duplicate_rejecting_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProposalValidationError(
                "DUPLICATE_JSON_KEY", f"duplicate JSON key {key!r} is not allowed"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ProposalValidationError(
        "NONFINITE_NUMBER", f"non-finite JSON number {value!r} is not allowed"
    )


def _load_document(payload: Mapping[str, Any] | str | bytes) -> dict[str, Any]:
    if isinstance(payload, bytes):
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ProposalValidationError("PAYLOAD_TOO_LARGE", "proposal exceeds 1,000,000 bytes")
        try:
            raw = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProposalValidationError("INVALID_UTF8", "proposal must be UTF-8") from exc
    elif isinstance(payload, str):
        if len(payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ProposalValidationError("PAYLOAD_TOO_LARGE", "proposal exceeds 1,000,000 bytes")
        raw = payload
    elif isinstance(payload, Mapping):
        try:
            raw = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        except (RecursionError, TypeError, ValueError) as exc:
            raise ProposalValidationError("MALFORMED_STRUCTURE", "proposal is not JSON-safe") from exc
    else:
        raise ProposalValidationError("WRONG_TYPE", "proposal must be an object or JSON object")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=_reject_json_constant,
        )
    except ProposalValidationError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ProposalValidationError("MALFORMED_JSON", "proposal is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ProposalValidationError("WRONG_TYPE", "proposal root must be an object")
    if _nesting_depth(value) > MAX_JSON_NESTING:
        raise ProposalValidationError("EXCESSIVE_NESTING", "proposal nesting exceeds the resource limit")
    return value


def _nesting_depth(value: Any) -> int:
    maximum = 0
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        maximum = max(maximum, depth)
        if maximum > MAX_JSON_NESTING:
            return maximum
        if isinstance(item, Mapping):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
    return maximum


def _forbidden_fields(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in FORBIDDEN_AUTHORITY_FIELDS and key not in found:
                found.append(key)
            _collect_forbidden(item, found)
    elif isinstance(value, list):
        for item in value:
            _collect_forbidden(item, found)
    return found


def _collect_forbidden(value: Any, found: list[str]) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in FORBIDDEN_AUTHORITY_FIELDS and key not in found:
                found.append(key)
            _collect_forbidden(item, found)
    elif isinstance(value, list):
        for item in value:
            _collect_forbidden(item, found)


def _closed_keys(
    value: Any,
    *,
    allowed: frozenset[str],
    required: frozenset[str],
    path: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProposalValidationError("WRONG_TYPE", "value must be an object", path=path)
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProposalValidationError(
            "UNKNOWN_PROPOSAL_FIELD", f"unknown fields: {', '.join(unknown)}", path=path
        )
    missing = sorted(required - set(value))
    if missing:
        raise ProposalValidationError(
            "MISSING_REQUIRED_FIELD", f"missing fields: {', '.join(missing)}", path=path
        )
    return value


def _text(value: Any, field_name: str, path: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ProposalValidationError("WRONG_TYPE", f"{field_name} must be text", path=path)
    if len(value) > MAX_TEXT_LENGTH:
        raise ProposalValidationError("TEXT_TOO_LONG", f"{field_name} exceeds {MAX_TEXT_LENGTH} characters", path=path)
    if _CONTROL_RE.search(value) or any(
        unicodedata.category(character) in {"Cf", "Cs"}
        for character in value
    ):
        raise ProposalValidationError("CONTROL_CHARACTER", f"{field_name} contains control characters", path=path)
    if not allow_empty and not value.strip():
        raise ProposalValidationError("EMPTY_TEXT", f"{field_name} cannot be empty", path=path)
    return value


def _identifier(value: Any, field_name: str, path: str) -> str:
    text = _text(value, field_name, path)
    if len(text) > MAX_ID_LENGTH or not _ID_RE.fullmatch(text):
        raise ProposalValidationError("INVALID_IDENTIFIER", f"invalid {field_name}", path=path)
    return text


def _ordinal(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProposalValidationError("INVALID_ORDINAL", "ordinal must be a non-negative integer", path=path)
    return value


def _boolean(value: Any, field_name: str, path: str) -> bool:
    if not isinstance(value, bool):
        raise ProposalValidationError("WRONG_TYPE", f"{field_name} must be boolean", path=path)
    return value


def _canonical_date(value: Any, path: str) -> date:
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ProposalValidationError("NONCANONICAL_DATE", "date must use YYYY-MM-DD", path=path)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ProposalValidationError("INVALID_DATE", "date is not a calendar date", path=path) from exc
    if parsed.isoformat() != value:
        raise ProposalValidationError("NONCANONICAL_DATE", "date is not canonical", path=path)
    return parsed


def _exact_sequence(value: Any, field_name: str, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise ProposalValidationError("WRONG_TYPE", f"{field_name} must be an array", path=path)
    return value


def _check_contiguous(values: Sequence[int], path: str) -> None:
    if len(values) != len(set(values)):
        raise ProposalValidationError("DUPLICATE_ORDINAL", "ordinals must be unique", path=path)
    if sorted(values) != list(range(len(values))):
        raise ProposalValidationError("NONCONTIGUOUS_ORDINAL", "ordinals must be contiguous from zero", path=path)


def _leaf(
    raw: Any,
    *,
    path: str,
    bounds: ProposalBounds,
    catalog: PrescriptionTemplateCatalog,
    seen_ids: set[str],
) -> tuple[WorkoutSegment, int]:
    item = _closed_keys(raw, allowed=_LEAF_KEYS, required=_LEAF_REQUIRED, path=path)
    segment_id = _identifier(item["id"], "segment id", f"{path}.id")
    if segment_id in seen_ids:
        raise ProposalValidationError("DUPLICATE_ID", f"duplicate ID {segment_id!r}", path=f"{path}.id")
    seen_ids.add(segment_id)
    ordinal = _ordinal(item["ordinal"], f"{path}.ordinal")
    try:
        kind = parse_enum(SegmentKind, item["kind"], "segment kind")
        mode = parse_enum(LoadMode, item["load_mode"], "load mode")
    except DomainError as exc:
        raise ProposalValidationError("UNKNOWN_ENUM", str(exc), path=path) from exc
    purpose = _text(item["purpose"], "segment purpose", f"{path}.purpose")
    template_id = _identifier(item["template_id"], "template ID", f"{path}.template_id")
    prescription = catalog.resolve(template_id, bounds.allowed_template_ids, f"{path}.template_id")

    duration = item.get("duration_seconds")
    distance = item.get("distance_metres")
    if mode is LoadMode.DURATION:
        if duration is None or distance is not None:
            raise ProposalValidationError(
                "AMBIGUOUS_LOAD", "DURATION requires only duration_seconds", path=path
            )
        duration = bounds.duration_seconds.require(duration, "duration_seconds", f"{path}.duration_seconds")
        duration_role = MeasureRole.AUTHORITATIVE
        distance_role = None
    elif mode is LoadMode.DISTANCE:
        if distance is None or duration is not None:
            raise ProposalValidationError(
                "AMBIGUOUS_LOAD", "DISTANCE requires only distance_metres", path=path
            )
        distance = bounds.distance_metres.require(distance, "distance_metres", f"{path}.distance_metres")
        duration_role = None
        distance_role = MeasureRole.AUTHORITATIVE
    else:
        if duration is not None or distance is not None:
            raise ProposalValidationError("AMBIGUOUS_LOAD", "OPEN cannot carry numeric load", path=path)
        if kind not in {SegmentKind.FREE_RUN, SegmentKind.EVENT}:
            raise ProposalValidationError(
                "INVALID_OPEN_SEGMENT", "OPEN is allowed only for FREE_RUN or EVENT", path=path
            )
        duration_role = distance_role = None

    return (
        WorkoutSegment(
            kind=kind,
            load_mode=mode,
            duration_seconds=duration,
            distance_metres=distance,
            duration_role=duration_role,
            distance_role=distance_role,
            purpose=purpose,
            prescription=prescription,
        ),
        ordinal,
    )


def _segments(
    raw: Any,
    *,
    path: str,
    bounds: ProposalBounds,
    catalog: PrescriptionTemplateCatalog,
    seen_ids: set[str],
) -> tuple[WorkoutSegment, ...]:
    items = _exact_sequence(raw, "segments", path)
    if not items:
        raise ProposalValidationError("EMPTY_SEGMENTS", "workout requires at least one segment", path=path)
    if len(items) > bounds.max_authored_segments_per_workout:
        raise ProposalValidationError("TOO_MANY_SEGMENTS", "authored segment count exceeds the resource limit", path=path)
    expanded: list[WorkoutSegment] = []
    ordinals: list[int] = []
    authored_leaf_count = 0
    for index, raw_item in enumerate(items):
        item_path = f"{path}[{index}]"
        if not isinstance(raw_item, Mapping):
            raise ProposalValidationError("WRONG_TYPE", "segment must be an object", path=item_path)
        if "repeat_count" not in raw_item:
            authored_leaf_count += 1
            if authored_leaf_count > bounds.max_authored_segments_per_workout:
                raise ProposalValidationError(
                    "TOO_MANY_SEGMENTS",
                    "authored segment count exceeds the resource limit",
                    path=path,
                )
            if len(expanded) + 1 > bounds.max_expanded_segments_per_workout:
                raise ProposalValidationError(
                    "REPEAT_EXPANSION_LIMIT",
                    "expanded segment count exceeds the resource limit",
                    path=item_path,
                )
            leaf, ordinal = _leaf(
                raw_item, path=item_path, bounds=bounds, catalog=catalog, seen_ids=seen_ids
            )
            expanded.append(leaf)
            ordinals.append(ordinal)
            continue
        repeat = _closed_keys(
            raw_item, allowed=_REPEAT_KEYS, required=_REPEAT_REQUIRED, path=item_path
        )
        repeat_id = _identifier(repeat["id"], "repeat id", f"{item_path}.id")
        if repeat_id in seen_ids:
            raise ProposalValidationError("DUPLICATE_ID", f"duplicate ID {repeat_id!r}", path=f"{item_path}.id")
        seen_ids.add(repeat_id)
        ordinals.append(_ordinal(repeat["ordinal"], f"{item_path}.ordinal"))
        count = bounds.repeat_count.require(
            repeat["repeat_count"], "repeat_count", f"{item_path}.repeat_count"
        )
        children = _exact_sequence(repeat["children"], "children", f"{item_path}.children")
        if not children:
            raise ProposalValidationError("EMPTY_REPEAT", "repeat requires at least one child", path=item_path)
        authored_leaf_count += len(children)
        if authored_leaf_count > bounds.max_authored_segments_per_workout:
            raise ProposalValidationError(
                "TOO_MANY_SEGMENTS",
                "authored segment count exceeds the resource limit",
                path=f"{item_path}.children",
            )
        if len(expanded) + count * len(children) > bounds.max_expanded_segments_per_workout:
            raise ProposalValidationError(
                "REPEAT_EXPANSION_LIMIT",
                "expanded segment count exceeds the resource limit",
                path=item_path,
            )
        child_ordinals: list[int] = []
        child_leaves: list[WorkoutSegment] = []
        for child_index, child in enumerate(children):
            if isinstance(child, Mapping) and "repeat_count" in child:
                raise ProposalValidationError("NESTED_REPEAT", "nested repeats are not supported", path=f"{item_path}.children[{child_index}]")
            leaf, child_ordinal = _leaf(
                child,
                path=f"{item_path}.children[{child_index}]",
                bounds=bounds,
                catalog=catalog,
                seen_ids=seen_ids,
            )
            child_leaves.append(leaf)
            child_ordinals.append(child_ordinal)
        _check_contiguous(child_ordinals, f"{item_path}.children")
        for iteration in range(1, count + 1):
            expanded.extend(
                replace(leaf, repeat_group=repeat_id, repeat_iteration=iteration)
                for leaf in child_leaves
            )
    _check_contiguous(ordinals, path)
    return tuple(expanded)


def parse_ai_proposal_v2(
    payload: Mapping[str, Any] | str | bytes,
    *,
    methodology_id: MethodologyId | str,
    bounds: ProposalBounds,
    prescription_templates: PrescriptionTemplateCatalog,
    expected_candidate_id: str,
    expected_parent_revision_id: str | None,
    expected_parent_content_hash: str | None,
) -> AIProposalV2:
    """Parse V2 without coercion and deterministically rehydrate domain values."""

    method = parse_enum(MethodologyId, methodology_id, "methodology_id")
    if method is MethodologyId.LEGACY_UNSPECIFIED:
        raise ProposalValidationError("UNSUPPORTED_METHODOLOGY", "legacy plans cannot claim V2 named-method composition")
    if prescription_templates.methodology_id is not method:
        raise ProposalValidationError("FOREIGN_TEMPLATE_CATALOG", "template catalog methodology does not match application context")
    if bounds.allowed_template_ids - set(prescription_templates.templates):
        raise ProposalValidationError("MISSING_APPLICATION_TEMPLATE", "bounds reference an unavailable application template")
    if not isinstance(expected_candidate_id, str) or not _ID_RE.fullmatch(expected_candidate_id):
        raise DomainError("expected_candidate_id must be an application-owned canonical ID")
    if expected_parent_revision_id is not None and (
        not isinstance(expected_parent_revision_id, str)
        or not _ID_RE.fullmatch(expected_parent_revision_id)
    ):
        raise DomainError("expected_parent_revision_id must be a canonical ID or null")
    if expected_parent_content_hash is not None and (
        not isinstance(expected_parent_content_hash, str)
        or not _SHA256_RE.fullmatch(expected_parent_content_hash)
    ):
        raise DomainError("expected_parent_content_hash must be lowercase SHA-256 or null")
    if (expected_parent_revision_id is None) != (expected_parent_content_hash is None):
        raise DomainError("application parent ID and hash must both be present or both be null")

    document = _load_document(payload)
    forbidden = _forbidden_fields(document)
    if forbidden:
        raise ProposalValidationError(
            "FORBIDDEN_AI_AUTHORITY",
            f"AI may not author: {', '.join(forbidden)}",
        )
    document = dict(
        _closed_keys(document, allowed=_TOP_LEVEL_KEYS, required=_TOP_LEVEL_REQUIRED, path="$")
    )
    if document["schema"] != AI_PROPOSAL_SCHEMA:
        raise ProposalValidationError("UNKNOWN_SCHEMA", f"schema must be {AI_PROPOSAL_SCHEMA!r}", path="$.schema")
    if isinstance(document["version"], bool) or document["version"] != AI_PROPOSAL_VERSION:
        raise ProposalValidationError("UNKNOWN_SCHEMA_VERSION", f"version must be {AI_PROPOSAL_VERSION}", path="$.version")
    candidate_id = _identifier(document["candidate_id"], "candidate ID", "$.candidate_id")
    parent_revision_id = document["parent_revision_id"]
    parent_content_hash = document["parent_content_hash"]
    if parent_revision_id is not None:
        parent_revision_id = _identifier(parent_revision_id, "parent revision ID", "$.parent_revision_id")
    if parent_content_hash is not None and (
        not isinstance(parent_content_hash, str) or not _SHA256_RE.fullmatch(parent_content_hash)
    ):
        raise ProposalValidationError("INVALID_PARENT_HASH", "parent hash must be lowercase SHA-256 or null", path="$.parent_content_hash")
    if (parent_revision_id is None) != (parent_content_hash is None):
        raise ProposalValidationError("INCOMPLETE_PARENT_IDENTITY", "parent ID and hash must both be present or both be null")
    for actual, expected, field in (
        (candidate_id, expected_candidate_id, "candidate_id"),
        (parent_revision_id, expected_parent_revision_id, "parent_revision_id"),
        (parent_content_hash, expected_parent_content_hash, "parent_content_hash"),
    ):
        if actual != expected:
            raise ProposalValidationError("STALE_PROPOSAL_IDENTITY", f"{field} does not match deterministic input", path=f"$.{field}")

    workout_items = _exact_sequence(document["workouts"], "workouts", "$.workouts")
    if not workout_items:
        raise ProposalValidationError("EMPTY_WORKOUTS", "proposal requires at least one workout", path="$.workouts")
    if len(workout_items) > bounds.max_workouts:
        raise ProposalValidationError("TOO_MANY_WORKOUTS", "workout count exceeds the resource limit", path="$.workouts")
    seen_ids: set[str] = set()
    ordinals: list[int] = []
    workouts: list[PlannedWorkout] = []
    for index, raw_workout in enumerate(workout_items):
        path = f"$.workouts[{index}]"
        item = _closed_keys(raw_workout, allowed=_WORKOUT_KEYS, required=_WORKOUT_REQUIRED, path=path)
        workout_id = _identifier(item["id"], "workout id", f"{path}.id")
        if workout_id in seen_ids:
            raise ProposalValidationError("DUPLICATE_ID", f"duplicate ID {workout_id!r}", path=f"{path}.id")
        seen_ids.add(workout_id)
        ordinal = _ordinal(item["ordinal"], f"{path}.ordinal")
        ordinals.append(ordinal)
        scheduled_date = _canonical_date(item["date"], f"{path}.date")
        if not bounds.window_start <= scheduled_date <= bounds.window_end:
            raise ProposalValidationError("DATE_OUTSIDE_WINDOW", "workout date is outside deterministic scheduling bounds", path=f"{path}.date")
        if item["sport"] != Sport.RUNNING.value:
            raise ProposalValidationError("UNSUPPORTED_SPORT", "named V1 proposal workouts must use RUNNING", path=f"{path}.sport")
        family = _text(item["family"], "family", f"{path}.family")
        if family not in bounds.allowed_families:
            raise ProposalValidationError("UNKNOWN_WORKOUT_FAMILY", f"family {family!r} was not supplied in the deterministic envelope", path=f"{path}.family")
        if family not in RUNNING_WORKOUT_FAMILIES:
            raise ProposalValidationError(
                "UNSUPPORTED_FAMILY_FOR_SPORT",
                f"family {family!r} is not a running workout family",
                path=f"{path}.family",
            )
        purpose = _text(item["purpose"], "purpose", f"{path}.purpose")
        description = _text(item["description"], "description", f"{path}.description", allow_empty=True)
        segments = _segments(
            item["segments"],
            path=f"{path}.segments",
            bounds=bounds,
            catalog=prescription_templates,
            seen_ids=seen_ids,
        )
        workouts.append(
            PlannedWorkout(
                scheduled_date=scheduled_date,
                sport=Sport.RUNNING,
                family=family,
                purpose=purpose,
                description=description,
                segments=segments,
                event_flag=_boolean(item.get("event_flag", False), "event_flag", f"{path}.event_flag"),
                workout_id=workout_id,
                ordinal=ordinal,
                title=None if item.get("title") is None else _text(item["title"], "title", f"{path}.title"),
                phase=None if item.get("phase") is None else _text(item["phase"], "phase", f"{path}.phase"),
                quality_flag=_boolean(item.get("quality_flag", False), "quality_flag", f"{path}.quality_flag"),
                long_run_flag=_boolean(item.get("long_run_flag", False), "long_run_flag", f"{path}.long_run_flag"),
                metadata={
                    "proposal_schema": AI_PROPOSAL_SCHEMA,
                    "proposal_version": AI_PROPOSAL_VERSION,
                },
            )
        )
    _check_contiguous(ordinals, "$.workouts")

    rationale = document.get("rationale")
    if rationale is not None:
        rationale = _text(rationale, "rationale", "$.rationale", allow_empty=True)
    warning_values = _exact_sequence(document.get("warnings", []), "warnings", "$.warnings")
    if len(warning_values) > 50:
        raise ProposalValidationError("TOO_MANY_WARNINGS", "warning count exceeds the resource limit", path="$.warnings")
    warnings = tuple(
        _text(value, "warning", f"$.warnings[{index}]", allow_empty=False)
        for index, value in enumerate(warning_values)
    )
    return AIProposalV2(
        candidate_id=candidate_id,
        parent_revision_id=parent_revision_id,
        parent_content_hash=parent_content_hash,
        workouts=tuple(workouts),
        rationale=rationale,
        warnings=warnings,
    )


def build_candidate_from_ai_proposal(
    proposal: AIProposalV2,
    *,
    plan_id: str,
    reason: RevisionReason | str,
    manifest: Mapping[str, Any],
    goal_snapshot: Mapping[str, Any],
    athlete_snapshot: Mapping[str, Any],
    parameter_snapshot: Mapping[str, Any],
    provenance: AIProvenance,
    validation_summary: Mapping[str, Any] | None = None,
    constraints: Mapping[str, Any] | None = None,
    change_summary: Mapping[str, Any] | None = None,
) -> PlanRevisionCandidate:
    """Construct an unapproved candidate whose hash is application-generated."""

    revision_provenance = provenance.to_revision_value()
    revision_provenance["expected_parent_content_hash"] = proposal.parent_content_hash
    revision_provenance["proposal_rationale"] = proposal.rationale
    revision_provenance["proposal_warnings"] = proposal.warnings
    return PlanRevisionCandidate(
        revision_id=proposal.candidate_id,
        plan_id=plan_id,
        parent_revision_id=proposal.parent_revision_id,
        reason=parse_enum(RevisionReason, reason, "reason"),
        manifest=manifest,
        goal_snapshot=goal_snapshot,
        athlete_snapshot=athlete_snapshot,
        parameter_snapshot=parameter_snapshot,
        workouts=proposal.workouts,
        provenance=revision_provenance,
        validation_summary=validation_summary,
        constraints=constraints,
        change_summary=change_summary,
        approval_state=ApprovalState.CANDIDATE,
    )


_ALLOWED_ATHLETE_FACTS = frozenset(
    {
        "completed_age",
        "threshold_speed_mps",
        "lthr_bpm",
        "recent_run_days_per_week",
        "weekly_run_duration_seconds",
        "weekly_run_distance_metres",
        "longest_recent_run_seconds",
        "available_run_days",
        "preferred_long_run_day",
    }
)
_SENSITIVE_INPUT_KEYS = frozenset(
    {
        "raw_health_answers",
        "raw_questionnaire_answers",
        "medication_details",
        "injury_details",
        "credentials",
        "garmin_token",
        "session_token",
        "session_cookie",
        "environment",
        "environment_value",
        "database_rows",
        "raw_database_field",
        "raw_database_row",
        "medication",
        "injury",
    }
)


def build_ai_input_envelope(
    *,
    candidate_id: str,
    parent_revision_id: str | None,
    parent_content_hash: str | None,
    goal_snapshot: Mapping[str, Any],
    methodology_manifest: Mapping[str, Any],
    athlete_facts: Mapping[str, Any],
    consumed_athlete_fields: Sequence[str],
    constraints: Sequence[Mapping[str, Any]],
    bounds: ProposalBounds,
    current_schedule: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Project frozen application state into a privacy-minimized AI envelope."""

    if not isinstance(candidate_id, str) or not _ID_RE.fullmatch(candidate_id):
        raise DomainError("candidate_id must be an application-owned canonical ID")
    if parent_revision_id is not None and (
        not isinstance(parent_revision_id, str) or not _ID_RE.fullmatch(parent_revision_id)
    ):
        raise DomainError("parent_revision_id must be a canonical ID or null")
    if parent_content_hash is not None and (
        not isinstance(parent_content_hash, str) or not _SHA256_RE.fullmatch(parent_content_hash)
    ):
        raise DomainError("parent_content_hash must be lowercase SHA-256 or null")
    if (parent_revision_id is None) != (parent_content_hash is None):
        raise DomainError("parent ID and hash must both be present or both be null")
    consumed = tuple(consumed_athlete_fields)
    if len(consumed) != len(set(consumed)):
        raise DomainError("consumed athlete fact names must be unique")
    disallowed = sorted(set(consumed) - _ALLOWED_ATHLETE_FACTS)
    if disallowed:
        raise DomainError(f"athlete facts are not AI-allowlisted: {', '.join(disallowed)}")
    missing = sorted(set(consumed) - set(athlete_facts))
    if missing:
        raise DomainError(f"consumed athlete facts are missing: {', '.join(missing)}")
    selected_facts = {name: deepcopy(athlete_facts[name]) for name in consumed}
    projected = {
        "schema": "garmin-data-hub.plan-proposal-input",
        "version": 2,
        "candidate_id": candidate_id,
        "parent_revision_id": parent_revision_id,
        "parent_content_hash": parent_content_hash,
        "goal_snapshot": dict(goal_snapshot),
        "methodology_manifest": dict(methodology_manifest),
        "athlete_state": selected_facts,
        "constraints": [dict(item) for item in constraints],
        "allowed_workout_families": sorted(bounds.allowed_families),
        "allowed_prescription_template_ids": sorted(bounds.allowed_template_ids),
        "numeric_bounds": {
            "duration_seconds": [bounds.duration_seconds.minimum, bounds.duration_seconds.maximum],
            "distance_metres": [bounds.distance_metres.minimum, bounds.distance_metres.maximum],
            "repeat_count": [bounds.repeat_count.minimum, bounds.repeat_count.maximum],
        },
        "scheduling": {
            "window_start": bounds.window_start.isoformat(),
            "window_end": bounds.window_end.isoformat(),
            "max_workouts": bounds.max_workouts,
            "max_authored_segments_per_workout": bounds.max_authored_segments_per_workout,
            "max_expanded_segments_per_workout": bounds.max_expanded_segments_per_workout,
        },
        "current_schedule": [dict(item) for item in current_schedule],
    }
    sensitive = _find_named_keys(projected, _SENSITIVE_INPUT_KEYS)
    if sensitive:
        raise DomainError(f"sensitive fields are forbidden in the AI envelope: {', '.join(sensitive)}")
    try:
        return json.loads(
            json.dumps(projected, ensure_ascii=False, allow_nan=False),
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError) as exc:
        raise DomainError("AI input envelope must contain only JSON-safe frozen values") from exc


def _find_named_keys(value: Any, names: frozenset[str]) -> list[str]:
    found: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in names:
                found.add(key)
            found.update(_find_named_keys(item, names))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.update(_find_named_keys(item, names))
    return sorted(found)


_COMPATIBILITY_KEYS = frozenset({"family", "segments", "placement", "description"})
_COMPATIBILITY_SEGMENT_KEYS = frozenset({"kind", "template_ref"})
_COMPATIBILITY_PLACEMENT_KEYS = frozenset({"date"})
_KNOWN_TEMPLATE_REFS = {
    MethodologyId.FITZGERALD_80_20_RUNNING_V1: frozenset(
        {f"F80.ZONE_{suffix}" for suffix in ("1", "2", "X", "3", "Y", "4", "5")}
    ),
    MethodologyId.MAFFETONE_RUNNING_V1: frozenset(
        {"MAF.AEROBIC", "MAF.MAF_AEROBIC_RANGE", "MAF.MAF_CEILING", "MAF.SUB_MAF"}
    ),
}


def validate_ai_proposal(
    *, methodology_id: str, proposal: Mapping[str, Any], **_: Any
) -> dict[str, Any]:
    """Contract-facing authority check, distinct from strict V2 parsing.

    Unversioned input is never returned as an ``AIProposalV2`` or candidate.
    This narrow compatibility validator exists for the frozen PLAN-2 contracts;
    production V2 callers must use :func:`parse_ai_proposal_v2`.
    """

    try:
        method = parse_enum(MethodologyId, methodology_id, "methodology_id")
    except DomainError:
        return {"accepted": False, "reason": "UNKNOWN_METHODOLOGY"}
    if not isinstance(proposal, Mapping):
        return {"accepted": False, "reason": "MALFORMED_STRUCTURE"}
    forbidden = _forbidden_fields(proposal)
    if forbidden:
        maf_authority = {
            "selected_adjustment",
            "maf_adjustment",
            "ceiling_bpm",
            "maf_ceiling_bpm",
            "maf_ceiling",
            "state",
            "state_transition",
            "higher_intensity_state",
        }
        reason = (
            "AI_METHODOLOGY_AUTHORITY_FORBIDDEN"
            if any(field in maf_authority for field in forbidden)
            else "AI_NUMERIC_AUTHORITY_FORBIDDEN"
        )
        return {
            "accepted": False,
            "severity": "ERROR",
            "reason": reason,
            "forbidden_fields": forbidden,
        }
    unknown = sorted(set(proposal) - _COMPATIBILITY_KEYS)
    if unknown:
        return {
            "accepted": False,
            "severity": "ERROR",
            "reason": "UNKNOWN_PROPOSAL_FIELD",
            "unknown_fields": unknown,
        }
    family = proposal.get("family")
    if family not in WORKOUT_FAMILIES:
        return {"accepted": False, "severity": "ERROR", "reason": "UNKNOWN_WORKOUT_FAMILY"}
    placement = proposal.get("placement")
    if not isinstance(placement, Mapping) or set(placement) != _COMPATIBILITY_PLACEMENT_KEYS:
        return {"accepted": False, "severity": "ERROR", "reason": "MALFORMED_PLACEMENT"}
    try:
        _canonical_date(placement.get("date"), "$.placement.date")
    except ProposalValidationError:
        return {"accepted": False, "severity": "ERROR", "reason": "NONCANONICAL_DATE"}
    segments = proposal.get("segments")
    if not isinstance(segments, list) or not segments:
        return {"accepted": False, "severity": "ERROR", "reason": "EMPTY_SEGMENTS"}
    for segment in segments:
        if not isinstance(segment, Mapping) or set(segment) != _COMPATIBILITY_SEGMENT_KEYS:
            return {"accepted": False, "severity": "ERROR", "reason": "MALFORMED_SEGMENT"}
        if segment.get("kind") not in {kind.value for kind in SegmentKind}:
            return {"accepted": False, "severity": "ERROR", "reason": "UNKNOWN_SEGMENT_KIND"}
        if segment.get("template_ref") not in _KNOWN_TEMPLATE_REFS.get(method, frozenset()):
            return {
                "accepted": False,
                "severity": "ERROR",
                "reason": "UNKNOWN_PRESCRIPTION_TEMPLATE",
            }
    if not isinstance(proposal.get("description", ""), str):
        return {"accepted": False, "severity": "ERROR", "reason": "MALFORMED_DESCRIPTION"}
    return {
        "accepted": True,
        "authoritative_prescriptions_rehydrated": True,
        "proposal_schema_version": None,
        "candidate_constructed": False,
    }
