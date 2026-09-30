"""Layered deterministic validation and finding ownership."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping

from .canonical import canonical_json
from .domain import DomainError


class ValidationLayer(str, Enum):
    STRUCTURAL = "STRUCTURAL"
    SHARED_PLAN_INTEGRITY = "SHARED_PLAN_INTEGRITY"
    SHARED_SAFETY = "SHARED_SAFETY"
    METHODOLOGY = "METHODOLOGY"
    PERSISTENCE_PRECONDITIONS = "PERSISTENCE_PRECONDITIONS"


class FindingSeverity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFORMATIONAL = "INFORMATIONAL"


DEFAULT_OWNER = {
    ValidationLayer.STRUCTURAL: "DATA_HUB_STRUCTURAL_VALIDATION",
    ValidationLayer.SHARED_PLAN_INTEGRITY: "DATA_HUB_PLAN_INTEGRITY",
    ValidationLayer.SHARED_SAFETY: "DATA_HUB_SHARED_SAFETY",
    ValidationLayer.METHODOLOGY: "SELECTED_METHODOLOGY_POLICY",
    ValidationLayer.PERSISTENCE_PRECONDITIONS: "DATA_HUB_REVISION_REPOSITORY",
}

LAYER_ORDER = (
    ValidationLayer.STRUCTURAL,
    ValidationLayer.SHARED_PLAN_INTEGRITY,
    ValidationLayer.SHARED_SAFETY,
    ValidationLayer.METHODOLOGY,
    ValidationLayer.PERSISTENCE_PRECONDITIONS,
)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        raise DomainError("unordered validation evidence is not supported")
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    rule_id: str
    layer: ValidationLayer
    owner: str
    severity: FindingSeverity
    message: str
    workout_id: str | None = None
    segment_id: str | None = None
    evidence: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        try:
            layer = self.layer if isinstance(self.layer, ValidationLayer) else ValidationLayer(self.layer)
            severity = self.severity if isinstance(self.severity, FindingSeverity) else FindingSeverity(self.severity)
        except ValueError as exc:
            raise DomainError("invalid validation layer or severity") from exc
        object.__setattr__(self, "layer", layer)
        object.__setattr__(self, "severity", severity)
        if not self.rule_id or not self.owner or not self.message:
            raise DomainError("validation finding rule, owner, and message are required")
        if self.workout_id is not None and not isinstance(self.workout_id, str):
            raise DomainError("validation finding workout_id must be text or null")
        if self.segment_id is not None and not isinstance(self.segment_id, str):
            raise DomainError("validation finding segment_id must be text or null")
        if self.evidence is not None:
            if not isinstance(self.evidence, Mapping):
                raise DomainError("validation finding evidence must be an object")
            object.__setattr__(self, "evidence", _freeze(self.evidence))

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "layer": self.layer.value,
            "owner": self.owner,
            "severity": self.severity.value,
            "message": self.message,
            "workout_id": self.workout_id,
            "segment_id": self.segment_id,
            "evidence": None if self.evidence is None else _thaw(self.evidence),
        }


def _coerce_finding(value: ValidationFinding | Mapping[str, Any], layer: ValidationLayer) -> ValidationFinding:
    if isinstance(value, ValidationFinding):
        if value.layer is not layer:
            raise DomainError("finding was supplied to the wrong validation layer")
        return value
    if not isinstance(value, Mapping):
        raise DomainError("validation findings must be structured values")
    supplied_layer = value.get("layer", layer.value)
    if supplied_layer != layer.value:
        raise DomainError("finding layer does not match its pipeline owner")
    severity = value.get("severity")
    if not isinstance(severity, str):
        raise DomainError("finding severity is required")
    rule_id = value.get("rule_id", value.get("rule"))
    try:
        parsed_severity = FindingSeverity(severity.upper())
    except ValueError as exc:
        raise DomainError(f"invalid finding severity: {severity!r}") from exc
    return ValidationFinding(
        rule_id=str(rule_id or ""),
        layer=layer,
        owner=str(value.get("owner") or DEFAULT_OWNER[layer]),
        severity=parsed_severity,
        message=str(value.get("message") or rule_id or "validation finding"),
        workout_id=value.get("workout_id"),
        segment_id=value.get("segment_id"),
        evidence=value.get("evidence"),
    )


def _coerce_layer(
    values: Iterable[ValidationFinding | Mapping[str, Any]], layer: ValidationLayer
) -> tuple[ValidationFinding, ...]:
    findings = tuple(_coerce_finding(value, layer) for value in values)
    return tuple(
        sorted(
            findings,
            key=lambda item: (
                item.rule_id,
                item.workout_id or "",
                item.segment_id or "",
                item.severity.value,
                item.owner,
                item.message,
                canonical_json(item.evidence),
            ),
        )
    )


def validate_candidate(
    *,
    methodology_findings: Iterable[ValidationFinding | Mapping[str, Any]] = (),
    safety_findings: Iterable[ValidationFinding | Mapping[str, Any]] = (),
    structural_findings: Iterable[ValidationFinding | Mapping[str, Any]] = (),
    plan_integrity_findings: Iterable[ValidationFinding | Mapping[str, Any]] = (),
    persistence_findings: Iterable[ValidationFinding | Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Combine five owned layers without permitting cross-layer waivers."""

    by_layer = {
        ValidationLayer.STRUCTURAL: _coerce_layer(structural_findings, ValidationLayer.STRUCTURAL),
        ValidationLayer.SHARED_PLAN_INTEGRITY: _coerce_layer(
            plan_integrity_findings, ValidationLayer.SHARED_PLAN_INTEGRITY
        ),
        ValidationLayer.SHARED_SAFETY: _coerce_layer(safety_findings, ValidationLayer.SHARED_SAFETY),
        ValidationLayer.METHODOLOGY: _coerce_layer(methodology_findings, ValidationLayer.METHODOLOGY),
        ValidationLayer.PERSISTENCE_PRECONDITIONS: _coerce_layer(
            persistence_findings, ValidationLayer.PERSISTENCE_PRECONDITIONS
        ),
    }
    blocking_layer = next(
        (
            layer
            for layer in LAYER_ORDER
            if any(finding.severity is FindingSeverity.ERROR for finding in by_layer[layer])
        ),
        None,
    )
    findings = tuple(finding for layer in LAYER_ORDER for finding in by_layer[layer])
    method_owners = {finding.owner for finding in by_layer[ValidationLayer.METHODOLOGY]}
    safety_owners = {finding.owner for finding in by_layer[ValidationLayer.SHARED_SAFETY]}
    return {
        "accepted": blocking_layer is None,
        "blocking_layer": None if blocking_layer is None else blocking_layer.value,
        "methodology_waiver_allowed": False,
        "provenance_distinct": not bool(method_owners & safety_owners),
        "findings": [finding.to_dict() for finding in findings],
        "layers_evaluated": [layer.value for layer in LAYER_ORDER],
    }


FindingValidator = Callable[[Any], Iterable[ValidationFinding | Mapping[str, Any]]]


def run_validation_pipeline(
    candidate: Any,
    *,
    structural_validator: FindingValidator | None = None,
    plan_integrity_validator: FindingValidator | None = None,
    shared_safety_policy: FindingValidator | None = None,
    methodology_policy: FindingValidator | None = None,
    persistence_validator: FindingValidator | None = None,
    methodology_owner: str = "SELECTED_METHODOLOGY_POLICY",
) -> dict[str, Any]:
    """Invoke owned validators; an absent method policy is explicit, not PASS."""

    evaluated = [ValidationLayer.STRUCTURAL]
    structural = tuple(structural_validator(candidate)) if structural_validator else ()
    structural_result = validate_candidate(structural_findings=structural)
    if not structural_result["accepted"]:
        structural_result["layers_evaluated"] = [layer.value for layer in evaluated]
        return structural_result

    evaluated.append(ValidationLayer.SHARED_PLAN_INTEGRITY)
    integrity = tuple(plan_integrity_validator(candidate)) if plan_integrity_validator else ()
    evaluated.append(ValidationLayer.SHARED_SAFETY)
    safety = tuple(shared_safety_policy(candidate)) if shared_safety_policy else ()
    evaluated.append(ValidationLayer.METHODOLOGY)
    if methodology_policy is None:
        methodology: tuple[ValidationFinding | Mapping[str, Any], ...] = (
            ValidationFinding(
                rule_id="METHODOLOGY_VALIDATION_DEFERRED",
                layer=ValidationLayer.METHODOLOGY,
                owner=methodology_owner,
                severity=FindingSeverity.INFORMATIONAL,
                message="Full methodology policy is not evaluated in this phase.",
            ),
        )
    else:
        methodology = tuple(methodology_policy(candidate))
    pre_persistence = validate_candidate(
        structural_findings=structural,
        plan_integrity_findings=integrity,
        safety_findings=safety,
        methodology_findings=methodology,
    )
    if not pre_persistence["accepted"]:
        pre_persistence["layers_evaluated"] = [layer.value for layer in evaluated]
        return pre_persistence

    evaluated.append(ValidationLayer.PERSISTENCE_PRECONDITIONS)
    persistence = tuple(persistence_validator(candidate)) if persistence_validator else ()
    result = validate_candidate(
        structural_findings=structural,
        plan_integrity_findings=integrity,
        safety_findings=safety,
        methodology_findings=methodology,
        persistence_findings=persistence,
    )
    result["layers_evaluated"] = [layer.value for layer in evaluated]
    return result
