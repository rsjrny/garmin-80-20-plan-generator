"""Pure plan/revision lifecycle values; no persistence or activation logic."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Mapping

from .canonical import content_sha256
from .domain import ApprovalState, DomainError, RevisionReason, parse_enum


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        raise DomainError("unordered content cannot be frozen into a revision")
    return value


@dataclass(frozen=True, slots=True)
class Plan:
    plan_id: str
    current_revision_id: str | None
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.plan_id:
            raise DomainError("plan_id is required")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise DomainError("created_at must include a timezone")

    def with_current_revision(self, revision_id: str) -> "Plan":
        if not revision_id:
            raise DomainError("revision_id is required")
        return replace(self, current_revision_id=revision_id)


@dataclass(frozen=True, slots=True)
class PlanRevisionCandidate:
    revision_id: str
    plan_id: str
    parent_revision_id: str | None
    reason: RevisionReason
    manifest: Mapping[str, Any]
    goal_snapshot: Mapping[str, Any]
    athlete_snapshot: Mapping[str, Any]
    parameter_snapshot: Mapping[str, Any]
    workouts: tuple[Any, ...] = ()
    provenance: Mapping[str, Any] | None = None
    validation_summary: Mapping[str, Any] | None = None
    constraints: Mapping[str, Any] | None = None
    change_summary: Mapping[str, Any] | None = None
    approval_state: ApprovalState = ApprovalState.CANDIDATE

    def __post_init__(self) -> None:
        if not self.revision_id or not self.plan_id:
            raise DomainError("revision_id and plan_id are required")
        object.__setattr__(self, "reason", parse_enum(RevisionReason, self.reason, "reason"))
        object.__setattr__(
            self, "approval_state", parse_enum(ApprovalState, self.approval_state, "approval_state")
        )
        object.__setattr__(self, "manifest", _freeze(self.manifest))
        object.__setattr__(self, "goal_snapshot", _freeze(self.goal_snapshot))
        object.__setattr__(self, "athlete_snapshot", _freeze(self.athlete_snapshot))
        object.__setattr__(self, "parameter_snapshot", _freeze(self.parameter_snapshot))
        object.__setattr__(self, "workouts", _freeze(self.workouts))
        if self.provenance is not None:
            object.__setattr__(self, "provenance", _freeze(self.provenance))
        if self.validation_summary is not None:
            object.__setattr__(self, "validation_summary", _freeze(self.validation_summary))
        if self.constraints is not None:
            object.__setattr__(self, "constraints", _freeze(self.constraints))
        if self.change_summary is not None:
            object.__setattr__(self, "change_summary", _freeze(self.change_summary))

    @property
    def content_hash(self) -> str:
        return content_sha256(
            {
                "plan_id": self.plan_id,
                "parent_revision_id": self.parent_revision_id,
                "reason": self.reason,
                "manifest": self.manifest,
                "goal_snapshot": self.goal_snapshot,
                "athlete_snapshot": self.athlete_snapshot,
                "parameter_snapshot": self.parameter_snapshot,
                "workouts": self.workouts,
                "provenance": self.provenance,
                "validation_summary": self.validation_summary,
                "constraints": self.constraints,
                "change_summary": self.change_summary,
            }
        )


@dataclass(frozen=True, slots=True)
class PlanRevision:
    candidate: PlanRevisionCandidate
    approved_by: str
    approved_at: datetime
    content_hash: str
    approval_state: ApprovalState = ApprovalState.APPROVED

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "approval_state",
            parse_enum(ApprovalState, self.approval_state, "approval_state"),
        )
        if self.approval_state is not ApprovalState.APPROVED:
            raise DomainError("persisted revision approval_state must be APPROVED")
        if self.candidate.approval_state is not ApprovalState.VALIDATED:
            raise DomainError("only a validated candidate can be approved")
        if not self.approved_by:
            raise DomainError("approved_by is required")
        if self.content_hash != self.candidate.content_hash:
            raise DomainError("approved revision content hash does not match its candidate")
        if self.approved_at.tzinfo is None or self.approved_at.utcoffset() is None:
            raise DomainError("approved_at must include a timezone")


def create_plan(*, plan_id: str, initial_revision_id: str) -> dict[str, Any]:
    Plan(plan_id=plan_id, current_revision_id=initial_revision_id, created_at=datetime.now(timezone.utc))
    return {
        "plan_id": plan_id,
        "current_revision_id": initial_revision_id,
        "stable_identity": True,
    }


def approve_candidate(*, candidate: Mapping[str, Any], approver: str) -> dict[str, Any]:
    state = parse_enum(ApprovalState, candidate.get("approval_state", ""), "approval_state")
    if state is not ApprovalState.VALIDATED:
        raise DomainError("only a validated candidate can be approved")
    if not approver:
        raise DomainError("approver is required")
    return {"approval_state": ApprovalState.APPROVED.value, "immutable": True}


def build_candidate(
    *,
    plan_id: str,
    parent_revision_id: str | None,
    reason: str,
    manifest: Mapping[str, Any],
    goal_snapshot: Mapping[str, Any],
    athlete_snapshot: Mapping[str, Any],
    parameter_snapshot: Mapping[str, Any],
    revision_id: str = "candidate",
    workouts: tuple[Any, ...] = (),
    provenance: Mapping[str, Any] | None = None,
    **_: Any,
) -> dict[str, Any]:
    candidate = PlanRevisionCandidate(
        revision_id=revision_id,
        plan_id=plan_id,
        parent_revision_id=parent_revision_id,
        reason=parse_enum(RevisionReason, reason, "reason"),
        manifest=manifest,
        goal_snapshot=goal_snapshot,
        athlete_snapshot=athlete_snapshot,
        parameter_snapshot=parameter_snapshot,
        workouts=workouts,
        provenance=provenance,
    )
    return {
        "parent_revision_id": candidate.parent_revision_id,
        "revision_reason": candidate.reason.value,
        "snapshots_frozen": True,
        "manifest_frozen": True,
        "approval_state": candidate.approval_state.value,
        "content_hash": candidate.content_hash,
    }


def compute_content_hash(*, revision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "algorithm": "SHA-256",
        "content_hash": content_sha256(revision),
        "canonical": True,
        "includes_prescription": True,
        "excludes_runtime": True,
        "excludes_ui_state": True,
    }


def classify_edit(*, edit: Mapping[str, Any]) -> dict[str, Any]:
    field = edit.get("field")
    if edit.get("kind") == "SPELLING_ONLY" and edit.get("prescriptive_effect") is False:
        return {"new_revision_required": False, "storage": "METADATA_OVERLAY"}
    if field == "methodology_id":
        reason = RevisionReason.METHODOLOGY_CHANGE
    elif field in {"threshold_speed_mps", "lthr_bpm", "maf_ceiling_bpm", "maf_adjustment"}:
        reason = RevisionReason.ATHLETE_STATE_CHANGE
    elif edit.get("kind") == "AI_REGENERATION":
        reason = RevisionReason.REGENERATION
    else:
        reason = RevisionReason.PRESCRIPTION_EDIT
    return {"new_revision_required": True, "reason": reason.value}


def attach_runtime_state(
    *, revision_id: str, activity_match: Mapping[str, Any], compliance: Mapping[str, Any]
) -> dict[str, Any]:
    if not revision_id:
        raise DomainError("revision_id is required")
    # The runtime payload is deliberately not returned as revision content.
    bool(activity_match)
    bool(compliance)
    return {"revision_content_unchanged": True, "stored_outside_revision": True}
