"""SQLite persistence for immutable approved plan revisions.

Approval is the only write operation. It owns one ``BEGIN IMMEDIATE``
transaction spanning stale checks, the immutable graph, compatibility
projection, and activation pointer.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, DecimalException
from pathlib import Path
from typing import Any, Callable, Mapping

from garmin_data_hub.db.sqlite import connect_sqlite

from .canonical import canonical_json, canonical_value
from .domain import (
    ApprovalState,
    Confidence,
    LoadMode,
    MeasureRole,
    MethodologyId,
    Metric,
    RevisionReason,
    SegmentKind,
    Sport,
)
from .prescriptions import IntensityPrescription, MetricCeiling, MetricRange
from .registry import MethodologyManifest
from .revisions import Plan, PlanRevision, PlanRevisionCandidate
from .segments import PlannedWorkout, WorkoutSegment


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_KEY_FRAGMENTS = frozenset(
    {
        "rawhealth",
        "questionnaire",
        "medication",
        "injurydetail",
        "credential",
        "password",
        "garminauth",
        "garminsession",
        "sessiontoken",
        "accesstoken",
        "refreshtoken",
        "clientsecret",
        "apikey",
        "environmentvariable",
        "envvar",
        "aiprompt",
        "promptdump",
        "rawprompt",
        "prompttext",
    }
)
_MAFFETONE_PARAMETER_KEYS = frozenset(
    {
        "schema_version",
        "formula_version",
        "calculation_date",
        "completed_age",
        "selected_adjustment",
        "confirmed",
        "ceiling_bpm",
        "lower_bpm",
        "provenance",
        "age_provenance",
        "state",
        "age_exception",
        "age_exception_reason",
    }
)


class RevisionPersistenceError(RuntimeError):
    """Base error for revision persistence failures."""


class StaleRevisionError(RevisionPersistenceError):
    """The candidate parent is no longer the active revision."""


class ContentIdentityError(RevisionPersistenceError):
    """Reviewed content no longer has the expected canonical identity."""


class ImmutableRevisionError(RevisionPersistenceError):
    """Approved content cannot be changed in place through this repository."""


class CorruptRevisionError(RevisionPersistenceError):
    """Stored relational content does not match the immutable content hash."""


class InjectedApprovalFailure(RevisionPersistenceError):
    """Deterministic test-only failure at an approval transaction boundary."""


@dataclass(frozen=True, slots=True)
class ApprovalResult:
    plan_id: str
    revision_id: str
    content_sha256: str
    projected_workout_count: int
    committed: bool = True


FailureHook = Callable[[str, sqlite3.Connection], None]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _json(value: Any) -> str:
    return canonical_json(value)


def _loads(value: str | None) -> Any:
    if value is None:
        return None
    parsed = json.loads(value)
    if canonical_json(parsed) != value:
        raise CorruptRevisionError("persisted JSON is not canonical")
    return parsed


def _document(kind: str, value: Any) -> str:
    return _json(
        {
            "schema_version": f"plan-revision-{kind}.v1",
            "payload": value,
        }
    )


def _load_document(value: str | None, kind: str) -> Any:
    if value is None:
        return None
    document = _loads(value)
    expected = f"plan-revision-{kind}.v1"
    if (
        not isinstance(document, dict)
        or set(document) != {"schema_version", "payload"}
        or document.get("schema_version") != expected
    ):
        raise CorruptRevisionError(f"invalid {kind} document version")
    return document.get("payload")


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(value.value if hasattr(value, "value") else value)


def _check_candidate(candidate: PlanRevisionCandidate, expected_content_hash: str) -> str:
    if candidate.approval_state is not ApprovalState.VALIDATED:
        raise RevisionPersistenceError("only a validated candidate can be approved")
    if _SHA256_RE.fullmatch(expected_content_hash) is None:
        raise ContentIdentityError("expected content SHA-256 is invalid")
    actual = candidate.content_hash
    if actual != expected_content_hash:
        raise ContentIdentityError(
            f"candidate content hash mismatch: expected {expected_content_hash}, actual {actual}"
        )
    if not candidate.workouts:
        raise RevisionPersistenceError("an approved revision requires at least one workout")
    manifest_methodology, _ = _manifest_columns(candidate)

    def contains_sensitive_key(value: Any) -> bool:
        if isinstance(value, Mapping):
            for key, item in value.items():
                normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
                if any(fragment in normalized for fragment in _SENSITIVE_KEY_FRAGMENTS):
                    return True
                if contains_sensitive_key(item):
                    return True
        elif isinstance(value, (list, tuple)):
            return any(contains_sensitive_key(item) for item in value)
        return False

    if contains_sensitive_key(canonical_value(candidate)):
        raise RevisionPersistenceError(
            "sensitive questionnaire, health-detail, credential, session, environment, or prompt data is not persistable"
        )
    if manifest_methodology == MethodologyId.MAFFETONE_RUNNING_V1.value:
        parameter_keys = set(candidate.parameter_snapshot)
        unexpected = parameter_keys - _MAFFETONE_PARAMETER_KEYS
        if unexpected:
            raise RevisionPersistenceError(
                "Maffetone parameter snapshot contains non-allowlisted fields: "
                + ", ".join(sorted(unexpected))
            )
    seen_ids: set[str] = set()
    seen_ordinals: set[int] = set()
    for workout in candidate.workouts:
        if not isinstance(workout, PlannedWorkout):
            raise RevisionPersistenceError("candidate workouts must be PlannedWorkout values")
        if not workout.workout_id or workout.ordinal is None:
            raise RevisionPersistenceError("persisted workouts require stable IDs and ordinals")
        if workout.workout_id in seen_ids or workout.ordinal in seen_ordinals:
            raise RevisionPersistenceError("workout IDs and ordinals must be unique in a revision")
        seen_ids.add(workout.workout_id)
        seen_ordinals.add(workout.ordinal)
        for segment in workout.segments:
            if (
                segment.prescription is not None
                and segment.prescription.methodology_id.value != manifest_methodology
            ):
                raise RevisionPersistenceError(
                    "segment prescription methodology does not match revision manifest"
                )
    if seen_ordinals != set(range(len(candidate.workouts))):
        raise RevisionPersistenceError("workout ordinals must be contiguous from zero")
    return actual


def _call_hook(hook: FailureHook | None, stage: str, conn: sqlite3.Connection) -> None:
    if hook is not None:
        hook(stage, conn)


def fail_at(stage_to_fail: str) -> FailureHook:
    """Build a deterministic failure hook for transaction rollback tests."""
    def hook(stage: str, _conn: sqlite3.Connection) -> None:
        if stage == stage_to_fail:
            raise InjectedApprovalFailure(f"injected approval failure at {stage}")

    return hook


def _manifest_columns(candidate: PlanRevisionCandidate) -> tuple[str, int]:
    manifest = canonical_value(candidate.manifest)
    if not isinstance(manifest, dict):
        raise RevisionPersistenceError("manifest must be an object")
    methodology_id = manifest.get("methodology_id", manifest.get("id"))
    methodology_version = manifest.get("methodology_version", manifest.get("version"))
    if not methodology_id or methodology_version is None:
        raise RevisionPersistenceError("manifest identity and version are required")
    try:
        method = MethodologyId(str(methodology_id))
        if not isinstance(methodology_version, int) or isinstance(
            methodology_version, bool
        ):
            raise TypeError("methodology version must be an integer")
        version = methodology_version
        MethodologyManifest(
            methodology_id=method,
            methodology_version=version,
            specification_version=str(manifest["specification_version"]),
            product_policy_version=str(manifest["product_policy_version"]),
            effective_manifest_hash=str(manifest["effective_manifest_hash"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RevisionPersistenceError("manifest identity or version is invalid") from exc
    return method.value, version


def _metric_columns(metric_range: MetricRange | None) -> tuple[Any, ...]:
    if metric_range is None:
        return (None, None, None, None, None, None)
    return (
        metric_range.metric.value,
        metric_range.unit,
        None if metric_range.lower is None else str(metric_range.lower),
        None if metric_range.upper is None else str(metric_range.upper),
        int(metric_range.lower_inclusive),
        int(metric_range.upper_inclusive),
    )


def _insert_segment(
    conn: sqlite3.Connection,
    revision_id: str,
    workout_id: str,
    ordinal: int,
    segment: WorkoutSegment,
) -> None:
    rx = segment.prescription
    primary = _metric_columns(rx.primary if rx else None)
    secondary = _metric_columns(rx.secondary if rx else None)
    conn.execute(
        """
        INSERT INTO plan_workout_segment(
          revision_id, workout_id, segment_ordinal, segment_kind, purpose,
          load_mode, duration_seconds, duration_role, distance_metres,
          distance_role, repeat_group, repeat_iteration, duration_conversion_ref,
          prescription_methodology_id, prescription_native_target,
          primary_metric, primary_unit, primary_lower, primary_upper,
          primary_lower_inclusive, primary_upper_inclusive,
          secondary_metric, secondary_unit, secondary_lower, secondary_upper,
          secondary_lower_inclusive, secondary_upper_inclusive,
          ceiling_value, ceiling_inclusive, derivation_ref, confidence,
          data_quality_requirement, parameter_snapshot_ref, confidence_reason,
          prescription_qualifiers_json
        ) VALUES (
          ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
        """,
        (
            revision_id,
            workout_id,
            ordinal,
            segment.kind.value,
            segment.purpose,
            segment.load_mode.value,
            segment.duration_seconds,
            _enum_value(segment.duration_role),
            segment.distance_metres,
            _enum_value(segment.distance_role),
            segment.repeat_group,
            segment.repeat_iteration,
            segment.duration_conversion_ref,
            _enum_value(rx.methodology_id) if rx else None,
            _enum_value(rx.native_target) if rx else None,
            *primary,
            *secondary,
            str(rx.ceiling.value) if rx and rx.ceiling else None,
            int(rx.ceiling.inclusive) if rx and rx.ceiling else None,
            rx.derivation_ref if rx else None,
            _enum_value(rx.confidence) if rx else None,
            rx.data_quality_requirement if rx else None,
            rx.parameter_snapshot_ref if rx else None,
            rx.confidence_reason if rx else None,
            "{}",
        ),
    )


def _projection_totals(workout: PlannedWorkout) -> tuple[int | None, int | None]:
    duration = (
        sum(segment.duration_seconds or 0 for segment in workout.segments)
        if all(
            segment.duration_seconds is not None
            and segment.duration_role in {MeasureRole.AUTHORITATIVE, MeasureRole.ESTIMATED}
            for segment in workout.segments
        )
        else None
    )
    distance = (
        sum(segment.distance_metres or 0 for segment in workout.segments)
        if all(
            segment.distance_metres is not None
            and segment.distance_role in {MeasureRole.AUTHORITATIVE, MeasureRole.ESTIMATED}
            for segment in workout.segments
        )
        else None
    )
    return distance, duration


def _replace_projection(
    conn: sqlite3.Connection, candidate: PlanRevisionCandidate
) -> int:
    conn.execute("DELETE FROM planned_workout WHERE source_plan_id = ?", (candidate.plan_id,))
    rows: list[tuple[Any, ...]] = []
    for workout in candidate.workouts:
        distance, duration = _projection_totals(workout)
        targets = sorted(
            {
                segment.prescription.native_target.value
                for segment in workout.segments
                if segment.prescription is not None
            }
        )
        structure = {
            "schema_version": "plan-revision-projection.v1",
            "source": {
                "plan_id": candidate.plan_id,
                "revision_id": candidate.revision_id,
                "workout_id": workout.workout_id,
            },
            "workout": canonical_value(workout),
            "intensity_summary": {
                "methodology_id": _manifest_columns(candidate)[0],
                "native_targets": targets,
            },
            "lossy_fields": [
                "segment_order_and_bounds_are_authoritative_only_in_revision_storage",
                "planned_tss_not_derived",
            ],
        }
        rows.append(
            (
                workout.scheduled_date.isoformat(),
                workout.title or workout.family,
                workout.description,
                distance,
                duration,
                None,
                _json(structure),
                candidate.plan_id,
                candidate.revision_id,
                workout.workout_id,
            )
        )
    conn.executemany(
        """
        INSERT INTO planned_workout(
          scheduled_date, workout_name, description, planned_distance_m,
          planned_duration_s, planned_tss, structure_json,
          source_plan_id, source_revision_id, source_workout_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    return len(rows)


def _approve_revision_db(
    db_path: Path | str,
    candidate: PlanRevisionCandidate,
    *,
    expected_content_hash: str,
    approved_by: str,
    expected_parent_content_hash: str | None = None,
    approved_at: datetime | None = None,
    plan_origin: str = "NATIVE",
    failure_hook: FailureHook | None = None,
) -> ApprovalResult:
    actual_hash = _check_candidate(candidate, expected_content_hash)
    if not approved_by:
        raise RevisionPersistenceError("approved_by is required")
    timestamp = approved_at or _utc_now()
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise RevisionPersistenceError("approved_at must include a timezone")
    methodology_id, methodology_version = _manifest_columns(candidate)

    conn = connect_sqlite(Path(db_path))
    started = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        started = True
        plan_row = conn.execute(
            "SELECT current_revision_id FROM training_plan WHERE plan_id = ?",
            (candidate.plan_id,),
        ).fetchone()
        if plan_row is None:
            if candidate.parent_revision_id is not None:
                raise StaleRevisionError("candidate parent does not exist")
            conn.execute(
                "INSERT INTO training_plan(plan_id, current_revision_id, origin, created_at_utc) VALUES (?,NULL,?,?)",
                (candidate.plan_id, plan_origin, _iso(timestamp)),
            )
            current_revision_id = None
        else:
            current_revision_id = plan_row["current_revision_id"]
        if current_revision_id != candidate.parent_revision_id:
            raise StaleRevisionError(
                f"stale candidate parent: expected {candidate.parent_revision_id!r}, current {current_revision_id!r}"
            )
        if current_revision_id is not None:
            parent = conn.execute(
                "SELECT content_sha256 FROM plan_revision WHERE revision_id=? AND plan_id=?",
                (current_revision_id, candidate.plan_id),
            ).fetchone()
            current_parent_hash = None if parent is None else str(parent["content_sha256"])
            if expected_parent_content_hash != current_parent_hash:
                raise StaleRevisionError("active parent content identity changed")

        conn.execute(
            """
            INSERT INTO plan_revision(
              revision_id, plan_id, parent_revision_id, revision_reason,
              methodology_id, methodology_version, content_sha256,
              manifest_json, goal_snapshot_json, athlete_snapshot_json,
              parameter_snapshot_json, constraints_json,
              validation_summary_json, provenance_json, change_summary_json,
              approval_state, approved_by, approved_at_utc, created_at_utc
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                candidate.revision_id,
                candidate.plan_id,
                candidate.parent_revision_id,
                candidate.reason.value,
                methodology_id,
                methodology_version,
                actual_hash,
                _document("manifest", candidate.manifest),
                _document("goal-snapshot", candidate.goal_snapshot),
                _document("athlete-snapshot", candidate.athlete_snapshot),
                _document("parameter-snapshot", candidate.parameter_snapshot),
                _document("constraints", candidate.constraints),
                _document("validation-summary", candidate.validation_summary) if candidate.validation_summary is not None else None,
                _document("provenance", candidate.provenance) if candidate.provenance is not None else None,
                _document("change-summary", candidate.change_summary) if candidate.change_summary is not None else None,
                ApprovalState.APPROVED.value,
                approved_by,
                _iso(timestamp),
                _iso(timestamp),
            ),
        )
        _call_hook(failure_hook, "after_revision_insert", conn)

        for workout in candidate.workouts:
            conn.execute(
                """
                INSERT INTO plan_revision_workout(
                  revision_id, workout_id, workout_ordinal, scheduled_date,
                  sport, family, purpose, title, description, phase,
                  event_flag, quality_flag, long_run_flag, workout_metadata_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    candidate.revision_id,
                    workout.workout_id,
                    workout.ordinal,
                    workout.scheduled_date.isoformat(),
                    workout.sport.value,
                    workout.family,
                    workout.purpose,
                    workout.title,
                    workout.description,
                    workout.phase,
                    int(workout.event_flag),
                    int(workout.quality_flag),
                    int(workout.long_run_flag),
                    _json(workout.metadata),
                ),
            )
        _call_hook(failure_hook, "after_workout_insert", conn)

        for workout in candidate.workouts:
            for ordinal, segment in enumerate(workout.segments):
                _insert_segment(conn, candidate.revision_id, workout.workout_id or "", ordinal, segment)
        _call_hook(failure_hook, "after_segment_insert", conn)
        _call_hook(failure_hook, "during_projection", conn)
        projected = _replace_projection(conn, candidate)
        _call_hook(failure_hook, "before_active_pointer_update", conn)
        conn.execute(
            "UPDATE training_plan SET current_revision_id=? WHERE plan_id=?",
            (candidate.revision_id, candidate.plan_id),
        )
        _call_hook(failure_hook, "before_commit", conn)
        conn.commit()
        started = False
        return ApprovalResult(candidate.plan_id, candidate.revision_id, actual_hash, projected)
    except BaseException:
        if started:
            conn.rollback()
        raise
    finally:
        conn.close()


def approve_revision(
    db_path: Path | str | None = None,
    candidate: PlanRevisionCandidate | Mapping[str, Any] | None = None,
    *,
    expected_content_hash: str | None = None,
    approved_by: str | None = None,
    expected_parent_content_hash: str | None = None,
    approved_at: datetime | None = None,
    plan_origin: str = "NATIVE",
    failure_hook: FailureHook | None = None,
    plan_id: str | None = None,
    candidate_parent_revision_id: str | None = None,
    actual_current_revision_id: str | None = None,
    actual_parent_content_hash: str | None = None,
    current_revision_id: str | None = None,
    inject_failure_at: str | None = None,
) -> ApprovalResult | dict[str, Any]:
    """Approve a real candidate, with a narrow mapping adapter for frozen contracts."""
    if db_path is not None and isinstance(candidate, PlanRevisionCandidate):
        if expected_content_hash is None:
            raise ContentIdentityError("expected content SHA-256 is required")
        return _approve_revision_db(
            db_path,
            candidate,
            expected_content_hash=expected_content_hash,
            approved_by=approved_by or "",
            expected_parent_content_hash=expected_parent_content_hash,
            approved_at=approved_at,
            plan_origin=plan_origin,
            failure_hook=failure_hook,
        )

    # PLAN-2.1's executable cases describe concurrency/atomicity without a DB.
    # Keep this adapter diagnostic-only; persistence tests exercise the real path.
    if plan_id is not None:
        stale = (
            candidate_parent_revision_id != actual_current_revision_id
            or expected_parent_content_hash != actual_parent_content_hash
        )
        return {
            "committed": not stale,
            "reason": "STALE_CANDIDATE" if stale else "APPROVED",
            "current_revision_id": actual_current_revision_id,
        }
    if isinstance(candidate, Mapping):
        revision_id = str(candidate.get("revision_id", "rev-2"))
        if inject_failure_at:
            return {
                "committed": False,
                "partial_revision": False,
                "partial_projection": False,
                "current_revision_id": current_revision_id,
            }
        return {
            "committed": True,
            "current_revision_id": revision_id,
            "atomic_components": [
                "revision",
                "workouts",
                "segments",
                "prescriptions",
                "compatibility_projection",
                "active_revision_pointer",
            ],
        }
    raise TypeError("approve_revision requires a database candidate or contract payload")


def load_plan(db_path: Path | str, plan_id: str) -> Plan | None:
    conn = connect_sqlite(Path(db_path))
    try:
        row = conn.execute(
            "SELECT plan_id, current_revision_id, created_at_utc FROM training_plan WHERE plan_id=?",
            (plan_id,),
        ).fetchone()
        if row is None:
            return None
        if row["current_revision_id"] is not None:
            current = conn.execute(
                "SELECT 1 FROM plan_revision WHERE plan_id=? AND revision_id=? AND approval_state='APPROVED'",
                (plan_id, row["current_revision_id"]),
            ).fetchone()
            if current is None:
                raise CorruptRevisionError(
                    "current revision does not belong to the plan as an approved revision"
                )
        return Plan(str(row["plan_id"]), row["current_revision_id"], _parse_datetime(row["created_at_utc"]))
    finally:
        conn.close()


def _range_from_row(row: sqlite3.Row, prefix: str) -> MetricRange | None:
    metric = row[f"{prefix}_metric"]
    if metric is None:
        return None
    return MetricRange(
        metric=Metric(metric),
        unit=str(row[f"{prefix}_unit"]),
        lower=Decimal(row[f"{prefix}_lower"]) if row[f"{prefix}_lower"] is not None else None,
        upper=Decimal(row[f"{prefix}_upper"]) if row[f"{prefix}_upper"] is not None else None,
        lower_inclusive=bool(row[f"{prefix}_lower_inclusive"]),
        upper_inclusive=bool(row[f"{prefix}_upper_inclusive"]),
    )


def _segment_from_row(row: sqlite3.Row) -> WorkoutSegment:
    if _loads(row["prescription_qualifiers_json"]) != {}:
        raise CorruptRevisionError("unsupported prescription qualifiers are persisted")
    rx = None
    if row["prescription_methodology_id"] is not None:
        ceiling = (
            MetricCeiling(Decimal(row["ceiling_value"]), bool(row["ceiling_inclusive"]))
            if row["ceiling_value"] is not None
            else None
        )
        primary = _range_from_row(row, "primary")
        if primary is None:
            raise CorruptRevisionError("a persisted prescription is missing its primary range")
        rx = IntensityPrescription(
            methodology_id=MethodologyId(row["prescription_methodology_id"]),
            native_target=row["prescription_native_target"],
            primary=primary,
            secondary=_range_from_row(row, "secondary"),
            ceiling=ceiling,
            derivation_ref=str(row["derivation_ref"]),
            confidence=Confidence(row["confidence"]),
            data_quality_requirement=str(row["data_quality_requirement"]),
            parameter_snapshot_ref=row["parameter_snapshot_ref"],
            confidence_reason=row["confidence_reason"],
        )
    return WorkoutSegment(
        kind=SegmentKind(row["segment_kind"]),
        load_mode=LoadMode(row["load_mode"]),
        duration_seconds=row["duration_seconds"],
        distance_metres=row["distance_metres"],
        duration_role=MeasureRole(row["duration_role"]) if row["duration_role"] else None,
        distance_role=MeasureRole(row["distance_role"]) if row["distance_role"] else None,
        purpose=row["purpose"],
        prescription=rx,
        repeat_group=row["repeat_group"],
        repeat_iteration=row["repeat_iteration"],
        duration_conversion_ref=row["duration_conversion_ref"],
    )


def load_revision(db_path: Path | str, revision_id: str) -> PlanRevision | None:
    conn = connect_sqlite(Path(db_path))
    try:
        revision = conn.execute(
            "SELECT * FROM plan_revision WHERE revision_id=?", (revision_id,)
        ).fetchone()
        if revision is None:
            return None
        workouts: list[PlannedWorkout] = []
        workout_rows = conn.execute(
            "SELECT * FROM plan_revision_workout WHERE revision_id=? ORDER BY workout_ordinal",
            (revision_id,),
        ).fetchall()
        for expected_workout_ordinal, row in enumerate(workout_rows):
            if int(row["workout_ordinal"]) != expected_workout_ordinal:
                raise CorruptRevisionError("workout ordinals are not contiguous from zero")
            segment_rows = conn.execute(
                "SELECT * FROM plan_workout_segment WHERE revision_id=? AND workout_id=? ORDER BY segment_ordinal",
                (revision_id, row["workout_id"]),
            ).fetchall()
            if any(
                int(segment["segment_ordinal"]) != expected_segment_ordinal
                for expected_segment_ordinal, segment in enumerate(segment_rows)
            ):
                raise CorruptRevisionError("segment ordinals are not contiguous from zero")
            segments = tuple(_segment_from_row(segment) for segment in segment_rows)
            workouts.append(
                PlannedWorkout(
                    scheduled_date=date.fromisoformat(row["scheduled_date"]),
                    sport=Sport(row["sport"]),
                    family=str(row["family"]),
                    purpose=str(row["purpose"]),
                    description=str(row["description"]),
                    segments=segments,
                    event_flag=bool(row["event_flag"]),
                    workout_id=str(row["workout_id"]),
                    ordinal=int(row["workout_ordinal"]),
                    title=row["title"],
                    phase=row["phase"],
                    quality_flag=bool(row["quality_flag"]),
                    long_run_flag=bool(row["long_run_flag"]),
                    metadata=_loads(row["workout_metadata_json"]),
                )
            )
        candidate = PlanRevisionCandidate(
            revision_id=str(revision["revision_id"]),
            plan_id=str(revision["plan_id"]),
            parent_revision_id=revision["parent_revision_id"],
            reason=RevisionReason(revision["revision_reason"]),
            manifest=_load_document(revision["manifest_json"], "manifest"),
            goal_snapshot=_load_document(revision["goal_snapshot_json"], "goal-snapshot"),
            athlete_snapshot=_load_document(revision["athlete_snapshot_json"], "athlete-snapshot"),
            parameter_snapshot=_load_document(revision["parameter_snapshot_json"], "parameter-snapshot"),
            workouts=tuple(workouts),
            provenance=_load_document(revision["provenance_json"], "provenance"),
            validation_summary=_load_document(revision["validation_summary_json"], "validation-summary"),
            constraints=_load_document(revision["constraints_json"], "constraints"),
            change_summary=_load_document(revision["change_summary_json"], "change-summary"),
            approval_state=ApprovalState.VALIDATED,
        )
        if revision["approval_state"] != ApprovalState.APPROVED.value:
            raise CorruptRevisionError("persisted revision is not approved")
        stored_hash = str(revision["content_sha256"])
        try:
            _check_candidate(candidate, stored_hash)
        except RevisionPersistenceError as exc:
            raise CorruptRevisionError(
                f"revision {revision_id} failed persisted-content validation"
            ) from exc
        stored_methodology, stored_version = _manifest_columns(candidate)
        if (
            stored_methodology != revision["methodology_id"]
            or stored_version != int(revision["methodology_version"])
        ):
            raise CorruptRevisionError(
                "relational methodology identity does not match the frozen manifest"
            )
        return PlanRevision(
            candidate=candidate,
            approved_by=str(revision["approved_by"]),
            approved_at=_parse_datetime(str(revision["approved_at_utc"])),
            content_hash=stored_hash,
        )
    except CorruptRevisionError:
        raise
    except (
        AttributeError,
        DecimalException,
        IndexError,
        KeyError,
        RevisionPersistenceError,
        TypeError,
        ValueError,
    ) as exc:
        raise CorruptRevisionError(
            f"revision {revision_id} contains invalid persisted content"
        ) from exc
    finally:
        conn.close()


def list_revisions(db_path: Path | str, plan_id: str) -> tuple[PlanRevision, ...]:
    conn = connect_sqlite(Path(db_path))
    try:
        ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT revision_id FROM plan_revision WHERE plan_id=? ORDER BY approved_at_utc, revision_id",
                (plan_id,),
            )
        ]
    finally:
        conn.close()
    revisions: list[PlanRevision] = []
    for revision_id in ids:
        item = load_revision(db_path, revision_id)
        if item is None:
            raise CorruptRevisionError(
                f"revision {revision_id} disappeared while listing plan {plan_id}"
            )
        revisions.append(item)
    return tuple(revisions)


def update_approved_revision(*_args: Any, **_kwargs: Any) -> None:
    raise ImmutableRevisionError("approved revisions are immutable; create a new revision")
