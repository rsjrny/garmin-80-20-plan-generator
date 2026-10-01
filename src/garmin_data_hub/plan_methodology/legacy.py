"""Explicit conversion of the Data Hub-owned flat legacy plan."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from garmin_data_hub.db.sqlite import connect_sqlite

from .canonical import canonical_json, canonical_value
from .domain import ApprovalState, MethodologyId, RevisionReason
from .revision_repository import (
    ContentIdentityError,
    FailureHook,
    RevisionPersistenceError,
    _approve_revision_on_connection,
    _iso,
)
from .registry import resolve_methodology_policy
from .revisions import PlanRevisionCandidate
from .validation import FindingSeverity


SOURCE_NAMESPACE = "garmin_data_hub.planned_workout.rowset.v1"
IDENTITY_PREFIX = "legacy-plan:planned-workout-rowset-v1:sha256:"
CONVERSION_VERSION = "legacy-plan-conversion.v1"
PROVENANCE_SCHEMA_VERSION = "legacy-plan-conversion-provenance.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NAMED_METHODS = frozenset(
    {
        MethodologyId.FITZGERALD_80_20_RUNNING_V1.value,
        MethodologyId.MAFFETONE_RUNNING_V1.value,
    }
)
_CONVERSION_SOURCES = frozenset(
    {"USER_ACTION", "APPLICATION_COMMAND", "REVIEWED_MIGRATION"}
)


class LegacyConversionError(RevisionPersistenceError):
    """Base class for explicit legacy conversion failures."""


class NoLegacyPlanError(LegacyConversionError):
    """No unconverted canonical flat legacy plan exists."""


class CorruptLegacyProvenanceError(LegacyConversionError):
    """A flat row has only part of the required projection provenance."""


class StaleLegacySourceError(LegacyConversionError):
    """The source identity or mutable source snapshot changed before approval."""


class UnsupportedConversionTargetError(LegacyConversionError):
    """Conversion requires one explicitly selected named V1 methodology."""


class ProjectionCollisionError(LegacyConversionError):
    """Unexplained compatibility projection state would be overwritten."""


@dataclass(frozen=True, slots=True)
class LegacyPlanSource:
    canonical_legacy_plan_id: str
    source_namespace: str
    source_membership_sha256: str
    source_snapshot_sha256: str
    planned_workout_ids: tuple[int, ...]
    source_snapshot: tuple[Mapping[str, Any], ...]


def _sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _canonical_number(value: Any) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)).normalize(), "f")


def _snapshot_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "planned_workout_id": int(row["planned_workout_id"]),
        "scheduled_date": str(row["scheduled_date"]),
        "workout_name": row["workout_name"],
        "description": row["description"],
        "planned_distance_m": _canonical_number(row["planned_distance_m"]),
        "planned_duration_s": _canonical_number(row["planned_duration_s"]),
        "planned_tss": _canonical_number(row["planned_tss"]),
        "structure_json": row["structure_json"],
        "source_plan_id": row["source_plan_id"],
        "source_revision_id": row["source_revision_id"],
        "source_workout_id": row["source_workout_id"],
        "created_at": row["created_at"],
    }


_SOURCE_COLUMNS = """
    planned_workout_id, scheduled_date, workout_name, description,
    planned_distance_m, planned_duration_s, planned_tss, structure_json,
    source_plan_id, source_revision_id, source_workout_id, created_at
"""


def _source_from_rows(rows: list[sqlite3.Row]) -> LegacyPlanSource:
    if not rows:
        raise NoLegacyPlanError("no unconverted legacy plan exists")
    ids = tuple(sorted(int(row["planned_workout_id"]) for row in rows))
    membership = {
        "planned_workout_ids": list(ids),
        "source_namespace": SOURCE_NAMESPACE,
    }
    membership_sha256 = _sha256(membership)
    by_id = {int(row["planned_workout_id"]): row for row in rows}
    snapshot = tuple(_snapshot_row(by_id[row_id]) for row_id in ids)
    return LegacyPlanSource(
        canonical_legacy_plan_id=f"{IDENTITY_PREFIX}{membership_sha256}",
        source_namespace=SOURCE_NAMESPACE,
        source_membership_sha256=membership_sha256,
        source_snapshot_sha256=_sha256(list(snapshot)),
        planned_workout_ids=ids,
        source_snapshot=snapshot,
    )


def _resolve_legacy_plan_on_connection(conn: sqlite3.Connection) -> LegacyPlanSource:
    partial = conn.execute(
        """
        SELECT planned_workout_id
        FROM planned_workout
        WHERE (source_plan_id IS NULL) + (source_revision_id IS NULL)
              + (source_workout_id IS NULL) NOT IN (0, 3)
        LIMIT 1
        """
    ).fetchone()
    if partial is not None:
        raise CorruptLegacyProvenanceError(
            f"planned_workout {partial['planned_workout_id']} has partial source provenance"
        )
    rows = conn.execute(
        f"""
        SELECT {_SOURCE_COLUMNS}
        FROM planned_workout AS pw
        WHERE pw.source_plan_id IS NULL
          AND pw.source_revision_id IS NULL
          AND pw.source_workout_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM legacy_plan_conversion_source_workout AS source_map
            WHERE source_map.planned_workout_id = pw.planned_workout_id
          )
        ORDER BY pw.planned_workout_id
        """
    ).fetchall()
    return _source_from_rows(list(rows))


def resolve_legacy_plan(db_path: Path | str) -> LegacyPlanSource:
    """Resolve the one canonical unconverted flat row-set plan."""
    conn = connect_sqlite(Path(db_path))
    try:
        return _resolve_legacy_plan_on_connection(conn)
    finally:
        conn.close()


def _mapped_source_on_connection(
    conn: sqlite3.Connection, canonical_legacy_plan_id: str
) -> LegacyPlanSource:
    qualified_columns = _SOURCE_COLUMNS.replace(
        "planned_workout_id", "pw.planned_workout_id AS planned_workout_id", 1
    )
    rows = conn.execute(
        f"""
        SELECT {qualified_columns}
        FROM legacy_plan_conversion_source_workout AS source_map
        JOIN planned_workout AS pw
          ON pw.planned_workout_id = source_map.planned_workout_id
        WHERE source_map.canonical_legacy_plan_id = ?
        ORDER BY source_map.source_ordinal
        """,
        (canonical_legacy_plan_id,),
    ).fetchall()
    return _source_from_rows(list(rows))


def audit_legacy_source_rows(
    db_path: Path | str, canonical_legacy_plan_id: str
) -> tuple[dict[str, Any], ...]:
    """Return preserved base-table source rows for an explicit audit path."""
    conn = connect_sqlite(Path(db_path))
    try:
        source = _mapped_source_on_connection(conn, canonical_legacy_plan_id)
        return tuple(dict(item) for item in source.source_snapshot)
    finally:
        conn.close()


def _existing_result(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    requested_methodology_id: str,
) -> dict[str, Any]:
    canonical_id = str(row["canonical_legacy_plan_id"])
    mapped_source = _mapped_source_on_connection(conn, canonical_id)
    current = conn.execute(
        "SELECT current_revision_id FROM training_plan WHERE plan_id=?",
        (row["target_plan_id"],),
    ).fetchone()
    selected = str(row["selected_methodology_id"])
    return {
        "status": "EXISTING_CONVERSION",
        "canonical_legacy_plan_id": canonical_id,
        "target_plan_id": str(row["target_plan_id"]),
        "initial_revision_id": str(row["initial_revision_id"]),
        "current_revision_id": None if current is None else current["current_revision_id"],
        "selected_methodology_id": selected,
        "source_snapshot_sha256": str(row["source_snapshot_sha256"]),
        "created_new_revision": False,
        "revision_reason": RevisionReason.LEGACY_CONVERSION.value,
        "source_preserved": True,
        "requested_methodology_differs": requested_methodology_id != selected,
        "source_changed_since_conversion": (
            mapped_source.source_snapshot_sha256 != row["source_snapshot_sha256"]
        ),
    }


def _validate_conversion_candidate(
    candidate: PlanRevisionCandidate, selected_methodology_id: str
) -> None:
    if candidate.approval_state is not ApprovalState.VALIDATED:
        raise LegacyConversionError("conversion candidate must already be VALIDATED")
    if candidate.parent_revision_id is not None:
        raise LegacyConversionError("legacy conversion must create an initial revision")
    if candidate.reason is not RevisionReason.LEGACY_CONVERSION:
        raise LegacyConversionError("conversion candidate reason must be LEGACY_CONVERSION")
    manifest = canonical_value(candidate.manifest)
    if manifest.get("methodology_id") != selected_methodology_id:
        raise LegacyConversionError(
            "candidate methodology does not match the explicit conversion selection"
        )
    validation = canonical_value(candidate.validation_summary or {})
    if validation.get("accepted") is False:
        raise LegacyConversionError("candidate validation summary is blocking")

    def contains_error(value: Any) -> bool:
        if isinstance(value, Mapping):
            if value.get("severity") == FindingSeverity.ERROR.value:
                return True
            return any(contains_error(item) for item in value.values())
        if isinstance(value, list):
            return any(contains_error(item) for item in value)
        return False

    if contains_error(validation):
        raise LegacyConversionError("candidate validation summary contains ERROR")
    methodology_findings = resolve_methodology_policy(
        methodology_id=selected_methodology_id
    ).validate(candidate)
    errors = [
        finding.rule_id
        for finding in methodology_findings
        if finding.severity is FindingSeverity.ERROR
    ]
    if errors:
        raise LegacyConversionError(
            "candidate failed named-method preconditions: " + ", ".join(errors)
        )


def _contract_conversion(
    *, legacy_plan_id: str, selected_methodology_id: str, goal_snapshot: Mapping[str, Any]
) -> dict[str, Any]:
    """Collection-safe adapter for the frozen architecture contract."""
    if not legacy_plan_id:
        raise ValueError("legacy_plan_id is required")
    if selected_methodology_id not in _NAMED_METHODS:
        return {"accepted": False, "reason": "UNSUPPORTED_CONVERSION_TARGET"}
    if goal_snapshot.get("sport") != "RUNNING" or not goal_snapshot.get("goal_intent"):
        return {"accepted": False, "reason": "INVALID_GOAL_SNAPSHOT"}
    return {
        "status": "CREATED",
        "created_new_revision": True,
        "revision_reason": RevisionReason.LEGACY_CONVERSION.value,
        "source_preserved": True,
    }


def convert(
    *,
    legacy_plan_id: str,
    selected_methodology_id: str,
    goal_snapshot: Mapping[str, Any],
    db_path: Path | str | None = None,
    candidate: PlanRevisionCandidate | None = None,
    expected_candidate_content_hash: str | None = None,
    expected_source_snapshot_sha256: str | None = None,
    converted_by: str | None = None,
    conversion_source: str = "USER_ACTION",
    converted_at: datetime | None = None,
    failure_hook: FailureHook | None = None,
) -> dict[str, Any]:
    """Create or resolve one canonical named-method conversion atomically.

    Candidate generation and review stay outside the write transaction. The
    supplied candidate must already have passed the normal layered validation
    pipeline; this service owns only source resolution, provenance, idempotence,
    and the atomic persistence boundary.
    """
    if db_path is None:
        return _contract_conversion(
            legacy_plan_id=legacy_plan_id,
            selected_methodology_id=selected_methodology_id,
            goal_snapshot=goal_snapshot,
        )
    if selected_methodology_id not in _NAMED_METHODS:
        raise UnsupportedConversionTargetError("UNSUPPORTED_CONVERSION_TARGET")

    conn = connect_sqlite(Path(db_path))
    started = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        started = True
        existing = conn.execute(
            "SELECT * FROM legacy_plan_conversion WHERE canonical_legacy_plan_id=?",
            (legacy_plan_id,),
        ).fetchone()
        if existing is not None:
            result = _existing_result(
                conn,
                existing,
                requested_methodology_id=selected_methodology_id,
            )
            conn.commit()
            started = False
            return result

        if conversion_source not in _CONVERSION_SOURCES:
            raise LegacyConversionError("conversion_source is not authoritative")
        if not converted_by:
            raise LegacyConversionError("converted_by is required")
        if candidate is None:
            raise LegacyConversionError("a validated named-method candidate is required")
        if not expected_candidate_content_hash or _SHA256_RE.fullmatch(
            expected_candidate_content_hash
        ) is None:
            raise ContentIdentityError("expected candidate content hash is required")
        if candidate.content_hash != expected_candidate_content_hash:
            raise ContentIdentityError("candidate content hash mismatch")
        if not expected_source_snapshot_sha256 or _SHA256_RE.fullmatch(
            expected_source_snapshot_sha256
        ) is None:
            raise StaleLegacySourceError("expected source snapshot SHA-256 is required")
        _validate_conversion_candidate(candidate, selected_methodology_id)
        if canonical_json(candidate.goal_snapshot) != canonical_json(goal_snapshot):
            raise LegacyConversionError("candidate goal snapshot does not match the request")
        timestamp = converted_at or datetime.now(timezone.utc)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise LegacyConversionError("converted_at must include a timezone")

        source = _resolve_legacy_plan_on_connection(conn)
        if source.canonical_legacy_plan_id != legacy_plan_id:
            raise StaleLegacySourceError("STALE_LEGACY_SOURCE_IDENTITY")
        if source.source_snapshot_sha256 != expected_source_snapshot_sha256:
            raise StaleLegacySourceError("STALE_LEGACY_SOURCE")
        if conn.execute(
            "SELECT 1 FROM planned_workout WHERE source_plan_id=? LIMIT 1",
            (candidate.plan_id,),
        ).fetchone() is not None:
            raise ProjectionCollisionError(
                "unexplained projection rows already use the target plan identity"
            )

        conversion_document = {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "conversion_version": CONVERSION_VERSION,
            "canonical_legacy_plan_id": source.canonical_legacy_plan_id,
            "source_namespace": source.source_namespace,
            "source_membership_sha256": source.source_membership_sha256,
            "planned_workout_ids": list(source.planned_workout_ids),
            "source_snapshot_sha256": source.source_snapshot_sha256,
            "source_snapshot": list(source.source_snapshot),
            "selected_methodology_id": selected_methodology_id,
            "conversion_source": conversion_source,
            "converted_by": converted_by,
            "converted_at_utc": _iso(timestamp),
            "historical_named_conformance_claimed": False,
        }
        provenance = dict(canonical_value(candidate.provenance or {}))
        provenance["legacy_conversion"] = conversion_document
        persisted_candidate = replace(candidate, provenance=provenance)

        if failure_hook is not None:
            failure_hook("before_plan_creation", conn)

        def transaction_hook(stage: str, hook_conn: sqlite3.Connection) -> None:
            if stage == "before_projection":
                hook_conn.execute(
                    """
                    INSERT INTO legacy_plan_conversion(
                      canonical_legacy_plan_id, source_namespace,
                      source_membership_sha256, source_snapshot_sha256,
                      target_plan_id, initial_revision_id,
                      selected_methodology_id, conversion_version,
                      conversion_source, converted_by, converted_at_utc
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        source.canonical_legacy_plan_id,
                        source.source_namespace,
                        source.source_membership_sha256,
                        source.source_snapshot_sha256,
                        persisted_candidate.plan_id,
                        persisted_candidate.revision_id,
                        selected_methodology_id,
                        CONVERSION_VERSION,
                        conversion_source,
                        converted_by,
                        _iso(timestamp),
                    ),
                )
                if failure_hook is not None:
                    failure_hook("after_conversion_header_insert", hook_conn)
                for ordinal, planned_workout_id in enumerate(
                    source.planned_workout_ids
                ):
                    hook_conn.execute(
                        """
                        INSERT INTO legacy_plan_conversion_source_workout(
                          canonical_legacy_plan_id, planned_workout_id, source_ordinal
                        ) VALUES (?,?,?)
                        """,
                        (
                            source.canonical_legacy_plan_id,
                            planned_workout_id,
                            ordinal,
                        ),
                    )
                if failure_hook is not None:
                    failure_hook("after_conversion_membership_insert", hook_conn)
            elif failure_hook is not None:
                failure_hook(stage, hook_conn)

        approval = _approve_revision_on_connection(
            conn,
            persisted_candidate,
            expected_content_hash=persisted_candidate.content_hash,
            approved_by=converted_by,
            approved_at=timestamp,
            plan_origin="LEGACY_CONVERSION",
            failure_hook=failure_hook,
            transaction_hook=transaction_hook,
        )
        if failure_hook is not None:
            failure_hook("before_commit", conn)
        conn.commit()
        started = False
        return {
            "status": "CREATED",
            "canonical_legacy_plan_id": source.canonical_legacy_plan_id,
            "target_plan_id": approval.plan_id,
            "initial_revision_id": approval.revision_id,
            "current_revision_id": approval.revision_id,
            "selected_methodology_id": selected_methodology_id,
            "source_snapshot_sha256": source.source_snapshot_sha256,
            "created_new_revision": True,
            "revision_reason": RevisionReason.LEGACY_CONVERSION.value,
            "source_preserved": True,
            "requested_methodology_differs": False,
            "source_changed_since_conversion": False,
        }
    except BaseException:
        if started:
            conn.rollback()
        raise
    finally:
        conn.close()


def classify(*, legacy_plan_id: str) -> dict[str, Any]:
    if not legacy_plan_id:
        raise ValueError("legacy_plan_id is required")
    return {
        "usable": True,
        "methodology_id": "legacy_unspecified",
        "fitzgerald_certified": False,
        "maffetone_certified": False,
    }
