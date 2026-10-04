"""Reusable, fully prescribed approved-revision fixtures."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal


from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.domain import ApprovalState, Confidence, FitzgeraldTarget, LoadMode, MaffetoneTarget, MeasureRole, MethodologyId, Metric, RevisionReason, SegmentKind, Sport
from garmin_data_hub.plan_methodology.prescriptions import IntensityPrescription, MetricCeiling, MetricRange
from garmin_data_hub.plan_methodology.revision_repository import approve_revision
from garmin_data_hub.plan_methodology.revisions import PlanRevisionCandidate
from garmin_data_hub.plan_methodology.segments import PlannedWorkout, WorkoutSegment


def _database(tmp_path, name="revision.db"):
    path = tmp_path / name
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE activity(activity_id INTEGER PRIMARY KEY, activity_type TEXT)"
    )
    apply_schema(conn, schema_sql_path())
    conn.close()
    return path


def _manifest(methodology_id):
    return {
        "methodology_id": methodology_id.value,
        "methodology_version": 1,
        "specification_version": "1.0.0",
        "product_policy_version": "1.0.0",
        "effective_manifest_hash": "sha256:synthetic-manifest",
    }


def _fitzgerald_candidate(revision_id="rev-a", parent=None, day=1):
    rx = IntensityPrescription(
        methodology_id=MethodologyId.FITZGERALD_80_20_RUNNING_V1,
        native_target=FitzgeraldTarget.ZONE_2,
        primary=MetricRange(Metric.SPEED, "m/s", Decimal("3.04"), Decimal("3.48"), True, False),
        secondary=MetricRange(Metric.HEART_RATE, "bpm", Decimal("137.7"), Decimal("153"), True, False),
        derivation_ref="F80-SEVEN-ZONE-1.0.0",
        confidence=Confidence.HIGH,
        data_quality_requirement="VALID_PACE",
        parameter_snapshot_ref="threshold-1",
    )
    workout = PlannedWorkout(
        scheduled_date=date(2026, 10, day),
        sport=Sport.RUNNING,
        family="FOUNDATION_RUN",
        purpose="AEROBIC_DEVELOPMENT",
        title=f"Foundation Run {revision_id}",
        description="Run easily, then finish with controlled work.",
        segments=(
            WorkoutSegment(SegmentKind.WARMUP, LoadMode.DURATION, 600, None, MeasureRole.AUTHORITATIVE, None, prescription=rx),
            WorkoutSegment(SegmentKind.WORK, LoadMode.DURATION, 1800, 6000, MeasureRole.AUTHORITATIVE, MeasureRole.ESTIMATED, prescription=rx),
            WorkoutSegment(SegmentKind.COOLDOWN, LoadMode.DURATION, 600, None, MeasureRole.AUTHORITATIVE, None, prescription=rx),
        ),
        workout_id="workout-1",
        ordinal=0,
        phase="BASE",
        quality_flag=False,
        long_run_flag=False,
        metadata={"schema_version": "workout.v1"},
    )
    return PlanRevisionCandidate(
        revision_id=revision_id,
        plan_id="plan-1",
        parent_revision_id=parent,
        reason=RevisionReason.INITIAL_GENERATION if parent is None else RevisionReason.ADAPTATION,
        manifest=_manifest(MethodologyId.FITZGERALD_80_20_RUNNING_V1),
        goal_snapshot={"sport": "RUNNING", "goal_intent": "COMPLETION"},
        athlete_snapshot={"threshold_speed_mps": "4"},
        parameter_snapshot={"threshold_speed_mps": "4", "evidence": "MEASURED"},
        workouts=(workout,),
        provenance={"generator": "synthetic-test", "version": "1"},
        validation_summary={"schema_version": "validation.v1", "findings": []},
        constraints={"schema_version": "constraints.v1", "items": []},
        change_summary={"changed_fields": ["workouts"]},
        approval_state=ApprovalState.VALIDATED,
    )


def _maffetone_candidate():
    rx = IntensityPrescription(
        methodology_id=MethodologyId.MAFFETONE_RUNNING_V1,
        native_target=MaffetoneTarget.MAF_AEROBIC_RANGE,
        primary=MetricRange(Metric.HEART_RATE, "bpm", Decimal("125"), Decimal("135"), True, True),
        ceiling=MetricCeiling(Decimal("135"), True),
        derivation_ref="MAF-180-RANGE-1.0.0",
        confidence=Confidence.HIGH,
        data_quality_requirement="VALID_HR",
        parameter_snapshot_ref="maf-params-1",
    )
    workout = PlannedWorkout(
        scheduled_date=date(2026, 10, 2),
        sport=Sport.RUNNING,
        family="MAF_AEROBIC",
        purpose="AEROBIC_BASE",
        title="MAF Aerobic Run",
        description="Stay at or below the inclusive MAF ceiling.",
        segments=(
            WorkoutSegment(SegmentKind.WARMUP, LoadMode.DURATION, 600, None, MeasureRole.AUTHORITATIVE, None, prescription=rx),
            WorkoutSegment(SegmentKind.WORK, LoadMode.DURATION, 2400, None, MeasureRole.AUTHORITATIVE, None, prescription=rx),
        ),
        workout_id="maf-workout-1",
        ordinal=0,
        metadata={"schema_version": "workout.v1"},
    )
    return PlanRevisionCandidate(
        revision_id="maf-rev-1",
        plan_id="maf-plan-1",
        parent_revision_id=None,
        reason=RevisionReason.INITIAL_GENERATION,
        manifest=_manifest(MethodologyId.MAFFETONE_RUNNING_V1),
        goal_snapshot={"sport": "RUNNING", "goal_intent": "COMPLETION"},
        athlete_snapshot={"completed_age": 40},
        parameter_snapshot={
            "schema_version": "maf-parameters.v1",
            "formula_version": "MAF-180-RANGE-1.0.0",
            "completed_age": 40,
            "selected_adjustment": -5,
            "confirmed": True,
            "ceiling_bpm": 135,
            "lower_bpm": 125,
            "provenance": "USER_SELECTED",
        },
        workouts=(workout,),
        provenance={"generator": "synthetic-test"},
        validation_summary={"findings": []},
        constraints={"items": []},
        approval_state=ApprovalState.VALIDATED,
    )


def _approve(path, candidate, parent_hash=None, hook=None):
    return approve_revision(
        path,
        candidate,
        expected_content_hash=candidate.content_hash,
        expected_parent_content_hash=parent_hash,
        approved_by="synthetic-reviewer",
        approved_at=datetime(2026, 9, 30, 12, tzinfo=timezone.utc),
        failure_hook=hook,
    )


