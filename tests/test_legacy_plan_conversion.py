from __future__ import annotations

import sqlite3
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from garmin_data_hub.db.migrate import CURRENT_SCHEMA_VERSION, apply_schema
from garmin_data_hub.db import migrate
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.domain import (
    ApprovalState,
    Confidence,
    FitzgeraldTarget,
    LoadMode,
    MeasureRole,
    MethodologyId,
    MaffetoneTarget,
    Metric,
    RevisionReason,
    SegmentKind,
    Sport,
)
from garmin_data_hub.plan_methodology.legacy import (
    CorruptLegacyProvenanceError,
    LegacyConversionError,
    NoLegacyPlanError,
    ProjectionCollisionError,
    StaleLegacySourceError,
    audit_legacy_source_rows,
    convert,
    resolve_legacy_plan,
)
from garmin_data_hub.plan_methodology.prescriptions import (
    IntensityPrescription,
    MetricCeiling,
    MetricRange,
)
from garmin_data_hub.plan_methodology.revision_repository import (
    InjectedApprovalFailure,
    RevisionPersistenceError,
    fail_at,
)
from garmin_data_hub.plan_methodology.revisions import PlanRevisionCandidate
from garmin_data_hub.plan_methodology.segments import PlannedWorkout, WorkoutSegment


GOAL = {"sport": "RUNNING", "goal_intent": "COMPLETION"}
METHOD = MethodologyId.FITZGERALD_80_20_RUNNING_V1.value


def _database(tmp_path, name="legacy-conversion.db"):
    path = tmp_path / name
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE activity(activity_id INTEGER PRIMARY KEY)")
    apply_schema(conn, schema_sql_path())
    assert CURRENT_SCHEMA_VERSION == 15
    conn.execute(
        """
        INSERT INTO planned_workout(
          scheduled_date, workout_name, description, planned_distance_m,
          planned_duration_s, planned_tss, structure_json, created_at
        ) VALUES ('2026-10-01','Legacy Easy','Historical notes',5000,1800,42,
                  '{"legacy_zone":2}','2026-09-01T00:00:00.000Z')
        """
    )
    conn.commit()
    conn.close()
    return path


def _candidate(plan_id="converted-plan", revision_id="converted-revision"):
    prescription = IntensityPrescription(
        methodology_id=MethodologyId.FITZGERALD_80_20_RUNNING_V1,
        native_target=FitzgeraldTarget.ZONE_2,
        primary=MetricRange(
            Metric.SPEED,
            "m/s",
            Decimal("3.04"),
            Decimal("3.48"),
            True,
            False,
        ),
        derivation_ref="F80-SEVEN-ZONE-1.0.0",
        confidence=Confidence.HIGH,
        data_quality_requirement="VALID_PACE",
        parameter_snapshot_ref="threshold-pace-1",
    )
    workout = PlannedWorkout(
        scheduled_date=date(2026, 10, 1),
        sport=Sport.RUNNING,
        family="AEROBIC",
        purpose="AEROBIC_DEVELOPMENT",
        title="Generated Foundation Run",
        description="Current named-method prescription, not translated legacy intensity.",
        segments=(
            WorkoutSegment(
                SegmentKind.WORK,
                LoadMode.DURATION,
                1800,
                None,
                MeasureRole.AUTHORITATIVE,
                None,
                purpose="AEROBIC_DEVELOPMENT",
                prescription=prescription,
            ),
        ),
        workout_id="generated-workout-1",
        ordinal=0,
        metadata={"schema_version": "workout.v1"},
    )
    return PlanRevisionCandidate(
        revision_id=revision_id,
        plan_id=plan_id,
        parent_revision_id=None,
        reason=RevisionReason.LEGACY_CONVERSION,
        manifest={
            "methodology_id": METHOD,
            "methodology_version": 1,
            "specification_version": "1.0.0",
            "product_policy_version": "1.0.0",
            "effective_manifest_hash": "sha256:synthetic-manifest",
        },
        goal_snapshot=GOAL,
        athlete_snapshot={
            "threshold_speed_mps": "4",
            "evidence_class": "PRESCRIPTION_GRADE",
        },
        parameter_snapshot={
            "pace": {
                "value": "4",
                "quality": "VALID",
                "evidence_class": "MEASURED",
            },
            "lthr": {},
            "context": "STEADY_AEROBIC",
        },
        workouts=(workout,),
        provenance={"generator": "deterministic-test-generator"},
        validation_summary={
            "layers": [
                "STRUCTURAL",
                "SHARED_PLAN_INTEGRITY",
                "SHARED_SAFETY",
                "METHODOLOGY",
                "PERSISTENCE_PRECONDITIONS",
            ],
            "findings": [],
        },
        constraints={"items": []},
        approval_state=ApprovalState.VALIDATED,
    )


def _maffetone_candidate():
    prescription = IntensityPrescription(
        methodology_id=MethodologyId.MAFFETONE_RUNNING_V1,
        native_target=MaffetoneTarget.MAF_AEROBIC_RANGE,
        primary=MetricRange(
            Metric.HEART_RATE,
            "bpm",
            Decimal("125"),
            Decimal("135"),
            True,
            True,
        ),
        ceiling=MetricCeiling(Decimal("135"), True),
        derivation_ref="MAF-180-RANGE-1.0.0",
        confidence=Confidence.HIGH,
        data_quality_requirement="VALID_HR",
        parameter_snapshot_ref="maf-params-1",
    )
    segments = tuple(
        WorkoutSegment(
            kind,
            LoadMode.DURATION,
            seconds,
            None,
            MeasureRole.AUTHORITATIVE,
            None,
            purpose="AEROBIC_BASE",
            prescription=prescription,
        )
        for kind, seconds in (
            (SegmentKind.WARMUP, 720),
            (SegmentKind.WORK, 1800),
            (SegmentKind.COOLDOWN, 720),
        )
    )
    workout = PlannedWorkout(
        scheduled_date=date(2026, 10, 1),
        sport=Sport.RUNNING,
        family="AEROBIC",
        purpose="AEROBIC_BASE",
        title="Generated MAF Aerobic Run",
        description="Stay inside the confirmed inclusive MAF range.",
        segments=segments,
        workout_id="generated-maf-workout-1",
        ordinal=0,
        metadata={"schema_version": "workout.v1"},
    )
    return PlanRevisionCandidate(
        revision_id="converted-maf-revision",
        plan_id="converted-maf-plan",
        parent_revision_id=None,
        reason=RevisionReason.LEGACY_CONVERSION,
        manifest={
            "methodology_id": MethodologyId.MAFFETONE_RUNNING_V1.value,
            "methodology_version": 1,
            "specification_version": "1.0.0",
            "product_policy_version": "1.0.0",
            "effective_manifest_hash": "sha256:synthetic-maf-manifest",
        },
        goal_snapshot=GOAL,
        athlete_snapshot={"completed_age": 40},
        parameter_snapshot={
            "schema_version": "maf-parameters.v1",
            "formula_version": "MAF-180-RANGE-1.0.0",
            "calculation_date": "2026-09-30",
            "completed_age": 40,
            "selected_adjustment": -5,
            "confirmed": True,
            "ceiling_bpm": 135,
            "lower_bpm": 125,
            "provenance": "USER_SELECTED",
            "age_provenance": "DATE_OF_BIRTH_AS_OF_CALCULATION_DATE",
            "state": "AEROBIC_BASE",
        },
        workouts=(workout,),
        provenance={"generator": "deterministic-test-generator"},
        validation_summary={"accepted": True, "findings": []},
        constraints={"items": []},
        approval_state=ApprovalState.VALIDATED,
    )


def _convert(path, source, candidate=None, **kwargs):
    reviewed_candidate = candidate or _candidate()
    return convert(
        db_path=path,
        legacy_plan_id=source.canonical_legacy_plan_id,
        selected_methodology_id=METHOD,
        goal_snapshot=GOAL,
        candidate=reviewed_candidate,
        expected_candidate_content_hash=kwargs.pop(
            "expected_candidate_content_hash", reviewed_candidate.content_hash
        ),
        expected_source_snapshot_sha256=source.source_snapshot_sha256,
        converted_by="reviewer-1",
        converted_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
        **kwargs,
    )


def _counts(path):
    conn = sqlite3.connect(path)
    try:
        return tuple(
            conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "training_plan",
                "plan_revision",
                "plan_revision_workout",
                "plan_workout_segment",
                "legacy_plan_conversion",
                "legacy_plan_conversion_source_workout",
            )
        ) + (
            conn.execute(
                "SELECT COUNT(*) FROM planned_workout WHERE source_plan_id IS NOT NULL"
            ).fetchone()[0],
        )
    finally:
        conn.close()


def test_identity_uses_only_namespace_and_sorted_row_ids(tmp_path):
    path = _database(tmp_path)
    first = resolve_legacy_plan(path)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        UPDATE planned_workout
        SET scheduled_date='2030-01-01', workout_name='Changed', description='Changed',
            planned_distance_m=9999, planned_duration_s=999, planned_tss=99,
            structure_json='{"generic_zone":7}', created_at='2030-01-01Z'
        WHERE planned_workout_id=?
        """,
        (first.planned_workout_ids[0],),
    )
    conn.commit()
    conn.close()
    second = resolve_legacy_plan(path)
    assert second.canonical_legacy_plan_id == first.canonical_legacy_plan_id
    assert second.source_membership_sha256 == first.source_membership_sha256
    assert second.source_snapshot_sha256 != first.source_snapshot_sha256


def test_partial_projection_provenance_blocks_enumeration(tmp_path):
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute("UPDATE planned_workout SET source_plan_id='partial'")
    conn.commit()
    conn.close()
    with pytest.raises(CorruptLegacyProvenanceError):
        resolve_legacy_plan(path)


def test_first_and_repeat_conversion_are_canonical_and_source_preserving(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    conn = sqlite3.connect(path)
    before = conn.execute("SELECT * FROM planned_workout").fetchall()
    conn.close()

    created = _convert(path, source)
    assert created["status"] == "CREATED"
    assert created["created_new_revision"] is True
    assert created["revision_reason"] == "LEGACY_CONVERSION"
    assert created["source_preserved"] is True
    assert _counts(path) == (1, 1, 1, 1, 1, 1, 1)

    repeated = convert(
        db_path=path,
        legacy_plan_id=source.canonical_legacy_plan_id,
        selected_methodology_id=MethodologyId.MAFFETONE_RUNNING_V1.value,
        goal_snapshot=GOAL,
    )
    assert repeated["status"] == "EXISTING_CONVERSION"
    assert repeated["created_new_revision"] is False
    assert repeated["requested_methodology_differs"] is True
    assert _counts(path) == (1, 1, 1, 1, 1, 1, 1)

    repeated_again = convert(
        db_path=path,
        legacy_plan_id=source.canonical_legacy_plan_id,
        selected_methodology_id=METHOD,
        goal_snapshot=GOAL,
    )
    assert repeated_again["status"] == "EXISTING_CONVERSION"
    assert repeated_again["target_plan_id"] == created["target_plan_id"]
    assert repeated_again["initial_revision_id"] == created["initial_revision_id"]
    assert _counts(path) == (1, 1, 1, 1, 1, 1, 1)

    conn = sqlite3.connect(path)
    original = conn.execute(
        "SELECT * FROM planned_workout WHERE planned_workout_id=?",
        (source.planned_workout_ids[0],),
    ).fetchall()
    active = conn.execute(
        "SELECT planned_workout_id, source_workout_id FROM active_planned_workout"
    ).fetchall()
    projection = conn.execute(
        "SELECT planned_workout_id FROM planned_workout WHERE source_plan_id IS NOT NULL"
    ).fetchone()
    provenance = conn.execute(
        "SELECT provenance_json FROM plan_revision"
    ).fetchone()[0]
    matches = conn.execute("SELECT COUNT(*) FROM activity_workout_match").fetchone()[0]
    conn.close()
    assert original == before
    assert active == [(projection[0], "generated-workout-1")]
    assert projection[0] not in source.planned_workout_ids
    legacy_provenance = json.loads(provenance)["payload"]["legacy_conversion"]
    assert legacy_provenance["historical_named_conformance_claimed"] is False
    assert legacy_provenance["source_snapshot"][0]["structure_json"] == '{"legacy_zone":2}'
    assert matches == 0
    assert audit_legacy_source_rows(path, source.canonical_legacy_plan_id)[0][
        "workout_name"
    ] == "Legacy Easy"


def test_mapped_source_is_immutable_and_unrelated_rows_remain_active(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    _convert(path, source)
    conn = sqlite3.connect(path)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute(
            "UPDATE planned_workout SET workout_name='forbidden' WHERE planned_workout_id=?",
            (source.planned_workout_ids[0],),
        )
    conn.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
        conn.execute(
            "DELETE FROM planned_workout WHERE planned_workout_id=?",
            (source.planned_workout_ids[0],),
        )
    conn.rollback()
    conn.execute(
        """
        INSERT INTO planned_workout(
          scheduled_date, workout_name, description, planned_distance_m,
          planned_duration_s, planned_tss, structure_json, created_at
        ) SELECT scheduled_date, workout_name, description, planned_distance_m,
                 planned_duration_s, planned_tss, structure_json, created_at
          FROM planned_workout WHERE planned_workout_id=?
        """,
        (source.planned_workout_ids[0],),
    )
    conn.commit()
    assert conn.execute(
        "SELECT workout_name FROM active_planned_workout ORDER BY planned_workout_id"
    ).fetchall() == [("Generated Foundation Run",), ("Legacy Easy",)]
    conn.close()


def test_valid_maffetone_conversion_uses_confirmed_privacy_minimized_state(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    candidate = _maffetone_candidate()
    result = convert(
        db_path=path,
        legacy_plan_id=source.canonical_legacy_plan_id,
        selected_methodology_id=MethodologyId.MAFFETONE_RUNNING_V1.value,
        goal_snapshot=GOAL,
        candidate=candidate,
        expected_candidate_content_hash=candidate.content_hash,
        expected_source_snapshot_sha256=source.source_snapshot_sha256,
        converted_by="reviewer-1",
    )
    assert result["status"] == "CREATED"
    conn = sqlite3.connect(path)
    persisted = conn.execute(
        "SELECT parameter_snapshot_json, provenance_json FROM plan_revision"
    ).fetchone()
    conn.close()
    assert "raw_health" not in persisted[0].lower()
    assert "questionnaire" not in persisted[0].lower()
    assert "raw_health" not in persisted[1].lower()
    assert "questionnaire" not in persisted[1].lower()


def test_maffetone_raw_health_state_is_rejected_without_writes(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    candidate = _maffetone_candidate()
    parameters = dict(candidate.parameter_snapshot)
    parameters["raw_health_answers"] = {"sensitive-sentinel": True}
    candidate = replace(candidate, parameter_snapshot=parameters)
    with pytest.raises(LegacyConversionError, match="preconditions"):
        convert(
            db_path=path,
            legacy_plan_id=source.canonical_legacy_plan_id,
            selected_methodology_id=MethodologyId.MAFFETONE_RUNNING_V1.value,
            goal_snapshot=GOAL,
            candidate=candidate,
            expected_candidate_content_hash=candidate.content_hash,
            expected_source_snapshot_sha256=source.source_snapshot_sha256,
            converted_by="reviewer-1",
        )
    assert _counts(path) == (0, 0, 0, 0, 0, 0, 0)


def test_changed_source_after_conversion_reports_drift_without_revision_write(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    _convert(path, source)
    conn = sqlite3.connect(path)
    original_hash = conn.execute("SELECT content_sha256 FROM plan_revision").fetchone()[0]
    conn.execute("DROP TRIGGER trg_mapped_legacy_planned_workout_no_update")
    conn.execute(
        "UPDATE planned_workout SET description='externally corrupted' WHERE planned_workout_id=?",
        (source.planned_workout_ids[0],),
    )
    conn.commit()
    conn.close()
    repeated = convert(
        db_path=path,
        legacy_plan_id=source.canonical_legacy_plan_id,
        selected_methodology_id=METHOD,
        goal_snapshot=GOAL,
    )
    assert repeated["source_changed_since_conversion"] is True
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT content_sha256 FROM plan_revision").fetchone()[0] == original_hash
    assert conn.execute("SELECT COUNT(*) FROM plan_revision").fetchone()[0] == 1
    conn.close()


def test_two_distinct_row_sets_with_same_content_have_distinct_identity(tmp_path):
    path = _database(tmp_path)
    first = resolve_legacy_plan(path)
    _convert(path, first)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        INSERT INTO planned_workout(
          scheduled_date, workout_name, description, planned_distance_m,
          planned_duration_s, planned_tss, structure_json, created_at
        ) SELECT scheduled_date, workout_name, description, planned_distance_m,
                 planned_duration_s, planned_tss, structure_json, created_at
          FROM planned_workout WHERE planned_workout_id=?
        """,
        (first.planned_workout_ids[0],),
    )
    conn.commit()
    conn.close()
    second = resolve_legacy_plan(path)
    assert second.canonical_legacy_plan_id != first.canonical_legacy_plan_id


def test_stale_source_and_projection_collision_fail_before_any_graph_write(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    conn = sqlite3.connect(path)
    conn.execute("UPDATE planned_workout SET description='changed before approval'")
    conn.commit()
    conn.close()
    with pytest.raises(StaleLegacySourceError, match="STALE_LEGACY_SOURCE"):
        _convert(path, source)
    assert _counts(path) == (0, 0, 0, 0, 0, 0, 0)

    current = resolve_legacy_plan(path)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        INSERT INTO planned_workout(
          scheduled_date, source_plan_id, source_revision_id, source_workout_id
        ) VALUES ('2026-10-02','converted-plan','unrelated-revision','collision')
        """
    )
    conn.commit()
    conn.close()
    with pytest.raises(ProjectionCollisionError):
        _convert(path, current)
    assert _counts(path) == (0, 0, 0, 0, 0, 0, 1)


def test_reviewed_candidate_content_hash_is_required_and_enforced(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    reviewed = _candidate()
    tampered = replace(reviewed, provenance={"generator": "changed-after-review"})

    with pytest.raises(RevisionPersistenceError, match="content hash"):
        _convert(
            path,
            source,
            candidate=tampered,
            expected_candidate_content_hash=reviewed.content_hash,
        )

    assert _counts(path) == (0, 0, 0, 0, 0, 0, 0)


@pytest.mark.parametrize(
    "stage",
    (
        "before_plan_creation",
        "after_plan_insert",
        "after_revision_insert",
        "after_workout_insert",
        "after_segment_insert",
        "after_conversion_header_insert",
        "after_conversion_membership_insert",
        "during_projection",
        "after_projection",
        "before_active_pointer_update",
        "after_active_pointer_update",
        "before_commit",
    ),
)
def test_failure_injection_rolls_back_every_conversion_boundary(tmp_path, stage):
    path = _database(tmp_path, f"failure-{stage}.db")
    source = resolve_legacy_plan(path)
    conn = sqlite3.connect(path)
    before = conn.execute("SELECT * FROM planned_workout").fetchall()
    conn.close()
    with pytest.raises(InjectedApprovalFailure, match=stage):
        _convert(path, source, failure_hook=fail_at(stage))
    assert _counts(path) == (0, 0, 0, 0, 0, 0, 0)
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT * FROM planned_workout").fetchall() == before
    assert conn.execute("SELECT COUNT(*) FROM active_planned_workout").fetchone()[0] == 1
    conn.close()


def test_concurrent_duplicate_attempts_create_one_conversion(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)

    def invoke(_):
        return _convert(path, source)["status"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(invoke, range(2)))
    assert sorted(statuses) == ["CREATED", "EXISTING_CONVERSION"]
    assert _counts(path) == (1, 1, 1, 1, 1, 1, 1)


def test_no_eligible_rows_means_no_unconverted_plan(tmp_path):
    path = _database(tmp_path)
    source = resolve_legacy_plan(path)
    _convert(path, source)
    with pytest.raises(NoLegacyPlanError):
        resolve_legacy_plan(path)


def test_v11_objects_and_version_record_roll_back_together(tmp_path, monkeypatch):
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute("DROP VIEW active_planned_workout")
    conn.execute("DROP TRIGGER trg_mapped_legacy_planned_workout_no_update")
    conn.execute("DROP TRIGGER trg_mapped_legacy_planned_workout_no_delete")
    conn.execute("DROP TABLE legacy_plan_conversion_source_workout")
    conn.execute("DROP TABLE legacy_plan_conversion")
    conn.execute("DROP INDEX uq_planned_workout_revision_projection")
    conn.execute("DELETE FROM schema_migrations WHERE version>=11")
    conn.commit()
    original = migrate._migration_11_add_legacy_plan_conversion

    def fail_after_ddl(connection, schema_sql):
        original(connection, schema_sql)
        raise sqlite3.OperationalError("injected v11 failure")

    monkeypatch.setattr(migrate, "_migration_11_add_legacy_plan_conversion", fail_after_ddl)
    with pytest.raises(sqlite3.OperationalError, match="injected v11 failure"):
        apply_schema(conn, schema_sql_path())
    assert conn.execute(
        "SELECT COUNT(*) FROM schema_migrations WHERE version=11"
    ).fetchone()[0] == 0
    for object_type, name in (
        ("table", "legacy_plan_conversion"),
        ("table", "legacy_plan_conversion_source_workout"),
        ("view", "active_planned_workout"),
        ("index", "uq_planned_workout_revision_projection"),
    ):
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type=? AND name=?",
            (object_type, name),
        ).fetchone()[0] == 0
    conn.close()
