from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from garmin_data_hub.db import migrate
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.migrate import CURRENT_SCHEMA_VERSION, apply_schema
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.canonical import canonical_json
from garmin_data_hub.plan_methodology.domain import (
    ApprovalState,
    Confidence,
    FitzgeraldTarget,
    LoadMode,
    MaffetoneTarget,
    MeasureRole,
    MethodologyId,
    Metric,
    RevisionReason,
    SegmentKind,
    Sport,
    DomainError,
)
from garmin_data_hub.plan_methodology import revision_repository as repository
from garmin_data_hub.plan_methodology.prescriptions import (
    IntensityPrescription,
    MetricCeiling,
    MetricRange,
)
from garmin_data_hub.plan_methodology.revision_repository import (
    ContentIdentityError,
    CorruptRevisionError,
    ImmutableRevisionError,
    InjectedApprovalFailure,
    RevisionPersistenceError,
    StaleRevisionError,
    approve_revision,
    fail_at,
    list_revisions,
    load_plan,
    load_revision,
    update_approved_revision,
)
from garmin_data_hub.plan_methodology.activity_workout_match import create_match
from garmin_data_hub.plan_methodology.runtime_compliance import evaluate_confirmed_match
from garmin_data_hub.plan_methodology.revisions import PlanRevisionCandidate
from garmin_data_hub.plan_methodology.segments import PlannedWorkout, WorkoutSegment


from revision_fixtures import (
    _database, _manifest, _fitzgerald_candidate, _maffetone_candidate, _approve,
)


def test_confirmed_match_resolves_frozen_revision_and_targeted_activity(tmp_path):
    path = _database(tmp_path, "runtime.db")
    _approve(path, _fitzgerald_candidate())
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.execute(
            "INSERT INTO activity(activity_id,activity_type) VALUES (77,'running')"
        )
        conn.executemany(
            """INSERT INTO activity_trackpoints(
            activity_id,seq,timestamp_utc,speed_mps,heart_rate_bpm)
            VALUES (77,?,?,?,?)""",
            [
                (0, "2026-10-01T12:00:00Z", 3.2, 145),
                (1, "2026-10-01T12:00:30Z", 3.2, 145),
            ],
        )
        create_match(
            conn,
            revision_id="rev-a",
            workout_id="workout-1",
            activity_id=77,
            status="CONFIRMED",
            source="MANUAL",
            confidence="HIGH",
            reviewer="tester",
            reason="explicit identity review",
        )
        conn.commit()
    finally:
        conn.close()

    result = evaluate_confirmed_match(
        path, revision_id="rev-a", workout_id="workout-1"
    )
    assert result["revision_id"] == "rev-a"
    assert result["activity_id"] == 77
    assert result["primary_metric"] == "SPEED"
    assert result["coverage"]["metric_supported_seconds"] == Decimal("30.0")


def test_runtime_uses_frozen_speed_threshold_for_hr_primary_secondary_evidence(tmp_path):
    path = _database(tmp_path, "runtime-secondary.db")
    candidate = _fitzgerald_candidate()
    hr_primary = IntensityPrescription(
        methodology_id=MethodologyId.FITZGERALD_80_20_RUNNING_V1,
        native_target=FitzgeraldTarget.ZONE_3,
        primary=MetricRange(
            Metric.HEART_RATE,
            "bpm",
            Decimal("160.0"),
            Decimal("175.1"),
            True,
            False,
        ),
        secondary=MetricRange(
            Metric.SPEED,
            "m/s",
            Decimal("3.76"),
            Decimal("4.12"),
            True,
            False,
        ),
        derivation_ref="F80-SEVEN-ZONE-1.0.0",
        confidence=Confidence.REDUCED,
        data_quality_requirement="VALID_HR",
        parameter_snapshot_ref="threshold-hr-1",
    )
    workout = replace(
        candidate.workouts[0],
        segments=tuple(
            replace(segment, prescription=hr_primary)
            for segment in candidate.workouts[0].segments
        ),
    )
    candidate = replace(
        candidate,
        parameter_snapshot={
            "lthr": {"evidence_class": "MEASURED", "value": "170"},
            "pace": {"evidence_class": "MEASURED", "value": "4"},
        },
        workouts=(workout,),
    )
    _approve(path, candidate)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.execute(
            "INSERT INTO activity(activity_id,activity_type) VALUES (78,'running')"
        )
        conn.executemany(
            """INSERT INTO activity_trackpoints(
            activity_id,seq,timestamp_utc,speed_mps,heart_rate_bpm)
            VALUES (78,?,?,?,?)""",
            [
                (0, "2026-10-01T12:00:00Z", 4.0, 170),
                (1, "2026-10-01T12:00:30Z", 4.0, 170),
            ],
        )
        create_match(
            conn,
            revision_id="rev-a",
            workout_id="workout-1",
            activity_id=78,
            status="CONFIRMED",
            source="MANUAL",
            confidence="HIGH",
            reviewer="tester",
            reason="explicit identity review",
        )
        conn.commit()
    finally:
        conn.close()

    result = evaluate_confirmed_match(
        path, revision_id="rev-a", workout_id="workout-1"
    )
    secondary = result["secondary_evidence"]
    assert result["primary_metric"] == "HEART_RATE"
    assert secondary["metric"] == "SPEED"
    assert sum(secondary["native_zone_seconds"].values()) == Decimal("30.0")
    assert secondary["coverage"]["quality"] == "VALID"


def test_runtime_rejects_non_running_activity_before_methodology_evaluation(tmp_path):
    path = _database(tmp_path, "runtime-non-running.db")
    _approve(path, _fitzgerald_candidate())
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.execute(
            "INSERT INTO activity(activity_id,activity_type) VALUES (79,'cycling')"
        )
        create_match(
            conn,
            revision_id="rev-a",
            workout_id="workout-1",
            activity_id=79,
            status="CONFIRMED",
            source="MANUAL",
            confidence="HIGH",
            reviewer="tester",
            reason="explicit identity review",
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(DomainError, match="non-running"):
        evaluate_confirmed_match(
            path, revision_id="rev-a", workout_id="workout-1"
        )


def test_v9_schema_is_additive_idempotent_and_has_expected_integrity(tmp_path):
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("INSERT INTO planned_workout(scheduled_date, workout_name) VALUES ('2026-01-01','Legacy')")
        conn.commit()
        apply_schema(conn, schema_sql_path())
        apply_schema(conn, schema_sql_path())
        assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == CURRENT_SCHEMA_VERSION == 15
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"training_plan", "plan_revision", "plan_revision_workout", "plan_workout_segment"} <= tables
        columns = {row[1] for row in conn.execute("PRAGMA table_info(planned_workout)")}
        assert {"source_plan_id", "source_revision_id", "source_workout_id"} <= columns
        indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        assert {"idx_plan_revision_parent", "idx_plan_revision_workout_calendar", "idx_plan_workout_segment_target"} <= indexes
        assert conn.execute("SELECT workout_name FROM planned_workout").fetchall() == [("Legacy",)]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_prior_v8_database_upgrades_without_mutating_legacy_rows(tmp_path):
    path = tmp_path / "v8.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE planned_workout(planned_workout_id INTEGER PRIMARY KEY, scheduled_date TEXT NOT NULL, workout_name TEXT, description TEXT, planned_distance_m REAL, planned_duration_s REAL, planned_tss REAL, structure_json TEXT, created_at TEXT)"
        )
        conn.execute("CREATE TABLE activity_metrics(activity_id INTEGER PRIMARY KEY)")
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at_utc TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.executemany(
            "INSERT INTO schema_migrations(version,name) VALUES (?,?)",
            [(version, f"migration {version}") for version in range(1, 9)],
        )
        conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name) VALUES ('2026-01-01','Legacy v8')")
        conn.commit()
        apply_schema(conn, schema_sql_path())
        assert conn.execute("SELECT workout_name, source_plan_id FROM planned_workout").fetchall() == [("Legacy v8", None)]
        assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == CURRENT_SCHEMA_VERSION
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_v9_migration_and_version_record_roll_back_together(tmp_path, monkeypatch):
    path = tmp_path / "v9-failure.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE planned_workout(planned_workout_id INTEGER PRIMARY KEY, scheduled_date TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at_utc TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.executemany(
            "INSERT INTO schema_migrations(version,name) VALUES (?,?)",
            [(version, f"migration {version}") for version in range(1, 9)],
        )
        conn.commit()

        def fail_v9(connection, _schema_sql):
            connection.execute("CREATE TABLE injected_partial_v9(value TEXT)")
            raise sqlite3.OperationalError("injected v9 migration failure")

        monkeypatch.setattr(migrate, "_migration_9_add_immutable_plan_revisions", fail_v9)
        with pytest.raises(sqlite3.OperationalError, match="injected v9"):
            apply_schema(conn, schema_sql_path())
        assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 8
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='injected_partial_v9'"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_recorded_v9_repairs_an_incomplete_restart_state(tmp_path):
    path = tmp_path / "incomplete-v9.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE planned_workout(planned_workout_id INTEGER PRIMARY KEY, scheduled_date TEXT NOT NULL)")
        conn.execute("CREATE TABLE activity_metrics(activity_id INTEGER PRIMARY KEY)")
        conn.execute(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at_utc TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.executemany(
            "INSERT INTO schema_migrations(version,name) VALUES (?,?)",
            [(version, f"migration {version}") for version in range(1, 10)],
        )
        conn.commit()
        apply_schema(conn, schema_sql_path())
        columns = {row[1] for row in conn.execute("PRAGMA table_info(planned_workout)")}
        assert {"source_plan_id", "source_revision_id", "source_workout_id"} <= columns
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_planned_workout_revision_projection'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='plan_revision'"
        ).fetchone()[0] == 1
        assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == CURRENT_SCHEMA_VERSION
    finally:
        conn.close()


def test_fitzgerald_roundtrip_preserves_semantics_hash_and_projection(tmp_path):
    path = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    result = _approve(path, candidate)
    loaded = load_revision(path, candidate.revision_id)
    assert result.content_sha256 == candidate.content_hash
    assert loaded is not None
    assert loaded.content_hash == candidate.content_hash == loaded.candidate.content_hash
    assert canonical_json(loaded.candidate) == canonical_json(candidate)
    assert load_plan(path, "plan-1").current_revision_id == candidate.revision_id
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT scheduled_date, workout_name, description, planned_distance_m, planned_duration_s, planned_tss, structure_json FROM planned_workout WHERE source_plan_id='plan-1'"
        ).fetchone()
        assert row[:3] == ("2026-10-01", "Foundation Run rev-a", "Run easily, then finish with controlled work.")
        assert row[3] is None  # not all leaves have a distance; no invented total
        assert row[4] == 3000
        assert row[5] is None
        assert "F80.ZONE_2" in row[6]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_maffetone_roundtrip_is_inclusive_and_privacy_minimized(tmp_path):
    path = _database(tmp_path)
    candidate = _maffetone_candidate()
    _approve(path, candidate)
    loaded = load_revision(path, candidate.revision_id)
    assert loaded is not None and loaded.candidate.content_hash == candidate.content_hash
    rx = loaded.candidate.workouts[0].segments[1].prescription
    assert rx.primary.lower_inclusive and rx.primary.upper_inclusive
    assert rx.ceiling.value == Decimal("135") and rx.ceiling.inclusive
    raw = path.read_bytes()
    assert b"SYNTHETIC-SENSITIVE-QUESTIONNAIRE-ANSWER" not in raw


def test_raw_maffetone_questionnaire_payload_is_rejected_without_writes(tmp_path):
    path = _database(tmp_path)
    candidate = _maffetone_candidate()
    unsafe = replace(
        candidate,
        parameter_snapshot={
            **dict(candidate.parameter_snapshot),
            "raw_health_answers": {"sentinel": "SYNTHETIC-SENSITIVE-QUESTIONNAIRE-ANSWER"},
        },
    )
    with pytest.raises(RevisionPersistenceError, match="questionnaire"):
        _approve(path, unsafe)
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM training_plan").fetchone()[0] == 0
    finally:
        conn.close()


def test_stale_parent_and_hash_guards_preserve_current_revision_and_projection(tmp_path):
    path = _database(tmp_path)
    first = _fitzgerald_candidate()
    _approve(path, first)
    winner = _fitzgerald_candidate("rev-b", "rev-a", 2)
    loser = _fitzgerald_candidate("rev-c", "rev-a", 3)
    with pytest.raises(StaleRevisionError, match="content identity"):
        _approve(path, winner, "0" * 64)
    _approve(path, winner, first.content_hash)
    with pytest.raises(StaleRevisionError):
        _approve(path, loser, first.content_hash)
    tampered = replace(winner, revision_id="rev-tampered", change_summary={"changed_fields": ["tampered"]})
    with pytest.raises(ContentIdentityError):
        approve_revision(path, tampered, expected_content_hash=winner.content_hash)
    assert load_plan(path, "plan-1").current_revision_id == "rev-b"
    assert [item.candidate.revision_id for item in list_revisions(path, "plan-1")] == ["rev-a", "rev-b"]
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT DISTINCT source_revision_id FROM planned_workout WHERE source_plan_id='plan-1'").fetchall() == [("rev-b",)]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "stage",
    [
        "after_revision_insert",
        "after_workout_insert",
        "after_segment_insert",
        "during_projection",
        "before_active_pointer_update",
        "before_commit",
    ],
)
def test_failure_injection_rolls_back_graph_projection_and_pointer(tmp_path, stage):
    path = _database(tmp_path, f"failure-{stage}.db")
    first = _fitzgerald_candidate()
    _approve(path, first)
    candidate = _fitzgerald_candidate("rev-b", "rev-a", 2)
    with pytest.raises(InjectedApprovalFailure, match=stage):
        _approve(path, candidate, first.content_hash, fail_at(stage))
    assert load_plan(path, "plan-1").current_revision_id == "rev-a"
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM plan_revision WHERE revision_id='rev-b'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM plan_revision_workout WHERE revision_id='rev-b'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM plan_workout_segment WHERE revision_id='rev-b'").fetchone()[0] == 0
        assert conn.execute("SELECT DISTINCT source_revision_id FROM planned_workout WHERE source_plan_id='plan-1'").fetchall() == [("rev-a",)]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_projection_refresh_does_not_touch_legacy_rows_or_revision_history(tmp_path):
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute("INSERT INTO planned_workout(scheduled_date, workout_name) VALUES ('2026-10-01','Legacy Uncertified')")
    conn.commit()
    conn.close()
    first = _fitzgerald_candidate()
    _approve(path, first)
    second = _fitzgerald_candidate("rev-b", "rev-a", 2)
    _approve(path, second, first.content_hash)
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM plan_revision").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM planned_workout WHERE source_plan_id='plan-1'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM planned_workout WHERE workout_name='Legacy Uncertified' AND source_plan_id IS NULL").fetchone()[0] == 1
    finally:
        conn.close()


def test_normal_repository_api_has_no_in_place_revision_mutation(tmp_path):
    path = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    _approve(path, candidate)
    with pytest.raises(ImmutableRevisionError):
        update_approved_revision(path, candidate.revision_id, methodology_id="changed")
    loaded = load_revision(path, candidate.revision_id)
    assert loaded.content_hash == candidate.content_hash


def test_actual_commit_exception_rolls_back_and_discards_connection(tmp_path, monkeypatch):
    path = _database(tmp_path)
    first = _fitzgerald_candidate()
    _approve(path, first)
    second = _fitzgerald_candidate("rev-b", "rev-a", 2)
    connections = []

    class FailingCommitConnection(sqlite3.Connection):
        rollback_called = False
        close_called = False

        def commit(self):
            raise sqlite3.OperationalError("injected actual commit failure")

        def rollback(self):
            self.rollback_called = True
            return super().rollback()

        def close(self):
            self.close_called = True
            return super().close()

    def failing_connect(db_path):
        conn = sqlite3.connect(str(db_path), factory=FailingCommitConnection)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        connections.append(conn)
        return conn

    monkeypatch.setattr(repository, "connect_sqlite", failing_connect)
    with pytest.raises(sqlite3.OperationalError, match="actual commit failure"):
        _approve(path, second, first.content_hash)
    assert connections[0].rollback_called is True
    assert connections[0].close_called is True
    assert load_plan(path, "plan-1").current_revision_id == "rev-a"
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM plan_revision WHERE revision_id='rev-b'").fetchone()[0] == 0
        assert conn.execute("SELECT DISTINCT source_revision_id FROM planned_workout WHERE source_plan_id='plan-1'").fetchall() == [("rev-a",)]
    finally:
        conn.close()


def test_complex_ordered_roundtrip_covers_open_bounds_distance_event_and_same_date(tmp_path):
    path = _database(tmp_path)
    candidate = _fitzgerald_candidate("complex-rev")
    open_bound_rx = IntensityPrescription(
        methodology_id=MethodologyId.FITZGERALD_80_20_RUNNING_V1,
        native_target=FitzgeraldTarget.ZONE_5,
        primary=MetricRange(Metric.SPEED, "m/s", Decimal("4.5000"), None, True, False),
        secondary=None,
        derivation_ref="F80-SEVEN-ZONE-1.0.0",
        confidence=Confidence.REDUCED,
        data_quality_requirement="VALID_PACE",
    )
    distance_workout = PlannedWorkout(
        scheduled_date=date(2026, 10, 5),
        sport=Sport.RUNNING,
        family="INTERVAL",
        purpose="HIGH_INTENSITY",
        title="Distance Repetition",
        description="One distance-authoritative repetition.",
        segments=(
            WorkoutSegment(
                SegmentKind.WORK,
                LoadMode.DISTANCE,
                duration_seconds=300,
                distance_metres=1000,
                duration_role=MeasureRole.ESTIMATED,
                distance_role=MeasureRole.AUTHORITATIVE,
                prescription=open_bound_rx,
            ),
        ),
        workout_id="distance-workout",
        ordinal=0,
    )
    event_workout = PlannedWorkout(
        scheduled_date=date(2026, 10, 5),
        sport=Sport.RUNNING,
        family="EVENT",
        purpose="RACE",
        title="Goal Event",
        description="Event-day execution is not a training-zone prescription.",
        segments=(WorkoutSegment(SegmentKind.EVENT, LoadMode.OPEN),),
        event_flag=True,
        workout_id="event-workout",
        ordinal=1,
    )
    candidate = replace(candidate, workouts=(distance_workout, event_workout))
    _approve(path, candidate)
    loaded = load_revision(path, candidate.revision_id)
    assert canonical_json(loaded.candidate) == canonical_json(candidate)
    assert loaded.content_hash == candidate.content_hash
    assert [workout.workout_id for workout in loaded.candidate.workouts] == [
        "distance-workout",
        "event-workout",
    ]
    assert loaded.candidate.workouts[0].segments[0].prescription.primary.lower == Decimal("4.5")
    assert loaded.candidate.workouts[0].segments[0].prescription.secondary is None
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            "SELECT workout_name, planned_distance_m, planned_duration_s, structure_json FROM planned_workout WHERE source_plan_id='plan-1' ORDER BY planned_workout_id"
        ).fetchall()
        assert rows[0][:3] == ("Distance Repetition", 1000.0, 300.0)
        assert rows[1][:3] == ("Goal Event", None, None)
        assert '"event_flag":true' in rows[1][3]
    finally:
        conn.close()


def test_empty_and_invalid_ordering_edges_follow_domain_and_repository_invariants(tmp_path):
    path = _database(tmp_path)
    empty = replace(_fitzgerald_candidate(), workouts=())
    with pytest.raises(RevisionPersistenceError, match="at least one workout"):
        _approve(path, empty)
    with pytest.raises(DomainError, match="requires ordered leaf segments"):
        PlannedWorkout(
            date(2026, 10, 1),
            Sport.RUNNING,
            "EMPTY",
            "EMPTY",
            "Empty",
            (),
        )
    missing_ordinal = replace(
        _fitzgerald_candidate(),
        workouts=(replace(_fitzgerald_candidate().workouts[0], ordinal=None),),
    )
    with pytest.raises(RevisionPersistenceError, match="stable IDs and ordinals"):
        _approve(path, missing_ordinal)
    non_contiguous = replace(
        _fitzgerald_candidate(),
        workouts=(replace(_fitzgerald_candidate().workouts[0], ordinal=2),),
    )
    with pytest.raises(RevisionPersistenceError, match="contiguous"):
        _approve(path, non_contiguous)
    first_workout = _fitzgerald_candidate().workouts[0]
    duplicate_identity = replace(
        _fitzgerald_candidate(),
        workouts=(first_workout, replace(first_workout, ordinal=1)),
    )
    with pytest.raises(RevisionPersistenceError, match="unique"):
        _approve(path, duplicate_identity)
    duplicate_ordinal = replace(
        _fitzgerald_candidate(),
        workouts=(first_workout, replace(first_workout, workout_id="workout-2")),
    )
    with pytest.raises(RevisionPersistenceError, match="unique"):
        _approve(path, duplicate_ordinal)


def test_child_identity_constraints_and_production_foreign_keys(tmp_path):
    path = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    _approve(path, candidate)
    other = _maffetone_candidate()
    _approve(path, other)
    conn = connect_sqlite(path)
    try:
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO plan_workout_segment(revision_id,workout_id,segment_ordinal,segment_kind,load_mode) VALUES ('rev-a','workout-1',0,'WORK','OPEN')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO plan_workout_segment(revision_id,workout_id,segment_ordinal,segment_kind,load_mode) VALUES ('rev-a','another-workout',99,'EVENT','OPEN')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO plan_workout_segment(revision_id,workout_id,segment_ordinal,segment_kind,load_mode) VALUES ('rev-a','maf-workout-1',99,'EVENT','OPEN')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO plan_revision_workout(revision_id,workout_id,workout_ordinal,scheduled_date,sport,family,purpose,description) VALUES ('missing-revision','orphan',0,'2026-10-01','RUNNING','X','X','X')"
            )
        conn.rollback()
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_deferred_current_pointer_rejects_other_plan_revision_at_commit(tmp_path):
    path = _database(tmp_path)
    first = _fitzgerald_candidate()
    second_plan = _maffetone_candidate()
    _approve(path, first)
    _approve(path, second_plan)
    conn = connect_sqlite(path)
    try:
        conn.execute("BEGIN")
        conn.execute(
            "UPDATE training_plan SET current_revision_id=? WHERE plan_id=?",
            (second_plan.revision_id, first.plan_id),
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.commit()
        conn.rollback()
        conn.execute("BEGIN")
        conn.execute(
            "UPDATE training_plan SET current_revision_id='missing-revision' WHERE plan_id='plan-1'"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.commit()
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO plan_revision(revision_id,plan_id,revision_reason,methodology_id,methodology_version,content_sha256,manifest_json,goal_snapshot_json,athlete_snapshot_json,parameter_snapshot_json,approval_state,approved_by,approved_at_utc,created_at_utc) VALUES ('candidate-row','plan-1','ADAPTATION','FITZGERALD_80_20_RUNNING_V1',1,?,'{}','{}','{}','{}','CANDIDATE','reviewer','2026-01-01Z','2026-01-01Z')",
                ("f" * 64,),
            )
        conn.rollback()
        assert conn.execute(
            "SELECT current_revision_id FROM training_plan WHERE plan_id='plan-1'"
        ).fetchone()[0] == "rev-a"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_projection_replacement_is_scoped_to_one_of_multiple_plans(tmp_path):
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name) VALUES ('2026-10-01','Legacy')")
    conn.commit()
    conn.close()
    first = _fitzgerald_candidate()
    other = _maffetone_candidate()
    _approve(path, first)
    _approve(path, other)
    replacement = _fitzgerald_candidate("rev-b", "rev-a", 2)
    _approve(path, replacement, first.content_hash)
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            "SELECT source_plan_id, source_revision_id, workout_name FROM planned_workout ORDER BY planned_workout_id"
        ).fetchall()
        assert (None, None, "Legacy") in rows
        assert ("plan-1", "rev-b", "Foundation Run rev-b") in rows
        assert ("maf-plan-1", "maf-rev-1", "MAF Aerobic Run") in rows
        assert not any(row[1] == "rev-a" for row in rows)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "sensitive_key",
    [
        "raw_health_questionnaire",
        "medication_detail",
        "injury_detail",
        "credentials",
        "garmin_auth_session",
        "environment_variable",
        "ai_prompt_dump",
    ],
)
def test_sensitive_storage_keys_are_rejected_before_any_write(tmp_path, sensitive_key):
    path = _database(tmp_path, f"privacy-{sensitive_key}.db")
    candidate = _fitzgerald_candidate()
    unsafe = replace(
        candidate,
        provenance={**dict(candidate.provenance), sensitive_key: "SYNTHETIC-SENSITIVE-SENTINEL"},
    )
    with pytest.raises(RevisionPersistenceError, match="sensitive"):
        _approve(path, unsafe)
    assert all(
        b"SYNTHETIC-SENSITIVE-SENTINEL" not in candidate_path.read_bytes()
        for candidate_path in path.parent.glob(path.name + "*")
    )


def test_manifest_and_prescription_methodology_must_agree(tmp_path):
    path = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    maffetone_rx = _maffetone_candidate().workouts[0].segments[0].prescription
    mixed_workout = replace(
        candidate.workouts[0],
        segments=(replace(candidate.workouts[0].segments[0], prescription=maffetone_rx),),
    )
    mixed = replace(candidate, workouts=(mixed_workout,))
    with pytest.raises(RevisionPersistenceError, match="does not match"):
        _approve(path, mixed)


def test_candidate_aliases_cannot_create_hash_toctou_gap(tmp_path):
    path = _database(tmp_path)
    goal = {"sport": "RUNNING", "goal_intent": "COMPLETION"}
    metadata = {"schema_version": "workout.v1", "nested": {"value": "reviewed"}}
    base = _fitzgerald_candidate()
    candidate = replace(
        base,
        goal_snapshot=goal,
        workouts=(replace(base.workouts[0], metadata=metadata),),
    )
    reviewed_hash = candidate.content_hash
    goal["goal_intent"] = "ALTERED"
    metadata["nested"]["value"] = "ALTERED"
    assert candidate.content_hash == reviewed_hash
    result = approve_revision(
        path,
        candidate,
        expected_content_hash=reviewed_hash,
        approved_by="synthetic-reviewer",
    )
    assert result.content_sha256 == reviewed_hash
    altered = replace(candidate, change_summary={"changed_fields": ["ALTERED"]})
    with pytest.raises(ContentIdentityError):
        approve_revision(
            path,
            altered,
            expected_content_hash=reviewed_hash,
            approved_by="synthetic-reviewer",
        )


@pytest.mark.parametrize(
    "corruption",
    [
        "unknown_methodology",
        "malformed_manifest_json",
        "invalid_metric",
        "mismatched_hash",
        "missing_segment",
        "impossible_native_target",
        "segment_ordinal_gap",
        "noncanonical_document",
        "unsupported_qualifiers",
    ],
)
def test_corrupt_revision_rows_fail_explicitly(tmp_path, corruption):
    path = _database(tmp_path, f"corrupt-{corruption}.db")
    candidate = _fitzgerald_candidate()
    _approve(path, candidate)
    conn = sqlite3.connect(path)
    try:
        if corruption == "unknown_methodology":
            conn.execute("UPDATE plan_revision SET methodology_id='UNKNOWN' WHERE revision_id='rev-a'")
        elif corruption == "malformed_manifest_json":
            conn.execute("UPDATE plan_revision SET manifest_json='{' WHERE revision_id='rev-a'")
        elif corruption == "invalid_metric":
            conn.execute("UPDATE plan_workout_segment SET primary_metric='CADENCE' WHERE revision_id='rev-a' AND segment_ordinal=0")
        elif corruption == "mismatched_hash":
            conn.execute("UPDATE plan_revision SET content_sha256=? WHERE revision_id='rev-a'", ("0" * 64,))
        elif corruption == "missing_segment":
            conn.execute("DELETE FROM plan_workout_segment WHERE revision_id='rev-a' AND segment_ordinal=1")
        elif corruption == "impossible_native_target":
            conn.execute("UPDATE plan_workout_segment SET prescription_native_target='F80.IMPOSSIBLE' WHERE revision_id='rev-a' AND segment_ordinal=0")
        elif corruption == "segment_ordinal_gap":
            conn.execute("UPDATE plan_workout_segment SET segment_ordinal=9 WHERE revision_id='rev-a' AND segment_ordinal=1")
        elif corruption == "noncanonical_document":
            value = conn.execute(
                "SELECT goal_snapshot_json FROM plan_revision WHERE revision_id='rev-a'"
            ).fetchone()[0]
            conn.execute(
                "UPDATE plan_revision SET goal_snapshot_json=? WHERE revision_id='rev-a'",
                (value.replace(":", ": ", 1),),
            )
        else:
            conn.execute(
                "UPDATE plan_workout_segment SET prescription_qualifiers_json=? WHERE revision_id='rev-a' AND segment_ordinal=0",
                ('{"unexpected":true}',),
            )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(CorruptRevisionError):
        load_revision(path, "rev-a")


def test_load_plan_rejects_corrupt_cross_plan_current_pointer(tmp_path):
    path = _database(tmp_path)
    first = _fitzgerald_candidate()
    other = _maffetone_candidate()
    _approve(path, first)
    _approve(path, other)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "UPDATE training_plan SET current_revision_id=? WHERE plan_id=?",
            (other.revision_id, first.plan_id),
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(CorruptRevisionError, match="does not belong"):
        load_plan(path, first.plan_id)


def test_projection_cleanup_and_parent_delete_preserve_revision_history(tmp_path):
    path = _database(tmp_path)
    first = _fitzgerald_candidate()
    _approve(path, first)
    conn = connect_sqlite(path)
    try:
        conn.execute("DELETE FROM planned_workout WHERE source_plan_id='plan-1'")
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM plan_revision").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM training_plan WHERE plan_id='plan-1'")
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM plan_revision").fetchone()[0] == 1
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_fresh_and_v8_upgrade_have_matching_v9_revision_schema(tmp_path):
    fresh = _database(tmp_path, "fresh-parity.db")
    upgraded = tmp_path / "upgraded-parity.db"
    conn = sqlite3.connect(upgraded)
    conn.execute(
        "CREATE TABLE planned_workout(planned_workout_id INTEGER PRIMARY KEY, scheduled_date TEXT NOT NULL, workout_name TEXT, description TEXT, planned_distance_m REAL, planned_duration_s REAL, planned_tss REAL, structure_json TEXT, created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))"
    )
    conn.execute("CREATE TABLE activity_metrics(activity_id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at_utc TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.executemany(
        "INSERT INTO schema_migrations(version,name) VALUES (?,?)",
        [(version, f"migration {version}") for version in range(1, 9)],
    )
    conn.commit()
    apply_schema(conn, schema_sql_path())
    conn.close()

    def shape(path):
        database = sqlite3.connect(path)
        try:
            tables = {}
            for table in ("training_plan", "plan_revision", "plan_revision_workout", "plan_workout_segment"):
                tables[table] = database.execute(f"PRAGMA table_info({table})").fetchall()
            indexes = {
                row[0]: " ".join(row[1].split())
                for row in database.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type='index' AND (name LIKE 'idx_plan_%' OR name='idx_planned_workout_revision_projection')"
                )
            }
            projection_columns = {
                row[1]: row[2:6]
                for row in database.execute("PRAGMA table_info(planned_workout)")
                if row[1].startswith("source_")
            }
            return tables, indexes, projection_columns
        finally:
            database.close()

    assert shape(fresh) == shape(upgraded)
