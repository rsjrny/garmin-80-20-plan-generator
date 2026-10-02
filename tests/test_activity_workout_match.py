from __future__ import annotations

import sqlite3

import pytest

from garmin_data_hub.db import migrate
from garmin_data_hub.db.migrate import CURRENT_SCHEMA_VERSION, apply_schema
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.activity_workout_match import (
    MatchPersistenceError,
    create_match,
    load_confirmed_match,
    review_match,
)


def _database(tmp_path):
    conn = sqlite3.connect(tmp_path / "match.db")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("CREATE TABLE activity(activity_id INTEGER PRIMARY KEY)")
    apply_schema(conn, schema_sql_path())
    conn.execute(
        "INSERT INTO training_plan(plan_id,current_revision_id,created_at_utc) VALUES ('p',NULL,'2026-01-01T00:00:00Z')"
    )
    conn.execute(
        """INSERT INTO plan_revision(
        revision_id,plan_id,parent_revision_id,revision_reason,methodology_id,
        methodology_version,content_sha256,manifest_json,goal_snapshot_json,
        athlete_snapshot_json,parameter_snapshot_json,constraints_json,
        approval_state,approved_by,approved_at_utc,created_at_utc)
        VALUES ('r','p',NULL,'INITIAL_GENERATION','MAFFETONE_RUNNING_V1',1,
        ?, '{}','{}','{}','{}','{}','APPROVED','tester','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')""",
        ("a" * 64,),
    )
    conn.execute(
        """INSERT INTO plan_revision_workout(
        revision_id,workout_id,workout_ordinal,scheduled_date,sport,family,purpose,description)
        VALUES ('r','w',0,'2026-01-02','RUNNING','BASE','BASE','base')"""
    )
    conn.execute("INSERT INTO activity(activity_id) VALUES (7)")
    conn.commit()
    return conn


def test_match_migration_and_confirmed_uniqueness(tmp_path):
    conn = _database(tmp_path)
    try:
        assert CURRENT_SCHEMA_VERSION == 14
        first = create_match(
            conn,
            revision_id="r",
            workout_id="w",
            activity_id=7,
            status="CONFIRMED",
            source="MANUAL",
            confidence="HIGH",
            reviewer="tester",
            reason="explicit review",
        )
        loaded = load_confirmed_match(conn, revision_id="r", workout_id="w")
        assert loaded == first
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                "DELETE FROM activity_workout_match WHERE activity_workout_match_id=?",
                (first.activity_workout_match_id,),
            )
        with pytest.raises(MatchPersistenceError):
            create_match(
                conn,
                revision_id="r",
                workout_id="w",
                activity_id=7,
                status="CONFIRMED",
                source="MANUAL",
                confidence="HIGH",
            )
        assert conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0] == 1
    finally:
        conn.close()


def test_match_foreign_keys_and_transaction_rollback(tmp_path):
    conn = _database(tmp_path)
    try:
        with pytest.raises(MatchPersistenceError):
            create_match(
                conn,
                revision_id="missing",
                workout_id="w",
                activity_id=7,
                status="CONFIRMED",
                source="MANUAL",
                confidence="HIGH",
            )
        assert conn.execute("SELECT COUNT(*) FROM activity_workout_match").fetchone()[0] == 0
    finally:
        conn.close()


def test_candidate_requires_explicit_review_to_become_confirmed(tmp_path):
    conn = _database(tmp_path)
    try:
        candidate = create_match(
            conn,
            revision_id="r",
            workout_id="w",
            activity_id=7,
            status="CANDIDATE",
            source="RECONCILIATION",
            confidence="UNKNOWN",
        )
        assert load_confirmed_match(conn, activity_id=7) is None
        confirmed = review_match(
            conn,
            activity_workout_match_id=candidate.activity_workout_match_id,
            status="CONFIRMED",
            reviewer="tester",
            reason="explicit identity review",
        )
        assert confirmed.status == "CONFIRMED"
        with pytest.raises(MatchPersistenceError, match="already confirmed"):
            review_match(
                conn,
                activity_workout_match_id=confirmed.activity_workout_match_id,
                status="REJECTED",
                reviewer="tester",
                reason="attempted mutation",
            )
    finally:
        conn.close()


def test_v10_table_and_version_record_roll_back_together(tmp_path, monkeypatch):
    conn = sqlite3.connect(tmp_path / "rollback.db")
    try:
        conn.execute("CREATE TABLE activity(activity_id INTEGER PRIMARY KEY)")
        apply_schema(conn, schema_sql_path())
        conn.execute("DELETE FROM schema_migrations WHERE version>=10")
        conn.execute("DROP VIEW active_planned_workout")
        conn.execute("DROP TRIGGER trg_mapped_legacy_planned_workout_no_update")
        conn.execute("DROP TRIGGER trg_mapped_legacy_planned_workout_no_delete")
        conn.execute("DROP TABLE legacy_plan_conversion_source_workout")
        conn.execute("DROP TABLE legacy_plan_conversion")
        conn.execute("DROP INDEX uq_planned_workout_revision_projection")
        conn.execute("DROP TRIGGER trg_activity_workout_match_confirmed_no_update")
        conn.execute("DROP TRIGGER trg_activity_workout_match_confirmed_no_delete")
        conn.execute("DROP TABLE activity_workout_match")
        conn.commit()
        original = migrate._migration_10_add_activity_workout_match

        def fail_after_ddl(connection, schema_sql):
            original(connection, schema_sql)
            raise sqlite3.OperationalError("injected v10 failure")

        monkeypatch.setattr(migrate, "_migration_10_add_activity_workout_match", fail_after_ddl)
        with pytest.raises(sqlite3.OperationalError, match="injected v10"):
            apply_schema(conn, schema_sql_path())

        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='activity_workout_match'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 9
    finally:
        conn.close()


def test_confirmed_match_requires_explicit_review_provenance(tmp_path):
    conn = _database(tmp_path)
    try:
        with pytest.raises(MatchPersistenceError, match="reviewer and reason"):
            create_match(
                conn,
                revision_id="r",
                workout_id="w",
                activity_id=7,
                status="CONFIRMED",
                source="MANUAL",
                confidence="HIGH",
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO activity_workout_match(
                revision_id,workout_id,activity_id,status,source,confidence)
                VALUES ('r','w',7,'CONFIRMED','MANUAL','HIGH')"""
            )
    finally:
        conn.close()


def test_confirmed_lookup_requires_deterministic_identity(tmp_path):
    conn = _database(tmp_path)
    try:
        create_match(
            conn,
            revision_id="r",
            workout_id="w",
            activity_id=7,
            status="CONFIRMED",
            source="MANUAL",
            confidence="HIGH",
            reviewer="tester",
            reason="explicit identity review",
        )
        with pytest.raises(MatchPersistenceError, match="revision_id and workout_id"):
            load_confirmed_match(conn, revision_id="r")
        with pytest.raises(MatchPersistenceError, match="revision_id and workout_id"):
            load_confirmed_match(conn, workout_id="w")
    finally:
        conn.close()


def test_confirmed_uniqueness_blocks_incompatible_workout_and_activity_links(tmp_path):
    conn = _database(tmp_path)
    try:
        conn.execute(
            """INSERT INTO plan_revision_workout(
            revision_id,workout_id,workout_ordinal,scheduled_date,sport,family,purpose,description)
            VALUES ('r','w2',1,'2026-01-03','RUNNING','BASE','BASE','base two')"""
        )
        conn.execute("INSERT INTO activity(activity_id) VALUES (8)")
        create_match(
            conn,
            revision_id="r",
            workout_id="w",
            activity_id=7,
            status="CONFIRMED",
            source="MANUAL",
            confidence="HIGH",
            reviewer="tester",
            reason="explicit identity review",
        )
        with pytest.raises(MatchPersistenceError):
            create_match(
                conn,
                revision_id="r",
                workout_id="w",
                activity_id=8,
                status="CONFIRMED",
                source="MANUAL",
                confidence="HIGH",
                reviewer="tester",
                reason="conflicting activity",
            )
        with pytest.raises(MatchPersistenceError):
            create_match(
                conn,
                revision_id="r",
                workout_id="w2",
                activity_id=7,
                status="CONFIRMED",
                source="MANUAL",
                confidence="HIGH",
                reviewer="tester",
                reason="conflicting workout",
            )
        assert conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM activity_workout_match WHERE status='CONFIRMED'"
        ).fetchone()[0] == 1
    finally:
        conn.close()
