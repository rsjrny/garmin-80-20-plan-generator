"""Reproduce Y0 architecture gaps using temporary synthetic databases only.

Run from the repository root with .venv/Scripts/python.exe
reports/yearly_y0/discovery_probe.py. No live database is opened.
"""
from dataclasses import replace
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import runpy

from garmin_data_hub.db.migrate import apply_schema, CURRENT_SCHEMA_VERSION
from garmin_data_hub.db.queries import delete_planned_workouts_in_range
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.activity_workout_match import (
    create_match, load_confirmed_match, MatchPersistenceError,
)
from garmin_data_hub.plan_methodology.revision_repository import approve_revision, load_revision
from garmin_data_hub.services.plan_persistence import active_plan_sha256

ROOT = Path(__file__).resolve().parents[2]
# Reuse an existing representative canonical candidate, without invoking its tests.
make_candidate = runpy.run_path(str(ROOT / "tests/test_plan_revision_persistence.py"))["_fitzgerald_candidate"]


def main():
    with TemporaryDirectory(prefix="training-planner-y0-") as temporary:
        database = Path(temporary) / "synthetic.db"
        conn = connect_sqlite(database)
        conn.execute("CREATE TABLE activity(activity_id INTEGER PRIMARY KEY, activity_type TEXT)")
        apply_schema(conn, schema_sql_path())
        assert CURRENT_SCHEMA_VERSION == 11
        conn.execute("INSERT INTO activity VALUES (7, 'running')")
        conn.commit()
        first = make_candidate()
        approve_revision(database, first, expected_content_hash=first.content_hash, approved_by="y0-probe")
        before_match = active_plan_sha256(conn)
        create_match(conn, revision_id=first.revision_id, workout_id="workout-1",
                     activity_id=7, status="CONFIRMED", source="MANUAL", confidence="HIGH",
                     reviewer="y0-probe", reason="synthetic completion evidence")
        conn.commit()
        assert active_plan_sha256(conn) == before_match
        print("PASS: confirming an activity leaves the existing active schedule hash unchanged")

        second = replace(first, revision_id="rev-b", parent_revision_id=first.revision_id,
                         change_summary={"probe": "carry unchanged workout"})
        approve_revision(database, second, expected_content_hash=second.content_hash,
                         expected_parent_content_hash=first.content_hash, approved_by="y0-probe")
        assert load_confirmed_match(conn, revision_id=first.revision_id, workout_id="workout-1") is not None
        assert load_confirmed_match(conn, revision_id=second.revision_id, workout_id="workout-1") is None
        try:
            create_match(conn, revision_id=second.revision_id, workout_id="workout-1",
                         activity_id=7, status="CONFIRMED", source="MANUAL", confidence="HIGH",
                         reviewer="y0-probe", reason="cannot duplicate confirmed activity")
        except MatchPersistenceError:
            conn.rollback()
        else:
            raise AssertionError("confirmed activity unexpectedly duplicated across revisions")
        print("PASS: unchanged copied workout needs lineage; confirmed activity cannot be duplicated")

        assert conn.execute("SELECT DISTINCT source_revision_id FROM planned_workout WHERE source_plan_id=?",
                            (first.plan_id,)).fetchall()[0][0] == second.revision_id
        assert load_revision(database, first.revision_id).candidate.workouts == first.workouts
        print("PASS: approval replaces the whole plan projection while retaining prior immutable workouts")

        # This demonstrates a current legacy helper gap; it mutates only synthetic.db.
        delete_planned_workouts_in_range(conn, "2026-10-01", "2026-10-01")
        assert conn.execute("SELECT COUNT(*) FROM active_planned_workout").fetchone()[0] == 0
        assert conn.execute("SELECT current_revision_id FROM training_plan").fetchone()[0] == second.revision_id
        assert load_revision(database, second.revision_id) is not None
        print("PASS: legacy range helper can delete canonical projection without changing its revision pointer")

        columns = {row[1] for row in conn.execute("PRAGMA table_info(planned_workout)")}
        assert not columns & {"locked", "manually_edited", "completed", "season_plan_id"}
        apply_schema(conn, schema_sql_path())
        assert load_confirmed_match(conn, revision_id=first.revision_id, workout_id="workout-1") is not None
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        conn.close()
        print("PASS: v11 has no explicit protection flags; idempotent migration retains completion evidence")
    print("5 discovery probes passed; temporary synthetic database removed")


if __name__ == "__main__":
    main()
