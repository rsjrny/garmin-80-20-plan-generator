from __future__ import annotations

import copy
import hashlib
import json
import sqlite3

import pytest

from architecture_cases import REGISTRY_SNAPSHOT_CASES, REVISION_CASES
from conftest import assert_contract_result
from revision_fixtures import _database, _fitzgerald_candidate, _approve
from garmin_data_hub.plan_methodology.activity_workout_match import create_match
from garmin_data_hub.plan_methodology.revision_repository import load_revision
from garmin_data_hub.plan_methodology.runtime_compliance import evaluate_confirmed_match


@pytest.mark.parametrize("case", REVISION_CASES, ids=lambda case: case.test_id)
def test_revision_contract(case, future_api, tmp_path):
    if case.test_id == "ARCH-REV-011":
        # Verify the real persistence/evaluation boundary, not compatibility flags.
        db = _database(tmp_path)
        _approve(db, _fitzgerald_candidate())
        before = load_revision(db, "rev-a")
        with sqlite3.connect(db) as conn:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("INSERT INTO activity VALUES (77, 'running')")
            conn.executemany(
                "INSERT INTO activity_trackpoints(activity_id,seq,timestamp_utc,speed_mps) "
                "VALUES(77,?,?,3.2)",
                [(0, "2026-10-01T12:00:00Z"), (1, "2026-10-01T12:00:30Z")],
            )
            create_match(conn, revision_id="rev-a", workout_id="workout-1", activity_id=77,
                         status="CONFIRMED", source="MANUAL", confidence="HIGH",
                         reviewer="contract-reviewer", reason="Reviewed activity identity")
        result = evaluate_confirmed_match(db, revision_id="rev-a", workout_id="workout-1")
        assert result["activity_id"] == 77
        assert result["coverage"]["metric_supported_seconds"] == 30
        after = load_revision(db, "rev-a")
        assert after == before
        with sqlite3.connect(db) as conn:
            match = conn.execute(
                "SELECT activity_id FROM activity_workout_match WHERE revision_id='rev-a'"
            ).fetchone()
            assert match == (77,)
        return

    actual = future_api.call(case)
    assert_contract_result(actual, case.expected)
    if case.test_id == "ARCH-REV-004":
        revision = dict(case.payload["revision"])
        prescribed = {key: value for key, value in revision.items()
                      if key not in {"runtime_matches", "ui_state"}}
        expected = hashlib.sha256(json.dumps(
            prescribed, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()).hexdigest()
        assert actual["content_hash"] == expected
        modified = copy.deepcopy(revision)
        modified["runtime_matches"] = [{"activity_id": "different-observation"}]
        modified["ui_state"] = {"selected": False, "expanded": True}
        changed = type(case)(**{**case.__dict__, "payload": {"revision": modified}})
        assert future_api.call(changed)["content_hash"] == expected
        modified["workouts"][0]["segments"] = [1, 3]
        assert future_api.call(changed)["content_hash"] != expected


@pytest.mark.parametrize("case", REGISTRY_SNAPSHOT_CASES, ids=lambda case: case.test_id)
def test_registry_and_snapshot_contract(case, future_api):
    assert_contract_result(future_api.call(case), case.expected)
