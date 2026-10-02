from dataclasses import replace
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db.queries import delete_planned_workouts_in_range, insert_planned_workout
from garmin_data_hub.plan_methodology.activity_workout_match import create_match, load_confirmed_match
from garmin_data_hub.plan_methodology.revision_repository import approve_revision
from garmin_data_hub.services import season_plans as s
from garmin_data_hub.services.plan_persistence import get_active_plan_sha256, save_generated_plan, save_imported_plan
from test_plan_revision_persistence import _database, _fitzgerald_candidate, _approve
from test_plan_persistence import _make_imported_plan
from test_legacy_plan_conversion import _database as legacy_database, _convert, resolve_legacy_plan


def draft(**changes):
    return replace(s.SeasonDraft("2026 season", "2026-01-01", "2026-12-31", "America/New_York"), **changes)


def snapshot(db):
    conn = sqlite3.connect(db)
    try:
        return tuple(conn.iterdump())
    finally:
        conn.close()


def linked(db):
    candidate = _fitzgerald_candidate()
    _approve(db, candidate)
    season = s.create_season(db, draft())
    return s.link_plan(db, season.season_id, candidate.plan_id, expected_version=season.input_version,
                       expected_revision_id=candidate.revision_id, expected_content_hash=candidate.content_hash), candidate


@pytest.mark.parametrize("changes", [
    {"name": " "}, {"start_date": "2026-02-30"}, {"start_date": "20260101"},
    {"end_date": "2025-12-31"}, {"end_date": "2027-01-02"}, {"timezone": "Not/AZone"},
    {"inputs": replace(s.SeasonInputs(), age=True)},
    {"inputs": replace(s.SeasonInputs(), run_days_per_week=0)},
    {"inputs": replace(s.SeasonInputs(), training_method="custom")},
    {"inputs": replace(s.SeasonInputs(), available_weekdays=(0, 1))},
    {"inputs": replace(s.SeasonInputs(), available_weekdays=(0, 1, 2, 3, 4), run_days_per_week=4)},
    {"inputs": replace(s.SeasonInputs(), available_weekdays=(0, 1, 2, 3, 4, 5, 5))},
    {"inputs": replace(s.SeasonInputs(), hrmax=160, lthr=165)},
    {"inputs": replace(s.SeasonInputs(), starting_duration_seconds=-1)},
])
def test_invalid_season_never_writes(tmp_path, changes):
    db = _database(tmp_path)
    before = snapshot(db)
    with pytest.raises(s.SeasonError):
        s.create_season(db, draft(**changes))
    assert snapshot(db) == before


def test_leap_year_and_cross_year_dates_persist(tmp_path):
    db = _database(tmp_path)
    one = s.create_season(db, draft(name="Leap", start_date="2028-01-01", end_date="2028-12-31"))
    two = s.create_season(db, draft(name="Cross-year", start_date="2026-10-02", end_date="2027-10-01"))
    assert s.get_season(db, one.season_id) == one
    assert s.list_seasons(db) == (two, one)
    with pytest.raises(s.SeasonError, match="overlap"):
        s.create_season(db, draft(start_date="2027-09-01", end_date="2027-10-31"))


def test_event_crud_priority_and_source_version(tmp_path):
    db = _database(tmp_path)
    season = s.create_season(db, draft())
    later = s.save_event(db, season.season_id, s.EventDraft("Fall", "2026-10-10", priority="B"), expected_version=1)
    first = s.save_event(db, season.season_id, s.EventDraft("Spring", "2026-04-01", "5K", "C"), expected_version=2)
    assert s.list_events(db, season.season_id) == (first, later)
    updated = s.save_event(db, season.season_id, replace(later.draft, priority="A", status="cancelled"),
                           expected_version=3, event_id=later.event_id)
    assert updated.draft.priority == "A"
    assert s.get_season(db, season.season_id).input_version == 4
    before = snapshot(db)
    with pytest.raises(s.StaleSeasonError):
        s.save_event(db, season.season_id, first.draft, expected_version=3)
    assert snapshot(db) == before
    s.remove_event(db, season.season_id, later.event_id, expected_version=4)
    assert s.list_events(db, season.season_id) == (first,)


@pytest.mark.parametrize("changes", [
    {"priority": "D"}, {"event_date": "2027-01-01"}, {"event_date": "2026-02-30"},
    {"distance_code": "custom"}, {"event_type": "cycle"}, {"status": "unknown"},
    {"goal_intent": "WIN"}, {"taper_days": 29}, {"recovery_days": 6},
    {"target_seconds": 0, "goal_intent": "PERFORMANCE"},
    {"target_speed_mps": "NaN", "goal_intent": "PERFORMANCE"},
    {"target_speed_mps": "1e-999999", "goal_intent": "PERFORMANCE"},
    {"target_speed_mps": "-1", "goal_intent": "PERFORMANCE"},
    {"target_seconds": 3600, "target_speed_mps": "3", "goal_intent": "PERFORMANCE"},
    {"target_seconds": 3600}, {"name": ""},
])
def test_invalid_event_never_writes(tmp_path, changes):
    db = _database(tmp_path)
    season = s.create_season(db, draft())
    before = snapshot(db)
    with pytest.raises(s.SeasonError):
        s.save_event(db, season.season_id, replace(s.EventDraft("Half", "2026-05-01"), **changes), expected_version=1)
    assert snapshot(db) == before


def test_target_units_duplicate_dates_and_complete_history(tmp_path):
    db = _database(tmp_path)
    season = s.create_season(db, draft())
    event = s.save_event(db, season.season_id, s.EventDraft("Half", "2026-04-01", goal_intent="PERFORMANCE", target_speed_mps="3.50"), expected_version=1)
    assert event.draft.target_speed_mps == "3.5" and event.distance_metres == 21098
    before = snapshot(db)
    with pytest.raises(s.SeasonError, match="Another active"):
        s.save_event(db, season.season_id, s.EventDraft("Duplicate", "2026-04-01"), expected_version=2)
    assert snapshot(db) == before
    event = s.save_event(db, season.season_id, replace(event.draft, status="completed"), expected_version=2, event_id=event.event_id)
    with pytest.raises(s.SeasonError, match="Completed"):
        s.save_event(db, season.season_id, replace(event.draft, event_date="2026-04-02"), expected_version=3, event_id=event.event_id)
    with pytest.raises(s.SeasonError, match="Completed"):
        s.remove_event(db, season.season_id, event.event_id, expected_version=3)
    future = s.create_season(db, draft(name="Future", start_date="2099-01-01", end_date="2099-12-31"))
    with pytest.raises(s.SeasonError, match="future"):
        s.save_event(db, future.season_id, s.EventDraft("Future", "2099-05-01", status="completed"), expected_version=1)


def test_concurrent_edit_only_one_version_wins(tmp_path):
    db = _database(tmp_path)
    season = s.create_season(db, draft())
    def edit(name):
        try:
            s.update_season(db, season.season_id, draft(name=name), expected_version=1)
            return "saved"
        except s.StaleSeasonError:
            return "stale"
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(edit, ["First", "Second"])) == ["saved", "stale"]
    assert s.get_season(db, season.season_id).input_version == 2


def test_dates_and_archiving_retain_events_and_block_edits(tmp_path):
    db = _database(tmp_path)
    season = s.create_season(db, draft())
    s.save_event(db, season.season_id, s.EventDraft("Half", "2026-05-01", status="cancelled"), expected_version=1)
    with pytest.raises(s.SeasonError, match="include all"):
        s.update_season(db, season.season_id, draft(start_date="2026-06-01"), expected_version=2)
    archived = s.set_archived(db, season.season_id, expected_version=2, archived=True)
    assert archived.status == "archived"
    with pytest.raises(s.SeasonError, match="Restore"):
        s.save_event(db, season.season_id, s.EventDraft("Other", "2026-07-01"), expected_version=3)
    s.create_season(db, draft(name="Replacement"))
    with pytest.raises(s.SeasonError, match="overlap"):
        s.set_archived(db, season.season_id, expected_version=3, archived=False)


def test_link_plan_retains_revision_projection_and_completion(tmp_path):
    db = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    _approve(db, candidate)
    conn = connect_sqlite(db)
    conn.execute("INSERT INTO activity(activity_id,activity_type) VALUES(7,'running')")
    create_match(conn, revision_id=candidate.revision_id, workout_id="workout-1", activity_id=7,
                 status="CONFIRMED", source="MANUAL", confidence="HIGH", reviewer="reviewer", reason="reviewed")
    conn.commit()
    active = get_active_plan_sha256(db)
    season = s.create_season(db, draft())
    linked_season = s.link_plan(db, season.season_id, candidate.plan_id, expected_version=1,
                               expected_revision_id=candidate.revision_id, expected_content_hash=candidate.content_hash)
    assert get_active_plan_sha256(db) == active
    assert load_confirmed_match(conn, revision_id=candidate.revision_id, workout_id="workout-1")
    assert linked_season.current_revision_id == candidate.revision_id
    assert not s.linkable_plans(db)
    with pytest.raises(s.SeasonError, match="methodology"):
        s.update_season(db, season.season_id, draft(inputs=replace(s.SeasonInputs(), training_method="maffetone")), expected_version=2)
    conn.close()


@pytest.mark.parametrize("column,value", [("workout_name", "Changed"), ("planned_distance_m", 999),
                                         ("planned_duration_s", 999), ("planned_tss", 99)])
def test_link_refuses_stale_plan_unmanaged_overlap_and_projection_drift(tmp_path, column, value):
    db = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    _approve(db, candidate)
    season = s.create_season(db, draft())
    def link():
        return s.link_plan(db, season.season_id, candidate.plan_id, expected_version=1,
                           expected_revision_id=candidate.revision_id, expected_content_hash=candidate.content_hash)
    before = snapshot(db)
    with pytest.raises(s.StaleSeasonError):
        s.link_plan(db, season.season_id, candidate.plan_id, expected_version=1,
                    expected_revision_id="old", expected_content_hash=candidate.content_hash)
    assert snapshot(db) == before
    conn = connect_sqlite(db)
    conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name) VALUES('2026-06-01','Unmanaged')")
    conn.commit()
    with pytest.raises(s.SeasonError, match="unmanaged"):
        link()
    conn.execute("DELETE FROM planned_workout WHERE source_plan_id IS NULL")
    conn.execute(f"UPDATE planned_workout SET {column}=? WHERE source_plan_id IS NOT NULL", (value,))
    conn.commit()
    with pytest.raises(s.SeasonError, match="projection"):
        link()
    assert s.get_season(db, season.season_id).plan_id is None
    conn.close()


def test_link_converted_legacy_source_is_retained_and_hidden(tmp_path):
    db = legacy_database(tmp_path)
    result = _convert(db, resolve_legacy_plan(db))
    choice = s.linkable_plans(db)[0]
    assert choice["origin"] == "LEGACY_CONVERSION"
    before = get_active_plan_sha256(db)
    season = s.create_season(db, draft())
    s.link_plan(db, season.season_id, result["target_plan_id"], expected_version=1,
                expected_revision_id=choice["current_revision_id"], expected_content_hash=choice["content_sha256"])
    assert get_active_plan_sha256(db) == before
    conn = connect_sqlite(db)
    assert conn.execute("SELECT COUNT(*) FROM planned_workout").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM active_planned_workout").fetchone()[0] == 1
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()


@pytest.mark.parametrize("writer", ["generated", "imported", "legacy_export", "delete_helper", "insert_helper", "revision", "new_revision"])
def test_every_old_writer_rejects_linked_season_atomically(tmp_path, writer):
    db = _database(tmp_path)
    season, candidate = linked(db)
    before = snapshot(db)
    imported = _make_imported_plan(get_active_plan_sha256(db))
    conn = connect_sqlite(db)
    try:
        with pytest.raises(s.SeasonOwnershipError):
            if writer == "generated":
                save_generated_plan(db, imported.inputs, imported.analysis, imported.day_plans, imported.weekly_rows)
            elif writer == "legacy_export":
                from garmin_data_hub.exports.forever.build_daily_plan import save_generated_plan as legacy_save
                legacy_save(db, imported.inputs, imported.analysis, imported.day_plans, imported.weekly_rows)
            elif writer == "imported":
                save_imported_plan(db, imported)
            elif writer == "delete_helper":
                delete_planned_workouts_in_range(conn, "2026-01-01", "2026-12-31")
            elif writer == "insert_helper":
                insert_planned_workout(conn, "2026-03-01", "New", "", None, None, None)
            elif writer == "revision":
                next_candidate = replace(candidate, revision_id="new", parent_revision_id=candidate.revision_id)
                approve_revision(db, next_candidate, expected_content_hash=next_candidate.content_hash,
                                 expected_parent_content_hash=candidate.content_hash, approved_by="reviewer")
            else:
                next_candidate = replace(candidate, plan_id="unowned-plan", revision_id="new", parent_revision_id=None)
                approve_revision(db, next_candidate, expected_content_hash=next_candidate.content_hash, approved_by="reviewer")
        assert not conn.in_transaction
        assert snapshot(db) == before
    finally:
        conn.close()


def test_linked_event_removal_and_archive_do_not_release_ownership(tmp_path):
    db = _database(tmp_path)
    season, _ = linked(db)
    event = s.save_event(db, season.season_id, s.EventDraft("Half", "2026-05-01"), expected_version=2)
    s.remove_event(db, season.season_id, event.event_id, expected_version=3)
    assert s.list_events(db, season.season_id)[0].draft.status == "cancelled"
    s.set_archived(db, season.season_id, expected_version=4, archived=True)
    conn = connect_sqlite(db)
    with pytest.raises(s.SeasonOwnershipError):
        delete_planned_workouts_in_range(conn, "2026-02-01", "2026-02-01")
    insert_planned_workout(conn, "2027-02-01", "Outside", "", None, 1800, None)
    assert conn.execute("SELECT COUNT(*) FROM planned_workout WHERE scheduled_date='2027-02-01'").fetchone()[0] == 1
    conn.close()


def test_unlinked_intent_does_not_block_single_event_plan(tmp_path):
    db = _database(tmp_path)
    s.create_season(db, draft())
    imported = _make_imported_plan(get_active_plan_sha256(db))
    assert save_imported_plan(db, imported).applied
    assert s.list_seasons(db)[0].plan_id is None
