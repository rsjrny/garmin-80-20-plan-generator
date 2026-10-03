"""Y4 calendar refinement, persisted tuning and protection acceptance tests."""
from dataclasses import asdict, replace
from datetime import date
import json
import sqlite3

import pytest

from garmin_data_hub.db import migrate
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.canonical import canonical_value
from garmin_data_hub.plan_methodology.revision_repository import load_revision
from garmin_data_hub.services import season_plans as intent, season_generation as service
from garmin_data_hub.services.season_schedule import (
    GenerationSettings, event_windows, _timeline, event_resolution_notes, GENERATOR_VERSION,
)
from garmin_data_hub.services.season_regeneration import affected_range, RegenerationRequest
from test_season_generation import setup_season, SETTINGS, TODAY
from test_season_regeneration import active, apply, lock
from test_season_plans import snapshot


@pytest.fixture(autouse=True)
def local_clock(monkeypatch):
    monkeypatch.setattr(service, "_local_today", lambda tz: TODAY)


def test_supporting_taper_yields_to_a_build_but_not_recovery(tmp_path):
    db, season = setup_season(tmp_path, [intent.EventDraft("Primary", "2027-10-17", "MAR"),
        intent.EventDraft("Supporting", "2027-09-05", "5K", "B")])
    events = intent.list_events(db, season.season_id)
    supporting, primary = events
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert p.can_apply, p.schedule.errors
    control = {c.day.isoformat(): c for c in p.schedule.controls}
    assert control["2027-09-03"].event_id == primary.event_id
    assert control["2027-09-03"].phase == "EVENT_SPECIFIC"
    assert set(control["2027-09-03"].influencing_event_ids) == {primary.event_id, supporting.event_id}
    assert control["2027-09-05"].phase == "EVENT"
    assert control["2027-09-06"].phase == "RECOVERY"
    assert control["2027-09-07"].phase == "RECOVERY"
    assert control["2027-09-08"].event_id == primary.event_id
    assert "SUPPORTING_TAPER_SUPPRESSED" in {i.code for i in p.schedule.warnings}
    assert "A_PREPARATION_CONSTRAINED" in {i.code for i in p.schedule.warnings}
    constrained_issue = next(i for i in p.schedule.warnings if i.code == "A_PREPARATION_CONSTRAINED")
    assert "Recovery and other controlling event phases" in event_resolution_notes(constrained_issue, p.schedule.windows)[0]
    # Calendar arbitration must be independent of incoming event order.
    windows = event_windows(season, tuple(reversed(events)))
    assert _timeline(season, windows)[0] == p.schedule.controls


def test_standalone_b_keeps_taper_and_recovery(tmp_path):
    db, season = setup_season(tmp_path, [intent.EventDraft("Supporting", "2027-09-05", "5K", "B")])
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert next(c for c in p.schedule.controls if c.day == date(2027,9,3)).phase == "TAPER"
    assert "SUPPORTING_TAPER_SUPPRESSED" not in {i.code for i in p.schedule.issues}


@pytest.mark.parametrize("method", ["eighty_twenty", "maffetone"])
def test_known_easy_completion_in_a_taper_is_reviewable_and_audited(tmp_path, method):
    db, season = setup_season(tmp_path, [intent.EventDraft("Primary", "2027-10-17", "MAR"),
        intent.EventDraft("Easy participation", "2027-10-10", "5K", "C", participation_seconds=600)],
        inputs=intent.SeasonInputs(training_method=method, starting_duration_seconds=10800, lthr=170))
    settings = SETTINGS if method == "eighty_twenty" else GenerationSettings(maf_adjustment=0, maf_confirmed=True)
    before = snapshot(db)
    p = service.preview_season(db, season.season_id, settings)
    assert snapshot(db) == before
    assert p.can_apply, p.schedule.errors
    assert "EASY_EVENT_IN_A_TAPER" in {i.code for i in p.schedule.warnings}
    event = next(w for w in p.schedule.candidate.workouts if w.title == "Easy participation")
    assert event.segments[0].duration_seconds == 600
    assert event.metadata["participation_seconds"] == 600
    assert event.metadata["target_seconds"] is None
    assert "ZONE_2" in event.segments[0].prescription.native_target.value if method == "eighty_twenty" else "MAF" in event.segments[0].prescription.native_target.value
    apply(db, p)
    assert apply(db, p).status == "duplicate"
    audit = json.loads(service.list_applications(db, season.season_id)[0]["review_json"])
    assert audit["event_assessments"] == canonical_value(p.schedule.candidate.constraints["event_assessments"])
    assert len(audit["event_assessments"]) == 2
    stored = intent.list_events(db, season.season_id)[0]
    assert stored.draft.participation_seconds == 600
    # A locked easy event remains eligible with its exact origin on regeneration.
    current = intent.get_season(db, season.season_id)
    lock(db, current, event, locked=True)
    q = service.preview_season(db, season.season_id, settings)
    assert q.can_apply, q.schedule.errors
    assert event.workout_id in q.schedule.candidate.change_summary["preserved_workout_ids"]
    assert "EASY_EVENT_IN_A_TAPER" in {i.code for i in q.schedule.warnings}


@pytest.mark.parametrize("changes", [
    {"participation_seconds": None}, {"participation_seconds": 604800},
    {"distance_code": "10K"},
    {"goal_intent": "PERFORMANCE", "participation_seconds": None, "target_seconds": 600},
])
def test_unsafe_taper_participation_blocks_without_mutation(tmp_path, changes):
    draft = replace(intent.EventDraft("Supporting", "2027-10-10", "5K", "C", participation_seconds=600), **changes)
    db, season = setup_season(tmp_path, [intent.EventDraft("Primary", "2027-10-17", "MAR"), draft])
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert "EVENT_EXCEEDS_A_TAPER" in {i.code for i in p.schedule.errors}
    before = snapshot(db)
    with pytest.raises(intent.SeasonError):
        apply(db, p)
    assert snapshot(db) == before


def test_fixed_performance_event_cannot_bypass_new_a_taper(tmp_path):
    db, season, parent = active(tmp_path, [intent.EventDraft("Protected race", "2027-10-10", "5K", "C",
        goal_intent="PERFORMANCE", target_seconds=600)])
    event = next(w for w in parent.workouts if w.event_flag)
    lock(db, season, event, locked=True)
    intent.save_event(db, season.season_id, intent.EventDraft("New primary", "2027-10-17", "MAR"), expected_version=season.input_version)
    before = snapshot(db)
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert "EVENT_EXCEEDS_A_TAPER" in {i.code for i in p.schedule.errors}
    assert event.workout_id in p.schedule.candidate.change_summary["preserved_workout_ids"]
    assert snapshot(db) == before
    with pytest.raises(intent.SeasonError):
        apply(db, p)
    assert snapshot(db) == before


@pytest.mark.parametrize("value", [0, -1, 604801, True, 1.5, "600"])
def test_invalid_participation_values_do_not_touch_intent(tmp_path, value):
    db, season = setup_season(tmp_path)
    before = snapshot(db)
    with pytest.raises(intent.SeasonError, match="participation"):
        intent.save_event(db, season.season_id, intent.EventDraft("Easy", "2027-10-10", participation_seconds=value), expected_version=season.input_version)
    assert snapshot(db) == before


def test_performance_estimate_and_completion_goal_remain_distinct(tmp_path):
    db, season = setup_season(tmp_path)
    with pytest.raises(intent.SeasonError, match="completion goals"):
        intent.save_event(db, season.season_id, intent.EventDraft("Race", "2027-10-10", goal_intent="PERFORMANCE", participation_seconds=600), expected_version=season.input_version)
    with pytest.raises(intent.SeasonError, match="performance goal"):
        intent.save_event(db, season.season_id, intent.EventDraft("Easy", "2027-10-10", target_seconds=600), expected_version=season.input_version)
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert all(w.event_duration_seconds == 0 for w in p.schedule.weeks)


def test_tuning_invalidates_old_preview_and_enters_affected_diff(tmp_path):
    db, season, parent = active(tmp_path, [intent.EventDraft("Easy", "2027-09-05", "5K", "C", participation_seconds=900)])
    old = service.preview_season(db, season.season_id, SETTINGS)
    event = intent.list_events(db, season.season_id)[0]
    intent.save_event(db, season.season_id, replace(event.draft, participation_seconds=1200), event_id=event.event_id, expected_version=season.input_version)
    before = snapshot(db)
    with pytest.raises(service.StalePreviewError):
        apply(db, old)
    assert snapshot(db) == before
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert p.can_apply, p.schedule.errors
    delta = p.schedule.candidate.change_summary["event_delta"]["changed"][0]
    assert delta["before"]["draft"]["participation_seconds"] == 900
    assert delta["after"]["draft"]["participation_seconds"] == 1200
    assert p.schedule.candidate.change_summary["preserved_workout_ids"]
    apply(db, p)
    assert load_revision(db, parent.revision_id).content_hash == parent.content_hash


def test_peak_assessments_explain_shared_and_missing_days_without_readiness_claim(tmp_path):
    db, season = setup_season(tmp_path, [intent.EventDraft("First", "2027-05-16"), intent.EventDraft("Next", "2027-07-18")])
    p = service.preview_season(db, season.season_id, SETTINGS)
    reports = p.schedule.candidate.constraints["event_assessments"]
    report = next(r for r in reports if r["name"] == "Next")
    assert report["controlled_preparation_days"] + report["constrained_preparation_days"] == report["preparation_days_in_season"]
    assert report["shared_preparation_days"] > 0
    assert report["constrained_preparation_days"] > 0
    assert report["constraining_event_ids"]
    assert "readiness" not in report
    assert "SHARED_A_BUILD" in {i.code for i in p.schedule.warnings}


def test_conflict_guidance_keeps_recovery_and_calendar_limits_explicit(tmp_path):
    db, season = setup_season(tmp_path, [intent.EventDraft("First", "2027-10-17", "MAR"), intent.EventDraft("Next", "2027-10-24", "MAR")])
    p = service.preview_season(db, season.season_id, SETTINGS)
    recovery = next(i for i in p.schedule.errors if i.code == "EVENT_IN_RECOVERY")
    overlap = next(i for i in p.schedule.errors if i.code == "A_PEAKS_OVERLAP")
    assert "2027-11-01" in event_resolution_notes(recovery, p.schedule.windows)[0]
    assert "2027-11-15" in event_resolution_notes(overlap, p.schedule.windows)[0]
    assert "does not shorten" in event_resolution_notes(recovery, p.schedule.windows)[1]
    assert not p.can_apply


def test_old_policy_requires_full_future_review_without_rewriting_parent(tmp_path):
    db, season, parent = active(tmp_path, [intent.EventDraft("Easy", "2027-09-05", "5K", "C")])
    prior = replace(parent, provenance=dict(parent.provenance, generator="season-generator.v1", policy="season-windows-load.v1"))
    selected, recommended, reasons, _ = affected_range(season, intent.list_events(db, season.season_id), prior, TODAY, RegenerationRequest(), SETTINGS)
    assert selected == recommended == (date(2027,1,1), date(2027,12,31))
    assert any("policy changed" in r for r in reasons)
    assert parent.provenance["generator"] == GENERATOR_VERSION


def v14_snapshot(conn):
    conn.execute("ALTER TABLE season_event DROP COLUMN participation_seconds")
    conn.execute("DELETE FROM schema_migrations WHERE version=15")
    conn.commit()


def test_v15_upgrade_retains_events_revisions_audits_and_replays(tmp_path):
    db, season, parent = active(tmp_path, [intent.EventDraft("Existing", "2027-05-16")])
    conn = connect_sqlite(db)
    v14_snapshot(conn)
    old_event = dict(conn.execute("SELECT * FROM season_event").fetchone())
    audits = [tuple(r) for r in conn.execute("SELECT * FROM season_revision_application")]
    migrate.apply_schema(conn, schema_sql_path())
    migrate.apply_schema(conn, schema_sql_path())
    event = dict(conn.execute("SELECT * FROM season_event").fetchone())
    assert event.pop("participation_seconds") is None
    assert event == old_event
    assert audits == [tuple(r) for r in conn.execute("SELECT * FROM season_revision_application")]
    assert load_revision(db, parent.revision_id).content_hash == parent.content_hash
    assert migrate.get_current_schema_version(conn) == 15
    assert not conn.execute("PRAGMA foreign_key_check").fetchall()
    conn.close()


@pytest.mark.parametrize("repair", [False, True])
def test_v15_failure_rolls_back_column_and_version(tmp_path, monkeypatch, repair):
    db, season = setup_season(tmp_path)
    conn = connect_sqlite(db)
    v14_snapshot(conn)
    if repair:
        conn.execute("INSERT INTO schema_migrations(version,name) VALUES(15,'missing column')")
        conn.commit()
    before = snapshot(db)
    original = migrate._migration_15_add_event_participation
    def fail(c):
        original(c)
        raise sqlite3.OperationalError("injected v15")
    monkeypatch.setattr(migrate, "_migration_15_add_event_participation", fail)
    with pytest.raises(sqlite3.OperationalError, match="injected v15"):
        migrate.apply_schema(conn, schema_sql_path())
    assert snapshot(db) == before
    assert "participation_seconds" not in {r[1] for r in conn.execute("PRAGMA table_info(season_event)")}
    monkeypatch.setattr(migrate, "_migration_15_add_event_participation", original)
    migrate.apply_schema(conn, schema_sql_path())
    assert migrate.get_current_schema_version(conn) == 15
    conn.close()


@pytest.mark.parametrize("value,goal", [(0,"COMPLETION"), (600.5,"COMPLETION"), (600,"PERFORMANCE")])
def test_database_rejects_invalid_participation_even_without_service(tmp_path, value, goal):
    db, season = setup_season(tmp_path, [intent.EventDraft("Existing", "2027-05-16")])
    conn = connect_sqlite(db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE season_event SET participation_seconds=?,goal_intent=?", (value, goal))
    conn.close()


def test_cancelled_protected_race_still_obeys_other_a_taper(tmp_path):
    db, season, parent = active(tmp_path, [intent.EventDraft("Protected race", "2027-10-10", "5K", "C",
        goal_intent="PERFORMANCE", target_seconds=600)])
    workout = next(w for w in parent.workouts if w.event_flag)
    lock(db, season, workout, locked=True)
    event = intent.list_events(db, season.season_id)[0]
    intent.remove_event(db, season.season_id, event.event_id, expected_version=season.input_version)
    current = intent.get_season(db, season.season_id)
    intent.save_event(db, season.season_id, intent.EventDraft("New primary", "2027-10-17", "MAR"), expected_version=current.input_version)
    before = snapshot(db)
    p = service.preview_season(db, season.season_id, SETTINGS)
    assert workout.workout_id in p.schedule.candidate.change_summary["preserved_workout_ids"]
    assert "PRESERVED_EVENT_INTENT" in {i.code for i in p.schedule.warnings}
    assert "EVENT_EXCEEDS_A_TAPER" in {i.code for i in p.schedule.errors}
    assert snapshot(db) == before
