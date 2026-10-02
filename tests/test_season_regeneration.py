from dataclasses import replace
from datetime import date
import json
import sqlite3

import pytest

from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db import migrate
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.revision_repository import load_revision, fail_at, InjectedApprovalFailure, CorruptRevisionError
from garmin_data_hub.plan_methodology.activity_workout_match import create_match, load_confirmed_match
from garmin_data_hub.plan_methodology.workout_origin import prescribed_content
from garmin_data_hub.services import season_generation as service, season_plans as intent
from garmin_data_hub.services.season_regeneration import RegenerationRequest
from garmin_data_hub.services.season_workouts import set_workout_protection
from test_season_generation import setup_season, SETTINGS, TODAY
from test_season_plans import snapshot


@pytest.fixture(autouse=True)
def local_clock(monkeypatch):
    monkeypatch.setattr(service,"_local_today",lambda tz:TODAY)


def active(tmp_path, events=()):
    db,s = setup_season(tmp_path,events)
    p = service.preview_season(db,s.season_id,SETTINGS)
    service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True)
    return db,intent.get_season(db,s.season_id),p.schedule.candidate


def apply(db,p):
    return service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True)


def lock(db,s,w,**kw):
    set_workout_protection(db,s.season_id,w.workout_id,expected_version=0,editor="r",reason="preserve",**kw)


def test_event_move_merges_full_revision_with_exact_preserved_origins(tmp_path):
    db,s,parent = active(tmp_path,[intent.EventDraft("Spring","2027-05-16")])
    e = intent.list_events(db,s.season_id)[0]
    intent.save_event(db,s.season_id,replace(e.draft,event_date="2027-06-06"),event_id=e.event_id,expected_version=s.input_version)
    before = snapshot(db)
    p = service.preview_season(db,s.season_id,SETTINGS)
    assert p.can_apply,p.schedule.errors
    assert snapshot(db)==before
    diff = p.schedule.candidate.change_summary
    assert diff["mode"]=="AFFECTED" and diff["event_delta"]["changed"]
    assert diff["range_start"]<="2027-02-01" and diff["range_end"]>"2027-06-13"
    assert diff["preserved_workout_ids"] and diff["changed"]
    apply(db,p)
    result = load_revision(db,p.schedule.candidate.revision_id).candidate
    old = {w.workout_id:w for w in parent.workouts}
    for w in result.workouts:
        if w.workout_id in old:
            assert prescribed_content(w)==prescribed_content(old[w.workout_id])
    assert [w.scheduled_date.isoformat() for w in result.workouts if w.event_flag]==["2027-06-06"]
    assert apply(db,p).status=="duplicate"
    audit = json.loads(service.list_applications(db,s.season_id)[-1]["review_json"])
    assert audit["previous_revision_sha256"]==parent.content_hash
    assert audit["preserved"] and audit["diff"]["changed"]


def test_history_completed_candidate_and_locks_cannot_be_silently_replaced(tmp_path,monkeypatch):
    db,s,parent = active(tmp_path)
    monkeypatch.setattr(service,"_local_today",lambda tz:date(2027,2,10))
    future = next(w for w in parent.workouts if w.scheduled_date==date(2027,2,12))
    lock(db,s,future,locked=True)
    done = next(w for w in parent.workouts if w.scheduled_date==date(2027,2,13))
    lock(db,s,done,explicitly_completed=True)
    provisional = next(w for w in parent.workouts if w.scheduled_date==date(2027,2,14))
    conn=connect_sqlite(db)
    conn.execute("INSERT INTO activity(activity_id) VALUES(999)")
    create_match(conn,revision_id=parent.revision_id,workout_id=provisional.workout_id,activity_id=999,status="CANDIDATE",source="MANUAL",confidence="LOW")
    conn.commit();conn.close()
    p=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("FROM_TODAY"))
    ids=set(p.schedule.candidate.change_summary["preserved_workout_ids"])
    assert {future.workout_id,done.workout_id,provisional.workout_id}<=ids
    assert all(w.workout_id in ids for w in parent.workouts if w.scheduled_date<date(2027,2,10))
    for w in (done,provisional,parent.workouts[0]):
        with pytest.raises(intent.SeasonError,match="Override"):
            service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("FROM_TODAY",override_workout_ids=(w.workout_id,)))
    q=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("FROM_TODAY",override_workout_ids=(future.workout_id,)))
    assert q.schedule.candidate.change_summary["overrides"]==(future.workout_id,)


def test_custom_range_rejects_missing_moved_event_and_history(tmp_path):
    db,s,parent=active(tmp_path,[intent.EventDraft("Spring","2027-05-16")])
    e=intent.list_events(db,s.season_id)[0]
    intent.save_event(db,s.season_id,replace(e.draft,event_date="2027-06-06"),event_id=e.event_id,expected_version=s.input_version)
    p=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("CUSTOM","2027-05-01","2027-05-10"))
    assert not p.can_apply
    assert "EVENT_NOT_REPRESENTED" in {i.code for i in p.schedule.errors}
    with pytest.raises(intent.SeasonError):
        service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("CUSTOM","2026-10-01","2027-05-10"))


@pytest.mark.parametrize("stage",["after_recheck","after_ownership","after_revision_insert","after_workout_insert","after_segment_insert","during_projection","before_active_pointer_update","after_origins","after_state","after_nutrition","after_audit","before_commit"])
def test_regeneration_failure_rolls_back_every_component(tmp_path,stage):
    db,s,parent=active(tmp_path)
    p=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("CUSTOM","2027-03-01","2027-03-31"))
    before=snapshot(db)
    with pytest.raises(InjectedApprovalFailure):
        service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True,failure_hook=fail_at(stage))
    assert snapshot(db)==before


def test_lock_or_completion_after_preview_invalidates_apply(tmp_path):
    db,s,parent=active(tmp_path)
    p=service.preview_season(db,s.season_id,SETTINGS)
    lock(db,s,parent.workouts[5],locked=True)
    before=snapshot(db)
    with pytest.raises(service.StalePreviewError):
        apply(db,p)
    assert snapshot(db)==before


def test_three_revisions_flatten_origins_and_route_new_matches(tmp_path):
    db,s,parent=active(tmp_path)
    p=service.preview_season(db,s.season_id,SETTINGS)
    apply(db,p)
    q=service.preview_season(db,s.season_id,SETTINGS)
    apply(db,q)
    latest=load_revision(db,q.schedule.candidate.revision_id).candidate
    assert all(o["origin_revision_id"]==parent.revision_id for o in latest.change_summary["origins"])
    conn=connect_sqlite(db)
    conn.execute("INSERT INTO activity(activity_id) VALUES(998)")
    match=create_match(conn,revision_id=latest.revision_id,workout_id=latest.workouts[1].workout_id,activity_id=998,status="CONFIRMED",source="MANUAL",confidence="HIGH",reviewer="r",reason="reviewed")
    assert match.revision_id==parent.revision_id
    assert load_confirmed_match(conn,revision_id=latest.revision_id,workout_id=latest.workouts[1].workout_id)==match
    conn.commit();conn.close()
    fresh=service.preview_season(db,s.season_id,SETTINGS)
    assert "completed" in next(d["reasons"] for d in fresh.schedule.candidate.change_summary["preserved"] if d["workout_id"]==match.workout_id)


def test_origin_table_tampering_is_detected_on_load(tmp_path):
    db,s,parent=active(tmp_path)
    p=service.preview_season(db,s.season_id,SETTINGS)
    apply(db,p)
    conn=connect_sqlite(db)
    with pytest.raises(sqlite3.IntegrityError,match="immutable"):
        conn.execute("DELETE FROM plan_revision_workout_origin")
    conn.execute("DROP TRIGGER trg_workout_origin_no_delete")
    conn.execute("DELETE FROM plan_revision_workout_origin WHERE workout_id=?",(parent.workouts[0].workout_id,))
    conn.commit();conn.close()
    with pytest.raises(CorruptRevisionError,match="origins"):
        load_revision(db,p.schedule.candidate.revision_id)


def test_v14_upgrade_replays_retains_v13_audits_and_rolls_back(tmp_path,monkeypatch):
    db,s,parent=active(tmp_path)
    conn=connect_sqlite(db)
    # Recreate the old CHECK without changing any existing row or frozen document.
    conn.execute("DROP TABLE season_workout_state")
    conn.execute("DROP TABLE plan_revision_workout_origin")
    for name in ("trg_season_application_owner","trg_season_application_no_update","trg_season_application_no_delete"):
        conn.execute("DROP TRIGGER "+name)
    ddl=conn.execute("SELECT sql FROM sqlite_master WHERE name='season_revision_application'").fetchone()[0]
    ddl=ddl.replace("season_revision_application", "audit_old",1).replace("mode IN ('INITIAL_FULL','FROM_TODAY','AFFECTED','CUSTOM','MANUAL_EDIT')","mode = 'INITIAL_FULL'")
    conn.execute(ddl)
    conn.execute("INSERT INTO audit_old SELECT * FROM season_revision_application")
    conn.execute("DROP TABLE season_revision_application")
    conn.execute("ALTER TABLE audit_old RENAME TO season_revision_application")
    conn.execute("DELETE FROM schema_migrations WHERE version>=14")
    conn.commit()
    before=snapshot(db)
    original=migrate._migration_14_add_season_regeneration
    def fail(c,s):
        original(c,s)
        raise sqlite3.OperationalError("injected v14")
    monkeypatch.setattr(migrate,"_migration_14_add_season_regeneration",fail)
    with pytest.raises(sqlite3.OperationalError,match="injected"):
        migrate.apply_schema(conn,schema_sql_path())
    assert snapshot(db)==before
    monkeypatch.setattr(migrate,"_migration_14_add_season_regeneration",original)
    migrate.apply_schema(conn,schema_sql_path())
    migrate.apply_schema(conn,schema_sql_path())
    assert load_revision(db,parent.revision_id).content_hash==parent.content_hash
    assert len(service.list_applications(db,s.season_id))==1
    assert conn.execute("PRAGMA foreign_key_check").fetchall()==[]
    conn.close()


def test_manual_prescription_revision_marks_new_occurrence_and_carries_every_other_workout(tmp_path):
    db,s,parent=active(tmp_path)
    old=next(w for w in parent.workouts if w.sport.value=="RUNNING" and not w.event_flag)
    edited=replace(old,workout_id="manual-new",title="My easy run",segments=tuple(replace(seg,duration_seconds=max(1,seg.duration_seconds//2)) for seg in old.segments))
    p=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("MANUAL_EDIT",manual_workout=edited,replaces_workout_id=old.workout_id))
    assert p.can_apply,p.schedule.errors
    apply(db,p)
    conn=connect_sqlite(db)
    state=dict(conn.execute("SELECT * FROM season_workout_state WHERE workout_id='manual-new'").fetchone())
    assert state["manually_edited"]==1 and state["version"]==1
    conn.close()
    loaded=load_revision(db,p.schedule.candidate.revision_id).candidate
    assert len(loaded.workouts)==len(parent.workouts)
    assert old.workout_id not in {w.workout_id for w in loaded.workouts}
    follow=service.preview_season(db,s.season_id,SETTINGS)
    assert "manual-new" in follow.schedule.candidate.change_summary["preserved_workout_ids"]
    assert "manually_edited" in next(d["reasons"] for d in follow.schedule.candidate.change_summary["preserved"] if d["workout_id"]=="manual-new")


def test_manual_state_failure_rolls_back_new_prescription_and_protection(tmp_path):
    db,s,parent=active(tmp_path)
    old=next(w for w in parent.workouts if w.sport.value=="RUNNING")
    edited=replace(old,workout_id="manual-failure",title="Reviewed run")
    p=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest("MANUAL_EDIT",manual_workout=edited,replaces_workout_id=old.workout_id))
    before=snapshot(db)
    with pytest.raises(InjectedApprovalFailure):
        service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True,failure_hook=fail_at("after_state"))
    assert snapshot(db)==before


def test_parameter_refresh_preserves_original_targets_and_requires_confirmation(tmp_path):
    db,s,parent=active(tmp_path)
    intent.update_season(db,s.season_id,intent.SeasonDraft(s.name,s.start_date,s.end_date,s.timezone,replace(s.inputs,lthr=175)),expected_version=s.input_version)
    with pytest.raises(intent.SeasonError,match="confirmation"):
        service.preview_season(db,s.season_id,service.GenerationSettings(),RegenerationRequest(refresh_parameters=True))
    # Preserve a single locked run while the new generated runs use current parameters.
    run=next(w for w in parent.workouts if w.sport.value=="RUNNING")
    lock(db,s,run,locked=True)
    p=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest(refresh_parameters=True))
    assert p.can_apply,p.schedule.errors
    assert p.schedule.candidate.parameter_snapshot!=parent.parameter_snapshot
    assert prescribed_content(next(w for w in p.schedule.candidate.workouts if w.workout_id==run.workout_id))==prescribed_content(run)
    apply(db,p)
    assert load_revision(db,p.schedule.candidate.revision_id).content_hash==p.schedule.candidate.content_hash


def test_cancelled_locked_event_is_retained_until_specific_future_override(tmp_path):
    db,s,parent=active(tmp_path,[intent.EventDraft("Spring","2027-05-16")])
    race=next(w for w in parent.workouts if w.event_flag)
    lock(db,s,race,locked=True)
    e=intent.list_events(db,s.season_id)[0]
    intent.save_event(db,s.season_id,replace(e.draft,status="cancelled"),event_id=e.event_id,expected_version=s.input_version)
    p=service.preview_season(db,s.season_id,SETTINGS)
    assert p.can_apply,p.schedule.errors
    assert race.workout_id in p.schedule.candidate.change_summary["preserved_workout_ids"]
    assert "PRESERVED_EVENT_INTENT" in {i.code for i in p.schedule.warnings}
    q=service.preview_season(db,s.season_id,SETTINGS,RegenerationRequest(override_workout_ids=(race.workout_id,)))
    assert q.can_apply,q.schedule.errors
    assert race.workout_id in q.schedule.candidate.change_summary["removed_workout_ids"]
    assert not any(w.event_flag for w in q.schedule.candidate.workouts)
    apply(db,q)
    assert load_revision(db,q.schedule.candidate.revision_id).content_hash==q.schedule.candidate.content_hash


def test_locked_hard_workout_in_new_taper_blocks_without_widening_range(tmp_path):
    db,s,parent=active(tmp_path,[intent.EventDraft("Spring","2027-05-16")])
    quality=next(w for w in parent.workouts if w.quality_flag and w.scheduled_date.month==4)
    lock(db,s,quality,locked=True)
    e=intent.list_events(db,s.season_id)[0]
    intent.save_event(db,s.season_id,replace(e.draft,event_date=(quality.scheduled_date+__import__('datetime').timedelta(days=7)).isoformat()),event_id=e.event_id,expected_version=s.input_version)
    p=service.preview_season(db,s.season_id,SETTINGS)
    assert not p.can_apply
    assert "PROTECTED_PHASE_CONFLICT" in {i.code for i in p.schedule.errors}
    before=snapshot(db)
    with pytest.raises(intent.SeasonError):
        apply(db,p)
    assert snapshot(db)==before


def test_two_regeneration_previews_and_concurrent_retry(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    db,s,parent=active(tmp_path)
    p=service.preview_season(db,s.season_id,SETTINGS)
    q=service.preview_season(db,s.season_id,SETTINGS)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:apply(db,p).status,range(2)))
    assert sorted(results)==["applied","duplicate"]
    with pytest.raises(service.StalePreviewError):
        apply(db,q)


def test_neighbor_window_expansion_and_repricing_history_require_full_future_evaluation(tmp_path):
    from garmin_data_hub.services.season_regeneration import affected_range
    from garmin_data_hub.services.season_schedule import CompletedWeek
    db,s,parent=active(tmp_path,[intent.EventDraft("Spring","2027-05-16"),intent.EventDraft("Fall","2027-10-17","MAR")])
    e=intent.list_events(db,s.season_id)[0]
    intent.save_event(db,s.season_id,replace(e.draft,event_date="2027-06-06"),event_id=e.event_id,expected_version=s.input_version)
    s=intent.get_season(db,s.season_id)
    events=intent.list_events(db,s.season_id)
    selected,recommended,reasons,_=affected_range(s,events,parent,TODAY,RegenerationRequest(),SETTINGS)
    assert recommended[1]>date(2027,10,31)
    updated=replace(SETTINGS,completed_weeks=(CompletedWeek("2026-08-31",7200,True,"explicit"),))
    selected,_,reasons,_=affected_range(s,events,parent,TODAY,RegenerationRequest(),updated)
    assert tuple(d.isoformat() for d in selected)==(s.start_date,s.end_date)


def test_protection_version_is_monotonic_and_completion_cannot_be_cleared(tmp_path):
    db,s,parent=active(tmp_path)
    w=parent.workouts[0]
    lock(db,s,w,explicitly_completed=True)
    with pytest.raises(intent.StaleSeasonError):
        lock(db,s,w,locked=True)
    with pytest.raises(intent.SeasonError,match="cannot be cleared"):
        set_workout_protection(db,s.season_id,w.workout_id,expected_version=1,editor="r",reason="clear",explicitly_completed=False)


def test_calendar_displays_locks_manual_completion_and_preservation(tmp_path):
    from garmin_data_hub.ui_nicegui.data import plan_rows
    db,s,parent=active(tmp_path)
    w=parent.workouts[0]
    lock(db,s,w,locked=True,explicitly_completed=True)
    p=service.preview_season(db,s.season_id,SETTINGS)
    apply(db,p)
    row=next(r for r in plan_rows(db) if r["date"]==w.scheduled_date.isoformat())
    assert row["protection"]=="Locked · Completed · Preserved"


@pytest.mark.parametrize("converted",[False,True])
def test_adopted_native_and_converted_history_keep_original_prescriptions_and_sources(tmp_path,converted):
    from test_plan_revision_persistence import _database, _approve, _fitzgerald_candidate
    from test_legacy_plan_conversion import _database as legacy_database, _convert, resolve_legacy_plan
    if converted:
        db=legacy_database(tmp_path)
        result=_convert(db,resolve_legacy_plan(db))
        choice=intent.linkable_plans(db)[0]
        parent=load_revision(db,choice["current_revision_id"]).candidate
    else:
        db=_database(tmp_path)
        parent=_fitzgerald_candidate()
        parent=replace(parent,workouts=(replace(parent.workouts[0],family="AEROBIC",title="Easy aerobic run",
                segments=tuple(replace(seg,purpose="AEROBIC_DEVELOPMENT") for seg in parent.workouts[0].segments)),))
        _approve(db,parent)
    season=intent.create_season(db,intent.SeasonDraft("Adopted history","2026-10-01","2026-10-31","America/New_York",intent.SeasonInputs(starting_duration_seconds=10800,lthr=170)))
    intent.link_plan(db,season.season_id,parent.plan_id,expected_version=1,expected_revision_id=parent.revision_id,expected_content_hash=parent.content_hash)
    conn=connect_sqlite(db)
    source_rows=[tuple(r) for r in conn.execute("SELECT * FROM planned_workout WHERE source_plan_id IS NULL")]
    conn.close()
    p=service.preview_season(db,season.season_id,SETTINGS,RegenerationRequest(refresh_parameters=True))
    assert p.can_apply,p.schedule.errors
    apply(db,p)
    copied=load_revision(db,p.schedule.candidate.revision_id).candidate
    assert prescribed_content(next(w for w in copied.workouts if w.workout_id==parent.workouts[0].workout_id))==prescribed_content(parent.workouts[0])
    conn=connect_sqlite(db)
    assert [tuple(r) for r in conn.execute("SELECT * FROM planned_workout WHERE source_plan_id IS NULL")]==source_rows
    assert conn.execute("SELECT COUNT(*) FROM active_planned_workout WHERE source_plan_id IS NULL").fetchone()[0]==0
    assert conn.execute("PRAGMA foreign_key_check").fetchall()==[]
    conn.close()
