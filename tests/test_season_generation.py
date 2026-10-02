from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.db import migrate
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.plan_methodology.domain import DomainError, LoadMode, SegmentKind, Sport
from garmin_data_hub.plan_methodology.segments import PlannedWorkout, WorkoutSegment
from garmin_data_hub.plan_methodology.revision_repository import approve_revision, load_revision, fail_at, InjectedApprovalFailure
from garmin_data_hub.services import season_plans as intent, season_generation as service
from garmin_data_hub.services.season_schedule import (
    CompletedWeek, GenerationSettings, generate_season_schedule, event_windows, validate_season_workload,
)
from garmin_data_hub.services.training_policy import normalize_policy_sessions
from garmin_data_hub.ui_nicegui.data import plan_rows, nutrition_rows
from test_plan_revision_persistence import _database, _approve, _fitzgerald_candidate
from test_season_plans import snapshot

TODAY = date(2026,10,2)
SETTINGS = GenerationSettings(lthr_confirmed=True)


@pytest.fixture(autouse=True)
def local_clock(monkeypatch):
    monkeypatch.setattr(service,"_local_today",lambda tz:TODAY)


def setup_season(tmp_path, events=(), **changes):
    tmp_path.mkdir(parents=True,exist_ok=True)
    db = _database(tmp_path)
    draft = intent.SeasonDraft("2027 season","2027-01-01","2027-12-31","America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800,lthr=170))
    season = intent.create_season(db,replace(draft,**changes))
    for event in events:
        intent.save_event(db,season.season_id,event,expected_version=season.input_version)
        season = intent.get_season(db,season.season_id)
    return db,season


def preview(db,season,settings=SETTINGS):
    return service.preview_season(db,season.season_id,settings)


def codes(p):
    return {i.code for i in p.schedule.issues}


def test_single_a_half_year_is_one_coherent_timeline(tmp_path):
    db,season = setup_season(tmp_path,[intent.EventDraft("Spring Half","2027-05-16")])
    before = snapshot(db)
    p = preview(db,season)
    assert snapshot(db)==before
    assert p.can_apply, p.schedule.errors
    rows = p.schedule.candidate.workouts
    assert len(rows)==365 and len({w.workout_id for w in rows})==365
    assert [w.scheduled_date.isoformat() for w in rows if w.event_flag]==["2027-05-16"]
    assert all(w.phase=="TAPER" for w in rows if "2027-05-06"<=w.scheduled_date.isoformat()<= "2027-05-15")
    assert all(w.phase=="RECOVERY" for w in rows if "2027-05-17"<=w.scheduled_date.isoformat()<= "2027-05-23")
    assert rows[-1].phase=="MAINTENANCE" and not rows[-1].event_flag
    assert "EVENT_DURATION_UNKNOWN" in codes(p)
    assert "READINESS_UNVERIFIED" in codes(p)
    assert all(w.training_seconds<=w.envelope_seconds for w in p.schedule.weeks)
    assert any(w.sport is Sport.STRENGTH for w in rows)
    assert any(w.sport is Sport.MOBILITY for w in rows)
    assert all(s.duration_seconds is None for w in rows if w.sport is Sport.REST for s in w.segments)


def test_distinct_a_events_and_c_event_replace_run_slots(tmp_path):
    events = [intent.EventDraft("Spring","2027-05-16"),
              intent.EventDraft("Fall","2027-10-17","MAR"),
              intent.EventDraft("Tune-up","2027-09-05","5K","C")]
    db,season = setup_season(tmp_path,events)
    p = preview(db,season)
    assert p.can_apply,p.schedule.errors
    assert sum(w.event_flag for w in p.schedule.candidate.workouts)==3
    assert all(w.hard_days<=2 for w in p.schedule.weeks)
    assert any(c.phase=="RECOVERY" and c.day==date(2027,9,6) for c in p.schedule.controls)
    assert not any(i.code=="DURATION_GROWTH" for i in p.schedule.errors)


@pytest.mark.parametrize("events,expected",[
    ([intent.EventDraft("First","2027-10-17","MAR"),intent.EventDraft("Second","2027-10-24","MAR")],"A_PEAKS_OVERLAP"),
    ([intent.EventDraft("A","2027-10-17","MAR"),intent.EventDraft("B","2027-10-10","HM","B")],"EVENT_EXCEEDS_A_TAPER"),
    ([intent.EventDraft("A","2027-10-17","MAR"),intent.EventDraft("C","2027-10-20","10K","C")],"EVENT_IN_RECOVERY"),
])
def test_priority_conflicts_block_apply_without_schedule_mutation(tmp_path,events,expected):
    db,season = setup_season(tmp_path,events)
    p = preview(db,season)
    assert expected in codes(p) and not p.can_apply
    before = snapshot(db)
    with pytest.raises(intent.SeasonError):
        service.apply_season_preview(db,p,approved_by="reviewer",acknowledge_warnings=True)
    assert snapshot(db)==before


def test_shared_build_warns_and_earlier_a_controls(tmp_path):
    db,season = setup_season(tmp_path,[intent.EventDraft("First","2027-05-16"),intent.EventDraft("Next","2027-07-18")])
    p=preview(db,season)
    assert "SHARED_A_BUILD" in codes(p)
    first=intent.list_events(db,season.season_id)[0]
    assert next(c for c in p.schedule.controls if c.day==date(2027,5,1)).event_id==first.event_id


@pytest.mark.parametrize("code,p,t,r",[("5K",42,7,2),("10K",56,7,3),("10M",56,7,3),
    ("HM",84,10,7),("20M",112,14,14),("MAR",112,14,14),("50K",140,14,21),
    ("50M",140,14,21),("100K",168,21,28),("100M",168,21,28)])
def test_versioned_windows_and_b_c_rules(tmp_path,code,p,t,r):
    db,season = setup_season(tmp_path,[intent.EventDraft("Event","2027-10-01",code)])
    e=intent.list_events(db,season.season_id)[0]
    w=event_windows(season,(e,))[0]
    assert (w.preparation_days,w.taper_days,w.recovery_days)==(p,t,r)
    b=replace(e,draft=replace(e.draft,priority="B"))
    wb=event_windows(season,(b,))[0]
    assert (wb.preparation_days,wb.taper_days,wb.recovery_days)==((p+1)//2,min(t,3),r)
    c=replace(e,draft=replace(e.draft,priority="C",taper_days=28))
    wc=event_windows(season,(c,))[0]
    assert (wc.preparation_days,wc.taper_days,wc.recovery_days)==(0,0,r)


def test_year_boundary_leap_and_short_preparation(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("January","2027-01-03","MAR")],
        start_date="2026-10-02",end_date="2027-10-01")
    p=preview(db,season)
    assert p.can_apply and "INSUFFICIENT_PREPARATION" in codes(p)
    assert next(c for c in p.schedule.controls if c.day==date(2026,12,31)).phase=="TAPER"
    assert len(p.schedule.candidate.workouts)==365
    db2,leap=setup_season(tmp_path/"leap",start_date="2028-01-01",end_date="2028-12-31")
    pl=preview(db2,leap)
    assert len(pl.schedule.candidate.workouts)==366
    assert any(w.scheduled_date==date(2028,2,29) for w in pl.schedule.candidate.workouts)


def test_zero_events_is_maintenance_and_completed_event_has_no_race(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("Cancelled","2027-05-16",status="cancelled")])
    p=preview(db,season)
    assert p.can_apply and all(c.phase=="MAINTENANCE" for c in p.schedule.controls)
    # Use a real historical event to respect immutable completed intent validation.
    db2,old=setup_season(tmp_path/"old",[intent.EventDraft("Done","2026-10-01","5K",status="completed")],
        start_date="2026-10-01",end_date="2026-10-31")
    po=preview(db2,old)
    assert not po.can_apply
    assert not any(w.event_flag for w in po.schedule.candidate.workouts)
    assert next(c for c in po.schedule.controls if c.day==date(2026,10,2)).phase=="RECOVERY"


def test_covered_history_mean_and_missingness(tmp_path):
    db,season=setup_season(tmp_path,inputs=intent.SeasonInputs(lthr=170))
    weeks=tuple(CompletedWeek((date(2026,8,31)+timedelta(days=7*i)).isoformat(),7200+600*i,True,f"coverage-{i}") for i in range(4))
    p=preview(db,season,replace(SETTINGS,completed_weeks=weeks))
    assert p.schedule.baseline_seconds==8100
    assert "READINESS_UNVERIFIED" not in codes(p)
    assert "STARTING_LOAD_REQUIRED" not in codes(p)
    missing=preview(db,season,replace(SETTINGS,completed_weeks=(replace(weeks[0],complete=False),)))
    assert "STARTING_LOAD_REQUIRED" in codes(missing) and not missing.can_apply


@pytest.mark.parametrize("inputs,settings",[
    (intent.SeasonInputs(starting_duration_seconds=10800),SETTINGS),
    (intent.SeasonInputs(starting_duration_seconds=10800,lthr=170),GenerationSettings()),
    (intent.SeasonInputs(training_method="maffetone",starting_duration_seconds=10800),GenerationSettings()),
    (intent.SeasonInputs(age=16,training_method="maffetone",starting_duration_seconds=10800),GenerationSettings(maf_adjustment=0,maf_confirmed=True)),
])
def test_numeric_methodology_requires_explicit_parameters(tmp_path,inputs,settings):
    db,season=setup_season(tmp_path,inputs=inputs)
    before=snapshot(db)
    with pytest.raises(intent.SeasonError):
        preview(db,season,settings)
    assert snapshot(db)==before


def test_maf_native_prescriptions_and_auxiliary_accounting(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("A","2027-05-16")],
        inputs=intent.SeasonInputs(training_method="maffetone",starting_duration_seconds=14400))
    p=preview(db,season,GenerationSettings(maf_adjustment=-5,maf_confirmed=True))
    assert p.can_apply,p.schedule.errors
    assert p.schedule.candidate.parameter_snapshot["ceiling_bpm"]==135
    for w in p.schedule.candidate.workouts:
        if w.sport is Sport.RUNNING:
            assert all(s.prescription.primary.upper==135 for s in w.segments)
        else:
            assert all(s.prescription is None for s in w.segments)
    normalized=normalize_policy_sessions(p.schedule.candidate.workouts)
    assert all(w.sport in {"run","rest","strength","mobility"} for w in normalized)
    assert sum(w.sport=="run" for w in normalized)==sum(w.sport is Sport.RUNNING for w in p.schedule.candidate.workouts)
    assert not any(i.code.startswith("MAF") and i.severity=="error" for i in p.schedule.issues)


@pytest.mark.parametrize("sport,family,segment",[
    (Sport.STRENGTH,"REST",WorkoutSegment(SegmentKind.WORK,LoadMode.DURATION,60,duration_role="AUTHORITATIVE")),
    (Sport.REST,"REST",WorkoutSegment(SegmentKind.FREE_RUN,LoadMode.OPEN)),
    (Sport.RUNNING,"REST",WorkoutSegment(SegmentKind.NON_TRAINING,LoadMode.OPEN)),
])
def test_auxiliary_invariants(sport,family,segment):
    with pytest.raises(DomainError):
        PlannedWorkout(TODAY,sport,family,"TEST","Test",(segment,))


def test_rest_cannot_fabricate_duration():
    with pytest.raises(DomainError):
        WorkoutSegment(SegmentKind.NON_TRAINING,LoadMode.OPEN,duration_seconds=1)


def test_apply_has_full_audit_projection_and_idempotence(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("Half","2027-05-16")])
    p=preview(db,season)
    before=snapshot(db)
    with pytest.raises(intent.SeasonError,match="acknowledge"):
        service.apply_season_preview(db,p,approved_by="reviewer")
    assert snapshot(db)==before
    result=service.apply_season_preview(db,p,approved_by="reviewer",acknowledge_warnings=True)
    assert result.status=="applied" and result.workout_count==365
    active=intent.get_season(db,season.season_id)
    assert active.plan_id==result.plan_id and active.current_revision_id==result.revision_id
    stored=load_revision(db,result.revision_id)
    assert stored.content_hash==p.schedule.candidate.content_hash
    assert stored.candidate.workouts==p.schedule.candidate.workouts
    applications=service.list_applications(db,season.season_id)
    assert len(applications)==1
    audit=json.loads(applications[0]["review_json"])
    assert audit["warnings_acknowledged"] and len(audit["diff"]["added_workout_ids"])==365
    rows=plan_rows(db)
    assert len(rows)==365 and any(r["intensity"]=="F80.ZONE_2" for r in rows)
    assert {r["sport"] for r in rows}=={"RUNNING","REST","STRENGTH","MOBILITY"}
    after=snapshot(db)
    duplicate=service.apply_season_preview(db,p,approved_by="reviewer",acknowledge_warnings=True)
    assert duplicate.status=="duplicate" and snapshot(db)==after
    with pytest.raises(intent.SeasonOwnershipError):
        approve_revision(db,replace(p.schedule.candidate,revision_id="bypass",parent_revision_id=result.revision_id),expected_content_hash="0"*64,approved_by="reviewer")
    conn=connect_sqlite(db)
    with pytest.raises(sqlite3.IntegrityError,match="immutable"):
        conn.execute("UPDATE season_revision_application SET approved_by='other'")
    assert conn.execute("PRAGMA foreign_key_check").fetchall()==[]
    conn.close()


@pytest.mark.parametrize("stage",["after_recheck","after_ownership","after_revision_insert","after_workout_insert", "after_segment_insert","during_projection","before_active_pointer_update","after_nutrition","after_audit","before_commit"])
def test_all_apply_failures_roll_back_every_component(tmp_path,stage):
    db,season=setup_season(tmp_path)
    p=preview(db,season)
    before=snapshot(db)
    with pytest.raises(InjectedApprovalFailure):
        service.apply_season_preview(db,p,approved_by="reviewer",acknowledge_warnings=True,failure_hook=fail_at(stage))
    assert snapshot(db)==before


@pytest.mark.parametrize("change",["event","input","archive","other_schedule","runtime","local_day","raw_hidden"])
def test_composite_stale_identity_rejects_without_apply(tmp_path,monkeypatch,change):
    db,season=setup_season(tmp_path)
    p=preview(db,season)
    if change=="event":
        intent.save_event(db,season.season_id,intent.EventDraft("New","2027-05-16"),expected_version=1)
    elif change=="input":
        intent.update_season(db,season.season_id,intent.SeasonDraft(season.name,season.start_date,season.end_date,season.timezone,replace(season.inputs,age=41)),expected_version=1)
    elif change=="archive":
        intent.set_archived(db,season.season_id,expected_version=1,archived=True)
    elif change=="local_day":
        monkeypatch.setattr(service,"_local_today",lambda tz:TODAY+timedelta(days=1))
    elif change=="runtime":
        _approve(db,_fitzgerald_candidate())
        # Create a preview after the outside-range schedule exists, then add completion evidence.
        p=preview(db,season)
        from garmin_data_hub.plan_methodology.activity_workout_match import create_match
        conn=connect_sqlite(db)
        conn.execute("INSERT INTO activity(activity_id,activity_type) VALUES(7,'running')")
        create_match(conn,revision_id="rev-a",workout_id="workout-1",activity_id=7,status="CONFIRMED",source="MANUAL",confidence="HIGH",reviewer="r",reason="reviewed")
        conn.commit();conn.close()
    else:
        conn=connect_sqlite(db)
        conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name,source_plan_id,source_revision_id,source_workout_id) VALUES(?,?,?,?,?)",
            ("2026-11-01","Outside change","hidden" if change=="raw_hidden" else None,"inactive" if change=="raw_hidden" else None,"hidden" if change=="raw_hidden" else None))
        conn.commit();conn.close()
    before=snapshot(db)
    with pytest.raises(service.StalePreviewError):
        service.apply_season_preview(db,p,approved_by="reviewer",acknowledge_warnings=True)
    assert snapshot(db)==before


def test_candidate_forgery_recomputed_not_trusted(tmp_path):
    db,season=setup_season(tmp_path)
    p=preview(db,season)
    modified=replace(p.schedule.candidate,athlete_snapshot={"age":90})
    forged=replace(p,schedule=replace(p.schedule,candidate=modified))
    before=snapshot(db)
    with pytest.raises(service.StalePreviewError,match="candidate"):
        service.apply_season_preview(db,forged,approved_by="reviewer",acknowledge_warnings=True)
    assert snapshot(db)==before


def test_concurrent_apply_one_revision_and_one_duplicate(tmp_path):
    db,season=setup_season(tmp_path)
    p=preview(db,season)
    def apply(_):
        return service.apply_season_preview(db,p,approved_by="reviewer",acknowledge_warnings=True).status
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(apply,[1,2]))==["applied","duplicate"]
    assert len(service.list_applications(db,season.season_id))==1


def test_linked_full_preview_is_read_only_and_keeps_parameters(tmp_path):
    db,season=setup_season(tmp_path)
    p=preview(db,season)
    service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True)
    active=intent.get_season(db,season.season_id)
    intent.update_season(db,active.season_id,intent.SeasonDraft(active.name,active.start_date,active.end_date,active.timezone,replace(active.inputs,lthr=175)),expected_version=active.input_version)
    before=snapshot(db)
    next_preview=preview(db,active,GenerationSettings())
    assert next_preview.can_apply
    assert next_preview.schedule.candidate.parameter_snapshot==p.schedule.candidate.parameter_snapshot
    assert snapshot(db)==before
    service.apply_season_preview(db,next_preview,approved_by="r",acknowledge_warnings=True)


@pytest.mark.parametrize("double",[False,True])
def test_nutrition_invalidation_keeps_unaffected_cache(tmp_path,double):
    db,season=setup_season(tmp_path)
    data={"day_plans":[{"iso_date":"2027-02-01","nutrition":{"day_type":"old"}},
                       {"iso_date":"2026-10-03","nutrition":{"day_type":"keep"}}]}
    conn=connect_sqlite(db)
    blob=json.dumps(data)
    conn.execute("INSERT INTO app_settings(key,value) VALUES('last_generated_plan',?)",(json.dumps(blob) if double else blob,))
    conn.commit();conn.close()
    p=preview(db,season)
    service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True)
    assert [r["date"] for r in nutrition_rows(db)]==["2026-10-03"]


def test_v13_upgrade_replay_repair_and_failure(tmp_path,monkeypatch):
    db,season=setup_season(tmp_path)
    conn=connect_sqlite(db)
    conn.execute("DROP TABLE season_revision_application")
    conn.execute("DELETE FROM schema_migrations WHERE version>=13")
    conn.commit()
    before=snapshot(db)
    original=migrate._migration_13_add_season_applications
    def fail(c,s):
        original(c,s)
        raise sqlite3.OperationalError("injected v13")
    monkeypatch.setattr(migrate,"_migration_13_add_season_applications",fail)
    with pytest.raises(sqlite3.OperationalError,match="injected"):
        migrate.apply_schema(conn,schema_sql_path())
    assert snapshot(db)==before
    monkeypatch.setattr(migrate,"_migration_13_add_season_applications",original)
    migrate.apply_schema(conn,schema_sql_path())
    migrate.apply_schema(conn,schema_sql_path())
    assert migrate.get_current_schema_version(conn)==migrate.CURRENT_SCHEMA_VERSION
    assert intent.get_season(db,season.season_id)==season
    conn.execute("DROP TRIGGER trg_season_application_no_update")
    conn.commit()
    migrate.apply_schema(conn,schema_sql_path())
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='trg_season_application_no_update'").fetchone()
    conn.close()


@pytest.mark.parametrize("run_days",range(1,8))
def test_every_run_day_count_and_availability_preserves_weekly_capacity(tmp_path,run_days):
    inputs=intent.SeasonInputs(run_days_per_week=run_days,long_run_day="Saturday",
                              starting_duration_seconds=10800,lthr=170)
    db,season=setup_season(tmp_path,inputs=inputs)
    p=preview(db,season)
    assert p.can_apply,p.schedule.errors
    from collections import defaultdict
    weeks=defaultdict(set)
    for w in p.schedule.candidate.workouts:
        if w.sport is Sport.RUNNING:
            weeks[w.scheduled_date.isocalendar()[:2]].add(w.scheduled_date)
    assert all(len(days)<=run_days for days in weeks.values())


def test_recovery_return_is_conservative_even_in_maintenance(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("Half","2027-05-16")])
    p=preview(db,season)
    before=max(w.training_seconds for w in p.schedule.weeks if w.monday<"2027-05-16")
    after=next(w for w in p.schedule.weeks if w.monday=="2027-05-24")
    assert before>p.schedule.baseline_seconds
    assert after.training_seconds<=p.schedule.baseline_seconds


def test_intersecting_recovery_taper_uses_lower_fraction(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("B","2027-05-01","HM","B"),
        intent.EventDraft("A","2027-05-14","HM")])
    p=preview(db,season)
    assert next(c for c in p.schedule.controls if c.day==date(2027,5,7)).load_fraction==Decimal("0.25")


def test_event_on_unavailable_day_blocks_without_extra_run_slot(tmp_path):
    inputs=intent.SeasonInputs(run_days_per_week=3,available_weekdays=(1,3,5),starting_duration_seconds=10800,lthr=170)
    db,season=setup_season(tmp_path,[intent.EventDraft("Unavailable Sunday","2027-05-16")],inputs=inputs)
    p=preview(db,season)
    assert "EVENT_UNAVAILABLE" in codes(p) and not p.can_apply


def test_recovery_overflow_blocks_known_following_workouts(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("Late event","2027-12-30","MAR")])
    conn=connect_sqlite(db)
    conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name) VALUES('2028-01-02','Existing run')")
    conn.commit();conn.close()
    p=preview(db,season)
    assert "RECOVERY_OVERFLOW" in codes(p)
    assert not p.can_apply and any("following workouts" in m for m in p.apply_blockers)


def test_preceding_owned_event_recovery_blocks_initial_boundary(tmp_path):
    db,season=setup_season(tmp_path)
    prior=intent.create_season(db,intent.SeasonDraft("Prior","2026-12-01","2026-12-31","America/New_York",
        intent.SeasonInputs(starting_duration_seconds=10800,lthr=170)))
    intent.save_event(db,prior.season_id,intent.EventDraft("Late marathon","2026-12-31","MAR"),expected_version=1)
    pp=preview(db,prior)
    # The next season has intent but no owned schedule yet; overflow is warned.
    assert pp.can_apply
    service.apply_season_preview(db,pp,approved_by="r",acknowledge_warnings=True)
    p=preview(db,season)
    assert not p.can_apply and any("preceding season" in m for m in p.apply_blockers)


def test_explicit_target_duration_is_estimated_not_authoritative(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("Performance","2027-05-16",goal_intent="PERFORMANCE",target_speed_mps="3.5")])
    p=preview(db,season)
    event=next(w for w in p.schedule.candidate.workouts if w.event_flag)
    leaf=event.segments[0]
    assert leaf.distance_role.value=="AUTHORITATIVE" and leaf.duration_role.value=="ESTIMATED"
    assert leaf.duration_seconds==int(Decimal(21098)/Decimal("3.5"))
    assert leaf.prescription.native_target.value=="F80.ZONE_3"
    assert "EVENT_LOAD_ESTIMATED" in codes(p)


def test_distance_growth_checked_only_when_all_training_distance_known(tmp_path):
    db,season=setup_season(tmp_path,[intent.EventDraft("Half","2027-05-16")])
    p=preview(db,season)
    rows=[]
    for w in p.schedule.candidate.workouts:
        if w.sport is Sport.RUNNING and not w.event_flag:
            distance=2000 if w.scheduled_date>=date(2027,3,1) else 1000
            w=replace(w,segments=tuple(replace(s,distance_metres=distance,distance_role="ESTIMATED") for s in w.segments))
        rows.append(w)
    candidate=replace(p.schedule.candidate,workouts=tuple(rows))
    issues=validate_season_workload(season,p.events,candidate,p.schedule.weeks,p.schedule.baseline_seconds)
    assert any(i.code=="DISTANCE_GROWTH" for i in issues)


def test_empty_nutrition_cache_does_not_block_initial_apply(tmp_path):
    db,season=setup_season(tmp_path)
    conn=connect_sqlite(db)
    conn.execute("INSERT INTO app_settings(key,value) VALUES('last_generated_plan','\"\"')")
    conn.commit();conn.close()
    p=preview(db,season)
    assert service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True).status=="applied"


def test_auxiliary_goal_cannot_be_created():
    from garmin_data_hub.plan_methodology.snapshots import GoalSnapshot
    with pytest.raises(DomainError,match="running only"):
        GoalSnapshot(Sport.STRENGTH,"COMPLETION")


def test_auxiliary_confirmed_activity_is_excluded_from_running_compliance(tmp_path):
    from garmin_data_hub.plan_methodology.activity_workout_match import create_match
    from garmin_data_hub.plan_methodology.runtime_compliance import evaluate_confirmed_match
    from garmin_data_hub.plan_methodology.fitzgerald_policy import aggregate_runtime_results
    db,season=setup_season(tmp_path)
    p=preview(db,season)
    result=service.apply_season_preview(db,p,approved_by="r",acknowledge_warnings=True)
    strength=next(w for w in p.schedule.candidate.workouts if w.sport is Sport.STRENGTH)
    conn=connect_sqlite(db)
    conn.execute("INSERT INTO activity(activity_id,activity_type) VALUES(7,'strength_training')")
    create_match(conn,revision_id=result.revision_id,workout_id=strength.workout_id,activity_id=7,
        status="CONFIRMED",source="MANUAL",confidence="HIGH",reviewer="r",reason="reviewed")
    conn.commit();conn.close()
    compliance=evaluate_confirmed_match(db,activity_id=7)
    assert compliance["status"]=="NOT_APPLICABLE" and compliance["excluded_from_running_distribution"]
    totals=aggregate_runtime_results(activity_results=[compliance])
    assert totals["context"]["REVISION"]["low_seconds"]==0
