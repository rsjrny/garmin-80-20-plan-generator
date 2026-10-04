from dataclasses import replace
from datetime import date, timedelta
import sqlite3
import time

import pandas as pd
import pytest

from garmin_data_hub.analytics.chart_overview import prepare_overview
from garmin_data_hub.analytics.chart_plan_comparison import prepare_plan_comparison, comparison_figures
from garmin_data_hub.analytics.chart_explorer import chart_state, resolve_click
from garmin_data_hub.services.chart_plan_data import active_chart_plan_data
from garmin_data_hub.plan_methodology.activity_workout_match import create_match
from garmin_data_hub.plan_methodology.revision_repository import CorruptRevisionError
from revision_fixtures import _database, _approve, _fitzgerald_candidate

TODAY = date(2026,10,4)


def workout(day="2026-10-01", **kw):
    row = dict(plan_id="p",revision_id="r",workout_id=day,date=day,title="Easy run",sport="RUNNING",rest=False,
        duration_s=3600,distance_m=None,duration_estimated=False,distance_estimated=False,load=None,
        explicitly_completed=False,candidates=0,activity_id=None,activity_date=None,activity_sport=None,missing_activity=False)
    row.update(kw)
    return row


def comparison(workouts, activities=(), start="2026-09-28", end="2026-10-04", **kw):
    snapshot = dict(plans=[dict(id="p",anchor="2026-10-01",revision="r")],workouts=workouts,unmanaged=0)
    raw = pd.DataFrame([dict(activity_id=aid,activity_date=day,sport=sport,total_elapsed_s=seconds,tss=load,total_distance_m=1609.344)
                        for aid,day,sport,seconds,load in activities])
    prepared = prepare_overview(raw,date.fromisoformat(start),date.fromisoformat(end),kw.pop("sport","All sports"),kw.pop("units","Metric"),today=TODAY)
    return prepare_plan_comparison(snapshot,prepared,today=TODAY,**kw)


def test_completion_substitutions_candidates_rest_and_unmatched():
    data = comparison([
        workout("2026-09-28",activity_id=1,activity_date="2026-09-29",activity_sport="running"),
        workout("2026-09-29",sport="REST",rest=True,duration_s=0,distance_m=0),
        workout("2026-09-30",activity_id=2,activity_date="2026-09-30",activity_sport="cycling"),
        workout("2026-10-01",explicitly_completed=True),
        workout("2026-10-02",candidates=1),workout("2026-10-03"),workout("2026-10-04")],
        [(1,"2026-09-29","running",3000,40),(2,"2026-09-30","cycling",4000,None),(3,"2026-10-01","running",1800,0)])
    row = data["weekly"][0]
    assert row["planned_sessions"] == 6 and row["rest_days"] == 1
    assert row["due_sessions"] == 5 and row["completed_due"] == 3 and row["completion_pct"] == 60
    assert row["substitutions"] == 2 and row["candidates"] == 1 and row["unmatched"] == 1
    assert row["planned_duration"] == 6 and row["duration_variance"] == pytest.approx(8800/3600-6)
    assert row["planned_load"] is None and row["load_variance"] is None
    assert row["actual_load"] == 40 and row["actual_load_count"] == 2
    assert data["workouts"][-1]["status"] == "Not yet due"
    assert data["workouts"][-2]["status"] == "No confirmed completion"
    assert data["sources"][-1]["relation"] == "Unmatched activity"


def test_missing_plan_measure_is_partial_total_with_no_percentage_or_variance():
    row = comparison([workout("2026-09-28"),workout("2026-10-04",duration_s=None)],[(1,"2026-10-01","running",3600,10)])["weekly"][0]
    assert row["planned_duration"] == 1 and row["planned_duration_count"] == 1
    assert row["duration_variance"] is None and row["duration_pct"] is None
    assert row["planned_distance"] is None and row["distance_pct"] is None


def test_missing_actual_duration_and_zero_load_are_distinct():
    row = comparison([workout("2026-09-28"),workout("2026-10-04")],[(1,"2026-10-01","running",None,0)])["weekly"][0]
    assert row["actual_duration"] is None and row["actual_load"] == 0
    assert row["duration_variance"] is None


def test_rest_only_and_outside_plan_weeks_have_no_completion_denominator():
    rows = comparison([workout("2026-10-01",rest=True,sport="REST",duration_s=0,distance_m=0)],start="2026-09-21")["weekly"]
    assert rows[0]["planned_duration"] is None and rows[0]["duration_variance"] is None
    assert rows[1]["planned_duration"] == 0 and rows[1]["completion_pct"] is None
    assert rows[1]["planned_sessions"] == 0 and rows[1]["rest_days"] == 1


def test_plan_alignment_clipped_range_and_imperial_estimate():
    data = comparison([workout(distance_m=1609.344,distance_estimated=True),workout("2026-10-04",distance_m=1609.344)],
        [(1,"2026-10-02","running",3600,30)],start="2026-10-02",units="Imperial",alignment="Plan weeks",plan_id="p")
    row = data["weekly"][0]
    assert row["week"] == "2026-10-01" and row["label"] == "Plan week 1*"
    assert row["planned_distance"] == 1 and row["actual_distance"] == 1 and row["distance_pct"] == 100
    assert row["partial"] and row["plan_days"] == 3
    assert len(data["workouts"]) == 1  # First day's plan excluded, not prorated.


def test_matches_outside_filters_still_prove_completion_and_are_explained():
    data = comparison([workout(activity_id=9,activity_date="2026-09-27",activity_sport="cycling"),workout("2026-10-04")],sport="running")
    row = data["workouts"][0]
    assert row["completed"] and row["substitution"] and row["activity_scope"] == "Outside date/sport filters"
    assert data["weekly"][0]["actual_duration"] == 0 and data["weekly"][0]["confirmed"] == 1


@pytest.mark.parametrize("extra", [dict(activity_id=5,missing_activity=True),dict(activity_id=5,activity_date="2026-10-05",activity_sport="running")])
def test_missing_and_future_activity_do_not_prove_completion(extra):
    assert not comparison([workout(**extra)])["workouts"][0]["completed"]


def test_confirmed_identity_with_unknown_recorded_date_retains_completion_evidence():
    row = comparison([workout(activity_id=5,activity_sport="running")])["workouts"][0]
    assert row["completed"] and row["activity_scope"] == "Recorded date unavailable"


def test_candidate_activity_is_unconfirmed_and_rejected_matches_have_no_effect():
    data = comparison([workout(candidates=1,candidate_activity_ids=[1])],[(1,"2026-10-01","running",3600,1),(2,"2026-10-01","running",3600,1)])
    assert data["sources"][0]["relation"] == "Candidate match; unconfirmed"
    assert data["sources"][1]["relation"] == "Unmatched activity"
    assert data["weekly"][0]["unmatched"] == 2 and data["weekly"][0]["completed_due"] == 0


def test_running_subtype_and_unrelated_sport_filters():
    data = comparison([workout()],[(1,"2026-10-01","trail_running",1000,1)],sport="trail_running")
    assert len(data["workouts"]) == 1 and data["weekly"][0]["activities"] == 1
    data = comparison([workout()],[(1,"2026-10-01","cycling",1000,1)],sport="cycling")
    assert data["workouts"] == [] and data["weekly"][0]["planned_duration"] == 0


def test_figures_keep_server_week_ids_and_state_roundtrips():
    data = comparison([workout()])
    for card in comparison_figures(data):
        if card["figure"] is None:
            assert card["title"] == "Weekly planned versus completed load"
            continue
        assert resolve_click(card["figure"],dict(curveNumber=0,pointNumber=0)) == dict(kind="week",key="2026-09-28")
        assert card["figure"].layout.xaxis.type == "category"
    state = chart_state(dict(section="Plan comparison",plan="p",alignment="Plan weeks"),[],TODAY)
    assert state["section"] == "Plan comparison" and state["plan"] == "p" and state["alignment"] == "Plan weeks"


def test_multiple_plans_require_selection_for_plan_alignment():
    snapshot = dict(plans=[],workouts=[],unmanaged=0)
    prepared = prepare_overview(pd.DataFrame(),TODAY,TODAY,today=TODAY)
    with pytest.raises(ValueError,match="Select one"):
        prepare_plan_comparison(snapshot,prepared,alignment="Plan weeks",today=TODAY)


def test_overlapping_active_plans_are_additive_and_selection_does_not_hide_actuals():
    rows = [workout("2026-09-28"),workout("2026-10-04"),workout("2026-09-28",plan_id="p2",activity_id=1,activity_date="2026-09-28",activity_sport="running")]
    snapshot = dict(plans=[dict(id="p",anchor="2026-09-28"),dict(id="p2",anchor="2026-09-28")],workouts=rows,unmanaged=0)
    prepared = prepare_overview(pd.DataFrame([dict(activity_id=1,activity_date="2026-09-28",sport="running",total_elapsed_s=3600)]),date(2026,9,28),TODAY,today=TODAY)
    all_plans = prepare_plan_comparison(snapshot,prepared,today=TODAY)
    assert all_plans["weekly"][0]["planned_duration"] == 3
    one_plan = prepare_plan_comparison(snapshot,prepared,plan_id="p",today=TODAY)
    assert one_plan["weekly"][0]["actual_duration"] == 1 and one_plan["weekly"][0]["unmatched"] == 0
    assert one_plan["sources"][0]["relation"] == "Matched to another plan"
    with pytest.raises(ValueError,match="Select one"):
        prepare_plan_comparison(snapshot,prepared,alignment="Plan weeks",today=TODAY)


def test_year_boundary_leap_day_and_inactive_weeks_keep_calendar_identity():
    snapshot = dict(plans=[dict(id="p",anchor="2023-12-31")],workouts=[workout("2023-12-31"),workout("2024-03-01")],unmanaged=0)
    prepared = prepare_overview(pd.DataFrame(),date(2023,12,31),date(2024,3,1),today=date(2024,3,2))
    rows = prepare_plan_comparison(snapshot,prepared,today=date(2024,3,2))["weekly"]
    assert rows[0]["week"] == "2023-12-25" and rows[-1]["week"] == "2024-02-26"
    assert rows[-1]["plan_days"] == 5 and rows[-1]["partial"]
    assert rows[1]["planned_duration"] == 0 and rows[1]["actual_duration"] == 0 and rows[1]["completion_pct"] is None


def test_query_uses_canonical_totals_and_explicit_match_not_projection_or_same_day(tmp_path):
    db = _database(tmp_path)
    _approve(db,_fitzgerald_candidate())
    conn = sqlite3.connect(db)
    conn.execute("ALTER TABLE activity ADD COLUMN start_time_local TEXT")
    conn.execute("INSERT INTO activity VALUES(7,'running','2026-10-02T00:30:00-04:00')")
    conn.execute("INSERT INTO activity VALUES(8,'running','2026-10-01')")
    create_match(conn,revision_id="rev-a",workout_id="workout-1",activity_id=7,status="CONFIRMED",source="MANUAL",confidence="HIGH",reviewer="r",reason="date substitution")
    conn.execute("UPDATE planned_workout SET planned_duration_s=999,planned_tss=100")
    conn.execute("INSERT INTO planned_workout(scheduled_date,workout_name) VALUES('2026-10-03','Legacy')")
    conn.commit()
    before = list(conn.iterdump())
    snapshot = active_chart_plan_data(db)
    assert list(conn.iterdump()) == before
    conn.close()
    row = snapshot["workouts"][0]
    assert row["duration_s"] == 3000 and row["distance_m"] is None and row["load"] is None
    assert row["activity_id"] == 7 and row["activity_date"] == "2026-10-02"
    assert snapshot["unmanaged"] == 1


def test_query_keeps_complete_estimated_distance_without_converting_time(tmp_path):
    from garmin_data_hub.plan_methodology.domain import MeasureRole
    db = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    workout = candidate.workouts[0]
    workout = replace(workout,segments=tuple(replace(s,distance_metres=1000,distance_role=MeasureRole.ESTIMATED) for s in workout.segments))
    _approve(db,replace(candidate,workouts=(workout,)))
    row = active_chart_plan_data(db)["workouts"][0]
    assert row["distance_m"] == 3000 and row["distance_estimated"]
    assert row["duration_s"] == 3000 and not row["duration_estimated"]


def test_query_rejects_missing_members_and_corrupt_revision(tmp_path):
    db = _database(tmp_path)
    candidate = _fitzgerald_candidate()
    candidate = replace(candidate,workouts=(candidate.workouts[0],replace(candidate.workouts[0],workout_id="w2",ordinal=1)))
    _approve(db,candidate)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM planned_workout WHERE source_workout_id='w2'")
    conn.commit()
    with pytest.raises(CorruptRevisionError,match="membership"):
        active_chart_plan_data(db)
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='plan_revision_workout'").fetchall():
        conn.execute(f'DROP TRIGGER "{name}"')
    conn.execute("UPDATE plan_revision_workout SET title='Tampered'")
    conn.commit()
    conn.close()
    with pytest.raises(CorruptRevisionError):
        active_chart_plan_data(db)


@pytest.mark.parametrize("mutation", ["DELETE FROM planned_workout", "UPDATE planned_workout SET scheduled_date='2026-10-02'"])
def test_query_rejects_entire_missing_projection_and_moved_projection_date(tmp_path,mutation):
    db = _database(tmp_path)
    _approve(db,_fitzgerald_candidate())
    conn = sqlite3.connect(db)
    conn.execute(mutation); conn.commit(); conn.close()
    with pytest.raises(CorruptRevisionError):
        active_chart_plan_data(db)


def test_carried_origin_match_and_explicit_completion_after_regeneration(tmp_path,monkeypatch):
    from test_season_regeneration import active, apply, lock
    from test_season_generation import SETTINGS, TODAY as SEASON_TODAY
    from garmin_data_hub.services import season_generation as service
    monkeypatch.setattr(service,"_local_today",lambda tz:SEASON_TODAY)
    db, season, parent = active(tmp_path)
    lock(db,season,parent.workouts[1],explicitly_completed=True)
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO activity(activity_id,activity_type) VALUES(99,'running')")
    create_match(conn,revision_id=parent.revision_id,workout_id=parent.workouts[0].workout_id,activity_id=99,status="CONFIRMED",source="MANUAL",confidence="HIGH",reviewer="r",reason="confirmed")
    conn.commit(); conn.close()
    preview = service.preview_season(db,season.season_id,SETTINGS)
    apply(db,preview)
    rows = {w["workout_id"]:w for w in active_chart_plan_data(db)["workouts"]}
    assert rows[parent.workouts[0].workout_id]["activity_id"] == 99
    assert rows[parent.workouts[1].workout_id]["explicitly_completed"]


def test_multi_year_comparison_is_bounded():
    start = date(2022,1,1)
    rows = [workout((start+timedelta(days=i)).isoformat()) for i in range((TODAY-start).days+1)]
    activities = [(i+1,w["date"],"running",3600,50) for i,w in enumerate(rows)]
    beginning = time.perf_counter()
    data = comparison(rows,activities,start=start.isoformat())
    assert len(data["weekly"]) < 260 and time.perf_counter()-beginning < 5
