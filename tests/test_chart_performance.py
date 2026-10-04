from datetime import date
import sqlite3
import time

import pandas as pd
import pytest

from garmin_data_hub.analytics.chart_overview import prepare_overview
from garmin_data_hub.analytics.chart_explorer import chart_state, resolve_click
from garmin_data_hub.analytics.chart_performance import (
    DEFAULTS, PERFORMANCE_CATALOG, parse_pace, performance_figures,
    performance_state, prepare_performance, threshold_usable,
)
from garmin_data_hub.analytics.temporal_metrics import TemporalSample
from garmin_data_hub.services.chart_performance_data import advanced_chart_evidence, summarize_samples
from garmin_data_hub.services.thresholds import RUNNING_FTP_ALGORITHM_VERSION, ESTIMATED_LTHR_ALGORITHM_VERSION
from garmin_data_hub.db.queries import ACTIVITY_METRICS_PROVENANCE_VERSION, get_activities_dataframe
from garmin_data_hub.ui_nicegui.data import chart_dataframe
from test_activity_calendar_days import _database, _insert_activity

TODAY = date(2026,10,4)


def row(aid=1, **changes):
    result = dict(activity_id=aid, activity_date="2026-09-20", sport="running",
        total_distance_m=10800, total_elapsed_s=3600, moving_time_s=3500,
        total_ascent_m=0, avg_hr_bpm=130, avg_speed_mps=3,
        avg_cadence_spm=170, avg_power_w=200, normalized_power_w=220,
        tss=50, metric_provenance_version=ACTIVITY_METRICS_PROVENANCE_VERSION,
        pace_decoupling_pct=-2, hr_drift_pct=3, power_ftp_w=250, power_zone_status="current")
    result.update({f"power_zone_{z}_s": 3600 if z==2 else 0 for z in range(1,8)})
    result.update({f"peak_power_{d}s_w": 300-d/20 for d in (5,30,60,300,1200)})
    result.update(changes)
    return result


def samples(duration=3600,speed=3,hr=130,power=200):
    return [TemporalSample(t,speed,hr,power,seq=t) for t in range(0,duration+1,30)]


def thresholds():
    return dict(lthr_effective=160,ftp_effective=250,
        lthr_status=dict(effective_source="manual_override",source_age_status="unknown"),
        ftp_status=dict(effective_source="manual_override",source_age_status="unknown"))


def prepare(rows=None, summaries=None, contexts=None, th=None, sport="running", unit="Metric", **options):
    rows = [row()] if rows is None else rows
    evidence = dict(thresholds=thresholds() if th is None else th,
        summaries={r['activity_id']:summarize_samples(samples()) for r in rows} if summaries is None else summaries,
        contexts=contexts or {})
    overview = prepare_overview(pd.DataFrame(rows),date(2026,9,1),TODAY,sport=sport,unit_system=unit,today=TODAY)
    return prepare_performance(overview,evidence,{**DEFAULTS,**options})


def test_weighted_bands_duplicates_order_gaps_and_final_sample():
    summary = summarize_samples([
        TemporalSample(40,3,130,100,4), TemporalSample(0,1,120,0,0),
        TemporalSample(0,2,130,200,2), TemporalSample(10,4,150,200,3),
        TemporalSample(100,9,190,300,5), TemporalSample(110,9,190,300,6)])
    assert summary['observed_s'] == 110
    assert summary['paired_s'] == 50  # 40-100 gap unsupported; no final held sample
    assert summary['mean_speed'] == pytest.approx((2*10+4*30+9*10)/50)
    assert summary['mean_hr'] == pytest.approx((130*10+150*30+190*10)/50)
    assert summary['power_s'] == 50 and summary['longest_power_run_s'] == 40
    assert (2,130,10) in summary['bands']


def test_band_means_units_signed_durability_and_distinct_power_ratios():
    data = prepare(charts=[k for k,_ in PERFORMANCE_CATALOG])
    r = data['frame'].iloc[0]
    assert r.speed_at_hr == 3 and r.hr_at_pace == 130
    assert r.running_efficiency == pytest.approx(3/130)
    assert r.pace_decoupling_pct == -2 and r.hr_drift_pct == 3
    assert r.avg_power == 200 and r.normalized_power == 220
    assert r.variability == 1.1 and r.power_efficiency == pytest.approx(220/130)
    assert r.cadence == 170 and r.power_zone_1_s == 0 and r.zone_usable
    cards = {c['key']:c for c in performance_figures(data,intensity="Percent")}
    assert all(c['figure'] is not None for c in cards.values())
    assert sum(t.y[0] for t in cards['power_zones']['figure'].data) == 100
    assert cards['long_runs']['figure'].layout.yaxis2.title.text == "min"
    pace = cards['pace_at_hr']['figure']
    assert list(pace.layout.xaxis.range) == ['2026-09-01','2026-10-05']
    from nicegui import json
    json.dumps(pace.to_plotly_json())  # The live socket serializer must accept the date bounds.
    assert pace.data[0].y[0] == pytest.approx(1000/3/60)
    assert pace.layout.yaxis.autorange == "reversed"
    assert resolve_click(pace,dict(curveNumber=0,pointNumber=0)) == dict(kind="activity",key=1)
    assert resolve_click(pace,dict(curveNumber=1,pointNumber=0)) is None
    imperial = performance_figures(prepare(unit="Imperial"))[0]['figure']
    assert imperial.data[0].y[0] == pytest.approx(1609.344/3/60)
    speed = performance_figures(prepare(unit="Imperial"),velocity_display="Speed")[0]['figure']
    assert speed.data[0].y[0] == pytest.approx(3*2.236936292)


@pytest.mark.parametrize('change,reason',[
    (dict(paired_s=3100),'coverage below 90%'),
    (dict(speed_cv=.11),'variation exceeds 10%'),
    (dict(observed_s=1000,paired_s=1000),'coverage below 90%'),
])
def test_sparse_or_variable_sessions_are_excluded(change,reason):
    summary = {**summarize_samples(samples()),**change}
    r = prepare(summaries={1:summary})['frame'].iloc[0]
    assert not r.steady and reason in r.qualification
    assert pd.isna(r.speed_at_hr) and pd.isna(r.pace_decoupling_pct)


@pytest.mark.parametrize('context,reason',[
    (dict(family="INTERVAL_RUN",hard=True,sport="RUNNING"),'quality/event'),
    (dict(family="RACE",hard=True,sport="RUNNING"),'quality/event'),
    (dict(family="STRENGTH",hard=False,sport="STRENGTH"),'non-running'),
])
def test_original_confirmed_workout_rules(context,reason):
    r = prepare(contexts={1:context})['frame'].iloc[0]
    assert not r.steady and reason in r.qualification
    assert pd.isna(r.running_efficiency)
    assert prepare()['frame'].iloc[0].qualification == "Qualified; workout type unconfirmed"


def test_subtypes_terrain_families_and_same_day_identities_stay_separate():
    rows = [row(1),row(2,total_ascent_m=500),row(3,total_ascent_m=None),row(4,sport="trail_running")]
    data = prepare(rows,terrain="All terrain",contexts={1:dict(family="FOUNDATION_RUN",sport="RUNNING",hard=False)})
    assert data['frame'].activity_id.tolist() == [1,2,3]
    assert data['frame'].group.nunique() == 3
    chart = performance_figures(data)[0]['figure']
    points = [t for t in chart.data if t.meta]
    assert len(points) == 3
    assert {resolve_click(chart,dict(curveNumber=i,pointNumber=0))['key'] for i,t in enumerate(chart.data) if t.meta} == {1,2,3}
    r = prepare(rows)['frame'].set_index('activity_id')
    assert not r.loc[2,'steady'] and not r.loc[3,'steady']
    assert prepare(rows,sport="trail_running")['frame'].activity_id.tolist() == [4]


def test_durability_half_coverage_aerobic_duration_and_current_metrics():
    summary = summarize_samples(samples())
    assert pd.isna(prepare(summaries={1:{**summary,'half_coverage':.89}})['frame'].iloc[0].pace_decoupling_pct)
    assert pd.isna(prepare(summaries={1:{**summary,'mean_hr':150}})['frame'].iloc[0].pace_decoupling_pct)
    assert pd.isna(prepare(th={})['frame'].iloc[0].pace_decoupling_pct)
    assert pd.isna(prepare([row(metric_provenance_version=1)])['frame'].iloc[0].pace_decoupling_pct)
    short = summarize_samples(samples(1800))
    r = prepare([row(total_elapsed_s=1800)],summaries={1:short})['frame'].iloc[0]
    assert r.steady and "40 minutes" in r.durability_reason
    outside_band = prepare(hr_low=150,hr_high=160)['frame'].iloc[0]
    assert outside_band.steady and outside_band.hr_band_minutes == 0 and pd.isna(outside_band.speed_at_hr)


@pytest.mark.parametrize('missing_final_gap',[False,True])
def test_durability_does_not_hide_unsupported_elapsed_edges(missing_final_gap):
    track = samples(3300)
    if missing_final_gap:
        track.append(TemporalSample(3600,3,130,200,seq=3600))
    summary = summarize_samples(track)
    assert summary['half_coverage'] == 1  # Canonical metric half-span omits the unsupported tail.
    r = prepare(summaries={1:summary})['frame'].iloc[0]
    assert r.steady  # Overall coverage is still above 90%.
    assert r.durability_reason == 'Each elapsed half needs 90% paired support'
    assert pd.isna(r.pace_decoupling_pct)


@pytest.mark.parametrize('mutation',[
    dict(ftp_effective=None),dict(ftp_effective=float('nan')),
    dict(ftp_status=dict(effective_source="calculated",source_age_status="stale")),
    dict(ftp_provenance=dict(algorithm_version="legacy_unknown_v1")),
    dict(ftp_provenance=dict(threshold_type="running_ftp",algorithm_version=RUNNING_FTP_ALGORITHM_VERSION,source_sport="cycling",calculated_value=250)),
    dict(ftp_provenance=dict(threshold_type="running_ftp",algorithm_version=RUNNING_FTP_ALGORITHM_VERSION,source_sport="running",calculated_value=249)),
])
def test_threshold_provenance_excludes_unknown_stale_or_wrong_sport(mutation):
    th = thresholds()
    th.update(ftp_status=dict(effective_source="calculated",source_age_status="current"),
        ftp_provenance=dict(threshold_type="running_ftp",algorithm_version=RUNNING_FTP_ALGORITHM_VERSION,source_sport="running",calculated_value=250))
    assert threshold_usable(th,'ftp')
    th.update(mutation)
    assert not threshold_usable(th,'ftp')
    r = prepare(th=th)['frame'].iloc[0]
    assert pd.isna(r.avg_power) and pd.isna(r.peak_5) and not r.zone_usable


def test_lthr_and_cycling_power_require_appropriate_thresholds():
    th = thresholds()
    th.update(lthr_status=dict(effective_source="calculated",source_age_status="current"),
        lthr_provenance=dict(threshold_type="estimated_lthr",algorithm_version=ESTIMATED_LTHR_ALGORITHM_VERSION,calculated_value=160))
    assert threshold_usable(th,'lthr')
    th['lthr_provenance']['calculated_value'] = 159
    assert not threshold_usable(th,'lthr')
    r = prepare([row(sport="cycling")],sport="cycling")['frame'].iloc[0]
    assert "other sports" in r.power_reason and pd.isna(r.avg_power)
    assert all(c['figure'] is None for c in performance_figures(prepare([row()],sport="All sports")))


@pytest.mark.parametrize('change,reason',[
    (dict(power_zone_1_s=None),'seven zone'),(dict(power_zone_1_s=-1),'seven zone'),
    (dict(power_zone_status="stale"),'stale'),(dict(power_ftp_w=249),'threshold differs'),
    (dict(power_zone_2_s=3000),'coverage'),(dict(power_zone_2_s=4000),'coverage'),
])
def test_power_zones_preserve_missing_partial_stale_and_invalid(change,reason):
    r = prepare([row(**change)])['frame'].iloc[0]
    assert not r.zone_usable and reason in r.zone_reason
    assert pd.isna(r.power_zone_1_s)


def test_power_support_and_exact_duration_peak_sources():
    summaries = {1:summarize_samples(samples()),2:summarize_samples(samples())}
    summaries[2]['longest_power_run_s'] = 600  # Enough total power but no supported 20-minute window.
    data = prepare([row(1),row(2,peak_power_5s_w=400,peak_power_1200s_w=500)],summaries=summaries,charts=['power_peaks'])
    card = performance_figures(data)[0]
    fig = card['figure']
    assert resolve_click(fig,dict(curveNumber=0,pointNumber=0))['key'] == 2
    assert resolve_click(fig,dict(curveNumber=0,pointNumber=4))['key'] == 1
    assert card['summary'][0]['contributing_activities'] == 2
    assert card['summary'][4]['contributing_activities'] == 1
    low = prepare(summaries={1:{**summaries[1],'power_s':3400}})['frame'].iloc[0]
    assert pd.isna(low.avg_power) and pd.isna(low.peak_5)


def test_measured_zero_power_is_distinct_from_missing_and_zero_denominator():
    values = {f'peak_power_{d}s_w':0 for d in (5,30,60,300,1200)}
    data = prepare([row(avg_power_w=0,normalized_power_w=0,**values)],
        summaries={1:summarize_samples(samples(power=0))},charts=['power','power_efficiency','power_peaks'])
    r = data['frame'].iloc[0]
    assert r.avg_power == 0 and r.normalized_power == 0 and r.power_efficiency == 0 and r.peak_1200 == 0
    assert pd.isna(r.variability)
    assert all(c['figure'] is not None for c in performance_figures(data))


def test_state_pace_validation_empty_missing_and_longest_week():
    assert performance_state(dict(hr_low=True,hr_high=float('nan'),speed_high=0,terrain=[],charts='bad')) == DEFAULTS
    assert parse_pace('5:30',1000) == pytest.approx(1000/330)
    assert parse_pace('8:51',1609.344) == pytest.approx(1609.344/531)
    for text in ('5.5','0:00','4:99','bad',None):
        with pytest.raises(ValueError): parse_pace(text,1000)
    assert chart_state(dict(section='Performance',performance=dict(charts=['cadence'])),['running'],TODAY)['performance']['charts'] == ['cadence']
    assert chart_state(dict(section=[]),[],TODAY)['section'] == 'Overview'
    assert all(c['figure'] is None for c in performance_figures(prepare([])))
    missing = prepare(summaries={})
    assert performance_figures(missing)[0]['figure'] is None
    rows = [row(1,total_distance_m=10000),row(2,total_distance_m=12000),row(3,total_distance_m=15000,moving_time_s=None,total_elapsed_s=None)]
    chart = performance_figures(prepare(rows,charts=['long_runs']))[0]['figure']
    assert chart.data[0].meta['targets'] == [dict(kind='activity',key=2)]


def test_read_only_reader_query_missingness_and_original_match(tmp_path):
    from revision_fixtures import _approve, _fitzgerald_candidate
    from garmin_data_hub.plan_methodology.activity_workout_match import create_match
    db = _database(tmp_path)
    for aid in (1,2,3):
        _insert_activity(db,aid,local='2026-09-20T12:00:00',gmt='2026-09-20T12:00:00')
    candidate = _fitzgerald_candidate()
    _approve(db,candidate)
    conn = sqlite3.connect(db)
    conn.execute('UPDATE athlete_profile SET ftp_override=250,lthr_override=160')
    for aid,version in ((1,ACTIVITY_METRICS_PROVENANCE_VERSION),(2,1)):
        conn.execute('INSERT INTO activity_metrics(activity_id,refresh_provenance_version,threshold_ftp_w,power_zone_1_s,power_zone_2_s,power_zone_3_s,power_zone_4_s,power_zone_5_s,power_zone_6_s,power_zone_7_s,pace_decoupling_pct) VALUES(?,?,250,0,3600,0,0,0,0,0,-2)',(aid,version))
    conn.executemany('INSERT INTO activity_trackpoints(activity_id,seq,timestamp_utc,speed_mps,heart_rate_bpm,power_w) VALUES(1,?,?,?,?,?)',[(s.seq,f'2026-09-20T{12+s.seq//3600:02}:{s.seq%3600//60:02}:{s.seq%60:02}Z',s.speed_mps,s.heart_rate_bpm,s.power_w) for s in samples()])
    create_match(conn,revision_id='rev-a',workout_id='workout-1',activity_id=1,status='CONFIRMED',source='MANUAL',confidence='HIGH',reviewer='r',reason='Known original')
    conn.commit()
    before = list(conn.iterdump())
    evidence = advanced_chart_evidence(db,[1,1,2,3])
    assert list(conn.iterdump()) == before
    assert evidence['contexts'][1]['family'] == 'FOUNDATION_RUN'
    assert not evidence['contexts'][1]['hard']
    assert evidence['summaries'][1]['paired_s'] == 3600
    conn.close()
    nullable = chart_dataframe(db,start_date='2026-09-20',end_date='2026-09-20').set_index('activity_id')
    assert nullable.loc[1,'power_zone_1_s'] == 0
    assert nullable.loc[1,'pace_decoupling_pct'] == -2
    assert pd.isna(nullable.loc[2,'power_zone_2_s']) and pd.isna(nullable.loc[3,'power_zone_2_s'])
    conn = sqlite3.connect(db)
    legacy = get_activities_dataframe(conn,'2026-09-20').set_index('activity_id')
    conn.close()
    assert legacy.loc[2,'power_zone_2_s'] == 0 and legacy.loc[3,'power_zone_2_s'] == 0
    # Pending matches never supply a workout group.
    conn = sqlite3.connect(db)
    create_match(conn,revision_id='rev-a',workout_id='workout-1',activity_id=3,status='CANDIDATE',source='RECONCILIATION',confidence='LOW')
    conn.commit(); conn.close()
    assert advanced_chart_evidence(db,[3])['contexts'] == {}


def test_multiyear_qualification_is_bounded_and_numeric_noise_safe():
    rows = [row(i,activity_date=(pd.Timestamp('2022-01-01')+pd.Timedelta(hours=8*i)).date().isoformat()) for i in range(4000)]
    overview = prepare_overview(pd.DataFrame(rows),date(2022,1,1),TODAY,sport='running',today=TODAY)
    facts = summarize_samples(samples())
    start = time.perf_counter()
    data = prepare_performance(overview,dict(thresholds=thresholds(),summaries={i:facts for i in range(4000)},contexts={}),DEFAULTS)
    assert len(data['frame']) == 4000 and time.perf_counter()-start < 5
    bad = prepare([row(avg_power_w='bad',normalized_power_w=float('inf'),avg_cadence_spm='bad',hr_drift_pct='bad')])['frame'].iloc[0]
    assert pd.isna(bad.avg_power) and pd.isna(bad.normalized_power) and pd.isna(bad.cadence) and pd.isna(bad.hr_drift_pct)


def test_original_workout_context_survives_season_regeneration(tmp_path,monkeypatch):
    from test_season_regeneration import active, apply
    from test_season_generation import SETTINGS, TODAY as SEASON_TODAY
    from garmin_data_hub.services import season_generation as service
    from garmin_data_hub.plan_methodology.activity_workout_match import create_match
    monkeypatch.setattr(service,'_local_today',lambda tz:SEASON_TODAY)
    db,season,parent = active(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO activity(activity_id,activity_type) VALUES(99,'running')")
    original = parent.workouts[0]
    create_match(conn,revision_id=parent.revision_id,workout_id=original.workout_id,activity_id=99,status='CONFIRMED',source='MANUAL',confidence='HIGH',reviewer='r',reason='Original workout')
    conn.commit(); conn.close()
    apply(db,service.preview_season(db,season.season_id,SETTINGS))
    context = advanced_chart_evidence(db,[99])['contexts'][99]
    assert context['family'] == original.family
    assert context['hard'] == (original.quality_flag or original.event_flag)
    assert context['sport'] == original.sport.value
