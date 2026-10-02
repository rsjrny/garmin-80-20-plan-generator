from datetime import date
import pandas as pd
import pytest
from garmin_data_hub.analytics.chart_overview import period_range, comparison_range, prepare_overview, overview_figures

TODAY = date(2026,10,2)


def raw():
    return pd.DataFrame([
        dict(activity_id=1,activity_date="2026-09-01",sport="running",total_distance_m=5000,total_elapsed_s=1800,moving_time_s=1500,tss=0,trimp=40,avg_speed_mps=3,avg_power_w=None,total_ascent_m=0,hr_zone_status="current",zone_1_s=0,zone_2_s=1200,zone_3_s=300,zone_4_s=0,zone_5_s=0),
        dict(activity_id=2,activity_date="2026-09-02",sport="running",total_distance_m=5000,total_elapsed_s=1800,moving_time_s=None,tss=None,trimp=45,avg_speed_mps=0,total_ascent_m=None,hr_zone_status="stale",zone_1_s=0,zone_2_s=1000,zone_3_s=300,zone_4_s=0,zone_5_s=0),
        dict(activity_id=3,activity_date="2026-09-30",sport="strength_training",total_distance_m=0,total_elapsed_s=3600,tss=None,trimp=None,avg_speed_mps=0,hr_zone_status="missing"),
        dict(activity_id=4,activity_date="2026-08-01",sport="running",total_distance_m=10000,total_elapsed_s=3600,moving_time_s=3000,tss=50,trimp=70,avg_speed_mps=3,total_ascent_m=20),
    ])


def prepare(frame=None, **kwargs):
    return prepare_overview(raw() if frame is None else frame,date(2026,9,1),TODAY,today=TODAY,**kwargs)


def test_ranges_are_inclusive_and_previous_period_equal():
    assert period_range("4 weeks",TODAY) == (date(2026,9,5),TODAY)
    assert period_range("12 weeks",TODAY) == (date(2026,7,11),TODAY)
    assert period_range("Year to date",TODAY)[0] == date(2026,1,1)
    assert period_range("6 months",TODAY)[0] == date(2026,4,3)
    assert period_range("1 year",date(2024,2,29))[0] == date(2023,3,1)
    assert comparison_range(date(2026,9,1),TODAY) == (date(2026,7,31),date(2026,8,31))
    with pytest.raises(ValueError): comparison_range(TODAY,date(2026,9,1))
    with pytest.raises(ValueError): prepare_overview(raw(),TODAY,date(2026,10,3),today=TODAY)


def test_weekly_missing_zero_empty_and_partial_are_distinct():
    data = prepare()
    weekly = data["weekly"]
    assert weekly.loc["2026-08-31","load"] == 0  # real measured zero
    assert weekly.loc["2026-08-31","load_count"] == 1
    assert weekly.loc["2026-09-07","activities"] == 0
    assert weekly.loc["2026-09-07","load"] == 0  # no activities
    assert pd.isna(weekly.loc["2026-09-28","load"])  # activity without load
    assert weekly.iloc[0].partial and weekly.iloc[-1].partial
    assert weekly.load_rolling.isna().all()
    assert data["summary"]["load"]["change"] is None
    assert data["elapsed_fallback"] == 2
    assert data["summary"]["duration"]["value"] == pytest.approx(1500/3600+.5+1)


def test_units_load_choice_and_comparison():
    data = prepare(sport="running",unit_system="Imperial",load="trimp")
    assert data["unit"] == "mi" and data["elevation_unit"] == "ft"
    assert data["summary"]["distance"]["value"] == pytest.approx(10000/1609.344)
    assert data["summary"]["load"]["value"] == 85
    assert data["summary"]["load"]["change"] == pytest.approx((85/70-1)*100)
    assert data["summary"]["elevation"]["change"] is None


def test_stale_and_partial_zones_do_not_become_zero():
    data = prepare()
    assert data["zone_count"] == 1 and data["stale_zones"] == 1
    assert data["current"].loc[data["current"].activity_id.eq(2),"zone_2_s"].isna().all()
    cards = overview_figures(data,intensity="Percent")
    assert len(cards) == 4
    assert sum(trace.y[0] for trace in cards[2]["figure"].data) == pytest.approx(100)
    assert cards[3]["figure"] is None  # mixed sports cannot be compared


def test_pace_speed_and_sport_appropriate_analysis():
    data = prepare(sport="running",unit_system="Imperial")
    pace = overview_figures(data)[3]["figure"]
    assert len(pace.data[0].x) == 1  # zero speed omitted
    assert pace.layout.yaxis.autorange == "reversed"
    assert pace.layout.yaxis.ticktext[0].count(":") == 1
    speed = overview_figures(data,velocity_display="Speed")[3]["figure"]
    assert speed.data[0].y[0] == pytest.approx(3*2.236936292)
    strength = prepare(sport="strength_training")
    assert overview_figures(strength)[3]["figure"].data[0].y[0] == 60
    cycling = raw().copy()
    cycling["sport"] = "cycling"
    cycling["avg_power_w"] = [100,150,None,130]
    cycling["normalized_power_w"] = [110,160,None,140]
    power = overview_figures(prepare(cycling,sport="cycling"))[3]["figure"]
    assert power.layout.yaxis.title.text == "W (average power)"
    assert power.data[-1].name == "Normalized power"


def test_empty_and_bad_numbers_are_honest():
    data = prepare(pd.DataFrame())
    cards = overview_figures(data)
    assert len(data["weekly"]) == 5 and len(cards) == 4
    assert cards[0]["figure"].data[0].y == (0,0,0,0,0)
    assert data["summary"]["duration"]["value"] == 0
    bad = raw()
    bad["tss"] = [float("inf"),-1,None,None]
    bad["moving_time_s"] = [-1,float("inf"),None,None]
    data = prepare(bad)
    assert data["summary"]["load"]["value"] is None
    assert data["elapsed_fallback"] == 3


def test_chart_query_keeps_missing_and_stale_zones_and_local_end_date(tmp_path):
    from test_activity_calendar_days import _database, _insert_activity
    from garmin_data_hub.db.sqlite import connect_sqlite
    from garmin_data_hub.db import queries
    from garmin_data_hub.ui_nicegui.data import chart_dataframe
    db = _database(tmp_path)
    _insert_activity(db,1,local="2026-09-01T23:30:00-05:00",gmt="2026-09-02T04:30:00Z")
    _insert_activity(db,2,local="2026-09-02T12:00:00",gmt="2026-09-02T12:00:00")
    conn = connect_sqlite(db)
    conn.execute("INSERT INTO activity_metrics(activity_id,refresh_provenance_version,zone_1_s,zone_2_s,zone_3_s,zone_4_s,zone_5_s) VALUES(1,?,0,500,0,0,0)",(queries.ACTIVITY_METRICS_PROVENANCE_VERSION-1,))
    conn.commit()
    conn.close()
    frame = chart_dataframe(db,start_date="2026-09-01",end_date="2026-09-01")
    assert frame.activity_id.tolist() == [1]
    assert frame.iloc[0].hr_zone_status == "stale"
    assert frame["zone_2_s"].isna().all()
    frame = chart_dataframe(db,start_date="2026-09-02",end_date="2026-09-02")
    assert frame.iloc[0].hr_zone_status == "missing"
    assert frame["zone_2_s"].isna().all()


def test_multiyear_preparation_is_bounded():
    import time
    dates = pd.date_range("2022-01-01",periods=4000,freq="8h")
    frame = pd.DataFrame(dict(activity_date=dates,sport="running",total_distance_m=5000,total_elapsed_s=1800,tss=30,avg_speed_mps=3))
    started = time.perf_counter()
    data = prepare_overview(frame,date(2022,1,1),TODAY,sport="running",today=TODAY)
    assert len(data["current"]) == 4000
    assert len(data["weekly"]) < 260
    assert time.perf_counter()-started < 5
