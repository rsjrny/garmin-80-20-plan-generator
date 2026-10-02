from datetime import date
import pandas as pd
import pytest
from test_chart_overview import raw, prepare, TODAY
from garmin_data_hub.analytics.chart_explorer import (
    CATALOG, chart_state, contributors, explorer_figures, resolve_click, source_rows, tag_overview,
)
from garmin_data_hub.analytics.chart_overview import overview_figures


def test_same_day_points_keep_ids_after_sorting_and_metric_exclusions():
    frame = raw()
    frame.loc[1, "activity_date"] = frame.loc[0, "activity_date"]
    frame.loc[1, "avg_speed_mps"] = 4
    frame.loc[0, "activity_name"] = "Morning run"
    data = prepare(frame, sport="running")
    cards = tag_overview(overview_figures(data), data)
    figure = cards[3]["figure"]
    assert resolve_click(figure, dict(curveNumber=0, pointNumber=0)) == dict(kind="activity", key=1)
    assert resolve_click(figure, dict(curveNumber=0, pointNumber=1)) == dict(kind="activity", key=2)
    assert "Morning run" in str(figure.data[0].customdata)
    assert resolve_click(figure, dict(curveNumber=1, pointNumber=0)) is None  # rolling line
    for payload in [{}, dict(curveNumber=-1, pointNumber=0), dict(curveNumber=0, pointNumber=-1), dict(curveNumber=True,pointNumber=0), dict(curveNumber=99,pointNumber=0)]:
        assert resolve_click(figure,payload) is None


def test_week_targets_preserve_full_dates_and_sport_and_include_missing():
    data = prepare()
    cards = tag_overview(overview_figures(data),data)
    target = resolve_click(cards[0]["figure"],dict(curveNumber=0,pointNumber=0))
    assert target == dict(kind="week",key="2026-08-31",sport="running")
    rows = source_rows(contributors(data["current"],target),data)
    assert [r["id"] for r in rows] == [1,2]
    assert rows[0]["load"] == 0 and rows[1]["load"] is None
    assert rows[1]["time_source"] == "elapsed fallback"
    assert contributors(data["current"],dict(kind="week",key="2026-09-07")).empty
    assert contributors(data["current"],dict(kind="week",key="2026-08-24")).empty  # comparison excluded


def test_full_catalog_is_retained_and_builds_only_selection():
    data = prepare(sport="running")
    cards = explorer_figures(data, {key for key,_ in CATALOG})
    assert len(cards) == 11
    assert explorer_figures(data,set()) == []
    weekly = explorer_figures(data,{"weekly_training_stress"})[0]["figure"]
    assert len(weekly.data[0].y) == len(data["weekly"])
    assert weekly.data[0].y[0] == 0
    hr = explorer_figures(data,{"average_heart_rate"})[0]
    assert hr["figure"] is None
    pace = explorer_figures(data,{"average_velocity"})[0]["figure"]
    assert resolve_click(pace,dict(curveNumber=0,pointNumber=0))["key"] == 1
    assert explorer_figures(prepare(),{"average_velocity"})[0]["figure"] is None
    empty = explorer_figures(prepare(pd.DataFrame()),{key for key,_ in CATALOG})
    assert len(empty) == 11


def test_custom_and_relative_tab_state_and_stale_selections():
    saved = dict(quick="Custom",start="2026-09-01",end="2026-09-15",sport="running",load="trimp",volume="Distance",intensity="Percent",section="Explorer",charts=["weekly_distance","obsolete"])
    state = chart_state(saved,["running"],TODAY)
    assert state["start"] == saved["start"] and state["end"] == saved["end"]
    assert state["charts"] == ["weekly_distance"] and state["section"] == "Explorer"
    saved["quick"] = "4 weeks"
    assert chart_state(saved,["running"],date(2026,10,3))["end"] == "2026-10-03"
    assert chart_state(saved,[],TODAY)["sport"] == "All sports"
    assert chart_state(saved,[],TODAY)["volume"] == "Time"
    saved.update(quick="Custom",end="garbage")
    assert chart_state(saved,["running"],TODAY)["quick"] == "12 weeks"
    assert chart_state(None,[],TODAY)["charts"] == ["average_heart_rate"]


def test_names_are_optional_and_do_not_require_schema_migration(tmp_path):
    from test_activity_calendar_days import _database, _insert_activity
    from garmin_data_hub.db.sqlite import connect_sqlite
    from garmin_data_hub.ui_nicegui.data import chart_dataframe
    db = _database(tmp_path)
    _insert_activity(db,1,local="2026-09-01T12:00:00",gmt="2026-09-01T12:00:00")
    assert chart_dataframe(db,start_date="2026-09-01").iloc[0].activity_name == "Activity 1"
    conn = connect_sqlite(db)
    conn.execute("ALTER TABLE activity ADD COLUMN activity_name TEXT")
    conn.execute("UPDATE activity SET activity_name='Lunch run'")
    conn.commit()
    conn.close()
    assert chart_dataframe(db,start_date="2026-09-01").iloc[0].activity_name == "Lunch run"
