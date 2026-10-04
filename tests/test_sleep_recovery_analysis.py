"""Sleep and recovery analysis data and UI wiring."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from nicegui import ui

from ui_test_support import run_ui

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.analytics.sleep_recovery import analyze_sleep_recovery
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.ui_nicegui import pages


def _health_database(tmp_path: Path) -> Path:
    db_path = tmp_path / "health.db"
    conn = connect_sqlite(db_path)
    conn.executescript(
        """
        CREATE TABLE sleep (
            calendar_date TEXT PRIMARY KEY,
            sleep_time_seconds INTEGER,
            sleep_need_minutes INTEGER,
            deep_sleep_seconds INTEGER,
            light_sleep_seconds INTEGER,
            rem_sleep_seconds INTEGER,
            awake_sleep_seconds INTEGER,
            sleep_score_overall INTEGER,
            average_hr_sleep REAL,
            resting_heart_rate INTEGER,
            avg_sleep_stress REAL,
            body_battery_change INTEGER,
            average_spo2 REAL,
            lowest_spo2 REAL,
            average_respiration REAL,
            avg_skin_temp_deviation_c REAL,
            sleep_score_feedback TEXT,
            sleep_score_insight TEXT
        );
        CREATE TABLE daily_summary (
            calendar_date TEXT PRIMARY KEY,
            resting_heart_rate INTEGER,
            average_stress_level INTEGER,
            body_battery_at_wake INTEGER,
            body_battery_highest INTEGER,
            body_battery_lowest INTEGER,
            body_battery_charged INTEGER,
            average_spo2 REAL,
            lowest_spo2 REAL,
            avg_waking_respiration REAL
        );
        CREATE TABLE hrv (
            calendar_date TEXT PRIMARY KEY,
            last_night_avg REAL,
            weekly_avg REAL,
            status TEXT,
            feedback_phrase TEXT,
            baseline_low REAL,
            baseline_upper REAL
        );
        CREATE TABLE training_readiness (
            calendar_date TEXT PRIMARY KEY,
            score REAL,
            level TEXT,
            feedback_short TEXT,
            recovery_time REAL
        );
        CREATE TABLE body_battery (
            calendar_date TEXT PRIMARY KEY,
            at_wake INTEGER,
            charged INTEGER,
            highest INTEGER,
            lowest INTEGER,
            during_sleep INTEGER
        );
        CREATE TABLE activity (
            activity_id INTEGER PRIMARY KEY,
            start_time_local TEXT,
            duration_seconds REAL,
            training_load REAL,
            training_stress_score REAL
        );
        """
    )
    first = date(2026, 8, 1)
    for index in range(14):
        day = (first + timedelta(days=index)).isoformat()
        sleep_hours = 6 if index < 7 else 7
        conn.execute(
            """
            INSERT INTO sleep VALUES (?, ?, 480, 5400, 10800, 5400, 1200,
                ?, 52, 50, 18, 35, 97, 93, 14, 0.1, 'Good', 'Keep a routine')
            """,
            (day, sleep_hours * 3600, 70 + index),
        )
        conn.execute(
            "INSERT INTO daily_summary VALUES (?, 50, ?, ?, 85, 20, 45, 97, 93, 15)",
            (day, 30 - index, 55 + index),
        )
        conn.execute(
            "INSERT INTO hrv VALUES (?, ?, ?, 'BALANCED', 'Within baseline', 35, 55)",
            (day, 40 + index, 42 + index / 2),
        )
        conn.execute(
            "INSERT INTO training_readiness VALUES (?, ?, 'HIGH', 'Ready', 8)",
            (day, 60 + index),
        )
        conn.execute(
            "INSERT INTO activity VALUES (?, ?, 3600, ?, NULL)",
            (index + 1, f"{day}T07:00:00", 20 + index * 3),
        )
    conn.commit()
    conn.close()
    return db_path


def test_sleep_recovery_analysis_combines_sources_and_compares_weeks(tmp_path):
    result = analyze_sleep_recovery(
        _health_database(tmp_path),
        "2026-08-01",
        "2026-08-14",
    )

    assert result["summary"]["sleep_nights"] == 14
    assert result["summary"]["sleep_coverage_pct"] == 100.0
    assert result["summary"]["average_sleep_hours"] == 6.5
    assert result["summary"]["sleep_duration_sd_hours"] == 0.5
    assert result["rows"][-1]["sleep_score"] == 83.0
    assert result["rows"][-1]["body_battery_wake"] == 68.0
    assert result["rows"][-1]["hrv_status"] == "BALANCED"
    sleep_trend = next(
        trend for trend in result["trends"] if trend["metric"] == "Sleep duration"
    )
    assert sleep_trend == {
        "metric": "Sleep duration",
        "recent_7d": 7.0,
        "previous_7d": 6.0,
        "change": 1.0,
        "unit": "h",
        "recent_days": 7,
        "previous_days": 7,
    }
    assert result["relationships"]
    assert any(item["title"] == "Sleep need comparison" for item in result["insights"])
    assert all(source["available"] for source in result["sources"])


def test_sleep_recovery_analysis_tolerates_missing_health_tables(tmp_path):
    db_path = tmp_path / "empty.db"
    conn = connect_sqlite(db_path)
    conn.execute("CREATE TABLE app_settings(key TEXT PRIMARY KEY, value TEXT)")
    conn.close()

    result = analyze_sleep_recovery(db_path, "2026-08-01", "2026-08-07")

    assert result["rows"] == []
    assert result["summary"]["sleep_coverage_pct"] == 0.0
    assert not any(source["available"] for source in result["sources"])


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("bad", "2026-08-01", "Start date"),
        ("2026-08-02", "2026-08-01", "on or before"),
        ("2024-01-01", "2026-08-01", "limited to 730 days"),
    ],
)
def test_sleep_recovery_analysis_validates_date_window(
    tmp_path, start, end, message
):
    db_path = tmp_path / "validation.db"
    connect_sqlite(db_path).close()

    with pytest.raises(ValueError, match=message):
        analyze_sleep_recovery(db_path, start, end)


@pytest.mark.parametrize("with_records", [True, False], ids=["records", "empty"])
def test_recovery_action_renders_analysis_or_missing_data(monkeypatch, tmp_path, with_records):
    db_path = _health_database(tmp_path) if with_records else tmp_path / "empty.db"
    conn = connect_sqlite(db_path)
    apply_schema(conn, schema_sql_path())
    conn.close()
    monkeypatch.setattr(pages, "describe_readonly_tools", lambda _db: {})

    async def scenario(user):
        await user.open("/query")
        next(iter(user.find("Start date").elements)).value = "2026-08-01"
        next(iter(user.find("End date").elements)).value = "2026-08-14"
        user.find("Analyze recovery").click()
        if with_records:
            await user.should_see("Deep-dive observations", retries=100)
            await user.should_see("Sleep duration vs. need")
            await user.should_see("Daily detail")
            grids = user.find(ui.aggrid).elements
            assert any(len(grid.options.get("rowData", [])) == 14 for grid in grids)
            figures = user.find(ui.plotly).elements
            sleep = next(plot for plot in figures if any(
                trace.name == "Recorded sleep" for trace in plot.figure.data
            ))
            trace = next(trace for trace in sleep.figure.data if trace.name == "Recorded sleep")
            assert list(trace.y) == [6.0] * 7 + [7.0] * 7
        else:
            await user.should_see("No sleep or recovery records were found", retries=100)
            await user.should_not_see(ui.plotly)

    run_ui(db_path, scenario)
