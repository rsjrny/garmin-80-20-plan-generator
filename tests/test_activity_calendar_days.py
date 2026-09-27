"""Regression coverage for Garmin-recorded local activity calendar days."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from garmin_data_hub.analytics.sleep_recovery import analyze_sleep_recovery
from garmin_data_hub.analytics.training_load import get_daily_tss
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import coach_chat
from garmin_data_hub.services.coaching_packet import build_coaching_packet
from garmin_data_hub.ui_nicegui.data import compliance_data, list_activities
from garmin_data_hub.ui_nicegui.pages import _training_chart_figures


def _database(tmp_path: Path, name: str = "activity-days.db") -> Path:
    db_path = tmp_path / name
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
        conn.execute(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                activity_type TEXT,
                start_time_local TEXT,
                start_time_gmt TEXT,
                distance_meters REAL,
                duration_seconds REAL,
                elapsed_duration_seconds REAL,
                average_hr REAL,
                max_hr REAL,
                elevation_gain REAL,
                average_speed REAL,
                avg_cadence REAL,
                avg_power REAL,
                norm_power REAL,
                intensity_factor REAL,
                training_load REAL,
                training_stress_score REAL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _insert_activity(
    db_path: Path,
    activity_id: int,
    *,
    local: str | None,
    gmt: str | None,
    distance_m: float = 10_000,
    duration_s: float = 3_600,
    tss: float = 50,
) -> None:
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            INSERT INTO activity(
                activity_id, activity_type, start_time_local, start_time_gmt,
                distance_meters, duration_seconds, elapsed_duration_seconds,
                average_hr, max_hr, elevation_gain, average_speed, avg_cadence,
                avg_power, norm_power, intensity_factor, training_load,
                training_stress_score
            ) VALUES (?, 'running', ?, ?, ?, ?, ?, 145, 175, 100, 2.78,
                      170, 240, 250, 0.8, ?, ?)
            """,
            (
                activity_id,
                local,
                gmt,
                distance_m,
                duration_s,
                duration_s,
                tss,
                tss,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_plan(
    db_path: Path,
    day: str,
    *,
    distance_m: float = 10_000,
    duration_s: float = 3_600,
) -> None:
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, planned_distance_m,
                planned_duration_s, planned_tss
            ) VALUES (?, 'Planned run', ?, ?, 50)
            """,
            (day, distance_m, duration_s),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("local", "gmt", "expected_day"),
    [
        pytest.param(
            "2026-01-01T23:30:00-05:00",
            "2026-01-02T04:30:00Z",
            "2026-01-01",
            id="late-local-gmt-next-day",
        ),
        pytest.param(
            "2026-02-01T00:30:00+05:00",
            "2026-01-31T19:30:00Z",
            "2026-02-01",
            id="early-local-gmt-previous-month",
        ),
        pytest.param(
            "2026-03-10T08:00:00-05:00",
            "2026-03-10T13:00:00Z",
            "2026-03-10",
            id="ordinary-agreeing-day",
        ),
        pytest.param(
            "2026-01-01T00:00:00+14:00",
            "2025-12-31T10:00:00Z",
            "2026-01-01",
            id="exact-midnight-and-year-boundary",
        ),
        pytest.param(
            "2024-02-29T23:45:00-05:00",
            "2024-03-01T04:45:00Z",
            "2024-02-29",
            id="leap-day-and-month-boundary",
        ),
    ],
)
def test_activity_listing_uses_recorded_local_day(
    tmp_path: Path,
    local: str,
    gmt: str,
    expected_day: str,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(db_path, 1, local=local, gmt=gmt)

    rows = list_activities(
        db_path,
        start_date=expected_day,
        end_date=expected_day,
    )

    assert [row["date"] for row in rows] == [expected_day]
    gmt_day = gmt[:10]
    if gmt_day != expected_day:
        assert list_activities(
            db_path,
            start_date=gmt_day,
            end_date=gmt_day,
        ) == []


def test_activity_listing_date_range_is_inclusive_at_both_boundaries(
    tmp_path: Path,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2025-12-31T23:30:00-05:00",
        gmt="2026-01-01T04:30:00Z",
    )
    _insert_activity(
        db_path,
        2,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
    )
    _insert_activity(
        db_path,
        3,
        local="2026-01-02T00:00:00-05:00",
        gmt="2026-01-02T05:00:00Z",
    )
    _insert_activity(
        db_path,
        4,
        local="2026-01-03T00:01:00-05:00",
        gmt="2026-01-03T05:01:00Z",
    )

    rows = list_activities(
        db_path,
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert {row["id"] for row in rows} == {2, 3}
    assert {row["date"] for row in rows} == {"2026-01-01", "2026-01-02"}


def test_missing_or_malformed_local_timestamp_uses_utc_day_fallback(
    tmp_path: Path,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(db_path, 1, local=None, gmt="2026-04-02T01:00:00Z")
    _insert_activity(db_path, 2, local="", gmt="2026-04-03T01:00:00Z")
    _insert_activity(
        db_path,
        3,
        local="not-a-timestamp",
        gmt="2026-04-04T01:00:00Z",
    )
    _insert_activity(
        db_path,
        4,
        local="2026-02-30T10:00:00",
        gmt="2026-04-05T01:00:00Z",
    )
    _insert_activity(db_path, 5, local="bad", gmt="also-bad")

    rows = {row["id"]: row["date"] for row in list_activities(db_path)}

    assert rows == {
        1: "2026-04-02",
        2: "2026-04-03",
        3: "2026-04-04",
        4: "2026-04-05",
        5: None,
    }
    assert list_activities(
        db_path,
        start_date="2026-04-01",
        end_date="2026-04-30",
    ) == [row for row in list_activities(db_path) if row["id"] != 5]


def test_compliance_matches_local_day_plan_and_preserves_full_completion_math(
    tmp_path: Path,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
    )
    _insert_plan(db_path, "2026-01-01")

    result = compliance_data(db_path)

    assert result["totals"]["distance_compliance_pct"] == 100.0
    assert result["totals"]["duration_compliance_pct"] == 100.0
    assert result["rows"] == [
        {
            "date": "2026-01-01",
            "planned_km": 10.0,
            "actual_km": 10.0,
            "planned_hours": 1.0,
            "actual_hours": 1.0,
        }
    ]


def test_compliance_does_not_match_activity_to_disagreeing_gmt_day_plan(
    tmp_path: Path,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
    )
    _insert_plan(db_path, "2026-01-02")

    result = compliance_data(db_path)

    assert result["totals"]["distance_compliance_pct"] == 0.0
    assert result["totals"]["duration_compliance_pct"] == 0.0
    assert result["rows"][0]["date"] == "2026-01-02"
    assert result["rows"][0]["actual_km"] == 0.0


def test_compliance_percentage_formula_is_unchanged_after_day_assignment(
    tmp_path: Path,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
        distance_m=5_000,
        duration_s=1_800,
    )
    _insert_plan(db_path, "2026-01-01", distance_m=20_000, duration_s=7_200)

    result = compliance_data(db_path)

    assert result["totals"]["distance_compliance_pct"] == 25.0
    assert result["totals"]["duration_compliance_pct"] == 25.0


def test_daily_training_load_groups_activities_across_local_midnight(
    tmp_path: Path,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:59:00-05:00",
        gmt="2026-01-02T04:59:00Z",
        tss=10,
    )
    _insert_activity(
        db_path,
        2,
        local="2026-01-02T00:01:00-05:00",
        gmt="2026-01-02T05:01:00Z",
        tss=20,
    )
    conn = connect_sqlite(db_path)
    try:
        frame = get_daily_tss(conn, "2026-01-01")
        upper_boundary = get_daily_tss(conn, "2026-01-02")
    finally:
        conn.close()

    assert frame["tss"].to_dict() == {
        pd.Timestamp("2026-01-01"): 10.0,
        pd.Timestamp("2026-01-02"): 20.0,
    }
    assert upper_boundary["tss"].to_dict() == {
        pd.Timestamp("2026-01-02"): 20.0,
    }


def test_sleep_recovery_activity_load_uses_local_day(tmp_path: Path) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
        tss=55,
    )

    result = analyze_sleep_recovery(db_path, "2026-01-01", "2026-01-01")

    assert [row["date"] for row in result["rows"]] == ["2026-01-01"]
    assert result["rows"][0]["activity_sessions"] == 1
    assert result["rows"][0]["activity_load"] == 55.0


def test_coaching_packet_reports_local_activity_day(tmp_path: Path) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
    )

    packet = build_coaching_packet(
        db_path,
        as_of="2026-01-01",
        lookback_days=1,
        plan_horizon_days=1,
    )

    history = packet["context"]["training_history"]
    assert history["summary"]["activities"] == 1
    assert history["recent_activities"][0]["date"] == "2026-01-01"


def test_coach_chat_plan_comparison_uses_local_activity_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
    )
    _insert_plan(db_path, "2026-01-01")
    monkeypatch.setattr(
        coach_chat,
        "build_coaching_packet",
        lambda *args, **kwargs: {"context": {}},
    )

    context = coach_chat.build_chat_context(
        db_path,
        as_of=pd.Timestamp("2026-01-02").date(),
    )

    assert context["plan_comparison"]["actual"]["sessions"] == 1
    assert context["plan_comparison"]["actual"]["km"] == 10.0


def test_training_chart_query_and_axis_use_local_activity_day(tmp_path: Path) -> None:
    db_path = _database(tmp_path)
    _insert_activity(
        db_path,
        1,
        local="2026-01-01T23:30:00-05:00",
        gmt="2026-01-02T04:30:00Z",
    )
    conn = connect_sqlite(db_path)
    try:
        frame = queries.get_activities_dataframe(
            conn,
            "2026-01-01T00:00:00",
            lthr=None,
            use_temp_zone_metrics=False,
        )
    finally:
        conn.close()

    assert frame["activity_date"].tolist() == ["2026-01-01"]
    figures = _training_chart_figures(
        frame,
        {"average_velocity"},
        unit="km",
        unit_system="Metric",
        velocity_display="Speed",
    )
    assert len(figures) == 1
    assert str(figures[0].data[0].x[0]).startswith("2026-01-01")
