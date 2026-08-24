from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.garmin_credentials import GarminCredentials
from garmin_data_hub.services.plan_persistence import save_plan_setting
from garmin_data_hub.ui_nicegui import data as nicegui_data
from garmin_data_hub.ui_nicegui.data import (
    SyncJob,
    activity_detail,
    activity_sports,
    compliance_data,
    distance_from_km,
    distance_unit,
    get_sync_job,
    interface_settings,
    list_activities,
    nutrition_rows,
    planning_settings,
    run_read_only_query,
    save_interface_settings,
    save_planning_settings,
    validate_read_only_sql,
)
from garmin_data_hub.ui_nicegui.pages import _fit_leaflet_route, _show_activity_detail


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
        conn.execute(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                activity_type TEXT,
                start_time_gmt TEXT,
                distance_meters REAL,
                elapsed_duration_seconds REAL,
                average_hr REAL,
                max_hr REAL,
                elevation_gain REAL,
                average_speed REAL,
                training_stress_score REAL,
                start_latitude REAL,
                start_longitude REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO activity VALUES
            (1, 'running', '2026-08-23T10:00:00', 10000, 3600,
             145, 172, 120, 2.78, 70, NULL, NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO activity_trackpoints(
                activity_id, seq, timestamp_utc, latitude, longitude,
                altitude_m, distance_m, heart_rate_bpm
            ) VALUES (1, 1, '2026-08-23T10:00:00Z', 40.0000, -75.0000,
                      100, 0, 140),
                     (1, 2, '2026-08-23T10:00:05Z', 40.0005, -74.9995,
                      101, 15, 142)
            """
        )
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss
            ) VALUES ('2026-08-23', 'Race', 'Test', 10000, 3600, 80)
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_activity_filters_and_compliance_use_shared_sqlite_contract(tmp_path):
    db_path = _database(tmp_path)

    assert activity_sports(db_path) == ["running"]
    assert list_activities(db_path, sport="running")[0]["distance_km"] == 10.0
    assert list_activities(db_path, sport="cycling") == []

    compliance = compliance_data(db_path)
    assert compliance["totals"]["duration_compliance_pct"] == 100.0
    assert compliance["totals"]["distance_compliance_pct"] == 100.0


def test_activity_detail_returns_stored_gps_track(tmp_path):
    detail = activity_detail(_database(tmp_path), 1)

    assert detail is not None
    assert len(detail["trackpoints"]) == 2
    assert detail["trackpoints"][0]["lat_deg"] == 40.0
    assert detail["trackpoints"][1]["lon_deg"] == -74.9995


def test_activity_selection_refreshes_and_navigates_to_detail_card():
    state = {"selected_id": None}
    calls = {"refresh": 0, "target": None}
    detail_card = object()
    detail_panel = SimpleNamespace(
        refresh=lambda: calls.__setitem__("refresh", calls["refresh"] + 1)
    )
    navigation = SimpleNamespace(
        to=lambda target: calls.__setitem__("target", target)
    )

    assert _show_activity_detail(
        state, 123, detail_panel, detail_card, navigation
    )
    assert state["selected_id"] == 123
    assert calls == {"refresh": 1, "target": detail_card}
    assert not _show_activity_detail(
        state, 123, detail_panel, detail_card, navigation
    )
    assert calls == {"refresh": 1, "target": detail_card}


def test_route_fit_does_not_wait_for_javascript_response():
    class MustNotBeAwaited:
        def __await__(self):
            raise AssertionError("map commands should be fire-and-forget")
            yield

    class RouteMap:
        def __init__(self):
            self.initialized_called = False
            self.commands = []

        async def initialized(self):
            self.initialized_called = True

        def run_map_method(self, name, *args):
            self.commands.append((name, args))
            return MustNotBeAwaited()

    route_map = RouteMap()
    coordinates = [[40.0, -75.0], [40.1, -74.9]]

    asyncio.run(_fit_leaflet_route(route_map, coordinates))

    assert route_map.initialized_called is True
    assert route_map.commands == [
        ("invalidateSize", ()),
        ("fitBounds", (coordinates, {"padding": [24, 24]})),
    ]


def test_plan_configuration_persists_for_codex_workspace(tmp_path):
    db_path = _database(tmp_path)
    values = {
        "athlete_name": "Runner",
        "age": 48,
        "distance": "20 Miler",
        "event_name": "Autumn 20",
        "run_days_per_week": 5,
        "long_run_day": "Sunday",
        "sodium_mg_per_hour": 750,
        "plan_start": "2026-08-23",
        "event_date": "2026-10-18",
        "output_directory": str(tmp_path / "exports"),
        "output_filename": "autumn-20.xlsx",
    }

    save_planning_settings(db_path, values)
    saved = planning_settings(db_path)

    assert {key: saved[key] for key in values} == values


@pytest.mark.parametrize(
    "output_filename",
    ["", "plan.xls", "nested/plan.xlsx", r"nested\plan.xlsx"],
)
def test_planning_settings_reject_invalid_workbook_filename(
    tmp_path, output_filename
):
    db_path = _database(tmp_path)
    values = {
        "athlete_name": "Runner",
        "age": 48,
        "distance": "10K",
        "event_name": "Autumn 10K",
        "run_days_per_week": 5,
        "long_run_day": "Sunday",
        "sodium_mg_per_hour": 750,
        "plan_start": "2026-08-23",
        "event_date": "2026-10-18",
        "output_directory": str(tmp_path),
        "output_filename": output_filename,
    }

    with pytest.raises(ValueError, match="Workbook filename"):
        save_planning_settings(db_path, values)


def test_planning_settings_roll_back_as_one_transaction(tmp_path):
    db_path = _database(tmp_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            CREATE TRIGGER reject_plan_distance_setting
            BEFORE INSERT ON app_settings
            WHEN NEW.key = 'plan_distance'
            BEGIN
                SELECT RAISE(ABORT, 'settings failure');
            END
            """
        )
        conn.commit()
    finally:
        conn.close()
    values = {
        "athlete_name": "Runner",
        "age": 48,
        "distance": "10K",
        "event_name": "Autumn 10K",
        "run_days_per_week": 5,
        "long_run_day": "Sunday",
        "sodium_mg_per_hour": 750,
        "plan_start": "2026-08-23",
        "event_date": "2026-10-18",
    }

    with pytest.raises(sqlite3.IntegrityError, match="settings failure"):
        save_planning_settings(db_path, values)

    conn = connect_sqlite(db_path)
    try:
        saved_count = conn.execute(
            "SELECT COUNT(*) FROM app_settings WHERE key LIKE 'plan_%'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert saved_count == 0


def test_interface_settings_persist_and_convert_distance_units(tmp_path):
    db_path = _database(tmp_path)
    values = {
        "unit_system": "Metric",
        "activity_lookback_days": 180,
        "activity_row_limit": 250,
        "activity_default_sport": "running",
        "chart_lookback_days": 730,
        "sync_lookback_days": 30,
        "dashboard_item_limit": 12,
    }

    assert save_interface_settings(db_path, values) == values
    assert interface_settings(db_path) == values
    assert distance_unit("Metric") == "km"
    assert distance_from_km(10, "Metric") == 10.0
    assert distance_unit("Imperial") == "mi"
    assert distance_from_km(10, "Imperial") == 6.21

    with pytest.raises(ValueError, match="between 25 and 5,000"):
        save_interface_settings(db_path, {**values, "activity_row_limit": 10})


def test_data_query_is_read_only_and_bounded(tmp_path):
    db_path = _database(tmp_path)

    rows = run_read_only_query(
        db_path,
        "SELECT activity_id, activity_type FROM activity ORDER BY activity_id",
        limit=1,
    )
    assert rows == [{"activity_id": 1, "activity_type": "running"}]

    for unsafe in (
        "DELETE FROM activity",
        "PRAGMA table_info(activity)",
        "SELECT 1; SELECT 2",
        "WITH removed AS (DELETE FROM activity RETURNING *) SELECT * FROM removed",
    ):
        with pytest.raises(ValueError):
            validate_read_only_sql(unsafe)


def test_sync_controller_survives_page_navigation(tmp_path):
    db_path = _database(tmp_path)

    assert get_sync_job(db_path) is get_sync_job(db_path)


def test_sync_command_does_not_pass_legacy_chrome_flag(tmp_path):
    command = SyncJob(tmp_path / "garmin.db")._command(days=30)

    assert "--visible" in command
    assert "--chrome" not in command


def test_sync_credentials_are_passed_only_in_child_environment(
    monkeypatch, tmp_path
):
    recorded: dict[str, object] = {}

    class FakeProcess:
        pid = 43210

        @staticmethod
        def poll():
            return None

    class FakeProcessTree:
        warning = None

    def fake_popen(command, **kwargs):
        recorded["command"] = command
        recorded["kwargs"] = kwargs
        return FakeProcess()

    job = SyncJob(tmp_path / "garmin.db")
    monkeypatch.setattr(job, "_command", lambda _days: ["sync-helper", "--visible"])
    monkeypatch.setattr(nicegui_data.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        nicegui_data,
        "attach_process_tree",
        lambda _process: FakeProcessTree(),
    )
    monkeypatch.setattr(job, "_start_monitor_locked", lambda *_args: None)

    credentials = GarminCredentials("athlete@example.com", "super-secret")
    job.start(days=30, credentials=credentials)

    assert recorded["command"] == ["sync-helper", "--visible"]
    kwargs = recorded["kwargs"]
    assert kwargs["env"]["GARMIN_EMAIL"] == "athlete@example.com"
    assert kwargs["env"]["GARMIN_PASSWORD"] == "super-secret"
    assert kwargs["stdin"] is nicegui_data.subprocess.DEVNULL
    assert "super-secret" not in job.log_path.read_text(encoding="utf-8")
    assert "athlete@example.com" not in job.log_path.read_text(encoding="utf-8")


def test_accepted_macro_schedule_is_available_on_plan_page(tmp_path):
    db_path = _database(tmp_path)
    save_plan_setting(
        db_path,
        "last_generated_plan",
        {
            "day_plans": [
                {
                    "iso_date": "2026-08-23",
                    "nutrition": {
                        "day_type": "race",
                        "carbohydrate_g_per_kg_min": 5,
                        "carbohydrate_g_per_kg_max": 7,
                        "protein_g_per_kg_min": 1.4,
                        "protein_g_per_kg_max": 1.8,
                        "fat_g_per_kg_min": 0.8,
                        "fat_g_per_kg_max": 1.2,
                        "during_training_carbohydrate_g_per_hour_min": 30,
                        "during_training_carbohydrate_g_per_hour_max": 60,
                        "notes": "Educational range.",
                    },
                }
            ]
        },
    )

    assert nutrition_rows(db_path)[0]["carbohydrate_g_per_kg"] == "5-7"
