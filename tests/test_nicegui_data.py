from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
import pandas as pd

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
    dashboard_data,
    distance_from_km,
    distance_unit,
    get_sync_job,
    interface_settings,
    list_activities,
    nutrition_rows,
    pace_minutes_from_mps,
    pace_text_from_mps,
    pace_unit,
    planning_settings,
    run_read_only_query,
    save_interface_settings,
    save_planning_settings,
    speed_from_mps,
    speed_unit,
    validate_read_only_sql,
)
from garmin_data_hub.ui_nicegui.pages import (
    CHART_OPTIONS,
    TRACK_MAP_FALLBACK_TILE_URL,
    TRACK_MAP_TILE_ATTRIBUTION,
    TRACK_MAP_TILE_URL,
    TRANSPARENT_TILE_URL,
    _chart_ids_from_labels,
    _climb_summary_rows,
    _downhill_summary_rows,
    _elevation_profile_figure,
    _fit_leaflet_route,
    _gradient_distribution_figure,
    _gradient_distribution_rows,
    _show_activity_detail,
    _split_rows,
    _terrain_summary,
    _training_chart_figures,
    _use_track_map_tiles,
)


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


def test_route_map_replaces_default_osm_tile_endpoint():
    class RouteMap:
        def __init__(self):
            self.layers_cleared = False
            self.tile_layers = []

        def clear_layers(self):
            self.layers_cleared = True

        def tile_layer(self, *, url_template, options):
            self.tile_layers.append(
                {"url_template": url_template, "options": options}
            )

    route_map = RouteMap()

    _use_track_map_tiles(route_map)

    assert route_map.layers_cleared is True
    assert "openstreetmap.org" not in TRACK_MAP_TILE_URL
    assert "tile.osm.org" not in TRACK_MAP_TILE_URL
    assert "openstreetmap.org" not in TRACK_MAP_FALLBACK_TILE_URL
    assert "tile.osm.org" not in TRACK_MAP_FALLBACK_TILE_URL
    assert route_map.tile_layers == [
        {
            "url_template": TRACK_MAP_FALLBACK_TILE_URL,
            "options": {
                "attribution": TRACK_MAP_TILE_ATTRIBUTION,
                "maxZoom": 19,
                "zIndex": 1,
            },
        },
        {
            "url_template": TRACK_MAP_TILE_URL,
            "options": {
                "attribution": TRACK_MAP_TILE_ATTRIBUTION,
                "errorTileUrl": TRANSPARENT_TILE_URL,
                "maxZoom": 19,
                "zIndex": 2,
            },
        }
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
        "training_method": "maffetone",
        "output_directory": str(tmp_path / "exports"),
        "output_filename": "autumn-20.xlsx",
    }

    save_planning_settings(db_path, values)
    saved = planning_settings(db_path)

    assert {key: saved[key] for key in values} == values


def test_plan_configuration_defaults_to_eighty_twenty(tmp_path):
    db_path = _database(tmp_path)

    saved = planning_settings(db_path)

    assert saved["training_method"] == "eighty_twenty"


def test_plan_configuration_rejects_unknown_training_method(tmp_path):
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
        "training_method": "mystery",
    }

    with pytest.raises(ValueError, match="Unsupported training philosophy"):
        save_planning_settings(db_path, values)


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
        "activity_velocity_display": "Speed",
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
    assert speed_unit("Metric") == "km/h"
    assert speed_from_mps(2.78, "Metric") == 10.01
    assert speed_unit("Imperial") == "mph"
    assert speed_from_mps(2.78, "Imperial") == 6.22
    assert pace_unit("Metric") == "min/km"
    assert pace_text_from_mps(2.78, "Metric") == "6:00 min/km"
    assert pace_unit("Imperial") == "min/mi"
    assert pace_text_from_mps(2.78, "Imperial") == "9:39 min/mi"
    assert pace_minutes_from_mps(0, "Imperial") is None

    with pytest.raises(ValueError, match="between 25 and 5,000"):
        save_interface_settings(db_path, {**values, "activity_row_limit": 10})

    with pytest.raises(ValueError, match="Pace or Speed"):
        save_interface_settings(
            db_path,
            {**values, "activity_velocity_display": "Meters per second"},
        )


def test_dashboard_recent_activities_include_speed_source(tmp_path):
    db_path = _database(tmp_path)

    recent = dashboard_data(db_path)["recent"]

    assert recent[0]["speed_mps"] == 2.78


def test_split_rows_show_configured_pace_or_speed():
    rows = [{"split_number": 1, "distance_meters": 1609.344, "speed_mps": 2.78}]

    pace_rows = _split_rows(rows, "Imperial", "Pace")
    speed_rows = _split_rows(rows, "Imperial", "Speed")

    assert pace_rows == [
        {"split_number": 1, "distance_mi": 1.0, "split_pace": "9:39 min/mi"}
    ]
    assert speed_rows == [
        {"split_number": 1, "distance_mi": 1.0, "split_speed_mph": 6.22}
    ]


def test_elevation_profile_identifies_climbs_with_grade_scores():
    points = pd.DataFrame(
        [
            {"distance_m": 0, "altitude_m": 100, "speed_mps": 2.4},
            {"distance_m": 100, "altitude_m": 106, "speed_mps": 2.5},
            {"distance_m": 200, "altitude_m": 113, "speed_mps": 2.3},
            {"distance_m": 300, "altitude_m": 121, "speed_mps": 2.2},
            {"distance_m": 400, "altitude_m": 122, "speed_mps": 2.6},
            {"distance_m": 500, "altitude_m": 116, "speed_mps": 2.8},
            {"distance_m": 600, "altitude_m": 116, "speed_mps": 2.9},
            {"distance_m": 700, "altitude_m": 122, "speed_mps": 2.4},
            {"distance_m": 800, "altitude_m": 131, "speed_mps": 2.2},
            {"distance_m": 900, "altitude_m": 143, "speed_mps": 2.0},
            {"distance_m": 1000, "altitude_m": 143, "speed_mps": 2.5},
        ]
    )

    climbs = _climb_summary_rows(points, "Metric")
    downhill = _downhill_summary_rows(points, "Metric")
    summary = _terrain_summary(points, climbs, "Metric")

    assert climbs == [
        {
            "climb": 1,
            "start_km": 0.0,
            "end_km": 0.4,
            "distance_km": 0.4,
            "gain_m": 18.0,
            "avg_grade_pct": 4.5,
            "category": "Unclassified",
            "fiets": 0.2,
            "duration": "2:47",
            "pace": "6:58 min/km",
            "vam_m_per_h": 387,
        },
        {
            "climb": 2,
            "start_km": 0.6,
            "end_km": 1.0,
            "distance_km": 0.4,
            "gain_m": 27.0,
            "avg_grade_pct": 6.8,
            "category": "5",
            "fiets": 0.46,
            "duration": "2:57",
            "pace": "7:23 min/km",
            "vam_m_per_h": 549,
        },
    ]
    assert downhill == [
        {
            "climb": 1,
            "start_km": 0.4,
            "end_km": 0.6,
            "distance_km": 0.2,
            "avg_grade_pct": -2.5,
            "category": "Unclassified",
            "fiets": 0.03,
            "duration": "1:10",
            "pace": "5:51 min/km",
            "vam_m_per_h": 256,
            "drop_m": 5.0,
        },
    ]
    assert summary == {
        "gdh_terrain_score": 4.3,
        "distance_km": 1.0,
        "gain_m": 33.0,
        "hilly_pct": 90,
        "flat_pct": 10,
    }
    figure = _elevation_profile_figure(points, climbs, "Metric")
    assert figure is not None
    assert figure.layout.title.text == "GDH terrain score"
    assert len(figure.layout.shapes) == 2
    assert not figure.layout.annotations
    distribution_rows = _gradient_distribution_rows(points, "Metric")
    assert [row["gradient"] for row in distribution_rows] == [
        "Flat",
        "Ascending > +2%",
        "Descending < -2%",
    ]
    distribution_figure = _gradient_distribution_figure(points, "Metric")
    assert distribution_figure is not None
    assert distribution_figure.layout.title.text == "Gradient distribution"
    assert len(distribution_figure.data[0].x) == 25
    assert distribution_figure.data[0].x[0] == "-12%"
    assert distribution_figure.data[0].x[-1] == "12%"
    assert distribution_figure.layout.plot_bgcolor == "white"
    assert distribution_figure.layout.xaxis.tickvals[0] == "-12%"


def test_chart_selection_labels_map_to_stable_ids():
    labels = [label for _, label in CHART_OPTIONS]

    assert _chart_ids_from_labels(labels[:2]) == {
        "activity_distribution",
        "weekly_distance",
    }
    assert _chart_ids_from_labels([]) == set()
    assert _chart_ids_from_labels("Weekly HR zones") == {"weekly_hr_zones"}


def test_training_chart_figures_include_selected_new_charts():
    frame = pd.DataFrame(
        [
            {
                "start_time_utc": "2026-08-17T10:00:00",
                "sport": "running",
                "total_distance_m": 10000,
                "total_elapsed_s": 3600,
                "total_ascent_m": 120,
                "avg_hr_bpm": 145,
                "avg_speed_mps": 2.78,
                "tss": 70,
                "zone_1_s": 600,
                "zone_2_s": 2400,
                "zone_3_s": 600,
                "zone_4_s": 0,
                "zone_5_s": 0,
                "aerobic_decoupling_pct": 3.2,
                "hr_drift_pct": 2.5,
            },
            {
                "start_time_utc": "2026-08-20T10:00:00",
                "sport": "running",
                "total_distance_m": 5000,
                "total_elapsed_s": 1800,
                "total_ascent_m": 40,
                "avg_hr_bpm": 140,
                "avg_speed_mps": 2.9,
                "tss": 35,
                "zone_1_s": 300,
                "zone_2_s": 1200,
                "zone_3_s": 300,
                "zone_4_s": 0,
                "zone_5_s": 0,
                "aerobic_decoupling_pct": 1.8,
                "hr_drift_pct": 1.2,
            },
        ]
    )

    figures = _training_chart_figures(
        frame,
        {
            "average_velocity",
            "weekly_hr_zones",
            "weekly_elevation",
            "longest_activity",
            "load_vs_duration",
            "drift_decoupling",
        },
        unit="mi",
        unit_system="Imperial",
        velocity_display="Pace",
    )

    titles = [figure.layout.title.text for figure in figures]
    assert titles == [
        "Average pace (min/mi)",
        "Weekly HR zones (hours)",
        "Weekly elevation gain (ft)",
        "Longest activity by week (mi)",
        "Load vs duration",
        "Drift and decoupling (%)",
    ]
    assert figures[0].layout.yaxis.autorange == "reversed"


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
