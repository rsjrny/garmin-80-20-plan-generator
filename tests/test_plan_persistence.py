from __future__ import annotations

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.plan_persistence import (
    load_plan_settings,
    save_plan_setting,
)


def test_load_plan_settings_returns_defaults(tmp_path):
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()

    settings = load_plan_settings(db_path)

    assert settings["plan_athlete_name"] == "Runner"
    assert settings["plan_distance"] == "50K"
    assert settings["plan_event_name"] == "50K Training Plan"
    assert settings["plan_out_name"] == "Runner_master_workbook.xlsx"


def test_save_plan_setting_round_trips_values(tmp_path):
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()

    save_plan_setting(db_path, "plan_athlete_name", "Casey")
    save_plan_setting(db_path, "plan_distance", "HM")

    settings = load_plan_settings(db_path)
    assert settings["plan_athlete_name"] == "Casey"
    assert settings["plan_distance"] == "HM"
    assert settings["plan_event_name"] == "HM Training Plan"
    assert settings["plan_out_name"] == "Casey_master_workbook.xlsx"

    save_plan_setting(db_path, "plan_event_name", "Spring Half Marathon")
    save_plan_setting(db_path, "plan_out_name", "casey_plan.xlsx")

    updated = load_plan_settings(db_path)
    assert updated["plan_event_name"] == "Spring Half Marathon"
    assert updated["plan_out_name"] == "casey_plan.xlsx"
