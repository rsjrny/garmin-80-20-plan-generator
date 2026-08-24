from __future__ import annotations

from pathlib import Path

from openpyxl import load_workbook

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.exports.master_export import (
    generate_master_workbook,
    generate_plan_data,
)
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.plan_persistence import save_generated_plan
from garmin_data_hub.ui_nicegui import pages
from garmin_data_hub.ui_nicegui.data import plan_rows


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return db_path


def _baseline_data():
    return generate_plan_data(
        athlete_name="Offline Athlete",
        age=42,
        lthr=165,
        hrmax=190,
        sodium_mg_per_hr_hot=700,
        event_name="Autumn 10K",
        distance="10K",
        start_date_iso="2026-08-24",
        event_date_iso="2026-09-06",
        run_days_per_week=4,
        long_run_day="Saturday",
    )


def test_plan_page_offers_offline_workbook_and_codex_generation_paths():
    source = Path(pages.__file__).read_text(encoding="utf-8")
    plan_page = source.split('    @ui.page("/plan")', maxsplit=1)[1].split(
        '    @ui.page("/compliance")', maxsplit=1
    )[0]

    assert '"Generate offline baseline"' in plan_page
    assert '"Generate baseline + workbook"' in plan_page
    assert '"Open Codex Coach"' in plan_page
    assert "@ui.refreshable" in plan_page
    assert ".refresh()" in plan_page
    assert 'state["confirmation_pending"]' in plan_page
    assert "expected_active_plan_sha256=expected_plan_sha256" in plan_page
    assert 'state["last_workbook"] = None' in plan_page


def test_offline_baseline_persists_rows_consumed_by_plan_schedule(tmp_path):
    db_path = _database(tmp_path)
    inputs, analysis, day_plans, weekly_rows = _baseline_data()

    save_generated_plan(db_path, inputs, analysis, day_plans, weekly_rows)

    schedule = plan_rows(db_path)
    expected_workouts = [day for day in day_plans if day.workout]
    assert len(schedule) == len(expected_workouts)
    assert schedule[0]["date"] == expected_workouts[0].iso_date
    assert schedule[-1]["date"] == expected_workouts[-1].iso_date
    assert schedule[-1]["intensity"] == "race"
    assert "run" in {row["sport"] for row in schedule}
    assert all(row["workout"] for row in schedule)


def test_offline_workbook_contains_generated_calendar(tmp_path):
    output_path = tmp_path / "Offline_Athlete_master_workbook.xlsx"

    result = generate_master_workbook(
        athlete_name="Offline Athlete",
        age=42,
        lthr=165,
        hrmax=190,
        sodium_mg_per_hr_hot=700,
        event_name="Autumn 10K",
        distance="10K",
        start_date_iso="2026-08-24",
        event_date_iso="2026-09-06",
        run_days_per_week=4,
        long_run_day="Saturday",
        out_path=output_path,
    )

    assert result == output_path
    assert output_path.is_file()
    workbook = load_workbook(output_path, read_only=True, data_only=True)
    try:
        assert workbook.sheetnames == [
            "Calendar",
            "Metrics",
            "Garmin Analysis",
            "Forever Plan",
            "Workout Library",
            "Nutrition",
        ]
        calendar = workbook["Calendar"]
        assert calendar.max_row == 15
        assert calendar["B2"].value == "2026-08-24"
        assert calendar[f"B{calendar.max_row}"].value == "2026-09-06"
    finally:
        workbook.close()
