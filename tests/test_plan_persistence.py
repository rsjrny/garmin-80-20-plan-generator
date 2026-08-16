from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.exports.forever.calendar_builder import DayPlan
from garmin_data_hub.exports.forever.models import (
    AnalysisSummary,
    AthleteProfile,
    EventProfile,
    Inputs,
)
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.ai_plan_import import (
    CHATGPT_PLAN_CONTRACT,
    CHATGPT_PLAN_VERSION,
    ImportedTrainingPlan,
    ImportedWorkout,
)
from garmin_data_hub.services.plan_persistence import (
    StalePlanWriteError,
    _parse_planned_workout_metrics,
    get_active_plan_sha256,
    get_active_plan_snapshot,
    load_generated_plan,
    load_plan_settings,
    save_generated_plan,
    save_imported_plan,
    save_plan_setting,
)


def _create_db(db_path):
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity "
            "(activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()


def _make_imported_plan(active_hash: str, *, workout_name: str = "Easy run"):
    inputs = Inputs(
        athlete=AthleteProfile(
            athlete_name="Casey",
            age=45,
            sport="run",
            hrmax=185,
            lthr=165,
            sodium_mg_per_hr_hot=700,
            notes="No current limitations",
        ),
        event=EventProfile(
            event_name="Spring 10K",
            distance="10K",
            event_date="2026-01-04",
            start_date="2026-01-02",
            run_days_per_week=4,
        ),
        garmin_files=[],
        output_dir=Path("."),
    )
    analysis = AnalysisSummary(
        hrmax_observed=182,
        hrmax_robust=180,
        lthr_suggested=164,
        active_weeks=8,
        avg_weekly_hours=4.25,
        avg_weekly_miles=22.0,
        z2_fraction=0.81,
        notes="Keep the increase conservative.",
        raw={"source": "test"},
    )
    day_plans = (
        DayPlan(
            iso_date="2026-01-02",
            day="Friday",
            week=1,
            phase="Base",
            flags="",
            workout="Rest Day",
            notes="Rest",
        ),
        DayPlan(
            iso_date="2026-01-03",
            day="Saturday",
            week=1,
            phase="Base",
            flags="",
            workout=workout_name,
            notes="Keep it conversational",
        ),
        DayPlan(
            iso_date="2026-01-04",
            day="Sunday",
            week=1,
            phase="Race",
            flags="RACE",
            workout="Race",
            notes="Controlled effort",
        ),
    )
    workouts = (
        ImportedWorkout(
            iso_date="2026-01-03",
            sport="run",
            phase="Base",
            workout=workout_name,
            notes="Keep it conversational",
            flags=(),
            intensity="easy",
            duration_minutes=42.5,
            distance_km=7.25,
            tss=35.5,
        ),
        ImportedWorkout(
            iso_date="2026-01-04",
            sport="run",
            phase="Race",
            workout="Race",
            notes="Controlled effort",
            flags=("RACE",),
            intensity="race",
            duration_minutes=55.0,
            distance_km=10.0,
            tss=90.0,
        ),
    )
    return ImportedTrainingPlan(
        contract=CHATGPT_PLAN_CONTRACT,
        version=CHATGPT_PLAN_VERSION,
        request_id="1" * 64,
        active_plan_sha256=active_hash,
        primary_sport="run",
        event_sport="run",
        inputs=inputs,
        analysis=analysis,
        workouts=workouts,
        day_plans=day_plans,
        weekly_rows=(
            {
                "Week": 1,
                "Start": "2026-01-02",
                "End": "2026-01-04",
                "Planned workouts": 2,
            },
        ),
        nutrition_guidance=("Practice event fueling during the longer session.",),
        strength_guidance=("Use two light, technique-first sessions.",),
        rationale="The recent load supports a conservative progression.",
        warnings=("Educational guidance only.",),
    )


def test_load_plan_settings_returns_defaults(tmp_path):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)

    settings = load_plan_settings(db_path)

    assert settings["plan_athlete_name"] == "Runner"
    assert settings["plan_distance"] == "50K"
    assert settings["plan_event_name"] == "50K Training Plan"
    assert settings["plan_out_name"] == "Runner_master_workbook.xlsx"


def test_save_plan_setting_round_trips_values(tmp_path):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)

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


@pytest.mark.parametrize(
    ("text", "expected_distance_m", "expected_duration_s"),
    [
        ("Easy run 60 min", None, 3600.0),
        ("Easy run 45–60 min @ Z2", None, 3150.0),
        ("Easy run 45-60 minutes", None, 3150.0),
        ("Long run 2.3 hours", None, 8280.0),
        ("Easy run 5 mi in 45 min", 8046.7, 2700.0),
        ("Aerobic run 10 km, 60 minutes", 10000.0, 3600.0),
        ("Trail run 12 kilometres, 90 mins", 12000.0, 5400.0),
    ],
)
def test_planned_workout_metric_parser_uses_complete_unit_names(
    text, expected_distance_m, expected_duration_s
):
    distance_m, duration_s = _parse_planned_workout_metrics(text)

    if expected_distance_m is None:
        assert distance_m is None
    else:
        assert distance_m == pytest.approx(expected_distance_m)
    assert duration_s == pytest.approx(expected_duration_s)


def test_save_generated_plan_does_not_store_minutes_as_miles(tmp_path):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)
    plan = _make_imported_plan(get_active_plan_sha256(db_path))
    day_plan = DayPlan(
        iso_date="2026-01-03",
        day="Saturday",
        week=1,
        phase="Base",
        flags="",
        workout="Easy Run",
        notes="45–60 min @ Z2. Conversational pace.",
    )

    save_generated_plan(
        db_path,
        plan.inputs,
        plan.analysis,
        [day_plan],
        [],
    )

    conn = connect_sqlite(db_path)
    try:
        stored = conn.execute(
            """
            SELECT planned_distance_m, planned_duration_s
            FROM planned_workout
            WHERE scheduled_date = '2026-01-03'
            """
        ).fetchone()
    finally:
        conn.close()

    assert stored["planned_distance_m"] is None
    assert stored["planned_duration_s"] == 3150.0


def test_active_plan_hash_is_semantic_and_insertion_order_independent(tmp_path):
    first_path = tmp_path / "first.db"
    second_path = tmp_path / "second.db"
    rows = [
        (
            "2026-01-03",
            "Easy run",
            "Conversational",
            7250.0,
            2550.0,
            35.5,
            '{"b":2,"a":1}',
        ),
        ("2026-01-03", "Mobility", "Short routine", None, 900.0, None, None),
    ]
    for db_path, ordered_rows in ((first_path, rows), (second_path, rows[::-1])):
        _create_db(db_path)
        conn = connect_sqlite(db_path)
        try:
            conn.executemany(
                """
                INSERT INTO planned_workout(
                    scheduled_date, workout_name, description,
                    planned_distance_m, planned_duration_s, planned_tss,
                    structure_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ordered_rows,
            )
            conn.commit()
        finally:
            conn.close()

    assert get_active_plan_sha256(first_path) == get_active_plan_sha256(second_path)

    conn = connect_sqlite(second_path)
    try:
        conn.execute(
            "UPDATE planned_workout SET description = 'Changed' WHERE workout_name = 'Mobility'"
        )
        conn.commit()
    finally:
        conn.close()

    assert get_active_plan_sha256(first_path) != get_active_plan_sha256(second_path)


def test_save_imported_plan_replaces_full_day_plan_window_and_updates_legacy_data(
    tmp_path,
):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)
    conn = connect_sqlite(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss,
                structure_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ("2026-01-01", "Keep before", "Outside", None, None, None, None),
                (
                    "2026-01-02",
                    "Old workout on new rest day",
                    "Must be removed",
                    5000.0,
                    1800.0,
                    30.0,
                    None,
                ),
                (
                    "2026-01-03",
                    "Old in range",
                    "Must be replaced",
                    6000.0,
                    2100.0,
                    40.0,
                    None,
                ),
                ("2026-01-05", "Keep after", "Outside", None, None, None, None),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    expected_hash = get_active_plan_sha256(db_path)
    plan = _make_imported_plan(expected_hash)
    result = save_imported_plan(db_path, plan)

    assert result.applied
    assert not result.duplicate
    assert result.replace_start_date == "2026-01-02"
    assert result.replace_end_date == "2026-01-04"
    assert result.workout_count == 2
    assert result.replaced_workout_count == 2
    assert result.active_plan_sha256 == get_active_plan_sha256(db_path)

    conn = connect_sqlite(db_path)
    try:
        rows = conn.execute(
            """
            SELECT scheduled_date, workout_name, planned_distance_m,
                   planned_duration_s, planned_tss, structure_json
            FROM planned_workout
            ORDER BY scheduled_date
            """
        ).fetchall()
        history = conn.execute(
            "SELECT * FROM plan_import_history WHERE plan_import_id = ?",
            (result.plan_import_id,),
        ).fetchone()
    finally:
        conn.close()

    assert [row["scheduled_date"] for row in rows] == [
        "2026-01-01",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
    ]
    imported = rows[1]
    assert imported["workout_name"] == "Easy run"
    assert imported["planned_distance_m"] == 7250.0
    assert imported["planned_duration_s"] == 2550.0
    assert imported["planned_tss"] == 35.5
    structure = json.loads(imported["structure_json"])
    assert structure["source"] == "chatgpt_manual_upload"
    assert structure["request_id"] == plan.request_id
    assert structure["workout"]["duration_minutes"] == 42.5
    assert structure["workout"]["distance_km"] == 7.25

    assert history["source"] == "chatgpt_manual_upload"
    assert history["plan_name"] == "Spring 10K"
    assert json.loads(history["payload_json"]) == plan.to_dict()
    assert json.loads(history["validation_warnings_json"]) == list(plan.warnings)
    previous = json.loads(history["previous_plan_json"])
    assert previous["active_plan_sha256"] == expected_hash
    assert {
        row["scheduled_date"]
        for row in previous["active_plan"]["planned_workouts"]
    } == {"2026-01-01", "2026-01-02", "2026-01-03", "2026-01-05"}

    inputs, analysis, day_plans, weekly_rows = load_generated_plan(db_path)
    assert inputs["athlete"]["athlete_name"] == "Casey"
    assert inputs["athlete"]["sodium"] == 700
    assert analysis["nutrition_guidance"] == list(plan.nutrition_guidance)
    assert analysis["strength_guidance"] == list(plan.strength_guidance)
    assert analysis["rationale"] == plan.rationale
    assert analysis["provenance"]["request_id"] == plan.request_id
    assert analysis["provenance"]["active_plan_sha256_after"] == result.active_plan_sha256
    assert [item["iso_date"] for item in day_plans] == [
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
    ]
    assert weekly_rows == list(plan.weekly_rows)


def test_save_imported_plan_preserves_cached_days_outside_replacement_window(
    tmp_path,
):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)
    previous_plan = {
        "day_plans": [
            {
                "iso_date": "2026-01-05",
                "day": "Monday",
                "week": 1,
                "phase": "Recovery",
                "flags": "",
                "workout": "Keep after",
                "notes": "Future cached day",
            },
            {
                "iso_date": "2026-01-03",
                "day": "Saturday",
                "week": 1,
                "phase": "Base",
                "flags": "",
                "workout": "Replace this cached workout",
                "notes": "Inside imported window",
            },
            {
                "iso_date": "2026-01-01",
                "day": "Thursday",
                "week": 1,
                "phase": "Base",
                "flags": "",
                "workout": "Keep before",
                "notes": "Completed cached day",
            },
        ],
        "weekly_rows": [],
        "analysis": {"notes": "Previous analysis"},
        "inputs": {},
    }
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO app_settings(key, value) VALUES (?, ?)",
            ("last_generated_plan", json.dumps(json.dumps(previous_plan))),
        )
        conn.commit()
    finally:
        conn.close()

    plan = _make_imported_plan(get_active_plan_sha256(db_path))
    save_imported_plan(db_path, plan)

    _, _, day_plans, _ = load_generated_plan(db_path)
    assert [day["iso_date"] for day in day_plans] == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
    ]
    by_date = {day["iso_date"]: day for day in day_plans}
    assert by_date["2026-01-01"] == previous_plan["day_plans"][2]
    assert by_date["2026-01-05"] == previous_plan["day_plans"][0]
    assert by_date["2026-01-03"]["workout"] == "Easy run"
    assert by_date["2026-01-03"]["notes"] == "Keep it conversational"


def test_save_imported_plan_rejects_stale_hash_without_writes(tmp_path):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO planned_workout(scheduled_date, workout_name) VALUES (?, ?)",
            ("2026-01-03", "Original"),
        )
        conn.execute(
            "INSERT INTO app_settings(key, value) VALUES (?, ?)",
            ("last_generated_plan", json.dumps("original-plan-value")),
        )
        conn.commit()
    finally:
        conn.close()

    stale_hash = get_active_plan_sha256(db_path)
    plan = _make_imported_plan(stale_hash)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "UPDATE planned_workout SET workout_name = 'Changed after export'"
        )
        conn.commit()
        raw_setting_before = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'last_generated_plan'"
        ).fetchone()[0]
    finally:
        conn.close()
    snapshot_before = get_active_plan_snapshot(db_path)

    with pytest.raises(StalePlanWriteError) as caught:
        save_imported_plan(db_path, plan)

    assert caught.value.expected_sha256 == stale_hash
    assert caught.value.current_sha256 == get_active_plan_sha256(db_path)
    assert get_active_plan_snapshot(db_path) == snapshot_before
    conn = connect_sqlite(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM plan_import_history").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT value FROM app_settings WHERE key = 'last_generated_plan'"
            ).fetchone()[0]
            == raw_setting_before
        )
    finally:
        conn.close()


def test_save_imported_plan_duplicate_is_noop_even_after_active_plan_changes(tmp_path):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)
    plan = _make_imported_plan(get_active_plan_sha256(db_path))
    first = save_imported_plan(db_path, plan)

    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO planned_workout(scheduled_date, workout_name) VALUES (?, ?)",
            ("2026-02-01", "Later manual addition"),
        )
        conn.commit()
        setting_before = conn.execute(
            "SELECT value, updated_at FROM app_settings WHERE key = 'last_generated_plan'"
        ).fetchone()
    finally:
        conn.close()
    snapshot_before = get_active_plan_snapshot(db_path)

    duplicate = save_imported_plan(db_path, plan)

    assert duplicate.duplicate
    assert not duplicate.applied
    assert duplicate.plan_import_id == first.plan_import_id
    assert duplicate.content_sha256 == first.content_sha256
    assert duplicate.replaced_workout_count == 0
    assert duplicate.active_plan_sha256 == get_active_plan_sha256(db_path)
    assert get_active_plan_snapshot(db_path) == snapshot_before
    conn = connect_sqlite(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM plan_import_history").fetchone()[0] == 1
        setting_after = conn.execute(
            "SELECT value, updated_at FROM app_settings WHERE key = 'last_generated_plan'"
        ).fetchone()
        assert tuple(setting_after) == tuple(setting_before)
    finally:
        conn.close()


def test_save_imported_plan_rolls_back_delete_when_an_insert_fails(tmp_path):
    db_path = tmp_path / "garmin.db"
    _create_db(db_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO planned_workout(scheduled_date, workout_name) VALUES (?, ?)",
            ("2026-01-03", "Original"),
        )
        conn.execute(
            """
            CREATE TRIGGER reject_imported_workout
            BEFORE INSERT ON planned_workout
            WHEN NEW.workout_name = 'Explode'
            BEGIN
                SELECT RAISE(ABORT, 'test insert failure');
            END
            """
        )
        conn.commit()
    finally:
        conn.close()

    snapshot_before = get_active_plan_snapshot(db_path)
    plan = _make_imported_plan(
        get_active_plan_sha256(db_path), workout_name="Explode"
    )

    with pytest.raises(sqlite3.IntegrityError, match="test insert failure"):
        save_imported_plan(db_path, plan)

    assert get_active_plan_snapshot(db_path) == snapshot_before
    conn = connect_sqlite(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM plan_import_history").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM app_settings WHERE key = 'last_generated_plan'"
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()
