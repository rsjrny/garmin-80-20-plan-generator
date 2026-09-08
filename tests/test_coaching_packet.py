from __future__ import annotations

import json

import pytest

from garmin_data_hub.db import queries as db_queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.ai_plan_import import (
    MAX_WORKOUTS,
    MAX_WORKOUTS_PER_DAY,
    parse_chatgpt_plan,
)
from garmin_data_hub.services.coaching_packet import (
    COACHING_PACKET_SCHEMA_VERSION,
    TRAINING_PLAN_UPDATE_CONTRACT,
    TRAINING_PLAN_UPDATE_VERSION,
    build_coaching_packet,
    coaching_packet_to_json,
    get_active_plan_sha256,
    write_coaching_packet,
)
from garmin_data_hub.services.plan_persistence import (
    get_active_plan_sha256 as get_persistence_active_plan_sha256,
)


def _create_packet_db(tmp_path):
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    conn.execute(
        """
        CREATE TABLE activity (
            activity_id INTEGER PRIMARY KEY,
            start_time_gmt TEXT,
            activity_type TEXT,
            distance_meters REAL,
            elapsed_duration_seconds REAL,
            average_hr REAL,
            max_hr REAL,
            avg_power REAL,
            norm_power REAL,
            training_stress_score REAL,
            start_latitude REAL,
            start_longitude REAL
        )
        """
    )
    apply_schema(conn, schema_sql_path())
    return db_path, conn


def test_build_packet_is_deterministic_and_privacy_minimized(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    try:
        conn.executemany(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, activity_type, distance_meters,
                elapsed_duration_seconds, average_hr, max_hr, avg_power,
                norm_power, training_stress_score, start_latitude, start_longitude
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    1,
                    "2026-08-15T06:12:34Z",
                    "running",
                    10000,
                    3600,
                    145,
                    172,
                    250,
                    270,
                    None,
                    41.123456,
                    -72.654321,
                ),
                (
                    2,
                    "2026-08-10T07:45:00Z",
                    "running",
                    5000,
                    1800,
                    135,
                    155,
                    None,
                    None,
                    42,
                    40.111111,
                    -71.222222,
                ),
                (
                    3,
                    "2026-01-01T09:00:00Z",
                    "running",
                    1000,
                    300,
                    100,
                    120,
                    None,
                    None,
                    5,
                    None,
                    None,
                ),
            ],
        )
        conn.execute(
            """
            UPDATE athlete_profile
            SET hrmax_calc=180, lthr_calc=165, ftp_calc=280,
                hrmax_override=185, resting_hr=48
            WHERE profile_id=1
            """
        )
        conn.execute(
            """
            INSERT INTO activity_metrics(
                activity_id, tss, aerobic_decoupling_pct,
                zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s
            ) VALUES (1, 75, 4.2, 300, 1800, 900, 300, 0)
            """
        )
        settings = {
            "plan_athlete_name": "Private Person",
            "plan_event_name": "Secret City Race",
            "plan_out_dir": "C:/Users/private/Documents",
            "plan_age": 44,
            "plan_run_days": 5,
            "plan_distance": "50K",
            "plan_long_run_day": "Saturday",
            "plan_event_date": "2026-11-07",
            "plan_start_date": "2026-08-17",
            "plan_sodium": 800,
        }
        for key, value in settings.items():
            db_queries.set_setting(conn, key, value)
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss
            ) VALUES ('2026-08-18', 'Easy run', 'Private free-text note',
                      8000, 3000, 55)
            """
        )
        conn.commit()
    finally:
        conn.close()

    first = build_coaching_packet(
        db_path,
        as_of="2026-08-16",
        lookback_days=14,
        recent_activity_limit=10,
        plan_horizon_days=14,
        preferences={
            "injuries_or_limitations": "Avoid deep knee flexion",
            "strength_equipment": "Dumbbells and bands",
            "strength_experience": "Beginner",
            "dietary_preferences": "Vegetarian",
            "allergies_or_intolerances": "Peanuts",
            "gi_considerations": "Sensitive during long runs",
            "scheduling_notes": "No training on Wednesday evenings",
        },
    )
    second = build_coaching_packet(
        db_path,
        as_of="2026-08-16",
        lookback_days=14,
        recent_activity_limit=10,
        plan_horizon_days=14,
        preferences={
            "injuries_or_limitations": "Avoid deep knee flexion",
            "strength_equipment": "Dumbbells and bands",
            "strength_experience": "Beginner",
            "dietary_preferences": "Vegetarian",
            "allergies_or_intolerances": "Peanuts",
            "gi_considerations": "Sensitive during long runs",
            "scheduling_notes": "No training on Wednesday evenings",
        },
    )

    assert first == second
    assert coaching_packet_to_json(first) == coaching_packet_to_json(second)
    assert first["schema_version"] == COACHING_PACKET_SCHEMA_VERSION
    assert len(first["request_id"]) == 64
    assert len(first["active_plan_sha256"]) == 64
    assert first["active_plan_sha256"] == get_active_plan_sha256(db_path)

    context = first["context"]
    assert context["athlete"]["age"] == 44
    assert context["athlete"]["hrmax_bpm"] == 185
    assert context["athlete"]["sodium_mg_per_hour"] == 800
    assert (
        context["training_constraints"]["max_heart_rate_source"]
        == "athlete_override"
    )
    assert context["training_constraints"]["local_acceptance_policy"] == {
        "max_sessions_per_day": 3,
        "max_run_days_per_week": 5,
        "max_hard_or_race_sessions_per_week": 2,
        "max_strength_sessions_per_week": 3,
        "min_strength_sessions_per_full_base_build_week": 1,
        "max_weekly_run_distance_increase_fraction": 0.1,
        "target_easy_endurance_duration_fraction": 0.8,
        "consecutive_hard_days_allowed": False,
        "rest_and_active_sessions_same_day_allowed": False,
    }
    assert context["event"]["start_date"] == "2026-08-16"
    assert context["event"]["event_date"] == "2026-08-29"
    assert context["training_history"]["summary"]["activities"] == 2
    assert context["training_history"]["summary"]["training_stress_score"] == 117
    assert [
        row["activity_id"]
        for row in context["training_history"]["recent_activities"]
    ] == [1, 2]
    assert context["runner_profile"] == {
        "age": 44,
        "experience_level": "limited_recent_load",
        "experience_basis": (
            "Derived from the selected Garmin lookback summary and recent "
            "date-only activities; do not treat as lifetime athletic history."
        ),
        "history_window_days": 14,
        "active_days_in_window": 2,
        "recent_activity_count": 2,
        "recent_run_activity_count": 2,
        "weekly_average_training_hours": 0.75,
        "weekly_average_run_distance_km": 7.5,
        "weekly_average_run_hours": 0.75,
        "longest_recent_run": {
            "date": "2026-08-15",
            "distance_km": 10.0,
            "duration_min": 60.0,
        },
        "easy_hr_zone_fraction": 0.64,
        "explicit_strength_experience": "Beginner",
        "explicit_limitations": "Avoid deep knee flexion",
        "data_quality_flags": [],
    }
    assert context["preferences"]["strength_equipment"] == "Dumbbells and bands"
    assert context["current_plan"]["sessions"] == [
        {
            "date": "2026-08-18",
            "sport": None,
            "phase": None,
            "workout": "Easy run",
            "flags": [],
            "intensity": None,
            "duration_minutes": 50.0,
            "distance_km": 8.0,
            "tss": 55.0,
        }
    ]

    rendered = coaching_packet_to_json(first)
    for private_value in (
        "Private Person",
        "Secret City Race",
        "Private free-text note",
        "41.123456",
        "-72.654321",
        "06:12:34",
        "C:/Users/private",
    ):
        assert private_value not in rendered


def test_packet_contains_copyable_prompt_and_explicit_output_schema(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    conn.close()

    packet = build_coaching_packet(db_path, as_of="2026-08-16")
    chatgpt = packet["chatgpt"]
    output_schema = chatgpt["requested_output_schema"]

    assert packet["request_id"] in chatgpt["copyable_prompt"]
    assert packet["active_plan_sha256"] in chatgpt["copyable_prompt"]
    assert "Return only one JSON object" in chatgpt["copyable_prompt"]
    assert "context.runner_profile" in chatgpt["copyable_prompt"]
    assert "uploaded Garmin coaching packet" not in chatgpt["copyable_prompt"]
    assert "chatgpt.requested_output_schema" not in chatgpt["copyable_prompt"]
    assert output_schema["additionalProperties"] is False
    assert (
        output_schema["properties"]["contract"]["const"]
        == TRAINING_PLAN_UPDATE_CONTRACT
    )
    assert (
        output_schema["properties"]["version"]["const"]
        == TRAINING_PLAN_UPDATE_VERSION
    )
    assert {
        "contract",
        "version",
        "request_id",
        "active_plan_sha256",
        "athlete",
        "event",
        "analysis",
        "workouts",
        "nutrition_targets",
        "rationale",
    } == set(output_schema["required"])
    update_item = output_schema["properties"]["workouts"]["items"]
    assert update_item["additionalProperties"] is False
    assert {
        "date",
        "sport",
        "phase",
        "workout",
        "notes",
        "flags",
        "intensity",
        "duration_minutes",
        "distance_km",
        "tss",
    } == set(update_item["required"])
    assert output_schema["properties"]["strength_guidance"]["type"] == "array"
    assert output_schema["properties"]["nutrition_guidance"]["type"] == "array"
    assert output_schema["properties"]["nutrition_targets"]["type"] == "array"
    assert "food-agnostic" in chatgpt["copyable_prompt"]
    assert output_schema["properties"]["workouts"]["maxItems"] == MAX_WORKOUTS
    assert (
        f"at most {MAX_WORKOUTS_PER_DAY} distinct sessions per date"
        in chatgpt["copyable_prompt"]
    )
    assert "short plain-language change summary" in chatgpt["copyable_prompt"]

    parsed = json.loads(coaching_packet_to_json(packet))
    assert parsed == packet


def test_write_packet_and_validate_public_options(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    conn.close()
    packet = build_coaching_packet(db_path, as_of="2026-08-16")

    output_path = tmp_path / "exports" / "coaching_packet.json"
    resolved = write_coaching_packet(packet, output_path)

    assert resolved == output_path.resolve()
    assert json.loads(output_path.read_text(encoding="utf-8")) == packet

    with pytest.raises(ValueError, match="lookback_days"):
        build_coaching_packet(db_path, as_of="2026-08-16", lookback_days=0)
    with pytest.raises(ValueError, match="recent_activity_limit"):
        build_coaching_packet(
            db_path, as_of="2026-08-16", recent_activity_limit=501
        )
    with pytest.raises(ValueError, match="ISO date"):
        build_coaching_packet(db_path, as_of="08/16/2026")
    with pytest.raises(ValueError, match="end in .json"):
        write_coaching_packet(packet, tmp_path / "packet.txt")
    with pytest.raises(ValueError, match="unsupported preference"):
        build_coaching_packet(
            db_path,
            as_of="2026-08-16",
            preferences={"athlete_name": "Do not include me"},
        )
    with pytest.raises(ValueError, match="unsupported plan context"):
        build_coaching_packet(
            db_path,
            as_of="2026-08-16",
            plan_context={"output_path": "C:/private"},
        )
    with pytest.raises(FileNotFoundError):
        build_coaching_packet(tmp_path / "missing.db", as_of="2026-08-16")


def test_live_plan_context_overrides_unpersisted_defaults(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    conn.close()

    packet = build_coaching_packet(
        db_path,
        as_of="2026-08-16",
        plan_horizon_days=7,
        plan_context={
            "age": 57,
            "run_days_per_week": 4,
            "distance": "HM",
            "long_run_day": "Sunday",
            "sodium_mg_per_hour": 750,
        },
    )

    assert packet["context"]["athlete"] == {
        "name": "Athlete",
        "age": 57,
        "primary_sport": "run",
        "hrmax_bpm": None,
        "lthr_bpm": None,
        "sodium_mg_per_hour": 750,
        "notes": "",
    }
    assert packet["context"]["event"] == {
        "name": "Training Event",
        "sport": "run",
        "distance": "HM",
        "start_date": "2026-08-16",
        "event_date": "2026-08-22",
        "run_days_per_week": 4,
    }
    assert (
        packet["context"]["training_constraints"]["preferred_long_session_day"]
        == "Sunday"
    )


def test_packet_can_be_built_before_first_garmin_sync(tmp_path):
    db_path = tmp_path / "garmin.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()

    packet = build_coaching_packet(
        db_path,
        as_of="2026-08-16",
        plan_context={
            "age": 50,
            "run_days_per_week": 5,
            "distance": "10K",
            "long_run_day": "Saturday",
        },
    )

    history = packet["context"]["training_history"]
    assert history["summary"]["activities"] == 0
    assert history["recent_activities"] == []
    assert (
        history["data_quality"]["training_history_availability"]
        == "unavailable_before_first_garmin_sync"
    )


def test_history_cutoff_is_independent_from_future_plan_start(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    try:
        conn.execute(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, activity_type, distance_meters,
                elapsed_duration_seconds
            ) VALUES (1, '2026-08-15T06:00:00Z', 'running', 5000, 1800)
            """
        )
        conn.commit()
    finally:
        conn.close()

    packet = build_coaching_packet(
        db_path,
        as_of="2026-08-16",
        plan_start="2026-10-01",
        lookback_days=28,
        plan_horizon_days=7,
    )

    context = packet["context"]
    assert context["training_history"]["summary"]["activities"] == 1
    assert context["training_history"]["window_end"] == "2026-08-16"
    assert context["current_plan"]["window_start"] == "2026-10-01"
    assert context["event"]["start_date"] == "2026-10-01"
    assert context["event"]["event_date"] == "2026-10-07"


def test_packet_context_always_satisfies_required_echo_fields(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    conn.close()

    packet = build_coaching_packet(db_path, as_of="2026-08-16")

    assert packet["context"]["athlete"]["age"] == 50
    assert packet["context"]["event"]["distance"] == "50K"
    assert packet["context"]["event"]["run_days_per_week"] == 5


def test_explicit_unspecified_sodium_overrides_stored_value(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    try:
        db_queries.set_setting(conn, "plan_sodium", 900)
        conn.commit()
    finally:
        conn.close()

    packet = build_coaching_packet(
        db_path,
        as_of="2026-08-16",
        plan_context={"sodium_mg_per_hour": None},
    )

    assert packet["context"]["athlete"]["sodium_mg_per_hour"] is None


def test_packet_rejects_lthr_that_is_not_below_hrmax(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    try:
        conn.execute(
            """
            UPDATE athlete_profile
            SET hrmax_override = 170, lthr_override = 170
            WHERE profile_id = 1
            """
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError, match="LTHR must be lower than HRmax"):
        build_coaching_packet(db_path, as_of="2026-08-16")


def test_schema_conforming_response_is_accepted_by_importer(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    conn.close()
    packet = build_coaching_packet(db_path, as_of="2026-08-16")
    schema = packet["chatgpt"]["requested_output_schema"]

    response = {
        "contract": TRAINING_PLAN_UPDATE_CONTRACT,
        "version": TRAINING_PLAN_UPDATE_VERSION,
        "request_id": packet["request_id"],
        "active_plan_sha256": packet["active_plan_sha256"],
        "athlete": {
            "name": "Athlete",
            "age": 40,
            "primary_sport": "run",
            "hrmax_bpm": None,
            "lthr_bpm": None,
            "sodium_mg_per_hour": None,
            "notes": "",
        },
        "event": {
            "name": "Training Event",
            "sport": "run",
            "distance": "5K",
            "start_date": "2026-08-16",
            "event_date": "2026-08-16",
            "run_days_per_week": 5,
        },
        "analysis": {
            "hrmax_observed_bpm": None,
            "hrmax_robust_bpm": None,
            "lthr_suggested_bpm": None,
            "active_weeks": None,
            "avg_weekly_hours": None,
            "avg_weekly_miles": None,
            "z2_fraction": None,
            "notes": "Insufficient history for additional conclusions.",
        },
        "workouts": [
            {
                "date": "2026-08-16",
                "sport": "run",
                "phase": "Race",
                "workout": "5K event",
                "notes": "Use a conservative pacing strategy.",
                "flags": ["RACE"],
                "intensity": "race",
                "duration_minutes": 30,
                "distance_km": 5,
                "tss": None,
            }
        ],
        "nutrition_targets": [
            {
                "date": "2026-08-16",
                "day_type": "race",
                "carbohydrate_g_per_kg_min": 5.0,
                "carbohydrate_g_per_kg_max": 7.0,
                "protein_g_per_kg_min": 1.4,
                "protein_g_per_kg_max": 1.8,
                "fat_g_per_kg_min": 0.8,
                "fat_g_per_kg_max": 1.2,
                "during_training_carbohydrate_g_per_hour_min": 30,
                "during_training_carbohydrate_g_per_hour_max": 60,
                "notes": "Educational range only.",
            }
        ],
        "nutrition_guidance": ["General education only; use familiar foods."],
        "strength_guidance": ["General education only; avoid fatigue before race day."],
        "rationale": "The one-day plan contains the required event workout.",
        "warnings": [],
    }

    assert set(response) == set(schema["required"]) | {
        "nutrition_guidance",
        "strength_guidance",
        "rationale",
        "warnings",
    }
    assert set(response["athlete"]) == set(
        schema["properties"]["athlete"]["required"]
    )
    assert set(response["event"]) == set(
        schema["properties"]["event"]["required"]
    )
    assert set(response["analysis"]) == set(
        schema["properties"]["analysis"]["required"]
    )
    assert set(response["workouts"][0]) == set(
        schema["properties"]["workouts"]["items"]["required"]
    )

    parsed = parse_chatgpt_plan(
        response,
        expected_request_id=packet["request_id"],
        expected_active_plan_sha256=packet["active_plan_sha256"],
    )
    assert parsed.request_id == packet["request_id"]
    assert parsed.active_plan_sha256 == packet["active_plan_sha256"]
    assert parsed.workouts[0].iso_date == "2026-08-16"


def test_packet_uses_authoritative_active_plan_hash_for_same_date_sessions(tmp_path):
    db_path, conn = _create_packet_db(tmp_path)
    try:
        conn.executemany(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("2026-08-20", "Strength", "Dumbbell session", None, 2400, 30),
                ("2026-08-20", "Easy run", "Aerobic", 8000, 3000, 50),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    packet = build_coaching_packet(db_path, as_of="2026-08-16")

    assert packet["active_plan_sha256"] == get_persistence_active_plan_sha256(
        db_path
    )
    assert packet["active_plan_sha256"] == get_active_plan_sha256(db_path)
