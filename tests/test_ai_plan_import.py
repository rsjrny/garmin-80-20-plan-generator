from __future__ import annotations

from copy import deepcopy
import json

import pytest

from garmin_data_hub.exports.forever.calendar_builder import DayPlan
from garmin_data_hub.exports.forever.models import AnalysisSummary, Inputs
from garmin_data_hub.services.ai_plan_import import (
    CHATGPT_PLAN_CONTRACT,
    PlanContractError,
    PlanSafetyError,
    StalePlanResponseError,
    parse_chatgpt_plan,
)


REQUEST_ID = "a" * 64
ACTIVE_PLAN_SHA256 = "b" * 64


def _valid_payload() -> dict:
    return {
        "contract": CHATGPT_PLAN_CONTRACT,
        "version": 1,
        "request_id": REQUEST_ID,
        "active_plan_sha256": ACTIVE_PLAN_SHA256,
        "athlete": {
            "name": "Casey",
            "age": 35,
            "primary_sport": "run",
            "hrmax_bpm": 190,
            "lthr_bpm": 170,
            "sodium_mg_per_hour": 800,
            "notes": "Returning runner",
        },
        "event": {
            "name": "Autumn 10K",
            "sport": "run",
            "distance": "10K",
            "start_date": "2026-09-01",
            "event_date": "2026-09-07",
            "run_days_per_week": 4,
        },
        "analysis": {
            "hrmax_observed_bpm": 188,
            "hrmax_robust_bpm": 186,
            "lthr_suggested_bpm": 169,
            "active_weeks": 10,
            "avg_weekly_hours": 4.25,
            "avg_weekly_miles": 22.5,
            "z2_fraction": 0.78,
            "notes": "Keep most work easy.",
        },
        "workouts": [
            {
                "date": "2026-09-01",
                "sport": "run",
                "phase": "Taper",
                "workout": "Easy Run",
                "notes": "Conversational effort.",
                "flags": ["TAPER"],
                "intensity": "easy",
                "duration_minutes": 45,
                "distance_km": 8.25,
                "tss": 42,
            },
            {
                "date": "2026-09-02",
                "sport": "strength",
                "phase": "Taper",
                "workout": "Strength Maintenance",
                "notes": "Light technique work.",
                "flags": ["TAPER"],
                "intensity": "moderate",
                "duration_minutes": 30,
                "distance_km": None,
                "tss": 18.5,
            },
            {
                "date": "2026-09-04",
                "sport": "run",
                "phase": "Taper",
                "workout": "Short Threshold",
                "notes": "Controlled repetitions.",
                "flags": ["TAPER"],
                "intensity": "hard",
                "duration_minutes": 50,
                "distance_km": 10,
                "tss": 75,
            },
            {
                "date": "2026-09-07",
                "sport": "run",
                "phase": "Race",
                "workout": "10K Race",
                "notes": "Race by effort.",
                "flags": ["RACE"],
                "intensity": "race",
                "duration_minutes": 60,
                "distance_km": 10,
                "tss": 100,
            },
        ],
        "nutrition_guidance": ["Practice the planned breakfast before race day."],
        "strength_guidance": ["Keep race-week lifting light."],
        "rationale": "The plan tapers volume while retaining one short quality session.",
        "warnings": ["Stop if pain changes running form."],
    }


def test_parse_normalizes_for_existing_persistence_and_retains_exact_values():
    result = parse_chatgpt_plan(
        _valid_payload(),
        expected_request_id=REQUEST_ID,
        expected_active_plan_sha256=ACTIVE_PLAN_SHA256,
    )

    assert result.request_id == REQUEST_ID
    assert result.active_plan_sha256 == ACTIVE_PLAN_SHA256
    assert result.primary_sport == "run"
    assert result.event_sport == "run"
    assert isinstance(result.inputs, Inputs)
    assert isinstance(result.analysis, AnalysisSummary)
    assert result.inputs.athlete.athlete_name == "Casey"
    assert result.workouts[0].distance_km == 8.25
    assert result.workouts[1].sport == "strength"
    assert result.workouts[1].tss == 18.5

    assert len(result.day_plans) == 7
    assert all(isinstance(day, DayPlan) for day in result.day_plans)
    assert result.day_plans[2].workout == "Rest Day"
    assert result.day_plans[2].sport == "rest"
    assert result.day_plans[2].intensity == "rest"
    assert result.day_plans[2].session_count == 0
    assert result.day_plans[0].sport == "run"
    assert result.day_plans[0].intensity == "easy"
    assert result.day_plans[0].session_count == 1
    assert "45 min" in result.day_plans[0].notes
    assert "8.25 km" in result.day_plans[0].notes

    inputs, analysis, day_plans, weekly_rows = result.as_persistence_args()
    assert inputs is result.inputs
    assert analysis is result.analysis
    assert isinstance(day_plans, list)
    assert isinstance(weekly_rows, list)
    assert weekly_rows[0]["Run Days"] == 3
    assert weekly_rows[0]["Strength Days"] == 1


def test_normalized_payload_is_deterministic_and_keeps_guidance():
    payload = _valid_payload()
    payload["workouts"] = list(reversed(payload["workouts"]))

    result = parse_chatgpt_plan(payload)
    normalized = result.to_dict()

    assert [item["date"] for item in normalized["workouts"]] == sorted(
        item["date"] for item in normalized["workouts"]
    )
    assert normalized["nutrition_guidance"] == [
        "Practice the planned breakfast before race day."
    ]
    assert normalized["strength_guidance"] == ["Keep race-week lifting light."]
    assert json.loads(result.canonical_payload) == normalized
    assert result.canonical_payload == parse_chatgpt_plan(normalized).canonical_payload


@pytest.mark.parametrize("encoding", ["text", "bytes"])
def test_accepts_json_text_and_utf8_bytes(encoding):
    raw = json.dumps(_valid_payload())
    payload = raw if encoding == "text" else raw.encode("utf-8")

    assert parse_chatgpt_plan(payload).contract == CHATGPT_PLAN_CONTRACT


def test_rejects_duplicate_json_keys_and_non_object_payloads():
    with pytest.raises(PlanContractError, match="Duplicate JSON key"):
        parse_chatgpt_plan('{"contract":"one","contract":"two"}')

    with pytest.raises(PlanContractError, match="must be a JSON object"):
        parse_chatgpt_plan("[]")

    with pytest.raises(PlanContractError, match="Payload must"):
        parse_chatgpt_plan(123)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("location", "key"),
    [
        ((), "surprise"),
        (("athlete",), "weight_kg"),
        (("event",), "timezone"),
        (("analysis",), "diagnosis"),
        (("workouts", 0), "shell_command"),
    ],
)
def test_rejects_unknown_fields_at_every_contract_level(location, key):
    payload = _valid_payload()
    target = payload
    for part in location:
        target = target[part]
    target[key] = "not allowed"

    with pytest.raises(PlanContractError, match="unknown keys"):
        parse_chatgpt_plan(payload)


def test_rejects_wrong_version_bad_hash_and_stale_echoes():
    payload = _valid_payload()
    payload["version"] = 2
    with pytest.raises(PlanContractError, match="version"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["request_id"] = "ABC"
    with pytest.raises(PlanContractError, match="lowercase SHA-256"):
        parse_chatgpt_plan(payload)

    with pytest.raises(StalePlanResponseError, match="response is stale"):
        parse_chatgpt_plan(_valid_payload(), expected_request_id="c" * 64)

    with pytest.raises(StalePlanResponseError, match="response is stale"):
        parse_chatgpt_plan(
            _valid_payload(), expected_active_plan_sha256="d" * 64
        )


def test_rejects_missing_out_of_range_or_non_race_event_workouts():
    payload = _valid_payload()
    payload["workouts"][-1]["date"] = "2026-09-06"
    with pytest.raises(PlanSafetyError, match="only on event_date"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["workouts"][0]["date"] = "2026-08-31"
    with pytest.raises(PlanContractError, match="outside"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["workouts"][-1]["intensity"] = "easy"
    with pytest.raises(PlanSafetyError, match="must be 'race'"):
        parse_chatgpt_plan(payload)


def test_supports_multiple_typed_sessions_per_date_and_counts_unique_days():
    payload = _valid_payload()
    payload["event"]["run_days_per_week"] = 3
    payload["workouts"][1]["date"] = "2026-09-01"
    recovery_run = deepcopy(payload["workouts"][0])
    recovery_run.update(
        {
            "workout": "Recovery Shakeout",
            "notes": "Very light second run.",
            "intensity": "recovery",
            "duration_minutes": 20,
            "distance_km": 3,
            "tss": 12,
        }
    )
    payload["workouts"].insert(1, recovery_run)

    result = parse_chatgpt_plan(payload)

    same_day = [
        workout for workout in result.workouts if workout.iso_date == "2026-09-01"
    ]
    assert {(workout.sport, workout.intensity) for workout in same_day} == {
        ("run", "easy"),
        ("run", "recovery"),
        ("strength", "moderate"),
    }
    first_day = result.day_plans[0]
    assert first_day.session_count == 3
    assert first_day.sport == "strength"
    assert first_day.intensity == "moderate"
    assert first_day.phase == "Taper"
    assert first_day.flags == "TAPER"
    assert "Easy Run" in first_day.workout
    assert "Recovery Shakeout" in first_day.workout
    assert "Strength Maintenance" in first_day.workout
    assert "Easy Run:" in first_day.notes
    assert "Strength Maintenance:" in first_day.notes

    week = result.weekly_rows[0]
    assert week["Run Days"] == 3
    assert week["Strength Days"] == 1
    assert week["Quality Sessions"] == 2
    assert week["Run Hours (est)"] == 2.92

    reparsed = parse_chatgpt_plan(result.to_dict())
    assert reparsed.canonical_payload == result.canonical_payload
    assert reparsed.day_plans[0].session_count == 3

    reversed_payload = deepcopy(payload)
    reversed_payload["workouts"] = list(reversed(reversed_payload["workouts"]))
    assert (
        parse_chatgpt_plan(reversed_payload).canonical_payload
        == result.canonical_payload
    )


def test_rejects_duplicate_excess_rest_or_hard_same_day_sessions():
    payload = _valid_payload()
    duplicate = deepcopy(payload["workouts"][0])
    payload["workouts"].insert(1, duplicate)
    with pytest.raises(PlanContractError, match="duplicates another session"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["workouts"][1].update(
        {
            "date": "2026-09-01",
            "sport": "rest",
            "workout": "Rest",
            "intensity": "rest",
            "duration_minutes": 0,
            "distance_km": None,
            "tss": 0,
        }
    )
    with pytest.raises(PlanSafetyError, match="cannot mix rest"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    second_hard = deepcopy(payload["workouts"][2])
    second_hard.update(
        {
            "date": "2026-09-04",
            "sport": "cycle",
            "workout": "Hard Bike",
            "distance_km": 20,
        }
    )
    payload["workouts"].insert(3, second_hard)
    with pytest.raises(PlanSafetyError, match="more than one hard/race"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    for index in range(3):
        extra = deepcopy(payload["workouts"][1])
        extra.update(
            {
                "date": "2026-09-01",
                "sport": "mobility",
                "workout": f"Mobility {index}",
                "intensity": "recovery",
                "duration_minutes": 10 + index,
                "tss": None,
            }
        )
        payload["workouts"].insert(index + 1, extra)
    with pytest.raises(PlanSafetyError, match="more than 3 sessions"):
        parse_chatgpt_plan(payload)


def test_rejects_unsafe_metrics_active_content_and_rest_mismatch():
    payload = _valid_payload()
    payload["workouts"][0]["duration_minutes"] = 800
    with pytest.raises(PlanSafetyError, match="12-hour training cap"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["analysis"]["notes"] = "<script>alert(1)</script>"
    with pytest.raises(PlanSafetyError, match="active HTML"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["workouts"][0]["sport"] = "rest"
    with pytest.raises(PlanSafetyError, match="must both be 'rest'"):
        parse_chatgpt_plan(payload)


def test_rejects_age_capped_or_consecutive_hard_sessions():
    payload = _valid_payload()
    payload["athlete"]["age"] = 50
    with pytest.raises(PlanSafetyError, match="age-based maximum is 1"):
        parse_chatgpt_plan(payload)

    payload = _valid_payload()
    payload["athlete"]["age"] = 20
    payload["workouts"][1].update(
        {
            "date": "2026-09-03",
            "sport": "cycle",
            "workout": "Hard Bike",
            "intensity": "hard",
            "distance_km": 20,
        }
    )
    with pytest.raises(PlanSafetyError, match="are consecutive"):
        parse_chatgpt_plan(payload)


def test_rejects_run_volume_jump_over_ten_percent():
    payload = _valid_payload()
    payload["athlete"]["age"] = 20
    payload["event"].update(
        {"event_date": "2026-09-21", "run_days_per_week": 4}
    )
    payload["workouts"] = [
        {
            "date": "2026-09-01",
            "sport": "run",
            "phase": "Base",
            "workout": "Easy Run",
            "notes": "Easy.",
            "flags": [],
            "intensity": "easy",
            "duration_minutes": 30,
            "distance_km": 5,
            "tss": 25,
        },
        {
            "date": "2026-09-03",
            "sport": "run",
            "phase": "Base",
            "workout": "Easy Run",
            "notes": "Easy.",
            "flags": [],
            "intensity": "easy",
            "duration_minutes": 30,
            "distance_km": 5,
            "tss": 25,
        },
        {
            "date": "2026-09-08",
            "sport": "run",
            "phase": "Build",
            "workout": "Easy Run",
            "notes": "Easy.",
            "flags": [],
            "intensity": "easy",
            "duration_minutes": 36,
            "distance_km": 6,
            "tss": 30,
        },
        {
            "date": "2026-09-10",
            "sport": "run",
            "phase": "Build",
            "workout": "Easy Run",
            "notes": "Easy.",
            "flags": [],
            "intensity": "easy",
            "duration_minutes": 36,
            "distance_km": 6,
            "tss": 30,
        },
        {
            "date": "2026-09-21",
            "sport": "run",
            "phase": "Race",
            "workout": "10K Race",
            "notes": "Race.",
            "flags": ["RACE"],
            "intensity": "race",
            "duration_minutes": 60,
            "distance_km": 10,
            "tss": 100,
        },
    ]

    with pytest.raises(PlanSafetyError, match="increases 20.0%"):
        parse_chatgpt_plan(payload)
