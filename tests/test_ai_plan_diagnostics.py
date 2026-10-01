from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import codex_plan_generator as generator
from garmin_data_hub.services.ai_plan_import import (
    PlanValidationError,
    ValidationFinding,
    parse_chatgpt_plan,
)
from garmin_data_hub.services.coaching_packet import _build_copyable_prompt
from garmin_data_hub.ui_nicegui import workspace


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "codex_maffetone_95_day_request.json"


def _nutrition(start: date, end: date) -> list[dict]:
    return [
        {
            "date": (start + timedelta(days=offset)).isoformat(),
            "day_type": "race" if start + timedelta(days=offset) == end else "easy",
            "carbohydrate_g_per_kg_min": 4.0,
            "carbohydrate_g_per_kg_max": 6.0,
            "protein_g_per_kg_min": 1.4,
            "protein_g_per_kg_max": 1.8,
            "fat_g_per_kg_min": 0.8,
            "fat_g_per_kg_max": 1.2,
            "during_training_carbohydrate_g_per_hour_min": None,
            "during_training_carbohydrate_g_per_hour_max": None,
            "notes": "Educational range.",
        }
        for offset in range((end - start).days + 1)
    ]


def _proposal(
    *,
    start: date = date(2026, 10, 1),
    end: date = date(2026, 10, 8),
    phase: str = "Base",
) -> dict:
    response = {
        "contract": "garmin-data-hub.chatgpt-plan",
        "version": 1,
        "request_id": "a" * 64,
        "active_plan_sha256": "b" * 64,
        "athlete": {
            "name": "Sanitized Athlete",
            "age": 70,
            "primary_sport": "run",
            "hrmax_bpm": None,
            "lthr_bpm": None,
            "sodium_mg_per_hour": None,
            "notes": "",
        },
        "event": {
            "name": "Sanitized Event",
            "sport": "run",
            "distance": "10K",
            "start_date": start.isoformat(),
            "event_date": end.isoformat(),
            "run_days_per_week": 4,
        },
        "analysis": {
            "hrmax_observed_bpm": None,
            "hrmax_robust_bpm": None,
            "lthr_suggested_bpm": None,
            "active_weeks": 0,
            "avg_weekly_hours": 0,
            "avg_weekly_miles": 0,
            "z2_fraction": None,
            "notes": "Sanitized fixture.",
        },
        "workouts": [
            {
                "date": start.isoformat(),
                "sport": "run",
                "phase": phase,
                "workout": "Easy Run",
                "notes": "Stay at or below the 110 bpm MAF cap.",
                "flags": [],
                "intensity": "easy",
                "duration_minutes": 30,
                "distance_km": 5,
                "tss": 20,
            },
            {
                "date": end.isoformat(),
                "sport": "run",
                "phase": "Race",
                "workout": "10K Race",
                "notes": "Warm up, race by effort, then cool down.",
                "flags": ["RACE"],
                "intensity": "race",
                "duration_minutes": 60,
                "distance_km": 10,
                "tss": 100,
            },
        ],
        "nutrition_targets": _nutrition(start, end),
        "nutrition_guidance": [],
        "strength_guidance": [],
        "rationale": "A conservative sanitized proposal.",
        "warnings": [],
    }
    return response


def _finding_ids(exc: PlanValidationError) -> list[str]:
    return [finding.rule_id for finding in exc.findings]


def test_sanitized_95_day_fixture_reproduces_complete_acceptance_path():
    request = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    start = date.fromisoformat(request["context"]["current_plan"]["window_start"])
    end = date.fromisoformat(request["context"]["current_plan"]["window_end"])
    response = _proposal(start=start, end=end)
    response["request_id"] = request["request_id"]
    response["active_plan_sha256"] = request["active_plan_sha256"]
    response["workouts"] = []
    current = start
    week = 1
    while current + timedelta(days=6) <= end:
        response["workouts"].append(
            {
                "date": current.isoformat(),
                "sport": "strength",
                "phase": "Base",
                "workout": f"Strength Week {week}",
                "notes": "Bodyweight technique: 2 sets of controlled repetitions.",
                "flags": [],
                "intensity": "easy",
                "duration_minutes": 25,
                "distance_km": None,
                "tss": 10,
            }
        )
        saturday = current + timedelta(days=(5 - current.weekday()) % 7)
        if saturday <= current + timedelta(days=6):
            response["workouts"].append(
                {
                    "date": saturday.isoformat(),
                    "sport": "run",
                    "phase": "Base",
                    "workout": f"Long Easy Run Week {week}",
                    "notes": "Stay at or below the 110 bpm MAF cap.",
                    "flags": [],
                    "intensity": "easy",
                    "duration_minutes": 60,
                    "distance_km": 8,
                    "tss": 30,
                }
            )
        current += timedelta(days=7)
        week += 1
    response["workouts"].append(
        {
            "date": end.isoformat(),
            "sport": "run",
            "phase": "Race",
            "workout": "10K Race",
            "notes": "Warm up, race, and cool down within this sole race-day session.",
            "flags": ["RACE"],
            "intensity": "race",
            "duration_minutes": 60,
            "distance_km": 10,
            "tss": 100,
        }
    )

    assert (end - start).days + 1 == 95
    assert request["context"]["current_plan"]["sessions"] == []
    parsed = parse_chatgpt_plan(
        json.dumps(response),
        expected_request_id=request["request_id"],
        expected_active_plan_sha256=request["active_plan_sha256"],
        minimum_strength_sessions_per_week=1,
        training_method="maffetone",
    )
    assert len(parsed.nutrition_targets) == 95
    assert parsed.workouts[-1].intensity == "race"


def test_race_day_requires_one_total_workout_and_embedded_warmup_notes():
    invalid = _proposal()
    invalid["workouts"].append(
        {
            **invalid["workouts"][-1],
            "workout": "Separate Warmup",
            "intensity": "easy",
            "duration_minutes": 15,
            "distance_km": 2,
            "flags": [],
        }
    )
    with pytest.raises(PlanValidationError) as captured:
        parse_chatgpt_plan(invalid, training_method="maffetone")
    race = next(item for item in captured.value.findings if item.rule_id == "race_invariant")
    assert race.path == "$.workouts"

    corrected = _proposal()
    parsed = parse_chatgpt_plan(corrected, training_method="maffetone")
    assert "Warm up" in parsed.workouts[-1].notes


@pytest.mark.parametrize("intensity", ["moderate", "hard"])
def test_maffetone_rejects_non_race_moderate_and_hard(intensity):
    proposal = _proposal()
    proposal["workouts"][0]["intensity"] = intensity
    with pytest.raises(PlanValidationError) as captured:
        parse_chatgpt_plan(proposal, training_method="maffetone")
    assert "training_method_intensity" in _finding_ids(captured.value)


@pytest.mark.parametrize("intensity", ["easy", "recovery"])
def test_maffetone_accepts_easy_and_recovery(intensity):
    proposal = _proposal()
    proposal["workouts"][0]["intensity"] = intensity
    assert parse_chatgpt_plan(proposal, training_method="maffetone")


@pytest.mark.parametrize("phase", ["Base", "Build", "Peak", "Maintenance"])
def test_each_qualifying_full_week_phase_requires_strength(phase):
    proposal = _proposal(phase=phase)
    with pytest.raises(PlanValidationError) as captured:
        parse_chatgpt_plan(
            proposal,
            minimum_strength_sessions_per_week=1,
            training_method="maffetone",
        )
    assert "strength_minimum" in _finding_ids(captured.value)


def test_nutrition_findings_cover_each_required_failure_mode():
    cases = []
    missing = _proposal()
    missing["nutrition_targets"].pop()
    cases.append((missing, "nutrition_missing_date"))
    duplicate = _proposal()
    duplicate["nutrition_targets"][-1]["date"] = duplicate["nutrition_targets"][0]["date"]
    cases.append((duplicate, "nutrition_duplicate_date"))
    outside = _proposal()
    outside["nutrition_targets"][-1]["date"] = "2027-01-01"
    cases.append((outside, "nutrition_outside_window"))
    empty = _proposal()
    empty["nutrition_targets"] = []
    cases.append((empty, "nutrition_exact_coverage"))
    reversed_range = _proposal()
    reversed_range["nutrition_targets"][0]["protein_g_per_kg_min"] = 2.0
    reversed_range["nutrition_targets"][0]["protein_g_per_kg_max"] = 1.0
    cases.append((reversed_range, "nutrition_minimum_maximum"))
    half_null = _proposal()
    half_null["nutrition_targets"][0]["during_training_carbohydrate_g_per_hour_min"] = 30
    cases.append((half_null, "nutrition_during_training_pair"))

    for proposal, rule_id in cases:
        with pytest.raises(PlanValidationError) as captured:
            parse_chatgpt_plan(proposal, training_method="maffetone")
        assert rule_id in _finding_ids(captured.value)


@pytest.mark.parametrize(
    ("mutate", "path"),
    [
        (lambda value: value.update(rationale=""), "$.rationale"),
        (lambda value: value.update(rationale="x" * 4_001), "$.rationale"),
        (lambda value: value["analysis"].update(notes="x" * 4_001), "$.analysis.notes"),
        (lambda value: value.update(warnings=["x" * 1_001]), "$.warnings[0]"),
        (lambda value: value.update(strength_guidance=["x" * 1_001]), "$.strength_guidance[0]"),
        (lambda value: value.update(nutrition_guidance=["x" * 1_001]), "$.nutrition_guidance[0]"),
    ],
)
def test_local_text_constraints_have_deterministic_paths(mutate, path):
    proposal = _proposal()
    mutate(proposal)
    with pytest.raises(PlanValidationError) as captured:
        parse_chatgpt_plan(proposal, training_method="maffetone")
    assert path in [finding.path for finding in captured.value.findings]


def test_multi_failure_is_aggregated_ordered_blocking_and_ui_safe():
    proposal = _proposal()
    proposal["workouts"].append({**proposal["workouts"][-1], "workout": "Warmup"})
    proposal["workouts"][0]["intensity"] = "hard"
    proposal["nutrition_targets"] = []
    proposal["rationale"] = ""
    with pytest.raises(PlanValidationError) as captured:
        parse_chatgpt_plan(proposal, training_method="maffetone")

    findings = captured.value.findings
    assert len(findings) >= 5
    assert findings == tuple(sorted(findings, key=lambda item: (
        {"workouts": 1, "nutrition": 2, "text": 3, "policy": 4}.get(item.stage, 99),
        item.path, item.rule_id, item.expected, item.actual_type, repr(item.actual_value),
    )))
    assert all(item.blocking and item.severity == "error" for item in findings)
    summary = workspace._rejection_summary(findings)
    assert summary.startswith(f"Codex proposal rejected: {len(findings)} validation findings")
    assert "Warmup" not in json.dumps([item.to_safe_dict() for item in findings])


def test_retry_uses_same_context_safe_findings_and_stops_after_success(monkeypatch):
    packet = {"request_id": "a" * 64, "active_plan_sha256": "b" * 64}
    calls = []

    def generate(received, **kwargs):
        calls.append((received, kwargs["prompt"]))
        return SimpleNamespace(response_json=f'{{"attempt": {len(calls)}}}')

    def review(response):
        if json.loads(response)["attempt"] == 1:
            raise PlanValidationError((ValidationFinding(
                stage="text", rule_id="text_length", path="$.rationale",
                expected="rationale must contain at most 4000 characters",
                actual_type="string",
            ),))
        return SimpleNamespace(can_apply=True)

    monkeypatch.setattr(workspace, "generate_plan_with_codex", generate)
    job = workspace.GenerationJob()
    job.start(packet, prompt="original prompt", reviewer=review)
    snapshot = _wait(job)

    assert snapshot.state == "completed"
    assert len(calls) == 2
    assert calls[0][0] is packet and calls[1][0] is packet
    assert calls[1][0]["request_id"] == packet["request_id"]
    assert calls[1][0]["active_plan_sha256"] == packet["active_plan_sha256"]
    assert "text_length" in calls[1][1]
    assert "actual_value" not in calls[1][1]


def test_second_rejection_has_no_third_attempt_and_database_is_unchanged(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "safety.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
        conn.execute("CREATE TABLE sentinel(value TEXT NOT NULL)")
        conn.execute("INSERT INTO sentinel VALUES ('unchanged')")
        conn.commit()
        tracked_tables = (
            "planned_workout",
            "training_plan",
            "plan_revision",
            "plan_revision_workout",
            "plan_import_history",
        )
        before_counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tracked_tables
        }
    finally:
        conn.close()
    before = db_path.read_bytes()
    calls = []

    def generate(*args, **kwargs):
        calls.append(kwargs["prompt"])
        return SimpleNamespace(response_json='{"private_note": "must not be retained"}')

    finding = ValidationFinding(
        stage="nutrition", rule_id="nutrition_exact_coverage",
        path="$.nutrition_targets", expected="one item per plan date",
        actual_type="array", actual_value=0,
    )
    monkeypatch.setattr(workspace, "generate_plan_with_codex", generate)
    job = workspace.GenerationJob()
    job.start(
        {"request_id": "a" * 64, "active_plan_sha256": "b" * 64},
        prompt="original prompt",
        reviewer=lambda response: (_ for _ in ()).throw(PlanValidationError((finding,))),
    )
    snapshot = _wait(job)

    assert snapshot.state == "rejected"
    assert len(calls) == 2
    assert snapshot.response_json is None
    assert "private_note" not in (snapshot.error or "")
    assert db_path.read_bytes() == before
    with sqlite3.connect(db_path) as conn:
        assert {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tracked_tables
        } == before_counts


def test_default_prompt_states_the_actual_local_acceptance_rules():
    prompt = _build_copyable_prompt(request_id="a" * 64, active_plan_sha256="b" * 64)
    for wording in (
        "exactly one total workout",
        "Do not output a separate race-day warmup workout",
        "schema-defined response fields",
        "workouts does not require one entry per calendar date",
        'sport "rest" and intensity "rest"',
        'only intensity "easy" or "recovery"',
        "no structured MAF target or prescription field",
        "Base, Build, Peak, and Maintenance",
        'sport "strength"',
        "every calendar date from the plan window start through end inclusive",
        "both be null or both be numeric",
    ):
        assert wording in prompt


def test_string_lengths_remain_authoritative_in_local_schema_and_importer():
    local = generator.TRAINING_PLAN_UPDATE_SCHEMA
    cli = generator._strict_output_schema()
    assert local["properties"]["rationale"]["minLength"] == 1
    assert local["properties"]["rationale"]["maxLength"] == 4_000
    assert "minLength" not in cli["properties"]["rationale"]
    assert "maxLength" not in cli["properties"]["rationale"]


def _wait(job: workspace.GenerationJob):
    import time

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        snapshot = job.snapshot()
        if snapshot.state not in {"idle", "running", "cancelling"}:
            return snapshot
        time.sleep(0.005)
    raise AssertionError("generation job did not finish")
