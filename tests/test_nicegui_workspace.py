from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.codex_plan_generator import CodexCliCancelledError
from garmin_data_hub.services.ai_plan_import import PlanSafetyError
from garmin_data_hub.services.plan_persistence import save_plan_setting
from garmin_data_hub.ui_nicegui import workspace


TEST_PLAN_DATE = date.today().isoformat()


def _database(tmp_path: Path, *, name: str = "source.db") -> Path:
    db_path = tmp_path / name
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    for key, value in {
        "plan_athlete_name": "Prototype Athlete",
        "plan_age": 42,
        "plan_distance": "10K",
        "plan_event_name": "Prototype 10K",
        "plan_run_days": 4,
        "plan_long_run_day": "Saturday",
        "plan_sodium": 700,
        "plan_start_date": TEST_PLAN_DATE,
        "plan_event_date": TEST_PLAN_DATE,
    }.items():
        save_plan_setting(db_path, key, value)
    return db_path


def _response_for(packet: dict) -> dict:
    athlete = packet["context"]["athlete"]
    event = packet["context"]["event"]
    start = packet["context"]["current_plan"]["window_start"]
    end = packet["context"]["current_plan"]["window_end"]
    return {
        "contract": "garmin-data-hub.chatgpt-plan",
        "version": 1,
        "request_id": packet["request_id"],
        "active_plan_sha256": packet["active_plan_sha256"],
        "athlete": {
            "name": athlete["name"],
            "age": athlete["age"],
            "primary_sport": athlete["primary_sport"],
            "hrmax_bpm": athlete["hrmax_bpm"],
            "lthr_bpm": athlete["lthr_bpm"],
            "sodium_mg_per_hour": athlete["sodium_mg_per_hour"],
            "notes": athlete["notes"],
        },
        "event": {
            "name": event["name"],
            "sport": event["sport"],
            "distance": event["distance"],
            "start_date": start,
            "event_date": end,
            "run_days_per_week": event["run_days_per_week"],
        },
        "analysis": {
            "hrmax_observed_bpm": None,
            "hrmax_robust_bpm": None,
            "lthr_suggested_bpm": None,
            "active_weeks": 0,
            "avg_weekly_hours": 0,
            "avg_weekly_miles": 0,
            "z2_fraction": None,
            "notes": "One-day prototype proposal.",
        },
        "workouts": [
            {
                "date": end,
                "sport": event["sport"],
                "phase": "Race",
                "workout": "10K Race",
                "notes": "Race by effort.",
                "flags": ["RACE"],
                "intensity": "race",
                "duration_minutes": 60,
                "distance_km": 10,
                "tss": 100,
            }
        ],
        "nutrition_targets": [
            {
                "date": end,
                "day_type": "race",
                "carbohydrate_g_per_kg_min": 5,
                "carbohydrate_g_per_kg_max": 7,
                "protein_g_per_kg_min": 1.4,
                "protein_g_per_kg_max": 1.8,
                "fat_g_per_kg_min": 0.8,
                "fat_g_per_kg_max": 1.2,
                "during_training_carbohydrate_g_per_hour_min": 30,
                "during_training_carbohydrate_g_per_hour_max": 60,
                "notes": "Food-agnostic educational range.",
            }
        ],
        "nutrition_guidance": ["Practice the planned fueling range."],
        "strength_guidance": ["Keep race-day strength work omitted."],
        "rationale": "The proposal preserves the event and adds a reviewed race session.",
        "warnings": [],
    }


def test_snapshot_and_apply_never_modify_source_database(tmp_path):
    source = _database(tmp_path)
    preview = workspace.create_database_snapshot(source, tmp_path / "preview.db")
    context = workspace.load_workspace_context(preview, sandboxed=True)
    packet = workspace.build_workspace_packet(context)
    response = _response_for(packet)

    review = workspace.review_proposal(context, packet, json.dumps(response))

    assert review.can_apply
    assert review.changes[0]["Change"] == "Added"
    result = workspace.save_review_to_database(context, review)
    assert result.applied
    assert workspace._plan_changes(preview, review.plan) == ()

    source_conn = connect_sqlite(source)
    preview_conn = connect_sqlite(preview)
    try:
        assert source_conn.execute("SELECT COUNT(*) FROM planned_workout").fetchone()[0] == 0
        assert preview_conn.execute("SELECT COUNT(*) FROM planned_workout").fetchone()[0] == 1
    finally:
        source_conn.close()
        preview_conn.close()


def test_review_rejects_locked_context_changes(tmp_path):
    preview = workspace.create_database_snapshot(
        _database(tmp_path), tmp_path / "preview.db"
    )
    context = workspace.load_workspace_context(preview, sandboxed=True)
    packet = workspace.build_workspace_packet(context)
    response = _response_for(packet)
    response["athlete"]["sodium_mg_per_hour"] = 900
    response["athlete"]["primary_sport"] = "cycle"
    response["event"]["sport"] = "cycle"
    response["workouts"][0]["sport"] = "cycle"

    review = workspace.review_proposal(context, packet, json.dumps(response))

    assert not review.can_apply
    assert any("sodium setting" in message for message in review.errors)
    assert any("primary sport" in message for message in review.errors)
    assert any("event sport" in message for message in review.errors)


def test_review_diff_detects_metric_changes_with_same_workout_name(tmp_path):
    db_path = _database(tmp_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss,
                structure_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                TEST_PLAN_DATE,
                "10K Race",
                "Race by effort.",
                10_000,
                1_800,
                100,
                json.dumps(
                    {
                        "workout": {
                            "sport": "run",
                            "phase": "Race",
                            "workout": "10K Race",
                            "intensity": "race",
                            "duration_minutes": 30,
                            "distance_km": 10,
                            "tss": 100,
                            "flags": ["RACE"],
                            "notes": "Race by effort.",
                        }
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    context = workspace.load_workspace_context(db_path, sandboxed=True)
    packet = workspace.build_workspace_packet(context)
    review = workspace.review_proposal(
        context, packet, json.dumps(_response_for(packet))
    )

    assert len(review.changes) == 1
    assert review.changes[0]["Change"] == "Changed"
    assert "Duration Minutes" in review.changes[0]["Changed fields"]


def test_review_diff_shows_note_only_changes(tmp_path):
    db_path = _database(tmp_path)
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, description,
                planned_distance_m, planned_duration_s, planned_tss,
                structure_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                TEST_PLAN_DATE,
                "10K Race",
                "Original note.",
                10_000,
                3_600,
                100,
                json.dumps(
                    {
                        "workout": {
                            "sport": "run",
                            "phase": "Race",
                            "workout": "10K Race",
                            "intensity": "race",
                            "duration_minutes": 60,
                            "distance_km": 10,
                            "tss": 100,
                            "flags": ["RACE"],
                            "notes": "Original note.",
                        }
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    context = workspace.load_workspace_context(db_path, sandboxed=True)
    packet = workspace.build_workspace_packet(context)
    response = _response_for(packet)
    response["workouts"][0]["notes"] = "Updated note."
    review = workspace.review_proposal(context, packet, json.dumps(response))

    assert len(review.changes) == 1
    assert review.changes[0]["Changed fields"] == "Notes"
    assert "Notes: Original note." in review.changes[0]["Current"]
    assert "Notes: Updated note." in review.changes[0]["Proposed"]


def test_future_plan_start_keeps_training_history_as_of_today(monkeypatch, tmp_path):
    context = workspace.load_workspace_context(_database(tmp_path), sandboxed=True)
    future_start = date.today() + timedelta(days=30)
    future = workspace.WorkspaceContext(
        **{
            **context.__dict__,
            "plan_start": future_start,
            "event_date": future_start + timedelta(days=30),
        }
    )
    captured = {}

    def fake_build(*args, **kwargs):
        captured.update(kwargs)
        return {"packet": True}

    monkeypatch.setattr(workspace, "build_coaching_packet", fake_build)

    assert workspace.build_workspace_packet(future) == {"packet": True}
    assert captured["as_of"] == date.today()
    assert captured["plan_start"] == future_start
    assert captured["plan_context"]["training_method"] == "eighty_twenty"


def test_workspace_context_includes_training_method(tmp_path):
    db_path = _database(tmp_path)
    save_plan_setting(db_path, "plan_training_method", "maffetone")

    context = workspace.load_workspace_context(db_path, sandboxed=True)
    packet = workspace.build_workspace_packet(context)

    assert context.training_method == "maffetone"
    assert packet["context"]["training_constraints"]["training_method"] == "maffetone"


def test_workspace_review_rejects_maffetone_hard_endurance(tmp_path):
    db_path = _database(tmp_path)
    save_plan_setting(db_path, "plan_training_method", "maffetone")
    save_plan_setting(
        db_path,
        "plan_event_date",
        (date.today() + timedelta(days=8)).isoformat(),
    )
    context = workspace.load_workspace_context(db_path, sandboxed=True)
    packet = workspace.build_workspace_packet(context)
    response = _response_for(packet)
    start = date.fromisoformat(packet["context"]["current_plan"]["window_start"])
    end = date.fromisoformat(packet["context"]["current_plan"]["window_end"])
    nutrition_template = response["nutrition_targets"][0]
    response["nutrition_targets"] = [
        {
            **nutrition_template,
            "date": (start + timedelta(days=offset)).isoformat(),
            "day_type": "race" if start + timedelta(days=offset) == end else "easy",
        }
        for offset in range((end - start).days + 1)
    ]
    response["workouts"].insert(
        0,
        {
            "date": packet["context"]["current_plan"]["window_start"],
            "sport": "run",
            "phase": "Base",
            "workout": "Tempo",
            "notes": "Hard tempo session.",
            "flags": [],
            "intensity": "hard",
            "duration_minutes": 30,
            "distance_km": 5,
            "tss": 50,
        },
    )
    response["workouts"].insert(
        1,
        {
            "date": (start + timedelta(days=1)).isoformat(),
            "sport": "strength",
            "phase": "Base",
            "workout": "Strength A",
            "notes": "General strength support.",
            "flags": [],
            "intensity": "moderate",
            "duration_minutes": 30,
            "distance_km": None,
            "tss": 20,
        },
    )

    with pytest.raises(PlanSafetyError, match="Maffetone"):
        workspace.review_proposal(context, packet, json.dumps(response))


def test_workspace_context_validation_catches_contract_bounds(tmp_path):
    context = workspace.load_workspace_context(_database(tmp_path), sandboxed=True)

    invalid = workspace.WorkspaceContext(
        **{**context.__dict__, "age": 101, "run_days_per_week": 8}
    )

    errors = workspace.validate_workspace_context(invalid)
    assert any("age" in message.lower() for message in errors)
    assert any("run days" in message.lower() for message in errors)


def test_custom_generation_prompt_persists_with_fresh_hash_placeholders(tmp_path):
    db_path = _database(tmp_path)
    context = workspace.load_workspace_context(db_path, sandboxed=True)
    packet = workspace.build_workspace_packet(context)
    custom = (
        f"Keep the normal contract. Request {packet['request_id']} and "
        f"plan {packet['active_plan_sha256']}. Add mobility guidance."
    )

    workspace.save_workspace_prompt(db_path, custom, packet)

    loaded = workspace.load_workspace_prompt(db_path, packet)
    assert loaded == custom
    assert "Add mobility guidance" in loaded


def test_generation_job_completes_without_blocking(monkeypatch):
    class Result:
        response_json = '{"proposal": true}'

    def generate(*args, **kwargs):
        kwargs["progress_callback"](1.5)
        return Result()

    monkeypatch.setattr(workspace, "generate_plan_with_codex", generate)
    job = workspace.GenerationJob()

    job.start({"packet": True}, prompt="test")
    snapshot = _wait_for_terminal(job)

    assert snapshot.state == "completed"
    assert snapshot.response_json == Result.response_json


def test_generation_job_cancels_the_running_operation(monkeypatch):
    def generate(*args, **kwargs):
        cancel_event = kwargs["cancel_event"]
        while not cancel_event.wait(0.005):
            pass
        raise CodexCliCancelledError("Codex CLI generation was cancelled")

    monkeypatch.setattr(workspace, "generate_plan_with_codex", generate)
    job = workspace.GenerationJob()
    job.start({"packet": True}, prompt="test")

    assert job.cancel()
    snapshot = _wait_for_terminal(job)

    assert snapshot.state == "cancelled"
    assert "cancelled" in (snapshot.error or "").lower()


def test_nicegui_page_builds_and_serves(tmp_path):
    pytest.importorskip("nicegui")
    from fastapi.testclient import TestClient
    from nicegui import app

    from garmin_data_hub.ui_nicegui.app import create_ui

    preview = workspace.create_database_snapshot(
        _database(tmp_path), tmp_path / "preview.db"
    )
    create_ui(preview, sandboxed=True)
    app.config.add_run_config(
        reload=False,
        title="test",
        viewport="width=device-width, initial-scale=1",
        favicon=None,
        dark=False,
        language="en-US",
        binding_refresh_interval=0.1,
        reconnect_timeout=3.0,
        message_history_length=1000,
        tailwind=True,
        unocss=None,
        prod_js=True,
        show_welcome_message=False,
        markdown=False,
    )

    with TestClient(app) as client:
        responses = {
            route: client.get(route)
            for route in (
                "/",
                "/sync",
                "/activities",
                "/charts",
                "/plan",
                "/coach",
                "/compliance",
                "/query",
                "/settings",
                "/guide",
            )
        }

    assert all(response.status_code == 200 for response in responses.values())
    assert all("NiceGUI" in response.text for response in responses.values())


def _wait_for_terminal(job: workspace.GenerationJob):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        snapshot = job.snapshot()
        if snapshot.state not in {"idle", "running", "cancelling"}:
            return snapshot
        time.sleep(0.005)
    raise AssertionError("generation job did not finish")
