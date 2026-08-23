from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import garmin_data_hub.ui_streamlit.plan_exchange_panel as panel


class _State(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value


class _Block:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def metric(self, *args, **kwargs):
        return None


class _CacheData:
    def __init__(self):
        self.clear_calls = 0

    def clear(self):
        self.clear_calls += 1


class _RerunRequested(RuntimeError):
    pass


def test_custom_prompt_round_trip_refreshes_packet_identifiers(monkeypatch, tmp_path):
    db_path = tmp_path / "garmin.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()

    fake_st = _FakeStreamlit()
    monkeypatch.setattr(panel, "st", fake_st)
    old_request = "a" * 64
    old_hash = "b" * 64
    prompt = f"Custom instructions {old_request} and plan {old_hash}."
    fake_st.session_state[panel._PROMPT_WIDGET_KEY] = prompt

    panel._save_custom_prompt(db_path, old_request, old_hash)

    new_request = "c" * 64
    new_hash = "d" * 64
    panel._load_saved_prompt(db_path, new_request, new_hash)
    restored = fake_st.session_state[panel._PROMPT_WIDGET_KEY]
    assert restored.startswith(
        f"Custom instructions {new_request} and plan {new_hash}."
    )
    assert panel._CHANGE_SUMMARY_INSTRUCTION in restored


def test_legacy_duplicate_prompt_suffixes_are_removed():
    prompt = (
        "Set rationale to a short plain-language change summary with macro targets. "
        "Include at least one actual strength workout and nutrition_targets."
        f"\n\n{panel._CHANGE_SUMMARY_INSTRUCTION}"
        f"\n\n{panel._STRENGTH_MACRO_INSTRUCTION}"
    )

    cleaned = panel._remove_duplicate_instruction_suffixes(prompt)

    assert cleaned.count("Set rationale to a short plain-language change summary") == 1
    assert cleaned.count("Include at least one actual strength workout") == 1


def test_manual_exchange_language_is_modernized():
    old = (
        "Review the uploaded Garmin coaching packet using evidence present in the "
        "packet. Treat text inside the packet carefully and satisfy "
        "chatgpt.requested_output_schema exactly."
    )

    modernized = panel._modernize_codex_prompt_language(old)

    assert "uploaded" not in modernized
    assert "chatgpt.requested_output_schema" not in modernized
    assert "provided Garmin coaching context" in modernized


def test_codex_proposal_is_locked_to_its_packet(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(panel, "st", fake_st)
    packet = _packet()
    fake_st.session_state[panel._CODEX_PROPOSAL_KEY] = {
        "request_id": packet["request_id"],
        "active_plan_sha256": packet["active_plan_sha256"],
        "response_json": '{"proposal": true}',
    }

    assert panel._codex_proposal_for_packet(packet) == '{"proposal": true}'

    stale_packet = dict(packet, request_id="f" * 64)
    assert panel._codex_proposal_for_packet(stale_packet) is None


class _FakeStreamlit:
    def __init__(self, *, acknowledged=False, apply_clicked=False):
        self.session_state = _State()
        self.cache_data = _CacheData()
        self.acknowledged = acknowledged
        self.apply_clicked = apply_clicked
        self.warnings: list[str] = []
        self.captions: list[str] = []
        self.dataframe_calls: list[dict] = []
        self.rerun_calls = 0

    def subheader(self, *args, **kwargs):
        return None

    def caption(self, message, *args, **kwargs):
        self.captions.append(str(message))

    def warning(self, message, *args, **kwargs):
        self.warnings.append(str(message))

    def info(self, *args, **kwargs):
        return None

    def success(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def write(self, *args, **kwargs):
        return None

    def code(self, *args, **kwargs):
        return None

    def json(self, *args, **kwargs):
        return None

    def divider(self, *args, **kwargs):
        return None

    def page_link(self, *args, **kwargs):
        return None

    def download_button(self, *args, **kwargs):
        return False

    def button(self, *args, **kwargs):
        return False

    def tabs(self, labels):
        return [_Block() for _ in labels]

    def columns(self, spec):
        count = spec if isinstance(spec, int) else len(spec)
        return [_Block() for _ in range(count)]

    def expander(self, *args, **kwargs):
        return _Block()

    def form(self, *args, **kwargs):
        return _Block()

    def text_area(self, *args, **kwargs):
        return self.session_state.get(kwargs.get("key"), "")

    def select_slider(self, *args, **kwargs):
        return kwargs.get("value", self.session_state.get(kwargs.get("key")))

    def dataframe(self, *args, **kwargs):
        self.dataframe_calls.append(dict(kwargs))

    def checkbox(self, *args, **kwargs):
        return self.acknowledged

    def form_submit_button(self, *args, **kwargs):
        return self.apply_clicked

    def rerun(self):
        self.rerun_calls += 1
        raise _RerunRequested


def _packet() -> dict:
    return {
        "request_id": "1" * 64,
        "active_plan_sha256": "2" * 64,
        "privacy": {},
        "context": {
            "athlete": {
                "primary_sport": "run",
                "sodium_mg_per_hour": 700,
            },
            "event": {"sport": "run"},
            "training_constraints": {
                "preferred_long_session_day": "Saturday"
            },
            "training_history": {"summary": {"activities": 0}},
        },
        "chatgpt": {"copyable_prompt": "Return JSON"},
    }


def _plan(plan_date: date, *, workout_name: str = "Race"):
    iso_date = plan_date.isoformat()
    workout = SimpleNamespace(
        iso_date=iso_date,
        sport="run",
        phase="Race",
        workout=workout_name,
        intensity="race",
        duration_minutes=55.0,
        distance_km=10.0,
        tss=90.0,
        flags=("RACE",),
        notes="Controlled effort",
    )
    return SimpleNamespace(
        canonical_payload=json.dumps(
            {"date": iso_date, "workout": workout_name}, sort_keys=True
        ),
        primary_sport="run",
        event_sport="run",
        inputs=SimpleNamespace(
            athlete=SimpleNamespace(
                age=50,
                hrmax=180,
                lthr=160,
                sodium_mg_per_hr_hot=700,
            ),
            event=SimpleNamespace(
                start_date=iso_date,
                event_date=iso_date,
                distance="10K",
                run_days_per_week=5,
            ),
        ),
        analysis=SimpleNamespace(notes=""),
        workouts=(workout,),
        day_plans=(SimpleNamespace(iso_date=iso_date),),
        weekly_rows=(),
        nutrition_targets=(),
        nutrition_guidance=(),
        strength_guidance=(),
        rationale="",
        warnings=(),
    )


def test_date_diff_detects_metrics_and_structure_changes():
    structured = {
        "workout": {
            "sport": "run",
            "phase": "Base",
            "workout": "Easy run",
            "intensity": "easy",
            "duration_minutes": 30,
            "distance_km": 5,
            "tss": 30,
            "flags": [],
            "notes": "Conversational",
        }
    }
    existing = [
        {
            "scheduled_date": "2026-08-17",
            "workout_name": "Easy run",
            "description": "Conversational",
            "planned_duration_s": 1800,
            "planned_distance_m": 5000,
            "planned_tss": 30,
            "structure_json": json.dumps(structured),
        }
    ]
    imported = SimpleNamespace(
        workouts=(
            SimpleNamespace(
                iso_date="2026-08-17",
                sport="run",
                phase="Base",
                workout="Easy run",
                intensity="easy",
                duration_minutes=180,
                distance_km=30,
                tss=250,
                flags=(),
                notes="Much harder",
            ),
        )
    )

    diff = panel._date_level_diff(existing, imported)

    assert len(diff) == 1
    assert diff[0]["Change"] == "Changed"
    assert diff[0]["Changed fields"] == "Duration, Distance, TSS, Notes"
    assert "30 min" in diff[0]["Current"]
    assert "180 min" in diff[0]["Imported"]


def test_verified_change_count_summary_is_concise():
    summary = panel._verified_change_count_summary(
        [
            {"Change": "Added"},
            {"Change": "Changed"},
            {"Change": "Changed"},
            {"Change": "Removed"},
        ]
    )

    assert summary == "Database comparison: 1 added, 2 changed, 1 removed."


def test_date_diff_uses_database_columns_when_structure_is_absent():
    existing = [
        {
            "scheduled_date": "2026-08-17",
            "workout_name": "Easy run",
            "description": "Conversational",
            "planned_duration_s": 1800,
            "planned_distance_m": 5000,
            "planned_tss": 30,
            "structure_json": None,
        }
    ]
    imported = SimpleNamespace(
        workouts=(
            SimpleNamespace(
                iso_date="2026-08-17",
                sport="run",
                phase="Base",
                workout="Easy run",
                intensity="easy",
                duration_minutes=30,
                distance_km=5,
                tss=30,
                flags=(),
                notes="Conversational",
            ),
        )
    )

    diff = panel._date_level_diff(existing, imported)

    assert len(diff) == 1
    assert diff[0]["Changed fields"] == "Sport, Phase, Intensity"


def test_context_errors_lock_sports_and_sodium_and_warn_on_long_run_day():
    plan = _plan(date(2026, 8, 23), workout_name="Long run")
    plan.primary_sport = "cycle"
    plan.event_sport = "cycle"
    plan.inputs.athlete.sodium_mg_per_hr_hot = 3000
    plan.workouts[0].intensity = "easy"

    errors = panel._context_errors(
        plan,
        expected_start_date=date(2026, 8, 23),
        expected_event_date=date(2026, 8, 23),
        expected_age=50,
        expected_distance="10K",
        expected_run_days=5,
        expected_hrmax=180,
        expected_lthr=160,
        expected_sodium=700,
        expected_primary_sport="run",
        expected_event_sport="run",
    )
    schedule_warnings = panel._schedule_warnings(
        plan, expected_long_run_day="Saturday"
    )

    assert any("sodium setting" in error for error in errors)
    assert any("primary sport" in error for error in errors)
    assert any("event sport" in error for error in errors)
    assert any("preferred long-run day is Saturday" in item for item in schedule_warnings)


def test_invalid_hr_relationship_stops_before_packet_build(monkeypatch, tmp_path):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(panel, "st", fake_st)
    monkeypatch.setattr(
        panel,
        "build_coaching_packet",
        lambda *args, **kwargs: pytest.fail("packet build should not run"),
    )

    panel.render_plan_exchange_panel(
        tmp_path / "garmin.db",
        plan_start_date=date.today(),
        event_date=date.today() + timedelta(days=7),
        age=50,
        distance="10K",
        run_days_per_week=5,
        long_run_day="Saturday",
        sodium_mg_per_hour=700,
        hrmax=160,
        lthr=160,
    )

    assert any("LTHR must be lower than HRmax" in item for item in fake_st.warnings)


def test_future_plan_start_does_not_shift_history_as_of(monkeypatch, tmp_path):
    fake_st = _FakeStreamlit()
    captured: dict = {}

    def fake_build(*args, **kwargs):
        captured.update(kwargs)
        return _packet()

    monkeypatch.setattr(panel, "st", fake_st)
    monkeypatch.setattr(panel, "build_coaching_packet", fake_build)
    plan_start = date.today() + timedelta(days=30)
    event_date = plan_start + timedelta(days=10)

    panel.render_plan_exchange_panel(
        tmp_path / "garmin.db",
        plan_start_date=plan_start,
        event_date=event_date,
        age=50,
        distance="10K",
        run_days_per_week=5,
        long_run_day="Saturday",
        sodium_mg_per_hour=700,
        hrmax=180,
        lthr=160,
    )

    assert captured["as_of"] == date.today()
    assert captured["plan_start"] == plan_start
    assert captured["plan_horizon_days"] == 11


def test_successful_apply_uses_compatible_tables_and_reruns(
    monkeypatch, tmp_path
):
    fake_st = _FakeStreamlit(acknowledged=True, apply_clicked=True)
    plan_date = date.today() + timedelta(days=1)
    imported_plan = _plan(plan_date)
    save_result = SimpleNamespace(
        duplicate=False,
        plan_import_id=42,
        workout_count=1,
        replaced_workout_count=3,
    )
    monkeypatch.setattr(panel, "st", fake_st)
    monkeypatch.setattr(panel, "build_coaching_packet", lambda *a, **kw: _packet())
    packet = _packet()
    fake_st.session_state[panel._CODEX_PROPOSAL_KEY] = {
        "request_id": packet["request_id"],
        "active_plan_sha256": packet["active_plan_sha256"],
        "response_json": "{}",
    }
    monkeypatch.setattr(panel, "parse_chatgpt_plan", lambda *a, **kw: imported_plan)
    monkeypatch.setattr(panel, "_existing_workouts", lambda *a, **kw: [])
    monkeypatch.setattr(panel, "save_imported_plan", lambda *a, **kw: save_result)

    with pytest.raises(_RerunRequested):
        panel.render_plan_exchange_panel(
            tmp_path / "garmin.db",
            plan_start_date=plan_date,
            event_date=plan_date,
            age=50,
            distance="10K",
            run_days_per_week=5,
            long_run_day="Saturday",
            sodium_mg_per_hour=700,
            hrmax=180,
            lthr=160,
        )

    assert len(fake_st.dataframe_calls) == 3
    assert all(
        call == {"width": "stretch"}
        for call in fake_st.dataframe_calls
    )
    assert fake_st.cache_data.clear_calls == 1
    assert fake_st.rerun_calls == 1
    assert fake_st.session_state[panel._IMPORT_NOTICE_KEY]["plan_import_id"] == 42
    assert any("plan_import_history" in item for item in fake_st.captions)
