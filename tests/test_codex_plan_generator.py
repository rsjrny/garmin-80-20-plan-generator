from __future__ import annotations

import json
import subprocess
import threading
from types import SimpleNamespace

import pytest

from garmin_data_hub.services import codex_plan_generator as generator


def _packet() -> dict:
    return {
        "request_id": "a" * 64,
        "active_plan_sha256": "b" * 64,
        "context": {"athlete": {"name": "Runner"}},
        "chatgpt": {
            "copyable_prompt": "Build a plan",
            "requested_output_schema": {"large": "duplicate"},
        },
    }


def test_codex_generation_uses_saved_login_and_isolated_read_only_run(
    monkeypatch,
):
    captured: dict = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        schema_path = command[command.index("--output-schema") + 1]
        captured["schema"] = json.loads(open(schema_path, encoding="utf-8").read())
        return SimpleNamespace(
            returncode=0,
            stdout='{"workouts": [{"date": "2026-08-23"}]}',
            stderr="",
        )

    monkeypatch.setenv("OPENAI_API_KEY", "must-not-be-forwarded")
    monkeypatch.setenv("CODEX_API_KEY", "must-not-be-forwarded")
    monkeypatch.setattr(generator.subprocess, "run", fake_run)

    result = generator.generate_plan_with_codex(
        _packet(), prompt="Build the plan", executable="codex-test"
    )

    assert json.loads(result.response_json) == {
        "workouts": [{"date": "2026-08-23"}]
    }
    assert captured["command"][:2] == ["codex-test", "exec"]
    assert "--ephemeral" in captured["command"]
    assert captured["command"][captured["command"].index("--sandbox") + 1] == "read-only"
    assert "--ignore-user-config" in captured["command"]
    config_index = captured["command"].index("--config")
    assert captured["command"][config_index + 1] == 'model_reasoning_effort="low"'
    assert captured["command"][-1] == "-"
    assert "--output-last-message" in captured["command"]
    assert captured["shell"] is False
    assert "OPENAI_API_KEY" not in captured["env"]
    assert "CODEX_API_KEY" not in captured["env"]
    assert "Build the plan" in captured["input"]
    assert "<coaching_packet_json>" in captured["input"]
    submitted_packet = json.loads(
        captured["input"].split("<coaching_packet_json>\n", 1)[1].split(
            "\n</coaching_packet_json>", 1
        )[0]
    )
    assert "copyable_prompt" not in submitted_packet["chatgpt"]
    assert "requested_output_schema" not in submitted_packet["chatgpt"]
    assert "copyable_prompt" in _packet()["chatgpt"]
    assert "requested_output_schema" in _packet()["chatgpt"]
    assert captured["schema"]["required"] == list(
        captured["schema"]["properties"]
    )
    assert captured["schema"]["properties"]["contract"] == {
        "type": "string",
        "enum": ["garmin-data-hub.chatgpt-plan"],
    }
    assert captured["schema"]["properties"]["version"] == {
        "type": "integer",
        "enum": [1],
    }


def test_codex_schema_uses_only_supported_generation_constraints():
    schema = generator._strict_output_schema()
    stack = [schema]
    while stack:
        value = stack.pop()
        if isinstance(value, list):
            stack.extend(value)
            continue
        if not isinstance(value, dict):
            continue
        assert not generator._UNSUPPORTED_STRUCTURED_OUTPUT_KEYS.intersection(value)
        assert "const" not in value
        if value.get("type") == "object":
            assert value["additionalProperties"] is False
            assert value["required"] == list(value["properties"])
        stack.extend(value.values())


def test_codex_generation_requires_installed_cli(monkeypatch):
    monkeypatch.setattr(generator, "find_codex_cli", lambda: None)

    with pytest.raises(generator.CodexCliNotFoundError, match="not found"):
        generator.generate_plan_with_codex(_packet(), prompt="Build the plan")


def test_codex_generation_reports_nonzero_exit(monkeypatch):
    monkeypatch.setattr(
        generator.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=7, stdout="", stderr="saved-login authentication failed"
        ),
    )

    with pytest.raises(generator.CodexPlanGenerationError, match="authentication failed"):
        generator.generate_plan_with_codex(
            _packet(), prompt="Build the plan", executable="codex-test"
        )


def test_codex_generation_reads_dedicated_final_response_file(monkeypatch):
    def fake_run(command, **kwargs):
        output_path = command[command.index("--output-last-message") + 1]
        with open(output_path, "w", encoding="utf-8") as output_file:
            json.dump({"workouts": [{"date": "2026-08-23"}]}, output_file)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(generator.subprocess, "run", fake_run)

    result = generator.generate_plan_with_codex(
        _packet(), prompt="Build the plan", executable="codex-test"
    )

    assert json.loads(result.response_json)["workouts"]


def test_codex_generation_rejects_empty_proposal(monkeypatch):
    monkeypatch.setattr(
        generator.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0, stdout="{}", stderr=""
        ),
    )

    with pytest.raises(generator.CodexPlanGenerationError, match="empty"):
        generator.generate_plan_with_codex(
            _packet(), prompt="Build the plan", executable="codex-test"
        )


def test_codex_failure_detail_does_not_echo_packet_content():
    detail = generator._failure_detail(
        'private coaching packet\nERROR: {"message":"invalid schema"}\n'
        'ERROR: {"message":"invalid schema"}',
        "",
    )

    assert "private coaching packet" not in detail
    assert detail == 'ERROR: {"message":"invalid schema"}'


def test_codex_generation_reports_timeout(monkeypatch):
    def raise_timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="codex", timeout=60)

    monkeypatch.setattr(generator.subprocess, "run", raise_timeout)

    with pytest.raises(generator.CodexCliTimeoutError, match="1 minute"):
        generator.generate_plan_with_codex(
            _packet(),
            prompt="Build the plan",
            executable="codex-test",
            timeout_seconds=60,
        )


def test_codex_generation_can_be_cancelled_before_launch(monkeypatch):
    cancel_event = threading.Event()
    cancel_event.set()
    monkeypatch.setattr(
        generator.subprocess,
        "Popen",
        lambda *args, **kwargs: pytest.fail("cancelled job must not launch Codex"),
    )

    with pytest.raises(generator.CodexCliCancelledError, match="cancelled"):
        generator.generate_plan_with_codex(
            _packet(),
            prompt="Build the plan",
            executable="codex-test",
            cancel_event=cancel_event,
        )
