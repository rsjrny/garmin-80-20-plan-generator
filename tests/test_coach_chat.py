import json
import subprocess
import threading
from datetime import date

import pytest

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import coach_chat
from garmin_data_hub.services.codex_plan_generator import CodexPlanGenerationError


def test_context_is_read_only_and_recovery_is_opt_in(tmp_path, monkeypatch):
    path = tmp_path / "training.db"
    conn = connect_sqlite(path)
    apply_schema(conn, schema_sql_path())
    conn.close()
    before = path.read_bytes()
    calls = []

    def recovery(*args):
        calls.append(args)
        return {"period": {}, "summary": {}, "trends": [], "sources": {}, "rows": ["private raw data"]}

    monkeypatch.setattr(coach_chat, "analyze_sleep_recovery", recovery)
    context = coach_chat.build_chat_context(path, as_of=date(2026, 9, 21))
    assert "recovery" not in context
    assert not calls
    assert context["plan_comparison"]["end"] == "2026-09-20"
    assert context["plan_comparison"]["actual"] is None
    context = coach_chat.build_chat_context(path, include_recovery=True)
    assert len(calls) == 1
    assert "rows" not in context["recovery"]
    assert path.read_bytes() == before


@pytest.mark.parametrize("response,valid", [
    ({"answer": "No recent training is available.", "evidence": [], "missing_data": ["Activities"], "followups": []}, True),
    ({"answer": ""}, False),
    ({"answer": "hello", "evidence": "wrong type", "missing_data": [], "followups": []}, False),
    (["wrong shape"], False),
])
def test_generator_contract_and_isolation(monkeypatch, response, valid):
    def run(command, **kwargs):
        assert "--ephemeral" in command
        assert "read-only" in command
        assert "--ignore-user-config" in command
        payload = json.loads(kwargs["input_text"].split("\n", 1)[1])
        assert len(payload["conversation"]) == 12
        assert payload["question"] == "How am I doing?"
        assert {p.name for p in kwargs["cwd"].iterdir()} == {"answer.schema.json"}
        return subprocess.CompletedProcess(command, 0, json.dumps(response), "")

    monkeypatch.setattr(coach_chat, "_run_cancellable", run)
    monkeypatch.setattr(coach_chat, "external_tool_environment", lambda _: {})
    args = ({}, "How am I doing?", [{"role": "user", "content": "previous"}] * 20)
    kwargs = {"cancel_event": threading.Event(), "executable": "codex"}
    if valid:
        assert coach_chat.generate_chat_answer(*args, **kwargs) == response
    else:
        with pytest.raises(CodexPlanGenerationError, match="invalid answer"):
            coach_chat.generate_chat_answer(*args, **kwargs)


def test_generator_does_not_echo_failed_process_output(monkeypatch):
    monkeypatch.setattr(coach_chat, "external_tool_environment", lambda _: {})
    monkeypatch.setattr(coach_chat, "_run_cancellable", lambda *a, **kw: subprocess.CompletedProcess([], 1, "private health data", "private health data"))
    with pytest.raises(CodexPlanGenerationError) as caught:
        coach_chat.generate_chat_answer({}, "Hello", [], cancel_event=threading.Event(), executable="codex")
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("requested", [["garmin_sleep"], ["garmin_sync"], ["garmin_query"],
                                        ["garmin_training_status"] * 4, [123]])
def test_mcp_rejects_unshared_or_unapproved_tools(monkeypatch, tmp_path, requested):
    from garmin_data_hub import mcp_sidecar_client as client
    monkeypatch.setattr(client, "list_readonly_tools", lambda _: set(coach_chat.TRAINING_LOOKUPS) | set(coach_chat.RECOVERY_LOOKUPS))
    monkeypatch.setattr(coach_chat, "_generate_json", lambda *a, **kw: {"tools": requested})
    monkeypatch.setattr(client, "call_readonly_tools", lambda *a: pytest.fail("Forbidden lookup executed"))
    with pytest.raises(CodexPlanGenerationError, match="unsupported"):
        coach_chat.add_mcp_context(tmp_path / "db", {"as_of_date": "2026-09-21", "recovery_included": False}, "Question", [], cancel_event=threading.Event())


def test_mcp_recovery_opt_in_and_bounded_payload(monkeypatch, tmp_path):
    from garmin_data_hub import mcp_sidecar_client as client
    monkeypatch.setattr(client, "list_readonly_tools", lambda _: {"garmin_sleep"})
    monkeypatch.setattr(coach_chat, "_generate_json", lambda *a, **kw: {"tools": ["garmin_sleep"]})

    def call(path, calls):
        assert calls == [("garmin_sleep", {"days": 14})]
        return {"garmin_sleep": {"is_error": False, "text": json.dumps({"data": "x" * 25000})}}

    monkeypatch.setattr(client, "call_readonly_tools", call)
    result = coach_chat.add_mcp_context(tmp_path / "db", {"as_of_date": "2026-09-21", "recovery_included": True}, "Sleep?", [], cancel_event=threading.Event())
    assert "size limit" in result["tools"]["garmin_sleep"]["error"]


def test_mcp_cancellation_prevents_discovery(monkeypatch, tmp_path):
    from garmin_data_hub import mcp_sidecar_client as client
    from garmin_data_hub.services.codex_plan_generator import CodexCliCancelledError
    monkeypatch.setattr(client, "list_readonly_tools", lambda _: pytest.fail("Discovery after cancellation"))
    event = threading.Event()
    event.set()
    with pytest.raises(CodexCliCancelledError):
        coach_chat.add_mcp_context(tmp_path / "db", {}, "Question", [], cancel_event=event)
