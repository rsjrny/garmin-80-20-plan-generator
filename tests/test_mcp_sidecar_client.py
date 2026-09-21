"""Framework-neutral regression tests for the garmin_mcp sidecar client."""

from __future__ import annotations

from unittest.mock import patch

import pytest
import json
import sqlite3


def test_frozen_app_reuses_its_executable_for_mcp_sidecar(monkeypatch):
    from garmin_data_hub import mcp_sidecar_client

    monkeypatch.setattr(mcp_sidecar_client.sys, "frozen", True, raising=False)
    assert mcp_sidecar_client._sidecar_command() == (
        mcp_sidecar_client.sys.executable,
        ["--mcp-sidecar"],
    )


def test_check_sidecar_available_returns_false_on_exception(tmp_path):
    from garmin_data_hub.mcp_sidecar_client import check_sidecar_available

    db_path = tmp_path / "garmin.db"
    db_path.touch()
    with patch(
        "garmin_data_hub.mcp_sidecar_client.call_tool_via_sidecar",
        side_effect=RuntimeError("sidecar not found"),
    ):
        available, error = check_sidecar_available(db_path)
    assert available is False
    assert "sidecar not found" in error


def test_call_tool_timeout_raises(tmp_path):
    from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar

    db_path = tmp_path / "garmin.db"
    db_path.touch()
    with patch(
        "garmin_data_hub.mcp_sidecar_client.anyio.run",
        side_effect=TimeoutError("timed out"),
    ):
        with pytest.raises(TimeoutError):
            call_tool_via_sidecar(
                "garmin_schema", None, db_path, timeout_sec=1, max_retries=0
            )


def test_call_tool_retries_on_transient_error(tmp_path):
    from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar

    db_path = tmp_path / "garmin.db"
    db_path.touch()
    call_count = {"n": 0}

    def fail_then_succeed(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise ConnectionError("transient")
        return "{}"

    with patch(
        "garmin_data_hub.mcp_sidecar_client.anyio.run",
        side_effect=fail_then_succeed,
    ):
        with patch("garmin_data_hub.mcp_sidecar_client.time.sleep"):
            result = call_tool_via_sidecar(
                "garmin_schema", None, db_path, max_retries=2
            )
    assert result == "{}"
    assert call_count["n"] == 2


def test_runtime_tool_error_is_not_retried(tmp_path):
    from garmin_data_hub.mcp_sidecar_client import call_tool_via_sidecar

    db_path = tmp_path / "garmin.db"
    db_path.touch()
    call_count = {"n": 0}

    def always_runtime(*args, **kwargs):
        call_count["n"] += 1
        raise RuntimeError("bad sql")

    with patch(
        "garmin_data_hub.mcp_sidecar_client.anyio.run",
        side_effect=always_runtime,
    ):
        with pytest.raises(RuntimeError, match="bad sql"):
            call_tool_via_sidecar(
                "garmin_query", {"sql": "DROP TABLE x"}, db_path, max_retries=2
            )
    assert call_count["n"] == 1


def test_real_upstream_mcp_uses_exact_path_and_rejects_writes(tmp_path):
    from garmin_data_hub.mcp_sidecar_client import describe_readonly_tools, call_readonly_tools

    path = tmp_path / "renamed-preview.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE marker (value TEXT)")
        conn.execute("INSERT INTO marker VALUES ('selected preview')")
        conn.execute("CREATE TABLE training_status (calendar_date TEXT, status TEXT, acute_load REAL, chronic_load REAL)")
        conn.execute("INSERT INTO training_status VALUES (date('now'), 'PRODUCTIVE', 100, 90)")
    before = path.read_bytes()
    tools = describe_readonly_tools(path)
    assert "garmin_sleep" in tools
    assert "input_schema" in tools["garmin_query"]
    result = call_readonly_tools(path, [("garmin_query", {"sql": "SELECT value FROM marker"})])
    assert json.loads(result["garmin_query"]["text"]) == [{"value": "selected preview"}]
    status = call_readonly_tools(path, [("garmin_training_status", {"days": 28})])
    assert json.loads(status["garmin_training_status"]["text"])["current_status"] == "PRODUCTIVE"
    rejected = call_readonly_tools(path, [("garmin_query", {"sql": "DELETE FROM marker"})])
    assert "error" in json.loads(rejected["garmin_query"]["text"])
    assert before == path.read_bytes()
    assert not (tmp_path / "garmin.db").exists()


def test_readonly_calls_cannot_launch_sync(tmp_path):
    from garmin_data_hub.mcp_sidecar_client import call_readonly_tools, call_tool_via_sidecar
    with pytest.raises(ValueError, match="cannot start"):
        call_readonly_tools(tmp_path / "garmin.db", [("garmin_sync", {})])
    with pytest.raises(ValueError, match="requires a database named"):
        call_tool_via_sidecar("garmin_sync", {"refresh": True}, tmp_path / "preview.db")


def test_server_connections_enforce_read_only(monkeypatch, tmp_path):
    from garmin_mcp import db
    from garmin_data_hub.mcp_server import configure_database

    for name in ("DB_PATH", "get_connection", "init_db"):
        monkeypatch.setattr(db, name, getattr(db, name))
    path = tmp_path / "source.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE marker (value TEXT)")
    configure_database(path, read_only=True)
    conn = db.get_connection()
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO marker VALUES ('not allowed')")
    finally:
        conn.close()
    with pytest.raises(ValueError, match="different database"):
        db.get_connection(str(tmp_path / "other.db"))
