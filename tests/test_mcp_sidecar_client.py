"""Framework-neutral regression tests for the garmin_mcp sidecar client."""

from __future__ import annotations

from unittest.mock import patch

import pytest


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
