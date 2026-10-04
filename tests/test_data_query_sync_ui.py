"""Data Query fresh-sync actions use the application's shared, guarded job."""

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest
from nicegui import ui

from garmin_data_hub.services.garmin_credentials import GarminCredentials
from garmin_data_hub.ui_nicegui import pages
from ui_test_support import run_ui, wait_until


class SharedJob:
    def __init__(self, state="idle"):
        self.state = state
        self.starts = []

    def snapshot(self):
        return SimpleNamespace(state=self.state, progress=0.4, elapsed_seconds=12,
                               return_code=None, error=None, log="Shared progress")

    def start(self, **kwargs):
        self.starts.append(kwargs)
        self.state = "running"


@pytest.fixture
def sync_action(monkeypatch):
    results = []
    job = SharedJob()
    credentials = GarminCredentials("test@example.com", "synthetic-password")
    monkeypatch.setattr(pages, "describe_readonly_tools", lambda _db: {
        "garmin_sync": {"input_schema": {"properties": {"refresh": {"default": False}}}}
    })
    monkeypatch.setattr(pages, "call_readonly_tools", lambda *_args, **_kwargs:
                        pytest.fail("Fresh sync must use the application job"))
    monkeypatch.setattr(pages, "load_credentials", lambda: credentials)
    monkeypatch.setattr(pages, "get_sync_job", lambda _db: job)
    render = pages.render_mcp_result

    def capture(name, raw, *, is_error):
        results.append((json.loads(raw), is_error))
        render(name, raw, is_error=is_error)

    monkeypatch.setattr(pages, "render_mcp_result", capture)

    async def execute(user):
        await user.open("/query")
        user.find(kind=ui.tab, content="MCP tool").click()
        await user.should_see("Connected: 1 Garmin tools", retries=100)
        user.find("Pull fresh data from Garmin").click()
        user.find("Run MCP tool").click()
        await wait_until(lambda: bool(results) or user.notify.contains("unavailable")
                         or user.notify.contains("requires the live database"))

    return SimpleNamespace(job=job, credentials=credentials, results=results, execute=execute)


def test_fresh_sync_starts_shared_job_with_saved_credentials(ui_database, sync_action):
    run_ui(ui_database, sync_action.execute, sandboxed=False)
    assert sync_action.job.starts == [{"days": 0, "credentials": sync_action.credentials}]
    payload, is_error = sync_action.results[0]
    assert is_error is False
    assert payload["message"] == "Garmin Sync started."
    assert payload["sync_state"]["state"] == "running"
    assert "synthetic-password" not in json.dumps(payload)


def test_fresh_sync_missing_login_is_actionable_and_never_starts(
    monkeypatch, ui_database, sync_action
):
    monkeypatch.setattr(pages, "load_credentials", lambda: None)
    run_ui(ui_database, sync_action.execute, sandboxed=False)
    assert sync_action.job.starts == []
    payload, is_error = sync_action.results[0]
    assert is_error is True
    assert "Open Garmin Sync and configure Garmin login" in payload["error"]


@pytest.mark.parametrize("state", ["running", "cancelling", "resetting_login"])
def test_fresh_sync_observes_existing_shared_job_without_duplicate_start(
    ui_database, sync_action, state
):
    sync_action.job.state = state
    run_ui(ui_database, sync_action.execute, sandboxed=False)
    assert sync_action.job.starts == []
    payload, is_error = sync_action.results[0]
    assert is_error is False
    assert payload["message"] == "Garmin Sync is already in progress."
    assert payload["sync_state"]["state"] == state
    assert payload["sync_state"]["log"] == "Shared progress"
    assert payload["sync_state"]["progress"] == 0.4


@pytest.mark.parametrize("sandboxed,filename,reason", [
    (True, "garmin.db", "unavailable for a sandboxed database"),
    (False, "custom.db", "requires the live database named garmin.db"),
])
def test_fresh_sync_rejects_unsafe_targets_without_writes_or_launch(
    monkeypatch, db_conn, ui_database, tmp_path, sync_action, sandboxed, filename, reason
):
    db = tmp_path / filename
    if db != ui_database:
        with sqlite3.connect(db) as target:
            db_conn.backup(target)
    before = db.read_bytes()
    monkeypatch.setattr(pages, "load_credentials", lambda:
                        pytest.fail("Rejected targets must not load credentials"))

    async def scenario(user):
        await sync_action.execute(user)
        assert user.notify.contains(reason)

    run_ui(db, scenario, sandboxed=sandboxed)
    assert sync_action.job.starts == []
    assert sync_action.results == []
    assert db.read_bytes() == before
