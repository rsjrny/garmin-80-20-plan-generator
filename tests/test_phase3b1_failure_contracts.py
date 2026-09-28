"""Intentional RED contracts for the Phase 3B persistence hardening work.

These tests describe required behavior, not the current implementation. Keep this
module excluded from the pre-Phase-3B regression run until Phase 3B.2 makes the
contracts green.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

from nicegui.testing import user_simulation

from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import athlete_metrics_service
from garmin_data_hub.services.codex_prerequisites import (
    CodexPrerequisiteStatus,
    ToolStatus,
)
from garmin_data_hub.ui_nicegui import app as nicegui_app
from garmin_data_hub.ui_nicegui import data as nicegui_data
from garmin_data_hub.ui_nicegui import pages, workspace


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "phase3b1-contract.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return db_path


def _execute(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _profile(db_path: Path) -> tuple[int | None, ...]:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override, "
            "ftp_override FROM athlete_profile WHERE profile_id = 1"
        ).fetchone()
    finally:
        conn.close()


def _settings(db_path: Path, keys: list[str]) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            f"SELECT key, value FROM app_settings WHERE key IN "
            f"({','.join('?' for _ in keys)})",
            keys,
        ).fetchall()
    finally:
        conn.close()
    return {key: json.loads(value) for key, value in rows}


def _seed_settings(db_path: Path, values: dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(
            "INSERT OR REPLACE INTO app_settings(key, value) VALUES (?, ?)",
            [(key, json.dumps(value)) for key, value in values.items()],
        )
        conn.commit()
    finally:
        conn.close()


def _capture_error(call) -> BaseException | None:
    try:
        call()
    except sqlite3.Error as exc:
        return exc
    return None


class RollbackThenRaiseOnCommit:
    """Model SQLite ending a failed transaction by rolling it back."""

    def __init__(self, raw: sqlite3.Connection, message: str) -> None:
        self.raw = raw
        self.message = message
        self.commit_calls = 0
        self.rollback_calls = 0

    @property
    def in_transaction(self) -> bool:
        return self.raw.in_transaction

    def execute(self, *args, **kwargs):
        return self.raw.execute(*args, **kwargs)

    def commit(self) -> None:
        self.commit_calls += 1
        self.raw.rollback()
        assert not self.raw.in_transaction
        raise sqlite3.OperationalError(self.message)

    def rollback(self) -> None:
        self.rollback_calls += 1
        self.raw.rollback()


class NotificationRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, message: str, **kwargs: Any) -> None:
        self.calls.append((str(message), kwargs))

    @property
    def positives(self) -> list[str]:
        return [
            message
            for message, kwargs in self.calls
            if kwargs.get("type", kwargs.get("color")) == "positive"
        ]

    @property
    def negatives(self) -> list[str]:
        return [
            message
            for message, kwargs in self.calls
            if kwargs.get("type", kwargs.get("color")) == "negative"
        ]


def _assert_one_safe_failure(
    notifications: NotificationRecorder, *, forbidden: tuple[str, ...] = ()
) -> None:
    assert notifications.positives == []
    assert len(notifications.negatives) == 1
    message = notifications.negatives[0]
    assert any(
        phrase in message.casefold()
        for phrase in ("could not", "unable", "not saved", "failed")
    )
    assert all(secret.casefold() not in message.casefold() for secret in forbidden)


def _run_page_action(
    db_path: Path,
    path: str,
    label: str,
    assertions,
    *,
    before_click_seconds: float = 0,
    settle_seconds: float = 0.05,
) -> None:
    async def scenario() -> None:
        async with user_simulation(
            root=lambda: nicegui_app.create_ui(db_path, sandboxed=True)
        ) as user:
            await user.open("/")
            await user.open(path)
            if before_click_seconds:
                await asyncio.sleep(before_click_seconds)
            notifications = NotificationRecorder()
            user.notify = notifications
            interaction = user.find(label)
            interaction.click()
            await asyncio.sleep(settle_seconds)
            assertions(user, notifications)

    asyncio.run(scenario())


def test_threshold_override_execution_failure_propagates_from_service(tmp_path):
    db_path = _database(tmp_path)
    athlete_metrics_service.set_override_metrics(db_path, 188, 168)
    _execute(
        db_path,
        """
        CREATE TRIGGER reject_threshold_override
        BEFORE UPDATE OF hrmax_override, lthr_override ON athlete_profile
        BEGIN
            SELECT RAISE(ABORT, 'threshold update rejected');
        END
        """,
    )

    error = _capture_error(
        lambda: athlete_metrics_service.set_override_metrics(db_path, 192, 172)
    )

    assert (_profile(db_path)[2:4], type(error)) == (
        (188, 168),
        sqlite3.IntegrityError,
    )


def test_threshold_genuine_commit_failure_rolls_back_and_propagates(tmp_path):
    db_path = _database(tmp_path)
    athlete_metrics_service.set_override_metrics(db_path, 188, 168)
    raw = sqlite3.connect(db_path)

    class FailBeforeCommit:
        rollback_calls = 0

        @property
        def in_transaction(self):
            return raw.in_transaction

        def execute(self, *args, **kwargs):
            return raw.execute(*args, **kwargs)

        def commit(self):
            raise sqlite3.OperationalError("commit rejected before durability")

        def rollback(self):
            self.rollback_calls += 1
            raw.rollback()

    wrapped = FailBeforeCommit()
    try:
        error = _capture_error(
            lambda: queries.set_override_metrics(wrapped, 193, 173, 270)
        )
        durable = sqlite3.connect(db_path)
        try:
            stored = durable.execute(
                "SELECT hrmax_override, lthr_override, ftp_override "
                "FROM athlete_profile WHERE profile_id = 1"
            ).fetchone()
        finally:
            durable.close()
    finally:
        raw.close()

    assert (wrapped.rollback_calls, stored, type(error)) == (
        1,
        (188, 168, None),
        sqlite3.OperationalError,
    )


def test_set_setting_auto_rollback_commit_failure_propagates(tmp_path):
    db_path = _database(tmp_path)
    _seed_settings(db_path, {"commit-truth": "old"})
    raw = sqlite3.connect(db_path)
    wrapped = RollbackThenRaiseOnCommit(raw, "single setting commit rolled back")

    try:
        error = _capture_error(
            lambda: queries.set_setting(wrapped, "commit-truth", "new")
        )
        stored = _settings(db_path, ["commit-truth"])
    finally:
        raw.close()

    assert (wrapped.commit_calls, wrapped.rollback_calls) == (1, 0)
    assert (type(error), str(error), stored) == (
        sqlite3.OperationalError,
        "single setting commit rolled back",
        {"commit-truth": "old"},
    )


def test_set_settings_auto_rollback_commit_failure_propagates(tmp_path):
    db_path = _database(tmp_path)
    before = {"commit-truth-a": "old-a", "commit-truth-b": "old-b"}
    _seed_settings(db_path, before)
    raw = sqlite3.connect(db_path)
    wrapped = RollbackThenRaiseOnCommit(raw, "grouped settings commit rolled back")

    try:
        error = _capture_error(
            lambda: queries.set_settings(
                wrapped,
                {"commit-truth-a": "new-a", "commit-truth-b": "new-b"},
            )
        )
        stored = _settings(db_path, list(before))
    finally:
        raw.close()

    assert (wrapped.commit_calls, wrapped.rollback_calls) == (1, 0)
    assert (type(error), str(error), stored) == (
        sqlite3.OperationalError,
        "grouped settings commit rolled back",
        before,
    )


def test_threshold_auto_rollback_commit_failure_propagates(tmp_path):
    db_path = _database(tmp_path)
    athlete_metrics_service.set_override_metrics(db_path, 188, 168)
    raw = sqlite3.connect(db_path)
    wrapped = RollbackThenRaiseOnCommit(raw, "threshold commit rolled back")

    try:
        error = _capture_error(
            lambda: queries.set_override_metrics(wrapped, 193, 173, 270)
        )
        stored = _profile(db_path)[2:]
    finally:
        raw.close()

    assert (wrapped.commit_calls, wrapped.rollback_calls) == (1, 0)
    assert (type(error), str(error), stored) == (
        sqlite3.OperationalError,
        "threshold commit rolled back",
        (188, 168, None),
    )


def test_threshold_save_failure_has_one_error_and_no_success_notification(tmp_path):
    db_path = _database(tmp_path)
    athlete_metrics_service.set_override_metrics(db_path, 188, 168)
    _execute(
        db_path,
        """
        CREATE TRIGGER reject_threshold_save
        BEFORE UPDATE OF hrmax_override, lthr_override ON athlete_profile
        BEGIN
            SELECT RAISE(ABORT, 'secret threshold SQL failure');
        END
        """,
    )

    def assertions(_user, notifications):
        _assert_one_safe_failure(
            notifications, forbidden=("secret threshold SQL failure", str(db_path))
        )
        assert _profile(db_path)[2:4] == (188, 168)

    _run_page_action(db_path, "/plan", "Save override", assertions)


def test_threshold_clear_failure_has_one_error_and_keeps_all_overrides(tmp_path):
    db_path = _database(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        queries.set_override_metrics(conn, 188, 168, 255)
    finally:
        conn.close()
    _execute(
        db_path,
        """
        CREATE TRIGGER reject_threshold_clear
        BEFORE UPDATE OF hrmax_override, lthr_override, ftp_override
        ON athlete_profile
        WHEN NEW.hrmax_override IS NULL
        BEGIN
            SELECT RAISE(ABORT, 'secret clear SQL failure');
        END
        """,
    )

    def assertions(_user, notifications):
        _assert_one_safe_failure(
            notifications, forbidden=("secret clear SQL failure", str(db_path))
        )
        assert _profile(db_path)[2:] == (188, 168, 255)

    _run_page_action(db_path, "/plan", "Clear override", assertions)


def test_recalculation_write_failure_keeps_controls_and_reports_one_error(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    athlete_metrics_service.set_calculated_metrics(db_path, 184, 164)
    _execute(
        db_path,
        """
        CREATE TRIGGER reject_calculated_thresholds
        BEFORE UPDATE OF hrmax_calc, lthr_calc ON athlete_profile
        BEGIN
            SELECT RAISE(ABORT, 'secret recalculation SQL failure');
        END
        """,
    )
    monkeypatch.setattr(
        pages,
        "calculate_metrics_from_db_sources",
        lambda *_args, **_kwargs: (191, 171, None),
    )

    def assertions(user, notifications):
        _assert_one_safe_failure(
            notifications,
            forbidden=("secret recalculation SQL failure", str(db_path)),
        )
        hrmax = next(iter(user.find("HRmax override (0 clears)").elements))
        lthr = next(iter(user.find("LTHR override (0 clears)").elements))
        assert (hrmax.value, lthr.value) == (None, None)
        assert _profile(db_path)[:2] == (184, 164)

    _run_page_action(
        db_path, "/plan", "Recalculate", assertions, settle_seconds=0.15
    )


def test_interface_settings_failure_is_atomic_and_propagates(tmp_path):
    db_path = _database(tmp_path)
    before = dict(nicegui_data.INTERFACE_SETTING_DEFAULTS)
    keys = list(nicegui_data.INTERFACE_SETTING_KEYS.values())
    _seed_settings(
        db_path,
        {
            nicegui_data.INTERFACE_SETTING_KEYS[friendly]: value
            for friendly, value in before.items()
        },
    )
    rejected_key = nicegui_data.INTERFACE_SETTING_KEYS["activity_row_limit"]
    _execute(
        db_path,
        f"""
        CREATE TRIGGER reject_middle_interface_setting
        BEFORE INSERT ON app_settings
        WHEN NEW.key = '{rejected_key}'
        BEGIN
            SELECT RAISE(ABORT, 'middle interface setting rejected');
        END
        """,
    )
    requested = {
        "unit_system": "Metric",
        "activity_velocity_display": "Speed",
        "activity_lookback_days": 180,
        "activity_row_limit": 250,
        "activity_default_sport": "running",
        "chart_lookback_days": 730,
        "sync_lookback_days": 30,
        "dashboard_item_limit": 12,
    }

    error = _capture_error(
        lambda: nicegui_data.save_interface_settings(db_path, requested)
    )
    stored = _settings(db_path, keys)
    expected = {
        nicegui_data.INTERFACE_SETTING_KEYS[friendly]: value
        for friendly, value in before.items()
    }

    assert (stored, type(error)) == (expected, sqlite3.IntegrityError)


def test_interface_settings_ui_reports_one_safe_error_and_no_success(tmp_path):
    db_path = _database(tmp_path)
    rejected_key = nicegui_data.INTERFACE_SETTING_KEYS["activity_row_limit"]
    _execute(
        db_path,
        f"""
        CREATE TRIGGER reject_interface_settings_ui
        BEFORE INSERT ON app_settings
        WHEN NEW.key = '{rejected_key}'
        BEGIN
            SELECT RAISE(ABORT, 'secret interface SQL failure');
        END
        """,
    )

    def assertions(_user, notifications):
        _assert_one_safe_failure(
            notifications, forbidden=("secret interface SQL failure", str(db_path))
        )

    _run_page_action(db_path, "/settings", "Save settings", assertions)


def test_workspace_prompt_failure_propagates_and_keeps_old_prompt(tmp_path):
    db_path = _database(tmp_path)
    key = workspace.PROMPT_SETTING_KEY
    _seed_settings(db_path, {key: "old prompt"})
    _execute(
        db_path,
        f"""
        CREATE TRIGGER reject_prompt_setting
        BEFORE INSERT ON app_settings
        WHEN NEW.key = '{key}'
        BEGIN
            SELECT RAISE(ABORT, 'prompt setting rejected');
        END
        """,
    )
    packet = {"request_id": "request-1", "active_plan_sha256": "plan-1"}

    error = _capture_error(
        lambda: workspace.save_workspace_prompt(db_path, "new prompt", packet)
    )

    assert (_settings(db_path, [key]), type(error)) == (
        {key: "old prompt"},
        sqlite3.IntegrityError,
    )


def test_workspace_prompt_ui_reports_one_safe_error_and_no_success(tmp_path):
    db_path = _database(tmp_path)
    key = workspace.PROMPT_SETTING_KEY
    _execute(
        db_path,
        f"""
        CREATE TRIGGER reject_prompt_setting_ui
        BEFORE INSERT ON app_settings
        WHEN NEW.key = '{key}'
        BEGIN
            SELECT RAISE(ABORT, 'secret prompt SQL failure');
        END
        """,
    )

    def assertions(_user, notifications):
        _assert_one_safe_failure(
            notifications, forbidden=("secret prompt SQL failure", str(db_path))
        )

    _run_page_action(db_path, "/coach", "Save prompt", assertions)


def test_workspace_preferences_failure_is_atomic_and_propagates(tmp_path):
    db_path = _database(tmp_path)
    setting_keys = [*workspace.PREFERENCE_TO_SETTING.values()]
    setting_keys.append("chatgpt_exchange_lookback_weeks")
    before = {key: f"old-{index}" for index, key in enumerate(setting_keys[:-1])}
    before[setting_keys[-1]] = 12
    _seed_settings(db_path, before)
    rejected_key = workspace.PREFERENCE_TO_SETTING["strength_experience"]
    _execute(
        db_path,
        f"""
        CREATE TRIGGER reject_middle_workspace_preference
        BEFORE INSERT ON app_settings
        WHEN NEW.key = '{rejected_key}'
        BEGIN
            SELECT RAISE(ABORT, 'middle workspace preference rejected');
        END
        """,
    )
    context = workspace.load_workspace_context(db_path, sandboxed=True)
    requested = {
        preference: f"new-{index}"
        for index, preference in enumerate(workspace.PREFERENCE_TO_SETTING)
    }

    error = _capture_error(
        lambda: workspace.persist_workspace_preferences(
            context, requested, lookback_weeks=24
        )
    )

    assert (_settings(db_path, setting_keys), type(error)) == (
        before,
        sqlite3.IntegrityError,
    )


def test_generation_does_not_start_after_workspace_preference_failure(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    rejected_key = workspace.PREFERENCE_TO_SETTING["strength_experience"]
    _execute(
        db_path,
        f"""
        CREATE TRIGGER reject_generation_preference
        BEFORE INSERT ON app_settings
        WHEN NEW.key = '{rejected_key}'
        BEGIN
            SELECT RAISE(ABORT, 'generation preference rejected');
        END
        """,
    )
    ready = CodexPrerequisiteStatus(
        node=ToolStatus("node", "node", "v22"),
        npm=ToolStatus("npm", "npm", "10"),
        codex=ToolStatus("codex", "codex", "1"),
        auth_state="signed_in",
    )
    starts: list[dict[str, Any]] = []
    monkeypatch.setattr(nicegui_app, "detect_codex_prerequisites", lambda: ready)
    monkeypatch.setattr(
        nicegui_app.GenerationJob,
        "start",
        lambda _self, packet, **kwargs: starts.append(
            {"packet": packet, "kwargs": kwargs}
        ),
    )

    def assertions(_user, notifications):
        assert starts == []
        _assert_one_safe_failure(
            notifications,
            forbidden=("generation preference rejected", str(db_path)),
        )

    _run_page_action(
        db_path,
        "/coach",
        "Generate proposal",
        assertions,
        before_click_seconds=0.2,
        settle_seconds=0.3,
    )


def test_direct_plan_save_operational_error_has_one_safe_negative_notification(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    secret = "near private_table: syntax error in C:\\private\\garmin.db"
    monkeypatch.setattr(
        pages,
        "save_planning_settings",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError(secret)
        ),
    )

    def assertions(_user, notifications):
        _assert_one_safe_failure(notifications, forbidden=(secret, "private_table"))

    _run_page_action(db_path, "/plan", "Save settings", assertions)
