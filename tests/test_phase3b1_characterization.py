from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from nicegui.testing import user_simulation

from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import athlete_metrics_service
from garmin_data_hub.ui_nicegui.app import create_ui


# Phase 3B.1 set_setting caller audit:
# - Active UI paths: data.save_interface_settings and
#   plan_persistence.save_plan_setting (the latter serves workspace preferences
#   and the saved generation prompt).
# - Dormant/legacy path: exports.forever.build_daily_plan.save_generated_plan;
#   no production module or project entry point references that module.
# None of these callers documents or tests intentional best-effort suppression,
# so this phase does not add a compatibility test that would require it.


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "phase3b1.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return db_path


def _profile_values(db_path: Path) -> tuple[int | None, ...]:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override, "
            "ftp_override FROM athlete_profile WHERE profile_id = 1"
        ).fetchone()
    finally:
        conn.close()


def test_top_level_threshold_override_write_persists_hr_values(tmp_path):
    db_path = _database(tmp_path)

    athlete_metrics_service.set_override_metrics(db_path, 191, 171)

    assert _profile_values(db_path)[2:4] == (191, 171)


def test_nested_threshold_failure_rolls_back_savepoint_and_preserves_outer_work(
    tmp_path,
):
    db_path = _database(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO app_settings(key, value) VALUES ('outer-work', 'true')"
        )
        conn.execute(
            """
            CREATE TEMP TRIGGER reject_nested_threshold_update
            BEFORE UPDATE OF lthr_override ON athlete_profile
            WHEN NEW.lthr_override = 171
            BEGIN
                SELECT RAISE(ABORT, 'nested threshold failure');
            END
            """
        )

        try:
            queries.set_override_metrics(conn, 191, 171, 255)
        except sqlite3.IntegrityError as exc:
            assert "nested threshold failure" in str(exc)
        else:
            raise AssertionError("nested threshold failure did not propagate")

        assert conn.in_transaction
        assert conn.execute(
            "SELECT value FROM app_settings WHERE key = 'outer-work'"
        ).fetchone() == ("true",)
        assert conn.execute(
            "SELECT hrmax_override, lthr_override, ftp_override "
            "FROM athlete_profile WHERE profile_id = 1"
        ).fetchone() == (None, None, None)
    finally:
        conn.rollback()
        conn.close()


def test_hr_only_override_save_currently_clears_ftp_override(tmp_path):
    db_path = _database(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        queries.set_override_metrics(conn, 188, 168, 260)
    finally:
        conn.close()

    athlete_metrics_service.set_override_metrics(db_path, 190, 170)

    assert _profile_values(db_path)[2:] == (190, 170, None)


def test_clear_override_currently_clears_hr_lthr_and_ftp(tmp_path):
    db_path = _database(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        queries.set_override_metrics(conn, 188, 168, 260)
    finally:
        conn.close()

    athlete_metrics_service.clear_override_metrics(db_path)

    assert _profile_values(db_path)[2:] == (None, None, None)


def test_successful_manual_calculated_threshold_write_persists_hr_values(tmp_path):
    db_path = _database(tmp_path)

    athlete_metrics_service.set_calculated_metrics(db_path, 187, 166)

    assert _profile_values(db_path)[:2] == (187, 166)


def test_post_commit_exception_propagates_despite_durable_threshold_write(tmp_path):
    db_path = _database(tmp_path)
    raw = sqlite3.connect(db_path)

    class RaiseAfterCommit:
        rollback_calls = 0

        @property
        def in_transaction(self):
            return raw.in_transaction

        def execute(self, *args, **kwargs):
            return raw.execute(*args, **kwargs)

        def commit(self):
            raw.commit()
            raise sqlite3.OperationalError("raised after durable commit")

        def rollback(self):
            self.rollback_calls += 1
            raw.rollback()

    wrapped = RaiseAfterCommit()
    try:
        try:
            queries.set_override_metrics(wrapped, 192, 172, 265)
        except sqlite3.OperationalError as exc:
            assert str(exc) == "raised after durable commit"
        else:
            raise AssertionError("commit exception did not propagate")
        assert wrapped.rollback_calls == 0
    finally:
        raw.close()

    assert _profile_values(db_path)[2:] == (192, 172, 265)


def test_zero_in_threshold_ui_clears_hr_overrides_and_uses_calculated_fallback(
    tmp_path,
):
    db_path = _database(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 184, 164)
        queries.set_override_metrics(conn, 190, 170, 255)
    finally:
        conn.close()

    async def scenario() -> None:
        async with user_simulation(
            root=lambda: create_ui(db_path, sandboxed=True)
        ) as user:
            await user.open("/")
            await user.open("/plan")
            user.find("HRmax override (0 clears)").clear().type("0")
            user.find("LTHR override (0 clears)").clear().type("0")
            user.find("Save override").click()
            assert user.notify.contains("Threshold overrides saved")

    asyncio.run(scenario())

    assert _profile_values(db_path)[2:] == (None, None, None)
    assert athlete_metrics_service.get_athlete_metrics(db_path)[
        "hrmax_effective"
    ] == 184
    assert athlete_metrics_service.get_athlete_metrics(db_path)[
        "lthr_effective"
    ] == 164
