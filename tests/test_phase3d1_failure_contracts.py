"""Threshold regressions for persistence, refresh failures and independent overrides."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from nicegui.testing import user_simulation

from garmin_data_hub.analytics.athlete_profile import update_athlete_profile
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import athlete_metrics_service, coaching_packet
from garmin_data_hub.ui_nicegui import app as nicegui_app
from garmin_data_hub.ui_nicegui import pages


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "phase3d1-failure-contract.db"
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT,
                elapsed_duration_seconds REAL,
                moving_duration_seconds REAL,
                max_hr REAL,
                average_hr REAL,
                activity_type TEXT,
                avg_power REAL,
                max_power REAL,
                norm_power REAL
            )
            """
        )
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()
    return db_path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _profile(db_path: Path) -> sqlite3.Row:
    conn = _connect(db_path)
    try:
        return conn.execute(
            "SELECT * FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
    finally:
        conn.close()


def _control_values(user) -> tuple[int | float | None, int | float | None]:
    high = next(iter(user.find("HRmax override (0 clears)").elements))
    threshold = next(iter(user.find("LTHR override (0 clears)").elements))
    return high.value, threshold.value


def _run_ui(db_path: Path, scenario) -> None:
    async def run() -> None:
        async with user_simulation(
            root=lambda: nicegui_app.create_ui(db_path, sandboxed=True)
        ) as user:
            await user.open("/")
            await user.open("/plan")
            await scenario(user)

    asyncio.run(run())


def test_override_controls_load_raw_nulls_not_effective_calculated_values(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 184, 158)
    finally:
        conn.close()

    async def scenario(user):
        assert _control_values(user) == (None, None)

    _run_ui(db_path, scenario)


def test_calculated_and_effective_hr_values_have_separate_read_only_displays(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 184, 158)
    finally:
        conn.close()

    async def scenario(user):
        assert user.find("Calculated HRmax").elements
        assert user.find("Effective HRmax").elements
        assert user.find("Estimated LTHR").elements

    _run_ui(db_path, scenario)


def test_save_without_change_does_not_manufacture_manual_overrides(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 184, 158)
    finally:
        conn.close()

    async def scenario(user):
        user.find("Save override").click()
        await asyncio.sleep(0.05)

    _run_ui(db_path, scenario)
    row = _profile(db_path)
    assert (row["hrmax_override"], row["lthr_override"]) == (None, None)
    assert (row["hrmax_calc"], row["lthr_calc"]) == (184, 158)


def test_hr_save_preserves_unrelated_ftp_override(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_override_metrics(conn, None, None, 260)
    finally:
        conn.close()

    athlete_metrics_service.set_override_metrics(db_path, 190, 170)

    assert _profile(db_path)["ftp_override"] == 260


def test_hr_clear_preserves_unrelated_ftp_override(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_override_metrics(conn, 190, 170, 260)
    finally:
        conn.close()

    athlete_metrics_service.clear_override_metrics(db_path)

    row = _profile(db_path)
    assert (row["hrmax_override"], row["lthr_override"], row["ftp_override"]) == (
        None,
        None,
        260,
    )


def test_successful_clear_reloads_raw_override_controls_from_persisted_truth(
    tmp_path,
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_override_metrics(conn, 190, 170, None)
    finally:
        conn.close()

    async def scenario(user):
        user.find("Clear override").click()
        await asyncio.sleep(0.05)
        assert _control_values(user) == (None, None)

    _run_ui(db_path, scenario)


def test_successful_recalc_reloads_raw_overrides_not_new_calculated_values(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 180, 155)
        queries.set_override_metrics(conn, 190, 170, None)
    finally:
        conn.close()
    monkeypatch.setattr(
        pages,
        "calculate_metrics_from_db_sources",
        lambda *_args, **_kwargs: (185, 159, None),
    )

    async def scenario(user):
        user.find("Recalculate").click()
        await asyncio.sleep(0.15)
        assert _control_values(user) == (190, 170)

    _run_ui(db_path, scenario)
    row = _profile(db_path)
    assert (
        row["hrmax_calc"],
        row["lthr_calc"],
        row["hrmax_override"],
        row["lthr_override"],
    ) == (185, 159, 190, 170)




def test_equivalent_space_and_t_timestamps_include_the_exact_cutoff(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, elapsed_duration_seconds,
                moving_duration_seconds, max_hr, activity_type
            ) VALUES (1, '2026-06-30 12:00:00', 1200, 1200, 185, 'running')
            """
        )
        assert queries.get_hrmax_robust_and_lthr(
            conn, "2026-06-30T12:00:00"
        ) == (185, 159)
    finally:
        conn.close()


def test_hr_recalculation_does_not_refresh_ftp_calculation_age(monkeypatch, tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE athlete_profile
            SET hrmax_calc=180, lthr_calc=155, ftp_calc=250,
                calc_updated_utc='2025-01-01T00:00:00Z'
            WHERE profile_id=1
            """
        )
        monkeypatch.setattr(
            queries, "_utc_now_iso", lambda: "2026-09-28T12:00:00Z"
        )
        queries.set_calculated_metrics(conn, 185, 159)
        metrics = queries.get_athlete_metrics(conn)
        assert metrics["ftp_calculated_at"] == "2025-01-01T00:00:00Z"
        assert metrics["hrmax_calculated_at"] == "2026-09-28T12:00:00Z"
    finally:
        conn.close()


def test_ftp_recalculation_does_not_refresh_hr_calculation_age(monkeypatch, tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE athlete_profile
            SET hrmax_calc=180, lthr_calc=155, ftp_calc=250,
                calc_updated_utc='2025-01-01T00:00:00Z'
            WHERE profile_id=1
            """
        )
        monkeypatch.setattr(
            queries, "_utc_now_iso", lambda: "2026-09-28T12:00:00Z"
        )
        queries.set_calculated_ftp(conn, 275)
        metrics = queries.get_athlete_metrics(conn)
        assert metrics["hrmax_calculated_at"] == "2025-01-01T00:00:00Z"
        assert metrics["ftp_calculated_at"] == "2026-09-28T12:00:00Z"
    finally:
        conn.close()


@pytest.mark.parametrize("name", ["hrmax", "lthr", "ftp"])
@pytest.mark.parametrize("override", [0, -5])
def test_nonpositive_override_uses_calculated_source_consistently(name, override):
    metrics = {
        f"{name}_override": override,
        f"{name}_calc": 180,
        f"{name}_effective": 180,
    }
    assert coaching_packet._metric_source(metrics, name) == "unknown"


def test_default_resting_hr_is_exposed_as_default_not_measured_garmin():
    context = coaching_packet._build_athlete_context(
        {
            "hrmax_effective": 184,
            "lthr_effective": 158,
            "ftp_effective": None,
            "resting_hr": None,
        },
        {},
    )
    assert context["resting_heart_rate_bpm"] == 60
    assert context["resting_heart_rate_source"] == "default_fallback"
