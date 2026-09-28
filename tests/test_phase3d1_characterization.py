"""GREEN characterizations of the threshold behavior that Phase 3D.2 replaces.

These tests deliberately describe the repository at the Phase 3D.1 checkpoint.
Approved future behavior belongs in the two intentional-RED modules instead.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
import sqlite3
from pathlib import Path

import pytest
from nicegui.testing import user_simulation

from garmin_data_hub.analytics.athlete_profile import update_athlete_profile
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import athlete_metrics_service, coaching_packet, thresholds
from garmin_data_hub.ui_nicegui import app as nicegui_app
from garmin_data_hub.ui_nicegui import pages


NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


class FrozenDate(date):
    @classmethod
    def today(cls):
        return NOW.date()


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "phase3d1-characterization.db"
    conn = connect_sqlite(db_path)
    try:
        conn.executescript(
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
            );
            CREATE TABLE daily_summary (
                calendar_date TEXT PRIMARY KEY,
                resting_heart_rate INTEGER
            );
            CREATE TABLE sleep (
                calendar_date TEXT PRIMARY KEY,
                resting_heart_rate INTEGER
            );
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


def _insert_activity(
    conn: sqlite3.Connection,
    activity_id: int,
    *,
    start: str = "2026-09-01T06:00:00Z",
    sport: str = "running",
    max_hr: float | None = None,
    duration_s: float = 1200,
    norm_power: float | None = None,
    avg_power: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, max_hr, activity_type, norm_power, avg_power
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            activity_id,
            start,
            duration_s,
            duration_s,
            max_hr,
            sport,
            norm_power,
            avg_power,
        ),
    )


def _insert_hr_support(
    conn: sqlite3.Connection, activity_id: int, candidate: int, support_s: float = 5.0
) -> None:
    start = conn.execute(
        "SELECT start_time_gmt FROM activity WHERE activity_id=?", (activity_id,)
    ).fetchone()[0]
    base = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
    conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, heart_rate_bpm
        ) VALUES (?, ?, ?, ?)
        """,
        [
            (activity_id, 1, base.isoformat(), candidate - 2),
            (
                activity_id,
                2,
                (base + timedelta(seconds=support_s)).isoformat(),
                candidate,
            ),
        ],
    )


def _profile(db_path: Path) -> tuple:
    conn = _connect(db_path)
    try:
        return tuple(
            conn.execute(
                """
                SELECT hrmax_calc, lthr_calc, ftp_calc,
                       hrmax_override, lthr_override, ftp_override, resting_hr
                FROM athlete_profile WHERE profile_id=1
                """
            ).fetchone()
        )
    finally:
        conn.close()


def _run_ui(db_path: Path, scenario) -> None:
    async def run() -> None:
        async with user_simulation(
            root=lambda: nicegui_app.create_ui(db_path, sandboxed=True)
        ) as user:
            await user.open("/")
            await user.open("/plan")
            await scenario(user)

    asyncio.run(run())


@pytest.mark.parametrize(
    ("candidate_count", "expected"),
    [(1, 101), (10, 110), (201, 300)],
)
def test_current_hrmax_uses_floor_index_percentile_near_the_max(
    tmp_path, candidate_count, expected
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        for offset in range(candidate_count):
            _insert_activity(conn, offset + 1, max_hr=101 + offset)
        actual, lthr = queries.get_hrmax_robust_and_lthr(
            conn, "2026-01-01T00:00:00Z", percentile=0.995
        )
    finally:
        conn.close()

    assert actual == expected
    assert lthr == round(expected * 0.86)


def test_current_hrmax_includes_all_sport_summaries(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, sport="running", max_hr=181)
        _insert_activity(conn, 2, sport="cycling", max_hr=194)
        _insert_activity(conn, 3, sport="hiking", max_hr=188)
        assert queries.get_hrmax_robust_and_lthr(
            conn, "2026-01-01T00:00:00Z"
        ) == (194, 167)
    finally:
        conn.close()


def test_current_automatic_hr_window_is_90_days(monkeypatch, tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, start="2026-06-28T12:00:00Z", max_hr=210)
        _insert_activity(conn, 2, start="2026-09-01T12:00:00Z", max_hr=184)
        _insert_hr_support(conn, 1, 210)
        _insert_hr_support(conn, 2, 184)
        conn.commit()
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        update_athlete_profile(conn)
        row = conn.execute(
            "SELECT hrmax_calc, lthr_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert tuple(row) == (184, 158)
    finally:
        conn.close()


def test_current_manual_recalculation_uses_years_times_365(monkeypatch, tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, start="2021-09-27T12:00:00Z", max_hr=210)
        _insert_activity(conn, 2, start="2021-09-29T12:00:00Z", max_hr=185)
        _insert_hr_support(conn, 1, 210)
        _insert_hr_support(conn, 2, 185)
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(athlete_metrics_service, "date", FrozenDate)
    assert athlete_metrics_service.calculate_metrics_from_db_sources(
        db_path, years_back=5
    ) == (185, 159, None)


def test_effective_lthr_is_unavailable_without_validated_profile_value(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, start="2001-01-01T00:00:00Z", max_hr=180)
        assert queries.get_effective_lthr(conn) is None
        row = conn.execute(
            "SELECT hrmax_calc, lthr_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert tuple(row) == (None, None)
    finally:
        conn.close()


def test_current_automatic_refresh_retains_hr_values_when_no_evidence(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 187, 161)
        update_athlete_profile(conn)
        row = conn.execute(
            "SELECT hrmax_calc, lthr_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert tuple(row) == (187, 161)
    finally:
        conn.close()


def test_summary_power_and_nonrunning_sports_do_not_establish_running_ftp(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, sport="running", norm_power=400)
        _insert_activity(conn, 2, sport="hiking", norm_power=420)
        _insert_activity(conn, 3, sport="cycling", norm_power=250)
        assert queries._estimate_ftp_from_recent_power(conn) is None
    finally:
        conn.close()


def test_running_ftp_does_not_fall_back_to_other_sports_or_summary_power(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, sport="running", norm_power=280)
        _insert_activity(conn, 2, sport="hiking", norm_power=310)
        assert queries._estimate_ftp_from_recent_power(conn) is None
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("norm_power", "avg_power", "expected"),
    [(250, 400, None), (None, 300, None)],
)
def test_current_ftp_uses_summary_norm_power_then_average_power(
    tmp_path, norm_power, avg_power, expected
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(
            conn, 1, sport="cycling", norm_power=norm_power, avg_power=avg_power
        )
        assert queries._estimate_ftp_from_recent_power(conn) == expected
    finally:
        conn.close()


@pytest.mark.parametrize(("duration_s", "expected"), [(1200, None), (1199.9, None)])
def test_current_ftp_requires_1200_seconds_of_activity_elapsed_duration(
    tmp_path, duration_s, expected
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(
            conn, 1, sport="cycling", duration_s=duration_s, norm_power=250
        )
        assert queries._estimate_ftp_from_recent_power(conn) == expected
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("summary_power", "expected"),
    [(84, None), (526, None), (83, None), (527, None)],
)
def test_current_ftp_accepts_only_rounded_results_from_80_through_500(
    tmp_path, summary_power, expected
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, sport="cycling", norm_power=summary_power)
        assert queries._estimate_ftp_from_recent_power(conn) == expected
    finally:
        conn.close()


def test_running_ftp_requires_trackpoint_power_coverage(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, sport="cycling", norm_power=300)
        assert conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints"
        ).fetchone()[0] == 0
        assert queries._estimate_ftp_from_recent_power(conn) is None
    finally:
        conn.close()


def test_legacy_ftp_helper_does_not_admit_cycling_at_cutoff(monkeypatch, tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        monkeypatch.setattr(queries, "datetime", FrozenDateTime)
        _insert_activity(
            conn,
            1,
            start="2026-03-31T11:59:59Z",
            sport="cycling",
            norm_power=400,
        )
        _insert_activity(
            conn,
            2,
            start="2026-04-01T12:00:00Z",
            sport="cycling",
            norm_power=250,
        )
        assert queries._estimate_ftp_from_recent_power(conn) is None
    finally:
        conn.close()


def test_current_effective_ftp_short_circuits_on_existing_calculation(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_ftp(conn, 200)
        _insert_activity(conn, 1, sport="cycling", norm_power=300)
        assert queries.get_effective_ftp(conn) == 200
    finally:
        conn.close()


def test_automatic_refresh_never_copies_ftp_override_to_calculation(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_ftp(conn, 200)
        queries.set_override_metrics(conn, None, None, 260)
        update_athlete_profile(conn)
        row = conn.execute(
            "SELECT ftp_calc, ftp_override FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert tuple(row) == (200, 260)
    finally:
        conn.close()


@pytest.mark.parametrize(("stored", "expected"), [(48, 48), (0, 60), (None, 60)])
def test_current_resting_hr_comes_from_profile_or_falls_back_to_60(
    tmp_path, stored, expected
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE athlete_profile SET resting_hr=? WHERE profile_id=1", (stored,)
        )
        assert queries.get_current_activity_metric_provenance(conn)[2] == expected
    finally:
        conn.close()


def test_resting_hr_uses_daily_garmin_value_before_same_day_sleep(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute("INSERT INTO daily_summary VALUES ('2026-09-27', 47)")
        conn.execute("INSERT INTO sleep VALUES ('2026-09-27', 49)")
        assert queries.get_current_activity_metric_provenance(conn)[2] == 47
    finally:
        conn.close()


def test_ui_loads_raw_hr_overrides_not_calculated_values(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 184, 158)
    finally:
        conn.close()

    async def scenario(user):
        high = next(iter(user.find("HRmax override (0 clears)").elements))
        threshold = next(iter(user.find("LTHR override (0 clears)").elements))
        assert (high.value, threshold.value) == (None, None)

    _run_ui(db_path, scenario)


def test_ui_save_without_edits_preserves_null_hr_and_existing_ftp_override(
    tmp_path,
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_calculated_metrics(conn, 184, 158)
        queries.set_override_metrics(conn, None, None, 260)
    finally:
        conn.close()

    async def scenario(user):
        user.find("Save override").click()
        await asyncio.sleep(0.05)

    _run_ui(db_path, scenario)
    assert _profile(db_path)[3:6] == (None, None, 260)


def test_ui_clear_reloads_empty_hr_controls_and_preserves_ftp(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_override_metrics(conn, 190, 170, 260)
    finally:
        conn.close()

    async def scenario(user):
        user.find("Clear override").click()
        await asyncio.sleep(0.05)
        high = next(iter(user.find("HRmax override (0 clears)").elements))
        threshold = next(iter(user.find("LTHR override (0 clears)").elements))
        assert (high.value, threshold.value) == (None, None)

    _run_ui(db_path, scenario)
    assert _profile(db_path)[3:6] == (None, None, 260)


def test_ui_recalc_keeps_raw_overrides_visible_and_effective(
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
        high = next(iter(user.find("HRmax override (0 clears)").elements))
        threshold = next(iter(user.find("LTHR override (0 clears)").elements))
        assert (high.value, threshold.value) == (190, 170)

    _run_ui(db_path, scenario)
    assert _profile(db_path)[:5] == (185, 159, None, 190, 170)


def test_space_timestamp_equal_to_t_cutoff_is_included_by_datetime_semantics(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(
            conn, 1, start="2026-06-30 12:00:00", sport="running", max_hr=185
        )
        assert queries.get_hrmax_robust_and_lthr(
            conn, "2026-06-30T12:00:00"
        ) == (185, 159)
    finally:
        conn.close()


@pytest.mark.parametrize("override", [0, -5])
def test_coaching_source_ignores_nonpositive_override(
    override,
):
    metrics = {
        "hrmax_override": override,
        "hrmax_calc": 184,
        "hrmax_effective": 184,
    }
    assert coaching_packet._metric_source(metrics, "hrmax") == "unknown"


@pytest.mark.parametrize("name", ["hrmax", "lthr", "ftp"])
def test_current_coaching_source_handles_positive_override_and_calculation(name):
    source_kind = {
        "hrmax": "validated_hrmax",
        "lthr": "estimated_from_validated_hrmax",
        "ftp": "running_ftp",
    }[name]
    calculated = {
        f"{name}_override": None,
        f"{name}_calc": 180,
        f"{name}_provenance": {"source_kind": source_kind},
    }
    overridden = {f"{name}_override": 190, f"{name}_calc": 180}
    assert coaching_packet._metric_source(calculated, name) == source_kind
    assert coaching_packet._metric_source(overridden, name) == "athlete_override"
