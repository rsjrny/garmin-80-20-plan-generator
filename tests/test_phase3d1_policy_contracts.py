"""Approved Phase 3D product-policy contracts (mostly intentional RED).

The tests use temporary databases and real production seams.  The few contracts
that need the future provenance/staleness service assert its smallest proposed
API directly; they are intentionally not xfailed.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import sqlite3
from pathlib import Path

import pytest
from nicegui.testing import user_simulation

from garmin_data_hub.analytics import temporal_metrics
from garmin_data_hub.analytics.athlete_profile import update_athlete_profile
from garmin_data_hub.analytics.temporal_metrics import TemporalSample
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import coaching_packet, thresholds
from garmin_data_hub.ui_nicegui import app as nicegui_app


NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
RUNNING_FAMILY = ("running", "trail_running", "indoor_running")


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


def _database(tmp_path: Path) -> Path:
    db_path = tmp_path / "phase3d1-policy-contract.db"
    conn = connect_sqlite(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT,
                elapsed_duration_seconds REAL,
                moving_duration_seconds REAL,
                average_speed REAL,
                max_hr REAL,
                average_hr REAL,
                activity_type TEXT,
                avg_power REAL,
                max_power REAL,
                norm_power REAL,
                training_stress_score REAL,
                intensity_factor REAL
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
    sport: str = "running",
    start: str = "2026-09-01T06:00:00Z",
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


def _power_samples(power_w: float, support_s: float = 1200) -> list[TemporalSample]:
    timestamps: list[float] = []
    current = 0.0
    while current < support_s:
        timestamps.append(current)
        current = min(current + 30.0, support_s)
    timestamps.append(support_s)
    return [TemporalSample(second, power_w=power_w) for second in timestamps]


def _insert_power_trackpoints(
    conn: sqlite3.Connection,
    activity_id: int,
    power_w: float,
    *,
    support_s: float = 1200,
) -> float | None:
    samples = _power_samples(power_w, support_s)
    result = temporal_metrics.calculate_temporal_metrics(samples)
    base = NOW - timedelta(days=20)
    conn.executemany(
        """
        INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc, power_w)
        VALUES (?, ?, ?, ?)
        """,
        [
            (
                activity_id,
                index + 1,
                (base + timedelta(seconds=float(sample.timestamp_utc))).isoformat(),
                sample.power_w,
            )
            for index, sample in enumerate(samples)
        ],
    )
    return result.power_peaks_w[1200]


def _insert_hr_support(
    conn: sqlite3.Connection,
    activity_id: int,
    candidate: int,
    support_s: float,
) -> None:
    base = NOW - timedelta(days=20, hours=activity_id)
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


def _refresh_temporal(conn: sqlite3.Connection, activity_ids: list[int]) -> None:
    conn.commit()
    summary = queries.refresh_persisted_activity_metrics(
        conn, activity_ids=activity_ids, lthr=160
    )
    assert summary["errors"] == 0


@pytest.mark.parametrize("running_sport", RUNNING_FAMILY)
def test_running_ftp_uses_only_the_exact_running_family_sports(
    monkeypatch, tmp_path, running_sport
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        fixtures = [
            (1, running_sport, 200),
            (2, "hiking", 350),
            (3, "cycling", 300),
        ]
        for activity_id, sport, power in fixtures:
            _insert_activity(
                conn, activity_id, sport=sport, norm_power=power, avg_power=power
            )
            assert _insert_power_trackpoints(conn, activity_id, power) == power
        _refresh_temporal(conn, [1, 2, 3])

        update_athlete_profile(conn)

        row = conn.execute(
            "SELECT ftp_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert row[0] == round(200 * 0.95)
    finally:
        conn.close()


def test_automatic_ftp_reestimates_even_when_ftp_calc_already_exists(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        _insert_activity(conn, 1, sport="running", norm_power=300, avg_power=300)
        peak = _insert_power_trackpoints(conn, 1, 300)
        assert peak == 300
        _refresh_temporal(conn, [1])
        queries.set_calculated_ftp(conn, 200)

        update_athlete_profile(conn)

        row = conn.execute(
            "SELECT ftp_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert row[0] == round(peak * 0.95)
    finally:
        conn.close()


def test_running_ftp_requires_canonical_complete_1200_second_power_support(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        # Summary-only power must not qualify.
        _insert_activity(conn, 1, sport="running", norm_power=300, avg_power=300)
        # Elapsed duration and intermittent support must not qualify.
        _insert_activity(conn, 2, sport="running", duration_s=1800)
        assert _insert_power_trackpoints(conn, 2, 280, support_s=1199) is None
        # This is the sole complete canonical 20-minute effort.
        _insert_activity(conn, 3, sport="running")
        peak = _insert_power_trackpoints(conn, 3, 200)
        assert peak == 200
        _refresh_temporal(conn, [1, 2, 3])

        update_athlete_profile(conn)

        row = conn.execute(
            "SELECT ftp_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()
        assert row[0] == round(peak * 0.95)
    finally:
        conn.close()


def test_running_ftp_rounding_uses_integer_bankers_rounding_of_peak_times_095(
    monkeypatch, tmp_path
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        _insert_activity(conn, 1, sport="running", norm_power=211, avg_power=211)
        peak = _insert_power_trackpoints(conn, 1, 211)
        _refresh_temporal(conn, [1])
        update_athlete_profile(conn)
        stored = conn.execute(
            "SELECT ftp_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0]
        assert peak == 211
        assert round(peak * 0.95) == 200
        assert stored == 200
    finally:
        conn.close()


def test_zero_watts_remain_measured_but_do_not_create_a_bounded_ftp():
    peak = temporal_metrics.calculate_temporal_metrics(
        _power_samples(0)
    ).power_peaks_w[1200]
    assert peak == 0
    assert round(peak * 0.95) == 0


def test_hrmax_rejects_candidates_outside_100_through_220(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        for activity_id, candidate in enumerate((99, 221, 190), start=1):
            _insert_activity(conn, activity_id, max_hr=candidate)
            _insert_hr_support(conn, activity_id, candidate, 5)
        update_athlete_profile(conn)
        actual = conn.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0]
        assert actual == 190
    finally:
        conn.close()


def test_hrmax_requires_at_least_five_supported_seconds_within_two_bpm(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, max_hr=205)
        _insert_hr_support(conn, 1, 205, 5.0)
        _insert_activity(conn, 2, max_hr=210)
        _insert_hr_support(conn, 2, 210, 4.999)
        update_athlete_profile(conn)
        actual = conn.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0]
        assert actual == 205
    finally:
        conn.close()


def test_unsupported_summary_spike_cannot_beat_highest_validated_hrmax(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        _insert_activity(conn, 1, max_hr=200)
        _insert_hr_support(conn, 1, 200, 5.0)
        _insert_activity(conn, 2, max_hr=215)
        update_athlete_profile(conn)
        actual = conn.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0]
        assert actual == 200
    finally:
        conn.close()


def test_calculated_lthr_source_is_identified_as_estimated_from_validated_hrmax():
    metrics = {
        "lthr_override": None,
        "lthr_calc": 159,
        "lthr_effective": 159,
        "lthr_provenance": {
            "source_kind": "estimated_from_validated_hrmax"
        },
    }
    assert coaching_packet._metric_source(metrics, "lthr") == (
        "estimated_from_validated_hrmax"
    )


def test_threshold_ui_uses_estimated_lthr_terminology(tmp_path):
    db_path = _database(tmp_path)

    async def scenario() -> None:
        async with user_simulation(
            root=lambda: nicegui_app.create_ui(db_path, sandboxed=True)
        ) as user:
            await user.open("/")
            await user.open("/plan")
            assert user.find("Estimated LTHR").elements

    asyncio.run(scenario())


def test_resting_hr_is_median_of_latest_seven_available_days_with_daily_preference(
    tmp_path,
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        # daily_summary is the canonical daily Garmin value.  sleep is used only
        # when that date has no daily_summary value, preventing double weighting.
        conn.executemany(
            "INSERT INTO daily_summary VALUES (?, ?)",
            [
                ("2026-09-27", 50),
                ("2026-09-23", 52),
                ("2026-09-18", 54),
                ("2026-09-01", 56),
                ("2026-08-20", 10),
            ],
        )
        conn.executemany(
            "INSERT INTO sleep VALUES (?, ?)",
            [
                ("2026-09-27", 80),  # loses same-day precedence
                ("2026-09-25", 51),
                ("2026-09-20", 53),
                ("2026-09-10", 55),
            ],
        )
        conn.commit()

        effective = queries.get_current_activity_metric_provenance(conn)[2]

        assert effective == 53
        provenance = queries.get_athlete_metrics(conn)["resting_hr_provenance"]
        assert provenance["latest_evidence_date"] == "2026-09-27"
        assert provenance["observation_count"] == 7
        assert provenance["same_day_preference"] == "daily_summary"
    finally:
        conn.close()


def test_resting_hr_provenance_keeps_latest_evidence_date_and_aggregate(tmp_path):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute("INSERT INTO daily_summary VALUES ('2026-09-27', 50)")
        conn.execute("UPDATE athlete_profile SET resting_hr=50 WHERE profile_id=1")
        metrics = queries.get_athlete_metrics(conn)
        assert "resting_hr_provenance" in metrics
        assert metrics["resting_hr_provenance"] == {
            "source_kind": "garmin_daily_aggregate",
            "algorithm_version": metrics["resting_hr_provenance"][
                "algorithm_version"
            ],
            "latest_evidence_date": "2026-09-27",
            "observations": [{"date": "2026-09-27", "value_bpm": 50}],
            "observation_count": 1,
        }
    finally:
        conn.close()


def test_threshold_staleness_boundaries_are_derived_and_values_are_retained():
    derive = getattr(queries, "derive_threshold_status", None)
    assert callable(derive), "Phase 3D.2 must add the threshold staleness service seam"

    cases = [
        ("hrmax", 184, 90, 90, "current"),
        ("hrmax", 184, 90, 90 + 1 / 86400, "stale"),
        ("estimated_lthr", 158, 90, 90, "current"),
        ("estimated_lthr", 158, 90, 91, "stale"),
        ("running_ftp", 250, 180, 180, "current"),
        ("running_ftp", 250, 180, 181, "stale"),
        ("resting_hr", 50, 14, 14, "current"),
        ("resting_hr", 50, 14, 15, "stale"),
    ]
    for name, value, stale_after_days, age_days, expected in cases:
        state = derive(
            threshold=name,
            calculated_value=value,
            override_value=None,
            evidence_at_utc=NOW - timedelta(days=age_days),
            as_of_utc=NOW,
            stale_after_days=stale_after_days,
        )
        assert state["source_age_status"] == expected
        assert state["calculated_value"] == value
        assert state["effective_value"] == value
        assert "activity_metric_freshness" not in state


def test_stale_calculation_remains_effective_until_manual_override_supersedes_it():
    derive = getattr(queries, "derive_threshold_status", None)
    assert callable(derive), "Phase 3D.2 must add the threshold staleness service seam"
    state = derive(
        threshold="running_ftp",
        calculated_value=250,
        override_value=275,
        evidence_at_utc=NOW - timedelta(days=181),
        as_of_utc=NOW,
        stale_after_days=180,
    )
    assert state["source_age_status"] == "stale"
    assert state["calculated_value"] == 250
    assert state["effective_value"] == 275
    assert state["effective_source"] == "manual_override"


@pytest.mark.parametrize(
    ("metric", "required_fields"),
    [
        (
            "hrmax",
            {
                "algorithm_version",
                "calculated_at_utc",
                "evidence_cutoff_utc",
                "source_activity_id",
                "source_activity_timestamp_utc",
                "source_sport",
                "evidence_hr_bpm",
                "candidate_count",
            },
        ),
        (
            "lthr",
            {
                "algorithm_version",
                "calculated_at_utc",
                "source_kind",
                "source_hrmax_bpm",
                "source_hrmax_provenance_id",
            },
        ),
        (
            "ftp",
            {
                "algorithm_version",
                "calculated_at_utc",
                "evidence_cutoff_utc",
                "source_activity_id",
                "source_activity_timestamp_utc",
                "source_sport",
                "evidence_peak_power_w",
                "evidence_duration_s",
            },
        ),
        (
            "resting_hr",
            {
                "algorithm_version",
                "calculated_at_utc",
                "latest_evidence_date",
                "observations",
                "observation_count",
                "source_kind",
            },
        ),
    ],
)
def test_calculated_thresholds_expose_minimum_explainable_provenance(
    tmp_path, metric, required_fields
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE athlete_profile
            SET hrmax_calc=184, lthr_calc=158, ftp_calc=250, resting_hr=50
            WHERE profile_id=1
            """
        )
        metrics = queries.get_athlete_metrics(conn)
        key = f"{metric}_provenance"
        assert key in metrics
        provenance = metrics[key]
        assert provenance["source_kind"] != "manual_override"
        assert required_fields <= set(provenance)
        if metric == "lthr":
            assert provenance["source_kind"] == "unknown"
            assert provenance["source_hrmax_bpm"] is None
            assert provenance["source_hrmax_provenance_id"] is None
        if metric == "ftp":
            assert provenance["evidence_duration_s"] is None
    finally:
        conn.close()


def test_manual_override_without_calculation_has_no_fake_calculated_provenance(
    tmp_path,
):
    db_path = _database(tmp_path)
    conn = _connect(db_path)
    try:
        queries.set_override_metrics(conn, 190, 170, 275)
        metrics = queries.get_athlete_metrics(conn)
        for metric in ("hrmax", "lthr", "ftp"):
            key = f"{metric}_provenance"
            assert key in metrics
            assert metrics[key] is None
            assert metrics[f"{metric}_override"] > 0
    finally:
        conn.close()
