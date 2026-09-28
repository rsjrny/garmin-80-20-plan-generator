from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3
from pathlib import Path

import pytest

from garmin_data_hub.analytics.athlete_profile import update_athlete_profile
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema, get_current_schema_version
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import coaching_packet, thresholds


NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
REFERENCE_COLUMNS = {
    "hrmax_calculation_id",
    "lthr_calculation_id",
    "ftp_calculation_id",
    "resting_hr_calculation_id",
}


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


def _create_activity_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE activity (
            activity_id INTEGER PRIMARY KEY,
            start_time_gmt TEXT,
            activity_type TEXT,
            distance_meters REAL,
            elapsed_duration_seconds REAL,
            moving_duration_seconds REAL,
            average_hr REAL,
            max_hr REAL,
            avg_power REAL,
            max_power REAL,
            norm_power REAL,
            training_stress_score REAL,
            start_latitude REAL,
            start_longitude REAL
        )
        """
    )


def _threshold_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    _create_activity_table(conn)
    conn.executescript(
        """
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
    return conn


def _insert_hr_support(
    conn: sqlite3.Connection,
    activity_id: int,
    started_at: str,
    candidate: int,
) -> None:
    base = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, heart_rate_bpm
        ) VALUES (?, ?, ?, ?)
        """,
        [
            (activity_id, 1, base.isoformat(), candidate - 2),
            (activity_id, 2, (base + timedelta(seconds=5)).isoformat(), candidate),
        ],
    )


@pytest.mark.parametrize(
    "threshold_type",
    ["hrmax", "estimated_lthr", "running_ftp", "resting_hr"],
)
def test_orphan_unknown_provenance_contains_no_invented_evidence(threshold_type):
    provenance = thresholds.unknown_provenance(
        threshold_type,
        180,
        calculated_at_utc="2025-01-01T00:00:00Z",
    )

    assert provenance["source_kind"] == "unknown"
    assert provenance["algorithm_version"] == thresholds.LEGACY_UNKNOWN_ALGORITHM_VERSION
    assert provenance["evidence_at_utc"] is None
    assert provenance["source_activity_id"] is None
    assert provenance["source_activity_timestamp_utc"] is None
    assert provenance["source_sport"] is None
    assert provenance["evidence_duration_s"] is None
    assert provenance["parent_calculation_id"] is None
    if threshold_type == "estimated_lthr":
        assert provenance["source_hrmax_bpm"] is None
        assert provenance["source_hrmax_provenance_id"] is None
    if threshold_type == "running_ftp":
        assert provenance["evidence_peak_power_w"] is None
    if threshold_type == "resting_hr":
        assert provenance["observations"] == []
        assert provenance["observation_count"] == 0
        assert provenance["same_day_preference"] is None


def test_numeric_only_calculated_setter_does_not_invent_lthr_ancestry(tmp_path):
    conn = _threshold_db(tmp_path / "numeric-only-setter.db")
    try:
        queries.set_calculated_metrics(conn, 184, 158)
        provenance = queries.get_athlete_metrics(conn)["lthr_provenance"]
        assert provenance["source_kind"] == "unknown"
        assert provenance["algorithm_version"] == (
            thresholds.LEGACY_UNKNOWN_ALGORITHM_VERSION
        )
        assert provenance["source_hrmax_bpm"] is None
        assert provenance["source_hrmax_provenance_id"] is None
    finally:
        conn.close()


def _downgrade_fresh_database_to_v6_with_legacy_profile(
    conn: sqlite3.Connection,
) -> None:
    conn.executescript(
        """
        DROP TABLE athlete_profile;
        DROP TABLE threshold_calculation;
        DELETE FROM schema_migrations WHERE version=7;
        CREATE TABLE athlete_profile (
            profile_id INTEGER PRIMARY KEY DEFAULT 1,
            hrmax_calc INTEGER,
            lthr_calc INTEGER,
            ftp_calc INTEGER,
            calc_updated_utc TEXT,
            hrmax_override INTEGER,
            lthr_override INTEGER,
            ftp_override INTEGER,
            resting_hr INTEGER,
            override_updated_utc TEXT
        );
        INSERT INTO athlete_profile VALUES (
            1, 184, 158, 251, '2025-01-01T00:00:00Z',
            NULL, NULL, NULL, 49, NULL
        );
        """
    )
    conn.commit()


def test_migrated_unknown_thresholds_stay_unknown_in_real_coaching_packet(tmp_path):
    db_path = tmp_path / "migrated-coaching.db"
    conn = sqlite3.connect(db_path)
    try:
        _create_activity_table(conn)
        apply_schema(conn, schema_sql_path())
        _downgrade_fresh_database_to_v6_with_legacy_profile(conn)
        apply_schema(conn, schema_sql_path())
    finally:
        conn.close()

    packet = coaching_packet.build_coaching_packet(db_path, as_of="2026-09-28")
    constraints = packet["context"]["training_constraints"]
    assert constraints["max_heart_rate_source"] == "unknown"
    assert constraints["lactate_threshold_heart_rate_source"] == "unknown"
    assert constraints["functional_threshold_power_source"] == "unknown"
    assert constraints["resting_heart_rate_source"] == "unknown"


@pytest.mark.parametrize(
    ("name", "source_kind"),
    [
        ("hrmax", "validated_hrmax"),
        ("lthr", "estimated_from_validated_hrmax"),
        ("ftp", "running_ftp"),
    ],
)
def test_coaching_uses_canonical_calculated_provenance(name, source_kind):
    metrics = {
        f"{name}_calc": 180,
        f"{name}_override": None,
        f"{name}_provenance": {"source_kind": source_kind},
    }
    assert coaching_packet._metric_source(metrics, name) == source_kind


def test_coaching_uses_manual_garmin_resting_and_default_sources():
    assert coaching_packet._metric_source(
        {"hrmax_calc": 184, "hrmax_override": 190}, "hrmax"
    ) == "athlete_override"
    assert coaching_packet._metric_source(
        {
            "resting_hr": 48,
            "resting_hr_provenance": {
                "source_kind": "garmin_daily_aggregate"
            },
        },
        "resting_hr",
    ) == "garmin_daily_aggregate"
    assert coaching_packet._metric_source(
        {"resting_hr": None}, "resting_hr"
    ) == "default_fallback"


def _legacy_v6(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT);
        CREATE TABLE activity_metrics (
            activity_id INTEGER PRIMARY KEY,
            refresh_provenance_version INTEGER,
            threshold_lthr_bpm INTEGER,
            threshold_ftp_w INTEGER,
            threshold_resting_hr_bpm INTEGER
        );
        CREATE TABLE athlete_profile (
            profile_id INTEGER PRIMARY KEY DEFAULT 1,
            hrmax_calc INTEGER,
            lthr_calc INTEGER,
            ftp_calc INTEGER,
            calc_updated_utc TEXT,
            hrmax_override INTEGER,
            lthr_override INTEGER,
            ftp_override INTEGER,
            resting_hr INTEGER,
            override_updated_utc TEXT
        );
        INSERT INTO athlete_profile(profile_id) VALUES (1);
        CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.executemany(
        "INSERT INTO schema_migrations(version, name) VALUES (?, ?)",
        [(version, f"migration {version}") for version in range(1, 7)],
    )
    conn.commit()


def _schema_shape(conn: sqlite3.Connection) -> dict[str, object]:
    threshold_columns = [
        tuple(row[1:6]) for row in conn.execute("PRAGMA table_info(threshold_calculation)")
    ]
    profile_columns = [
        tuple(row[1:6])
        for row in conn.execute("PRAGMA table_info(athlete_profile)")
        if row[1] in REFERENCE_COLUMNS
    ]
    threshold_fks = sorted(
        (row[2], row[3], row[4])
        for row in conn.execute("PRAGMA foreign_key_list(threshold_calculation)")
    )
    profile_fks = sorted(
        (row[2], row[3], row[4])
        for row in conn.execute("PRAGMA foreign_key_list(athlete_profile)")
        if row[3] in REFERENCE_COLUMNS
    )
    indexes = {
        row[1] for row in conn.execute("PRAGMA index_list(threshold_calculation)")
    }
    return {
        "version": get_current_schema_version(conn),
        "threshold_columns": threshold_columns,
        "profile_columns": profile_columns,
        "threshold_fks": threshold_fks,
        "profile_fks": profile_fks,
        "indexes": indexes,
    }


def test_fresh_migrated_and_compatibility_initialization_have_schema_parity(tmp_path):
    shapes = []
    for name in ("fresh", "migrated", "compatibility"):
        conn = sqlite3.connect(tmp_path / f"{name}.db")
        try:
            if name == "fresh":
                conn.execute(
                    "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
                )
            elif name == "migrated":
                _legacy_v6(conn)
            else:
                conn.execute(
                    "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
                )
                queries.ensure_athlete_profile_table(conn)
            apply_schema(conn, schema_sql_path())
            shapes.append(_schema_shape(conn))
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        finally:
            conn.close()

    assert shapes[0] == shapes[1] == shapes[2]
    assert len(shapes[0]["threshold_fks"]) == 1
    assert len(shapes[0]["profile_fks"]) == 4
    assert "idx_threshold_calculation_type_id" in shapes[0]["indexes"]


def test_v5_to_v6_to_v7_preserves_values_and_adds_honest_provenance(tmp_path):
    conn = sqlite3.connect(tmp_path / "v5-chain.db")
    try:
        conn.executescript(
            """
            CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT);
            CREATE TABLE activity_metrics (
                activity_id INTEGER PRIMARY KEY,
                lthr_est_bpm REAL,
                peak_power_1200s_w REAL
            );
            INSERT INTO activity_metrics VALUES (7, 158, NULL);
            CREATE TABLE athlete_profile (
                profile_id INTEGER PRIMARY KEY DEFAULT 1,
                hrmax_calc INTEGER,
                lthr_calc INTEGER,
                ftp_calc INTEGER,
                calc_updated_utc TEXT,
                hrmax_override INTEGER,
                lthr_override INTEGER,
                ftp_override INTEGER,
                resting_hr INTEGER,
                override_updated_utc TEXT
            );
            INSERT INTO athlete_profile VALUES (
                1, 184, 158, 251, '2025-01-01T00:00:00Z',
                190, NULL, 275, NULL, '2026-01-01T00:00:00Z'
            );
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.executemany(
            "INSERT INTO schema_migrations(version, name) VALUES (?, ?)",
            [(version, f"migration {version}") for version in range(1, 6)],
        )
        conn.commit()

        apply_schema(conn, schema_sql_path())

        assert get_current_schema_version(conn) == 7
        metric_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(activity_metrics)")
        }
        assert {
            "refresh_provenance_version",
            "threshold_lthr_bpm",
            "threshold_ftp_w",
            "threshold_resting_hr_bpm",
        } <= metric_columns
        assert conn.execute(
            """
            SELECT refresh_provenance_version, threshold_lthr_bpm,
                   threshold_ftp_w, threshold_resting_hr_bpm
            FROM activity_metrics WHERE activity_id=7
            """
        ).fetchone() == (None, None, None, None)
        profile = conn.execute(
            """
            SELECT hrmax_calc, lthr_calc, ftp_calc, resting_hr,
                   hrmax_override, lthr_override, ftp_override
            FROM athlete_profile WHERE profile_id=1
            """
        ).fetchone()
        assert profile == (184, 158, 251, None, 190, None, 275)
        rows = conn.execute(
            """
            SELECT threshold_type, source_kind, source_activity_id,
                   evidence_value, evidence_duration_s, parent_calculation_id
            FROM threshold_calculation ORDER BY threshold_calculation_id
            """
        ).fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ("hrmax", "unknown"),
            ("estimated_lthr", "unknown"),
            ("running_ftp", "unknown"),
        ]
        assert all(all(value is None for value in row[2:]) for row in rows)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_top_level_commit_false_is_invisible_to_observer_and_rollbackable(tmp_path):
    db_path = tmp_path / "observer.db"
    writer = _threshold_db(db_path)
    observer = sqlite3.connect(db_path)
    try:
        started_at = "2026-09-27T12:00:00Z"
        writer.execute(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, elapsed_duration_seconds,
                moving_duration_seconds, max_hr, activity_type
            ) VALUES (1, ?, 1200, 1200, 190, 'running')
            """,
            (started_at,),
        )
        _insert_hr_support(writer, 1, started_at, 190)
        writer.commit()

        thresholds.refresh_thresholds(
            writer,
            activity_metric_provenance_version=(
                queries.ACTIVITY_METRICS_PROVENANCE_VERSION
            ),
            as_of_utc=NOW,
            commit=False,
        )

        assert writer.in_transaction is True
        assert writer.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0] == 190
        assert observer.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0] is None

        writer.rollback()
        assert observer.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0] is None
        assert observer.execute(
            "SELECT COUNT(*) FROM threshold_calculation"
        ).fetchone()[0] == 0
    finally:
        observer.close()
        writer.close()


def test_commit_false_preserves_caller_owned_transaction(tmp_path):
    db_path = tmp_path / "caller-transaction.db"
    writer = _threshold_db(db_path)
    observer = sqlite3.connect(db_path)
    try:
        started_at = "2026-09-27T12:00:00Z"
        writer.execute(
            """
            INSERT INTO activity(
                activity_id, start_time_gmt, elapsed_duration_seconds,
                moving_duration_seconds, max_hr, activity_type
            ) VALUES (1, ?, 1200, 1200, 190, 'running')
            """,
            (started_at,),
        )
        _insert_hr_support(writer, 1, started_at, 190)
        writer.commit()

        writer.execute("BEGIN")
        writer.execute(
            "INSERT INTO app_settings(key, value) VALUES ('caller_sentinel', 'pending')"
        )
        thresholds.refresh_thresholds(
            writer,
            activity_metric_provenance_version=(
                queries.ACTIVITY_METRICS_PROVENANCE_VERSION
            ),
            as_of_utc=NOW,
            commit=False,
        )

        assert writer.in_transaction is True
        assert writer.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0] == 190
        assert observer.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0] is None
        assert observer.execute(
            "SELECT value FROM app_settings WHERE key='caller_sentinel'"
        ).fetchone() is None

        writer.rollback()
        assert observer.execute(
            "SELECT hrmax_calc FROM athlete_profile WHERE profile_id=1"
        ).fetchone()[0] is None
        assert observer.execute(
            "SELECT value FROM app_settings WHERE key='caller_sentinel'"
        ).fetchone() is None
    finally:
        observer.close()
        writer.close()


@pytest.mark.parametrize(
    ("exact", "one_second_before"),
    [
        ("2026-06-30T12:00:00", "2026-06-30T11:59:59"),
        ("2026-06-30 12:00:00", "2026-06-30 11:59:59"),
    ],
)
def test_automatic_hr_uses_exact_90_day_cutoff_on_canonical_path(
    monkeypatch, tmp_path, exact, one_second_before
):
    conn = _threshold_db(tmp_path / "automatic-hr.db")
    try:
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        for activity_id, started_at, candidate in (
            (1, exact, 190),
            (2, one_second_before, 210),
        ):
            conn.execute(
                """
                INSERT INTO activity(
                    activity_id, start_time_gmt, elapsed_duration_seconds,
                    moving_duration_seconds, max_hr, activity_type
                ) VALUES (?, ?, 1200, 1200, ?, 'running')
                """,
                (activity_id, started_at, candidate),
            )
            _insert_hr_support(conn, activity_id, started_at, candidate)
        conn.commit()

        update_athlete_profile(conn)

        metrics = queries.get_athlete_metrics(conn)
        assert metrics["hrmax_calc"] == 190
        assert metrics["hrmax_provenance"]["source_activity_id"] == 1
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("exact", "one_second_before"),
    [
        ("2026-04-01T12:00:00", "2026-04-01T11:59:59"),
        ("2026-04-01 12:00:00", "2026-04-01 11:59:59"),
    ],
)
def test_automatic_running_ftp_uses_exact_180_day_cutoff_on_canonical_path(
    monkeypatch, tmp_path, exact, one_second_before
):
    conn = _threshold_db(tmp_path / "automatic-ftp.db")
    try:
        monkeypatch.setattr(thresholds, "datetime", FrozenDateTime)
        for activity_id, started_at, peak in (
            (1, exact, 200.0),
            (2, one_second_before, 400.0),
        ):
            conn.execute(
                """
                INSERT INTO activity(
                    activity_id, start_time_gmt, elapsed_duration_seconds,
                    moving_duration_seconds, activity_type
                ) VALUES (?, ?, 1200, 1200, 'running')
                """,
                (activity_id, started_at),
            )
            conn.execute(
                """
                INSERT INTO activity_metrics(
                    activity_id, peak_power_1200s_w, refresh_provenance_version
                ) VALUES (?, ?, ?)
                """,
                (
                    activity_id,
                    peak,
                    queries.ACTIVITY_METRICS_PROVENANCE_VERSION,
                ),
            )
        conn.commit()

        update_athlete_profile(conn)

        metrics = queries.get_athlete_metrics(conn)
        assert metrics["ftp_calc"] == round(200.0 * 0.95)
        assert metrics["ftp_provenance"]["source_activity_id"] == 1
    finally:
        conn.close()
