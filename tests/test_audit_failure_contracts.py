"""Phase-one contracts for failure modes found by the technical audit.

These tests intentionally describe the desired safety and data-correctness
contracts.  A failure against the current implementation is evidence for the
corresponding remediation item; assertions must not be relaxed to preserve the
current behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.db import queries
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.ingest import trackpoints
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.ui_nicegui.data import compliance_data, list_activities


def _insert_activity(
    conn,
    activity_id: int,
    *,
    start_time_gmt: str = "2026-04-01T06:00:00Z",
    elapsed_duration_seconds: float | None = 3600,
    moving_duration_seconds: float | None = 3300,
    average_speed: float | None = 3.0,
    average_hr: int | None = 145,
    max_hr: int | None = 180,
    avg_power: float | None = None,
    max_power: float | None = None,
    norm_power: float | None = None,
    avg_cadence: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, average_speed, average_hr, max_hr,
            activity_type, avg_power, max_power, norm_power, avg_cadence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'cycling', ?, ?, ?, ?)
        """,
        (
            activity_id,
            start_time_gmt,
            elapsed_duration_seconds,
            moving_duration_seconds,
            average_speed,
            average_hr,
            max_hr,
            avg_power,
            max_power,
            norm_power,
            avg_cadence,
        ),
    )
    conn.commit()


def _insert_metric_trackpoints(conn, activity_id: int, rows: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps,
            heart_rate_bpm, cadence, power_w, temperature_c
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(activity_id, *row) for row in rows],
    )
    conn.commit()


def _set_ftp_override(conn, ftp: int) -> None:
    conn.execute(
        "UPDATE athlete_profile SET ftp_override = ? WHERE profile_id = 1",
        (ftp,),
    )
    conn.commit()


def _activity_day_database(tmp_path: Path) -> Path:
    db_path = tmp_path / "activity-days.db"
    conn = connect_sqlite(db_path)
    try:
        apply_schema(conn, schema_sql_path())
        conn.execute(
            """
            CREATE TABLE activity(
                activity_id INTEGER PRIMARY KEY,
                activity_type TEXT,
                start_time_gmt TEXT,
                start_time_local TEXT,
                distance_meters REAL,
                elapsed_duration_seconds REAL,
                average_hr REAL,
                max_hr REAL,
                elevation_gain REAL,
                average_speed REAL,
                training_stress_score REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO activity VALUES(
                1, 'running', '2026-01-02T04:30:00Z',
                '2026-01-01T23:30:00-05:00', 10000, 3600,
                145, 175, 100, 2.78, 70
            )
            """
        )
        conn.execute(
            """
            INSERT INTO planned_workout(
                scheduled_date, workout_name, planned_distance_m,
                planned_duration_s, planned_tss
            ) VALUES ('2026-01-01', 'Evening run', 10000, 3600, 70)
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_sync_rejects_custom_database_filename_before_any_work(
    monkeypatch, tmp_path, capsys
):
    """Sync accepts arbitrary directories but only the exact filename garmin.db."""
    opened_paths: list[Path] = []

    class FakeConnection:
        def close(self) -> None:
            pass

    def unexpected_work(*_args, **_kwargs):
        pytest.fail("custom filename rejection must happen before sync work starts")

    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", unexpected_work)
    monkeypatch.setattr(cli_backup_ingest, "_find_givemydata_cmd", unexpected_work)
    monkeypatch.setattr(cli_backup_ingest.subprocess, "run", unexpected_work)
    monkeypatch.setattr(
        "garmin_data_hub.db.sqlite.connect_sqlite",
        lambda path: opened_paths.append(Path(path)) or FakeConnection(),
    )
    monkeypatch.setattr(
        "garmin_data_hub.db.migrate.apply_schema", lambda _conn, _schema: None
    )
    monkeypatch.setattr(
        "garmin_data_hub.analytics.post_sync_refresh.refresh_post_sync_tables",
        lambda _conn, **_kwargs: {"errors": 0},
    )
    monkeypatch.setattr(
        "garmin_data_hub.paths.schema_sql_path", lambda: tmp_path / "schema.sql"
    )

    requested_path = tmp_path / "data" / "athlete-history.sqlite"
    result = cli_backup_ingest.run_sync(requested_path)

    assert result != 0
    assert opened_paths == []
    output = capsys.readouterr().out
    assert "athlete-history.sqlite" in output
    assert "must be exactly 'garmin.db'" in output


def test_metric_refresh_rolls_back_every_change_when_one_activity_fails(db_conn):
    """One failed activity must not commit earlier rows from the same refresh."""
    _insert_activity(db_conn, 1, moving_duration_seconds=3000)
    _insert_activity(db_conn, 2, moving_duration_seconds=3100)
    db_conn.executemany(
        "INSERT INTO activity_metrics(activity_id, moving_time_s) VALUES (?, 111)",
        [(1,), (2,)],
    )
    db_conn.execute(
        """
        CREATE TRIGGER fail_second_metric_update
        BEFORE UPDATE ON activity_metrics
        WHEN NEW.activity_id = 2
        BEGIN
            SELECT RAISE(ABORT, 'injected metric failure');
        END
        """
    )
    db_conn.commit()

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[1, 2], lthr=160
    )

    assert summary["errors"] == 1
    values = db_conn.execute(
        "SELECT activity_id, moving_time_s FROM activity_metrics ORDER BY activity_id"
    ).fetchall()
    assert [tuple(row) for row in values] == [(1, 111), (2, 111)]


def test_threshold_change_marks_dependent_metrics_for_refresh(db_conn):
    """Changing effective thresholds must invalidate metrics derived from them."""
    _insert_activity(db_conn, 3)
    _insert_metric_trackpoints(
        db_conn,
        3,
        [
            (1, "2026-04-01T06:00:00Z", 3.0, 100, None, None, None),
            (2, "2026-04-01T06:00:10Z", 3.0, 130, None, None, None),
            (3, "2026-04-01T06:00:20Z", 3.0, 160, None, None, None),
        ],
    )
    initial = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[3], lthr=150
    )
    assert initial["errors"] == 0
    assert 3 not in queries.list_activities_needing_metrics(db_conn)

    queries.set_override_metrics(db_conn, hrmax=190, lthr=170)

    assert queries.get_effective_lthr(db_conn) == 170
    assert 3 in queries.list_activities_needing_metrics(db_conn)


def test_refresh_clears_cached_metrics_when_source_values_become_null(db_conn):
    """A refresh must mirror removed source data instead of retaining stale values."""
    _insert_activity(
        db_conn,
        4,
        avg_power=220,
        max_power=410,
        norm_power=245,
        avg_cadence=88,
    )
    _set_ftp_override(db_conn, 250)
    _insert_metric_trackpoints(
        db_conn,
        4,
        [
            (1, "2026-04-01T06:00:00Z", 3.0, 140, 86, 200, 18.0),
            (2, "2026-04-01T06:00:01Z", 3.0, 142, 90, 300, 20.0),
        ],
    )
    assert queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[4], lthr=160
    )["errors"] == 0

    db_conn.execute(
        """
        UPDATE activity
        SET avg_power = NULL, max_power = NULL, norm_power = NULL,
            avg_cadence = NULL
        WHERE activity_id = 4
        """
    )
    db_conn.execute(
        """
        UPDATE activity_trackpoints
        SET power_w = NULL, cadence = NULL, temperature_c = NULL
        WHERE activity_id = 4
        """
    )
    db_conn.commit()

    assert queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[4], lthr=160
    )["errors"] == 0
    cached = db_conn.execute(
        """
        SELECT avg_power_w, max_power_w, np_w, avg_cadence_spm,
               peak_power_5s_w, avg_temperature_c
        FROM activity_metrics WHERE activity_id = 4
        """
    ).fetchone()
    assert cached is not None
    assert tuple(cached) == (None, None, None, None, None, None)


def test_five_second_power_peak_uses_elapsed_time_not_five_rows(db_conn):
    """Power-peak durations are seconds, independent of sampling frequency."""
    _insert_activity(db_conn, 5)
    _set_ftp_override(db_conn, 250)
    powers = [100] * 5 + [500] * 5 + [100]
    rows = []
    for index, power in enumerate(powers):
        whole_seconds, half = divmod(index, 2)
        timestamp = f"2026-04-01T06:00:0{whole_seconds}{'.500' if half else ''}Z"
        rows.append((index + 1, timestamp, 3.0, 140, 85, power, None))
    _insert_metric_trackpoints(db_conn, 5, rows)

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[5], lthr=160
    )
    peak = db_conn.execute(
        "SELECT peak_power_5s_w FROM activity_metrics WHERE activity_id = 5"
    ).fetchone()[0]

    assert summary["errors"] == 0
    assert peak == pytest.approx(300.0)


def test_decoupling_time_weights_irregular_trackpoint_intervals(db_conn):
    """Uneven sampling must not give short intervals the same weight as long ones."""
    _insert_activity(db_conn, 6)
    _insert_metric_trackpoints(
        db_conn,
        6,
        [
            (1, "2026-04-01T06:00:00Z", 4.0, 100, None, None, None),
            (2, "2026-04-01T06:00:01Z", 2.0, 100, None, None, None),
            (3, "2026-04-01T06:00:09Z", 1.0, 100, None, None, None),
            (4, "2026-04-01T06:00:10Z", 1.0, 100, None, None, None),
        ],
    )

    assert queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[6], lthr=160
    )["errors"] == 0
    cached = db_conn.execute(
        """
        SELECT aerobic_decoupling_pct, pace_decoupling_pct
        FROM activity_metrics WHERE activity_id = 6
        """
    ).fetchone()

    assert cached is not None
    assert cached[0] == pytest.approx(25.0)
    assert cached[1] == pytest.approx(25.0)


def test_decoupling_does_not_mix_intermittent_power_with_speed(db_conn):
    """Incomplete power must use one coherent fallback stream, not watts plus m/s."""
    _insert_activity(db_conn, 7)
    _insert_metric_trackpoints(
        db_conn,
        7,
        [
            (1, "2026-04-01T06:00:00Z", 2.0, 100, None, 200, None),
            (2, "2026-04-01T06:00:01Z", 2.0, 100, None, None, None),
            (3, "2026-04-01T06:00:02Z", 1.0, 100, None, 200, None),
            (4, "2026-04-01T06:00:03Z", 1.0, 100, None, None, None),
        ],
    )

    assert queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[7], lthr=160
    )["errors"] == 0
    cached = db_conn.execute(
        """
        SELECT aerobic_decoupling_pct, pace_decoupling_pct
        FROM activity_metrics WHERE activity_id = 7
        """
    ).fetchone()

    assert cached is not None
    assert cached[0] == pytest.approx(50.0)
    assert cached[0] == pytest.approx(cached[1])


def test_activity_list_filters_and_labels_by_local_calendar_day(tmp_path):
    """The athlete's local day, not its GMT date, owns an activity."""
    rows = list_activities(
        _activity_day_database(tmp_path),
        start_date="2026-01-01",
        end_date="2026-01-01",
    )

    assert len(rows) == 1
    assert rows[0]["date"] == "2026-01-01"


def test_plan_compliance_matches_activities_on_local_calendar_day(tmp_path):
    """Late local activities must match the plan scheduled for that local day."""
    compliance = compliance_data(_activity_day_database(tmp_path))

    assert compliance["totals"]["distance_compliance_pct"] == 100.0
    assert compliance["totals"]["duration_compliance_pct"] == 100.0


def test_trackpoint_replacement_restores_old_rows_when_insert_fails(
    db_conn, monkeypatch, tmp_path
):
    """Replacement must be atomic for each activity once old rows are deleted."""
    activity_id = 1_234_567
    _insert_activity(db_conn, activity_id)
    db_conn.execute(
        """
        INSERT INTO activity_trackpoints(
            activity_id, seq, timestamp_utc, speed_mps, heart_rate_bpm
        ) VALUES (?, 99, '2026-04-01T05:59:59Z', 2.5, 135)
        """,
        (activity_id,),
    )
    db_conn.commit()
    archive = tmp_path / f"2026-04-01_{activity_id}_ride.zip"
    archive.touch()

    duplicate_sequence_rows = [
        (1, "2026-04-01T06:00:00Z", None, None, None, 0.0, 3.0, 140, 80, 200, 18.0),
        (1, "2026-04-01T06:00:01Z", None, None, None, 3.0, 3.1, 142, 82, 210, 18.0),
    ]
    monkeypatch.setattr(
        trackpoints,
        "parse_trackpoints_from_fit_archive",
        lambda _path: (activity_id, duplicate_sequence_rows),
    )

    summary = trackpoints.ingest_trackpoints_from_fit_archives(
        db_conn,
        tmp_path,
        replace_existing=True,
        archive_paths=[archive],
    )
    stored = db_conn.execute(
        """
        SELECT seq, timestamp_utc, speed_mps, heart_rate_bpm
        FROM activity_trackpoints WHERE activity_id = ? ORDER BY seq
        """,
        (activity_id,),
    ).fetchall()

    assert summary["errors"] == 1
    assert [tuple(row) for row in stored] == [
        (99, "2026-04-01T05:59:59Z", 2.5, 135)
    ]
