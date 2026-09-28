"""Acceptance regressions for the Phase 3M.4B.4A corrections."""

from __future__ import annotations

import json
import sqlite3
import zipfile
from pathlib import Path

import pytest

from garmin_data_hub import cli_archive_reconcile, cli_backup_ingest
from garmin_data_hub.analytics.post_sync_refresh import refresh_post_sync_tables
from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.ingest.archive_parser import (
    ActivityIdentity,
    ArchiveParseStatus,
    parse_activity_archive,
)
from garmin_data_hub.ingest.reconciliation import BASELINE_SETTING_KEY
from garmin_data_hub.paths import schema_sql_path


ACTIVITY_ID = 9_400_001
SECOND_ACTIVITY_ID = 9_400_002
MISSING_ACTIVITY_ID = 9_499_999
START_UTC = "2026-09-20T04:05:06Z"


def _insert_activity(
    conn: sqlite3.Connection,
    activity_id: int,
    *,
    activity_type: str = "running",
    start_time_utc: str = START_UTC,
) -> None:
    conn.execute(
        """
        INSERT INTO activity (
          activity_id, start_time_gmt, elapsed_duration_seconds,
          moving_duration_seconds, average_speed, max_hr, average_hr,
          activity_type, avg_cadence
        ) VALUES (?, ?, 600.0, 590.0, 3.0, 180, 150, ?, 86.0)
        """,
        (activity_id, start_time_utc, activity_type),
    )


def _database(path: Path, *activity_ids: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE activity (
              activity_id INTEGER PRIMARY KEY,
              start_time_gmt TEXT,
              elapsed_duration_seconds REAL,
              moving_duration_seconds REAL,
              average_speed REAL,
              max_hr INTEGER,
              training_stress_score REAL,
              average_hr REAL,
              activity_type TEXT,
              avg_power REAL,
              max_power REAL,
              norm_power REAL,
              intensity_factor REAL,
              avg_cadence REAL,
              elevation_gain REAL,
              elevation_loss REAL,
              min_elevation REAL,
              max_elevation REAL,
              aerobic_training_effect REAL,
              anaerobic_training_effect REAL,
              min_temperature REAL,
              max_temperature REAL
            )
            """
        )
        for activity_id in activity_ids:
            _insert_activity(conn, activity_id)
        apply_schema(conn, schema_sql_path())


def _gpx(*, timestamp: str = START_UTC) -> str:
    return f"""
    <gpx xmlns="http://www.topografix.com/GPX/1/1" version="1.1" creator="test">
      <trk><trkseg>
        <trkpt lat="39.1" lon="-76.2"><time>{timestamp}</time></trkpt>
        <trkpt lat="39.2" lon="-76.3"><time>{timestamp}</time></trkpt>
      </trkseg></trk>
    </gpx>
    """


def _archive(
    directory: Path,
    activity_id: int,
    payload: str,
    *,
    member_name: str | None = None,
    archive_name: str | None = None,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (
        archive_name or f"2026-09-20_{activity_id}_acceptance.zip"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(member_name or f"{activity_id}_UNKNOWN.gpx", payload)
    return path


def _baseline(conn: sqlite3.Connection) -> dict[str, object] | None:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key=?", (BASELINE_SETTING_KEY,)
    ).fetchone()
    return json.loads(row[0]) if row else None


def test_zero_exact_refresh_targets_do_not_expand_to_all(db_conn) -> None:
    _insert_activity(db_conn, ACTIVITY_ID)
    _insert_activity(db_conn, SECOND_ACTIVITY_ID)
    db_conn.commit()

    summary = refresh_post_sync_tables(db_conn, activity_ids=[])

    assert summary["target_activities"] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM activity_metrics").fetchone()[0] == 0


def test_days_and_topoff_exclude_quarantined_historical_activity(db_conn) -> None:
    _insert_activity(db_conn, ACTIVITY_ID)
    _insert_activity(db_conn, SECOND_ACTIVITY_ID)
    db_conn.execute(
        """
        INSERT INTO activity_metrics(activity_id, refresh_provenance_version)
        VALUES (?, NULL)
        """,
        (SECOND_ACTIVITY_ID,),
    )
    db_conn.commit()

    summary = refresh_post_sync_tables(
        db_conn,
        activity_ids=[ACTIVITY_ID],
        start_ts_iso="2026-09-01T00:00:00Z",
        excluded_activity_ids=[SECOND_ACTIVITY_ID],
    )

    refreshed = db_conn.execute(
        """
        SELECT activity_id, refresh_provenance_version,
               threshold_lthr_bpm, threshold_ftp_w,
               threshold_resting_hr_bpm
        FROM activity_metrics
        ORDER BY activity_id
        """
    ).fetchall()
    assert summary["target_activities"] == 1
    assert refreshed[0][0] == ACTIVITY_ID
    assert refreshed[0][1] == 2
    assert tuple(refreshed[1]) == (SECOND_ACTIVITY_ID, None, None, None, None)


def test_mixed_exact_targets_refresh_only_successful_recovery(db_conn) -> None:
    _insert_activity(db_conn, ACTIVITY_ID)
    _insert_activity(db_conn, SECOND_ACTIVITY_ID)
    db_conn.commit()

    summary = refresh_post_sync_tables(
        db_conn,
        activity_ids=[ACTIVITY_ID],
        excluded_activity_ids=[SECOND_ACTIVITY_ID],
    )

    refreshed_ids = {
        int(row[0])
        for row in db_conn.execute("SELECT activity_id FROM activity_metrics")
    }
    assert summary["target_activities"] == 1
    assert refreshed_ids == {ACTIVITY_ID}


def test_baseline_gating_excludes_unreconciled_history_from_days_refresh(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    archive = _archive(fit_dir, ACTIVITY_ID, _gpx())

    with sqlite3.connect(db_path) as conn:
        summary = cli_backup_ingest._run_historical_archive_reconciliation(
            conn, fit_dir, [archive]
        )
        refresh = refresh_post_sync_tables(
            conn,
            activity_ids=[],
            start_ts_iso="2026-09-01T00:00:00Z",
            excluded_activity_ids=summary["excluded_activity_ids"],
        )
        metric_count = conn.execute(
            "SELECT COUNT(*) FROM activity_metrics"
        ).fetchone()[0]

    assert summary["baseline_required"] is True
    assert summary["candidate_archives"] == 1
    assert summary["excluded_activity_ids"] == [ACTIVITY_ID]
    assert refresh["target_activities"] == 0
    assert metric_count == 0


def test_unroutable_candidate_is_accounted_and_blocks_baseline(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(
        fit_dir,
        ACTIVITY_ID,
        "<broken>",
        archive_name="unroutable.zip",
        member_name="track.gpx",
    )

    summary = cli_archive_reconcile.run_reconciliation(
        db_path, fit_dir, apply=True
    )
    repeated = cli_archive_reconcile.run_reconciliation(
        db_path, fit_dir, apply=True
    )

    with sqlite3.connect(db_path) as conn:
        ledger = conn.execute(
            "SELECT target_activity_id, status, reason_code "
            "FROM archive_reconciliation"
        ).fetchall()
        baseline = _baseline(conn)
    assert summary["candidate_archives"] == 1
    assert summary["accounted_archives"] == 1
    assert summary["unresolved_archives"] == 1
    assert summary["terminal_archives"] == 0
    assert summary["errors"] == 1
    assert repeated["errors"] == 1
    assert ledger == [(None, "ambiguous", "unresolved_activity_id")]
    assert baseline is None


def test_missing_activity_candidate_is_accounted_and_blocks_baseline(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, MISSING_ACTIVITY_ID, _gpx())

    summary = cli_archive_reconcile.run_reconciliation(
        db_path, fit_dir, apply=True
    )

    with sqlite3.connect(db_path) as conn:
        ledger = conn.execute(
            "SELECT target_activity_id, status, reason_code, identity_evidence_json "
            "FROM archive_reconciliation"
        ).fetchone()
        baseline = _baseline(conn)
    assert summary["candidate_archives"] == 1
    assert summary["accounted_archives"] == 1
    assert summary["unresolved_archives"] == 1
    assert summary["errors"] == 1
    assert ledger[:3] == (None, "ambiguous", "activity_not_found")
    assert json.loads(ledger[3])["candidate_activity_id"] == MISSING_ACTIVITY_ID
    assert baseline is None


def test_durably_classified_terminal_candidate_allows_complete_baseline(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, "<broken>")

    summary = cli_archive_reconcile.run_reconciliation(
        db_path, fit_dir, apply=True
    )

    with sqlite3.connect(db_path) as conn:
        ledger = conn.execute(
            "SELECT status FROM archive_reconciliation"
        ).fetchall()
        baseline = _baseline(conn)
    assert summary["candidate_archives"] == 1
    assert summary["accounted_archives"] == 1
    assert summary["terminal_archives"] == 1
    assert summary["unresolved_archives"] == 0
    assert summary["baseline_complete"] is True
    assert ledger == [("malformed",)]
    assert baseline is not None


def test_maintenance_refreshes_only_successfully_recovered_ids(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _database(db_path, ACTIVITY_ID, SECOND_ACTIVITY_ID)
    fit_dir = tmp_path / "fit"
    _archive(fit_dir, ACTIVITY_ID, _gpx())
    _archive(
        fit_dir,
        SECOND_ACTIVITY_ID,
        _gpx(timestamp="2026-09-21T04:05:06Z"),
    )

    summary = cli_archive_reconcile.run_reconciliation(
        db_path, fit_dir, apply=True
    )

    with sqlite3.connect(db_path) as conn:
        metric_rows = conn.execute(
            "SELECT activity_id, refresh_provenance_version "
            "FROM activity_metrics ORDER BY activity_id"
        ).fetchall()
        ledger_rows = conn.execute(
            "SELECT target_activity_id, status FROM archive_reconciliation "
            "ORDER BY target_activity_id"
        ).fetchall()
    assert summary["updated_activity_ids"] == [ACTIVITY_ID]
    assert summary["metric_refresh"]["target_activities"] == 1
    assert metric_rows == [(ACTIVITY_ID, 2)]
    assert ledger_rows == [
        (ACTIVITY_ID, "ingested"),
        (SECOND_ACTIVITY_ID, "mismatch"),
    ]


def _tcx(sport: str) -> str:
    return f"""
    <TrainingCenterDatabase
      xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
      xmlns:ae="http://www.garmin.com/xmlschemas/ActivityExtension/v2">
      <Activities><Activity Sport="{sport}">
        <Id>{START_UTC}</Id>
        <Lap StartTime="{START_UTC}"><Track>
          <Trackpoint><Time>{START_UTC}</Time></Trackpoint>
        </Track></Lap>
      </Activity></Activities>
    </TrainingCenterDatabase>
    """


@pytest.mark.parametrize(
    ("tcx_sport", "activity_type"),
    [
        ("Running", "running"),
        ("Running", "trail_running"),
        ("Running", "indoor_running"),
        ("Biking", "cycling"),
        ("Cycling", "mountain_biking"),
        ("Biking", "gravel_cycling"),
        ("Biking", "indoor_cycling"),
    ],
)
def test_tcx_sport_families_accept_supported_subtypes(
    tmp_path: Path,
    tcx_sport: str,
    activity_type: str,
) -> None:
    archive = _archive(
        tmp_path,
        ACTIVITY_ID,
        _tcx(tcx_sport),
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
    )

    result = parse_activity_archive(
        archive,
        ActivityIdentity(
            activity_id=ACTIVITY_ID,
            start_time_utc=START_UTC,
            duration_s=600.0,
            activity_type=activity_type,
        ),
    )

    assert result.status is ArchiveParseStatus.PARSED


@pytest.mark.parametrize(
    ("tcx_sport", "activity_type"),
    [
        ("Running", "cycling"),
        ("Biking", "running"),
        ("Cycling", "running"),
    ],
)
def test_tcx_sport_families_reject_cross_family_matches(
    tmp_path: Path,
    tcx_sport: str,
    activity_type: str,
) -> None:
    archive = _archive(
        tmp_path,
        ACTIVITY_ID,
        _tcx(tcx_sport),
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
    )

    result = parse_activity_archive(
        archive,
        ActivityIdentity(
            activity_id=ACTIVITY_ID,
            start_time_utc=START_UTC,
            duration_s=600.0,
            activity_type=activity_type,
        ),
    )

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


def test_tcx_unknown_sport_is_not_a_permissive_match(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path,
        ACTIVITY_ID,
        _tcx("SpaceWalking"),
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
    )

    result = parse_activity_archive(
        archive,
        ActivityIdentity(
            activity_id=ACTIVITY_ID,
            start_time_utc=START_UTC,
            duration_s=600.0,
            activity_type="running",
        ),
    )

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"


@pytest.mark.parametrize(
    ("tcx_sport", "activity_type"),
    [
        ("Running", "street_running"),
        ("Cycling", "road_biking"),
        ("Biking", "virtual_ride"),
    ],
)
def test_tcx_sport_families_do_not_admit_unsubstantiated_aliases(
    tmp_path: Path,
    tcx_sport: str,
    activity_type: str,
) -> None:
    archive = _archive(
        tmp_path,
        ACTIVITY_ID,
        _tcx(tcx_sport),
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
    )

    result = parse_activity_archive(
        archive,
        ActivityIdentity(
            activity_id=ACTIVITY_ID,
            start_time_utc=START_UTC,
            duration_s=600.0,
            activity_type=activity_type,
        ),
    )

    assert result.status is ArchiveParseStatus.MISMATCH
    assert result.reason_code == "sport_mismatch"
