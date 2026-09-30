"""Expected-red contracts for Phase 3M.4B.2 backlog remediation.

Production intentionally does not implement these contracts yet.  Do not
xfail, skip, or weaken them: each test names a behavior required from the next
implementation phase.  Fixtures are synthetic and contain no live Garmin
coordinates, timestamps, or archive bytes.
"""

from __future__ import annotations

import importlib
import sqlite3
import subprocess
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub import paths as hub_paths
from garmin_data_hub.analytics import post_sync_refresh
from garmin_data_hub.db import migrate as db_migrate
from garmin_data_hub.db.migrate import (
    CURRENT_SCHEMA_VERSION,
    apply_schema,
    get_current_schema_version,
)
from garmin_data_hub.ingest import trackpoints
from garmin_data_hub.paths import schema_sql_path


ACTIVITY_ID = 9_200_001
START_UTC = "2035-01-02T03:04:05Z"
PARSER_VERSION = "archive-v1"
LEDGER_TABLE = "archive_reconciliation"


def _required_module(name: str, purpose: str) -> ModuleType:
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pytest.fail(f"missing {purpose} module: {name}")


def _parser() -> ModuleType:
    module = _required_module(
        "garmin_data_hub.ingest.archive_parser", "canonical archive parser"
    )
    for symbol in ("ActivityIdentity", "parse_activity_archive"):
        assert hasattr(module, symbol), f"archive parser must expose {symbol}"
    return module


def _reconciliation() -> ModuleType:
    module = _required_module(
        "garmin_data_hub.ingest.reconciliation",
        "historical archive reconciliation",
    )
    assert hasattr(module, "reconcile_historical_archives")
    return module


def _target(parser: ModuleType, activity_id: int = ACTIVITY_ID):
    return parser.ActivityIdentity(
        activity_id=activity_id,
        start_time_utc=START_UTC,
        duration_s=600.0,
        activity_type="running",
    )


def _status(result) -> str:
    value = result.status
    return str(getattr(value, "value", value))


def _format(result) -> str:
    value = result.archive_format
    return str(getattr(value, "value", value))


def _zip(
    directory: Path,
    *,
    activity_id: int = ACTIVITY_ID,
    member_name: str,
    payload: str | bytes,
    suffix: str = "synthetic",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"2035-01-02_{activity_id}_{suffix}.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, payload)
    return path


def _gpx(
    *,
    times: tuple[str, ...] = (START_UTC, "2035-01-02T03:04:06Z"),
    include_all_fields: bool = True,
) -> str:
    points: list[str] = []
    for index, timestamp in enumerate(times):
        extensions = ""
        if include_all_fields:
            extensions = f"""
            <extensions>
              <gpxtpx:TrackPointExtension>
                <gpxtpx:hr>{141 + index}</gpxtpx:hr>
                <gpxtpx:cad>{83 + index}</gpxtpx:cad>
              </gpxtpx:TrackPointExtension>
              <power>{211 + index}</power>
            </extensions>
            """
        points.append(
            f"""
            <trkpt lat="38.{900 + index}" lon="-77.{100 + index}">
              <ele>{120 + index}.5</ele>
              <time>{timestamp}</time>
              {extensions}
            </trkpt>
            """
        )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <gpx xmlns="http://www.topografix.com/GPX/1/1"
         xmlns:gpxtpx="http://www.garmin.com/xmlschemas/TrackPointExtension/v1"
         version="1.1" creator="synthetic-contract">
      <trk><name>Invented Run</name><trkseg>{''.join(points)}</trkseg></trk>
    </gpx>
    """


def _tcx_activity(
    *,
    identity: str = START_UTC,
    sport: str = "Running",
    points: tuple[str, ...] = (START_UTC, "2035-01-02T03:04:06Z"),
    include_all_fields: bool = True,
) -> str:
    trackpoints: list[str] = []
    for index, timestamp in enumerate(points):
        fields = ""
        if include_all_fields:
            fields = f"""
              <Position>
                <LatitudeDegrees>38.{800 + index}</LatitudeDegrees>
                <LongitudeDegrees>-77.{200 + index}</LongitudeDegrees>
              </Position>
              <AltitudeMeters>{130 + index}.5</AltitudeMeters>
              <DistanceMeters>{10 + index}.0</DistanceMeters>
              <HeartRateBpm><Value>{145 + index}</Value></HeartRateBpm>
              <Cadence>{85 + index}</Cadence>
              <Extensions><ae:TPX>
                <ae:Speed>{3.1 + index}</ae:Speed>
                <ae:Watts>{220 + index}</ae:Watts>
              </ae:TPX></Extensions>
            """
        trackpoints.append(
            f"<Trackpoint><Time>{timestamp}</Time>{fields}</Trackpoint>"
        )
    tracks = f"<Track>{''.join(trackpoints)}</Track>" if trackpoints else ""
    return f"""
      <Activity Sport="{sport}">
        <Id>{identity}</Id>
        <Lap StartTime="{identity}">
          <TotalTimeSeconds>600</TotalTimeSeconds>
          <DistanceMeters>1000</DistanceMeters>
          {tracks}
        </Lap>
      </Activity>
    """


def _tcx(*activities: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <TrainingCenterDatabase
      xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
      xmlns:ae="http://www.garmin.com/xmlschemas/ActivityExtension/v2">
      <Activities>{''.join(activities)}</Activities>
    </TrainingCenterDatabase>
    """


def _activity_database(
    path: Path,
    activity_id: int = ACTIVITY_ID,
    *,
    with_old_row: bool = False,
) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT,
                elapsed_duration_seconds REAL,
                activity_type TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO activity VALUES (?, ?, 600.0, 'running')",
            (activity_id, START_UTC),
        )
        apply_schema(conn, schema_sql_path())
        if with_old_row:
            conn.execute(
                "INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc) "
                "VALUES (?, 99, 'old-complete-row')",
                (activity_id,),
            )


def _canonical_ingest(conn: sqlite3.Connection, archive: Path) -> dict[str, object]:
    assert hasattr(trackpoints, "ingest_trackpoints_from_archives"), (
        "the canonical multi-format ingester is not implemented"
    )
    return trackpoints.ingest_trackpoints_from_archives(
        conn,
        archive.parent,
        replace_existing=True,
        archive_paths=[archive],
    )


def _reconcile(conn: sqlite3.Connection, archive: Path) -> dict[str, object]:
    module = _reconciliation()
    return module.reconcile_historical_archives(
        conn,
        archive.parent,
        archive_paths=[archive],
        algorithm_version=PARSER_VERSION,
    )


def _latest_ledger(conn: sqlite3.Connection, activity_id: int = ACTIVITY_ID):
    return conn.execute(
        f"""
        SELECT status, reason_code, trackpoint_count, source_size,
               source_mtime_ns, source_sha256, algorithm_version
        FROM {LEDGER_TABLE}
        WHERE target_activity_id=?
        ORDER BY reconciliation_id DESC
        LIMIT 1
        """,
        (activity_id,),
    ).fetchone()


def test_03_gpx_matched_trackpoints_are_parsed(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload=_gpx(),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "parsed"
    assert _format(result) == "gpx"
    assert result.activity_id == ACTIVITY_ID
    assert result.rows == [
        (0, START_UTC, 38.9, -77.1, 120.5, None, None, 141, 83, 211, None),
        (
            1,
            "2035-01-02T03:04:06Z",
            38.901,
            -77.101,
            121.5,
            None,
            None,
            142,
            84,
            212,
            None,
        ),
    ]


def test_04_tcx_matched_trackpoints_are_parsed(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity()),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "parsed"
    assert _format(result) == "tcx"
    assert result.rows == [
        (0, START_UTC, 38.8, -77.2, 130.5, 10.0, 3.1, 145, 85, 220, None),
        (
            1,
            "2035-01-02T03:04:06Z",
            38.801,
            -77.201,
            131.5,
            11.0,
            4.1,
            146,
            86,
            221,
            None,
        ),
    ]


def test_05_tcx_matched_zero_record_activity_is_recognized(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity(points=())),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "recognized_no_records"
    assert result.activity_id == ACTIVITY_ID
    assert result.rows == []


def test_06_malformed_gpx_is_classified_explicitly(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload="<gpx><trk>",
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "malformed"
    assert result.reason_code


def test_07_malformed_tcx_is_classified_explicitly(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload="<TrainingCenterDatabase><Activities>",
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "malformed"
    assert result.reason_code


def test_08_archive_without_supported_member_is_explicitly_unsupported(
    tmp_path: Path,
) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.json",
        payload="{}",
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "unsupported"
    assert result.rows == []


def test_09_semantic_activity_mismatch_is_explicit(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(
                identity="2034-01-02T03:04:05Z",
                points=("2034-01-02T03:04:05Z",),
            )
        ),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "mismatch"
    assert result.rows == []
    assert result.identity_evidence["target_activity_id"] == ACTIVITY_ID


def test_10_multiple_matching_internal_activities_are_ambiguous(
    tmp_path: Path,
) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(points=(START_UTC,)),
            _tcx_activity(points=("2035-01-02T03:04:07Z",)),
        ),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "ambiguous"
    assert result.rows == []
    assert result.identity_evidence["matching_candidate_count"] == 2


def test_11_unavailable_measurements_remain_null(tmp_path: Path) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(points=(START_UTC,), include_all_fields=False)
        ),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "parsed"
    assert result.rows == [(0, START_UTC, None, None, None, None, None, None, None, None, None)]


def test_12_source_order_duplicate_timestamps_and_utc_normalization_are_deterministic(
    tmp_path: Path,
) -> None:
    parser = _parser()
    archive = _zip(
        tmp_path,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload=_gpx(
            times=(
                "2035-01-02T04:04:05+01:00",
                "2035-01-02T03:04:05Z",
            ),
            include_all_fields=False,
        ),
    )

    result = parser.parse_activity_archive(archive, _target(parser))

    assert _status(result) == "parsed"
    assert [(row[0], row[1]) for row in result.rows] == [
        (0, START_UTC),
        (1, START_UTC),
    ]


@pytest.mark.parametrize(
    ("member_name", "payload", "expected_status"),
    [
        (f"{ACTIVITY_ID}_UNKNOWN.gpx", "<gpx><trk>", "malformed"),
    ],
)
def test_13_changed_new_malformed_archive_remains_fatal(
    tmp_path: Path, member_name: str, payload: str, expected_status: str
) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path, with_old_row=True)
    archive = _zip(tmp_path / "fit", member_name=member_name, payload=payload)

    with sqlite3.connect(db_path) as conn:
        summary = _canonical_ingest(conn, archive)
        stored = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints WHERE activity_id=?",
            (ACTIVITY_ID,),
        ).fetchall()

    assert summary["errors"] == 1
    assert summary["result_statuses"][ACTIVITY_ID] == expected_status
    assert stored == [(99, "old-complete-row")]


def test_14_changed_new_unsupported_archive_remains_fatal(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path, with_old_row=True)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.json",
        payload="{}",
    )

    with sqlite3.connect(db_path) as conn:
        summary = _canonical_ingest(conn, archive)

    assert summary["errors"] == 1
    assert summary["result_statuses"][ACTIVITY_ID] == "unsupported"


def test_15_changed_new_semantic_mismatch_remains_fatal(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path, with_old_row=True)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(
                identity="2034-01-02T03:04:05Z",
                points=("2034-01-02T03:04:05Z",),
            )
        ),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _canonical_ingest(conn, archive)

    assert summary["errors"] == 1
    assert summary["result_statuses"][ACTIVITY_ID] == "mismatch"


def test_16_changed_new_ambiguous_archive_remains_fatal(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path, with_old_row=True)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(points=(START_UTC,)),
            _tcx_activity(points=("2035-01-02T03:04:07Z",)),
        ),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _canonical_ingest(conn, archive)

    assert summary["errors"] == 1
    assert summary["result_statuses"][ACTIVITY_ID] == "ambiguous"


def test_18_changed_new_matched_zero_record_activity_skips_safely(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path, with_old_row=True)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity(points=())),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _canonical_ingest(conn, archive)
        stored = conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints WHERE activity_id=?",
            (ACTIVITY_ID,),
        ).fetchall()

    assert summary["errors"] == 0
    assert summary["skipped_no_records"] == 1
    assert summary["result_statuses"][ACTIVITY_ID] == "recognized_no_records"
    assert summary["updated_activity_ids"] == []
    assert stored == [(99, "old-complete-row")]


def test_20_matched_gpx_historical_backlog_ingests(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload=_gpx(),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        count = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints WHERE activity_id=?",
            (ACTIVITY_ID,),
        ).fetchone()[0]
        ledger = _latest_ledger(conn)

    assert summary["updated_activity_ids"] == [ACTIVITY_ID]
    assert count == 2
    assert ledger[0] == "ingested"
    assert ledger[2] == 2


def test_21_matched_tcx_historical_backlog_ingests(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity()),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        ledger = _latest_ledger(conn)

    assert summary["updated_activity_ids"] == [ACTIVITY_ID]
    assert ledger[0] == "ingested"
    assert ledger[2] == 2


def test_22_matched_zero_record_archive_resolves_durably(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity(points=())),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        ledger = _latest_ledger(conn)

    assert summary["resolved_no_records"] == 1
    assert summary["updated_activity_ids"] == []
    assert ledger[0] == "resolved_no_records"
    assert ledger[2] == 0


def test_23_historical_mismatch_is_recorded_without_ingestion(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(
                identity="2034-01-02T03:04:05Z",
                points=("2034-01-02T03:04:05Z",),
            )
        ),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        point_count = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints"
        ).fetchone()[0]
        ledger = _latest_ledger(conn)

    assert summary["warnings"] == 1
    assert point_count == 0
    assert ledger[0] == "mismatch"


def test_24_historical_unsupported_archive_is_recorded(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.json",
        payload="{}",
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        ledger = _latest_ledger(conn)

    assert summary["warnings"] == 1
    assert ledger[0] == "unsupported"


def test_25_historical_malformed_archive_is_recorded(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload="<gpx><trk>",
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        ledger = _latest_ledger(conn)

    assert summary["warnings"] == 1
    assert ledger[0] == "malformed"


def test_26_historical_ambiguous_archive_is_recorded(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(
            _tcx_activity(points=(START_UTC,)),
            _tcx_activity(points=("2035-01-02T03:04:07Z",)),
        ),
    )

    with sqlite3.connect(db_path) as conn:
        summary = _reconcile(conn, archive)
        ledger = _latest_ledger(conn)

    assert summary["warnings"] == 1
    assert ledger[0] == "ambiguous"


def test_27_unchanged_terminal_source_is_not_retried(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity(points=())),
    )

    with sqlite3.connect(db_path) as conn:
        first = _reconcile(conn, archive)
        before = _latest_ledger(conn)
        second = _reconcile(conn, archive)
        after = _latest_ledger(conn)
        ledger_count = conn.execute(
            f"SELECT COUNT(*) FROM {LEDGER_TABLE} WHERE target_activity_id=?",
            (ACTIVITY_ID,),
        ).fetchone()[0]

    assert first["attempted_archives"] == 1
    assert second["attempted_archives"] == 0
    assert second["unchanged_terminal"] == 1
    assert ledger_count == 1
    assert before == after


def test_28_source_fingerprint_change_makes_archive_retryable(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    fit_dir = tmp_path / "fit"
    archive = _zip(
        fit_dir,
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity(points=())),
    )

    with sqlite3.connect(db_path) as conn:
        first = _reconcile(conn, archive)
        first_ledger = _latest_ledger(conn)
        archive = _zip(
            fit_dir,
            member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
            payload=_tcx(
                _tcx_activity(
                    identity="2035-01-02T03:04:06Z",
                    points=(),
                )
            ),
        )
        second = _reconcile(conn, archive)
        second_ledger = _latest_ledger(conn)

    assert first["attempted_archives"] == 1
    assert second["attempted_archives"] == 1
    assert first_ledger[5] != second_ledger[5]


def test_29_successful_backlog_ingestion_is_not_retried(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload=_gpx(),
    )

    with sqlite3.connect(db_path) as conn:
        first = _reconcile(conn, archive)
        second = _reconcile(conn, archive)
        count = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints WHERE activity_id=?",
            (ACTIVITY_ID,),
        ).fetchone()[0]

    assert first["updated_activity_ids"] == [ACTIVITY_ID]
    assert second["attempted_archives"] == 0
    assert second["unchanged_terminal"] == 1
    assert count == 2


def test_30_historical_write_failure_leaves_no_partial_trackpoints(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.gpx",
        payload=_gpx(),
    )

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TRIGGER reject_second_synthetic_point
            BEFORE INSERT ON activity_trackpoints
            WHEN NEW.activity_id = 9200001 AND NEW.seq = 1
            BEGIN
              SELECT RAISE(ABORT, 'synthetic insertion failure');
            END
            """
        )
        summary = _reconcile(conn, archive)
        point_count = conn.execute(
            "SELECT COUNT(*) FROM activity_trackpoints WHERE activity_id=?",
            (ACTIVITY_ID,),
        ).fetchone()[0]
        ledger = _latest_ledger(conn)

    assert summary["errors"] == 1
    assert summary["updated_activity_ids"] == []
    assert point_count == 0
    assert ledger[0] == "write_error"


def _patch_sync_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    reconciliation_summary: dict[str, object],
) -> tuple[Path, dict[str, object]]:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)
    archive = _zip(
        tmp_path / "fit",
        member_name=f"{ACTIVITY_ID}_UNKNOWN.tcx",
        payload=_tcx(_tcx_activity(points=())),
    )
    captured: dict[str, object] = {"reconcile_calls": 0}

    monkeypatch.setattr(cli_backup_ingest, "ensure_app_dirs", lambda: None)
    monkeypatch.setattr(
        cli_backup_ingest, "_find_givemydata_cmd", lambda: ["synthetic-upstream"]
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    monkeypatch.setattr(db_migrate, "apply_schema", lambda _conn, _schema: None)
    monkeypatch.setattr(hub_paths, "schema_sql_path", lambda: Path("schema.sql"))

    def changed_ingest(_conn, _fit_dir, **_kwargs):
        return {
            "errors": 0,
            "updated_activity_ids": [],
            "target_activity_ids": [],
            "ingested_activities": 0,
        }

    monkeypatch.setattr(
        trackpoints, "ingest_trackpoints_from_fit_archives", changed_ingest
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_fit_archives",
        changed_ingest,
        raising=False,
    )
    monkeypatch.setattr(
        trackpoints,
        "ingest_trackpoints_from_archives",
        changed_ingest,
        raising=False,
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_archives",
        changed_ingest,
        raising=False,
    )

    def reconcile(_conn, _fit_dir, **kwargs):
        captured["reconcile_calls"] = int(captured["reconcile_calls"]) + 1
        captured["reconcile_paths"] = list(kwargs.get("archive_paths", []))
        return reconciliation_summary

    monkeypatch.setattr(
        cli_backup_ingest,
        "reconcile_historical_archives",
        reconcile,
        raising=False,
    )

    def refresh(_conn, **kwargs):
        captured["refresh_activity_ids"] = kwargs.get("activity_ids")
        return {"errors": 0}

    monkeypatch.setattr(post_sync_refresh, "refresh_post_sync_tables", refresh)
    captured["archive"] = archive
    return db_path, captured


def test_31_successfully_recovered_activity_enters_metric_refresh_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recovered_id = ACTIVITY_ID
    db_path, captured = _patch_sync_pipeline(
        monkeypatch,
        tmp_path,
        reconciliation_summary={
            "attempted_archives": 1,
            "updated_activity_ids": [recovered_id],
            "ingested_activities": 1,
            "warnings": 0,
            "errors": 0,
        },
    )

    assert cli_backup_ingest.run_sync(db_path, days=7) == 0
    assert captured["reconcile_calls"] == 1
    assert captured["refresh_activity_ids"] == [recovered_id]


def test_32_unresolved_activity_does_not_receive_false_metric_freshness(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path, captured = _patch_sync_pipeline(
        monkeypatch,
        tmp_path,
        reconciliation_summary={
            "attempted_archives": 1,
            "updated_activity_ids": [],
            "ingested_activities": 0,
            "warnings": 1,
            "errors": 0,
            "status_counts": {"mismatch": 1},
        },
    )

    assert cli_backup_ingest.run_sync(db_path, days=7) == 0
    assert captured["reconcile_calls"] == 1
    assert captured["refresh_activity_ids"] == []


def test_34_historical_quarantine_has_truthful_partial_terminal_semantics(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
) -> None:
    db_path, _captured = _patch_sync_pipeline(
        monkeypatch,
        tmp_path,
        reconciliation_summary={
            "attempted_archives": 1,
            "updated_activity_ids": [],
            "ingested_activities": 0,
            "warnings": 1,
            "errors": 0,
            "status_counts": {"mismatch": 1},
        },
    )

    assert cli_backup_ingest.run_sync(db_path, days=7) == 0
    output = capsys.readouterr().out
    assert "[PARTIAL] Sync complete with historical reconciliation warnings" in output
    assert "[SUCCESS] Sync complete" not in output


def test_35_v8_ledger_shape_constraints_and_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)

    with sqlite3.connect(db_path) as conn:
        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION
        columns = {
            row[1]: {"type": row[2], "notnull": row[3], "default": row[4]}
            for row in conn.execute(f"PRAGMA table_info({LEDGER_TABLE})")
        }
        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (LEDGER_TABLE,),
        ).fetchone()[0]
        indexes = {
            row[1] for row in conn.execute(f"PRAGMA index_list({LEDGER_TABLE})")
        }

    assert set(columns) == {
        "reconciliation_id",
        "archive_identity",
        "target_activity_id",
        "archive_format",
        "source_size",
        "source_mtime_ns",
        "source_sha256",
        "algorithm_version",
        "status",
        "reason_code",
        "identity_evidence_json",
        "trackpoint_count",
        "attempted_at_utc",
        "completed_at_utc",
    }
    for required in (
        "archive_identity",
        "source_size",
        "source_mtime_ns",
        "source_sha256",
        "algorithm_version",
        "status",
        "identity_evidence_json",
        "trackpoint_count",
        "attempted_at_utc",
    ):
        assert columns[required]["notnull"] == 1
    for status in (
        "ingested",
        "resolved_no_records",
        "unsupported",
        "malformed",
        "mismatch",
        "ambiguous",
        "write_error",
    ):
        assert status in ddl
    assert "idx_archive_reconciliation_fingerprint" in indexes
    assert "idx_archive_reconciliation_status" in indexes


def test_36_v8_migration_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    _activity_database(db_path)

    with sqlite3.connect(db_path) as conn:
        apply_schema(conn, schema_sql_path())
        apply_schema(conn, schema_sql_path())
        versions = conn.execute(
            "SELECT version, COUNT(*) FROM schema_migrations "
            "GROUP BY version ORDER BY version"
        ).fetchall()
        table_count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (LEDGER_TABLE,),
        ).fetchone()[0]

    assert versions == [
        (version, 1) for version in range(1, CURRENT_SCHEMA_VERSION + 1)
    ]
    assert table_count == 1


def test_37_v8_does_not_change_upstream_owned_activity_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT,
                elapsed_duration_seconds REAL,
                activity_type TEXT,
                upstream_sentinel TEXT DEFAULT 'preserve-me'
            )
            """
        )
        before = conn.execute("PRAGMA table_info(activity)").fetchall()
        before_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity'"
        ).fetchone()[0]

        apply_schema(conn, schema_sql_path())

        after = conn.execute("PRAGMA table_info(activity)").fetchall()
        after_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity'"
        ).fetchone()[0]
        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION

    assert after == before
    assert after_sql == before_sql


def test_38_v7_to_v8_preserves_existing_v7_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "garmin.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE activity (activity_id INTEGER PRIMARY KEY, start_time_gmt TEXT)"
        )
        apply_schema(conn, schema_sql_path())
        # Once current schema exists, this creates the same starting point as a
        # real v7 DB so all later additive migrations replay together.
        conn.execute("DELETE FROM schema_migrations WHERE version >= 8")
        conn.execute(f"DROP TABLE IF EXISTS {LEDGER_TABLE}")
        conn.execute(
            """
            INSERT INTO activity_metrics(
                activity_id, moving_time_s, refresh_provenance_version,
                threshold_lthr_bpm, threshold_ftp_w, threshold_resting_hr_bpm
            ) VALUES (9200038, 321.0, 6, 155, 250, 48)
            """
        )
        cursor = conn.execute(
            """
            INSERT INTO threshold_calculation(
                threshold_type, calculated_value, algorithm_version,
                calculated_at_utc, source_kind, aggregate_evidence_json
            ) VALUES ('running_ftp', 250, 'threshold-v7',
                      '2035-01-02T00:00:00Z', 'activities', '{"count": 3}')
            """
        )
        threshold_id = int(cursor.lastrowid)
        conn.execute(
            "UPDATE athlete_profile SET ftp_calc=250, ftp_calculation_id=? "
            "WHERE profile_id=1",
            (threshold_id,),
        )
        conn.commit()

        apply_schema(conn, schema_sql_path())

        metric = conn.execute(
            """
            SELECT moving_time_s, refresh_provenance_version,
                   threshold_lthr_bpm, threshold_ftp_w, threshold_resting_hr_bpm
            FROM activity_metrics WHERE activity_id=9200038
            """
        ).fetchone()
        threshold = conn.execute(
            """
            SELECT calculated_value, algorithm_version, source_kind,
                   aggregate_evidence_json
            FROM threshold_calculation WHERE threshold_calculation_id=?
            """,
            (threshold_id,),
        ).fetchone()
        profile = conn.execute(
            "SELECT ftp_calc, ftp_calculation_id FROM athlete_profile WHERE profile_id=1"
        ).fetchone()

        assert get_current_schema_version(conn) == CURRENT_SCHEMA_VERSION

    assert metric == (321.0, 6, 155, 250, 48)
    assert threshold == (250, "threshold-v7", "activities", '{"count": 3}')
    assert profile == (250, threshold_id)
