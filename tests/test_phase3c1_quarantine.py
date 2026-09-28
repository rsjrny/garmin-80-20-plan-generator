"""Phase 3C.1 contracts for quarantining legacy trackpoint rebuild routes."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from garmin_data_hub import cli_backup_ingest
from garmin_data_hub.ingest import trackpoints
from garmin_data_hub.ui_nicegui import app as nicegui_app


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--rebuild-trackpoints"],
        ["--days", "14", "--rebuild-trackpoints", "--visible"],
        ["--rebuild-t"],
        ["--json-import", "legacy.json"],
        ["--json-import=legacy.json"],
    ],
)
def test_supported_source_cli_quarantines_upstream_utility_modes_before_setup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    extra_args: list[str],
) -> None:
    """GREEN CHARACTERIZATION: strict validation blocks every source route."""
    db_path = tmp_path / "data" / "garmin.db"
    monkeypatch.setattr(
        cli_backup_ingest,
        "ensure_app_dirs",
        lambda: pytest.fail("rejected arguments reached application setup"),
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_find_givemydata_cmd",
        lambda: pytest.fail("rejected arguments reached upstream discovery"),
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail(
            "rejected arguments reached upstream process launch"
        ),
    )

    assert cli_backup_ingest.run_sync(db_path, extra_args=extra_args) == 2
    assert not db_path.exists()
    assert not db_path.parent.exists()


@pytest.mark.parametrize(
    "arguments",
    [
        ["--rebuild-trackpoints"],
        ["--days", "14", "--rebuild-trackpoints", "--visible"],
        ["--rebuild-t"],
        ["--json-import", "legacy.json"],
        ["--json-import=legacy.json"],
    ],
)
def test_frozen_dispatch_quarantines_upstream_utility_modes_before_handler(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    """GREEN CHARACTERIZATION: frozen self-dispatch has the same allowlist."""
    upstream = ModuleType("garmin_givemydata")
    upstream.main = lambda: pytest.fail("rejected arguments reached upstream handler")
    monkeypatch.setitem(sys.modules, "garmin_givemydata", upstream)
    monkeypatch.setenv("GARMIN_DATA_DIR", str(tmp_path))

    assert cli_backup_ingest._run_bundled_givemydata(arguments) == 2
    assert not (tmp_path / "drivers").exists()


def test_normal_startup_schema_initialization_does_not_rebuild_trackpoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """GREEN CHARACTERIZATION: startup applies schema and preserves history only."""
    activity_id = 31_000_001
    db_path = tmp_path / "garmin.db"
    fit_dir = tmp_path / "fit"
    fit_dir.mkdir()
    (fit_dir / f"2026-09-28_{activity_id}_run.zip").write_bytes(b"sentinel")

    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE activity (
                activity_id INTEGER PRIMARY KEY,
                start_time_gmt TEXT
            );
            CREATE TABLE activity_trackpoints (
                activity_id INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                timestamp_utc TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                altitude_m REAL,
                distance_m REAL,
                speed_mps REAL,
                heart_rate_bpm INTEGER,
                cadence INTEGER,
                power_w INTEGER,
                temperature_c REAL,
                PRIMARY KEY (activity_id, seq),
                FOREIGN KEY (activity_id) REFERENCES activity(activity_id)
            );
            INSERT INTO activity(activity_id, start_time_gmt)
            VALUES (31000001, '2026-09-28T08:00:00Z');
            INSERT INTO activity_trackpoints(activity_id, seq, timestamp_utc)
            VALUES (31000001, 7, 'historical-sentinel');
            """
        )

    real_iterdir = Path.iterdir

    def reject_fit_enumeration(path: Path):
        if path == fit_dir:
            pytest.fail("ordinary startup enumerated historical FIT archives")
        return real_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", reject_fit_enumeration)
    monkeypatch.setattr(
        cli_backup_ingest,
        "_snapshot_fit_archives",
        lambda *_args, **_kwargs: pytest.fail(
            "ordinary startup entered the sync archive snapshot path"
        ),
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "_run_bundled_givemydata",
        lambda *_args, **_kwargs: pytest.fail(
            "ordinary startup invoked the frozen upstream handler"
        ),
    )
    monkeypatch.setattr(
        cli_backup_ingest.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail(
            "ordinary startup launched an upstream process"
        ),
    )

    def reject_historical_ingestion(*_args, **_kwargs):
        pytest.fail("ordinary startup invoked historical trackpoint ingestion")

    monkeypatch.setattr(
        trackpoints,
        "ingest_trackpoints_from_fit_archives",
        reject_historical_ingestion,
    )
    monkeypatch.setattr(
        cli_backup_ingest,
        "ingest_trackpoints_from_fit_archives",
        reject_historical_ingestion,
    )

    prepared_path, sandboxed = nicegui_app._prepare_database(
        SimpleNamespace(
            source_db=str(db_path),
            sandbox=False,
            preview_db=None,
            live_db=False,
        )
    )

    assert prepared_path == db_path
    assert sandboxed is False
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT seq, timestamp_utc FROM activity_trackpoints "
            "WHERE activity_id = ?",
            (activity_id,),
        ).fetchall() == [(7, "historical-sentinel")]
