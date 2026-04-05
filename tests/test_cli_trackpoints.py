from __future__ import annotations

from pathlib import Path

from garmin_data_hub.cli_backup_ingest import _select_trackpoint_archive_paths


def test_select_trackpoint_archives_uses_changed_files_when_present():
    changed = [Path("a.zip"), Path("b.zip")]

    result = _select_trackpoint_archive_paths(changed, rebuild_trackpoints=False)

    assert result == changed


def test_select_trackpoint_archives_backfills_from_existing_cache_when_none_changed():
    result = _select_trackpoint_archive_paths([], rebuild_trackpoints=False)

    assert result is None


def test_select_trackpoint_archives_rebuild_mode_scans_all_archives():
    changed = [Path("a.zip")]

    result = _select_trackpoint_archive_paths(changed, rebuild_trackpoints=True)

    assert result is None
