"""Verify migration on a private SQLite backup; source is opened read-only.

Usage: python reports/yearly_y1/verify_real_snapshot.py path/to/garmin.db
Only aggregate results are printed. The temporary full backup is removed.
"""
import argparse
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from garmin_data_hub.db.migrate import apply_schema, get_current_schema_version
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.plan_persistence import active_plan_snapshot


def state(conn):
    tables = ("planned_workout", "training_plan", "plan_revision", "plan_revision_workout",
              "plan_workout_segment", "activity_workout_match", "legacy_plan_conversion",
              "legacy_plan_conversion_source_workout", "plan_import_history", "app_settings")
    # Compare complete app-owned state, not just counts. Do not print its contents.
    values = {}
    for table in tables:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        if exists:
            values[table] = sorted(tuple(row) for row in conn.execute(f"SELECT * FROM {table}"))
    return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    options = parser.parse_args()
    source_path = options.source.resolve(strict=True)
    with TemporaryDirectory(prefix="training-planner-y1-snapshot-") as temporary:
        destination = Path(temporary) / "snapshot.db"
        # The sandbox cannot create SQLite shared-memory sidecars beside the source.
        # Only use immutable read mode for a checkpointed, stable source: reject a
        # WAL and verify the source file has not changed throughout the backup.
        wal = Path(str(source_path) + "-wal")
        if wal.exists():
            raise RuntimeError("Source has a WAL; close the app and checkpoint before snapshot verification.")
        fingerprint = (source_path.stat().st_size, source_path.stat().st_mtime_ns)
        source = sqlite3.connect(source_path.as_uri() + "?mode=ro&immutable=1", uri=True)
        copy = sqlite3.connect(destination)
        try:
            source.backup(copy)
            if wal.exists() or (source_path.stat().st_size, source_path.stat().st_mtime_ns) != fingerprint:
                raise RuntimeError("Source changed during backup; discard this snapshot and retry while idle.")
        except BaseException:
            copy.close()
            raise
        finally:
            source.close()
        try:
            copy.row_factory = sqlite3.Row
            copy.execute("PRAGMA foreign_keys=ON")
            version = get_current_schema_version(copy)
            before = state(copy)
            active = active_plan_snapshot(copy)
            violations = [tuple(row) for row in copy.execute("PRAGMA foreign_key_check")]
            apply_schema(copy, schema_sql_path())
            apply_schema(copy, schema_sql_path())
            assert get_current_schema_version(copy) == 12
            assert state(copy) == before, "app-owned state changed"
            assert active_plan_snapshot(copy) == active, "active schedule changed"
            assert [tuple(row) for row in copy.execute("PRAGMA foreign_key_check")] == violations
            assert copy.execute("SELECT COUNT(*) FROM season_plan").fetchone()[0] == 0
            assert copy.execute("SELECT COUNT(*) FROM season_event").fetchone()[0] == 0
            print(f"PASS: real read-only SQLite backup upgraded v{version} -> v12 and replayed")
            print("PASS: every app-owned row and active schedule unchanged")
            print(f"PASS: foreign-key state unchanged ({len(violations)} pre-existing violations)")
            print("PASS: season/event tables empty; no automatic event inference or conversion")
        finally:
            copy.close()
    print("PASS: private full database backup removed; source was never migrated")


if __name__ == "__main__":
    main()
