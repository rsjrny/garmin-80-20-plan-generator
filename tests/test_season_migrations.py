import json
import sqlite3

import pytest

from garmin_data_hub.db import migrate
from garmin_data_hub.db.sqlite import connect_sqlite
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services.plan_persistence import active_plan_snapshot
from test_plan_revision_persistence import _database, _fitzgerald_candidate, _approve
from test_legacy_plan_conversion import _database as legacy_database, _convert, resolve_legacy_plan


def remove_v12(conn):
    conn.execute("DROP TABLE season_revision_application")
    conn.execute("DROP TABLE season_event")
    conn.execute("DROP TABLE season_plan")
    conn.execute("DELETE FROM schema_migrations WHERE version >= 12")
    conn.commit()


def preserved(conn):
    return (active_plan_snapshot(conn), tuple(tuple(r) for r in conn.execute("SELECT * FROM plan_revision ORDER BY revision_id")),
            tuple(tuple(r) for r in conn.execute("SELECT * FROM planned_workout ORDER BY planned_workout_id")),
            tuple(tuple(r) for r in conn.execute("SELECT * FROM app_settings ORDER BY key")))


@pytest.mark.parametrize("kind", ["fresh", "legacy", "native", "converted"])
def test_representative_v11_snapshots_upgrade_without_plan_mutation(tmp_path, kind):
    if kind in {"legacy", "converted"}:
        db = legacy_database(tmp_path)
        if kind == "converted":
            _convert(db, resolve_legacy_plan(db))
    else:
        db = _database(tmp_path)
        if kind == "native":
            _approve(db, _fitzgerald_candidate())
    conn = connect_sqlite(db)
    try:
        conn.execute("INSERT INTO app_settings(key,value) VALUES('last_generated_plan',?)",
                     (json.dumps(json.dumps({"day_plans": [], "source": "legacy"})),))
        conn.commit()
        remove_v12(conn)
        before = preserved(conn)
        migrate.apply_schema(conn, schema_sql_path())
        migrate.apply_schema(conn, schema_sql_path())
        assert migrate.get_current_schema_version(conn) == migrate.CURRENT_SCHEMA_VERSION
        assert conn.execute("SELECT COUNT(*) FROM season_plan").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM season_event").fetchone()[0] == 0
        assert preserved(conn) == before
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_v12_ddl_and_version_roll_back_together(tmp_path, monkeypatch):
    db = _database(tmp_path)
    conn = connect_sqlite(db)
    remove_v12(conn)
    before = preserved(conn)
    original = migrate._migration_12_add_season_intent
    def fail_after_ddl(connection, schema):
        original(connection, schema)
        raise sqlite3.OperationalError("injected season migration failure")
    monkeypatch.setattr(migrate, "_migration_12_add_season_intent", fail_after_ddl)
    with pytest.raises(sqlite3.OperationalError, match="injected"):
        migrate.apply_schema(conn, schema_sql_path())
    assert conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 11
    assert not conn.execute("SELECT name FROM sqlite_master WHERE name IN ('season_plan','season_event')").fetchall()
    assert preserved(conn) == before
    conn.close()


def test_current_version_repairs_missing_objects_but_reports_incompatible_tables(tmp_path):
    db = _database(tmp_path)
    conn = connect_sqlite(db)
    conn.execute("DROP INDEX idx_season_event_calendar")
    conn.execute("DROP TABLE season_event")
    conn.commit()
    migrate.apply_schema(conn, schema_sql_path())
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='idx_season_event_calendar'").fetchone()
    conn.execute("DROP TABLE season_event")
    conn.execute("CREATE TABLE season_event(event_id TEXT, season_id TEXT, event_date TEXT, status TEXT)")
    conn.commit()
    with pytest.raises(sqlite3.OperationalError, match="incompatible season_event"):
        migrate.apply_schema(conn, schema_sql_path())
    conn.close()
