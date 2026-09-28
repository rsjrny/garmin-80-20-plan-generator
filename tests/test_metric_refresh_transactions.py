from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from garmin_data_hub.db import queries
from garmin_data_hub.db.sqlite import connect_sqlite


def _insert_activity(conn, activity_id: int) -> None:
    conn.execute(
        """
        INSERT INTO activity(
            activity_id, start_time_gmt, elapsed_duration_seconds,
            moving_duration_seconds, average_speed, average_hr, max_hr,
            activity_type
        ) VALUES (?, '2026-09-01T12:00:00Z', 3600, 3300, 3.0, 145, 180,
                  'running')
        """,
        (activity_id,),
    )
    conn.commit()


def _database_path(conn) -> Path:
    row = conn.execute("PRAGMA database_list").fetchone()
    assert row is not None
    return Path(row[2])


def _fail_after_scalar_refresh(monkeypatch, exception: Exception) -> None:
    def fail(*_args, **_kwargs):
        raise exception

    monkeypatch.setattr(queries, "_refresh_trackpoint_derived_metrics", fail)


def _setting_exists(conn, key: str) -> bool:
    return (
        conn.execute("SELECT 1 FROM app_settings WHERE key = ?", (key,)).fetchone()
        is not None
    )


def test_top_level_refresh_success_commits_its_work(db_conn):
    _insert_activity(db_conn, 101)
    assert not db_conn.in_transaction

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[101], lthr=160
    )

    assert summary == {
        "target_activities": 1,
        "rows_upserted": 1,
        "zones_updated": 1,
        "errors": 0,
    }
    assert not db_conn.in_transaction
    observer = connect_sqlite(_database_path(db_conn))
    try:
        assert observer.execute(
            "SELECT moving_time_s FROM activity_metrics WHERE activity_id = 101"
        ).fetchone()[0] == pytest.approx(3300)
    finally:
        observer.close()


def test_top_level_refresh_failure_rolls_back_every_refresh_write(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 102)
    db_conn.execute(
        "INSERT INTO activity_metrics(activity_id, moving_time_s) VALUES (102, 111)"
    )
    db_conn.execute(
        """
        CREATE TRIGGER fail_metric_update
        BEFORE UPDATE ON activity_metrics
        WHEN NEW.activity_id = 102
        BEGIN
            SELECT RAISE(ABORT, 'injected metric failure');
        END
        """
    )
    db_conn.commit()

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[102], lthr=160
    )

    assert summary["errors"] == 1
    assert summary["rows_upserted"] == 0
    assert summary["zones_updated"] == 0
    assert db_conn.execute(
        "SELECT moving_time_s FROM activity_metrics WHERE activity_id = 102"
    ).fetchone()[0] == pytest.approx(111)


def test_refresh_inside_caller_transaction_success_releases_only_savepoint(db_conn):
    _insert_activity(db_conn, 103)
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('caller_success', 'pending')"
    )

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[103], lthr=160
    )

    assert summary["errors"] == 0
    assert db_conn.in_transaction
    assert db_conn.execute(
        "SELECT moving_time_s FROM activity_metrics WHERE activity_id = 103"
    ).fetchone()[0] == pytest.approx(3300)
    db_conn.commit()
    assert not db_conn.in_transaction


def test_refresh_inside_caller_transaction_failure_rolls_back_only_savepoint(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 104)
    db_conn.execute(
        "INSERT INTO activity_metrics(activity_id, moving_time_s) VALUES (104, 111)"
    )
    db_conn.commit()
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('caller_failure', 'pending')"
    )
    _fail_after_scalar_refresh(monkeypatch, RuntimeError("ordinary refresh failure"))

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[104], lthr=160
    )

    assert summary["errors"] == 1
    assert db_conn.in_transaction
    assert _setting_exists(db_conn, "caller_failure")
    assert db_conn.execute(
        "SELECT moving_time_s FROM activity_metrics WHERE activity_id = 104"
    ).fetchone()[0] == pytest.approx(111)
    db_conn.rollback()


def test_caller_changes_remain_uncommitted_after_nested_refresh_success(db_conn):
    _insert_activity(db_conn, 105)
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('pending_success', 'caller')"
    )

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[105], lthr=160
    )

    assert summary["errors"] == 0
    observer = connect_sqlite(_database_path(db_conn))
    try:
        assert not _setting_exists(observer, "pending_success")
        assert observer.execute(
            "SELECT 1 FROM activity_metrics WHERE activity_id = 105"
        ).fetchone() is None
    finally:
        observer.close()
    assert _setting_exists(db_conn, "pending_success")
    db_conn.rollback()
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 105"
    ).fetchone() is None


def test_caller_changes_remain_uncommitted_after_nested_refresh_failure(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 106)
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('pending_failure', 'caller')"
    )
    _fail_after_scalar_refresh(monkeypatch, RuntimeError("ordinary refresh failure"))

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[106], lthr=160
    )

    assert summary["errors"] == 1
    assert db_conn.in_transaction
    assert _setting_exists(db_conn, "pending_failure")
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 106"
    ).fetchone() is None
    observer = connect_sqlite(_database_path(db_conn))
    try:
        assert not _setting_exists(observer, "pending_failure")
    finally:
        observer.close()
    db_conn.rollback()


def test_failure_after_profile_preparation_rolls_back_preparation_write(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 107)

    def fail_during_ftp_persistence(conn, ftp, *, commit):
        assert ftp == 250
        assert commit is False
        conn.execute(
            "INSERT INTO app_settings(key, value) VALUES ('refresh_preparation', 'written')"
        )
        raise RuntimeError("failure during profile preparation")

    monkeypatch.setattr(
        queries,
        "get_effective_ftp",
        lambda conn, *, commit: fail_during_ftp_persistence(
            conn, 250, commit=commit
        ),
    )

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[107], lthr=160
    )

    assert summary["errors"] == 1
    assert not _setting_exists(db_conn, "refresh_preparation")
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 107"
    ).fetchone() is None


def test_new_activity_metrics_row_disappears_after_failed_refresh(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 108)
    _fail_after_scalar_refresh(monkeypatch, sqlite3.OperationalError("injected"))

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[108], lthr=160
    )

    assert summary["errors"] == 1
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 108"
    ).fetchone() is None


def test_top_level_failure_leaks_no_transaction_and_reports_no_persisted_rows(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 109)
    _fail_after_scalar_refresh(monkeypatch, sqlite3.OperationalError("injected"))

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[109], lthr=160
    )

    assert not db_conn.in_transaction
    assert summary["errors"] == 1
    assert summary["rows_upserted"] == 0
    assert summary["zones_updated"] == 0
    persisted_summary = queries.get_setting(
        db_conn, queries.ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY, None
    )
    assert persisted_summary == summary
    assert not db_conn.in_transaction


def test_ordinary_exception_after_partial_refresh_work_is_cleaned_up(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 110)
    _fail_after_scalar_refresh(monkeypatch, RuntimeError("not a sqlite exception"))

    summary = queries.refresh_persisted_activity_metrics(
        db_conn, activity_ids=[110], lthr=160
    )

    assert summary["errors"] == 1
    assert summary["rows_upserted"] == 0
    assert summary["zones_updated"] == 0
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 110"
    ).fetchone() is None
    assert not db_conn.in_transaction


class _CommitProxy:
    def __init__(self, conn, *, commit_before_raise: bool):
        self._conn = conn
        self._commit_before_raise = commit_before_raise
        self._raise_on_next_commit = True

    @property
    def in_transaction(self):
        return self._conn.in_transaction

    def execute(self, *args, **kwargs):
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._conn.executemany(*args, **kwargs)

    def rollback(self):
        return self._conn.rollback()

    def commit(self):
        if self._raise_on_next_commit:
            self._raise_on_next_commit = False
            if self._commit_before_raise:
                self._conn.commit()
            raise RuntimeError("injected commit finalization failure")
        return self._conn.commit()


def test_commit_failure_rolls_back_refresh_before_reporting_failure(db_conn):
    _insert_activity(db_conn, 111)
    proxy = _CommitProxy(db_conn, commit_before_raise=False)

    summary = queries.refresh_persisted_activity_metrics(
        proxy, activity_ids=[111], lthr=160
    )

    assert summary["errors"] == 1
    assert summary["rows_upserted"] == 0
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 111"
    ).fetchone() is None
    assert not db_conn.in_transaction


def test_post_commit_exception_does_not_report_committed_refresh_as_failed(db_conn):
    _insert_activity(db_conn, 112)
    proxy = _CommitProxy(db_conn, commit_before_raise=True)

    summary = queries.refresh_persisted_activity_metrics(
        proxy, activity_ids=[112], lthr=160
    )

    assert summary["errors"] == 0
    assert summary["rows_upserted"] == 1
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 112"
    ).fetchone() is not None
    assert not db_conn.in_transaction


class _ReleaseFailureProxy:
    def __init__(self, conn):
        self._conn = conn
        self._raise_on_next_release = True

    @property
    def in_transaction(self):
        return self._conn.in_transaction

    def execute(self, sql, *args, **kwargs):
        if (
            self._raise_on_next_release
            and sql.startswith("RELEASE SAVEPOINT refresh_persisted_activity_metrics_")
        ):
            self._raise_on_next_release = False
            raise RuntimeError("injected savepoint release failure")
        return self._conn.execute(sql, *args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._conn.executemany(*args, **kwargs)

    def rollback(self):
        pytest.fail("nested refresh must not roll back the caller transaction")

    def commit(self):
        pytest.fail("nested refresh must not commit the caller transaction")


def test_nested_finalization_exception_rolls_back_refresh_savepoint(db_conn):
    _insert_activity(db_conn, 113)
    db_conn.execute(
        "INSERT INTO app_settings(key, value) VALUES ('release_failure', 'caller')"
    )
    proxy = _ReleaseFailureProxy(db_conn)

    summary = queries.refresh_persisted_activity_metrics(
        proxy, activity_ids=[113], lthr=160
    )

    assert summary["errors"] == 1
    assert summary["rows_upserted"] == 0
    assert db_conn.in_transaction
    assert _setting_exists(db_conn, "release_failure")
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 113"
    ).fetchone() is None
    db_conn.rollback()


def test_error_summary_commit_failure_is_rolled_back_without_leaking_transaction(
    db_conn, monkeypatch
):
    _insert_activity(db_conn, 114)
    _fail_after_scalar_refresh(monkeypatch, RuntimeError("refresh failure"))
    proxy = _CommitProxy(db_conn, commit_before_raise=False)

    summary = queries.refresh_persisted_activity_metrics(
        proxy, activity_ids=[114], lthr=160
    )

    assert summary["errors"] == 1
    assert not db_conn.in_transaction
    assert not _setting_exists(
        db_conn, queries.ACTIVITY_METRICS_LAST_REFRESH_SUMMARY_KEY
    )
    assert db_conn.execute(
        "SELECT 1 FROM activity_metrics WHERE activity_id = 114"
    ).fetchone() is None
