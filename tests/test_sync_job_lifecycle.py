"""Lifecycle regressions for the NiceGUI Garmin sync controller."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from garmin_data_hub.ui_nicegui import data as nicegui_data
from garmin_data_hub.ui_nicegui import pages as nicegui_pages


class FakeProcess:
    def __init__(self, return_code: int | None = None):
        self.pid = 43210
        self.return_code = return_code
        self.wait_calls: list[float | None] = []

    def poll(self) -> int | None:
        return self.return_code

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls.append(timeout)
        if self.return_code is None:
            raise TimeoutError("fake process is still running")
        return self.return_code


class BrokenWaitProcess(FakeProcess):
    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls.append(timeout)
        raise OSError("wait handle failed")


class BlockingProcessTree:
    def __init__(
        self,
        *,
        return_code: int = -9,
        failure: BaseException | None = None,
    ):
        self.return_code = return_code
        self.failure = failure
        self.terminate_started = threading.Event()
        self.release_terminate = threading.Event()
        self.terminate_timeouts: list[float] = []
        self.close_calls = 0

    def terminate(self, process: FakeProcess, *, timeout: float) -> int:
        self.terminate_timeouts.append(timeout)
        self.terminate_started.set()
        if not self.release_terminate.wait(timeout=2.0):
            raise TimeoutError("test did not release process-tree termination")
        if self.failure is not None:
            raise self.failure
        process.return_code = self.return_code
        return self.return_code

    def close_after_exit(self) -> None:
        self.close_calls += 1


class BlockingCleanupTree(BlockingProcessTree):
    def __init__(self):
        super().__init__()
        self.cleanup_started = threading.Event()
        self.release_cleanup = threading.Event()

    def close_after_exit(self) -> None:
        self.cleanup_started.set()
        assert self.release_cleanup.wait(timeout=2.0)
        super().close_after_exit()


def _running_job(
    tmp_path: Path,
    process: FakeProcess,
    process_tree: BlockingProcessTree,
) -> nicegui_data.SyncJob:
    job = nicegui_data.SyncJob(tmp_path / "garmin.db")
    job._process = process
    job._process_tree = process_tree
    job._state = "running"
    job._started = time.monotonic()
    job._return_code = None
    job._error = None
    return job


def _wait_for_snapshot(
    job: nicegui_data.SyncJob,
    predicate,
    *,
    timeout: float = 2.0,
) -> nicegui_data.SyncSnapshot:
    deadline = time.monotonic() + timeout
    snapshot = job.snapshot()
    while not predicate(snapshot) and time.monotonic() < deadline:
        time.sleep(0.01)
        snapshot = job.snapshot()
    assert predicate(snapshot), snapshot
    return snapshot


def test_terminal_failure_freezes_elapsed_time(monkeypatch, tmp_path):
    process = FakeProcess(return_code=23)
    process_tree = BlockingProcessTree()
    job = _running_job(tmp_path, process, process_tree)
    job._started = 10.0
    now = [15.0]
    monkeypatch.setattr(nicegui_data.time, "monotonic", lambda: now[0])

    failed = job.snapshot()
    now[0] = 60.0
    later = job.snapshot()

    assert failed.state == "failed"
    assert failed.return_code == 23
    assert failed.elapsed_seconds == 5.0
    assert later.elapsed_seconds == failed.elapsed_seconds


def test_cancel_returns_before_process_tree_termination_is_confirmed(tmp_path):
    process = FakeProcess()
    process_tree = BlockingProcessTree(return_code=-9)
    job = _running_job(tmp_path, process, process_tree)

    assert job.cancel() is True
    assert process_tree.terminate_started.wait(timeout=1.0)

    cancelling = job.snapshot()
    assert cancelling.state == "cancelling"
    assert cancelling.return_code is None

    process_tree.release_terminate.set()
    cancelled = _wait_for_snapshot(job, lambda item: item.state == "cancelled")

    assert cancelled.return_code == -9
    assert cancelled.error is None
    assert process_tree.close_calls == 1


def test_cancel_failure_does_not_report_false_terminal_state(tmp_path):
    process = FakeProcess()
    process_tree = BlockingProcessTree(failure=OSError("taskkill failed"))
    job = _running_job(tmp_path, process, process_tree)

    assert job.cancel() is True
    assert process_tree.terminate_started.wait(timeout=1.0)
    process_tree.release_terminate.set()

    recovered = _wait_for_snapshot(
        job,
        lambda item: item.state == "running" and item.error is not None,
    )

    assert "taskkill failed" in recovered.error
    assert recovered.return_code is None
    assert process.poll() is None
    assert process_tree.close_calls == 0


def test_cancel_falls_back_synchronously_if_worker_thread_cannot_start(
    monkeypatch, tmp_path
):
    process = FakeProcess()
    process_tree = BlockingProcessTree(return_code=-9)
    process_tree.release_terminate.set()
    job = _running_job(tmp_path, process, process_tree)
    monkeypatch.setattr(
        nicegui_data.threading.Thread,
        "start",
        lambda _self: (_ for _ in ()).throw(RuntimeError("no thread slots")),
    )

    assert job.cancel() is True

    snapshot = job.snapshot()
    assert snapshot.state == "cancelled"
    assert snapshot.return_code == -9


def test_monitor_thread_start_failure_stops_the_process_tree(monkeypatch, tmp_path):
    process = FakeProcess()
    process_tree = BlockingProcessTree(return_code=-9)
    process_tree.release_terminate.set()
    job = _running_job(tmp_path, process, process_tree)
    monkeypatch.setattr(
        nicegui_data.threading.Thread,
        "start",
        lambda _self: (_ for _ in ()).throw(RuntimeError("no thread slots")),
    )

    with pytest.raises(RuntimeError, match="sync monitor"):
        job._start_monitor_locked(process, process_tree)

    snapshot = job.snapshot()
    assert snapshot.state == "failed"
    assert snapshot.return_code == -9
    assert process_tree.close_calls == 1


def test_start_rejects_stale_terminal_state_with_live_process(
    monkeypatch, tmp_path
):
    process = FakeProcess()
    process_tree = BlockingProcessTree()
    job = _running_job(tmp_path, process, process_tree)
    job._state = "cancelled"
    monkeypatch.setattr(job, "_command", lambda _days: ["sync-helper"])
    monkeypatch.setattr(
        nicegui_data.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail(
            "a second sync process must not be spawned"
        ),
    )

    with pytest.raises(RuntimeError, match="running"):
        job.start(days=0)


def test_monitor_publishes_terminal_state_only_after_tree_cleanup(tmp_path):
    process = FakeProcess(return_code=0)
    process_tree = BlockingCleanupTree()
    job = _running_job(tmp_path, process, process_tree)
    monitor = threading.Thread(
        target=job._monitor_process,
        args=(process, process_tree),
        daemon=True,
    )
    job._monitor_thread = monitor
    monitor.start()
    assert process_tree.cleanup_started.wait(timeout=1.0)

    assert job.snapshot().state == "running"

    process_tree.release_cleanup.set()
    monitor.join(timeout=1.0)
    assert job.snapshot().state == "completed"


def test_monitor_wait_error_still_cleans_tree_after_root_exit(tmp_path):
    process = BrokenWaitProcess()
    process_tree = BlockingProcessTree()
    job = _running_job(tmp_path, process, process_tree)
    monitor = threading.Thread(
        target=job._monitor_process,
        args=(process, process_tree),
        daemon=True,
    )
    job._monitor_thread = monitor
    monitor.start()
    _wait_for_snapshot(job, lambda item: item.error is not None)

    process.return_code = 17
    monitor.join(timeout=1.0)

    snapshot = job.snapshot()
    assert snapshot.state == "failed"
    assert snapshot.return_code == 17
    assert process_tree.close_calls == 1


def test_cancel_worker_preserves_monitor_cleanup_failure(tmp_path):
    process = FakeProcess(return_code=-9)
    process_tree = BlockingProcessTree(return_code=-9)
    process_tree.release_terminate.set()
    job = _running_job(tmp_path, process, process_tree)
    job._state = "failed"
    job._error = "Sync exited, but descendant cleanup failed: access denied"

    job._finish_cancel(process, process_tree)

    snapshot = job.snapshot()
    assert snapshot.state == "failed"
    assert snapshot.error is not None
    assert "descendant cleanup failed" in snapshot.error


def test_shutdown_retries_once_but_does_not_claim_a_live_tree_stopped(tmp_path):
    process = FakeProcess()
    process_tree = BlockingProcessTree(failure=OSError("tree survived"))
    process_tree.release_terminate.set()
    job = _running_job(tmp_path, process, process_tree)

    job.shutdown()

    snapshot = job.snapshot()
    assert process_tree.terminate_timeouts == [
        job._STOP_TIMEOUT_SECONDS,
        job._STOP_TIMEOUT_SECONDS,
    ]
    assert snapshot.state == "running"
    assert snapshot.return_code is None
    assert snapshot.error is not None
    assert "tree survived" in snapshot.error


def test_shutdown_waits_for_in_progress_descendant_cleanup(tmp_path):
    process = FakeProcess(return_code=0)
    process_tree = BlockingCleanupTree()
    job = _running_job(tmp_path, process, process_tree)
    monitor = threading.Thread(
        target=job._monitor_process,
        args=(process, process_tree),
        daemon=True,
    )
    job._monitor_thread = monitor
    monitor.start()
    assert process_tree.cleanup_started.wait(timeout=1.0)

    shutdown = threading.Thread(target=job.shutdown)
    shutdown.start()
    time.sleep(0.05)
    assert shutdown.is_alive()

    process_tree.release_cleanup.set()
    shutdown.join(timeout=1.0)
    assert not shutdown.is_alive()
    assert job.snapshot().state == "completed"


def test_browser_login_reset_blocks_sync_start_across_clients(
    monkeypatch, tmp_path
):
    reset_started = threading.Event()
    release_reset = threading.Event()

    def blocking_reset(_data_directory):
        reset_started.set()
        assert release_reset.wait(timeout=2.0)
        return nicegui_data.BrowserSessionResetResult(False, False)

    monkeypatch.setattr(
        nicegui_data,
        "reset_garmin_browser_session",
        blocking_reset,
    )
    job = nicegui_data.SyncJob(tmp_path / "garmin.db")
    reset_thread = threading.Thread(target=job.reset_browser_session)
    reset_thread.start()
    assert reset_started.wait(timeout=1.0)

    assert job.snapshot().state == "resetting_login"
    with pytest.raises(RuntimeError, match="being reset"):
        job.start(days=0)

    release_reset.set()
    reset_thread.join(timeout=1.0)
    assert not reset_thread.is_alive()
    assert job.snapshot().state == "idle"


def test_atomic_login_update_prevents_start_between_reset_and_operation(
    monkeypatch, tmp_path
):
    operation_started = threading.Event()
    release_operation = threading.Event()
    competing_start_called = threading.Event()
    spawned_processes: list[FakeProcess] = []
    update_results: list[object] = []
    update_errors: list[BaseException] = []
    competing_errors: list[BaseException] = []

    profile = tmp_path / "browser_profile"
    profile.mkdir()

    def fake_reset(_data_directory):
        return nicegui_data.BrowserSessionResetResult(True, False)

    class AttachedTree:
        warning = None

    def fake_popen(_command, **_kwargs):
        process = FakeProcess()
        spawned_processes.append(process)
        return process

    monkeypatch.setattr(
        nicegui_data,
        "reset_garmin_browser_session",
        fake_reset,
    )
    monkeypatch.setattr(nicegui_data.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        nicegui_data,
        "attach_process_tree",
        lambda _process: AttachedTree(),
    )

    job = nicegui_data.SyncJob(tmp_path / "garmin.db")
    monkeypatch.setattr(job, "_command", lambda _days: ["sync-helper"])
    monkeypatch.setattr(job, "_start_monitor_locked", lambda *_args: None)

    def update_operation():
        operation_started.set()
        assert release_operation.wait(timeout=2.0)
        job.start(days=0)
        return "updated"

    def apply_update():
        try:
            update_results.append(
                job.apply_browser_login_update(
                    update_operation,
                    reset_confirmed=True,
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            update_errors.append(exc)

    def competing_start():
        competing_start_called.set()
        try:
            job.start(days=0)
        except BaseException as exc:
            competing_errors.append(exc)

    update_thread = threading.Thread(target=apply_update)
    competing_thread = threading.Thread(target=competing_start)
    update_thread.start()
    assert operation_started.wait(timeout=1.0)
    competing_thread.start()
    assert competing_start_called.wait(timeout=1.0)
    try:
        time.sleep(0.05)
        assert competing_thread.is_alive()
        assert spawned_processes == []
    finally:
        release_operation.set()
        update_thread.join(timeout=1.0)
        competing_thread.join(timeout=1.0)

    assert not update_thread.is_alive()
    assert not competing_thread.is_alive()
    assert len(update_results) == 1
    reset_result, operation_result = update_results[0]
    assert reset_result == nicegui_data.BrowserSessionResetResult(True, False)
    assert operation_result == "updated"
    assert update_errors == []
    assert len(spawned_processes) == 1
    assert len(competing_errors) == 1
    assert isinstance(competing_errors[0], RuntimeError)
    assert "already running" in str(competing_errors[0])


def test_atomic_login_update_requires_reset_before_operation_when_state_exists(
    monkeypatch, tmp_path
):
    session = tmp_path / "garmin_session.json"
    session.write_text("{}", encoding="utf-8")
    operation_calls = 0

    def operation():
        nonlocal operation_calls
        operation_calls += 1

    monkeypatch.setattr(
        nicegui_data,
        "reset_garmin_browser_session",
        lambda _directory: pytest.fail("an unconfirmed reset must not run"),
    )
    job = nicegui_data.SyncJob(tmp_path / "garmin.db")

    with pytest.raises(
        nicegui_data.BrowserSessionResetRequired,
        match="reset",
    ):
        job.apply_browser_login_update(operation, reset_confirmed=False)

    assert operation_calls == 0
    assert session.is_file()
    assert job.snapshot().state == "idle"


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("idle", (True, False, True)),
        ("running", (False, True, False)),
        ("cancelling", (False, False, False)),
        ("resetting_login", (False, False, False)),
        ("completed", (True, False, True)),
        ("failed", (True, False, True)),
        ("cancelled", (True, False, True)),
    ],
)
def test_sync_controls_follow_shared_job_state(state, expected):
    assert nicegui_pages._sync_controls_for_state(state) == expected
