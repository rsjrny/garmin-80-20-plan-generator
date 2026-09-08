"""OS-level process-tree checks for Garmin sync cancellation."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from garmin_data_hub.ui_nicegui.process_tree import (
    attach_process_tree,
    popen_process_tree_kwargs,
)
from garmin_data_hub.ui_nicegui import process_tree as process_tree_module


def test_taskkill_fallback_confirms_root_exit(monkeypatch) -> None:
    class FakeProcess:
        pid = 24680
        return_code: int | None = None

        def poll(self) -> int | None:
            return self.return_code

        def wait(self, timeout: float | None = None) -> int:
            assert timeout is not None and timeout > 0
            assert self.return_code is not None
            return self.return_code

    process = FakeProcess()

    def fake_run(*_args, **kwargs):
        assert 0 < kwargs["timeout"] <= 5.0
        process.return_code = -9
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        process_tree_module,
        "_taskkill_path",
        lambda: Path(r"C:\Windows\System32\taskkill.exe"),
    )
    monkeypatch.setattr(process_tree_module.subprocess, "run", fake_run)

    controller = process_tree_module._WindowsTaskkillTree()
    assert controller.terminate(process, timeout=1.0) == -9


def _windows_process_is_running(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    synchronize = 0x00100000
    wait_timeout = 0x00000102
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(synchronize, False, pid)
    if not handle:
        return False
    try:
        return kernel32.WaitForSingleObject(handle, 0) == wait_timeout
    finally:
        kernel32.CloseHandle(handle)


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object integration")
def test_windows_process_tree_stops_inherited_grandchild() -> None:
    child_code = "import time; time.sleep(60)"
    parent_code = (
        "import subprocess, sys, time\n"
        "sys.stdin.readline()\n"
        f"child = subprocess.Popen([sys.executable, '-c', {child_code!r}])\n"
        "print(child.pid, flush=True)\n"
        "time.sleep(60)\n"
    )
    parent = subprocess.Popen(
        [sys.executable, "-u", "-c", parent_code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **popen_process_tree_kwargs(),
    )
    process_tree = attach_process_tree(parent)
    child_pid: int | None = None
    try:
        assert parent.stdin is not None
        assert parent.stdout is not None
        parent.stdin.write("start\n")
        parent.stdin.flush()
        child_pid = int(parent.stdout.readline().strip())
        assert _windows_process_is_running(child_pid)

        return_code = process_tree.terminate(parent, timeout=5.0)
        process_tree.close_after_exit()

        assert isinstance(return_code, int)
        deadline = time.monotonic() + 3.0
        while _windows_process_is_running(child_pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _windows_process_is_running(child_pid)
    finally:
        if parent.poll() is None:
            try:
                process_tree.terminate(parent, timeout=2.0)
            except Exception:
                parent.kill()
                parent.wait(timeout=2.0)
        if child_pid is not None and _windows_process_is_running(child_pid):
            os.kill(child_pid, 9)
