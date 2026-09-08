"""Cross-platform lifetime control for subprocess trees started by NiceGUI."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any


class ProcessTreeError(RuntimeError):
    """Raised when a process tree could not be confirmed stopped."""


_MINIMUM_WAIT_SECONDS = 0.05
_WINDOWS_CREATE_SUSPENDED = 0x00000004


def _deadline(timeout: float) -> float:
    return time.monotonic() + max(_MINIMUM_WAIT_SECONDS, timeout)


def _remaining(deadline: float) -> float:
    return max(_MINIMUM_WAIT_SECONDS, deadline - time.monotonic())


class ProcessTree:
    """Own and terminate a root process together with its descendants."""

    warning: str | None = None

    def terminate(
        self, process: subprocess.Popen[str], *, timeout: float
    ) -> int:
        raise NotImplementedError

    def close_after_exit(self) -> None:
        """Release ownership and stop any descendants left after root exit."""


def popen_process_tree_kwargs() -> dict[str, Any]:
    """Return creation options required for later tree-wide termination."""
    if os.name == "nt":
        return {"creationflags": _WINDOWS_CREATE_SUSPENDED}
    if os.name == "posix":
        return {"start_new_session": True}
    return {}


def _confirmed_wait(process: subprocess.Popen[str], timeout: float) -> int:
    try:
        return int(process.wait(timeout=max(_MINIMUM_WAIT_SECONDS, timeout)))
    except subprocess.TimeoutExpired as exc:
        raise ProcessTreeError(
            f"Process {process.pid} was still running after {timeout:.1f} seconds"
        ) from exc


class _SingleProcessTree(ProcessTree):
    """Last-resort controller for platforms without process-group support."""

    def terminate(
        self, process: subprocess.Popen[str], *, timeout: float
    ) -> int:
        code = process.poll()
        if code is not None:
            return int(code)
        process.terminate()
        return _confirmed_wait(process, timeout)


class _PosixProcessTree(ProcessTree):
    """Control a process group created through ``start_new_session``."""

    def __init__(self, process: subprocess.Popen[str]):
        self._pgid = process.pid
        self._cleanup_lock = threading.Lock()
        self._cleanup_complete = False

    def terminate(
        self, process: subprocess.Popen[str], *, timeout: float
    ) -> int:
        code = process.poll()
        if code is not None:
            return int(code)
        try:
            os.killpg(self._pgid, signal.SIGTERM)
        except ProcessLookupError:
            code = process.poll()
            if code is not None:
                return int(code)
            process.terminate()
        graceful_timeout = max(0.1, timeout * 0.6)
        try:
            return int(process.wait(timeout=graceful_timeout))
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self._pgid, signal.SIGKILL)
            except ProcessLookupError:
                process.kill()
            return _confirmed_wait(process, max(0.1, timeout - graceful_timeout))

    def close_after_exit(self) -> None:
        # The root may exit before one of its descendants. Clean up that group
        # immediately while its process-group ID is still unambiguous.
        with self._cleanup_lock:
            if self._cleanup_complete:
                return
            try:
                os.killpg(self._pgid, signal.SIGTERM)
            except ProcessLookupError:
                self._cleanup_complete = True
                return
            time.sleep(0.1)
            try:
                os.killpg(self._pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self._cleanup_complete = True


def _taskkill_path() -> Path:
    system_root = Path(os.environ.get("SystemRoot", r"C:\Windows"))
    executable = system_root / "System32" / "taskkill.exe"
    if not executable.is_file():
        raise ProcessTreeError(f"Windows taskkill was not found at {executable}")
    return executable


class _WindowsTaskkillTree(ProcessTree):
    """Validated taskkill fallback when a Windows Job Object is unavailable."""

    def __init__(self, warning: str | None = None):
        self.warning = warning

    def terminate(
        self, process: subprocess.Popen[str], *, timeout: float
    ) -> int:
        stop_deadline = _deadline(timeout)
        code = process.poll()
        if code is not None:
            return int(code)
        executable = _taskkill_path()
        try:
            result = subprocess.run(
                [str(executable), "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                check=False,
                text=True,
                timeout=min(_remaining(stop_deadline), 5.0),
            )
        except subprocess.TimeoutExpired as exc:
            code = process.poll()
            if code is None:
                raise ProcessTreeError(
                    f"Windows timed out while stopping process tree {process.pid}"
                ) from exc
            return int(code)
        code = process.poll()
        if result.returncode != 0 and code is None:
            detail = (result.stderr or result.stdout or "unknown taskkill error").strip()
            raise ProcessTreeError(
                f"Windows could not stop process tree {process.pid}: {detail}"
            )
        if code is not None:
            return int(code)
        return _confirmed_wait(process, _remaining(stop_deadline))


class _WindowsJobTree(ProcessTree):
    """A kill-on-close Windows Job Object inherited by all descendants."""

    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

    def __init__(self, process: subprocess.Popen[str]):
        import ctypes
        from ctypes import wintypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = (
            wintypes.HANDLE,
            wintypes.HANDLE,
        )
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            self._raise_last_error(ctypes, "create a Windows Job Object")
        try:
            information = ExtendedLimitInformation()
            information.BasicLimitInformation.LimitFlags = (
                self._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            )
            if not kernel32.SetInformationJobObject(
                handle,
                self._JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                ctypes.byref(information),
                ctypes.sizeof(information),
            ):
                self._raise_last_error(ctypes, "configure a Windows Job Object")
            process_handle = wintypes.HANDLE(int(process._handle))  # type: ignore[attr-defined]
            if not kernel32.AssignProcessToJobObject(handle, process_handle):
                self._raise_last_error(ctypes, "assign the sync process to its Job Object")
        except Exception:
            kernel32.CloseHandle(handle)
            raise

        self._kernel32 = kernel32
        self._handle = handle
        self._lock = threading.Lock()
        self._fallback = _WindowsTaskkillTree()

    @staticmethod
    def _raise_last_error(ctypes_module: Any, action: str) -> None:
        code = ctypes_module.get_last_error()
        raise OSError(code, f"Could not {action}: {ctypes_module.FormatError(code)}")

    def _close(self) -> None:
        with self._lock:
            handle = self._handle
            if handle is None:
                return
            self._handle = None
        if not self._kernel32.CloseHandle(handle):
            import ctypes

            self._raise_last_error(ctypes, "close the sync Job Object")

    def terminate(
        self, process: subprocess.Popen[str], *, timeout: float
    ) -> int:
        stop_deadline = _deadline(timeout)
        code = process.poll()
        if code is not None:
            self._close()
            return int(code)
        try:
            self._close()
            return _confirmed_wait(process, _remaining(stop_deadline))
        except (OSError, ProcessTreeError) as job_error:
            try:
                return self._fallback.terminate(
                    process,
                    timeout=_remaining(stop_deadline),
                )
            except (OSError, ProcessTreeError) as fallback_error:
                raise ProcessTreeError(
                    f"Job Object stop failed ({job_error}); "
                    f"taskkill fallback failed ({fallback_error})"
                ) from fallback_error

    def close_after_exit(self) -> None:
        self._close()


def _resume_windows_process(pid: int) -> None:
    """Resume the initial thread of a process created with CREATE_SUSPENDED."""
    import ctypes
    from ctypes import wintypes

    class ThreadEntry32(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ThreadID", wintypes.DWORD),
            ("th32OwnerProcessID", wintypes.DWORD),
            ("tpBasePri", wintypes.LONG),
            ("tpDeltaPri", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
        ]

    snapshot_threads = 0x00000004
    thread_suspend_resume = 0x0002
    resume_failed = 0xFFFFFFFF
    invalid_handle = ctypes.c_void_p(-1).value
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Thread32First.argtypes = (wintypes.HANDLE, ctypes.c_void_p)
    kernel32.Thread32First.restype = wintypes.BOOL
    kernel32.Thread32Next.argtypes = (wintypes.HANDLE, ctypes.c_void_p)
    kernel32.Thread32Next.restype = wintypes.BOOL
    kernel32.OpenThread.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenThread.restype = wintypes.HANDLE
    kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
    kernel32.ResumeThread.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL

    snapshot = kernel32.CreateToolhelp32Snapshot(snapshot_threads, 0)
    if int(snapshot) == invalid_handle:
        code = ctypes.get_last_error()
        raise OSError(code, f"Could not enumerate sync threads: {ctypes.FormatError(code)}")
    try:
        entry = ThreadEntry32()
        entry.dwSize = ctypes.sizeof(entry)
        has_entry = bool(kernel32.Thread32First(snapshot, ctypes.byref(entry)))
        while has_entry:
            if entry.th32OwnerProcessID == pid:
                thread_handle = kernel32.OpenThread(
                    thread_suspend_resume,
                    False,
                    entry.th32ThreadID,
                )
                if not thread_handle:
                    code = ctypes.get_last_error()
                    raise OSError(
                        code,
                        f"Could not open suspended sync thread: {ctypes.FormatError(code)}",
                    )
                try:
                    if kernel32.ResumeThread(thread_handle) == resume_failed:
                        code = ctypes.get_last_error()
                        raise OSError(
                            code,
                            f"Could not resume the sync process: {ctypes.FormatError(code)}",
                        )
                finally:
                    kernel32.CloseHandle(thread_handle)
                return
            has_entry = bool(kernel32.Thread32Next(snapshot, ctypes.byref(entry)))
    finally:
        kernel32.CloseHandle(snapshot)
    raise ProcessLookupError(f"No suspended thread was found for sync process {pid}")


def attach_process_tree(process: subprocess.Popen[str]) -> ProcessTree:
    """Attach OS lifetime control immediately after spawning ``process``."""
    if os.name == "nt":
        process_tree: _WindowsJobTree | None = None
        try:
            process_tree = _WindowsJobTree(process)
            _resume_windows_process(process.pid)
            return process_tree
        except Exception as exc:
            if process_tree is not None:
                try:
                    process_tree.terminate(process, timeout=2.0)
                except Exception:
                    pass
            raise ProcessTreeError(
                f"Windows Job Object attachment failed: {exc}"
            ) from exc
    if os.name == "posix":
        return _PosixProcessTree(process)
    return _SingleProcessTree()


def fallback_process_tree(process: subprocess.Popen[str]) -> ProcessTree:
    """Return the best available controller after primary attachment fails."""
    if os.name == "nt":
        return _WindowsTaskkillTree()
    if os.name == "posix":
        return _PosixProcessTree(process)
    return _SingleProcessTree()
