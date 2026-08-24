"""Detect and provision the optional local Codex CLI prerequisites.

The Garmin application itself does not require Node.js or Codex.  These tools
are used only by the Codex Coach workflow. Detection invokes only health/status
commands; every installation requires an explicit user confirmation.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


NODE_WINGET_PACKAGE_ID = "OpenJS.NodeJS.LTS"
CODEX_NPM_PACKAGE = "@openai/codex"
OPENAI_CODEX_INSTALL_URL = "https://learn.chatgpt.com/docs/codex/cli"
OPENAI_CODEX_AUTH_URL = "https://learn.chatgpt.com/docs/auth"
NODE_DOWNLOAD_URL = "https://nodejs.org/en/download"

_API_KEY_ENVIRONMENT_VARIABLES = ("OPENAI_API_KEY", "CODEX_API_KEY")
_VERSION_TIMEOUT_SECONDS = 15
_INSTALL_TIMEOUT_SECONDS = 20 * 60
_MAX_DETAIL_LENGTH = 1200
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_SIGNED_OUT_DETAIL_MARKERS = (
    "not logged in",
    "not signed in",
    "no credentials",
    "login required",
    "sign-in required",
    "run codex login",
)
_install_lock = threading.Lock()


class CodexPrerequisiteError(RuntimeError):
    """Base error for optional Codex prerequisite setup."""


class CodexPrerequisiteConsentRequired(CodexPrerequisiteError):
    """A mutating setup action was requested without explicit consent."""


@dataclass(frozen=True)
class ToolStatus:
    """Health of one external command."""

    name: str
    path: str | None
    version: str | None = None
    error: str | None = None

    @property
    def available(self) -> bool:
        return bool(self.path and self.version and not self.error)


@dataclass(frozen=True)
class CodexPrerequisiteStatus:
    """Current Node.js, npm, Codex, and saved-login state."""

    node: ToolStatus
    npm: ToolStatus
    codex: ToolStatus
    auth_state: str
    auth_detail: str | None = None

    @property
    def commands_ready(self) -> bool:
        return self.node.available and self.npm.available and self.codex.available

    @property
    def ready_for_generation(self) -> bool:
        return self.codex.available and self.auth_state == "signed_in"

    @property
    def missing_tools(self) -> tuple[str, ...]:
        return tuple(
            tool.name for tool in (self.node, self.npm, self.codex) if not tool.available
        )


@dataclass(frozen=True)
class PrerequisiteInstallStep:
    name: str
    attempted: bool
    success: bool
    detail: str


@dataclass(frozen=True)
class CodexPrerequisiteInstallResult:
    before: CodexPrerequisiteStatus
    after: CodexPrerequisiteStatus
    steps: tuple[PrerequisiteInstallStep, ...]

    @property
    def success(self) -> bool:
        return self.after.commands_ready

    @property
    def detail(self) -> str:
        if not self.steps:
            return "All prerequisite commands were already available."
        return "\n".join(step.detail for step in self.steps)


@dataclass(frozen=True)
class CodexLoginLaunch:
    executable: str
    process_id: int


def _is_windows() -> bool:
    return os.name == "nt"


def _clean_detail(*values: str | None) -> str:
    combined = "\n".join(str(value or "").strip() for value in values if value)
    combined = _ANSI_ESCAPE.sub("", combined).strip()
    if not combined:
        return "The command did not return any details."
    return combined[-_MAX_DETAIL_LENGTH:]


def _version_text(stdout: str | None, stderr: str | None) -> str:
    detail = "\n".join(
        str(value or "").strip() for value in (stdout, stderr) if value
    ).strip()
    if not detail:
        return ""
    detail = _ANSI_ESCAPE.sub("", detail)
    lines = [line.strip() for line in detail.splitlines() if line.strip()]
    for line in lines:
        if any(character.isdigit() for character in line):
            return line[:240]
    return ""


def _sanitized_environment(
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    environment = dict(os.environ if environ is None else environ)
    for variable in _API_KEY_ENVIRONMENT_VARIABLES:
        environment.pop(variable, None)
    return environment


def _run_command(
    command: Sequence[str],
    *,
    timeout: int,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        shell=False,
        env=None if env is None else dict(env),
    )


def _append_candidate(
    candidates: list[str], seen: set[str], candidate: str | Path | None
) -> None:
    if not candidate:
        return
    path = os.path.abspath(os.fspath(candidate))
    key = os.path.normcase(path)
    if key in seen or not Path(path).is_file():
        return
    seen.add(key)
    candidates.append(path)


def _path_candidates(name: str, environment: Mapping[str, str]) -> list[str]:
    """Return every plausible executable, including stale-PATH fallbacks."""

    candidates: list[str] = []
    seen: set[str] = set()
    search_path = environment.get("PATH")
    _append_candidate(candidates, seen, shutil.which(name, path=search_path))

    if search_path:
        extensions = [""]
        if _is_windows():
            extensions.extend(
                extension.lower()
                for extension in environment.get(
                    "PATHEXT", ".COM;.EXE;.BAT;.CMD"
                ).split(os.pathsep)
                if extension
            )
        for directory in search_path.split(os.pathsep):
            if not directory:
                continue
            for extension in extensions:
                suffix = "" if Path(name).suffix else extension
                _append_candidate(candidates, seen, Path(directory) / f"{name}{suffix}")

    if not _is_windows():
        return candidates

    program_files = environment.get("ProgramFiles")
    program_files_x86 = environment.get("ProgramFiles(x86)")
    local_app_data = environment.get("LOCALAPPDATA")
    roaming_app_data = environment.get("APPDATA")
    user_profile = environment.get("USERPROFILE")

    if name == "node":
        for root in (program_files, program_files_x86):
            if root:
                _append_candidate(candidates, seen, Path(root) / "nodejs" / "node.exe")
        if local_app_data:
            _append_candidate(
                candidates,
                seen,
                Path(local_app_data) / "Programs" / "nodejs" / "node.exe",
            )
    elif name == "npm":
        for root in (program_files, program_files_x86):
            if root:
                _append_candidate(candidates, seen, Path(root) / "nodejs" / "npm.cmd")
        if local_app_data:
            _append_candidate(
                candidates,
                seen,
                Path(local_app_data) / "Programs" / "nodejs" / "npm.cmd",
            )
        if roaming_app_data:
            _append_candidate(candidates, seen, Path(roaming_app_data) / "npm" / "npm.cmd")
    elif name == "codex":
        if roaming_app_data:
            _append_candidate(
                candidates, seen, Path(roaming_app_data) / "npm" / "codex.cmd"
            )
        if local_app_data:
            standalone_root = (
                Path(local_app_data) / "Programs" / "OpenAI" / "Codex" / "bin"
            )
            _append_candidate(candidates, seen, standalone_root / "codex.exe")
            _append_candidate(candidates, seen, standalone_root / "codex.cmd")
        if user_profile:
            _append_candidate(
                candidates,
                seen,
                Path(user_profile) / ".local" / "bin" / "codex.exe",
            )
    elif name == "winget" and local_app_data:
        _append_candidate(
            candidates,
            seen,
            Path(local_app_data) / "Microsoft" / "WindowsApps" / "winget.exe",
        )
    return candidates


def _probe_candidates(
    name: str,
    candidates: Sequence[str],
    arguments: Sequence[str],
    environment: Mapping[str, str],
) -> ToolStatus:
    first_failure: ToolStatus | None = None
    for candidate in candidates:
        try:
            completed = _run_command(
                [candidate, *arguments],
                timeout=_VERSION_TIMEOUT_SECONDS,
                env=environment,
            )
        except subprocess.TimeoutExpired:
            failure = ToolStatus(
                name,
                candidate,
                error=f"Timed out while checking {name}.",
            )
        except OSError as exc:
            failure = ToolStatus(
                name,
                candidate,
                error=f"Could not start {name}: {exc}",
            )
        else:
            if completed.returncode == 0:
                version = _version_text(completed.stdout, completed.stderr)
                if version:
                    return ToolStatus(name, candidate, version=version)
                failure = ToolStatus(
                    name,
                    candidate,
                    error=f"{name} did not report a version.",
                )
            else:
                failure = ToolStatus(
                    name,
                    candidate,
                    error=(
                        f"{name} exited with status {completed.returncode}: "
                        f"{_clean_detail(completed.stderr, completed.stdout)}"
                    ),
                )
        if first_failure is None:
            first_failure = failure
    return first_failure or ToolStatus(name, None, error=f"{name} was not found.")


def _probe_tool(
    name: str,
    *,
    environment: Mapping[str, str],
    extra_candidates: Sequence[str] = (),
) -> ToolStatus:
    candidates = _path_candidates(name, environment)
    seen = {os.path.normcase(path) for path in candidates}
    for candidate in extra_candidates:
        _append_candidate(candidates, seen, candidate)
    return _probe_candidates(name, candidates, ("--version",), environment)


def _npm_codex_candidates(
    npm: ToolStatus, environment: Mapping[str, str]
) -> tuple[str, ...]:
    if not npm.available or not npm.path:
        return ()
    try:
        completed = _run_command(
            [npm.path, "config", "get", "prefix"],
            timeout=_VERSION_TIMEOUT_SECONDS,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ()
    if completed.returncode != 0:
        return ()
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        return ()
    prefix = Path(lines[-1])
    if _is_windows():
        return (str(prefix / "codex.cmd"), str(prefix / "codex.exe"))
    return (str(prefix / "bin" / "codex"),)


def external_tool_environment(
    codex_executable: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return a saved-login environment with stale PATH fallbacks prepended."""

    environment = _sanitized_environment(environ)
    directories: list[str] = []
    if codex_executable:
        directories.append(str(Path(codex_executable).resolve().parent))
    for name in ("node", "npm"):
        candidates = _path_candidates(name, environment)
        if candidates:
            directories.append(str(Path(candidates[0]).resolve().parent))
    existing = environment.get("PATH", "")
    existing_keys = {
        os.path.normcase(os.path.abspath(item))
        for item in existing.split(os.pathsep)
        if item
    }
    prefixes: list[str] = []
    for directory in directories:
        key = os.path.normcase(os.path.abspath(directory))
        if key not in existing_keys:
            existing_keys.add(key)
            prefixes.append(directory)
    environment["PATH"] = (
        os.pathsep.join([*prefixes, existing])
        if existing
        else os.pathsep.join(prefixes)
    )
    return environment


def detect_codex_prerequisites(
    *, environ: Mapping[str, str] | None = None
) -> CodexPrerequisiteStatus:
    """Inspect optional commands and saved Codex authentication without installing."""

    environment = _sanitized_environment(environ)
    node = _probe_tool("node", environment=environment)
    npm = _probe_tool("npm", environment=environment)
    codex_environment = external_tool_environment(environ=environment)
    codex = _probe_tool(
        "codex",
        environment=codex_environment,
        extra_candidates=_npm_codex_candidates(npm, codex_environment),
    )
    if not codex.available or not codex.path:
        return CodexPrerequisiteStatus(node, npm, codex, "not_installed")

    login_environment = external_tool_environment(codex.path, environ=environment)
    try:
        completed = _run_command(
            [codex.path, "login", "status"],
            timeout=_VERSION_TIMEOUT_SECONDS,
            env=login_environment,
        )
    except subprocess.TimeoutExpired:
        auth_state = "unknown"
        auth_detail = "Timed out while checking the saved Codex login."
    except OSError as exc:
        auth_state = "unknown"
        auth_detail = f"Could not check the saved Codex login: {exc}"
    else:
        auth_detail = _clean_detail(completed.stdout, completed.stderr)
        if completed.returncode == 0:
            auth_state = "signed_in"
        elif any(
            marker in auth_detail.casefold()
            for marker in _SIGNED_OUT_DETAIL_MARKERS
        ):
            auth_state = "signed_out"
        else:
            auth_state = "unknown"
    return CodexPrerequisiteStatus(node, npm, codex, auth_state, auth_detail)


def find_codex_cli() -> str | None:
    """Return a working Codex launcher from PATH or a known per-user install."""

    codex = detect_codex_prerequisites().codex
    return codex.path if codex.available else None


def _completed_install_step(
    name: str, completed: subprocess.CompletedProcess[str]
) -> PrerequisiteInstallStep:
    success = completed.returncode == 0
    detail = _clean_detail(completed.stdout, completed.stderr)
    if success:
        message = f"{name} completed successfully."
    else:
        message = f"{name} failed with status {completed.returncode}: {detail}"
    return PrerequisiteInstallStep(name, True, success, message)


def _failed_install_step(name: str, detail: str) -> PrerequisiteInstallStep:
    return PrerequisiteInstallStep(name, True, False, detail)


def install_missing_codex_prerequisites(
    *,
    consent: bool = False,
    environ: Mapping[str, str] | None = None,
) -> CodexPrerequisiteInstallResult:
    """Install only missing tools after an explicit user confirmation."""

    if not consent:
        raise CodexPrerequisiteConsentRequired(
            "Explicit consent is required before downloading prerequisites."
        )
    if not _install_lock.acquire(blocking=False):
        raise CodexPrerequisiteError("Prerequisite installation is already running.")

    try:
        environment = _sanitized_environment(environ)
        before = detect_codex_prerequisites(environ=environment)
        if before.commands_ready:
            return CodexPrerequisiteInstallResult(before, before, ())

        steps: list[PrerequisiteInstallStep] = []
        current = before
        if not current.node.available or not current.npm.available:
            if not _is_windows():
                steps.append(
                    _failed_install_step(
                        "Node.js LTS and npm installation",
                        "Automatic Node.js installation is available only on Windows. "
                        f"Install an LTS release from {NODE_DOWNLOAD_URL}.",
                    )
                )
                return CodexPrerequisiteInstallResult(
                    before,
                    detect_codex_prerequisites(environ=environment),
                    tuple(steps),
                )
            winget = _probe_tool("winget", environment=environment)
            if not winget.available or not winget.path:
                steps.append(
                    _failed_install_step(
                        "Node.js LTS and npm installation",
                        "Windows Package Manager (winget) is unavailable or broken. "
                        f"Install Node.js LTS manually from {NODE_DOWNLOAD_URL}, then retry.",
                    )
                )
                return CodexPrerequisiteInstallResult(
                    before,
                    detect_codex_prerequisites(environ=environment),
                    tuple(steps),
                )
            try:
                completed = _run_command(
                    [
                        winget.path,
                        "install",
                        "--id",
                        NODE_WINGET_PACKAGE_ID,
                        "--exact",
                        "--source",
                        "winget",
                        "--accept-package-agreements",
                        "--accept-source-agreements",
                        "--silent",
                        "--disable-interactivity",
                    ],
                    timeout=_INSTALL_TIMEOUT_SECONDS,
                    env=environment,
                )
            except subprocess.TimeoutExpired:
                steps.append(
                    _failed_install_step(
                        "Node.js LTS and npm installation",
                        "Node.js installation timed out. Check Windows Package Manager "
                        "and retry, or install Node.js LTS manually.",
                    )
                )
            except OSError as exc:
                steps.append(
                    _failed_install_step(
                        "Node.js LTS and npm installation",
                        f"Could not start Windows Package Manager: {exc}",
                    )
                )
            else:
                steps.append(
                    _completed_install_step(
                        "Node.js LTS and npm installation", completed
                    )
                )

            current = detect_codex_prerequisites(environ=environment)
            if not current.node.available or not current.npm.available:
                return CodexPrerequisiteInstallResult(before, current, tuple(steps))

        if not current.codex.available:
            npm_path = current.npm.path
            if not npm_path:
                steps.append(
                    _failed_install_step(
                        "Codex CLI installation",
                        "npm is still unavailable. Install Node.js LTS and restart the app.",
                    )
                )
                return CodexPrerequisiteInstallResult(before, current, tuple(steps))
            install_environment = external_tool_environment(environ=environment)
            try:
                completed = _run_command(
                    [
                        npm_path,
                        "install",
                        "--global",
                        CODEX_NPM_PACKAGE,
                        "--no-audit",
                        "--no-fund",
                    ],
                    timeout=_INSTALL_TIMEOUT_SECONDS,
                    env=install_environment,
                )
            except subprocess.TimeoutExpired:
                steps.append(
                    _failed_install_step(
                        "Codex CLI installation",
                        "Codex installation timed out. Check the network or proxy, "
                        f"then follow {OPENAI_CODEX_INSTALL_URL}.",
                    )
                )
            except OSError as exc:
                steps.append(
                    _failed_install_step(
                        "Codex CLI installation",
                        f"Could not start npm: {exc}",
                    )
                )
            else:
                steps.append(_completed_install_step("Codex CLI installation", completed))

        after = detect_codex_prerequisites(environ=environment)
        if not after.commands_ready and all(step.success for step in steps):
            steps.append(
                _failed_install_step(
                    "Post-install verification",
                    "Installation finished, but one or more commands could not be "
                    "verified. Restart Garmin Data Hub and use Check again.",
                )
            )
        return CodexPrerequisiteInstallResult(before, after, tuple(steps))
    finally:
        _install_lock.release()


def launch_codex_login(
    *, environ: Mapping[str, str] | None = None
) -> CodexLoginLaunch:
    """Open the official interactive Codex browser-login flow in a terminal."""

    status = detect_codex_prerequisites(environ=environ)
    if not status.codex.available or not status.codex.path:
        raise CodexPrerequisiteError("Install Codex CLI before starting sign-in.")
    environment = external_tool_environment(status.codex.path, environ=environ)

    if _is_windows():
        powershell = shutil.which("powershell.exe", path=environment.get("PATH"))
        if not powershell:
            system_root = environment.get("SystemRoot", r"C:\Windows")
            powershell_candidate = (
                Path(system_root)
                / "System32"
                / "WindowsPowerShell"
                / "v1.0"
                / "powershell.exe"
            )
            if powershell_candidate.is_file():
                powershell = str(powershell_candidate)
        if not powershell:
            raise CodexPrerequisiteError(
                "Windows PowerShell was not found; run 'codex login' manually."
            )
        environment["GARMIN_DATA_HUB_CODEX_EXE"] = status.codex.path
        command = [
            powershell,
            "-NoLogo",
            "-NoProfile",
            "-NoExit",
            "-Command",
            "& $env:GARMIN_DATA_HUB_CODEX_EXE login",
        ]
        process = subprocess.Popen(
            command,
            shell=False,
            env=environment,
            creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
        )
    else:
        process = subprocess.Popen(
            [status.codex.path, "login"],
            shell=False,
            env=environment,
        )
    return CodexLoginLaunch(status.codex.path, process.pid)
