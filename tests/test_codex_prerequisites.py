"""Unit tests for the optional Node.js/npm/Codex prerequisite bootstrap."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from garmin_data_hub.services import codex_prerequisites as prerequisites


def _tool(name: str, *, available: bool = True) -> prerequisites.ToolStatus:
    if available:
        suffix = ".cmd" if name in {"npm", "codex"} else ".exe"
        return prerequisites.ToolStatus(
            name,
            str(Path("C:/tools") / f"{name}{suffix}"),
            version=f"{name} 1.2.3",
        )
    return prerequisites.ToolStatus(
        name,
        None,
        error=f"{name} was not found.",
    )


def _status(
    *,
    node: bool = True,
    npm: bool = True,
    codex: bool = True,
    auth_state: str = "signed_in",
) -> prerequisites.CodexPrerequisiteStatus:
    return prerequisites.CodexPrerequisiteStatus(
        node=_tool("node", available=node),
        npm=_tool("npm", available=npm),
        codex=_tool("codex", available=codex),
        auth_state=auth_state if codex else "not_installed",
    )


def _completed(
    command: list[str],
    *,
    returncode: int = 0,
    stdout: str = "completed",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        command,
        returncode,
        stdout=stdout,
        stderr=stderr,
    )


def test_install_requires_explicit_consent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: pytest.fail("detection must not run before consent"),
    )

    with pytest.raises(
        prerequisites.CodexPrerequisiteConsentRequired,
        match="Explicit consent",
    ):
        prerequisites.install_missing_codex_prerequisites(consent=False)


def test_concurrent_install_is_rejected_before_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: pytest.fail("a second install must not start detection"),
    )
    assert prerequisites._install_lock.acquire(blocking=False)
    try:
        with pytest.raises(
            prerequisites.CodexPrerequisiteError,
            match="already running",
        ):
            prerequisites.install_missing_codex_prerequisites(consent=True)
    finally:
        prerequisites._install_lock.release()


def test_healthy_prerequisites_are_a_noop_and_are_not_upgraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = _status()
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: ready,
    )
    monkeypatch.setattr(
        prerequisites,
        "_run_command",
        lambda *_args, **_kwargs: pytest.fail(
            "healthy prerequisite commands must not be installed or upgraded"
        ),
    )

    result = prerequisites.install_missing_codex_prerequisites(consent=True)

    assert result.before is ready
    assert result.after is ready
    assert result.steps == ()
    assert result.success is True
    assert result.detail == "All prerequisite commands were already available."


def test_probe_skips_a_broken_candidate_and_uses_the_next_working_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(command, **_kwargs):
        command = list(command)
        commands.append(command)
        if command[0] == "broken-codex.cmd":
            return _completed(
                command,
                returncode=1,
                stderr="launcher is broken",
            )
        return _completed(command, stdout="codex-cli 0.149.0")

    monkeypatch.setattr(prerequisites, "_run_command", fake_run)

    status = prerequisites._probe_candidates(
        "codex",
        ["broken-codex.cmd", "working-codex.exe"],
        ("--version",),
        {"PATH": ""},
    )

    assert commands == [
        ["broken-codex.cmd", "--version"],
        ["working-codex.exe", "--version"],
    ]
    assert status.available is True
    assert status.path == "working-codex.exe"
    assert status.version == "codex-cli 0.149.0"


def test_probe_rejects_success_without_a_version_and_uses_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command, **_kwargs):
        if command[0] == "malformed-codex.cmd":
            return _completed(list(command), stdout="command completed")
        return _completed(list(command), stdout="codex-cli 0.149.0")

    monkeypatch.setattr(prerequisites, "_run_command", fake_run)

    status = prerequisites._probe_candidates(
        "codex",
        ["malformed-codex.cmd", "working-codex.exe"],
        ("--version",),
        {"PATH": ""},
    )

    assert status.available is True
    assert status.path == "working-codex.exe"


def test_missing_codex_runs_only_the_exact_npm_install_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _status(codex=False)
    after = _status()
    detections = iter((before, after))
    commands: list[tuple[list[str], dict]] = []

    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: next(detections),
    )
    monkeypatch.setattr(
        prerequisites,
        "external_tool_environment",
        lambda **_kwargs: {"PATH": "test-path"},
    )
    monkeypatch.setattr(
        prerequisites,
        "_probe_tool",
        lambda *_args, **_kwargs: pytest.fail(
            "WinGet must not be checked when Node.js and npm are healthy"
        ),
    )

    def fake_run(command, **kwargs):
        commands.append((list(command), kwargs))
        return _completed(list(command), stdout="added Codex")

    monkeypatch.setattr(prerequisites, "_run_command", fake_run)

    result = prerequisites.install_missing_codex_prerequisites(
        consent=True,
        environ={
            "PATH": "test-path",
            "OPENAI_API_KEY": "must-not-be-forwarded",
            "CODEX_API_KEY": "must-not-be-forwarded",
        },
    )

    assert [command for command, _kwargs in commands] == [
        [
            before.npm.path,
            "install",
            "--global",
            prerequisites.CODEX_NPM_PACKAGE,
            "--no-audit",
            "--no-fund",
        ]
    ]
    assert commands[0][1]["timeout"] == prerequisites._INSTALL_TIMEOUT_SECONDS
    assert commands[0][1]["env"] == {"PATH": "test-path"}
    assert result.after is after
    assert result.success is True
    assert [step.name for step in result.steps] == ["Codex CLI installation"]


def test_missing_node_and_npm_run_exact_winget_then_npm_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _status(node=False, npm=False, codex=False)
    after_node = _status(codex=False)
    after_codex = _status()
    detections = iter((before, after_node, after_codex))
    winget = prerequisites.ToolStatus(
        "winget",
        "C:/Windows/winget.exe",
        version="v1.12.0",
    )
    commands: list[list[str]] = []

    monkeypatch.setattr(prerequisites, "_is_windows", lambda: True)
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: next(detections),
    )
    monkeypatch.setattr(
        prerequisites,
        "_probe_tool",
        lambda name, **_kwargs: winget
        if name == "winget"
        else pytest.fail(f"unexpected probe for {name}"),
    )
    monkeypatch.setattr(
        prerequisites,
        "external_tool_environment",
        lambda **_kwargs: {"PATH": "augmented-path"},
    )

    def fake_run(command, **_kwargs):
        command = list(command)
        commands.append(command)
        return _completed(command)

    monkeypatch.setattr(prerequisites, "_run_command", fake_run)

    result = prerequisites.install_missing_codex_prerequisites(
        consent=True,
        environ={"PATH": "original-path"},
    )

    assert commands == [
        [
            winget.path,
            "install",
            "--id",
            prerequisites.NODE_WINGET_PACKAGE_ID,
            "--exact",
            "--source",
            "winget",
            "--accept-package-agreements",
            "--accept-source-agreements",
            "--silent",
            "--disable-interactivity",
        ],
        [
            after_node.npm.path,
            "install",
            "--global",
            prerequisites.CODEX_NPM_PACKAGE,
            "--no-audit",
            "--no-fund",
        ],
    ]
    assert result.after is after_codex
    assert result.success is True
    assert [step.success for step in result.steps] == [True, True]


def test_failed_npm_install_is_reported_without_claiming_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _status(codex=False)
    detections = iter((before, before))
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: next(detections),
    )
    monkeypatch.setattr(
        prerequisites,
        "external_tool_environment",
        lambda **_kwargs: {"PATH": "test-path"},
    )
    monkeypatch.setattr(
        prerequisites,
        "_run_command",
        lambda command, **_kwargs: _completed(
            list(command),
            returncode=7,
            stderr="registry unavailable",
        ),
    )

    result = prerequisites.install_missing_codex_prerequisites(consent=True)

    assert result.success is False
    assert len(result.steps) == 1
    assert result.steps[0].success is False
    assert "status 7" in result.steps[0].detail
    assert "registry unavailable" in result.steps[0].detail


def test_timed_out_winget_install_stops_before_npm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _status(node=False, npm=False, codex=False)
    detections = iter((before, before))
    winget = prerequisites.ToolStatus(
        "winget",
        "C:/Windows/winget.exe",
        version="v1.12.0",
    )
    commands: list[list[str]] = []

    monkeypatch.setattr(prerequisites, "_is_windows", lambda: True)
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: next(detections),
    )
    monkeypatch.setattr(prerequisites, "_probe_tool", lambda *_args, **_kwargs: winget)

    def time_out(command, **_kwargs):
        command = list(command)
        commands.append(command)
        raise subprocess.TimeoutExpired(command, prerequisites._INSTALL_TIMEOUT_SECONDS)

    monkeypatch.setattr(prerequisites, "_run_command", time_out)

    result = prerequisites.install_missing_codex_prerequisites(consent=True)

    assert len(commands) == 1
    assert commands[0][0] == winget.path
    assert result.success is False
    assert len(result.steps) == 1
    assert "timed out" in result.steps[0].detail.lower()


@pytest.mark.parametrize(
    ("returncode", "expected_state"),
    [(0, "signed_in"), (1, "signed_out")],
)
def test_auth_check_strips_api_keys_and_uses_exit_status(
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    expected_state: str,
) -> None:
    tools = {
        "node": _tool("node"),
        "npm": _tool("npm"),
        "codex": _tool("codex"),
    }
    calls: list[tuple[list[str], dict]] = []

    def fake_probe(name, *, environment, **_kwargs):
        assert "OPENAI_API_KEY" not in environment
        assert "CODEX_API_KEY" not in environment
        return tools[name]

    def fake_run(command, **kwargs):
        calls.append((list(command), kwargs))
        return _completed(
            list(command),
            returncode=returncode,
            stdout="Logged in with ChatGPT" if returncode == 0 else "",
            stderr="Not logged in" if returncode else "",
        )

    monkeypatch.setattr(prerequisites, "_probe_tool", fake_probe)
    monkeypatch.setattr(prerequisites, "_npm_codex_candidates", lambda *_args: ())
    monkeypatch.setattr(prerequisites, "_path_candidates", lambda *_args: [])
    monkeypatch.setattr(prerequisites, "_run_command", fake_run)

    result = prerequisites.detect_codex_prerequisites(
        environ={
            "PATH": "",
            "PRESERVED": "yes",
            "OPENAI_API_KEY": "must-not-be-forwarded",
            "CODEX_API_KEY": "must-not-be-forwarded",
        }
    )

    assert result.auth_state == expected_state
    assert [command for command, _kwargs in calls] == [
        [tools["codex"].path, "login", "status"]
    ]
    auth_environment = calls[0][1]["env"]
    assert auth_environment["PRESERVED"] == "yes"
    assert "OPENAI_API_KEY" not in auth_environment
    assert "CODEX_API_KEY" not in auth_environment


def test_auth_check_timeout_reports_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    tools = {name: _tool(name) for name in ("node", "npm", "codex")}
    monkeypatch.setattr(
        prerequisites,
        "_probe_tool",
        lambda name, **_kwargs: tools[name],
    )
    monkeypatch.setattr(prerequisites, "_npm_codex_candidates", lambda *_args: ())
    monkeypatch.setattr(prerequisites, "_path_candidates", lambda *_args: [])
    monkeypatch.setattr(
        prerequisites,
        "_run_command",
        lambda command, **_kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(command, prerequisites._VERSION_TIMEOUT_SECONDS)
        ),
    )

    result = prerequisites.detect_codex_prerequisites(environ={"PATH": ""})

    assert result.auth_state == "unknown"
    assert result.auth_detail == "Timed out while checking the saved Codex login."


def test_auth_check_does_not_mislabel_configuration_failure_as_signed_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools = {name: _tool(name) for name in ("node", "npm", "codex")}
    monkeypatch.setattr(
        prerequisites,
        "_probe_tool",
        lambda name, **_kwargs: tools[name],
    )
    monkeypatch.setattr(prerequisites, "_npm_codex_candidates", lambda *_args: ())
    monkeypatch.setattr(prerequisites, "_path_candidates", lambda *_args: [])
    monkeypatch.setattr(
        prerequisites,
        "_run_command",
        lambda command, **_kwargs: _completed(
            list(command),
            returncode=2,
            stderr="Error loading configuration: access denied",
        ),
    )

    result = prerequisites.detect_codex_prerequisites(environ={"PATH": ""})

    assert result.auth_state == "unknown"
    assert "access denied" in (result.auth_detail or "")


def test_find_codex_cli_returns_only_an_available_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken = prerequisites.ToolStatus(
        "codex",
        "C:/broken/codex.cmd",
        error="launcher failed",
    )
    broken_status = prerequisites.CodexPrerequisiteStatus(
        _tool("node"),
        _tool("npm"),
        broken,
        "not_installed",
    )
    working_status = _status()
    detections = iter((broken_status, working_status))
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda: next(detections),
    )

    assert prerequisites.find_codex_cli() is None
    assert prerequisites.find_codex_cli() == working_status.codex.path


def test_external_tool_environment_augments_path_and_removes_api_keys(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    codex_dir = tmp_path / "npm-global"
    node_dir = tmp_path / "nodejs"
    existing_dir = tmp_path / "existing"
    codex_path = codex_dir / "codex.cmd"
    candidates = {
        "node": [str(node_dir / "node.exe")],
        "npm": [str(node_dir / "npm.cmd")],
    }
    monkeypatch.setattr(
        prerequisites,
        "_path_candidates",
        lambda name, _environment: candidates[name],
    )

    environment = prerequisites.external_tool_environment(
        str(codex_path),
        environ={
            "PATH": str(existing_dir),
            "PRESERVED": "yes",
            "OPENAI_API_KEY": "must-not-be-forwarded",
            "CODEX_API_KEY": "must-not-be-forwarded",
        },
    )

    assert environment["PATH"].split(os.pathsep) == [
        str(codex_dir.resolve()),
        str(node_dir.resolve()),
        str(existing_dir),
    ]
    assert environment["PRESERVED"] == "yes"
    assert "OPENAI_API_KEY" not in environment
    assert "CODEX_API_KEY" not in environment


def test_windows_login_launch_uses_a_fixed_environment_variable_not_interpolation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = _status(auth_state="signed_out")
    captured: dict[str, object] = {}

    monkeypatch.setattr(prerequisites, "_is_windows", lambda: True)
    monkeypatch.setattr(
        prerequisites,
        "detect_codex_prerequisites",
        lambda **_kwargs: ready,
    )
    monkeypatch.setattr(
        prerequisites,
        "external_tool_environment",
        lambda *_args, **_kwargs: {"PATH": "safe-path", "PRESERVED": "yes"},
    )
    monkeypatch.setattr(
        prerequisites.shutil,
        "which",
        lambda name, **_kwargs: "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
        if name == "powershell.exe"
        else None,
    )

    def fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured.update(kwargs)
        return SimpleNamespace(pid=4321)

    monkeypatch.setattr(prerequisites.subprocess, "Popen", fake_popen)

    result = prerequisites.launch_codex_login(
        environ={
            "OPENAI_API_KEY": "must-not-be-forwarded",
            "CODEX_API_KEY": "must-not-be-forwarded",
        }
    )

    assert result == prerequisites.CodexLoginLaunch(ready.codex.path, 4321)
    assert captured["command"] == [
        "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
        "-NoLogo",
        "-NoProfile",
        "-NoExit",
        "-Command",
        "& $env:GARMIN_DATA_HUB_CODEX_EXE login",
    ]
    assert captured["shell"] is False
    assert captured["creationflags"] == getattr(
        subprocess,
        "CREATE_NEW_CONSOLE",
        0,
    )
    environment = captured["env"]
    assert environment["GARMIN_DATA_HUB_CODEX_EXE"] == ready.codex.path
    assert environment["PRESERVED"] == "yes"
    assert "OPENAI_API_KEY" not in environment
    assert "CODEX_API_KEY" not in environment
