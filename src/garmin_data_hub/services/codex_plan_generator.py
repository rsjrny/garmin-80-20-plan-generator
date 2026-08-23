"""Generate a training-plan proposal through an authenticated Codex CLI.

This module deliberately does not use the OpenAI SDK or an API key.  It starts
the locally installed ``codex exec`` command, which reuses the user's saved CLI
login, and returns a schema-constrained JSON proposal for the existing strict
import/approval workflow.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from garmin_data_hub.services.coaching_packet import TRAINING_PLAN_UPDATE_SCHEMA


DEFAULT_CODEX_TIMEOUT_SECONDS = 360
DEFAULT_CODEX_REASONING_EFFORT = "low"
_API_KEY_ENVIRONMENT_VARIABLES = ("OPENAI_API_KEY", "CODEX_API_KEY")
_UNSUPPORTED_STRUCTURED_OUTPUT_KEYS = {
    "$schema",
    "$id",
    "title",
    "minLength",
    "maxLength",
    "uniqueItems",
}


class CodexPlanGenerationError(RuntimeError):
    """Base error for a failed local Codex generation run."""


class CodexCliNotFoundError(CodexPlanGenerationError):
    """The Codex executable could not be resolved from PATH."""


class CodexCliTimeoutError(CodexPlanGenerationError):
    """The Codex generation exceeded its local timeout."""


class CodexCliCancelledError(CodexPlanGenerationError):
    """The user cancelled the local Codex generation."""


@dataclass(frozen=True)
class CodexPlanGenerationResult:
    response_json: str
    executable: str


def find_codex_cli() -> str | None:
    """Return the installed Codex launcher path, if one is available."""
    return shutil.which("codex")


def _json_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if value is None:
        return "null"
    raise TypeError(f"Unsupported JSON constant type: {type(value).__name__}")


def _structured_output_node(value: Any) -> Any:
    """Convert full JSON Schema into the Structured Outputs subset."""
    if isinstance(value, list):
        return [_structured_output_node(item) for item in value]
    if not isinstance(value, dict):
        return value

    normalized = {
        key: _structured_output_node(item)
        for key, item in value.items()
        if key not in _UNSUPPORTED_STRUCTURED_OUTPUT_KEYS and key != "const"
    }
    if "const" in value:
        constant = value["const"]
        normalized.setdefault("type", _json_type(constant))
        normalized["enum"] = [constant]

    if normalized.get("type") == "object":
        properties = normalized.get("properties", {})
        if not isinstance(properties, dict):
            raise TypeError("Object schema properties must be a mapping")
        normalized["additionalProperties"] = False
        normalized["required"] = list(properties)
    return normalized


def _strict_output_schema() -> dict[str, Any]:
    """Return a Codex-compatible strict version of the importer schema.

    The manual importer permits older responses that omit the three guidance
    arrays.  A newly generated response can and should return every field, which
    also keeps the schema compatible with strict structured-output engines.
    Unsupported generation-only keywords are removed here; the unchanged local
    importer remains authoritative for length, uniqueness, and semantic checks.
    """
    return _structured_output_node(copy.deepcopy(TRAINING_PLAN_UPDATE_SCHEMA))


def _generation_input(packet: Mapping[str, Any], prompt: str) -> str:
    # The same (large) schema is supplied to Codex through --output-schema.
    # Removing its duplicate from the packet reduces input tokens without
    # changing the packet used by Streamlit or the strict local validation.
    cli_packet = copy.deepcopy(dict(packet))
    chatgpt = cli_packet.get("chatgpt")
    if isinstance(chatgpt, dict):
        chatgpt.pop("requested_output_schema", None)
        chatgpt["output_schema_note"] = (
            "The required response schema is enforced by the Codex CLI."
        )
    packet_json = json.dumps(cli_packet, ensure_ascii=False, sort_keys=True)
    return (
        f"{prompt.strip()}\n\n"
        "Use only the coaching packet below as athlete and plan context. "
        "Do not inspect files, run commands, browse the web, or modify anything. "
        "Return the complete requested JSON object, including a non-empty "
        "workouts array, even when retaining the current schedule. Never return "
        "an empty object.\n\n"
        "<coaching_packet_json>\n"
        f"{packet_json}\n"
        "</coaching_packet_json>\n"
    )


def _failure_detail(stderr: str, stdout: str) -> str:
    """Return actionable CLI errors without echoing the submitted packet."""
    combined = stderr or stdout or "Unknown Codex CLI error"
    error_lines: list[str] = []
    for line in combined.splitlines():
        stripped = line.strip()
        if stripped.startswith("ERROR:") and stripped not in error_lines:
            error_lines.append(stripped)
    if error_lines:
        return "\n".join(error_lines[-3:])
    detail = combined.strip()
    return detail[-1500:] if len(detail) > 1500 else detail


def _stop_child_process(process: subprocess.Popen[str]) -> tuple[str, str]:
    """Stop the exact child process tree started for this generation run."""
    if process.poll() is not None:
        return process.communicate()
    if os.name == "nt":
        # The installed Codex launcher is commonly a .cmd wrapper around Node.
        # Stopping only that wrapper can leave its child running, so terminate
        # the tree rooted at the PID we created. No unrelated PID is targeted.
        try:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            process.terminate()
    else:
        process.terminate()
    try:
        return process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.communicate()


def _run_cancellable(
    command: list[str],
    *,
    input_text: str,
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: int,
    cancel_event: threading.Event,
    progress_callback: Callable[[float], None] | None,
) -> subprocess.CompletedProcess[str]:
    """Run Codex while polling for cancellation and elapsed-time updates."""
    if cancel_event.is_set():
        raise CodexCliCancelledError("Codex generation was cancelled.")
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=cwd,
            env=dict(environment),
            shell=False,
        )
    except OSError as exc:
        raise CodexPlanGenerationError(f"Could not start Codex CLI: {exc}") from exc

    started = time.monotonic()
    first_communicate = True
    while True:
        try:
            stdout, stderr = process.communicate(
                input=input_text if first_communicate else None,
                timeout=0.25,
            )
            return subprocess.CompletedProcess(
                command,
                process.returncode,
                stdout=stdout,
                stderr=stderr,
            )
        except subprocess.TimeoutExpired:
            first_communicate = False
            elapsed = time.monotonic() - started
            if progress_callback is not None:
                progress_callback(elapsed)
            if cancel_event.is_set():
                _stop_child_process(process)
                raise CodexCliCancelledError("Codex generation was cancelled.")
            if elapsed >= timeout_seconds:
                _stop_child_process(process)
                raise CodexCliTimeoutError(
                    f"Codex did not finish within "
                    f"{timeout_seconds // 60 or 1} minute(s)."
                )


def generate_plan_with_codex(
    packet: Mapping[str, Any],
    *,
    prompt: str,
    timeout_seconds: int = DEFAULT_CODEX_TIMEOUT_SECONDS,
    executable: str | None = None,
    cancel_event: threading.Event | None = None,
    progress_callback: Callable[[float], None] | None = None,
) -> CodexPlanGenerationResult:
    """Run ``codex exec`` with saved account auth and return its final JSON.

    API-key environment variables are explicitly removed from the child process
    so the invocation cannot silently switch away from the user's saved Codex
    login.  The child runs read-only, without user configuration, in an isolated
    temporary directory that contains only the response schema.
    """
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("prompt must be a non-empty string")
    if not isinstance(timeout_seconds, int) or timeout_seconds < 1:
        raise ValueError("timeout_seconds must be a positive integer")

    codex_executable = executable or find_codex_cli()
    if not codex_executable:
        raise CodexCliNotFoundError(
            "Codex CLI was not found on PATH. Install it and run 'codex login', "
            "then restart Garmin Data Hub."
        )

    child_environment = os.environ.copy()
    for variable in _API_KEY_ENVIRONMENT_VARIABLES:
        child_environment.pop(variable, None)

    with tempfile.TemporaryDirectory(prefix="garmin-data-hub-codex-") as temp_dir:
        temp_path = Path(temp_dir)
        schema_path = temp_path / "training-plan-response.schema.json"
        final_response_path = temp_path / "final-response.json"
        schema_path.write_text(
            json.dumps(_strict_output_schema(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        command = [
            codex_executable,
            "exec",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--config",
            f'model_reasoning_effort="{DEFAULT_CODEX_REASONING_EFFORT}"',
            "--color",
            "never",
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(final_response_path),
            "-",
        ]
        if cancel_event is not None:
            completed = _run_cancellable(
                command,
                input_text=_generation_input(packet, prompt),
                cwd=temp_path,
                environment=child_environment,
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
            )
        else:
            try:
                completed = subprocess.run(
                    command,
                    input=_generation_input(packet, prompt),
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    capture_output=True,
                    cwd=temp_path,
                    env=child_environment,
                    timeout=timeout_seconds,
                    check=False,
                    shell=False,
                )
            except subprocess.TimeoutExpired as exc:
                raise CodexCliTimeoutError(
                    f"Codex did not finish within "
                    f"{timeout_seconds // 60 or 1} minute(s)."
                ) from exc
            except OSError as exc:
                raise CodexPlanGenerationError(
                    f"Could not start Codex CLI: {exc}"
                ) from exc

        if completed.returncode != 0:
            detail = _failure_detail(completed.stderr, completed.stdout)
            raise CodexPlanGenerationError(
                f"Codex CLI exited with status {completed.returncode}: {detail}"
            )

        # --output-last-message is the CLI's dedicated final-answer channel and
        # is more reliable than depending on terminal stdout alone. Keep stdout
        # as a compatibility fallback for older CLI builds and test doubles.
        response = ""
        if final_response_path.is_file():
            response = final_response_path.read_text(
                encoding="utf-8", errors="replace"
            ).strip()
        if not response:
            response = completed.stdout.strip()

    if not response:
        raise CodexPlanGenerationError(
            "Codex finished without returning a proposal. No plan was saved; "
            "run generation again."
        )
    try:
        parsed = json.loads(response)
    except json.JSONDecodeError as exc:
        raise CodexPlanGenerationError(
            "Codex completed but did not return a valid JSON object."
        ) from exc
    if not isinstance(parsed, dict):
        raise CodexPlanGenerationError(
            "Codex completed but its final response was not a JSON object."
        )
    if not isinstance(parsed.get("workouts"), list) or not parsed["workouts"]:
        raise CodexPlanGenerationError(
            "Codex returned an empty training-plan proposal. No plan was saved; "
            "run generation again."
        )
    return CodexPlanGenerationResult(
        response_json=json.dumps(parsed, ensure_ascii=False, indent=2),
        executable=codex_executable,
    )
