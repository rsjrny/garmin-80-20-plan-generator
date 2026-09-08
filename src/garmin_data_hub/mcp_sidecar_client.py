from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path
from typing import Any

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _get_logger() -> logging.Logger:
    """Return the MCP call logger, creating a rotating-file handler on first use."""
    logger = logging.getLogger("garmin_data_hub.mcp")
    if logger.handlers:
        return logger  # already configured
    logger.setLevel(logging.DEBUG)

    local_app_data = os.environ.get("LOCALAPPDATA", "")
    if local_app_data:
        log_dir = Path(local_app_data) / "GarminDataHub" / "logs"
    else:
        log_dir = Path.home() / ".garmin_data_hub" / "logs"

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            log_dir / "mcp_calls.log",
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3,
            encoding="utf-8",
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(fh)
    except OSError:
        logger.addHandler(logging.NullHandler())

    return logger


def _summarize_args(arguments: dict | None) -> str:
    """Compact single-line summary of tool arguments for log lines."""
    if not arguments:
        return "(none)"
    parts = []
    for k, v in arguments.items():
        sv = str(v)
        if len(sv) > 80:
            sv = sv[:77] + "..."
        parts.append(f"{k}={sv!r}")
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_MAX_RETRIES = 2
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SEC = 1.0


def _sidecar_command() -> tuple[str, list[str]]:
    if getattr(sys, "frozen", False):
        return sys.executable, ["--mcp-sidecar"]
    return sys.executable, ["-m", "garmin_mcp"]


def _extract_text_payload(call_result: Any) -> str:
    parts: list[str] = []
    for item in getattr(call_result, "content", []) or []:
        text = getattr(item, "text", None)
        if isinstance(text, str):
            parts.append(text)
    return "\n".join(parts).strip()


async def _call_tool_async(
    tool_name: str,
    arguments: dict[str, Any] | None,
    db_path: Path,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> str:
    """Call an MCP tool with timeout support.
    
    Args:
        tool_name: Name of the MCP tool to call (e.g., 'garmin_schema')
        arguments: Tool arguments dict, or None if no args needed
        db_path: Path to the Garmin database
        timeout_sec: Timeout in seconds (default: 30s)
        
    Raises:
        TimeoutError: If the tool call exceeds the timeout
        RuntimeError: If the MCP tool returns an error
    """
    env = {
        **os.environ,
        "GARMIN_DATA_DIR": str(db_path.parent),
    }
    server_command, server_args = _sidecar_command()
    server = StdioServerParameters(
        command=server_command,
        args=server_args,
        env=env,
    )

    try:
        with anyio.fail_after(timeout_sec):
            async with stdio_client(server) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments or {})
                    text_payload = _extract_text_payload(result)
                    if result.isError:
                        message = text_payload or f"MCP tool '{tool_name}' returned an error"
                        raise RuntimeError(message)
                    return text_payload
    except TimeoutError:
        raise TimeoutError(
            f"MCP tool '{tool_name}' exceeded {timeout_sec}s timeout. "
            "The sidecar may be busy or unresponsive."
        )


def call_tool_via_sidecar(
    tool_name: str,
    arguments: dict[str, Any] | None,
    db_path: Path,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> str:
    """Call an MCP tool with timeout and retry logic.
    
    Args:
        tool_name: Name of the MCP tool to call
        arguments: Tool arguments dict, or None if no args needed
        db_path: Path to the Garmin database
        timeout_sec: Timeout per attempt in seconds
        max_retries: Maximum retry attempts on transient failures
        
    Returns:
        Tool output as a string
        
    Raises:
        TimeoutError: If all retries timeout or final attempt exceeds timeout
        RuntimeError: If tool returns error that isn't retryable
        Exception: If sidecar cannot be started or other critical failures
    """
    log = _get_logger()
    last_error = None
    t_start = time.monotonic()
    log.info("CALL  tool=%s args=%s", tool_name, _summarize_args(arguments))

    for attempt in range(max_retries + 1):
        attempt_start = time.monotonic()
        try:
            result = anyio.run(
                _call_tool_async, tool_name, arguments, db_path, timeout_sec
            )
            duration_ms = int((time.monotonic() - t_start) * 1000)
            log.info("OK    tool=%s duration_ms=%d", tool_name, duration_ms)
            return result
        except TimeoutError as e:
            attempt_ms = int((time.monotonic() - attempt_start) * 1000)
            log.warning(
                "TIMEOUT tool=%s attempt=%d/%d duration_ms=%d",
                tool_name, attempt + 1, max_retries + 1, attempt_ms,
            )
            last_error = e
            if attempt < max_retries:
                backoff = INITIAL_BACKOFF_SEC * (BACKOFF_FACTOR ** attempt)
                time.sleep(backoff)
                continue
            raise
        except RuntimeError as e:
            duration_ms = int((time.monotonic() - t_start) * 1000)
            log.error(
                "ERROR tool=%s type=RuntimeError duration_ms=%d msg=%s",
                tool_name, duration_ms, e,
            )
            raise
        except Exception as e:
            attempt_ms = int((time.monotonic() - attempt_start) * 1000)
            if attempt < max_retries:
                log.warning(
                    "RETRY tool=%s attempt=%d/%d type=%s duration_ms=%d msg=%s",
                    tool_name, attempt + 1, max_retries + 1, type(e).__name__, attempt_ms, e,
                )
                last_error = e
                backoff = INITIAL_BACKOFF_SEC * (BACKOFF_FACTOR ** attempt)
                time.sleep(backoff)
                continue
            duration_ms = int((time.monotonic() - t_start) * 1000)
            log.error(
                "FAIL  tool=%s type=%s duration_ms=%d msg=%s",
                tool_name, type(e).__name__, duration_ms, e,
            )
            raise

    if last_error:
        raise last_error
    raise RuntimeError(f"Unexpected failure calling MCP tool '{tool_name}'")


def check_sidecar_available(
    db_path: Path, timeout_sec: float = DEFAULT_TIMEOUT_SEC
) -> tuple[bool, str]:
    """Check if MCP sidecar is available by calling garmin_schema.
    
    Args:
        db_path: Path to the Garmin database
        timeout_sec: Timeout for availability check
        
    Returns:
        Tuple of (is_available, error_message). is_available is True if sidecar
        responded successfully. error_message is empty string on success.
    """
    try:
        output = call_tool_via_sidecar(
            "garmin_schema", None, db_path, timeout_sec=timeout_sec, max_retries=1
        )
        return (bool(output), "")
    except Exception as exc:
        return (False, str(exc))
