from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

# Configuration constants
DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_MAX_RETRIES = 2
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SEC = 1.0


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
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "garmin_mcp"],
        env=env,
    )

    try:
        async with anyio.fail_after(timeout_sec):
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
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            return anyio.run(
                _call_tool_async, tool_name, arguments, db_path, timeout_sec
            )
        except TimeoutError as e:
            last_error = e
            if attempt < max_retries:
                backoff = INITIAL_BACKOFF_SEC * (BACKOFF_FACTOR ** attempt)
                time.sleep(backoff)
                continue
            raise
        except RuntimeError as e:
            # Non-retryable tool errors (invalid SQL, bad args, etc.)
            raise
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                # Retry on transient errors (connection issues, etc.)
                backoff = INITIAL_BACKOFF_SEC * (BACKOFF_FACTOR ** attempt)
                time.sleep(backoff)
                continue
            raise
    
    # Should not reach here, but just in case
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
