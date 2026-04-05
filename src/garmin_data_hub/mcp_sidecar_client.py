from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


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
) -> str:
    env = {
        **os.environ,
        "GARMIN_DATA_DIR": str(db_path.parent),
    }
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "garmin_mcp"],
        env=env,
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments or {})
            text_payload = _extract_text_payload(result)
            if result.isError:
                message = text_payload or f"MCP tool '{tool_name}' returned an error"
                raise RuntimeError(message)
            return text_payload


def call_tool_via_sidecar(
    tool_name: str,
    arguments: dict[str, Any] | None,
    db_path: Path,
) -> str:
    return anyio.run(_call_tool_async, tool_name, arguments, db_path)


def check_sidecar_available(db_path: Path) -> tuple[bool, str]:
    try:
        output = call_tool_via_sidecar("garmin_schema", None, db_path)
        return (bool(output), "")
    except Exception as exc:
        return (False, str(exc))
