"""Shared support for tests that exercise NiceGUI's real event callbacks."""

from __future__ import annotations

import asyncio
import sys

from nicegui.testing import user_simulation

from garmin_data_hub.ui_nicegui import app as nicegui_app


async def wait_until(predicate, *, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("UI action did not reach the expected state")
        await asyncio.sleep(0.01)


def run_ui(db_path, scenario, *, sandboxed=True):
    async def run():
        async with user_simulation(
            root=lambda: nicegui_app.create_ui(db_path, sandboxed=sandboxed)
        ) as user:
            await user.open("/")
            await scenario(user)

    # NiceGUI evicts route modules and their parent packages on teardown.
    # Keep the already imported application modules consistent for later tests.
    modules = {name: module for name, module in sys.modules.items()
               if name == "garmin_data_hub" or name.startswith("garmin_data_hub.")}
    try:
        asyncio.run(run())
    finally:
        sys.modules.update(modules)
