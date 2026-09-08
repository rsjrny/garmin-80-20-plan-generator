"""PyInstaller entry point for the primary NiceGUI application."""

import multiprocessing
import os
from pathlib import Path
import runpy
import sys
import traceback


def _write_startup_error() -> None:
    """Leave a useful diagnostic behind when a windowed build cannot start."""
    root = Path(os.getenv("LOCALAPPDATA", Path.home())) / "GarminDataHub" / "logs"
    root.mkdir(parents=True, exist_ok=True)
    (root / "nicegui_startup_error.log").write_text(traceback.format_exc(), encoding="utf-8")


def _run() -> None:
    try:
        if "--mcp-sidecar" in sys.argv:
            runpy.run_module("garmin_mcp", run_name="__main__")
            return
        from garmin_data_hub.ui_nicegui.app import main

        main()
    except BaseException:  # the frozen window has no console for startup failures
        _write_startup_error()
        raise


if __name__ == "__main__":
    multiprocessing.freeze_support()
    _run()
