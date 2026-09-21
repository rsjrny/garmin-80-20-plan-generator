"""PyInstaller entry point for the primary NiceGUI application."""

import multiprocessing
import os
from pathlib import Path
import runpy
import sys
import traceback


def _write_startup_error() -> None:
    root = Path(os.getenv("LOCALAPPDATA", Path.home())) / "GarminDataHub" / "logs"
    root.mkdir(parents=True, exist_ok=True)
    (root / "nicegui_startup_error.log").write_text(traceback.format_exc(), encoding="utf-8")


def _run() -> None:
    try:
        if "--mcp-sidecar" in sys.argv:
            from garmin_data_hub.mcp_server import main as mcp_main
            mcp_main()
            return
        from garmin_data_hub.ui_nicegui.app import main

        main()
    except BaseException:
        _write_startup_error()
        raise


if __name__ == "__main__":
    multiprocessing.freeze_support()
    _run()
