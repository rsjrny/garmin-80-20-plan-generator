"""Database package exports for garmin_data_hub.db

Expose commonly-used submodules so callers can `from garmin_data_hub.db import queries`.

This module implements a resilient, lazy import for submodules so that
packaged/frozen builds (PyInstaller) or partial installs still allow
`import garmin_data_hub.db` to succeed and only import heavy submodules
when they are actually accessed.
"""

from . import sqlite, migrate
import importlib
from typing import Any

# Try eager import for convenience; if it fails, leave a placeholder and
# implement a module-level __getattr__ to import lazily on access.
queries = None
try:
    queries = importlib.import_module(f"{__package__}.queries")
except Exception:
    queries = None


def __getattr__(name: str) -> Any:
    """Lazily import optional submodules on attribute access.

    Allows `from garmin_data_hub.db import queries` to work even when the
    submodule wasn't bundled at package import time (common in frozen apps).
    """
    if name == "queries":
        mod = importlib.import_module(f"{__package__}.queries")
        globals()["queries"] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["sqlite", "migrate", "queries"]
