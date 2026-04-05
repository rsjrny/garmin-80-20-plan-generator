from __future__ import annotations

import os
import sys
from importlib import resources
from pathlib import Path

APP_NAME = "GarminDataHub"


def default_db_path() -> Path:
    """Return the path to the garmin-givemydata SQLite database.

      Resolution order:
    1. GARMIN_DATA_DIR env var  → <dir>/garmin.db
    2. GARMIN_DATA_HUB_DB env var (legacy override kept for compatibility)
          3. %LOCALAPPDATA%/GarminDataHub/garmin.db
    """
    env_dir = os.environ.get("GARMIN_DATA_DIR")
    if env_dir:
        return Path(env_dir) / "garmin.db"

    legacy = os.environ.get("GARMIN_DATA_HUB_DB")
    if legacy:
        return Path(legacy).expanduser()

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        base_dir = Path(local_app_data) / APP_NAME
    else:
        # Fallback for environments where LOCALAPPDATA is unavailable.
        base_dir = Path.home() / "AppData" / "Local" / APP_NAME

    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / "garmin.db"


def default_backup_dir() -> Path:
    """Retained for API compatibility; garmin-givemydata manages its own files."""
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / APP_NAME
    return Path.home() / "AppData" / "Local" / APP_NAME


def ensure_app_dirs() -> None:
    """Ensure the givemydata data directory exists."""
    default_db_path().parent.mkdir(parents=True, exist_ok=True)


def _schema_sql_candidates() -> list[Path]:
    candidates = [Path(__file__).resolve().parent / "db" / "schema.sql"]

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.extend(
            [
                exe_dir / "garmin_data_hub" / "db" / "schema.sql",
                exe_dir / "_internal" / "garmin_data_hub" / "db" / "schema.sql",
            ]
        )

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        candidates.append(Path(meipass) / "garmin_data_hub" / "db" / "schema.sql")

    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        deduped.append(candidate)
        seen.add(candidate)
    return deduped


def schema_sql_path() -> Path:
    for candidate in _schema_sql_candidates():
        if candidate.exists():
            return candidate
    return _schema_sql_candidates()[0]


def read_schema_sql() -> str:
    try:
        return (
            resources.files("garmin_data_hub.db")
            .joinpath("schema.sql")
            .read_text(encoding="utf-8")
        )
    except (AttributeError, FileNotFoundError, ModuleNotFoundError, OSError):
        pass

    for candidate in _schema_sql_candidates():
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")

    searched = "\n".join(f" - {path}" for path in _schema_sql_candidates())
    raise FileNotFoundError(
        "Could not locate garmin_data_hub db schema.sql. Searched:\n" + searched
    )
