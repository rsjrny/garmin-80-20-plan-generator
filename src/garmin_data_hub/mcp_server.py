"""Launch the upstream server against the application's exact database."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


def configure_database(path: Path, *, read_only: bool) -> None:
    from garmin_mcp import db

    path = path.resolve(strict=True)
    db.DB_PATH = str(path)
    if read_only:
        def connect(db_path=None):
            # Upstream helpers may pass their default path explicitly.
            if db_path is not None and Path(db_path).resolve() != path:
                raise ValueError("MCP cannot open a different database")
            conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            return conn

        db.get_connection = connect
        # Upstream initializes schema at import time; chat must not migrate it.
        db.init_db = lambda conn: None


def main() -> None:
    path = os.environ.get("GARMIN_DATA_HUB_MCP_DB")
    if path:
        configure_database(Path(path), read_only=os.environ.get("GARMIN_DATA_HUB_MCP_READ_ONLY") == "1")
    from garmin_mcp.server import main as upstream_main

    upstream_main()


if __name__ == "__main__":
    main()
