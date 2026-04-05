from __future__ import annotations
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def connect_sqlite(db_path: Path, timeout: float = 5.0) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    # Keep temporary tables in memory for faster processing of import operations
    try:
        conn.execute("PRAGMA temp_store=MEMORY;")
    except sqlite3.DatabaseError:
        # Some SQLite builds may not support this pragma; keep going with defaults.
        logger.debug(
            "SQLite pragma temp_store=MEMORY is not available for %s",
            db_path,
            exc_info=True,
        )
    return conn
