from __future__ import annotations
import sqlite3
from pathlib import Path


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
    except Exception:
        # Some SQLite builds may not support this pragma; ignore failures
        pass
    return conn
