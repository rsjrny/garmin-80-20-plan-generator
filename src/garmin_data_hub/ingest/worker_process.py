from __future__ import annotations
import traceback
from pathlib import Path
import sqlite3
import json
import time

from .writer import ingest_one_file
from garmin_data_hub.db.sqlite import connect_sqlite

MAX_RETRIES = 5
BASE_RETRY_DELAY = 0.1  # Start with 100ms


def ingest_file_in_process(db_path: str, file_path: str) -> dict:
    """Process a single file with retry logic for database locks."""
    p = Path(file_path)

    for attempt in range(MAX_RETRIES):
        try:
            conn = connect_sqlite(Path(db_path), timeout=30.0)

            try:
                activity_id, inserted, source_id = ingest_one_file(conn, p)
                conn.commit()
                return {
                    "ok": True,
                    "source_id": source_id,
                    "activity_id": activity_id,
                    "inserted": inserted,
                    "path": str(p),
                }
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    conn.close()
                    if attempt < MAX_RETRIES - 1:
                        # Exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s, 1.6s
                        delay = BASE_RETRY_DELAY * (2**attempt)
                        time.sleep(delay)
                        continue
                    else:
                        return {
                            "ok": False,
                            "path": str(p),
                            "error": f"Database locked after {MAX_RETRIES} retries",
                            "trace": traceback.format_exc(),
                        }
                else:
                    raise
            except Exception as e:
                # python-level parse error
                # The new schema does not have a 'problems' table, so we just log the error to the console
                # and return it in the result dict.
                print(f"Error processing {p}: {e}")
                return {
                    "ok": False,
                    "path": str(p),
                    "error": str(e),
                    "trace": traceback.format_exc(),
                }
            finally:
                conn.close()
        except Exception as e:
            if "database is locked" in str(e).lower() and attempt < MAX_RETRIES - 1:
                delay = BASE_RETRY_DELAY * (2**attempt)
                time.sleep(delay)
                continue
            return {
                "ok": False,
                "path": str(file_path),
                "error": str(e),
                "trace": traceback.format_exc(),
            }
