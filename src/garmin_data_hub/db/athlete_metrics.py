from __future__ import annotations
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

@dataclass
class AthleteMetrics:
    hrmax_calc: int | None
    lthr_calc: int | None
    hrmax_override: int | None
    lthr_override: int | None

    @property
    def hrmax_effective(self) -> int | None:
        return self.hrmax_override if self.hrmax_override is not None else self.hrmax_calc

    @property
    def lthr_effective(self) -> int | None:
        return self.lthr_override if self.lthr_override is not None else self.lthr_calc


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_row(conn: sqlite3.Connection) -> None:
    conn.execute("INSERT OR IGNORE INTO athlete_metrics(athlete_id) VALUES (1)")
    conn.commit()


def get_metrics(conn: sqlite3.Connection) -> AthleteMetrics:
    ensure_row(conn)
    row = conn.execute(
        "SELECT hrmax_calc, lthr_calc, hrmax_override, lthr_override FROM athlete_metrics WHERE athlete_id=1"
    ).fetchone()
    return AthleteMetrics(
        hrmax_calc=row[0], lthr_calc=row[1],
        hrmax_override=row[2], lthr_override=row[3],
    )


def set_calculated(conn: sqlite3.Connection, hrmax: int | None, lthr: int | None) -> None:
    ensure_row(conn)
    conn.execute(
        "UPDATE athlete_metrics SET hrmax_calc=?, lthr_calc=?, calc_updated_at=? WHERE athlete_id=1",
        (hrmax, lthr, _utc_now()),
    )
    conn.commit()


def set_override(conn: sqlite3.Connection, hrmax: int | None, lthr: int | None) -> None:
    ensure_row(conn)
    conn.execute(
        "UPDATE athlete_metrics SET hrmax_override=?, lthr_override=?, override_updated_at=? WHERE athlete_id=1",
        (hrmax, lthr, _utc_now()),
    )
    conn.commit()


def clear_override(conn: sqlite3.Connection) -> None:
    ensure_row(conn)
    conn.execute(
        "UPDATE athlete_metrics SET hrmax_override=NULL, lthr_override=NULL, override_updated_at=? WHERE athlete_id=1",
        (_utc_now(),),
    )
    conn.commit()
