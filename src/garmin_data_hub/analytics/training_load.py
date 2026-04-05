from __future__ import annotations
import pandas as pd
import sqlite3


def get_daily_tss(conn: sqlite3.Connection, start_date_iso: str) -> pd.DataFrame:
    """
    Aggregates TSS for each day from the activity table (garmin-givemydata schema).
    """
    query = """
        SELECT
            date(a.start_time_gmt) as activity_date,
            SUM(a.training_stress_score) as tss
        FROM activity a
        WHERE a.start_time_gmt >= ? AND a.training_stress_score IS NOT NULL
        GROUP BY activity_date
        ORDER BY activity_date ASC
    """
    df = pd.read_sql_query(query, conn, params=(start_date_iso,))
    if df.empty:
        return pd.DataFrame(columns=["date", "tss"]).set_index("date")

    df["date"] = pd.to_datetime(df["activity_date"])
    df = df.set_index("date")

    # Create a full date range to ensure we have a value for every day (even with 0 TSS)
    full_date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq="D")
    df = df.reindex(full_date_range, fill_value=0)

    return df[["tss"]]


def calculate_ctl_atl_tsb(
    daily_tss: pd.DataFrame, ctl_period: int = 42, atl_period: int = 7
) -> pd.DataFrame:
    """
    Calculates Chronic Training Load (CTL), Acute Training Load (ATL),
    and Training Stress Balance (TSB) from daily TSS.
    """
    if daily_tss.empty:
        return pd.DataFrame(columns=["ctl", "atl", "tsb"])

    # Use exponential moving average
    daily_tss["ctl"] = daily_tss["tss"].ewm(span=ctl_period, adjust=False).mean()
    daily_tss["atl"] = daily_tss["tss"].ewm(span=atl_period, adjust=False).mean()

    # TSB = CTL - ATL
    daily_tss["tsb"] = daily_tss["ctl"] - daily_tss["atl"]

    return daily_tss[["ctl", "atl", "tsb"]]
