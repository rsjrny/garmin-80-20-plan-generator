from __future__ import annotations
from pathlib import Path
import pandas as pd
import json

from .parsers_fit import ParsedActivity

def _to_epoch_seconds_any(x) -> int | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return int(x)
    dt = pd.to_datetime(x, errors="coerce", utc=True)
    if pd.isna(dt):
        return None
    return int(dt.to_pydatetime().timestamp())

def parse_csv(path: Path) -> ParsedActivity:
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]

    rename = {
        "time": "timestamp",
        "datetime": "timestamp",
        "date": "timestamp",
        "hr": "heart_rate_bpm",
        "heartrate": "heart_rate_bpm",
        "heart_rate": "heart_rate_bpm",
        "cadence": "cadence_spm",
        "speed": "speed_mps",
        "distance": "distance_m",
        "altitude": "alt_m",
        "elevation": "alt_m",
        "power": "power_w",
        "watts": "power_w",
        "temperature": "temp_c",
        "lat": "lat_deg",
        "lon": "lon_deg",
        "long": "lon_deg",
        "lng": "lon_deg",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "timestamp" not in df.columns:
        raise ValueError("CSV must contain a timestamp/time/date column")

    df["t"] = df["timestamp"].apply(_to_epoch_seconds_any)
    df = df.dropna(subset=["t"]).sort_values("t")

    records = []
    for _, r in df.iterrows():
        rec = {
            "t": int(r["t"]),
            "lat_deg": float(r["lat_deg"]) if "lat_deg" in df.columns and pd.notna(r.get("lat_deg")) else None,
            "lon_deg": float(r["lon_deg"]) if "lon_deg" in df.columns and pd.notna(r.get("lon_deg")) else None,
            "alt_m": float(r["alt_m"]) if "alt_m" in df.columns and pd.notna(r.get("alt_m")) else None,
            "distance_m": float(r["distance_m"]) if "distance_m" in df.columns and pd.notna(r.get("distance_m")) else None,
            "speed_mps": float(r["speed_mps"]) if "speed_mps" in df.columns and pd.notna(r.get("speed_mps")) else None,
            "heart_rate_bpm": int(r["heart_rate_bpm"]) if "heart_rate_bpm" in df.columns and pd.notna(r.get("heart_rate_bpm")) else None,
            "cadence_spm": int(r["cadence_spm"]) if "cadence_spm" in df.columns and pd.notna(r.get("cadence_spm")) else None,
            "power_w": int(r["power_w"]) if "power_w" in df.columns and pd.notna(r.get("power_w")) else None,
            "temp_c": float(r["temp_c"]) if "temp_c" in df.columns and pd.notna(r.get("temp_c")) else None,
            "extra_json": None,
        }
        records.append(rec)

    start_t = int(records[0]["t"]) if records else None
    end_t = int(records[-1]["t"]) if records else None

    return ParsedActivity(
        sport="running", sub_sport=None,
        start_time_utc=start_t, end_time_utc=end_t,
        session={}, laps=[], events=[], records=records
    )
