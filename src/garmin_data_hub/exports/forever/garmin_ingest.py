from __future__ import annotations
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import math
import pandas as pd
from datetime import date

try:
    from fitparse import FitFile
except Exception:
    FitFile = None

from .models import AnalysisSummary

def _safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None

def analyze_garmin(files: List[Path], cutoff_date: Optional[date] = None) -> AnalysisSummary:
    """
    Accepts:
      - activities_summary.csv (preferred) with columns including max_hr, Z1_min..Z5_min, week
      - weekly_summary.csv (optional) with hours/miles
      - monthly_summary.csv (optional)
      - .zip containing FIT files (optional) to extract max HR
      - individual .fit (optional)

    Returns a conservative summary with robust HRmax (99.5th percentile) and suggested LTHR (~0.86*robust HRmax).
    """
    raw: Dict[str, Any] = {}
    act_df = None
    week_df = None

    # Load CSVs if present
    for f in files:
        if f.suffix.lower() == ".csv":
            try:
                df = pd.read_csv(f)
            except Exception:
                continue
            cols = set(map(str, df.columns))
            if "max_hr" in cols and "Z2_min" in cols and "week" in cols:
                act_df = df
                raw["activities_summary"] = {"path": str(f), "rows": int(len(df)), "cols": list(df.columns)}
            elif "week" in cols and "hours" in cols and "miles" in cols:
                week_df = df
                raw["weekly_summary"] = {"path": str(f), "rows": int(len(df)), "cols": list(df.columns)}

    # HRmax from activities_summary.csv if possible
    hrmax_observed = None
    hrmax_robust = None
    z2_fraction = None

    if act_df is not None and "max_hr" in act_df.columns:
        # Filter by date if cutoff_date is provided
        if cutoff_date is not None and "date" in act_df.columns:
            act_df["date"] = pd.to_datetime(act_df["date"], errors="coerce").dt.date
            act_df = act_df[act_df["date"] >= cutoff_date]

        # drop NaNs
        m = pd.to_numeric(act_df["max_hr"], errors="coerce").dropna()
        if len(m) > 0:
            hrmax_observed = int(m.max())
            # robust: 99.5th percentile to avoid occasional spikes
            hrmax_robust = int(round(m.quantile(0.995)))
        # Z2 fraction from minutes distribution (only if minutes columns exist)
        z_cols = ["Z1_min","Z2_min","Z3_min","Z4_min","Z5_min"]
        if all(c in act_df.columns for c in z_cols):
            z = act_df[z_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
            total = z.sum(axis=1).sum()
            z2 = z["Z2_min"].sum()
            if total > 0:
                z2_fraction = float(z2/total)

    # HRmax from FIT/FIT ZIP (optional)
    fit_hrmax = None
    if FitFile is not None:
        for f in files:
            if f.suffix.lower() == ".fit":
                fit_hrmax = max(fit_hrmax or 0, _max_hr_from_fit(f))
            elif f.suffix.lower() == ".zip":
                fit_hrmax = max(fit_hrmax or 0, _max_hr_from_zip(f))
    if fit_hrmax:
        raw["fit_hrmax"] = int(fit_hrmax)
        # Prefer robust from CSV, else use FIT-derived
        if hrmax_observed is None:
            hrmax_observed = int(fit_hrmax)
        if hrmax_robust is None:
            hrmax_robust = int(fit_hrmax)

    # weekly averages
    active_weeks = None
    avg_weekly_hours = None
    avg_weekly_miles = None
    if week_df is not None:
        if "hours" in week_df.columns and "miles" in week_df.columns:
            hrs = pd.to_numeric(week_df["hours"], errors="coerce").dropna()
            mi = pd.to_numeric(week_df["miles"], errors="coerce").dropna()
            if len(hrs) > 0:
                active_weeks = int(len(hrs))
                avg_weekly_hours = float(hrs.mean())
            if len(mi) > 0:
                avg_weekly_miles = float(mi.mean())

    # Suggested LTHR: conservative fraction of robust HRmax if available
    lthr_suggested = None
    if hrmax_robust is not None:
        lthr_suggested = int(round(hrmax_robust * 0.86))

    notes = []
    if hrmax_observed is None:
        notes.append("No HRmax could be inferred from provided files. (Provide activities_summary.csv or FIT zip.)")
    else:
        notes.append(f"Inferred HRmax observed={hrmax_observed}, robust≈{hrmax_robust}.")
    if lthr_suggested is not None:
        notes.append(f"Suggested LTHR≈{lthr_suggested} (0.86 × robust HRmax) as a conservative starting point.")
    if z2_fraction is not None:
        notes.append(f"Z2 share (time-based)≈{z2_fraction:.0%}.")
    if avg_weekly_hours is not None:
        notes.append(f"Avg weekly hours≈{avg_weekly_hours:.1f}, miles≈{avg_weekly_miles:.1f}.")

    return AnalysisSummary(
        hrmax_observed=hrmax_observed,
        hrmax_robust=hrmax_robust,
        lthr_suggested=lthr_suggested,
        active_weeks=active_weeks,
        avg_weekly_hours=avg_weekly_hours,
        avg_weekly_miles=avg_weekly_miles,
        z2_fraction=z2_fraction,
        notes=" ".join(notes),
        raw=raw,
    )

def _max_hr_from_fit(path: Path) -> int:
    try:
        ff = FitFile(str(path))
        mx = 0
        for msg in ff.get_messages("record"):
            for d in msg:
                if d.name == "heart_rate" and d.value is not None:
                    mx = max(mx, int(d.value))
        return mx
    except Exception:
        return 0

def _max_hr_from_zip(zip_path: Path) -> int:
    import zipfile, tempfile, os
    mx = 0
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            with tempfile.TemporaryDirectory() as td:
                for name in z.namelist():
                    if name.lower().endswith(".fit"):
                        out = Path(td) / Path(name).name
                        with z.open(name) as src, open(out, "wb") as dst:
                            dst.write(src.read())
                        mx = max(mx, _max_hr_from_fit(out))
        return mx
    except Exception:
        return 0
