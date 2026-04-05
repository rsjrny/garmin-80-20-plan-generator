import re
from collections import defaultdict

def _mins_from_notes(notes: str) -> float:
    if not notes:
        return 0.0
    t = str(notes).replace("–","-")
    
    # Check for "X.Y hours" format first (e.g. "1.5 hours")
    h_match = re.search(r'(\d+\.?\d*)\s*hours?', t, flags=re.I)
    if h_match:
        return float(h_match.group(1)) * 60.0

    m = re.findall(r'(\d+):(\d{2})', t)
    if len(m) >= 2:
        a = int(m[0][0])*60 + int(m[0][1])
        b = int(m[1][0])*60 + int(m[1][1])
        return (a+b)/2
    if len(m) == 1:
        return int(m[0][0])*60 + int(m[0][1])
    mr = re.search(r'(\d+)\s*-\s*(\d+)\s*min', t, flags=re.I)
    if mr:
        return (int(mr.group(1)) + int(mr.group(2))) / 2
    ms = re.search(r'(\d+)\s*min', t, flags=re.I)
    if ms:
        return float(ms.group(1))
    return 0.0

def build_weekly_metrics(day_plans):
    weeks = defaultdict(lambda: {
        "Run Days": 0,
        "Strength Days": 0,
        "Quality Sessions": 0,
        "Run Hours (est)": 0.0,
        "Long Run (Sat) hrs": 0.0,
        "Back-to-Back (Sun) hrs": 0.0,
        "Phase": "",
        "Flags": "",
    })

    for dp in day_plans:
        w = weeks[dp.week]
        w["Phase"] = dp.phase
        w["Flags"] = dp.flags

        # Check if it's a run day. 
        # Previously, it checked if workout != "OFF", but "Rest Day" is also an OFF day.
        # The workout_db uses "Rest Day" for OFF.
        # We should check if the workout name implies a run or if it's explicitly NOT a rest day.
        
        is_rest = dp.workout in ("OFF", "Rest Day", "Rest") or "Rest" in dp.workout
        
        if not is_rest:
            w["Run Days"] += 1

        # Strength days are often on OFF days, but can be standalone
        if is_rest and "Strength" in (dp.notes or ""):
            w["Strength Days"] += 1

        if "LT Intervals" in (dp.workout or "") or "Hill Strength" in (dp.workout or "") or "Tempo" in (dp.workout or "") or "Cruise" in (dp.workout or ""):
            w["Quality Sessions"] += 1

        mins = _mins_from_notes(dp.notes or "")
        # Only add to run hours if it's a run day
        if not is_rest:
            w["Run Hours (est)"] += mins / 60.0

        if dp.day == "Saturday" and "Long Trail" in (dp.workout or ""):
            w["Long Run (Sat) hrs"] = mins / 60.0
        if dp.day == "Sunday" and "Back-to-Back" in (dp.workout or ""):
            w["Back-to-Back (Sun) hrs"] = mins / 60.0

    rows = []
    for wk in sorted(weeks.keys()):
        w = weeks[wk]
        rows.append({
            "Week#": wk,
            "Phase": w["Phase"],
            "Flags": w["Flags"],
            "Run Days": w["Run Days"],
            "Strength Days": w["Strength Days"],
            "Quality Sessions": w["Quality Sessions"],
            "Run Hours (est)": round(w["Run Hours (est)"], 2),
            "Long Run (Sat) hrs": round(w["Long Run (Sat) hrs"], 2),
            "Back-to-Back (Sun) hrs": round(w["Back-to-Back (Sun) hrs"], 2),
        })
    return rows
