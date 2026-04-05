from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Dict, Optional
import json
from datetime import datetime
from fitparse import FitFile

@dataclass
class FitMessageRaw:
    msg_name: str
    msg_index: int
    timestamp_utc: str | None
    fields: List[Dict[str, Any]]

@dataclass
class ParsedFitFile:
    messages: List[FitMessageRaw]
    # We can also extract high-level objects for easier insertion into canonical tables
    sessions: List[Dict[str, Any]]
    laps: List[Dict[str, Any]]
    records: List[Dict[str, Any]]
    events: List[Dict[str, Any]]
    file_id: Dict[str, Any] | None
    hr_zone_data: Dict[str, int] | None

def _format_timestamp(dt) -> str | None:
    if dt is None:
        return None
    try:
        # Return ISO 8601 string
        return dt.isoformat()
    except Exception:
        return None

def _to_epoch_seconds(dt) -> int | None:
    if dt is None:
        return None
    try:
        return int(dt.timestamp())
    except Exception:
        return None

def calculate_hr_zones_from_samples(hr_samples: list, lthr: int) -> dict | None:
    """
    Calculate time in each HR zone from heart rate samples.
    
    Args:
        hr_samples: List of (timestamp, heart_rate) tuples
        lthr: Lactate threshold heart rate
        
    Returns:
        dict with zone_1_s through zone_5_s, or None if calculation fails
    """
    if not hr_samples or len(hr_samples) < 2 or not lthr:
        return None
    
    # Estimate max HR from LTHR (LTHR ≈ 86% of MaxHR)
    max_hr = int(lthr / 0.86)
    
    # Zone boundaries (% of MaxHR)
    zone_thresholds = [
        (0, max_hr * 0.60),           # Z1: Recovery
        (max_hr * 0.60, max_hr * 0.70), # Z2: Aerobic
        (max_hr * 0.70, max_hr * 0.80), # Z3: Tempo
        (max_hr * 0.80, max_hr * 0.90), # Z4: Threshold
        (max_hr * 0.90, 250)           # Z5: VO2max+
    ]
    
    zone_times = {f'zone_{i}_s': 0 for i in range(1, 6)}
    
    # Calculate time in each zone
    for i in range(1, len(hr_samples)):
        prev_ts, _ = hr_samples[i-1]
        curr_ts, hr = hr_samples[i]
        duration = curr_ts - prev_ts
        
        for zone_idx, (lower, upper) in enumerate(zone_thresholds, start=1):
            if lower <= hr < upper:
                zone_times[f'zone_{zone_idx}_s'] += duration
                break
    
    # Only return if we actually calculated some zone time
    total_time = sum(zone_times.values())
    if total_time <= 0:
        return None
    
    return zone_times

def parse_fit(path: Path, lthr: Optional[int] = None) -> ParsedFitFile:
    ff = FitFile(str(path))
    
    messages = []
    sessions = []
    laps = []
    records = []
    events = []
    file_id = None
    hr_samples = []
    
    # Counters for message index per type
    msg_counters = {}

    for msg in ff.get_messages():
        name = msg.name
        
        # Increment index
        idx = msg_counters.get(name, 0)
        msg_counters[name] = idx + 1
        
        # Extract fields
        fields_data = []
        msg_values = {}
        timestamp = None
        
        for field in msg:
            f_name = field.name
            val = field.value
            units = field.units
            
            # Basic type inference for storage
            base_type = "unknown"
            val_int = None
            val_real = None
            val_text = None
            val_blob = None
            
            if isinstance(val, int):
                base_type = "int"
                val_int = val
            elif isinstance(val, float):
                base_type = "float"
                val_real = val
            elif isinstance(val, str):
                base_type = "string"
                val_text = val
            elif isinstance(val, bytes):
                base_type = "blob"
                val_blob = val
            elif isinstance(val, datetime):
                base_type = "datetime"
                val_text = val.isoformat()
            else:
                # Fallback for lists/tuples/etc
                base_type = "json"
                val_text = json.dumps(val, default=str)

            fields_data.append({
                "name": f_name,
                "base_type": base_type,
                "units": units,
                "val_int": val_int,
                "val_real": val_real,
                "val_text": val_text,
                "val_blob": val_blob
            })
            
            msg_values[f_name] = val
            
            if f_name == "timestamp" and isinstance(val, datetime):
                timestamp = val

        # Create raw message object
        messages.append(FitMessageRaw(
            msg_name=name,
            msg_index=idx,
            timestamp_utc=_format_timestamp(timestamp),
            fields=fields_data
        ))
        
        # Extract canonical entities
        if name == "session":
            sessions.append(msg_values)
        elif name == "lap":
            laps.append(msg_values)
        elif name == "record":
            records.append(msg_values)
            # Collect HR samples for zone calculation
            if timestamp and "heart_rate" in msg_values and msg_values["heart_rate"]:
                hr_samples.append((_to_epoch_seconds(timestamp), msg_values["heart_rate"]))
        elif name == "event":
            events.append(msg_values)
        elif name == "file_id":
            file_id = msg_values

    # Calculate HR zones if LTHR is provided and we have HR data
    hr_zone_data = None
    if lthr and hr_samples:
        hr_zone_data = calculate_hr_zones_from_samples(hr_samples, lthr)

    return ParsedFitFile(
        messages=messages,
        sessions=sessions,
        laps=laps,
        records=records,
        events=events,
        file_id=file_id,
        hr_zone_data=hr_zone_data
    )
