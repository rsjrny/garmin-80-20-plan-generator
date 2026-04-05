from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any

@dataclass
class AthleteProfile:
    athlete_name: str
    age: int
    sport: str
    hrmax: Optional[int]
    lthr: Optional[int]
    sodium_mg_per_hr_hot: Optional[int]
    notes: str

@dataclass
class EventProfile:
    event_name: str
    distance: str
    event_date: str  # ISO YYYY-MM-DD
    start_date: str  # ISO YYYY-MM-DD
    run_days_per_week: int

@dataclass
class AnalysisSummary:
    # Garmin-derived suggestions (may be None if unavailable)
    hrmax_observed: Optional[int]
    hrmax_robust: Optional[int]
    lthr_suggested: Optional[int]
    active_weeks: Optional[int]
    avg_weekly_hours: Optional[float]
    avg_weekly_miles: Optional[float]
    z2_fraction: Optional[float]  # 0..1
    notes: str
    raw: Dict[str, Any]

@dataclass
class Inputs:
    athlete: AthleteProfile
    event: EventProfile
    garmin_files: List[Path]
    output_dir: Path
    use_llm: bool
    openai_api_key: Optional[str]
