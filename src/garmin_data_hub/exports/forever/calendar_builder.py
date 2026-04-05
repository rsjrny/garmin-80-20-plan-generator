from datetime import date, timedelta
from dataclasses import dataclass
from .workout_db import WORKOUTS, Workout
from .training_rules import build_weekly_schedule

@dataclass
class DayPlan:
    iso_date: str
    day: str
    week: int
    phase: str
    flags: str
    workout: str
    notes: str

def _week_index(start: date, d: date) -> int:
    return ((d - start).days // 7) + 1

def _get_phase(d: date, race_date: date, total_weeks: int) -> str:
    weeks_from_race = (race_date - d).days // 7
    if weeks_from_race <= 2:
        return "Taper"
    if weeks_from_race <= 8:
        return "Peak"
    if weeks_from_race <= total_weeks * 0.66:
        return "Build"
    return "Base"

def build_calendar(start_iso: str, end_iso: str, race_iso: str, run_days_per_week: int = 5, age: int = 40, race_distance: str = "50K", long_run_day: str = "Saturday"):
    start = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    race = date.fromisoformat(race_iso)
    total_weeks = (race - start).days // 7

    # Convert long_run_day to weekday number (Monday=0, Sunday=6)
    day_map = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
    long_run_dow = day_map.get(long_run_day, 5)  # Default to Saturday if invalid

    # Age-based adjustments
    is_masters = age >= 50
    quality_frequency = 3 if is_masters else 2
    long_run_progression_factor = 0.8 if is_masters else 1.0

    # Distance-based adjustments
    distance_caps = {
        "5K": (1.5, 0.0),
        "10K": (2.0, 0.0),
        "HM": (2.5, 0.0),
        "MAR": (3.5, 0.0),
        "50K": (5.0, 2.0),
        "50M": (6.0, 2.5),
        "100K": (7.0, 3.0),
        "100M": (8.0, 4.0)
    }
    
    max_sat_h, max_sun_h = distance_caps.get(race_distance, (5.0, 2.0))

    # Get weekly schedule (enforces Monday/Friday rest for 5 days/week)
    weekly_schedule = build_weekly_schedule(run_days_per_week, long_run_day)
    
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    schedule_by_dow = {i: weekly_schedule[days_of_week[i]] for i in range(7)}

    def get_workout_for_day(d: date, phase: str, week_of_plan: int, is_cutback: bool) -> tuple[Workout, str]:
        dow = d.weekday()  # Monday is 0, Sunday is 6
        day_name = days_of_week[dow]

        # Check weekly schedule first - rest days take priority
        is_rest_day = schedule_by_dow[dow] == "rest"
        
        if is_rest_day:
            return WORKOUTS["OFF"], WORKOUTS["OFF"].description

        # From here: only for "run" days per weekly schedule
        
        # Taper Phase
        if phase == "Taper":
            if dow == 0:
                return WORKOUTS["OFF"], WORKOUTS["OFF"].description
            if dow == 1:
                return WORKOUTS["RECOVERY"], WORKOUTS["RECOVERY"].description
            if dow == 2:
                return WORKOUTS["STRIDES"], WORKOUTS["STRIDES"].description
            if dow == 3:
                return WORKOUTS["OFF"], WORKOUTS["OFF"].description
            if dow == 4:
                return WORKOUTS["RECOVERY"], WORKOUTS["RECOVERY"].description
            if dow == 5:
                return WORKOUTS["OFF"], WORKOUTS["OFF"].description
            if d == race:
                return WORKOUTS["LONG_TRAIL"], f"RACE DAY: {race_distance}!"
            return WORKOUTS["OFF"], WORKOUTS["OFF"].description

        # Cutback Week Logic
        if is_cutback:
            if dow == 1 or dow == 2:
                return WORKOUTS["EASY_Z2"], WORKOUTS["EASY_Z2"].description
            if dow == long_run_dow:
                return WORKOUTS["EASY_LONG"], WORKOUTS["EASY_LONG"].description
            return WORKOUTS["EASY_Z2"], WORKOUTS["EASY_Z2"].description

        # Standard Week Logic
        back_to_back_dow = (long_run_dow + 1) % 7
        
        # Long run day
        if dow == long_run_dow:
            base_duration = 1.5
            progression = (week_of_plan / total_weeks) * (max_sat_h - base_duration) * long_run_progression_factor
            duration_h = base_duration + progression
            return WORKOUTS["LONG_TRAIL"], f"{min(duration_h, max_sat_h):.1f} hours"
        
        # Back-to-back day
        if dow == back_to_back_dow and max_sun_h > 0:
            duration_h = 1 + (week_of_plan / total_weeks) * (max_sun_h - 1.0)
            return WORKOUTS["BACK_TO_BACK"], f"{min(duration_h, max_sun_h):.1f} hours"
        
        # Wednesday: Strides or Easy
        if dow == 2:
            if is_masters and week_of_plan % 2 == 0:
                return WORKOUTS["EASY_Z2"], WORKOUTS["EASY_Z2"].description
            return WORKOUTS["STRIDES"], WORKOUTS["STRIDES"].description
        
        # Thursday: Quality day
        if dow == 3:
            if is_masters and week_of_plan % quality_frequency != 0:
                return WORKOUTS["EASY_Z2"], WORKOUTS["EASY_Z2"].description
            
            if phase == "Base":
                return WORKOUTS["HILL_REPEATS"], WORKOUTS["HILL_REPEATS"].description
            if phase == "Build":
                return WORKOUTS["CRUISE_INTERVALS"], WORKOUTS["CRUISE_INTERVALS"].description
            if phase == "Peak":
                return WORKOUTS["TEMPO_STEADY"], WORKOUTS["TEMPO_STEADY"].description
            
            return WORKOUTS["EASY_Z2"], WORKOUTS["EASY_Z2"].description
        
        # All other run days
        return WORKOUTS["EASY_Z2"], WORKOUTS["EASY_Z2"].description

    # Build the calendar
    plans = []
    d = start
    while d <= end:
        week_of_plan = _week_index(start, d)
        phase = _get_phase(d, race, total_weeks)
        is_cutback = week_of_plan % 4 == 0 and phase not in ["Taper"]
        
        flags = []
        if is_cutback:
            flags.append("CUTBACK")
        if phase == "Taper":
            flags.append("TAPER")
        
        workout, notes = get_workout_for_day(d, phase, week_of_plan, is_cutback)
        
        plans.append(DayPlan(
            iso_date=d.isoformat(),
            day=d.strftime("%A"),
            week=week_of_plan,
            phase=phase,
            flags=", ".join(flags) if flags else "",
            workout=workout.name,
            notes=notes
        ))
        d += timedelta(days=1)

    return plans