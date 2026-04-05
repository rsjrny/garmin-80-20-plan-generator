"""
Training rules engine: age-aware, distance-aware, phase-based progression.
Enforces hard/easy separation, recovery, and science-backed training principles.
"""

from dataclasses import dataclass
from typing import Tuple


# =====================================================================
# AGE-BASED ADJUSTMENTS
# =====================================================================

def get_recovery_multiplier(age: int) -> float:
    """Recovery needs increase with age (add recovery days, longer Z1 phases)."""
    if age < 30:
        return 1.0
    elif age < 40:
        return 1.1
    elif age < 50:
        return 1.2
    else:
        return 1.3  # 30% more recovery for 50+


def get_intensity_cap(age: int) -> int:
    """Cap VO2max/Anaerobic efforts by age (injury prevention)."""
    if age < 35:
        return 3  # 3 hard sessions/week OK
    elif age < 45:
        return 2  # 2 per week
    else:
        return 1  # 1 per week for 45+


def phase_duration_by_age(base_weeks: int, age: int) -> int:
    """Older athletes need longer base phases for adaptation."""
    if age >= 50:
        return int(base_weeks * 1.2)
    elif age >= 40:
        return int(base_weeks * 1.1)
    return base_weeks


def cutback_week_frequency(age: int) -> int:
    """How often to insert cutback weeks (reduce volume 20-30%)."""
    if age >= 50:
        return 2  # Every 2 weeks
    elif age >= 40:
        return 3  # Every 3 weeks
    else:
        return 4  # Every 4 weeks (younger athletes recover faster)


# =====================================================================
# DISTANCE-BASED TRAINING PROFILES
# =====================================================================

DISTANCE_PROFILES = {
    "5K": {
        "total_weeks": 8,
        "base_weeks": 2,
        "build_weeks": 4,
        "peak_weeks": 2,
        "vo2_focus": True,
        "threshold_focus": False,
        "long_run_cap": "10K",
        "z2_target": 0.50,      # 50% Z2 (more hard workouts)
        "intensity_days": 3,    # 3 hard sessions/week
        "z1_recovery_days": 1,
        "back_to_back_long_runs": False,
        "vertical_focus": False,
    },
    "10K": {
        "total_weeks": 10,
        "base_weeks": 3,
        "build_weeks": 5,
        "peak_weeks": 2,
        "vo2_focus": True,
        "threshold_focus": True,
        "long_run_cap": "12K",
        "z2_target": 0.55,
        "intensity_days": 3,
        "z1_recovery_days": 1,
        "back_to_back_long_runs": False,
        "vertical_focus": False,
    },
    "HM": {  # Half Marathon
        "total_weeks": 12,
        "base_weeks": 4,
        "build_weeks": 6,
        "peak_weeks": 2,
        "vo2_focus": True,
        "threshold_focus": True,
        "long_run_cap": "18K",
        "z2_target": 0.60,
        "intensity_days": 2,
        "z1_recovery_days": 1,
        "back_to_back_long_runs": False,
        "vertical_focus": False,
    },
    "MAR": {  # Marathon
        "total_weeks": 16,
        "base_weeks": 6,
        "build_weeks": 8,
        "peak_weeks": 2,
        "vo2_focus": False,
        "threshold_focus": True,
        "long_run_cap": "42K",
        "z2_target": 0.70,      # 70% Z2 (aerobic focus)
        "intensity_days": 2,
        "z1_recovery_days": 2,
        "back_to_back_long_runs": False,
        "vertical_focus": False,
    },
    "50K": {
        "total_weeks": 20,
        "base_weeks": 8,
        "build_weeks": 10,
        "peak_weeks": 2,
        "vo2_focus": False,
        "threshold_focus": False,
        "long_run_cap": "50K",
        "z2_target": 0.75,      # 75% Z2 (heavy aerobic)
        "intensity_days": 1,
        "z1_recovery_days": 3,
        "back_to_back_long_runs": True,
        "vertical_focus": False,
    },
    "50M": {
        "total_weeks": 24,
        "base_weeks": 10,
        "build_weeks": 12,
        "peak_weeks": 2,
        "vo2_focus": False,
        "threshold_focus": False,
        "long_run_cap": "50M",
        "z2_target": 0.80,
        "intensity_days": 1,
        "z1_recovery_days": 4,
        "back_to_back_long_runs": True,
        "vertical_focus": True,
    },
    "100K": {
        "total_weeks": 28,
        "base_weeks": 12,
        "build_weeks": 14,
        "peak_weeks": 2,
        "vo2_focus": False,
        "threshold_focus": False,
        "long_run_cap": "100K",
        "z2_target": 0.85,
        "intensity_days": 1,
        "z1_recovery_days": 4,
        "back_to_back_long_runs": True,
        "vertical_focus": True,
    },
    "100M": {
        "total_weeks": 32,
        "base_weeks": 14,
        "build_weeks": 16,
        "peak_weeks": 2,
        "vo2_focus": False,
        "threshold_focus": False,
        "long_run_cap": "100M",
        "z2_target": 0.90,      # 90% Z2 (almost all aerobic)
        "intensity_days": 0,    # NO hard workouts
        "z1_recovery_days": 5,
        "back_to_back_long_runs": True,
        "vertical_focus": True,
    },
}


def distance_to_km(dist_str: str) -> float:
    """Convert '5K', '10K', '50M' to kilometers."""
    d = dist_str.upper()
    if "M" in d:
        miles = int(d.replace("M", ""))
        return miles * 1.609344
    else:
        return float(d.replace("K", ""))


# =====================================================================
# PHASE STRUCTURE WITH AGE/DISTANCE AWARENESS
# =====================================================================

@dataclass
class PhaseConfig:
    """Configuration for a training phase."""
    weeks: int
    z2_target: float          # Target % of volume in Z2
    intensity_days: int       # Hard sessions per week
    long_run_cap_km: float    # Don't exceed this distance
    recovery_multiplier: float


def build_phase_structure(distance: str, age: int) -> dict:
    """
    Build phase structure respecting age and distance constraints.
    Returns: {"Base": PhaseConfig, "Build": PhaseConfig, "Peak": PhaseConfig}
    """
    profile = DISTANCE_PROFILES.get(distance, DISTANCE_PROFILES["50K"])
    
    # Adjust phase durations for age
    base_weeks = phase_duration_by_age(profile["base_weeks"], age)
    build_weeks = profile["build_weeks"]
    peak_weeks = profile["peak_weeks"]
    
    # Cap intensity sessions by age
    actual_intensity_days = min(profile["intensity_days"], get_intensity_cap(age))
    recovery_mult = get_recovery_multiplier(age)
    long_run_km = distance_to_km(profile["long_run_cap"])
    
    return {
        "Base": PhaseConfig(
            weeks=base_weeks,
            z2_target=profile["z2_target"] + 0.05,  # More Z2 in base phase
            intensity_days=0,                        # Base phase = easy only
            long_run_cap_km=long_run_km * 0.6,      # 60% of cap
            recovery_multiplier=recovery_mult
        ),
        "Build": PhaseConfig(
            weeks=build_weeks,
            z2_target=profile["z2_target"],
            intensity_days=actual_intensity_days,
            long_run_cap_km=long_run_km * 0.9,      # 90% of cap
            recovery_multiplier=recovery_mult
        ),
        "Peak": PhaseConfig(
            weeks=peak_weeks,
            z2_target=max(0.50, profile["z2_target"] - 0.10),  # Taper reduces Z2
            intensity_days=actual_intensity_days,
            long_run_cap_km=long_run_km * 0.7,      # Reduced for taper
            recovery_multiplier=recovery_mult
        ),
    }


# =====================================================================
# WEEKLY TRAINING RULES (Hard/Easy Separation, Recovery)
# =====================================================================

@dataclass
class WeekValidation:
    """Result of validating a week's workouts."""
    is_valid: bool
    issues: list  # List of warning/error strings


def validate_week_structure(day_intensities: list) -> WeekValidation:
    """
    Validate that a week follows hard/easy principles.
    
    day_intensities: list of strings like ["Recovery", "Easy", "Hard", "Easy", "VO2max", "Easy", "Recovery"]
    """
    issues = []
    
    hard_intensity_map = {
        "Hard": True,
        "Threshold": True,
        "VO2max": True,
        "Anaerobic": True,
        "Race Pace": True,
    }
    
    easy_intensity_map = {
        "Easy": True,
        "Recovery": True,
    }
    
    hard_days = [i for i, x in enumerate(day_intensities) if hard_intensity_map.get(x)]
    easy_days = [i for i, x in enumerate(day_intensities) if easy_intensity_map.get(x)]
    
    # Check for consecutive hard days
    consecutive_hard = 0
    max_consecutive = 0
    for i in range(len(day_intensities)):
        if hard_intensity_map.get(day_intensities[i]):
            consecutive_hard += 1
            max_consecutive = max(max_consecutive, consecutive_hard)
        else:
            consecutive_hard = 0
    
    # Validation rules
    if len(hard_days) > 3:
        issues.append(f"⚠️ Too many hard sessions ({len(hard_days)}) – max 3/week for recovery")
    
    if max_consecutive > 1:
        issues.append(f"⚠️ {max_consecutive} consecutive hard days – violates hard/easy principle")
    
    if len(easy_days) < 3:
        issues.append(f"⚠️ Only {len(easy_days)} easy/recovery days – need ≥3/week")
    
    return WeekValidation(
        is_valid=len(issues) == 0,
        issues=issues
    )


# =====================================================================
# TAPER LOGIC (Science-backed)
# =====================================================================

TAPER_VOLUME_REDUCTION = {
    "3_weeks_before": 0.70,   # 70% of peak (30% reduction)
    "2_weeks_before": 0.50,   # 50% of peak (50% reduction)
    "1_week_before": 0.25,    # 25% of peak (75% reduction)
    "3_days_before": 0.15,    # 15% of peak (85% reduction)
}

TAPER_LONG_RUN_REDUCTION = {
    "3_weeks_before": 0.80,
    "2_weeks_before": 0.60,
    "1_week_before": 0.40,
    "3_days_before": 0.20,
}


def get_taper_multiplier(days_until_race: int) -> float:
    """Get volume reduction factor based on days until race."""
    if days_until_race <= 3:
        return TAPER_VOLUME_REDUCTION["3_days_before"]
    elif days_until_race <= 7:
        return TAPER_VOLUME_REDUCTION["1_week_before"]
    elif days_until_race <= 14:
        return TAPER_VOLUME_REDUCTION["2_weeks_before"]
    elif days_until_race <= 21:
        return TAPER_VOLUME_REDUCTION["3_weeks_before"]
    else:
        return 1.0  # No taper


# =====================================================================
# TSS (Training Stress Score) ESTIMATION
# =====================================================================

def estimate_tss(workout_description: str, duration_minutes: int, lthr: int) -> int:
    """
    Rough TSS estimation from workout text.
    TSS ≈ (duration_min × intensity_factor) / 100
    """
    desc_lower = workout_description.lower()
    
    # Map intensity descriptions to intensity factors
    intensity_factor = 0.75  # Default (easy)
    
    if any(x in desc_lower for x in ["recovery", "z1", "zone 1"]):
        intensity_factor = 0.50
    elif any(x in desc_lower for x in ["easy", "z2", "zone 2"]):
        intensity_factor = 0.75
    elif any(x in desc_lower for x in ["aerobic", "z3", "zone 3"]):
        intensity_factor = 0.88
    elif any(x in desc_lower for x in ["threshold", "lthr", "z4", "zone 4"]):
        intensity_factor = 1.06
    elif any(x in desc_lower for x in ["vo2", "vo2max", "z5", "zone 5", "max"]):
        intensity_factor = 1.20
    elif any(x in desc_lower for x in ["anaerobic", "sprint"]):
        intensity_factor = 1.50
    
    tss = round((duration_minutes * intensity_factor) / 100 * 100)
    return max(tss, 10)  # Minimum 10 TSS


# =====================================================================
# VOLUME PROGRESSION RULES
# =====================================================================

def check_volume_increase(prev_week_km: float, current_week_km: float) -> Tuple[bool, str]:
    """
    Validate that week-over-week volume increase is safe (<10%).
    Returns: (is_safe, message)
    """
    if prev_week_km == 0:
        return True, "No previous week to compare"
    
    increase_pct = (current_week_km - prev_week_km) / prev_week_km
    
    if increase_pct > 0.10:
        return False, f"⚠️ Volume jump +{increase_pct:.0%} (>10%) – injury risk. Recommend max +10%."
    elif increase_pct > 0.05:
        return True, f"ℹ️ Moderate increase +{increase_pct:.0%} – OK but monitor."
    else:
        return True, f"✓ Safe increase +{increase_pct:.0%}"
    

def build_weekly_schedule(run_days_per_week: int, long_run_day: str = "Saturday") -> dict[str, str]:
    """
    Build weekly schedule enforcing rest/run days.
    
    Args:
        run_days_per_week: Number of running days (3-7)
        long_run_day: Day for long run (default Saturday)
    
    Returns:
        Dict mapping day name to "rest", "run", or "long_run"
    """
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    # Start with all days as rest
    schedule = {day: "rest" for day in days}
    
    # Mark the long run day
    schedule[long_run_day] = "long_run"
    
    # Calculate how many additional run days we need (excluding long run)
    additional_run_days = run_days_per_week - 1  # -1 because long run already counted
    
    if additional_run_days <= 0:
        # Edge case: if someone picks 1 day/week, just long run
        return schedule
    
    # Define rest day rules based on total run days
    if run_days_per_week == 3:
        # 3 days: Monday, Wednesday, Thursday, Friday rest
        # Only Tue, Sat (long), Sun run
        force_rest = ["Monday", "Wednesday", "Thursday", "Friday"]
    elif run_days_per_week == 4:
        # 4 days: Monday, Wednesday, Friday rest
        # Tue, Thu, Sat (long), Sun run
        force_rest = ["Monday", "Wednesday", "Friday"]
    elif run_days_per_week == 5:
        # 5 days: Monday, Friday rest
        # Tue, Wed, Thu, Sat (long), Sun run
        force_rest = ["Monday", "Friday"]
    elif run_days_per_week == 6:
        # 6 days: Monday rest only
        force_rest = ["Monday"]
    else:  # 7 days
        # No forced rest days
        force_rest = []
    
    # Apply forced rest days
    for day in force_rest:
        schedule[day] = "rest"
    
    # Now fill in remaining run days
    # Count how many run days we need to add (excluding long run and forced rest)
    current_run_count = sum(1 for v in schedule.values() if v in ["run", "long_run"])
    needed_run_days = run_days_per_week - current_run_count
    
    # Fill in run days for non-rest, non-long-run days
    for day in days:
        if needed_run_days <= 0:
            break
        if schedule[day] == "rest" and day not in force_rest:
            schedule[day] = "run"
            needed_run_days -= 1
    
    # If we still need more run days, override some forced rest days
    # (happens when long_run_day conflicts with our assumptions)
    if needed_run_days > 0:
        for day in days:
            if needed_run_days <= 0:
                break
            if schedule[day] == "rest":
                schedule[day] = "run"
                needed_run_days -= 1
    
    return schedule

# Add this new dataclass
@dataclass
class IntensityDistribution:
    """Result of analyzing weekly intensity distribution."""
    z1_percent: float      # Recovery zone
    z2_percent: float      # Aerobic (easy)
    z3_percent: float      # Tempo
    z4_percent: float      # Threshold
    z5_percent: float      # VO2max
    z6_percent: float      # Anaerobic/Sprint
    is_compliant: bool     # True if follows 80/20
    warnings: list[str]


def calculate_weekly_intensity_distribution(
    day_workouts: list[str],
    phase: str,
    distance: str,
    age: int
) -> IntensityDistribution:
    """
    Analyze weekly workout list to verify 80/20 compliance.
    
    Args:
        day_workouts: List of 7 workout descriptions (one per day)
        phase: "Base", "Build", or "Peak"
        distance: "5K", "50K", etc.
        age: Athlete age
    
    Returns:
        IntensityDistribution with compliance flag and warnings
    """
    # Zone definitions (TSS per workout estimated)
    zone_tss_map = {
        "Z1": (1, 0),           # (min_tss, max_tss)
        "Z2": (40, 100),        # Easy runs
        "Z3": (80, 120),        # Tempo/Aerobic
        "Z4": (100, 150),       # Threshold
        "Z5": (120, 200),       # VO2max
        "Z6": (150, 300),       # Anaerobic
        "OFF": (0, 0),          # Rest
        "Recovery": (10, 30),
        "Easy": (40, 80),
        "Long": (100, 250),     # Long run (mostly Z2)
    }
    
    # Estimate TSS for each day
    daily_tss = []
    for workout in day_workouts:
        workout_lower = workout.lower()
        
        tss = 0
        if any(x in workout_lower for x in ["off", "rest"]):
            tss = 0
        elif any(x in workout_lower for x in ["recovery", "z1"]):
            tss = 20
        elif any(x in workout_lower for x in ["easy", "z2"]):
            tss = 60
        elif any(x in workout_lower for x in ["long"]):
            tss = 150  # Long runs are mostly aerobic
        elif any(x in workout_lower for x in ["tempo", "z3", "aerobic"]):
            tss = 100
        elif any(x in workout_lower for x in ["threshold", "lthr", "z4"]):
            tss = 120
        elif any(x in workout_lower for x in ["vo2", "z5", "max"]):
            tss = 160
        elif any(x in workout_lower for x in ["anaerobic", "sprint", "z6"]):
            tss = 200
        else:
            tss = 50  # Default to easy
        
        daily_tss.append(tss)
    
    total_tss = sum(daily_tss)
    
    if total_tss == 0:
        return IntensityDistribution(
            z1_percent=0, z2_percent=0, z3_percent=0, z4_percent=0, z5_percent=0, z6_percent=0,
            is_compliant=True,
            warnings=["No workouts in week"]
        )
    
    # Categorize each day's TSS into zones (simplified)
    z1_tss = sum(tss for i, tss in enumerate(daily_tss) if "recovery" in day_workouts[i].lower() or "z1" in day_workouts[i].lower())
    z2_tss = sum(tss for i, tss in enumerate(daily_tss) if any(x in day_workouts[i].lower() for x in ["easy", "z2", "long"]))
    hard_tss = total_tss - z1_tss - z2_tss  # Everything else is hard
    
    z2_percent = (z2_tss / total_tss * 100) if total_tss > 0 else 0
    hard_percent = (hard_tss / total_tss * 100) if total_tss > 0 else 0
    
    # Determine target based on phase and distance
    profile = DISTANCE_PROFILES.get(distance, DISTANCE_PROFILES["50K"])
    target_z2 = profile["z2_target"] * 100
    
    # Get profile target for this phase
    phase_structure = build_phase_structure(distance, age)
    if phase in phase_structure:
        target_z2 = phase_structure[phase].z2_target * 100
    
    # Check compliance (allow ±10% tolerance)
    tolerance = 10
    is_compliant = abs(z2_percent - target_z2) <= tolerance
    
    warnings = []
    if z2_percent < target_z2 - tolerance:
        warnings.append(f"⚠️ Too much hard work: {hard_percent:.0f}% hard (target: {100-target_z2:.0f}%)")
    elif z2_percent > target_z2 + tolerance:
        warnings.append(f"ℹ️ More easy work than target: {z2_percent:.0f}% (target: {target_z2:.0f}%)")
    
    # Age-based intensity check
    max_hard_days = get_intensity_cap(age)
    hard_workout_count = sum(1 for w in day_workouts if any(x in w.lower() for x in ["hard", "vo2", "threshold", "z4", "z5", "z6"]))
    
    if hard_workout_count > max_hard_days:
        warnings.append(f"🔴 Too many hard sessions: {hard_workout_count} (max for age {age}: {max_hard_days})")
        is_compliant = False
    
    return IntensityDistribution(
        z1_percent=(z1_tss / total_tss * 100) if total_tss > 0 else 0,
        z2_percent=z2_percent,
        z3_percent=0,  # Could break this down further
        z4_percent=0,
        z5_percent=0,
        z6_percent=0,
        is_compliant=is_compliant,
        warnings=warnings
    )