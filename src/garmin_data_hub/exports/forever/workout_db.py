from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Workout:
    id: str
    name: str
    description: str
    category: str  # "Recovery", "Aerobic", "Threshold", "VO2max", "Long", "Strength"
    
    # HR Zone Time Data (in seconds) - populated from actual activity records
    time_in_zone_1_s: Optional[float] = None  # Zone 1: Recovery (< 50% of LTHR)
    time_in_zone_2_s: Optional[float] = None  # Zone 2: Aerobic (50-70% of LTHR)
    time_in_zone_3_s: Optional[float] = None  # Zone 3: Tempo (70-85% of LTHR)
    time_in_zone_4_s: Optional[float] = None  # Zone 4: Threshold (85-100% of LTHR)
    time_in_zone_5_s: Optional[float] = None  # Zone 5: VO2max (> 100% of LTHR)
    
    # Convenience totals
    total_time_s: Optional[float] = None      # Total time with HR data
    avg_hr_bpm: Optional[float] = None        # Average HR during workout
    max_hr_bpm: Optional[int] = None          # Maximum HR during workout

# A database of simple, proven workouts for endurance runners
WORKOUTS = {
    # --- Rest & Recovery ---
    "OFF": Workout(
        id="OFF",
        name="Rest Day",
        description="Complete rest. Focus on sleep and nutrition.",
        category="Recovery"
    ),
    "RECOVERY": Workout(
        id="RECOVERY",
        name="Recovery Run",
        description="30–45 min @ Z1. Very easy effort to promote blood flow.",
        category="Recovery"
    ),

    # --- Aerobic Base ---
    "EASY_Z2": Workout(
        id="EASY_Z2",
        name="Easy Run",
        description="45–60 min @ Z2. Conversational pace.",
        category="Aerobic"
    ),
    "EASY_LONG": Workout(
        id="EASY_LONG",
        name="Aerobic Endurance",
        description="60–75 min @ Z2. Building durability.",
        category="Aerobic"
    ),
    "STRIDES": Workout(
        id="STRIDES",
        name="Easy + Strides",
        description="30–40 min easy + 4–6 x 20s strides (fast but relaxed) w/ full recovery.",
        category="Aerobic"
    ),

    # --- Threshold / Tempo (Build Phase) ---
    "TEMPO_STEADY": Workout(
        id="TEMPO_STEADY",
        name="Steady State Tempo",
        description="WU 15m; 20–30 min @ Z3 (Marathon Pace effort); CD 10m.",
        category="Threshold"
    ),
    "CRUISE_INTERVALS": Workout(
        id="CRUISE_INTERVALS",
        name="Cruise Intervals",
        description="WU 15m; 3–4 x 8 min @ Z4 (Threshold) w/ 2 min jog rest; CD 10m.",
        category="Threshold"
    ),
    "PROGRESSION": Workout(
        id="PROGRESSION",
        name="Progression Run",
        description="45 min run: First 30 min easy, last 15 min build to moderate-hard.",
        category="Threshold"
    ),

    # --- Hills / Strength (Base Phase) ---
    "HILL_REPEATS": Workout(
        id="HILL_REPEATS",
        name="Hill Repeats",
        description="WU 20m; 6–8 x 60s uphill (strong effort); walk down recovery; CD 10m.",
        category="Strength"
    ),
    "HILLY_RUN": Workout(
        id="HILLY_RUN",
        name="Hilly Aerobic Run",
        description="45–60 min @ Z2 over rolling terrain. Power hike the steeps.",
        category="Strength"
    ),

    # --- Long Runs ---
    "LONG_TRAIL": Workout(
        id="LONG_TRAIL",
        name="Long Trail Run",
        description="Duration varies (2h–5h). Z2 cap. Practice fueling and hiking.",
        category="Long"
    ),
    "BACK_TO_BACK": Workout(
        id="BACK_TO_BACK",
        name="Back-to-Back Run",
        description="Run on tired legs. 90–120 min @ Z1/Z2.",
        category="Long"
    ),

    # --- Strength Training ---
    "STRENGTH_A": Workout(
        id="STRENGTH_A",
        name="Strength A (Legs/Core)",
        description="Split squats, RDLs, Calf raises, Planks. 30–40 min.",
        category="Strength"
    ),
    "STRENGTH_B": Workout(
        id="STRENGTH_B",
        name="Strength B (Hips/Stability)",
        description="Step-ups, Band walks, Single-leg balance, Pallof press. 30–40 min.",
        category="Strength"
    ),
}


def calculate_time_in_zones(heart_rates: list[int], lthr_bpm: int) -> dict[str, float]:
    """
    Calculate time spent in each HR zone based on recorded heart rates and athlete's LTHR.
    
    HR Zones based on LTHR (Lactate Threshold Heart Rate):
    - Zone 1: < 50% LTHR (Recovery)
    - Zone 2: 50-70% LTHR (Aerobic Base)
    - Zone 3: 70-85% LTHR (Tempo)
    - Zone 4: 85-100% LTHR (Threshold)
    - Zone 5: > 100% LTHR (VO2max)
    
    Args:
        heart_rates: List of HR values in bpm (typically 1 per second)
        lthr_bpm: Athlete's Lactate Threshold Heart Rate
        
    Returns:
        Dictionary with time in each zone (in seconds) and totals
    """
    if not heart_rates or lthr_bpm <= 0:
        return {
            'zone_1_s': 0.0,
            'zone_2_s': 0.0,
            'zone_3_s': 0.0,
            'zone_4_s': 0.0,
            'zone_5_s': 0.0,
            'total_s': 0.0,
            'avg_hr_bpm': 0.0,
            'max_hr_bpm': 0,
        }
    
    zone_1_s = 0.0
    zone_2_s = 0.0
    zone_3_s = 0.0
    zone_4_s = 0.0
    zone_5_s = 0.0
    
    valid_hrs = [hr for hr in heart_rates if hr and hr > 0]
    
    if not valid_hrs:
        return {
            'zone_1_s': 0.0,
            'zone_2_s': 0.0,
            'zone_3_s': 0.0,
            'zone_4_s': 0.0,
            'zone_5_s': 0.0,
            'total_s': 0.0,
            'avg_hr_bpm': 0.0,
            'max_hr_bpm': 0,
        }
    
    z1_threshold = lthr_bpm * 0.50
    z2_threshold = lthr_bpm * 0.70
    z3_threshold = lthr_bpm * 0.85
    z4_threshold = lthr_bpm * 1.00
    
    for hr in valid_hrs:
        if hr < z1_threshold:
            zone_1_s += 1
        elif hr < z2_threshold:
            zone_2_s += 1
        elif hr < z3_threshold:
            zone_3_s += 1
        elif hr < z4_threshold:
            zone_4_s += 1
        else:
            zone_5_s += 1
    
    total_time_s = zone_1_s + zone_2_s + zone_3_s + zone_4_s + zone_5_s
    avg_hr = sum(valid_hrs) / len(valid_hrs)
    max_hr = max(valid_hrs)
    
    return {
        'zone_1_s': zone_1_s,
        'zone_2_s': zone_2_s,
        'zone_3_s': zone_3_s,
        'zone_4_s': zone_4_s,
        'zone_5_s': zone_5_s,
        'total_s': total_time_s,
        'avg_hr_bpm': round(avg_hr, 1),
        'max_hr_bpm': int(max_hr),
    }
