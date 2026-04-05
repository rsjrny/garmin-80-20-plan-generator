from .workout_db import WORKOUTS

def forever_manifesto():
    return [
        "I train to remain capable, not to impress.",
        "Consistency is my superpower. Five modest days beat one heroic one.",
        "I protect my aerobic base like a retirement account.",
        "Intensity is a tool, not a habit.",
        "I stop workouts before they take something from me.",
        "I listen to early signals, not late warnings.",
        "Strength keeps me upright. Balance keeps me independent.",
        "I adjust for seasons without judgment.",
        "I train so age does not negotiate my freedom.",
        "I am training for decades, not dates.",
    ]

def workout_library(lthr: int | None):
    # Helper to format HR zones if LTHR is available
    def format_desc(desc: str) -> str:
        if not lthr:
            return desc
        
        # Simple replacements for generic zone labels with specific HR ranges
        # This is a basic implementation; could be more robust with regex if needed
        z1_cap = int(lthr * 0.81) # Top of Z1/Bottom of Z2 approx
        z2_cap = int(lthr * 0.89) # Top of Z2
        z4_low = int(lthr * 0.95) # Bottom of Z4
        z4_high = int(lthr * 1.05) # Top of Z4
        
        replacements = {
            "@ Z1": f"@ Z1 (<{z1_cap} bpm)",
            "@ Z2": f"@ Z2 ({z1_cap}-{z2_cap} bpm)",
            "@ Z3": f"@ Z3 ({z2_cap}-{z4_low} bpm)",
            "@ Z4": f"@ Z4 ({z4_low}-{z4_high} bpm)",
            "Z1 cap": f"Z1 cap (<{z1_cap} bpm)",
            "Z2 cap": f"Z2 cap (<{z2_cap} bpm)",
        }
        
        for key, val in replacements.items():
            desc = desc.replace(key, val)
        return desc

    # Build library from WORKOUTS database
    library = []
    
    # Define the order we want them to appear
    order = [
        "EASY_Z2", "EASY_LONG", "STRIDES", "RECOVERY",
        "HILL_REPEATS", "HILLY_RUN",
        "TEMPO_STEADY", "CRUISE_INTERVALS", "PROGRESSION",
        "LONG_TRAIL", "BACK_TO_BACK",
        "STRENGTH_A", "STRENGTH_B"
    ]
    
    for workout_id in order:
        if workout_id in WORKOUTS:
            w = WORKOUTS[workout_id]
            library.append((
                w.name,
                [
                    format_desc(w.description),
                    f"Category: {w.category}"
                ]
            ))
            
    return library

def nutrition_sections(sodium_hot: int | None, distance: str = "50K"):
    sodium_hot = sodium_hot or 900
    
    # Determine sweater type based on sodium input
    if sodium_hot < 700:
        sweater_type = "Light/Moderate sweater baseline."
    elif sodium_hot < 1100:
        sweater_type = "Heavy sweater baseline."
    else:
        sweater_type = "Very heavy/Salty sweater baseline."

    # Calculate Sodium Targets
    # Hot: User input +/- 100mg buffer
    # Cool: ~65% of hot needs (physiologically sound reduction for lower sweat rate)
    sod_hot_low = max(0, sodium_hot - 100)
    sod_hot_high = sodium_hot + 100
    
    sod_cool_target = int(sodium_hot * 0.65)
    sod_cool_low = max(0, sod_cool_target - 100)
    sod_cool_high = sod_cool_target + 100

    # Calculate Carb Targets based on distance/duration
    # Short/Fast (<2h): 30-60g
    # Ultra (>3h): 60-90g
    if distance in ["5K", "10K", "HM"]:
        carb_target = "30–50 g"
        carb_note = "(shorter duration)"
    elif distance == "MAR":
        carb_target = "50–70 g"
        carb_note = "(marathon intensity)"
    else: # Ultras (50K+)
        carb_target = "60–90 g"
        carb_note = "(ultra duration)"

    # Fluid Logic (correlated with sodium needs as a proxy for sweat rate)
    # High sodium often correlates with high sweat rate
    if sodium_hot > 1000:
        fluid_hot = "700–900 ml"
        fluid_cool = "500–700 ml"
    elif sodium_hot < 600:
        fluid_hot = "400–600 ml"
        fluid_cool = "300–500 ml"
    else:
        fluid_hot = "600–750 ml"
        fluid_cool = "400–600 ml"

    return [
        ("Baseline", f"{sweater_type} Hot/humid sodium target ~{sodium_hot} mg/hr (LMNT or equivalent)."),
        ("Hourly Targets", f"Hot: {fluid_hot} fluid · {sod_hot_low}–{sod_hot_high} mg sodium · {carb_target} carbs/hr {carb_note}. Cool: {fluid_cool} fluid · {sod_cool_low}–{sod_cool_high} mg sodium · carbs unchanged."),
        ("Salt Tabs", "Yes—salt tabs/capsules can substitute or top-up. Check your brand label; often ~200–250 mg sodium each. Spread intake evenly; always with water."),
        ("Fueling Rules", "Fuel by the clock (start 20–30 min in). Small and often. Don’t ‘catch up’. Separate sodium and calories if taste fatigue hits."),
        ("Warning Signs", "Cramping: sodium up. Slosh: pause fluid briefly. Brain fog: carbs now + sodium soon. Clear pee hourly: too much water / too little sodium."),
    ]
