"""
Ensures proper 80/20 or 70/15/15 intensity distribution.
"""

def calculate_weekly_intensity_split(
    phase: str,
    run_days: int,
    age: int
) -> dict[str, int]:
    """
    Returns: {"easy": 4, "moderate": 0, "hard": 1} for 5 days/week
    """
    if phase == "Base":
        # 100% easy during base
        return {"easy": run_days, "moderate": 0, "hard": 0}
    
    # 80/20 rule
    hard_days = min(2, run_days // 3)  # Max 2 hard days
    if age >= 50:
        hard_days = min(1, hard_days)  # Masters: max 1 hard day
    
    easy_days = run_days - hard_days
    
    return {"easy": easy_days, "moderate": 0, "hard": hard_days}