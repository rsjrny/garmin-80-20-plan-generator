from __future__ import annotations
from pathlib import Path

from garmin_data_hub.exports.forever.models import AthleteProfile, EventProfile, Inputs, AnalysisSummary
from garmin_data_hub.exports.forever.calendar_builder import build_calendar
from garmin_data_hub.exports.forever.metrics_builder import build_weekly_metrics
from garmin_data_hub.exports.forever.garmin_ingest import analyze_garmin
from garmin_data_hub.exports.forever.excel_writer import write_master_workbook

def generate_plan_data(
    athlete_name: str,
    age: int,
    lthr: int | None,
    hrmax: int | None,
    sodium_mg_per_hr_hot: int | None,
    event_name: str,
    distance: str,
    start_date_iso: str,
    event_date_iso: str,
    run_days_per_week: int = 5,
    long_run_day: str = "Saturday",
    garmin_files: list[Path] | None = None,
    out_dir: Path | None = None,
):
    """Generate training plan data."""
    
    # Build athlete profile
    athlete = AthleteProfile(
        athlete_name=athlete_name,
        age=age,
        sport="Running",
        hrmax=hrmax,
        lthr=lthr,
        sodium_mg_per_hr_hot=sodium_mg_per_hr_hot,
        notes=""
    )
    
    # Build event profile
    event = EventProfile(
        event_name=event_name,
        distance=distance,
        event_date=event_date_iso,
        start_date=start_date_iso,
        run_days_per_week=run_days_per_week
    )
    
    # Build inputs
    inputs = Inputs(
        athlete=athlete,
        event=event,
        garmin_files=garmin_files or [],
        output_dir=out_dir or Path.cwd(),
        use_llm=False,
        openai_api_key=None
    )
    
    # Generate calendar (returns DayPlan objects)
    day_plans = build_calendar(
        start_iso=start_date_iso,
        end_iso=event_date_iso,
        race_iso=event_date_iso,
        run_days_per_week=run_days_per_week,
        long_run_day=long_run_day,
        age=age,
        race_distance=distance
    )
    
    # Build weekly metrics (uses DayPlan objects)
    weekly_metrics = build_weekly_metrics(day_plans)
    
    # Analyze Garmin data
    if garmin_files:
        analysis = analyze_garmin(garmin_files)
    else:
        analysis = AnalysisSummary(
            hrmax_observed=None,
            hrmax_robust=None,
            lthr_suggested=None,
            active_weeks=None,
            avg_weekly_hours=None,
            avg_weekly_miles=None,
            z2_fraction=None,
            notes="No Garmin data provided",
            raw={}
        )
    
    # Return DayPlan objects (not dicts!) for Excel writer
    return inputs, analysis, day_plans, weekly_metrics


def generate_master_workbook(
    athlete_name: str,
    age: int,
    lthr: int | None,
    hrmax: int | None,
    sodium_mg_per_hr_hot: int | None,
    event_name: str,
    distance: str,
    start_date_iso: str,
    event_date_iso: str,
    run_days_per_week: int = 5,
    long_run_day: str = "Saturday",
    garmin_files: list[Path] | None = None,
    out_path: Path | None = None,
) -> Path:
    """Generate Excel workbook."""
    
    inputs, analysis, day_plans, weekly_metrics = generate_plan_data(
        athlete_name=athlete_name,
        age=age,
        lthr=lthr,
        hrmax=hrmax,
        sodium_mg_per_hr_hot=sodium_mg_per_hr_hot,
        event_name=event_name,
        distance=distance,
        start_date_iso=start_date_iso,
        event_date_iso=event_date_iso,
        run_days_per_week=run_days_per_week,
        long_run_day=long_run_day,
        garmin_files=garmin_files,
        out_dir=out_path.parent if out_path else None
    )
    
    # Call excel_writer with correct parameter order and names
    output_path = write_master_workbook(
        out_path=out_path,          # ✅ First parameter
        inputs=inputs,               # ✅ Second parameter
        analysis=analysis,           # ✅ Third parameter (not 'summary')
        day_plans=day_plans,         # ✅ DayPlan objects (not dicts)
        weekly_rows=weekly_metrics,  # ✅ Fifth parameter
        narrative=None               # ✅ Optional sixth parameter
    )
    
    return output_path
