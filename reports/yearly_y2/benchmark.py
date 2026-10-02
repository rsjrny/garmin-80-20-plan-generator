"""Bounded full-season timing on an automatically removed synthetic database."""
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
import sqlite3

from garmin_data_hub.db.migrate import apply_schema
from garmin_data_hub.paths import schema_sql_path
from garmin_data_hub.services import season_plans as intent, season_generation as generation
from garmin_data_hub.services.season_schedule import GenerationSettings

with TemporaryDirectory(prefix="training-planner-y2-benchmark-") as temp:
    for method in ("eighty_twenty","maffetone"):
        db=Path(temp)/(method+".db")
        conn=sqlite3.connect(db)
        conn.execute("CREATE TABLE activity(activity_id INTEGER PRIMARY KEY,activity_type TEXT)")
        apply_schema(conn,schema_sql_path())
        conn.close()
        start=date(2099,1,1)
        season=intent.create_season(db,intent.SeasonDraft("Synthetic benchmark",start.isoformat(),(start+timedelta(days=365)).isoformat(),"America/New_York",
            intent.SeasonInputs(training_method=method,starting_duration_seconds=14400,lthr=170)))
        for i,(offset,code) in enumerate(((140,"HM"),(210,"5K"),(300,"MAR"))):
            intent.save_event(db,season.season_id,intent.EventDraft("Synthetic event "+str(i),(start+timedelta(days=offset)).isoformat(),code,"C" if code=="5K" else "A"),expected_version=i+1)
        settings=GenerationSettings(lthr_confirmed=method=="eighty_twenty",maf_adjustment=0 if method=="maffetone" else None,maf_confirmed=method=="maffetone")
        times=[]
        for _ in range(3):
            started=perf_counter()
            preview=generation.preview_season(db,season.season_id,settings)
            times.append(perf_counter()-started)
        assert preview.can_apply,preview.schedule.errors
        assert len(preview.schedule.candidate.workouts)==366
        started=perf_counter()
        result=generation.apply_season_preview(db,preview,approved_by="SYNTHETIC_BENCHMARK",acknowledge_warnings=True)
        elapsed=perf_counter()-started
        print(f"{method}: 366 days, 3 events, preview seconds min/mean/max={min(times):.3f}/{sum(times)/3:.3f}/{max(times):.3f}; atomic apply={elapsed:.3f}s; {result.workout_count} projected sessions")
print("Synthetic databases removed; no live schedules were changed.")
