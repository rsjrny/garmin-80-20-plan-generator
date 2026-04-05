"""Debug script to test metrics calculation on a single activity."""
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from garmin_data_hub.paths import default_db_path

def main():
    db_path = default_db_path()
    print(f"Database: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Get first activity with HR data
    activity = conn.execute("""
        SELECT a.activity_id, a.sport, s.avg_hr_bpm, s.max_hr_bpm, s.total_timer_s
        FROM activity a
        JOIN session s ON a.activity_id = s.activity_id
        WHERE s.avg_hr_bpm IS NOT NULL AND s.avg_hr_bpm > 0
        ORDER BY a.activity_id
        LIMIT 1
    """).fetchone()
    
    if not activity:
        print("No activities with HR data found!")
        return 1
    
    activity_id = activity['activity_id']
    print(f"\n=== Testing Activity {activity_id} ({activity['sport']}) ===")
    print(f"Avg HR: {activity['avg_hr_bpm']}, Max HR: {activity['max_hr_bpm']}, Duration: {activity['total_timer_s']}s")
    
    # Check record count with HR
    hr_count = conn.execute("""
        SELECT COUNT(*) FROM record r
        JOIN session s ON r.session_id = s.session_id
        WHERE s.activity_id = ? AND r.heart_rate_bpm IS NOT NULL AND r.heart_rate_bpm > 0
    """, (activity_id,)).fetchone()[0]
    print(f"Records with HR: {hr_count}")
    
    # Get sample HR records
    print("\n=== Sample HR Records ===")
    records = conn.execute("""
        SELECT r.timestamp_utc, r.heart_rate_bpm 
        FROM record r
        JOIN session s ON r.session_id = s.session_id
        WHERE s.activity_id = ? AND r.heart_rate_bpm IS NOT NULL AND r.heart_rate_bpm > 0
        ORDER BY r.timestamp_utc
        LIMIT 10
    """, (activity_id,)).fetchall()
    
    for r in records:
        print(f"  {r['timestamp_utc']}: {r['heart_rate_bpm']} bpm")
    
    # Get LTHR
    athlete = conn.execute("SELECT lthr_calc FROM athlete_profile WHERE profile_id = 1").fetchone()
    lthr = athlete['lthr_calc'] if athlete else None
    print(f"\n=== Using LTHR: {lthr} ===")
    
    if not lthr:
        print("No LTHR available!")
        return 1
    
    # Test zone calculation manually
    print("\n=== Testing Zone Calculation ===")
    max_hr = int(lthr / 0.86)
    print(f"Estimated Max HR: {max_hr}")
    
    zone_thresholds = [
        (0, max_hr * 0.60),
        (max_hr * 0.60, max_hr * 0.70),
        (max_hr * 0.70, max_hr * 0.80),
        (max_hr * 0.80, max_hr * 0.90),
        (max_hr * 0.90, 250)
    ]
    print(f"Zone thresholds: {zone_thresholds}")
    
    # Get all HR records with timestamps
    all_records = conn.execute("""
        SELECT r.timestamp_utc, r.heart_rate_bpm 
        FROM record r
        JOIN session s ON r.session_id = s.session_id
        WHERE s.activity_id = ? AND r.heart_rate_bpm IS NOT NULL AND r.heart_rate_bpm > 0
        ORDER BY r.timestamp_utc
    """, (activity_id,)).fetchall()
    
    print(f"Total HR records: {len(all_records)}")
    
    if len(all_records) < 2:
        print("Not enough records!")
        return 1
    
    # Calculate zones
    zone_times = {f'zone_{i}_s': 0.0 for i in range(1, 6)}
    valid_intervals = 0
    skipped_intervals = 0
    
    for i in range(1, len(all_records)):
        prev_ts_str = all_records[i-1]['timestamp_utc']
        curr_ts_str = all_records[i]['timestamp_utc']
        hr = all_records[i]['heart_rate_bpm']
        
        try:
            prev_ts = datetime.fromisoformat(prev_ts_str)
            curr_ts = datetime.fromisoformat(curr_ts_str)
            duration = (curr_ts - prev_ts).total_seconds()
            
            if duration <= 0 or duration > 30:
                skipped_intervals += 1
                continue
            
            valid_intervals += 1
            for zone_idx, (lower, upper) in enumerate(zone_thresholds, start=1):
                if lower <= hr < upper:
                    zone_times[f'zone_{zone_idx}_s'] += duration
                    break
        except Exception as e:
            print(f"Error at index {i}: {e}")
            skipped_intervals += 1
    
    print(f"\nValid intervals: {valid_intervals}, Skipped: {skipped_intervals}")
    print(f"\nZone times:")
    for zone, time in zone_times.items():
        print(f"  {zone}: {time:.1f}s ({time/60:.1f} min)")
    
    total = sum(zone_times.values())
    print(f"\nTotal zone time: {total:.1f}s ({total/60:.1f} min)")
    
    # Now run the actual calculation
    print("\n=== Running calculate_activity_metrics ===")
    from garmin_data_hub.ingest.writer import calculate_activity_metrics
    
    try:
        calculate_activity_metrics(conn, activity_id, hr_zone_data=None)
        conn.commit()
        print("Calculation completed!")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Check result
    result = conn.execute("""
        SELECT zone_1_s, zone_2_s, zone_3_s, zone_4_s, zone_5_s, trimp, tss
        FROM activity_metrics WHERE activity_id = ?
    """, (activity_id,)).fetchone()
    
    if result:
        print(f"\n=== Result ===")
        print(f"Zone 1: {result['zone_1_s']}")
        print(f"Zone 2: {result['zone_2_s']}")
        print(f"Zone 3: {result['zone_3_s']}")
        print(f"Zone 4: {result['zone_4_s']}")
        print(f"Zone 5: {result['zone_5_s']}")
        print(f"TRIMP: {result['trimp']}")
        print(f"TSS: {result['tss']}")
    else:
        print("No result in activity_metrics!")
    
    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
