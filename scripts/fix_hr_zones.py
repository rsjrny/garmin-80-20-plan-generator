"""
Standalone script to recalculate HR zones, TRIMP, TSS and other metrics for all activities.
Run this after importing data if metrics are missing.

Usage:
    python scripts/fix_hr_zones.py
    python scripts/fix_hr_zones.py --db path/to/garmin.sqlite3
    python scripts/fix_hr_zones.py --lthr 165  # Override LTHR
    python scripts/fix_hr_zones.py --ftp 250   # Set FTP for power metrics
    python scripts/fix_hr_zones.py --force-all # Recalculate ALL activities
"""
import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from garmin_data_hub.paths import default_db_path
from garmin_data_hub.ingest.writer import recalculate_missing_metrics, calculate_activity_metrics, _get_row_value


def main():
    parser = argparse.ArgumentParser(description="Recalculate HR zones, TRIMP, TSS and other metrics")
    parser.add_argument("--db", help="Path to SQLite database")
    parser.add_argument("--lthr", type=int, help="Override LTHR value (bpm)")
    parser.add_argument("--ftp", type=int, help="Set FTP value (watts) for power metrics")
    parser.add_argument("--force-all", action="store_true", help="Recalculate ALL activities")
    args = parser.parse_args()
    
    db_path = Path(args.db) if args.db else default_db_path()
    print(f"Database: {db_path}")
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 1
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Check current state
    print("\n=== Current State ===")
    
    # Count records with HR
    result = conn.execute("""
        SELECT COUNT(*) as total, 
               SUM(CASE WHEN heart_rate_bpm IS NOT NULL AND heart_rate_bpm > 0 THEN 1 ELSE 0 END) as with_hr,
               SUM(CASE WHEN power_w IS NOT NULL AND power_w > 0 THEN 1 ELSE 0 END) as with_power
        FROM record
    """).fetchone()
    print(f"Records: {result['total']:,} total, {result['with_hr']:,} with HR, {result['with_power']:,} with power")
    
    # Get max HR from sessions
    max_hr_row = conn.execute("SELECT MAX(max_hr_bpm) FROM session WHERE max_hr_bpm > 0").fetchone()
    max_hr = max_hr_row[0] if max_hr_row else None
    print(f"Max HR from sessions: {max_hr}")
    
    # Check athlete profile
    athlete = conn.execute("SELECT * FROM athlete_profile WHERE profile_id = 1").fetchone()
    if athlete:
        print(f"Athlete profile: HRmax_calc={athlete['hrmax_calc']}, LTHR_calc={athlete['lthr_calc']}, "
              f"HRmax_override={athlete['hrmax_override']}, LTHR_override={athlete['lthr_override']}")
        try:
            print(f"                FTP_calc={athlete['ftp_calc']}, FTP_override={athlete['ftp_override']}")
        except:
            pass
    else:
        print("Athlete profile: NOT FOUND")
    
    # Count activities with missing data
    missing = conn.execute("""
        SELECT COUNT(*) FROM activity a
        LEFT JOIN activity_metrics am ON a.activity_id = am.activity_id
        WHERE am.activity_id IS NULL OR am.zone_1_s IS NULL OR am.trimp IS NULL OR am.tss IS NULL
    """).fetchone()[0]
    total_activities = conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0]
    print(f"Activities: {total_activities} total, {missing} missing metrics")
    
    # Apply overrides if provided
    if args.lthr or args.ftp:
        print(f"\n=== Applying Overrides ===")
        updates = []
        params = []
        
        if args.lthr:
            updates.append("lthr_override = ?")
            params.append(args.lthr)
            print(f"Setting LTHR override: {args.lthr} bpm")
        
        if args.ftp:
            updates.append("ftp_override = ?")
            params.append(args.ftp)
            print(f"Setting FTP override: {args.ftp} W")
        
        updates.append("override_updated_utc = ?")
        params.append(datetime.utcnow().isoformat())
        
        # Ensure profile exists
        conn.execute("""
            INSERT INTO athlete_profile (profile_id) VALUES (1)
            ON CONFLICT(profile_id) DO NOTHING
        """)
        
        conn.execute(f"""
            UPDATE athlete_profile SET {', '.join(updates)} WHERE profile_id = 1
        """, params)
        conn.commit()
    
    # Run recalculation
    print(f"\n=== Recalculating Metrics ===")
    updated = recalculate_missing_metrics(conn, force_all=args.force_all)
    
    # Show results
    print(f"\n=== Results ===")
    print(f"Updated: {updated} activities")
    
    # Verify results
    sample = conn.execute("""
        SELECT a.activity_id, a.sport, 
               am.zone_1_s, am.zone_2_s, am.zone_3_s, am.zone_4_s, am.zone_5_s, 
               am.trimp, am.tss, am.aerobic_decoupling_pct
        FROM activity a
        JOIN activity_metrics am ON a.activity_id = am.activity_id
        WHERE am.zone_1_s IS NOT NULL
        ORDER BY a.start_time_utc DESC
        LIMIT 5
    """).fetchall()
    
    if sample:
        print("\nSample results (most recent):")
        for row in sample:
            z_total = (row['zone_1_s'] or 0) + (row['zone_2_s'] or 0) + (row['zone_3_s'] or 0) + (row['zone_4_s'] or 0) + (row['zone_5_s'] or 0)
            print(f"  Activity {row['activity_id']} ({row['sport']}): "
                  f"Zones={z_total/60:.0f}min, TRIMP={row['trimp']}, TSS={row['tss']}, "
                  f"Decoupling={row['aerobic_decoupling_pct']}%")
    
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
