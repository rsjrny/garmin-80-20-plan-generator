"""Add new columns to activity_metrics table."""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from garmin_data_hub.paths import default_db_path

def main():
    db_path = default_db_path()
    print(f"Database: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    
    # Check existing columns
    cursor = conn.execute("PRAGMA table_info(activity_metrics)")
    existing = {row[1] for row in cursor.fetchall()}
    print(f"Existing columns: {len(existing)}")
    
    # New columns to add
    new_columns = [
        ("hr_drift_pct", "REAL"),
        ("hr_recovery_60s_bpm", "REAL"),
        ("avg_hr_to_max_pct", "REAL"),
        ("variability_index", "REAL"),
        ("avg_power_w", "REAL"),
        ("max_power_w", "REAL"),
        ("peak_power_5s_w", "REAL"),
        ("peak_power_30s_w", "REAL"),
        ("peak_power_60s_w", "REAL"),
        ("peak_power_300s_w", "REAL"),
        ("peak_power_1200s_w", "REAL"),
        ("power_zone_1_s", "REAL"),
        ("power_zone_2_s", "REAL"),
        ("power_zone_3_s", "REAL"),
        ("power_zone_4_s", "REAL"),
        ("power_zone_5_s", "REAL"),
        ("power_zone_6_s", "REAL"),
        ("power_zone_7_s", "REAL"),
        ("efficiency_factor", "REAL"),
        ("pace_decoupling_pct", "REAL"),
        ("avg_cadence_spm", "REAL"),
        ("avg_stride_length_m", "REAL"),
        ("avg_vertical_osc_cm", "REAL"),
        ("avg_ground_contact_ms", "REAL"),
        ("avg_vertical_ratio", "REAL"),
        ("gct_balance_avg_pct", "REAL"),
        ("avg_temperature_c", "REAL"),
        ("min_temperature_c", "REAL"),
        ("max_temperature_c", "REAL"),
        ("total_ascent_m", "REAL"),
        ("total_descent_m", "REAL"),
        ("max_altitude_m", "REAL"),
        ("min_altitude_m", "REAL"),
        ("training_effect_aerobic", "REAL"),
        ("training_effect_anaerobic", "REAL"),
        ("performance_condition_start", "REAL"),
        ("performance_condition_end", "REAL"),
    ]
    
    added = 0
    for col_name, col_type in new_columns:
        if col_name not in existing:
            print(f"Adding column: {col_name}")
            conn.execute(f"ALTER TABLE activity_metrics ADD COLUMN {col_name} {col_type}")
            added += 1
    
    conn.commit()
    conn.close()
    print(f"Done! Added {added} columns.")

if __name__ == "__main__":
    main()
