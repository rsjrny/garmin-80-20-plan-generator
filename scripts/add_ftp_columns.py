"""Add FTP and resting_hr columns to athlete_profile table."""
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
    cursor = conn.execute("PRAGMA table_info(athlete_profile)")
    existing = {row[1] for row in cursor.fetchall()}
    print(f"Existing columns: {existing}")
    
    # Add missing columns
    new_columns = [
        ("ftp_calc", "INTEGER"),
        ("ftp_override", "INTEGER"),
        ("resting_hr", "INTEGER"),
    ]
    
    for col_name, col_type in new_columns:
        if col_name not in existing:
            print(f"Adding column: {col_name}")
            conn.execute(f"ALTER TABLE athlete_profile ADD COLUMN {col_name} {col_type}")
        else:
            print(f"Column already exists: {col_name}")
    
    conn.commit()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
