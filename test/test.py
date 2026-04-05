import sqlite3
conn = sqlite3.connect(r"garmin.sqlite3")
conn.row_factory = sqlite3.Row

# Check metrics
rows = conn.execute("""
    SELECT a.activity_id, a.sport, am.zone_1_s, am.zone_2_s, am.zone_3_s, 
           am.zone_4_s, am.zone_5_s, am.trimp, am.tss
    FROM activity a
    LEFT JOIN activity_metrics am ON a.activity_id = am.activity_id
    LIMIT 10
""").fetchall()

for r in rows:
    print(f"Activity {r['activity_id']} ({r['sport']}): z1={r['zone_1_s']}, trimp={r['trimp']}, tss={r['tss']}")

conn.close()