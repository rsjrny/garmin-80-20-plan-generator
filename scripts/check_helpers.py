import garmin_data_hub.db.queries as q

names = [
    "get_activities_dataframe",
    "get_hrmax_robust_and_lthr",
    "get_activity_records",
    "insert_fit_file_messages",
]
for n in names:
    print(n, hasattr(q, n))
