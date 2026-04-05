import sys

try:
    import garmin_data_hub.db as db

    q = getattr(db, "queries", None)
    print("HAS_QUERIES", q is not None)
    if q is not None:
        callables = [
            n for n in dir(q) if callable(getattr(q, n)) and not n.startswith("_")
        ]
        print("CALLABLES", callables)
    sys.exit(0)
except Exception as e:
    print("IMPORT_ERROR", repr(e))
    raise
