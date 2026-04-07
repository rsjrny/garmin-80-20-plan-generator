# Unit Test Strategy

This project uses `pytest` with the test root configured in `pyproject.toml` as `tests/`.

## 1. Current Test Layout

Active regression coverage currently lives in:

- `tests/conftest.py` — temporary SQLite fixture setup
- `tests/test_cli_trackpoints.py` — CLI / trackpoint ingest behavior
- `tests/test_metrics_refresh.py` — derived metrics, FTP/power zones, refresh regressions
- `tests/test_plan_persistence.py` — plan persistence behavior
- `tests/test_schema_migrations.py` — schema evolution checks
- `tests/test_sync_progress.py` — sync progress/completion heuristics for the Streamlit UI

## 2. What to Prioritize

- **Derived metrics logic** — refresh correctness, fallback behavior, and preservation of cached values
- **SQLite schema changes** — migrations must remain backward-compatible
- **Sync lifecycle** — progress heuristics and completion detection should survive UI changes
- **Plan persistence** — saved plans and settings must round-trip cleanly
- **CLI behavior** — help/entry points and trackpoint ingest should stay functional

## 3. Test Style

- Use `pytest`
- Follow **Arrange / Act / Assert**
- Prefer temp SQLite fixtures over heavy mocking
- Add regression tests for bugs before fixing them
- Mock only true external boundaries, not internal business logic

## 4. Recommended Commands

Run the full suite:

```powershell
python -m pytest
```

Run the most relevant sync/metrics regression checks:

```powershell
python -m pytest tests/test_sync_progress.py tests/test_metrics_refresh.py
```

## 5. Coverage Goals

- Strong coverage on DB query helpers and refresh logic
- High confidence on schema migrations and persistence paths
- 100% regression coverage for previously reported production bugs

## 6. Rule of Thumb

If a bug reaches the UI or sync flow, add a focused regression test in `tests/` before shipping the fix.
