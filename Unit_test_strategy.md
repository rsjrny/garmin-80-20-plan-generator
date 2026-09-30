# Unit Test Strategy

This project uses `pytest` with the test root configured in `pyproject.toml` as `tests/`.

## 1. Current Test Layout

Representative active regression coverage includes:

- `tests/conftest.py` — temporary SQLite fixture setup
- `tests/test_trackpoint_parser_wiring.py` and `tests/test_phase3m5a_identity_and_gpx.py` — archive identity and trackpoint ingestion
- `tests/test_metrics_refresh.py` and `tests/test_temporal_metrics.py` — derived metrics and elapsed-time semantics
- `tests/test_phase3d2_threshold_service.py` — automatic thresholds and provenance
- `tests/test_plan_persistence.py` — plan persistence behavior
- `tests/test_schema_migrations.py` — schema evolution checks
- `tests/test_sync_progress.py` and `tests/test_sync_job_lifecycle.py` — sync progress and lifecycle
- `tests/test_phase3e1_failure_contracts.py` — credential, privacy, and failure boundaries
- `tests/test_frozen_sync_packaging.py` and `tests/test_phase3h1_bundled_runtime_diagnostic.py` — frozen runtime packaging

## 2. What to Prioritize

- **Derived metrics logic** — refresh correctness, fallback behavior, and preservation of cached values
- **SQLite schema changes** — migrations must remain backward-compatible
- **Sync lifecycle** — progress heuristics and completion detection should survive UI changes
- **Plan persistence** — saved plans and settings must round-trip cleanly
- **CLI behavior** — public help/entry points, writable DB guards, trackpoint
  ingestion, internal argument rejection, and the safe bundled-runtime
  diagnostic should stay functional

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
python -m pytest tests/test_sync_progress.py tests/test_metrics_refresh.py tests/test_temporal_metrics.py
```

## 5. Coverage Goals

- Strong coverage on DB query helpers and refresh logic
- High confidence on schema migrations and persistence paths
- 100% regression coverage for previously reported production bugs

## 6. Rule of Thumb

If a bug reaches the UI or sync flow, add a focused regression test in `tests/` before shipping the fix.
