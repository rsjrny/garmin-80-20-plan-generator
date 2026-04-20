# Packaging Pipeline

This repository uses a **Windows/PowerShell** packaging flow driven by `packaging/build.ps1`.

## 1. Prerequisites

Before packaging, ensure:

- the project environment is installed (`pip install -e .[dev]`)
- `PyInstaller` is available in the active environment
- project virtual environment exists at `.venv` (build uses `.venv\\Scripts\\python.exe`)
- `garmin-givemydata` is installable from your selected source mode (`pypi` by default, or local repo)
- optional: `ISCC.exe` is installed if you want the Inno Setup installer built

## 2. Build Command

From the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0
```

By default, builds now run in `pypi` mode.

Common variants:

```powershell
# Default (PyPI)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0

# Local repo mode (for pre-release / PR validation)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0 -GivemydataSource local

# PyPI mode with explicit version pin
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0 -GivemydataSource pypi -GivemydataPypiSpec "garmin-givemydata==0.1.10"
```

## 2.1 Build Modes Quick Reference

Use these ready-to-run commands for current release `0.2.3`:

```powershell
# Default (PyPI mode)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.2.3

# Local mode (use local ../garmin-givemydata repo)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.2.3 -GivemydataSource local

# PyPI mode with explicit pin
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.2.3 -GivemydataSource pypi -GivemydataPypiSpec "garmin-givemydata==0.1.10"

# PyPI mode, no auto-update (validate-only)
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.2.3 -GivemydataSource pypi -AutoUpdateGivemydata:$false
```

## 3. What `build.ps1` Does

1. Updates `pyproject.toml` with the requested version
2. Prints a garmin-givemydata mode banner (`LOCAL` or `PYPI`) at startup
3. Resolves and validates `garmin-givemydata` in `.venv` based on selected source mode:
	- `pypi` (default): installs/upgrades from `-GivemydataPypiSpec`
	- `local`: validates against sibling repo `../garmin-givemydata` and optionally auto-refreshes
2. Cleans and recreates `build/` and `release/<version>/`
3. Builds the Streamlit app into `build/streamlit_app/dist/GarminDataHub/`
4. Builds the sync CLI into `build/cli_tool/dist/cli_backup_ingest/`
5. Bundles `garmin-givemydata.exe` from `.venv\Scripts` next to the CLI and GUI artifacts
6. Copies the final outputs to `release/<version>/`
7. Optionally builds the installer from `packaging/installer/GarminDataHub.iss`

## 4. Expected Release Outputs

After a successful run, verify:

- `release/<version>/GarminDataHub/GarminDataHub.exe`
- `release/<version>/cli_backup_ingest/cli_backup_ingest.exe`
- bundled `garmin-givemydata.exe` is present in the release folders

## 5. Validation Checklist

Before publishing a release, confirm:

- startup banner shows expected source mode (`PYPI` or `LOCAL`)
- the app launches successfully from the packaged GUI build
- the CLI help works: `cli_backup_ingest.exe --help`
- the sync flow can open the browser and finish a run
- `src/garmin_data_hub/db/schema.sql` is included in the packaged artifacts
- version numbers match the intended release

## 6. Known Gotchas

- `pyproject.toml` must be written back as **UTF-8 without BOM** or `pytest`/packaging tools may fail to parse it correctly
- `schema.sql` must be included via PyInstaller `--add-data` or the packaged app will fail to initialize the DB schema
- the packaging flow is currently **Windows-first**; the examples here are not intended for Unix shell usage

## 7. Related Files

- `packaging/build.ps1`
- `packaging/launcher.py`
- `packaging/installer/GarminDataHub.iss`
- `pyproject.toml`
