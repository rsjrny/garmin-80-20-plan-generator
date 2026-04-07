# Packaging Pipeline

This repository uses a **Windows/PowerShell** packaging flow driven by `packaging/build.ps1`.

## 1. Prerequisites

Before packaging, ensure:

- the project environment is installed (`pip install -e .[dev]`)
- `PyInstaller` is available in the active environment
- `garmin-givemydata` is installed and resolvable on `PATH`
- optional: `ISCC.exe` is installed if you want the Inno Setup installer built

## 2. Build Command

From the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\packaging\build.ps1 -Version 0.1.0
```

## 3. What `build.ps1` Does

1. Updates `pyproject.toml` with the requested version
2. Cleans and recreates `build/` and `release/<version>/`
3. Builds the Streamlit app into `build/streamlit_app/dist/GarminDataHub/`
4. Builds the sync CLI into `build/cli_tool/dist/cli_backup_ingest/`
5. Bundles `garmin-givemydata.exe` next to the CLI and GUI artifacts
6. Copies the final outputs to `release/<version>/`
7. Optionally builds the installer from `packaging/installer/GarminDataHub.iss`

## 4. Expected Release Outputs

After a successful run, verify:

- `release/<version>/GarminDataHub/GarminDataHub.exe`
- `release/<version>/cli_backup_ingest/cli_backup_ingest.exe`
- bundled `garmin-givemydata.exe` is present in the release folders

## 5. Validation Checklist

Before publishing a release, confirm:

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
