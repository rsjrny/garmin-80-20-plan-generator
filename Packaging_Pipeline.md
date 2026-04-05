# Packaging Pipeline

## 1. Clean Build Environment
rm -rf build/ dist/ release/

## 2. Inject Version Metadata
- Read version from pyproject.toml
- Write version into:
  - src/<package>/__init__.py
  - VERSION file
  - build metadata

## 3. Build Executables
### Streamlit App
pyinstaller app.spec

### CLI Tool
pyinstaller cli.spec

## 4. Validate Outputs
- Ensure dist/ contains expected binaries
- Ensure version metadata matches pyproject.toml
- Ensure no missing dependencies

## 5. Release Folder
- Copy artifacts to release/<version>/
- Generate checksums
- Generate release notes
