#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
VERSION_FILE = ROOT / "VERSION"

# Adjust this to your package name
PACKAGE_INIT = ROOT / "src" / "<your_package>" / "__init__.py"


def read_version():
    if VERSION_FILE.exists():
        return VERSION_FILE.read_text().strip()
    raise FileNotFoundError("VERSION file not found")


def write_version(new_version):
    VERSION_FILE.write_text(new_version + "\n")


def bump(version, part):
    major, minor, patch = map(int, version.split("."))

    if part == "major":
        major += 1
        minor = 0
        patch = 0
    elif part == "minor":
        minor += 1
        patch = 0
    elif part == "patch":
        patch += 1
    else:
        raise ValueError("part must be major, minor, or patch")

    return f"{major}.{minor}.{patch}"


def update_pyproject(new_version):
    text = PYPROJECT.read_text()
    updated = re.sub(
        r'version\s*=\s*"[0-9]+\.[0-9]+\.[0-9]+"', f'version = "{new_version}"', text
    )
    PYPROJECT.write_text(updated)


def update_init(new_version):
    if not PACKAGE_INIT.exists():
        print(f"Warning: {PACKAGE_INIT} not found, skipping")
        return

    text = PACKAGE_INIT.read_text()
    updated = re.sub(
        r'__version__\s*=\s*"[0-9]+\.[0-9]+\.[0-9]+"',
        f'__version__ = "{new_version}"',
        text,
    )
    PACKAGE_INIT.write_text(updated)


def main():
    if len(sys.argv) != 2:
        print("Usage: bump_version.py [major|minor|patch]")
        sys.exit(1)

    part = sys.argv[1]
    old_version = read_version()
    new_version = bump(old_version, part)

    print(f"Bumping version: {old_version} → {new_version}")

    write_version(new_version)
    update_pyproject(new_version)
    update_init(new_version)

    print("Version updated successfully.")


if __name__ == "__main__":
    main()
