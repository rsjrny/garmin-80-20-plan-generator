"""Help & About page navigation and release metadata checks."""

from __future__ import annotations

import re
from pathlib import Path

from nicegui import ui

from ui_test_support import run_ui

from garmin_data_hub import __version__
from garmin_data_hub.ui_nicegui import pages
from garmin_data_hub.ui_nicegui.layout import NAVIGATION
from garmin_data_hub.ui_nicegui.pages import ABOUT_LINKS, HELP_WORKFLOW_SECTIONS


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_help_about_replaces_guide_in_navigation_without_changing_its_route():
    help_entries = [entry for entry in NAVIGATION if entry[1] == "/guide"]

    assert help_entries == [("Help & About", "/guide", "help_outline")]
    assert len({route for _, route, _ in NAVIGATION}) == len(NAVIGATION)


def test_help_about_content_links_to_known_pages_and_support_resources():
    navigation_routes = {route for _, route, _ in NAVIGATION}
    workflow_routes = {route for *_, route in HELP_WORKFLOW_SECTIONS}
    support_links = dict(ABOUT_LINKS)

    assert [step for step, *_ in HELP_WORKFLOW_SECTIONS] == [
        str(number) for number in range(1, 8)
    ]
    assert workflow_routes <= navigation_routes
    assert [
        (title, route)
        for _, title, _, _, route in HELP_WORKFLOW_SECTIONS
    ] == [
        ("Set preferences", "/settings"),
        ("Sync Garmin", "/sync"),
        ("Review history", "/activities"),
        ("Configure the goal", "/plan"),
        ("Generate safely", "/coach"),
        ("Review and apply", "/coach"),
        ("Track compliance", "/compliance"),
    ]
    assert support_links == {
        "Project source": "https://github.com/rsjrny/garmin-80-20-plan-generator",
        "Report an issue": (
            "https://github.com/rsjrny/garmin-80-20-plan-generator/issues"
        ),
        "Releases": (
            "https://github.com/rsjrny/garmin-80-20-plan-generator/releases"
        ),
    }


def test_help_about_renders_version_workflow_and_support_links(ui_database):
    async def scenario(user):
        await user.open("/guide")
        await user.should_see(f"Version {__version__}")
        await user.should_see("Local by design")
        await user.should_see("Open-source & legal")
        for _, title, _, label, route in HELP_WORKFLOW_SECTIONS:
            await user.should_see(title)
            link = next(element for element in user.find(label).elements
                        if isinstance(element, ui.link) and element.text == label)
            assert link._props["href"] == route
        for label, target in ABOUT_LINKS:
            link = next(iter(user.find(label).elements))
            assert link._props["href"] == target
            assert link._props["target"] == "_blank"

    run_ui(ui_database, scenario)


def test_displayed_version_matches_project_and_release_build_metadata():
    pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    project_block = pyproject.split("[project]", maxsplit=1)[1].split(
        "[project.optional-dependencies]", maxsplit=1
    )[0]
    version_match = re.search(
        r'^version\s*=\s*"([^"]+)"\s*$',
        project_block,
        flags=re.MULTILINE,
    )
    build_script = (PROJECT_ROOT / "packaging" / "build.ps1").read_text(
        encoding="utf-8"
    )

    assert version_match is not None
    assert __version__ == version_match.group(1)
    assert (
        '$PackageInitPath = Join-Path $ProjectRoot '
        '"src\\garmin_data_hub\\__init__.py"'
    ) in build_script
    assert (
        "Update-PackageVersion -FilePath $PackageInitPath -NewVersion $Version"
        in build_script
    )
    project_update = (
        "Update-PyProjectVersion -FilePath $PyProjectPath -NewVersion $Version"
    )
    package_update = (
        "Update-PackageVersion -FilePath $PackageInitPath -NewVersion $Version"
    )
    assert (
        build_script.index(project_update)
        < build_script.index(package_update)
        < build_script.index("$pyinstallerArgsGui = @(")
    )
