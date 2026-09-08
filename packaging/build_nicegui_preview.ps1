param(
    [string]$OutputDirectory = ""
)

$ErrorActionPreference = "Stop"
$ScriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDirectory
$PythonExecutable = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$Launcher = Join-Path $ScriptDirectory "nicegui_launcher.py"

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path $ProjectRoot "build\nicegui_preview"
}

$resolvedProject = [System.IO.Path]::GetFullPath($ProjectRoot)
$resolvedOutput = [System.IO.Path]::GetFullPath($OutputDirectory)
if ($resolvedOutput -eq $resolvedProject) {
    throw "The packaging output must not be the project root."
}
if (-not (Test-Path -LiteralPath $PythonExecutable -PathType Leaf)) {
    throw "Project virtual-environment Python was not found: $PythonExecutable"
}
if (-not (Test-Path -LiteralPath $Launcher -PathType Leaf)) {
    throw "NiceGUI launcher was not found: $Launcher"
}

& $PythonExecutable -c "import nicegui, webview, PyInstaller"
if ($LASTEXITCODE -ne 0) {
    throw 'Install packaging dependencies first: .venv\Scripts\python.exe -m pip install -e . pyinstaller'
}

$distributionDirectory = Join-Path $resolvedOutput "dist"
$workDirectory = Join-Path $resolvedOutput "work"
$specDirectory = Join-Path $resolvedOutput "spec"

New-Item -ItemType Directory -Path $distributionDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $workDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $specDirectory -Force | Out-Null

$arguments = @(
    "--name", "GarminDataHubNiceGUI",
    "--onedir",
    "--windowed",
    "--distpath", $distributionDirectory,
    "--workpath", $workDirectory,
    "--specpath", $specDirectory,
    "--collect-all", "nicegui",
    "--collect-all", "webview",
    "--collect-all", "garmin_mcp",
    "--hidden-import", "webview.platforms.edgechromium",
    "--add-data", ((Join-Path $ProjectRoot "src\garmin_data_hub\db\schema.sql") + ";garmin_data_hub/db"),
    $Launcher
)

Push-Location $ProjectRoot
try {
    & $PythonExecutable -m PyInstaller @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "PyInstaller failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

Write-Host "NiceGUI application built at: $distributionDirectory" -ForegroundColor Green
Write-Host "The signed-in Codex CLI remains an external prerequisite." -ForegroundColor Cyan
