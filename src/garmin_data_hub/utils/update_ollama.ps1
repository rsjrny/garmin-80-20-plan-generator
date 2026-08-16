Write-Host "=== Ollama Model Update Script ===" -ForegroundColor Cyan

# Force Ollama path
$ollamaPath = "C:\Users\russl\AppData\Local\Programs\Ollama"
$env:Path = "$env:Path;$ollamaPath"

Write-Host "Using Ollama path: $ollamaPath" -ForegroundColor Yellow

# Check if ollama is available
Write-Host "Checking Ollama availability..." -ForegroundColor Yellow
$ollamaCheck = (Get-Command ollama -ErrorAction SilentlyContinue)

if (-not $ollamaCheck) {
    Write-Host "ERROR: 'ollama' not found even after updating PATH." -ForegroundColor Red
    Write-Host "Check if ollama.exe exists in: $ollamaPath" -ForegroundColor Red
    exit
}

Write-Host "Ollama found. Continuing..." -ForegroundColor Green

# Get installed models
Write-Host "`nFetching installed models..." -ForegroundColor Cyan
$raw = ollama list

Write-Host "`nRaw output from 'ollama list':" -ForegroundColor DarkGray
Write-Host $raw

$models = $raw | ForEach-Object {
    ($_ -split "\s+")[0]
}

if ($models.Count -eq 0) {
    Write-Host "`nNo models found. Exiting." -ForegroundColor Yellow
    exit
}

Write-Host "`nModels detected:" -ForegroundColor Cyan
$models | ForEach-Object { Write-Host " - $_" }

# Update each model
Write-Host "`nUpdating models..." -ForegroundColor Cyan

foreach ($model in $models) {
    Write-Host "`nPulling latest for: $model" -ForegroundColor Green
    ollama pull $model
}

# Cleanup
Write-Host "`nRunning prune..." -ForegroundColor Cyan
ollama prune

Write-Host "`nAll updates complete." -ForegroundColor Green
