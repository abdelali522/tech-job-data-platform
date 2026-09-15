$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot "venv\Scripts\python.exe"
$logDirectory = Join-Path $projectRoot "storage\logs"
$logFile = Join-Path $logDirectory ("ingestion_{0}.log" -f (Get-Date -Format "yyyyMMdd"))

New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null

Push-Location $projectRoot
try {
    & $python "ingestion\api_ingestion.py" *>> $logFile
    if ($LASTEXITCODE -ne 0) {
        throw "Ingestion failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
