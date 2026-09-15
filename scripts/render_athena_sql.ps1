$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$templateDirectory = Join-Path $projectRoot "aws\athena"
$outputDirectory = Join-Path $projectRoot "build\athena"

if (-not (Test-Path $envFile)) {
    throw ".env file not found at $envFile"
}

$envValues = @{}
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
        return
    }

    $name, $value = $line.Split("=", 2)
    $envValues[$name.Trim()] = $value.Trim().Trim('"').Trim("'")
}

if (-not $envValues.ContainsKey("S3_BUCKET") -or -not $envValues["S3_BUCKET"]) {
    throw "S3_BUCKET must be set in .env"
}

New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

Get-ChildItem -Path $templateDirectory -Filter "*.sql" | ForEach-Object {
    $content = Get-Content -Raw -Path $_.FullName
    $content = $content.Replace('${S3_BUCKET}', $envValues["S3_BUCKET"])

    $outputPath = Join-Path $outputDirectory $_.Name
    Set-Content -Path $outputPath -Value $content -Encoding UTF8
    Write-Host "Rendered $outputPath"
}
