param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
)

$ErrorActionPreference = 'Stop'

$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$script = Join-Path $ProjectRoot 'scripts\sync_official_contracts.py'
$logDir = Join-Path $ProjectRoot 'logs'
$logFile = Join-Path $logDir 'sync_official_contracts.log'

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
"[$timestamp] Starting official contracts sync" | Out-File -FilePath $logFile -Append -Encoding utf8

if (-not (Test-Path $python)) {
    throw "Python executable not found: $python"
}

if (-not (Test-Path $script)) {
    throw "Sync script not found: $script"
}

& $python $script *>> $logFile

$exitCode = $LASTEXITCODE
$timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
"[$timestamp] Finished official contracts sync with exit code $exitCode" | Out-File -FilePath $logFile -Append -Encoding utf8

exit $exitCode
