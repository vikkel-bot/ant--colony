$ErrorActionPreference = "Stop"

$ROOT = "C:\Users\vikke\OneDrive\bitvavo-bot_clean"
$PY = Join-Path $ROOT ".venv\Scripts\python.exe"
$SCRIPT = Join-Path $ROOT "ant_colony\run_colony_execution_cycle_lite.py"

$STDOUT_LOG = "C:\Trading\ANT_OUT\execution_cycle_runner_lite.stdout.log"
$STDERR_LOG = "C:\Trading\ANT_OUT\execution_cycle_runner_lite.stderr.log"

Write-Host "RUNNING_COLONY_EXECUTION_CYCLE"
Write-Host "ROOT=$ROOT"
Write-Host "PY=$PY"
Write-Host "SCRIPT=$SCRIPT"
Write-Host "STDOUT_LOG=$STDOUT_LOG"
Write-Host "STDERR_LOG=$STDERR_LOG"
Write-Host ""

if (!(Test-Path $PY)) {
    Write-Error "Python executable not found: $PY"
}

if (!(Test-Path $SCRIPT)) {
    Write-Error "Runner script not found: $SCRIPT"
}

Push-Location $ROOT
try {
    & $PY $SCRIPT 1> $STDOUT_LOG 2> $STDERR_LOG
    $exitCode = $LASTEXITCODE

    if (Test-Path $STDOUT_LOG) {
        Get-Content $STDOUT_LOG
    }

    if ($exitCode -ne 0 -and (Test-Path $STDERR_LOG)) {
        Get-Content $STDERR_LOG
    }
}
finally {
    Pop-Location
}

exit $exitCode
