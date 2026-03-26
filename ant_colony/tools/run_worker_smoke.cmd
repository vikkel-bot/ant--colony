@echo off
setlocal

set "ROOT=%~dp0..\.."
set "PY=%ROOT%\.venv\Scripts\python.exe"

if not exist "%PY%" (
    echo ERROR: .venv python not found
    echo Expected: %PY%
    exit /b 1
)

if "%~1"=="" (
    echo Usage:
    echo run_worker_smoke.cmd ant_colony\workers\BTC-EUR\smoke_market_data_interface.py
    exit /b 1
)

echo ==========================================
echo ANT COLONY WORKER SMOKE RUNNER
echo ROOT   = %ROOT%
echo PY     = %PY%
echo SCRIPT = %1
echo ==========================================

"%PY%" "%ROOT%\%1"
exit /b %ERRORLEVEL%