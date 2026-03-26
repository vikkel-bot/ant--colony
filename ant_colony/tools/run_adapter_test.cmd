@echo off
setlocal

REM ==========================================
REM Ant Colony Adapter Test Runner
REM Forces use of project .venv Python
REM ==========================================

set "ROOT=%~dp0..\.."
set "PY=%ROOT%\.venv\Scripts\python.exe"

if not exist "%PY%" (
    echo ERROR: .venv python not found
    echo Expected: %PY%
    exit /b 1
)

if "%~1"=="" (
    echo Usage:
    echo run_adapter_test.cmd ant_colony\tools\test_bitvavo_adapter_connection.py
    exit /b 1
)

echo ==========================================
echo ANT COLONY ADAPTER TEST RUNNER
echo ROOT = %ROOT%
echo PY   = %PY%
echo SCRIPT = %1
echo ==========================================

"%PY%" "%ROOT%\%1"

exit /b %ERRORLEVEL%