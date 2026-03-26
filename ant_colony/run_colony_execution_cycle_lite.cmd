@echo off
setlocal

set ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean
set PS1=%ROOT%\ant_colony\run_colony_execution_cycle_lite.ps1

echo RUNNING_COLONY_EXECUTION_CYCLE_CMD
echo ROOT=%ROOT%
echo PS1=%PS1%
echo.

if not exist "%PS1%" (
  echo ERROR: runner script not found: %PS1%
  exit /b 1
)

powershell -ExecutionPolicy Bypass -File "%PS1%"
exit /b %ERRORLEVEL%
