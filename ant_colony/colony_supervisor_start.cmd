@echo off
setlocal

cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean

set WATCHDOG_LOOP=C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\colony_watchdog_loop.cmd

if not exist "%WATCHDOG_LOOP%" (
  echo ERROR: watchdog loop not found: %WATCHDOG_LOOP%
  exit /b 1
)

echo STARTING_COLONY_SUPERVISOR
echo WATCHDOG_LOOP=%WATCHDOG_LOOP%

start "COLONY_SUPERVISOR" cmd /k C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\colony_watchdog_loop.cmd

endlocal
exit /b 0