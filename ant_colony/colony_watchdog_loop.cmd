@echo off
setlocal

cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean

set WATCHDOG_START_IF_NEEDED=C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\colony_watchdog_start_if_needed.cmd
set SLEEP_SECONDS=60

if not exist "%WATCHDOG_START_IF_NEEDED%" (
  echo ERROR: watchdog wrapper not found: %WATCHDOG_START_IF_NEEDED%
  exit /b 1
)

echo STARTING_COLONY_WATCHDOG_LOOP
echo WATCHDOG_START_IF_NEEDED=%WATCHDOG_START_IF_NEEDED%
echo SLEEP_SECONDS=%SLEEP_SECONDS%

:loop
call "%WATCHDOG_START_IF_NEEDED%"
timeout /t %SLEEP_SECONDS% >nul
goto loop