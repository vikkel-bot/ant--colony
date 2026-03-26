@echo off
setlocal

cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean

set PYEXE=C:\Users\vikke\OneDrive\bitvavo-bot_clean\.venv\Scripts\python.exe
set CHECK_SCRIPT=C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\colony_watchdog_check_lite.py
set MAX_AGE_SECONDS=120

if not exist "%PYEXE%" (
  echo ERROR: python not found: %PYEXE%
  exit /b 1
)

if not exist "%CHECK_SCRIPT%" (
  echo ERROR: watchdog script not found: %CHECK_SCRIPT%
  exit /b 1
)

echo RUNNING_COLONY_WATCHDOG_CHECK
echo PYEXE=%PYEXE%
echo CHECK_SCRIPT=%CHECK_SCRIPT%
echo MAX_AGE_SECONDS=%MAX_AGE_SECONDS%

"%PYEXE%" "%CHECK_SCRIPT%" %MAX_AGE_SECONDS%

endlocal