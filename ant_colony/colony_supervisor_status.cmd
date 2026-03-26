@echo off
setlocal

cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean

set PYEXE=C:\Users\vikke\OneDrive\bitvavo-bot_clean\.venv\Scripts\python.exe
set STATUS_SCRIPT=C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\colony_supervisor_status_lite.py

if not exist "%PYEXE%" (
  echo ERROR: python not found: %PYEXE%
  exit /b 1
)

if not exist "%STATUS_SCRIPT%" (
  echo ERROR: supervisor status script not found: %STATUS_SCRIPT%
  exit /b 1
)

echo RUNNING_COLONY_SUPERVISOR_STATUS
echo PYEXE=%PYEXE%
echo STATUS_SCRIPT=%STATUS_SCRIPT%

"%PYEXE%" "%STATUS_SCRIPT%"

endlocal