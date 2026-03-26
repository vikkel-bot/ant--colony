@echo off
setlocal

cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean

set PYEXE=C:\Users\vikke\OneDrive\bitvavo-bot_clean\.venv\Scripts\python.exe
set PANEL_SCRIPT=C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\show_colony_supervisor_panel.py

if not exist "%PYEXE%" (
  echo ERROR: python not found: %PYEXE%
  exit /b 1
)

if not exist "%PANEL_SCRIPT%" (
  echo ERROR: panel script not found: %PANEL_SCRIPT%
  exit /b 1
)

echo RUNNING_COLONY_SUPERVISOR_PANEL
echo PYEXE=%PYEXE%
echo PANEL_SCRIPT=%PANEL_SCRIPT%
echo.

"%PYEXE%" "%PANEL_SCRIPT%"

endlocal