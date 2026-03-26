@echo off
setlocal

set ROOT=%~dp0..
for %%I in ("%ROOT%") do set ROOT=%%~fI

set PYEXE=%ROOT%\.venv\Scripts\python.exe
set RECOVER_SCRIPT=%ROOT%\ant_colony\colony_supervisor_recover_lite.py
set OUTDIR=C:\Trading\ANT_OUT

echo RUNNING_COLONY_RECOVER_SUPERVISOR
echo PYEXE=%PYEXE%
echo RECOVER_SCRIPT=%RECOVER_SCRIPT%
echo OUTDIR=%OUTDIR%

if not exist "%PYEXE%" (
  echo PYTHON_NOT_FOUND
  exit /b 1
)

if not exist "%RECOVER_SCRIPT%" (
  echo RECOVER_SCRIPT_NOT_FOUND
  exit /b 1
)

"%PYEXE%" "%RECOVER_SCRIPT%"
set RC=%ERRORLEVEL%

echo RECOVER_RC=%RC%
exit /b %RC%