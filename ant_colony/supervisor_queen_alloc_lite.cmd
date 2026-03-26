@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPTDIR=%~dp0"
for %%I in ("%SCRIPTDIR%..") do set "REPOROOT=%%~fI"
set "PY=%REPOROOT%\.venv\Scripts\python.exe"
set "OUTDIR=C:\Trading\ANT_OUT"
set "LOG=%OUTDIR%\queen_alloc_lite_supervisor.log"

if not exist "%OUTDIR%" mkdir "%OUTDIR%" >nul 2>&1

echo ==== SUPERVISOR_QUEEN_ALLOC_LITE START %DATE% %TIME% ====>> "%LOG%"
echo REPOROOT=%REPOROOT%>> "%LOG%"
echo PY=%PY%>> "%LOG%"

if not exist "%PY%" (
  echo ERROR python not found: %PY%>> "%LOG%"
  exit /b 1
)

for /L %%A in (1,0,2) do (
  echo.>> "%LOG%"
  echo ==== LOOP %DATE% %TIME% ====>> "%LOG%"

  "%PY%" "%SCRIPTDIR%queen_alloc_lite.py" --out-dir "%OUTDIR%" --once >> "%LOG%" 2>&1
  "%PY%" "%SCRIPTDIR%queen_colony_risk_lite.py" >> "%LOG%" 2>&1
  "%PY%" "%SCRIPTDIR%queen_combine_lite.py" >> "%LOG%" 2>&1
  "%PY%" "%SCRIPTDIR%market_health_lite.py" >> "%LOG%" 2>&1

  timeout /t 60 /nobreak >nul
)

endlocal
exit /b 0