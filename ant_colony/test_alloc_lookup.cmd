@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "MARKET=%~1"
if "%MARKET%"=="" set "MARKET=BTC-EUR"

set "SCRIPTDIR=%~dp0"
for %%I in ("%SCRIPTDIR%..") do set "REPOROOT=%%~fI"

set "OUTDIR=C:\Trading\ANT_OUT"
set "ALLOC_TMP=%OUTDIR%\alloc_lookup_%MARKET%.env"

echo MARKET=%MARKET%
echo SCRIPTDIR=%SCRIPTDIR%
echo REPOROOT=%REPOROOT%
echo ALLOC_TMP=%ALLOC_TMP%

python "%REPOROOT%\ant_colony\alloc_lookup.py" "%MARKET%" > "%ALLOC_TMP%"

echo PYTHON_EXIT=%ERRORLEVEL%

if exist "%ALLOC_TMP%" (
  echo ALLOC_TMP_EXISTS=1
  type "%ALLOC_TMP%"
) else (
  echo ALLOC_TMP_EXISTS=0
)

endlocal