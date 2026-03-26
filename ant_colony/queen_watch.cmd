@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ScriptDir = ...\bitvavo-bot_clean\ant_colony\
set "SCRIPTDIR=%~dp0"
REM RepoRoot  = ...\bitvavo-bot_clean\
for %%I in ("%SCRIPTDIR%..") do set "REPOROOT=%%~fI"

set "LOG=%REPOROOT%\ant_colony\queen_watch.log"
if not exist "%REPOROOT%\ant_colony" mkdir "%REPOROOT%\ant_colony" >nul 2>&1

echo ==== QUEEN_WATCH START %DATE% %TIME% ====>> "%LOG%"
echo REPOROOT=%REPOROOT%>> "%LOG%"

REM Infinite loop WITHOUT GOTO (no label issues)
for /L %%A in (1,0,2) do (
  echo.>> "%LOG%"
  echo ==== LOOP %DATE% %TIME% ====>> "%LOG%"

  powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Trading\ANT_RUN\queen_lite.ps1" -RepoRoot "%REPOROOT%" -OutDir "C:\Trading\ANT_OUT" >> "%LOG%" 2>&1

  timeout /t 60 /nobreak >nul
)

endlocal
exit /b 0