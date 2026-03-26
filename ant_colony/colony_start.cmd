@echo off
setlocal EnableExtensions

set "SCRIPTDIR=%~dp0"
for %%I in ("%SCRIPTDIR%..") do set "REPOROOT=%%~fI"

echo Starting supervisors...
"%ComSpec%" /d /c ""%SCRIPTDIR%supervise_workers.cmd""
echo OK started supervisors.

echo Starting queen alloc lite supervisor...
if exist "%SCRIPTDIR%supervisor_queen_alloc_lite.cmd" (
  start "SUP_QUEEN_ALLOC_LITE" /min "%ComSpec%" /d /c ""%SCRIPTDIR%supervisor_queen_alloc_lite.cmd""
  echo OK started queen alloc lite supervisor.
) else (
  echo ERROR: supervisor_queen_alloc_lite.cmd not found.
)

if "%ANT_ENABLE_QUEEN_WATCH%"=="1" (
  echo Starting queen watch...
  if exist "%SCRIPTDIR%queen_watch.cmd" (
    start "QUEEN_WATCH" /min "%ComSpec%" /d /c ""%SCRIPTDIR%queen_watch.cmd""
    echo OK started queen watch.
  ) else (
    echo WARN: queen_watch.cmd not found.
  )
) else (
  echo Queen watch disabled (set ANT_ENABLE_QUEEN_WATCH=1 to enable).
)

endlocal
exit /b 0
