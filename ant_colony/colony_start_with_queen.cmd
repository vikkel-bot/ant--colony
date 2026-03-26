@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"

echo Starting colony (base) in background...
if exist "%ROOT%colony_start.cmd" (
  start "COLONY_BASE" /min cmd /c ""%ROOT%colony_start.cmd""
  echo OK started colony_start.cmd (background).
) else (
  echo WARN: colony_start.cmd not found. Skipping base.
)

echo Starting queen alloc lite supervisor...
if exist "%ROOT%supervisor_queen_alloc_lite.cmd" (
  start "SUP_QUEEN_ALLOC_LITE" /min cmd /c ""%ROOT%supervisor_queen_alloc_lite.cmd""
  echo OK started queen alloc lite supervisor.
) else (
  echo ERROR: supervisor_queen_alloc_lite.cmd not found.
)

exit /b 0
