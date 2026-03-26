@echo off
setlocal EnableExtensions
echo === WORKER_CYCLE START %DATE% %TIME% ===
cd /d "C:\Users\vikke\OneDrive\bitvavo-bot_clean\ant_colony\workers\ETH-EUR"

set ROOT=C:\Users\vikke\OneDrive\bitvavo-bot_clean
set PY=%ROOT%\.venv\Scripts\python.exe

echo --- step1: CB20 ---
call .\run_cb20.cmd
if errorlevel 1 (
  echo CB20_FAILED errorlevel=%ERRORLEVEL%
  exit /b %ERRORLEVEL%
)

echo --- step2: CB21 GATE ---
call .\run_cb21_edge3.cmd
if errorlevel 1 (
  echo CB21_FAILED errorlevel=%ERRORLEVEL%
  exit /b %ERRORLEVEL%
)

echo === WORKER_CYCLE OK ===
exit /b 0
