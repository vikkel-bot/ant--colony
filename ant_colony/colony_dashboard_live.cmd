@echo off
setlocal EnableExtensions

set "SCRIPTDIR=%~dp0"
for %%I in ("%SCRIPTDIR%..") do set "REPOROOT=%%~fI"
set "PY=%REPOROOT%\.venv\Scripts\python.exe"

if not exist "%PY%" (
  echo ERROR: python not found at "%PY%"
  exit /b 1
)

:LOOP
cls

echo ============================================================
echo                LIVE UNIFIED ANT COLONY DASHBOARD
echo ============================================================
echo RepoRoot: %REPOROOT%
echo Python  : %PY%
echo.
echo Refreshing layers...
echo.

REM --- queen / colony upstream ---
"%PY%" "%SCRIPTDIR%queen_alloc_lite.py" --out-dir "C:\Trading\ANT_OUT" --once >nul 2>&1
"%PY%" "%SCRIPTDIR%queen_colony_risk_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%queen_combine_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%market_health_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%combined_colony_status_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%queen_strategy_router_lite.py" >nul 2>&1

REM --- worker / execution downstream ---
"%PY%" "%SCRIPTDIR%worker_strategy_selection_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_entry_rules_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_execution_plan_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_context_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_consumer_stub_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_runtime_intent_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_runtime_dispatch_stub_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_execution_bridge_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_execution_simulator_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_market_price_feed_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_portfolio_simulator_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_exit_rules_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_exit_simulator_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_exit_apply_stub_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_trade_lifecycle_lite.py" >nul 2>&1
"%PY%" "%SCRIPTDIR%worker_trade_history_lite.py" >nul 2>&1

"%PY%" "%SCRIPTDIR%show_colony_dashboard.py"

echo.
echo Druk Ctrl+C om te stoppen...
timeout /t 5 /nobreak >nul
goto LOOP