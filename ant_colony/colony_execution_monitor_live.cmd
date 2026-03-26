@echo off
setlocal

cd /d C:\Users\vikke\OneDrive\bitvavo-bot_clean

:loop
cls
echo ============================================================
echo ANT COLONY EXECUTION MONITOR LIVE
echo ============================================================
echo.

python .\ant_colony\show_colony_dashboard.py
echo.
echo ============================================================
echo WORKER STRATEGY SELECTION
echo ============================================================
echo.
python .\ant_colony\show_worker_strategy_selection.py
echo.
echo ============================================================
echo WORKER EXECUTION PLAN
echo ============================================================
echo.
python .\ant_colony\show_worker_execution_plan.py
echo.
echo ============================================================
echo WORKER CONTEXT
echo ============================================================
echo.
python .\ant_colony\show_worker_context.py
echo.
echo ============================================================
echo WORKER CONSUMER STUB
echo ============================================================
echo.
python .\ant_colony\show_worker_consumer_stub.py
echo.
echo ============================================================
echo WORKER RUNTIME INTENT
echo ============================================================
echo.
python .\ant_colony\show_worker_runtime_intent.py
echo.
echo ============================================================
echo WORKER EXECUTION SIMULATOR
echo ============================================================
echo.
python .\ant_colony\show_worker_execution_simulator.py
echo.
echo ============================================================
echo WORKER MARKET PRICE FEED
echo ============================================================
echo.
python .\ant_colony\show_worker_market_price_feed.py
echo.
echo ============================================================
echo WORKER PORTFOLIO
echo ============================================================
echo.
python .\ant_colony\show_worker_portfolio.py
echo.
echo ============================================================
echo WORKER EXIT RULES
echo ============================================================
echo.
python .\ant_colony\show_worker_exit_rules.py
echo.
echo ============================================================
echo WORKER EXIT SIMULATOR
echo ============================================================
echo.
python .\ant_colony\show_worker_exit_simulator.py
echo.
echo ============================================================
echo WORKER EXIT APPLY STUB
echo ============================================================
echo.
python .\ant_colony\show_worker_exit_apply_stub.py
echo.
echo ============================================================
echo WORKER TRADE LIFECYCLE
echo ============================================================
echo.
python .\ant_colony\show_worker_trade_lifecycle.py
echo.
echo ============================================================
echo WORKER TRADE HISTORY
echo ============================================================
echo.
python .\ant_colony\show_worker_trade_history.py
echo.
echo Refresh every 10 seconds - press Ctrl+C to stop
timeout /t 10 >nul
goto loop