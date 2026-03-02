@echo off
set ROOT=%~dp0
cd /d "%ROOT%"

rem Base position fraction (before CB20 scaling)
set EDGE3_BASE_PF=0.5

"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\cb21_edge3_gate.py" ^
  --market BTC-EUR --interval 4h ^
  --start-iso 2022-01-01 --end-iso 2026-02-25 ^
  --initial-equity 1000 ^
  --taker-fee 0.0025 --maker-fee 0.0015 ^
  --slippage-bps 3 ^
  --entry-mode limit_maker --reclaim-limit-offset-bps -10 ^
  --fill-prob 0.70 --stop-extra-slip-bps 1 ^
  --vol-filter atr_percentile --atr-period 14 --atr-regime-window 200 --atr-regime-percentile 0.50
