CRYPTOBOT clean v4 (monitor + regime + EDGE3 gate)

What this contains (minimal runnable chain):
- CB20: cb20_regime.py -> writes reports/cb20_regime.json
- CB21: cb21_edge3_gate.py -> runs EDGE3 backtest via run_edge3_fast.py using CB20 size_mult
- Bridges: cb19_cb20_bridge.py + cb19_edge3_bridge.py -> 1-line status for logs
- Runner:
    - run_cb19_once.cmd : one-shot (CB20 -> CB20 bridge -> EDGE3 bridge) then exits
    - run_live.ps1      : watchdog loop; calls run_cb19_once.cmd and appends output to live.log

Not included:
- .venv (keep your existing venv locally)
- legacy research scripts, walk-forward grids, old bot.py SIM trader, screenshots, caches

Setup (Windows / PowerShell):
1) Put this folder at: C:\Users\vikke\OneDrive\bitvavo-bot
2) Ensure venv exists: .venv\Scripts\python.exe works
3) Test once:
    cmd.exe /c C:\Users\vikke\OneDrive\bitvavo-bot\run_cb19_once.cmd
4) Scheduled task should run runner\run_live.ps1 (or root run_live.ps1 if you prefer)

Notes:
- EDGE3 is treated as frozen. CB20/CB21 are gating/monitoring layers only.
- This is SIM/monitor chain; it does NOT place trades.
