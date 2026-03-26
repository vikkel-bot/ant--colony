@echo off
cd /d C:\Users\vikke\OneDrive\bitvavo-bot

call C:\Users\vikke\OneDrive\bitvavo-bot\run_cb20.cmd
C:\Users\vikke\OneDrive\bitvavo-bot\.venv\Scripts\python.exe C:\Users\vikke\OneDrive\bitvavo-bot\cb19_cb20_bridge.py
C:\Users\vikke\OneDrive\bitvavo-bot\.venv\Scripts\python.exe C:\Users\vikke\OneDrive\bitvavo-bot\cb19_edge3_bridge.py
C:\Users\vikke\OneDrive\bitvavo-bot\.venv\Scripts\python.exe C:\Users\vikke\OneDrive\bitvavo-bot\optional\cb19_monitor.py --root . --roll-n 20
