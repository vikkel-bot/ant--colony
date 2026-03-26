# bitvavo-bot_clean (CB20/CB21/CB19 mini-stack)

## Wat is wat
- **CB20** = regime snapshot (trend/vol/gate/size) -> `reports/cb20_regime.json`
- **CB21** = EDGE3 gate: leest CB20 snapshot en schaalt position_fraction + schrijft:
  - `reports/edge3_cb21_meta.json`
  - `reports/edge3_snapshot.json`
- **CB19** = monitor/bridge: print 1 regel voor CB20 + 1 regel voor EDGE3 (geschikt voor live.log)

## 1x draaien (handmatig)
In PowerShell (in deze map):

```powershell
cmd.exe /c .\run_cb19_once.cmd
```

Je ziet dan 3 regels:
1) CB20 print
2) CB20 bridge line
3) EDGE3 bridge line

## Live loop (Task Scheduler)
`run_live.ps1` draait elke 60s:
- `cmd.exe /c run_cb19_once.cmd`
- logt stdout/stderr naar `%USERPROFILE%\Documents\logs\live.log`

Test handmatig:
```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_live.ps1
```

## Belangrijk
Alle scripts zijn **portable**: geen hardcoded paden. Alles gebruikt `%~dp0` (cmd) en `$PSScriptRoot` (ps1).
