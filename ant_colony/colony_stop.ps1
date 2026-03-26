param(
  [string]$OutDir = "C:\Trading\ANT_OUT"
)

Write-Host "=== STOP COLONY (supervisors + queen_watch) ==="

# Kill CMD supervisors
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -like "*supervise_one_market.cmd*" -or $_.CommandLine -like "*supervise_workers.cmd*" } |
  ForEach-Object {
    try {
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
      Write-Host ("KILLED: pid={0} cmd={1}" -f $_.ProcessId, $_.CommandLine)
    } catch {}
  }

# Kill queen_watch
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -like "*queen_watch.cmd*" } |
  ForEach-Object {
    try {
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
      Write-Host ("KILLED: pid={0} cmd={1}" -f $_.ProcessId, $_.CommandLine)
    } catch {}
  }

# Optional: mark stop in ANT_OUT
try {
  if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }
  $p = Join-Path $OutDir "colony_stop_marker.json"
  $o = @{ ts_utc = (Get-Date).ToUniversalTime().ToString("o"); action="stop" }
  ($o | ConvertTo-Json -Compress) | Set-Content -Path $p -Encoding Ascii
} catch {}

Write-Host "OK: stop completed."