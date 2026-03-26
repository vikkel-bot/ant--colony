param(
  [Parameter(Mandatory=$true)][string]$Market,
  [int]$SleepSeconds = 60
)

$ErrorActionPreference = "Stop"

$root = (Get-Location).Path
$once = Join-Path $root "ant_colony\tools\run_worker_once.ps1"

if (!(Test-Path $once)) { throw "Missing: $once" }

while ($true) {
  powershell -ExecutionPolicy Bypass -File $once -Market $Market
  $rc = $LASTEXITCODE

  if ($rc -ne 0) {
    exit $rc   # stop on FATAL (no silent limping)
  }

  Start-Sleep -Seconds $SleepSeconds
}
