param(
  [int]$SleepSeconds = 5
)

$ErrorActionPreference = "Stop"

$root = (Get-Location).Path
$queen = Join-Path $root "ant_colony\tools\queen_status.ps1"
$out = Join-Path $root "ant_colony\colony_status.json"

if (!(Test-Path $queen)) { throw "Missing: $queen" }

while($true){
  powershell -ExecutionPolicy Bypass -File $queen | Out-Null

  $j = Get-Content $out -Raw | ConvertFrom-Json
  $ts = $j.ts_utc

  # Print 1 line per worker
  foreach($w in $j.workers){
    $m = $w.market
    $wk = $w.worker
    $gt = $w.gate
    $sm = $w.size_mult
    $ex = $w.exit

    $cb21_gate = $null
    $cb21_reason = $null
    if ($w.cb21) {
      $cb21_gate = $w.cb21.edge3_combined_gate
      $cb21_reason = $w.cb21.edge3_health_reason
    }

    if($cb21_gate){
      "{0} {1} worker={2} gate={3} size={4} exit={5} cb21={6} reason={7}" -f $ts,$m,$wk,$gt,$sm,$ex,$cb21_gate,$cb21_reason
    } else {
      "{0} {1} worker={2} gate={3} size={4} exit={5}" -f $ts,$m,$wk,$gt,$sm,$ex
    }
  }

  Start-Sleep -Seconds $SleepSeconds
}
