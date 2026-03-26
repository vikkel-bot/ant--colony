param(
  [string]$OutDir = $env:ANT_OUT_DIR
)

$ErrorActionPreference = "Stop"

function UtcNowIso() {
  (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

function ReadAllTextShared([string]$path){
  $fs = $null
  $sr = $null
  try {
    $fs = [System.IO.File]::Open(
      $path,
      [System.IO.FileMode]::Open,
      [System.IO.FileAccess]::Read,
      [System.IO.FileShare]::ReadWrite
    )
    $sr = New-Object System.IO.StreamReader($fs, [System.Text.Encoding]::UTF8, $true)
    return $sr.ReadToEnd()
  } finally {
    if($sr){ $sr.Dispose() }
    if($fs){ $fs.Dispose() }
  }
}

function TryReadJsonShared([string]$path){
  try {
    if(!(Test-Path $path)){ return $null }
    $txt = ReadAllTextShared $path
    if([string]::IsNullOrWhiteSpace($txt)){ return $null }
    return ($txt | ConvertFrom-Json -ErrorAction Stop)
  } catch {
    return $null
  }
}

function TryReadLastLineShared([string]$path){
  try {
    if(!(Test-Path $path)){ return $null }

    $txt = ReadAllTextShared $path
    if([string]::IsNullOrWhiteSpace($txt)){ return $null }

    $parts = [regex]::Split($txt, "\r?\n")
    for($i = $parts.Length - 1; $i -ge 0; $i--){
      $line = $parts[$i]
      if(-not [string]::IsNullOrWhiteSpace($line)){
        return $line.Trim()
      }
    }

    return $null
  } catch {
    return $null
  }
}

$workersRoot = "C:\Trading\EDGE3\ant_colony\workers"

if([string]::IsNullOrWhiteSpace($OutDir)){
  $OutDir = "C:\Trading\ANT_OUT"
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$workers = @()

if(Test-Path $workersRoot){
  $dirs = Get-ChildItem -Path $workersRoot -Directory -ErrorAction SilentlyContinue
  foreach($d in $dirs){
    $market = $d.Name
    $wroot = $d.FullName

    $healthPath = Join-Path $wroot "health.txt"
    $logPath    = Join-Path $wroot "logs\worker.log"

    $cb20Path   = Join-Path $wroot "reports\cb20_regime.json"
    $cb21Path   = Join-Path $wroot "reports\edge3_cb21_meta.json"
    $snapPath   = Join-Path $wroot "reports\edge3_snapshot.json"

    $cb20 = TryReadJsonShared $cb20Path
    $cb21 = TryReadJsonShared $cb21Path
    $healthLine = TryReadLastLineShared $healthPath

    $latestSnap = $null
    if(Test-Path $snapPath){ $latestSnap = $snapPath }

    $workerState = "UNKNOWN"
    $gate = "UNKNOWN"
    $sizeMult = $null
    $exit = $null

    if($healthLine -match "worker=(\S+)"){ $workerState = $Matches[1] }
    if($healthLine -match "exit=(\S+)"){
      try { $exit = [int]$Matches[1] } catch { $exit = $null }
    }

    if($cb21 -ne $null){
      $gate = $cb21.edge3_combined_gate
      $sizeMult = $cb21.edge3_combined_size_mult
      if($workerState -eq "UNKNOWN"){ $workerState = "OK" }
      if($exit -eq $null){ $exit = 0 }
    }

    $workers += [pscustomobject]@{
      market = $market
      worker = $workerState
      gate = $gate
      size_mult = $sizeMult
      exit = $exit
      health_line = $healthLine
      latest_snapshot = $latestSnap
      cb20 = $cb20
      cb21 = $cb21
      log_path = $logPath
    }
  }
}

$status = [pscustomobject]@{
  ts_utc = UtcNowIso
  workers_root = $workersRoot
  worker_count = $workers.Count
  workers = $workers
}

$outPath = Join-Path $OutDir "colony_status.json"
$tmp = "$outPath.tmp.$PID"

($status | ConvertTo-Json -Depth 12) | Set-Content -Encoding UTF8 $tmp
Move-Item -Force $tmp $outPath
Remove-Item -Force $tmp -ErrorAction SilentlyContinue

Write-Output "OK wrote $outPath"
Write-Output "worker_count=$($workers.Count)"