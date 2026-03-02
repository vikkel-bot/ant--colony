param(
  [int]$SleepSeconds = 60,
  [int]$MaxSnapshotAgeSeconds = 180
)

$ErrorActionPreference = "Stop"
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ROOT

$ReportsDir = Join-Path $ROOT "reports"
$LogsDir    = Join-Path $ROOT "logs"
$LogPath    = Join-Path $LogsDir "live.log"
$HealthPath = Join-Path $ReportsDir "cb19_health.txt"

foreach($d in @($ReportsDir, $LogsDir)){
  if(!(Test-Path $d)){ New-Item -ItemType Directory -Path $d | Out-Null }
}
if(!(Test-Path $LogPath)){ New-Item -ItemType File -Path $LogPath | Out-Null }

function UtcNowIso { (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") }
function LogLine([string]$line){ Add-Content -Path $LogPath -Value "$(UtcNowIso) $line" }

function WriteHealth([string]$line){
  $tmp = $HealthPath + ".tmp"
  Set-Content -Path $tmp -Value $line -Encoding UTF8
  Move-Item -Force $tmp $HealthPath
}

function RunStep([string]$name, [string]$scriptRelPath, [string[]]$args=@()){
  $scriptPath = Join-Path $ROOT $scriptRelPath
  if(!(Test-Path $scriptPath)){
    LogLine "CB19_FATAL step=$name missing_script=$scriptPath"
    return 9009
  }

  $tmp = Join-Path $env:TEMP ("cb19_" + $name + "_" + (Get-Date -Format "yyyyMMdd_HHmmss_fff") + ".log")
  LogLine "STEP_START $name cmd=python `"$scriptPath`" $($args -join ' ')"
  & python "$scriptPath" @args *> $tmp
  $rc = $LASTEXITCODE

  if(Test-Path $tmp){
    $lines = Get-Content $tmp
    foreach($l in $lines){
      if($l -ne ""){ Add-Content -Path $LogPath -Value ("    " + $l) }
    }
    Remove-Item -Force $tmp -ErrorAction SilentlyContinue
  }

  if($rc -ne 0){ LogLine "CB19_FATAL step=$name rc=$rc" } else { LogLine "STEP_OK $name rc=0" }
  return $rc
}

function ReadJsonSafe([string]$relPath){
  $p = Join-Path $ROOT $relPath
  if(!(Test-Path $p)){ return $null }
  try { return (Get-Content $p -Raw | ConvertFrom-Json) } catch { return $null }
}

function SnapshotAgeSeconds([string]$relPath){
  $p = Join-Path $ROOT $relPath
  if(!(Test-Path $p)){ return $null }
  $age = (Get-Date) - (Get-Item $p).LastWriteTime
  return [int]$age.TotalSeconds
}

LogLine "CB19_LIVE_START root=$ROOT sleep_s=$SleepSeconds max_snap_age_s=$MaxSnapshotAgeSeconds"

while($true){
  try{
    LogLine "TICK_START"

    $rc1 = RunStep "CB20" "cb20_regime.py"
    $rc2 = RunStep "CB20_BRIDGE" "cb19_cb20_bridge.py"

    $rc3 = RunStep "CB21_EDGE3" "cb21_edge3_gate.py"
    $rc4 = RunStep "EDGE3_BRIDGE" "cb19_edge3_bridge.py"

    $cb20 = ReadJsonSafe "reports\cb20_regime.json"
    $e3   = ReadJsonSafe "reports\edge3_snapshot.json"
    $meta = ReadJsonSafe "reports\edge3_cb21_meta.json"

    $trend = if($cb20){ $cb20.trend_regime } else { "NA" }
    $vol   = if($cb20){ $cb20.vol_regime } else { "NA" }
    $gate  = if($meta){ $meta.cb20_gate } else { "NA" }

    $e3_status = if($e3){ $e3.status } else { "NOFILE" }
    $pf = if($e3 -and $e3.profit_factor -ne $null){ [double]$e3.profit_factor } else { [double]::NaN }
    $eq = if($e3 -and $e3.ending_equity -ne $null){ [double]$e3.ending_equity } else { [double]::NaN }

    $age_e3 = SnapshotAgeSeconds "reports\edge3_snapshot.json"
    $age_cb20 = SnapshotAgeSeconds "reports\cb20_regime.json"

    if($age_cb20 -ne $null -and $age_cb20 -gt $MaxSnapshotAgeSeconds){
      LogLine "CB19_FATAL snapshot_stale name=CB20 age_s=$age_cb20 max_age_s=$MaxSnapshotAgeSeconds"
    }
    if($age_e3 -ne $null -and $age_e3 -gt $MaxSnapshotAgeSeconds){
      LogLine "CB19_FATAL snapshot_stale name=EDGE3 age_s=$age_e3 max_age_s=$MaxSnapshotAgeSeconds"
    }

    $health = "$(UtcNowIso) gate=$gate trend=$trend vol=$vol edge3_status=$e3_status pf=$pf equity=$eq snap_age_s=$age_e3"
    WriteHealth $health
    LogLine "HEALTH $health"

    LogLine "TICK_END rc_cb20=$rc1 rc_cb20_bridge=$rc2 rc_cb21=$rc3 rc_edge3_bridge=$rc4"
  }
  catch {
    LogLine ("CB19_FATAL unhandled_exception=" + $_.Exception.Message)
  }

  Start-Sleep -Seconds $SleepSeconds
}
