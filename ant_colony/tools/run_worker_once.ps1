param(
  [Parameter(Mandatory=$true)][string]$Market
)

$ErrorActionPreference = "Stop"

$root = "C:\Trading\EDGE3"
$w = Join-Path $root ("ant_colony\workers\" + $Market)

if (!(Test-Path $w)) { throw "Worker missing: $w" }

$logPath = Join-Path $w "logs\worker.log"
$health  = Join-Path $w "health.txt"
$tmpHealth = $health + ".tmp"

function UtcNowIso() { (Get-Date).ToUniversalTime().ToString("s") + "Z" }

function Find-RunnerPath([string]$folder) {
  # Prefer CB21 edge3 chain. CB19 is only used if nothing else exists.
  $names = @(
    "run_cb21_edge3.cmd","run_cb21_edge3.bat","run_cb21_edge3",
    "run_cb20.cmd","run_cb20.bat","run_cb20",
    "run_cb19_once.cmd","run_cb19_once.bat","run_cb19_once"
  )
  foreach($name in $names){
    $p = Join-Path $folder $name
    if(Test-Path $p){ return [string]$p }
  }
  return $null
}

New-Item -ItemType Directory -Force -Path (Join-Path $w "logs") | Out-Null

Push-Location $w
try {
  $ts = UtcNowIso
  $runner = Find-RunnerPath $w

  if (-not $runner) {
    $line = "$ts market=$Market worker=FATAL gate=UNKNOWN size_mult=0.00 exit=2 err=""missing_runner"""
    $line | Set-Content -Encoding UTF8 $tmpHealth
    Move-Item -Force $tmpHealth $health
    $line | Add-Content -Encoding UTF8 $logPath
    exit 2
  }

  $rc = 1
  $outText = ""

  try {
    # robust cmd invocation:
    $cmdArg = '""' + $runner + '""'
    $outText = (cmd.exe /c $cmdArg 2>&1 | Out-String)
    $rc = $LASTEXITCODE
  }
  catch {
    $outText = "EXCEPTION: " + $_.Exception.Message
    $rc = 2
  }

  # best-effort EDGE3 bridge line
  $edgeLine = $null
  if ($outText) {
    $lines = $outText -split "`r?`n"
    $edgeLine = ($lines | Where-Object { $_ -match "\bEDGE3\b" } | Select-Object -Last 1)
  }

  if ($rc -eq 0) {
    if ($edgeLine -and $edgeLine.Trim().Length -gt 0) {
      $line = $edgeLine.Trim()
    } else {
      $line = "$ts market=$Market worker=OK gate=ALLOW size_mult=1.00 exit=0"
    }
  } else {
    $line = "$ts market=$Market worker=FATAL gate=UNKNOWN size_mult=0.00 exit=$rc runner=""$([IO.Path]::GetFileName($runner))"""
  }

  # atomic health
  $line | Set-Content -Encoding UTF8 $tmpHealth
  Move-Item -Force $tmpHealth $health

  # append log
  ("=== " + $ts + " RUNNER=" + $runner + " EXIT=" + $rc + " ===") | Add-Content -Encoding UTF8 $logPath
  $outText | Add-Content -Encoding UTF8 $logPath

  exit $rc
}
finally {
  Pop-Location
}
