param(
  [string]$RepoRoot = "C:\Users\vikke\OneDrive\bitvavo-bot_clean",
  [string]$AntOut   = "C:\Trading\ANT_OUT"
)

$ErrorActionPreference = "Stop"
$ts = Get-Date -Format "yyyyMMdd_HHmmss"

function Ensure-Dir([string]$p) {
  New-Item -ItemType Directory -Force -Path $p | Out-Null
}

# 1) SRC zip (exclude common junk + _zips)
$srcZipDir = Join-Path $RepoRoot "_zips"
Ensure-Dir $srcZipDir
$srcZip = Join-Path $srcZipDir ("ant_colony_src_{0}.zip" -f $ts)
if (Test-Path $srcZip) { Remove-Item -Force $srcZip }

$exclude = @(
  "\.venv\", "\__pycache__\", "\.git\", "\.mypy_cache\", "\.pytest_cache\", "\.ruff_cache\",
  "\node_modules\", "\var\", "\logs\", "\data_cache\", "\_zips\"
)

$files = Get-ChildItem -Path $RepoRoot -Recurse -File | Where-Object {
  $p = $_.FullName
  -not ($exclude | Where-Object { $p -like "*$_*" })
}

Compress-Archive -Path $files.FullName -DestinationPath $srcZip -Force
Write-Host "OK SRC zip: $srcZip"

# 2) ANT_OUT zip (CRITICAL: do NOT create zip inside the folder you're zipping)
$outZipDir = Join-Path $AntOut "_zips"
Ensure-Dir $outZipDir
$outZipFinal = Join-Path $outZipDir ("ant_out_{0}.zip" -f $ts)
if (Test-Path $outZipFinal) { Remove-Item -Force $outZipFinal }

# Build file list excluding _zips to avoid self-inclusion, and exclude transient tmp files
$outExclude = @("\_zips\", "\_tmp\", "\.tmp.")
$outFiles = Get-ChildItem -Path $AntOut -Recurse -File | Where-Object {
  $p = $_.FullName
  -not ($outExclude | Where-Object { $p -like "*$_*" })
}

$tmpZip = Join-Path $env:TEMP ("ant_out_{0}.zip" -f $ts)
if (Test-Path $tmpZip) { Remove-Item -Force $tmpZip }

Compress-Archive -Path $outFiles.FullName -DestinationPath $tmpZip -Force

Move-Item -Force $tmpZip $outZipFinal
Write-Host "OK OUT zip: $outZipFinal"

# 3) Pointer file in ANT_OUT (ASCII for compatibility)
try {
  $p = Join-Path $AntOut "last_snapshot.json"
  $o = @{
    ts_utc  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    src_zip = $srcZip
    out_zip = $outZipFinal
  }
  ($o | ConvertTo-Json -Compress) | Set-Content -Path $p -Encoding Ascii
} catch {
  Write-Host "WARN: failed to write last_snapshot.json"
}
