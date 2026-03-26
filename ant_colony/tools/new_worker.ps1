param(
  [Parameter(Mandatory=$true)][string]$Market
)

$ErrorActionPreference = "Stop"

$root = (Get-Location).Path
$template = Join-Path $root "frozen_template"
$dst = Join-Path $root ("ant_colony\workers\" + $Market)

if (!(Test-Path $template)) { throw "Missing frozen_template at: $template" }

New-Item -ItemType Directory -Force -Path $dst | Out-Null

# Copy template -> worker folder (isolated per market)
Copy-Item -Recurse -Force "$template\*" $dst

# Ensure worker-local folders
New-Item -ItemType Directory -Force -Path "$dst\reports","$dst\data_cache","$dst\logs" | Out-Null

# Patch market string in cb21 runner (if hardcoded BTC-EUR in cmd)
$cmd21 = Join-Path $dst "run_cb21_edge3.cmd"
if (Test-Path $cmd21) {
  (Get-Content $cmd21 -Raw) -replace "BTC-EUR", $Market | Set-Content -Encoding ASCII $cmd21
}

# Patch cb19_once to call cb21_edge3 when cb21_once is missing
$cmd19 = Join-Path $dst "run_cb19_once.cmd"
$cmd21once = Join-Path $dst "run_cb21_once.cmd"
if ((Test-Path $cmd19) -and !(Test-Path $cmd21once) -and (Test-Path $cmd21)) {
  $raw = Get-Content $cmd19 -Raw
  $raw2 = $raw -replace "run_cb21_once\.cmd", "run_cb21_edge3.cmd"
  $raw2 | Set-Content -Encoding ASCII $cmd19
}

"OK worker created: $dst"
