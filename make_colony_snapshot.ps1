$ErrorActionPreference = "Stop"

$CodeSrc = "C:\Users\vikke\OneDrive\bitvavo-bot_clean"
$RuntimeSrc = "C:\Trading\ANT_OUT"
$OutDir = "C:\Users\vikke\OneDrive"
$Ts = Get-Date -Format "yyyyMMdd_HHmm"

$CodeZip = Join-Path $OutDir "edge_colony_code_snapshot_$Ts.zip"
$RuntimeZip = Join-Path $OutDir "edge_colony_runtime_snapshot_$Ts.zip"

Write-Host ""
Write-Host "=== MAKE COLONY SNAPSHOT ==="
Write-Host "Timestamp : $Ts"
Write-Host "CodeSrc   : $CodeSrc"
Write-Host "RuntimeSrc: $RuntimeSrc"
Write-Host "OutDir    : $OutDir"
Write-Host ""

if (-not (Test-Path $CodeSrc)) {
    throw "Code source not found: $CodeSrc"
}

if (-not (Test-Path $RuntimeSrc)) {
    throw "Runtime source not found: $RuntimeSrc"
}

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

Write-Host "Collecting code files..."
$codeFiles = Get-ChildItem $CodeSrc -Recurse -File | Where-Object {
    $_.FullName -notmatch '\\\.venv\\' -and
    $_.FullName -notmatch '\\\.git\\' -and
    $_.FullName -notmatch '\\__pycache__\\' -and
    $_.FullName -notmatch '\\_zips\\' -and
    $_.FullName -notmatch '\\logs\\' -and
    $_.FullName -notmatch '\\data_cache\\'
}

if (-not $codeFiles -or $codeFiles.Count -eq 0) {
    throw "No code files selected for snapshot."
}

if (Test-Path $CodeZip) {
    Remove-Item $CodeZip -Force
}

Write-Host "Creating code snapshot..."
Compress-Archive -Path $codeFiles.FullName -DestinationPath $CodeZip -CompressionLevel Optimal

Write-Host "Collecting runtime files..."
$runtimeFiles = Get-ChildItem $RuntimeSrc -Recurse -File

if (-not $runtimeFiles -or $runtimeFiles.Count -eq 0) {
    throw "No runtime files selected for snapshot."
}

if (Test-Path $RuntimeZip) {
    Remove-Item $RuntimeZip -Force
}

Write-Host "Creating runtime snapshot..."
Compress-Archive -Path $runtimeFiles.FullName -DestinationPath $RuntimeZip -CompressionLevel Optimal

Write-Host ""
Write-Host "=== SNAPSHOTS CREATED ==="
Get-Item $CodeZip, $RuntimeZip | Select-Object FullName, Length, LastWriteTime | Format-Table -AutoSize
Write-Host ""