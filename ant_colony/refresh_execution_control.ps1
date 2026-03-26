param(
    [ValidateSet(0,1)]
    [int]$ExecutionEnabled = 0
)

$path = "C:\Trading\ANT_OUT\execution_control.json"

if (-not (Test-Path $path)) {
    throw "Missing file: $path"
}

$data = Get-Content $path -Raw | ConvertFrom-Json
$tsUtc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$enabledBool = [bool]$ExecutionEnabled

$data.ts_utc = $tsUtc
$data.source_component = "execution_control_manual"
$data.execution_enabled = $enabledBool

foreach ($p in $data.markets.PSObject.Properties) {
    if ($null -eq $p.Value.execution_enabled) {
        $p.Value | Add-Member -NotePropertyName execution_enabled -NotePropertyValue $false -Force
    }
}

$data | ConvertTo-Json -Depth 10 | Set-Content $path -Encoding UTF8

Write-Host "UPDATED $path :: execution_enabled -> $enabledBool"
Get-Content $path
