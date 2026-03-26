param(
    [Parameter(Mandatory=$true)]
    [string]$Market,

    [Parameter(Mandatory=$true)]
    [ValidateSet(0,1)]
    [int]$Enabled
)

$path = "C:\Trading\ANT_OUT\execution_control.json"

if (-not (Test-Path $path)) {
    throw "Missing file: $path"
}

$data = Get-Content $path -Raw | ConvertFrom-Json
$tsUtc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$enabledBool = [bool]$Enabled

if (-not $data.markets) {
    throw "execution_control.json has no markets object"
}

$marketProp = $data.markets.PSObject.Properties | Where-Object { $_.Name -eq $Market }

if (-not $marketProp) {
    throw "Unknown market: $Market"
}

$marketProp.Value.execution_enabled = $enabledBool
$data.ts_utc = $tsUtc
$data.source_component = "execution_control_manual"

$data | ConvertTo-Json -Depth 10 | Set-Content $path -Encoding UTF8

Write-Host "UPDATED $path :: $Market -> $enabledBool"
Get-Content $path
