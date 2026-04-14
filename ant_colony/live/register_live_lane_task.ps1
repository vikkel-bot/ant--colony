# AC-187: Register ANT Live Lane Runner as a Windows Scheduled Task.
#
# What it does: Runs live_lane_runner every 15 minutes, survives reboots.
# Why it exists: Replaces manual triggers with an automated, self-healing loop.
#
# Run once as Administrator from the repo root:
#   powershell -ExecutionPolicy Bypass -File ant_colony\live\register_live_lane_task.ps1
#
# To remove the task:
#   Unregister-ScheduledTask -TaskName "ANT_LiveLaneRunner" -Confirm:$false
#
# To inspect:
#   Get-ScheduledTask -TaskName "ANT_LiveLaneRunner" | Format-List
#   Get-ScheduledTaskInfo -TaskName "ANT_LiveLaneRunner"

param(
    [string]$TaskName     = "ANT_LiveLaneRunner",
    [string]$RepoRoot     = "",
    [int]   $IntervalMins = 15,
    [int]   $StartupDelayMins = 2
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Resolve repo root (two levels up from this script: live\ -> ant_colony\ -> root)
# ---------------------------------------------------------------------------
if (-not $RepoRoot) {
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)
}
if (-not (Test-Path $RepoRoot)) {
    Write-Error "Repo root not found: $RepoRoot"
    exit 1
}
Write-Host "Repo root : $RepoRoot"

# ---------------------------------------------------------------------------
# Resolve Python executable
# ---------------------------------------------------------------------------
$PythonExe = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $PythonExe) {
    Write-Error "python not found on PATH. Install Python or activate the correct environment."
    exit 1
}
Write-Host "Python    : $PythonExe"

# ---------------------------------------------------------------------------
# Task components
# ---------------------------------------------------------------------------

# Action: run the runner module from the repo root
$Action = New-ScheduledTaskAction `
    -Execute        $PythonExe `
    -Argument       "-m ant_colony.live.live_lane_runner" `
    -WorkingDirectory $RepoRoot

# Trigger 1: Repeating every N minutes (perpetual, starting now)
$StartTime       = (Get-Date).AddSeconds(10)   # small delay so registration completes first
$RepeatDuration  = [System.TimeSpan]::MaxValue  # run indefinitely

$RepeatTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At $StartTime `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMins) `
    -RepetitionDuration $RepeatDuration

# Trigger 2: At system startup (with a short delay so the network/filesystem is ready)
$StartupTrigger = New-ScheduledTaskTrigger -AtStartup
# Add a delay via CIM (PowerShell trigger objects don't expose Delay directly)
$StartupTrigger.Delay = "PT${StartupDelayMins}M"   # ISO 8601 duration string

# Settings
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit     (New-TimeSpan -Minutes 10) `
    -MultipleInstances      IgnoreNew `
    -RestartCount           3 `
    -RestartInterval        (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable     $true `
    -RunOnlyIfNetworkAvailable $false `
    -WakeToRun              $false

# Principal: run as current user, highest available privileges
$Principal = New-ScheduledTaskPrincipal `
    -UserId   $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

# ---------------------------------------------------------------------------
# Register (or update if already exists)
# ---------------------------------------------------------------------------
$Task = Register-ScheduledTask `
    -TaskName  $TaskName `
    -Action    $Action `
    -Trigger   @($RepeatTrigger, $StartupTrigger) `
    -Settings  $Settings `
    -Principal $Principal `
    -Description "AC-187: ANT live lane runner — periodic execution every $IntervalMins min, self-healing on reboot. Non-binding heartbeat written to C:\Trading\ANT_LIVE\heartbeat.json." `
    -Force

Write-Host ""
Write-Host "Task registered successfully."
Write-Host "  Name     : $($Task.TaskName)"
Write-Host "  State    : $($Task.State)"
Write-Host "  Interval : every $IntervalMins minutes"
Write-Host "  Startup  : yes (after ${StartupDelayMins}-minute delay)"
Write-Host ""
Write-Host "Inspect with:"
Write-Host "  Get-ScheduledTask -TaskName '$TaskName' | Format-List"
Write-Host "  Get-ScheduledTaskInfo -TaskName '$TaskName'"
