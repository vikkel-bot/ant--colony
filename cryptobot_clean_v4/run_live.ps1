# run_live.ps1
# Loopt in een loop en schrijft alles naar een logbestand.
# BELANGRIJK: gebruikt paden relatief aan deze map (portable).

$ErrorActionPreference = "Stop"

$root = $PSScriptRoot
$log  = Join-Path $env:USERPROFILE "Documents\logs\live.log"

# Interval tussen runs (sec)
$SLEEP_S = 60

New-Item -ItemType Directory -Force (Split-Path $log) | Out-Null

"===== CLEAN_START $(Get-Date -Format s) =====" | Set-Content -Encoding UTF8 $log

while ($true) {
  Add-Content $log ("===============================")
  Add-Content $log ("START " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
  Add-Content $log ("PWD=" + $root)

  try {
    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = "cmd.exe"
    $pinfo.Arguments = "/c `"$root\run_cb19_once.cmd`""
    $pinfo.WorkingDirectory = $root
    $pinfo.RedirectStandardOutput = $true
    $pinfo.RedirectStandardError  = $true
    $pinfo.UseShellExecute = $false
    $pinfo.CreateNoWindow = $true

    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    [void]$p.Start()

    $stdout = $p.StandardOutput.ReadToEnd()
    $stderr = $p.StandardError.ReadToEnd()
    $p.WaitForExit()

    if ($stdout) { Add-Content $log $stdout.TrimEnd() }
    if ($stderr) {
      Add-Content $log "STDERR:"
      Add-Content $log $stderr.TrimEnd()
    }

    Add-Content $log ("STOP " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " EXITCODE=" + $p.ExitCode)
  } catch {
    Add-Content $log ("FATAL: " + $_.Exception.Message)
  }

  Start-Sleep -Seconds $SLEEP_S
}
