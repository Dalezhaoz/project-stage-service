$ErrorActionPreference = "Continue"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$candidatePaths = @(
    (Join-Path $scriptDir "dist_new\DingTalkProxy.exe"),
    (Join-Path $scriptDir "dist\DingTalkProxy.exe")
)
$exePath = $candidatePaths | Where-Object { Test-Path $_ } | Select-Object -First 1
$logDir = Join-Path $scriptDir "logs"

if ([string]::IsNullOrWhiteSpace($exePath)) {
    Add-Type -AssemblyName System.Windows.Forms
    [System.Windows.Forms.MessageBox]::Show("Cannot find DingTalkProxy.exe in dist_new or dist.", "DingTalkProxy")
    exit 1
}

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$launcherLog = Join-Path $logDir "watchdog.log"

while ($true) {
    $now = Get-Date
    Add-Content -Path $launcherLog -Value ("[{0}] starting proxy: {1}" -f $now.ToString("yyyy-MM-dd HH:mm:ss"), $exePath)

    try {
        $proc = Start-Process -FilePath $exePath -WorkingDirectory (Split-Path $exePath -Parent) -PassThru
        $proc.WaitForExit()
        $exitCode = $proc.ExitCode
    }
    catch {
        $exitCode = -1
        Add-Content -Path $launcherLog -Value ("[{0}] failed to start proxy: {1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $_.Exception.Message)
    }

    Add-Content -Path $launcherLog -Value ("[{0}] proxy exited with code {1}, restart in 5s" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $exitCode)
    Start-Sleep -Seconds 5
}
