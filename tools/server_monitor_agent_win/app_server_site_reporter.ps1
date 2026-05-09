$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$configPath = Join-Path $scriptRoot "site_reporter_config.json"
$logDir = Join-Path $scriptRoot "logs"
$logPath = Join-Path $logDir "site_reporter.log"

if (!(Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}

function Write-Log {
  param(
    [string]$Level,
    [string]$Message
  )
  $line = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
  Add-Content -Path $logPath -Value $line
}

function Load-Config {
  if (!(Test-Path $configPath)) {
    throw "Config not found: $configPath"
  }
  return (Get-Content $configPath -Raw | ConvertFrom-Json)
}

function Get-IisSiteNames {
  param([bool]$IgnoreStoppedSites)
  Import-Module WebAdministration -ErrorAction Stop

  $names = New-Object System.Collections.Generic.HashSet[string]([System.StringComparer]::OrdinalIgnoreCase)

  $websites = Get-Website
  foreach ($site in $websites) {
    if ($IgnoreStoppedSites -and $site.State -ne "Started") {
      continue
    }
    [void]$names.Add($site.Name)
    $apps = Get-WebApplication -Site $site.Name -ErrorAction SilentlyContinue
    foreach ($app in $apps) {
      $path = $app.Path
      if ([string]::IsNullOrWhiteSpace($path)) { continue }
      $path = $path.Trim("/")
      if ([string]::IsNullOrWhiteSpace($path)) { continue }
      [void]$names.Add(("{0}/{1}" -f $site.Name, $path))
    }
  }

  return @($names.ToArray() | Sort-Object)
}

function Invoke-Report {
  param($Config)

  $serverName = [string]$Config.ServerName
  if ([string]::IsNullOrWhiteSpace($serverName)) {
    $serverName = $env:COMPUTERNAME
  }

  $sites = Get-IisSiteNames -IgnoreStoppedSites ([bool]$Config.IgnoreStoppedSites)
  $base = [string]$Config.MainServer
  $path = [string]$Config.ReportPath
  if ([string]::IsNullOrWhiteSpace($path)) {
    $path = "/api/app-server-sites/report-agent"
  }
  $uri = $base.TrimEnd("/") + "/" + $path.TrimStart("/")

  $payload = @{
    serverName = $serverName
    siteNames = $sites
    collectedAt = (Get-Date).ToString("o")
    token = [string]$Config.Token
  } | ConvertTo-Json -Depth 8

  $resp = Invoke-RestMethod -Method Post -Uri $uri -Body $payload -ContentType "application/json" -TimeoutSec 20
  Write-Log "INFO" ("report ok | server={0} sites={1}" -f $serverName, $sites.Count)
}

Write-Log "INFO" "app_server_site_reporter started."

while ($true) {
  try {
    $cfg = Load-Config
    if ([string]::IsNullOrWhiteSpace([string]$cfg.MainServer)) {
      throw "MainServer is empty in site_reporter_config.json"
    }
    if ([string]::IsNullOrWhiteSpace([string]$cfg.Token)) {
      throw "Token is empty in site_reporter_config.json"
    }
    Invoke-Report -Config $cfg
  }
  catch {
    Write-Log "ERROR" $_.Exception.Message
  }

  $interval = 300
  try {
    $cfg2 = Load-Config
    if ($cfg2.IntervalSeconds -gt 10) {
      $interval = [int]$cfg2.IntervalSeconds
    }
  }
  catch {}
  Start-Sleep -Seconds $interval
}

