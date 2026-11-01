# Health check for the CDC stack. Exit code 0 = healthy, 1 = problem (use with Task Scheduler / any alerting tool).
# Usage: .\check-cdc.ps1 [-ConnectUrl http://localhost:8083] [-Connector emob-oracle-cdc] [-MinFreeGB 10] [-WebhookUrl <url>]
param(
  [string]$ConnectUrl = 'http://localhost:8083',
  [string]$Connector  = 'emob-oracle-cdc',
  [int]   $MinFreeGB  = 10,
  [string]$WebhookUrl = ''
)
$problems = @()

foreach ($c in 'kafka','connect','kafka-ui') {
  $state = docker inspect -f '{{.State.Status}}' $c 2>$null
  if ($state -ne 'running') { $problems += "container $c is '$state'" }
}

try {
  $s = Invoke-RestMethod "$ConnectUrl/connectors/$Connector/status" -TimeoutSec 10
  if ($s.connector.state -ne 'RUNNING') { $problems += "connector state: $($s.connector.state)" }
  foreach ($t in $s.tasks) { if ($t.state -ne 'RUNNING') { $problems += "task $($t.id) state: $($t.state)" } }
} catch { $problems += "cannot read connector status: $($_.Exception.Message)" }

$drive = Get-PSDrive -Name (Split-Path -Qualifier $PSScriptRoot).TrimEnd(':')
$freeGB = [math]::Round($drive.Free / 1GB, 1)
if ($freeGB -lt $MinFreeGB) { $problems += "low disk space: $freeGB GB free" }

if ($problems) {
  $msg = "CDC ALERT: " + ($problems -join '; ')
  Write-Host $msg
  if ($WebhookUrl) { try { Invoke-RestMethod -Method Post -Uri $WebhookUrl -ContentType 'application/json' -Body (@{ text = $msg } | ConvertTo-Json) } catch {} }
  exit 1
}
Write-Host "CDC OK (disk free: $freeGB GB)"
