# Health check for the whole CDC pipeline. Exit code 0 = healthy, 1 = problem (use with Task Scheduler / any alerting tool).
#
# Checks: the Kafka, Connect and Kafka UI containers; the Debezium connector and its task; Elasticsearch health and the
# three indices (they must exist and hold documents); the cdc-es-sync service (/health and consumer lag); free disk.
#
# Usage: .\check-cdc.ps1 [-ConnectUrl http://localhost:8083] [-Connector emob-oracle-cdc]
#                        [-EsUrl https://localhost:9200] [-EsUser elastic] [-EsPassword <pw>] [-EsCaCert <ca.crt>]
#                        [-SyncUrl http://localhost:9108] [-MaxLag 1000] [-SkipSync] [-MinFreeGB 10] [-WebhookUrl <url>]
# Elasticsearch needs a login and trusts its own CA: the password is read from -EsPassword or $env:ES_PASSWORD, the user from
# $env:ES_USER (default elastic), the CA file defaults to elastic\ca\ca.crt next to this script ($env:ES_CA_CERT overrides).
# -SkipSync: the sync service is not deployed yet (use it before README step 6).
param(
  [string]$ConnectUrl = 'http://localhost:8083',
  [string]$Connector  = 'emob-oracle-cdc',
  [string]$EsUrl      = 'https://localhost:9200',
  [string]$EsUser     = $(if ($env:ES_USER) { $env:ES_USER } else { 'elastic' }),
  [string]$EsPassword = $env:ES_PASSWORD,
  [string]$EsCaCert   = $(if ($env:ES_CA_CERT) { $env:ES_CA_CERT } else { Join-Path $PSScriptRoot 'elastic/ca/ca.crt' }),
  [string]$SyncUrl    = 'http://localhost:9108',
  [string[]]$Indices  = @('customer_data', 'ac_balance_data', 'transaction_history_'),
  [int]   $MaxLag     = 1000,
  [switch]$SkipSync,
  [int]   $MinFreeGB  = 10,
  [string]$WebhookUrl = ''
)
$problems = @()

# Elasticsearch calls go through curl: it takes a custom CA file and keeps the password off the command line.
$curl = if ($PSVersionTable.PSVersion.Major -ge 6 -and -not $IsWindows) { 'curl' } else { 'curl.exe' }
[string[]]$curlTls = if ($curl -eq 'curl.exe') { '--ssl-no-revoke' } else { @() }     # Windows (schannel) cannot check revocation of a private CA
function Invoke-Es([string]$Path, [int]$Timeout = 10) {
  $cfg = "user = `"${EsUser}:${EsPassword}`""
  $out = $cfg | & $curl -sS -f --max-time $Timeout @curlTls --cacert $EsCaCert -K - "$EsUrl$Path" 2>&1
  if ($LASTEXITCODE -ne 0) { throw "$out" }
  $out | ConvertFrom-Json
}
if (-not $EsPassword) { $problems += "Elasticsearch password not set (use -EsPassword or `$env:ES_PASSWORD)" }
elseif (-not (Test-Path $EsCaCert)) { $problems += "Elasticsearch CA file not found: $EsCaCert (start the elastic stack first, or use -EsCaCert)" }

foreach ($c in 'kafka','connect','kafka-ui') {
  $state = docker inspect -f '{{.State.Status}}' $c 2>$null
  if ($state -ne 'running') { $problems += "container $c is '$state'" }
}

try {
  $s = Invoke-RestMethod "$ConnectUrl/connectors/$Connector/status" -TimeoutSec 10
  if ($s.connector.state -ne 'RUNNING') { $problems += "connector state: $($s.connector.state)" }
  foreach ($t in $s.tasks) { if ($t.state -ne 'RUNNING') { $problems += "task $($t.id) state: $($t.state)" } }
} catch { $problems += "cannot read connector status: $($_.Exception.Message)" }

if ($EsPassword -and (Test-Path $EsCaCert)) {
try {
  $h = Invoke-Es '/_cluster/health'
  if ($h.status -eq 'red') { $problems += "Elasticsearch cluster status is red" }
  foreach ($i in $Indices) {
    try {
      $n = (Invoke-Es "/$i/_count" 20).count
      if ($n -eq 0) { $problems += "index $i is empty - run the initial load (README step 5)" }
    } catch { $problems += "index $i is missing - run the initial load (README step 5)" }
  }
} catch { $problems += "cannot reach Elasticsearch at ${EsUrl}: $($_.Exception.Message)" }
}

if (-not $SkipSync) {
  try {
    $r = Invoke-WebRequest "$SyncUrl/health" -UseBasicParsing -TimeoutSec 10
    if ($r.StatusCode -ne 200) { $problems += "cdc-es-sync /health answered $($r.StatusCode)" }
    $m = (Invoke-WebRequest "$SyncUrl/metrics" -UseBasicParsing -TimeoutSec 10).Content
    $lag = 0
    foreach ($line in $m -split "`n") { if ($line -match '^cdc_consumer_lag\{.*\}\s+([0-9.eE+]+)') { $lag += [double]$Matches[1] } }
    if ($lag -gt $MaxLag) { $problems += "cdc-es-sync consumer lag is $lag events (limit $MaxLag)" }
  } catch { $problems += "cdc-es-sync is not answering at ${SyncUrl}: $($_.Exception.Message)" }
}

$drive = Get-PSDrive -Name (Split-Path -Qualifier $PSScriptRoot).TrimEnd(':')
$freeGB = [math]::Round($drive.Free / 1GB, 1)
if ($freeGB -lt $MinFreeGB) { $problems += "low disk space: $freeGB GB free" }

if ($problems) {
  $msg = "CDC ALERT: " + ($problems -join '; ')
  Write-Host $msg
  if ($WebhookUrl) { try { Invoke-RestMethod -Method Post -Uri $WebhookUrl -ContentType 'application/json' -Body (@{ text = $msg } | ConvertTo-Json) } catch {} }
  exit 1
}
Write-Host "CDC OK (connector running, Elasticsearch green with $($Indices.Count) indices, sync service healthy, disk free: $freeGB GB)"
