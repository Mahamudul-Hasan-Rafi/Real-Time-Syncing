# Compares row counts and amount totals between Oracle and Elasticsearch.
# Exit code 0 = all match, 1 = mismatch. Logstash reports a normal finish even when a JDBC query
# fails midway, so this check is what tells you a load is complete.
param(
    [string]$EsUrl = "https://10.11.200.109:9200",
    [string]$EsUser = $(if ($env:ES_USER) { $env:ES_USER } else { "elastic" }),
    [string]$EsPassword = $env:ES_PASSWORD,
    [string]$EsCaCert = $(if ($env:ES_CA_CERT) { $env:ES_CA_CERT } else { Join-Path $PSScriptRoot "../../../ca/ca.crt" }),
    [string]$DbConnect = "//10.11.200.37:1521/emob",
    [string]$DbUser = "emob"
)
$ErrorActionPreference = "Stop"
$sumField = @{ ac_balance_data = "balance"; customer_data = "initial_amt"; transaction_history_ = "amount" }

# Elasticsearch needs a login and trusts its own CA; curl takes the CA file and keeps the password off the command line.
if (-not $EsPassword) { throw "Elasticsearch password not set (use -EsPassword or `$env:ES_PASSWORD)" }
if (-not (Test-Path $EsCaCert)) { throw "Elasticsearch CA file not found: $EsCaCert (use -EsCaCert)" }
$curl = if ($PSVersionTable.PSVersion.Major -ge 6 -and -not $IsWindows) { "curl" } else { "curl.exe" }
[string[]]$curlTls = if ($curl -eq "curl.exe") { "--ssl-no-revoke" } else { @() }     # Windows (schannel) cannot check revocation of a private CA
function Invoke-Es([string]$Path, [string]$Body = "") {
    $curlArgs = $curlTls + @("-sS", "-f", "--cacert", $EsCaCert, "-K", "-", "-X", "POST", "-H", "Content-Type: application/json")
    $tmp = $null
    if ($Body) {
        # -K - reads the login from stdin, so the request body goes through a temp file
        $tmp = Join-Path ([IO.Path]::GetTempPath()) ("es_" + [guid]::NewGuid().ToString("N") + ".json")
        [IO.File]::WriteAllText($tmp, $Body)
        $curlArgs += @("--data-binary", "@$tmp")
    }
    try {
        $out = "user = `"${EsUser}:${EsPassword}`"" | & $curl @curlArgs "$EsUrl$Path" 2>&1
        if ($LASTEXITCODE -ne 0) { throw "Elasticsearch request failed: $out" }
        $out | ConvertFrom-Json
    } finally { if ($tmp) { Remove-Item $tmp -Force -ErrorAction SilentlyContinue } }
}

$password = $env:ORACLE_PASSWORD
if (-not $password) {
    $secure = Read-Host "Oracle password for $DbUser" -AsSecureString
    $password = [Net.NetworkCredential]::new("", $secure).Password
}

# A temp script keeps the password out of the process list. It is deleted straight after the call.
# Piping to sqlplus instead made Windows PowerShell 5.1 prepend a byte-order mark that SQL*Plus rejects.
$sqlFile = Join-Path $PSScriptRoot "reconcile.sql"
$tmp = Join-Path $env:TEMP ("reconcile_" + [guid]::NewGuid().ToString("N") + ".sql")
try {
    Set-Content -Path $tmp -Encoding ascii -Value "connect $DbUser/$password@$DbConnect", "@`"$sqlFile`""
    $lines = & sqlplus -L -S /nolog "@$tmp"
} finally {
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
}
$oracleErrors = $lines | Where-Object { $_ -match '^(ORA|SP2)-\d+' }
if ($oracleErrors) { throw "Oracle query failed: $oracleErrors" }

$source = @{}
foreach ($line in $lines) {
    $parts = "$line".Trim().Split('|')
    if ($parts.Count -eq 3) { $source[$parts[0]] = @{ rows = [long]$parts[1]; sum = [decimal]$parts[2] } }
}

$failed = $false
$report = foreach ($index in $sumField.Keys | Sort-Object) {
    $field = $sumField[$index]
    Invoke-Es "/$index/_refresh" | Out-Null
    $body = @{ size = 0; track_total_hits = $true; aggs = @{ total = @{ sum = @{ field = $field } } } } | ConvertTo-Json -Depth 5
    $result = Invoke-Es "/$index/_search" $body
    $esRows = [long]$result.hits.total.value
    $esSum = [decimal]$result.aggregations.total.value
    $src = $source[$index]
    # ES sums in floating point, so allow a tolerance of 1 currency unit on the total.
    $ok = ($esRows -eq $src.rows) -and ([math]::Abs($esSum - $src.sum) -lt 1)
    if (-not $ok) { $failed = $true }
    [pscustomobject]@{
        Index = $index; OracleRows = $src.rows; EsRows = $esRows
        OracleSum = $src.sum; EsSum = $esSum; Status = $(if ($ok) { "OK" } else { "MISMATCH" })
    }
}
$report | Format-Table -AutoSize | Out-String -Width 160 | Write-Output
if ($failed) { exit 1 }
exit 0
