<#
 Downloads what the repository does not contain and puts it next to our config:
   * Logstash 8.4.3 (with its bundled JDK)   from artifacts.elastic.co, checked against Elastic's SHA-512
   * the Oracle JDBC driver ojdbc8 21.1.0.0  from Maven Central, checked against its SHA-1
 Files that already exist (our config/, pipeline/, templates/, scripts/) are never overwritten.

   scripts\setup.ps1              download and install
   scripts\setup.ps1 -CheckOnly   only check that the download addresses answer
#>
param([switch]$CheckOnly)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ls       = Split-Path -Parent $PSScriptRoot
$lsUrl    = 'https://artifacts.elastic.co/downloads/logstash/logstash-8.4.3-windows-x86_64.zip'
$jdbcUrl  = 'https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.1.0.0/ojdbc8-21.1.0.0.jar'
$jdbcSha1 = $jdbcUrl + '.sha1'

if ($CheckOnly) {
    foreach ($u in $lsUrl, "$lsUrl.sha512", $jdbcUrl, $jdbcSha1) {
        $r = Invoke-WebRequest -Method Head -Uri $u -UseBasicParsing
        "{0}  {1}  {2:N1} MB" -f $r.StatusCode, $u, ([double]$r.Headers['Content-Length'] / 1MB)
    }
    return
}

$tmp = Join-Path ([IO.Path]::GetTempPath()) ("ls-setup-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
    if (Test-Path (Join-Path $ls 'bin\logstash.bat')) {
        'Logstash is already installed - skipping.'
    } else {
        $zip = Join-Path $tmp 'logstash.zip'
        "Downloading $lsUrl"
        Invoke-WebRequest -Uri $lsUrl -OutFile $zip -UseBasicParsing
        $expected = ((Invoke-WebRequest -Uri "$lsUrl.sha512" -UseBasicParsing).Content -split '\s+')[0].Trim().ToLower()
        $actual = (Get-FileHash -Algorithm SHA512 $zip).Hash.ToLower()
        if ($actual -ne $expected) { throw "Checksum mismatch for the Logstash download (expected $expected, got $actual)" }
        'Checksum OK, extracting (a few minutes) ...'
        Expand-Archive -Path $zip -DestinationPath $tmp
        # robocopy with /XC /XN /XO copies only files that do not exist yet, so our own files stay untouched.
        robocopy (Join-Path $tmp 'logstash-8.4.3') $ls /E /XC /XN /XO /NFL /NDL /NJH /NJS /NP | Out-Null
        if ($LASTEXITCODE -ge 8) { throw "robocopy failed with exit code $LASTEXITCODE" }
    }

    $jar = Join-Path $ls 'drivers\ojdbc8-21.1.0.0.jar'
    if (Test-Path $jar) {
        'Oracle JDBC driver is already installed - skipping.'
    } else {
        New-Item -ItemType Directory -Force -Path (Split-Path $jar) | Out-Null
        "Downloading $jdbcUrl"
        $dl = Join-Path $tmp 'ojdbc8.jar'
        Invoke-WebRequest -Uri $jdbcUrl -OutFile $dl -UseBasicParsing
        $expected = ((Invoke-WebRequest -Uri $jdbcSha1 -UseBasicParsing).Content -split '\s+')[0].Trim().ToLower()
        $actual = (Get-FileHash -Algorithm SHA1 $dl).Hash.ToLower()
        if ($actual -ne $expected) { throw "Checksum mismatch for the JDBC driver (expected $expected, got $actual)" }
        Copy-Item $dl $jar
    }
    'Done. Next: set ORACLE_USER / ORACLE_PASSWORD (environment or Packages\cdc\elastic\.env), then scripts\run_pipelines.cmd'
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
