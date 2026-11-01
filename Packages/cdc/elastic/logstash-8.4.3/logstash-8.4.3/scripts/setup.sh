#!/usr/bin/env sh
# Downloads what the repository does not contain and puts it next to our config:
#   * Logstash 8.4.3 (with its bundled JDK)   from artifacts.elastic.co, checked against Elastic's SHA-512
#   * the Oracle JDBC driver ojdbc8 21.1.0.0  from Maven Central, checked against its SHA-1
# Files that already exist (our config/, pipeline/, templates/, scripts/) are never overwritten.
#   scripts/setup.sh                download and install
#   scripts/setup.sh --check-only   only check that the download addresses answer
set -eu
LS_HOME=$(cd "$(dirname "$0")/.." && pwd)
ARCH=$(uname -m)
case "$ARCH" in x86_64|amd64) ARCH=x86_64 ;; aarch64|arm64) ARCH=aarch64 ;; *) echo "unsupported CPU $ARCH" >&2; exit 1 ;; esac
LS_URL="https://artifacts.elastic.co/downloads/logstash/logstash-8.4.3-linux-$ARCH.tar.gz"
JDBC_URL="https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.1.0.0/ojdbc8-21.1.0.0.jar"

if [ "${1:-}" = "--check-only" ]; then
  for u in "$LS_URL" "$LS_URL.sha512" "$JDBC_URL" "$JDBC_URL.sha1"; do
    printf '%s  %s\n' "$(curl -fsSI -o /dev/null -w '%{http_code}' "$u")" "$u"
  done
  exit 0
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
if [ -x "$LS_HOME/bin/logstash" ]; then
  echo "Logstash is already installed - skipping."
else
  echo "Downloading $LS_URL"
  curl -fsSL -o "$TMP/ls.tar.gz" "$LS_URL"
  expected=$(curl -fsSL "$LS_URL.sha512" | cut -d' ' -f1)
  actual=$(sha512sum "$TMP/ls.tar.gz" | cut -d' ' -f1)
  [ "$expected" = "$actual" ] || { echo "Checksum mismatch for the Logstash download" >&2; exit 1; }
  tar -xzf "$TMP/ls.tar.gz" -C "$TMP"
  cp -rn "$TMP/logstash-8.4.3/." "$LS_HOME/"         # -n: never overwrite our own files
fi

JAR="$LS_HOME/drivers/ojdbc8-21.1.0.0.jar"
if [ -f "$JAR" ]; then
  echo "Oracle JDBC driver is already installed - skipping."
else
  mkdir -p "$LS_HOME/drivers"
  echo "Downloading $JDBC_URL"
  curl -fsSL -o "$TMP/ojdbc8.jar" "$JDBC_URL"
  expected=$(curl -fsSL "$JDBC_URL.sha1" | cut -d' ' -f1)
  actual=$(sha1sum "$TMP/ojdbc8.jar" | cut -d' ' -f1)
  [ "$expected" = "$actual" ] || { echo "Checksum mismatch for the JDBC driver" >&2; exit 1; }
  cp "$TMP/ojdbc8.jar" "$JAR"
fi
echo "Done. Next: set ORACLE_USER / ORACLE_PASSWORD (environment or Packages/cdc/elastic/.env), then scripts/run_pipelines.sh"
