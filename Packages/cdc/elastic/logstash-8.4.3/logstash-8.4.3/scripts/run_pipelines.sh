#!/usr/bin/env sh
# Runs all three load pipelines from config/pipelines.yml. Logstash exits when all have finished.
#   scripts/run_pipelines.sh           run the pipelines
#   scripts/run_pipelines.sh --test    validate the configs and exit
#
# ORACLE_USER, ORACLE_PASSWORD, ORACLE_JDBC_URL, ES_HOSTS, ES_USER, ES_PASSWORD and ES_CA_CERT are read from the environment, or from
# Packages/cdc/elastic/.env if that file exists (copy .env.example there). Variables already set win.
set -eu
LS_HOME=$(cd "$(dirname "$0")/.." && pwd)
cd "$LS_HOME"                      # the pipelines use paths relative to the Logstash folder
export LS_ROOT="$LS_HOME"            # the JDBC driver path in the pipelines must be absolute

ENVFILE="$LS_HOME/../../.env"
if [ -f "$ENVFILE" ]; then
  while IFS='=' read -r key value || [ -n "$key" ]; do
    case "$key" in ''|\#*) continue ;; esac
    eval "[ -n \"\${$key:-}\" ]" || export "$key=$value"
  done < "$ENVFILE"
fi

[ -x bin/logstash ] || { echo "Logstash is not installed in $LS_HOME. Run scripts/setup.sh first." >&2; exit 1; }
[ -f drivers/ojdbc8-21.1.0.0.jar ] || { echo "The Oracle JDBC driver is missing. Run scripts/setup.sh first." >&2; exit 1; }

unset LS_JAVA_HOME                 # use the JDK bundled with Logstash
if [ "${1:-}" = "--test" ]; then exec bin/logstash --config.test_and_exit; fi
exec bin/logstash
