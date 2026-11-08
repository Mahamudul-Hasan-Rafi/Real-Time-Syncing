#!/bin/bash
# Restarts a Debezium connector task that has ended up FAILED. Kafka Connect never does this by itself: a failed
# task stays failed ("will not recover until manually restarted") while the container still shows healthy.
#
# What it does:  polls the connector status; on FAILED it calls the Connect restart endpoint (failed tasks only),
#                waiting longer after each attempt (STEP, 2*STEP, ... up to DELAY_MAX).
# After MAX_ATTEMPTS restarts that did not recover it stops restarting, writes one record to watchdog-giveup.log
# and keeps checking every GIVEUP_RECHECK seconds (it resumes by itself if someone fixes the connector).
# What it never does: change the connector config or offsets, restart a task that is RUNNING, or touch a PAUSED or
# STOPPED connector (those are deliberate). A restart cannot help when the SCN the connector resumes from is no
# longer in the database's archive logs: that needs a snapshot, which needs a person.
#
# A task counts as recovered only after STABLE_SECONDS of staying RUNNING, so a task that starts, runs a minute and
# fails again keeps using up its attempts instead of resetting them.
set -u

CONNECT_URL=${CONNECT_URL:-http://connect:8083}
CONNECTOR=${CONNECTOR_NAME:-emob-oracle-cdc}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-10}
CHECK_INTERVAL=${CHECK_INTERVAL:-30}
RETRY_DELAY_STEP=${RETRY_DELAY_STEP:-30}
RETRY_DELAY_MAX=${RETRY_DELAY_MAX:-300}
STABLE_SECONDS=${STABLE_SECONDS:-600}
GIVEUP_RECHECK=${GIVEUP_RECHECK:-300}
LOG_DIR=${LOG_DIR:-/logs}
LOG_MAX_BYTES=5242880

mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/watchdog.log"
GIVEUP_LOG="$LOG_DIR/watchdog-giveup.log"

stamp() { date '+%Y-%m-%dT%H:%M:%S%z'; }

log() {
  local line="$(stamp) $*"
  echo "$line"
  echo "$line" >> "$LOG"
  if [ "$(wc -c < "$LOG")" -gt "$LOG_MAX_BYTES" ]; then mv -f "$LOG" "$LOG.1"; fi
}

# The first "Caused by" line of the task's stack trace, else the start of the trace.
failure_reason() {
  local reason
  reason=$(echo "$1" | grep -o 'Caused by: [^\\]*' | head -1 | cut -c1-400)
  [ -z "$reason" ] && reason=$(echo "$1" | grep -oE '"trace" *: *"[^\\]*' | head -1 | sed -E 's/^"trace" *: *"//' | cut -c1-400)
  echo "${reason:-no error text in the status}"
}

attempts=0
gave_up=0
running_since=0
log "started: connector=$CONNECTOR url=$CONNECT_URL max_attempts=$MAX_ATTEMPTS"

while true; do
  status=$(curl -s -m 10 "$CONNECT_URL/connectors/$CONNECTOR/status")
  now=$(date +%s)

  if echo "$status" | grep -qE '"state" *: *"FAILED"'; then
    running_since=0
    if [ "$attempts" -lt "$MAX_ATTEMPTS" ]; then
      attempts=$((attempts + 1))
      code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' -X POST \
        "$CONNECT_URL/connectors/$CONNECTOR/restart?includeTasks=true&onlyFailed=true")
      log "FAILED: restart attempt $attempts/$MAX_ATTEMPTS (HTTP $code). Reason: $(failure_reason "$status")"
      delay=$((attempts * RETRY_DELAY_STEP))
      [ "$delay" -gt "$RETRY_DELAY_MAX" ] && delay=$RETRY_DELAY_MAX
      sleep "$delay"
    else
      if [ "$gave_up" -eq 0 ]; then
        gave_up=1
        msg="GAVE UP: $CONNECTOR is still FAILED after $MAX_ATTEMPTS restart attempts; manual action needed. Last reason: $(failure_reason "$status")"
        log "$msg"
        echo "$(stamp) $msg" >> "$GIVEUP_LOG"
      fi
      sleep "$GIVEUP_RECHECK"
    fi

  elif echo "$status" | grep -qE '"state" *: *"RUNNING"'; then
    [ "$running_since" -eq 0 ] && running_since=$now
    if [ "$attempts" -gt 0 ] && [ $((now - running_since)) -ge "$STABLE_SECONDS" ]; then
      log "RECOVERED: $CONNECTOR has been RUNNING for ${STABLE_SECONDS}s; attempt counter reset"
      attempts=0
      gave_up=0
    fi
    sleep "$CHECK_INTERVAL"

  else
    running_since=0   # Connect unreachable, connector missing, paused or stopped: leave it alone
    sleep "$CHECK_INTERVAL"
  fi
done
