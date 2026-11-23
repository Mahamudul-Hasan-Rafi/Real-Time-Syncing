#!/usr/bin/env bash
# ==================================================================================================
#  dba_cdc_05_archivelog_cleanup.sh  -  CDC-SAFE ARCHIVE LOG CLEANUP FOR THE RECOVERY AREA
#  Run as   : the oracle OS user on the database server (needs "sqlplus / as sysdba" and "rman target /")
#  Schedule : cron, for example every 6 hours:
#               0 */6 * * * /home/oracle/scripts/dba_cdc_05_archivelog_cleanup.sh >> /home/oracle/logs/arch_cleanup.log 2>&1
#  Deletes  : only archive logs that are ALL of
#               1. completed more than KEEP_DAYS ago
#               2. wholly before the Debezium connector's resume SCN (it still needs everything after it)
#               3. allowed by the RMAN ARCHIVELOG DELETION POLICY (for example: backed up)
#  Safe     : if the connector position cannot be read, nothing is deleted (unless ALLOW_WITHOUT_CDC=1).
#             DRY_RUN=1 only lists what would be deleted.
#  Not for  : a production database whose archive logs are already managed by the DBA team's RMAN jobs.
#             There, give them the connector's resume SCN rule instead of adding a second cleanup.
# ==================================================================================================
set -euo pipefail

# ---- settings (override with environment variables) ---------------------------------------------
KEEP_DAYS="${KEEP_DAYS:-3}"                                   # keep at least this many days on disk
CONNECT_URL="${CONNECT_URL:-http://10.11.200.109:8083}"       # Kafka Connect REST API
CONNECTOR="${CONNECTOR:-emob-oracle-cdc}"
DRY_RUN="${DRY_RUN:-0}"                                       # 1 = list only, delete nothing
ALLOW_WITHOUT_CDC="${ALLOW_WITHOUT_CDC:-0}"                   # 1 = delete by age only if Connect is unreachable
# For cron, which starts with an empty environment:
# export ORACLE_HOME=/u01/app/oracle/product/19.0.0/dbhome_1 ORACLE_SID=ORCLCDB PATH=$ORACLE_HOME/bin:$PATH

log() { echo "$(date '+%F %T') $*"; }
sql() { sqlplus -s / as sysdba <<EOF
set heading off feedback off pagesize 0 linesize 200 trimspool on verify off
whenever sqlerror exit failure
$1
EOF
}
fra_usage() { sql "select 'recovery area: ' || round(space_used/1024/1024/1024,2) || ' GB used of ' || round(space_limit/1024/1024/1024,2) || ' GB (' || round(100*space_used/space_limit,1) || '%), reclaimable ' || round(space_reclaimable/1024/1024/1024,2) || ' GB' from v\$recovery_file_dest;"; }

log "start: KEEP_DAYS=$KEEP_DAYS DRY_RUN=$DRY_RUN connector=$CONNECTOR"
log "$(fra_usage)"

# ---- 1. the SCN Debezium resumes from (it needs every archive log after it) ----------------------
CDC_SCN=""
if offsets=$(curl -sf --max-time 10 "$CONNECT_URL/connectors/$CONNECTOR/offsets"); then
  CDC_SCN=$(printf '%s' "$offsets" | grep -o '"scn" *: *"\?[0-9]\+' | grep -o '[0-9]\+$' | head -1 || true)
fi
if [[ -z "$CDC_SCN" ]]; then
  if [[ "$ALLOW_WITHOUT_CDC" != "1" ]]; then
    log "ABORT: cannot read the resume SCN of $CONNECTOR from $CONNECT_URL - nothing deleted."
    log "       Fix the connection, or set ALLOW_WITHOUT_CDC=1 to delete by age only (the connector may then be unable to resume)."
    exit 2
  fi
  log "WARNING: connector position unknown; deleting by age only (ALLOW_WITHOUT_CDC=1)"
  CDC_SCN=999999999999999999999
else
  log "connector resume SCN: $CDC_SCN"
fi

# ---- 2. archive logs that are old enough AND entirely before that SCN, per redo thread ------------
ranges=$(sql "select thread# || ' ' || min(sequence#) || ' ' || max(sequence#) || ' ' || count(*) || ' ' || round(sum(blocks*block_size)/1024/1024)
                from v\$archived_log
               where dest_id in (select dest_id from v\$archive_dest_status where type = 'LOCAL' and status <> 'INACTIVE')
                 and deleted = 'NO' and standby_dest = 'NO'
                 and completion_time < sysdate - $KEEP_DAYS
                 and next_change# <= $CDC_SCN
               group by thread#;")

if [[ -z "${ranges// /}" ]]; then
  log "nothing to delete (no archive log is both older than $KEEP_DAYS days and before the connector SCN)"
  exit 0
fi

# ---- 3. delete (or list) through RMAN, so the recovery area accounting stays correct ---------------
rman_cmds="CROSSCHECK ARCHIVELOG ALL;
DELETE NOPROMPT EXPIRED ARCHIVELOG ALL;"
while read -r thread first last count mb; do
  [[ -z "$thread" ]] && continue
  log "thread $thread: sequences $first-$last ($count logs, ${mb} MB) qualify"
  if [[ "$DRY_RUN" == "1" ]]; then
    rman_cmds+=$'\n'"LIST ARCHIVELOG SEQUENCE BETWEEN $first AND $last THREAD $thread;"
  else
    rman_cmds+=$'\n'"DELETE NOPROMPT ARCHIVELOG SEQUENCE BETWEEN $first AND $last THREAD $thread;"
  fi
done <<< "$ranges"

rman target / <<EOF
$rman_cmds
EXIT;
EOF

log "$(fra_usage)"
log "done"
