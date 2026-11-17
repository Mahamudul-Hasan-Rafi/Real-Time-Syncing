# Oracle CDC runbook: errors seen and fixes

Connector: `emob-oracle-cdc` (Debezium 3.7 Oracle, LogMiner). Connect REST API: `http://<connect-host>:8083`.

## 1. Task FAILED: change stream "no longer available on the server"

- **Symptom:** `DebeziumException: The connector is trying to read change stream starting at OracleOffsetContext [scn=...] but this is no longer available on the server` (from `validateSchemaHistory`).
- **Cause:** Connect had a stored offset (SCN), but the schema-history topic `schema-history.emob` was empty or missing. This happens after a Kafka reset, a recreated topic, or retention cleanup. Archive logs for that SCN being gone can cause it too.
- **Fix:**
  1. Stop the connector: `PUT /connectors/emob-oracle-cdc/stop`.
  2. Clear its offsets: `DELETE /connectors/emob-oracle-cdc/offsets`.
  3. Delete the connector, then delete the `schema-history.emob` topic.
  4. Recreate the connector. With `snapshot.mode=no_data` it captures the schema and starts from the current SCN.
- **Side effect:** changes made while the connector was down are lost.
- **Prevention:** do not delete the schema-history topic on its own. Treat `connect_offsets` and `schema-history.*` as a pair, and back them up or keep long retention.

## 2. Registration fails: HTTP 400 "Failed to resolve Oracle database version"

- **Symptom:** `connector-init` printed `Unable to connect (primary): Failed to resolve Oracle database version`.
- **Cause:** the Connect log showed `ORA-01017: invalid username/password; logon denied`. The listener was reachable, but `c##cdcuser` was rejected: wrong or missing `CDC_USER_PASSWORD` in `.env`, or the user does not exist.
- **Fix:** use a user and password that exist. The running connector uses `c##debezium`. Confirm with `select username, account_status from dba_users where username like '%DEBEZIUM%' or username like '%CDC%';`.
- **Prevention:** keep the committed JSON user in sync with the real one, and test the login with `sqlplus` before registering.

## 3. Task FAILED: no valid archive destination

- **Symptom:** `DebeziumException: Failed to locate a local and valid archive destination in Oracle` (`ArchiveDestinationNameResolver.validate`).
- **Diagnosis:** `v$archive_dest` showed `LOG_ARCHIVE_DEST_1` with `STATUS = ERROR`, `DESTINATION = USE_DB_RECOVERY_FILE_DEST` and `ORA-16032: parameter destination string cannot be translated`. The recovery area `/u01/oracle_logs` was not full (1.2 GB of 15 GB used).
- **Fix (this single command solved it):**
  ```sql
  alter system set log_archive_dest_1='LOCATION=USE_DB_RECOVERY_FILE_DEST' scope=both;
  select dest_id, status, error from v$archive_dest where dest_id = 1;   -- now VALID
  ```
  After the destination showed `VALID`, restart the failed task: `POST /connectors/emob-oracle-cdc/tasks/0/restart`.
- **Not needed:** the `log_archive_dest_state_1` defer/enable commands and `alter system archive log current` were not run and were not required.
- **Prevention:** monitor `v$archive_dest_status` and alert if the status is not `VALID`. Keep the recovery area below about 80% full and set RMAN deletion for archived logs.

## 4. Connector RUNNING but one table never produces events

- **Symptom:** `MB_ACCOUNT_MST` was captured but `MB_CUSTOMER_MST` had no messages.
- **Cause:** the live `table.include.list` was `EMOB.MB_ACCOUNT_MST` only. The JSON file said `EMOB\\..*`, but Connect does not read the file after registration. The connector had been registered by hand with different settings.
- **Fix:** update the running config with a PUT, or delete and recreate from the edited file.
  ```bash
  curl -s http://<connect-host>:8083/connectors/emob-oracle-cdc/config \
    | sed 's/"table.include.list":"[^"]*"/"table.include.list":"EMOB.MB_ACCOUNT_MST,EMOB.MB_CUSTOMER_MST"/' \
    | curl -s -X PUT -H "Content-Type: application/json" --data-binary @- \
        http://<connect-host>:8083/connectors/emob-oracle-cdc/config
  ```
  To apply the file instead: `curl -s -X DELETE .../connectors/emob-oracle-cdc` then `docker compose run --rm connector-init`. Running `connector-init` alone does nothing while the connector exists. Offsets are kept because the name is the same.
- **Prevention:** after any change, compare the live config (`GET /connectors/emob-oracle-cdc/config`) with the file. Make the file the single source of truth.

## 5. Warning: missing supplemental logging

- **Symptom:** `Database table 'ORCLPDB1.EMOB.MB_ACCOUNT_MST' not configured with supplemental logging "(ALL) COLUMNS"; only explicitly changed columns will be captured`.
- **Impact:** updates carry only the changed columns, so unchanged columns are missing from events.
- **Fix:** `ALTER TABLE EMOB.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;` for every captured table.
- **Prevention:** add this to the table onboarding checklist.

## 6. Not errors, but confusing

- **Empty topic in Kafka UI:** with `snapshot.mode=no_data`, existing rows are never emitted. Only changes made after the connector starts appear, and only if committed.
- **Config drift:** the JSON file and the live connector differed on `database.user` and `table.include.list` (items 2 and 4).

## Quick health check

```bash
curl -s http://<connect-host>:8083/connectors/emob-oracle-cdc/status    # connector and task 0 RUNNING
curl -s http://<connect-host>:8083/connectors/emob-oracle-cdc/config    # matches the JSON file
docker logs connect --since 30m 2>&1 | grep -E "ORA-|ERROR"             # no Oracle errors
```

```sql
select dest_id, status, error from v$archive_dest where dest_id = 1;    -- VALID
select * from v$recovery_file_dest;                                     -- used well below limit
```
