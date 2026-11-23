/* ==================================================================================================
   dba_cdc_07_logminer_capture_test.sql  -  CAN LOGMINER SEE CHANGES ON THE CAPTURED TABLES?   (read-only)
   Run as   : the CDC user, connected to the CDB root - this also proves its privileges are enough:
                sqlplus c##debezium@//<oracle-host>:1521/<cdb-service> @dba_cdc_07_logminer_capture_test.sql
              (or as SYSDBA: sqlplus / as sysdba @dba_cdc_07_logminer_capture_test.sql)
   Does     : what Debezium does - adds the archive logs and online redo logs of the last N minutes (DEFINE minutes) to a
              LogMiner session, starts LogMiner with the online catalog, and reports the committed changes it
              finds per captured table, with sample SQL_REDO.
   Changes  : nothing. The LogMiner session lives only in this SQL*Plus session and is ended at the end.
   No rows? : nothing changed on those tables in the window. Make a test change (section 0, optional), or
              raise DEFINE minutes, then run again.
   ================================================================================================== */
SET ECHO OFF  VERIFY OFF  DEFINE ON  FEEDBACK OFF  LINESIZE 220  PAGESIZE 200  TRIMSPOOL ON  SERVEROUTPUT ON
WHENEVER SQLERROR EXIT FAILURE

-- pdb_name: PDB that holds the schema (upper case), schema: owner of the captured tables, minutes: how far back to mine
DEFINE pdb_name = 'ORCLPDB1'
DEFINE schema   = 'EMOB'
DEFINE minutes  = 60

/* ---- 0. Optional: make one test change first (as the schema owner, in the PDB, in ANOTHER session) ----------
   It changes no data: the column is set to its own value. Then run this script.
     ALTER SESSION SET CONTAINER = ORCLPDB1;
     UPDATE EMOB.ST_OPERATING_INSTRUCTION SET OPR_INS = OPR_INS WHERE ROWNUM = 1;
     COMMIT;
   ---------------------------------------------------------------------------------------------------------- */

PROMPT
PROMPT ===== 1. Prerequisites ================================================================
COLUMN log_mode FORMAT A12
COLUMN sup_min  FORMAT A8
SELECT log_mode, supplemental_log_data_min AS sup_min, force_logging,
       CASE WHEN log_mode = 'ARCHIVELOG' AND supplemental_log_data_min IN ('YES', 'IMPLICIT') THEN 'OK' ELSE 'PROBLEM' END AS status
  FROM v$database;

PROMPT
PROMPT ===== 2. Start a LogMiner session over the last &minutes minutes ======================
DECLARE
  opt     PLS_INTEGER := DBMS_LOGMNR.NEW;
  n_arch  PLS_INTEGER := 0;
  n_redo  PLS_INTEGER := 0;
BEGIN
  -- archive logs that overlap the window, from one LOCAL destination (each sequence once)
  FOR r IN (SELECT name FROM v$archived_log
             WHERE dest_id = (SELECT MIN(dest_id) FROM v$archive_dest_status WHERE type = 'LOCAL' AND status = 'VALID')
               AND deleted = 'NO' AND status = 'A' AND name IS NOT NULL
               AND next_time > SYSDATE - &minutes / 1440
             ORDER BY thread#, sequence#) LOOP
    DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => r.name, OPTIONS => opt);
    opt := DBMS_LOGMNR.ADDFILE;
    n_arch := n_arch + 1;
  END LOOP;
  -- online redo logs not archived yet, including the CURRENT one: the newest changes are only here
  FOR r IN (SELECT MIN(f.member) AS member FROM v$log l JOIN v$logfile f ON f.group# = l.group#
             WHERE l.archived = 'NO' GROUP BY l.group#) LOOP
    DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => r.member, OPTIONS => opt);
    opt := DBMS_LOGMNR.ADDFILE;
    n_redo := n_redo + 1;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('logs added: ' || n_arch || ' archived, ' || n_redo || ' online');
  DBMS_LOGMNR.START_LOGMNR(STARTTIME => SYSDATE - &minutes / 1440,
                           OPTIONS   => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY);
  DBMS_OUTPUT.PUT_LINE('LogMiner started: OK');
END;
/

PROMPT
PROMPT ===== 3. Committed changes LogMiner sees on the captured tables ======================
COLUMN table_name FORMAT A28
COLUMN operation  FORMAT A10
COLUMN first_seen FORMAT A19
COLUMN last_seen  FORMAT A19
SELECT table_name, operation, COUNT(*) AS changes,
       TO_CHAR(MIN(timestamp), 'YYYY-MM-DD HH24:MI:SS') AS first_seen,
       TO_CHAR(MAX(timestamp), 'YYYY-MM-DD HH24:MI:SS') AS last_seen
  FROM v$logmnr_contents
 WHERE src_con_name = '&pdb_name' AND seg_owner = '&schema'
   AND operation IN ('INSERT', 'UPDATE', 'DELETE')
   AND table_name IN ('MB_ACCOUNT_MST','UNION_INFO','UPAZILA_INFO','DISTRICT_INFO','MB_ACCOUNT_DAILY_BALANCE',
                      'MB_ACCOUNT_OPERATOR','MB_ADDRESS_MST','MB_CUSTOMER_MST','MB_DOCUMENT_MST','MB_PRODUCT_MST',
                      'STCODMAS','ST_AGENT_POINT','ST_OPERATING_INSTRUCTION','ST_POINT_CATEGORY','ST_SECTOR_CODE',
                      'ST_TRANSACTION_CODE','NOMINEE_MST')
 GROUP BY table_name, operation
 ORDER BY table_name, operation;

PROMPT
PROMPT ===== 4. Sample UPDATE redo (the log-group columns should appear in the WHERE part) ====
COLUMN sql_redo FORMAT A200 WORD_WRAPPED
SELECT table_name, SUBSTR(sql_redo, 1, 400) AS sql_redo
  FROM (SELECT table_name, sql_redo FROM v$logmnr_contents
         WHERE src_con_name = '&pdb_name' AND seg_owner = '&schema' AND operation = 'UPDATE'
         ORDER BY timestamp DESC)
 WHERE ROWNUM <= 3;

EXECUTE DBMS_LOGMNR.END_LOGMNR;

PROMPT
PROMPT ===== How to read the result ==========================================================
PROMPT Section 2 "LogMiner started: OK" : the user can run LogMiner (LOGMINING, DBMS_LOGMNR, V$ views are fine).
PROMPT Section 3 has rows               : LogMiner captures changes on those tables - Debezium will see them.
PROMPT Section 3 has no rows            : no committed change in the window. Make a test change (section 0) and re-run.
PROMPT Section 4 WHERE part             : must contain the log-group columns (for example CUST_NO), not only ROWID.
PROMPT ORA-01291 / ORA-01284            : a needed log file is missing on disk - check the archive cleanup.
PROMPT ORA-01031                        : missing privilege - run dba_cdc_06_check_cdc_privileges.sql.
