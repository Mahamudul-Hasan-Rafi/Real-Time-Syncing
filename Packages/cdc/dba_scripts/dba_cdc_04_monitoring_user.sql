/* ==================================================================================================
   dba_cdc_04_monitoring_user.sql  -  READ-ONLY USER FOR THE GRAFANA / PROMETHEUS ORACLE EXPORTER   (online, no restart)
   Run as   : sqlplus / as sysdba @dba_cdc_04_monitoring_user.sql
   Creates  : common user c##cdcmon that can only read the V$ views the exporter queries
              (archive logs, recovery area, archive destination, database state). It cannot read application data.
   Contains : no passwords - the password is asked for when the script runs.
   Then set in Packages/cdc/.env (the # in the user name must be URL-encoded as %23):
              ORACLE_MON_USER=c%23%23cdcmon
              ORACLE_MON_PASSWORD=<the password>
   ================================================================================================== */
SET ECHO ON  VERIFY OFF  DEFINE ON  LINESIZE 200  PAGESIZE 100  TRIMSPOOL ON
WHENEVER SQLERROR EXIT FAILURE

ACCEPT mon_password CHAR PROMPT 'Password for the monitoring user c##cdcmon: ' HIDE

ALTER SESSION SET CONTAINER = CDB$ROOT;

CREATE USER c##cdcmon IDENTIFIED BY "&mon_password" CONTAINER = ALL;
GRANT CREATE SESSION, SET CONTAINER TO c##cdcmon CONTAINER = ALL;

GRANT SELECT ON v_$database             TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$instance             TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$pdbs                 TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$recovery_file_dest   TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$recovery_area_usage  TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$archive_dest_status  TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$archived_log         TO c##cdcmon CONTAINER = ALL;
GRANT SELECT ON v_$log_history          TO c##cdcmon CONTAINER = ALL;

-- Verification (read-only): the user must exist, be OPEN, and see the recovery area
SELECT username, account_status FROM dba_users WHERE username = 'C##CDCMON';
SELECT table_name FROM dba_tab_privs WHERE grantee = 'C##CDCMON' ORDER BY table_name;
