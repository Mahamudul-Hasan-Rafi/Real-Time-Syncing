/* ==================================================================================================
   dba_cdc_03_cdc_setup.sql  -  CDC SETUP FOR DEBEZIUM / LOGMINER   (online, no restart)
   Run as   : sqlplus / as sysdba @dba_cdc_03_cdc_setup.sql
   Requires : ARCHIVELOG mode already enabled.  The script refuses to run otherwise.
   Contains : no passwords - the CDC user's password is asked for when the script runs.
   Stops    : at the first error (WHENEVER SQLERROR), so nothing runs on top of a failed step.

   Step 1  Database-level supplemental logging and force logging       (CDB$ROOT)
   Step 2  Common user c##cdcuser and its privileges                    (CDB$ROOT)
   Step 3  Read access on the 18 captured tables                        (PDB)
   Step 4  One supplemental log group per captured table                (PDB)
   Step 5  Verification (read-only)

   Run Step 4 in a quiet period: it applies to redo written from now on.
   ================================================================================================== */
SET ECHO ON  VERIFY OFF  DEFINE ON  LINESIZE 200  PAGESIZE 100  TRIMSPOOL ON
WHENEVER SQLERROR EXIT FAILURE

DEFINE pdb_name = 'ORCLPDB1'               -- pluggable database that holds the application schema
DEFINE schema   = 'EMOB'                   -- schema whose tables are captured

ACCEPT cdc_password CHAR PROMPT 'Password for the CDC user c##cdcuser: ' HIDE

ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Guard: LogMiner needs ARCHIVELOG mode
BEGIN
  FOR r IN (SELECT log_mode FROM v$database WHERE log_mode <> 'ARCHIVELOG') LOOP
    RAISE_APPLICATION_ERROR(-20002, 'Database is in ' || r.log_mode || ' - run dba_cdc_02_archivelog.sql first');
  END LOOP;
END;
/

/* ---- Step 1: database-level logging ------------------------------------------------------------ */
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;    -- minimal level: LogMiner cannot read row changes without it
ALTER DATABASE FORCE LOGGING;                -- NOLOGGING operations would bypass the redo and be missed by CDC

/* ---- Step 2: CDC user (common user: LogMiner reads the redo of the whole CDB) ------------------ */
CREATE USER c##cdcuser IDENTIFIED BY "&cdc_password"
  DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users
  CONTAINER = ALL;

GRANT CREATE SESSION, SET CONTAINER             TO c##cdcuser CONTAINER = ALL;
GRANT LOGMINING                                 TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ANY TRANSACTION                    TO c##cdcuser CONTAINER = ALL;
GRANT SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO c##cdcuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_LOGMNR                    TO c##cdcuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D                  TO c##cdcuser CONTAINER = ALL;
GRANT CREATE TABLE, CREATE SEQUENCE             TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$DATABASE                     TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$LOG                          TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$LOG_HISTORY                  TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGFILE                      TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$ARCHIVED_LOG                 TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS          TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_LOGS                  TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS              TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS            TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$TRANSACTION                  TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$MYSTAT                       TO c##cdcuser CONTAINER = ALL;
GRANT SELECT ON V_$STATNAME                     TO c##cdcuser CONTAINER = ALL;
-- Not granted by design: SELECT ANY TABLE, FLASHBACK ANY TABLE, LOCK ANY TABLE, DBA.

/* ---- Step 3: read access on the captured tables only (snapshot and flashback) ------------------ */
ALTER SESSION SET CONTAINER = &pdb_name;

GRANT SELECT, FLASHBACK ON &schema..DISTRICT_INFO              TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_ACCOUNT_DAILY_BALANCE   TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_ACCOUNT_MST             TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_ACCOUNT_OPERATOR        TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_ADDRESS_MST             TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_CUSTOMER_MST            TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_DOCUMENT_MST            TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_PRODUCT_MST             TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..MB_TRANSACTION_DTL         TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..NOMINEE_MST                TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..STCODMAS                   TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..ST_AGENT_POINT             TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..ST_OPERATING_INSTRUCTION   TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..ST_POINT_CATEGORY          TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..ST_SECTOR_CODE             TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..ST_TRANSACTION_CODE        TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..UNION_INFO                 TO c##cdcuser;
GRANT SELECT, FLASHBACK ON &schema..UPAZILA_INFO               TO c##cdcuser;

/* ---- Step 4: supplemental log groups ------------------------------------------------------------
   ALWAYS = the group's columns are logged on every update of the row, not only when one of them changes.
   Columns = row identity (key) plus the columns the downstream consumer uses to locate related records.
   Group names are unique per schema, hence one name per table.                                       */
ALTER TABLE &schema..DISTRICT_INFO              ADD SUPPLEMENTAL LOG GROUP g_cdc_district_info              (DISTRICT_CODE_GOV, DISTRICT_CODE) ALWAYS;
ALTER TABLE &schema..MB_ACCOUNT_DAILY_BALANCE   ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_account_daily_balance   (AC_ID, VALUE_DATE) ALWAYS;
ALTER TABLE &schema..MB_ACCOUNT_MST             ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_account_mst             (AC_NO, AC_ID) ALWAYS;
ALTER TABLE &schema..MB_ACCOUNT_OPERATOR        ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_account_operator        (AC_ID, CUST_NO, STATUS) ALWAYS;
ALTER TABLE &schema..MB_ADDRESS_MST             ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_address_mst             (ADDRESS_ID, REF_NO) ALWAYS;
ALTER TABLE &schema..MB_CUSTOMER_MST            ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_customer_mst            (CUST_NO) ALWAYS;
ALTER TABLE &schema..MB_DOCUMENT_MST            ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_document_mst            (DOCUMENT_ID, CUST_NO) ALWAYS;
ALTER TABLE &schema..MB_PRODUCT_MST             ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_product_mst             (AC_TYPE_ID) ALWAYS;
ALTER TABLE &schema..MB_TRANSACTION_DTL         ADD SUPPLEMENTAL LOG GROUP g_cdc_mb_transaction_dtl         (TRAN_ID) ALWAYS;
ALTER TABLE &schema..NOMINEE_MST                ADD SUPPLEMENTAL LOG GROUP g_cdc_nominee_mst                (NOMINEE_ID, AC_ID) ALWAYS;
ALTER TABLE &schema..STCODMAS                   ADD SUPPLEMENTAL LOG GROUP g_cdc_stcodmas                   (HARCOD, SOFCOD) ALWAYS;
ALTER TABLE &schema..ST_AGENT_POINT             ADD SUPPLEMENTAL LOG GROUP g_cdc_st_agent_point             (POINT_ID) ALWAYS;
ALTER TABLE &schema..ST_OPERATING_INSTRUCTION   ADD SUPPLEMENTAL LOG GROUP g_cdc_st_operating_instruction   (OPR_INS) ALWAYS;
ALTER TABLE &schema..ST_POINT_CATEGORY          ADD SUPPLEMENTAL LOG GROUP g_cdc_st_point_category          (POINT_CATEGORY_ID) ALWAYS;
ALTER TABLE &schema..ST_SECTOR_CODE             ADD SUPPLEMENTAL LOG GROUP g_cdc_st_sector_code             (SRLNO) ALWAYS;
ALTER TABLE &schema..ST_TRANSACTION_CODE        ADD SUPPLEMENTAL LOG GROUP g_cdc_st_transaction_code        (TRAN_CODE) ALWAYS;
ALTER TABLE &schema..UNION_INFO                 ADD SUPPLEMENTAL LOG GROUP g_cdc_union_info                 (UNION_CODE_GOV, DISTRICT_CODE, UPAZILA_CODE, UNION_CODE) ALWAYS;
ALTER TABLE &schema..UPAZILA_INFO               ADD SUPPLEMENTAL LOG GROUP g_cdc_upazila_info               (UPAZILA_CODE_GOV, DISTRICT_CODE, UPAZILA_CODE) ALWAYS;

/* ---- Step 5: verification (read-only) ------------------------------------------------------------ */
COLUMN username       FORMAT A14
COLUMN account_status FORMAT A16
COLUMN table_name     FORMAT A28
COLUMN log_group_name FORMAT A32
COLUMN log_group_type FORMAT A18

-- Expect: C##CDCUSER, OPEN
SELECT username, account_status FROM dba_users WHERE username = 'C##CDCUSER';

-- Expect: 18 rows, every LOG_GROUP_TYPE = USER LOG GROUP, every ALWAYS = ALWAYS
SELECT table_name, log_group_name, log_group_type, always
  FROM dba_log_groups
 WHERE owner = '&schema'
 ORDER BY table_name;

-- Expect: 18
SELECT COUNT(*) AS tables_granted
  FROM dba_tab_privs
 WHERE grantee = 'C##CDCUSER' AND owner = '&schema' AND privilege = 'SELECT'
   AND table_name IN ('DISTRICT_INFO','MB_ACCOUNT_DAILY_BALANCE','MB_ACCOUNT_MST','MB_ACCOUNT_OPERATOR','MB_ADDRESS_MST','MB_CUSTOMER_MST','MB_DOCUMENT_MST','MB_PRODUCT_MST','MB_TRANSACTION_DTL','NOMINEE_MST','STCODMAS','ST_AGENT_POINT','ST_OPERATING_INSTRUCTION','ST_POINT_CATEGORY','ST_SECTOR_CODE','ST_TRANSACTION_CODE','UNION_INFO','UPAZILA_INFO');

-- Expect: SUP_MIN = YES, SUP_ALL = NO, FORCE_LOGGING = YES, LOG_MODE = ARCHIVELOG
SELECT supplemental_log_data_min AS sup_min, supplemental_log_data_all AS sup_all, force_logging, log_mode
  FROM v$database;

-- Expect: PASSWORD_LIFE_TIME of the user's profile is UNLIMITED, or the password is rotated before it expires
SELECT p.limit AS password_life_time
  FROM dba_users u JOIN dba_profiles p ON p.profile = u.profile AND p.resource_name = 'PASSWORD_LIFE_TIME'
 WHERE u.username = 'C##CDCUSER';