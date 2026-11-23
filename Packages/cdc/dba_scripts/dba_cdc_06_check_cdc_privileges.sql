/* ==================================================================================================
   dba_cdc_06_check_cdc_privileges.sql  -  DOES THE DEBEZIUM USER HAVE EVERYTHING IT NEEDS?   (read-only)
   Run as   : sqlplus / as sysdba @dba_cdc_06_check_cdc_privileges.sql      (connected to CDB$ROOT)
   Checks   : 1. the user exists and is OPEN in CDB$ROOT and in the PDB
              2. every privilege granted by dba_cdc_03_cdc_setup.sql, in CDB$ROOT and in the PDB
                 (CONTAINER = ALL grants must reach both). Privileges inherited through a role also count.
              3. quota on the default tablespace in the PDB (Debezium creates its LOG_MINING_FLUSH table there)
              4. SELECT and FLASHBACK on each captured table in the PDB
              One row per check, problems first: MISSING / NO TABLE, then OK / VIA ROLE.
   Changes  : nothing. Safe on production.
   Edit     : the DEFINEs below, and the table list in section 4 if the captured tables change.
   ================================================================================================== */
SET ECHO OFF  VERIFY OFF  DEFINE ON  FEEDBACK OFF  LINESIZE 200  PAGESIZE 200  TRIMSPOOL ON
WHENEVER SQLERROR EXIT FAILURE

-- cdc_user: the connector's database.user, pdb_name: its database.pdb.name, schema: owner of the tables (all upper case)
DEFINE cdc_user = 'C##DEBEZIUM'
DEFINE pdb_name = 'ORCLPDB1'
DEFINE schema   = 'EMOB'

ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Guard: the CDB_* views only show the PDB to SYSDBA (or a user with CONTAINER_DATA = ALL). Without it every
-- PDB check would wrongly report MISSING / NO TABLE, so stop instead.
BEGIN
  FOR r IN (SELECT 1 FROM dual WHERE NOT EXISTS (SELECT 1 FROM v$containers WHERE name = '&pdb_name' AND open_mode LIKE 'READ%')) LOOP
    RAISE_APPLICATION_ERROR(-20006, 'PDB &pdb_name is not visible or not open in this session - run as SYSDBA (sqlplus / as sysdba)');
  END LOOP;
END;
/

COLUMN container      FORMAT A10
COLUMN kind           FORMAT A8
COLUMN check_on       FORMAT A48
COLUMN status         FORMAT A8
COLUMN username       FORMAT A14
COLUMN account_status FORMAT A16
COLUMN detail         FORMAT A60

PROMPT
PROMPT ===== 1. The user =====================================================================
SELECT c.name AS container, NVL(u.username, '&cdc_user') AS username, NVL(u.account_status, '(does not exist)') AS account_status, u.common,
       CASE WHEN u.account_status = 'OPEN' THEN 'OK' ELSE 'MISSING' END AS status
  FROM v$containers c LEFT JOIN cdb_users u ON u.con_id = c.con_id AND u.username = '&cdc_user'
 WHERE c.name IN ('CDB$ROOT', '&pdb_name')
 ORDER BY c.con_id;

PROMPT
PROMPT ===== 2. Privileges needed in CDB$ROOT and in &pdb_name ==============================
WITH expected (kind, priv, obj) AS (
            SELECT 'SYSTEM', 'CREATE SESSION',          NULL FROM dual
  UNION ALL SELECT 'SYSTEM', 'SET CONTAINER',           NULL FROM dual
  UNION ALL SELECT 'SYSTEM', 'LOGMINING',               NULL FROM dual
  UNION ALL SELECT 'SYSTEM', 'SELECT ANY TRANSACTION',  NULL FROM dual
  UNION ALL SELECT 'SYSTEM', 'CREATE TABLE',            NULL FROM dual
  UNION ALL SELECT 'SYSTEM', 'CREATE SEQUENCE',         NULL FROM dual
  UNION ALL SELECT 'ROLE',   'SELECT_CATALOG_ROLE',     NULL FROM dual
  UNION ALL SELECT 'ROLE',   'EXECUTE_CATALOG_ROLE',    NULL FROM dual
  UNION ALL SELECT 'OBJECT', 'EXECUTE', 'DBMS_LOGMNR'          FROM dual
  UNION ALL SELECT 'OBJECT', 'EXECUTE', 'DBMS_LOGMNR_D'        FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$DATABASE'          FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$LOG'               FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$LOG_HISTORY'       FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$LOGFILE'           FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$ARCHIVED_LOG'      FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$ARCHIVE_DEST_STATUS' FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$LOGMNR_LOGS'       FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$LOGMNR_CONTENTS'   FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$LOGMNR_PARAMETERS' FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$TRANSACTION'       FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$MYSTAT'            FROM dual
  UNION ALL SELECT 'OBJECT', 'SELECT',  'V_$STATNAME'          FROM dual
), cons AS (
  SELECT con_id, name FROM v$containers WHERE name IN ('CDB$ROOT', '&pdb_name')
), roles AS (          -- every role the user holds in each container, including roles granted to those roles
  SELECT DISTINCT con_id, granted_role AS role FROM cdb_role_privs
   START WITH grantee = '&cdc_user'
   CONNECT BY NOCYCLE PRIOR granted_role = grantee AND PRIOR con_id = con_id
), holders AS (        -- direct = the user itself, via = one of its roles
  SELECT con_id, '&cdc_user' AS grantee, 'direct' AS how FROM cons
  UNION ALL SELECT con_id, role, 'via' FROM roles
), found AS (
  SELECT c.name AS container, e.kind, e.priv, e.obj,
         (SELECT MIN(h.how) FROM holders h WHERE h.con_id = c.con_id AND (
              (e.kind = 'SYSTEM' AND EXISTS (SELECT 1 FROM cdb_sys_privs p WHERE p.con_id = c.con_id AND p.grantee = h.grantee AND p.privilege = e.priv))
           OR (e.kind = 'ROLE'   AND EXISTS (SELECT 1 FROM cdb_role_privs r WHERE r.con_id = c.con_id AND r.grantee = h.grantee AND r.granted_role = e.priv))
           OR (e.kind = 'OBJECT' AND EXISTS (SELECT 1 FROM cdb_tab_privs t WHERE t.con_id = c.con_id AND t.grantee = h.grantee
                                              AND t.owner = 'SYS' AND t.table_name = e.obj AND t.privilege = e.priv))
         )) AS how
    FROM expected e CROSS JOIN cons c
)
SELECT container, kind,
       CASE WHEN obj IS NULL THEN priv ELSE priv || ' ON ' || obj END AS check_on,
       CASE how WHEN 'direct' THEN 'OK' WHEN 'via' THEN 'VIA ROLE' ELSE 'MISSING' END AS status
  FROM found
 ORDER BY CASE WHEN how IS NULL THEN 0 ELSE 1 END, container, kind, check_on;

PROMPT
PROMPT ===== 3. Space for Debezium's LOG_MINING_FLUSH table (in &pdb_name) ===================
-- With database.pdb.name set, Debezium creates this table in the PDB, in the user's default tablespace there.
SELECT '&pdb_name' AS container,
       'default tablespace ' || u.default_tablespace AS check_on,
       CASE
         WHEN EXISTS (SELECT 1 FROM cdb_ts_quotas q WHERE q.con_id = u.con_id AND q.username = u.username
                        AND q.tablespace_name = u.default_tablespace AND (q.max_bytes = -1 OR q.max_bytes > q.bytes)) THEN 'OK'
         WHEN EXISTS (SELECT 1 FROM cdb_sys_privs p WHERE p.con_id = u.con_id AND p.grantee = u.username AND p.privilege = 'UNLIMITED TABLESPACE') THEN 'OK'
         ELSE 'MISSING'
       END AS status,
       CASE WHEN EXISTS (SELECT 1 FROM cdb_tables t WHERE t.con_id = u.con_id AND t.owner = u.username AND t.table_name = 'LOG_MINING_FLUSH')
            THEN 'LOG_MINING_FLUSH exists: the connector has started at least once'
            ELSE 'LOG_MINING_FLUSH not created yet (made at the connector''s first start)' END AS detail
  FROM cdb_users u
 WHERE u.username = '&cdc_user' AND u.con_id = (SELECT con_id FROM v$containers WHERE name = '&pdb_name');

PROMPT
PROMPT ===== 4. SELECT and FLASHBACK on the captured tables in &pdb_name =====================
WITH tabs (table_name) AS (
            SELECT 'MB_ACCOUNT_MST'           FROM dual
  UNION ALL SELECT 'UNION_INFO'               FROM dual
  UNION ALL SELECT 'UPAZILA_INFO'             FROM dual
  UNION ALL SELECT 'DISTRICT_INFO'            FROM dual
  UNION ALL SELECT 'MB_ACCOUNT_DAILY_BALANCE' FROM dual
  UNION ALL SELECT 'MB_ACCOUNT_OPERATOR'      FROM dual
  UNION ALL SELECT 'MB_ADDRESS_MST'           FROM dual
  UNION ALL SELECT 'MB_CUSTOMER_MST'          FROM dual
  UNION ALL SELECT 'MB_DOCUMENT_MST'          FROM dual
  UNION ALL SELECT 'MB_PRODUCT_MST'           FROM dual
  UNION ALL SELECT 'STCODMAS'                 FROM dual
  UNION ALL SELECT 'ST_AGENT_POINT'           FROM dual
  UNION ALL SELECT 'ST_OPERATING_INSTRUCTION' FROM dual
  UNION ALL SELECT 'ST_POINT_CATEGORY'        FROM dual
  UNION ALL SELECT 'ST_SECTOR_CODE'           FROM dual
  UNION ALL SELECT 'ST_TRANSACTION_CODE'      FROM dual
  UNION ALL SELECT 'NOMINEE_MST'              FROM dual
), privs (priv) AS (SELECT 'SELECT' FROM dual UNION ALL SELECT 'FLASHBACK' FROM dual
), pdb AS (SELECT con_id FROM v$containers WHERE name = '&pdb_name'
), result AS (
  SELECT '&pdb_name' AS container, 'TABLE' AS kind, p.priv || ' ON &schema..' || t.table_name AS check_on,
         CASE
           WHEN NOT EXISTS (SELECT 1 FROM cdb_tables x, pdb WHERE x.con_id = pdb.con_id AND x.owner = '&schema' AND x.table_name = t.table_name) THEN 'NO TABLE'
           WHEN EXISTS (SELECT 1 FROM cdb_tab_privs g, pdb WHERE g.con_id = pdb.con_id AND g.grantee = '&cdc_user'
                          AND g.owner = '&schema' AND g.table_name = t.table_name AND g.privilege = p.priv) THEN 'OK'
           ELSE 'MISSING'
         END AS status
    FROM tabs t CROSS JOIN privs p
)
SELECT container, kind, check_on, status FROM result ORDER BY status, check_on;

PROMPT
PROMPT ===== How to read the result ==========================================================
PROMPT OK / VIA ROLE : present (VIA ROLE = inherited through a role, which Debezium can use).
PROMPT MISSING       : re-run that GRANT from dba_cdc_03_cdc_setup.sql.
PROMPT                 OK in CDB$ROOT but MISSING in the PDB = granted without CONTAINER = ALL.
PROMPT                 Quota MISSING = ALTER USER &cdc_user QUOTA UNLIMITED ON <default tablespace> CONTAINER = ALL;
PROMPT NO TABLE      : the table does not exist in the PDB (check the name and the schema).
