export type Risk = "read-only" | "change" | "downtime";

export type Command = {
  /** Where to run it, e.g. "SQL*Plus as SYSDBA, connected to CDB$ROOT". */
  where: string;
  lang: "sql" | "bash";
  code: string;
};

export type Step = {
  id: string;
  title: string;
  summary: string;
  risk: Risk;
  /** Condition for running the step. Absent means: always run it. */
  when?: string;
  /** The part of the dba_scripts file that does the same thing. */
  script?: string;
  commands: Command[];
  expect?: string;
  /** Plain text. [[step-id]] becomes a link to that step. */
  notes?: string[];
};

export type Section = {
  id: string;
  title: string;
  intro: string;
  steps: Step[];
};

export type Variable = {
  key: string;
  label: string;
  value: string;
  help?: string;
  secret?: boolean;
};

/** Values the reader can edit. Commands use {{KEY}}. */
export const VARIABLES: Variable[] = [
  { key: "PDB_NAME", label: "PDB name", value: "ORCLPDB1", help: "pluggable database that holds the application schema" },
  { key: "SCHEMA", label: "Schema", value: "EMOB", help: "schema whose tables are captured" },
  { key: "CDC_USER", label: "CDC user", value: "c##debezium", help: "common user, must start with c##" },
  { key: "CDC_PASSWORD", label: "CDC password", value: "<password>", secret: true, help: "never saved by this page" },
  { key: "FRA_PATH", label: "Recovery area path", value: "/u01/oracle_logs", help: "OS directory for archive logs" },
  { key: "FRA_SIZE", label: "Recovery area size", value: "15G" },
  { key: "MON_USER", label: "Monitoring user", value: "c##cdcmon", help: "read-only, used by Grafana (section 8)" },
  { key: "MON_PASSWORD", label: "Monitoring password", value: "<password>", secret: true },
  { key: "ORACLE_HOST", label: "Oracle host", value: "10.11.200.37" },
  { key: "ORACLE_PORT", label: "Oracle port", value: "1521" },
  { key: "CDB_SERVICE", label: "CDB service name", value: "orclcdb", help: "the connector's database.dbname" },
  { key: "CONNECT_URL", label: "Kafka Connect URL", value: "http://10.11.200.109:8083" },
  { key: "CONNECTOR", label: "Connector name", value: "emob-oracle-cdc" },
  { key: "KEEP_DAYS", label: "Archive logs kept (days)", value: "3", help: "section 10" },
  { key: "MINUTES", label: "LogMiner test window (minutes)", value: "60", help: "step 7.3" },
  { key: "ORACLE_HOME", label: "ORACLE_HOME on the DB server", value: "/u01/app/oracle/product/19.0.0/dbhome_1" },
  { key: "ORACLE_SID", label: "ORACLE_SID of the CDB", value: "ORCLCDB" },
];

/** The captured tables and the columns logged for each one. Same list as dba_scripts/dba_cdc_03_cdc_setup.sql. */
export const TABLES: { name: string; columns: string[]; captured?: boolean }[] = [
  { name: "DISTRICT_INFO", columns: ["DISTRICT_CODE_GOV", "DISTRICT_CODE"] },
  { name: "MB_ACCOUNT_DAILY_BALANCE", columns: ["AC_ID", "VALUE_DATE"] },
  { name: "MB_ACCOUNT_MST", columns: ["AC_NO", "AC_ID"] },
  { name: "MB_ACCOUNT_OPERATOR", columns: ["AC_ID", "CUST_NO", "STATUS"] },
  { name: "MB_ADDRESS_MST", columns: ["ADDRESS_ID", "REF_NO"] },
  { name: "MB_CUSTOMER_MST", columns: ["CUST_NO"] },
  { name: "MB_DOCUMENT_MST", columns: ["DOCUMENT_ID", "CUST_NO"] },
  { name: "MB_PRODUCT_MST", columns: ["AC_TYPE_ID"] },
  { name: "MB_TRANSACTION_DTL", columns: ["TRAN_ID"], captured: false }, // set up, but excluded from the connector in this phase
  { name: "NOMINEE_MST", columns: ["NOMINEE_ID", "AC_ID"] },
  { name: "STCODMAS", columns: ["HARCOD", "SOFCOD"] },
  { name: "ST_AGENT_POINT", columns: ["POINT_ID"] },
  { name: "ST_OPERATING_INSTRUCTION", columns: ["OPR_INS"] },
  { name: "ST_POINT_CATEGORY", columns: ["POINT_CATEGORY_ID"] },
  { name: "ST_SECTOR_CODE", columns: ["SRLNO"] },
  { name: "ST_TRANSACTION_CODE", columns: ["TRAN_CODE"] },
  { name: "UNION_INFO", columns: ["UNION_CODE_GOV", "DISTRICT_CODE", "UPAZILA_CODE", "UNION_CODE"] },
  { name: "UPAZILA_INFO", columns: ["UPAZILA_CODE_GOV", "DISTRICT_CODE", "UPAZILA_CODE"] },
];

const pad = (s: string, n: number) => s.padEnd(n);
const W = Math.max(...TABLES.map((t) => t.name.length));

const TABLE_GRANTS = TABLES.map(
  (t) => `grant select, flashback on {{SCHEMA}}.${pad(t.name, W)} to {{CDC_USER}};`,
).join("\n");

const LOG_GROUPS = TABLES.map(
  (t) =>
    `alter table {{SCHEMA}}.${pad(t.name, W)} add supplemental log group g_cdc_kafka_${t.name.toLowerCase()} (${t.columns.join(", ")}) always;`,
).join("\n");

const TABLE_NAME_LIST = TABLES.map((t) => `'${t.name}'`).join(",");
/** Tables the connector captures in this phase (all set-up tables except the ones marked captured: false). */
export const CAPTURED = TABLES.filter((t) => t.captured !== false);
const NOT_CAPTURED = TABLES.filter((t) => t.captured === false).map((t) => t.name).join(", ");
export const INCLUDE_LIST = CAPTURED.map((t) => `{{SCHEMA}}.${t.name}`).join(",");

export const INTRO = {
  title: "Oracle CDC database setup runbook",
  audience: "DBAs preparing an Oracle database for Debezium LogMiner change capture into Kafka.",
  summary:
    "Run the sections in order. Each step says where to run it, whether it changes anything, what you should see, and which part of the dba_scripts files does the same job. The scripts are the source of truth: this runbook shows the same statements so you can run them one at a time and check each result.",
  before: [
    "A SYSDBA login on the Oracle server, and a maintenance window if section 2 applies (it restarts the database).",
    "The database is a CDB with the application schema in a PDB. The commands use ORCLPDB1, EMOB, c##debezium and /u01/oracle_logs: replace them with your own values, and replace every password placeholder.",
    "Sections 5 and 6 list the captured tables. If that list changes, change both sections and the connector's table.include.list (section 9).",
  ],
  scripts: [
    { file: "dba_cdc_01_check.sql", does: "Read-only check of the current state", covers: "Section 1" },
    { file: "dba_cdc_02_archivelog.sql", does: "Recovery area, enable ARCHIVELOG (restarts the database). Only for a database that does not archive yet", covers: "Section 2 (2.2 and 2.3)" },
    { file: "dba_cdc_03_cdc_setup.sql", does: "Database logging, CDC user, table grants, log groups, verification", covers: "Sections 3 to 7" },
    { file: "dba_cdc_04_monitoring_user.sql", does: "Read-only user for the Grafana exporter", covers: "Section 8" },
    { file: "dba_cdc_05_archivelog_cleanup.sh", does: "Scheduled, CDC-safe archive log cleanup (RMAN)", covers: "Section 10" },
    { file: "dba_cdc_06_check_cdc_privileges.sql", does: "Read-only check of every privilege the CDC user needs", covers: "Step 7.2" },
    { file: "dba_cdc_07_logminer_capture_test.sql", does: "Read-only test that LogMiner sees changes on the captured tables", covers: "Step 7.3" },
  ],
};

export const RUNBOOK: Section[] = [
  {
    id: "check",
    title: "Check the current state",
    intro: "Read-only. The result decides whether section 2 is needed.",
    steps: [
      {
        id: "check-state",
        title: "Read archive mode, logging, recovery area and PDBs",
        summary: "Shows what is already in place so you only run the steps that are needed.",
        risk: "read-only",
        script: "dba_cdc_01_check.sql",
        commands: [
          {
            where: "SQL*Plus as SYSDBA",
            lang: "sql",
            code: `set linesize 200 pagesize 100
select name, cdb, log_mode, force_logging, supplemental_log_data_min, supplemental_log_data_all
  from v$database;

show parameter db_recovery_file_dest

select name, open_mode from v$pdbs;`,
          },
        ],
        expect: "CDB = YES and your PDB is READ WRITE. Note LOG_MODE.",
        notes: [
          "Whatever LOG_MODE shows, continue with [[archive-check]]: it decides which other steps of section 2 apply.",
        ],
      },
    ],
  },
  {
    id: "archive",
    title: "Archive logging",
    intro:
      "LogMiner reads archived redo logs, and Debezium refuses to start unless a LOCAL archive destination is VALID. A production database is normally archiving already: then this section is only the read-only check in 2.1, and the existing archive setup must not be changed.",
    steps: [
      {
        id: "archive-check",
        title: "Check the existing archive setup",
        summary: "Read-only. Shows whether the database already archives and whether Debezium can use the destination.",
        risk: "read-only",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `set linesize 200 pagesize 100
col dest_name format a22
col destination format a40
col error format a50
select log_mode from v$database;

select dest_id, dest_name, status, type, destination, error
  from v$archive_dest_status
 where status <> 'INACTIVE';

select round(space_limit/1024/1024/1024, 1)       limit_gb,
       round(space_used/1024/1024/1024, 1)        used_gb,
       round(space_reclaimable/1024/1024/1024, 1) reclaimable_gb
  from v$recovery_file_dest;`,
          },
        ],
        expect: "LOG_MODE = ARCHIVELOG and at least one row with TYPE = LOCAL and STATUS = VALID.",
        notes: [
          "ARCHIVELOG with one LOCAL destination VALID (normal for production): nothing to change. Skip the rest of section 2 and go to [[db-logging]].",
          "ARCHIVELOG with several LOCAL destinations VALID: no database change. Tell the platform team which one Debezium should read, for the connector setting log.mining.archive.destination.name (for example LOG_ARCHIVE_DEST_1).",
          "ARCHIVELOG but no LOCAL destination VALID (only remote or standby destinations, or one in ERROR): stop and agree a fix with the DBA team that owns the archive setup. See [[archive-dest]].",
          "NOARCHIVELOG (new or test database): do [[recovery-dir]], [[enable-archivelog]] and [[archive-dest]].",
          "On a database that already archives, never change db_recovery_file_dest or log_archive_dest_n: backups, retention and standby databases depend on them.",
        ],
      },
      {
        id: "recovery-dir",
        title: "Create the recovery area directory",
        summary: "The OS directory must exist and belong to the oracle user before Oracle is pointed at it.",
        risk: "change",
        when: "Only if LOG_MODE = NOARCHIVELOG (a database that does not archive yet)",
        commands: [
          {
            where: "Shell on the database server, as root",
            lang: "bash",
            code: `mkdir -p {{FRA_PATH}}
chown oracle:oinstall {{FRA_PATH}}
chmod 750 {{FRA_PATH}}`,
          },
        ],
        notes: ["The group name (oinstall) can differ on your server."],
      },
      {
        id: "enable-archivelog",
        title: "Set the recovery area and enable ARCHIVELOG",
        summary: "Set the size before the location. Switching the log mode needs a restart.",
        risk: "downtime",
        when: "Only if LOG_MODE = NOARCHIVELOG",
        script: "dba_cdc_02_archivelog.sql",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `alter system set db_recovery_file_dest_size = {{FRA_SIZE}}   scope = both;
alter system set db_recovery_file_dest      = '{{FRA_PATH}}' scope = both;

shutdown immediate;
startup mount;
alter database archivelog;
alter database open;

archive log list`,
          },
        ],
        expect: "Database log mode = Archive Mode, Automatic archival = Enabled.",
        notes: ["All sessions are disconnected. Schedule it and tell the application owners."],
      },
      {
        id: "archive-dest",
        title: "Point the archive destination at the recovery area",
        summary:
          "Sends archive logs to the recovery area set in the previous step, then verifies it. This also repaired an ERROR destination (ORA-16032) on the test database.",
        risk: "change",
        when: "Only after [[enable-archivelog]], or to repair a destination in ERROR on a database whose archive setup you own. Never on a production database that already archives",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `select value from v$parameter where name = 'log_archive_dest_1';   -- record the current value first

alter system set log_archive_dest_1 = 'LOCATION=USE_DB_RECOVERY_FILE_DEST' scope = both;

select dest_id, dest_name, status, type, destination, error
  from v$archive_dest
 where dest_id = 1;`,
          },
        ],
        expect: "STATUS = VALID, TYPE = LOCAL, ERROR empty.",
        notes: [
          "Keep the value recorded by the first query: it is how you undo the change.",
          "Do not continue to [[db-logging]] until a LOCAL destination is VALID. Re-run [[archive-check]] to confirm.",
        ],
      },
    ],
  },
  {
    id: "logging",
    title: "Database-level logging",
    intro: "Two switches for the whole database. Online, no restart.",
    steps: [
      {
        id: "db-logging",
        title: "Enable minimal supplemental logging and force logging",
        summary:
          "Minimal supplemental logging lets LogMiner read row changes. FORCE LOGGING stops NOLOGGING operations from bypassing the redo, which CDC would miss.",
        risk: "change",
        script: "dba_cdc_03_cdc_setup.sql, Step 1",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `alter session set container = CDB$ROOT;
alter database add supplemental log data;
alter database force logging;

select supplemental_log_data_min, force_logging, log_mode from v$database;`,
          },
        ],
        expect: "SUPPLEMENTAL_LOG_DATA_MIN = YES, FORCE_LOGGING = YES, LOG_MODE = ARCHIVELOG.",
        notes: ["The per-table log groups come later, in [[log-groups]]."],
      },
    ],
  },
  {
    id: "user",
    title: "CDC user and privileges",
    intro:
      "A common user, because LogMiner reads the redo of the whole CDB. It gets only what Debezium needs and no broad ANY privileges.",
    steps: [
      {
        id: "create-user",
        title: "Create the CDC user",
        summary: "Common user in all containers, on the USERS tablespace.",
        risk: "change",
        script: "dba_cdc_03_cdc_setup.sql, Step 2",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `alter session set container = CDB$ROOT;

create user {{CDC_USER}} identified by "{{CDC_PASSWORD}}"
  default tablespace users
  quota unlimited on users
  container = all;`,
          },
        ],
        notes: [
          "The script asks for the password at run time, so it never sits in the file. Replace the placeholder with the real password, and do not save it in this document.",
        ],
      },
      {
        id: "grant-privileges",
        title: "Grant the LogMiner privileges",
        summary: "Session, LogMiner, and read access on the V$ views Debezium queries.",
        risk: "change",
        script: "dba_cdc_03_cdc_setup.sql, Step 2",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `grant create session, set container             to {{CDC_USER}} container = all;
grant logmining                                 to {{CDC_USER}} container = all;
grant select any transaction                    to {{CDC_USER}} container = all;
grant select_catalog_role, execute_catalog_role to {{CDC_USER}} container = all;
grant execute on dbms_logmnr                    to {{CDC_USER}} container = all;
grant execute on dbms_logmnr_d                  to {{CDC_USER}} container = all;
grant create table, create sequence             to {{CDC_USER}} container = all;
grant select on v_$database                     to {{CDC_USER}} container = all;
grant select on v_$log                          to {{CDC_USER}} container = all;
grant select on v_$log_history                  to {{CDC_USER}} container = all;
grant select on v_$logfile                      to {{CDC_USER}} container = all;
grant select on v_$archived_log                 to {{CDC_USER}} container = all;
grant select on v_$archive_dest_status          to {{CDC_USER}} container = all;
grant select on v_$logmnr_logs                  to {{CDC_USER}} container = all;
grant select on v_$logmnr_contents              to {{CDC_USER}} container = all;
grant select on v_$logmnr_parameters            to {{CDC_USER}} container = all;
grant select on v_$transaction                  to {{CDC_USER}} container = all;
grant select on v_$mystat                       to {{CDC_USER}} container = all;
grant select on v_$statname                     to {{CDC_USER}} container = all;`,
          },
        ],
        notes: [
          "Not granted by design: SELECT ANY TABLE, FLASHBACK ANY TABLE, LOCK ANY TABLE, DBA. Access to the data tables is given per table in [[table-grants]].",
        ],
      },
    ],
  },
  {
    id: "tables",
    title: "Read access on the captured tables",
    intro: `Debezium reads the ${TABLES.length} captured tables for the initial schema and for flashback. Grant on those tables only.`,
    steps: [
      {
        id: "table-grants",
        title: "Grant SELECT and FLASHBACK on each captured table",
        summary: "Run in the PDB, as the privilege has to be given where the tables live.",
        risk: "change",
        script: "dba_cdc_03_cdc_setup.sql, Step 3",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to the PDB",
            lang: "sql",
            code: `alter session set container = {{PDB_NAME}};

${TABLE_GRANTS}`,
          },
        ],
        expect: `${TABLES.length} grants succeed.`,
        notes: ["The user must exist first ([[create-user]])."],
      },
    ],
  },
  {
    id: "loggroups",
    title: "Supplemental log groups",
    intro:
      "Per-table logging of only the columns CDC needs. This replaces ALL COLUMNS logging, which writes every column of every changed row into the redo and fills the archive area for no benefit.",
    steps: [
      {
        id: "log-groups",
        title: "Add one supplemental log group per captured table",
        summary:
          "Each group holds the row key plus the columns the downstream consumer uses to find related records. ALWAYS means those columns are logged on every update of the row, not only when one of them changes.",
        risk: "change",
        when: "After [[db-logging]] and [[table-grants]]. Run it in a quiet period",
        script: "dba_cdc_03_cdc_setup.sql, Step 4",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to the PDB",
            lang: "sql",
            code: `alter session set container = {{PDB_NAME}};

${LOG_GROUPS}`,
          },
        ],
        expect: `${TABLES.length} statements succeed. Verify in [[verify]].`,
        notes: [
          "Applies to redo written from now on. Changes made before this step carry only the changed columns.",
          "Group names are unique per schema, so each table has its own name (g_cdc_kafka_ followed by the table name).",
          "Do not use ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS on these tables.",
        ],
      },
      {
        id: "change-log-group",
        title: "Change the columns of an existing group, or add a table",
        summary: "A log group cannot be edited. Drop it and add it again with the new column list.",
        risk: "change",
        when: "Only when the captured columns or tables change",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to the PDB",
            lang: "sql",
            code: `alter session set container = {{PDB_NAME}};

-- example: add a column to MB_CUSTOMER_MST
alter table {{SCHEMA}}.MB_CUSTOMER_MST drop supplemental log group g_cdc_kafka_mb_customer_mst;
alter table {{SCHEMA}}.MB_CUSTOMER_MST add supplemental log group g_cdc_kafka_mb_customer_mst (CUST_NO, <new_column>) always;`,
          },
        ],
        notes: [
          "For a new table, also run [[table-grants]] for it and add it to the connector's table.include.list ([[handoff]]).",
        ],
      },
    ],
  },
  {
    id: "verify-all",
    title: "Verify the setup",
    intro: "Read-only. Run it after sections 3 to 6 and keep the output as the hand-over evidence.",
    steps: [
      {
        id: "verify",
        title: "Check the user, log groups, table grants and database flags",
        summary: "Four queries, each with its expected result.",
        risk: "read-only",
        script: "dba_cdc_03_cdc_setup.sql, Step 5",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to the PDB",
            lang: "sql",
            code: `alter session set container = {{PDB_NAME}};
set linesize 200 pagesize 100
col username format a14
col account_status format a16
col table_name format a28
col log_group_name format a32
col log_group_type format a18

-- 1. the CDC user
select username, account_status from dba_users where username = upper('{{CDC_USER}}');

-- 2. the log groups
select table_name, log_group_name, log_group_type, always
  from dba_log_groups
 where owner = '{{SCHEMA}}'
 order by table_name;

-- 3. the table grants
select count(*) as tables_granted
  from dba_tab_privs
 where grantee = upper('{{CDC_USER}}') and owner = '{{SCHEMA}}' and privilege = 'SELECT'
   and table_name in (${TABLE_NAME_LIST});

-- 4. the database flags
select supplemental_log_data_min as sup_min, supplemental_log_data_all as sup_all, force_logging, log_mode
  from v$database;`,
          },
        ],
        expect: `1) the user, ACCOUNT_STATUS = OPEN. 2) ${TABLES.length} rows, LOG_GROUP_TYPE = USER LOG GROUP, ALWAYS = ALWAYS. 3) ${TABLES.length}. 4) SUP_MIN = YES, SUP_ALL = NO, FORCE_LOGGING = YES, LOG_MODE = ARCHIVELOG.`,
        notes: [
          "SUP_ALL = NO is correct: it confirms nobody enabled ALL COLUMNS logging at database level.",
          "The archive destination was already checked in [[archive-check]].",
        ],
      },
      {
        id: "check-privileges",
        title: "Check every privilege of the CDC user",
        summary:
          "One row per requirement, problems first: the user in both containers, the 22 LogMiner privileges in CDB$ROOT and in the PDB, quota for Debezium's LOG_MINING_FLUSH table, and SELECT and FLASHBACK on each captured table.",
        risk: "read-only",
        script: "dba_cdc_06_check_cdc_privileges.sql",
        commands: [
          {
            where: "Edit the DEFINE lines at the top of the script (values in upper case)",
            lang: "sql",
            code: `DEFINE cdc_user = '{{CDC_USER_UC}}'
DEFINE pdb_name = '{{PDB_NAME_UC}}'
DEFINE schema   = '{{SCHEMA}}'`,
          },
          {
            where: "Shell on the database server, as oracle",
            lang: "bash",
            code: `sqlplus / as sysdba @dba_cdc_06_check_cdc_privileges.sql`,
          },
        ],
        expect: "Every row OK or VIA ROLE. Section 3 says LOG_MINING_FLUSH exists once the connector has started at least once.",
        notes: [
          "It must run as SYSDBA: as any other user it stops with ORA-20006, because the PDB checks would be wrong.",
          "OK in CDB$ROOT but MISSING in the PDB: the GRANT was given without CONTAINER = ALL. Re-run it from [[grant-privileges]].",
          "Quota MISSING: ALTER USER {{CDC_USER}} QUOTA UNLIMITED ON USERS CONTAINER = ALL;",
          `Section 4 of the script lists the ${CAPTURED.length} captured tables. Edit its table list when the captured tables change.`,
        ],
      },
      {
        id: "logminer-test",
        title: "Test that LogMiner captures changes on the captured tables",
        summary:
          "Does what Debezium does, as the CDC user: adds the recent archive and online redo logs to a LogMiner session, starts it, and counts committed changes per captured table, with sample redo.",
        risk: "read-only",
        script: "dba_cdc_07_logminer_capture_test.sql",
        commands: [
          {
            where: "Optional, so there is something to find: one no-op change as the schema owner, in another session",
            lang: "sql",
            code: `alter session set container = {{PDB_NAME}};
update {{SCHEMA}}.ST_OPERATING_INSTRUCTION set OPR_INS = OPR_INS where rownum = 1;
commit;`,
          },
          {
            where: "Edit the DEFINE lines at the top of the script",
            lang: "sql",
            code: `DEFINE pdb_name = '{{PDB_NAME_UC}}'
DEFINE schema   = '{{SCHEMA}}'
DEFINE minutes  = {{MINUTES}}`,
          },
          {
            where: "Any machine with sqlplus, as the CDC user (this also proves its privileges are enough)",
            lang: "bash",
            code: `sqlplus '{{CDC_USER}}@//{{ORACLE_HOST}}:{{ORACLE_PORT}}/{{CDB_SERVICE}}' @dba_cdc_07_logminer_capture_test.sql`,
          },
        ],
        expect:
          'Section 2 prints "LogMiner started: OK". Section 3 lists the changed tables with their change counts. Section 4 shows UPDATE redo whose WHERE part contains the log-group columns (for example AC_ID, AC_NO), not only ROWID.',
        notes: [
          "No rows in section 3: nothing was committed on those tables in the window. Make the optional change above, or raise the window, and run again.",
          "ORA-01291 or ORA-01284: a log file LogMiner needs is no longer on disk. Check the cleanup in [[cleanup-decide]].",
          "ORA-01031: a privilege is missing. Run [[check-privileges]].",
        ],
      },
    ],
  },
  {
    id: "monitoring",
    title: "Monitoring user (optional)",
    intro: "Needed only for the Grafana dashboards. A separate read-only user that cannot read application data.",
    steps: [
      {
        id: "monitoring-user",
        title: "Create the read-only monitoring user",
        summary: "Lets the Oracle exporter read archive log, recovery area and database state views.",
        risk: "change",
        when: "Only if Grafana monitoring is being set up",
        script: "dba_cdc_04_monitoring_user.sql",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `alter session set container = CDB$ROOT;

create user {{MON_USER}} identified by "{{MON_PASSWORD}}" container = all;
grant create session, set container to {{MON_USER}} container = all;

grant select on v_$database            to {{MON_USER}} container = all;
grant select on v_$instance            to {{MON_USER}} container = all;
grant select on v_$pdbs                to {{MON_USER}} container = all;
grant select on v_$recovery_file_dest  to {{MON_USER}} container = all;
grant select on v_$recovery_area_usage to {{MON_USER}} container = all;
grant select on v_$archive_dest_status to {{MON_USER}} container = all;
grant select on v_$archived_log        to {{MON_USER}} container = all;
grant select on v_$log_history         to {{MON_USER}} container = all;

select username, account_status from dba_users where username = upper('{{MON_USER}}');`,
          },
        ],
        expect: "ACCOUNT_STATUS = OPEN.",
        notes: ["Hand the user and password to whoever runs Grafana. Setup of the exporter is in monitoring/README.md."],
      },
    ],
  },
  {
    id: "handoff-section",
    title: "Hand-off to the connector",
    intro: "What the Debezium connector needs from this setup. The connector itself is registered by the platform team.",
    steps: [
      {
        id: "handoff",
        title: "Values to give the platform team",
        summary: "These must match what you created, or the connector fails to log in or captures the wrong tables.",
        risk: "read-only",
        commands: [
          {
            where: "For reference: the connector settings these values go into",
            lang: "bash",
            code: `database.user           = {{CDC_USER}}
database.password       = <the CDC user's password>
database.dbname         = <CDB name: select name from v$database>
database.pdb.name       = {{PDB_NAME}}
table.include.list      = ${INCLUDE_LIST}`,
          },
        ],
        notes: [
          "The user name is case-sensitive in the connector config. Use the name exactly as created.",
          `table.include.list holds the ${CAPTURED.length} captured tables. ${NOT_CAPTURED} has its grant and log group but is excluded from the connector in this phase.`,
          "A new table needs a grant ([[table-grants]]), a log group ([[log-groups]]) and an entry in table.include.list.",
          "With snapshot.mode = no_data only changes made after the connector starts are captured, and only when committed.",
        ],
      },
    ],
  },
  {
    id: "maintenance",
    title: "Archive log maintenance",
    intro:
      "Ongoing, after go-live. Archive logs never shrink on their own, and when the archive destination is full Oracle stops accepting changes (ORA-00257). Delete them on a schedule, but never one the connector still needs.",
    steps: [
      {
        id: "cleanup-decide",
        title: "Decide who cleans up the archive logs",
        summary: "A production database usually has RMAN backup and cleanup jobs already. Do not add a second one there.",
        risk: "read-only",
        commands: [
          {
            where: "Shell on the database server, as oracle",
            lang: "bash",
            code: `echo "SHOW ARCHIVELOG DELETION POLICY; LIST BACKUP OF ARCHIVELOG ALL SUMMARY;" | rman target /
crontab -l | grep -i rman`,
          },
        ],
        expect: "Either an existing RMAN backup and cleanup job, or none.",
        notes: [
          "Existing job (normal for production): leave it in place and give the DBA team one rule: never delete an archive log newer than the connector's resume SCN, shown as Required SCN on the Grafana dashboard. Skip the rest of this section.",
          "No job (test or new database): continue with [[cleanup-dry-run]] and [[cleanup-schedule]].",
          "Deletion policy TO BACKED UP and no backups exist: RMAN deletes nothing. On a test database without backups: CONFIGURE ARCHIVELOG DELETION POLICY TO NONE;",
        ],
      },
      {
        id: "cleanup-dry-run",
        title: "Dry-run the cleanup script",
        summary:
          "Lists the archive logs it would delete: older than the retention and wholly before the connector's resume SCN, read from Kafka Connect. It deletes nothing, and stops, if Kafka Connect cannot be reached.",
        risk: "read-only",
        when: "Only if no cleanup job exists ([[cleanup-decide]])",
        script: "dba_cdc_05_archivelog_cleanup.sh",
        commands: [
          {
            where: "Shell on the database server, as oracle",
            lang: "bash",
            code: `chmod 750 /home/oracle/scripts/dba_cdc_05_archivelog_cleanup.sh
DRY_RUN=1 KEEP_DAYS={{KEEP_DAYS}} CONNECT_URL={{CONNECT_URL}} CONNECTOR={{CONNECTOR}} /home/oracle/scripts/dba_cdc_05_archivelog_cleanup.sh`,
          },
        ],
        expect: 'The connector resume SCN, then either the sequences that qualify or "nothing to delete". The recovery area usage is printed before and after.',
        notes: ["ABORT, cannot read the resume SCN: the database server cannot reach the Kafka Connect URL. Fix that first, and do not schedule the script with ALLOW_WITHOUT_CDC=1."],
      },
      {
        id: "cleanup-schedule",
        title: "Schedule the cleanup",
        summary: "Every 6 hours through cron, with a log file. cron starts with an empty environment, so ORACLE_HOME and ORACLE_SID are set on the line.",
        risk: "change",
        when: "After a clean [[cleanup-dry-run]]",
        script: "dba_cdc_05_archivelog_cleanup.sh",
        commands: [
          {
            where: "crontab -e, as oracle on the database server (one line)",
            lang: "bash",
            code: `0 */6 * * * ORACLE_HOME={{ORACLE_HOME}} ORACLE_SID={{ORACLE_SID}} PATH={{ORACLE_HOME}}/bin:/usr/bin:/bin KEEP_DAYS={{KEEP_DAYS}} CONNECT_URL={{CONNECT_URL}} CONNECTOR={{CONNECTOR}} /home/oracle/scripts/dba_cdc_05_archivelog_cleanup.sh >> /home/oracle/logs/arch_cleanup.log 2>&1`,
          },
        ],
        expect: 'A new block in /home/oracle/logs/arch_cleanup.log every 6 hours, ending with "done" or "nothing to delete".',
        notes: [
          "Choose the retention from the archive rate: at least the longest connector outage you want to survive, plus a margin.",
          "Never delete archive files with rm. If someone did, the CROSSCHECK the script runs removes the stale records.",
          "Grafana warns at 80% and 90% recovery area use and shows Hours until full.",
        ],
      },
    ],
  },
];
