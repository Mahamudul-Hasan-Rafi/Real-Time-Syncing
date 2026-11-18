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
];

/** The captured tables and the columns logged for each one. Same list as dba_scripts/dba_cdc_03_cdc_setup.sql. */
export const TABLES: { name: string; columns: string[] }[] = [
  { name: "DISTRICT_INFO", columns: ["DISTRICT_CODE_GOV", "DISTRICT_CODE"] },
  { name: "MB_ACCOUNT_DAILY_BALANCE", columns: ["AC_ID", "VALUE_DATE"] },
  { name: "MB_ACCOUNT_MST", columns: ["AC_NO", "AC_ID"] },
  { name: "MB_ACCOUNT_OPERATOR", columns: ["AC_ID", "CUST_NO", "STATUS"] },
  { name: "MB_ADDRESS_MST", columns: ["ADDRESS_ID", "REF_NO"] },
  { name: "MB_CUSTOMER_MST", columns: ["CUST_NO"] },
  { name: "MB_DOCUMENT_MST", columns: ["DOCUMENT_ID", "CUST_NO"] },
  { name: "MB_PRODUCT_MST", columns: ["AC_TYPE_ID"] },
  { name: "MB_TRANSACTION_DTL", columns: ["TRAN_ID"] },
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
export const INCLUDE_LIST = TABLES.map((t) => `{{SCHEMA}}.${t.name}`).join(",");

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
    { file: "dba_cdc_02_archivelog.sql", does: "Recovery area, enable ARCHIVELOG (restarts the database)", covers: "Section 2" },
    { file: "dba_cdc_03_cdc_setup.sql", does: "Database logging, CDC user, table grants, log groups, verification", covers: "Sections 3 to 7" },
    { file: "dba_cdc_04_monitoring_user.sql", does: "Read-only user for the Grafana exporter", covers: "Section 8" },
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
          "LOG_MODE = NOARCHIVELOG: do all of section 2.",
          "LOG_MODE = ARCHIVELOG: skip [[recovery-dir]] and [[enable-archivelog]], but still do [[archive-dest]].",
        ],
      },
    ],
  },
  {
    id: "archive",
    title: "Archive logging",
    intro:
      "LogMiner reads archived redo logs, and Debezium refuses to start unless a LOCAL archive destination is VALID. Archive logs fill the recovery area, so its size matters too.",
    steps: [
      {
        id: "recovery-dir",
        title: "Create the recovery area directory",
        summary: "The OS directory must exist and belong to the oracle user before Oracle is pointed at it.",
        risk: "change",
        when: "Only if the directory does not exist yet",
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
        title: "Set and verify the archive destination",
        summary: "Debezium needs a LOCAL destination with status VALID. Run this even if the database was already in ARCHIVELOG mode.",
        risk: "change",
        commands: [
          {
            where: "SQL*Plus as SYSDBA, connected to CDB$ROOT",
            lang: "sql",
            code: `alter system set log_archive_dest_1 = 'LOCATION=USE_DB_RECOVERY_FILE_DEST' scope = both;

set linesize 200 pagesize 100
col dest_name format a22
col destination format a30
col error format a60
select dest_id, dest_name, status, type, destination, error
  from v$archive_dest
 where dest_id = 1;

select round(space_limit/1024/1024/1024, 1)       limit_gb,
       round(space_used/1024/1024/1024, 1)        used_gb,
       round(space_reclaimable/1024/1024/1024, 1) reclaimable_gb
  from v$recovery_file_dest;`,
          },
        ],
        expect: "STATUS = VALID, TYPE = LOCAL, ERROR empty, and used_gb well below limit_gb.",
        notes: ["Do not continue to [[db-logging]] until the destination is VALID."],
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
          "The archive destination was already checked in [[archive-dest]].",
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
          "A new table needs a grant ([[table-grants]]), a log group ([[log-groups]]) and an entry in table.include.list.",
          "With snapshot.mode = no_data only changes made after the connector starts are captured, and only when committed.",
        ],
      },
    ],
  },
];
