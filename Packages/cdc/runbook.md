# Oracle CDC database setup runbook

**For:** DBAs preparing an Oracle database for Debezium LogMiner change capture into Kafka.

Run the sections in order. Each step says where to run it, whether it changes anything, what you should see, and which part of the dba_scripts files does the same job. The scripts are the source of truth: this runbook shows the same statements so you can run them one at a time and check each result.

## Before you start

- A SYSDBA login on the Oracle server, and a maintenance window if section 2 applies (it restarts the database).
- The database is a CDB with the application schema in a PDB. The commands use ORCLPDB1, EMOB, c##debezium and /u01/oracle_logs: replace them with your own values, and replace every password placeholder.
- Sections 5 and 6 list the captured tables. If that list changes, change both sections and the connector's table.include.list (section 9).

## Scripts

| Script (dba_scripts/)            | What it does                                                       | Runbook         |
| -------------------------------- | ------------------------------------------------------------------ | --------------- |
| `dba_cdc_01_check.sql`           | Read-only check of the current state                               | Section 1       |
| `dba_cdc_02_archivelog.sql`      | Recovery area, enable ARCHIVELOG (restarts the database)           | Section 2       |
| `dba_cdc_03_cdc_setup.sql`       | Database logging, CDC user, table grants, log groups, verification | Sections 3 to 7 |
| `dba_cdc_04_monitoring_user.sql` | Read-only user for the Grafana exporter                            | Section 8       |

## Contents

1. [Check the current state](#s1)
    - 1.1 [Read archive mode, logging, recovery area and PDBs](#s1-1)
2. [Archive logging](#s2)
    - 2.1 [Create the recovery area directory](#s2-1)
    - 2.2 [Set the recovery area and enable ARCHIVELOG](#s2-2)
    - 2.3 [Set and verify the archive destination](#s2-3)
3. [Database-level logging](#s3)
    - 3.1 [Enable minimal supplemental logging and force logging](#s3-1)
4. [CDC user and privileges](#s4)
    - 4.1 [Create the CDC user](#s4-1)
    - 4.2 [Grant the LogMiner privileges](#s4-2)
5. [Read access on the captured tables](#s5)
    - 5.1 [Grant SELECT and FLASHBACK on each captured table](#s5-1)
6. [Supplemental log groups](#s6)
    - 6.1 [Add one supplemental log group per captured table](#s6-1)
    - 6.2 [Change the columns of an existing group, or add a table](#s6-2)
7. [Verify the setup](#s7)
    - 7.1 [Check the user, log groups, table grants and database flags](#s7-1)
8. [Monitoring user (optional)](#s8)
    - 8.1 [Create the read-only monitoring user](#s8-1)
9. [Hand-off to the connector](#s9)
    - 9.1 [Values to give the platform team](#s9-1)

<a id="s1"></a>
## 1. Check the current state

Read-only. The result decides whether section 2 is needed, and I would recommend to run these commands one by one from this runbook rather than from the scripts or the whole script all at once.

<a id="s1-1"></a>
### 1.1 Read archive mode, logging, recovery area and PDBs

**read-only** · **script:** `dba_cdc_01_check.sql`

Shows what is already in place so you only run the steps that are needed.

Run in: SQL*Plus as SYSDBA

```sql
set linesize 200 pagesize 100
select name, cdb, log_mode, force_logging, supplemental_log_data_min, supplemental_log_data_all
  from v$database;

show parameter db_recovery_file_dest

select name, open_mode from v$pdbs;
```

**Expected:** CDB = YES and your PDB is READ WRITE. Note LOG_MODE.

- LOG_MODE = NOARCHIVELOG: do all of section 2.
- LOG_MODE = ARCHIVELOG: still need to do all of section 2 for re-ensuring.

[Back to contents](#contents)

<a id="s2"></a>
## 2. Archive logging

LogMiner reads archived redo logs, and Debezium refuses to start unless a LOCAL archive destination is VALID. Archive logs fill the recovery area, so its size matters too.

<a id="s2-1"></a>
### 2.1 Create the recovery area directory

**change** · **when:** Only if the directory does not exist yet

The OS directory must exist and belong to the oracle user before Oracle is pointed at it.

Run in: Shell on the database server, as root

```bash
mkdir -p /u01/oracle_logs
chown oracle:oinstall /u01/oracle_logs
chmod 750 /u01/oracle_logs
```

- The group name (oinstall) can differ on your server.

[Back to contents](#contents)

<a id="s2-2"></a>
### 2.2 Set the recovery area and enable ARCHIVELOG

**downtime** · **when:** Only if LOG_MODE = NOARCHIVELOG · **script:** `dba_cdc_02_archivelog.sql`

Set the size before the location. Switching the log mode needs a restart.

Run in: SQL*Plus as SYSDBA, connected to CDB$ROOT

```sql
alter system set db_recovery_file_dest_size = 15G   scope = both;
alter system set db_recovery_file_dest      = '/u01/oracle_logs' scope = both;

shutdown immediate;
startup mount;
alter database archivelog;
alter database open;

archive log list
```

**Expected:** Database log mode = Archive Mode, Automatic archival = Enabled.

- All sessions are disconnected. Schedule it and tell the application owners.

[Back to contents](#contents)

<a id="s2-3"></a>
### 2.3 Set and verify the archive destination

**change**

Debezium needs a LOCAL destination with status VALID. Run this even if the database was already in ARCHIVELOG mode.

Run in: SQL*Plus as SYSDBA, connected to CDB$ROOT

```sql
alter system set log_archive_dest_1 = 'LOCATION=USE_DB_RECOVERY_FILE_DEST' scope = both;

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
  from v$recovery_file_dest;
```

**Expected:** STATUS = VALID, TYPE = LOCAL, ERROR empty, and used_gb well below limit_gb.

- Do not continue to [step 3.1](#s3-1) until the destination is VALID.

[Back to contents](#contents)

<a id="s3"></a>
## 3. Database-level logging

Two switches for the whole database. Online, no restart.

<a id="s3-1"></a>
### 3.1 Enable minimal supplemental logging and force logging

**change** · **script:** `dba_cdc_03_cdc_setup.sql, Step 1`

Minimal supplemental logging lets LogMiner read row changes. FORCE LOGGING stops NOLOGGING operations from bypassing the redo, which CDC would miss.

Run in: SQL*Plus as SYSDBA, connected to CDB$ROOT

```sql
alter session set container = CDB$ROOT;
alter database add supplemental log data;
alter database force logging;

select supplemental_log_data_min, force_logging, log_mode from v$database;
```

**Expected:** SUPPLEMENTAL_LOG_DATA_MIN = YES, FORCE_LOGGING = YES, LOG_MODE = ARCHIVELOG.

- The per-table log groups come later, in [step 6.1](#s6-1).

[Back to contents](#contents)

<a id="s4"></a>
## 4. CDC user and privileges

A common user, because LogMiner reads the redo of the whole CDB. It gets only what Debezium needs and no broad ANY privileges.

<a id="s4-1"></a>
### 4.1 Create the CDC user

**change** · **script:** `dba_cdc_03_cdc_setup.sql, Step 2`

Common user in all containers, on the USERS tablespace.

Run in: SQL*Plus as SYSDBA, connected to CDB$ROOT

```sql
alter session set container = CDB$ROOT;

create user c##debezium identified by "<password>"
  default tablespace users
  quota unlimited on users
  container = all;
```

- The script asks for the password at run time, so it never sits in the file. Replace the placeholder with the real password, and do not save it in this document.

[Back to contents](#contents)

<a id="s4-2"></a>
### 4.2 Grant the LogMiner privileges

**change** · **script:** `dba_cdc_03_cdc_setup.sql, Step 2`

Session, LogMiner, and read access on the V$ views Debezium queries.

Run in: SQL*Plus as SYSDBA, connected to CDB$ROOT

```sql
grant create session, set container             to c##debezium container = all;
grant logmining                                 to c##debezium container = all;
grant select any transaction                    to c##debezium container = all;
grant select_catalog_role, execute_catalog_role to c##debezium container = all;
grant execute on dbms_logmnr                    to c##debezium container = all;
grant execute on dbms_logmnr_d                  to c##debezium container = all;
grant create table, create sequence             to c##debezium container = all;
grant select on v_$database                     to c##debezium container = all;
grant select on v_$log                          to c##debezium container = all;
grant select on v_$log_history                  to c##debezium container = all;
grant select on v_$logfile                      to c##debezium container = all;
grant select on v_$archived_log                 to c##debezium container = all;
grant select on v_$archive_dest_status          to c##debezium container = all;
grant select on v_$logmnr_logs                  to c##debezium container = all;
grant select on v_$logmnr_contents              to c##debezium container = all;
grant select on v_$logmnr_parameters            to c##debezium container = all;
grant select on v_$transaction                  to c##debezium container = all;
grant select on v_$mystat                       to c##debezium container = all;
grant select on v_$statname                     to c##debezium container = all;
```

- Not granted by design: SELECT ANY TABLE, FLASHBACK ANY TABLE, LOCK ANY TABLE, DBA. Access to the data tables is given per table in [step 5.1](#s5-1).

[Back to contents](#contents)

<a id="s5"></a>
## 5. Read access on the captured tables

Debezium reads the 18 captured tables for the initial schema and for flashback. Grant on those tables only.

<a id="s5-1"></a>
### 5.1 Grant SELECT and FLASHBACK on each captured table

**change** · **script:** `dba_cdc_03_cdc_setup.sql, Step 3`

Run in the PDB, as the privilege has to be given where the tables live.

Run in: SQL*Plus as SYSDBA, connected to the PDB

```sql
alter session set container = ORCLPDB1;

grant select, flashback on EMOB.DISTRICT_INFO            to c##debezium;
grant select, flashback on EMOB.MB_ACCOUNT_DAILY_BALANCE to c##debezium;
grant select, flashback on EMOB.MB_ACCOUNT_MST           to c##debezium;
grant select, flashback on EMOB.MB_ACCOUNT_OPERATOR      to c##debezium;
grant select, flashback on EMOB.MB_ADDRESS_MST           to c##debezium;
grant select, flashback on EMOB.MB_CUSTOMER_MST          to c##debezium;
grant select, flashback on EMOB.MB_DOCUMENT_MST          to c##debezium;
grant select, flashback on EMOB.MB_PRODUCT_MST           to c##debezium;
grant select, flashback on EMOB.NOMINEE_MST              to c##debezium;
grant select, flashback on EMOB.STCODMAS                 to c##debezium;
grant select, flashback on EMOB.ST_AGENT_POINT           to c##debezium;
grant select, flashback on EMOB.ST_OPERATING_INSTRUCTION to c##debezium;
grant select, flashback on EMOB.ST_POINT_CATEGORY        to c##debezium;
grant select, flashback on EMOB.ST_SECTOR_CODE           to c##debezium;
grant select, flashback on EMOB.ST_TRANSACTION_CODE      to c##debezium;
grant select, flashback on EMOB.UNION_INFO               to c##debezium;
grant select, flashback on EMOB.UPAZILA_INFO             to c##debezium;
```

**Expected:** 17 grants succeed.

- The user must exist first ([step 4.1](#s4-1)).

[Back to contents](#contents)

<a id="s6"></a>
## 6. Supplemental log groups

Per-table logging of only the columns CDC needs. This replaces ALL COLUMNS logging, which writes every column of every changed row into the redo and fills the archive area for no benefit.

<a id="s6-1"></a>
### 6.1 Add one supplemental log group per captured table

**change** · **when:** After [step 3.1](#s3-1) and [step 5.1](#s5-1). Run it in a quiet period · **script:** `dba_cdc_03_cdc_setup.sql, Step 4`

Each group holds the row key plus the columns the downstream consumer uses to find related records. ALWAYS means those columns are logged on every update of the row, not only when one of them changes.

Run in: SQL*Plus as SYSDBA, connected to the PDB

```sql
alter session set container = ORCLPDB1;

alter table EMOB.DISTRICT_INFO            add supplemental log group g_cdc_kafka_district_info (DISTRICT_CODE_GOV, DISTRICT_CODE) always;
alter table EMOB.MB_ACCOUNT_DAILY_BALANCE add supplemental log group g_cdc_kafka_mb_account_daily_balance (AC_ID, VALUE_DATE) always;
alter table EMOB.MB_ACCOUNT_MST           add supplemental log group g_cdc_kafka_mb_account_mst (AC_NO, AC_ID) always;
alter table EMOB.MB_ACCOUNT_OPERATOR      add supplemental log group g_cdc_kafka_mb_account_operator (AC_ID, CUST_NO, STATUS) always;
alter table EMOB.MB_ADDRESS_MST           add supplemental log group g_cdc_kafka_mb_address_mst (ADDRESS_ID, REF_NO) always;
alter table EMOB.MB_CUSTOMER_MST          add supplemental log group g_cdc_kafka_mb_customer_mst (CUST_NO) always;
alter table EMOB.MB_DOCUMENT_MST          add supplemental log group g_cdc_kafka_mb_document_mst (DOCUMENT_ID, CUST_NO) always;
alter table EMOB.MB_PRODUCT_MST           add supplemental log group g_cdc_kafka_mb_product_mst (AC_TYPE_ID) always;
alter table EMOB.NOMINEE_MST              add supplemental log group g_cdc_kafka_nominee_mst (NOMINEE_ID, AC_ID) always;
alter table EMOB.STCODMAS                 add supplemental log group g_cdc_kafka_stcodmas (HARCOD, SOFCOD) always;
alter table EMOB.ST_AGENT_POINT           add supplemental log group g_cdc_kafka_st_agent_point (POINT_ID) always;
alter table EMOB.ST_OPERATING_INSTRUCTION add supplemental log group g_cdc_kafka_st_operating_instruction (OPR_INS) always;
alter table EMOB.ST_POINT_CATEGORY        add supplemental log group g_cdc_kafka_st_point_category (POINT_CATEGORY_ID) always;
alter table EMOB.ST_SECTOR_CODE           add supplemental log group g_cdc_kafka_st_sector_code (SRLNO) always;
alter table EMOB.ST_TRANSACTION_CODE      add supplemental log group g_cdc_kafka_st_transaction_code (TRAN_CODE) always;
alter table EMOB.UNION_INFO               add supplemental log group g_cdc_kafka_union_info (UNION_CODE_GOV, DISTRICT_CODE, UPAZILA_CODE, UNION_CODE) always;
alter table EMOB.UPAZILA_INFO             add supplemental log group g_cdc_kafka_upazila_info (UPAZILA_CODE_GOV, DISTRICT_CODE, UPAZILA_CODE) always;
```

**Expected:** 17 statements succeed. Verify in [step 7.1](#s7-1).

- Applies to redo written from now on. Changes made before this step carry only the changed columns.
- Group names are unique per schema, so each table has its own name (g_cdc_kafka_ followed by the table name).
- Do not use ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS on these tables.

[Back to contents](#contents)

<a id="s6-2"></a>
### 6.2 Change the columns of an existing group, or add a table

**change** · **when:** Only when the captured columns or tables change

A log group cannot be edited. Drop it and add it again with the new column list.

Run in: SQL*Plus as SYSDBA, connected to the PDB

```sql
alter session set container = ORCLPDB1;

-- example: if we need to add a column to MB_CUSTOMER_MST

--firstly, drop the existing group
alter table EMOB.MB_CUSTOMER_MST drop supplemental log group g_cdc_kafka_mb_customer_mst;

--then, add the new log group with updated members
alter table EMOB.MB_CUSTOMER_MST add supplemental log group g_cdc_kafka_mb_customer_mst (CUST_NO, <new_column>) always;
```

- For a new table, also run [step 5.1](#s5-1) for it and add it to the connector's table.include.list ([step 9.1](#s9-1)).

[Back to contents](#contents)

<a id="s7"></a>
## 7. Verify the setup

Read-only. Run it after sections 3 to 6 and keep the output as the hand-over evidence.

<a id="s7-1"></a>
### 7.1 Check the user, log groups, table grants and database flags

**read-only** · **script:** `dba_cdc_03_cdc_setup.sql, Step 5`

Four queries, each with its expected result.

Run in: SQL*Plus as SYSDBA, connected to the PDB

```sql
alter session set container = ORCLPDB1;
set linesize 200 pagesize 100
col username format a14
col account_status format a16
col table_name format a28
col log_group_name format a32
col log_group_type format a18

-- 1. the CDC user
select username, account_status from dba_users where username = upper('c##debezium');

-- 2. the log groups
select table_name, log_group_name, log_group_type, always
  from dba_log_groups
 where owner = 'EMOB'
 order by table_name;

-- 3. the table grants
select count(*) as tables_granted
  from dba_tab_privs
 where grantee = upper('c##debezium') and owner = 'EMOB' and privilege = 'SELECT'
   and table_name in ('DISTRICT_INFO','MB_ACCOUNT_DAILY_BALANCE','MB_ACCOUNT_MST','MB_ACCOUNT_OPERATOR','MB_ADDRESS_MST','MB_CUSTOMER_MST','MB_DOCUMENT_MST','MB_PRODUCT_MST','MB_TRANSACTION_DTL','NOMINEE_MST','STCODMAS','ST_AGENT_POINT','ST_OPERATING_INSTRUCTION','ST_POINT_CATEGORY','ST_SECTOR_CODE','ST_TRANSACTION_CODE','UNION_INFO','UPAZILA_INFO');

-- 4. the database flags
select supplemental_log_data_min as sup_min, supplemental_log_data_all as sup_all, force_logging, log_mode
  from v$database;
```

**Expected:** 1) the user, ACCOUNT_STATUS = OPEN. 2) 18 rows, LOG_GROUP_TYPE = USER LOG GROUP, ALWAYS = ALWAYS. 3) 18. 4) SUP_MIN = YES, SUP_ALL = NO, FORCE_LOGGING = YES, LOG_MODE = ARCHIVELOG.

- SUP_ALL = NO is correct: it confirms nobody enabled ALL COLUMNS logging at database level.
- The archive destination was already checked in [step 2.3](#s2-3).

[Back to contents](#contents)

<a id="s8"></a>
## 8. Monitoring user (optional)

Needed only for the Grafana dashboards. A separate read-only user that cannot read application data.

<a id="s8-1"></a>
### 8.1 Create the read-only monitoring user

**change** · **when:** Only if Grafana monitoring is being set up · **script:** `dba_cdc_04_monitoring_user.sql`

Lets the Oracle exporter read archive log, recovery area and database state views.

Run in: SQL*Plus as SYSDBA, connected to CDB$ROOT

```sql
alter session set container = CDB$ROOT;

create user c##cdcmon identified by "<password>" container = all;
grant create session, set container to c##cdcmon container = all;

grant select on v_$database            to c##cdcmon container = all;
grant select on v_$instance            to c##cdcmon container = all;
grant select on v_$pdbs                to c##cdcmon container = all;
grant select on v_$recovery_file_dest  to c##cdcmon container = all;
grant select on v_$recovery_area_usage to c##cdcmon container = all;
grant select on v_$archive_dest_status to c##cdcmon container = all;
grant select on v_$archived_log        to c##cdcmon container = all;
grant select on v_$log_history         to c##cdcmon container = all;

select username, account_status from dba_users where username = upper('c##cdcmon');
```

**Expected:** ACCOUNT_STATUS = OPEN.

- Hand the user and password to whoever runs Grafana. Setup of the exporter is in monitoring/README.md.

[Back to contents](#contents)

<a id="s9"></a>
## 9. Hand-off to the connector

What the Debezium connector needs from this setup. The connector itself is registered by the platform team.

<a id="s9-1"></a>
### 9.1 Values to give the platform team

**read-only**

These must match what you created, or the connector fails to log in or captures the wrong tables.

Run in: For reference: the connector settings these values go into

```bash
database.user           = c##debezium
database.password       = <the CDC user's password>
database.dbname         = <CDB name: select name from v$database>
database.pdb.name       = ORCLPDB1
table.include.list      = EMOB.DISTRICT_INFO,EMOB.MB_ACCOUNT_DAILY_BALANCE,EMOB.MB_ACCOUNT_MST,EMOB.MB_ACCOUNT_OPERATOR,EMOB.MB_ADDRESS_MST,EMOB.MB_CUSTOMER_MST,EMOB.MB_DOCUMENT_MST,EMOB.MB_PRODUCT_MST,EMOB.NOMINEE_MST,EMOB.STCODMAS,EMOB.ST_AGENT_POINT,EMOB.ST_OPERATING_INSTRUCTION,EMOB.ST_POINT_CATEGORY,EMOB.ST_SECTOR_CODE,EMOB.ST_TRANSACTION_CODE,EMOB.UNION_INFO,EMOB.UPAZILA_INFO
```

- The user name is case-sensitive in the connector config. Use the name exactly as created.
- A new table needs a grant ([step 5.1](#s5-1)), a log group ([step 6.1](#s6-1)) and an entry in table.include.list.
- With snapshot.mode = no_data only changes made after the connector starts are captured, and only when committed.

[Back to contents](#contents)
