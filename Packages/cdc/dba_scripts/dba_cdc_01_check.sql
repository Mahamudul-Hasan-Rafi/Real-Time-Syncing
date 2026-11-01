/* ==================================================================================================
   dba_cdc_01_check.sql  -  CURRENT STATE (read-only, changes nothing)
   Run as   : sqlplus / as sysdba @dba_cdc_01_check.sql
   Decision : LOG_MODE = NOARCHIVELOG  ->  run dba_cdc_02_archivelog.sql, then dba_cdc_03_cdc_setup.sql
              LOG_MODE = ARCHIVELOG    ->  skip 02, run dba_cdc_03_cdc_setup.sql
   ================================================================================================== */
SET ECHO ON  VERIFY OFF  LINESIZE 200  PAGESIZE 100  TRIMSPOOL ON
COLUMN name       FORMAT A12
COLUMN log_mode   FORMAT A14
COLUMN open_mode  FORMAT A12

SELECT name, cdb, log_mode, force_logging, supplemental_log_data_min, supplemental_log_data_all
  FROM v$database;

SHOW PARAMETER db_recovery_file_dest

SELECT name, open_mode FROM v$pdbs;
