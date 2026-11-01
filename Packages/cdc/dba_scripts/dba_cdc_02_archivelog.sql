/* ==================================================================================================
   dba_cdc_02_archivelog.sql  -  ENABLE ARCHIVELOG MODE           ** RESTARTS THE DATABASE **
   Run as   : sqlplus / as sysdba @dba_cdc_02_archivelog.sql
   When     : only when dba_cdc_01_check.sql shows NOARCHIVELOG (the script refuses to run otherwise)
   Needs    : a maintenance window (all sessions are disconnected); the instance must use an SPFILE
   ================================================================================================== */
SET ECHO ON  VERIFY OFF  DEFINE ON  LINESIZE 200  PAGESIZE 100  TRIMSPOOL ON
WHENEVER SQLERROR EXIT FAILURE

-- Guard: stop here if the database is already archiving
BEGIN
  FOR r IN (SELECT log_mode FROM v$database WHERE log_mode <> 'NOARCHIVELOG') LOOP
    RAISE_APPLICATION_ERROR(-20001, 'Database is already in ' || r.log_mode || ' - nothing to do');
  END LOOP;
END;
/

-- The size must be set before the location
ALTER SYSTEM SET db_recovery_file_dest_size = &fra_size   SCOPE = BOTH;
ALTER SYSTEM SET db_recovery_file_dest      = '&fra_path' SCOPE = BOTH;

-- Expected: Database log mode = Archive Mode, Automatic archival = Enabled, Archive destination = USE_DB_RECOVERY_FILE_DEST
ARCHIVE LOG LIST