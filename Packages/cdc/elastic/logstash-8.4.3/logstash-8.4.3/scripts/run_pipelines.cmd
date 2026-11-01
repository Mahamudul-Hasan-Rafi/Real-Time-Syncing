@echo off
rem Runs all three load pipelines from config\pipelines.yml. Logstash exits when all have finished.
rem   scripts\run_pipelines.cmd           run the pipelines
rem   scripts\run_pipelines.cmd --test    validate the configs and exit
rem
rem ORACLE_USER, ORACLE_PASSWORD, ORACLE_JDBC_URL and ES_HOSTS are read from the environment, or from
rem Packages\cdc\elastic\.env if that file exists (copy .env.example there). Variables already set win.
setlocal
for %%I in ("%~dp0..") do set "LS_HOME=%%~fI"
rem The pipelines use paths relative to the Logstash folder, so always start from there.
cd /d "%LS_HOME%"
rem The JDBC plugin loads the driver jar through JRuby, which needs an absolute path (forward slashes).
set "LS_ROOT=%LS_HOME:\=/%"

set "ENVFILE=%LS_HOME%\..\..\.env"
if exist "%ENVFILE%" (
  for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%ENVFILE%") do if not defined %%A set "%%A=%%B"
)

if not exist "bin\logstash.bat" (
  echo Logstash is not installed in %LS_HOME%. Run scripts\setup.ps1 first.
  exit /b 1
)
if not exist "drivers\ojdbc8-21.1.0.0.jar" (
  echo The Oracle JDBC driver is missing. Run scripts\setup.ps1 first.
  exit /b 1
)

rem Use the JDK 17 bundled with Logstash. 8.4.3 does not support newer JDKs.
set "LS_JAVA_HOME="

set "EXTRA_ARGS="
if /I "%~1"=="--test" set "EXTRA_ARGS=--config.test_and_exit"

call "%LS_HOME%\bin\logstash.bat" %EXTRA_ARGS%
exit /b %ERRORLEVEL%
