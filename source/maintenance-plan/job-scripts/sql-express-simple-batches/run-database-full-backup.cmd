@echo off
cls
if "%1" =="-help" goto help
 
if "%1" == "help" goto help
if "%1" == "/?" goto help

if  !%1==! goto error
if  !%2==! goto error

set server=%1
set dbname=%2
set userid=%3
set password=%4

if  !%3==! goto trusted_connection

set autentif=-U%userid% -P%password%
goto start


:trusted_connection
set autentif=-E

:start
echo Checking connection...
sqlcmd.exe -S%server% %autentif% -d master -Q "" -b -m-1
if errorlevel 1 goto connect
echo Ok

echo *-----------------------------------------------------------------------------*
echo Running SQL script...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "run-database-full-backup.sql" -o "run-database-full-backup.log" -d master -v dbName=%dbname% -b -r 1
if errorlevel 1 goto execution_err
goto done


:done
echo The execution was successful. Check run-database-full-backup.log for more details.
goto end

:connect
echo Can not connect 
goto end

:execution_err
echo One of the scripts had errors or does not exists in current path %cd%..\db.
goto end

:error
echo Incorrect Usage

:help
echo *-----------------------------------------------------------------------------*
echo CMD Run dbaTDPMon (Troubleshoot Database Performance / Monitoring)
echo *-----------------------------------------------------------------------------*
echo USAGE : SQL Server Authentication
echo run-database-full-backup.cmd "server_name" "db_name" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo run-database-full-backup.cmd "server_name" "db_name"
echo .
echo Example Call : SQL Server Authentication
echo run-database-full-backup.cmd . "dbaTDPMon" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo run-database-full-backup.cmd "LAB-SERVER" "dbaTDPMon"

echo *-----------------------------------------------------------------------------*
:end
