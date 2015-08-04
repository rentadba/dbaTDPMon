@echo off
cls
if "%1" =="-help" goto help
 
if "%1" == "help" goto help
if "%1" == "/?" goto help

if  !%1==! goto error
if  !%2==! goto error
if  !%3==! goto error

set server=%1
set dbname=%2
set forserver=%3
set userid=%4
set password=%5

if  !%4==! goto trusted_connection

set autentif=-U%userid% -P%password%

goto start


:trusted_connection
set autentif=-E

:start
echo Checking connection...
sqlcmd.exe -S%server% %autentif% -d master -Q "" -b -m-1
if errorlevel 1 goto connect
echo Ok

:mp

echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - LinkedServer - all.sql" -d %dbname% -v dbName=%dbname% LinkedServerName=%forserver% -b -r 1
if errorlevel 1 goto install_err

goto done


:done
echo The installation was successful.
goto end



:connect
echo Can not connect 
goto end

:install_err
echo One of the scripts had errors or does not exists in current path %cd%..\db.
goto end

:error
echo Incorrect Usage
:help
echo *-----------------------------------------------------------------------------*
echo Install dbaTDPMon (Troubleshoot Database Performance / Monitoring)
echo *-----------------------------------------------------------------------------*
echo USAGE : SQL Server Authentication
echo install-mp-agentless.bat "server_name" "db_name" "for_server_name" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo install-mp-agentless.bat "server_name" "db_name" "for_server_name" 
echo .
echo Example Call : SQL Server Authentication
echo install-mp-agentless.bat . "dbaTDPMon" "TESTSERVER\INST1" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo install-mp-agentless.bat "LAB-SERVER" "dbaTDPMon" "TESTSERVER\INST1"

echo *-----------------------------------------------------------------------------*
:end
