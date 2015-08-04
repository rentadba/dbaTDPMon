@echo off
cls
if "%1" =="-help" goto help
 
if "%1" == "help" goto help
if "%1" == "/?" goto help

if  !%1==! goto error
if  !%2==! goto error

set server=%1
set dbname=%2
set userid=%5
set password=%6

if  !%5==! goto trusted_connection

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
echo Performing cleanup...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "uninstall-stop-agent-jobs.sql" -d %dbname% -b -r 1
if errorlevel 1 goto install_err

echo *-----------------------------------------------------------------------------*
echo Dropping database...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "uninstall-drop-db.sql" -d master -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err


echo The uninstall was successful.
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
echo Drop database used by Troubleshoot Database Performance / Monitoring
echo *-----------------------------------------------------------------------------*
echo USAGE : SQL Server Authentication
echo uninstall.bat "server_name" "db_name" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo uninstall.bat "server_name" "db_name"
echo .
echo Example Call : SQL Server Authentication
echo uninstall.bat . "dbaTDPMon" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo uninstall.bat "LAB-SERVER" "dbaTDPMon" 

echo *-----------------------------------------------------------------------------*
:end
