@echo off
set local
cls

echo *-----------------------------------------------------------------------------*
echo * dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
echo * https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *
echo *-----------------------------------------------------------------------------*

if "%1" =="-help" goto help
 
if "%1" == "help" goto help
if "%1" == "/?" goto help

if  !%1==! goto help
if  !%2==! goto help

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
sqlcmd.exe -S%server% %autentif% -d %dbname% -Q "" -b -m-1
if errorlevel 1 goto connect

set data_files_path="/"
set log_files_path="/"

sqlcmd.exe -S%server% %autentif% -i "install-get-instance-info.sql" -d master -v dbName=%dbname% -o install-get-instance-info.out -b -r 1
if errorlevel 1 (
	type install-get-instance-info.out
	del /F /Q install-get-instance-info.out
	goto install_err
	)

FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "dataFilePath" install-get-instance-info.out') DO (SET product_version=%%B)
FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "logFilePath" install-get-instance-info.out') DO (SET product_version=%%B)
FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "productVersion" install-get-instance-info.out') DO (SET product_version=%%B)
FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "engineVersion" install-get-instance-info.out') DO (SET engine_version=%%B)
del /F /Q install-get-instance-info.out

echo Detected SQL Server version %product_version%

sqlcmd.exe -S%server% %autentif% -i "detect-version.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

:prompt
set doUninstall = "N"
set /P doUninstall=Continue with uninstall of dbaTDPMon (Y/[N])? 
if /I "%doUninstall%" neq "Y" goto end


echo *-----------------------------------------------------------------------------*
echo Performing cleanup...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "uninstall-stop-agent-jobs.sql" -d msdb -b -r 1
if errorlevel 1 goto install_err

echo *-----------------------------------------------------------------------------*
echo Dropping database...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "uninstall-drop-db.sql" -d master -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err


echo The uninstall was successful.
goto end


:connect
echo Could not connect to the specified SQL Server instance.
goto end

:install_err
echo One of the scripts had errors or does not exists in current path %cd%..\db.
goto end

:help
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
endlocal
