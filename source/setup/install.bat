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
set module=%3
set project=%4
set data_files_path=%5
set log_files_path=%6
set userid=%6
set password=%8

if !%4==! set project="DEFAULT"
if !%5==! set data_files_path=""
if !%6==! set log_files_path=""
if !%7==! goto trusted_connection

set autentif=-U%userid% -P%password%

goto start


:trusted_connection
set autentif=-E

:start
echo Checking connection...
sqlcmd.exe -S%server% %autentif% -d master -Q "" -b -m-1
if errorlevel 1 goto connect
echo Ok

	
if %module%=="all" goto common
if %module%=="health-check" goto common
if %module%=="maintenance-plan" goto common
if %module%=="maintenance-plan-2k" goto common
if %module%=="monitoring" goto common
goto help

     
:common

echo *-----------------------------------------------------------------------------*
echo Performing cleanup...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "uninstall-stop-agent-jobs.sql" -d master -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

echo *-----------------------------------------------------------------------------*
echo Dropping database...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "uninstall-drop-db.sql" -d master -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

echo *-----------------------------------------------------------------------------*
echo Creating database...
echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "install-create-db.sql" -d master -v dbName=%dbname% data_files_path=%data_files_path% log_files_path=%log_files_path% -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Common: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

set runscript=false
set run2k5mode=true

if %module%=="all" set runscript=true
if %module%=="health-check" set runscript=true
if %module%=="maintenance-plan-2k" set run2k5mode=false
	
sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.appConfigurations.sql" -d %dbname% -v projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogProjects.sql" -d %dbname% -v projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogMachineNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogInstanceNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogDatabaseNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.logEventMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.logServerAnalysisMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogReportHTMLGraphics.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.reportHTML.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.reportHTMLOptions.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogHardcodedFilters.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.reportHTMLSkipRules.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.jobExecutionQueue.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_catalogInstanceNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_catalogDatabaseNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_logEventMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_logServerAnalysisMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_jobExecutionQueue.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err



echo *-----------------------------------------------------------------------------*
echo Common: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_checkIP4Address.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_formatSQLQueryForLinkedServer.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_getMilisecondsBetweenDates.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_convertLSNToNumeric.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLFomatTimeValue.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLGetAnchorName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLGetClusterNodeNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLGetImage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLPrepareText.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_addLinkedSQLServer.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_logPrintMessage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_logEventMessage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_SQLSMTPMail.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_logEventMessageAndSendEmail.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_getSQLServerVersion.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlExecuteAndLog.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_changeServerConfigurationOption.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_createFolderOnDisk.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJobEmailStatusReport.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_tableGetRowCount.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJobCheckStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJob.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJobStartAndWatch.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_JobQueueGetStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_JobQueueExecute.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_refreshMachineCatalogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_removeFromCatalog.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_refreshProjectCatalogsAndDiscovery.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%runscript%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_reportHTMLGetStorageFolder.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err



if %module%=="all" goto mp
if %module%=="health-check" goto hc
if %module%=="maintenance-plan" goto mp
if %module%=="maintenance-plan-2k" goto mp
if %module%=="monitoring" goto mon
goto help


:mp
echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\schema\create-schema-maintenance-plan.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\tables\maintenance-plan.statsMaintenancePlanInternals.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\functions\dbo.ufn_mpBackupBuildFileName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpCheckIndexOnlineOperation.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpMarkInternalAction.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpGetIndexCreationScript.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableForeignKeys.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableTriggers.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableIndexes.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpCheckAndRevertInternalActions.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseKillConnections.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseConsistencyCheck.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseShrink.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpUpdateStatisticsBasedOnStrategy.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpTableDataSynchronizeInsert.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableRebuildHeap.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseOptimize.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDeleteFileOnDisk.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseBackupCleanup.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseBackup.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Maintenance - System DBs.sql" -d %dbname% -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Maintenance - User DBs.sql" -d %dbname% -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Backup - Full and Diff.sql" -d %dbname% -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Backup - Log.sql" -d %dbname% -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

if %module%=="all" goto hc
goto done

:hc
echo *-----------------------------------------------------------------------------*
echo Health Check: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\health-check\schema\create-schema-health-check.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsSQLServerAgentJobsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsSQLServerErrorlogDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsOSEventLogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsSQLServerAgentJobsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsSQLServerErrorlogDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsOSEventLogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo Health Check: Creating Functions / Stored Procedures

sqlcmd.exe -S%server% %autentif% -i "..\health-check\functions\dbo.ufn_hcGetIndexesFrequentlyFragmented.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectSQLServerAgentJobsStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectDiskSpaceUsage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectEventMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectErrorlogMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectOSEventLogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_reportHTMLBuildHealthCheck.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcChangeFillFactorForIndexesFrequentlyFragmented.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcJobQueueCreate.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Health Check: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\health-check\job-scripts\job-script-dbaTDPMon - Discovery & Health Check.sql" -d %dbname% -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

if %module%=="all" goto mon
goto done


:mon
echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

if "%run2k5mode%"=="true" sqlcmd.exe -S%server% %autentif% -i "..\monitoring\schema\create-schema-monitoring.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\alarm-custom\monitoring.alertThresholds.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\alarm-custom\monitoring.alertSkipRules.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

echo Monitoring: Creating Functions / Stored Procedures

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\alarm-custom\dbo.usp_monAlarmCustomFreeDiskSpace.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\job-scripts\job-script-dbaTDPMon - Monitoring - Disk Space.sql" -d %dbname% -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

if %module%=="all" goto done
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
echo install.bat "server_name" "db_name" "module" "project_code" "data_files_path" "log_files_path" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo install.bat "server_name" "db_name" "module" "project_code" "data_files_path" "log_files_path"
echo .
echo "module: {all | health-check | maintenance-plan | maintenance-plan-2k | monitoring}"
echo .
echo Example Call : SQL Server Authentication
echo install.bat . "dbaTDPMon" "all" "TEST" "D:\SQLData\Data\" "D:\SQLData\Log\" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo install.bat "LAB-SERVER" "dbaTDPMon" "all" "LAB"

echo *-----------------------------------------------------------------------------*
:end
