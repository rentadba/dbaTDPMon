@echo off
cls

echo *-----------------------------------------------------------------------------*
echo * dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *
echo * http://dbatdpmon.codeplex.com, under GNU (GPLv3) licence model              *
echo *-----------------------------------------------------------------------------*

if "%1" =="-help" goto help
 
if "%1" == "help" goto help
if "%1" == "/?" goto help

if  !%1==! goto help
if  !%2==! goto help
if  !%3==! goto help

set server=%1
set dbname=%2
set module=%3
set project=%4
set data_files_path=%5
set log_files_path=%6
set userid=%7
set password=%8

set module=%module:"=%

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

sqlcmd.exe -S%server% %autentif% -i "install-get-instance-info.sql" -d master -v dbName=%dbname% -o install-get-instance-info.out -b -r 1
if errorlevel 1 (
	type install-get-instance-info.out
	del /F /Q install-get-instance-info.out
	goto install_err
	)

FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "dataFilePath" install-get-instance-info.out') DO (SET data_files_path=%%B)
FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "logFilePath" install-get-instance-info.out') DO (SET log_files_path=%%B)
FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "productVersion" install-get-instance-info.out') DO (SET product_version=%%B)
FOR /F "tokens=1,2 delims==" %%A IN ('FINDSTR /R "engineVersion" install-get-instance-info.out') DO (SET engine_version=%%B)
del /F /Q install-get-instance-info.out

echo Detected SQL Server version %product_version%

set run2kmode=false
if "%engine_version%"=="8" (
	set run2kmode=true
	if not "%module%" == "maintenance-plan" (
		echo Update mode is not supported for SQL Server 2000. Run Install mode.
		goto help
		)
	)

if "%module%"=="all" goto common
if "%module%"=="health-check" goto common
if "%module%"=="maintenance-plan" goto common
if "%module%"=="monitoring" goto common
goto help

     
:common


echo *-----------------------------------------------------------------------------*
echo Running table's patching scripts...
echo *-----------------------------------------------------------------------------*

if "%module%"=="all" sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="all" sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="health-check" sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="maintenance-plan" sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="maintenance-plan" sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="monitoring" sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err



echo *-----------------------------------------------------------------------------*
echo Common: Creating Views ...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_getObjectQuoteName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_catalogInstanceNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_formatPlatformSpecificPath.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_catalogDatabaseNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_logEventMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_logAnalysisMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_jobExecutionQueue.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_jobExecutionHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Common: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_checkIP4Address.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_formatSQLQueryForLinkedServer.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_getTableFromStringList.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_getMilisecondsBetweenDates.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_convertLSNToNumeric.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLFomatTimeValue.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLGetAnchorName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLGetClusterNodeNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLGetImage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_reportHTMLPrepareText.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_addLinkedSQLServer.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_logPrintMessage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_logEventMessage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_logEventMessageAndSendEmail.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_getSQLServerVersion.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlExecuteAndLog.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_changeServerConfigurationOption.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_changeServerOption_xp_cmdshell.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_createFolderOnDisk.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJobEmailStatusReport.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_tableGetRowCount.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJobCheckStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJob.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_sqlAgentJobStartAndWatch.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_jobQueueGetStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_jobQueueExecute.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_refreshMachineCatalogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_removeFromCatalog.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_refreshProjectCatalogsAndDiscovery.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_reportHTMLGetStorageFolder.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_purgeHistoryData.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

	
if "%module%"=="all" goto mp
if "%module%"=="health-check" goto hc
if "%module%"=="maintenance-plan" goto mp
if "%module%"=="monitoring" goto mon
goto help


:mp
echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating Views ...
echo *-----------------------------------------------------------------------------*

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\views\maintenance-plan.vw_internalScheduler.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\views\maintenance-plan.vw_objectsSkipList.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\functions\dbo.ufn_mpBackupBuildFileName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\functions\dbo.ufn_mpCheckTaskSchedulerForDate.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\functions\dbo.ufn_mpObjectQuoteName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpCheckIndexOnlineOperation.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpCheckAvailabilityGroupLimitations.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpMarkInternalAction.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpGetIndexCreationScript.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableForeignKeys.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableTriggers.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableIndexes.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpCheckAndRevertInternalActions.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseKillConnections.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseConsistencyCheck.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseShrink.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpUpdateStatisticsBasedOnStrategy.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpTableDataSynchronizeInsert.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableRebuildHeap.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseOptimize.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDeleteFileOnDisk.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseBackupCleanup.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseBackup.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpDatabaseGetMostRecentBackupFromLocation.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpJobQueueCreate.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


if "%module%"=="all" goto hc
goto done

:hc
echo *-----------------------------------------------------------------------------*
echo Health Check: Creating Views ...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDatabaseAlwaysOnDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsSQLServerAgentJobsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsSQLAgentJobsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsSQLServerErrorlogDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsErrorlogDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsOSEventLogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDatabaseUsageHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDatabaseAlwaysOnDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Health Check: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

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

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpCheckIndexOnlineOperation.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpMarkInternalAction.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableForeignKeys.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpAlterTableIndexes.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcChangeFillFactorForIndexesFrequentlyFragmented.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcJobQueueCreate.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="all" goto mon
goto done


:mon
echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating Views ...
echo *-----------------------------------------------------------------------------*


echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\dbo.usp_monAlarmCustomFreeDiskSpace.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\dbo.usp_monAlarmCustomReplicationLatency.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\dbo.usp_monGetTransactionsStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\dbo.usp_monAlarmCustomTransactionsStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\dbo.usp_monGetSQLAgentFailedJobs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\stored-procedures\dbo.usp_monAlarmCustomSQLAgentFailedJobs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


if "%module%"=="all" goto done
goto done

:done
echo *-----------------------------------------------------------------------------*
echo The update was successful.
goto end



:connect
echo *-----------------------------------------------------------------------------*
echo Could not connect to the specified SQL Server instance.
goto end

:install_err
echo *-----------------------------------------------------------------------------*
echo An error occured while running installation script(s).
goto end

:help
echo USAGE : SQL Server Authentication
echo update.bat "server_name" "db_name" "module" "project_code" "data_files_path" "log_files_path" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo update.bat "server_name" "db_name" "module" "project_code" "data_files_path" "log_files_path"
echo .
echo "module: {all | health-check | maintenance-plan | monitoring}"
echo .
echo Example Call : SQL Server Authentication
echo update.bat . "dbaTDPMon" "all" "TEST" "D:\SQLData\Data\" "D:\SQLData\Log\" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo update.bat "LAB-SERVER" "dbaTDPMon" "all" "LAB"

echo *-----------------------------------------------------------------------------*
:end
