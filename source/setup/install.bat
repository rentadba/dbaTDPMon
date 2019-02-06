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
		echo Only maintenance-plan module is supported on SQL Server 2000. Will install it.
		SET module=maintenance-plan
		)
	)

if "%module%"=="all" goto common
if "%module%"=="health-check" goto common
if "%module%"=="maintenance-plan" goto common
if "%module%"=="monitoring" goto common
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

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\schema\create-schema-report.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.appConfigurations.sql" -d %dbname% -v projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogSolutions.sql" -d %dbname% -v projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogProjects.sql" -d %dbname% -v projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogMachineNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogInstanceNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.catalogDatabaseNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.appInternalTasks.sql" -d %dbname% 
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.logEventMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.jobExecutionQueue.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.jobExecutionHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.jobExecutionStatisticsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\dbo.logAnalysisMessages.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\report.hardcodedFilters.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\report.htmlGraphics.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\report.htmlOptions.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\report.htmlSkipRules.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\tables\report.htmlContent.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_getObjectQuoteName.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_catalogInstanceNames.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_formatPlatformSpecificPath.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_catalogProjects.sql" -d %dbname%  -b -r 1
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

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_jobExecutionStatistics.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_jobExecutionStatisticsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\views\dbo.vw_jobSchedulerDetails.sql" -d %dbname%  -b -r 1
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

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\functions\dbo.ufn_getProjectCode.sql" -d %dbname%  -b -r 1
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

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\common\stored-procedures\dbo.usp_jobExecutionSaveStatistics.sql" -d %dbname%  -b -r 1
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

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -Q "EXEC dbo.usp_refreshMachineCatalogs DEFAULT, @@SERVERNAME;" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -Q "SET NOCOUNT ON; UPDATE [dbo].[appConfigurations] SET [value] = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, [value]) WHERE [name] = 'Local storage path for HTML reports' AND [module] = 'common';" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -Q "SET NOCOUNT ON; UPDATE [dbo].[appConfigurations] SET [value] = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, [value]) WHERE [name] = 'Default folder for logs' AND [module] = 'common';" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -Q "SET NOCOUNT ON; UPDATE [dbo].[appConfigurations] SET [value] = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, [value]) WHERE [name] = 'Default backup location' AND [module] = 'maintenance-plan';" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

	
if "%module%"=="all" goto mp
if "%module%"=="health-check" goto hc
if "%module%"=="maintenance-plan" goto mp
if "%module%"=="monitoring" goto mon
goto help


:mp
echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\schema\create-schema-maintenance-plan.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\tables\insert-into-dbo.appInternalTasks.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\tables\maintenance-plan.logInternalAction.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\tables\maintenance-plan.internalScheduler.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\tables\maintenance-plan.objectSkipList.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

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

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\stored-procedures\dbo.usp_mpJobQueueCreate.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Maintenance - System DBs.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Maintenance - User DBs.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Maintenance - User DBs - Parallel.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Backup - Full and Diff.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Backup - Full and Diff - Parallel.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Backup - Log.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -i "..\maintenance-plan\job-scripts\job-script-dbaTDPMon - Database Backup - Log - Parallel.sql" -d msdb -v dbName=%dbname% -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="all" goto hc
goto done

:hc
echo *-----------------------------------------------------------------------------*
echo Health Check: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\health-check\schema\create-schema-health-check.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\insert-into-dbo.appInternalTasks.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsSQLAgentJobsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsErrorlogDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsOSEventLogs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDatabaseUsageHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDatabaseAlwaysOnDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsSQLAgentJobsHistory.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
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


echo *-----------------------------------------------------------------------------*
echo Health Check: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\health-check\job-scripts\job-script-dbaTDPMon - Discovery & Health Check.sql" -d msdb -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

if "%module%"=="all" goto mon
goto done


:mon
echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating Table / Views and Indexes...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\schema\create-schema-monitoring.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\insert-into-dbo.appInternalTasks.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\monitoring.alertThresholds.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\monitoring.alertSkipRules.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\monitoring.statsReplicationLatency.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\monitoring.statsTransactionsStatus.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\monitoring.statsSQLAgentJobs.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\tables\monitoring.alertAdditionalRecipients.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating Functions / Stored Procedures
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\functions\dbo.ufn_monGetAdditionalAlertRecipients.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

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

set healthCheckInstalled=true
if "%module%"=="monitoring" set healthCheckInstalled=false

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\schema\create-schema-health-check.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDatabaseDetails.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\tables\health-check.statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\views\health-check.vw_statsDiskSpaceInfo.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcJobQueueCreate.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

if "%healthCheckInstalled%" == "false" sqlcmd.exe -S%server% %autentif% -i "..\health-check\stored-procedures\dbo.usp_hcCollectDiskSpaceUsage.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err



echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating SQL Server Agent Jobs
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\job-scripts\job-script-dbaTDPMon - Monitoring - Disk Space.sql" -d msdb -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\job-scripts\job-script-dbaTDPMon - Monitoring - Replication.sql" -d msdb -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\job-scripts\job-script-dbaTDPMon - Monitoring - TransactionStatus.sql" -d msdb -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\monitoring\job-scripts\job-script-dbaTDPMon - Monitoring - SQLAgentFailedJobs.sql" -d msdb -v dbName=%dbname% projectCode=%project% -b -r 1
if errorlevel 1 goto install_err




if "%module%"=="all" goto done
goto done

:done
if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -Q "SET NOCOUNT ON; UPDATE [dbo].[appConfigurations] SET [value] = N'2019.02.06' WHERE [module] = 'common' AND [name] = 'Application Version'" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err  

echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "detect-version.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

echo The installation was successful.
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
echo install.bat "server_name" "db_name" "module" "project_code" "data_files_path" "log_files_path" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo install.bat "server_name" "db_name" "module" "project_code" "data_files_path" "log_files_path"
echo .
echo "module: {all | health-check | maintenance-plan | monitoring}"
echo .
echo Example Call : SQL Server Authentication
echo install.bat . "dbaTDPMon" "all" "TEST" "D:\SQLData\Data\" "D:\SQLData\Log\" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo install.bat "LAB-SERVER" "dbaTDPMon" "all" "LAB"

echo *-----------------------------------------------------------------------------*
:end
endlocal
