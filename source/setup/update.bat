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
set userid=%3
set password=%4

if !%3==! goto trusted_connection
set autentif=-U%userid% -P%password%

goto start

:trusted_connection
set autentif=-E

:start
echo Checking connection...
sqlcmd.exe -S%server% %autentif% -d master -Q "" -b -m-1
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

set run2kmode=false
if "%engine_version%"=="8" (
	echo Update mode is not supported for SQL Server 2000. Run Install mode.
	goto end
	)
    
:common

sqlcmd.exe -S%server% %autentif% -i "detect-version.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

echo *-----------------------------------------------------------------------------*
echo Common: Running table's patching scripts...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\patches\20160825-patch-upgrade-from-v2016_6-to-v2016_9-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20161119-patch-upgrade-from-v2016_11-to-v2017_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170324-patch-upgrade-from-v2016_11-to-v2017_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170428-patch-upgrade-from-v2017_4-to-v2017_5-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170509-patch-upgrade-from-v2017_4-to-v2017_5-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170516-patch-upgrade-from-v2017_4-to-v2017_6-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180131-patch-upgrade-from-v2017_12-to-v2018_1-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180301-patch-upgrade-from-v2018_1-to-v2018_3-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180405-patch-upgrade-from-v2018_3-to-v2018_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180411-patch-upgrade-from-v2018_3-to-v2018_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180417-patch-upgrade-from-v2018_3-to-v2018_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180420-patch-upgrade-from-v2018_3-to-v2018_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180423-patch-upgrade-from-v2018_3-to-v2018_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180426-patch-upgrade-from-v2018_3-to-v2018_4-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180501-patch-upgrade-from-v2018_4-to-v2018_5-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180605-patch-upgrade-from-v2018_5-to-v2018_6-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180620-patch-upgrade-from-v2018_5-to-v2018_6-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180628-patch-upgrade-from-v2018_5-to-v2018_6-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180703-patch-upgrade-from-v2018_6-to-v2018_7-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180709-patch-upgrade-from-v2018_6-to-v2018_7-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180711-patch-upgrade-from-v2018_6-to-v2018_7-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180713-patch-upgrade-from-v2018_6-to-v2018_7-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20181010-patch-upgrade-from-v2018_9-to-v2018_10-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20181017-patch-upgrade-from-v2018_9-to-v2018_10-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20181127-patch-upgrade-from-v2018_10-to-v2018_11-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20190109-patch-upgrade-from-v2018_12-to-v2019_01-common.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20190110-patch-upgrade-from-v2018_12-to-v2019_01-common.sql" -d %dbname%  -b -r 1
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
	
:mp
sqlcmd.exe -S%server% %autentif% -Q "set nocount on; select ltrim(rtrim(name)) from sys.schemas where name='maintenance-plan'" -d %dbname% -o check-schema.out -b -r 1

SET schema_installed=0
FOR /F "tokens=1 delims==" %%A IN ('FINDSTR /R "maintenance-plan" check-schema.out') DO (SET schema_installed=1)
del check-schema.out
if "%schema_installed%" == "0" goto hc

echo *-----------------------------------------------------------------------------*
echo Maintenance Plan: Running table's patching scripts...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\patches\20161025-patch-upgrade-from-v2016_9-to-v2016_11-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170324-patch-upgrade-from-v2016_11-to-v2017_4-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170520-patch-upgrade-from-v2017_4-to-v2017_6-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180112-patch-upgrade-from-v2017_6-to-v2017_12-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180301-patch-upgrade-from-v2018_1-to-v2018_3-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180411-patch-upgrade-from-v2018_3-to-v2018_4-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180412-patch-upgrade-from-v2018_3-to-v2018_4-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180416-patch-upgrade-from-v2018_3-to-v2018_4-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180818-patch-upgrade-from-v2018_7-to-v2018_8-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180828-patch-upgrade-from-v2018_7-to-v2018_8-mp.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


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

:hc
SET schema_installed=0
sqlcmd.exe -S%server% %autentif% -Q "set nocount on; select name from sys.schemas where name='health-check' and exists(select * from sys.objects where name='usp_hcCollectDatabaseDetails')" -d %dbname% -o check-schema.out -b -r 1
FOR /F "tokens=1 delims==" %%A IN ('FINDSTR /R "health-check" check-schema.out') DO (SET schema_installed=1)
del check-schema.out
if "%schema_installed%" == "0" goto mon

echo *-----------------------------------------------------------------------------*
echo Health Check: Running table's patching scripts...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170324-patch-upgrade-from-v2016_11-to-v2017_4-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20170526-patch-upgrade-from-v2017_4-to-v2017_6-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180411-patch-upgrade-from-v2018_3-to-v2018_4-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180426-patch-upgrade-from-v2018_3-to-v2018_4-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180613-patch-upgrade-from-v2018_5-to-v2018_6-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180619-patch-upgrade-from-v2018_5-to-v2018_6-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180703-patch-upgrade-from-v2018_6-to-v2018_7-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180820-patch-upgrade-from-v2018_7-to-v2018_8-hc.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Health Check: Creating Views ...
echo *-----------------------------------------------------------------------------*

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

:mon
SET schema_installed=0
sqlcmd.exe -S%server% %autentif% -Q "set nocount on; select name from sys.schemas where name='monitoring'" -d %dbname% -o check-schema.out -b -r 1
FOR /F "tokens=1 delims==" %%A IN ('FINDSTR /R "monitoring" check-schema.out') DO (SET schema_installed=1)
del check-schema.out
if "%schema_installed%" == "0" goto done

echo *-----------------------------------------------------------------------------*
echo Monitoring: Running table's patching scripts...
echo *-----------------------------------------------------------------------------*

sqlcmd.exe -S%server% %autentif% -i "..\patches\20160624-patch-upgrade-from-v2015_12-to-v2016_6-mon.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180128-patch-upgrade-from-v2017_12-to-v2018_1-mon.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180411-patch-upgrade-from-v2018_3-to-v2018_4-mon.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20180418-patch-upgrade-from-v2018_3-to-v2018_4-mon.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

sqlcmd.exe -S%server% %autentif% -i "..\patches\20181017-patch-upgrade-from-v2018_9-to-v2018_10-mon.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err


echo *-----------------------------------------------------------------------------*
echo Monitoring: Creating Views ...
echo *-----------------------------------------------------------------------------*


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


:done
if "%run2kmode%"=="false" sqlcmd.exe -S%server% %autentif% -Q "SET NOCOUNT ON; UPDATE [dbo].[appConfigurations] SET [value] = N'2019.01.11' WHERE [module] = 'common' AND [name] = 'Application Version'" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err  

echo *-----------------------------------------------------------------------------*
sqlcmd.exe -S%server% %autentif% -i "detect-version.sql" -d %dbname%  -b -r 1
if errorlevel 1 goto install_err

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
echo update.bat "server_name" "db_name" "module" "login_id" "login_password"
echo .
echo USAGE : Windows Authentication
echo update.bat "server_name" "db_name" "module" 
echo .
echo Example Call : SQL Server Authentication
echo update.bat . "dbaTDPMon" "testuser" "testpassword"
echo .
echo Example Call : Windows Authentication
echo update.bat "LAB-SERVER" "dbaTDPMon"

echo *-----------------------------------------------------------------------------*
:end
endlocal
