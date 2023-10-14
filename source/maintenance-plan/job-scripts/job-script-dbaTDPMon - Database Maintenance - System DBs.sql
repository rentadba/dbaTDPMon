-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 15.12.2014
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : system databases: master & msdb maintenance 
--					   default log file for the job is placed under %DefaultTraceFileLocation% if detected, is not, under C:\
-------------------------------------------------------------------------------
-- Change date		 : 08.01.2015
-- Description		 : unify code for 2k and 2k+ maintenance
-------------------------------------------------------------------------------
RAISERROR('Create job: Database Maintenance - System DBs', 10, 1) WITH NOWAIT

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @stepName			[sysname]
		, @queryParameters	[nvarchar](512)
		, @databaseName		[sysname]

------------------------------------------------------------------------------------------------------------------------------------------
--get default folder for SQL Agent jobs
SELECT	@logFileLocation = [value]
FROM	[$(dbName)].[dbo].[appConfigurations]
WHERE	[name] = N'Default folder for logs'
		AND [module] = 'common'

IF @logFileLocation IS NULL
		SELECT @logFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
		FROM (
				SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
			)er

SET @logFileLocation = ISNULL(@logFileLocation, N'C:\')
IF RIGHT(@logFileLocation, 1)<>'\' SET @logFileLocation = @logFileLocation + '\'

---------------------------------------------------------------------------------------------------
/* setting the job name & job log location */
---------------------------------------------------------------------------------------------------
SET @databaseName = N'$(dbName)'
SET @job_name = @databaseName + N' - Database Maintenance - System DBs'
SET @logFileLocation = @logFileLocation + N'job-' + @job_name + N'.log'
SET @logFileLocation = [$(dbName)].[dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, @logFileLocation)
IF CAST(SERVERPROPERTY('EngineEdition') AS [int]) IN (5, 6, 8) SET @logFileLocation = NULL

---------------------------------------------------------------------------------------------------
/* will not drop/recreate the job if it exists */
---------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @job_name)
	GOTO EndSave;
	--EXEC msdb.dbo.sp_delete_job @job_name=@job_name, @delete_unused_schedule=1		

---------------------------------------------------------------------------------------------------
/* creating the job */
---------------------------------------------------------------------------------------------------
BEGIN TRANSACTION

	DECLARE @ReturnCode INT
	SELECT @ReturnCode = 0
	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
	BEGIN
		EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB',
													@type=N'LOCAL', 
													@name=N'Database Maintenance'
		IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	END

	---------------------------------------------------------------------------------------------------
	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=1, 
											@notify_level_eventlog=0, 
											@notify_level_email=0, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'Custom Maintenance Plan for System Databases
https://github.com/rentadba/dbaTDPMon',
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* on the 1st of each month */
IF (DAY(GETDATE())=1)
	EXEC master.dbo.sp_cycle_errorlog'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'master - Cycle errorlog file (monthly)', 
												@step_id=1, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=4
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* only once a week on Saturday */
IF DATENAME(weekday, GETDATE()) = ''Saturday''
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= ''master'',
												@tableSchema			= ''%'',
												@tableName				= ''%'',
												@flgActions				= 1,
												@flgOptions				= 0,
												@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'master - Consistency Checks (weekly)', 
												@step_id=2, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* only once a week on Saturday */
IF DATENAME(weekday, GETDATE()) = ''Saturday''
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= ''msdb'',
												@tableSchema			= ''%'',
												@tableName				= ''%'',
												@flgActions				= 1,
												@flgOptions				= 0,
												@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Consistency Checks (weekly)', 
												@step_id=3, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2,
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* only once a week on Saturday */
IF DATENAME(weekday, GETDATE()) = ''Saturday''
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= ''model'',
												@tableSchema			= ''%'',
												@tableName				= ''%'',
												@flgActions				= 1,
												@flgOptions				= 0,
												@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'model - Consistency Checks (weekly)', 
												@step_id=4, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* only once a week on Saturday */
IF DATENAME(weekday, GETDATE()) = ''Saturday''
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= ''tempdb'',
												@tableSchema			= ''%'',
												@tableName				= ''%'',
												@flgActions				= 1,
												@flgOptions				= 0,
												@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'tempdb - Consistency Checks (weekly)', 
												@step_id=5, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* only once a week on Saturday */
IF DATENAME(weekday, GETDATE()) = ''Saturday'' AND EXISTS (SELECT * FROM master.dbo.sysdatabases WHERE [name]=''distribution'')
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= ''distribution'',
												@tableSchema			= ''%'',
												@tableName				= ''%'',
												@flgActions				= 1,
												@flgOptions				= 0,
												@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'distribution - Consistency Checks (weekly)', 
												@step_id=6, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* keep only last 6 months of backup history */
DECLARE		@oldestDate	[datetime],
			@str		[varchar](32)

SELECT @oldestDate=MIN([backup_finish_date])
FROM [msdb].[dbo].[backupset]

WHILE @oldestDate <= DATEADD(month, -6, GETDATE())
	begin
		SET @oldestDate=DATEADD(day, 1, @oldestDate)
		SET @str=CONVERT([varchar](20), @oldestDate, 120)

		RAISERROR(@str, 10, 1) WITH NOWAIT

		EXEC msdb.dbo.sp_delete_backuphistory @oldest_date = @oldestDate
	end
'
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Backup History Retention (6 months)', 
												@step_id=7, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* keep only last 12 months of job execution history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -12, GETDATE())
EXEC msdb.dbo.sp_purge_jobhistory @oldest_date = @oldestDate'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Job History Retention (12 months)', 
												@step_id=8, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* keep only last 6 months of maintenance plan history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
EXECUTE msdb.dbo.sp_maintplan_delete_log null, null, @oldestDate
DELETE FROM msdb.dbo.sysdbmaintplan_history WHERE end_time < @oldestDate  
DELETE FROM msdb.dbo.sysmaintplan_logdetail WHERE end_time < @oldestDate'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Maintenance Plan History Retention (6 months)', 
												@step_id=9, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName,
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* delete old mail items; especially, if you are sending attachements */
/* keep only last 6 months of history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
EXEC msdb.dbo.sysmail_delete_mailitems_sp @sent_before = @oldestDate'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Purge Old Mail Items (6 months)', 
												@step_id=10, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* delete the log of the sent items */
/* keep only last 6 months of history */

DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
EXEC msdb.dbo.sysmail_delete_log_sp @logged_before = @oldestDate'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Purge Old Mail Logs (6 months)', 
												@step_id=11, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* keep only last 6 months of replication alerts history */
BEGIN TRY
	EXEC sp_executesql N''DELETE FROM msdb.dbo.sysreplicationalerts WHERE time <= DATEADD(month, -6, GETDATE())''
END TRY
BEGIN CATCH
	PRINT ERROR_MESSAGE()
END CATCH'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Replication Alerts Retention (6 months)', 
												@step_id=12, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = '
/* only once a week on Sunday */
IF DATENAME(weekday, GETDATE()) = ''Sunday''
	EXEC [dbo].[usp_mpDatabaseOptimize]		@sqlServerName			= @@SERVERNAME,
											@dbName					= ''master'',
											@tableSchema			= ''%'',
											@tableName				= ''%'',
											@flgActions				= 11,
											@flgOptions				= DEFAULT,
											@defragIndexThreshold	= DEFAULT,
											@rebuildIndexThreshold	= DEFAULT,
											@statsSamplePercent		= DEFAULT,
											@statsAgeDays			= DEFAULT,
											@statsChangePercent		= DEFAULT,
											@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId,
												@step_name=N'master - Index & Statistics Maintenance (weekly)', 
												@step_id=13, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun,
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = '
/* only once a week on Sunday */
IF DATENAME(weekday, GETDATE()) = ''Sunday''
	EXEC [dbo].[usp_mpDatabaseOptimize]		@sqlServerName			= @@SERVERNAME,
											@dbName					= ''msdb'',
											@tableSchema			= ''%'',
											@tableName				= ''%'',
											@flgActions				= 11,
											@flgOptions				= DEFAULT,
											@defragIndexThreshold	= DEFAULT,
											@rebuildIndexThreshold	= DEFAULT,
											@statsSamplePercent		= DEFAULT,
											@statsAgeDays			= DEFAULT,
											@statsChangePercent		= DEFAULT,
											@debugMode				= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId,
												@step_name=N'msdb - Index & Statistics Maintenance (weekly)', 
												@step_id=14, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun,
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'DECLARE @databaseName [sysname]
/* only once a week on Monday */
IF DATENAME(weekday, GETDATE()) = ''Monday''
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] IN (''master'', ''msdb'', ''distribution'')
											AND [status] <> 0
											AND CASE WHEN [status] & 32 = 32 THEN ''LOADING''
													 WHEN [status] & 64 = 64 THEN ''PRE RECOVERY''
													 WHEN [status] & 128 = 128 THEN ''RECOVERING''
													 WHEN [status] & 256 = 256 THEN ''NOT RECOVERED''
													 WHEN [status] & 512 = 512 THEN ''OFFLINE''
													 WHEN [status] & 2097152 = 2097152 THEN ''STANDBY''
													 WHEN [status] & 1024 = 1024 THEN ''READ ONLY''
													 WHEN [status] & 2048 = 2048 THEN ''DBO USE ONLY''
													 WHEN [status] & 4096 = 4096 THEN ''SINGLE USER''
													 WHEN [status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													 WHEN [status] & 4194584 = 4194584 THEN ''SUSPECT''
													 ELSE ''ONLINE''
												END = ''ONLINE''
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@flgActions			= 2,	
													@flgOptions			= 1,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: Shrink Database (TRUNCATEONLY)', 
												@step_id=15, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'DECLARE @databaseName [sysname]
/* on the first Saturday of the month */
IF DATENAME(weekday, GETDATE()) = ''Saturday'' AND DATEPART(dd, GETDATE())<=7
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] IN (''master'', ''msdb'', ''tempdb'', ''distribution'')
											AND [status] <> 0
											AND CASE WHEN [status] & 32 = 32 THEN ''LOADING''
													 WHEN [status] & 64 = 64 THEN ''PRE RECOVERY''
													 WHEN [status] & 128 = 128 THEN ''RECOVERING''
													 WHEN [status] & 256 = 256 THEN ''NOT RECOVERED''
													 WHEN [status] & 512 = 512 THEN ''OFFLINE''
													 WHEN [status] & 2097152 = 2097152 THEN ''STANDBY''
													 WHEN [status] & 1024 = 1024 THEN ''READ ONLY''
													 WHEN [status] & 2048 = 2048 THEN ''DBO USE ONLY''
													 WHEN [status] & 4096 = 4096 THEN ''SINGLE USER''
													 WHEN [status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													 WHEN [status] & 4194584 = 4194584 THEN ''SUSPECT''
													 ELSE ''ONLINE''
												END = ''ONLINE''
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@flgActions			= 1,	
													@flgOptions			= 0,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end
	'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Monthly: Shrink Log File', 
												@step_id=16, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	DECLARE @successJobAction	[tinyint]

	SET @successJobAction= 3

	SET @queryToRun = 'EXEC dbo.usp_purgeHistoryData'

	SET @stepName = @databaseName + N' - Event Messages Retention'
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId,
												@step_name=@stepName, 
												@step_id=17, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun,
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=6

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @job_name + ''',
										@logFileLocation='+ + CASE WHEN CAST(SERVERPROPERTY('EngineEdition') AS [int]) NOT IN (5, 6, 8) THEN '''' + @logFileLocation + '''' ELSE 'null' END + ',
										@module			=''maintenance-plan'',
										@sendLogAsAttachment = 1,
										@eventType		= 2'
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Send email', 
												@step_id=18, 
												@cmdexec_success_code=0, 
												@on_success_action=1, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@flags=0
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=1, 
									@on_fail_action=4, 
									@on_fail_step_id=2

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=2, 
									@on_fail_action=4, 
									@on_fail_step_id=18

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=3, 
									@on_fail_action=4,
									@on_fail_step_id=18

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=4, 
									@on_fail_action=4, 
									@on_fail_step_id=18

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=5, 
									@on_fail_action=4, 
									@on_fail_step_id=18

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=6, 
									@on_fail_action=4, 
									@on_fail_step_id=18

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=7, 
									@on_fail_action=4, 
									@on_fail_step_id=8

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=8, 
									@on_fail_action=4, 
									@on_fail_step_id=9

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=9, 
									@on_fail_action=4, 
									@on_fail_step_id=10

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=10, 
									@on_fail_action=4, 
									@on_fail_step_id=11

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=11, 
									@on_fail_action=4, 
									@on_fail_step_id=12

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=12, 
									@on_fail_action=4, 
									@on_fail_step_id=13

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId,
									@step_id=13, 
									@on_fail_action=4, 
									@on_fail_step_id=14

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId,
									@step_id=14, 
									@on_fail_action=4, 
									@on_fail_step_id=15

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=15, 
									@on_fail_action=4, 
									@on_fail_step_id=16

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=16, 
									@on_fail_action=4, 
									@on_fail_step_id=17
	
	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId,
									@step_id=17, 
									@on_fail_action=4, 
									@on_fail_step_id=18
	
	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job	@job_id = @jobId, 
												@start_step_id = 1
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	DECLARE @startDate [int]
	SET @startDate = CAST(CONVERT([varchar](8), GETDATE(), 112) AS [int])

	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Daily at 0 am', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=1, 
													@freq_subday_interval=0, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=@startDate, 
													@active_end_date=99991231, 
													@active_start_time=000000, 
													@active_end_time=235959
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

---------------------------------------------------------------------------------------------------
COMMIT TRANSACTION
GOTO EndSave

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

---------------------------------------------------------------------------------------------------
GO
