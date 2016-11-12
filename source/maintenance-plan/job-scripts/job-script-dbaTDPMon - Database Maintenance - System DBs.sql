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
GO
USE [msdb]
GO

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @stepName			[sysname]
		, @queryParameters	[nvarchar](512)
		, @databaseName		[sysname]

DECLARE @SQLMajorVersion [int]

SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(CAST(SERVERPROPERTY('ProductVersion') AS [varchar](32)), ''), 2), '.', '') 

------------------------------------------------------------------------------------------------------------------------------------------
--get default folder for SQL Agent jobs
BEGIN TRY
	SELECT	@logFileLocation = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Default folder for logs'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @logFileLocation = NULL
END CATCH

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


---------------------------------------------------------------------------------------------------
/* dropping job if exists */
---------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @job_name)
	IF @SQLMajorVersion > 8
		EXEC msdb.dbo.sp_delete_job @job_name=@job_name, @delete_unused_schedule=1		
	ELSE
		EXEC msdb.dbo.sp_delete_job @job_name=@job_name

DECLARE @failedJobStep		[int],
		@failedJobAction	[int],
		@successJobStep		[int],
		@successJobAction	[int]

IF @SQLMajorVersion > 8
	begin
		SET @failedJobStep   = 18
		SET @failedJobAction = 4
		SET @successJobStep	 = 0
		SET @successJobAction= 3
	end
ELSE
	begin
		SET @failedJobStep   = 0
		SET @failedJobAction = 2
		SET @successJobStep	 = 0
		SET @successJobAction= 3
	end

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
http://dbaTDPMon.codeplex.com',
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
/* on the 1st of each month */
IF (DAY(GETDATE())=1)
	EXEC master.dbo.sp_cycle_errorlog'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 2
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'master - Cycle errorlog file (monthly)', 
												@step_id=1, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 18
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'master - Consistency Checks (weekly)', 
												@step_id=2, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 18
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Consistency Checks (weekly)', 
												@step_id=3, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction,
												@on_fail_step_id=@failedJobStep, 
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

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 18
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'model - Consistency Checks (weekly)', 
												@step_id=4, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 18
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'tempdb - Consistency Checks (weekly)', 
												@step_id=5, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
IF DATENAME(weekday, GETDATE()) = ''Saturday'' AND EXISTS (SELECT * FROM sys.databases WHERE [name]=''distribution'')
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= ''distribution'',
												@tableSchema			= ''%'',
												@tableName				= ''%'',
												@flgActions				= 1,
												@flgOptions				= 0,
												@debugMode				= DEFAULT'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 18
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'distribution - Consistency Checks (weekly)', 
												@step_id=6, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	
	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 8
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Backup History Retention (6 months)', 
												@step_id=7, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		SET @queryToRun = N'
/* keep only last 12 months of job execution history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -12, GETDATE())
EXEC msdb.dbo.sp_purge_jobhistory @oldest_date = @oldestDate'
	ELSE
		SET @queryToRun = N'
/* keep only last 12 months of job execution history */
DECLARE   @oldestDate	[datetime]
		, @oldRunDate	[int]

SET @oldestDate=DATEADD(month, -12, GETDATE())
SET @oldRunDate = CONVERT([int], CONVERT([varchar], @oldestDate, 112))  
DELETE FROM msdb.dbo.sysjobhistory WHERE (run_date < @oldRunDate)  '

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 9
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Job History Retention (12 months)', 
												@step_id=8, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		SET @queryToRun = N'
/* keep only last 6 months of maintenance plan history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
EXECUTE msdb.dbo.sp_maintplan_delete_log null, null, @oldestDate
DELETE FROM msdb.dbo.sysdbmaintplan_history WHERE end_time < @oldestDate  
DELETE FROM msdb.dbo.sysmaintplan_logdetail WHERE end_time < @oldestDate'
	ELSE
		SET @queryToRun = N'
/* keep only last 6 months of maintenance plan history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
DELETE FROM msdb.dbo.sysdbmaintplan_history WHERE end_time < @oldestDate'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 10
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Maintenance Plan History Retention (6 months)', 
												@step_id=9, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		SET @queryToRun = N'
/* delete old mail items; especially, if you are sending attachements */
/* keep only last 6 months of history */
DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
EXEC msdb.dbo.sysmail_delete_mailitems_sp @sent_before = @oldestDate'
	ELSE
		SET @queryToRun = N'/* only for SQL versions +2K5*/'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 11
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Purge Old Mail Items (6 months)', 
												@step_id=10, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		SET @queryToRun = N'
/* delete the log of the sent items */
/* keep only last 6 months of history */

DECLARE   @oldestDate	[datetime]

SET @oldestDate=DATEADD(month, -6, GETDATE())
EXEC msdb.dbo.sysmail_delete_log_sp @logged_before = @oldestDate'
	ELSE
		SET @queryToRun = N'/* only for SQL versions +2K5 */'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 12
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Purge Old Mail Logs (6 months)', 
												@step_id=11, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		SET @queryToRun = N'
/* keep only last 6 months of replication alerts history */
BEGIN TRY
	EXEC (''DELETE FROM msdb.dbo.sysreplicationalerts WHERE time <= DATEADD(month, -6, GETDATE())'')
END TRY
BEGIN CATCH
	PRINT ERROR_MESSAGE()
END CATCH'
	ELSE
		SET @queryToRun = N'/* only for SQL versions +2K5 */'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 13
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'msdb - Replication Alerts Retention (6 months)', 
												@step_id=12, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	EXEC [dbo].[usp_mpDatabaseOptimize]		@SQLServerName			= @@SERVERNAME,
											@DBName					= ''master'',
											@TableSchema			= ''%'',
											@TableName				= ''%'',
											@flgActions				= 11,
											@flgOptions				= DEFAULT,
											@DefragIndexThreshold	= DEFAULT,
											@RebuildIndexThreshold	= DEFAULT,
											@StatsSamplePercent		= DEFAULT,
											@StatsAgeDays			= DEFAULT,
											@StatsChangePercent		= DEFAULT,
											@DebugMode				= DEFAULT'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 14
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId,
												@step_name=N'master - Index & Statistics Maintenance (weekly)', 
												@step_id=13, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	EXEC [dbo].[usp_mpDatabaseOptimize]		@SQLServerName			= @@SERVERNAME,
											@DBName					= ''msdb'',
											@TableSchema			= ''%'',
											@TableName				= ''%'',
											@flgActions				= 11,
											@flgOptions				= DEFAULT,
											@DefragIndexThreshold	= DEFAULT,
											@RebuildIndexThreshold	= DEFAULT,
											@StatsSamplePercent		= DEFAULT,
											@StatsAgeDays			= DEFAULT,
											@StatsChangePercent		= DEFAULT,
											@DebugMode				= DEFAULT'


	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 15
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId,
												@step_name=N'msdb - Index & Statistics Maintenance (weekly)', 
												@step_id=14, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
									WHERE [name] IN (''master'', ''model'', ''msdb'', ''distribution'')
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
				EXEC [dbo].[usp_mpDatabaseShrink]	@SQLServerName		= @@SERVERNAME,
													@DBName				= @databaseName,
													@flgActions			= 2,	
													@flgOptions			= 1,
													@executionLevel		= DEFAULT,
													@DebugMode			= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 16
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: Shrink Database (TRUNCATEONLY)', 
												@step_id=15, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
									WHERE [name] IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
				EXEC [dbo].[usp_mpDatabaseShrink]	@SQLServerName		= @@SERVERNAME,
													@DBName				= @databaseName,
													@flgActions			= 1,	
													@flgOptions			= 0,
													@executionLevel		= DEFAULT,
													@DebugMode			= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end
	'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 17
			SET @failedJobAction = 4
		end

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Monthly: Shrink Log File', 
												@step_id=16, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 18
			SET @failedJobAction = 4
		end
	ELSE
		begin
			SET @successJobAction = 1
			SET @successJobStep = 0
			SET @failedJobStep   = 0
			SET @failedJobAction = 2
		end

	SET @queryToRun = 'DECLARE @retentionDays [int]

SELECT @retentionDays = [value]
FROM [dbo].[appConfigurations]
WHERE [name] = ''Log events retention (days)''
	AND [module] = ''common''

SET @retentionDays = ISNULL(@retentionDays, 0)
IF @retentionDays<>0
	begin
		PRINT (''Cleaning event history - keeping last '' + CAST(@retentionDays AS [varchar](32)) + '' days.'')
		SET ROWCOUNT 4096
		WHILE 1=1
			begin
				DELETE FROM [dbo].[logEventMessages]
				WHERE [event_date_utc] < DATEADD(dd, -@retentionDays, GETUTCDATE())

				IF @@ROWCOUNT=0
					BREAK
			end
		SET ROWCOUNT 0

		PRINT ''Done.''
	end'

	SET @stepName = @databaseName + N' - Event Messages Retention'
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId,
												@step_name=@stepName, 
												@step_id=17, 
												@cmdexec_success_code=0, 
												@on_success_action=@successJobAction, 
												@on_success_step_id=@successJobStep, 
												@on_fail_action=@failedJobAction, 
												@on_fail_step_id=@failedJobStep, 
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
	IF @SQLMajorVersion > 8
		begin
	SET @queryToRun=N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @job_name + ''',
												@logFileLocation=''' + @logFileLocation + ''',
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
	end

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job	@job_id = @jobId, 
												@start_step_id = 1
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Daily', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=1, 
													@freq_subday_interval=0, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=20141215, 
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
