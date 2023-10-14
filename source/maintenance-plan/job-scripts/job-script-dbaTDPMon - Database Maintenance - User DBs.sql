-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.01.2011
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : user databases
--					   default log file for the job is placed under %DefaultTraceFileLocation% if detected, is not, under C:\
-------------------------------------------------------------------------------
-- Change date		 : 08.01.2015
-- Description		 : unify code for 2k and 2k+ maintenance
-------------------------------------------------------------------------------
RAISERROR('Create job: Database Maintenance - User DBs', 10, 1) WITH NOWAIT

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @queryToRun1		[varchar](8000)
		, @queryToRun2		[varchar](8000)
		, @queryParameters	[nvarchar](512)
		, @databaseName		[sysname]
		, @jobEnabled		[tinyint]

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
SET @job_name = @databaseName + N' - Database Maintenance - User DBs'
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
	SET @jobEnabled = 1

	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=@jobEnabled, 
											@notify_level_eventlog=0, 
											@notify_level_email=0, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'Custom Maintenance Plan for User Databases
https://github.com/rentadba/dbaTDPMon', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'/* only for SQL versions +2K5 */'	 
	SET @queryToRun=N'EXEC [dbo].[usp_mpDatabaseKillConnections]	@sqlServerName		= @@SERVERNAME,
					@dbName		= DEFAULT,
					@flgOptions		= DEFAULT,
					@debugMode		= DEFAULT'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Daily: Kill Orphan Connections', 
												@step_id=1, 
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
												@flags=4
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'DECLARE @databaseName [sysname]
/* only once a week on Saturday */
IF DATENAME(weekday, GETDATE()) = ''Saturday''
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
												END IN (''ONLINE'', ''READ ONLY'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 1,
															@flgOptions				= 3,
															@maxDOP					= DEFAULT,
															@maxRunningTimeInMinutes= DEFAULT,
															@debugMode				= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: Database Consistency Check', 
												@step_id=2, 
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
							, @flgActions	[int]
	
	/* when running DBCC CHECKDB, skip running DBCC CHECKALLOC*/
	IF DATENAME(weekday, GETDATE()) = ''Saturday''
		SET @flgActions = 8
	ELSE
		SET @flgActions = 12

	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
												END IN (''ONLINE'', ''READ ONLY'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= @flgActions,
															@flgOptions				= DEFAULT,
															@maxDOP					= DEFAULT,
															@maxRunningTimeInMinutes= DEFAULT,
															@debugMode				= DEFAULT

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Daily: Allocation Consistency Check', 
												@step_id=3, 
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
/* only once a week on Sunday */
IF DATENAME(weekday, GETDATE()) = ''Sunday''
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
												END IN (''ONLINE'', ''READ ONLY'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 34,
															@flgOptions				= DEFAULT,
															@maxDOP					= DEFAULT,
															@maxRunningTimeInMinutes= DEFAULT,
															@debugMode				= DEFAULT

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: Tables Consistency Check', 
												@step_id=4, 
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
/* only once a week on Sunday */
IF DATENAME(weekday, GETDATE()) = ''Sunday''
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
												END IN (''ONLINE'', ''READ ONLY'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 16,
															@flgOptions				= DEFAULT,
															@maxDOP					= DEFAULT,
															@maxRunningTimeInMinutes= DEFAULT,
															@debugMode				= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: Reference Consistency Check', 
												@step_id=5, 
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
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
				EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 64,
															@flgOptions				= DEFAULT,
															@maxDOP					= DEFAULT,
															@maxRunningTimeInMinutes= DEFAULT,
															@debugMode				= DEFAULT

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: Perform Correction to Space Usage', 
												@step_id=6, 
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
	SET @queryToRun=N'/* only for SQL versions +2K5 */'	 
	SET @queryToRun=N'DECLARE @databaseName [sysname]
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
				EXEC [dbo].[usp_mpDatabaseOptimize]			@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 16,
															@flgOptions				= DEFAULT,
															@defragIndexThreshold	= DEFAULT,
															@rebuildIndexThreshold	= DEFAULT,
															@debugMode				= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Daily: Rebuild Heap Tables', 
												@step_id=7, 
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
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')	
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
				EXEC [dbo].[usp_mpDatabaseOptimize]			@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 3,
															@flgOptions				= DEFAULT,
															@defragIndexThreshold	= DEFAULT,
															@rebuildIndexThreshold	= DEFAULT,
															@debugMode				= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Daily: Rebuild or Reorganize Indexes', 
												@step_id=8, 
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
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
				EXEC [dbo].[usp_mpDatabaseOptimize]			@sqlServerName			= @@SERVERNAME,
															@dbName					= @databaseName,
															@tableSchema			= ''%'',
															@tableName				= ''%'',
															@flgActions				= 8,
															@flgOptions				= DEFAULT,
															@statsSamplePercent		= DEFAULT,
															@statsAgeDays			= DEFAULT,
															@statsChangePercent		= DEFAULT,
															@debugMode				= DEFAULT
				
				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Daily: Update Statistics', 
												@step_id=9, 
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
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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
												@step_id=10, 
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
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
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

	DECLARE @successJobAction [tinyint]

	SET @successJobAction = 3

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Monthly: Shrink Log File', 
												@step_id=11, 
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
												@step_id=12, 
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
									@on_fail_step_id=12

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=3, 
									@on_fail_action=4, 
									@on_fail_step_id=12

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=4, 
									@on_fail_action=4, 
									@on_fail_step_id=12

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=5, 
									@on_fail_action=4, 
									@on_fail_step_id=12

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=6, 
									@on_fail_action=4, 
									@on_fail_step_id=7

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

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	DECLARE @startDate [int]
	SET @startDate = CAST(CONVERT([varchar](8), GETDATE(), 112) AS [int])

	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Daily at 2 am', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=1, 
													@freq_subday_interval=0, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=@startDate, 
													@active_end_date=99991231, 
													@active_start_time=020000, 
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

