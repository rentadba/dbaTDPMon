-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.01.2011
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : user databases
--					   default log file for the job is placed under %DefaultTraceFileLocation% if detected, is not, under C:\
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create job: Database Backup - Full and Diff', 10, 1) WITH NOWAIT
GO
USE [msdb]
GO

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @queryToRun1		[varchar](8000)
		, @queryToRun2		[varchar](8000)
		, @queryParameters	[nvarchar](512)
		, @databaseName		[sysname]

DECLARE @SQLMajorVersion [int]

SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(CAST(SERVERPROPERTY('ProductVersion') AS [varchar](32)), ''), 2), '.', '') 

SELECT @logFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
FROM (
		SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
	)er


IF @logFileLocation IS NULL SET @logFileLocation =N'C:\'

---------------------------------------------------------------------------------------------------
/* setting the job name & job log location */
---------------------------------------------------------------------------------------------------
SET @databaseName = N'$(dbName)'
SET @job_name = @databaseName + N' - Database Backup - Full and Diff'
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
		SET @failedJobStep   = 4
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
											@description=N'Custom Maintenance Plan for Database Backup
http://dbaTDPMon.codeplex.com', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'DECLARE @databaseName [sysname]

/* only once a week on Saturday */
IF DATEPART(dw, GETUTCDATE())=7
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] IN (''master'', ''model'', ''msdb'', ''distribution'', ''distribution'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseBackup]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@backupLocation		= DEFAULT,
													@flgActions			= 1,	
													@flgOptions			= DEFAULT,	
													@retentionDays		= DEFAULT,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 2
			SET @failedJobAction = 4
		end
		
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: System Databases (full)', 
												@step_id=1, 
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
												@flags=4
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	IF @SQLMajorVersion > 8
		begin
			SET @failedJobStep   = 3
			SET @failedJobAction = 4
		end

	SET @queryToRun=N'DECLARE @databaseName [sysname]

/* all days except Saturday */
IF DATEPART(dw, GETUTCDATE())<>7
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseBackup]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@backupLocation		= DEFAULT,
													@flgActions			= 2,	
													@flgOptions			= DEFAULT,	
													@retentionDays		= DEFAULT,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Daily: User Databases (diff)', 
												@step_id=2, 
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
			SET @failedJobStep   = 4
			SET @failedJobAction = 4
		end
	ELSE
		begin
			SET @successJobAction = 1
			SET @successJobStep  = 0
			SET @failedJobStep   = 0
			SET @failedJobAction = 2
		end
		
	SET @queryToRun=N'DECLARE @databaseName [sysname]

/* only once a week on Saturday */
IF DATEPART(dw, GETUTCDATE())=7
	begin
		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [name] 
									FROM master.dbo.sysdatabases
									WHERE [name] NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''distribution'')
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_mpDatabaseBackup]	@sqlServerName		= @@SERVERNAME,
													@dbName				= @databaseName,
													@backupLocation		= DEFAULT,
													@flgActions			= 1,	
													@flgOptions			= DEFAULT,	
													@retentionDays		= DEFAULT,
													@executionLevel		= DEFAULT,
													@debugMode			= DEFAULT

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases
	end'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Weekly: User Databases (full)', 
												@step_id=3, 
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
												@eventType		= 5'

		EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
													@step_name=N'Send email', 
													@step_id=4, 
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
	EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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
