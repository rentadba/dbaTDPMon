-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.09.2019
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create job: $(dbName) - Database Maintenance - DBCC CHECKDB - Parallel', 10, 1) WITH NOWAIT
GO

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
SET @job_name = @databaseName + N' - Database Maintenance - DBCC CHECKDB - Parallel'
SET @logFileLocation = @logFileLocation + N'job-' + @job_name + N'.log'
SET @logFileLocation = [$(dbName)].[dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, @logFileLocation)

---------------------------------------------------------------------------------------------------
/* dropping job if exists */
---------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @job_name)
	EXEC msdb.dbo.sp_delete_job @job_name=@job_name, @delete_unused_schedule=1		

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
	SET @jobEnabled = 0

	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=@jobEnabled, 
											@notify_level_eventlog=0, 
											@notify_level_email=0, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'Custom Maintenance Plan for Database Backup
https://github.com/rentadba/dbaTDPMon', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'EXEC [dbo].[usp_runDatabaseCheckDBForAllSkippedWithinLastXDays]	
								@sqlServerNameFilter	= ''%'',
								@dbccCheckDBAgeDays		= 7,
								@maxDOP					= 1,
								@waitForDelay			= ''00:00:05'',
								@parallelJobs			= 2,
								@maxRunningTimeInMinutes= 360,
								@skipObjectsList		= NULL,
								@executeProjectBased	= 0,
								@onlyForProduction		= 1,
								@debugMode				= 0'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Run', 
												@step_id=1, 
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
												@output_file_name=@logFileLocation, 
												@flags=4
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
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
