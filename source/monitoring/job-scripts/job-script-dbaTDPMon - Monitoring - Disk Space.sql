-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : SQL Server 2005/2008/2008R2/2012/2014+
-- Description		 : monitor disk/volume free space and alert
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create job: Monitoring - Disk Space', 10, 1) WITH NOWAIT
GO

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @databaseName		[sysname]
		, @projectCode		[nvarchar](32)

------------------------------------------------------------------------------------------------------------------------------------------
--get default folder for SQL Agent jobs
BEGIN TRY
	SELECT	@logFileLocation = [value]
	FROM	[$(dbName)].[dbo].[appConfigurations]
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
SET @projectCode  = N'$(projectCode)'	/* add local project code here */

SET @databaseName = N'$(dbName)'
SET @job_name = @databaseName + N' - Monitoring - Disk Space'
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

	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=1, 
											@notify_level_eventlog=0, 
											@notify_level_email=2, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'Free Disk/Volume Space custom monitoring and alarms
https://github.com/rentadba/dbaTDPMon', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
DECLARE		@strMessage				[varchar](8000),
			@currentRunning			[int],
			@lastExecutionStatus	[int],
			@lastExecutionDate		[varchar](10),
			@lastExecutionTime 		[varchar](8),
			@runningTimeSec			[bigint],
			@jobName				[sysname],
			@jobID					[sysname]

SELECT		@strMessage			 = '''',
			@currentRunning		 = 0,
			@lastExecutionStatus = 0,
			@lastExecutionDate	 = '''',
			@lastExecutionTime 	 = '''',
			@runningTimeSec		 = 0


/* check execution overlapping with Health Check job */
SELECT TOP 1 @jobName = sj.[name], 
			 @jobID   = sj.[job_id]
FROM [msdb].dbo.sysjobs sj
INNER JOIN [msdb].dbo.sysjobsteps sjs ON sj.[job_id] = sjs.[job_id] 
WHERE sj.[name] LIKE ''%Discovery & Health Check%''
	AND sjs.[database_name] = DB_NAME()

SET @lastExecutionStatus = 4
WHILE @lastExecutionStatus = 4 AND @jobName IS NOT NULL
	begin
		EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @@SERVERNAME,
											@jobName				= @jobName,
											@jobID					= @jobID,
											@strMessage				= @strMessage OUTPUT,	
											@currentRunning			= @currentRunning OUTPUT,			
											@lastExecutionStatus	= @lastExecutionStatus OUTPUT,			
											@lastExecutionDate		= @lastExecutionDate OUTPUT,		
											@lastExecutionTime 		= @lastExecutionTime OUTPUT,	
											@runningTimeSec			= @runningTimeSec OUTPUT,
											@selectResult			= 0,
											@extentedStepDetails	= 0,		
											@debugMode				= 0
		IF @lastExecutionStatus = 4
			begin
				SET @strMessage = ''Job "'' + @jobName + ''" is currently running. Waiting for its completion...''
				RAISERROR(@strMessage, 10, 1) WITH NOWAIT

				WAITFOR DELAY ''00:00:30''
			end
	end
'
	
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Check Overlapping jobs', 
												@step_id=1, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'EXEC [dbo].[usp_hcJobQueueCreate]	@projectCode			= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N',
															@module					= ''monitoring'',
															@sqlServerNameFilter	= ''%'',
															@collectorDescriptor	= ''dbo.usp_hcCollectDiskSpaceUsage'',
															@enableXPCMDSHELL		= 1,
															@recreateMode			= 0,
															@debugMode				= 0'
	
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Generate Data Collector Job Queue', 
												@step_id=2, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=3, 
												@retry_interval=1, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'EXEC [dbo].[usp_jobQueueExecute]	@projectCode			= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N',
															@moduleFilter			= ''monitoring'',
															@descriptorFilter		= ''dbo.usp_hcCollectDiskSpaceUsage'',
															@waitForDelay			= DEFAULT,
															@debugMode				= 0'
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Run Job Queue', 
												@step_id=3, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=3, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'EXEC [dbo].[usp_monAlarmCustomFreeDiskSpace] @projectCode	= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N',
																	@sqlServerName	= ''%'''

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Raise Alarms', 
												@step_id=4, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @job_name + ''',
												@logFileLocation='+ + CASE WHEN CAST(SERVERPROPERTY('EngineEdition') AS [int]) NOT IN (5, 6, 8) THEN '''' + @logFileLocation + '''' ELSE 'null' END + ',
												@module			=''monitoring'',
												@sendLogAsAttachment = 1,
												@eventType		= 2'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Send email', 
												@step_id=5, 
												@cmdexec_success_code=0, 
												@on_success_action=1, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@flags=0

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=1, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=2, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=3, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=4, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job	@job_id = @jobId, 
												@start_step_id = 1

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	DECLARE @startDate [int]
	SET @startDate = CAST(CONVERT([varchar](8), GETDATE(), 112) AS [int])

	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Business Time - Every 15 minutes', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=4, 
													@freq_subday_interval=15, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=@startDate, 
													@active_end_date=99991231, 
													@active_start_time=63000, 
													@active_end_time=223000

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Early Morning - Every 30 minutes', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=4, 
													@freq_subday_interval=30, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=@startDate, 
													@active_end_date=99991231, 
													@active_start_time=0, 
													@active_end_time=53000

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Late Evening - Every 30 minutes', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=4, 
													@freq_subday_interval=30, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=@startDate, 
													@active_end_date=99991231, 
													@active_start_time=223000, 
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
