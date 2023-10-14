-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 22.01.2015
-- Module			 : SQL Server 2005/2008/2008R2/2012+
-- Description		 : run analysis for Discovery & Health Check
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create job: Discovery & Health Check', 10, 1) WITH NOWAIT
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
IF @projectCode IS NULL	SET @projectCode = 'DEFAULT'

SET @databaseName = N'$(dbName)'
SET @job_name = @databaseName + N' - Discovery & Health Check'
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

	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=1, 
											@notify_level_eventlog=0, 
											@notify_level_email=0, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'SQL Server instance discovery and daily health check report
https://github.com/rentadba/dbaTDPMon', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
	EXEC [dbo].[usp_refreshProjectCatalogsAndDiscovery] @projectCode	 = ''' + @projectCode + N''',
														@runDiscovery	 = 0,
														@enableXPCMDSHELL= 1,
														@debugMode		 = 0'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Catalog Upsert: Discovery & Update', 
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
	SET @queryToRun = N'EXEC [dbo].[usp_hcJobQueueCreate]	@projectCode			= ''' + @projectCode + N''',
															@module					= ''health-check'',
															@sqlServerNameFilter	= ''%'',
															@collectorDescriptor	= ''%'',
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
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'exec dbo.usp_jobQueueExecute	@projectCode			= ''' + @projectCode + N''',
														@moduleFilter			= ''health-check'',
														@descriptorFilter		= ''%'',
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
												@retry_interval=1, 
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'EXEC  [dbo].[usp_reportHTMLBuildHealthCheck]	@projectCode		= ''' + @projectCode + ''',
							@sqlServerNameFilter = DEFAULT,
							@flgActions		= DEFAULT,	
							@flgOptions		= DEFAULT,
							@reportDescription		= NULL,
							@reportFileName		= NULL,	/* if file name is null, than the name will be generated */
							@localStoragePath		= NULL,
							@dbMailProfileName	= DEFAULT,		
							@recipientsList		= DEFAULT,
							@sendReportAsAttachment	= 1		/* if set to 1, the report file will always be attached */'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Generate Daily Health Check Reports', 
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
	SET @queryToRun=N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @job_name + ''',
												@logFileLocation='+ + CASE WHEN CAST(SERVERPROPERTY('EngineEdition') AS [int]) NOT IN (5, 6, 8) THEN '''' + @logFileLocation + '''' ELSE 'null' END + ',
												@module			=''daily health check'',
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
												@os_run_priority=0, 
												@subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@flags=0
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=1, 
									@on_success_action=4, 
									@on_success_step_id=2, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=2, 
									@on_success_action=4, 
									@on_success_step_id=3, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=3, 
									@on_success_action=4, 
									@on_success_step_id=4, 
									@on_fail_action=4, 
									@on_fail_step_id=5

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=4, 
									@on_success_action=4, 
									@on_success_step_id=5, 
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
													@name=N'Daily at 5 am', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=1, 
													@freq_subday_interval=0, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=@startDate, 
													@active_end_date=99991231, 
													@active_start_time=50000, 
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


