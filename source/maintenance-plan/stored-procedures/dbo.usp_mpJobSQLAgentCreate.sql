RAISERROR('Create procedure: [dbo].[usp_mpJobSQLAgentCreate]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpJobSQLAgentCreate]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpJobSQLAgentCreate]
GO

CREATE PROCEDURE [dbo].[usp_mpJobSQLAgentCreate]
		@jobName				[sysname],
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@jobDescriptorList		[varchar](256)='%',		/*	dbo.usp_mpDatabaseConsistencyCheck
															dbo.usp_mpDatabaseOptimize
															dbo.usp_mpDatabaseShrink
															dbo.usp_mpDatabaseBackup(Data)
															dbo.usp_mpDatabaseBackup(Log)
														*/
		@flgActions				[int] = 16383,			/*	   1	Weekly: Database Consistency Check - only once a week on Saturday
															   2	Daily: Allocation Consistency Check
															   4	Weekly: Tables Consistency Check - only once a week on Sunday
															   8	Weekly: Reference Consistency Check - only once a week on Sunday
															  16	Monthly: Perform Correction to Space Usage - on the first Saturday of the month
															  32	Daily: Rebuild Heap Tables - only for SQL versions +2K5
															  64	Daily: Rebuild or Reorganize Indexes
															 128	Daily: Update Statistics 
															 256	Weekly: Shrink Database (TRUNCATEONLY) - only once a week on Sunday
															 512	Monthly: Shrink Log File - on the first Saturday of the month 
															1024	Daily: Backup User Databases (diff) 
															2048	Weekly: User Databases (full) - only once a week on Saturday 
															4096	Weekly: System Databases (full) - only once a week on Saturday 
															8192	Hourly: Backup User Databases Transaction Log 
														*/
		@skipDatabasesList		[nvarchar](1024) = ''	,/* databases list, comma separated, to be excluded from maintenance */
	    @recreateMode			[bit] = 0,				/*  1 - existings jobs will be dropped and created based on this stored procedure logic
															0 - jobs definition will be preserved; only status columns will be updated; new jobs are created, for newly discovered databases
														*/
		@enableJobs				[bit] = 1,
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS
SET NOCOUNT ON

-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 12.12.2019
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

IF CAST(SERVERPROPERTY('EngineEdition') AS [int]) IN (5, 6, 8) 
	AND (   CHARINDEX('dbo.usp_mpDatabaseBackup(Data)', @jobDescriptorList) > 0
		 OR CHARINDEX('dbo.usp_mpDatabaseBackup(Log)', @jobDescriptorList) > 0
		) 
	RETURN;


DECLARE   @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @databaseName		[sysname]
		, @strMessage		[nvarchar](1024)

SET @strMessage = 'Create job: ' + @jobName
RAISERROR(@strMessage, 10, 1) WITH NOWAIT

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

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
SET @logFileLocation = @logFileLocation + N'job-' + @jobName + N'.log'
SET @logFileLocation = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, @logFileLocation)
IF CAST(SERVERPROPERTY('EngineEdition') AS [int]) IN (5, 6, 8) SET @logFileLocation = NULL

---------------------------------------------------------------------------------------------------
/* will not drop/recreate the job if it exists */
---------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @jobName)
	RETURN;

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
		IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
			begin 
				ROLLBACK TRANSACTION
				RETURN
			end
	END

	---------------------------------------------------------------------------------------------------
	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@jobName, 
											@enabled=@enableJobs, 
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
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end

	---------------------------------------------------------------------------------------------------
	SET @databaseName = DB_NAME()

	SET @queryToRun=N'EXEC [dbo].[usp_mpJobQueueCreate]	@projectCode		= ' + CASE WHEN @projectCode IS NULL THEN N'DEFAULT' ELSE N'''' + @projectCode + N'''' END + N',
														@module				= ''maintenance-plan'',
														@sqlServerNameFilter= ''' + @sqlServerNameFilter + N''',
														@jobDescriptor		= ''' + @jobDescriptorList + N''',
														@flgActions			= ' + CAST(@flgActions AS [nvarchar]) + N',
														@skipDatabasesList  = ''' + ISNULL(@skipDatabasesList, '') + N''',
														@recreateMode		= ' + CAST(@recreateMode AS [nvarchar]) + N',
														@debugMode			= ' + CAST(@debugMode AS [nvarchar]) 

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Generate Job Queue', 
												@step_id=1, 
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
												@flags=4
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end
		
	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'EXEC dbo.usp_jobQueueExecute	@projectCode		= ' + CASE WHEN @projectCode IS NULL THEN N'DEFAULT' ELSE N'''' + @projectCode + N'''' END + N',
													@moduleFilter		= ''maintenance-plan'',
													@descriptorFilter	= ''' + @jobDescriptorList + N''',
													@waitForDelay		= DEFAULT,
													@parallelJobs		= DEFAULT,
													@maxRunningTimeInMinutes = DEFAULT,
													@debugMode			= ' + CAST(@debugMode AS [nvarchar])

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Execute Job Queue', 
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
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end

	---------------------------------------------------------------------------------------------------
	SET @queryToRun=N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @jobName + N''',
												@logFileLocation=' + CASE WHEN CAST(SERVERPROPERTY('EngineEdition') AS [int]) NOT IN (5, 6, 8) THEN '''' + @logFileLocation + '''' ELSE 'NULL' END + ',
												@module			=''maintenance-plan'',
												@sendLogAsAttachment = 1,
												@eventType		= 5'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Send email', 
												@step_id=3, 
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
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end

	---------------------------------------------------------------------------------------------------
	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=1, 
									@on_fail_action=4, 
									@on_fail_step_id=3

	EXEC msdb.dbo.sp_update_jobstep	@job_id=@jobId, 
									@step_id=2, 
									@on_fail_action=4, 
									@on_fail_step_id=3

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end

	---------------------------------------------------------------------------------------------------
	/* define "default" job schedule */
	DECLARE @startDate [int]
	SET @startDate = CAST(CONVERT([varchar](8), GETDATE(), 112) AS [int])

	IF CHARINDEX('dbo.usp_mpDatabaseBackup(Log)', @jobDescriptorList) > 0
		EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
														@name=N'Every 1 hour', 
														@enabled=1, 
														@freq_type=4, 
														@freq_interval=1, 
														@freq_subday_type=8, 
														@freq_subday_interval=1, 
														@freq_relative_interval=0, 
														@freq_recurrence_factor=0, 
														@active_start_date=@startDate, 
														@active_end_date=99991231, 
														@active_start_time=0, 
														@active_end_time=235959
	ELSE
		IF CHARINDEX('dbo.usp_mpDatabaseBackup(Data)', @jobDescriptorList) > 0
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
		ELSE
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

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
		begin 
			ROLLBACK TRANSACTION
			RETURN
		end

---------------------------------------------------------------------------------------------------
IF (@@TRANCOUNT > 0) COMMIT TRANSACTION
GO
