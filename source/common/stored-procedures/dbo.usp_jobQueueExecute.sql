RAISERROR('Create procedure: [dbo].[usp_jobQueueExecute]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_jobQueueExecute]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_jobQueueExecute]
GO

CREATE PROCEDURE dbo.usp_jobQueueExecute
		@projectCode				[varchar](32) = NULL,
		@moduleFilter				[varchar](32) = '%',
		@descriptorFilter			[varchar](256)= '%',
		@waitForDelay				[varchar](8)  = '00:00:05',
		@parallelJobs				[int]		  = NULL,
		@maxRunningTimeInMinutes	[smallint]	  = 0,
		@debugMode					[bit]		  = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @projectID					[smallint]
		, @jobName						[sysname]
		, @jobID						[sysname]
		, @jobStepName					[sysname]
		, @jobDBName					[sysname]
		, @sqlServerName				[sysname]
		, @jobCommand					[nvarchar](max)
		, @jobCreateTimeUTC				[datetime]
		, @defaultLogFileLocation		[nvarchar](512)
		, @logFileLocation				[nvarchar](512)
		, @jobQueueID					[int]
		, @configParallelJobs			[smallint]
		, @configMaxNumberOfRetries		[smallint]
		, @configFailMasterJob			[bit]
		, @configMaxSQLJobsOneMin		[smallint]
		, @configMaxSQLJobsRunning		[smallint]
		, @configMaxQueueExecutionTime	[smallint]
		, @runningJobs					[smallint]
		, @executedJobs					[int]
		, @jobQueueCount				[int]
			
		, @strMessage					[varchar](8000)	
		, @currentRunning				[int]
		, @lastExecutionStatus			[int]
		, @lastExecutionDate			[varchar](10)
		, @lastExecutionTime 			[varchar](8)
		, @runningTimeSec				[bigint]
		, @jobCurrentRunning			[bit]
		, @retryAttempts				[tinyint]
		, @serialExecutionMode			[bit]
		, @serialExecutionUsingJobs		[bit]

DECLARE   @ErrorNumber [int]
		, @ErrorLine [int]
		, @ErrorMessage [nvarchar](4000)
		, @ErrorSeverity [int]
		, @ErrorState [int]

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @serverEngine				[int]
		, @nestedExecutionLevel		[tinyint]

DECLARE	  @queryToRun  				[nvarchar](2048)
		, @queryParameters			[nvarchar](512)
		, @eventData				[varchar](8000)
		, @stopTimeLimit			[datetime]
			
/* for the jobs per volume limit */
DECLARE	  @moduleHealthCheckExists				[bit]
		, @forDatabaseName						[sysname]
		, @forDatabaseMountPoint				[nvarchar](512)
		, @jobsRunningOnSameVolume				[smallint]
		, @configMaxSQLJobsRunningOnSameVolume	[smallint]

---------------------------------------------------------------------------------------------
--determine when to stop current optimization task, based on @maxRunningTimeInMinutes value
---------------------------------------------------------------------------------------------
IF ISNULL(@maxRunningTimeInMinutes, 0)=0
	SET @stopTimeLimit = CONVERT([datetime], '9999-12-31 23:23:59', 120)
ELSE
	SET @stopTimeLimit = DATEADD(minute, @maxRunningTimeInMinutes, GETDATE())

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectID
IF @projectCode IS NOT NULL
	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

------------------------------------------------------------------------------------------------------------------------------------------
--check if parallel executor is enabled
IF @parallelJobs IS NULL
	begin
		BEGIN TRY
			SELECT	@configParallelJobs = [value]
			FROM	[dbo].[appConfigurations]
			WHERE	[name] = N'Parallel Execution Jobs'
					AND [module] = 'common'
		END TRY
		BEGIN CATCH
			SET @configParallelJobs = 1
		END CATCH

		SET @configParallelJobs = ISNULL(@configParallelJobs, 1)
	end
ELSE
	SET @configParallelJobs = @parallelJobs

SET @serialExecutionMode = CASE WHEN @configParallelJobs=1 THEN 1 ELSE 0 END

------------------------------------------------------------------------------------------------------------------------------------------
--check if SQL Agent jobs should be used when running in serial mode
IF @serialExecutionMode = 1
	begin
		BEGIN TRY
			SELECT	@serialExecutionUsingJobs = [value]
			FROM	[dbo].[appConfigurations]
			WHERE	[name] = N'In "serial" mode (parallel=1), execute tasks using SQL Agent jobs'
					AND [module] = 'common'
		END TRY
		BEGIN CATCH
			SET @serialExecutionUsingJobs = 1
		END CATCH

		SET @serialExecutionUsingJobs = ISNULL(@serialExecutionUsingJobs, 1)		
	end
	

------------------------------------------------------------------------------------------------------------------------------------------
--check if SQL Agent service is started
IF @serialExecutionMode = 0
	begin
		IF NOT EXISTS(
						SELECT TOP(1) 1 AS [status] 
						FROM master.sys.dm_exec_sessions 
						WHERE[program_name] in ('SQLAgent - Job invocation engine', 'SQLAgent - Generic Refresher')
					 )
		begin
			SET @strMessage='ERROR: SQL Server Agent service is not started. Cannot continue the execution using parallel mode. Please start SQL Agent service.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
			
			RETURN
		end
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get the number of retries in case of a failure
BEGIN TRY
	SELECT	@configMaxNumberOfRetries = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Maximum number of retries at failed job'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configMaxNumberOfRetries = 3
END CATCH

SET @configMaxNumberOfRetries = ISNULL(@configMaxNumberOfRetries, 3)

------------------------------------------------------------------------------------------------------------------------------------------
--get the maximum number of SQL Agent jobs that can be started in one minute 
/*https://blogs.msdn.microsoft.com/sqlserverfaq/2012/03/14/inf-limitations-for-sql-agent-when-you-have-many-jobs-running-in-sql-simultaneously/ */
------------------------------------------------------------------------------------------------------------------------------------------
BEGIN TRY
	SELECT	@configMaxSQLJobsOneMin = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Maximum SQL Agent jobs started per minute (KB306457)'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configMaxSQLJobsOneMin = 60
END CATCH

SET @configMaxSQLJobsOneMin = ISNULL(@configMaxSQLJobsOneMin, 60)

------------------------------------------------------------------------------------------------------------------------------------------
--get the maximum numbers of jobs that can be running, across all projects/tasks
BEGIN TRY
	SELECT	@configMaxSQLJobsRunning = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Maximum SQL Agent jobs running (0=unlimited)'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configMaxSQLJobsRunning = 0
END CATCH

SET @configMaxSQLJobsRunning = ISNULL(@configMaxSQLJobsRunning, 0)

------------------------------------------------------------------------------------------------------------------------------------------
--get the maximum numbers of jobs that can be running on the same physical volume
BEGIN TRY
	SELECT	@configMaxSQLJobsRunningOnSameVolume = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Maximum SQL Agent jobs running on the same physical volume (0=unlimited)'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configMaxSQLJobsRunningOnSameVolume = 0
END CATCH

SET @configMaxSQLJobsRunningOnSameVolume = ISNULL(@configMaxSQLJobsRunningOnSameVolume, 0)

------------------------------------------------------------------------------------------------------------------------------------------
--get the time limit for which the queue can execute (hours)
BEGIN TRY
	SELECT	@configMaxQueueExecutionTime = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Maximum job queue execution time (hours) (0=unlimited)'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configMaxQueueExecutionTime = 0
END CATCH

SET @configMaxQueueExecutionTime = ISNULL(@configMaxQueueExecutionTime, 0)

------------------------------------------------------------------------------------------------------------------------------------------
--get the number of retries in case of a failure
BEGIN TRY
	SELECT	@configFailMasterJob = CASE WHEN lower([value])='true' THEN 1 ELSE 0 END
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Fail master job if any queued job fails'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configFailMasterJob = 0
END CATCH

SET @configFailMasterJob = ISNULL(@configFailMasterJob, 0)

------------------------------------------------------------------------------------------------------------------------------------------
--get default folder for SQL Agent jobs
BEGIN TRY
	SELECT	@defaultLogFileLocation = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Default folder for logs'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @defaultLogFileLocation = NULL
END CATCH

IF @defaultLogFileLocation IS NULL
		SELECT @defaultLogFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
		FROM (
				SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
			)er

SET @defaultLogFileLocation = ISNULL(@defaultLogFileLocation, N'C:\')
IF RIGHT(@defaultLogFileLocation, 1)<>'\' SET @defaultLogFileLocation = @defaultLogFileLocation + '\'

SET @defaultLogFileLocation = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, @defaultLogFileLocation)

------------------------------------------------------------------------------------------------------------------------------------------
--for limitations that are defined cross-modules, check if module health-check is installed
SET @moduleHealthCheckExists = 0
IF EXISTS(SELECT * FROM sys.schemas WHERE [name] = 'health-check')
	AND EXISTS(SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='vw_statsDatabaseDetails')
	AND EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='health-check' AND TABLE_NAME='statsDatabaseDetails')
	SET @moduleHealthCheckExists = 1

------------------------------------------------------------------------------------------------------------------------------------------
--create folder on disk
SET @queryToRun = N'EXEC ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + '.[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @@SERVERNAME + N''',
																			@folderName		= ''' + @defaultLogFileLocation + N''',
																			@executionLevel	= 1,
																			@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

EXEC  [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
									@dbName			= NULL,
									@module			= 'dbo.usp_jobQueueExecute',
									@eventName		= 'create folder on disk',
									@queryToRun  	= @queryToRun,
									@flgOptions		= 32,
									@executionLevel	= 1,
									@debugMode		= @debugMode


------------------------------------------------------------------------------------------------------------------------------------------
SELECT @jobQueueCount = COUNT(*)
FROM [dbo].[vw_jobExecutionQueue]
WHERE  ([project_id] = @projectID OR @projectID IS NULL)
		AND [module] LIKE @moduleFilter
		AND (    [descriptor] LIKE @descriptorFilter
			  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
			)			
		AND [status]=-1
		AND (    DATEDIFF(minute, [event_date_utc], GETUTCDATE()) < (@configMaxQueueExecutionTime * 60)
				OR @configMaxQueueExecutionTime = 0
			)																

SET @strMessage='Number of jobs in the queue to be executed : ' + CAST(@jobQueueCount AS [varchar]) 
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

IF @jobQueueCount=0
	RETURN;

SET @runningJobs  = 0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsJobQueue CURSOR LOCAL FAST_FORWARD FOR	SELECT    [id], [project_id], [instance_name]
															, [job_name], [job_step_name], [job_database_name]
															, [job_command]
															, [database_name], [event_date_utc]
													FROM [dbo].[vw_jobExecutionQueue]
													WHERE  ([project_id] = @projectID OR @projectID IS NULL)
															AND [module] LIKE @moduleFilter
															AND (    [descriptor] LIKE @descriptorFilter
																  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																)			
															AND [status] = - 1
															AND (    DATEDIFF(minute, [event_date_utc], GETUTCDATE()) < (@configMaxQueueExecutionTime * 60)
																  OR @configMaxQueueExecutionTime = 0
																)																													
													ORDER BY [priority], [id]
OPEN crsJobQueue
FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @projectID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand, @forDatabaseName, @jobCreateTimeUTC
SET @executedJobs = 1
WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
	begin
		/* check if job should be "skipped" */
		IF NOT (DATEDIFF(minute, @jobCreateTimeUTC, GETUTCDATE()) < (@configMaxQueueExecutionTime * 60) OR @configMaxQueueExecutionTime = 0)
			begin
				FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @projectID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand, @forDatabaseName, @jobCreateTimeUTC				
			end
		/* execute the job */
		ELSE
			begin
				------------------------------------------------------------------------------------------------------------------------------------------
				--get destination server running version/edition
				EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
														@serverEdition		= @serverEdition OUT,
														@serverVersionStr	= @serverVersionStr OUT,
														@serverVersionNum	= @serverVersionNum OUT,
														@serverEngine		= @serverEngine OUT,
														@executionLevel		= 0,
														@debugMode			= @debugMode


				SET @strMessage='Executing job# : ' + CAST(@executedJobs AS [varchar]) + ' / ' + CAST(@jobQueueCount AS [varchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				IF @serialExecutionMode = 1 AND @serialExecutionUsingJobs = 0
					begin
						/* for "serial" mode, will not execute tasks/jobs as SQL Agent jobs but as an inline command */
						DECLARE   @startTime	[datetime]
								, @endTime		[datetime]
				
						/* mark job as running */
						SET @startTime = GETDATE()
						UPDATE [dbo].[jobExecutionQueue] 
							SET   [status] = 4
								, [execution_date] = @startTime
						WHERE [id] = @jobQueueID	
				
						-- start the job
						BEGIN TRY
							SET @strMessage='Executing command : ' + @jobCommand
							
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0
							EXEC sp_executesql @jobCommand
							SET @endTime = GETDATE();
							SET @runningTimeSec = DATEDIFF(second, @startTime, @endTime) 
				
							UPDATE [dbo].[jobExecutionQueue]
								SET [status] = 1,
									[running_time_sec] = @runningTimeSec
							WHERE [id] = @jobQueueID

							SET @executedJobs = @executedJobs + 1

							FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @projectID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand, @forDatabaseName, @jobCreateTimeUTC
						END TRY
						BEGIN CATCH		
								SET @ErrorNumber = ERROR_NUMBER();
								SET @ErrorLine = ERROR_LINE();
								SET @ErrorMessage = ERROR_MESSAGE();
								SET @ErrorSeverity = ERROR_SEVERITY();
								SET @ErrorState = ERROR_STATE();	
					
								SET @endTime = GETDATE();
								SET @runningTimeSec = DATEDIFF(second, @startTime, @endTime)
																				
								PRINT 'Actual error number: ' + CAST(@ErrorNumber AS VARCHAR(10));
								PRINT 'Actual line number: ' + CAST(@ErrorLine AS VARCHAR(10));
								SET @strMessage=N'Error executing command ' + @jobCommand + '. Error: ' + @ErrorMessage + '. Line number: ' + CAST(@ErrorLine AS VARCHAR(10));

								UPDATE [dbo].[jobExecutionQueue]
									SET [status] = 0,
										[running_time_sec] = @runningTimeSec,
										[log_message] = @strMessage
								WHERE [id] = @jobQueueID

								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
						END CATCH
					end
				ELSE
					begin
						SET @strMessage='Create SQL Agent job : "' + @jobName + '"'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

						---------------------------------------------------------------------------------------------------
						/* setting the job name & job log location */	
						SET @logFileLocation = @defaultLogFileLocation + [dbo].[ufn_getObjectQuoteName](N'job-' + @jobName + N'.log', 'filename')

						---------------------------------------------------------------------------------------------------
						/* check if job is running and stop it */
						SET @jobCurrentRunning = 0
						SET @jobID = NULL
						EXEC  dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
																@jobName				= @jobName,
																@jobID					= @jobID OUTPUT,
																@strMessage				= DEFAULT,	
																@currentRunning			= @jobCurrentRunning OUTPUT,			
																@lastExecutionStatus	= DEFAULT,			
																@lastExecutionDate		= DEFAULT,		
																@lastExecutionTime 		= DEFAULT,	
																@runningTimeSec			= DEFAULT,
																@selectResult			= DEFAULT,
																@extentedStepDetails	= DEFAULT,		
																@debugMode				= DEFAULT

						IF @jobCurrentRunning=1
							begin
								/* wait / retry mechanism for high active systems */
								SET @queryToRun = 'Job is still running. Waiting ' + @waitForDelay + ' before stopping it...'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								WAITFOR DELAY @waitForDelay

								SET @jobCurrentRunning = 0
								EXEC  dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
																		@jobName				= @jobName,
																		@jobID					= @jobID,
																		@strMessage				= DEFAULT,	
																		@currentRunning			= @jobCurrentRunning OUTPUT,			
																		@lastExecutionStatus	= DEFAULT,			
																		@lastExecutionDate		= DEFAULT,		
																		@lastExecutionTime 		= DEFAULT,	
																		@runningTimeSec			= DEFAULT,
																		@selectResult			= DEFAULT,
																		@extentedStepDetails	= DEFAULT,		
																		@debugMode				= DEFAULT
								IF @jobCurrentRunning=1
									begin
										SET @queryToRun = 'Job is still running. Stopping...'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

										EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
																		@jobName		= @jobName,
																		@jobID			= @jobID,
																		@operation		= 'Stop',
																		@dbName			= @jobDBName, 
																		@jobStepName 	= @jobStepName,
																		@debugMode		= @debugMode

										SET @retryAttempts = 1
										WHILE @jobCurrentRunning = 1 AND @retryAttempts <= @configMaxNumberOfRetries
											begin
												WAITFOR DELAY @waitForDelay

												SET @jobCurrentRunning = 0
												EXEC  dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
																						@jobName				= @jobName,
																						@jobID					= @jobID,
																						@strMessage				= DEFAULT,	
																						@currentRunning			= @jobCurrentRunning OUTPUT,			
																						@lastExecutionStatus	= DEFAULT,			
																						@lastExecutionDate		= DEFAULT,		
																						@lastExecutionTime 		= DEFAULT,	
																						@runningTimeSec			= DEFAULT,
																						@selectResult			= DEFAULT,
																						@extentedStepDetails	= DEFAULT,		
																						@debugMode				= DEFAULT
						
												SET @retryAttempts = @retryAttempts + 1
										end
									end
							end

						---------------------------------------------------------------------------------------------------
						/* defining job and start it */
						EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
														@jobName		= @jobName,
														@jobID			= @jobID,
														@operation		= 'Clean',
														@dbName			= @jobDBName, 
														@jobStepName 	= @jobStepName,
														@debugMode		= @debugMode

						SET @jobID = NULL
						SET @jobCommand = REPLACE(@jobCommand, '''', '''''')
						EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
														@jobName		= @jobName,
														@jobID			= @jobID OUTPUT,
														@operation		= 'Add',
														@dbName			= @jobDBName, 
														@jobStepName 	= @jobStepName,
														@jobStepCommand	= @jobCommand,
														@jobLogFileName	= @logFileLocation,
														@jobStepRetries = @configMaxNumberOfRetries,
														@debugMode		= @debugMode

						---------------------------------------------------------------------------------------------------
						/* https://blogs.msdn.microsoft.com/sqlserverfaq/2012/03/14/inf-limitations-for-sql-agent-when-you-have-many-jobs-running-in-sql-simultaneously/ */
						/* A design limitation imposes a one second delay between jobs. Because of this limitation, up to 60 jobs can be started in the same one-minute interval. */
		
						--jobs started within the last minute
						IF (SELECT COUNT(*) 
							FROM [dbo].[jobExecutionQueue]
							WHERE CONVERT([varchar](16), [execution_date], 120) = CONVERT([varchar](16), GETDATE(), 120)
							) >= @configMaxSQLJobsOneMin
							begin
								SET @strMessage='KB 306457 limitation: add one second delay between starting jobs.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								WAITFOR DELAY '00:00:01'
							end

						---------------------------------------------------------------------------------------------------
						/* check how many jobs are running. if cap is set, waiting for jobs to complete */
						IF (@serialExecutionMode = 0 AND @serverVersionNum > 10) AND (@configMaxSQLJobsRunning > 0 OR @configMaxSQLJobsRunningOnSameVolume > 0)
							begin
								/* get current job database mount point */
								SET @forDatabaseMountPoint = NULL
								IF @moduleHealthCheckExists = 1 AND @forDatabaseName IS NOT NULL AND @configMaxSQLJobsRunningOnSameVolume > 0
									begin
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'SELECT @forDatabaseMountPoint = [volume_mount_point]
																			FROM [health-check].[vw_statsDatabaseDetails]
																			WHERE [instance_name] = @sqlServerName
																					AND [database_name] = @forDatabaseName
																					AND [project_id] = @projectID' 
										SET @queryParameters = '@sqlServerName [sysname], @forDatabaseName [sysname], @projectID [smallint], @forDatabaseMountPoint [nvarchar](512) OUTPUT'
										IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
										EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName 
																						, @forDatabaseName = @forDatabaseName
																						, @projectID = @projectID
																						, @forDatabaseMountPoint = @forDatabaseMountPoint OUT
									end

								/* prepare the SQL code */
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'	SELECT	j.[name] 
																	FROM  [msdb].[dbo].[sysjobactivity] ja WITH (NOLOCK)
																	LEFT  JOIN [msdb].[dbo].[sysjobhistory] jh WITH (NOLOCK) ON ja.[job_history_id] = jh.[instance_id]
																	INNER JOIN [msdb].[dbo].[sysjobs] j WITH (NOLOCK) ON ja.[job_id] = j.[job_id]
																	INNER JOIN [msdb].[dbo].[sysjobsteps] js WITH (NOLOCK) ON ja.[job_id] = js.[job_id] AND ISNULL(ja.[last_executed_step_id], 0)+ 1  = js.[step_id]
																	WHERE	ja.[session_id] = (
																								SELECT TOP 1 [session_id] 
																								FROM [msdb].[dbo].[syssessions] WITH (NOLOCK)
																								ORDER BY [agent_start_date] DESC
																								)
																			AND ja.[start_execution_date] IS NOT NULL
																			AND ja.[stop_execution_date] IS NULL'
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								SET @queryToRun = N'SELECT    @currentRunningJobs = COUNT(*)
															, @jobsRunningOnSameVolume = SUM(CASE WHEN ' + 
																			CASE WHEN @moduleHealthCheckExists = 1 AND @forDatabaseMountPoint IS NOT NULL
																				THEN N'sdd.[catalog_database_id]'
																				ELSE N'NULL'
																			END + N' IS NOT NULL THEN ait.[is_resource_intensive] ELSE 0 END)
													FROM (' + @queryToRun + N') j
													INNER JOIN [dbo].[jobExecutionQueue] jeq WITH (NOLOCK) ON j.[name] = jeq.[job_name]
													INNER JOIN [dbo].[appInternalTasks] ait ON ait.[id] = jeq.[task_id]'
										
								/* get the jobs running on the same physical volume */
								IF @moduleHealthCheckExists = 1 AND @forDatabaseMountPoint IS NOT NULL
									SET @queryToRun = @queryToRun + N'
													LEFT JOIN [health-check].[vw_statsDatabaseDetails] sdd WITH (NOLOCK) ON jeq.[project_id] = sdd.[project_id] 
																															AND jeq.[database_name] = sdd.[database_name] 
																															AND sdd.[instance_name] = @sqlServerName
																															AND (   CHARINDEX(sdd.[volume_mount_point], @forDatabaseMountPoint) > 0
																																	OR CHARINDEX(@forDatabaseMountPoint, sdd.[volume_mount_point]) > 0
																																)'
								SET @queryParameters = '@sqlServerName [sysname], @forDatabaseMountPoint [nvarchar](512), @currentRunningJobs [int] OUTPUT, @jobsRunningOnSameVolume [smallint] OUTPUT'

								/* Maximum SQL Agent jobs running limit reached */
								SET @runningJobs = @configMaxSQLJobsRunning
								SET @jobsRunningOnSameVolume = @configMaxSQLJobsRunningOnSameVolume
								WHILE (@runningJobs >= @configMaxSQLJobsRunning AND @configMaxSQLJobsRunning > 0)
									begin
										IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
										EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
																						, @forDatabaseMountPoint = @forDatabaseMountPoint
																						, @currentRunningJobs = @runningJobs OUT
																						, @jobsRunningOnSameVolume = @jobsRunningOnSameVolume OUT
										
										SET @runningJobs			 = ISNULL(@runningJobs, 0)
										SET @jobsRunningOnSameVolume = ISNULL(@jobsRunningOnSameVolume, 0)

										IF @runningJobs >= @configMaxSQLJobsRunning
											begin
												SET @strMessage='Maximum SQL Agent jobs running limit reached. Waiting for some job(s) to complete.'
												EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

												SET @eventData='<alert><detail>' + 
																	'<severity>warning</severity>' + 
																	'<instance_name>' + @sqlServerName + '</instance_name>' + 
																	'<name>' + @strMessage + '</name>' +
																	'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@jobName, 'xml') + '</affected_object>' + 
																	'<event_date_utc>' + CONVERT([varchar](24), GETDATE(), 121) + '</event_date_utc>' + 
																'</detail></alert>'

												EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																								@dbName					= @jobDBName,
																								@objectName				= 'warning',
																								@childObjectName		= 'dbo.usp_jobQueueExecute',
																								@module					= 'common',
																								@eventName				= 'job queue execute',
																								@parameters				= NULL,	
																								@eventMessage			= @eventData,
																								@dbMailProfileName		= NULL,
																								@recipientsList			= NULL,
																								@eventType				= 6,	/* 6 - alert-custom */
																								@additionalOption		= 0

												WAITFOR DELAY @waitForDelay
											end
									end

								/* Maximum SQL Agent jobs running on the same physical volume limit reached */
								WHILE (@jobsRunningOnSameVolume >= @configMaxSQLJobsRunningOnSameVolume AND @configMaxSQLJobsRunningOnSameVolume > 0)
									begin
										SET @strMessage='Maximum SQL Agent jobs running on the same physical volume limit reached. Waiting for some job(s) to complete.'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

										SET @eventData='<alert><detail>' + 
															'<severity>warning</severity>' + 
															'<instance_name>' + @sqlServerName + '</instance_name>' + 
															'<name>' + @strMessage + '</name>' +
															'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@jobName, 'xml') + '</affected_object>' + 
															'<event_date_utc>' + CONVERT([varchar](24), GETDATE(), 121) + '</event_date_utc>' + 
														'</detail></alert>'

										EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																						@dbName					= @jobDBName,
																						@objectName				= 'warning',
																						@childObjectName		= 'dbo.usp_jobQueueExecute',
																						@module					= 'common',
																						@eventName				= 'job queue execute',
																						@parameters				= NULL,	
																						@eventMessage			= @eventData,
																						@dbMailProfileName		= NULL,
																						@recipientsList			= NULL,
																						@eventType				= 6,	/* 6 - alert-custom */
																						@additionalOption		= 0

										WAITFOR DELAY @waitForDelay

										IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
										EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
																						, @forDatabaseMountPoint = @forDatabaseMountPoint
																						, @currentRunningJobs = @runningJobs OUT
																						, @jobsRunningOnSameVolume = @jobsRunningOnSameVolume OUT
										
										SET @jobsRunningOnSameVolume = ISNULL(@jobsRunningOnSameVolume, 0)
									end
							end

						---------------------------------------------------------------------------------------------------						
						/* starting job: 0 = job started, 1 = error occured */
						EXEC @lastExecutionStatus = dbo.usp_sqlAgentJobStartAndWatch	@sqlServerName						= @sqlServerName,
																						@jobName							= @jobName,
																						@jobID								= @jobID,
																						@stepToStart						= 1,
																						@stepToStop							= 1,
																						@waitForDelay						= @waitForDelay,
																						@dontRunIfLastExecutionSuccededLast	= 0,
																						@startJobIfPrevisiousErrorOcured	= 1,
																						@watchJob							= @serialExecutionMode,
																						@jobQueueID							= @jobQueueID,
																						@debugMode							= @debugMode
						SET @runningJobs = @executedJobs
						BEGIN TRY
							EXEC @runningJobs = dbo.usp_jobQueueGetStatus	@projectCode			= @projectCode,
																			@moduleFilter			= @moduleFilter,
																			@descriptorFilter		= @descriptorFilter,
																			@waitForDelay			= @waitForDelay,
																			@minJobToRunBeforeExit	= @configParallelJobs,
																			@executionLevel			= 1,
																			@debugMode				= @debugMode
						END TRY
						BEGIN CATCH
								SET @ErrorNumber = ERROR_NUMBER();
								SET @ErrorLine = ERROR_LINE();
								SET @ErrorMessage = ERROR_MESSAGE();
								SET @ErrorSeverity = ERROR_SEVERITY();
								SET @ErrorState = ERROR_STATE();
 
								SET @strMessage='Actual error number: ' + CAST(@ErrorNumber AS VARCHAR(10))
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
				
								SET @strMessage='Actual line number: ' + CAST(@ErrorLine AS VARCHAR(10));
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
						END CATCH
		
						---------------------------------------------------------------------------------------------------
						IF @runningJobs < @jobQueueCount
							begin
								FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @projectID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand, @forDatabaseName, @jobCreateTimeUTC
								SET @executedJobs = @executedJobs + 1
							end
						ELSE
							BREAK
					end
			end
	end
CLOSE crsJobQueue
DEALLOCATE crsJobQueue

/* in parallel execution, wait for all jobs to complete the execution, but no more then 30 minutes */
IF @serialExecutionMode = 0
	begin
		SET @stopTimeLimit = DATEADD(minute, 30, GETDATE())
		SET @runningJobs = 1

		WHILE @runningJobs > 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				WAITFOR DELAY @waitForDelay

				EXEC @runningJobs = dbo.usp_jobQueueGetStatus	@projectCode			= @projectCode,
																@moduleFilter			= @moduleFilter,
																@descriptorFilter		= @descriptorFilter,
																@waitForDelay			= @waitForDelay,
																@minJobToRunBeforeExit	= 0,
																@executionLevel			= 1,
																@debugMode				= @debugMode
			end

		IF @configFailMasterJob=1 
			AND EXISTS(	SELECT *
					FROM [dbo].[vw_jobExecutionQueue]
					WHERE  [project_id] = @projectID 
							AND [module] LIKE @moduleFilter
							AND (    [descriptor] LIKE @descriptorFilter
								  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
								)			
							AND [status]=0 /* failed */
					)
			begin
				SET @strMessage=N'Execution failed. Check log for internal job failures (dbo.vw_jobExecutionQueue).'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
			end
	end

/* save the executions statistics */
EXEC [dbo].[usp_jobExecutionSaveStatistics]	@projectCode		= @projectCode,
											@moduleFilter		= @moduleFilter,
											@descriptorFilter	= @descriptorFilter
GO
