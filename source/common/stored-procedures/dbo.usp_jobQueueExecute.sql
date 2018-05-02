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
		@projectCode			[varchar](32) = NULL,
		@moduleFilter			[varchar](32) = '%',
		@descriptorFilter		[varchar](256)= '%',
		@waitForDelay			[varchar](8) = '00:00:01',
		@parallelJobs			[int] = NULL,
		@debugMode				[bit] = 0
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

DECLARE   @projectID				[smallint]
		, @jobName					[sysname]
		, @jobStepName				[sysname]
		, @jobDBName				[sysname]
		, @sqlServerName			[sysname]
		, @jobCommand				[nvarchar](max)
		, @defaultLogFileLocation	[nvarchar](512)
		, @logFileLocation			[nvarchar](512)
		, @jobQueueID				[int]

		, @configParallelJobs		[smallint]
		, @configMaxNumberOfRetries	[smallint]
		, @configFailMasterJob		[bit]
		, @configMaxSQLJobsOneMin	[smallint]
		, @configMaxSQLJobsRunning	[smallint]
		, @runningJobs				[smallint]
		, @executedJobs				[smallint]
		, @jobQueueCount			[smallint]

		, @strMessage				[varchar](8000)	
		, @currentRunning			[int]
		, @lastExecutionStatus		[int]
		, @lastExecutionDate		[varchar](10)
		, @lastExecutionTime 		[varchar](8)
		, @runningTimeSec			[bigint]
		, @jobCurrentRunning		[bit]
		, @retryAttempts			[tinyint]
		, @serialExecutionMode		[bit]
		, @serialExecutionUsingJobs	[bit]

DECLARE   @ErrorNumber [int]
		, @ErrorLine [int]
		, @ErrorMessage [nvarchar](4000)
		, @ErrorSeverity [int]
		, @ErrorState [int]

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @nestedExecutionLevel		[tinyint]

DECLARE	  @queryToRun  			[nvarchar](2048)
		, @queryParameters		[nvarchar](512)

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end
	
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
WHERE  [project_id] = @projectID 
		AND [module] LIKE @moduleFilter
		AND (    [descriptor] LIKE @descriptorFilter
			  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
			)			
		AND [status]=-1


SET @strMessage='Number of jobs in the queue to be executed : ' + CAST(@jobQueueCount AS [varchar]) 
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

IF @jobQueueCount=0
	RETURN;

SET @runningJobs  = 0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsJobQueue CURSOR LOCAL FAST_FORWARD FOR	SELECT  [id], [instance_name]
															, [job_name], [job_step_name], [job_database_name], REPLACE([job_command], '''', '''''') AS [job_command]
													FROM [dbo].[vw_jobExecutionQueue]
													WHERE  [project_id] = @projectID 
															AND [module] LIKE @moduleFilter
															AND (    [descriptor] LIKE @descriptorFilter
																  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																)			
															AND [status]=-1
													ORDER BY [id]
OPEN crsJobQueue
FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand
SET @executedJobs = 1
WHILE @@FETCH_STATUS=0
	begin
		------------------------------------------------------------------------------------------------------------------------------------------
		--get destination server running version/edition
		EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
												@serverEdition		= @serverEdition OUT,
												@serverVersionStr	= @serverVersionStr OUT,
												@serverVersionNum	= @serverVersionNum OUT,
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
					SET @jobCommand = REPLACE(@jobCommand, '''''', '''')	
					SET @jobCommand = REPLACE(@jobCommand, 'EXEC ', '')	
								
					EXEC (@jobCommand)
					SET @endTime = GETDATE();
					SET @runningTimeSec = DATEDIFF(second, @startTime, @endTime) 
				
					UPDATE [dbo].[jobExecutionQueue]
						SET [status] = 1,
							[running_time_sec] = @runningTimeSec
					WHERE [id] = @jobQueueID

					SET @executedJobs = @executedJobs + 1

					FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand
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
				EXEC  dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
														@jobName				= @jobName,
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
												@operation		= 'Clean',
												@dbName			= @jobDBName, 
												@jobStepName 	= @jobStepName,
												@debugMode		= @debugMode

				EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
												@jobName		= @jobName,
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
				IF @configMaxSQLJobsRunning > 0 AND @serialExecutionMode = 0 AND @serverVersionNum > 10 
					begin
						SET @runningJobs = @configMaxSQLJobsRunning
						WHILE @runningJobs >= @configMaxSQLJobsRunning
							begin
								SET @strMessage='Checking Maximum SQL Agent jobs running ...'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'	SELECT	j.[name] 
																	FROM  [msdb].[dbo].[sysjobactivity] ja WITH (NOLOCK)
																	LEFT  JOIN [msdb].[dbo].[sysjobhistory] jh WITH (NOLOCK) ON ja.[job_history_id] = jh.[instance_id]
																	INNER JOIN [msdb].[dbo].[sysjobs] j WITH (NOLOCK) ON ja.[job_id] = j.[job_id]
																	INNER JOIN [msdb].[dbo].[sysjobsteps] js WITH (NOLOCK) ON ja.[job_id] = js.[job_id] AND ISNULL(ja.[last_executed_step_id], 0)+ 1  = js.[step_id]
																	WHERE	ja.[session_id] = (
																								SELECT TOP 1 [session_id] 
																								FROM [msdb].[dbo].[syssessions] 
																								ORDER BY [agent_start_date] DESC
																								)
																			AND ja.[start_execution_date] IS NOT NULL
																			AND ja.[stop_execution_date] IS NULL'
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								SET @queryToRun = N'SELECT @currentRunningJobs = COUNT(*)
													FROM (' + @queryToRun + N') j
													INNER JOIN [dbo].[jobExecutionQueue] jeq WITH (NOLOCK) ON j.[name] = jeq.[job_name]'
								SET @queryParameters = '@currentRunningJobs [int] OUTPUT'
								IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
								EXEC sp_executesql @queryToRun, @queryParameters, @currentRunningJobs = @runningJobs OUT

								IF @runningJobs >= @configMaxSQLJobsRunning
									WAITFOR DELAY @waitForDelay
							end
					end

				---------------------------------------------------------------------------------------------------
				/* starting job: 0 = job started, 1 = error occured */
				EXEC @lastExecutionStatus = dbo.usp_sqlAgentJobStartAndWatch	@sqlServerName						= @sqlServerName,
																				@jobName							= @jobName,
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
						FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand
						SET @executedJobs = @executedJobs + 1
					end
			end
	end
CLOSE crsJobQueue
DEALLOCATE crsJobQueue

/* in parallel execution, wait for all jobs to complete the execution*/
IF @serialExecutionMode = 0
	begin
		WAITFOR DELAY @waitForDelay
		
		EXEC dbo.usp_jobQueueGetStatus	@projectCode			= @projectCode,
										@moduleFilter			= @moduleFilter,
										@descriptorFilter		= @descriptorFilter,
										@waitForDelay			= @waitForDelay,
										@minJobToRunBeforeExit	= 0,
										@executionLevel			= 1,
										@debugMode				= @debugMode

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
