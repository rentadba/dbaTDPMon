
USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.6 to 2017.8 (2017.08.07)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.08.07' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMONS', 10, 1) WITH NOWAIT

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Maximum SQL Agent jobs started per minute (KB306457)' and [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common' AS [module], 'Maximum SQL Agent jobs started per minute (KB306457)' AS [name], '60' AS [value]
GO


RAISERROR('Create procedure: [dbo].[usp_jobQueueGetStatus]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_jobQueueGetStatus]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_jobQueueGetStatus]
GO

CREATE PROCEDURE dbo.usp_jobQueueGetStatus
		@projectCode			[varchar](32) = NULL,
		@moduleFilter			[varchar](32) = '%',
		@descriptorFilter		[varchar](256)= '%',
		@waitForDelay			[varchar](8) = '00:00:30',
		@minJobToRunBeforeExit	[smallint] = 0,
		@executionLevel			[tinyint] = 0,
		@debugMode				[bit]=0
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
		, @sqlServerName			[sysname]
		, @jobDBName				[sysname]
		, @jobQueueID				[int]
		, @runningJobs				[smallint]

		, @strMessage				[varchar](8000)	
		, @currentRunning			[int]
		, @lastExecutionStatus		[int]
		, @lastExecutionDate		[varchar](10)
		, @lastExecutionTime 		[varchar](8)
		, @runningTimeSec			[bigint]
		, @queryToRun				[nvarchar](max)


------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end
	
------------------------------------------------------------------------------------------------------------------------------------------
SELECT @runningJobs = COUNT(*)
FROM [dbo].[vw_jobExecutionQueue]
WHERE  [project_id] = @projectID 
		AND [module] LIKE @moduleFilter
		AND (    [descriptor] LIKE @descriptorFilter
			  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
			)			
		AND [status]=4

WHILE (@runningJobs >= @minJobToRunBeforeExit AND @minJobToRunBeforeExit <> 0) OR (@runningJobs > @minJobToRunBeforeExit AND @minJobToRunBeforeExit = 0)
	begin
		---------------------------------------------------------------------------------------------------
		/* check running job status and make updates */
		SET @runningJobs = 0

		DECLARE crsRunningJobs CURSOR LOCAL FAST_FORWARD FOR	SELECT  [id], [instance_name], [job_name]
																FROM [dbo].[vw_jobExecutionQueue]
																WHERE  [project_id] = @projectID 
																		AND [module] LIKE @moduleFilter
																		AND (    [descriptor] LIKE @descriptorFilter
																			  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																			)			
																		AND [status]=4
																ORDER BY [id]
		OPEN crsRunningJobs
		FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage			= NULL
				SET @currentRunning		= NULL
				SET @lastExecutionStatus= NULL
				SET @lastExecutionDate	= NULL
				SET @lastExecutionTime 	= NULL
				SET @runningTimeSec		= NULL

				EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
													@jobName				= @jobName,
													@strMessage				= @strMessage OUTPUT,
													@currentRunning			= @currentRunning OUTPUT,
													@lastExecutionStatus	= @lastExecutionStatus OUTPUT,
													@lastExecutionDate		= @lastExecutionDate OUTPUT,
													@lastExecutionTime 		= @lastExecutionTime OUTPUT,
													@runningTimeSec			= @runningTimeSec OUTPUT,
													@selectResult			= 0,
													@extentedStepDetails	= 0,		
													@debugMode				= @debugMode

				IF @currentRunning = 0 AND @lastExecutionStatus<>5 /* Unknown */
					begin
						--double check
						WAITFOR DELAY '00:00:01'						
						EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
															@jobName				= @jobName,
															@strMessage				= @strMessage OUTPUT,
															@currentRunning			= @currentRunning OUTPUT,
															@lastExecutionStatus	= @lastExecutionStatus OUTPUT,
															@lastExecutionDate		= @lastExecutionDate OUTPUT,
															@lastExecutionTime 		= @lastExecutionTime OUTPUT,
															@runningTimeSec			= @runningTimeSec OUTPUT,
															@selectResult			= 0,
															@extentedStepDetails	= 0,		
															@debugMode				= @debugMode
						IF @currentRunning = 0 AND @lastExecutionStatus<>5 /* Unknown */
							begin
								
								IF @lastExecutionStatus = 0 /* failed */
									SET @strMessage = CASE	WHEN CHARINDEX('--Job execution return this message: ', @strMessage) > 0
															THEN SUBSTRING(@strMessage, CHARINDEX('--Job execution return this message: ', @strMessage) + 37, LEN(@strMessage))
															ELSE @strMessage
													  END
								ELSE
									SET @strMessage=NULL

								UPDATE [dbo].[jobExecutionQueue]
									SET [status] = @lastExecutionStatus,
										[running_time_sec] = @runningTimeSec,
										[log_message] = @strMessage
								WHERE [id] = @jobQueueID

								/* removing job */
								IF @debugMode=1 
									begin
										SET @strMessage='--Debug info: @lastExecutionStatus = ' + CAST(@lastExecutionStatus AS varchar) + '; @currentRunning=' + CAST(@currentRunning AS varchar)
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
									end

								EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
																@jobName		= @jobName,
																@operation		= 'Clean',
																@dbName			= @jobDBName, 
																@jobStepName 	= '',
																@debugMode		= @debugMode
							end
						ELSE
							begin
								IF @currentRunning <> 0
									SET @runningJobs = @runningJobs + 1
							end
					end
				ELSE
					IF @currentRunning <> 0
						SET @runningJobs = @runningJobs + 1

				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
			end
		CLOSE crsRunningJobs
		DEALLOCATE crsRunningJobs

		SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
		IF @runningJobs > @minJobToRunBeforeExit
			WAITFOR DELAY @waitForDelay
	end

IF @minJobToRunBeforeExit=0
	begin
		SET @strMessage='Performing cleanup...'
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		SET @queryToRun = N''
		SET @queryToRun = 'SELECT [name] FROM [msdb].[dbo].[sysjobs]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		IF OBJECT_ID('tempdb..#existingSQLAgentJobs') IS NOT NULL DROP TABLE #existingSQLAgentJobs
		CREATE TABLE #existingSQLAgentJobs
			(
				[job_name] [sysname]
			)

		INSERT	INTO #existingSQLAgentJobs([job_name])
				EXEC (@queryToRun)

		SET @runningJobs = 0
		DECLARE crsRunningJobs CURSOR LOCAL FAST_FORWARD FOR	SELECT  jeq.[id], jeq.[instance_name], jeq.[job_name]
																FROM [dbo].[vw_jobExecutionQueue] jeq
																INNER JOIN #existingSQLAgentJobs esaj ON esaj.[job_name] = jeq.[job_name]
																WHERE  jeq.[project_id] = @projectID 
																		AND jeq.[module] LIKE @moduleFilter
																		AND (    [descriptor] LIKE @descriptorFilter
																			  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																			)			
																		AND jeq.[status]<>-1
																ORDER BY jeq.[id]
		OPEN crsRunningJobs
		FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage			= NULL
				SET @currentRunning		= NULL
				SET @lastExecutionStatus= NULL
				SET @lastExecutionDate	= NULL
				SET @lastExecutionTime 	= NULL
				SET @runningTimeSec		= NULL

				EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
													@jobName				= @jobName,
													@strMessage				= @strMessage OUTPUT,
													@currentRunning			= @currentRunning OUTPUT,
													@lastExecutionStatus	= @lastExecutionStatus OUTPUT,
													@lastExecutionDate		= @lastExecutionDate OUTPUT,
													@lastExecutionTime 		= @lastExecutionTime OUTPUT,
													@runningTimeSec			= @runningTimeSec OUTPUT,
													@selectResult			= 0,
													@extentedStepDetails	= 0,		
													@debugMode				= @debugMode

				IF @currentRunning = 0
					begin
						/* removing job */
						IF @debugMode=1 
							begin
								SET @strMessage='--Debug info: @lastExecutionStatus = ' + CAST(@lastExecutionStatus AS varchar) + '; @currentRunning=' + CAST(@currentRunning AS varchar)
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
							end
		

						EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
														@jobName		= @jobName,
														@operation		= 'Clean',
														@dbName			= @jobDBName, 
														@jobStepName 	= '',
														@debugMode		= @debugMode
					end
				ELSE
					SET @runningJobs = @runningJobs + 1

				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
			end
		CLOSE crsRunningJobs
		DEALLOCATE crsRunningJobs

		SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
	end

RETURN @runningJobs
GO


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
		@waitForDelay			[varchar](8) = '00:00:30',
		@debugMode				[bit]=0
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


------------------------------------------------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end
	
------------------------------------------------------------------------------------------------------------------------------------------
--check if parallel collector is enabled
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

------------------------------------------------------------------------------------------------------------------------------------------
--create folder on disk
DECLARE @queryToRun nvarchar(1024)
SET @queryToRun = N'EXEC [' + DB_NAME() + '].[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @@SERVERNAME + N''',
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
		SET @strMessage='Executing job# : ' + CAST(@executedJobs AS [varchar]) + ' / ' + CAST(@jobQueueCount AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		SET @strMessage='Create SQL Agent job : "' + @jobName + '"'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

		---------------------------------------------------------------------------------------------------
		/* setting the job name & job log location */	
		SET @logFileLocation = @defaultLogFileLocation + N'job-' + @jobName + N'.log'

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
				RAISERROR('--Job is still running. Stopping...', 10, 1) WITH NOWAIT
				SET @retryAttempts = 1
				WHILE @jobCurrentRunning = 1 AND @retryAttempts <= @configMaxNumberOfRetries
					begin
						EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
														@jobName		= @jobName,
														@operation		= 'Stop',
														@dbName			= @jobDBName, 
														@jobStepName 	= @jobStepName,
														@debugMode		= @debugMode
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
		/* starting job: 0 = job started, 1 = error occured */
		EXEC @lastExecutionStatus = dbo.usp_sqlAgentJobStartAndWatch	@sqlServerName						= @sqlServerName,
																		@jobName							= @jobName,
																		@stepToStart						= 1,
																		@stepToStop							= 1,
																		@waitForDelay						= @waitForDelay,
																		@dontRunIfLastExecutionSuccededLast	= 0,
																		@startJobIfPrevisiousErrorOcured	= 1,
																		@watchJob							= 0,
																		@debugMode							= @debugMode
		
		/* mark job as running */
		IF @lastExecutionStatus = 0
			UPDATE [dbo].[jobExecutionQueue] 
				SET   [status] = 4
					, [execution_date] = GETDATE()
			WHERE [id] = @jobQueueID	
										

		--SET @runningJobs = @runningJobs + 1

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
				DECLARE @ErrorNumber [int]
				DECLARE @ErrorLine [int]
				DECLARE @ErrorMessage [nvarchar](4000)
				DECLARE @ErrorSeverity [int]
				DECLARE @ErrorState [int]

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
CLOSE crsJobQueue
DEALLOCATE crsJobQueue

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
GO


/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT

