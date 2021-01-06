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
		@waitForDelay			[varchar](8) = '00:00:05',
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
		, @jobID					[sysname]
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
--get default projectID
IF @projectCode IS NOT NULL
	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

------------------------------------------------------------------------------------------------------------------------------------------
SELECT @runningJobs = COUNT(*)
FROM [dbo].[vw_jobExecutionQueue]
WHERE   ([project_id] = @projectID OR @projectID IS NULL)
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

		DECLARE crsRunningJobs CURSOR LOCAL FAST_FORWARD FOR	SELECT  [id], [instance_name], [job_name], [job_id]
																FROM [dbo].[vw_jobExecutionQueue]
																WHERE   ([project_id] = @projectID OR @projectID IS NULL)
																		AND [module] LIKE @moduleFilter
																		AND (    [descriptor] LIKE @descriptorFilter
																			  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																			)			
																		AND [status]=4
																ORDER BY [id]
		OPEN crsRunningJobs
		FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName, @jobID
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
													@jobID					= @jobID,
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
															@jobID					= @jobID,
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
										SET @strMessage='Debug info: @lastExecutionStatus = ' + CAST(@lastExecutionStatus AS varchar) + '; @currentRunning=' + CAST(@currentRunning AS varchar)
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
									end
								EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
																@jobName		= @jobName,
																@jobID			= @jobID,
																@operation		= 'Clean',
																@dbName			= @jobDBName, 
																@jobStepName 	= '',
																@debugMode		= @debugMode
							end
						ELSE
							begin
								--IF @currentRunning <> 0
									SET @runningJobs = @runningJobs + 1
							end
					end
				ELSE
					begin
						IF @currentRunning = 0 AND @lastExecutionStatus=5
							begin
								/* job is not running and last execution status could not be retrieved. Job may also not be defined on the server */								
								UPDATE [dbo].[jobExecutionQueue]
								SET [status] = 1, /* default = successful */
									[running_time_sec] = 0,
									[log_message] = @strMessage
								WHERE [id] = @jobQueueID
							end
						ELSE
							SET @runningJobs = @runningJobs + 1
					end

				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName, @jobID
			end
		CLOSE crsRunningJobs
		DEALLOCATE crsRunningJobs

		SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
		IF @runningJobs >= @minJobToRunBeforeExit
			WAITFOR DELAY @waitForDelay
	end

IF @minJobToRunBeforeExit=0
	begin
		SET @strMessage='Performing cleanup...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0

		DECLARE crsInstanceJobsCleansing CURSOR READ_ONLY FAST_FORWARD FOR	SELECT DISTINCT [instance_name]
																			FROM [dbo].[vw_jobExecutionQueue]
																			WHERE   ([project_id] = @projectID OR @projectID IS NULL)
																					AND [module] LIKE @moduleFilter
																					AND (    [descriptor] LIKE @descriptorFilter
																						  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																						)			
																					AND [status] IN (0, 1, 3) /*Failed, Succeded, Canceled*/
																			ORDER BY [instance_name]
		OPEN crsInstanceJobsCleansing
		FETCH NEXT FROM crsInstanceJobsCleansing INTO @sqlServerName
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = 'SELECT [name] FROM [msdb].[dbo].[sysjobs] WITH (NOLOCK)'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				IF OBJECT_ID('tempdb..#existingSQLAgentJobs') IS NOT NULL DROP TABLE #existingSQLAgentJobs
				CREATE TABLE #existingSQLAgentJobs
					(
						[job_name] [sysname]
					)

				INSERT	INTO #existingSQLAgentJobs([job_name])
						EXEC sp_executesql  @queryToRun

				SET @runningJobs = 0
				DECLARE crsRunningJobs CURSOR LOCAL FAST_FORWARD FOR	SELECT  jeq.[id], jeq.[instance_name], jeq.[job_name], jeq.[job_id]
																		FROM [dbo].[vw_jobExecutionQueue] jeq
																		INNER JOIN #existingSQLAgentJobs esaj ON esaj.[job_name] = jeq.[job_name]
																		WHERE  (jeq.[project_id] = @projectID OR @projectID IS NULL)
																				AND jeq.[module] LIKE @moduleFilter
																				AND (    [descriptor] LIKE @descriptorFilter
																					  OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
																					)			
																				AND jeq.[status]<>-1
																		ORDER BY jeq.[id]
				OPEN crsRunningJobs
				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName, @jobID
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
															@jobID					= @jobID,
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
								IF @lastExecutionStatus <> 5
									begin
										UPDATE [dbo].[jobExecutionQueue]
												SET [status] = @lastExecutionStatus,
													[running_time_sec] = @runningTimeSec,
													[log_message] = @strMessage
											WHERE [id] = @jobQueueID
									end

								/* removing job */
								IF @debugMode=1 
									begin
										SET @strMessage='Debug info: @lastExecutionStatus = ' + CAST(@lastExecutionStatus AS varchar) + '; @currentRunning=' + CAST(@currentRunning AS varchar)
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
									end
		
								EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
																@jobName		= @jobName,
																@jobID			= @jobID,
																@operation		= 'Clean',
																@dbName			= @jobDBName, 
																@jobStepName 	= '',
																@debugMode		= @debugMode
							end
						ELSE
							SET @runningJobs = @runningJobs + 1

						FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName, @jobID
					end
				CLOSE crsRunningJobs
				DEALLOCATE crsRunningJobs

				SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				
				FETCH NEXT FROM crsInstanceJobsCleansing INTO @sqlServerName
			end
		CLOSE crsInstanceJobsCleansing
		DEALLOCATE crsInstanceJobsCleansing
		
		/* if there are "skipped" jobs, update status to success */
		UPDATE [dbo].[jobExecutionQueue]
			SET [status] = 0
		WHERE [id] IN (
						SELECT  [id]
						FROM [dbo].[vw_jobExecutionQueue]
						WHERE   ([project_id] = @projectID OR @projectID IS NULL)
								AND [module] LIKE @moduleFilter
								AND (    [descriptor] LIKE @descriptorFilter
										OR ISNULL(CHARINDEX([descriptor], @descriptorFilter), 0) <> 0
									)			
								AND [status]=4 /* in progress */
					  )
	end
RETURN @runningJobs
GO
