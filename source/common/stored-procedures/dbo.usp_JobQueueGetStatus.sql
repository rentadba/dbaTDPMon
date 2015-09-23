RAISERROR('Create procedure: [dbo].[usp_JobQueueGetStatus]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_JobQueueGetStatus]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_JobQueueGetStatus]
GO

CREATE PROCEDURE dbo.usp_JobQueueGetStatus
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
		AND [descriptor] LIKE @descriptorFilter
		AND [status]=4

WHILE @runningJobs > @minJobToRunBeforeExit
	begin
		---------------------------------------------------------------------------------------------------
		/* check running job status and make updates */
		SET @runningJobs = 0

		DECLARE crsRunningJobs CURSOR FOR	SELECT  [id], [instance_name], [job_name]
											FROM [dbo].[vw_jobExecutionQueue]
											WHERE  [project_id] = @projectID 
													AND [module] LIKE @moduleFilter
													AND [descriptor] LIKE @descriptorFilter
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

				IF @currentRunning = 0
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
						IF @currentRunning = 0
							begin
								UPDATE [dbo].[jobExecutionQueue]
									SET [status] = @lastExecutionStatus,
										[execution_date] = CONVERT([datetime], @lastExecutionDate + ' ' + @lastExecutionTime, 120),
										[running_time_sec] = @runningTimeSec
								WHERE [id] = @jobQueueID

								/* removing job */
								EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
																@jobName		= @jobName,
																@operation		= 'Clean',
																@dbName			= @jobDBName, 
																@jobStepName 	= '',
																@debugMode		= @debugMode
							end
					end
				ELSE
					SET @runningJobs = @runningJobs + 1

				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
			end
		CLOSE crsRunningJobs
		DEALLOCATE crsRunningJobs

		SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		--EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
		IF @runningJobs > @minJobToRunBeforeExit
			WAITFOR DELAY @waitForDelay
	end
	RETURN @runningJobs
GO
