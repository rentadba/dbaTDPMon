RAISERROR('Create procedure: [dbo].[usp_sqlAgentJobCheckStatus]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_sqlAgentJobCheckStatus]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_sqlAgentJobCheckStatus]
GO

CREATE PROCEDURE dbo.usp_sqlAgentJobCheckStatus
		@sqlServerName			[sysname],
		@jobName				[varchar](255),
		@jobID					[varchar](255) = NULL OUTPUT,
		@strMessage				[varchar](8000)=''	  OUTPUT,	
		@currentRunning			[int]=0 			  OUTPUT,			
		@lastExecutionStatus	[int]=0 			  OUTPUT,			
		@lastExecutionDate		[varchar](10)=''	  OUTPUT,		
		@lastExecutionTime 		[varchar](8)=''		  OUTPUT,	
		@runningTimeSec			[bigint]=0			  OUTPUT,
		@selectResult			[bit]=0,
		@extentedStepDetails	[bit]=0,		
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

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE 	@Message 			[varchar](8000), 
			@StepName			[varchar](255),
			@StepID				[int],
			@JobSessionID		[int],
			@RunDate			[varchar](10),
			@RunDateDetail		[varchar](10),
			@RunTime			[varchar](8),
			@RunTimeDetail		[varchar](8),
			@RunDuration		[varchar](8),
			@RunDurationDetail	[varchar](8),
			@RunStatus			[varchar](32),
			@RunStepStatus		[varchar](32),
			@RunStatusDetail	[varchar](32),
			@RunDurationLast	[varchar](8),
			@EventTime			[datetime],		
			@ReturnValue		[int],
			@queryToRun			[nvarchar](4000),
			@queryParams		[nvarchar](512)

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @nestedExecutionLevel		[tinyint]

SET NOCOUNT ON

IF OBJECT_ID('tempdb..#tmpCheck') IS NOT NULL DROP TABLE #tmpCheck
CREATE TABLE #tmpCheck (Result varchar(1024))

---------------------------------------------------------------------------------------------
SET	@strMessage			= NULL
SET	@currentRunning		= NULL
SET	@lastExecutionStatus= NULL
SET	@lastExecutionDate	= NULL
SET	@lastExecutionTime 	= NULL
SET	@runningTimeSec		= NULL

---------------------------------------------------------------------------------------------
IF ISNULL(@sqlServerName, '')=''
	begin
		SET @queryToRun=N'ERROR: The specified value for SOURCE server is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF LEN(@jobName)=0 OR ISNULL(@jobName, '')=''
	begin
		SET @queryToRun = 'ERROR: Must specify a job name.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF @sqlServerName != @@SERVERNAME
	begin
		SET @queryToRun=N'SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
		TRUNCATE TABLE #tmpCheck
		INSERT INTO #tmpCheck EXEC sp_executesql  @queryToRun
		IF (SELECT count(*) FROM #tmpCheck)=0
			begin
				SET @queryToRun=N'ERROR: SOURCE server [' + @sqlServerName + '] is not defined as linked server on THIS server [' + @sqlServerName + '].'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
				RETURN 1
			end
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
										@executionLevel		= 0,
										@debugMode			= @debugMode

---------------------------------------------------------------------------------------------
SET @ReturnValue	= 5 --Unknown

IF @jobID IS NULL
	begin
		SET @queryToRun='SELECT [job_id] FROM [msdb].[dbo].[sysjobs] WITH (NOLOCK) WHERE [name] = ''' + [dbo].[ufn_getObjectQuoteName](@jobName, 'sql') + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		TRUNCATE TABLE #tmpCheck
		INSERT INTO #tmpCheck EXEC sp_executesql  @queryToRun
		SELECT TOP 1 @jobID = ISNULL(Result,'') FROM #tmpCheck
	end
ELSE
	begin
		SET @queryToRun='SELECT [job_id] FROM [msdb].[dbo].[sysjobs] WITH (NOLOCK) WHERE [job_id] = ''' + [dbo].[ufn_getObjectQuoteName](@jobID, 'sql') + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		TRUNCATE TABLE #tmpCheck
		INSERT INTO #tmpCheck EXEC sp_executesql  @queryToRun
		SET @jobID = NULL
		SELECT TOP 1 @jobID = ISNULL(Result,'') FROM #tmpCheck
	end

IF @jobID IS NULL
	begin
		SET @strMessage='WARNING: The specified job name "' + @jobName + '" does not exists on this server [' + @sqlServerName + ']'
		IF @debugMode=1
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		SET @currentRunning = 0
		SET @ReturnValue = -5 --Unknown
	end
ELSE
	begin
		SELECT TOP 1 @jobID = [Result] FROM #tmpCheck
			
		IF OBJECT_ID('tempdb..#runningSQLAgentJobsProcess') IS NOT NULL DROP TABLE #runningSQLAgentJobsProcess
		CREATE TABLE #runningSQLAgentJobsProcess
			(
				  [step_id]		[int], 
				  [step_name]	[sysname],
				  [job_id]		[uniqueidentifier],
				  [session_id]	[int]
			)
		
		--check for active processes started by SQL Agent job
		SET @currentRunning=0
		IF @serverVersionNum >= 10
			SET @queryToRun=N'SELECT	js.[step_id], js.[step_name], ja.[job_id], es.[session_id]
							FROM  [msdb].[dbo].[sysjobactivity] ja WITH (NOLOCK)
							INNER JOIN [msdb].[dbo].[sysjobs] j WITH (NOLOCK) ON ja.[job_id] = j.[job_id]
							INNER JOIN [msdb].[dbo].[sysjobsteps] js WITH (NOLOCK) ON ja.[job_id] = js.[job_id] AND ISNULL(ja.[last_executed_step_id], 0)+ 1 = js.[step_id]
							INNER JOIN
								(
									SELECT  LEFT([intr1], CHARINDEX('':'', [intr1]) - 1) AS [job_id], [session_id]
									FROM [master].sys.dm_exec_sessions x
									CROSS APPLY (SELECT REPLACE(x.[program_name], ''SQLAgent - TSQL JobStep (Job '', '''')) cs(intr1)
									WHERE x.[program_name] LIKE ''SQLAgent - TSQL JobStep (Job %''	
								)es ON es.[job_id] = CONVERT(varchar(max), CONVERT(binary (16), ja.[job_id]), 1)
							WHERE	ja.[session_id] = (
														SELECT TOP 1 [session_id] 
														FROM [msdb].[dbo].[syssessions] WITH (NOLOCK)
														ORDER BY [agent_start_date] DESC
														)
									AND ja.[start_execution_date] IS NOT NULL
									AND ja.[stop_execution_date] IS NULL
									AND j.[job_id] = @jobID'
		ELSE
			SET @queryToRun=N'SELECT DISTINCT sp.[step_id], sjs.[step_name], sp.[job_id], sp.[spid]
							FROM (
								  SELECT  [step_id]
										, SUBSTRING([job_id], 7, 2) + SUBSTRING([job_id], 5, 2) + SUBSTRING([job_id], 3, 2) + LEFT([job_id], 2) + ''-'' + SUBSTRING([job_id], 11, 2) + SUBSTRING([job_id], 9, 2) + ''-'' + SUBSTRING([job_id], 15, 2) + SUBSTRING([job_id], 13, 2) + ''-'' + SUBSTRING([job_id], 17, 4) + ''-'' + RIGHT([job_id], 12) AS [job_id] 
										, [spid]
 								  FROM (
										SELECT SUBSTRING([program_name], CHARINDEX('': Step'', [program_name]) + 7, LEN([program_name]) - CHARINDEX('': Step'', [program_name]) - 7) [step_id]
											 , SUBSTRING([program_name], CHARINDEX(''(Job 0x'', [program_name]) + 7, CHARINDEX('' : Step '', [program_name]) - CHARINDEX(''(Job 0x'', [program_name]) - 7) [job_id]
											 , [spid]
			 							FROM [master].[dbo].[sysprocesses] WITH (NOLOCK) 
										WHERE [program_name] LIKE ''SQLAgent - %JobStep%''
									   ) sp
								) sp
							INNER JOIN [msdb].[dbo].[sysjobs] sj WITH (NOLOCK) ON sj.[job_id] = sp.[job_id]
							INNER JOIN [msdb].[dbo].[sysjobsteps] sjs WITH (NOLOCK) ON sj.[job_id] = sjs.[job_id]
							WHERE CHARINDEX(sj.[name], @jobName) <> 0
							UNION
							SELECT DISTINCT sjs.[step_id], sjs.[step_name], sj.[job_id], sp.[spid]
							FROM [master].[dbo].[sysprocesses] sp WITH (NOLOCK) 
							INNER JOIN [msdb].[dbo].[sysjobs]		sj WITH (NOLOCK) ON sj.[name] = sp.[program_name]
							INNER JOIN [msdb].[dbo].[sysjobsteps]	sjs WITH (NOLOCK) ON sjs.[job_id] = sj.[job_id]
							INNER JOIN [msdb].[dbo].[sysjobhistory] sjh WITH (NOLOCK) ON sjh.[job_id] = sj.[job_id] AND sjh.[step_id] = sjs.[step_id] AND sjh.[run_status] = 4
							WHERE CHARINDEX(sj.[name], @jobName) <> 0'
		SET @queryParams = '@jobID [sysname], @jobName [sysname]'
		
		IF @sqlServerName <> @@SERVERNAME
			begin
				SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
				SET @queryToRun = REPLACE(@queryToRun, '@jobName', '''' + @jobName + N'''');
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			end
		IF @debugMode = 1 
			begin
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @jobID, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			end
		
		INSERT	INTO #runningSQLAgentJobsProcess([step_id], [step_name], [job_id], [session_id])
				EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID, @jobName = @jobName

		SET @StepID = NULL
		SET @JobSessionID = NULL

		SELECT @currentRunning = COUNT(*) FROM #runningSQLAgentJobsProcess
		SELECT TOP 1  @StepID = [step_id]
					, @StepName = [step_name]
					, @jobID  = CAST([job_id] AS [varchar](255))
					, @JobSessionID = [session_id]
		FROM #runningSQLAgentJobsProcess	

		IF OBJECT_ID('tempdb..#runningSQLAgentJobsProcess') IS NOT NULL DROP TABLE #runningSQLAgentJobsProcess
	
		IF @currentRunning > 0 
			begin
				SET @lastExecutionStatus=4 -- in progress
				IF @debugMode=1
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				SET @ReturnValue=4

				--get job start date/time
				IF OBJECT_ID('tempdb..#jobStartInfo') IS NOT NULL DROP TABLE #jobStartInfo
				CREATE TABLE #jobStartInfo
					(
						[start_date]	[varchar](16), 
						[start_time]	[varchar](16), 
						[run_status]	[int], 
						[event_time]	[datetime]
					)

				SET @queryToRun=N'SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [start_date]
											, CAST(h.[run_time] AS varchar) AS [start_time]
											, NULL AS [run_status]
											, GETDATE() AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
								WHERE h.[job_id] = @jobID
										AND h.[instance_id] > (
																/* last job completion id */
																SELECT TOP 1 h1.[instance_id]
																FROM [msdb].[dbo].[sysjobhistory] h1 WITH (NOLOCK) 
																WHERE h1.[job_id] = @jobID
																		AND [step_name] =''(Job outcome)''
																ORDER BY h1.[instance_id] DESC
																)
								ORDER BY h.[instance_id] ASC'
				SET @queryParams = '@jobID [sysname]'
		
				IF @sqlServerName <> @@SERVERNAME
					begin
						SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					end
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				INSERT	INTO #jobStartInfo([start_date], [start_time], [run_status], [event_time])
						EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID

				IF (SELECT COUNT(*) FROM #jobStartInfo)=0
					begin
						IF @StepID <> 1
							begin
								/* job was cancelled, but process is still running, probably performing a rollback */
								SET @queryToRun=N'SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [start_date]
															, CAST(h.[run_time] AS varchar) AS [start_time]
															, h.[run_status]
															, GETDATE() AS [event_time]
												FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
												WHERE h.[job_id] = @jobID
														AND h.[instance_id] = (
																				/* last job completion id */
																				SELECT TOP 1 h1.[instance_id]
																				FROM [msdb].[dbo].[sysjobhistory] h1 WITH (NOLOCK) 
																				WHERE h1.[job_id] = @jobID
																						AND [step_name] =''(Job outcome)''
																				ORDER BY h1.[instance_id] DESC
																				)
												ORDER BY h.[instance_id] ASC'
							end
						ELSE
							begin
								SET @queryToRun=N'SELECT  REPLACE(SUBSTRING(CONVERT([varchar](19), [login_time], 120), 1, 10), ''-'', '''')  AS [start_date]
														, REPLACE(SUBSTRING(CONVERT([varchar](19), [login_time], 120), 12, 19), '':'', '''') AS [start_time]
														, 4 AS [run_status]
														, GETDATE() AS [event_time]
												FROM [master].[dbo].[sysprocesses] WITH (NOLOCK) 
												WHERE [spid] = @JobSessionID'
							end
						SET @queryParams = '@jobID [sysname], @JobSessionID [smallint]'
		
						IF @sqlServerName <> @@SERVERNAME
							begin
								SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
								SET @queryToRun = REPLACE(@queryToRun, '@JobSessionID ', CAST(@JobSessionID  AS [sysname]));
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
							end
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						INSERT	INTO #jobStartInfo([start_date], [start_time], [run_status], [event_time])
								EXEC sp_executesql  @queryToRun, @queryParams,	@jobID = @jobID, 
																				@JobSessionID = @JobSessionID
					end
									
				SET @RunDate	= NULL
				SET @RunTime	= NULL
				SET @EventTime	= NULL
				SELECT TOP 1  @RunDate	 = [start_date]
							, @RunTime	 = [start_time]
							, @RunStatus = CAST(ISNULL([run_status], @lastExecutionStatus) AS [varchar]) 
							, @EventTime = [event_time]
				FROM #jobStartInfo
	

				SET @RunTime = REPLICATE('0', 6 - LEN(@RunTime)) + @RunTime
				SET @RunTime = SUBSTRING(@RunTime, 1, 2) + ':' + SUBSTRING(@RunTime, 3, 2) + ':' + SUBSTRING(@RunTime, 5, 2)
				SET @RunDate = SUBSTRING(@RunDate, 1, 4) + '-' + SUBSTRING(@RunDate, 5, 2) + '-' + SUBSTRING(@RunDate, 7, 2)

				SET @lastExecutionDate = @RunDate
				SET @lastExecutionTime = @RunTime
				SET @runningTimeSec = [dbo].[ufn_getMilisecondsBetweenDates](CONVERT([datetime], @lastExecutionDate + ' ' + @lastExecutionTime, 120), @EventTime) / 1000

				SET @RunStatus = CASE @RunStatus WHEN '0' THEN 'Failed'
												 WHEN '1' THEN 'Succeded'				
												 WHEN '2' THEN 'Retry'
												 WHEN '3' THEN 'Canceled'
												 WHEN '4' THEN 'In progress'
								 END
				
				SET @strMessage=						 'Job currently running step: [' + CAST(@StepID AS varchar) + '] - [' + @StepName + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--	Job started at            : [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--	Execution status          : [' + ISNULL(@RunStatus, '') + ']'	
			end
		ELSE
			begin
				IF OBJECT_ID('tempdb..#jobLastRunDetails') IS NOT NULL DROP TABLE #jobLastRunDetails
				CREATE TABLE #jobLastRunDetails
					(
						[message]		[varchar](4000), 
						[step_id]		[int], 
						[step_name]		[varchar](255), 
						[run_status]	[int], 
						[run_date]		[varchar](16), 
						[run_time]		[varchar](16), 
						[run_duration]	[varchar](16), 
						[event_time]	[datetime])

				SET @queryToRun=N'SELECT TOP 1 h.[message], h.[step_id], h.[step_name], h.[run_status]
											, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
											, GETDATE() AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
								WHERE	h.[job_id] = @jobID
										AND h.[step_name] <> ''(Job outcome)''
								ORDER BY h.[instance_id] DESC'
				SET @queryParams = '@jobID [sysname]'
						
				IF @sqlServerName <> @@SERVERNAME
					begin
						SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					end
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
						EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID
				
				SET @Message	=null
				SET @StepID		=null
				SET @StepName	=null
				SET @lastExecutionStatus=null
				SET @RunStatus	=null
				SET @RunDate	=null
				SET @RunTime	=null
				SET @RunDuration=null
				SET @EventTime	=null
				SELECT TOP 1  @Message		= [message]
							, @StepID		= [step_id]
							, @StepName		= [step_name]
							, @RunDate		= [run_date]
							, @RunTime		= [run_time]
							, @RunDuration	= [run_duration] 
							, @EventTime	= [event_time]
							, @RunStepStatus= [run_status]
				FROM #jobLastRunDetails
				
				SET @queryToRun=N'SELECT TOP 1 NULL AS [message], NULL AS [step_id], NULL AS [step_name], [run_status], NULL AS [run_date], NULL AS [run_time], CAST([run_duration] AS varchar) AS [RunDuration], NULL AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory] WITH (NOLOCK) 
								WHERE	[job_id] = @jobID
										AND [step_name] =''(Job outcome)''
								ORDER BY [instance_id] DESC'
				SET @queryParams = '@jobID [sysname]'
		
				IF @sqlServerName <> @@SERVERNAME
					begin
						SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					end
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				TRUNCATE TABLE #jobLastRunDetails
				INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
						EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID
				
				SET @RunDurationLast=null
				SET @RunStatus=null
				SELECT TOP 1  @RunDurationLast	   = [run_duration]
							, @RunStatus		   = CAST([run_status] AS varchar)
							, @lastExecutionStatus = [run_status] 
				FROM #jobLastRunDetails
			
				SET @RunStatus = ISNULL(@RunStatus, @RunStepStatus)
				SET @lastExecutionStatus = ISNULL(@lastExecutionStatus, @RunStepStatus)

				--for failed jobs, get last step message
				IF @RunStatus=0
					begin
						SET @queryToRun='SELECT TOP 1 h.[message], NULL AS [step_id], NULL AS [step_name], NULL AS [run_status], NULL AS [run_date], NULL AS [run_time], NULL AS [run_duration], NULL AS [event_time]
									FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
									WHERE h.[job_id]= @jobID 
											AND h.[step_name] <> ''(Job outcome)'' 
											AND h.[run_status]=0
									ORDER BY h.[instance_id] DESC'
						SET @queryParams = '@jobID [sysname]'
		
						IF @sqlServerName <> @@SERVERNAME
							begin
								SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
							end
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						TRUNCATE TABLE #jobLastRunDetails
						INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
								EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID

						SELECT TOP 1 @Message=[message] 
						FROM #jobLastRunDetails
						
						SET @lastExecutionStatus=0
					end

				SET @RunDurationLast=REPLICATE('0', 6 - LEN(@RunDurationLast)) + @RunDurationLast
				SET @runningTimeSec = CAST(SUBSTRING(@RunDurationLast, 1, LEN(@RunDurationLast) - 4) AS [bigint])*3600 + CAST(SUBSTRING(RIGHT(@RunDurationLast, 4), 1, 2) AS [bigint])*60 + CAST(SUBSTRING(RIGHT(@RunDurationLast, 4), 3, 2) AS [bigint])
				SET @RunDurationLast=SUBSTRING(@RunDurationLast, 1, LEN(@RunDurationLast) - 4) + ':' + SUBSTRING(RIGHT(@RunDurationLast, 4), 1, 2) + ':' + SUBSTRING(RIGHT(@RunDurationLast, 4), 3, 2)

				/* 2019 patch - have job status information in another way*/
				IF @lastExecutionStatus IS NULL
					begin
						SET @queryToRun='SELECT TOP 1
												NULL AS [message], 
												js.[step_id], 
												js.step_name AS [step_name], 
												ISNULL(jh.run_status, js.last_run_outcome) AS [run_status], 
												REPLACE(CONVERT([varchar](10), ja.start_execution_date, 120), ''-'', '''') AS [run_date],
												REPLACE(CONVERT([varchar](8), ja.start_execution_date, 108), '':'', '''')  AS [run_time], 
												DATEDIFF(ss, ja.[start_execution_date], ISNULL(ja.[stop_execution_date], GETDATE())) AS [run_duration], 
												NULL AS [event_time]
											FROM msdb.dbo.sysjobs j
											LEFT JOIN msdb.dbo.sysjobactivity ja ON ja.job_id = j.job_id
																				AND ja.run_requested_date IS NOT NULL
																				AND ja.start_execution_date IS NOT NULL
											LEFT JOIN msdb.dbo.sysjobsteps js ON js.job_id = ja.job_id
																				AND js.step_id = ja.last_executed_step_id
											LEFT JOIN msdb.dbo.sysjobhistory jh ON jh.job_id = j.job_id
																				AND jh.instance_id = ja.job_history_id
											WHERE j.[job_id] = @jobID 
											ORDER BY ja.start_execution_date DESC'
						SET @queryParams = '@jobID [sysname]'
		
						IF @sqlServerName <> @@SERVERNAME
							begin
								SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
							end
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						TRUNCATE TABLE #jobLastRunDetails
						INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
								EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID

						SET @RunDurationLast=null
						SET @RunStatus=null
						SELECT TOP 1  @runningTimeSec		= [run_duration]
									, @RunStatus			= CAST([run_status] AS varchar)
									, @lastExecutionStatus	= CAST([run_status] AS varchar) 
									, @StepName				= [step_name]
									, @RunDate				= [run_date]
									, @RunTime				= [run_time]	
						FROM #jobLastRunDetails
					end

				IF @lastExecutionStatus IS NULL
					begin
						SET @RunStatus='Unknown'
						SET @lastExecutionStatus='5' 
					end

				SET @RunStatus = CASE @RunStatus WHEN '0' THEN 'Failed'
												 WHEN '1' THEN 'Succeded'				
												 WHEN '2' THEN 'Retry'
												 WHEN '3' THEN 'Canceled'
												 WHEN '4' THEN 'In progress'
								 END

				SET @RunTime=REPLICATE('0', 6 - LEN(@RunTime)) + @RunTime
				SET @RunTime=SUBSTRING(@RunTime, 1, 2) + ':' + SUBSTRING(@RunTime, 3, 2) + ':' + SUBSTRING(@RunTime, 5, 2)
				SET @RunDate=SUBSTRING(@RunDate, 1, 4) + '-' + SUBSTRING(@RunDate, 5, 2) + '-' + SUBSTRING(@RunDate, 7, 2)
				SET @RunDuration=REPLICATE('0', 6 - LEN(@RunDuration)) + @RunDuration
				--SET @RunDuration=SUBSTRING(@RunDuration, 1,2) + ':' + SUBSTRING(@RunDuration, 3,2) + ':' + SUBSTRING(@RunDuration, 5,2)
				SET @RunDuration=SUBSTRING(@RunDuration, 1, LEN(@RunDuration) - 4) + ':' + SUBSTRING(RIGHT(@RunDuration, 4), 1, 2) + ':' + SUBSTRING(RIGHT(@RunDuration, 4), 3, 2)
				
				SET @strMessage='The specified job [' + @sqlServerName + '].' + @jobName + ' is not currently running.'
				IF @RunStatus<>'Unknown'
					begin
						SET @strMessage=@strMessage + CHAR(13) + '--	Last execution step			: [' + ISNULL(CAST(@StepID AS varchar), '') + '] - [' + ISNULL(@StepName, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--	Last step finished at      	: [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--	Last step running time		: [' + ISNULL(@RunDuration, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--	Job execution time (total)	: [' + ISNULL(@RunDurationLast, '') + ']'	
					end
				SET @strMessage=@strMessage + CHAR(13) + '--	Last job execution status  	: [' + ISNULL(@RunStatus, 'Unknown') + ']'	

				SET @lastExecutionDate=@RunDate
				SET @lastExecutionTime=@RunTime

				SET @ReturnValue=@lastExecutionStatus
			end

			IF @extentedStepDetails=1
				begin
					IF OBJECT_ID('tempdb..#jobRunStepDetails') IS NOT NULL DROP TABLE #jobRunStepDetails
					CREATE TABLE #jobRunStepDetails
						(
							[message]		[varchar](4000), 
							[step_id]		[int], 
							[step_name]		[varchar](255), 
							[run_status]	[int], 
							[run_date]		[varchar](16), 
							[run_time]		[varchar](16), 
							[run_duration]	[varchar](16), 
							[event_time]	[datetime])

					--get job execution details: steps execution status
					IF @currentRunning = 0 
						SET @queryToRun=N'SELECT   h.[message], h.[step_id], h.[step_name], h.[run_status]
												, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
												, GETDATE() AS [event_time]
										FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
										WHERE	 h.[instance_id] < (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
																			WHERE	h.[job_id] = @jobID
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND	h.[instance_id] > ISNULL(
																	( SELECT [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
																			WHERE	h.[job_id] = @jobID
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	WHERE [instance_id] NOT IN 
																		(
																		SELECT TOP 1 [instance_id] 
																		FROM (	SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																				FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
																				WHERE	h.[job_id] = @jobID
																						AND h.[step_name] =''(Job outcome)''
																				ORDER BY h.[instance_id] DESC
																			)A
																		)),0)
												AND h.[job_id] = @jobID
											ORDER BY h.[instance_id]'
					ELSE
						SET @queryToRun=N'SELECT   h.[message], h.[step_id], h.[step_name], h.[run_status]
												, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
												, GETDATE() AS [event_time]
										FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
										WHERE	 h.[instance_id] > (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h WITH (NOLOCK) 
																			WHERE	h.[job_id] = @jobID 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND j.[job_id] = @jobID
											ORDER BY h.[instance_id]'
					SET @queryParams = '@jobID [sysname]'
		
					IF @sqlServerName <> @@SERVERNAME
						begin
							SET @queryToRun = REPLACE(@queryToRun, '@jobID', '''' + @jobID + N'''');
							SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						end
					IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					TRUNCATE TABLE #jobRunStepDetails
					INSERT	INTO #jobRunStepDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
							EXEC sp_executesql  @queryToRun, @queryParams, @jobID = @jobID
						
					DECLARE @maxLengthStepName [int]
					SELECT @maxLengthStepName = MAX(LEN([step_name]))
					FROM #jobRunStepDetails
					
					SET @maxLengthStepName = ISNULL(@maxLengthStepName, 16)

					DECLARE crsJobDetails CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT   [step_id]
																						, [step_name]
																						, [run_status]
																						, [run_date]
																						, [run_time]
																						, [run_duration]
																						, [message]
																		FROM #jobRunStepDetails
																		ORDER BY [run_date], [run_time]
					OPEN crsJobDetails
					FETCH NEXT FROM crsJobDetails INTO @StepID, @StepName, @RunStatusDetail, @RunDateDetail, @RunTimeDetail, @RunDurationDetail, @queryToRun

					IF @@FETCH_STATUS=0
						begin
							SET @queryToRun='[' + LEFT('Run Date' + SPACE(10), 10) + '] [' + LEFT('RunTime' + SPACE(8), 8) +'] [' + LEFT('Status' + SPACE(12), 12) + '] [' + LEFT('Duration' + SPACE(20), 20) + '] [' + LEFT('ID' + SPACE(3), 3) + '] [' + LEFT('Step Name' + SPACE(@maxLengthStepName), @maxLengthStepName) + ']'
							SET @strMessage=@strMessage + CHAR(13) + @queryToRun
						end
						
					WHILE @@FETCH_STATUS=0
						begin								
							SET @RunStatusDetail = CASE @RunStatusDetail WHEN '0' THEN 'Failed'
																			WHEN '1' THEN 'Succeded'				
																			WHEN '2' THEN 'Retry'
																			WHEN '3' THEN 'Canceled'
																			WHEN '4' THEN 'In progress'
														END
	
							SET @RunTimeDetail=REPLICATE('0', 6 - LEN(@RunTimeDetail)) + @RunTimeDetail
							SET @RunTimeDetail=SUBSTRING(@RunTimeDetail, 1, 2) + ':' + SUBSTRING(@RunTimeDetail, 3, 2) + ':' + SUBSTRING(@RunTimeDetail, 5, 2)
							SET @RunDateDetail=SUBSTRING(@RunDateDetail, 1, 4) + '-' + SUBSTRING(@RunDateDetail, 5, 2) + '-' + SUBSTRING(@RunDateDetail, 7, 2)

							SET @RunDurationDetail=REPLICATE('0', 6 - LEN(@RunDurationDetail)) + @RunDurationDetail
								
							SET @strMessage=@strMessage + CHAR(13) + ISNULL(
									'[' + LEFT(@RunDateDetail + SPACE(10), 10) + '] ' + 
									'[' + LEFT(@RunTimeDetail + SPACE(8), 8) + '] ' + 
									'[' + LEFT(@RunStatusDetail + SPACE(12), 12) + '] ' + 
									'[' + LEFT(dbo.ufn_reportHTMLFormatTimeValue((CAST(SUBSTRING(@RunDurationDetail, 1, LEN(@RunDurationDetail) - 4) AS [bigint])*3600 + CAST(SUBSTRING(RIGHT(@RunDurationDetail, 4), 1, 2) AS [bigint])*60 + CAST(SUBSTRING(RIGHT(@RunDurationDetail, 4), 3, 2) AS [bigint]))*1000) + SPACE(20), 20) + '] ' + 
									'[' + LEFT(CAST(@StepID AS varchar) + SPACE(3), 3) + '] ' + 
									'[' + LEFT(@StepName + SPACE(@maxLengthStepName), @maxLengthStepName) + ']', '')

							FETCH NEXT FROM crsJobDetails INTO @StepID, @StepName, @RunStatusDetail, @RunDateDetail, @RunTimeDetail, @RunDurationDetail, @queryToRun
						end
					CLOSE crsJobDetails
					DEALLOCATE crsJobDetails					
				end

			--final error message
			IF @currentRunning = 0  AND @RunStatus='Failed'
				begin
					SET @strMessage=@strMessage + CHAR(13) + '--Job execution return this message: ' + ISNULL(@Message, '')
					IF @debugMode=1
						begin
							SET @queryToRun = 'Job execution return this message: ' + ISNULL(@Message, '')
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						end

				end
	end

IF @lastExecutionStatus = 2 /* retry */ SET @currentRunning = 1

SET @lastExecutionStatus = ISNULL(@lastExecutionStatus, 5) --Unknown

SET @ReturnValue=ISNULL(@ReturnValue, 0)
IF @selectResult=1
	SELECT @strMessage AS StrMessage, @currentRunning AS CurrentRunning, @lastExecutionStatus AS LastExecutionStatus, @lastExecutionDate AS LastExecutionDate, @lastExecutionTime AS LastExecutionTime, @runningTimeSec AS RunningTimeSec
RETURN @ReturnValue



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO