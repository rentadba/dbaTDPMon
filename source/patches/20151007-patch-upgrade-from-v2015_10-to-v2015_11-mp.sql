USE [dbaTDPMon]
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Collect SQL Errorlog last files')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'health-check'		AS [module], 'Collect SQL Errorlog last files'	AS [name], '1'			AS [value]
GO


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
		@strMessage				[varchar](8000)=''	OUTPUT,	
		@currentRunning			[int]=0 			OUTPUT,			
		@lastExecutionStatus	[int]=0 			OUTPUT,			
		@lastExecutionDate		[varchar](10)=''	OUTPUT,		
		@lastExecutionTime 		[varchar](8)=''		OUTPUT,	
		@runningTimeSec			[bigint]=0			OUTPUT,
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
			@JobID				[varchar](255),
			@StepID				[int],
			@JobSessionID		[int],
			@RunDate			[varchar](10),
			@RunDateDetail		[varchar](10),
			@RunTime			[varchar](8),
			@RunTimeDetail		[varchar](8),
			@RunDuration		[varchar](8),
			@RunDurationDetail	[varchar](8),
			@RunStatus			[varchar](32),
			@RunStatusDetail	[varchar](32),
			@RunDurationLast	[varchar](8),
			@EventTime			[datetime],		
			@ReturnValue		[int],
			@queryToRun			[nvarchar](4000)

SET NOCOUNT ON

IF OBJECT_ID('tempdb..#tmpCheck') IS NOT NULL DROP TABLE #tmpCheck
CREATE TABLE #tmpCheck (Result varchar(1024))

IF ISNULL(@sqlServerName, '')=''
	begin
		SET @queryToRun=N'--	ERROR: The specified value for SOURCE server is not valid.'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

IF LEN(@jobName)=0 OR ISNULL(@jobName, '')=''
	begin
		RAISERROR('--ERROR: Must specify a job name.', 10, 1) WITH NOWAIT
		RETURN 1
	end

SET @queryToRun=N'SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
TRUNCATE TABLE #tmpCheck
INSERT INTO #tmpCheck EXEC (@queryToRun)
IF (SELECT count(*) FROM #tmpCheck)=0
	begin
		SET @queryToRun=N'--	ERROR: SOURCE server [' + @sqlServerName + '] is not defined as linked server on THIS server [' + @sqlServerName + '].'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

---------------------------------------------------------------------------------------------
SET	@strMessage			= NULL
SET	@currentRunning		= NULL
SET	@lastExecutionStatus= NULL
SET	@lastExecutionDate	= NULL
SET	@lastExecutionTime 	= NULL
SET	@runningTimeSec		= NULL


---------------------------------------------------------------------------------------------
SET @ReturnValue	= 5 --Unknown

SET @queryToRun=N'SELECT  CAST([job_id] AS [varchar](255)) AS [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' + @jobName + ''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode = 1 PRINT @queryToRun

TRUNCATE TABLE #tmpCheck
INSERT INTO #tmpCheck EXEC (@queryToRun)
------------------------------------------------------------------------------------------------------------------------------------------
IF (SELECT COUNT(*) FROM #tmpCheck)=0
	begin
		SET @strMessage='--SQL Server Agent: The specified job name [' + @jobName + '] does not exists on this server [' + @sqlServerName + ']'
		IF @debugMode=1
			RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		SET @currentRunning = 0
		SET @ReturnValue = -5 --Unknown
	end
ELSE
	begin
		SELECT TOP 1 @JobID = [Result] FROM #tmpCheck
			
		IF OBJECT_ID('tempdb..#runningSQLAgentJobsProcess') IS NOT NULL DROP TABLE #runningSQLAgentJobsProcess
		CREATE TABLE #runningSQLAgentJobsProcess
			(
				  [step_id]		[int], 
				  [job_id]		[uniqueidentifier],
				  [session_id]	[int]
			)
		
		--check for active processes started by SQL Agent job
		SET @currentRunning=0
		SET @queryToRun=N'SELECT DISTINCT sp.[step_id], sp.[job_id], sp.[spid]
						FROM (
							  SELECT  [step_id]
									, SUBSTRING([job_id], 7, 2) + SUBSTRING([job_id], 5, 2) + SUBSTRING([job_id], 3, 2) + LEFT([job_id], 2) + ''-'' + SUBSTRING([job_id], 11, 2) + SUBSTRING([job_id], 9, 2) + ''-'' + SUBSTRING([job_id], 15, 2) + SUBSTRING([job_id], 13, 2) + ''-'' + SUBSTRING([job_id], 17, 4) + ''-'' + RIGHT([job_id], 12) AS [job_id] 
									, [spid]
 							  FROM (
									SELECT SUBSTRING([program_name], CHARINDEX('': Step'', [program_name]) + 7, LEN([program_name]) - CHARINDEX('': Step'', [program_name]) - 7) [step_id]
										 , SUBSTRING([program_name], CHARINDEX(''(Job 0x'', [program_name]) + 7, CHARINDEX('' : Step '', [program_name]) - CHARINDEX(''(Job 0x'', [program_name]) - 7) [job_id]
										 , [spid]
			 						FROM [master].[dbo].[sysprocesses] 
									WHERE [program_name] LIKE ''SQLAgent - %JobStep%''
								   ) sp
							) sp
						INNER JOIN [msdb].[dbo].[sysjobs] sj ON sj.[job_id] = sp.[job_id]
						WHERE sj.[name]= ''' + @jobName + N'''
						UNION
						SELECT DISTINCT sjs.[step_id], sj.[job_id], sp.[spid]
						FROM [master].[dbo].[sysprocesses] sp
						INNER JOIN [msdb].[dbo].[sysjobs]		sj  ON sj.[name] = sp.[program_name]
						INNER JOIN [msdb].[dbo].[sysjobsteps]	sjs ON sjs.[job_id] = sj.[job_id]
						INNER JOIN [msdb].[dbo].[sysjobhistory] sjh ON sjh.[job_id] = sj.[job_id] AND sjh.[step_id] = sjs.[step_id] AND sjh.[run_status] = 4
						WHERE sj.[name]= ''' + @jobName + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun
		INSERT	INTO #runningSQLAgentJobsProcess([step_id], [job_id], [session_id])
				EXEC (@queryToRun)

		SET @StepID = NULL
		SET @JobSessionID = NULL

		SELECT @currentRunning = COUNT(*) FROM #runningSQLAgentJobsProcess
		SELECT TOP 1  @StepID = [step_id]
					, @JobID  = CAST([job_id] AS [varchar](255))
					, @JobSessionID = [session_id]
		FROM #runningSQLAgentJobsProcess	

		IF OBJECT_ID('tempdb..#runningSQLAgentJobsProcess') IS NOT NULL DROP TABLE #runningSQLAgentJobsProcess
	
		IF @currentRunning > 0 
			begin
				SET @queryToRun=N'SELECT [step_name] FROM [msdb].[dbo].[sysjobsteps] WHERE [step_id]=' + CAST(@StepID AS [nvarchar]) + ' AND [job_id]=''' + @JobID + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #tmpCheck
				INSERT INTO #tmpCheck EXEC (@queryToRun)
				SELECT TOP 1 @StepName=Result FROM #tmpCheck

				SET @lastExecutionStatus=4 -- in progress
				IF @debugMode=1
					RAISERROR(@strMessage, 10, 1) WITH NOWAIT
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
								FROM [msdb].[dbo].[sysjobhistory] h
								WHERE h.[job_id]=''' + @JobID + N''' 
										AND h.[instance_id] > (
																/* last job completion id */
																SELECT TOP 1 h1.[instance_id]
																FROM [msdb].[dbo].[sysjobhistory] h1
																WHERE h1.[job_id]=''' + @JobID + N''' 
																		AND [step_name] =''(Job outcome)''
																ORDER BY h1.[instance_id] DESC
																)
								ORDER BY h.[instance_id] ASC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				INSERT	INTO #jobStartInfo([start_date], [start_time], [run_status], [event_time])
						EXEC (@queryToRun)

				
				IF (SELECT COUNT(*) FROM #jobStartInfo)=0
					begin
						IF @StepID <> 1
							begin
								/* job was cancelled, but process is still running, probably performing a rollback */
								SET @queryToRun=N'SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [start_date]
															, CAST(h.[run_time] AS varchar) AS [start_time]
															, h.[run_status]
															, GETDATE() AS [event_time]
												FROM [msdb].[dbo].[sysjobhistory] h
												WHERE h.[job_id]=''' + @JobID + N''' 
														AND h.[instance_id] = (
																				/* last job completion id */
																				SELECT TOP 1 h1.[instance_id]
																				FROM [msdb].[dbo].[sysjobhistory] h1
																				WHERE h1.[job_id]=''' + @JobID + N''' 
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
												FROM [master].[dbo].[sysprocesses]
												WHERE [spid] = ' + CAST(@JobSessionID AS [nvarchar])
							end
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						INSERT	INTO #jobStartInfo([start_date], [start_time], [run_status], [event_time])
								EXEC (@queryToRun)
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
				
				SET @strMessage=                         '--Job currently running step: [' + CAST(@StepID AS varchar) + '] - [' + @StepName + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--Job started at            : [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--Execution status          : [' + ISNULL(@RunStatus, '') + ']'	
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
								FROM [msdb].[dbo].[sysjobhistory] h
								WHERE	h.[job_id]=''' + @JobID + N''' 
										AND h.[step_name] <> ''(Job outcome)''
								ORDER BY h.[instance_id] DESC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
						EXEC (@queryToRun)
				
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
				FROM #jobLastRunDetails
				
				SET @queryToRun=N'SELECT TOP 1 NULL AS [message], NULL AS [step_id], NULL AS [step_name], [run_status], NULL AS [run_date], NULL AS [run_time], CAST([run_duration] AS varchar) AS [RunDuration], NULL AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory]
								WHERE	[job_id] = ''' + @JobID + N'''
										AND [step_name] =''(Job outcome)''
								ORDER BY [instance_id] DESC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #jobLastRunDetails
				INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
						EXEC (@queryToRun)
				
				SET @RunDurationLast=null
				SET @RunStatus=null
				SELECT TOP 1  @RunDurationLast	   = [run_duration]
							, @RunStatus		   = CAST([run_status] AS varchar)
							, @lastExecutionStatus = [run_status] 
				FROM #jobLastRunDetails
			
				--for failed jobs, get last step message
				IF @RunStatus=0
					begin
						SET @queryToRun='SELECT TOP 1 h.[message], NULL AS [step_id], NULL AS [step_name], NULL AS [run_status], NULL AS [run_date], NULL AS [run_time], NULL AS [run_duration], NULL AS [event_time]
									FROM [msdb].[dbo].[sysjobhistory] h
									WHERE h.[job_id]=''' + @JobID + ''' 
											AND h.[step_name] <> ''(Job outcome)'' 
											AND h.[run_status]=0
									ORDER BY h.[instance_id] DESC'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						TRUNCATE TABLE #jobLastRunDetails
						INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
								EXEC (@queryToRun)

						SELECT TOP 1 @Message=[message] 
						FROM #jobLastRunDetails
						
						SET @lastExecutionStatus=0
					end

				SET @RunDurationLast=REPLICATE('0', 6 - LEN(@RunDurationLast)) + @RunDurationLast
				SET @runningTimeSec = CAST(SUBSTRING(@RunDurationLast, 1, LEN(@RunDurationLast) - 4) AS [bigint])*3600 + CAST(SUBSTRING(RIGHT(@RunDurationLast, 4), 1, 2) AS [bigint])*60 + CAST(SUBSTRING(RIGHT(@RunDurationLast, 4), 3, 2) AS [bigint])
				SET @RunDurationLast=SUBSTRING(@RunDurationLast, 1, LEN(@RunDurationLast) - 4) + ':' + SUBSTRING(RIGHT(@RunDurationLast, 4), 1, 2) + ':' + SUBSTRING(RIGHT(@RunDurationLast, 4), 3, 2)
				
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
				
				SET @strMessage='--The specified job [' + @sqlServerName + '].[' + @jobName + '] is not currently running.'
				IF @RunStatus<>'Unknown'
					begin
						SET @strMessage=@strMessage + CHAR(13) + '--Last execution step			: [' + ISNULL(CAST(@StepID AS varchar), '') + '] - [' + ISNULL(@StepName, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Last step finished at      	: [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Last step running time		: [' + ISNULL(@RunDuration, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Job execution time (total)	: [' + ISNULL(@RunDurationLast, '') + ']'	
					end
				SET @strMessage=@strMessage + CHAR(13) + '--Last job execution status  	: [' + ISNULL(@RunStatus, 'Unknown') + ']'	

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
										FROM [msdb].[dbo].[sysjobhistory] h
										WHERE	 h.[instance_id] < (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h
																			WHERE	h.[job_id]=''' + @JobID + N''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND	h.[instance_id] > ISNULL(
																	( SELECT [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h
																			WHERE	h.[job_id]=''' + @JobID + N''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	WHERE [instance_id] NOT IN 
																		(
																		SELECT TOP 1 [instance_id] 
																		FROM (	SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																				FROM [msdb].[dbo].[sysjobhistory] h
																				WHERE	h.[job_id]=''' + @JobID + N''' 
																						AND h.[step_name] =''(Job outcome)''
																				ORDER BY h.[instance_id] DESC
																			)A
																		)),0)
												AND h.[job_id] = ''' + @JobID + N'''
											ORDER BY h.[instance_id]'
					ELSE
						SET @queryToRun=N'SELECT   h.[message], h.[step_id], h.[step_name], h.[run_status]
												, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
												, GETDATE() AS [event_time]
										FROM [msdb].[dbo].[sysjobhistory] h
										WHERE	 h.[instance_id] > (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h
																			WHERE	h.[job_id]=''' + @JobID + N''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND j.[job_id] = ''' + @JobID + N'''
											ORDER BY h.[instance_id]'

					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 PRINT @queryToRun

					TRUNCATE TABLE #jobRunStepDetails
					INSERT	INTO #jobRunStepDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
							EXEC (@queryToRun)
						
					DECLARE @maxLengthStepName [int]
					SELECT @maxLengthStepName = MAX(LEN([step_name]))
					FROM #jobRunStepDetails
					
					SET @maxLengthStepName = ISNULL(@maxLengthStepName, 16)

					DECLARE crsJobDetails CURSOR FOR	SELECT DISTINCT   [step_id]
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
						print '--Job execution return this message: ' + ISNULL(@Message, '')
				end
	end

IF @debugMode=1
	print @strMessage
SET @ReturnValue=ISNULL(@ReturnValue, 0)
IF @selectResult=1
	SELECT @strMessage AS StrMessage, @currentRunning AS CurrentRunning, @lastExecutionStatus AS LastExecutionStatus, @lastExecutionDate AS LastExecutionDate, @lastExecutionTime AS LastExecutionTime, @runningTimeSec AS RunningTimeSec
RETURN @ReturnValue



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableForeignKeys]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableForeignKeys]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
		@SQLServerName		[sysname],
		@DBName				[sysname],
		@TableSchema		[sysname] = '%', 
		@TableName			[sysname] = '%',
		@ConstraintName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2049,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@ConstraintName	- specify constraint name to be enabled/disabled. default all
--		@flgAction:		 1	- Enable Constraints (default)
--						 0	- Disable Constraints
--		@flgOptions:	 1	- Use tables that have foreign key constraints that reffer current table (default)
--						 2	- Use tables that current table foreign key constraints reffer  
--						 4  - Enable constraints with NOCHECK. Default is to enable constraints using CHECK option
--						 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    				[nvarchar](max),
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@tmpSchemaName			[sysname],
			@tmpTableName			[sysname],
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@tmpConstraintName		[sysname],
			@errorCode				[int],
			@tmpFlgAction			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
				CREATE TABLE #tmpTableToAlterConstraints 
							(
								[TableSchema]		[sysname]
							  , [TableName]			[sysname]
							  , [ConstraintName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1	THEN 'Enable'
																ELSE 'Disable'
										END + ' foreign key constraints for: [' + @crtTableSchema + '].[' + @crtTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable foreign key constraint, will get only enabled constraints
						--if current action is to enable foreign key constraint, will get only disabled constraints
						IF (@flgOptions & 1 = 1)
							begin
								--list all tables that have foreign key constraints that reffers current table					
								SET @queryToRun=N'SELECT DISTINCT sch.[name] AS [schema_name], so.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM [' + @DBName + '].[sys].[objects] so
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN [' + @DBName + '].[sys].[foreign_keys]	sfk  ON so.[object_id] = sfk.[parent_object_id]
												INNER JOIN [' + @DBName + '].[sys].[objects]		so2  ON sfk.[referenced_object_id] = so2.[object_id]
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch2 ON sch2.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + @crtTableName + '''
														AND sch2.[name] = ''' + @crtTableSchema + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
														AND sfk.[name] LIKE ''' + @ConstraintName + ''''
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						IF (@flgOptions & 2 = 2)
							begin
								--list all tables that current table foreign key constraints reffers 
								SET @queryToRun='SELECT DISTINCT sch2.[name] AS [schema_name], so2.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM [' + @DBName + '].[sys].[objects] so
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN [' + @DBName + '].[sys].[foreign_keys]	sfk ON so.[object_id] = sfk.[referenced_object_id]
												INNER JOIN [' + @DBName + '].[sys].[objects]		so2 ON sfk.[parent_object_id] = so2.[object_id]
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch2 ON sch.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + @crtTableName + '''
														AND sch2.[name] = ''' + @crtTableSchema + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar])+ '
														AND sfk.[name] LIKE ''' + @ConstraintName + ''''

								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints ([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						DECLARE crsTableToAlterConstraints CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TableSchema], [TableName], [ConstraintName]
																							FROM #tmpTableToAlterConstraints
																							ORDER BY [TableName]						
						OPEN crsTableToAlterConstraints
						FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= '[' + @tmpSchemaName + '].[' + @tmpTableName + ']'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								--enable/disable foreign key constraints
								SET @queryToRun='ALTER TABLE [' + @DBName + '].[' + @tmpSchemaName + '].[' + @tmpTableName + ']' + 
												CASE WHEN @flgAction=1	
													 THEN ' WITH ' + 
															CASE WHEN @flgOptions & 4 = 4	THEN 'NOCHECK'
																							ELSE 'CHECK'
															END + ' CHECK '	
													 ELSE ' NOCHECK '
												END + 'CONSTRAINT [' + @tmpConstraintName + ']'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								--
								SET @objectName = '[' + @tmpSchemaName + '].[' + @tmpTableName + ']'
								SET @childObjectName = QUOTENAME(@tmpConstraintName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableForeignKeys',
																				@eventName		= 'database maintenance - alter constraints',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								IF @errorCode=0	
									begin
										/* 0 disable FK -> insert action 1 */
										/* 1 enable FK  -> delete action 2 */
										SET @tmpFlgAction = CASE WHEN @flgAction=1 THEN 2 ELSE 1 END
										EXEC [dbo].[usp_mpMarkInternalAction]		@actionName			= N'foreign-key-made-disable',
																					@flgOperation		= @tmpFlgAction,
																					@server_name		= @SQLServerName,
																					@database_name		= @DBName,
																					@schema_name		= @tmpSchemaName,
																					@object_name		= @tmpTableName,
																					@child_object_name	= @tmpConstraintName
									end
						
								FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
							end
						CLOSE crsTableToAlterConstraints
						DEALLOCATE crsTableToAlterConstraints
						
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		IF object_id('#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
	SET @errorCode = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableIndexes]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableIndexes]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableIndexes]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableIndexes]
		@SQLServerName				[sysname],
		@DBName						[sysname],
		@TableSchema				[sysname] = '%',
		@TableName					[sysname] = '%',
		@IndexName					[sysname] = '%',
		@IndexID					[int],
		@PartitionNumber			[int] = 1,
		@flgAction					[tinyint] = 1,
		@flgOptions					[int] = 6145, --4096 + 2048 + 1	/* 6177 for space optimized index rebuild */
		@MaxDOP						[smallint] = 1,
		@FillFactor					[tinyint] = 0,
		@executionLevel				[tinyint] = 0,
		@affectedDependentObjects	[nvarchar](max) OUTPUT,
		@DebugMode					[bit] = 0
/* WITH ENCRYPTION */
AS


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed.
--		@IndexName		- name of the index to be analyzed
--		@IndexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@PartitionNumber- index partition number. default value = 1 (index with no partitions)
--		@flgAction:		 1	- Rebuild index (default)
--						 2  - Reorganize indexes
--						 4	- Disable index
--		@flgOptions		 1  - Compact large objects (LOB) when reorganize  (default)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes
--						 8  - Disable non-clustered index before rebuild (save space) (won't apply when 4096 is applicable)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@strMessage				[nvarchar](4000),
			@sqlIndexCreate			[nvarchar](max),
			@sqlScriptOnline		[nvarchar](512),
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@crtIndexID				[int],
			@crtIndexName			[sysname],			
			@crtIndexType			[tinyint],
			@crtIndexAllowPageLocks	[bit],
			@crtIndexIsDisabled		[bit],
			@crtIndexIsPrimaryXML	[bit],
			@crtIndexHasDependentFK	[bit],
			@crtTableIsReplicated	[bit],
			@flgInheritOptions		[int],
			@tmpIndexName			[sysname],
			@tmpIndexIsPrimaryXML	[bit],
			@nestedExecutionLevel	[tinyint]

DECLARE   @flgRaiseErrorAndStop [bit]
		, @errorCode			[int]

DECLARE @DependentIndexes TABLE	(
									[index_name]		[sysname]	NULL
								  , [is_primary_xml]	[bit]		DEFAULT(0)
								)

SET NOCOUNT ON

DECLARE @tmpTableToAlterIndexes TABLE
			(
				[index_id]			[int]		NULL
			  , [index_name]		[sysname]	NULL
			  , [index_type]		[tinyint]	NULL
			  , [allow_page_locks]	[bit]		NULL
			  , [is_disabled]		[bit]		NULL
			  , [is_primary_xml]	[bit]		NULL
			  , [has_dependent_fk]	[bit]		NULL
			  , [is_replicated]		[bit]		NULL
			)


-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get configuration values
		---------------------------------------------------------------------------------------------
		DECLARE @queryLockTimeOut [int]
		SELECT	@queryLockTimeOut=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name] = 'Default lock timeout (ms)'
				AND [module] = 'common'

		---------------------------------------------------------------------------------------------		
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage=N'Alter indexes ON [' + @crtTableSchema + '].[' + @crtTableName + '] : ' + 
											CASE @flgAction WHEN 1 THEN 'REBUILD'
															WHEN 2 THEN 'REORGANIZE'
															WHEN 4 THEN 'DISABLE'
															ELSE 'N/A'
											END
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable/reorganize indexes, will get only enabled indexes
						--if current action is to rebuild, will get both enabled/disabled indexes
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT  si.[index_id]
														, si.[name]
														, si.[type]
														, si.[allow_page_locks]
														, si.[is_disabled]
														, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
														, CASE WHEN SUM(CASE WHEN fk.[name] IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS [has_dependent_fk]
														, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0) AS [is_replicated]
													FROM [' + @DBName + '].[sys].[indexes]				si
													INNER JOIN [' + @DBName + '].[sys].[objects]		so  ON so.[object_id] = si.[object_id]
													INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
													LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
													LEFT  JOIN [' + @DBName + '].[sys].[foreign_keys]	fk  ON fk.[referenced_object_id] = so.[object_id] AND fk.[key_index_id] = si.[index_id]
													LEFT  JOIN [' + @DBName + '].[sys].[tables]			st  ON st.[object_id] = so.[object_id]
													WHERE	so.[name] = ''' + @crtTableName + '''
															AND sch.[name] = ''' + @crtTableSchema + '''
															AND so.[is_ms_shipped] = 0' + 
															CASE	WHEN @IndexName IS NOT NULL 
																	THEN ' AND si.[name] LIKE ''' + @IndexName + ''''
																	ELSE CASE WHEN @IndexID  IS NOT NULL 
																			  THEN ' AND si.[index_id] = ' + CAST(@IndexID AS [nvarchar])
																			  ELSE ''
																		 END
															END + '
															AND si.[is_disabled] IN ( ' + CASE WHEN @flgAction IN (2, 4) THEN '0' ELSE '0,1' END + ')
													GROUP BY si.[index_id]
															, si.[name]
															, si.[type]
															, si.[allow_page_locks]
															, si.[is_disabled]
															, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END
															, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0)'

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @tmpTableToAlterIndexes
						INSERT	INTO @tmpTableToAlterIndexes([index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated])
								EXEC (@queryToRun)

						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList



				DECLARE crsTableToAlterIndexes CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated]
																				FROM @tmpTableToAlterIndexes
																				ORDER BY [index_id], [index_name]						
				OPEN crsTableToAlterIndexes
				FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage= '[' + @crtIndexName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @sqlScriptOnline=N''
						---------------------------------------------------------------------------------------------
						-- 1  - Rebuild indexes
						---------------------------------------------------------------------------------------------
						IF @flgAction = 1
							begin
								-- check for online operation mode	
								IF @flgOptions & 4096 = 4096
									begin
										SET @nestedExecutionLevel = @executionLevel + 3
										EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																						@dbName				= @DBName,
																						@tableSchema		= @crtTableSchema,
																						@tableName			= @crtTableName,
																						@indexName			= @crtIndexName,
																						@indexID			= @crtIndexID,
																						@partitionNumber	= @PartitionNumber,
																						@sqlScriptOnline	= @sqlScriptOnline OUT,
																						@flgOptions			= @flgOptions,
																						@executionLevel		= @nestedExecutionLevel,
																						@debugMode			= @DebugMode
									end

								---------------------------------------------------------------------------------------------
								--primary / unique index options
								-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
								IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
									AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) 
									AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
									begin
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @SQLServerName	= @SQLServerName
																					, @DBName			= @DBName
																					, @TableSchema		= @crtTableSchema
																					, @TableName		= @crtTableName
																					, @ConstraintName	= '%'
																					, @flgAction		= 0		-- Disable Constraints
																					, @flgOptions		= 1		-- Use tables that have foreign key constraints that reffers current table (default)
																					, @executionLevel	= @nestedExecutionLevel
																					, @DebugMode		= @DebugMode
									end

								---------------------------------------------------------------------------------------------
								--clustered/primary key index options
								IF @crtIndexType = 1
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										IF @flgOptions & 4 = 4
											begin
												--get all enabled non-clustered/xml/spatial indexes for current table
												SET @queryToRun = N''
												SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
																			FROM [' + @DBName + '].[sys].[indexes]				si
																			INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																			INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																			LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
																			WHERE	so.[name] = ''' + @crtTableName + '''
																					AND sch.[name] = ''' + @crtTableSchema + ''' 
																					AND si.[type] in (2,3,4)
																					AND si.[is_disabled] = 0'
												SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
												IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

												INSERT INTO @DependentIndexes ([index_name], [is_primary_xml])
													EXEC (@queryToRun)
											end

										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
											begin
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT [index_name]
																				FROM @DependentIndexes
																				ORDER BY [is_primary_xml]
												OPEN crsNonClusteredIndexes
												FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
												WHILE @@FETCH_STATUS=0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																								, @DBName			= @DBName
																								, @TableSchema		= @crtTableSchema
																								, @TableName		= @crtTableName
																								, @IndexName		= @tmpIndexName
																								, @IndexID			= NULL
																								, @PartitionNumber	= DEFAULT
																								, @flgAction		= 4				--disable
																								, @flgOptions		= @flgOptions
																								, @executionLevel	= @nestedExecutionLevel
																								, @affectedDependentObjects = @affectedDependentObjects OUT
																								, @DebugMode		= @DebugMode										

														FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
													end
												CLOSE crsNonClusteredIndexes
												DEALLOCATE crsNonClusteredIndexes
											end
									end
								ELSE
									---------------------------------------------------------------------------------------------
									--xml primary key index options
									IF @crtIndexType = 3 AND @crtIndexIsPrimaryXML=1
										begin
											--4  - Rebuild all dependent indexes when rebuild primary indexes
											IF @flgOptions & 4 = 4
												begin
													--get all enabled secondary xml indexes for current table
													SET @queryToRun = N''
													SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				FROM [' + @DBName + '].[sys].[indexes]				si
																				INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																				INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																				INNER JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id]
																				WHERE	so.[name] = ''' + @crtTableName + '''
																						AND sch.[name] = ''' + @crtTableSchema + ''' 
																						AND si.[type] = 3
																						AND xi.[using_xml_index_id] = ''' + CAST(@crtIndexID AS [sysname]) + '''
																						AND si.[is_disabled] = 0'
													SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
													IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

													INSERT INTO @DependentIndexes ([index_name])
														EXEC (@queryToRun)
												end

											--8  - Disable non-clustered index before rebuild (save space)
											--won't disable the index when performing online rebuild
											IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
												begin
													DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																					SELECT [index_name]
																					FROM @DependentIndexes
													OPEN crsNonClusteredIndexes
													FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
													WHILE @@FETCH_STATUS=0
														begin
															SET @nestedExecutionLevel = @executionLevel + 2
															EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																									, @DBName			= @DBName
																									, @TableSchema		= @crtTableSchema
																									, @TableName		= @crtTableName
																									, @IndexName		= @tmpIndexName
																									, @IndexID			= NULL
																									, @PartitionNumber	= DEFAULT
																									, @flgAction		= 4				--disable
																									, @flgOptions		= @flgOptions
																									, @executionLevel	= @nestedExecutionLevel
																									, @affectedDependentObjects = @affectedDependentObjects OUT
																									, @DebugMode		= @DebugMode										

															FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
														end
													CLOSE crsNonClusteredIndexes
													DEALLOCATE crsNonClusteredIndexes
												end
										end
									ELSE
										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild										
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
											begin
												SET @nestedExecutionLevel = @executionLevel + 2
												EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																						, @DBName			= @DBName
																						, @TableSchema		= @crtTableSchema
																						, @TableName		= @crtTableName
																						, @IndexName		= @crtIndexName
																						, @IndexID			= NULL
																						, @PartitionNumber	= @PartitionNumber
																						, @flgAction		= 4				--disable
																						, @flgOptions		= @flgOptions
																						, @executionLevel	= @nestedExecutionLevel
																						, @affectedDependentObjects = @affectedDependentObjects OUT
																						, @DebugMode		= @DebugMode										
										end

								---------------------------------------------------------------------------------------------
								/* FIX: Data corruption occurs in clustered index when you run online index rebuild in SQL Server 2012 or SQL Server 2014 https://support.microsoft.com/en-us/kb/2969896 */
								IF (@sqlScriptOnline = N'ONLINE = ON')
									begin
										--get destination server running version/edition
										DECLARE		@serverEdition					[sysname],
													@serverVersionStr				[sysname],
													@serverVersionNum				[numeric](9,6)

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
																				@serverEdition			= @serverEdition OUT,
																				@serverVersionStr		= @serverVersionStr OUT,
																				@serverVersionNum		= @serverVersionNum OUT,
																				@executionLevel			= @nestedExecutionLevel,
																				@debugMode				= @DebugMode
										
										IF     (@serverVersionNum >= 11.02100 AND @serverVersionNum < 11.03449) /* SQL Server 2012 RTM till SQL Server 2012 SP1 CU 11*/
											OR (@serverVersionNum >= 11.05058 AND @serverVersionNum < 11.05532) /* SQL Server 2012 SP2 till SQL Server 2012 SP2 CU 1*/
											OR (@serverVersionNum >= 12.02000 AND @serverVersionNum < 12.02370) /* SQL Server 2014 RTM CU 2*/
											begin
												SET @MaxDOP=1
											end
									end

								---------------------------------------------------------------------------------------------
								--generate rebuild index script
								SET @queryToRun = N''

								SET @queryToRun = @queryToRun + N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REBUILD'
					
								--rebuild options
								SET @queryToRun = @queryToRun + N' WITH (SORT_IN_TEMPDB = ON' + CASE WHEN ISNULL(@MaxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@MaxDOP AS [nvarchar]) ELSE N'' END + 
																						CASE WHEN ISNULL(@sqlScriptOnline, N'')<>N'' THEN N', ' + @sqlScriptOnline ELSE N'' END + 
																						CASE WHEN ISNULL(@FillFactor, 0) <> 0 THEN N', FILLFACTOR = ' + CAST(@FillFactor AS [nvarchar]) ELSE N'' END +
																N')'

								IF @PartitionNumber>1
									SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])

								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON'))
									begin
										SET @strMessage=N'performing index rebuild'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
									end

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - rebuilding index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								
								IF @errorCode=0
									EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-made-disable',
																			@flgOperation		= 2,
																			@server_name		= @SQLServerName,
																			@database_name		= @DBName,
																			@schema_name		= @crtTableSchema,
																			@object_name		= @crtTableName,
																			@child_object_name	= @crtIndexName

								---------------------------------------------------------------------------------------------
								--rebuild dependent indexes
								--clustered / xml primary key index options
								IF (@crtIndexType = 1) OR (@crtIndexType = 3 AND @crtIndexIsPrimaryXML=1)
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										--will rebuild only indexes disabled by this tool
										IF (@flgOptions & 4 = 4)
											begin											
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT DISTINCT di.[index_name], di.[is_primary_xml]
																				FROM @DependentIndexes di
																				LEFT JOIN [dbo].[statsMaintenancePlanInternals] smpi ON	smpi.[name]=N'index-made-disable'
																																		AND smpi.[server_name]=@SQLServerName
																																		AND smpi.[database_name]=@DBName
																																		AND smpi.[schema_name]=@crtTableSchema
																																		AND smpi.[object_name]=@crtTableName
																																		AND smpi.[child_object_name]=di.[index_name]
																				WHERE	(
																							/* index was disabled (option selected) and marked as disabled */
																							(@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name]=N'index-made-disable'
																						)
																						OR
																						(
																							/* index was not disabled (option selected) */
																							NOT (@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name] IS NULL
																						)
																				ORDER BY di.[is_primary_xml] DESC
												OPEN crsNonClusteredIndexes
												FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
												WHILE @@FETCH_STATUS=0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																								, @DBName			= @DBName
																								, @TableSchema		= @crtTableSchema
																								, @TableName		= @crtTableName
																								, @IndexName		= @tmpIndexName
																								, @IndexID			= NULL
																								, @PartitionNumber	= DEFAULT
																								, @flgAction		= 1		--rebuild
																								, @flgOptions		= @flgOptions
																								, @executionLevel	= @nestedExecutionLevel
																								, @affectedDependentObjects = @affectedDependentObjects OUT
																								, @DebugMode		= @DebugMode										

														FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
													end
												CLOSE crsNonClusteredIndexes
												DEALLOCATE crsNonClusteredIndexes
											end
									end		

								---------------------------------------------------------------------------------------------
								-- must enable previous disabled constraints
								-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
								IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
									AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) 
									AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
									begin
										SET @flgInheritOptions = 1								-- Use tables that have foreign key constraints that reffers current table (default)

										--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
										IF @flgOptions & 64 = 64
											SET @flgInheritOptions = @flgInheritOptions + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @SQLServerName	= @SQLServerName
																					, @DBName			= @DBName
																					, @TableSchema		= @crtTableSchema
																					, @TableName		= @crtTableName
																					, @ConstraintName	= '%'
																					, @flgAction		= 1		-- Enable Constraints
																					, @flgOptions		= @flgInheritOptions
																					, @executionLevel	= @nestedExecutionLevel
																					, @DebugMode		= @DebugMode
									end
							end

						---------------------------------------------------------------------------------------------
						-- 2  - Reorganize indexes
						---------------------------------------------------------------------------------------------
						-- avoid messages like:	The index [...] on table [..] cannot be reorganized because page level locking is disabled.		
						IF @flgAction = 2
							IF @crtIndexAllowPageLocks=1
								begin
									SET @queryToRun = N''
									SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
									SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REORGANIZE'
				
									--  1  - Compact large objects (LOB) (default)
									IF @flgOptions & 1 = 1
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = ON) '
									ELSE
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = OFF) '
				
									IF @PartitionNumber>1
										SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])
									IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


									SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
									SET @childObjectName = QUOTENAME(@crtIndexName)
									SET @nestedExecutionLevel = @executionLevel + 1

									EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																					@dbName			= @DBName,
																					@objectName		= @objectName,
																					@childObjectName= @childObjectName,
																					@module			= 'dbo.usp_mpAlterTableIndexes',
																					@eventName		= 'database maintenance - reorganize index',
																					@queryToRun  	= @queryToRun,
																					@flgOptions		= @flgOptions,
																					@executionLevel	= @nestedExecutionLevel,
																					@debugMode		= @DebugMode
								end
							ELSE
								begin
									SET @strMessage=N'--	index cannot be REORGANIZE because ALLOW_PAGE_LOCKS is set to OFF. Skipping...'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
								end

						---------------------------------------------------------------------------------------------
						-- 4  - Disable indexes 
						---------------------------------------------------------------------------------------------
						IF @flgAction = 4
							begin
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] DISABLE'
				
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - disable index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								/* 4 disable index -> insert action 1 */
								IF @errorCode=0
									EXEC [dbo].[usp_mpMarkInternalAction]	@actionName		= N'index-made-disable',
																			@flgOperation	= 1,
																			@server_name		= @SQLServerName,
																			@database_name		= @DBName,
																			@schema_name		= @crtTableSchema,
																			@object_name		= @crtTableName,
																			@child_object_name	= @crtIndexName
							end

						FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
					end
				CLOSE crsTableToAlterIndexes
				DEALLOCATE crsTableToAlterIndexes
			end

		SET @affectedDependentObjects=N''
		SELECT @affectedDependentObjects = @affectedDependentObjects + N'[' + [index_name] + N'];'
		FROM @DependentIndexes
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
		SET @errorCode = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableRebuildHeap]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableRebuildHeap]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableRebuildHeap]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableRebuildHeap]
		@SQLServerName		[sysname],
		@DBName				[sysname],
		@TableSchema		[sysname],
		@TableName			[sysname],
		@flgActions			[smallint] = 1,
		@flgOptions			[int] = 10264, --8192 + 2048 + 16 + 8
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2015
-- Module			 : Database Maintenance Scripts
-- ============================================================================
-- Change Date: 2015.03.04 / Andrei STEFAN
-- Description: heap tables with disabled unique indexes won't be rebuild
-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed.
--		@flgActions		- 1 - ALTER TABLE REBUILD (2k8+). If lower version is detected or error catched, will run CREATE CLUSTERED INDEX / DROP INDEX
--						- 2 - Rebuild table: copy records to a temp table, delete records from source, insert back records from source, rebuild non-clustered indexes
--		@flgOptions		 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding indexes (default)
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  8192  - when rebuilding heaps, disable/enable table triggers (default)
--					 16384  - for versions below 2008, do heap rebuild using temporary clustered index
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON

DECLARE		@queryToRun					[nvarchar](max),
			@objectName					[nvarchar](512),
			@CopyTableName				[sysname],
			@crtSchemaName				[sysname], 
			@crtTableName				[sysname], 
			@crtRecordCount				[int],
			@flgCopyMade				[bit],
			@flgErrorsOccured			[bit], 
			@nestExecutionLevel			[tinyint],
			@guid						[nvarchar](40),
			@affectedDependentObjects	[nvarchar](max),
			@flgOptionsNested			[int]


DECLARE		@flgRaiseErrorAndStop		[bit]
		  , @errorCode					[int]
		  

-----------------------------------------------------------------------------------------
DECLARE @tableGetRowCount TABLE	
		(
			[record_count]			[bigint]	NULL
		)

IF object_id('tempdb..#heapTableList') IS NOT NULL 
	DROP TABLE #heapTableList

CREATE TABLE #heapTableList		(
									[schema_name]			[sysname]	NULL,
									[table_name]			[sysname]	NULL,
									[record_count]			[bigint]	NULL
								)


SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 1

		---------------------------------------------------------------------------------------------
		--get configuration values
		---------------------------------------------------------------------------------------------
		DECLARE @queryLockTimeOut [int]
		SELECT	@queryLockTimeOut=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name]='Default lock timeout (ms)'
				AND [module] = 'common'
		
		---------------------------------------------------------------------------------------------
		--get destination server running version/edition
		DECLARE		@serverEdition					[sysname],
					@serverVersionStr				[sysname],
					@serverVersionNum				[numeric](9,6),
					@nestedExecutionLevel			[tinyint]

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
												@serverEdition			= @serverEdition OUT,
												@serverVersionStr		= @serverVersionStr OUT,
												@serverVersionNum		= @serverVersionNum OUT,
												@executionLevel			= @nestedExecutionLevel,
												@debugMode				= @DebugMode

		---------------------------------------------------------------------------------------------
		--get current index/heap properties, filtering only the ones not empty
		--heap tables with disabled unique indexes will be excluded: rebuild means also index rebuild, and unique indexes may enable unwanted constraints
		SET @TableName = REPLACE(@TableName, '''', '''''')
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'	SELECT    sch.[name] AS [schema_name]
													, so.[name]  AS [table_name]
													, rc.[record_count]
											FROM [' + @DBName + '].[sys].[objects] so WITH (READPAST)
											INNER JOIN [' + @DBName + '].[sys].[schemas] sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id] 
											INNER JOIN [' + @DBName + '].[sys].[indexes] si WITH (READPAST) ON si.[object_id] = so.[object_id] 
											INNER  JOIN 
													(
														SELECT ps.object_id,
																SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [record_count]
														FROM [' + @DBName + '].[sys].[dm_db_partition_stats] ps WITH (READPAST)
														GROUP BY ps.object_id		
													)rc ON rc.[object_id] = so.[object_id] 
											WHERE   so.[name] LIKE ''' + @TableName + '''
												AND sch.[name] LIKE ''' + @TableSchema + '''
												AND so.[is_ms_shipped] = 0
												AND si.[index_id] = 0
												AND rc.[record_count]<>0
												AND NOT EXISTS(
																SELECT *
																FROM [' + @DBName + '].sys.indexes si_unq
																WHERE si_unq.[object_id] = so.[object_id] 
																		AND si_unq.[is_disabled]=1
																		AND si_unq.[is_unique]=1
															  )'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #heapTableList
		INSERT INTO #heapTableList ([schema_name], [table_name], [record_count])
			EXEC (@queryToRun)


		---------------------------------------------------------------------------------------------
		DECLARE crsTableListToRebuild CURSOR LOCAL READ_ONLY FOR	SELECT [schema_name], [table_name], [record_count] 
																	FROM #heapTableList
																	ORDER BY [schema_name], [table_name]
 		OPEN crsTableListToRebuild
		FETCH NEXT FROM crsTableListToRebuild INTO @crtSchemaName, @crtTableName, @crtRecordCount
		WHILE @@FETCH_STATUS=0
			begin
				SET @objectName = '[' + @crtSchemaName + '].[' + @crtTableName + ']'
				SET @queryToRun=N'Rebuilding heap ON ' + @objectName
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			
				SET @flgErrorsOccured=0
				
				IF @flgActions=1
					begin
						IF @serverVersionNum >= 10
							begin
								SET @queryToRun= 'Running ALTER TABLE REBUILD...'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; ';
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + @objectName + ''') IS NOT NULL ALTER TABLE ' + @objectName + N' REBUILD'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								
							end

						IF (@flgOptions & 16384 = 16384) AND (@serverVersionNum < 10 OR @flgErrorsOccured=1)
							begin
								------------------------------------------------------------------------------------------------------------------------
								--disable table non-clustered indexes
								IF @flgOptions & 8 = 8
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_mpAlterTableIndexes]	@SQLServerName				= @SQLServerName,
																				@DBName						= @DBName,
																				@TableSchema				= @crtSchemaName,
																				@TableName					= @crtTableName,
																				@IndexName					= '%',
																				@IndexID					= NULL,
																				@PartitionNumber			= 1,
																				@flgAction					= 4,
																				@flgOptions					= DEFAULT,
																				@MaxDOP						= 1,
																				@executionLevel				= @nestExecutionLevel,
																				@affectedDependentObjects	= @affectedDependentObjects OUT,
																				@debugMode					= @DebugMode
									end

								------------------------------------------------------------------------------------------------------------------------
								--disable table constraints
								IF @flgOptions & 16 = 16
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										SET @flgOptionsNested = 3 + (@flgOptions & 2048)
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName		= @SQLServerName ,
																					@DBName				= @DBName,
																					@TableSchema		= @crtSchemaName, 
																					@TableName			= @crtTableName,
																					@ConstraintName		= '%',
																					@flgAction			= 0,
																					@flgOptions			= @flgOptionsNested,
																					@executionLevel		= @nestExecutionLevel,
																					@debugMode			= @DebugMode
									end

								SET @guid = CAST(NEWID() AS [nvarchar](38))

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Add a new temporary column [bigint]'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'ALTER TABLE [' + @DBName + N'].' + @objectName + N' ADD [' + @guid + N'] [bigint] IDENTITY'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Create a temporary clustered index'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N' CREATE CLUSTERED INDEX [PK_' + @guid + N'] ON [' + @DBName + N'].' + @objectName + N' ([' + @guid + N'])'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Drop the temporary clustered index'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'DROP INDEX [PK_' + @guid + N'] ON [' + @DBName + N'].' + @objectName 
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Drop the temporary column'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'ALTER TABLE [' + @DBName + N'].' + @objectName + N' DROP COLUMN [' + @guid + N']'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								---------------------------------------------------------------------------------------------------------
								--rebuild table non-clustered indexes
								IF @flgOptions & 8 = 8
									begin
										SET @nestExecutionLevel = @executionLevel + 1

										EXEC [dbo].[usp_mpAlterTableIndexes]	@SQLServerName				= @SQLServerName,
																				@DBName						= @DBName,
																				@TableSchema				= @crtSchemaName,
																				@TableName					= @crtTableName,
																				@IndexName					= '%',
																				@IndexID					= NULL,
																				@PartitionNumber			= 1,
																				@flgAction					= 1,
																				@flgOptions					= 6165,
																				@MaxDOP						= 1,
																				@executionLevel				= @nestExecutionLevel, 
																				@affectedDependentObjects	= @affectedDependentObjects OUT,
																				@debugMode					= @DebugMode
									end

								---------------------------------------------------------------------------------------------------------
								--enable table constraints
								IF @flgOptions & 16 = 16
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										SET @flgOptionsNested = 3 + (@flgOptions & 2048)
	
										--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
										IF @flgOptions & 64 = 64
											SET @flgOptionsNested = @flgOptionsNested + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

										EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName		= @SQLServerName ,
																					@DBName				= @DBName,
																					@TableSchema		= @crtSchemaName, 
																					@TableName			= @crtTableName,
																					@ConstraintName		= '%',
																					@flgAction			= 1,
																					@flgOptions			= @flgOptionsNested,
																					@executionLevel		= @nestExecutionLevel, 
																					@debugMode			= @DebugMode
									end
							end
					end

				-- 2 - Rebuild table: copy records to a temp table, delete records from source, insert back records from source, rebuild non-clustered indexes
				IF @flgActions=2
					begin
						SET @CopyTableName=@crtTableName + 'RebuildCopy'

						SET @queryToRun= 'Total Rows In Table To Be Exported To Temporary Storage: ' + CAST(@crtRecordCount AS [varchar](20))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @flgCopyMade=0
						--------------------------------------------------------------------------------------------------------
						--dropping copy table, if exists
						--------------------------------------------------------------------------------------------------------
						SET @queryToRun = 'IF EXISTS (	SELECT * 
														FROM [' + @DBName + '].[sys].[objects] so
														INNER JOIN [' + @DBName + '].[sys].[schemas] sch ON so.[schema_id] = sch.[schema_id]
														WHERE	sch.[name] = ''' + @crtSchemaName + ''' 
																AND so.[name] = ''' + @CopyTableName + '''
													) 
											DROP TABLE [' + @DBName + '].[' + @crtSchemaName + '].[' + @CopyTableName + ']'
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																		@dbName			= @DBName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																		@eventName		= 'database maintenance - rebuilding heap',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @DebugMode
				
						--------------------------------------------------------------------------------------------------------
						--create a copy of the source table
						--------------------------------------------------------------------------------------------------------
						SET @queryToRun = 'SELECT * INTO [' + @DBName + '].[' + @crtSchemaName + '].[' + @CopyTableName + '] FROM [' + @DBName + '].' + @objectName 
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																		@dbName			= @DBName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																		@eventName		= 'database maintenance - rebuilding heap',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @DebugMode

						IF @errorCode = 0
							SET @flgCopyMade=1
				
						IF @flgCopyMade=1
							begin
								--------------------------------------------------------------------------------------------------------
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'	SELECT    rc.[record_count]
																	FROM [' + @DBName + '].[sys].[objects] so WITH (READPAST)
																	INNER JOIN [' + @DBName + '].[sys].[schemas] sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id] 
																	INNER JOIN [' + @DBName + '].[sys].[indexes] si WITH (READPAST) ON si.[object_id] = so.[object_id] 
																	INNER  JOIN 
																			(
																				SELECT ps.object_id,
																						SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [record_count]
																				FROM [' + @DBName + '].[sys].[dm_db_partition_stats] ps WITH (READPAST)
																				GROUP BY ps.object_id		
																			)rc ON rc.[object_id] = so.[object_id] 
																	WHERE   so.[name] LIKE ''' + @CopyTableName + '''
																		AND sch.[name] LIKE ''' + @crtSchemaName + '''
																		AND si.[index_id] = 0'
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								DELETE FROM @tableGetRowCount
								INSERT INTO @tableGetRowCount([record_count])
									EXEC (@queryToRun)
							
								SELECT TOP 1 @crtRecordCount=[record_count] FROM @tableGetRowCount
								SET @queryToRun= '--	Total Rows In Temporary Storage Table After Export: ' + CAST(@crtRecordCount AS varchar(20))
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0


								--------------------------------------------------------------------------------------------------------
								--rebuild source table
								SET @nestExecutionLevel=@executionLevel + 2
								EXEC @flgErrorsOccured = [dbo].[usp_mpTableDataSynchronizeInsert]	@sourceServerName		= @SQLServerName,
																									@sourceDB				= @DBName,			
																									@sourceTableSchema		= @crtSchemaName,
																									@sourceTableName		= @CopyTableName,
																									@destinationServerName	= @SQLServerName,
																									@destinationDB			= @DBName,			
																									@destinationTableSchema	= @crtSchemaName,		
																									@destinationTableName	= @crtTableName,		
																									@flgActions				= 3,
																									@flgOptions				= @flgOptions,
																									@allowDataLoss			= 0,
																									@executionLevel			= @nestExecutionLevel,
																									@DebugMode				= @DebugMode
						
								--------------------------------------------------------------------------------------------------------
								--dropping copy table
								--------------------------------------------------------------------------------------------------------
								IF @flgErrorsOccured=0
									begin
										SET @queryToRun = 'IF EXISTS (	SELECT * 
																		FROM [' + @DBName + '].[sys].[objects] so
																		INNER JOIN [' + @DBName + '].[sys].[schemas] sch ON so.[schema_id] = sch.[schema_id]
																		WHERE	sch.[name] = ''' + @crtSchemaName + ''' 
																				AND so.[name] = ''' + @CopyTableName + '''
																	) 
															DROP TABLE [' + @DBName + '].[' + @crtSchemaName + '].[' + @CopyTableName + ']'
										IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																						@dbName			= @DBName,
																						@objectName		= @objectName,
																						@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																						@eventName		= 'database maintenance - rebuilding heap',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @DebugMode
									end
							end
					end

				FETCH NEXT FROM crsTableListToRebuild INTO @crtSchemaName, @crtTableName, @crtRecordCount
			end
		CLOSE crsTableListToRebuild
		DEALLOCATE crsTableListToRebuild
	
		----------------------------------------------------------------------------------
		IF object_id('#tmpRebuildTableList') IS NOT NULL DROP TABLE #tmpRebuildTableList
		IF OBJECT_ID('#heapTableIndexList') IS NOT NULL DROP TABLE #heapTableIndexList
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
	SET @errorCode = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableTriggers]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableTriggers]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableTriggers]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableTriggers]
		@SQLServerName		[sysname],
		@DBName				[sysname],
		@TableSchema		[sysname] = '%', 
		@TableName			[sysname] = '%',
		@TriggerName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2048,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2009
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@flgAction:		 1	- Enable Triggers (default)
--						 0	- Disable Triggers
--		@flgOptions:	 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@objectName				[varchar](512),
			@childObjectName		[sysname],
			@crtTableSchema			[sysname],
			@crtTableName 			[sysname],
			@crtTriggerName			[sysname],
			@errorCode				[int],
			@tmpFlgOptions			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema]	[sysname],
					[table_name]	[sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME FROM [' + @DBName + N'].INFORMATION_SCHEMA.TABLES
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + N''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterTriggers') IS NOT NULL DROP TABLE #tmpTableToAlterTriggers
				CREATE TABLE #tmpTableToAlterTriggers 
							(
								[TriggerName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT	[table_schema], [table_name]
																	FROM	#tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName

				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1  THEN 'Enable'
																ELSE 'Disable'
										END + ' triggers for: [' + @crtTableSchema + N'].[' + @crtTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable triggers, will get only enabled triggers
						--if current action is to enable triggers, will get only disabled triggers
						SET @queryToRun=N'SELECT DISTINCT st.[name]
									FROM [' + @DBName + '].[sys].[triggers] st
									INNER JOIN [' + @DBName + '].[sys].[objects] so ON so.[object_id] = st.[parent_id] 
									INNER JOIN [' + @DBName + '].[sys].[schemas] sch ON sch.[schema_id] = so.[schema_id] 
									WHERE	so.[name]=''' + @crtTableName + '''
											AND sch.[name] = ''' + @crtTableSchema + '''
											AND st.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
											AND st.[is_ms_shipped] = 0
											AND st.[name] LIKE ''' + @TriggerName + ''''
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						TRUNCATE TABLE #tmpTableToAlterTriggers
						INSERT	INTO #tmpTableToAlterTriggers([TriggerName])
								EXEC (@queryToRun)
								
						DECLARE crsTableToAlterTriggers CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TriggerName]
																						FROM #tmpTableToAlterTriggers
																						ORDER BY [TriggerName]
						OPEN crsTableToAlterTriggers
						FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= @crtTriggerName
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun=N'ALTER TABLE [' + @DBName + N'].[' + @crtTableSchema + N'].[' + @crtTableName + '] ' + 
													CASE WHEN @flgAction=1  THEN N'ENABLE'
																			ELSE N'DISABLE'
													END + N' TRIGGER [' + @crtTriggerName + ']'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

								--
								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtTriggerName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableTriggers',
																				@eventName		= 'database maintenance - alter triggers',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
							end
						CLOSE crsTableToAlterTriggers
						DEALLOCATE crsTableToAlterTriggers
											
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
	SET @errorCode = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpGetIndexCreationScript]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpGetIndexCreationScript]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
		@SQLServerName		[sysname]=@@SERVERNAME,
		@DBName				[sysname],
		@TableSchema		[sysname]='dbo',
		@TableName			[sysname],
		@IndexName			[sysname],
		@IndexID			[int],
		@flgOptions			[int] = 4099,
		@sqlIndexCreate		[nvarchar](max) OUTPUT,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 07.01.2010
-- Module     : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed
--		@IndexName		- name of the index to be analyzed
--		@IndexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@flgOptions:	1 - get also indexes that are created by a table constraint (primary or unique key) (default)
--						2 - use drop existing to recreate the index (default)
--					 4096 - use ONLINE=ON, if applicable (default)
--		@DebugMode:		1 - print dynamic SQL statements 
--						0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Output Parameters:
--		@sqlIndexCreate	- sql statement that will create the index
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE @queryToRun			[nvarchar](max),
		@sqlIndexInclude	[nvarchar](max),
		@sqlIndexWithClause [nvarchar](max),
		@sqlScriptOnline	[nvarchar](512),
		@crtIndexName		[sysname],
		@IndexType			[tinyint],
		@FillFactor			[tinyint],
		@IsUniqueConstraint	[int],
		@IsPadded			[int],
		@AllowRowLocks		[int],
		@AllowPageLocks		[int],
		@IgnoreDupKey		[int],
		@KeyOrdinal			[int],
		@IndexColumnID		[int],
		@IsIncludedColumn	[bit],
		@IsDescendingKey	[bit],
		@ColumnName			[sysname],
		@FileGroupName		[sysname],
		@ReturnValue		[int],
		@nestExecutionLevel	[tinyint]

DECLARE @IndexDetails TABLE	(
								[IndexName]			[sysname]	NULL,
								[IndexType]			[tinyint]	NULL,
								[FillFactor]		[tinyint]	NULL,
								[FileGroupName]		[sysname]	NULL,
								[IsUniqueConstraint][bit]		NULL,
								[IsPadded]			[bit]		NULL,
								[AllowRowLocks]		[bit]		NULL,
								[AllowPageLocks]	[bit]		NULL,
								[IgnoreDupKey]		[bit]		NULL
							)

DECLARE @IndexColumnDetails TABLE
							(
								[KeyOrdinal]		[int]		NULL,
								[IndexColumnID]		[int]		NULL,
								[IsIncludedColumn]	[bit]		NULL,
								[IsDescendingKey]	[bit]		NULL,
								[ColumnName]		[sysname]	NULL
							)

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 1

		--get current index properties
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT  idx.[name]
										, idx.[type]
										, idx.[fill_factor]
										, dSp.[name] AS [file_group_name]
										, idx.[is_unique]
										, idx.[is_padded]
										, idx.[allow_row_locks]
										, idx.[allow_page_locks]
										, idx.[ignore_dup_key]
									FROM [' + @DBName + '].[sys].[indexes]				idx
									INNER JOIN [' + @DBName + '].[sys].[objects]		obj ON  idx.[object_id] = obj.[object_id]
									INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON	sch.[schema_id] = obj.[schema_id]
									INNER JOIN [' + @DBName + '].[sys].[data_spaces]	dSp	ON  idx.[data_space_id] = dSp.[data_space_id]
									WHERE	obj.[name] = ''' + @TableName + '''
											AND sch.[name] = ''' + @TableSchema + '''' + 
											CASE	WHEN @IndexName IS NOT NULL 
													THEN ' AND idx.[name] = ''' + @IndexName + ''''
													ELSE ' AND idx.[index_id] = ' + CAST(@IndexID AS [nvarchar])
											END + 
											CASE WHEN @flgOptions & 1 <> 1
												 THEN '	AND NOT EXISTS	(
																			SELECT 1
																			FROM [' + @DBName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																			WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																					AND [CONSTRAINT_CATALOG]=''' + @DBName + '''
																					AND [TABLE_NAME]=''' + @TableName + '''
																					AND [TABLE_SCHEMA] = ''' + @TableSchema + '''
																					AND [CONSTRAINT_NAME]=''' + @IndexName + '''
																		)'
												ELSE ''
											END
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexDetails
		INSERT INTO @IndexDetails ([IndexName], [IndexType], [FillFactor], [FileGroupName], [IsUniqueConstraint], [IsPadded], [AllowRowLocks], [AllowPageLocks], [IgnoreDupKey])
			EXEC (@queryToRun)

		--get index fill factor and file group
		SELECT	  @crtIndexName		= ISNULL(@IndexName, [IndexName])
				, @IndexType		= [IndexType]
				, @FillFactor		= [FillFactor]
				, @FileGroupName	= [FileGroupName]
				, @IsUniqueConstraint = [IsUniqueConstraint]
				, @IsPadded			= [IsPadded]
				, @AllowRowLocks	= [AllowRowLocks]
				, @AllowPageLocks	= [AllowPageLocks]
				, @IgnoreDupKey		= [IgnoreDupKey]
		FROM @IndexDetails
		
		--get current index key columns and include columns and their properties
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT    
										  idxCol.[key_ordinal]
										, idxCol.[index_column_id]
										, idxCol.[is_included_column]
										, idxCol.[is_descending_key]
										, col.[name] AS [column_name]
								FROM [' + @DBName + '].[sys].[indexes] idx
								INNER JOIN [' + @DBName + '].[sys].[index_columns] idxCol ON	idx.[object_id] = idxCol.[object_id]
																								AND idx.[index_id] = idxCol.[index_id]
								INNER JOIN [' + @DBName + '].[sys].[columns]		 col	ON	idxCol.[object_id] = col.[object_id]
																								AND idxCol.[column_id] = col.[column_id]
								INNER JOIN [' + @DBName + '].[sys].[objects]		 obj	ON  idx.[object_id] = obj.[object_id]
								INNER JOIN [' + @DBName + '].[sys].[schemas]		 sch	ON	sch.[schema_id] = obj.[schema_id]
								WHERE	obj.[name] = ''' + @TableName + '''
										AND sch.[name] = ''' + @TableSchema + '''' + 
										CASE	WHEN @IndexName IS NOT NULL 
												THEN ' AND idx.[name] = ''' + @IndexName + ''''
												ELSE ' AND idx.[index_id] = ' + CAST(@IndexID AS [nvarchar])
										END + 
										CASE WHEN @flgOptions & 1 <> 1
											 THEN '	AND NOT EXISTS	(
																		SELECT 1
																		FROM [' + @DBName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																		WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																				AND [CONSTRAINT_CATALOG]=''' + @DBName + '''
																				AND [TABLE_NAME]=''' + @TableName + '''
																				AND [TABLE_SCHEMA]=''' + @TableSchema + '''
																				AND [CONSTRAINT_NAME]=''' + @IndexName + '''
																	)'
											ELSE ''
										END
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexColumnDetails
		INSERT INTO @IndexColumnDetails ([KeyOrdinal], [IndexColumnID], [IsIncludedColumn], [IsDescendingKey], [ColumnName])
			EXEC (@queryToRun)

		SET @sqlIndexCreate=N''
		IF EXISTS (SELECT 1 FROM @IndexColumnDetails)
			begin
				-- check for online operation mode, for reorganize/rebuild
				SET @nestExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																@dbName				= @DBName,
																@tableSchema		= @TableSchema,
																@tableName			= @TableName,
																@indexName			= @IndexName,
																@indexID			= @IndexID,
																@partitionNumber	= 1,
																@sqlScriptOnline	= @sqlScriptOnline OUT,
																@flgOptions			= @flgOptions,
																@executionLevel		= @nestExecutionLevel,
																@debugMode			= @DebugMode

				SET @sqlIndexCreate = @sqlIndexCreate + N'CREATE'
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IsUniqueConstraint=1	
																THEN ' UNIQUE' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IndexType=1	
																THEN ' CLUSTERED' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 ' INDEX [' + @crtIndexName + '] ON [' + @TableSchema + '].[' + @TableName + '] ('
				--index key columns
				DECLARE crsIndexKey CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName], [IsDescendingKey]
																	FROM @IndexColumnDetails
																	WHERE [IsIncludedColumn] = 0
																	ORDER BY [KeyOrdinal]
				OPEN crsIndexKey
				FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexCreate = @sqlIndexCreate + '[' + @ColumnName + ']' + 
												CASE WHEN @IsDescendingKey=1	THEN ' DESC'
																				ELSE '' END + ', '
						FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
					end
				CLOSE  crsIndexKey
				DEALLOCATE crsIndexKey
				IF LEN(@sqlIndexCreate)<>0
					SET @sqlIndexCreate = SUBSTRING(@sqlIndexCreate, 1, LEN(@sqlIndexCreate)-1) + ')'

				--index include columns
				SET @sqlIndexInclude = N''
				DECLARE crsIndexInclude CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName]
																		FROM @IndexColumnDetails
																		WHERE [IsIncludedColumn] = 1
																		ORDER BY [IndexColumnID]
				OPEN crsIndexInclude
				FETCH NEXT FROM crsIndexInclude INTO @ColumnName
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexInclude = @sqlIndexInclude + '[' + @ColumnName + '], '
						FETCH NEXT FROM crsIndexInclude INTO @ColumnName
					end
				CLOSE  crsIndexInclude
				DEALLOCATE crsIndexInclude
				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexInclude = SUBSTRING(@sqlIndexInclude, 1, LEN(@sqlIndexInclude)-1)


				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexCreate = @sqlIndexCreate + N' INCLUDE(' + @sqlIndexInclude + ')'

				--index options
				SET @sqlIndexWithClause = N''
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'PAD_INDEX = ' + CASE WHEN @IsPadded=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_ROW_LOCKS = ' + CASE WHEN @AllowRowLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_PAGE_LOCKS = ' + CASE WHEN @AllowPageLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'IGNORE_DUP_KEY = ' + CASE WHEN @IgnoreDupKey=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + @sqlScriptOnline
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'SORT_IN_TEMPDB = ON'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'STATISTICS_NORECOMPUTE = OFF'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'MAXDOP = 1'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE WHEN @FillFactor<>0	
											 THEN CASE	WHEN LEN(@sqlIndexWithClause)>0 
														THEN ', '
														ELSE ''
												  END + N'FILLFACTOR=' + CAST(@FillFactor AS [nvarchar])
											 ELSE ''
										END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + 
										CASE WHEN @flgOptions & 2 = 2 
											 THEN N'DROP_EXISTING = ON'
											 ELSE ''
										END
				--index storage filegroup
				SET @sqlIndexCreate = @sqlIndexCreate + 
										CASE WHEN LEN(@sqlIndexWithClause)>0
											 THEN N' WITH (' + @sqlIndexWithClause + ')'
											 ELSE ''
										END + N' ON [' + @FileGroupName + ']'
			end
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
	SET @ReturnValue = -1

    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @ReturnValue
GO


RAISERROR('Create procedure: [dbo].[usp_mpDatabaseOptimize]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseOptimize]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseOptimize]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseOptimize]
		@SQLServerName				[sysname]=@@SERVERNAME,
		@DBName						[sysname],
		@TableSchema				[sysname]	=   '%',
		@TableName					[sysname]   =   '%',
		@flgActions					[smallint]	=    27,
		@flgOptions					[int]		= 45697,--32768 + 8192 + 4096 + 512 + 128 + 1
		@DefragIndexThreshold		[smallint]	=     5,
		@RebuildIndexThreshold		[smallint]	=    30,
		@PageThreshold				[int]		=  1000,
		@RebuildIndexPageCountLimit	[int]		= 2147483647,	--16TB/no limit
		@StatsSamplePercent			[smallint]	=   100,
		@StatsAgeDays				[smallint]	=     7,
		@StatsChangePercent			[smallint]	=     1,
		@MaxDOP						[smallint]	=	  1,
		@MaxRunningTimeInMinutes	[smallint]	=     0,
		@executionLevel				[tinyint]	=     0,
		@DebugMode					[bit]		=     0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify % for all tables or a table name to be analyzed
--		@flgActions		 1	- Defragmenting database tables indexes (ALTER INDEX REORGANIZE)				(default)
--							  should be performed daily
--						 2	- Rebuild heavy fragmented indexes (ALTER INDEX REBUILD)						(default)
--							  should be performed daily
--					     4  - Rebuild all indexes (ALTER INDEX REBUILD)
--						 8  - Update statistics for table (UPDATE STATISTICS)								(default)
--							  should be performed daily
--						16  - Rebuild heap tables (SQL versions +2K5 only)									(default)
--		@flgOptions		 1  - Compact large objects (LOB) (default)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					    64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					   128  - Create statistics on index columns only (default). Default behaviour is to not create statistics on all eligible columns
--					   256  - Create statistics using default sample scan. Default behaviour is to create statistics using fullscan mode
--					   512  - update auto-created statistics (default)
--					  1024	- get index statistics using DETAILED analysis (default is to use LIMITED)
--							  for heaps, will always use DETAILED in order to get page density and forwarded records information
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--					  8192  - when rebuilding heaps, disable/enable table triggers (default)
--					 16384  - for versions below 2008, do heap rebuild using temporary clustered index
--					 32768  - analyze only tables with at least @PageThreshold pages reserved (+2k5 only)
--					 65536  - cleanup of ghost records (sp_clean_db_free_space)
--							- this may be forced by setting to true property 'Force cleanup of ghost records'

--		@DefragIndexThreshold		- min value for fragmentation level when to start reorganize it
--		@@RebuildIndexThreshold		- min value for fragmentation level when to start rebuild it
--		@PageThreshold				- the minimum number of pages for an index to be reorganized/rebuild
--		@RebuildIndexPageCountLimit	- the maximum number of page for an index to be rebuild. if index has more pages than @RebuildIndexPageCountLimit, it will be reorganized
--		@StatsSamplePercent			- value for sample percent when update statistics. if 100 is present, then fullscan will be used
--		@StatsAgeDays				- when statistics were last updated (stats ages); don't update statistics more recent then @StatsAgeDays days
--		@StatsChangePercent			- for more recent statistics, if percent of changes is greater of equal, perform update
--		@MaxDOP						- when applicable, use this MAXDOP value (ex. index rebuild)
--		@MaxRunningTimeInMinutes	- the number of minutes the optimization job will run. after time exceeds, it will exist. 0 or null means no limit
--		@DebugMode					- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    					[nvarchar](4000),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@childObjectName				[sysname],
			@IndexName						[sysname],
			@IndexTypeDesc					[sysname],
			@IndexType						[tinyint],
			@IndexFillFactor				[tinyint],
			@DatabaseID						[int], 
			@IndexID						[int],
			@ObjectID						[int],
			@CurrentFragmentation			[numeric] (6,2),
			@CurentPageDensityDeviation		[numeric] (6,2),
			@CurrentPageCount				[bigint],
			@CurrentForwardedRecordsPercent	[numeric] (6,2),
			@errorCode						[int],
			@ClusteredRebuildNonClustered	[bit],
			@flgInheritOptions				[int],
			@statsCount						[int], 
			@nestExecutionLevel				[tinyint],
			@analyzeIndexType				[nvarchar](32),
			@eventData						[varchar](8000),
			@affectedDependentObjects		[nvarchar](4000),
			@indexIsRebuilt					[bit],
			@stopTimeLimit					[datetime]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--determine when to stop current optimization task, based on @MaxRunningTimeInMinutes value
---------------------------------------------------------------------------------------------
IF ISNULL(@MaxRunningTimeInMinutes, 0)=0
	SET @stopTimeLimit = CONVERT([datetime], '9999-12-31 23:23:59', 120)
ELSE
	SET @stopTimeLimit = DATEADD(minute, @MaxRunningTimeInMinutes, GETDATE())


---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

-----------------------------------------------------------------------------------------
--get configuration values: Force cleanup of ghost records
---------------------------------------------------------------------------------------------
DECLARE   @forceCleanupGhostRecords [nvarchar](128)
		, @thresholdGhostRecords	[bigint]

SELECT	@forceCleanupGhostRecords=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Force cleanup of ghost records'
		AND [module] = 'maintenance-plan'

SET @forceCleanupGhostRecords = LOWER(ISNULL(@forceCleanupGhostRecords, 'false'))

--run index statistics using DETAILED option
IF LOWER(@forceCleanupGhostRecords)='true' AND @flgOptions & 1024 = 0
	SET @flgOptions = @flgOptions + 1024

--enable local cleanup of ghost records option
IF LOWER(@forceCleanupGhostRecords)='true' AND @flgOptions & 65536 = 0
	SET @flgOptions = @flgOptions + 65536

IF LOWER(@forceCleanupGhostRecords)='true' OR @flgOptions & 65536 = 65536
	begin
		SELECT	@thresholdGhostRecords=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name]='Ghost records cleanup threshold'
				AND [module] = 'maintenance-plan'
	end

SET @thresholdGhostRecords = ISNULL(@thresholdGhostRecords, 0)

---------------------------------------------------------------------------------------------
--get SQL Server running major version and database compatibility level
---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @DebugMode
---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
DECLARE @compatibilityLevel [tinyint]
IF object_id('tempdb..#databaseCompatibility') IS NOT NULL 
	DROP TABLE #databaseCompatibility

CREATE TABLE #databaseCompatibility
	(
		[compatibility_level]		[tinyint]
	)


SET @queryToRun = N''
IF @serverVersionNum >= 9
	SET @queryToRun = @queryToRun + N'SELECT [compatibility_level] FROM sys.databases WHERE [name] = ''' + @DBName + N''''
ELSE
	SET @queryToRun = @queryToRun + N'SELECT [cmptlevel] FROM master.dbo.sysdatabases WHERE [name] = ''' + @DBName + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC (@queryToRun)

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility

IF @serverVersionNum >= 9 AND @compatibilityLevel<=80
	SET @serverVersionNum = 8

---------------------------------------------------------------------------------------------

SET @errorCode				 = 0
SET @CurrentTableSchema		 = @TableSchema

IF ISNULL(@DefragIndexThreshold, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF ISNULL(@RebuildIndexThreshold, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Threshold value for rebuilding indexes should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF ISNULL(@StatsSamplePercent, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Sample percent value for update statistics sample should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF @DefragIndexThreshold > @RebuildIndexThreshold
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be smalller or equal to threshold value for rebuilding indexes.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end


---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
CREATE TABLE #CurrentIndexFragmentationStats 
		(	
			[ObjectName] 					[varchar] (255),
			[ObjectId] 						[int],
			[IndexName] 					[varchar] (255),
			[IndexId] 						[int],
			[Level] 						[int],
			[Pages]		 					[int],
			[Rows] 							[bigint],
			[MinimumRecordSize]				[int],
			[MaximumRecordSize]				[int],
			[AverageRecordSize] 			[int],
			[ForwardedRecords] 				[int],
			[Extents] 						[int],
			[ExtentSwitches] 				[int],
			[AverageFreeBytes] 				[int],
			[AveragePageDensity] 			[decimal](38,2),
			[ScanDensity] 					[decimal](38,2),
			[BestCount] 					[int],
			[ActualCount] 					[int],
			[LogicalFragmentation] 			[decimal](38,2),
			[ExtentFragmentation] 			[decimal](38,2),
			[ghost_record_count]			[bigint]		NULL
		)	
			
CREATE INDEX IX_CurrentIndexFragmentationStats ON #CurrentIndexFragmentationStats([ObjectId], [IndexId])


---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 
	DROP TABLE #databaseObjectsWithIndexList

CREATE TABLE #databaseObjectsWithIndexList(
											[database_id]					[int],
											[object_id]						[int],
											[table_schema]					[sysname],
											[table_name]					[sysname],
											[index_id]						[int],
											[index_name]					[sysname]	NULL,													
											[index_type]					[tinyint],
											[fill_factor]					[tinyint]	NULL,
											[is_rebuilt]					[bit]		NOT NULL DEFAULT (0),
											[page_count]					[bigint]	NULL,
											[avg_fragmentation_in_percent]	[decimal](38,2)	NULL,
											[ghost_record_count]			[bigint]	NULL,
											[forwarded_records_percentage]	[decimal](38,2)	NULL,
											[page_density_deviation]		[decimal](38,2)	NULL
											)
CREATE INDEX IX_databaseObjectsWithIndexList_TableName ON #databaseObjectsWithIndexList([table_schema], [table_name], [index_id], [avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])
CREATE INDEX IX_databaseObjectsWithIndexList_LogicalDefrag ON #databaseObjectsWithIndexList([avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseObjectsWithStatisticsList') IS NOT NULL 
	DROP TABLE #databaseObjectsWithStatisticsList

CREATE TABLE #databaseObjectsWithStatisticsList(
												[database_id]			[int],
												[object_id]				[int],
												[table_schema]			[sysname],
												[table_name]			[sysname],
												[stats_id]				[int],
												[stats_name]			[sysname],													
												[auto_created]			[bit],
												[rows]					[bigint]		NULL,
												[modification_counter]	[bigint]		NULL,
												[last_updated]			[datetime]		NULL,
												[percent_changes]		[decimal](38,2)	NULL
												)


---------------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

--------------------------------------------------------------------------------------------------
--16 - get current heap tables list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'0'

		SET @queryToRun=N'Create list of heap tables to be analyzed...' + @DBName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		SET @queryToRun = @queryToRun + 
							N'SELECT DISTINCT 
										DB_ID(''' + @DBName + ''') AS [database_id]
									, si.[object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[index_id]
									, si.[name] AS [index_name]
									, si.[type] AS [index_type]
									, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
							FROM [' + @DBName + '].[sys].[indexes]				si
							INNER JOIN [' + @DBName + '].[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
							INNER JOIN [' + @DBName + '].[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
							CASE WHEN @flgOptions & 32768 = 32768 
								THEN N'
							INNER JOIN
									(
											SELECT   [object_id]
												, SUM([reserved_page_count]) as [reserved_page_count]
											FROM [' + @DBName + '].sys.dm_db_partition_stats
											GROUP BY [object_id]
											HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
									) ps ON ps.[object_id] = ob.[object_id]'
								ELSE N''
								END + N'
							WHERE	ob.[name] LIKE ''' + @TableName + '''
									AND sc.[name] LIKE ''' + @TableSchema + '''
									AND si.[type] IN (' + @analyzeIndexType + N')
									AND ob.[type] IN (''U'', ''V'')'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor])
				EXEC (@queryToRun)
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))

			
--------------------------------------------------------------------------------------------------
--1/2	- Analyzing heap tables fragmentation
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Analyzing heap fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun='[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + CASE WHEN @IndexName IS NOT NULL THEN N' - [' + @IndexName + ']' ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun=N'SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
											, ips.[object_id]
											, si.[name] as index_name
											, ips.[index_id]
											, ips.[avg_fragmentation_in_percent]
											, ips.[page_count]
											, ips.[record_count]
											, ips.[forwarded_record_count]
											, ips.[avg_record_size_in_bytes]
											, ips.[avg_page_space_used_in_percent]
											, ips.[ghost_record_count]
									FROM [' + @DBName + '].sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
													'DETAILED'
											+ ''') ips
									INNER JOIN [' + @DBName + '].sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
									WHERE	si.[type] IN (' + @analyzeIndexType + N')'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
				IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
				INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count])  
						EXEC (@queryToRun)

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[LogicalFragmentation]
				, doil.[page_count] = cifs.[Pages]
				, doil.[ghost_record_count] = cifs.[ghost_record_count]
				, doil.[forwarded_records_percentage] = CASE WHEN ISNULL(cifs.[Rows], 0) > 0 
															 THEN (CAST(cifs.[ForwardedRecords] AS decimal(29,2)) / CAST(cifs.[Rows]  AS decimal(29,2))) * 100
															 ELSE 0
														END
				, doil.[page_density_deviation] =  ABS(CASE WHEN ISNULL(cifs.[AverageRecordSize], 0) > 0
															THEN ((FLOOR(8060. / ISNULL(cifs.[AverageRecordSize], 0)) * ISNULL(cifs.[AverageRecordSize], 0)) / 8060.) * doil.[fill_factor] - cifs.[AveragePageDensity]
															ELSE 0
													   END)
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #CurrentIndexFragmentationStats cifs ON cifs.[ObjectId] = doil.[object_id] AND cifs.[IndexId] = doil.[index_id]
	end


--------------------------------------------------------------------------------------------------
-- 16	- Rebuild heap tables (SQL versions +2K5 only)
-- implemented an algoritm based on Tibor Karaszi's one: http://sqlblog.com/blogs/tibor_karaszi/archive/2014/03/06/how-often-do-you-rebuild-your-heaps.aspx
-- rebuilding heaps also rebuild its non-clustered indexes. do heap maintenance before index maintenance
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database heap tables...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[forwarded_records_percentage]
		   									FROM	#databaseObjectsWithIndexList doil
											WHERE	(    doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
													  OR doil.[forwarded_records_percentage] >= @DefragIndexThreshold
													  OR doil.[page_density_deviation] >= @RebuildIndexThreshold
													)
													AND doil.[index_type] IN (0)
											ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   		SET @queryToRun=N'Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density deviation = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				--------------------------------------------------------------------------------------------------
				--log heap fragmentation information
				SET @eventData='<heap-fragmentation><detail>' + 
									'<database_name>' + @DBName + '</database_name>' + 
									'<object_name>' + @objectName + '</object_name>'+ 
									'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
									'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
									'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
									'<forwarded_records_percentage>' + CAST(@CurrentForwardedRecordsPercent AS [varchar](32)) + '</forwarded_records_percentage>' + 
								'</detail></heap-fragmentation>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
													@dbName			= @DBName,
													@objectName		= @objectName,
													@module			= 'dbo.usp_mpDatabaseOptimize',
													@eventName		= 'database maintenance - rebuilding heap',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				--------------------------------------------------------------------------------------------------
				SET @nestExecutionLevel = @executionLevel + 3
				EXEC [dbo].[usp_mpAlterTableRebuildHeap]	@SQLServerName		= @SQLServerName,
															@DBName				= @DBName,
															@TableSchema		= @CurrentTableSchema,
															@TableName			= @CurrentTableName,
															@flgActions			= 1,
															@flgOptions			= @flgOptions,
															@executionLevel		= @nestExecutionLevel,
															@DebugMode			= @DebugMode

				--mark heap as being rebuilt
				UPDATE doil
					SET [is_rebuilt]=1
				FROM	#databaseObjectsWithIndexList doil 
	   			WHERE	doil.[table_name] = @CurrentTableName
	   					AND doil.[table_schema] = @CurrentTableSchema
						AND doil.[index_type] = 0
				
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4 - get current index list: clustered, non-clustered, xml, spatial
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'1,2,3,4'		

		SET @queryToRun=N'Create list of indexes to be analyzed...' + @DBName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF @serverVersionNum >= 9
			SET @queryToRun = @queryToRun + 
								N'SELECT DISTINCT 
										  DB_ID(''' + @DBName + ''') AS [database_id]
										, si.[object_id]
										, sc.[name] AS [table_schema]
										, ob.[name] AS [table_name]
										, si.[index_id]
										, si.[name] AS [index_name]
										, si.[type] AS [index_type]
										, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
								FROM [' + @DBName + '].[sys].[indexes]				si
								INNER JOIN [' + @DBName + '].[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
								INNER JOIN [' + @DBName + '].[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
								CASE WHEN @flgOptions & 32768 = 32768 
									THEN N'
								INNER JOIN
										(
											 SELECT   [object_id]
													, SUM([reserved_page_count]) as [reserved_page_count]
											 FROM [' + @DBName + '].sys.dm_db_partition_stats
											 GROUP BY [object_id]
											 HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
										) ps ON ps.[object_id] = ob.[object_id]'
									ELSE N''
									END + N'
								WHERE	ob.[name] LIKE ''' + @TableName + '''
										AND sc.[name] LIKE ''' + @TableSchema + '''
										AND si.[type] IN (' + @analyzeIndexType + N')
										AND si.[is_disabled]=0
										AND ob.[type] IN (''U'', ''V'')'
		ELSE
			SET @queryToRun = @queryToRun + 
								N'SELECT DISTINCT 
									  DB_ID(''' + @DBName + ''') AS [database_id]
									, si.[id] AS [object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[indid] AS [index_id]
									, si.[name] AS [index_name]
									, CASE WHEN si.[indid]=1 THEN 1 ELSE 2 END AS [index_type]
									, CASE WHEN ISNULL(si.[OrigFillFactor], 0) = 0 THEN 100 ELSE si.[OrigFillFactor] END AS [fill_factor]
								FROM [' + @DBName + ']..sysindexes si
								INNER JOIN [' + @DBName + ']..sysobjects ob	ON ob.[id] = si.[id]
								INNER JOIN [' + @DBName + ']..sysusers sc	ON sc.[uid] = ob.[uid]
								WHERE	ob.[name] LIKE ''' + @TableName + '''
										AND sc.[name] LIKE ''' + @TableSchema + '''
										AND si.[status] & 64 = 0 
										AND si.[status] & 8388608 = 0 
										AND si.[status] & 16777216 = 0 
										AND si.[indid] > 0
										AND ob.[xtype] IN (''U'', ''V'')'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor])
				EXEC (@queryToRun)
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))



--------------------------------------------------------------------------------------------------
--8	- get current statistics list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Create list of statistics to be analyzed...' + @DBName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF @serverVersionNum >= 9 
			begin
				IF (@serverVersionNum >= 10.504000 AND @serverVersionNum < 11) OR @serverVersionNum >= 11.03000
					/* starting with SQL Server 2008 R2 SP2 / SQL Server 2012 SP1 */
					SET @queryToRun = @queryToRun + 
										N'USE [' + @DBName + ']; SELECT DISTINCT 
												  DB_ID(''' + @DBName + ''') AS [database_id]
												, ss.[object_id]
												, sc.[name] AS [table_schema]
												, ob.[name] AS [table_name]
												, ss.[stats_id]
												, ss.[name] AS [stats_name]
												, ss.[auto_created]
												, sp.[last_updated]
												, sp.[rows]
												, ABS(sp.[modification_counter]) AS [modification_counter]
												, (ABS(sp.[modification_counter]) * 100. / sp.[rows]) AS [percent_changes]
										FROM [' + @DBName + '].sys.stats ss
										INNER JOIN [' + @DBName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @DBName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]' +
										CASE WHEN @flgOptions & 32768 = 32768 
											THEN N'
										INNER JOIN
												(
													 SELECT   [object_id]
															, SUM([reserved_page_count]) as [reserved_page_count]
													 FROM [' + @DBName + '].sys.dm_db_partition_stats
													 GROUP BY [object_id]
													 HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
												) ps ON ps.[object_id] = ob.[object_id]'
											ELSE N''
											END + N'
										CROSS APPLY [' + @DBName + '].sys.dm_db_stats_properties(ss.object_id, ss.stats_id) AS sp
										WHERE	ob.[name] LIKE ''' + @TableName + '''
												AND sc.[name] LIKE ''' + @TableSchema + '''
												AND sp.[rows] > 0
												AND (    (    DATEDIFF(dd, sp.[last_updated], GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0
														 )
													 OR  
														 ( 
															  DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0 
														  AND (ABS(sp.[modification_counter]) * 100. / sp.[rows]) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
														 )
													)'
				ELSE
					/* SQL Server 2005 up to SQL Server 2008 R2 SP 2*/
					SET @queryToRun = @queryToRun + 
										N'USE [' + @DBName + ']; SELECT DISTINCT 
												  DB_ID(''' + @DBName + ''') AS [database_id]
												, ss.[object_id]
												, sc.[name] AS [table_schema]
												, ob.[name] AS [table_name]
												, ss.[stats_id]
												, ss.[name] AS [stats_name]
												, ss.[auto_created]
												, STATS_DATE(si.[id], si.[indid]) AS [last_updated]
												, si.[rowcnt] AS [rows]
												, ABS(si.[rowmodctr]) AS [modification_counter]
												, (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) AS [percent_changes]
										FROM [' + @DBName + '].sys.stats ss
										INNER JOIN [' + @DBName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @DBName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]
										INNER JOIN [' + @DBName + ']..sysindexes si ON si.[id] = ob.[object_id] AND si.[name] = ss.[name]' +
										CASE WHEN @flgOptions & 32768 = 32768 
											THEN N'
										INNER JOIN
												(
													 SELECT   [object_id]
															, SUM([reserved_page_count]) as [reserved_page_count]
													 FROM [' + @DBName + '].sys.dm_db_partition_stats
													 GROUP BY [object_id]
													 HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
												) ps ON ps.[object_id] = ob.[object_id]'
											ELSE N''
											END + N'
										WHERE	ob.[name] LIKE ''' + @TableName + '''
												AND sc.[name] LIKE ''' + @TableSchema + '''
												AND si.[rowcnt] > 0
												AND (    (    DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
														  AND si.[rowmodctr] <> 0
														 )
													 OR  
														( 
													 		  DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
														  AND si.[rowmodctr] <> 0 
														  AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
														)
												)'
			end
		ELSE
			/* SQL Server 2000 */
			SET @queryToRun = @queryToRun + 
								N'USE [' + @DBName + ']; SELECT DISTINCT 
										  DB_ID(''' + @DBName + ''') AS [database_id]
										, si.[id] AS [object_id]
										, sc.[name] AS [table_schema]
										, ob.[name] AS [table_name]
										, si.[indid] AS [stats_id]
										, si.[name] AS [stats_name]
										, CASE WHEN si.[status] & 8388608 <> 0 THEN 1 ELSE 0 END AS [auto_created]
										, STATS_DATE(si.[id], si.[indid]) AS [last_updated]
										, si.[rowcnt] AS [rows]
										, ABS(si.[rowmodctr]) AS [modification_counter]
										, (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) AS [percent_changes]
									FROM [' + @DBName + ']..sysindexes si
									INNER JOIN [' + @DBName + ']..sysobjects ob	ON ob.[id] = si.[id]
									INNER JOIN [' + @DBName + ']..sysusers sc	ON sc.[uid] = ob.[uid]
									WHERE	ob.[name] LIKE ''' + @TableName + '''
											AND sc.[name] LIKE ''' + @TableSchema + '''
											AND si.[indid] > 0 
											AND si.[indid] < 255
											AND ob.[xtype] <> ''S''
											AND si.[rowcnt] > 0
											AND (    (    DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
													  AND si.[rowmodctr] <> 0
													 )
												 OR  
													( 
													 	  DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
													  AND si.[rowmodctr] <> 0 
													  AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
													)
											)'

		IF @SQLServerName<>@@SERVERNAME
			SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @SQLServerName + N'], ''EXEC [' + @DBName + N'].sys.sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'


		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithStatisticsList([database_id], [object_id], [table_schema], [table_name], [stats_id], [stats_name], [auto_created], [last_updated], [rows], [modification_counter], [percent_changes])
				EXEC (@queryToRun)
	end

UPDATE #databaseObjectsWithStatisticsList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [stats_name] = LTRIM(RTRIM([stats_name]))

IF @flgOptions & 32768 = 32768
	SET @flgOptions = @flgOptions - 32768


--------------------------------------------------------------------------------------------------
--1/2	- Analyzing tables fragmentation
--		fragmentation information for the data and indexes of the specified table or view
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4))  AND (GETDATE() <= @stopTimeLimit)
	begin

		SET @queryToRun='Analyzing index fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	WHERE [index_type] <> 0 /* exclude heaps */
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun='[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + CASE WHEN @IndexName IS NOT NULL THEN N' - [' + @IndexName + ']' ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				IF @serverVersionNum < 9	/* SQL 2000 */
					begin
						IF @SQLServerName=@@SERVERNAME
							SET @queryToRun='USE [' + @DBName + N']; IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC SHOWCONTIG (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'', ''' + @IndexName + ''' ) WITH ' + CASE WHEN @flgOptions & 1024 = 1024 THEN '' ELSE 'FAST,' END + ' TABLERESULTS, NO_INFOMSGS'
						ELSE
							SET @queryToRun='SELECT * FROM OPENQUERY([' + @SQLServerName + N'], ''SET FMTONLY OFF; EXEC [' + @DBName + N'].dbo.sp_executesql N''''IF OBJECT_ID(''''''''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'''''''') IS NOT NULL DBCC SHOWCONTIG (''''''''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'''''''', ''''''''' + @IndexName + ''''''''' ) WITH ' + CASE WHEN @flgOptions & 1024 = 1024 THEN '' ELSE 'FAST,' END + ' TABLERESULTS, NO_INFOMSGS'''''')x'

						IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [Level], [Pages], [Rows], [MinimumRecordSize], [MaximumRecordSize], [AverageRecordSize], [ForwardedRecords], [Extents], [ExtentSwitches], [AverageFreeBytes], [AveragePageDensity], [ScanDensity], [BestCount], [ActualCount], [LogicalFragmentation], [ExtentFragmentation])
								EXEC (@queryToRun)
					end
				ELSE
					begin
						SET @queryToRun=N'SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
													, ips.[object_id]
													, si.[name] as index_name
													, ips.[index_id]
													, ips.[avg_fragmentation_in_percent]
													, ips.[page_count]
													, ips.[record_count]
													, ips.[forwarded_record_count]
													, ips.[avg_record_size_in_bytes]
													, ips.[avg_page_space_used_in_percent]
													, ips.[ghost_record_count]
											FROM [' + @DBName + '].sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
															CASE WHEN @flgOptions & 1024 = 1024 THEN 'DETAILED' ELSE 'LIMITED' END 
													+ ''') ips
											INNER JOIN [' + @DBName + '].sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
											WHERE	si.[type] IN (' + @analyzeIndexType + N')
													AND si.[is_disabled]=0'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
						INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count])  
								EXEC (@queryToRun)
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[LogicalFragmentation]
				, doil.[page_count] = cifs.[Pages]
				, doil.[ghost_record_count] = cifs.[ghost_record_count]
				, doil.[forwarded_records_percentage] = CASE WHEN ISNULL(cifs.[Rows], 0) > 0 
															 THEN (CAST(cifs.[ForwardedRecords] AS decimal(29,2)) / CAST(cifs.[Rows]  AS decimal(29,2))) * 100
															 ELSE 0
														END
				, doil.[page_density_deviation] =  ABS(CASE WHEN ISNULL(cifs.[AverageRecordSize], 0) > 0
															THEN ((FLOOR(8060. / ISNULL(cifs.[AverageRecordSize], 0)) * ISNULL(cifs.[AverageRecordSize], 0)) / 8060.) * doil.[fill_factor] - cifs.[AveragePageDensity]
															ELSE 0
													   END)
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #CurrentIndexFragmentationStats cifs ON cifs.[ObjectId] = doil.[object_id] AND cifs.[IndexId] = doil.[index_id]
	end


--------------------------------------------------------------------------------------------------
-- 1	Defragmenting database tables indexes
--		All indexes with a fragmentation level between defrag and rebuild threshold will be reorganized
--------------------------------------------------------------------------------------------------		
IF ((@flgActions & 1 = 1) AND (@flgActions & 4 = 0)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Defragmenting database tables indexes (fragmentation between ' + CAST(@DefragIndexThreshold AS [nvarchar]) + ' and ' + CAST(CAST(@RebuildIndexThreshold AS NUMERIC(6,2)) AS [nvarchar]) + ') and more than ' + CAST(@PageThreshold AS [nvarchar](4000)) + ' pages...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR FOR	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
		   								FROM	#databaseObjectsWithIndexList doil
										WHERE	doil.[page_count] >= @PageThreshold
												AND doil.[index_type] <> 0 /* heap tables will be excluded */
												AND	( 
														(
															 doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold 
														 AND doil.[avg_fragmentation_in_percent] < @RebuildIndexThreshold
														)
													OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
														(	  @flgOptions & 1024 = 1024 
														 AND doil.[page_density_deviation] >= @DefragIndexThreshold 
														 AND doil.[page_density_deviation] < @RebuildIndexThreshold
														)
													OR
														(	/* for very large tables, will performed reorganize instead of rebuild */
															doil.[page_count] >= @RebuildIndexPageCountLimit
															AND	( 
																	(
																		doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
																	)
																OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																	(	  @flgOptions & 1024 = 1024 
																		AND doil.[page_density_deviation] >= @RebuildIndexThreshold
																	)
																)
														)
													)
										ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun=N'[' + @CurrentTableSchema+ '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DECLARE crsIndexesToDegfragment CURSOR FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[object_id], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor]
							   								FROM	#databaseObjectsWithIndexList doil
   															WHERE	doil.[table_name] = @CurrentTableName
																	AND doil.[table_schema] = @CurrentTableSchema
																	AND doil.[page_count] >= @PageThreshold
																	AND doil.[index_type] <> 0 /* heap tables will be excluded */
																	AND	( 
																			(
																				 doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold 
																			 AND doil.[avg_fragmentation_in_percent] < @RebuildIndexThreshold
																			)
																		OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																			(	  @flgOptions & 1024 = 1024 
																			 AND doil.[page_density_deviation] >= @DefragIndexThreshold 
																			 AND doil.[page_density_deviation] < @RebuildIndexThreshold
																			)
																		OR
																			(	/* for very large tables, will performed reorganize instead of rebuild */
																				doil.[page_count] >= @RebuildIndexPageCountLimit
																				AND	( 
																						(
																							doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
																						)
																					OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																						(	  @flgOptions & 1024 = 1024 
																							AND doil.[page_density_deviation] >= @RebuildIndexThreshold
																						)
																					)
																			)
																		)																		
															ORDER BY doil.[index_id]
				OPEN crsIndexesToDegfragment
				FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered' 
															WHEN 2 THEN 'Nonclustered' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
											END
   						SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @eventData='<index-fragmentation><detail>' + 
											'<database_name>' + @DBName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<index_name>' + @childObjectName + '</index_name>' + 
											'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
											'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
											'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
											'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
											'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
										'</detail></index-fragmentation>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
															@dbName			= @DBName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - reorganize index',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						IF @serverVersionNum >= 9 
							begin
								SET @nestExecutionLevel = @executionLevel + 3

								EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName			= @SQLServerName
																		, @DBName					= @DBName
																		, @TableSchema				= @CurrentTableSchema
																		, @TableName				= @CurrentTableName
																		, @IndexName				= @IndexName
																		, @IndexID					= NULL
																		, @PartitionNumber			= DEFAULT
																		, @flgAction				= 2		--reorganize
																		, @flgOptions				= @flgOptions
																		, @MaxDOP					= @MaxDOP
																		, @executionLevel			= @nestExecutionLevel
																		, @affectedDependentObjects = @affectedDependentObjects OUT
																		, @DebugMode				= @DebugMode
							end
						ELSE
							begin
								SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
								SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC INDEXDEFRAG (0, ' + RTRIM(@ObjectID) + ', ' + RTRIM(@IndexID) + ') WITH NO_INFOMSGS'
								IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 1
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpDatabaseOptimize',
																				@eventName		= 'database maintenance - reorganize index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

							end
	   					FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
					end		
				CLOSE crsIndexesToDegfragment
				DEALLOCATE crsIndexesToDegfragment

				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
-- 2	- Rebuild heavy fragmented indexes
--		All indexes with a fragmentation level greater than rebuild threshold will be rebuild
--		If a clustered index needs to be rebuild, then all associated non-clustered indexes will be rebuild
--		http://technet.microsoft.com/en-us/library/ms189858.aspx
--------------------------------------------------------------------------------------------------
IF (@flgActions & 2 = 2) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database tables indexes (fragmentation between ' + CAST(@RebuildIndexThreshold AS [nvarchar]) + ' and 100) or small tables (no more than ' + CAST(@PageThreshold AS [nvarchar](4000)) + ' pages)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
																		
		DECLARE crsTableList CURSOR FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
		   									FROM	#databaseObjectsWithIndexList doil
											WHERE	    doil.[index_type] <> 0 /* heap tables will be excluded */
													AND doil.[page_count] >= @PageThreshold
													AND doil.[page_count] < @RebuildIndexPageCountLimit
													AND	( 
															(
																doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
															)
														OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
															(	  @flgOptions & 1024 = 1024 
															 AND doil.[page_density_deviation] >= @RebuildIndexThreshold
															)
														)
											ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @ClusteredRebuildNonClustered = 0

				DECLARE crsIndexesToRebuild CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor] 
				   							   								FROM	#databaseObjectsWithIndexList doil
		   																	WHERE	doil.[table_name] = @CurrentTableName
		   																			AND doil.[table_schema] = @CurrentTableSchema
																					AND doil.[page_count] >= @PageThreshold
																					AND doil.[page_count] < @RebuildIndexPageCountLimit
																					AND doil.[index_type] <> 0 /* heap tables will be excluded */
																					AND doil.[is_rebuilt] = 0
																					AND	( 
																							(
																								doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
																							)
																						OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																							(	  @flgOptions & 1024 = 1024 
																							 AND doil.[page_density_deviation] >= @RebuildIndexThreshold
																							)
																						)
																			ORDER BY doil.[index_id]

				OPEN crsIndexesToRebuild
				FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
				WHILE @@FETCH_STATUS = 0 AND @ClusteredRebuildNonClustered = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SELECT	@indexIsRebuilt = doil.[is_rebuilt]
						FROM	#databaseObjectsWithIndexList doil
						WHERE	doil.[table_schema] = @CurrentTableSchema 
		   						AND doil.[table_name] = @CurrentTableName
								AND doil.[index_id] = @IndexID

						IF @indexIsRebuilt = 0
							begin
								SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
																	WHEN 1 THEN 'Clustered' 
																	WHEN 2 THEN 'Nonclustered' 
																	WHEN 3 THEN 'XML'
																	WHEN 4 THEN 'Spatial' 
													END
		   						SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) +  ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
								SET @childObjectName = QUOTENAME(@IndexName)

								--------------------------------------------------------------------------------------------------
								--log index fragmentation information
								SET @eventData='<index-fragmentation><detail>' + 
													'<database_name>' + @DBName + '</database_name>' + 
													'<object_name>' + @objectName + '</object_name>'+ 
													'<index_name>' + @childObjectName + '</index_name>' + 
													'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
													'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
													'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
													'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
													'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
												'</detail></index-fragmentation>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
																	@dbName			= @DBName,
																	@objectName		= @objectName,
																	@childObjectName= @childObjectName,
																	@module			= 'dbo.usp_mpDatabaseOptimize',
																	@eventName		= 'database maintenance - rebuilding index',
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */
																						
								--------------------------------------------------------------------------------------------------
								--4  - Rebuild all dependent indexes when rebuild primary indexes
								IF @IndexType=1 AND (@flgOptions & 4 = 4)
									begin
										SET @ClusteredRebuildNonClustered = 1									
									end

								IF @serverVersionNum >= 9
									begin
										SET @nestExecutionLevel = @executionLevel + 3

										EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName			= @SQLServerName
																				, @DBName					= @DBName
																				, @TableSchema				= @CurrentTableSchema
																				, @TableName				= @CurrentTableName
																				, @IndexName				= @IndexName
																				, @IndexID					= NULL
																				, @PartitionNumber			= DEFAULT
																				, @flgAction				= 1		--rebuild
																				, @flgOptions				= @flgOptions
																				, @MaxDOP					= @MaxDOP
																				, @executionLevel			= @nestExecutionLevel
																				, @affectedDependentObjects = @affectedDependentObjects OUT
																				, @DebugMode				= @DebugMode

										--enable foreign key
										IF @IndexType=1
											begin
												 EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName	= @SQLServerName
																						  , @DBName			= @DBName
																						  , @TableSchema	= @CurrentTableSchema
																						  , @TableName		= @CurrentTableName
																						  , @ConstraintName = '%'
																						  , @flgAction		= 1
																						  , @flgOptions		= DEFAULT
																						  , @executionLevel	= @nestExecutionLevel
																						  , @DebugMode		= @DebugMode
											end
								
										IF @IndexType IN (1,3) AND @flgOptions & 4 = 4
											begin										
												--mark all dependent non-clustered/xml/spatial indexes as being rebuild
												UPDATE doil
													SET doil.[is_rebuilt]=1
												FROM	#databaseObjectsWithIndexList doil
	   											WHERE	doil.[table_name] = @CurrentTableName
	   													AND doil.[table_schema] = @CurrentTableSchema
														AND CHARINDEX(doil.[index_name], @affectedDependentObjects)<>0
											end
										end
								ELSE
									begin
										SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
										SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + ''', ''' + RTRIM(@IndexName) + ''') WITH NO_INFOMSGS'
										IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																						@dbName			= @DBName,
																						@objectName		= @objectName,
																						@childObjectName= @childObjectName,
																						@module			= 'dbo.usp_mpDatabaseOptimize',
																						@eventName		= 'database maintenance - rebuilding index',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @DebugMode
									end
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID

	   					FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
					end		
				CLOSE crsIndexesToRebuild
				DEALLOCATE crsIndexesToRebuild

				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
-- 4	- Rebuild all indexes 
--------------------------------------------------------------------------------------------------
IF (@flgActions & 4 = 4) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database tables indexes  (all)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--minimizing the list of indexes to be rebuild:
		--4  - Rebuild all dependent indexes when rebuild primary indexes
		IF (@flgOptions & 4 = 4)
			begin
				SET @queryToRun=N'optimizing index list to be rebuild'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
					

				DECLARE crsClusteredIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT doil.[table_schema], doil.[table_name], doil.[index_name]
																			FROM	#databaseObjectsWithIndexList doil
																			WHERE	doil.[index_type]=1 --clustered index
																					AND doil.[page_count] >= @PageThreshold
																					AND EXISTS (
																								SELECT 1
																								FROM #databaseObjectsWithIndexList b
																								WHERE b.[table_schema] = doil.[table_schema]
																										AND b.[table_name] = doil.[table_name]
																										AND CHARINDEX(CAST(b.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
																										AND b.[index_type] NOT IN (0, 1)
																										AND b.[is_rebuilt] = 0	--not yet rebuilt
																								)
																			ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]
				OPEN crsClusteredIndexes
				FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName
				WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
	
						--mark indexes as rebuilt
						UPDATE doil	
							SET doil.[is_rebuilt]=1
						FROM #databaseObjectsWithIndexList doil
						WHERE   doil.[table_schema] = @CurrentTableSchema
								AND doil.[table_name] = @CurrentTableName
								AND CHARINDEX(CAST(doil.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
								AND doil.[index_type] NOT IN (0, 1)
										
						FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName
					end
				CLOSE crsClusteredIndexes
				DEALLOCATE crsClusteredIndexes						
			end


		--rebuilding indexes
		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[index_name], doil.[index_type], doil.[index_id], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[fill_factor] 
							   										FROM	#databaseObjectsWithIndexList doil
   																	WHERE	doil.[index_type] <> 0 /* heap tables will be excluded */
																			AND doil.[is_rebuilt]=0
																			AND doil.[page_count] >= @PageThreshold
																			AND	( 
																					(
																						doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold
																					)
																				OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																					(	  @flgOptions & 1024 = 1024 
																						AND doil.[page_density_deviation] >= @DefragIndexThreshold
																					)
																				)
																	ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]

		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @indexIsRebuilt = 0
				--for XML indexes, check if it was not previously rebuilt by a primary XML index
				IF @IndexType=3
					SELECT	@indexIsRebuilt = doil.[is_rebuilt]
					FROM	#databaseObjectsWithIndexList doil
					WHERE	doil.[table_name] = @CurrentTableName
		   					AND doil.[table_schema] = @CurrentTableSchema 
							AND doil.[index_id] = @IndexID

				IF @indexIsRebuilt = 0
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered' 
															WHEN 2 THEN 'Nonclustered' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
											END

						--analyze curent object
						SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   				SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						SET @eventData='<index-fragmentation><detail>' + 
											'<database_name>' + @DBName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<index_name>' + @childObjectName + '</index_name>' + 
											'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
											'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
											'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
											'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
											'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
										'</detail></index-fragmentation>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
															@dbName			= @DBName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - rebuilding index',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						IF @serverVersionNum >= 9
							begin
								SET @nestExecutionLevel = @executionLevel + 3
								EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName			= @SQLServerName
																		, @DBName					= @DBName
																		, @TableSchema				= @CurrentTableSchema
																		, @TableName				= @CurrentTableName
																		, @IndexName				= @IndexName
																		, @IndexID					= NULL
																		, @PartitionNumber			= DEFAULT
																		, @flgAction				= 1		--rebuild
																		, @flgOptions				= @flgOptions
																		, @MaxDOP					= @MaxDOP
																		, @executionLevel			= @nestExecutionLevel
																		, @affectedDependentObjects = @affectedDependentObjects OUT
																		, @DebugMode				= @DebugMode
							--enable foreign key
							IF @IndexType=1
								begin
									 EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName	= @SQLServerName
																			  , @DBName			= @DBName
																			  , @TableSchema	= @CurrentTableSchema
																			  , @TableName		= @CurrentTableName
																			  , @ConstraintName = '%'
																			  , @flgAction		= 1
																			  , @flgOptions		= DEFAULT
																			  , @executionLevel	= @nestExecutionLevel
																			  , @DebugMode		= @DebugMode
								end

							--mark secondary indexes as being rebuilt, if primary xml was rebuilt
							IF @IndexType = 3 AND @flgOptions & 4 = 4
								begin										
									--mark all dependent xml indexes as being rebuild
									UPDATE doil
										SET doil.[is_rebuilt]=1
									FROM	#databaseObjectsWithIndexList doil
	   								WHERE	doil.[table_name] = @CurrentTableName
	   										AND doil.[table_schema] = @CurrentTableSchema
											AND CHARINDEX(doil.[index_name], @affectedDependentObjects)<>0
											AND doil.[is_rebuilt] = 0
								end
							end
						ELSE
							begin
								SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
								SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + ''', ''' + RTRIM(@IndexName) + ''') WITH NO_INFOMSGS'
								IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
								SET @childObjectName = QUOTENAME(@IndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpDatabaseOptimize',
																				@eventName		= 'database maintenance - rebuilding index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil 
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4	/ 16 
--------------------------------------------------------------------------------------------------
IF @serverVersionNum >= 9 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
		begin
			SET @nestExecutionLevel = @executionLevel + 1
			EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@sqlServerName	= @SQLServerName,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestExecutionLevel, 
																@debugMode		= @DebugMode
		end
	end



--------------------------------------------------------------------------------------------------
--cleanup of ghost records (sp_clean_db_free_space) (starting SQL Server 2005 SP3)
--exclude indexes which got rebuilt or reorganized, since ghost records were already cleaned
--------------------------------------------------------------------------------------------------
IF (@serverVersionNum >= 9.04035 AND @flgOptions & 65536 = 65536) AND (GETDATE() <= @stopTimeLimit)
	IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
			IF (
					SELECT SUM(doil.[ghost_record_count]) 
					FROM	#databaseObjectsWithIndexList doil
					WHERE	NOT (
									doil.[page_count] >= @PageThreshold
								AND doil.[index_type] <> 0 
								AND	( 
										(
											doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold 
										)
									OR  
										(	@flgOptions & 1024 = 1024 
										AND doil.[page_density_deviation] >= @DefragIndexThreshold 
										)
									)
								)
							AND doil.[is_rebuilt] = 0
				) >= @thresholdGhostRecords
				begin
					SET @queryToRun='sp_clean_db_free_space (ghost records cleanup)...'
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

					EXEC sp_clean_db_free_space @DBName
				end


--------------------------------------------------------------------------------------------------
--8  - Update statistics for all tables in database
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Update statistics for all tables (' + 
					CASE WHEN @StatsSamplePercent<100 
							THEN 'sample ' + CAST(@StatsSamplePercent AS [nvarchar]) + ' percent'
							ELSE 'fullscan'
					END + ')...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--remove tables with clustered indexes already rebuild
		SET @queryToRun=N'--	optimizing list (1)'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		DELETE dowsl
		FROM #databaseObjectsWithStatisticsList	dowsl
		WHERE EXISTS(
						SELECT 1
						FROM #databaseObjectsWithIndexList doil
						WHERE doil.[table_schema] = dowsl.[table_schema]
							AND doil.[table_name] = dowsl.[table_name]
							AND doil.[index_name] = dowsl.[stats_name]
							AND doil.[is_rebuilt] = 1
					)

		IF @flgOptions & 512 = 0
			begin
				--remove auto-created statistics
				SET @queryToRun=N'optimizing list (2)'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DELETE dowsl
				FROM #databaseObjectsWithStatisticsList	dowsl
				WHERE [auto_created]=1
			end

		DECLARE   @statsAutoCreated			[bit]
				, @tableRows				[bigint]
				, @statsModificationCounter	[bigint]
				, @lastUpdated				[datetime]
				, @percentChanges			[decimal](38,2)
				, @statsAge					[int]

		DECLARE crsTableList2 CURSOR FOR	SELECT [table_schema], [table_name], COUNT(*) AS [stats_count]
											FROM #databaseObjectsWithStatisticsList	
											GROUP BY [table_schema], [table_name]
											ORDER BY [table_name]
		OPEN crsTableList2
		FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @queryToRun=N'[' + @CurrentTableSchema+ '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @IndexID=1
				DECLARE crsTableStatsList CURSOR FOR	SELECT	  [stats_name], [auto_created], [rows], [modification_counter], [last_updated], [percent_changes]
																, DATEDIFF(dd, [last_updated], GETDATE()) AS [stats_age]
														FROM	#databaseObjectsWithStatisticsList	
														WHERE	[table_schema] = @CurrentTableSchema
																AND [table_name] = @CurrentTableName
														ORDER BY [stats_name]
				OPEN crsTableStatsList
				FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=CAST(@IndexID AS [nvarchar](64)) + '/' + CAST(@statsCount AS [nvarchar](64)) + ' - [' + @IndexName+ '] / age = ' + CAST(@statsAge AS [varchar](32)) + ' days / rows = ' + CAST(@tableRows AS [varchar](32)) + ' / changes = ' + CAST(@statsModificationCounter AS [varchar](32))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log statistics information
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						SET @eventData='<statistics-health><detail>' + 
											'<database_name>' + @DBName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<stats_name>' + @childObjectName + '</stats_name>' + 
											'<auto_created>' + CAST(@statsAutoCreated AS [varchar](32)) + '</auto_created>' + 
											'<rows>' + CAST(@tableRows AS [varchar](32)) + '</rows>' + 
											'<modification_counter>' + CAST(@statsModificationCounter AS [varchar](32)) + '</modification_counter>' + 
											'<percent_changes>' + CAST(@percentChanges AS [varchar](32)) + '</percent_changes>' + 
											'<last_updated>' + CONVERT([nvarchar](20), @lastUpdated, 120) + '</last_updated>' + 
											'<age_days>' + CAST(@statsAge AS [varchar](32)) + '</age_days>' + 
										'</detail></statistics-health>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
															@dbName			= @DBName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - update statistics',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
						SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL UPDATE STATISTICS [' + @CurrentTableSchema + '].[' + @CurrentTableName + ']([' +  @IndexName + ']) WITH '
								
						IF @StatsSamplePercent<100
							SET @queryToRun=@queryToRun + N'SAMPLE ' + CAST(@StatsSamplePercent AS [nvarchar]) + ' PERCENT'
						ELSE
							SET @queryToRun=@queryToRun + N'FULLSCAN'

						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)
						SET @nestedExecutionLevel = @executionLevel + 1

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																		@dbName			= @DBName,
																		@objectName		= @objectName,
																		@childObjectName= @childObjectName,
																		@module			= 'dbo.usp_mpDatabaseOptimize',
																		@eventName		= 'database maintenance - update statistics',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @DebugMode

						SET @IndexID = @IndexID + 1
						FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge
					end
				CLOSE crsTableStatsList
				DEALLOCATE crsTableStatsList

				FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
			end
		CLOSE crsTableList2
		DEALLOCATE crsTableList2
	end
	

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 	DROP TABLE #databaseObjectsWithIndexList

RETURN @errorCode
GO

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 01.10.2015
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012/2014+
-- Description		 : add indexes to msdb database in order to improve system maintenance execution times
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create additional indexes on msdb database, if required...', 10, 1) WITH NOWAIT
GO
USE [msdb]
GO
--  backupset
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_set_uuid'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_set_uuid' AND [id]=OBJECT_ID('dbo.backupset'))
begin   
	RAISERROR('--Creating index => [IX_backupset_backup_set_uuid] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_set_uuid] ON [dbo].[backupset]([backup_set_uuid])
end 

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'media_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_media_set_id' AND [id]=OBJECT_ID('dbo.backupset'))
begin   
	RAISERROR('--Creating index => [IX_backupset_media_set_id] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_media_set_id] ON [dbo].[backupset]([media_set_id])
end

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_finish_date'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_finish_date' AND [id]=OBJECT_ID('dbo.backupset'))
begin  
	RAISERROR('--Creating index => [IX_backupset_backup_finish_date] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_finish_date] ON [dbo].[backupset]([backup_finish_date])
end 

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_start_date'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_start_date' AND [id]=OBJECT_ID('dbo.backupset'))
begin
	RAISERROR('--Creating index => [IX_backupset_backup_start_date] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_start_date] ON [dbo].[backupset]([backup_start_date])
end


--  backupfile
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupfile')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupfile_backup_set_id' AND [id]=OBJECT_ID('dbo.backupfile'))
begin
	RAISERROR('--Creating index => [IX_backupfile_backup_set_id] ON [dbo].[backupfile]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupfile_backup_set_id] ON [dbo].[backupfile]([backup_set_id])
end

--  backupmediafamily
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupmediafamily')
						AND sc.[name] = 'media_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupmediafamily_media_set_id' AND [id]=OBJECT_ID('dbo.backupmediafamily'))
begin
	RAISERROR('--Creating index => [IX_backupmediafamily_media_set_id] ON [dbo].[backupmediafamily]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupmediafamily_media_set_id] ON [dbo].[backupmediafamily]([media_set_id])
end

--  backupfilegroup
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupfilegroup')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupfilegroup_backup_set_id' AND [id]=OBJECT_ID('dbo.backupfilegroup'))
begin
    RAISERROR('--Creating index => [IX_backupfilegroup_backup_set_id] ON [dbo].[backupfilegroup]', 10, 1) WITH NOWAIT
	CREATE INDEX [IX_backupfilegroup_backup_set_id] ON [dbo].[backupfilegroup]([backup_set_id])
end

--  restorehistory
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorehistory')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorehistory_restore_history_id' AND [id]=OBJECT_ID('dbo.restorehistory'))
begin
	RAISERROR('--Creating index => [IX_restorehistory_restore_history_id] ON [dbo].[restorehistory]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorehistory_restore_history_id] ON [dbo].[restorehistory]([restore_history_id])
end

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorehistory')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorehistory_backup_set_id' AND [id]=OBJECT_ID('dbo.restorehistory'))
begin
	RAISERROR('--Creating index => [IX_restorehistory_backup_set_id] ON [dbo].[restorehistory]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorehistory_backup_set_id] ON [dbo].[restorehistory]([backup_set_id])
end

--  restorefile
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorefile')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorefile_restore_history_id' AND [id]=OBJECT_ID('dbo.restorefile'))
begin
	RAISERROR('--Creating index => [IX_restorefile_restore_history_id] ON [dbo].[restorefile]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorefile_restore_history_id] ON [dbo].[restorefile]([restore_history_id])
end

--  restorefilegroup
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorefilegroup')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorefilegroup_restore_history_id' AND [id]=OBJECT_ID('dbo.restorefilegroup'))
begin
	RAISERROR('--Creating index => [IX_restorefilegroup_restore_history_id] ON [dbo].[restorefilegroup]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorefilegroup_restore_history_id] ON [dbo].[restorefilegroup]([restore_history_id])
end
