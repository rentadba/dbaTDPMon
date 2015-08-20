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
			@RunDate			[varchar](10),
			@RunDateDetail		[varchar](10),
			@RunTime			[varchar](8),
			@RunTimeDetail		[varchar](8),
			@RunDuration		[varchar](8),
			@RunDurationDetail	[varchar](8),
			@RunStatus			[varchar](32),
			@RunStatusDetail	[varchar](32),
			@RunDurationLast	[varchar](8),
			@ReturnValue		[int],
			@queryToRun			[nvarchar](4000)

SET NOCOUNT ON

IF OBJECT_ID('tempdb..#tmpCheck') IS NOT NULL DROP TABLE #tmpCheck
CREATE TABLE #tmpCheck (Result varchar(1024))

IF ISNULL(@sqlServerName, '')=''
	begin
		SET @queryToRun='--	ERROR: The specified value for SOURCE server is not valid.'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

IF LEN(@jobName)=0 OR ISNULL(@jobName, '')=''
	begin
		RAISERROR('--ERROR: Must specify a job name.', 10, 1) WITH NOWAIT
		RETURN 1
	end

SET @queryToRun='SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
TRUNCATE TABLE #tmpCheck
INSERT INTO #tmpCheck EXEC (@queryToRun)
IF (SELECT count(*) FROM #tmpCheck)=0
	begin
		SET @queryToRun='--	ERROR: SOURCE server [' + @sqlServerName + '] is not defined as linked server on THIS server [' + @sqlServerName + '].'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

---------------------------------------------------------------------------------------------
SET @currentRunning	=0
SET @ReturnValue	=5 --Unknown

SET @queryToRun='SELECT Count(*) FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' + @jobName + ''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode = 1 PRINT @queryToRun

TRUNCATE TABLE #tmpCheck
INSERT INTO #tmpCheck EXEC (@queryToRun)
------------------------------------------------------------------------------------------------------------------------------------------
IF (SELECT TOP 1 Result FROM #tmpCheck)=0
	begin
		SET @strMessage='--SQL Server Agent: The specified job name [' + @jobName + '] does not exists on this server [' + @sqlServerName + ']'
		IF @debugMode=1
			RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		SET @currentRunning=0
		SET @ReturnValue=-5 --Unknown
	end
ELSE
	begin
		IF OBJECT_ID('tempdb..#JobRunDetail') IS NOT NULL DROP TABLE #JobRunDetail
		CREATE TABLE #JobRunDetail(step_id int, job_id varchar(255))
		
		SET @currentRunning=1 

		SET @queryToRun='SELECT * 
						FROM (
							  SELECT B [step_id], SUBSTRING(A, 7, 2) + SUBSTRING(A, 5, 2) + SUBSTRING(A, 3, 2) + LEFT(A, 2) + ''-'' + SUBSTRING(A, 11, 2) + SUBSTRING(A, 9, 2) + ''-'' + SUBSTRING(A, 15, 2) + SUBSTRING(A, 13, 2) + ''-'' + SUBSTRING(A, 17, 4) + ''-'' + RIGHT(A , 12) [job_id] 
 							  FROM (
									SELECT SUBSTRING([program_name], CHARINDEX('': Step'', [program_name]) + 7, LEN([program_name]) - CHARINDEX('': Step'', [program_name]) - 7) B, SUBSTRING([program_name], CHARINDEX(''(Job 0x'', [program_name]) + 7, CHARINDEX('' : Step '', [program_name]) - CHARINDEX(''(Job 0x'', [program_name]) - 7) A
			 						FROM [master].[dbo].[sysprocesses] 
									WHERE [program_name] LIKE ''SQLAgent - %JobStep%''
								   ) A
							) A 
						WHERE [job_id] IN (
											SELECT DISTINCT [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [name]= ''' + @jobName + '''
										  )'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun
		INSERT INTO #JobRunDetail EXEC (@queryToRun)

		SET @StepID=null
		SET @JobID=null

		SELECT @currentRunning=count(*) FROM #JobRunDetail
		SELECT @StepID=[step_id], @JobID=[job_id] FROM #JobRunDetail	
		IF OBJECT_ID('tempdb..#JobRunDetail') IS NOT NULL DROP TABLE #JobRunDetail
	
		IF @currentRunning>0 
			begin
				SET @queryToRun='SELECT [step_name] FROM [msdb].[dbo].[sysjobsteps] WHERE [step_id]=' + CAST(@StepID AS varchar) + ' AND [job_id]=''' + @JobID + ''''
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
				IF OBJECT_ID('tempdb..#JobRunDetail3') IS NOT NULL DROP TABLE #JobRunDetail3
				CREATE TABLE #JobRunDetail3(run_date varchar(16), run_time varchar(16), run_status int)

				SET @queryToRun='SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [run_date]
											, CAST(h.[run_time] AS varchar) AS [run_time]
											, h.[run_status]
						FROM [msdb].[dbo].[sysjobs] j 
						RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
						WHERE j.[name]=''' + @jobName + ''' 
								AND h.[instance_id] > (
														/* last job completion id */
														SELECT TOP 1 h1.[instance_id]
														FROM [msdb].[dbo].[sysjobs] j1 
														RIGHT JOIN [msdb].[dbo].[sysjobhistory] h1 ON j1.[job_id] = h1.[job_id] 
														WHERE j1.[name]=''' + @jobName + ''' 
																AND [step_name] =''(Job outcome)''
														ORDER BY h1.[instance_id] DESC
														)
						ORDER BY h.[instance_id] ASC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #JobRunDetail3
				INSERT INTO #JobRunDetail3 EXEC (@queryToRun)

				/* job was cancelled, but process is still running, probably performing a rollback */
				IF (SELECT COUNT(*) FROM #JobRunDetail3)=0
					begin
						SET @queryToRun='SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [run_date]
													, CAST(h.[run_time] AS varchar) AS [run_time]
													, h.[run_status]
								FROM [msdb].[dbo].[sysjobs] j 
								RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
								WHERE j.[name]=''' + @jobName + ''' 
										AND h.[instance_id] = (
																/* last job completion id */
																SELECT TOP 1 h1.[instance_id]
																FROM [msdb].[dbo].[sysjobs] j1 
																RIGHT JOIN [msdb].[dbo].[sysjobhistory] h1 ON j1.[job_id] = h1.[job_id] 
																WHERE j1.[name]=''' + @jobName + ''' 
																		AND [step_name] =''(Job outcome)''
																ORDER BY h1.[instance_id] DESC
																)
								ORDER BY h.[instance_id] ASC'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						INSERT INTO #JobRunDetail3 EXEC (@queryToRun)
					end
									
				SET @RunDate	=null
				SET @RunTime	=null
				SELECT TOP 1 @RunDate=[run_date], @RunTime=[run_time], @RunStatus=CAST([run_status] AS varchar) FROM #JobRunDetail3
	

				SET @RunTime=REPLICATE('0', 6-LEN(@RunTime)) + @RunTime
				SET @RunDate=SUBSTRING(@RunDate, 1,4) + '-' + SUBSTRING(@RunDate, 5,2) + '-' + SUBSTRING(@RunDate, 7,2)
				SET @RunTime=SUBSTRING(@RunTime, 1,2) + ':' + SUBSTRING(@RunTime, 3,2) + ':' + SUBSTRING(@RunTime, 5,2)

				SET @lastExecutionDate=@RunDate
				SET @lastExecutionTime=@RunTime

				IF @RunStatus='0' SET @RunStatus='Failed'
				IF @RunStatus='1' SET @RunStatus='Succeded'				
				IF @RunStatus='2' SET @RunStatus='Retry'
				IF @RunStatus='3' SET @RunStatus='Canceled'
				IF @RunStatus='4' SET @RunStatus='In progress'
				
				SET @strMessage='--Job currently running step    : [' + CAST(@StepID AS varchar) + '] - [' + @StepName + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--Job started at         	    : [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--Job execution status  	    : [' + ISNULL(@RunStatus, '') + ']'	
			end
		ELSE
			begin
				IF OBJECT_ID('tempdb..#JobRunDetail2') IS NOT NULL DROP TABLE #JobRunDetail2
				CREATE TABLE #JobRunDetail2(message varchar(4000), step_id int, step_name varchar(255), run_status int, run_date varchar(16), run_time varchar(16), run_duration varchar(16))

				SET @queryToRun='SELECT TOP 1 h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
								FROM [msdb].[dbo].[sysjobs] j 
								RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
								WHERE	j.[name]=''' + @jobName + ''' 
										AND h.[step_name] <> ''(Job outcome)''
								ORDER BY h.[instance_id] DESC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #JobRunDetail2
				INSERT INTO #JobRunDetail2 EXEC (@queryToRun)
				
				SET @Message	=null
				SET @StepID		=null
				SET @StepName	=null
				SET @lastExecutionStatus=null
				SET @RunStatus	=null
				SET @RunDate	=null
				SET @RunTime	=null
				SET @RunDuration=null
				SELECT TOP 1 @Message=[message], @StepID=[step_id], @StepName=[step_name], @RunDate=[run_date], @RunTime=[run_time], @RunDuration=[run_duration] FROM #JobRunDetail2
				
				SET @queryToRun='SELECT TOP 1 null, null, null, [run_status], null, null, CAST([run_duration] AS varchar) AS [RunDuration]
								FROM [msdb].[dbo].[sysjobhistory]
								WHERE	[job_id] IN (
													 SELECT [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' + @jobName + '''
													)
										AND [step_name] =''(Job outcome)''
								ORDER BY [instance_id] DESC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #JobRunDetail2
				INSERT INTO #JobRunDetail2 EXEC (@queryToRun)
				
				SET @RunDurationLast=null
				SET @RunStatus=null
				SELECT TOP 1 @RunDurationLast=[run_duration], @RunStatus=CAST([run_status] AS varchar), @lastExecutionStatus=[run_status] FROM #JobRunDetail2
			
				IF @RunStatus=0
					begin
						SET @queryToRun='SELECT TOP 1 h.[message], null, null, null, null, null, null
									FROM [msdb].[dbo].[sysjobs] j 
									RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
									WHERE j.[name]=''' + @jobName + ''' 
											AND h.[step_name] <> ''(Job outcome)'' 
											AND h.[run_status]=0
									ORDER BY h.[instance_id] DESC'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						TRUNCATE TABLE #JobRunDetail2
						INSERT INTO #JobRunDetail2 EXEC (@queryToRun)
						SELECT TOP 1 @Message=[message] FROM #JobRunDetail2
						SET @lastExecutionStatus=0
					end
				SET @RunDurationLast=REPLICATE('0', 6-LEN(@RunDurationLast)) + @RunDurationLast
				SET @RunDurationLast=SUBSTRING(@RunDurationLast, 1,2) + ':' + SUBSTRING(@RunDurationLast, 3,2) + ':' + SUBSTRING(@RunDurationLast, 5,2)
				IF @lastExecutionStatus IS NULL
					begin
						SET @RunStatus='Unknown'
						SET @lastExecutionStatus='5' 
					end
				IF @RunStatus='0' SET @RunStatus='Failed'
				IF @RunStatus='1' SET @RunStatus='Succeded'				
				IF @RunStatus='2' SET @RunStatus='Retry'
				IF @RunStatus='3' SET @RunStatus='Canceled'
				IF @RunStatus='4' SET @RunStatus='In progress'
				SET @RunTime=REPLICATE('0', 6-LEN(@RunTime)) + @RunTime
				SET @RunDuration=REPLICATE('0', 6-LEN(@RunDuration)) + @RunDuration
				SET @RunDate=SUBSTRING(@RunDate, 1,4) + '-' + SUBSTRING(@RunDate, 5,2) + '-' + SUBSTRING(@RunDate, 7,2)
				SET @RunTime=SUBSTRING(@RunTime, 1,2) + ':' + SUBSTRING(@RunTime, 3,2) + ':' + SUBSTRING(@RunTime, 5,2)
				SET @RunDuration=SUBSTRING(@RunDuration, 1,2) + ':' + SUBSTRING(@RunDuration, 3,2) + ':' + SUBSTRING(@RunDuration, 5,2)
				
				SET @strMessage='--The specified job [' + @sqlServerName + '].[' + @jobName + '] is not currently running.'
				IF @RunStatus<>'Unknown'
					begin
						SET @strMessage=@strMessage + CHAR(13) + '--Last execution step			: [' + ISNULL(CAST(@StepID AS varchar), '') + '] - [' + ISNULL(@StepName, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Last step finished at      	: [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Last step running time		: [' + ISNULL(@RunDuration, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Job execution time (total)	: [' + ISNULL(@RunDurationLast, '') + ']'	
					end
				SET @strMessage=@strMessage + CHAR(13) + '--Last job execution status  	: [' + ISNULL(@RunStatus, '') + ']'	

				IF @extentedStepDetails=1
					begin
						--get job execution details: steps execution status
						SET @queryToRun='SELECT h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
										FROM [msdb].[dbo].[sysjobs] j 
										RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
										WHERE	 h.[instance_id] < (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobs] j 
																			RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
																			WHERE	j.[name]=''' + @jobName + ''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND	h.[instance_id] > ISNULL(
																	( SELECT [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobs] j 
																			RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
																			WHERE	j.[name]=''' + @jobName + ''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	WHERE [instance_id] NOT IN 
																		(
																		SELECT TOP 1 [instance_id] 
																		FROM (	SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																				FROM [msdb].[dbo].[sysjobs] j 
																				RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
																				WHERE	j.[name]=''' + @jobName + ''' 
																						AND h.[step_name] =''(Job outcome)''
																				ORDER BY h.[instance_id] DESC
																			)A
																		)),0)
												AND j.[job_id] IN (
																	SELECT [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [Name]=''' + @jobName + ''' 
																)
											ORDER BY h.[instance_id]'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						TRUNCATE TABLE #JobRunDetail2
						INSERT INTO #JobRunDetail2 EXEC (@queryToRun)
						DECLARE crsJobDetails CURSOR FOR	SELECT DISTINCT [step_id]
																			, [step_name]
																			, [run_status]
																			, [run_date]
																			, [run_time]
																			, [message]
															FROM #JobRunDetail2
															ORDER BY [run_date], [run_time]
						OPEN crsJobDetails

						SET @queryToRun='[Run Date  ] [RunTime ] [Status     ] [ID ] [Step Name                          ]'
						PRINT @queryToRun

						FETCH NEXT FROM crsJobDetails INTO @StepID, @StepName, @RunStatusDetail, @RunDateDetail, @RunTimeDetail, @queryToRun
						
						WHILE @@FETCH_STATUS=0
							begin								
								IF @RunStatusDetail='0' SET @RunStatusDetail='Failed     '
								IF @RunStatusDetail='1' SET @RunStatusDetail='Succeded   '				
								IF @RunStatusDetail='2' SET @RunStatusDetail='Retry      '
								IF @RunStatusDetail='3' SET @RunStatusDetail='Canceled   '
								IF @RunStatusDetail='4' SET @RunStatusDetail='In progress'
								SET @RunTimeDetail=REPLICATE('0', 6-LEN(@RunTimeDetail)) + @RunTimeDetail
								SET @RunDateDetail=SUBSTRING(@RunDateDetail, 1,4) + '-' + SUBSTRING(@RunDateDetail, 5,2) + '-' + SUBSTRING(@RunDateDetail, 7,2)
								SET @RunTimeDetail=SUBSTRING(@RunTimeDetail, 1,2) + ':' + SUBSTRING(@RunTimeDetail, 3,2) + ':' + SUBSTRING(@RunTimeDetail, 5,2)
								PRINT '[' + @RunDateDetail + '] [' + @RunTimeDetail + '] [' + @RunStatusDetail + '] [' + REPLICATE(' ', 3-CAST(@StepID AS varchar)) + CAST(@StepID AS varchar) + '] [' + @StepName + '] [' + @queryToRun + ']'

								FETCH NEXT FROM crsJobDetails INTO @StepID, @StepName, @RunStatusDetail, @RunDateDetail, @RunTimeDetail, @queryToRun
							end
						CLOSE crsJobDetails
						DEALLOCATE crsJobDetails					
					end

				IF @RunStatus='Failed'
					begin
						SET @strMessage=@strMessage + CHAR(13) + '--Job execution return this message: ' + ISNULL(@Message, '')
						IF @debugMode=1
							print '--Job execution return this message: ' + ISNULL(@Message, '')
					end

				SET @lastExecutionDate=@RunDate
				SET @lastExecutionTime=@RunTime

				SET @ReturnValue=@lastExecutionStatus
				IF OBJECT_ID('tempdb..#JobRunDetail2') IS NOT NULL DROP TABLE #JobRunDetail2
			end
	end
IF OBJECT_ID('tempdb..#tmpCheck') IS NOT NULL DROP TABLE #tmpCheck
IF @debugMode=1
	print @strMessage
SET @ReturnValue=ISNULL(@ReturnValue, 0)
IF @selectResult=1
	SELECT @strMessage AS StrMessage, @currentRunning AS CurrentRunning, @lastExecutionStatus AS LastExecutionStatus, @lastExecutionDate AS LastExecutionDate, @lastExecutionTime AS LastExecutionTime
RETURN @ReturnValue



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

