RAISERROR('Create procedure: [dbo].[usp_sqlAgentJobStartAndWatch]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_sqlAgentJobStartAndWatch]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_sqlAgentJobStartAndWatch]
GO

CREATE PROCEDURE dbo.usp_sqlAgentJobStartAndWatch
		@sqlServerName				[sysname],
		@jobName					[sysname],
		@stepToStart				[int],
		@stepToStop					[int],
		@waitForDelay				[varchar](8),
		@dontRunIfLastExecutionSuccededLast	[int]=0,		--numarul de minute 
		@startJobIfPrevisiousErrorOcured	[bit]=1,
		@watchJob					[bit]=1,
		@jobQueueID					[int]=NULL,
		@debugMode					[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE @currentRunning 		[int],
		@lastExecutionStatus	[int],
		@lastExecutionDate		[varchar](10),
		@lastExecutionTime		[varchar](8),
		@lastExecutionStep		[int],
		@runningTimeSec			[bigint],
		@strMessage				[varchar](4096),
		@lastMessage			[varchar](4096),
		@jobWasRunning			[bit],
		@returnValue			[bit],		--1=eroare, 0=succes
		@startJob				[bit],
		@jobID					[varchar](255),
		@stepName				[varchar](255),
		@lastStepSuccesAction	[int],
		@lastStepFailureAction	[int],
		@tmpServer				[varchar](1024),
		@queryToRun				[nvarchar](4000),
		@Error					[int]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
IF object_id('#tmpCheckParameters') IS NOT NULL DROP TABLE #tmpCheckParameters
CREATE TABLE #tmpCheckParameters (Result varchar(1024))

IF ISNULL(@sqlServerName, '')=''
	begin
		SET @queryToRun='ERROR: The specified value for SOURCE server is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF LEN(@jobName)=0 OR ISNULL(@jobName, '')=''
	begin
		SET @queryToRun = 'ERROR: Must specify a job name.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

SET @queryToRun='SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

TRUNCATE TABLE #tmpCheckParameters
INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
IF (SELECT count(*) FROM #tmpCheckParameters)=0
	begin
		SET @queryToRun='ERROR: SOURCE server [' + @sqlServerName + '] is not defined as linked server on THIS server [' + @sqlServerName + '].'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

SET @queryToRun='SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

SET @tmpServer='[' + @sqlServerName + '].master.dbo.sp_executesql'

TRUNCATE TABLE #tmpCheckParameters
INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
IF (SELECT count(*) FROM #tmpCheckParameters)=0
	begin
		SET @queryToRun='ERROR: THIS server [' + @sqlServerName + '] is not defined as linked server on SOURCE server [' + @sqlServerName + '].'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end


---------------------------------------------------------------------------------------------
SET @lastMessage	= ''
SET @currentRunning	= 1
SET @jobWasRunning	= 0
SET @startJob		= 0
SET @returnValue	= 0


--daca job-ul e pornit il monitorizez
WHILE @currentRunning<>0
	begin
		SET @currentRunning=1
		--verific daca job-ul este in curs de executie. daca da, afisez momentele de executie ale job-ului
		EXEC [dbo].[usp_sqlAgentJobCheckStatus] @sqlServerName, @jobName, @strMessage OUT, @currentRunning OUT, @lastExecutionStatus OUT, @lastExecutionDate OUT, @lastExecutionTime OUT, @runningTimeSec OUT, 0, 0, 0
		IF @currentRunning<>0
			begin
				IF ISNULL(@strMessage,'')<>ISNULL(@lastMessage, '')
					begin
						IF @watchJob=1
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						SET @lastMessage=@strMessage
					end
				IF @jobWasRunning=0
					SET @jobWasRunning=1
				IF @watchJob=0
					SET @currentRunning=0
				ELSE
					WAITFOR DELAY @waitForDelay
			end
		ELSE
			begin
				--job-ul s-a terminat sau nu s-a executat.
				IF @lastExecutionStatus=0
					begin
						--job-ul care a rulat si a  fost urmarit s-a terminat cu eroare
						IF @jobWasRunning=1
							begin
								--ultima executie a job-ului a fost cu eroare
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
								
								SET @strMessage = 'Execution failed. Please notify your Database Administrator.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
								
								SET @currentRunning=0
								SET @returnValue=1	--1=eroare, 0=succes
							end
						ELSE
							begin
								SET @strMessage = 'Warning: Last job execution failed.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
								IF @startJobIfPrevisiousErrorOcured=1
									SET @startJob=1
							end
					end
				ELSE
					--verific daca job-ul a fost lansat de aici sau a de catre o alta locatie si s-a asteptat terminarea executiei sale
					IF @jobWasRunning=0
						begin
							SET @currentRunning=1
							IF @lastExecutionStatus=1
								IF (@lastExecutionDate<>'') AND (@lastExecutionTime<>'')
									begin
										--daca job-ul s-a executat cu succes in ultimele 120 de minute, nu se va mai lansa
										SET @strMessage=@lastExecutionDate + ' ' + @lastExecutionTime
										IF ABS(DATEDIFF(minute, GetDate(), CONVERT(datetime, @strMessage, 120)))<@dontRunIfLastExecutionSuccededLast
											begin
												SET @currentRunning=0
												SET @strMessage = 'Job was previosly executed with a succes closing state.'
												EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
												SET @returnValue=0
											end
										end
							IF @currentRunning<>0
								begin
									SET @startJob=1
									SET @currentRunning=0
								end
						end
					ELSE
						SET @currentRunning=0
			end
		IF @watchJob=0
			SET @currentRunning=0
	end
--job-ul trebuie pornit
IF @startJob=1
	begin
		IF @stepToStart > @stepToStop
			begin
				SET @strMessage = 'The Start Step cannot be greater than the Stop Step when watching a job!'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
				RETURN 1
			end
	
		SET @queryToRun='SELECT CAST([job_id] AS varchar(255)) FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' +  [dbo].[ufn_getObjectQuoteName](@jobName, 'sql') + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun

		SET @jobID=NULL
		SELECT @jobID=Result FROM #tmpCheckParameters
		IF @jobID IS NOT NULL
			begin
				--verific existenta primului pas trimis ca parametru
				SET @queryToRun='SELECT MIN([step_id]) FROM [msdb].[dbo].[sysjobsteps] WHERE [job_id]=''' + @jobID + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
				
				IF (SELECT CAST(Result AS numeric) FROM #tmpCheckParameters)>@stepToStart
					begin
						SET @strMessage='The specified Start Step is not defined for this job.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
						SET @strMessage='Setting Start Step the job''s first defined step.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
						SELECT @stepToStart=CAST(Result AS numeric) FROM #tmpCheckParameters
					end
				
				--verific existenta ultimului pas trimis ca parametru
				SET @queryToRun='SELECT MAX([step_id]) FROM [msdb].[dbo].[sysjobsteps] WHERE [job_id]=''' + @jobID + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
				
				IF (SELECT CAST(Result AS numeric) FROM #tmpCheckParameters)<@stepToStop
					begin
						SET @strMessage='The specified Stop Step is not defined for this job.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						SET @strMessage='Setting Stop Step the job''s last defined step.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						SELECT @stepToStop=CAST(Result AS numeric) FROM #tmpCheckParameters
					end
		 		SET @strMessage='Setting execution Start Step: [' + CAST(@stepToStart AS varchar) + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				
				--incerc sa modific starea ultimul pas de executie. determinare stare curenta
				SET @lastStepSuccesAction=NULL
				SET @lastStepFailureAction=NULL

				SET @queryToRun='SELECT [on_success_action] FROM [msdb].[dbo].[sysjobsteps] WHERE [job_id]=''' + @jobID + ''' AND [step_id]=' + CAST(@stepToStop AS varchar)
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
				SELECT @lastStepSuccesAction=CAST(Result AS numeric) FROM #tmpCheckParameters

				SET @queryToRun='SELECT [on_fail_action] FROM [msdb].[dbo].[sysjobsteps] WHERE [job_id]=''' + @jobID + ''' AND [step_id]=' + CAST(@stepToStop AS varchar)
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
				SELECT @lastStepFailureAction=CAST(Result AS numeric) FROM #tmpCheckParameters

				IF (@lastStepSuccesAction IS NULL) OR (@lastStepFailureAction IS NULL)
					begin
						SET @strMessage = 'Cannot read job''s Start Step informations.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
						
						IF OBJECT_ID('#tmpCheckParameters') IS NOT NULL DROP TABLE #tmpCheckParameters
						RETURN 1
					end			
				ELSE
					begin
						SET @strMessage='Setting execution Stop Step : [' + CAST(@stepToStop AS varchar) + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
						--modific ultimul pas important
						SET @queryToRun='[' + @sqlServerName + '].[msdb].[dbo].[sp_update_jobstep] @job_id = ''' + @jobID + ''', @step_id = ' + CAST(@stepToStop AS varchar) + ', @on_success_action = 1, @on_fail_action=2'
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						EXEC sp_executesql @queryToRun

						SET @queryToRun='[' + @sqlServerName + '].[msdb].[dbo].[sp_update_jobstep] @job_id = ''' + @jobID + ''', @step_id = ' + CAST(@stepToStop AS varchar) + ', @on_success_action = 1, @on_fail_action=2'
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						EXEC sp_executesql @queryToRun

						IF @@ERROR<>0
							begin
								SET @strMessage = 'Failed in modifying job''s execution Stop Step.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
							end
						ELSE
							begin
								--extrag numele pasului de start
								SET @stepName=NULL
								SET @queryToRun='SELECT [step_name] FROM [msdb].[dbo].[sysjobsteps] WHERE [job_id]=''' + @jobID + ''' AND [step_id]=' + CAST(@stepToStart AS varchar)
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								TRUNCATE TABLE #tmpCheckParameters
								INSERT INTO #tmpCheckParameters EXEC sp_executesql @queryToRun
								SELECT @stepName=Result FROM #tmpCheckParameters

								IF @stepName IS NOT NULL
									begin
										SET @strMessage='Starting job: ' + @jobName
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

										SET @queryToRun='[' + @sqlServerName + '].[msdb].[dbo].[sp_start_job] @job_id=''' + @jobID + ''', @step_name=''' + [dbo].[ufn_getObjectQuoteName](@stepName, 'sql') + ''''
										IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

										BEGIN TRY
											EXEC sp_executesql @queryToRun
											SET @Error = @@ERROR
										END TRY
										BEGIN CATCH
											SET @Error = @@ERROR
											SET @queryToRun= ERROR_MESSAGE()
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0
										END CATCH
										
										IF @Error<>0
												begin
													SET @strMessage = 'Failed in starting job.'
													EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
												end
										ELSE
											begin
												IF @jobQueueID IS NOT NULL AND EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='jobExecutionQueue')
													begin
														SET @queryToRun='UPDATE [dbo].[jobExecutionQueue] 
																				SET   [status] = 4
																					, [execution_date] = GETDATE()
																			WHERE [id] = ' + CAST(@jobQueueID AS [nvarchar])
														EXEC sp_executesql @queryToRun
													end
												--monitorizare job
												IF @watchJob=1
													begin
														WAITFOR DELAY @waitForDelay
														SET @currentRunning=1	
													end
												ELSE
													SET @currentRunning=0
												--daca job-ul e pornit il monitorizez
												WHILE @currentRunning<>0
													begin
														SET @currentRunning=1
														--verific daca job-ul este in curs de executie. daca da, afisez momentele de executie ale job-ului
														EXEC [dbo].[usp_sqlAgentJobCheckStatus] @sqlServerName, @jobName, @strMessage OUT, @currentRunning OUT, @lastExecutionStatus OUT, @lastExecutionDate OUT, @lastExecutionTime OUT, @runningTimeSec OUT, 0, 0, 0
														IF @currentRunning<>0
															begin
																IF ISNULL(@strMessage,'')<>ISNULL(@lastMessage, '')
																	begin
																		IF @watchJob=1
																			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
																		SET @lastMessage=@strMessage
																	end
																IF @jobWasRunning=0
																	SET @jobWasRunning=1
																IF @watchJob=0
																	SET @currentRunning=0
																ELSE
																	WAITFOR DELAY @waitForDelay
															end
													end											
											end
									end
								ELSE
									begin
										SET @strMessage = 'Cannot read the name of the job''s last important step.'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1

										IF OBJECT_ID('#tmpCheckParameters') IS NOT NULL DROP TABLE #tmpCheckParameters
										RETURN 1
									end
							end

						--modific ultimul pas important (refacere)
						SET @queryToRun='[' + @sqlServerName + '].[msdb].[dbo].[sp_update_jobstep] @job_id = ''' + @jobID + ''', @step_id = ' + CAST(@stepToStop AS varchar) + ', @on_success_action = ' + CAST(@lastStepSuccesAction AS varchar) + ', @on_fail_action=' + CAST(@lastStepFailureAction AS varchar)
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						EXEC sp_executesql @queryToRun
						IF @@ERROR<>0
							begin
								SET @strMessage = 'Failed in modifying back job''s execution Stop Step.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1

								IF OBJECT_ID('#tmpCheckParameters') IS NOT NULL DROP TABLE #tmpCheckParameters
								RETURN 1
							end
					end
			end
		ELSE
			begin
				SET @strMessage='Cannot find the Job ID for the specified Job Name.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1

				IF OBJECT_ID('#tmpCheckParameters') IS NOT NULL DROP TABLE #tmpCheckParameters
				RETURN 1
			end
		IF @@ERROR <> 0
			begin
				SET @strMessage= 'Execution failed. Please notify your Database Administrator.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				SET @returnValue=1
			end
	end	
--afisez mesaje despre starea de executie a job-ului 
EXEC [dbo].[usp_sqlAgentJobCheckStatus] @sqlServerName, @jobName, @strMessage OUT, @currentRunning OUT, @lastExecutionStatus OUT, @lastExecutionDate OUT, @lastExecutionTime OUT, @runningTimeSec OUT, 0, 0, 0
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

IF @lastExecutionStatus=0
	begin
		SET @strMessage = 'Execution failed. Please notify your Database Administrator.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		SET @returnValue=1
	end
IF @watchJob=1
	begin
		SET @queryToRun = SUBSTRING(@strMessage, CHARINDEX(N'--Last execution step', @strMessage)+22, LEN(@strMessage))
		SET @queryToRun = SUBSTRING(@queryToRun, CHARINDEX('[', @queryToRun) + 1, LEN(@queryToRun))
		SET @queryToRun = SUBSTRING(@queryToRun, 1, CHARINDEX(']', @queryToRun)-1)
	
		SET @lastExecutionStep=CAST(@queryToRun as int)
		IF @lastExecutionStep<>@stepToStop
			begin
				SET @strMessage = 'The LAST EXECUTED STEP is DIFFERENT from the DEFINED STOP STEP. Please notify your Database Administrator.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1

				SET @returnValue=1
			end
	end
IF @lastExecutionStatus=1
	SET @returnValue=0
-------------------------------------------------------------------------------------------------------------------------
RETURN @returnValue
GO
