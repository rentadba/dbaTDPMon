SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_monGetSQLAgentFailedJobs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monGetSQLAgentFailedJobs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monGetSQLAgentFailedJobs]
GO

CREATE PROCEDURE [dbo].[usp_monGetSQLAgentFailedJobs]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.02.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@sqlServerVersion		[sysname],
		@SQLMajorVersion		[tinyint],
		@executionLevel			[tinyint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000),
		@minJobCompletionTime	[datetime],
		@jobName				[sysname]
		
/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#statsSQLAgentJobs') IS NOT NULL  DROP TABLE #statsSQLAgentJobs

CREATE TABLE #statsSQLAgentJobs
(
	[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
	[job_name]							[sysname]		NOT NULL,
	[job_completion_status]				[tinyint],
	[last_completion_time]				[datetime],
	[last_completion_time_utc]			[datetime],
	[local_server_date_utc]				[datetime]
)

SET @executionLevel = 0

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 1: Delete existing information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_monGetSQLAgentFailedJobs'


-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 2: Get Instance Details Information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		------------------------------------------------------------------------------------------------------------------------------------------
		SELECT	@minJobCompletionTime = MIN([last_completion_time])
		FROM	[monitoring].[statsSQLAgentJobs]
		WHERE	[instance_id] = @instanceID
				AND [project_id] = @projectID

		SET @minJobCompletionTime = ISNULL(@minJobCompletionTime, CONVERT([datetime], CONVERT([varchar](10), GETDATE(), 120), 120))

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		TRUNCATE TABLE #statsSQLAgentJobs
		
		IF @SQLMajorVersion > 8
			begin
				/* get failed jobs */
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  [job_name]
														, [job_completion_status]
														, [last_completion_time]
														, DATEADD(hh, DATEDIFF(hh, GETDATE(), GETUTCDATE()), [last_completion_time]) AS [last_completion_time_utc]
														, GETUTCDATE() AS [local_server_date_utc]
												FROM (
														SELECT	  @@SERVERNAME AS [instance_name]
																, sj.[name] AS [job_name]
																, sjh.[run_status] AS [job_completion_status]
																, CONVERT([datetime], SUBSTRING(CAST(sjh.[run_date] AS [varchar](8)), 1, 4) + ''-'' + 
																					  SUBSTRING(CAST(sjh.[run_date] AS [varchar](8)), 5, 2) + ''-'' + 
																					  SUBSTRING(CAST(sjh.[run_date] AS [varchar](8)), 7, 2) + '' '' + 
																					  SUBSTRING((REPLICATE(''0'', 6 - LEN(CAST(sjh.[run_time] AS [varchar](6)))) + CAST(sjh.[run_time] AS [varchar](6))), 1, 2) + '':'' + 
																					  SUBSTRING((REPLICATE(''0'', 6 - LEN(CAST(sjh.[run_time] AS [varchar](6)))) + CAST(sjh.[run_time] AS [varchar](6))), 3, 2) + '':'' + 
																					  SUBSTRING((REPLICATE(''0'', 6 - LEN(CAST(sjh.[run_time] AS [varchar](6)))) + CAST(sjh.[run_time] AS [varchar](6))), 5, 2)
																			, 120) AS [last_completion_time]

														FROM msdb.dbo.sysjobs sj
														INNER JOIN 
															(
																/* last job execution failed */
																SELECT [instance_id], [job_id], [run_date], [run_time], [run_status]
																FROM (
																		SELECT    [instance_id], [job_id], [run_status], [run_date], [run_time]
																				, ROW_NUMBER() OVER(PARTITION BY [job_id] ORDER BY [instance_id] DESC) [row_no]
																		FROM [msdb].[dbo].[sysjobhistory]
																		WHERE [step_name] =''(Job outcome)''
																	)X
																WHERE	[run_status] = 0
																		AND [row_no] = 1
															)sjh ON sj.[job_id]=sjh.[job_id]
													)jobs
												WHERE [last_completion_time] >''' + CONVERT([varchar](24), @minJobCompletionTime, 121)  + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				BEGIN TRY
						INSERT	INTO #statsSQLAgentJobs([job_name], [job_completion_status], [last_completion_time], [last_completion_time_utc], [local_server_date_utc])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetSQLAgentFailedJobs'
									, @strMessage
				END CATCH


				DECLARE crsFailedJobs CURSOR LOCAL FAST_FORWARD  FOR	SELECT j.[job_name]
																		FROM #statsSQLAgentJobs j
																		LEFT JOIN [monitoring].[statsSQLAgentJobs] saj ON	saj.[project_id] = @projectID
																															AND saj.[instance_id] = @instanceID
																															AND saj.[job_name] = j.[job_name]
																															AND saj.[last_completion_time] = j.[last_completion_time]
																		WHERE saj.[job_name] IS NULL
				OPEN crsFailedJobs
				FETCH NEXT FROM crsFailedJobs INTO @jobName
				WHILE @@FETCH_STATUS=0
					begin
						/* generating alarm/email event */
						BEGIN TRY
							EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@sqlServerName			= @sqlServerName,
																			@jobName				= @jobName,
																			@logFileLocation		= NULL,
																			@module					= 'monitoring',
																			@sendLogAsAttachment	= 1,
																			@eventType				= 2,
																			@currentlyRunning		= 0,
																			@debugMode				= @debugMode
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						END CATCH
						FETCH NEXT FROM crsFailedJobs INTO @jobName
					end
				CLOSE crsFailedJobs
				DEALLOCATE crsFailedJobs
			end								
				
		/* save results to stats table */
		INSERT INTO [monitoring].[statsSQLAgentJobs]([instance_id], [project_id], [event_date_utc], [job_name], [job_completion_status], [last_completion_time], [last_completion_time_utc], [local_server_date_utc])
				SELECT    @instanceID, @projectID, GETUTCDATE()
						, j.[job_name], j.[job_completion_status], j.[last_completion_time], j.[last_completion_time_utc], j.[local_server_date_utc]
				FROM #statsSQLAgentJobs j
				LEFT JOIN [monitoring].[statsSQLAgentJobs] saj ON	saj.[project_id] = @projectID
																	AND saj.[instance_id] = @instanceID
																	AND saj.[job_name] = j.[job_name]
																	AND saj.[last_completion_time] = j.[last_completion_time]
				WHERE saj.[job_name] IS NULL
								
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
