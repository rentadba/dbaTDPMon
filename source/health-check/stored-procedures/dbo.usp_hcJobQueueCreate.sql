RAISERROR('Create procedure: [dbo].[usp_hcJobQueueCreate]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcJobQueueCreate]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcJobQueueCreate]
GO

CREATE PROCEDURE [dbo].[usp_hcJobQueueCreate]
		@projectCode			[varchar](32)=NULL,
		@module					[varchar](32)='health-check',
		@sqlServerNameFilter	[sysname]='%',
		@collectorDescriptor	[varchar](256)='%',
		@enableXPCMDSHELL		[bit]=1,
	    @recreateMode			[bit] = 0,				/*  1 - existings jobs will be dropped and created based on this stored procedure logic
															0 - jobs definition will be preserved; only status columns will be updated; new jobs are created
														*/
		@debugMode				[bit]=0

/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
SET NOCOUNT ON

DECLARE   @codeDescriptor		[varchar](260)
		, @taskID				[bigint]
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @configParallelJobs	[int]
		, @maxPriorityValue		[int]

DECLARE @jobExecutionQueue TABLE
		(
			[id]					[int]			NOT NULL IDENTITY(1,1),
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[module]				[varchar](32)	NOT NULL,
			[descriptor]			[varchar](256)	NOT NULL,
			[filter]				[sysname]		NULL,
			[for_instance_id]		[smallint]		NOT NULL,
			[job_name]				[sysname]		NOT NULL,
			[job_step_name]			[sysname]		NOT NULL,
			[job_database_name]		[sysname]		NOT NULL,
			[job_command]			[nvarchar](max) NOT NULL,
			[task_id]				[bigint]		NOT NULL,
			[priority]				[int]			NULL
		)

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
SELECT TOP 1 @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [name] = @@SERVERNAME
		--AND [project_id] = @projectID
ORDER BY [id]

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsCollectorDescriptor CURSOR LOCAL FAST_FORWARD FOR	SELECT x.[descriptor], it.[id] AS [task_id]
																FROM
																	(
																		SELECT 'dbo.usp_hcCollectDiskSpaceUsage' AS [descriptor], 1 AS [execution_order] UNION ALL
																		SELECT 'dbo.usp_hcCollectDatabaseDetails' AS [descriptor], 2 AS [execution_order] UNION ALL
																		SELECT 'dbo.usp_hcCollectSQLServerAgentJobsStatus' AS [descriptor], 3 AS [execution_order] UNION ALL
																		SELECT 'dbo.usp_hcCollectErrorlogMessages' AS [descriptor], 4 AS [execution_order] UNION ALL
																		SELECT 'dbo.usp_hcCollectOSEventLogs' AS [descriptor], 5 AS [execution_order] UNION ALL
																		SELECT 'dbo.usp_hcCollectEventMessages' AS [descriptor], 6 AS [execution_order] UNION ALL
																		SELECT 'dbo.usp_hcCollectDatabaseGrowth' AS [descriptor], 7 AS [execution_order]
																	)x
																INNER JOIN [dbo].[appInternalTasks] it ON it.[descriptor] = x.[descriptor]
																WHERE x.[descriptor] LIKE @collectorDescriptor
																ORDER BY [execution_order]
OPEN crsCollectorDescriptor
FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor, @taskID
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Generating queue for : ' + @codeDescriptor
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

		/* save the previous executions statistics */
		EXEC [dbo].[usp_jobExecutionSaveStatistics]	@projectCode		= @projectCode,
													@moduleFilter		= @module,
													@descriptorFilter	= @codeDescriptor

		/* save the execution history */
		INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
												 [job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
												 [running_time_sec], [log_message], [status], [event_date_utc], [task_id], [database_name])
				SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
						[job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
						[running_time_sec], [log_message], [status], [event_date_utc], [task_id], [database_name]
				FROM [dbo].[jobExecutionQueue]
				WHERE [project_id] = @projectID
						AND [instance_id] = @instanceID
						AND [descriptor] = @codeDescriptor
						AND [module] = @module
						AND [status] <> -1

		IF @recreateMode = 1										
			DELETE FROM [dbo].[jobExecutionQueue]
			WHERE [project_id] = @projectID
					AND [instance_id] = @instanceID
					AND [descriptor] = @codeDescriptor
					AND [module] = @module

		DELETE FROM @jobExecutionQueue

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDatabaseDetails'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, 
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END + ' - '  ELSE ' - ' END + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectDatabaseDetails] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @databaseNameFilter = ''%'', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end
			
		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectSQLServerAgentJobsStatus'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, 
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END + ' - '  ELSE ' - ' END + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectSQLServerAgentJobsStatus] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @jobNameFilter = ''%'', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDiskSpaceUsage'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, 
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END + ' - '  ELSE ' - ' END + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectDiskSpaceUsage] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @enableXPCMDSHELL = ' + CAST(@enableXPCMDSHELL AS [varchar]) + ', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectErrorlogMessages'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, 
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END + ' - '  ELSE ' - ' END + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectErrorlogMessages] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectEventMessages'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, 
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END + ' - '  ELSE ' - ' END + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectEventMessages] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND cin.[instance_name] <> @@SERVERNAME
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end
		
		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectOSEventLogs'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id], [filter]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, CASE WHEN L.[log_type_name] <> '%' THEN L.[log_type_name] ELSE NULL END AS [filter],
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END ELSE '' END + CASE WHEN L.[log_type_name] <> '%' THEN ' (' + L.[log_type_name] + ')' ELSE '' END + ' - ' + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectOSEventLogs] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @logNameFilter = ''' + L.[log_type_name] + ''', @enableXPCMDSHELL = ' + CAST(@enableXPCMDSHELL AS [varchar]) + ', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT cin.[instance_id], cin.[instance_name]
								FROM (
										SELECT	  cin.[instance_id], cin.[instance_name]
												, ROW_NUMBER() OVER(PARTITION BY cin.[machine_name] ORDER BY cin.[instance_id]) AS [priority]
										FROM	[dbo].[vw_catalogInstanceNames] cin
										WHERE 	cin.[project_id] = @projectID
												AND cin.[instance_active]=1
												AND cin.[instance_name] LIKE @sqlServerNameFilter
												--AND cin.[instance_name] <> @@SERVERNAME
												AND @configParallelJobs <> 1
								
										UNION ALL

										SELECT @instanceID AS [instance_id], '%' AS [instance_name], 1 AS [priority]
										WHERE @configParallelJobs = 1
									)cin
								WHERE [priority] = 1
							)X,
							(
								SELECT '%'		 AS [log_type_name], 3 AS [log_type_id] 
							)L

				--cleaning machine names with multi-instance; keep only one instance, since machine logs will be fetched
				DELETE jeq1
				FROM @jobExecutionQueue jeq1
				INNER JOIN 
					(
						SELECT jeq.[id], ROW_NUMBER() OVER(PARTITION BY cin.[machine_id], jeq.[filter] ORDER BY cin.[instance_id]) AS row_no
						FROM @jobExecutionQueue jeq
						INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = jeq.[project_id] 
																		AND cin.[instance_id] = jeq.[for_instance_id]
						INNER JOIN
							(
								SELECT cin.[machine_id], cin.[machine_name], jeq.[filter], COUNT(*) AS cnt
								FROM @jobExecutionQueue jeq
								INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = jeq.[project_id] 
																				AND cin.[instance_id] = jeq.[for_instance_id]
								WHERE	jeq.[descriptor]=@codeDescriptor
										AND jeq.[instance_id] = @instanceID
										AND cin.[project_id] = @projectID
										AND cin.[instance_active] = 1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
								GROUP BY cin.[machine_id], cin.[machine_name], jeq.[filter]		
								HAVING COUNT(*)>1
							)x ON x.[machine_id] = cin.[machine_id] AND x.[machine_name] = cin.[machine_name] AND x.[filter] = jeq.[filter]
						WHERE	jeq.[descriptor]=@codeDescriptor
								AND jeq.[instance_id] = @instanceID
								AND cin.[project_id] = @projectID
								AND cin.[instance_active] = 1
								AND cin.[instance_name] LIKE @sqlServerNameFilter
					) y  on jeq1.[id] = y.[id]
				WHERE y.[row_no] <> 1
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDatabaseGrowth'
			begin
				INSERT	INTO @jobExecutionQueue(  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], @taskID, 
								X.[instance_id] AS [for_instance_id], 
								SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + CASE WHEN X.[instance_name] <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(X.[instance_name]) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(X.[instance_name]), '\', '$') ELSE SUBSTRING(UPPER(X.[instance_name]), 1, CHARINDEX('.', UPPER(X.[instance_name]))-1) END + ' - '  ELSE ' - ' END + @projectCode, 1, 128) AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectDatabaseGrowth] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @recreateMode = 0
			begin
				/* preserve any unfinished job and increase its priority */
				UPDATE jeqX
					SET jeqX.[priority] = X.[new_priority]
				FROM  @jobExecutionQueue jeqX
				INNER JOIN (
							SELECT	S.[id], 
									ROW_NUMBER() OVER (ORDER BY jeq.[id]) AS [new_priority]
							FROM [dbo].[jobExecutionQueue] jeq WITH (INDEX([IX_jobExecutionQueue_JobQueue]))
							INNER JOIN @jobExecutionQueue S ON		jeq.[for_instance_id] = S.[for_instance_id]
																AND jeq.[project_id] = S.[project_id]
																AND jeq.[task_id] = S.[task_id]
																--AND jeq.[database_name] = S.[database_name]
																AND jeq.[instance_id] = S.[instance_id]
																AND jeq.[module] = S.[module]
																AND jeq.[descriptor] = S.[descriptor]
																AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
																AND jeq.[job_step_name] = S.[job_step_name]
																AND jeq.[job_database_name] = S.[job_database_name]	
							WHERE [status] = -1 /* previosly not completed jobs */
							) X ON jeqX.[id] = X.[id]

				UPDATE @jobExecutionQueue SET [priority] = 0 WHERE [priority] IS NULL;

				SELECT @maxPriorityValue = MAX([priority])	
				FROM @jobExecutionQueue
						
				SET @maxPriorityValue = ISNULL(@maxPriorityValue, 0)

				/* assign priorities to current generated queue */
				UPDATE jeqX
					SET jeqX.[priority] = X.[new_priority]
				FROM  @jobExecutionQueue jeqX
				INNER JOIN (
							SELECT	[id], 
									@maxPriorityValue + ROW_NUMBER() OVER (ORDER BY [id]) AS [new_priority]
							FROM @jobExecutionQueue 
							WHERE [priority] IS NULL
							) X ON jeqX.[id] = X.[id] 

				/* reset current jobs state */
				UPDATE jeq
					SET   jeq.[execution_date] = NULL
						, jeq.[running_time_sec] = NULL
						, jeq.[log_message] = NULL
						, jeq.[status] = -1
						, jeq.[priority] = S.[priority]
						, jeq.[job_id] = NULL
						, jeq.[event_date_utc] = GETUTCDATE()
				FROM [dbo].[jobExecutionQueue] jeq WITH (INDEX([IX_jobExecutionQueue_JobQueue]))
				INNER JOIN @jobExecutionQueue S ON		jeq.[for_instance_id] = S.[for_instance_id]
													AND jeq.[project_id] = S.[project_id]
													AND jeq.[task_id] = S.[task_id]
													--AND jeq.[database_name] = S.[database_name]
													AND jeq.[instance_id] = S.[instance_id]
													AND jeq.[module] = S.[module]
													AND jeq.[descriptor] = S.[descriptor]
													AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
													AND jeq.[job_step_name] = S.[job_step_name]
													AND jeq.[job_database_name] = S.[job_database_name]			
			end
		
		------------------------------------------------------------------------------------------------------------------------------------------
		/* if recreate mode = 1, set default priority */
		IF @recreateMode = 1
			UPDATE jeqX
					SET jeqX.[priority] = X.[new_priority]
			FROM  @jobExecutionQueue jeqX
			INNER JOIN (
						SELECT	[id], 
								ROW_NUMBER() OVER (ORDER BY [id]) AS [new_priority]
						FROM @jobExecutionQueue 
						) X ON jeqX.[id] = X.[id] 

		INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor], [task_id]
												, [for_instance_id], [job_name], [job_step_name], [job_database_name]
												, [job_command], [priority])
				SELECT	  S.[instance_id], S.[project_id], S.[module], S.[descriptor], S.[task_id]
						, S.[for_instance_id]
						, REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_')	/* manage special characters in job names */
						, S.[job_step_name], S.[job_database_name]
						, S.[job_command], S.[priority]
				FROM @jobExecutionQueue S
				LEFT JOIN [dbo].[jobExecutionQueue] jeq ON		jeq.[for_instance_id] = S.[for_instance_id]
															AND jeq.[project_id] = S.[project_id]
															AND jeq.[task_id] = S.[task_id]
															--AND jeq.[database_name] = S.[database_name]
															AND jeq.[instance_id] = S.[instance_id]
															AND jeq.[module] = S.[module]
															AND jeq.[descriptor] = S.[descriptor]
															AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
															AND jeq.[job_step_name] = S.[job_step_name]
															AND jeq.[job_database_name] = S.[job_database_name]			
				WHERE	jeq.[job_name] IS NULL
			
		FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor, @taskID
	end
CLOSE crsCollectorDescriptor
DEALLOCATE crsCollectorDescriptor
GO
