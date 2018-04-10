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
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @configParallelJobs	[int]

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
SELECT @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [project_id] = @projectID
		AND [name] = @@SERVERNAME

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsCollectorDescriptor CURSOR LOCAL FAST_FORWARD FOR	SELECT [descriptor]
																FROM
																	(
																		SELECT 'dbo.usp_hcCollectDatabaseDetails' AS [descriptor] UNION ALL
																		SELECT 'dbo.usp_hcCollectSQLServerAgentJobsStatus' AS [descriptor] UNION ALL
																		SELECT 'dbo.usp_hcCollectDiskSpaceUsage' AS [descriptor] UNION ALL
																		SELECT 'dbo.usp_hcCollectErrorlogMessages' AS [descriptor] UNION ALL
																		SELECT 'dbo.usp_hcCollectOSEventLogs' AS [descriptor] UNION ALL
																		SELECT 'dbo.usp_hcCollectEventMessages' AS [descriptor]
																	)X
																WHERE [descriptor] LIKE @collectorDescriptor
OPEN crsCollectorDescriptor
FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Generating queue for : ' + @codeDescriptor
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

		/* save the execution history */
		INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
												 [job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
												 [running_time_sec], [log_message], [status], [event_date_utc])
				SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
						[job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
						[running_time_sec], [log_message], [status], [event_date_utc]
				FROM [dbo].[jobExecutionQueue]
				WHERE [project_id] = @projectID
						AND [instance_id] = @instanceID
						AND [descriptor] = @codeDescriptor
						AND [module] = @module
						AND [status] <> -1

		DELETE FROM [dbo].[jobExecutionQueue]
		WHERE [project_id] = @projectID
				AND [instance_id] = @instanceID
				AND [descriptor] = @codeDescriptor
				AND [module] = @module

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDatabaseDetails'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectDatabaseDetails' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectSQLServerAgentJobsStatus' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectDiskSpaceUsage' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectErrorlogMessages' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectEventMessages' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor], [filter]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], CASE WHEN L.[log_type_name] <> '%' THEN L.[log_type_name] ELSE NULL END,
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectOSEventLogs' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END  + CASE WHEN L.[log_type_name] <> '%' THEN ' (' + L.[log_type_name] + ')' ELSE '' END + ' - ' + @projectCode AS [job_name],
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
								/*
								SELECT 'Application' AS [log_type_name], 1 AS [log_type_id] UNION ALL
								SELECT 'System'		 AS [log_type_name], 2 AS [log_type_id] UNION ALL
								SELECT 'Setup'		 AS [log_type_name], 3 AS [log_type_id] 
								*/
								SELECT '%'		 AS [log_type_name], 3 AS [log_type_id] 
							)L

				--cleaning machine names with multi-instance; keep only one instance, since machine logs will be fetched
				DELETE jeq1
				FROM [dbo].[jobExecutionQueue] jeq1
				INNER JOIN 
					(
						SELECT jeq.[id], ROW_NUMBER() OVER(PARTITION BY cin.[machine_id], jeq.[filter] ORDER BY cin.[instance_id]) AS row_no
						FROM [dbo].[jobExecutionQueue] jeq
						INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = jeq.[project_id] 
																		AND cin.[instance_id] = jeq.[for_instance_id]
						INNER JOIN
							(
								SELECT cin.[machine_id], cin.[machine_name], jeq.[filter], COUNT(*) AS cnt
								FROM [dbo].[jobExecutionQueue] jeq
								INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = jeq.[project_id] 
																				AND cin.[instance_id] = jeq.[for_instance_id]
								WHERE	jeq.[descriptor]=@codeDescriptor
										AND jeq.[instance_id] = @instanceID
										AND jeq.[status]=-1
										AND cin.[project_id] = @projectID
										AND cin.[instance_active] = 1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
								GROUP BY cin.[machine_id], cin.[machine_name], jeq.[filter]		
								HAVING COUNT(*)>1
							)x ON x.[machine_id] = cin.[machine_id] AND x.[machine_name] = cin.[machine_name] AND x.[filter] = jeq.[filter]
						WHERE	jeq.[descriptor]=@codeDescriptor
								AND jeq.[instance_id] = @instanceID
								AND jeq.[status]=-1
								AND cin.[project_id] = @projectID
								AND cin.[instance_active] = 1
								AND cin.[instance_name] LIKE @sqlServerNameFilter
					) y  on jeq1.[id] = y.[id]
				WHERE y.[row_no] <> 1
			end
			
		FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
	end
CLOSE crsCollectorDescriptor
DEALLOCATE crsCollectorDescriptor
GO
