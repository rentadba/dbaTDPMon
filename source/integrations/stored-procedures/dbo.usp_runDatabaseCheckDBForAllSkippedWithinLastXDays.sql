RAISERROR('Create procedure: [dbo].[usp_runDatabaseCheckDBForAllSkippedWithinLastXDays]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_runDatabaseCheckDBForAllSkippedWithinLastXDays]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_runDatabaseCheckDBForAllSkippedWithinLastXDays]
GO

CREATE PROCEDURE [dbo].[usp_runDatabaseCheckDBForAllSkippedWithinLastXDays]
		@sqlServerNameFilter		[sysname]	= '%',
		@dbccCheckDBAgeDays			[int]		= NULL,
		@maxDOP						[smallint]	= 1,
		@waitForDelay				[varchar](8)= '00:00:05',
		@parallelJobs				[int]		= NULL,
		@maxRunningTimeInMinutes	[smallint]	= 0,
		@skipObjectsList			[nvarchar](1024) = NULL,
		@executeProjectBased		[bit]		= 0,
		@onlyForProduction			[bit]		= 0,
		@debugMode					[bit]		= 0
AS


-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan & Health-Check
-- Description		 : Run Consistency Checks for all databases with a time limit 
--					   Detect all databases with no dbcc checkdb in the last @dbccCheckDBAgeDays days (based on internal dictionary)
--					   Generate internal jobs and execute them using @parallelJobs degree of parallelism
--					   Run the jobs only for @maxRunningTimeInMinutes duration. 
--					   Execute this daily and will even distribute your dbcc checkdb across week/month...
-- ============================================================================

/*
--sample call
EXEC [dbo].[usp_runDatabaseCheckDBForAllSkippedWithinLastXDays]	@sqlServerNameFilter	= '%',
																@dbccCheckDBAgeDays		= 0,
																@maxDOP					= 1,
																@waitForDelay			= '00:00:05',
																@parallelJobs			= 2,
																@maxRunningTimeInMinutes= 360,
																@skipObjectsList		= NULL,
																@executeProjectBased	= 0,
																@onlyForProduction		= 1,
																@debugMode				= 0
*/
SET NOCOUNT ON

DECLARE @module				[varchar](32),
		@codeDescriptor		[varchar](256),
		@taskName			[varchar](256),
		@taskID				[bigint], 
		@instanceID			[smallint],
		@strMessage			[varchar](512),
		@projectCode		[sysname],
		@stopTimeLimit		[datetime],
		@remainingRunTime	[int]

SET @module = 'automation-dbcc-checkdb'
SET @codeDescriptor = 'dbo.usp_mpDatabaseConsistencyCheck'
SET @taskName = 'Database Consistency Check'

DECLARE @jobExecutionQueue TABLE
		(
			[id]						[int]			NOT NULL IDENTITY(1,1),
			[instance_id]				[smallint]		NOT NULL,
			[project_id]				[smallint]		NOT NULL,
			[module]					[varchar](32)	NOT NULL,
			[descriptor]				[varchar](256)	NOT NULL,
			[for_instance_id]			[smallint]		NOT NULL,
			[job_name]					[sysname]		NOT NULL,
			[job_step_name]				[sysname]		NOT NULL,
			[job_database_name]			[sysname]		NOT NULL,
			[job_command]				[nvarchar](max) NOT NULL,
			[task_id]					[bigint]		NULL,
			[database_name]				[sysname]		NULL,
			[last_dbcc_checkdb_time]	[datetime]		NULL,
			[size_mb]					[numeric](18,3) NULL,
			[is_production]				[bit]			NULL,
			[prev_run_time_minutes]		[int]			NULL,
			[priority]					[int]			NULL
		)

---------------------------------------------------------------------------------------------
--determine when to stop current optimization task, based on @maxRunningTimeInMinutes value
---------------------------------------------------------------------------------------------
IF ISNULL(@maxRunningTimeInMinutes, 0)=0
	SET @stopTimeLimit = CONVERT([datetime], '9999-12-31 23:23:59', 120)
ELSE
	SET @stopTimeLimit = DATEADD(minute, @maxRunningTimeInMinutes, GETDATE())

------------------------------------------------------------------------------------------------------------------------------------------
IF @dbccCheckDBAgeDays IS NULL
	begin
		BEGIN TRY
			SELECT	@dbccCheckDBAgeDays = [value]
			FROM	[report].[htmlOptions]
			WHERE	[name] = N'User Database DBCC CHECKDB Age (days)'
					AND [module] = 'health-check'
		END TRY
		BEGIN CATCH
			SET @dbccCheckDBAgeDays = 7
		END CATCH
		SET @dbccCheckDBAgeDays = ISNULL(@dbccCheckDBAgeDays, 7)
	end

------------------------------------------------------------------------------------------------------------------------------------------
SELECT TOP 1 @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [name] = @@SERVERNAME
ORDER BY [id]


------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Save old execution statistics...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

/* save the previous executions statistics */
EXEC [dbo].[usp_jobExecutionSaveStatistics]	@projectCode		= NULL,
											@moduleFilter		= @module,
											@descriptorFilter	= @codeDescriptor

/* save the execution history */
INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
										 [job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
										 [running_time_sec], [log_message], [status], [event_date_utc], [database_name])
		SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
				[job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
				[running_time_sec], [log_message], [status], [event_date_utc], [database_name]
		FROM [dbo].[jobExecutionQueue] jeq
		WHERE	[instance_id] = @instanceID
				AND [descriptor] = @codeDescriptor
				AND [module] = @module
				AND [status] <> -1
				AND (   @skipObjectsList IS NULL
						OR (    @skipObjectsList IS NOT NULL	
							AND (
								SELECT COUNT(*)
								FROM [dbo].[ufn_getTableFromStringList](@skipObjectsList, ',') X
								WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
							) = 0
						)
					)

/* clean the execution history */
DELETE jeq
FROM [dbo].[jobExecutionQueue]  jeq
WHERE	[instance_id] = @instanceID
		AND [descriptor] = @codeDescriptor
		AND [module] = @module
		AND (   @skipObjectsList IS NULL
				OR (    @skipObjectsList IS NOT NULL	
					AND (
						SELECT COUNT(*)
						FROM [dbo].[ufn_getTableFromStringList](@skipObjectsList, ',') X
						WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
					) = 0
				)
			)

DELETE FROM @jobExecutionQueue

------------------------------------------------------------------------------------------------------------------------------------------
/* generate internal jobs for DBCC CHECKDB for all projects/databases/instances */
SET @strMessage='Generating queue for : ' + @codeDescriptor
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

SET @taskID = NULL
SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = @taskName

INSERT	INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
								 , [for_instance_id], [job_name], [job_step_name], [job_database_name]
								 , [job_command], [task_id], [database_name], [last_dbcc_checkdb_time], [size_mb], [is_production])
		SELECT	@instanceID AS [instance_id], X.[project_id], 
				@module AS [module], 
				@codeDescriptor AS [descriptor],
				X.[for_instance_id], 
				SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - ' + @taskName + CASE WHEN X.[for_instance_name] <> @@SERVERNAME THEN ' - ' + REPLACE(X.[for_instance_name], '\', '$') + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
				'Run'		AS [job_step_name],
				DB_NAME()	AS [job_database_name],
				'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName	= ''' + X.[for_instance_name] + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 1, @flgOptions = 3, @maxDOP	= ' + CAST(@maxDOP AS [nvarchar]) + N', @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
				@taskID, [database_name], 
				X.[last_dbcc checkdb_time], X.[size_mb], X.[is_production] 
		FROM
			(
				SELECT	sdd.[project_id], sdd.[instance_id] AS [for_instance_id], 
						sdd.[instance_name] AS [for_instance_name],
						sdd.[database_id], sdd.[database_name], 
						CONVERT([datetime], CONVERT([varchar](10), sdd.[last_dbcc checkdb_time], 120), 120) [last_dbcc checkdb_time], 
						cp.[is_production],
						sdd.[size_mb]
				FROM [health-check].[vw_statsDatabaseDetails] sdd
				INNER JOIN [dbo].[catalogDatabaseNames] cdn ON sdd.[project_id] = cdn.[project_id] AND sdd.[catalog_database_id] = cdn.[id]
				INNER JOIN [dbo].[catalogProjects] cp ON sdd.[project_id] = cp.[id]
				LEFT JOIN [health-check].[statsDatabaseAlwaysOnDetails] sdaod ON sdaod.[instance_id] = cdn.[instance_id] AND sdaod.[catalog_database_id] = cdn.[id]
																				AND sdaod.[role_desc] = 'SECONDARY'
																				AND sdaod.readable_secondary_replica = 'NO'
				WHERE	cdn.[active] = 1
						AND cp.[active] = 1
						AND sdd.[instance_name] LIKE @sqlServerNameFilter
						AND sdd.[is_snapshot] = 0
						AND cdn.[name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
						AND cdn.[state_desc] IN  ('ONLINE', 'READ ONLY')
						AND DATEDIFF(day, sdd.[last_dbcc checkdb_time], GETDATE()) >= @dbccCheckDBAgeDays
						AND sdaod.[id] IS NULL
						AND (   (@onlyForProduction = 1 AND cp.[is_production] = 1)
							 OR @onlyForProduction = 0
							 OR @onlyForProduction IS NULL
							)
			)X


------------------------------------------------------------------------------------------------------------------------------------------
/* get previously execution run statistics */
SET @strMessage='Optimizing tasks priority in queue...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

IF OBJECT_ID('tempdb..#jobExecutionHistoryStats') IS NOT NULL DROP TABLE #jobExecutionHistoryStats
SELECT	[project_id], [for_instance_id], [database_name], 
		ROUND(AVG([running_time_sec]), 0)  AS [running_time_sec]
INTO #jobExecutionHistoryStats
FROM [dbo].[vw_jobExecutionHistory]
WHERE [module] = 'automation-dbcc-checkdb'
	AND [descriptor] = 'dbo.usp_mpDatabaseConsistencyCheck'
	AND [status] = 1
	AND [database_name] IN (SELECT [database_name] FROM @jobExecutionQueue)
GROUP BY [project_id], [for_instance_id], [database_name]

UPDATE jeq
	SET jeq.[prev_run_time_minutes] = ROUND(jehs.[running_time_sec] / 60, 0)
FROM @jobExecutionQueue jeq
INNER JOIN #jobExecutionHistoryStats jehs ON	jehs.[project_id] = jeq.[project_id]
												AND jehs.[for_instance_id] = jeq.[for_instance_id]
												AND jehs.[database_name] = jeq.[database_name]


/* optimize the queue priority based on runtime duration set and previously average execution times */
/* will take into account the degree of parallelism set */
DECLARE @id				[int],
		@runTime		[int],
		@priority		[int]

SET @remainingRunTime = @maxRunningTimeInMinutes * @maxDOP 
SET @priority = 1
DECLARE crsOptimizeQueue CURSOR READ_ONLY FAST_FORWARD FOR	SELECT [id], [prev_run_time_minutes]
															FROM @jobExecutionQueue
															WHERE [prev_run_time_minutes] IS NOT NULL
															ORDER BY [is_production] DESC, [last_dbcc_checkdb_time], [prev_run_time_minutes] DESC
OPEN crsOptimizeQueue
FETCH NEXT FROM crsOptimizeQueue INTO @id, @runTime
WHILE @@FETCH_STATUS=0 AND @remainingRunTime > 0
	begin
		IF @runTime <= @remainingRunTime AND @runTime <= @maxRunningTimeInMinutes
			begin
				UPDATE @jobExecutionQueue
					SET [priority] = @priority
				WHERE [id] = @id

				SET @priority = @priority + 1
				SET @remainingRunTime = @remainingRunTime - @runTime
			end

		FETCH NEXT FROM crsOptimizeQueue INTO @id, @runTime
	end
CLOSE crsOptimizeQueue
DEALLOCATE crsOptimizeQueue

/* set default priority for the remaining items, based on run_time, size and production flag */
UPDATE jeq
	SET jeq.[priority] = @priority -1 + X.[priority]
FROM @jobExecutionQueue jeq
INNER JOIN	(
			 SELECT [id], ROW_NUMBER() OVER(ORDER BY [is_production] DESC, 
													CASE WHEN [prev_run_time_minutes] IS NOT NULL THEN [prev_run_time_minutes] 
																								  ELSE [size_mb] 
													END DESC
											) [priority]
			 FROM @jobExecutionQueue
			 WHERE [priority] IS NULL
			)X ON X.[id] = jeq.[id]

IF @debugMode = 1 SELECT * FROM @jobExecutionQueue

------------------------------------------------------------------------------------------------------------------------------------------
INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
									   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
									   , [job_command], [task_id], [database_name], [priority])
SELECT DISTINCT
		  S.[instance_id], S.[project_id], S.[module], S.[descriptor]
		, S.[for_instance_id]
		, REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_')	/* manage special characters in job names */
		, S.[job_step_name], S.[job_database_name]
		, S.[job_command]
		, S.[task_id], S.[database_name], S.[priority]
FROM @jobExecutionQueue S
LEFT JOIN [dbo].[jobExecutionQueue] jeq ON		jeq.[for_instance_id] = S.[for_instance_id]
											AND jeq.[project_id] = S.[project_id]
											AND jeq.[task_id] = S.[task_id]
											AND jeq.[database_name] = S.[database_name]
											AND jeq.[instance_id] = S.[instance_id]
											AND jeq.[module] = S.[module]
											AND jeq.[descriptor] = S.[descriptor]
											AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
											AND jeq.[job_step_name] = S.[job_step_name]
											AND jeq.[job_database_name] = S.[job_database_name]
WHERE	jeq.[job_name] IS NULL
		AND (     @skipObjectsList IS NULL
				OR (    @skipObjectsList IS NOT NULL	
						AND (
							SELECT COUNT(*)
							FROM [dbo].[ufn_getTableFromStringList](@skipObjectsList, ',') X
							WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
						) = 0
					)
				)

------------------------------------------------------------------------------------------------------------------------------------------
/* running internal jobs for DBCC CHECKDB for all projects/databases/instances */
SET @strMessage='Running queue...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

IF @executeProjectBased = 1
	begin
		/* run project by project, pending on inter project task priority */
		DECLARE crsProjectsDatabaseJobs CURSOR READ_ONLY FAST_FORWARD FOR	SELECT TOP 1000000 [project_code]
																			FROM (
																					SELECT [project_code], MIN([priority]) [priority]
																					FROM (
																							SELECT [project_code], ROW_NUMBER() OVER(ORDER BY [priority]) [priority]
																							FROM [dbo].[vw_jobExecutionQueue]
																							WHERE	[module] = @module
																									AND [descriptor] = @codeDescriptor
																									AND [status] = -1
																						)X
																					GROUP BY [project_code]
																				)Y
																			ORDER BY [priority]
		OPEN crsProjectsDatabaseJobs
		FETCH NEXT FROM crsProjectsDatabaseJobs INTO @projectCode
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @remainingRunTime = DATEDIFF(minute, GETDATE(), @stopTimeLimit)

				EXEC dbo.usp_jobQueueExecute	@projectCode				= @projectCode,
												@moduleFilter				= @module,
												@descriptorFilter			= @codeDescriptor,
												@waitForDelay				= @waitForDelay,
												@parallelJobs				= @parallelJobs,
												@maxRunningTimeInMinutes	= @remainingRunTime,
												@debugMode					= @debugMode

				EXEC [dbo].[usp_hcCollectDatabaseDetails]	@projectCode			= @projectCode,
															@sqlServerNameFilter	= DEFAULT,
															@databaseNameFilter		= DEFAULT,
															@debugMode				= @debugMode	

				FETCH NEXT FROM crsProjectsDatabaseJobs INTO @projectCode
			end
		CLOSE crsProjectsDatabaseJobs
		DEALLOCATE crsProjectsDatabaseJobs
	end
ELSE
	begin
		/* run queue regardless of project, based on database task priority */
		EXEC dbo.usp_jobQueueExecute	@projectCode				= NULL,
										@moduleFilter				= @module,
										@descriptorFilter			= @codeDescriptor,
										@waitForDelay				= @waitForDelay,
										@parallelJobs				= @parallelJobs,
										@maxRunningTimeInMinutes	= @remainingRunTime,
										@debugMode					= @debugMode

		/* update internal catalogs */
		DECLARE crsProjectsDatabaseJobs CURSOR READ_ONLY FAST_FORWARD FOR	SELECT DISTINCT [project_code]
																			FROM [dbo].[vw_jobExecutionQueue]
																			WHERE	[module] = @module
																					AND [descriptor] = @codeDescriptor
																					AND [status] = 1
																			ORDER BY [project_code]
		OPEN crsProjectsDatabaseJobs
		FETCH NEXT FROM crsProjectsDatabaseJobs INTO @projectCode
		WHILE @@FETCH_STATUS=0
			begin
				EXEC [dbo].[usp_hcCollectDatabaseDetails]	@projectCode			= @projectCode,
															@sqlServerNameFilter	= DEFAULT,
															@databaseNameFilter		= DEFAULT,
															@debugMode				= @debugMode	

				FETCH NEXT FROM crsProjectsDatabaseJobs INTO @projectCode
			end
		CLOSE crsProjectsDatabaseJobs
		DEALLOCATE crsProjectsDatabaseJobs
	end
GO
