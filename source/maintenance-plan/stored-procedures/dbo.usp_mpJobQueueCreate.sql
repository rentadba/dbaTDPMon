RAISERROR('Create procedure: [dbo].[usp_mpJobQueueCreate]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpJobQueueCreate]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpJobQueueCreate]
GO

CREATE PROCEDURE [dbo].[usp_mpJobQueueCreate]
		@projectCode			[varchar](32)=NULL,
		@module					[varchar](32)='maintenance-plan',
		@sqlServerNameFilter	[sysname]='%',
		@jobDescriptor			[varchar](256)='%',		/*	dbo.usp_mpDatabaseConsistencyCheck
															dbo.usp_mpDatabaseOptimize
															dbo.usp_mpDatabaseShrink
															dbo.usp_mpDatabaseBackup(Data)
															dbo.usp_mpDatabaseBackup(Log)
														*/
		@flgActions				[int] = 16383,			/*	   1	Weekly: Database Consistency Check - only once a week on Saturday
															   2	Daily: Allocation Consistency Check
															   4	Weekly: Tables Consistency Check - only once a week on Sunday
															   8	Weekly: Reference Consistency Check - only once a week on Sunday
															  16	Monthly: Perform Correction to Space Usage - on the first Saturday of the month
															  32	Daily: Rebuild Heap Tables - only for SQL versions +2K5
															  64	Daily: Rebuild or Reorganize Indexes
															 128	Daily: Update Statistics 
															 256	Weekly: Shrink Database (TRUNCATEONLY) - only once a week on Sunday
															 512	Monthly: Shrink Log File - on the first Saturday of the month 
															1024	Daily: Backup User Databases (diff) 
															2048	Weekly: User Databases (full) - only once a week on Saturday 
															4096	Weekly: System Databases (full) - only once a week on Saturday 
															8192	Hourly: Backup User Databases Transaction Log 
														*/
		@skipDatabasesList		[nvarchar](1024) = NULL,/* databases list, comma separated, to be excluded from maintenance */
	    @recreateMode			[bit] = 0,				/*  1 - existings jobs will be dropped and created based on this stored procedure logic
															0 - jobs definition will be preserved; only status columns will be updated; new jobs are created, for newly discovered databases
														*/
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 23.08.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
SET NOCOUNT ON

DECLARE   @codeDescriptor		[varchar](260)
		, @taskID				[bigint]
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @featureflgActions	[int]
		, @forInstanceID		[int]
		, @forSQLServerName		[sysname]
		, @maxPriorityValue		[int]
		, @retryAttempts		[tinyint]
		, @isAzureSQLDatabase	[bit]
		, @addNewDatabasesToProject [bit]
		, @queryToRun			[nvarchar](1024)

DECLARE @jobExecutionQueue TABLE
		(
			[id]					[int]			NOT NULL IDENTITY(1,1),
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[module]				[varchar](32)	NOT NULL,
			[descriptor]			[varchar](256)	NOT NULL,
			[for_instance_id]		[smallint]		NOT NULL,
			[job_name]				[sysname]		NOT NULL,
			[job_step_name]			[sysname]		NOT NULL,
			[job_database_name]		[sysname]		NOT NULL,
			[job_command]			[nvarchar](max) NOT NULL,
			[task_id]				[bigint]		NULL,
			[database_name]			[sysname]		NULL,
			[priority]				[int]			NULL
		)

DECLARE @agNonReadableSecondaryReplicaDatabases TABLE
	(
		[project_id]		[smallint]	NOT NULL,
		[instance_id]		[smallint]	NOT NULL,
		[database_name]		[sysname]	NOT NULL
	)

DECLARE @mpCodeDescriptors TABLE
	(
		[descriptor]		[varchar](256)	NOT NULL,
		[priority]			[tinyint]		NOT NULL
	)

------------------------------------------------------------------------------------------------------------------------------------------
INSERT	INTO @mpCodeDescriptors([descriptor], [priority])
		SELECT 'dbo.usp_mpDatabaseBackup(Data)' AS [descriptor], 1 AS [priority] UNION ALL
		SELECT 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor], 2 AS [priority] UNION ALL
		SELECT 'dbo.usp_mpDatabaseOptimize' AS [descriptor], 3 AS [priority] UNION ALL
		SELECT 'dbo.usp_mpDatabaseBackup(Log)' AS [descriptor], 4 AS [priority] UNION ALL
		SELECT 'dbo.usp_mpDatabaseShrink' AS [descriptor], 5 AS [priority]

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
SELECT TOP 1 @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [name] = @@SERVERNAME
		--AND [project_id] = @projectID
ORDER BY [id]

IF @skipDatabasesList ='' SET @skipDatabasesList=NULL

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR	SELECT	cin.[instance_id], cin.[instance_name],
																	CASE WHEN cin.[engine] IN (5, 6) THEN 1 ELSE 0 END AS [isAzureSQLDatabase]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName, @isAzureSQLDatabase
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @forSQLServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		--refresh current server information on internal metadata tables
		SET @addNewDatabasesToProject = 0
		IF @forSQLServerName = @@SERVERNAME AND NOT EXISTS (SELECT [name] FROM sys.schemas WHERE [name]='health-check')
			SET @addNewDatabasesToProject = 1

		EXEC [dbo].[usp_refreshMachineCatalogs]	@projectCode	= @projectCode,
												@sqlServerName	= @forSQLServerName,
												@addNewDatabasesToProject = @addNewDatabasesToProject,
												@debugMode		= @debugMode

		DECLARE crsCollectorDescriptor CURSOR LOCAL FAST_FORWARD FOR	SELECT [descriptor]
																		FROM @mpCodeDescriptors x
																		WHERE (   [descriptor] LIKE @jobDescriptor
																				OR ISNULL(CHARINDEX([descriptor], @jobDescriptor), 0) <> 0
																				)	
																		ORDER BY [priority]

		OPEN crsCollectorDescriptor
		FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
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
														 [running_time_sec], [log_message], [status], [event_date_utc], [database_name])
						SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
								[job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
								[running_time_sec], [log_message], [status], [event_date_utc], [database_name]
						FROM [dbo].[jobExecutionQueue] jeq
						WHERE [project_id] = @projectID
								AND [instance_id] = @instanceID
								AND [descriptor] = @codeDescriptor
								AND [for_instance_id] = @forInstanceID 
								AND [module] = @module
								AND [status] <> -1
								AND (   @skipDatabasesList IS NULL
									 OR (    @skipDatabasesList IS NOT NULL	
										 AND (
											  SELECT COUNT(*)
											  FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
											  WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
											) = 0
										)
									)

				/* clean the execution history: delete jobs for non-active databases */
				DELETE jeq
				FROM [dbo].[jobExecutionQueue] jeq
				INNER JOIN [dbo].[catalogDatabaseNames] cdn ON	cdn.[project_id] = jeq.[project_id] 
																AND cdn.[instance_id] = jeq.[for_instance_id] 
																AND cdn.[name] = jeq.[database_name]
				WHERE	jeq.[project_id] = @projectID
						AND jeq.[instance_id] = @instanceID
						AND jeq.[descriptor] = @codeDescriptor
						AND jeq.[for_instance_id] = @forInstanceID 
						AND jeq.[module] = @module
						AND jeq.[status] <> -1
						AND cdn.[active] = 0
						AND (   @skipDatabasesList IS NULL
								OR (    @skipDatabasesList IS NOT NULL	
									AND (
										SELECT COUNT(*)
										FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
										WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
									) = 0
								)
							)

				IF @recreateMode = 1										
					DELETE jeq
					FROM [dbo].[jobExecutionQueue]  jeq
					WHERE [project_id] = @projectID
							AND [instance_id] = @instanceID
							AND [descriptor] = @codeDescriptor
							AND [for_instance_id] = @forInstanceID 
							AND [module] = @module
							AND (   @skipDatabasesList IS NULL
								 OR (    @skipDatabasesList IS NOT NULL	
									 AND (
										  SELECT COUNT(*)
										  FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
										  WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
										) = 0
									)
								)


				DELETE FROM @jobExecutionQueue

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseConsistencyCheck'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Database Consistency Check - only once a week on Saturday */
						IF @flgActions & 1 = 1 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Database Consistency Check', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Database Consistency Check'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Database Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName	= ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 1, @flgOptions = 3, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
								end

						/*-------------------------------------------------------------------*/
						/* Daily: Allocation Consistency Check */
						/* when running DBCC CHECKDB, skip running DBCC CHECKALLOC*/
						IF [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Database Consistency Check', GETDATE()) = 1
							SET @featureflgActions = 8
						ELSE
							SET @featureflgActions = 12

						IF @flgActions & 2 = 2 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Allocation Consistency Check', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Allocation Consistency Check'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Allocation Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [nvarchar]) + N', @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Weekly: Tables Consistency Check - only once a week on Sunday*/
						IF @flgActions & 4 = 4 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Tables Consistency Check', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Tables Consistency Check'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Tables Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 2, @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Weekly: Reference Consistency Check - only once a week on Sunday*/
						IF @flgActions & 8 = 8 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Reference Consistency Check', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Reference Consistency Check'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Reference Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE', 'READ ONLY')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Monthly: Perform Correction to Space Usage - on the first Saturday of the month */
						IF @flgActions & 16 = 16 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Perform Correction to Space Usage', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Perform Correction to Space Usage'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Perform Correction to Space Usage' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 64, @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end
					end


				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseOptimize'
					begin
						/*-------------------------------------------------------------------*/
						/* Daily: Rebuild Heap Tables - only for SQL versions +2K5*/
						IF @flgActions & 32 = 32 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Rebuild Heap Tables', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Rebuild Heap Tables'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Rebuild Heap Tables' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @defragIndexThreshold = DEFAULT, @rebuildIndexThreshold = DEFAULT, @pageThreshold = DEFAULT, @rebuildIndexPageCountLimit = DEFAULT, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Daily: Rebuild or Reorganize Indexes*/			
						IF @flgActions & 64 = 64 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Rebuild or Reorganize Indexes', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Rebuild or Reorganize Indexes'

								SET @featureflgActions = 3
								
								IF @flgActions & 128 = 128 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Update Statistics', GETDATE()) = 1 /* Daily: Update Statistics */
									SET @featureflgActions = 11

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Rebuild or Reorganize Indexes' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [varchar]) + ', @flgOptions = DEFAULT, @defragIndexThreshold = DEFAULT, @rebuildIndexThreshold = DEFAULT, @pageThreshold = DEFAULT, @rebuildIndexPageCountLimit = DEFAULT, @statsSamplePercent = DEFAULT, @statsAgeDays = DEFAULT, @statsChangePercent = DEFAULT, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Daily: Update Statistics */
						IF @flgActions & 128 = 128 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Update Statistics', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Update Statistics'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Update Statistics' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 8, @flgOptions = DEFAULT, @statsSamplePercent = DEFAULT, @statsAgeDays = DEFAULT, @statsChangePercent = DEFAULT, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseShrink'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Shrink Database (TRUNCATEONLY) - only once a week on Sunday*/
						IF @flgActions & 256 = 256 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Shrink Database (TRUNCATEONLY)', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Shrink Database (TRUNCATEONLY)'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Shrink Database (TRUNCATEONLY)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseShrink] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @flgActions = 2, @flgOptions = 1, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Monthly: Shrink Log File - on the first Saturday of the month */
						IF @flgActions & 512 = 512 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, @codeDescriptor, 'Shrink Log File', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = @codeDescriptor AND [task_name] = 'Shrink Log File'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Shrink Log File' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseShrink] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @flgActions = 1, @flgOptions = 0, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND [state_desc] IN  ('ONLINE')
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Data)'
					begin
						/*-------------------------------------------------------------------*/
						/* Daily: Backup User Databases (diff) */
						IF @flgActions & 1024 = 1024 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', GETDATE()) = 1
							AND NOT (@flgActions & 2048 = 2048 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)', GETDATE()) = 1)
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = 'dbo.usp_mpDatabaseBackup' AND [task_name] = 'User Databases (diff)'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup User Databases (diff)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 2, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Weekly: User Databases (full) - only once a week on Saturday */
						IF @flgActions & 2048 = 2048 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = 'dbo.usp_mpDatabaseBackup' AND [task_name] = 'User Databases (full)'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup User Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end

						/*-------------------------------------------------------------------*/
						/* Weekly: System Databases (full) - only once a week on Saturday */
						IF @flgActions & 4096 = 4096 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = 'dbo.usp_mpDatabaseBackup' AND [task_name] = 'System Databases (full)'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup System Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] IN ('master', 'model', 'msdb', 'distribution')														
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Log)'
					begin
						/*-------------------------------------------------------------------*/
						/* Hourly: Backup User Databases Transaction Log */
						IF @flgActions & 8192 = 8192 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', GETDATE()) = 1
							begin
								SET @taskID = NULL
								SELECT @taskID = [id] FROM [dbo].[appInternalTasks] WHERE [descriptor] = 'dbo.usp_mpDatabaseBackup' AND [task_name] = 'User Databases Transaction Log'

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command], [task_id], [database_name])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup User Databases (log)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@forSQLServerName) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@forSQLServerName), '\', '$') ELSE SUBSTRING(UPPER(@forSQLServerName), 1, CHARINDEX('.', UPPER(@forSQLServerName))-1) END + ' ' ELSE ' - ' END + '(dbid=' + CAST(X.[database_id] AS [nvarchar]) + ') - ' + [dbo].[ufn_getObjectQuoteName](X.[database_name], 'quoted'), 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 4, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar]),
										@taskID, [database_name]
								FROM
									(
										SELECT	  [dbo].[ufn_getObjectQuoteName]([name], 'sql') AS [database_name]
												, [database_id]
										FROM [dbo].[catalogDatabaseNames] cdn
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')	
												AND (     @isAzureSQLDatabase = 0
													 OR (    @isAzureSQLDatabase = 1
														 AND EXISTS (
																	 SELECT 1
																	 FROM sys.servers ss
																	 WHERE ss.[catalog] = cdn.[name] COLLATE SQL_Latin1_General_CP1_CI_AS
																	)
														)
													)
									)X
							end
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
											ROW_NUMBER() OVER (ORDER BY jeq.[priority], jeq.[id]) AS [new_priority]
									FROM [dbo].[jobExecutionQueue] jeq WITH (INDEX([IX_jobExecutionQueue_JobQueue]))
									INNER JOIN @jobExecutionQueue S ON		jeq.[for_instance_id] = S.[for_instance_id]
																		AND jeq.[project_id] = S.[project_id]
																		AND jeq.[task_id] = S.[task_id]
																		AND jeq.[database_name] = S.[database_name]
																		AND jeq.[instance_id] = S.[instance_id]
																		AND jeq.[module] = S.[module]
																		AND jeq.[descriptor] = S.[descriptor]
																		AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
																		AND jeq.[job_step_name] = S.[job_step_name]
																		AND jeq.[job_database_name] = S.[job_database_name]	
									WHERE (     @skipDatabasesList IS NULL
											OR (    @skipDatabasesList IS NOT NULL	
													AND (
														SELECT COUNT(*)
														FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
														WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
													) = 0
												)
										  )
										  AND [status] = -1 /* previosly not completed jobs */
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
									SELECT	S.[id], 
											@maxPriorityValue + ROW_NUMBER() OVER (ORDER BY S.[id]) AS [new_priority]
									FROM @jobExecutionQueue S
									WHERE S.[priority] IS NULL
								   ) X ON jeqX.[id] = X.[id] 

						UPDATE jeqX
							SET jeqX.[priority] = ait.[priority] * 1000000 + jeqX.[priority]
						FROM @jobExecutionQueue jeqX
						INNER JOIN [dbo].[appInternalTasks] ait ON jeqX.[task_id] = ait.[id]


						/* reset current jobs state */
						SET @retryAttempts = 1
						WHILE @retryAttempts <= 3
							begin
								BEGIN TRY
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
																		AND jeq.[database_name] = S.[database_name]
																		AND jeq.[instance_id] = S.[instance_id]
																		AND jeq.[module] = S.[module]
																		AND jeq.[descriptor] = S.[descriptor]
																		AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
																		AND jeq.[job_step_name] = S.[job_step_name]
																		AND jeq.[job_database_name] = S.[job_database_name]																		

									WHERE (     @skipDatabasesList IS NULL
											OR (    @skipDatabasesList IS NOT NULL	
													AND (
														SELECT COUNT(*)
														FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
														WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
													) = 0
												)
										  )
									SET @retryAttempts = 4
								END TRY
								BEGIN CATCH
									SET @strMessage=ERROR_MESSAGE()
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0							

									SET @retryAttempts = @retryAttempts + 1
									WAITFOR DELAY '00:00:01'
								END CATCH
							end
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				/* if recreate mode = 1, set default priority */
				IF @recreateMode = 1
					begin
						UPDATE jeqX
								SET jeqX.[priority] = X.[new_priority]
						FROM  @jobExecutionQueue jeqX
						INNER JOIN (
									SELECT	S.[id], 
											ROW_NUMBER() OVER (ORDER BY S.[id]) AS [new_priority]
									FROM @jobExecutionQueue S
									) X ON jeqX.[id] = X.[id] 

						UPDATE jeqX
							SET jeqX.[priority] = ait.[priority] * 1000000 + jeqX.[priority]
						FROM @jobExecutionQueue jeqX
						INNER JOIN [dbo].[appInternalTasks] ait ON jeqX.[task_id] = ait.[id]
					end
					
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
						AND (     @skipDatabasesList IS NULL
								OR (    @skipDatabasesList IS NOT NULL	
										AND (
											SELECT COUNT(*)
											FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
											WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
										) = 0
									)
								)

				------------------------------------------------------------------------------------------------------------------------------------------
				/* not allowing jobs to run on a Non-Readable Secondary Replica in an AlwaysOn Availability Group setup if denied by appConfigurations */
				IF		(	SELECT	[value] 
							FROM	[dbo].[appConfigurations] 
							WHERE	[name] = 'Do not create SQL Agent jobs for non-readable secondary replicas (AlwaysOn)' 
									AND [module] = 'maintenance-plan'
						) = 'true'						  
					AND EXISTS 
						(	SELECT	[name] FROM sys.schemas 
							WHERE	[name]='health-check' 
									AND EXISTS(SELECT * FROM sys.objects WHERE [name]='vw_statsDatabaseAlwaysOnDetails')
						)
					begin
						DELETE FROM @agNonReadableSecondaryReplicaDatabases
						
						/* only if health-check data was updated in the last 24 hours */
						SET @queryToRun = N'SELECT	[project_id], [instance_id], [database_name]
											FROM	[health-check].[vw_statsDatabaseAlwaysOnDetails]
											WHERE	[role_desc] = ''SECONDARY''
													AND [readable_secondary_replica] = ''NO''
													AND (
															/* operations on secondary node are restricted by config values */
															SELECT [value]
															FROM [dbo].[appConfigurations] 
															WHERE [module] = ''maintenance-plan''
																AND [name] = ''Allow DBCC operations on non-readable secondary replicas (AlwaysOn)''
														) = ''false''
													AND [event_date_utc] >= DATEADD(hour, -24, GETUTCDATE())'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						INSERT	INTO @agNonReadableSecondaryReplicaDatabases([project_id], [instance_id], [database_name])
								EXEC sp_executesql @queryToRun

						/* reset current jobs state */
						SET @retryAttempts = 1
						WHILE @retryAttempts <= 3
							begin
								BEGIN TRY
									UPDATE jeq
										SET   jeq.[execution_date] = NULL
											, jeq.[running_time_sec] = 0
											, jeq.[log_message] = NULL
											, jeq.[status] = 1
											, jeq.[priority] = S.[priority]
											, jeq.[job_id] = NULL
											, jeq.[event_date_utc] = GETUTCDATE()
									FROM [dbo].[jobExecutionQueue] jeq WITH (INDEX([IX_jobExecutionQueue_JobQueue]))
									INNER JOIN @jobExecutionQueue S ON		jeq.[for_instance_id] = S.[for_instance_id]
																		AND jeq.[project_id] = S.[project_id]
																		AND jeq.[task_id] = S.[task_id]
																		AND jeq.[database_name] = S.[database_name]
																		AND jeq.[instance_id] = S.[instance_id]
																		AND jeq.[module] = S.[module]
																		AND jeq.[descriptor] = S.[descriptor]
																		AND (jeq.[job_name] = S.[job_name] OR jeq.[job_name] = REPLACE(REPLACE(S.[job_name], '%', '_'), '''', '_'))
																		AND jeq.[job_step_name] = S.[job_step_name]
																					AND jeq.[job_database_name] = S.[job_database_name]																		
									INNER JOIN @agNonReadableSecondaryReplicaDatabases exAG ON	exAG.[project_id] = S.[project_id]
																								AND exAG.[instance_id] = S.[for_instance_id]
																								AND exAG.[database_name] = S.[database_name]
									WHERE (     @skipDatabasesList IS NULL
											OR (    @skipDatabasesList IS NOT NULL	
													AND (
														SELECT COUNT(*)
														FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
														WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + CASE WHEN UPPER(@@SERVERNAME) NOT LIKE '%.DATABASE.WINDOWS.NET' THEN REPLACE(UPPER(@@SERVERNAME), '\', '$') ELSE SUBSTRING(UPPER(@@SERVERNAME), 1, CHARINDEX('.', UPPER(@@SERVERNAME))-1) END + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
													) = 0
												)
										  )

									SET @retryAttempts = 4
								END TRY
								BEGIN CATCH
									SET @strMessage=ERROR_MESSAGE()
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0							

									SET @retryAttempts = @retryAttempts + 1
									WAITFOR DELAY '00:00:01'
								END CATCH
							end
					end

				FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
			end
		CLOSE crsCollectorDescriptor
		DEALLOCATE crsCollectorDescriptor

		FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName, @isAzureSQLDatabase
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
