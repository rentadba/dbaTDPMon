USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2016.09.05' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


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
		@debugMode				[bit]=0
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
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @featureflgActions	[int]
		, @forInstanceID		[int]
		, @forSQLServerName		[sysname]

DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

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
SELECT @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [project_id] = @projectID
		AND [name] = @@SERVERNAME

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsActiveInstances CURSOR FOR	SELECT	cin.[instance_id], cin.[instance_name]
										FROM	[dbo].[vw_catalogInstanceNames] cin
										WHERE 	cin.[project_id] = @projectID
												AND cin.[instance_active]=1
												AND cin.[instance_name] LIKE @sqlServerNameFilter
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @forSQLServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		--refresh current server information on internal metadata tables
		EXEC [dbo].[usp_refreshMachineCatalogs]	@projectCode	= @projectCode,
												@sqlServerName	= @forSQLServerName,
												@debugMode		= @debugMode


		--get destination server running version/edition
		SELECT @serverVersionNum = SUBSTRING([version], 1, CHARINDEX('.', [version])-1) + '.' + REPLACE(SUBSTRING([version], CHARINDEX('.', [version])+1, LEN([version])), '.', '')
		FROM	[dbo].[catalogInstanceNames]
		WHERE	[project_id] = @projectID
				AND [id] = @instanceID				

		DECLARE crsCollectorDescriptor CURSOR READ_ONLY FAST_FORWARD FOR	SELECT [descriptor]
																			FROM
																				(
																					SELECT 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor] UNION ALL
																					SELECT 'dbo.usp_mpDatabaseOptimize' AS [descriptor] UNION ALL
																					SELECT 'dbo.usp_mpDatabaseShrink' AS [descriptor] UNION ALL
																					SELECT 'dbo.usp_mpDatabaseBackup(Data)' AS [descriptor] UNION ALL
																					SELECT 'dbo.usp_mpDatabaseBackup(Log)' AS [descriptor]
																				)X
																			WHERE [descriptor] LIKE @jobDescriptor
		OPEN crsCollectorDescriptor
		FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage='Generating queue for : ' + @codeDescriptor
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				DELETE FROM [dbo].[jobExecutionQueue]
				WHERE [project_id] = @projectID
						AND [instance_id] = @instanceID
						AND [descriptor] = @codeDescriptor
						AND [for_instance_id] = @forInstanceID 
						AND [module] = @module

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseConsistencyCheck'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Database Consistency Check - only once a week on Saturday */
						IF @flgActions & 1 = 1 AND DATEPART(dw, GETUTCDATE())=7
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Database Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName	= ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 1, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE', 'READ ONLY')
										)X
			
						/*-------------------------------------------------------------------*/
						/* Daily: Allocation Consistency Check */
						/* when running DBCC CHECKDB, skip running DBCC CHECKALLOC*/
						IF DATEPART(dw, GETUTCDATE())=7
							SET @featureflgActions = 8
						ELSE
							SET @featureflgActions = 12

						IF @flgActions & 2 = 2
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	   , [job_command])
										SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
												@forInstanceID AS [for_instance_id], 
												DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Allocation Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
												'Run'		AS [job_step_name],
												DB_NAME()	AS [job_database_name],
												'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [nvarchar]) + N', @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
										FROM
											(
												SELECT [name] AS [database_name]
												FROM [dbo].[catalogDatabaseNames]
												WHERE	[project_id] = @projectID
														AND [instance_id] = @forInstanceID
														AND [active] = 1
														AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
														AND [state_desc] IN  ('ONLINE', 'READ ONLY')
											)X
			

						/*-------------------------------------------------------------------*/
						/* Weekly: Tables Consistency Check - only once a week on Sunday*/
						IF @flgActions & 4 = 4 AND  DATEPART(dw, GETUTCDATE())=1
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Tables Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 34, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE', 'READ ONLY')
										)X

						/*-------------------------------------------------------------------*/
						/* Weekly: Reference Consistency Check - only once a week on Sunday*/
						IF @flgActions & 8 = 8 AND DATEPART(dw, GETUTCDATE())=1
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Reference Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE', 'READ ONLY')
										)X

						/*-------------------------------------------------------------------*/
						/* Monthly: Perform Correction to Space Usage - on the first Saturday of the month */
						IF @flgActions & 16 = 16 AND DATEPART(dw, GETUTCDATE())=7 AND DATEPART(dd, GETUTCDATE())<=7
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseConsistencyCheck' + ' - Perform Correction to Space Usage' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 64, @flgOptions = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE')
										)X
					end


				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseOptimize'
					begin
						/*-------------------------------------------------------------------*/
						/* Daily: Rebuild Heap Tables - only for SQL versions +2K5*/
						IF @flgActions & 32 = 32 AND @serverVersionNum > 9
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Rebuild Heap Tables' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseOptimize] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @TableSchema = ''%'', @TableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @DefragIndexThreshold = DEFAULT, @RebuildIndexThreshold = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE')
										)X

						/*-------------------------------------------------------------------*/
						/* Daily: Rebuild or Reorganize Indexes*/
						IF @flgActions & 64 = 64 
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	, [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Rebuild or Reorganize Indexes' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseOptimize] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @TableSchema = ''%'', @TableName = ''%'', @flgActions = 3, @flgOptions = DEFAULT, @DefragIndexThreshold = DEFAULT, @RebuildIndexThreshold = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE')
										)X

						/*-------------------------------------------------------------------*/
						/* Daily: Update Statistics */
						IF @flgActions & 128 = 128
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	, [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Update Statistics' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseOptimize] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @TableSchema = ''%'', @TableName = ''%'', @flgActions = 8, @flgOptions = DEFAULT, @DefragIndexThreshold = DEFAULT, @RebuildIndexThreshold = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE')
										)X
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseShrink'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Shrink Database (TRUNCATEONLY) - only once a week on Sunday*/
						IF @flgActions & 256 = 256 AND DATEPART(dw, GETUTCDATE())= 1
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Shrink Database (TRUNCATEONLY)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseShrink] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @flgActions = 2, @flgOptions = 1, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE')
										)X

						/*-------------------------------------------------------------------*/
						/* Monthly: Shrink Log File - on the first Saturday of the month */
						IF @flgActions & 512 = 512 AND DATEPART(dw, GETUTCDATE())=7 AND DATEPART(dd, GETUTCDATE())<=7
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
																   , [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseOptimize' + ' - Shrink Log File' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseShrink] @SQLServerName = ''' + @forSQLServerName + N''', @DBName = ''' + X.[database_name] + N''', @flgActions = 1, @flgOptions = 0, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
													AND [state_desc] IN  ('ONLINE')
										)X

					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Data)'
					begin
						/*-------------------------------------------------------------------*/
						/* Daily: Backup User Databases (diff) */
						IF @flgActions & 1024 = 1024 AND DATEPART(dw, GETUTCDATE())<>7
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	, [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup User Databases (diff)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 2, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
										)X

						/*-------------------------------------------------------------------*/
						/* Weekly: User Databases (full) - only once a week on Saturday */
						IF @flgActions & 2048 = 2048 AND DATEPART(dw, GETUTCDATE())=7
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	, [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup User Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
										)X

						/*-------------------------------------------------------------------*/
						/* Weekly: System Databases (full) - only once a week on Saturday */
						IF @flgActions & 4096 = 4096 AND DATEPART(dw, GETUTCDATE())=7
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	, [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup System Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] IN ('master', 'model', 'msdb', 'tempdb', 'distribution')														
										)X
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Log)'
					begin
						/*-------------------------------------------------------------------*/
						/* Hourly: Backup User Databases Transaction Log */
						IF @flgActions & 8192 = 8192
							INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
																	, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																	, [job_command])
									SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
											@forInstanceID AS [for_instance_id], 
											DB_NAME() + ' - ' + 'dbo.usp_mpDatabaseBackup' + ' - Backup User Databases (log)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']' AS [job_name],
											'Run'		AS [job_step_name],
											DB_NAME()	AS [job_database_name],
											'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 4, @flgOptions = DEFAULT,	@retentionDays = DEFAULT, @executionLevel = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
									FROM
										(
											SELECT [name] AS [database_name]
											FROM [dbo].[catalogDatabaseNames]
											WHERE	[project_id] = @projectID
													AND [instance_id] = @forInstanceID
													AND [active] = 1
													AND [name] NOT IN ('master', 'model', 'msdb', 'tempdb', 'distribution')	
										)X
						end
				------------------------------------------------------------------------------------------------------------------------------------------

				FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
			end
		CLOSE crsCollectorDescriptor
		DEALLOCATE crsCollectorDescriptor
										

		FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO



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
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Step 1: Delete existing information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_monGetSQLAgentFailedJobs'


-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Step 2: Get Instance Details Information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
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

		SET @minJobCompletionTime = ISNULL(@minJobCompletionTime, CAST(GETDATE() AS [date]))

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
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetSQLAgentFailedJobs'
									, @strMessage
				END CATCH


				DECLARE crsFailedJobs CURSOR READ_ONLY FOR	SELECT j.[job_name]
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
							PRINT ERROR_MESSAGE()
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


USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
