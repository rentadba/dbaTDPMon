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

DECLARE		@tmpSQL    				[nvarchar](max),
			@strMessage				[nvarchar](max),
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
		, @errorString			[nvarchar](max)
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

		SET @tmpSQL = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + ''''
		SET @tmpSQL = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @tmpSQL)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@tmpSQL)

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
						SET @tmpSQL = N''
						SET @tmpSQL = @tmpSQL + N'SELECT  si.[index_id]
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

						SET @tmpSQL = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @tmpSQL)
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @tmpTableToAlterIndexes
						INSERT	INTO @tmpTableToAlterIndexes([index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated])
								EXEC (@tmpSQL)

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
												SET @tmpSQL = N''
												SET @tmpSQL = @tmpSQL + N'SELECT  si.[name]
																				, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
																			FROM [' + @DBName + '].[sys].[indexes]				si
																			INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																			INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																			LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
																			WHERE	so.[name] = ''' + @crtTableName + '''
																					AND sch.[name] = ''' + @crtTableSchema + ''' 
																					AND si.[type] in (2,3,4)
																					AND si.[is_disabled] = 0'
												SET @tmpSQL = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @tmpSQL)
												IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

												INSERT INTO @DependentIndexes ([index_name], [is_primary_xml])
													EXEC (@tmpSQL)
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
													SET @tmpSQL = N''
													SET @tmpSQL = @tmpSQL + N'SELECT  si.[name]
																				FROM [' + @DBName + '].[sys].[indexes]				si
																				INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																				INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																				INNER JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id]
																				WHERE	so.[name] = ''' + @crtTableName + '''
																						AND sch.[name] = ''' + @crtTableSchema + ''' 
																						AND si.[type] = 3
																						AND xi.[using_xml_index_id] = ''' + CAST(@crtIndexID AS [sysname]) + '''
																						AND si.[is_disabled] = 0'
													SET @tmpSQL = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @tmpSQL)
													IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

													INSERT INTO @DependentIndexes ([index_name])
														EXEC (@tmpSQL)
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
								SET @tmpSQL = N''

								SET @tmpSQL = @tmpSQL + N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @tmpSQL = @tmpSQL + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REBUILD'
					
								--rebuild options
								SET @tmpSQL = @tmpSQL + N' WITH (SORT_IN_TEMPDB = ON' + CASE WHEN ISNULL(@MaxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@MaxDOP AS [nvarchar]) ELSE N'' END + 
																						CASE WHEN ISNULL(@sqlScriptOnline, N'')<>N'' THEN N', ' + @sqlScriptOnline ELSE N'' END + 
																						CASE WHEN ISNULL(@FillFactor, 0) <> 0 THEN N', FILLFACTOR = ' + CAST(@FillFactor AS [nvarchar]) ELSE N'' END +
																N')'

								IF @PartitionNumber>1
									SET @tmpSQL = @tmpSQL + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])

								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

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
																				@queryToRun  	= @tmpSQL,
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
									SET @tmpSQL = N''
									SET @tmpSQL = @tmpSQL + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
									SET @tmpSQL = @tmpSQL + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REORGANIZE'
				
									--  1  - Compact large objects (LOB) (default)
									IF @flgOptions & 1 = 1
										SET @tmpSQL = @tmpSQL + N' WITH (LOB_COMPACTION = ON) '
									ELSE
										SET @tmpSQL = @tmpSQL + N' WITH (LOB_COMPACTION = OFF) '
				
									IF @PartitionNumber>1
										SET @tmpSQL = @tmpSQL + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])
									IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


									SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
									SET @childObjectName = QUOTENAME(@crtIndexName)
									SET @nestedExecutionLevel = @executionLevel + 1

									EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																					@dbName			= @DBName,
																					@objectName		= @objectName,
																					@childObjectName= @childObjectName,
																					@module			= 'dbo.usp_mpAlterTableIndexes',
																					@eventName		= 'database maintenance - reorganize index',
																					@queryToRun  	= @tmpSQL,
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
								SET @tmpSQL = N''
								SET @tmpSQL = @tmpSQL + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @tmpSQL = @tmpSQL + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] DISABLE'
				
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - disable index',
																				@queryToRun  	= @tmpSQL,
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
