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
		@sqlServerName				[sysname],
		@dbName						[sysname],
		@tableSchema				[sysname] = '%',
		@tableName					[sysname] = '%',
		@indexName					[sysname] = '%',
		@indexID					[int] = NULL,
		@partitionNumber			[int] = 0,
		@flgAction					[tinyint] = 1,
		@flgOptions					[int] = 6149, --4096 + 2048 + 1	/* 6177 for space optimized index rebuild */
		@maxDOP						[smallint] = 1,
		@fillFactor					[tinyint] = 0,
		@executionLevel				[tinyint] = 0,
		@affectedDependentObjects	[nvarchar](max) OUTPUT,
		@debugMode					[bit] = 0
/* WITH ENCRYPTION */
AS


-- ============================================================================
-- Copyright (c) 2004-2023 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed.
--		@indexName		- name of the index to be analyzed
--		@indexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@partitionNumber- index partition number. default value = 0 (index with no partitions)
--		@flgAction:		 1	- Rebuild index (default)
--						 2  - Reorganize indexes
--						 4	- Disable index (if no index id is specified, all non-clustered will be disabled except for the unique/clustered ones)
--		@flgOptions		 1  - Compact large objects (LOB) when reorganize  (default); for columnstore indexes will compress all row groups (COMPRESS_ALL_ROW_GROUPS)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes
--						 8  - Disable non-clustered index before rebuild (save space) (won't apply when 4096 is applicable)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  4096  - rebuild indexes using ONLINE=ON, if applicable (default)
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   			[nvarchar](max),
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
			@crtPartitionNumber		[int],
			@crtIsPartitioned		[bit],
			@crtDataCompressionDesc [nvarchar](60),
			@flgInheritOptions		[int],
			@tmpIndexName			[sysname],
			@tmpIndexIsPrimaryXML	[bit],
			@nestedExecutionLevel	[tinyint],
			@eventName				[sysname],
			@eventData				[varchar](8000)

DECLARE   @flgRaiseErrorAndStop [bit]
		, @errorCode			[int]

DECLARE @DependentIndexes TABLE	(
								    [schema_name]		[sysname]	NULL
								  , [table_name]		[sysname]	NULL								
								  , [index_name]		[sysname]	NULL
								  , [is_primary_xml]	[bit]		DEFAULT(0)
								)

SET NOCOUNT ON

DECLARE @tmpTableToAlterIndexes TABLE
			(
				[index_id]				[int]		NULL
			  , [index_name]			[sysname]	NULL
			  , [index_type]			[tinyint]	NULL
			  , [allow_page_locks]		[bit]		NULL
			  , [is_disabled]			[bit]		NULL
			  , [is_primary_xml]		[bit]		NULL
			  , [has_dependent_fk]		[bit]		NULL
			  , [is_replicated]			[bit]		NULL
			  , [partition_number]		[int]		NULL
			  , [is_partitioned]		[bit]		NULL
			  , [data_compression_desc] [nvarchar](60) NULL
			)

--------------------------------------------------------------------------------------------------
/*	Could not proceed with index DDL operation on table '...' because it conflicts with another concurrent operation that is already in progress on the object. 
	The concurrent operation could be an online index operation on the same object or another concurrent operation that moves index pages like DBCC SHRINKFILE.
*/
-----------------------------------------------------------------------------------------
SET @nestedExecutionLevel = 0
IF OBJECT_ID('#runtimeProperty') IS NOT NULL DROP TABLE #runtimeProperty
CREATE TABLE #runtimeProperty
			(
				[value]			[sysname]	NULL
			)
			
SET @queryToRun = N'SELECT CAST(COUNT(*) AS [sysname]) AS [session_count] FROM sys.dm_exec_requests
					WHERE	DB_NAME([database_id]) = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
							AND [command] LIKE ''Dbcc%'''
		
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #runtimeProperty
INSERT	INTO #runtimeProperty([value])
		EXEC sp_executesql @queryToRun

IF (SELECT CAST([value] AS [int]) FROM #runtimeProperty) > 0
	begin
		SET @queryToRun='A shrink operation is in progress for the current database. Index operations cannot run.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @eventName = CASE @flgAction  WHEN 1 THEN 'database maintenance - rebuilding index'
										  WHEN 2 THEN 'database maintenance - reorganize index'
										  WHEN 4 THEN 'database maintenance - disable index'
						 END
						 
		SET @eventData='<skipaction><detail>' + 
							'<name>database maintenance</name>' + 
							'<type>' + CASE @flgAction  WHEN 1 THEN 'rebuilding index'
														WHEN 2 THEN 'reorganize index'
														WHEN 4 THEN 'disable index'
										END + '</type>' + 
							'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
							'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
							'<reason>' + @queryToRun + '</reason>' + 
						'</detail></skipaction>'
						
		EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
											@dbName			= @dbName,
											@module			= 'dbo.usp_mpAlterTableIndexes',
											@eventName		= @eventName,
											@eventMessage	= @eventData,
											@eventType		= 0 /* info */
		RETURN 0
	end

--------------------------------------------------------------------------------------------------
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

		---------------------------------------------------------------------------------------------		
		--get destination server running version/edition
		DECLARE		@serverEdition					[sysname],
					@serverVersionStr				[sysname],
					@serverVersionNum				[numeric](9,6),
					@serverEngine					[int]

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
												@serverEdition			= @serverEdition OUT,
												@serverVersionStr		= @serverVersionStr OUT,
												@serverVersionNum		= @serverVersionNum OUT,
												@serverEngine			= @serverEngine OUT,
												@executionLevel			= @nestedExecutionLevel,
												@debugMode				= @debugMode

		SET @queryToRun = CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
						SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + ''' 
								AND TABLE_SCHEMA LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC sp_executesql  @queryToRun

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage=N'Alter indexes ON ' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ' : ' + 
											CASE @flgAction WHEN 1 THEN 'REBUILD'
															WHEN 2 THEN 'REORGANIZE'
															WHEN 4 THEN 'DISABLE'
															ELSE 'N/A'
											END
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable/reorganize indexes, will get only enabled indexes
						--if current action is to rebuild, will get both enabled/disabled indexes
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
													SELECT  si.[index_id]
														, si.[name]
														, si.[type]
														, si.[allow_page_locks]
														, si.[is_disabled]
														, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
														, CASE WHEN SUM(CASE WHEN fk.[name] IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS [has_dependent_fk]
														, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0) AS [is_replicated]
														, ISNULL(sp.[partition_number], 1) AS [partition_number]
														, CASE WHEN sp.[partition_count] <> 1 THEN 1 ELSE 0 END AS [is_partitioned]
														, ' + CASE WHEN @serverVersionNum > 13.0 THEN N'ISNULL(sp.[data_compression_desc], ''NONE'') ' ELSE 'NULL' END + N' AS [data_compression_desc]
													FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			si
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		so  ON so.[object_id] = si.[object_id]
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
													LEFT JOIN
																(
																	SELECT   [object_id], [index_id], [partition_number]' + CASE WHEN @serverVersionNum > 13.0 THEN N', [data_compression_desc]' ELSE N'' END + N'
																			, COUNT(*) OVER(PARTITION BY [object_id], [index_id]) AS [partition_count]
																	FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.partitions
																) sp ON sp.[object_id] = so.[object_id] AND sp.[index_id] = si.[index_id]
													LEFT  JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
													LEFT  JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[foreign_keys]	fk  ON fk.[referenced_object_id] = so.[object_id] AND fk.[key_index_id] = si.[index_id]
													LEFT  JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[tables]		st  ON st.[object_id] = so.[object_id]
													WHERE	so.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'sql') + '''
															AND sch.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'sql') + '''' + 
															CASE WHEN @dbName NOT IN ('msdb') THEN N' AND so.[is_ms_shipped] = 0' ELSE N'' END + 
															CASE	WHEN ISNULL(@partitionNumber, 0) <> 0
																	THEN N' AND ISNULL(sp.[partition_number], 1) = ' + CAST(@partitionNumber AS [varchar](32)) 
																	ELSE N''
															END + 
															CASE	WHEN @indexName IS NOT NULL 
																	THEN ' AND si.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@indexName, 'sql') + ''''
																	ELSE CASE WHEN @indexID  IS NOT NULL 
																			  THEN ' AND si.[index_id] = ' + CAST(@indexID AS [nvarchar])
																			  ELSE ''
																		 END
															END + '
															AND si.[is_disabled] IN ( ' + CASE WHEN @flgAction IN (2, 4) THEN '0' ELSE '0,1' END + ')' +
															CASE WHEN @indexID IS NULL AND @flgAction IN (4) 
																 THEN ' AND si.[type] <> 1 AND si.[is_unique] = 0'
																 ELSE ''
															END + '											
													GROUP BY si.[index_id]
															, si.[name]
															, si.[type]
															, si.[allow_page_locks]
															, si.[is_disabled]
															, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END
															, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0)
															, sp.[partition_number]
															, CASE WHEN sp.[partition_count] <> 1 THEN 1 ELSE 0 END
															' + CASE WHEN @serverVersionNum > 13.0 THEN N', sp.[data_compression_desc]' ELSE N'' END

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @tmpTableToAlterIndexes;
						INSERT	INTO @tmpTableToAlterIndexes([index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated], [partition_number], [is_partitioned], [data_compression_desc])
								EXEC sp_executesql  @queryToRun


						DECLARE crsTableToAlterIndexes CURSOR LOCAL DYNAMIC FOR	SELECT DISTINCT tai.[index_id], tai.[index_name], tai.[index_type], tai.[allow_page_locks], tai.[is_disabled]
																								, tai.[is_primary_xml], tai.[has_dependent_fk], tai.[is_replicated]
																								, tai.[partition_number], tai.[is_partitioned], tai.[data_compression_desc]
																						FROM @tmpTableToAlterIndexes tai
																						LEFT JOIN [maintenance-plan].[logInternalAction] smpi ON	smpi.[name]=N'index-made-disable'
																																							AND smpi.[server_name]=@sqlServerName
																																							AND smpi.[database_name]=@dbName
																																							AND smpi.[schema_name]=@crtTableSchema
																																							AND smpi.[object_name]=@crtTableName
																																							AND smpi.[child_object_name]=tai.[index_name]

																						WHERE smpi.[child_object_name] IS NULL
																						ORDER BY [index_id], [index_name], [partition_number]					
						OPEN crsTableToAlterIndexes
						FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated, @crtPartitionNumber, @crtIsPartitioned, @crtDataCompressionDesc
						WHILE @@FETCH_STATUS=0
							begin
								SET @strMessage= [dbo].[ufn_getObjectQuoteName](@crtIndexName, 'quoted') + CASE WHEN @crtIsPartitioned = 1 THEN ' (partition ' + CAST(@crtPartitionNumber AS [varchar](32)) + N')' ELSE N'' END
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
								IF @flgAction = 1 AND @flgOptions & 4 = 4 AND EXISTS (	SELECT * 
																						FROM [maintenance-plan].[logInternalAction] smpi 
																						WHERE	smpi.[name]=N'index-rebuild'
																							AND smpi.[server_name]=@sqlServerName
																							AND smpi.[database_name]=@dbName
																							AND smpi.[schema_name]=@crtTableSchema
																							AND smpi.[object_name]=@crtTableName
																							AND smpi.[child_object_name]=@crtIndexName
																					  )
									begin
										SET @queryToRun='Index was already rebuilt. Skipping it.'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @eventName = 'database maintenance - rebuilding index'
						 
										SET @eventData='<skipaction><detail>' + 
															'<name>database maintenance</name>' + 
															'<type>rebuilding index</type>' + 
															'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@strMessage, 'xml') + '</affected_object>' + 
															'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
															'<reason>' + @queryToRun + '</reason>' + 
														'</detail></skipaction>'
								
										SET @strMessage=[dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
										EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																			@dbName			= @dbName,
																			@objectName		= @strMessage,
																			@childObjectName= @crtIndexName,
																			@module			= 'dbo.usp_mpAlterTableIndexes',
																			@eventName		= @eventName,
																			@eventMessage	= @eventData,
																			@eventType		= 0 /* info */
									end
								ELSE
									begin
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
														EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @sqlServerName,
																										@dbName				= @dbName,
																										@tableSchema		= @crtTableSchema,
																										@tableName			= @crtTableName,
																										@indexName			= @crtIndexName,
																										@indexID			= @crtIndexID,
																										@partitionNumber	= @crtPartitionNumber,
																										@sqlScriptOnline	= @sqlScriptOnline OUT,
																										@flgOptions			= @flgOptions,
																										@executionLevel		= @nestedExecutionLevel,
																										@debugMode			= @debugMode
													end

												---------------------------------------------------------------------------------------------
												--primary / unique index options
												-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
												IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
													AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) 
													AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0 AND @crtIsPartitioned = 0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @sqlServerName	= @sqlServerName
																									, @dbName			= @dbName
																									, @tableSchema		= @crtTableSchema
																									, @tableName		= @crtTableName
																									, @constraintName	= '%'
																									, @flgAction		= 0		-- Disable Constraints
																									, @flgOptions		= 1		-- Use tables that have foreign key constraints that reffers current table (default)
																									, @executionLevel	= @nestedExecutionLevel
																									, @debugMode		= @debugMode
													end

												---------------------------------------------------------------------------------------------
												--clustered/primary key index options
												IF @crtIndexType IN (1, 5) AND @crtIsPartitioned = 0
													begin
														--4  - Rebuild all dependent indexes when rebuild primary indexes
														IF @flgOptions & 4 = 4
															begin
																--get all enabled non-clustered/xml/spatial indexes for current table
																SET @queryToRun = N''
																SET @queryToRun = @queryToRun + CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
																							SELECT  sch.[name] AS [schema_name], so.[name] AS [table_name], si.[name] AS [index_name]
																									, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
																							FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			si
																							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																							LEFT  JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
																							WHERE	so.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'sql') + '''
																									AND sch.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'sql') + ''' 
																									AND si.[type] in (2,3,4)
																									AND si.[is_disabled] = 0'
																SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
																IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

																INSERT INTO @DependentIndexes ([schema_name], [table_name], [index_name], [is_primary_xml])
																	EXEC sp_executesql  @queryToRun
															end

														--8  - Disable non-clustered index before rebuild (save space)
														--won't disable the index when performing online rebuild
														IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtTableIsReplicated=0 AND @crtIsPartitioned = 0
															begin
																DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																								SELECT DISTINCT [index_name], [is_primary_xml]
																								FROM @DependentIndexes
																								WHERE	[schema_name] = @crtTableSchema
																									AND [table_name] = @crtTableName
																								ORDER BY [is_primary_xml]
																OPEN crsNonClusteredIndexes
																FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
																WHILE @@FETCH_STATUS=0
																	begin
																		SET @nestedExecutionLevel = @executionLevel + 2
																		EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName	= @sqlServerName
																												, @dbName			= @dbName
																												, @tableSchema		= @crtTableSchema
																												, @tableName		= @crtTableName
																												, @indexName		= @tmpIndexName
																												, @indexID			= NULL
																												, @partitionNumber	= DEFAULT
																												, @flgAction		= 4				--disable
																												, @flgOptions		= @flgOptions
																												, @executionLevel	= @nestedExecutionLevel
																												, @affectedDependentObjects = @affectedDependentObjects OUT
																												, @debugMode		= @debugMode										

																		FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
																	end
																CLOSE crsNonClusteredIndexes
																DEALLOCATE crsNonClusteredIndexes
															end
													end
												ELSE
													---------------------------------------------------------------------------------------------
													--xml primary key index options
													IF @crtIndexType = 3 AND @crtIndexIsPrimaryXML=1 AND @crtIsPartitioned = 0
														begin
															--4  - Rebuild all dependent indexes when rebuild primary indexes
															IF @flgOptions & 4 = 4
																begin
																	--get all enabled secondary xml indexes for current table
																	SET @queryToRun = N''
																	SET @queryToRun = @queryToRun + CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
																								SELECT  sch.[name] AS [schema_name], so.[name] AS [table_name], si.[name] AS [index_name]
																								FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			si
																								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id]
																								WHERE	so.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'sql') + '''
																										AND sch.[name] = ''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'sql') + ''' 
																										AND si.[type] = 3
																										AND xi.[using_xml_index_id] = ''' + CAST(@crtIndexID AS [sysname]) + '''
																										AND si.[is_disabled] = 0'
																	SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
																	IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

																	INSERT INTO @DependentIndexes ([schema_name], [table_name], [index_name])
																		EXEC sp_executesql  @queryToRun
																end

															--8  - Disable non-clustered index before rebuild (save space)
															--won't disable the index when performing online rebuild
															IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtTableIsReplicated=0 AND @crtIsPartitioned = 0
																begin
																	DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																									SELECT DISTINCT [index_name]
																									FROM @DependentIndexes
																									WHERE	[schema_name] = @crtTableSchema
																										AND [table_name] = @crtTableName
																	OPEN crsNonClusteredIndexes
																	FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
																	WHILE @@FETCH_STATUS=0
																		begin
																			SET @nestedExecutionLevel = @executionLevel + 2
																			EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName	= @sqlServerName
																													, @dbName			= @dbName
																													, @tableSchema		= @crtTableSchema
																													, @tableName		= @crtTableName
																													, @indexName		= @tmpIndexName
																													, @indexID			= NULL
																													, @partitionNumber	= DEFAULT
																													, @flgAction		= 4				--disable
																													, @flgOptions		= @flgOptions
																													, @executionLevel	= @nestedExecutionLevel
																													, @affectedDependentObjects = @affectedDependentObjects OUT
																													, @debugMode		= @debugMode										

																			FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
																		end
																	CLOSE crsNonClusteredIndexes
																	DEALLOCATE crsNonClusteredIndexes
																end
														end
													ELSE
														--8  - Disable non-clustered index before rebuild (save space)
														--won't disable the index when performing online rebuild										
														IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0 AND @crtIsPartitioned = 0
															begin
																SET @nestedExecutionLevel = @executionLevel + 2
																EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName	= @sqlServerName
																										, @dbName			= @dbName
																										, @tableSchema		= @crtTableSchema
																										, @tableName		= @crtTableName
																										, @indexName		= @crtIndexName
																										, @indexID			= NULL
																										, @partitionNumber	= @crtPartitionNumber
																										, @flgAction		= 4				--disable
																										, @flgOptions		= @flgOptions
																										, @executionLevel	= @nestedExecutionLevel
																										, @affectedDependentObjects = @affectedDependentObjects OUT
																										, @debugMode		= @debugMode										
														end

												---------------------------------------------------------------------------------------------
												/* FIX: Data corruption occurs in clustered index when you run online index rebuild in SQL Server 2012 or SQL Server 2014 https://support.microsoft.com/en-us/kb/2969896 */
												IF (@sqlScriptOnline LIKE N'ONLINE = ON%')
													begin
														IF     (@serverVersionNum >= 11.02100 AND @serverVersionNum < 11.03449) /* SQL Server 2012 RTM till SQL Server 2012 SP1 CU 11*/
															OR (@serverVersionNum >= 11.05058 AND @serverVersionNum < 11.05532) /* SQL Server 2012 SP2 till SQL Server 2012 SP2 CU 1*/
															OR (@serverVersionNum >= 12.02000 AND @serverVersionNum < 12.02370) /* SQL Server 2014 RTM CU 2*/
															begin
																SET @maxDOP=1
															end
													end

												---------------------------------------------------------------------------------------------
												--generate rebuild index script
												SET @queryToRun = N''

												SET @queryToRun = @queryToRun + N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
												SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ''') IS NOT NULL ALTER INDEX ' + dbo.ufn_getObjectQuoteName(@crtIndexName, 'quoted') + ' ON ' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ' REBUILD'
					
												IF @crtIsPartitioned = 1 AND @crtIndexIsDisabled = 0
													SET @queryToRun = @queryToRun + N' PARTITION = ' + CAST(@crtPartitionNumber AS [nvarchar])

												--rebuild options
												SET @queryToRun = @queryToRun + N' WITH (' + CASE WHEN @crtIndexType NOT IN (5, 6) THEN N'SORT_IN_TEMPDB = ON, ' ELSE N'' END + 
																							 CASE WHEN ISNULL(@maxDOP, 0) <> 0 THEN N'MAXDOP = ' + CAST(@maxDOP AS [nvarchar]) + N', ' ELSE N'' END + 
																							 CASE WHEN ISNULL(@sqlScriptOnline, N'')<>N'' THEN @sqlScriptOnline + N', ' ELSE N'' END + 
																							 CASE WHEN ISNULL(@fillFactor, 0) <> 0 AND @crtIsPartitioned = 0
																									 THEN N'FILLFACTOR = ' + CAST(@fillFactor AS [nvarchar]) + N', '
																									 ELSE N'' 
																							 END +
																							 CASE WHEN @serverVersionNum >= 13.0 AND @crtIndexType NOT IN (5, 6) THEN N'DATA_COMPRESSION=' + @crtDataCompressionDesc + N', ' ELSE N'' END
												SET  @queryToRun = SUBSTRING(@queryToRun, 1, LEN(@queryToRun)-1) + N')'

												IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

												IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%'))
													begin
														SET @strMessage=N'performing index rebuild'
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
													end

												SET @objectName = [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
												SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@crtIndexName, 'quoted')
												SET @nestedExecutionLevel = @executionLevel + 1
												EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
												/*
												EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																								@dbName			= @dbName,
																								@objectName		= @objectName,
																								@childObjectName= @childObjectName,
																								@module			= 'dbo.usp_mpAlterTableIndexes',
																								@eventName		= 'database maintenance - rebuilding index',
																								@queryToRun  	= @queryToRun,
																								@flgOptions		= @flgOptions,
																								@executionLevel	= @nestedExecutionLevel,
																								@debugMode		= @debugMode
												*/
												IF @flgOptions & 4 = 4
													begin
														EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-rebuild',
																								@flgOperation		= 1,
																								@server_name		= @sqlServerName,
																								@database_name		= @dbName,
																								@schema_name		= @crtTableSchema,
																								@object_name		= @crtTableName,
																								@child_object_name	= @crtIndexName
													end

												IF @errorCode=0
													EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-made-disable',
																							@flgOperation		= 2,
																							@server_name		= @sqlServerName,
																							@database_name		= @dbName,
																							@schema_name		= @crtTableSchema,
																							@object_name		= @crtTableName,
																							@child_object_name	= @crtIndexName

												---------------------------------------------------------------------------------------------
												--rebuild dependent indexes
												--clustered / xml primary key index options
												IF ((@crtIndexType IN (1, 5)) OR (@crtIndexType = 3 AND @crtIndexIsPrimaryXML=1)) AND @crtIsPartitioned = 0
													begin
														--4  - Rebuild all dependent indexes when rebuild primary indexes
														--will rebuild only indexes disabled by this tool
														IF (@flgOptions & 4 = 4)
															begin		
																IF EXISTS (	SELECT DISTINCT di.[index_name], di.[is_primary_xml]
																			FROM @DependentIndexes di
																			LEFT JOIN [maintenance-plan].[logInternalAction] smpi ON	smpi.[name]=N'index-made-disable'
																																				AND smpi.[server_name]=@sqlServerName
																																				AND smpi.[database_name]=@dbName
																																				AND smpi.[schema_name]=di.[schema_name]
																																				AND smpi.[object_name]=di.[table_name]
																																				AND smpi.[child_object_name]=di.[index_name]
																			WHERE	di.[schema_name] = @crtTableSchema
																				AND	di.[table_name] = @crtTableName
																				AND (
																						(
																							/* index was disabled (option selected) and marked as disabled */
																							(@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name]=N'index-made-disable'
																						)
																						OR
																						(
																							/* index was not disabled (option selected) */
																							NOT (@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name] IS NULL
																						)
																					)
																			)	
																	begin
																		SET @strMessage=N'* Rebuilding all the dependent indexes' 
																		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 2, @stopExecution=0
																	end

																DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																								SELECT DISTINCT di.[index_name], di.[is_primary_xml]
																								FROM @DependentIndexes di
																								LEFT JOIN [maintenance-plan].[logInternalAction] smpi ON	smpi.[name]=N'index-made-disable'
																																									AND smpi.[server_name]=@sqlServerName
																																									AND smpi.[database_name]=@dbName
																																									AND smpi.[schema_name]=@crtTableSchema
																																									AND smpi.[object_name]=@crtTableName
																																									AND smpi.[child_object_name]=di.[index_name]
																								WHERE	di.[schema_name] = @crtTableSchema
																									AND	di.[table_name] = @crtTableName
																									AND (
																											(
																												/* index was disabled (option selected) and marked as disabled */
																												(@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																												AND smpi.[name]=N'index-made-disable'
																											)
																											OR
																											(
																												/* index was not disabled (option selected) */
																												NOT (@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																												AND smpi.[name] IS NULL
																											)
																										)
																								ORDER BY di.[is_primary_xml] DESC
																OPEN crsNonClusteredIndexes
																FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
																WHILE @@FETCH_STATUS=0
																	begin
																		SET @nestedExecutionLevel = @executionLevel + 2
																		EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName	= @sqlServerName
																												, @dbName			= @dbName
																												, @tableSchema		= @crtTableSchema
																												, @tableName		= @crtTableName
																												, @indexName		= @tmpIndexName
																												, @indexID			= NULL
																												, @partitionNumber	= DEFAULT
																												, @flgAction		= 1		--rebuild
																												, @flgOptions		= @flgOptions
																												, @executionLevel	= @nestedExecutionLevel
																												, @affectedDependentObjects = @affectedDependentObjects OUT
																												, @debugMode		= @debugMode										

																		EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-rebuild',
																												@flgOperation		= 1,
																												@server_name		= @sqlServerName,
																												@database_name		= @dbName,
																												@schema_name		= @crtTableSchema,
																												@object_name		= @crtTableName,
																												@child_object_name	= @tmpIndexName

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
													AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtTableIsReplicated=0 AND @crtIsPartitioned = 0
													begin
														SET @flgInheritOptions = 1								-- Use tables that have foreign key constraints that reffers current table (default)

														--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
														IF @flgOptions & 64 = 64
															SET @flgInheritOptions = @flgInheritOptions + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @sqlServerName	= @sqlServerName
																									, @dbName			= @dbName
																									, @tableSchema		= @crtTableSchema
																									, @tableName		= @crtTableName
																									, @constraintName	= '%'
																									, @flgAction		= 1		-- Enable Constraints
																									, @flgOptions		= @flgInheritOptions
																									, @executionLevel	= @nestedExecutionLevel
																									, @debugMode		= @debugMode
													end
											end
									end

								---------------------------------------------------------------------------------------------
								-- 2  - Reorganize indexes
								---------------------------------------------------------------------------------------------
								-- avoid messages like:	The index [...] on table [..] cannot be reorganized because page level locking is disabled.		
								IF @flgAction = 2
									IF (@crtIndexType NOT IN (5, 6) AND @crtIndexAllowPageLocks=1) OR (@crtIndexType IN (5, 6))
										begin
											SET @queryToRun = N''
											SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
											SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ''') IS NOT NULL ALTER INDEX ' + dbo.ufn_getObjectQuoteName(@crtIndexName, 'quoted') + ' ON ' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ' REORGANIZE'
				
											IF @crtIsPartitioned = 1
												SET @queryToRun = @queryToRun + N' PARTITION = ' + CAST(@crtPartitionNumber AS [nvarchar])

											--  1  - Compact large objects (LOB) (default)
											IF @crtIndexType NOT IN (5, 6)
												begin
													IF @flgOptions & 1 = 1
														SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = ON) '
													ELSE
														SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = OFF) '
												end
											ELSE
												begin
													IF @flgOptions & 1 = 1
														SET @queryToRun = @queryToRun + N' WITH (COMPRESS_ALL_ROW_GROUPS = ON) '
													ELSE
														SET @queryToRun = @queryToRun + N' WITH (COMPRESS_ALL_ROW_GROUPS = OFF) '
												end
											IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


											SET @objectName = [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
											SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@crtIndexName, 'quoted')
											SET @nestedExecutionLevel = @executionLevel + 1
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
											/*
											EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																							@dbName			= @dbName,
																							@objectName		= @objectName,
																							@childObjectName= @childObjectName,
																							@module			= 'dbo.usp_mpAlterTableIndexes',
																							@eventName		= 'database maintenance - reorganize index',
																							@queryToRun  	= @queryToRun,
																							@flgOptions		= @flgOptions,
																							@executionLevel	= @nestedExecutionLevel,
																							@debugMode		= @debugMode
											*/
										end
									ELSE
										begin
											SET @strMessage=N'index cannot be REORGANIZE because ALLOW_PAGE_LOCKS is set to OFF. Skipping...'
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
										end

								---------------------------------------------------------------------------------------------
								-- 4  - Disable indexes 
								---------------------------------------------------------------------------------------------
								IF @flgAction = 4
									begin
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
										SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ''') IS NOT NULL ALTER INDEX ' + dbo.ufn_getObjectQuoteName(@crtIndexName, 'quoted') + ' ON ' + [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted') + ' DISABLE'
				
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @objectName = [dbo].[ufn_getObjectQuoteName](@crtTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@crtTableName, 'quoted')
										SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@crtIndexName, 'quoted')
										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
										/*
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																						@dbName			= @dbName,
																						@objectName		= @objectName,
																						@childObjectName= @childObjectName,
																						@module			= 'dbo.usp_mpAlterTableIndexes',
																						@eventName		= 'database maintenance - disable index',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @debugMode
										*/
										/* 4 disable index -> insert action 1 */
										IF @errorCode=0
											EXEC [dbo].[usp_mpMarkInternalAction]	@actionName		= N'index-made-disable',
																					@flgOperation	= 1,
																					@server_name		= @sqlServerName,
																					@database_name		= @dbName,
																					@schema_name		= @crtTableSchema,
																					@object_name		= @crtTableName,
																					@child_object_name	= @crtIndexName
									end

								FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated, @crtPartitionNumber, @crtIsPartitioned, @crtDataCompressionDesc
							end
						CLOSE crsTableToAlterIndexes
						DEALLOCATE crsTableToAlterIndexes

						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		SET @affectedDependentObjects=N''
		SELECT @affectedDependentObjects = @affectedDependentObjects + [dbo].[ufn_getObjectQuoteName]([index_name], 'quoted') + N';'
		FROM @DependentIndexes

		/* remove entries from the log table */
		IF @flgOptions & 4 = 4 AND @executionLevel = 0
			DELETE FROM [maintenance-plan].[logInternalAction]
			WHERE [name] = N'index-rebuild'
				AND [session_id] = @@SPID

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
