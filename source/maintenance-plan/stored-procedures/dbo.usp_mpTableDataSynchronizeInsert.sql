RAISERROR('Create procedure: [dbo].[usp_mpTableDataSynchronizeInsert]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_mpTableDataSynchronizeInsert]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_mpTableDataSynchronizeInsert]
GO

CREATE PROCEDURE [dbo].[usp_mpTableDataSynchronizeInsert]
		@sourceServerName		[sysname]=@@SERVERNAME,
		@sourceDB				[sysname],			
		@sourceTableSchema		[sysname]='%',
		@sourceTableName		[sysname]='%',
		@destinationServerName	[sysname]=@@SERVERNAME,
		@destinationDB			[sysname],			
		@destinationTableSchema	[sysname]='%',		
		@destinationTableName	[sysname]='%',
		@flgActions				[bit] = 1,
		@flgOptions				[int] = 2048,
		@executionLevel			[tinyint] = 0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2019 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author		: Dan Andrei STEFAN
-- Create date	: 2004-2006, last updated 26.09.2019
-- Module		: Database Maintenance Scripts
-- ============================================================================
---------------------------------------------------------------------------------------------
--		@flgActions		 1 - push data
--						 0 - pull data
---------------------------------------------------------------------------------------------
--		@flgOptions		 8  - Disable non-clustered index
--						16  - Disable all foreign key constraints that reffered current table before rebuilding indexes
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  8192  - disable/enable table triggers
---------------------------------------------------------------------------------------------

DECLARE	@execServerDestination		[varchar](1024),
		@execServerSource			[varchar](1024),
		@queryToRun					[nvarchar](max),
		@queryToRunS				[nvarchar](max),
		@queryToRunD				[nvarchar](max),
		@strMessage					[nvarchar](512),
		@queryParam					[nvarchar](512),
		@tmpCount1					[int],
		@tmpCount2					[int],
		@ReturnValue				[int],
		@nestExecutionLevel			[tinyint],
		@flgOptionsNested			[int],
		@affectedDependentObjects	[nvarchar](max)


DECLARE @schemaNameSource		[sysname],
		@schemaNameDestination	[sysname],
		@tableNameSource		[sysname],
		@tableNameDestination	[sysname],
		@objectIDSource			[int],
		@objectIDDestination	[int],
		@insertColumnList		[nvarchar](max),
		@selectColumnList		[nvarchar](max),
		@hasIdentity			[bit]


IF object_id('#tmpDBSource') IS NOT NULL DROP TABLE #tmpDBSource
CREATE TABLE #tmpDBSource 
		(
			[object_id]		[int],
			[schema_name]	[sysname],
			[table_name]	[sysname]
		)

IF object_id('#tmpDBDestination') IS NOT NULL DROP TABLE #tmpDBDestination
CREATE TABLE #tmpDBDestination 
		(
			[object_id]		[int],
			[schema_name]	[sysname],
			[table_name]	[sysname],
			[priority]		[int]
		)

IF object_id('#tmpDBMixed') IS NOT NULL DROP TABLE #tmpDBMixed
CREATE TABLE #tmpDBMixed 
		(
			[source_object_id]			[int],
			[source_schema_name]		[sysname],
			[source_table_name]			[sysname],
			[destination_object_id]		[int],
			[destination_schema_name]	[sysname],
			[destination_table_name]	[sysname],
			[priority]					[int]
		)

IF object_id('#tmpTableColumns') IS NOT NULL DROP TABLE #tmpTableColumns
CREATE TABLE #tmpTableColumns 
		(
			[column_order]	[smallint],
			[column_name]	[sysname],
			[data_type]		[sysname],
			[is_identity]	[bit]
		)

---------------------------------------------------------------------------------------------
--get SQL Server running major version and database compatibility level
---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@destinationServerEdition		[sysname],
			@destinationServerVersionStr	[sysname],
			@destinationServerVersionNum	[numeric](9,6),
			@destinationServerEngine		[int],
			@nestedExecutionLevel			[tinyint]

EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @destinationServerName,
										@serverEdition			= @destinationServerEdition OUT,
										@serverVersionStr		= @destinationServerVersionStr OUT,
										@serverVersionNum		= @destinationServerVersionNum OUT,
										@serverEngine			= @destinationServerEngine OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

-----------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON
-----------------------------------------------------------------------------------------------------------------------------------------
-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 0
		SET @execServerSource		='[' + @sourceServerName + '].' + [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + '.[dbo].sp_executesql'
		SET @execServerDestination	='[' + @destinationServerName + '].' + [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.[dbo].sp_executesql'

		-----------------------------------------------------------------------------------------------------------------------------------------
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + CASE WHEN @sourceServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + '; ' ELSE N'' END + N'
										SELECT [object_id], OBJECT_SCHEMA_NAME([object_id]' + CASE WHEN @sourceServerName<>@@SERVERNAME THEN N', DB_ID(''' + @sourceDB + N''')' ELSE N'' END + N') AS [schema_name], [name] AS [table_name]
										FROM ' + CASE WHEN @sourceServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + '.' ELSE N'' END + N'sys.tables
										WHERE [is_ms_shipped] = 0
											AND OBJECT_SCHEMA_NAME([object_id]' + CASE WHEN @sourceServerName<>@@SERVERNAME THEN N', DB_ID(''' + @sourceDB + N''')' ELSE N'' END + N') LIKE ''' + @sourceTableSchema + '''
											AND (   [name] LIKE ''' + @sourceTableName + '''
												 OR CHARINDEX(''' + @sourceTableName + ''', [name]) > 0
												)'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpDBSource ([object_id], [schema_name], [table_name])
				EXEC sp_executesql @queryToRun
		SELECT @tmpCount1=count(*) from #tmpDBSource

		-----------------------------------------------------------------------------------------------------------------------------------------
		/* get the actual/destination table''s dependencies */
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + CASE WHEN @destinationServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '; ' ELSE N'' END + N'
		;WITH referencedTables AS
				(
					SELECT DISTINCT sos.[object_id], OBJECT_SCHEMA_NAME(sos.[object_id]' + CASE WHEN @sourceServerName<>@@SERVERNAME THEN N', DB_ID(''' + @destinationDB + N''')' ELSE N'' END + N') AS [schema_name], sos.[name] AS [table_name], sod.[object_id] AS [parent_object_id], ''child'' AS category
					FROM ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.foreign_keys fk
					INNER JOIN ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.objects sos ON fk.[parent_object_id] = sos.[object_id]
					INNER JOIN ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.objects sod ON fk.[referenced_object_id] = sod.[object_id]
					WHERE fk.[is_disabled] = 0
							AND sos.[is_ms_shipped] = 0
					UNION
					SELECT DISTINCT sod.[object_id], OBJECT_SCHEMA_NAME(sod.[object_id]' + CASE WHEN @sourceServerName<>@@SERVERNAME THEN N', DB_ID(''' + @destinationDB + N''')' ELSE N'' END + N') AS [schema_name], sod.[name] AS [table_name], NULL AS [parent_object_id], ''parent'' AS category
					FROM ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.foreign_keys fk
					INNER JOIN ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.objects sos ON fk.[parent_object_id] = sos.[object_id]
					INNER JOIN ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.objects sod ON fk.[referenced_object_id] = sod.[object_id]
					WHERE fk.[is_disabled] = 0
						AND sos.[is_ms_shipped] = 0
				),
				schemaHierarchy AS
				(
					SELECT	[object_id], [schema_name], [table_name], [parent_object_id], 1 AS [priority]
					FROM    referencedTables
					WHERE   [parent_object_id] IS NULL
					UNION ALL
					SELECT  rt.[object_id], rt.[schema_name], rt.[table_name], rt.[parent_object_id], [priority] + 1 AS [priority]
					FROM    referencedTables rt
					INNER JOIN schemaHierarchy CTE ON rt.[parent_object_id] = CTE.[object_id] AND rt.[object_id] <> CTE.[object_id]
					),
				tableCopyOrder AS
				(
					SELECT [object_id], OBJECT_SCHEMA_NAME([object_id]' + CASE WHEN @sourceServerName<>@@SERVERNAME THEN N', DB_ID(''' + @destinationDB + N''')' ELSE N'' END + N') AS [schema_name], [name] AS [table_name], 0 AS [priority]
					FROM ' + CASE WHEN @destinationServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' ELSE N'' END + N'sys.tables
					WHERE [object_id] NOT IN (SELECT [object_id] FROM schemaHierarchy)
						AND [is_ms_shipped] = 0
					UNION ALL
					SELECT [object_id], [schema_name], [table_name], MAX([priority]) AS [priority]
					FROM schemaHierarchy
					GROUP BY [object_id], [schema_name], [table_name]
				)
				SELECT * 
				FROM tableCopyOrder
				WHERE [schema_name] LIKE ''' + @destinationTableSchema + '''
					AND (   [table_name] LIKE ''' + @destinationTableName + '''
						 OR CHARINDEX(''' + @destinationTableName + ''', [table_name]) > 0
						)'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@destinationServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpDBDestination ([object_id], [schema_name], [table_name], [priority])
				EXEC sp_executesql @queryToRun
		SELECT @tmpCount2=count(*) from #tmpDBDestination

		-----------------------------------------------------------------------------------------------------------------------------------------
		/* data copy operations will be made only on the common tables, unless a map has been provided (1-1) */
		IF @sourceTableSchema<>'%' AND @sourceTableName<>'%' AND @destinationTableSchema<>'%' AND @destinationTableName<>'%' AND @tmpCount1 = 1 AND @tmpCount2 = 1
			SET @queryToRun=N'SELECT s.[object_id], s.[schema_name], s.[table_name],
									 d.[object_id], d.[schema_name], d.[table_name], 1 AS [priority]
							 FROM #tmpDBSource AS s, #tmpDBDestination AS d'
		ELSE
			SET @queryToRun=N'SELECT s.[object_id], s.[schema_name], s.[table_name],
									 d.[object_id], d.[schema_name], d.[table_name], d.[priority]
							FROM #tmpDBSource AS s
							INNER JOIN #tmpDBDestination AS d ON s.[schema_name] = d.[schema_name] AND s.[table_name] = d.[table_name]'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		INSERT	INTO #tmpDBMixed([source_object_id], [source_schema_name], [source_table_name], [destination_object_id], [destination_schema_name], [destination_table_name], [priority])
				EXEC sp_executesql @queryToRun

		----------------------------------------------------------------------------------------------------------------------------------------
		/* analyze only common tables for common columns (schema may be different) */
		-----------------------------------------------------------------------------------------------------------------------------------------
		DECLARE crsDBMixed CURSOR LOCAL FAST_FORWARD FOR	SELECT	[source_object_id], [source_schema_name], [source_table_name], 
																	[destination_object_id], [destination_schema_name], [destination_table_name]
															FROM #tmpDBMixed 
															ORDER BY [priority], [source_schema_name], [source_table_name], [destination_schema_name], [destination_table_name]
		OPEN crsDBMixed
		FETCH NEXT FROM crsDBMixed INTO @objectIDSource, @schemaNameSource, @tableNameSource, @objectIDDestination, @schemaNameDestination, @tableNameDestination
		WHILE @@FETCH_STATUS=0
			begin
				------------------------------------------------------------------------------------------------------------------------
				/* analyze table's schema */
				SET @strMessage='Analyze Source: [' + @sourceServerName + '].' + [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@schemaNameSource, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tableNameSource, 'quoted') + ' vs. Destination: [' + @destinationServerName + '].' + [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@schemaNameDestination, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tableNameDestination, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				SET @queryToRun='SELECT sc.[column_id], sc.[name] AS [source_column_name], sdt.[name] AS [source_data_type]
								FROM ' + [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + N'.sys.columns sc
								INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + N'.sys.types sdt ON sc.[user_type_id] = sdt.[user_type_id]
								WHERE	sc.[is_computed] = 0
										AND sc.[object_id] = @objectIDSource'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
				IF @sourceServerName <> @@SERVERNAME
					SET @queryToRun = REPLACE(@queryToRun, '@objectIDSource', @objectIDSource)

				SET @queryToRunD = 'SELECT sc.[column_id], sc.[name] AS [destination_column_name], sdt.[name] AS [destination_data_type], sc.[is_identity]
								FROM ' + [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + N'.sys.columns sc
								INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + N'.sys.types sdt ON sc.[user_type_id] = sdt.[user_type_id]
								WHERE	sc.[is_computed] = 0
										AND sc.[object_id] = @objectIDDestination'
				SET @queryToRunD = [dbo].[ufn_formatSQLQueryForLinkedServer](@destinationServerName, @queryToRunD)
				IF @destinationServerName <> @@SERVERNAME
					SET @queryToRunD = REPLACE(@queryToRunD, '@objectIDDestination', @objectIDDestination)

				SET @queryToRun = N'SELECT	s.[column_id], s.[source_column_name], d.[destination_data_type], d.[is_identity]
									FROM (' + @queryToRun + N') s
									INNER JOIN (' + @queryToRunD + N') d ON s.[source_column_name] = d.[destination_column_name] COLLATE SQL_Latin1_General_CP1_CI_AS'
				SET @queryParam = N'@objectIDSource [int], @objectIDDestination [int]'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #tmpTableColumns
				INSERT	INTO #tmpTableColumns ([column_order], [column_name], [data_type], [is_identity])
						EXEC sp_executesql @queryToRun, @queryParam , @objectIDSource = @objectIDSource
																	, @objectIDDestination = @objectIDDestination
				
				------------------------------------------------------------------------------------------------------------------------
				--disable table non-clustered indexes
				IF @flgOptions & 8 = 8
					begin
						SET @nestExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @destinationServerName,
																@dbName						= @destinationDB,
																@tableSchema				= @schemaNameDestination,
																@tableName					= @tableNameDestination,
																@indexName					= '%',
																@indexID					= NULL,
																@partitionNumber			= 1,
																@flgAction					= 4,
																@flgOptions					= DEFAULT,
																@maxDOP						= 1,
																@executionLevel				= @nestExecutionLevel,
																@affectedDependentObjects	= @affectedDependentObjects OUT,
																@debugMode					= @debugMode
					end

				------------------------------------------------------------------------------------------------------------------------
				--disable table constraints
				------------------------------------------------------------------------------------------------------------------------
				IF @flgOptions & 16 = 16
					begin
						SET @nestExecutionLevel = @executionLevel + 1
						SET @flgOptionsNested = 3 + (@flgOptions & 2048)
						EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName		= @destinationServerName ,
																	@dbName				= @destinationDB,
																	@tableSchema		= @schemaNameDestination, 
																	@tableName			= @tableNameDestination,
																	@constraintName		= '%',
																	@flgAction			= 0,
																	@flgOptions			= @flgOptionsNested,
																	@executionLevel		= @nestExecutionLevel,
																	@debugMode			= @debugMode
					end

				------------------------------------------------------------------------------------------------------------------------
				--disable table triggers
				------------------------------------------------------------------------------------------------------------------------
				IF @flgOptions & 8192 = 8192
					begin
						SET @nestExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_mpAlterTableTriggers]		@sqlServerName		= @destinationServerName,
																	@dbName				= @destinationDB,
																	@tableSchema		= @schemaNameDestination, 
																	@tableName			= @tableNameDestination,
																	@triggerName		= '%',
																	@flgAction			= 0,
																	@flgOptions			= @flgOptions,
																	@executionLevel		= @nestExecutionLevel,
																	@debugMode			= @debugMode
					end			
								
				---------------------------------------------------------------------------------------------------------
				SET @insertColumnList = ''
				SET @selectColumnList = ''
				SET @hasIdentity = 0
				SELECT  @insertColumnList = STUFF((	SELECT ',' + '[' + [column_name] + ']'
													FROM #tmpTableColumns
													FOR XML PATH('')
													) ,1,1,''),
						@selectColumnList = STUFF((	SELECT ',' + CASE WHEN [data_type] <> 'xml' THEN '[' + [column_name] + ']' ELSE 'CONVERT(xml, [' + [column_name] + '])' END
													FROM #tmpTableColumns
													FOR XML PATH('')
													) ,1,1,''),
						@hasIdentity = (SELECT COUNT(*) FROM #tmpTableColumns WHERE [is_identity] = 1)

				/* build data copy script */		
				SET @queryToRunD = N''
				SET @queryToRunS = N''
				SET @queryToRun  = N''

				IF @hasIdentity = 1
					SET @queryToRunD = @queryToRunD + N'SET IDENTITY_INSERT ' + 
										CASE WHEN @sourceServerName <> @destinationServerName
												OR (@sourceServerName = @destinationServerName AND @sourceDB <> @destinationDB)
											 THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + N'.'
											 ELSE N''
										END + [dbo].[ufn_getObjectQuoteName](@schemaNameDestination, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tableNameDestination, 'quoted') + ' ON;' + CHAR(13)

				SET @queryToRunS = @queryToRunS + 
									N'INSERT INTO ' + 
									CASE WHEN @sourceServerName <> @destinationServerName
										 THEN N'[' + @destinationServerName + N'].' 
										 ELSE N''
									END +
									CASE WHEN  @sourceServerName <> @destinationServerName
												OR (@sourceServerName = @destinationServerName AND @sourceDB <> @destinationDB)
										THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + N'.'
										ELSE N''
									END + 
									[dbo].[ufn_getObjectQuoteName](@schemaNameDestination, 'quoted') + N'.' + [dbo].[ufn_getObjectQuoteName](@tableNameDestination, 'quoted') + N' (' + @insertColumnList + N')' + CHAR(13) + 
									N'SELECT ' + @selectColumnList + N' FROM ' +
									CASE WHEN @sourceServerName <> @destinationServerName
										 THEN N'[' + @sourceServerName + N'].' 
										 ELSE N''
									END + 
									CASE WHEN  @sourceServerName <> @destinationServerName
												OR (@sourceServerName = @destinationServerName AND @sourceDB <> @destinationDB)
										THEN [dbo].[ufn_getObjectQuoteName](@sourceDB, 'quoted') + N'.' 
										ELSE N''
									END + 
									[dbo].[ufn_getObjectQuoteName](@schemaNameSource, 'quoted') + N'.' + [dbo].[ufn_getObjectQuoteName](@tableNameSource, 'quoted') + N';' + CHAR(13)

				IF @hasIdentity = 1
					SET @queryToRun = @queryToRun + N'SET IDENTITY_INSERT ' + 
										CASE WHEN @sourceServerName <> @destinationServerName
												OR (@sourceServerName = @destinationServerName AND @sourceDB <> @destinationDB)
											 THEN [dbo].[ufn_getObjectQuoteName](@destinationDB, 'quoted') + N'.'
											 ELSE N''
										END + [dbo].[ufn_getObjectQuoteName](@schemaNameDestination, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@tableNameDestination, 'quoted') + ' OFF;'
				
				/* push method checks */
				IF @flgActions = 1 AND @hasIdentity = 1
					begin
						IF @sourceServerName <> @destinationServerName
							begin
								SET @strMessage= 'ERROR: Tables with IDENTITY columns cannot be used along with PUSH copy option on REMOTE servers'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=1
							end
						ELSE
							begin
								SET @queryToRunS = @queryToRunD + @queryToRunS + @queryToRun;
								SET @queryToRunD = N''
								SET @queryToRun  = N''
							end
					end

				/* pull method checks*/
				IF @flgActions = 0 AND @destinationServerEngine IN (5, 6) /* Azure SQL database*/
					begin
						IF @sourceServerName <> @destinationServerName OR @sourceDB <> @destinationDB
							begin
								SET @strMessage= 'ERROR: Pull data copy method cannot be used on Azure SQL Database as it does not support linked servers.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=1
							end
						ELSE
							begin
								SET @queryToRunS = @queryToRunD + @queryToRunS + @queryToRun;
								SET @queryToRunD = N''
								SET @queryToRun  = N''
							end					
					end
				ELSE
					IF @flgActions = 0 AND @sourceServerName = @destinationServerName
						begin
							SET @queryToRunS = @queryToRunD + @queryToRunS + @queryToRun;
							SET @queryToRunD = N''
							SET @queryToRun  = N''
						end

				--------------------------------------------------------------------------------------------------------
				SET @nestExecutionLevel=@executionLevel+1
				EXEC @tmpCount1 = [dbo].[usp_tableGetRowCount]	@sqlServerName	= @destinationServerName,
																@databaseName	= @destinationDB,
																@schemaName		= @schemaNameDestination,
																@tableName		= @tableNameDestination,
																@executionLevel	= @nestExecutionLevel,
																@debugMode		= @debugMode

				SET @strMessage= 'Total Rows In Destination Table Before Insert: ' + CAST(@tmpCount1 AS varchar(20))
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
		
				--------------------------------------------------------------------------------------------------------
				SET @nestExecutionLevel=@executionLevel+1
				EXEC @tmpCount2 = [dbo].[usp_tableGetRowCount]	@sqlServerName	= @sourceServerName,
																@databaseName	= @sourceDB,
																@schemaName		= @schemaNameSource,
																@tableName		= @tableNameSource,
																@executionLevel	= @nestExecutionLevel,
																@debugMode		= @debugMode

				SET @strMessage= 'Total Rows In Source Table To Be Copied In Destination: ' + CAST(@tmpCount2 AS varchar(20))
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
				--------------------------------------------------------------------------------------------------------
				
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRunD, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRunS, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				SET @strMessage= 'Inserting records... '
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				/* set identity_insert on, if needed */
				EXEC @execServerDestination @queryToRunD
				
				/* insert/push the rows from the source */
				EXEC @execServerSource @queryToRunS

				/* set identity_insert off, if needed */
				EXEC @execServerDestination @queryToRun

				SET @ReturnValue=@@ERROR
				IF @@ERROR <> 0
					begin
						SET @strMessage='Error Returned: ' + CAST(@ReturnValue AS varchar)
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
					end						


				--------------------------------------------------------------------------------------------------------
				SET @nestExecutionLevel=@executionLevel+1
				EXEC @tmpCount1 = [dbo].[usp_tableGetRowCount]	@sqlServerName	= @destinationServerName,
																@databaseName	= @destinationDB,
																@schemaName		= @schemaNameDestination,
																@tableName		= @tableNameDestination,
																@executionLevel	= @nestExecutionLevel,
																@debugMode		= @debugMode
																
				SET @strMessage= 'Total Rows In Destination Table After Insert: ' + CAST(@tmpCount1 AS varchar(20))
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				---------------------------------------------------------------------------------------------------------
				--rebuild table non-clustered indexes
				---------------------------------------------------------------------------------------------------------
				IF @flgOptions & 8 = 8
					begin
						SET @nestExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @destinationServerName,
																@dbName						= @destinationDB,
																@tableSchema				= @schemaNameDestination,
																@tableName					= @tableNameDestination,
																@indexName					= '%',
																@indexID					= NULL,
																@partitionNumber			= 1,
																@flgAction					= 1,
																@flgOptions					= DEFAULT,
																@maxDOP						= 1,
																@executionLevel				= @nestExecutionLevel, 
																@affectedDependentObjects	= @affectedDependentObjects OUT,
																@debugMode					= @debugMode
					end

				---------------------------------------------------------------------------------------------------------
				--enable table constraints
				---------------------------------------------------------------------------------------------------------
				IF @flgOptions & 16 = 16
					begin
						SET @nestExecutionLevel = @executionLevel + 1
						SET @flgOptionsNested = 3 + (@flgOptions & 2048)
	
						--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
						IF @flgOptions & 64 = 64
							SET @flgOptionsNested = @flgOptionsNested + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

						EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName		= @destinationServerName ,
																	@dbName				= @destinationDB,
																	@tableSchema		= @schemaNameDestination, 
																	@tableName			= @tableNameDestination,
																	@constraintName		= '%',
																	@flgAction			= 1,
																	@flgOptions			= @flgOptionsNested,
																	@executionLevel		= @nestExecutionLevel, 
																	@debugMode			= @debugMode
					end

				---------------------------------------------------------------------------------------------------------
				--enable table triggers
				---------------------------------------------------------------------------------------------------------
				IF @flgOptions & 8192 = 8192
					begin
						SET @nestExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_mpAlterTableTriggers]		@sqlServerName		= @destinationServerName,
																	@dbName				= @destinationDB,
																	@tableSchema		= @schemaNameDestination, 
																	@tableName			= @tableNameDestination,
																	@triggerName		= '%',
																	@flgAction			= 1,
																	@flgOptions			= @flgOptions,
																	@executionLevel		= @nestExecutionLevel, 
																	@debugMode			= @debugMode
					end			

				---------------------------------------------------------------------------------------------------------
				FETCH NEXT FROM crsDBMixed INTO @objectIDSource, @schemaNameSource, @tableNameSource, @objectIDDestination, @schemaNameDestination, @tableNameDestination
			end
		CLOSE crsDBMixed
		DEALLOCATE crsDBMixed

		--------------------------------------------------------------------------------------------------------
		--checkident
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName	= @destinationServerName,
													@dbName			= @destinationDB,
													@tableSchema	= @sourceTableSchema,
													@tableName		= @sourceTableName,
													@flgActions		= 32,
													@flgOptions		= DEFAULT,
													@executionLevel	= @nestExecutionLevel,
													@debugMode		= @debugMode

		--update usage
		EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName	= @destinationServerName,
													@dbName			= @destinationDB,
													@tableSchema	= @destinationTableSchema,
													@tableName		= @destinationTableName,
													@flgActions		= 64,
													@flgOptions		= DEFAULT,
													@executionLevel	= @nestExecutionLevel,
													@debugMode		= @debugMode

		-----------------------------------------------------------------------------------------------------------------------------------------
		--sters tabelele temporare create
		IF object_id('#tmpDBSource') IS NOT NULL DROP TABLE #tmpDBSource
		IF object_id('#tmpDBDestination') IS NOT NULL DROP TABLE #tmpDBDestination
		IF object_id('#tmpDBMixed') IS NOT NULL DROP TABLE #tmpDBMixed
		IF object_id('#tmpTableColumns') IS NOT NULL DROP TABLE #tmpTableColumns
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
	SET @ReturnValue = -1

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

RETURN @ReturnValue
GO
