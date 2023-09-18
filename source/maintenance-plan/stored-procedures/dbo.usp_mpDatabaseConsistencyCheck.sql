RAISERROR('Create procedure: [dbo].[usp_mpDatabaseConsistencyCheck]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseConsistencyCheck]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseConsistencyCheck]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseConsistencyCheck]
		@sqlServerName				[sysname]=@@SERVERNAME,
		@dbName						[sysname],
		@tableSchema				[sysname]	=  '%',
		@tableName					[sysname]   =  '%',
		@flgActions					[smallint]	=   12,
		@flgOptions					[int]		=    0,
		@maxDOP						[smallint]	=	 1,
		@maxRunningTimeInMinutes	[smallint]	=    0,
		@skipObjectsList			[nvarchar](1024) = NULL,
		@executionLevel				[tinyint]	=    0,
		@debugMode					[bit]		=    0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan 
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : Consistency Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify % for all tables or a table name to be analyzed
--		@flgActions		1	- perform database consistency check (DBCC CHECKDB)
--							  should be performed weekly
--						2	- perform table consistency check (DBCC CHECKTABLE)
--							  should be performed weekly
--					    4   - perform consistency check of disk space allocation structures (DBCC CHECKALLOC) (default)
--							  should be performed daily
--					    8   - perform consistency check of catalogs (DBCC CHECKCATALOG) (default)
--							  should be performed daily
--					   16   - perform consistency check of table constraints (DBCC CHECKCONSTRAINTS)
--							  should be performed weekly
--					   32   - perform consistency check of table identity value (DBCC CHECKIDENT)
--							  should be performed weekly
--					   64   - perform correction to space usage (DBCC UPDATEUSAGE)
--							  should be performed once at 2 weeks
--					  128 	- Cleaning wasted space in Database (variable-length column) (DBCC CLEANTABLE)
--							  should be performed once a year
--		@flgOptions	    1	- run DBCC CHECKDB/DBCC CHECKTABLE using PHYSICAL_ONLY
--							  by default DBCC CHECKDB is doing all consistency checks and for a VLDB it may take a very long time
--					    2  - use NOINDEX when running DBCC CHECKTABLE. Index consistency errors are not critical
--					   32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--		@maxDOP						- when applicable, use this MAXDOP value (ex. dbcc checkdb
--		@maxRunningTimeInMinutes	- the number of minutes the optimization job will run. after time exceeds, it will exist. 0 or null means no limit
--		@skipObjectsList			- comma separated list of the objects (tables) to be excluded from maintenance.
--		@debugMode					- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= 'dbaTDPMon',
												@tableSchema			= '%',
												@tableName				= '%',
												@flgActions				= DEFAULT,
												@flgOptions				= DEFAULT,
												@maxRunningTimeInMinutes= DEFAULT,
												@debugMode				= DEFAULT
*/

DECLARE		@queryToRun  					[nvarchar](max),
			@queryParameters				[nvarchar](512),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@DBCCCheckTableBatchSize 		[int],
			@errorCode						[int],
			@databaseStatus					[int],
			@dbi_dbccFlags					[int],
			@executionDBName				[sysname],
			@stopTimeLimit					[datetime], 
			@isAzureSQLDatabase				[bit]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--determine when to stop current optimization task, based on @maxRunningTimeInMinutes value
---------------------------------------------------------------------------------------------
IF ISNULL(@maxRunningTimeInMinutes, 0)=0
	SET @stopTimeLimit = CONVERT([datetime], '9999-12-31 23:23:59', 120)
ELSE
	SET @stopTimeLimit = DATEADD(minute, @maxRunningTimeInMinutes, GETDATE())

---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint],
			@serverEngine					[int]

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@serverEngine			= @serverEngine OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

---------------------------------------------------------------------------------------------
SET @isAzureSQLDatabase = CASE WHEN @serverEngine IN (5, 6) THEN 1 ELSE 0 END

IF @isAzureSQLDatabase = 1
	begin
		SELECT @sqlServerName = CASE WHEN ss.[name] IS NOT NULL THEN ss.[name] ELSE NULL END 
		FROM	[dbo].[vw_catalogDatabaseNames] cdn
		LEFT JOIN [sys].[servers] ss ON ss.[catalog] = cdn.[database_name] COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE 	cdn.[instance_name] = @sqlServerName
				AND cdn.[active]=1
				AND cdn.[database_name] = @dbName

		IF @sqlServerName IS NULL
			begin
				SET @queryToRun=N'Could not find a linked server defined for Azure SQL database: [' + @dbName + ']' 
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
			end
	end

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @clusterName		 [sysname],
		@agInstanceRoleDesc	 [sysname],
		@agReadableSecondary [sysname],
		@agStopLimit		 [int],
		@actionType			 [sysname],
		@actionName			 [sysname]

SET @agStopLimit = 0
SET @actionType = NULL

IF @flgActions &  64 = 64	SET @actionType = 'update space usage'
IF @flgActions & 128 = 128	SET @actionType = 'clean wasted space - table'

SET @actionName	= 'database maintenance'
IF @flgActions &  1 =  1	SET @actionName = 'database consistency check'
IF @flgActions &  2 =  2	SET @actionName = 'database consistency check'
IF @flgActions &  4 =  4	SET @actionName = 'database consistency check'
IF @flgActions &  8 =  8	SET @actionName = 'database consistency check'
IF @flgActions & 16 = 16	SET @actionName = 'database consistency check'

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL AND @isAzureSQLDatabase = 0
	begin
		EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																			@dbName				= @dbName,
																			@actionName			= @actionName,
																			@actionType			= @actionType,
																			@flgActions			= @flgActions,
																			@flgOptions			= @flgOptions OUTPUT,
																			@clusterName		= @clusterName OUTPUT,
																			@agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT,
																			@agReadableSecondary= @agReadableSecondary OUTPUT,
																			@executionLevel		= @executionLevel,
																			@debugMode			= @debugMode
	end
IF @agStopLimit <> 0
	RETURN 0

SET @executionDBName = @dbName
IF @clusterName IS NOT NULL AND @agInstanceRoleDesc = 'SECONDARY' AND @agReadableSecondary='NO' 
	SET @executionDBName = 'master'

---------------------------------------------------------------------------------------------
DECLARE @compatibilityLevel [tinyint]
IF object_id('tempdb..#databaseCompatibility') IS NOT NULL 
	DROP TABLE #databaseCompatibility

CREATE TABLE #databaseCompatibility
	(
		[compatibility_level]		[tinyint]
	)


SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'SELECT [compatibility_level] FROM sys.databases WHERE [name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@compatibilityLevel, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC sp_executesql @queryToRun

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility


---------------------------------------------------------------------------------------------
SET @DBCCCheckTableBatchSize = 65536
SET @CurrentTableSchema		 = @tableSchema
SET @errorCode				 = 0

---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseTableList') IS NOT NULL 
	DROP TABLE #databaseTableList

CREATE TABLE #databaseTableList(
								[table_schema]	[sysname]	NULL,
								[table_name]	[sysname]	NULL,
								[type]			[sysname]	NULL
								)
CREATE INDEX IX_databaseTableList_TableName ON #databaseTableList([table_name])

---------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2 OR @flgActions & 16 = 16 OR @flgActions & 64 = 64 OR @flgActions & 128 = 128
	begin
		--get table list that will be analyzed including materialized views; will pick only tables with reserved pages
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT DISTINCT ob.[table_schema], ob.[table_name], ob.[type]
FROM (
		SELECT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name], obj.[type]
		FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.objects obj WITH (READPAST)
		INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + N'''
				AND sch.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + N'''
				AND obj.[is_ms_shipped] = 0' +

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL

		SELECT DISTINCT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name], obj.[type]
		FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.indexes idx WITH (READPAST)
		INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.objects obj WITH (READPAST) ON obj.[object_id] = idx.[object_id]
		INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + N'''
				AND sch.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + N'''
				AND obj.[is_ms_shipped] = 0'
		END + N'
	)ob
INNER JOIN
	(
		SELECT	ps.[object_id],
				sch.[name]	AS [schema_name],
				so.[name]	AS [table_name],
				ps.[reserved_page_count]
		FROM (
				SELECT	ps.[object_id]
						, SUM (ps.[reserved_page_count]) AS [reserved_page_count]
				FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.dm_db_partition_stats ps WITH (READPAST)
				GROUP BY ps.[object_id]
			) AS ps
		INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.objects so  WITH (READPAST) ON so.[object_id] = ps.[object_id] 
		INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.schemas sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id]
		WHERE	so.[type] in (''S'', ''U'', ''V'')
			AND ps.[reserved_page_count] > 0
	)ps ON ob.[object_id] = ps.[object_id]'	

		SET @queryToRun = @queryToRun + CASE WHEN @skipObjectsList IS NOT NULL
											 THEN N'	WHERE ob.[table_name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
											 ELSE N'' 
										END

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #databaseTableList
		INSERT	INTO #databaseTableList([table_schema], [table_name], [type])
				EXEC sp_executesql @queryToRun

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseTableList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[table_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE	@flgActions & osl.[flg_actions] = osl.[flg_actions]
								AND osl.[active] = 1'
		SET @queryParameters = '@flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @flgActions = @flgActions
	end

--------------------------------------------------------------------------------------------------
--when running DBCC CHECKDB, check if DATA_PURITY option should be used or not (run only when dbi_dbccFlags=0)
--------------------------------------------------------------------------------------------------
IF (@flgActions & 1 = 1 OR @flgActions & 2 = 2) AND @flgOptions & 1 = 0 AND @isAzureSQLDatabase = 0
	begin
		IF object_id('tempdb..#dbi_dbccFlags') IS NOT NULL DROP TABLE #dbccLastKnownGood
		CREATE TABLE #dbi_dbccFlags
		(
			[Value]					[sysname]			NULL
		)

		IF object_id('tempdb..#dbccDBINFO') IS NOT NULL DROP TABLE #dbccDBINFO
		CREATE TABLE #dbccDBINFO
			(
				[id]				[int] IDENTITY(1,1),
				[ParentObject]		[varchar](255),
				[Object]			[varchar](255),
				[Field]				[varchar](255),
				[Value]				[varchar](255)
			)
	
		IF @sqlServerName <> @@SERVERNAME
			begin
				IF @serverVersionNum < 11
					SET @queryToRun = N'SELECT MAX([VALUE]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC (''''DBCC DBINFO (' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N') WITH TABLERESULTS, NO_INFOMSGS'''')'')x
										WHERE [Field]=''dbi_dbccFlags'''
				ELSE
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC (''''DBCC DBINFO (' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N') WITH TABLERESULTS, NO_INFOMSGS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
										WHERE [Field]=''dbi_dbccFlags'''
			end
		ELSE
			begin							
				SET @queryToRun='DBCC DBINFO (''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''') WITH TABLERESULTS, NO_INFOMSGS'
				INSERT	INTO #dbccDBINFO
						EXEC sp_executesql @queryToRun

				SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccFlags'''											
			end

		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				
		DELETE FROM #dbi_dbccFlags
		INSERT	INTO #dbi_dbccFlags([Value])
				EXEC sp_executesql @queryToRun

		SELECT @dbi_dbccFlags = ISNULL([Value], 0)
		FROM #dbi_dbccFlags
		
	end
SET @dbi_dbccFlags = ISNULL(@dbi_dbccFlags, 0)


--------------------------------------------------------------------------------------------------
--database consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Database consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + '... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKDB(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS' + CASE WHEN @flgOptions & 1 = 1 THEN ', PHYSICAL_ONLY' ELSE '' END

		IF @flgOptions & 1 = 0 AND @dbi_dbccFlags <> 2
			SET @queryToRun = @queryToRun + ', DATA_PURITY'

		IF @compatibilityLevel >= 100 AND @flgOptions & 1 = 0
			SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

		IF @serverVersionNum > = 12.05000 /* MAXDOP: applies to: SQL Server 2014 SP2 onwards */
			SET @queryToRun = @queryToRun + ', MAXDOP=' + CAST(@maxDOP AS [nvarchar])

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @executionDBName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end	


--------------------------------------------------------------------------------------------------
--tables and views consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Tables/views consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + CASE WHEN @flgOptions & 2 = 2 THEN '(NOINDEX)' ELSE '' END + '... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
															FROM #databaseTableList	
															ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName=[dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKTABLE(''' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'sql'), 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'sql'), 'quoted') + '''' + CASE WHEN @flgOptions & 2 = 2 THEN ', NOINDEX' ELSE '' END + ') WITH ALL_ERRORMSGS, NO_INFOMSGS'
				
				IF @dbi_dbccFlags <> 2
					SET @queryToRun = @queryToRun + ', DATA_PURITY'
				
				IF @compatibilityLevel >= 100 AND @flgOptions & 2 = 0
					SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

				IF @serverVersionNum > = 12.05000 /* MAXDOP: applies to: SQL Server 2014 SP2 onwards */
					SET @queryToRun = @queryToRun + ', MAXDOP=' + CAST(@maxDOP AS [nvarchar])

				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
				
				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database consistency check - tables/views',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end			


--------------------------------------------------------------------------------------------------
--allocation structures consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 4 = 4 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Allocation structures consistency check ... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKALLOC(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS'
		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @executionDBName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check - allocation structures',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end			


--------------------------------------------------------------------------------------------------
--catalogs consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 8 = 8 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Catalogs consistency check ... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKCATALOG(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') WITH NO_INFOMSGS'
		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @executionDBName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check - catalogs',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end			


--------------------------------------------------------------------------------------------------
--table constraints consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 16 = 16 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Table constraints consistency check ... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
															FROM #databaseTableList	
															ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKCONSTRAINTS(''' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'sql'), 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'sql'), 'quoted') + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS'
				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database consistency check - table constraints',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end			


--------------------------------------------------------------------------------------------------
--table identity value consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 32 = 32 AND (GETDATE() <= @stopTimeLimit)
	begin
		DECLARE @runCheck [bit]
		SET @runCheck = 1
		IF @isAzureSQLDatabase = 0
			begin
				--get database status
				IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
				CREATE TABLE #serverPropertyConfig
							(
								[value]			[sysname]	NULL
							)
			
				SET @queryToRun = N'SELECT [status] FROM master.dbo.sysdatabases WHERE [name]=''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''' 
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #serverPropertyConfig
				INSERT	INTO #serverPropertyConfig([value])
						EXEC sp_executesql @queryToRun

				SELECT @databaseStatus = [value]
				FROM #serverPropertyConfig

				IF	@databaseStatus & 32 = 32				/* LOADING */
					OR @databaseStatus & 64 = 64			/* PRE RECOVERY */
					OR @databaseStatus & 128 = 128			/* RECOVERING */
					OR @databaseStatus & 256 = 256			/* NOT RECOVERED */
					OR @databaseStatus & 512 = 512			/* OFFLINE */
					OR @databaseStatus & 1024 = 1024		/* READ ONLY */
					OR @databaseStatus & 2048 = 2048		/* DBO USE ONLY */
					OR @databaseStatus & 4096 = 4096		/* SINGLE USER */
					OR @databaseStatus & 32768 = 32768		/* EMERGENCY MODE */
					OR @databaseStatus & 2097152 = 2097152	/* STANDBY */
					OR @databaseStatus & 4194584 = 4194584	/* SUSPECT */
					OR @databaseStatus = 0
					begin
						SET @queryToRun='Current database state does not allow running DBCC CHECKIDENT. It will be skipped.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
						SET @runCheck = 0
					end
				end
		IF @runCheck = 1
			begin
				IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun=N'Table identity value consistency check ... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
				---------------------------------------------------------------------------------------------
				--create temporary tables that will be used 
				---------------------------------------------------------------------------------------------
				IF object_id('tempdb..#databaseTableListIdent') IS NOT NULL 
					DROP TABLE #databaseTableListIdent

				CREATE TABLE #databaseTableListIdent(
														[table_schema]	[sysname],
														[table_name]	[sysname]
													)
				CREATE INDEX IX_databaseTableListIdent_TableName ON #databaseTableListIdent([table_name])


				--get table list that will be analyzed. only tables with identity columns
				SET @queryToRun = N''
				IF @serverVersionNum >= 13
					/* excluding memory optimized tables for SQL 2014 onwardss*/
					SET @queryToRun = @queryToRun + N'	SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.objects obj
												INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
												INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.tables tbl ON tbl.[object_id] = obj.[object_id]
												WHERE obj.[type] IN (''U'')
														AND tbl.[is_memory_optimized] = 0
														AND obj.[object_id] IN (
																			SELECT [object_id]
																			FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.columns
																			WHERE [is_identity] = 1
																			)
														AND obj.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
														AND sch.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + ''''
				ELSE
					SET @queryToRun = @queryToRun + N'	SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.objects obj
												INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
												WHERE obj.[type] IN (''U'')
														AND obj.[object_id] IN (
																			SELECT [object_id]
																			FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')  + '.sys.columns
																			WHERE [is_identity] = 1
																			)
														AND obj.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
														AND sch.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + ''''

				SET @queryToRun = @queryToRun + CASE WHEN @skipObjectsList IS NOT NULL
													 THEN N'	AND obj.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
													 ELSE N'' 
												END
				
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #databaseTableListIdent
				INSERT	INTO #databaseTableListIdent([table_schema], [table_name])
						EXEC sp_executesql @queryToRun

				--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'
								DELETE dtl
								FROM #databaseTableListIdent dtl
								INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																						AND dtl.[table_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
								WHERE	@flgActions & osl.[flg_actions] = osl.[flg_actions]
										AND osl.[active] = 1'
				SET @queryParameters = '@flgActions [int]'
				EXEC sp_executesql @queryToRun, @queryParameters, @flgActions = @flgActions

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
																	FROM #databaseTableListIdent	
																	ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @objectName=[dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC CHECKIDENT(''' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'sql'), 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'sql'), 'quoted') + ''', RESEED) WITH NO_INFOMSGS'
						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
						
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																		@eventName		= 'database consistency check - table identity value',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
																					
						FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList

				IF object_id('tempdb..#databaseTableListIdent') IS NOT NULL 
					DROP TABLE #databaseTableListIdent
			end			
	end

--------------------------------------------------------------------------------------------------
--correct space usage
--------------------------------------------------------------------------------------------------
IF @flgActions & 64 = 64 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Update space usage... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		IF @tableName='%' 
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql')  + ''') WITH NO_INFOMSGS'
				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @executionDBName,
																@objectName		= NULL,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database maintenance - update space usage',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
			end
		ELSE
			begin
				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
																	FROM #databaseTableList	
																	ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @objectName= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'sql'), 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName]([dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'sql'), 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'sql') + ''') WITH NO_INFOMSGS'
						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
						
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																		@eventName		= 'database maintenance - update space usage',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
																		
						FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end
	end			


--------------------------------------------------------------------------------------------------
--		Cleaning wasted space in Database
--		DBCC CLEANTABLE reclaims space after a variable-length column is dropped. 
--		A variable-length column can be one of the following data types:  varchar, nvarchar, varchar(max),
--		nvarchar(max), varbinary, varbinary(max), text, ntext, image, sql_variant, and xml. 
--		The command does not reclaim space after a fixed-length column is dropped.

--		Best Practices
--		DBCC CLEANTABLE should not be executed as a routine maintenance task. 
--		Instead, use DBCC CLEANTABLE after you make significant changes to variable-length columns in 
--		a table or indexed view and you need to immediately reclaim the unused space. 
--		Alternatively, you can rebuild the indexes on the table or view; however, doing so is a more 
--		resource-intensive operation.
--------------------------------------------------------------------------------------------------
IF @flgActions & 128 = 128 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Cleaning wasted space in variable length columns... ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
															FROM #databaseTableList	
															ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CLEANTABLE(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql')  + ''', ''' + [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'quoted') + ''', ' + CAST(@DBCCCheckTableBatchSize AS [nvarchar]) + ') WITH NO_INFOMSGS'
				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database maintenance - clean wasted space - table',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end

RETURN @errorCode
GO
