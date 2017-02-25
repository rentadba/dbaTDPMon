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
		@sqlServerName			[sysname]=@@SERVERNAME,
		@dbName					[sysname],
		@tableSchema			[sysname]	=  '%',
		@tableName				[sysname]   =  '%',
		@flgActions				[smallint]	=   12,
		@flgOptions				[int]		=    0,
		@executionLevel			[tinyint]	=    0,
		@debugMode				[bit]		=    0
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

--		@debugMode			- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= 'dbSQLTools',
												@tableSchema			= 'dbo',
												@tableName				= '%',
												@flgActions				= DEFAULT,
												@flgOptions				= DEFAULT,
												@debugMode				= DEFAULT
*/

DECLARE		@queryToRun  					[nvarchar](2048),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@DBCCCheckTableBatchSize 		[int],
			@errorCode						[int],
			@databaseStatus					[int],
			@dbi_dbccFlags					[int]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName			[sysname],
		@agStopLimit	[int],
		@actionType		[sysname]

SET @agStopLimit = 0
SET @actionType = NULL

IF @flgActions &  64 = 64	SET @actionType = 'update space usage'
IF @flgActions & 128 = 128	SET @actionType = 'clean wasted space - table'

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database maintenance',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0
	
---------------------------------------------------------------------------------------------
DECLARE @compatibilityLevel [tinyint]
IF object_id('tempdb..#databaseCompatibility') IS NOT NULL 
	DROP TABLE #databaseCompatibility

CREATE TABLE #databaseCompatibility
	(
		[compatibility_level]		[tinyint]
	)


SET @queryToRun = N''
IF @serverVersionNum >= 9
	SET @queryToRun = @queryToRun + N'SELECT [compatibility_level] FROM sys.databases WHERE [name] = ''' + @dbName + N''''
ELSE
	SET @queryToRun = @queryToRun + N'SELECT [cmptlevel] FROM master.dbo.sysdatabases WHERE [name] = ''' + @dbName + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@compatibilityLevel, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC (@queryToRun)

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility


---------------------------------------------------------------------------------------------
SET @DBCCCheckTableBatchSize = 65536
SET @CurrentTableSchema		 = @tableSchema
SET @tableName				 = REPLACE(@tableName, '''', '''''')
SET @errorCode				 = 0

---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseTableList') IS NOT NULL 
	DROP TABLE #databaseTableList

CREATE TABLE #databaseTableList(
								[table_schema]	[sysname]	NULL,
								[table_name]	[sysname]	NULL
								)
CREATE INDEX IX_databaseTableList_TableName ON #databaseTableList([table_name])



--------------------------------------------------------------------------------------------------
--get database status
-----------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)
			
SET @queryToRun = N'SELECT [status] FROM master.dbo.sysdatabases WHERE [name]=''' + @dbName + N'''' 
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #serverPropertyConfig
INSERT	INTO #serverPropertyConfig([value])
		EXEC (@queryToRun)

SELECT @databaseStatus = [value]
FROM #serverPropertyConfig

---------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2 OR @flgActions & 16 = 16 OR @flgActions & 64 = 64 OR @flgActions & 128 = 128
	begin
		--get table list that will be analyzed including materialized views; will pick only tables with reserved pages
		SET @queryToRun = N''
		IF @serverVersionNum >= 9
			SET @queryToRun = @queryToRun + N'SELECT DISTINCT ob.[table_schema], ob.[table_name]
FROM (
		SELECT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N'].sys.objects obj WITH (READPAST)
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''
				AND obj.[is_ms_shipped] = 0' +

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL

		SELECT DISTINCT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N'].sys.indexes idx WITH (READPAST)
		INNER JOIN [' + @dbName + N'].sys.objects obj WITH (READPAST) ON obj.[object_id] = idx.[object_id]
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''
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
				FROM [' + @dbName + N'].sys.dm_db_partition_stats ps WITH (READPAST)
				GROUP BY ps.[object_id]
			) AS ps
		INNER JOIN [' + @dbName + N'].sys.objects so  WITH (READPAST) ON so.[object_id] = ps.[object_id] 
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id]
		WHERE	so.[type] in (''S'', ''U'', ''V'')
			AND ps.[reserved_page_count] > 0
	)ps ON ob.[object_id] = ps.[object_id]'
		ELSE
			SET @queryToRun = @queryToRun + N'SELECT ob.[table_schema], ob.[table_name]
FROM (
		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N']..sysobjects obj
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = obj.[uid]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''' + 

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL			

		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N']..sysindexes idx
		INNER JOIN [' + @dbName + N']..sysobjects obj ON obj.[id] = idx.[id]
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = obj.[uid]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N''''
		END + N'
	)ob
INNER JOIN
	(
		SELECT si.[id] AS [object_id], sch.[name] AS [table_schema], so.[name] AS [table_name]
		FROM [' + @dbName + N']..sysobjects so
		INNER JOIN [' + @dbName + N']..sysindexes si on so.[id] = si.[id]
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = so.[uid]
		WHERE si.[reserved]<>0
	)ps ON ob.[object_id] = ps.[object_id]'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #databaseTableList
		INSERT	INTO #databaseTableList([table_schema], [table_name])
				EXEC (@queryToRun)
	end

--------------------------------------------------------------------------------------------------
--when running DBCC CHECKDB, check if DATA_PURITY option should be used or not (run only when dbi_dbccFlags=0)
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1 AND @serverVersionNum >= 9 AND @flgOptions & 1 = 0
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
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @dbName + N']) WITH TABLERESULTS'''')'')x
										WHERE [Field]=''dbi_dbccFlags'''
				ELSE
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @dbName + N']) WITH TABLERESULTS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
										WHERE [Field]=''dbi_dbccFlags'''
			end
		ELSE
			begin							
				INSERT	INTO #dbccDBINFO
						EXEC ('DBCC DBINFO (''' + @dbName + N''') WITH TABLERESULTS')

				SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccFlags'''											
			end

		IF @debugMode = 1 PRINT @queryToRun
				
		TRUNCATE TABLE #dbi_dbccFlags
		INSERT	INTO #dbi_dbccFlags([Value])
				EXEC (@queryToRun)

		SELECT @dbi_dbccFlags = ISNULL([Value], 0)
		FROM #dbi_dbccFlags
		
		SET @dbi_dbccFlags = ISNULL(@dbi_dbccFlags, 0)
	end


--------------------------------------------------------------------------------------------------
--database consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Database consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + '...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKDB(''' + @dbName + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS' + CASE WHEN @flgOptions & 1 = 1 THEN ', PHYSICAL_ONLY' ELSE '' END

		IF @serverVersionNum >= 9 AND @flgOptions & 1 = 0 AND @dbi_dbccFlags <> 2
			SET @queryToRun = @queryToRun + ', DATA_PURITY'

		IF @compatibilityLevel >= 100 AND @flgOptions & 1 = 0
			SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
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
IF @flgActions & 2 = 2
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Tables/views consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + CASE WHEN @flgOptions & 2 = 2 THEN '(NOINDEX)' ELSE '' END + '...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
															FROM #databaseTableList	
															ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKTABLE(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']''' + CASE WHEN @flgOptions & 2 = 2 THEN ', NOINDEX' ELSE '' END + ') WITH ALL_ERRORMSGS, NO_INFOMSGS'
				
				IF @serverVersionNum >= 9 AND @dbi_dbccFlags <> 2
					SET @queryToRun = @queryToRun + ', DATA_PURITY'
				
				IF @compatibilityLevel >= 10 AND @flgOptions & 2 = 0
					SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				
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
IF @flgActions & 4 = 4
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Allocation structures consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKALLOC(''' + @dbName + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
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
IF @flgActions & 8 = 8
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Catalogs consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKCATALOG(''' + @dbName + ''') WITH NO_INFOMSGS'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
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
IF @flgActions & 16 = 16
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Table constraints consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
															FROM #databaseTableList	
															ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKCONSTRAINTS(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'') WITH ALL_ERRORMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

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
IF @flgActions & 32 = 32
	begin
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
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			end
		ELSE
			begin
				IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun=N'Table identity value consistency check ...' + ' [' + @dbName + ']'
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
				IF @serverVersionNum >= 9
					SET @queryToRun = @queryToRun + N'	SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM [' + @dbName + '].sys.objects obj
												INNER JOIN [' + @dbName + '].sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
												WHERE obj.[type] IN (''U'')
														AND obj.[object_id] IN (
																			SELECT [object_id]
																			FROM [' + @dbName + '].sys.columns
																			WHERE [is_identity] = 1
																			)
														AND obj.[name] LIKE ''' + @tableName + '''
														AND sch.[name] LIKE ''' + @tableSchema + ''''
				ELSE
					SET @queryToRun = @queryToRun + N'SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM  [' + @dbName + ']..sysobjects obj
												INNER JOIN  [' + @dbName + ']..sysusers sch ON sch.[uid] = obj.[uid]
												WHERE obj.[type] IN (''U'')
														AND obj.[id] IN (
																		SELECT [id]
																		FROM  [' + @dbName + ']..syscolumns
																		WHERE [autoval] is not null
																		)
														AND obj.[name] LIKE ''' + @tableName + '''
														AND sch.[name] LIKE ''' + @tableSchema + ''''			
				
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #databaseTableListIdent
				INSERT	INTO #databaseTableListIdent([table_schema], [table_name])
						EXEC (@queryToRun)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
																	FROM #databaseTableListIdent	
																	ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0
					begin
						SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
						SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC CHECKIDENT(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'', RESEED)'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

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
IF @flgActions & 64 = 64
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Update space usage...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		IF @tableName='%' 
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + @dbName + ''') WITH NO_INFOMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
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
				WHILE @@FETCH_STATUS = 0
					begin
						SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
						SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + @dbName + ''', ''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'')'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

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
IF @flgActions & 128 = 128
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Cleaning wasted space in variable length columns...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [table_schema], [table_name] 
															FROM #databaseTableList	
															ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CLEANTABLE(''' + @dbName + ''', ''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'', ' + CAST(@DBCCCheckTableBatchSize AS [nvarchar]) + ')'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

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
