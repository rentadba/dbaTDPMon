RAISERROR('Create procedure: [dbo].[usp_mpUpdateStatisticsBasedOnStrategy]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpUpdateStatisticsBasedOnStrategy]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpUpdateStatisticsBasedOnStrategy]
GO

CREATE PROCEDURE [dbo].[usp_mpUpdateStatisticsBasedOnStrategy]
		@sqlServerName				[sysname],
		@dbName						[sysname],
		@tableSchema				[sysname]		= 'dbo',
		@tableName					[sysname],
		@columnName					[sysname]		= NULL,
		@indexName					[sysname]		= NULL,
		@columnValue				[nvarchar](max)	= NULL,
		@columnCardinality			[bigint]		= NULL,
		@rowmodctrPercentThreshold	[int]			=    1,
		@rowmodctrConstantThreshold	[bigint]		=  500,
		@densityThreshold			[int]			=   20,
		@densityConstantThreshold	[bigint]		=  500,
		@forceUpdateStatistics		[bit]			=    0,
		@flgOptions					[int]			=  528,
		@executionLevel				[tinyint]		=    0,
		@debugMode					[tinyint]		=    1
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.02.2010 
-- Module			 : Database Maintenance
-- Description		 : update statistics strategy based on current histogram analisys
-- ============================================================================

/*
----------------------------------------------------------------------------------------------------------------------------
Simulate MAXDIFF Histogram implementation for Microsoft SQL Server 2005/2008 
According with Query Planning using a MaxDiff Histogram (US Patent No. 6714938)
----------------------------------------------------------------------------------------------------------------------------
given a table name, a column name, a column value and a cardinality for this value, this code should determine whenever
an update statistics with fullscan operation is required or not for an index of the provided table name
the AutoUpdateStatiscs use the following trigger event:
		current table cardinality growth from 0 to positive
		current table cardinality is less then 500, and 500 changes have been made to the lead column
		current table cardinality is greater than 500, and 500 + 20% or table cardinality changes have been made to the lead column
----------------------------------------------------------------------------------------------------------------------------
our approach:
	if current rowmodctr for an index is greater than a threshold, trigger the update statistics and exit
	if current rowmodctr for an index is lower than provided @columnCardinality, exit (another update statistics or auto-update statistic operation just finished)
	if histogram analysis indicate a deviation between initial density and final density greater than a threshold, trigger the update statistics and exit
		for histogram analysis we use maxdiff histogram type as it's implemented in SQL Server 2005/2008 (http://technet.microsoft.com/en-us/library/cc966419.aspx)
*/
----------------------------------------------------------------------------------------------------------------------------
/*
--sample usage for impact analysis
EXEC [dbo].[usp_mpUpdateStatisticsBasedOnStrategy]	  @tableName					= 'REPORT_PERIOD_FILE'
											, @columnName					= 'RPF_REPORT_DEF_ID'
											, @indexName					= 'RPF_RD_ID_TIMECOLS_SI'
											, @columnValue					= 176446
											, @columnCardinality			= 131072
											, @rowmodctrPercentThreshold	=   1
											, @rowmodctrConstantThreshold	= 500
											, @densityThreshold				=   1
											, @densityConstantThreshold		= 500
											, @forceUpdateStatistics		=   0
											, @flgOptions					= 466	
											, @debugMode					=   1
*/
----------------------------------------------------------------------------------------------------------------------------
-- Input Parameters:
--		@tableSchema				= table schema that current table belongs to
--		@tableName					= table name to be analysed
--		@columnName					= column name that will have @columnValue value for @columnCardinality rows. if null, histogram analysis will be ignored
--		@indexName					= index name to be analysed. default is to analyze all indexes that have as lead column @columnName
--		@columnValue				= value for specified column name to be found in all @columnCardinality rows
--		@columnCardinality			= number fo rows for the specified name that will have the same value 
--		@rowmodctrPercentThreshold	= threshold for percent of changes to current index vs table cardinality (default autoupdate statistics use a value of 20)
--		@rowmodctrConstantThreshold	= number of rows to be added to percent thresold (default autoupdate statistics use a value of 500)
--		@densityThreshold			= difference between value old density and new density, in percent. computed value greater than this will trigger an update statistics for the current index
--		@densityConstantThreshold	= difference between value old density and new density, in rows. computed value greater than this will trigger an update statistics for the current index
--		@forceUpdateStatistics		=	1 - all indexes will be updated (previous thresholds values will be ignored)
--										0 - indexes wil be updated based on this procedure strategy (default)
--		@flgOptions					=	1 - return bucket to be split and splited buckets
--										2 - return status information for current index (default)
--										4 - return computed histogram for current index 
--										8 - return original histogram for current index 
--									   16 - run update statitics statement, if triggered (default)
--									   32 - return only bucket for current value when returning histogram
--									   64 - if update statistics is triggered, in status information return also current density (default)
--									  128 - get initial cardinality = real value cardinality (perform an index seek) (default)
--									  256 - if real cardinality is 0, do update statistics
--											if this option is not present, initial cardinality will be computed using uniform distribution formulas
--									  512 - print update statistics statement (default)
--									 1024 - always have statistics with full scan. if sample mode is used, do an update with fullscan
--		@debugMode					=   2 - print dynamic SQL statements
--										1 - print debug messages
--										0 - no messages will be printed
-----------------------------------------------------------------------------------------
-- Output : 
-----------------------------------------------------------------------------------------
-- Return : 
--		1 : statistics were updated
--		0 : statistics were not updated
--	   -1 : an error occured
-----------------------------------------------------------------------------------------

DECLARE @ReturnValue			[int],
		@crtIndexName			[sysname],
		@queryToRun				[nvarchar](max),
		@serverToRun			[varchar](max),
		@spParameterList		[nvarchar](512),
		@queryUpdateStats		[nvarchar](max),
		@oldValueDensity		[real],
		@newValueDensity		[real],
		@finalDensity			[real],
		@crtNumberOfBuckets		[tinyint],
		@crtIndexROWMODCTR		[bigint],
		@crtIndexStatsDate		[datetime],
		@crtindexStatsRows		[bigint],
		@ctrIndexSamplePercent	[numeric](6,2),
		@aproxTableROWCOUNT		[bigint],
		@flgDoStatsUpdate		[bit],
		@crtBucketRowNo			[tinyint],
		@SnapshotStartTime		[datetime],
		@columnType				[sysname],
		@columnValueNumeric		[numeric](38,10),
		@isColumnTypeNumeric	[bit],
		@run_spServerOption		[bit],
		@realInitCardinality	[bigint],
		@isCurrentSingleBucket	[bit]


DECLARE @crtTableIndexes TABLE	(
								 [table_object_id]	[int],
								 [index_name]		[sysname],
								 [index_id]			[int],
								 [aprox_rowcnt]		[bigint],
								 [rowmodctr]		[bigint]
								)

DECLARE @currentStatsHeader TABLE	(
										[stats_date]			[datetime]		NULL,
										[rows]					[bigint]		NULL,
										[rows_sampled]			[bigint]		NULL,									
										[sample_percent]		[numeric](6,2)	NULL
									)


DECLARE @initialColumnHistogram TABLE	(
										[rowno]					[tinyint]		NULL,
										[RANGE_HI_KEY]			[nvarchar](max)	NULL,
										[RANGE_ROWS]			[real]		NOT NULL DEFAULT (0),
										[EQ_ROWS]				[real]		NOT NULL DEFAULT (0),
										[DISTINCT_RANGE_ROWS]	[real]		NOT NULL DEFAULT (0),
										[AVG_RANGE_ROWS]		[real]		NOT NULL DEFAULT (1)
										)

DECLARE @finalColumnHistogram TABLE		(
										[rowno]					[tinyint]		NULL,
										[RANGE_HI_KEY]			[nvarchar](max)	NULL,
										[RANGE_ROWS]			[real]		NOT NULL DEFAULT (0),
										[EQ_ROWS]				[real]		NOT NULL DEFAULT (0),
										[DISTINCT_RANGE_ROWS]	[real]		NOT NULL DEFAULT (0),
										[AVG_RANGE_ROWS]		[real]		NOT NULL DEFAULT (1)
										)

DECLARE @maxDiffColumnHistogram TABLE	(
										[rowno]					[tinyint]		NULL,
										[RANGE_HI_KEY]			[nvarchar](max)	NULL,
										[RANGE_ROWS]			[real]		NOT NULL DEFAULT (0),
										[EQ_ROWS]				[real]		NOT NULL DEFAULT (0),
										[DISTINCT_RANGE_ROWS]	[real]		NOT NULL DEFAULT (0),
										[AVG_RANGE_ROWS]		[real]		NOT NULL DEFAULT (1),
										[variance_value]		[real]		NOT NULL DEFAULT (3.40E+38),
										[bucket_fill_factor]	[real]		NOT NULL DEFAULT (3.40E+38),
										[merge_density]			[real]		NOT NULL DEFAULT (0)
										)

DECLARE @crtValueCardinality TABLE	(
									 [row_count]		[bigint]
									)
									
DECLARE @columnsTypes	TABLE ([data_type] [sysname])

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
	SET @ReturnValue=0

	SET @SnapshotStartTime = GETUTCDATE()
	SET @serverToRun	  = N'[' + @sqlServerName + '].[' + @dbName + '].dbo.sp_executesql'

	----------------------------------------------------------------------------------------------------------------------------
	--get current table indexes
	----------------------------------------------------------------------------------------------------------------------------
	SET @queryToRun = N''
	SET @queryToRun = @queryToRun + N'
							SELECT    si.[id]							AS [table_object_id]
									, si.[name]							AS [index_name]
									, si.[indid]						AS [index_id]
									, si.[rowcnt]						AS [aprox_rowcnt]
									, si.[rowmodctr]					AS [rowmodctr]
							FROM [' + @dbName + N'].sys.sysindexes si
							INNER JOIN [' + @dbName + N'].sys.objects ob ON ob.[object_id] = si.[id] 
							WHERE	ob.[name]=@tableName
									AND si.[name] NOT LIKE ''_WA_Sys_%''
									AND ob.[schema_id] IN (	SELECT [schema_id] 
															FROM [' + @dbName + N'].sys.schemas 
															WHERE [name]=@tableSchema
														  )' + 
							CASE WHEN @columnName IS NOT NULL 
								THEN N'	AND si.[indid] IN (	SELECT ic.[index_id]
															FROM [' + @dbName + N'].sys.index_columns  ic
															INNER JOIN [' + @dbName + N'].sys.columns	cl ON	ic.[object_id] = cl.[object_id]
																													AND ic.[column_id] = cl.[column_id]
															INNER JOIN [' + @dbName + N'].sys.objects	ob ON	ob.[object_id] = ic.[object_id] 
															WHERE	ob.[name]=@tableName
																	AND ob.[schema_id] IN (SELECT [schema_id] FROM [' + @dbName + N'].sys.schemas WHERE [name]=@tableSchema)
																	AND cl.[name] =@columnName
															)'
								ELSE '' END + 
							CASE WHEN @indexName IS NOT NULL
								THEN N' AND si.[name] =@indexName'
								ELSE '' END
	IF @sqlServerName<>@@SERVERNAME
		begin
			SET @queryToRun=REPLACE(@queryToRun, '@tableName', '''' + @tableName + '''')
			SET @queryToRun=REPLACE(@queryToRun, '@tableSchema', '''' + @tableSchema + '''')
			IF @columnName IS NOT NULL
				SET @queryToRun=REPLACE(@queryToRun, '@columnName', '''' + @columnName + '''')
			IF @indexName IS NOT NULL
				SET @queryToRun=REPLACE(@queryToRun, '@indexName', '''' + @indexName + '''')
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

			IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

			INSERT	INTO @crtTableIndexes([table_object_id], [index_name], [index_id], [aprox_rowcnt], [rowmodctr])
					EXEC (@queryToRun)
		end	
	ELSE
		begin
			IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

			SET @spParameterList='@tableName [sysname], @tableSchema [sysname], @columnName [sysname], @indexName [sysname]'

			INSERT	INTO @crtTableIndexes([table_object_id], [index_name], [index_id], [aprox_rowcnt], [rowmodctr])
					EXEC sp_executesql @queryToRun, @spParameterList, @tableName = @tableName
																	, @tableSchema = @tableSchema 
																	, @columnName = @columnName 
																	, @indexName = @indexName
		end

	SET @columnType=NULL
	IF @columnName IS NOT NULL
		begin
			SET @queryToRun = N''
			SET @queryToRun = @queryToRun + N'
								SELECT [DATA_TYPE]
								FROM [' + @dbName + N'].INFORMATION_SCHEMA.COLUMNS
								WHERE [TABLE_NAME] = ''' + @tableName + N'''
										AND [TABLE_SCHEMA] = ''' + @tableSchema + N'''
										AND [COLUMN_NAME] = ''' + @columnName + ''''
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

			DELETE FROM @columnsTypes
			INSERT	INTO @columnsTypes([data_type])
					EXEC (@queryToRun)


			SELECT @columnType = [data_type] FROM @columnsTypes
		end
		
	SET @isColumnTypeNumeric=0
	IF @columnType IN ('bigint', 'decimal', 'int', 'numeric', 'smallint', 'money', 'tinyint', 'smallmoney', 'bit', 'float', 'real', 'double')
		begin
			SET @isColumnTypeNumeric=1
			SET @columnValueNumeric = CAST(@columnValue as [numeric](38,10))
		end


	SET @realInitCardinality = NULL
	IF @flgOptions & 128 = 128
		begin
			SET @queryToRun = N''
			SET @queryToRun = @queryToRun + N'
									SELECT COUNT(*) [row_count] 
									FROM [' + @dbName + N'].[' + @tableSchema + '].[' + @tableName + ']' + 
									CASE WHEN @columnName IS NOT NULL
										 THEN ' WHERE [' + @columnName + '] = ' + @columnValue
										 ELSE ''
									END
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
			INSERT	INTO @crtValueCardinality([row_count])
					EXEC (@queryToRun)

			SELECT @realInitCardinality = [row_count] FROM @crtValueCardinality
		end

	IF @debugMode & 1 = 1 
		begin
			SET @queryToRun= N'Get curent index information = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		end
	


	----------------------------------------------------------------------------------------------------------------------------
	--for all indexes having as lead column current column name
	----------------------------------------------------------------------------------------------------------------------------
	DECLARE crsTableIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT	  [index_name]
																	, CASE	WHEN @flgOptions & 128 = 128 
																			THEN @realInitCardinality 
																			ELSE [aprox_rowcnt] 
																	  END [aprox_rowcnt]
																	, [rowmodctr]
															FROM @crtTableIndexes
															ORDER BY [index_id]
	OPEN crsTableIndexes
	FETCH NEXT FROM crsTableIndexes INTO @crtIndexName, @aproxTableROWCOUNT, @crtIndexROWMODCTR
	WHILE @@FETCH_STATUS = 0
		begin
			----------------------------------------------------------------------------------------------------------------------------
			--3. update statistics strategy
			----------------------------------------------------------------------------------------------------------------------------
			SET @flgDoStatsUpdate = @forceUpdateStatistics
															
			--1. lowering the threshold
			IF @crtIndexROWMODCTR >= (@rowmodctrConstantThreshold + @aproxTableROWCOUNT * @rowmodctrPercentThreshold / 100.)
				SET @flgDoStatsUpdate = 1

			IF @flgDoStatsUpdate=0
				begin
					----------------------------------------------------------------------------------------------------------------------------
					--get current index statistics header 					
					----------------------------------------------------------------------------------------------------------------------------
					SET @queryToRun = N'SELECT	  [Updated]			AS [stats_date]
												, [Rows]			AS [rows]
												, [Rows Sampled]	AS [rows_sampled]
												, CAST(CASE WHEN [Rows]=0 THEN 0 ELSE ([Rows Sampled] * 100.)/ [Rows] END AS [numeric](6,2)) AS SamplePercent
										FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; USE [' + @dbName + ']; EXEC(''''DBCC SHOW_STATISTICS (''''''''[' + @tableSchema + '].[' + @tableName + ']'''''''', ''''''''' + @crtIndexName + ''''''''') WITH NO_INFOMSGS, STAT_HEADER'''')'')x'
					IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
					
					DELETE FROM @currentStatsHeader

					SET @run_spServerOption=0
					IF (SELECT is_data_access_enabled FROM sys.servers WHERE [name]=@@SERVERNAME)=0 AND @sqlServerName = @@SERVERNAME
						begin
							EXEC sp_serveroption @@SERVERNAME, 'data access', 'true'
							SET @run_spServerOption=1
						end

					INSERT	INTO @currentStatsHeader([stats_date], [rows], [rows_sampled], [sample_percent])
							EXEC (@queryToRun)

					IF @run_spServerOption=1
						EXEC sp_serveroption @@SERVERNAME, 'data access', 'false'


					SELECT	  @crtIndexStatsDate		= [stats_date]
							, @ctrIndexSamplePercent	= [sample_percent]
							, @crtindexStatsRows		= [rows]
					FROM @currentStatsHeader					
					
					IF @flgOptions & 1024 = 1024 AND @ctrIndexSamplePercent < 100
						begin
							SET @queryToRun= 'Current statistics sample percent is: ' + CAST(@ctrIndexSamplePercent AS [nvarchar](max)) + '%. Update to fullscan (100%).'
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

							SET @flgDoStatsUpdate = 1
						end
					
					--1. lowering the threshold (based on statistics header and real cardinality)
					IF @flgOptions & 128 = 128
						IF ABS(@crtindexStatsRows - @realInitCardinality) >= (@rowmodctrConstantThreshold + @realInitCardinality * @rowmodctrPercentThreshold / 100.)
							SET @flgDoStatsUpdate = 1					
				end
			
			IF @debugMode & 1 = 1 
					begin
						SET @queryToRun=N'	statistics analysis for [' + @crtIndexName + '] = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
					end
			
			--if a previous auto-update mechanism had been triggered, we'll do a short-circuit and not run the histogram analysis
			IF NOT (@flgDoStatsUpdate = 1 OR @crtIndexROWMODCTR < @columnCardinality)
				begin
					----------------------------------------------------------------------------------------------------------------------------
					--2. analyzing current index histogram using maxdiff histogram implementation
					----------------------------------------------------------------------------------------------------------------------------				
		
					----------------------------------------------------------------------------------------------------------------------------
					--get current index histogram 
					----------------------------------------------------------------------------------------------------------------------------
					SET @queryToRun = N'SELECT	  ROW_NUMBER() OVER(ORDER BY [RANGE_HI_KEY]) rowno
												, CAST(x.[RANGE_HI_KEY] AS [nvarchar](max))
												, x.[RANGE_ROWS]
												, x.[EQ_ROWS]
												, x.[DISTINCT_RANGE_ROWS]
												, x.[AVG_RANGE_ROWS]
										FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''DBCC SHOW_STATISTICS (''''''''[' + @dbName + '].[' + @tableSchema + '].[' + @tableName + ']'''''''', ''''''''' + @crtIndexName + ''''''''') WITH NO_INFOMSGS, HISTOGRAM'''')'')x'
					IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

					DELETE FROM @maxDiffColumnHistogram

					SET @run_spServerOption=0
					IF (SELECT is_data_access_enabled FROM sys.servers WHERE [name]=@@SERVERNAME)=0 AND @sqlServerName = @@SERVERNAME
						begin
							EXEC sp_serveroption @@SERVERNAME, 'data access', 'true'
							SET @run_spServerOption=1
						end

					INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
								EXEC (@queryToRun)


					IF @run_spServerOption=1
						EXEC sp_serveroption @@SERVERNAME, 'data access', 'false'

					SELECT @crtNumberOfBuckets = COUNT(*) FROM @maxDiffColumnHistogram


					--save original histogram for current index 					
					IF @flgOptions & 8 = 8 
						begin
							DELETE FROM @initialColumnHistogram

							INSERT	INTO @initialColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
									SELECT [rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS]
									FROM @maxDiffColumnHistogram
									ORDER BY [rowno]
						end

					IF @debugMode & 1 = 1 
						begin
							SET @queryToRun= N'Get curent index histogram done at = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						end

					IF @forceUpdateStatistics=0
						begin
							SET @isCurrentSingleBucket=1
							----------------------------------------------------------------------------------------------------------------------------
							--2.a is single value bucket?
							----------------------------------------------------------------------------------------------------------------------------
							SET @oldValueDensity=NULL

							IF @isColumnTypeNumeric = 1
								begin
									SELECT	  @oldValueDensity = [EQ_ROWS]
											, @crtBucketRowNo  = [rowno]
									FROM	@maxDiffColumnHistogram
									WHERE	CAST([RANGE_HI_KEY] AS [numeric](38,10)) = @columnValueNumeric

									UPDATE @maxDiffColumnHistogram
											SET [EQ_ROWS] = CASE WHEN @flgOptions & 128 = 128
																 THEN @realInitCardinality
																 ELSE CASE	WHEN [EQ_ROWS] +  @columnCardinality > 0 
																			THEN [EQ_ROWS] +  @columnCardinality
																			ELSE 0
																	  END
															END
									WHERE	CAST([RANGE_HI_KEY] AS [numeric](38,10)) = @columnValueNumeric
								end
							ELSE
								begin
									SELECT	  @oldValueDensity = [EQ_ROWS]	
											, @crtBucketRowNo  = [rowno]			
									FROM	@maxDiffColumnHistogram
									WHERE	[RANGE_HI_KEY] = @columnValue

									UPDATE @maxDiffColumnHistogram
											SET [EQ_ROWS] = CASE WHEN @flgOptions & 128 = 128
																 THEN @realInitCardinality
																 ELSE CASE	WHEN [EQ_ROWS] +  @columnCardinality > 0 
																			THEN [EQ_ROWS] +  @columnCardinality
																			ELSE 0
																	  END
															END
									WHERE	[RANGE_HI_KEY] = @columnValue
								end

							----------------------------------------------------------------------------------------------------------------------------
							--2.b insert a new bucket / split current bucket
							----------------------------------------------------------------------------------------------------------------------------
							IF @oldValueDensity IS NULL
								begin					
									SET @isCurrentSingleBucket=0
									IF @isColumnTypeNumeric = 1
										--get current position
										SELECT	@oldValueDensity = x.[AVG_RANGE_ROWS], 
												@crtBucketRowNo = x.[rowno]
										FROM (
												SELECT	  bckt2.[AVG_RANGE_ROWS]
														, bckt2.[rowno]
												FROM	@maxDiffColumnHistogram bckt1
												INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
												WHERE	@columnValueNumeric > CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) 
														AND @columnValueNumeric <= CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10))

												UNION ALL

												--column value is lower than first value in histogram
												SELECT	  bckt1.[AVG_RANGE_ROWS]
														, bckt1.[rowno]
												FROM	@maxDiffColumnHistogram bckt1
												WHERE	[rowno]=1
														AND @columnValueNumeric < CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))

												UNION ALL
												--column value is higher than last value in histogram			
												SELECT	  1   AS [AVG_RANGE_ROWS]
														, 254 AS [rowno]
												FROM	@maxDiffColumnHistogram bckt1
												WHERE	[rowno]=@crtNumberOfBuckets
														AND @columnValueNumeric > CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))
											)x
									ELSE
										--get current position
										SELECT	@oldValueDensity = x.[AVG_RANGE_ROWS],
												@crtBucketRowNo = x.[rowno]
										FROM (
												SELECT	  bckt2.[AVG_RANGE_ROWS]
														, bckt2.[rowno]
												FROM	@maxDiffColumnHistogram bckt1
												INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
												WHERE	@columnValue > bckt1.[RANGE_HI_KEY] 
														AND @columnValue <= bckt2.[RANGE_HI_KEY]

												UNION ALL
												--column value is lower than first value in histogram
												SELECT	  bckt1.[AVG_RANGE_ROWS]
														, bckt1.[rowno]
												FROM	@maxDiffColumnHistogram bckt1
												WHERE	[rowno]=1
														AND @columnValue < bckt1.[RANGE_HI_KEY]

												UNION ALL
												--column value is higher than last value in histogram			
												SELECT	  1   AS [AVG_RANGE_ROWS]
														, 254 AS [rowno]
												FROM	@maxDiffColumnHistogram bckt1
												WHERE	[rowno]=@crtNumberOfBuckets
														AND @columnValue > bckt1.[RANGE_HI_KEY]
											)x

									IF @crtBucketRowNo=254
										begin
											--insert a new bucket 
											INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
													SELECT    @crtNumberOfBuckets + 1	AS [rowno]
															, @columnValue				AS [RANGE_HI_KEY]
															, 0							AS [RANGE_ROWS]
															, CASE	WHEN @flgOptions & 128 = 128 
																	THEN @realInitCardinality
																	ELSE CASE	WHEN @columnCardinality > 0 
																				THEN @columnCardinality
																				ELSE 0
																		 END
															  END AS [EQ_ROWS]
															, 0							AS [DISTINCT_RANGE_ROWS]
															, 1							AS [AVG_RANGE_ROWS]

										end
									ELSE
										begin
											IF @isColumnTypeNumeric = 1
												begin
													--split current bucket (left bucket) - uniform distribution
													INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
													SELECT    @crtNumberOfBuckets + 1	AS [rowno]
															, @columnValue				AS [RANGE_HI_KEY]
															, CASE	WHEN (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) > 0
																	THEN 1. * (@columnValueNumeric - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) / (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) * bckt1.[RANGE_ROWS] - 
																			  CASE	WHEN @flgOptions & 128 = 128 
																					THEN @realInitCardinality 
																					ELSE  CASE	WHEN (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) > 0
																								THEN (1. * bckt1.[DISTINCT_RANGE_ROWS] / (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0))) * bckt1.[AVG_RANGE_ROWS]
																								ELSE 0
																						  END 
																			  END
																	ELSE  0
															  END AS [RANGE_ROWS]
															, CASE	WHEN @flgOptions & 128 = 128 
																	THEN @realInitCardinality 
																	ELSE  CASE	WHEN (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) > 0
																				THEN (1. * bckt1.[DISTINCT_RANGE_ROWS] / (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0))) * bckt1.[AVG_RANGE_ROWS]
																				ELSE 0
																		  END 
															  END AS [EQ_ROWS]
															, CASE	WHEN (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) > 0
																	THEN ((@columnValueNumeric - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) * bckt1.[DISTINCT_RANGE_ROWS] * 1.) / (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0))
																	ELSE 0
															  END AS [DISTINCT_RANGE_ROWS]
															, 1 
													FROM @maxDiffColumnHistogram bckt1
													LEFT JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]+1
													WHERE bckt1.[rowno] = @crtBucketRowNo

													--split current bucket (right bucket) - uniform distribution
													INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
													SELECT    @crtNumberOfBuckets + 2	AS [rowno]
															, bckt1.[RANGE_HI_KEY]		AS [RANGE_HI_KEY]
															, CASE	WHEN (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) > 0
																	THEN 1. * (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - @columnValueNumeric) / (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) * bckt1.[RANGE_ROWS] 
																	ELSE 0
															  END AS [RANGE_ROWS]
															, bckt1.[EQ_ROWS]			AS [EQ_ROWS]
															, CASE	WHEN (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) > 0
																	THEN 1. * bckt1.[DISTINCT_RANGE_ROWS] / (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - ISNULL(CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10)), 0)) * (CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) - @columnValueNumeric) 
																	ELSE 0
															  END AS [DISTINCT_RANGE_ROWS]
															, 1 
													FROM @maxDiffColumnHistogram bckt1
													LEFT JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]+1
													WHERE bckt1.[rowno] = @crtBucketRowNo

													UPDATE @maxDiffColumnHistogram
															SET [AVG_RANGE_ROWS] = [RANGE_ROWS] / [DISTINCT_RANGE_ROWS]
													WHERE [rowno] IN (@crtNumberOfBuckets + 1, @crtNumberOfBuckets + 2)
														  AND [DISTINCT_RANGE_ROWS]<>0
												end
											ELSE
												begin
													--split current bucket (left bucket) - uniform distribution
													--we'll split current bucket into 2 equal 
													INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
													SELECT    @crtNumberOfBuckets + 1	AS [rowno]
															, @columnValue				AS [RANGE_HI_KEY]
															, bckt1.[RANGE_ROWS] / 2. - CASE WHEN @flgOptions & 128 = 128 
																							 THEN @realInitCardinality 
																							 ELSE  0
																						END	AS [RANGE_ROWS]
															, CASE	WHEN @flgOptions & 128 = 128 
																	THEN @realInitCardinality 
																	ELSE  0
															  END AS [EQ_ROWS]
															, bckt1.[DISTINCT_RANGE_ROWS] / 2. AS [DISTINCT_RANGE_ROWS]
															, 1 AS [AVG_RANGE_ROWS]
													FROM @maxDiffColumnHistogram bckt1
													LEFT JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]+1
													WHERE bckt1.[rowno] = @crtBucketRowNo

													--split current bucket (right bucket) - uniform distribution
													INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
													SELECT    @crtNumberOfBuckets + 2	AS [rowno]
															, bckt1.[RANGE_HI_KEY]		AS [RANGE_HI_KEY]
															, bckt1.[RANGE_ROWS] / 2.	AS [RANGE_ROWS]
															, bckt1.[EQ_ROWS]			AS [EQ_ROWS]
															, bckt1.[DISTINCT_RANGE_ROWS] / 2. AS [DISTINCT_RANGE_ROWS]
															, 1 AS [AVG_RANGE_ROWS]
													FROM @maxDiffColumnHistogram bckt1
													LEFT JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]+1
													WHERE bckt1.[rowno] = @crtBucketRowNo

													UPDATE @maxDiffColumnHistogram
															SET [AVG_RANGE_ROWS] = [RANGE_ROWS] / [DISTINCT_RANGE_ROWS]
													WHERE [rowno] IN (@crtNumberOfBuckets + 1, @crtNumberOfBuckets + 2)
														  AND [DISTINCT_RANGE_ROWS]<>0
												end

											UPDATE @maxDiffColumnHistogram
													SET   [RANGE_ROWS] = CASE WHEN [RANGE_ROWS]<0 THEN 0 ELSE [RANGE_ROWS] END
														, [DISTINCT_RANGE_ROWS] = CASE WHEN [DISTINCT_RANGE_ROWS]<0 THEN 0 ELSE [DISTINCT_RANGE_ROWS] END
											WHERE [rowno] > @crtNumberOfBuckets

											
											IF (SELECT [RANGE_ROWS] FROM @maxDiffColumnHistogram WHERE [rowno] = @crtNumberOfBuckets + 2)=0
												UPDATE @maxDiffColumnHistogram
														SET   [RANGE_ROWS] = (SELECT [RANGE_ROWS] FROM @maxDiffColumnHistogram WHERE [rowno]=@crtBucketRowNo) - [AVG_RANGE_ROWS]
															, [DISTINCT_RANGE_ROWS] = ((SELECT [RANGE_ROWS] FROM @maxDiffColumnHistogram WHERE [rowno]=@crtBucketRowNo) - [AVG_RANGE_ROWS]) / [AVG_RANGE_ROWS]
												WHERE	[rowno] = @crtNumberOfBuckets + 2
														AND [AVG_RANGE_ROWS]<>0
														AND @isColumnTypeNumeric = 1

											IF (SELECT [RANGE_ROWS] FROM @maxDiffColumnHistogram WHERE [rowno] = @crtNumberOfBuckets + 1)=0
												UPDATE @maxDiffColumnHistogram
														SET   [DISTINCT_RANGE_ROWS] = 0
															, [AVG_RANGE_ROWS]		= 1
												WHERE	[rowno] = @crtNumberOfBuckets + 1

											--return bucket to be split and splited buckets
											IF @flgOptions & 1 = 1
												SELECT    @crtIndexName AS indexName
														, * 
												FROM @maxDiffColumnHistogram 
												WHERE	[rowno]>@crtNumberOfBuckets 
														OR [rowno]=@crtBucketRowNo
												ORDER BY [rowno]
			
											--update Left Bucket(lower values)
											IF @flgOptions & 128 <> 128 
												begin
													UPDATE @maxDiffColumnHistogram
															SET [EQ_ROWS] = CASE WHEN [EQ_ROWS] + @columnCardinality > 0
																				 THEN [EQ_ROWS] + @columnCardinality
																				 ELSE 0
																			END
													WHERE [rowno] = @crtNumberOfBuckets + 1
												end
										end
								end

							----------------------------------------------------------------------------------------------------------------------------
							--finish histogram modifications
							----------------------------------------------------------------------------------------------------------------------------
							IF @isCurrentSingleBucket=0
								IF @flgOptions & 128 <> 128 
									begin
										--delete splited bucket when not using real cardinality
										DELETE FROM @maxDiffColumnHistogram
										WHERE [rowno] = @crtBucketRowNo	
									end

							--if real cardinality is 0 after current operation, delete current bucket or splitted buckets
							IF @flgOptions & 128 = 128 
								begin
									--below code will be executed only in the case of a delete statement
									IF @realInitCardinality=0
										begin
											DELETE FROM @maxDiffColumnHistogram
											WHERE	(	--range bucket (splited buckets)
														[rowno] IN (@crtNumberOfBuckets + 1, @crtNumberOfBuckets + 2)
													 AND @isCurrentSingleBucket=0
													)

											--update range rows and distinct values for the current range bucket
											IF @isColumnTypeNumeric = 1
												UPDATE bckt2
													SET   bckt2.[RANGE_ROWS] =	CASE WHEN bckt2.[RANGE_ROWS] - ABS(@columnCardinality) > 0
												  				 					 THEN bckt2.[RANGE_ROWS] - ABS(@columnCardinality)
																					 ELSE 0
																				 END
														, bckt2.[DISTINCT_RANGE_ROWS] = CASE WHEN bckt2.[DISTINCT_RANGE_ROWS] - 1 > 0
																							 THEN bckt2.[DISTINCT_RANGE_ROWS] - 1
																							 ELSE 0
																						END
												FROM	@maxDiffColumnHistogram bckt1
												INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
												WHERE	@columnValueNumeric > CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) 
														AND @columnValueNumeric <= CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10))
														AND @isCurrentSingleBucket=0
											ELSE
												UPDATE bckt2
													SET   bckt2.[RANGE_ROWS] =	CASE WHEN bckt2.[RANGE_ROWS] - ABS(@columnCardinality) > 0
												  				 					 THEN bckt2.[RANGE_ROWS] - ABS(@columnCardinality)
																					 ELSE 0
																				 END
														, bckt2.[DISTINCT_RANGE_ROWS] = CASE WHEN bckt2.[DISTINCT_RANGE_ROWS] - 1 > 0
																							 THEN bckt2.[DISTINCT_RANGE_ROWS] - 1
																							 ELSE 0
																						END
												FROM	@maxDiffColumnHistogram bckt1
												INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
												WHERE	@columnValue > bckt1.[RANGE_HI_KEY] 
														AND @columnValue <= bckt2.[RANGE_HI_KEY]
														AND @isCurrentSingleBucket=0

											UPDATE @maxDiffColumnHistogram
													SET   [DISTINCT_RANGE_ROWS] = 0
														, [AVG_RANGE_ROWS] =1
											WHERE [RANGE_ROWS]=0

											UPDATE @maxDiffColumnHistogram
													SET   [RANGE_ROWS] = 0
														, [AVG_RANGE_ROWS] =1
											WHERE [DISTINCT_RANGE_ROWS]=0

											UPDATE @maxDiffColumnHistogram
													SET   [AVG_RANGE_ROWS] = [RANGE_ROWS] / [DISTINCT_RANGE_ROWS]
											WHERE	[DISTINCT_RANGE_ROWS]<> 0 
													AND [RANGE_ROWS] <> 0
										
											DELETE FROM @maxDiffColumnHistogram
											WHERE	[AVG_RANGE_ROWS] = 1
													AND [RANGE_ROWS] =0
													AND [DISTINCT_RANGE_ROWS] = 0
													AND [EQ_ROWS] = 0
										end
							end

							--re-sort histogram
							IF @isColumnTypeNumeric = 1
								begin
									UPDATE x
											SET x.[rowno] = y.[rowno]
									FROM @maxDiffColumnHistogram x	
									INNER JOIN (
												SELECT    [RANGE_HI_KEY]
														,  ROW_NUMBER() OVER(ORDER BY CAST([RANGE_HI_KEY] AS [numeric](38,10))) [rowno]
												FROM @maxDiffColumnHistogram 
												) y ON CAST(x.[RANGE_HI_KEY] AS [numeric](38,10)) = CAST(y.[RANGE_HI_KEY] AS [numeric](38,10))
								end
							ELSE
								begin
									UPDATE x
											SET x.[rowno] = y.[rowno]
									FROM @maxDiffColumnHistogram x	
									INNER JOIN (
												SELECT    [RANGE_HI_KEY]
														,  ROW_NUMBER() OVER(ORDER BY [RANGE_HI_KEY]) [rowno]
												FROM @maxDiffColumnHistogram 
												) y ON x.[RANGE_HI_KEY] = y.[RANGE_HI_KEY]
								end

							IF @debugMode & 1 = 1 
								begin
									SET @queryToRun= N'Split/Insert bucket to current histogram done at = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
								end
							

							----------------------------------------------------------------------------------------------------------------------------
							--2c. is the current histogram full (200 + x buckets)?
							----------------------------------------------------------------------------------------------------------------------------
							WHILE ((SELECT COUNT(*) FROM @maxDiffColumnHistogram) > 200)
									AND NOT ((@flgOptions & 128 = 128) AND (@realInitCardinality=0))
								begin
									--compute variance_value and bucket_fill_factor
									UPDATE bckt1
											SET   bckt1.[variance_value] = CASE	WHEN bckt1.[EQ_ROWS] >= x.[variance_value_right_term]
																				THEN bckt1.[EQ_ROWS]
																				ELSE x.[variance_value_right_term]
																		   END - 
																		   CASE	WHEN bckt1.[EQ_ROWS] < x.[variance_value_right_term]
																				THEN bckt1.[EQ_ROWS]
																				ELSE x.[variance_value_right_term]
																		   END
												, bckt1.[bucket_fill_factor] = x.[bucket_fill_factor]
												, [merge_density] = x.[variance_value_right_term]
									FROM @maxDiffColumnHistogram		bckt1
									INNER JOIN 
										(
											--compute right term from max and min formulas for variance_value
											SELECT	bckt1.[rowno]
													, CASE	WHEN bckt1.[RANGE_ROWS] + bckt2.[RANGE_ROWS] = 0
															THEN (bckt1.[EQ_ROWS] + bckt2.[EQ_ROWS]) / 2.
															ELSE ((bckt1.[RANGE_ROWS] + bckt2.[RANGE_ROWS] + bckt1.[EQ_ROWS]) * 1.) / (bckt1.[DISTINCT_RANGE_ROWS] + bckt2.[DISTINCT_RANGE_ROWS] + 1)
													  END AS [variance_value_right_term]
													, bckt1.[RANGE_ROWS] + bckt2.[RANGE_ROWS] + bckt1.[EQ_ROWS] AS [bucket_fill_factor]
											FROM @maxDiffColumnHistogram		bckt1
											INNER JOIN @maxDiffColumnHistogram	bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
										)x ON bckt1.[rowno] = x.[rowno]

									--select bucket to merge and merge buckets
									DECLARE @rowNoToMerge [tinyint]
									
									SELECT TOP 1 @rowNoToMerge = [rowno]
									FROM @maxDiffColumnHistogram
									WHERE [variance_value]<>0
									ORDER BY [variance_value], [bucket_fill_factor]

									--insert merged bucket 
									INSERT	INTO @maxDiffColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
										SELECT  254
												, bckt2.RANGE_HI_KEY
												, bckt1.RANGE_ROWS + bckt2.RANGE_ROWS + bckt1.EQ_ROWS
												, bckt2.EQ_ROWS
												, bckt1.DISTINCT_RANGE_ROWS + bckt2.DISTINCT_RANGE_ROWS + 1
												, (bckt1.RANGE_ROWS + bckt2.RANGE_ROWS + bckt1.EQ_ROWS) / (bckt1.DISTINCT_RANGE_ROWS + bckt2.DISTINCT_RANGE_ROWS + 1)		
										FROM @maxDiffColumnHistogram bckt1
										INNER JOIN @maxDiffColumnHistogram	bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
										WHERE bckt1.[rowno] = @rowNoToMerge

									--delete merged records
									DELETE FROM @maxDiffColumnHistogram
									WHERE [rowno] = @rowNoToMerge OR [rowno] = @rowNoToMerge + 1

									--re-sort histogram
									UPDATE x
											SET x.[rowno] = y.[rowno]
									FROM @maxDiffColumnHistogram x	
									INNER JOIN (
												SELECT    [RANGE_HI_KEY]
														,  ROW_NUMBER() OVER(ORDER BY [RANGE_HI_KEY]) [rowno]
												FROM @maxDiffColumnHistogram 
												) y ON x.[RANGE_HI_KEY] = y.[RANGE_HI_KEY]
								end

							--just compute new density
							SET @newValueDensity = NULL

							IF @isColumnTypeNumeric = 1
								SELECT	@newValueDensity = x.[AVG_RANGE_ROWS]
								FROM (
										SELECT	  CASE	WHEN bckt1.[EQ_ROWS] <> 0
														THEN bckt1.[EQ_ROWS]
														ELSE bckt1.[AVG_RANGE_ROWS]
												  END AS [AVG_RANGE_ROWS]
										FROM	@maxDiffColumnHistogram bckt1
										WHERE	@columnValueNumeric = CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))
									)x
							ELSE
								SELECT	@newValueDensity = x.[AVG_RANGE_ROWS]
								FROM (
										SELECT	  CASE	WHEN bckt1.[EQ_ROWS] <> 0
														THEN bckt1.[EQ_ROWS]
														ELSE bckt1.[AVG_RANGE_ROWS]
												  END AS [AVG_RANGE_ROWS]
										FROM	@maxDiffColumnHistogram bckt1
										WHERE	@columnValue = bckt1.[RANGE_HI_KEY]
									)x

							IF @newValueDensity IS NULL
								IF @isColumnTypeNumeric = 1
									SELECT	@newValueDensity = x.[AVG_RANGE_ROWS]
									FROM (
											SELECT	  bckt2.[AVG_RANGE_ROWS]
											FROM	@maxDiffColumnHistogram bckt1
											INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
											WHERE	@columnValueNumeric > CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) 
													AND @columnValueNumeric <= CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10))

											UNION ALL

											--column value is lower than first value in histogram
											SELECT	  bckt1.[AVG_RANGE_ROWS]
											FROM	@maxDiffColumnHistogram bckt1
											WHERE	[rowno]=1
													AND @columnValueNumeric < CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))

											UNION ALL
											--column value is higher than last value in histogram			
											SELECT	  1   AS [AVG_RANGE_ROWS]
											FROM	@maxDiffColumnHistogram bckt1
											WHERE	[rowno]=@crtNumberOfBuckets
													AND @columnValueNumeric > CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))
										)x
								ELSE
									--get current position
									SELECT	@newValueDensity = x.[AVG_RANGE_ROWS]
									FROM (
											SELECT	  bckt2.[AVG_RANGE_ROWS]
											FROM	@maxDiffColumnHistogram bckt1
											INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
											WHERE	@columnValue > bckt1.[RANGE_HI_KEY] 
													AND @columnValue <= bckt2.[RANGE_HI_KEY]

											UNION ALL
											--column value is lower than first value in histogram
											SELECT	  bckt1.[AVG_RANGE_ROWS]
											FROM	@maxDiffColumnHistogram bckt1
											WHERE	[rowno]=1
													AND @columnValue < bckt1.[RANGE_HI_KEY]

											UNION ALL
											--column value is higher than last value in histogram			
											SELECT	  1   AS [AVG_RANGE_ROWS]
											FROM	@maxDiffColumnHistogram bckt1
											WHERE	[rowno]=@crtNumberOfBuckets
													AND @columnValue > bckt1.[RANGE_HI_KEY]
										)x

							IF @debugMode & 1 = 1 
								begin
									SET @queryToRun= N'Compress current histogram done at = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
								end
						end

					----------------------------------------------------------------------------------------------------------------------------
					--3. update statistics strategy
					----------------------------------------------------------------------------------------------------------------------------
					IF ABS(@newValueDensity - @oldValueDensity) >= (@densityConstantThreshold + @oldValueDensity * @densityThreshold / 100.)
						SET @flgDoStatsUpdate = 1

					IF @flgOptions & 256 = 256 AND @realInitCardinality = 0
						SET @flgDoStatsUpdate = 1
				end


			----------------------------------------------------------------------------------------------------------------------------
			--4. update statistics
			----------------------------------------------------------------------------------------------------------------------------
			SET @queryUpdateStats = N''
			IF @flgDoStatsUpdate = 1
				begin
					SET @serverToRun	  = N'[' + @sqlServerName + '].[' + @dbName + '].dbo.sp_executesql'
					SET @queryUpdateStats = N'UPDATE STATISTICS [' + @tableSchema + '].[' + @tableName + '](' + @crtIndexName + ') WITH FULLSCAN'
				end

			-- return computed histogram for current index 
			IF (@flgOptions & 4 = 4) AND (@forceUpdateStatistics=0)
				IF @flgOptions & 32 <> 32
					SELECT    @crtIndexName AS indexName
							, * 
					FROM @maxDiffColumnHistogram
					ORDER BY [rowno]
				ELSE
					IF @isColumnTypeNumeric = 1
						SELECT    @crtIndexName AS indexName
								, bckt1.* 
						FROM @maxDiffColumnHistogram bckt1
						INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
						WHERE	@columnValueNumeric >= CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))
								AND @columnValueNumeric < CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10))
						ORDER BY bckt1.[rowno]
					ELSE
						SELECT    @crtIndexName AS indexName
								, bckt1.* 
						FROM @maxDiffColumnHistogram bckt1
						INNER JOIN @maxDiffColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
						WHERE	@columnValue >= bckt1.[RANGE_HI_KEY] 
								AND @columnValue < bckt2.[RANGE_HI_KEY]
						ORDER BY bckt1.[rowno]

			IF @flgOptions & 8 = 8 
				IF @flgOptions & 32 <> 32
					SELECT    @crtIndexName AS indexName
							, *
					FROM @initialColumnHistogram
					ORDER BY [rowno]
				ELSE
					begin
						IF @isColumnTypeNumeric = 1
							SELECT    @crtIndexName AS indexName
									, bckt1.* 
							FROM @initialColumnHistogram bckt1
							INNER JOIN @initialColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
							WHERE	@columnValueNumeric >= CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10))
									AND @columnValueNumeric < CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10))
							ORDER BY bckt1.[rowno]
						ELSE
							SELECT    @crtIndexName AS indexName
									, bckt1.* 
							FROM @initialColumnHistogram bckt1
							INNER JOIN @initialColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
							WHERE	@columnValue >= bckt1.[RANGE_HI_KEY] 
									AND @columnValue < bckt2.[RANGE_HI_KEY]
							ORDER BY bckt1.[rowno]
					end
			IF (@flgOptions & 16 = 16) AND (LEN(@queryUpdateStats)>0)
				begin
					IF @debugMode & 2 = 2	OR @flgOptions & 512 = 512
						begin
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryUpdateStats, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						end
					
					EXEC @serverToRun @queryUpdateStats
					SET @ReturnValue = 1

					IF @debugMode & 1 = 1 
						begin
							SET @queryToRun=N'Run update statistics script done at = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						end
					
					IF (@flgOptions & 64 = 64)
						begin
							--get histogram after update statistics
							SET @queryToRun = N'SELECT	  ROW_NUMBER() OVER(ORDER BY [RANGE_HI_KEY]) rowno
														, CAST(x.[RANGE_HI_KEY] AS [nvarchar](max))
														, x.[RANGE_ROWS]
														, x.[EQ_ROWS]
														, x.[DISTINCT_RANGE_ROWS]
														, x.[AVG_RANGE_ROWS]
												FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''DBCC SHOW_STATISTICS (''''''''[' + @dbName + '].[' + @tableSchema + '].[' + @tableName + ']'''''''', ''''''''' + @crtIndexName + ''''''''') WITH NO_INFOMSGS, HISTOGRAM'''')'')x'
							IF @debugMode & 2 = 2	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
																	
							SET @run_spServerOption=0
							IF (SELECT is_data_access_enabled FROM sys.servers WHERE [name]=@@SERVERNAME)=0 AND @sqlServerName = @@SERVERNAME
								begin
									EXEC sp_serveroption @@SERVERNAME, 'data access', 'true'
									SET @run_spServerOption=1
								end

							DELETE FROM @finalColumnHistogram

							INSERT	INTO @finalColumnHistogram([rowno], [RANGE_HI_KEY], [RANGE_ROWS], [EQ_ROWS], [DISTINCT_RANGE_ROWS], [AVG_RANGE_ROWS])
									EXEC (@queryToRun)

							IF @run_spServerOption=1
								EXEC sp_serveroption @@SERVERNAME, 'data access', 'false'

							IF @isColumnTypeNumeric = 1
								begin
									--get final density value
									SELECT	@finalDensity = [EQ_ROWS]
									FROM	@finalColumnHistogram
									WHERE	CAST([RANGE_HI_KEY] AS [numeric](38,10)) = @columnValueNumeric

									IF @finalDensity IS NULL
										begin
											SELECT @finalDensity = bckt2.[AVG_RANGE_ROWS]
											FROM	@finalColumnHistogram	 bckt1
											INNER JOIN @finalColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
											WHERE	@columnValueNumeric > CAST(bckt1.[RANGE_HI_KEY] AS [numeric](38,10)) 
													AND @columnValueNumeric <= CAST(bckt2.[RANGE_HI_KEY] AS [numeric](38,10))
										end
								end
							ELSE
								begin
									--get final density value
									SELECT	@finalDensity = [EQ_ROWS]
									FROM	@finalColumnHistogram
									WHERE	[RANGE_HI_KEY] = @columnValue

									IF @finalDensity IS NULL
										begin
											SELECT @finalDensity = bckt2.[AVG_RANGE_ROWS]
											FROM	@finalColumnHistogram	 bckt1
											INNER JOIN @finalColumnHistogram bckt2 ON bckt1.[rowno] = bckt2.[rowno]-1
											WHERE	@columnValue > bckt1.[RANGE_HI_KEY] 
													AND @columnValue <= bckt2.[RANGE_HI_KEY]
										end
								end

							IF @debugMode & 1 = 1 
								begin
									SET @queryToRun= N'Get final density value done at = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
								end
						end
				end

			-- return status information for current index
			IF (@flgOptions & 2 = 2)
				begin
					SELECT	  @columnValue			AS columnValue
							, @crtIndexName			AS indexName
							, @crtIndexStatsDate	AS crtIndexStatsDate
							, @crtIndexROWMODCTR	AS crtIndexROWMODCTR
							, @aproxTableROWCOUNT	AS aproxTableROWCOUNT
							, CAST((@rowmodctrConstantThreshold + @aproxTableROWCOUNT * @rowmodctrPercentThreshold / 100.) AS [bigint]) rowmodctrThreshold
							, @oldValueDensity		AS initialDensity
							, @realInitCardinality	AS realInitialCardinality
							, @newValueDensity		AS computedDensity
							, @finalDensity			AS finalDensity
							, abs(@newValueDensity - @oldValueDensity)	AS computedDensityDifference
							, @densityConstantThreshold + @densityThreshold * @oldValueDensity / 100. AS densityThreshold
							, @queryUpdateStats		AS queryToRun
				end
			FETCH NEXT FROM crsTableIndexes INTO @crtIndexName, @aproxTableROWCOUNT, @crtIndexROWMODCTR
		end
	CLOSE crsTableIndexes
	DEALLOCATE crsTableIndexes

	IF @debugMode & 1 = 1 
		begin
			SET @queryToRun= N'done in = ' + CAST(DATEDIFF(ms, @SnapshotStartTime, GETUTCDATE()) AS VARCHAR) + ' ms'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		end
END TRY

BEGIN CATCH
	--variable used for raise errors
	SET @ReturnValue=-1

	DECLARE	@ErrorMessage			[nvarchar](4000),
			@ErrorNumber			[int],
			@ErrorSeverity			[int],
			@ErrorState				[int],
			@ErrorLine				[int],
			@ErrorProcedure			[nvarchar](200);

    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
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

