USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.6 to 2017.7 (2017.07.06)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.07.06' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																								   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMON', 10, 1) WITH NOWAIT

RAISERROR('Create function: [dbo].[ufn_reportHTMLFormatTimeValue]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ufn_reportHTMLFormatTimeValue]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_reportHTMLFormatTimeValue]
GO

CREATE FUNCTION [dbo].[ufn_reportHTMLFormatTimeValue]
(		
	@valueInMS	[bigint]
)
RETURNS [nvarchar](64)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.10.2010
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-- { sql_statement | statement_block }
begin
	DECLARE @timeValue	[varchar](32),
			@crtValue	[varchar](3)

	SELECT    @timeValue = ''
			, @valueInMS = ISNULL(@valueInMS, 0)
	
	SELECT    @crtValue = CAST(@valueInMS / (1000 * 60 * 60 * 24) AS [varchar])
			, @valueInMS = @valueInMS % (1000 * 60 * 60 * 24)

	SET @timeValue = @timeValue + CASE WHEN @crtValue>0 THEN @crtValue  + 'd ' ELSE '' END

	SELECT    @crtValue = CAST(@valueInMS / (1000 * 60 * 60) AS [varchar])
			, @valueInMS = @valueInMS % (1000 * 60 * 60)

	SET @timeValue = @timeValue + REPLICATE('0', 2-CASE WHEN LEN(@crtValue) < 2 THEN LEN(@crtValue) ELSE 2 END) + @crtValue + ':'

	SELECT    @crtValue = CAST(@valueInMS / (1000 * 60) AS [varchar])
			, @valueInMS = @valueInMS % (1000 * 60)

	SET @timeValue = @timeValue + REPLICATE('0', 2-CASE WHEN LEN(@crtValue) < 2 THEN LEN(@crtValue) ELSE 2 END) + @crtValue + ':'

	SELECT    @crtValue = CAST(@valueInMS / (1000) AS [varchar])
			, @valueInMS = @valueInMS % (1000)

	SET @timeValue = @timeValue + REPLICATE('0', 2-CASE WHEN LEN(@crtValue) < 2 THEN LEN(@crtValue) ELSE 2 END) + @crtValue + '.'

	SELECT    @crtValue = CAST(@valueInMS AS [varchar])

	SET @timeValue = @timeValue + REPLICATE('0', 3-CASE WHEN LEN(@crtValue) < 3 THEN LEN(@crtValue) ELSE 3 END) + @crtValue

	RETURN @timeValue
end

GO


RAISERROR('Create function: [dbo].[ufn_formatSQLQueryForLinkedServer]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sysobjects WHERE [id] = OBJECT_ID(N'[dbo].[ufn_formatSQLQueryForLinkedServer]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[ufn_formatSQLQueryForLinkedServer]
GO

CREATE FUNCTION [dbo].[ufn_formatSQLQueryForLinkedServer]
(		
	@sqlServerName		[sysname],
	@sqlText			[nvarchar] (4000)
)
RETURNS [nvarchar](4000)
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.01.2010
-- Module			 : Database Analysis & Performance Monitoring
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance
--		@sqlText		- initial SQL statement to be executed.
--						  this string is formated as to be executed on local server
-----------------------------------------------------------------------------------------
-- Return : 
--		SQL statement formated to be executed over linked server using OPENQUERY or locally
-----------------------------------------------------------------------------------------
-- { sql_statement | statement_block }

begin
	DECLARE @SQLStatement [nvarchar] (4000)

	SET @SQLStatement = N''

	IF @sqlServerName=@@SERVERNAME
		SET @SQLStatement = @sqlText
	ELSE
		begin
			SET @SQLStatement = @SQLStatement + 
								N'SELECT x.* FROM OPENQUERY([' + @sqlServerName + '], ''' + 
								REPLACE(@sqlText, '''', '''''') + 
								''')x'
		end
	RETURN @SQLStatement
end

GO


if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[ufn_checkIP4Address]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[ufn_checkIP4Address]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

CREATE FUNCTION dbo.ufn_checkIP4Address
(
	@ipAddress [varchar](15)
)
RETURNS bit
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006 
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

begin 
	DECLARE @tmpStr		[varchar](15),
			@tmpIdx		[int],
			@tmpIdxOld	[int]
	
	SET @tmpIdxOld=0
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	IF @tmpIdx=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, @tmpIdx-@tmpIdxOld-1)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	IF @tmpIdx=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, @tmpIdx-@tmpIdxOld-1)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	IF @tmpIdx=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, @tmpIdx-@tmpIdxOld-1)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=CHARINDEX('.', @ipAddress, @tmpIdxOld+1)
	---------------------------------------------------------------------------------------------------------
	SET @tmpStr=null
	SET @tmpStr=SUBSTRING(@ipAddress, @tmpIdxOld+1, 255)
	SET @tmpIdxOld=@tmpIdx
	IF LEN(ISNULL(@tmpStr, ''))=0
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	SET @tmpIdx=null
	SET @tmpIdx=CAST(@tmpStr AS integer)
	IF (@@Error<>0) OR (@tmpIdx IS NULL)
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	---------------------------------------------------------------------------------------------------------
	IF @tmpIdx>255
		begin
			--The specified IP address is not valid'
			RETURN 1
		end
	RETURN 0

end



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO




/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: MAINTENANCE-PLAN', 10, 1) WITH NOWAIT

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
		@flgActions				[smallint] = 1,
		@flgOptions				[int] = 10328,
		@allowDataLoss			[bit]=0,
		@executionLevel			[tinyint] = 0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 2004-2006, last updated 04.02.2015
-- Module     : Database Maintenance Scripts
-- ============================================================================
---------------------------------------------------------------------------------------------
--		@flgActions		 1  - Copy records from Sources to Destination (default)
--						 2  - perform truncate on Destination before copy
---------------------------------------------------------------------------------------------
--		@flgOptions		 8  - Disable non-clustered index (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding indexes (default)
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  8192  - disable/enable table triggers (default)
---------------------------------------------------------------------------------------------

DECLARE	@tmpServerDestination		[varchar](1024),
		@queryToRun					[nvarchar](4000),
		@queryToRun1				[nvarchar](4000),
		@columnName					[sysname],
		@columnType					[sysname],
		@tmpCount					[int],
		@tmpCount1					[int],
		@tmpCount2					[int],
		@tableHasBlobs				[bit],
		@flgSkipSynchronizeInsert	[bit],
		@ReturnValue				[int],
		@nestExecutionLevel			[tinyint],
		@flgOptionsNested			[int],
		@affectedDependentObjects	[nvarchar](max)


DECLARE @schemaNameSource		[sysname],
		@schemaNameDestination	[sysname],
		@tableNameSource		[sysname],
		@tableNameDestination	[sysname],
		@columnSource			[sysname],
		@columnDestination		[sysname]


IF object_id('#tmpDBSource') IS NOT NULL DROP TABLE #tmpDBSource
CREATE TABLE #tmpDBSource 
		(
			[table_schema]	[sysname],
			[table_name]	[sysname]
		)

IF object_id('#tmpDBDestination') IS NOT NULL DROP TABLE #tmpDBDestination
CREATE TABLE #tmpDBDestination 
		(
			[table_schema]	[sysname],
			[table_name]	[sysname]
		)

IF object_id('#tmpDBMixed') IS NOT NULL DROP TABLE #tmpDBMixed
CREATE TABLE #tmpDBMixed 
		(
			[source_table_schema]		[sysname],
			[source_table_name]			[sysname],
			[destination_table_schema]	[sysname],
			[destination_table_name]	[sysname]
		)

IF object_id('#tmpTableColumnsBlobs') IS NOT NULL DROP TABLE #tmpTableColumnsBlobs
CREATE TABLE #tmpTableColumnsBlobs 
		(
			ColumnName varchar(255), 
			ColumnType varchar(255)
		)

IF object_id('#tmpTableColumnsSource') IS NOT NULL DROP TABLE #tmpTableColumnsSource
CREATE TABLE #tmpTableColumnsSource 
		(
			ColumnName varchar(255)
		)

IF object_id('#tmpTableColumnsDestination') IS NOT NULL DROP TABLE #tmpTableColumnsDestination
CREATE TABLE #tmpTableColumnsDestination 
		(
			ColumnName varchar(255)
		)

IF object_id('#tmpTableColumnsMixed') IS NOT NULL DROP TABLE #tmpTableColumnsMixed
CREATE TABLE #tmpTableColumnsMixed 
		(
			ColumnSource varchar(255), 
			ColumnDestination varchar(255)
		)

IF object_id('#tmpCount') IS NOT NULL DROP TABLE #tmpCount
CREATE TABLE #tmpCount 
		(
			[result] int
		)



-----------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON
-----------------------------------------------------------------------------------------------------------------------------------------
-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 0
		SET @tmpServerDestination	='[' + @destinationServerName + '].[' + @destinationDB + '].[dbo].sp_executesql'

		-----------------------------------------------------------------------------------------------------------------------------------------
		--get source database table information
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT TABLE_SCHEMA, TABLE_NAME 
										FROM [' + @sourceDB + '].INFORMATION_SCHEMA.TABLES 
										WHERE TABLE_TYPE = ''BASE TABLE'' 
												AND TABLE_SCHEMA LIKE ''' + @sourceTableSchema + '''
												AND TABLE_NAME LIKE ''' + @sourceTableName + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpDBSource ([table_schema], [table_name])
				EXEC (@queryToRun)
		SELECT @tmpCount1=count(*) from #tmpDBSource

		-----------------------------------------------------------------------------------------------------------------------------------------
		--get destination database table information
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT TABLE_SCHEMA, TABLE_NAME 
										FROM [' + @destinationDB + '].INFORMATION_SCHEMA.TABLES 
										WHERE TABLE_TYPE = ''BASE TABLE'' 
												AND TABLE_SCHEMA LIKE ''' + @destinationTableSchema + '''
												AND TABLE_NAME LIKE ''' + @destinationTableName + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpDBDestination ([table_schema], [table_name])
				EXEC (@queryToRun)
		SELECT @tmpCount2=count(*) from #tmpDBDestination


		-----------------------------------------------------------------------------------------------------------------------------------------
		--operatiunile de import date bulk se vor face numai pe tabelele comune celor 2 baze, sursa si destinatie
		IF @sourceTableSchema<>'%' AND @sourceTableName<>'%' AND @destinationTableSchema<>'%' AND @destinationTableName<>'%'
			SET @queryToRun=   'SELECT ''' + @sourceTableSchema + ''' AS [source_table_schema], ''' + @sourceTableName + ''' AS [source_table_name], ''' + @destinationTableSchema + ''' AS [destination_table_schema], ''' + @destinationTableName + ''' AS [destination_table_name]'
		ELSE
			SET @queryToRun=N'SELECT   S.[table_schema]		AS [source_table_schema]
									 , S.[table_name]		AS [source_table_name]
									 , D.[table_schema]		AS [destination_table_schema]
									 , D.[table_name]		AS [destination_table_name] 
							FROM #tmpDBSource AS S 
							INNER JOIN #tmpDBDestination AS D ON S.[table_schema]=D.[table_schema] AND S.[table_name]=D.[table_name]'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		INSERT	INTO #tmpDBMixed ([source_table_schema], [source_table_name], [destination_table_schema], [destination_table_name])
				EXEC (@queryToRun)

		-----------------------------------------------------------------------------------------------------------------------------------------
		--analizez tabelele comune din cele 2 baze de date dupa campurile comune
		-----------------------------------------------------------------------------------------------------------------------------------------
		IF @tmpCount1<>0 AND @tmpCount2<>0
			begin
				-----------------------------------------------------------------------------------------------------------------------------------------
				DECLARE crsDBMixed CURSOR LOCAL FAST_FORWARD FOR	SELECT [source_table_schema], [source_table_name], [destination_table_schema], [destination_table_name]
																	FROM #tmpDBMixed 
																	WHERE 	([destination_table_name] NOT LIKE '%dtproperties%') 
																			AND  ([destination_table_name] NOT LIKE '%sys%') 
																	ORDER BY [source_table_schema], [source_table_name], [destination_table_schema], [destination_table_name]
				OPEN crsDBMixed
				FETCH NEXT FROM crsDBMixed INTO @schemaNameSource, @tableNameSource, @schemaNameDestination, @tableNameDestination
				WHILE @@FETCH_STATUS=0
					begin
						------------------------------------------------------------------------------------------------------------------------
						SET @tableHasBlobs=0
						SET @flgSkipSynchronizeInsert=0
		
						------------------------------------------------------------------------------------------------------------------------
						--pentru fiecare tabela comuna, se vor cauta campurile comune
						SET @queryToRun='Analyze Source: [' + @sourceServerName + '].[' + @sourceDB + '].[' + @schemaNameSource + '].[' + @tableNameSource + '] vs Destination: [' + @destinationServerName + '].[' + @destinationDB + '].[' + @schemaNameDestination + '].' + @tableNameDestination + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
						------------------------------------------------------------------------------------------------------------------------
						SET @queryToRun='SELECT COLUMN_NAME, DATA_TYPE 
										 FROM [' + @sourceDB + '].INFORMATION_SCHEMA.COLUMNS 
										 WHERE TABLE_NAME=''' + @tableNameSource + ''' 
												AND TABLE_SCHEMA=''' + @schemaNameSource + '''
												AND DATA_TYPE IN (''text'', ''ntext'', ''image'')'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						TRUNCATE TABLE #tmpTableColumnsBlobs
						INSERT INTO #tmpTableColumnsBlobs ([ColumnName], [ColumnType])
								EXEC (@queryToRun)

						DECLARE crsTableFieldsBlobs CURSOR FOR	SELECT DISTINCT ColumnName, ColumnType 
																FROM #tmpTableColumnsBlobs
						OPEN crsTableFieldsBlobs
						FETCH NEXT FROM crsTableFieldsBlobs INTO @columnName, @columnType
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun='SELECT MAX(DATALENGTH(' + @columnName + ')) FROM [' + @sourceDB + '].[' + @schemaNameSource + '].[' + @tableNameSource + ']'
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
								TRUNCATE TABLE #tmpCount
								INSERT	INTO #tmpCount([Result])
										EXEC (@queryToRun)

								IF (SELECT Result FROM #tmpCount)>=8000 
									begin
										DELETE FROM #tmpTableColumnsBlobs WHERE ColumnName=@columnName
										SET @tableHasBlobs=1
									end					
								FETCH NEXT FROM crsTableFieldsBlobs INTO @columnName, @columnType
							end
						CLOSE crsTableFieldsBlobs
						DEALLOCATE crsTableFieldsBlobs
		
						SET @queryToRun='SELECT inf.COLUMN_NAME 
										FROM [' + @sourceDB + '].INFORMATION_SCHEMA.COLUMNS inf
										INNER JOIN (
													SELECT [name] FROM [' + @sourceDB + '].dbo.syscolumns 
													WHERE	[id]=OBJECT_ID(''[' + @sourceDB + '].[' + @schemaNameSource + '].[' + @tableNameSource + ']'') 
															AND [iscomputed]=0
													) cl ON inf.[COLUMN_NAME]=cl.[name]
										WHERE inf.TABLE_NAME=''' + @tableNameSource + ''' 
												AND inf.TABLE_SCHEMA=''' + @schemaNameSource + '''
												AND	inf.DATA_TYPE NOT IN (''text'', ''ntext'', ''image'', ''timestamp'')'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sourceServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						TRUNCATE TABLE #tmpTableColumnsSource
						INSERT	INTO #tmpTableColumnsSource ([ColumnName])
								EXEC (@queryToRun)

						INSERT INTO #tmpTableColumnsSource SELECT DISTINCT ColumnName FROM #tmpTableColumnsBlobs
				
						IF @tableHasBlobs=1 AND @allowDataLoss=0
							begin
								EXEC [dbo].[usp_logPrintMessage] @customMessage = 'WARNING: Source table contains lob columns that cannot be copied. Skipping...', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								SET @flgSkipSynchronizeInsert=1
							end
		
						IF @flgSkipSynchronizeInsert=0
							begin
								IF @flgActions & 1 = 1
									begin
										------------------------------------------------------------------------------------------------------------------------
										SET @queryToRun='SELECT COLUMN_NAME, DATA_TYPE 
														 FROM [' + @destinationDB + '].INFORMATION_SCHEMA.COLUMNS 
														 WHERE TABLE_NAME=''' + @tableNameDestination + ''' 
																AND TABLE_SCHEMA=''' + @tableNameDestination + '''
																AND DATA_TYPE IN (''text'', ''ntext'', ''image'')'
										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@destinationServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

										TRUNCATE TABLE #tmpTableColumnsBlobs
										INSERT INTO #tmpTableColumnsBlobs ([ColumnName], [ColumnType])
												EXEC (@queryToRun)
						
										DECLARE crsTableFieldsBlobs CURSOR FOR	SELECT DISTINCT ColumnName, ColumnType 
																				FROM #tmpTableColumnsBlobs
										OPEN crsTableFieldsBlobs
										FETCH NEXT FROM crsTableFieldsBlobs INTO @columnName, @columnType
										WHILE @@FETCH_STATUS=0
											begin
												SET @queryToRun='SELECT MAX(DATALENGTH(' + @columnName + ')) FROM [' + @destinationDB + '].[' + @tableNameDestination + '].[' + @tableNameDestination + ']'
												SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@destinationServerName, @queryToRun)
												IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
												TRUNCATE TABLE #tmpCount
												INSERT	INTO #tmpCount([Result])
														EXEC (@queryToRun)

												IF (SELECT Result FROM #tmpCount)>=8000 
													begin
														DELETE FROM #tmpTableColumnsBlobs WHERE ColumnName=@columnName
													end					
												FETCH NEXT FROM crsTableFieldsBlobs INTO @columnName, @columnType
											end
										CLOSE crsTableFieldsBlobs
										DEALLOCATE crsTableFieldsBlobs
		
										SET @queryToRun='SELECT inf.COLUMN_NAME 
														FROM [' + @destinationDB + '].INFORMATION_SCHEMA.COLUMNS inf
														INNER JOIN (
																	SELECT [name] FROM [' + @destinationDB + '].dbo.syscolumns 
																	WHERE	[id]=OBJECT_ID(''[' + @destinationDB + '].[' + @schemaNameDestination + '].[' + @tableNameDestination + ']'') 
																			AND [iscomputed]=0
																	) cl ON inf.[COLUMN_NAME]=cl.[name]
														WHERE inf.TABLE_NAME=''' + @tableNameDestination + ''' 
																AND inf.TABLE_SCHEMA=''' + @schemaNameDestination + '''
																AND	inf.DATA_TYPE NOT IN (''text'', ''ntext'', ''image'', ''timestamp'')'
										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@destinationServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

										TRUNCATE TABLE #tmpTableColumnsDestination
										INSERT INTO #tmpTableColumnsDestination([ColumnName])
												EXEC (@queryToRun)

										INSERT INTO #tmpTableColumnsDestination SELECT DISTINCT ColumnName FROM #tmpTableColumnsBlobs
		
										------------------------------------------------------------------------------------------------------------------------
										SET @queryToRun='SELECT S.ColumnName AS ColumnSource, D.ColumnName AS ColumnDestination FROM #tmpTableColumnsSource AS S INNER JOIN #tmpTableColumnsDestination AS D ON S.ColumnName=D.ColumnName'
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

										TRUNCATE TABLE #tmpTableColumnsMixed
										INSERT	INTO #tmpTableColumnsMixed ([ColumnSource], [ColumnDestination])
												EXEC (@queryToRun)
									end

								------------------------------------------------------------------------------------------------------------------------
								--disable table non-clustered indexes
								------------------------------------------------------------------------------------------------------------------------
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
										--rebuild PK, we might need it
										EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @destinationServerName,
																				@dbName						= @destinationDB,
																				@tableSchema				= @schemaNameDestination,
																				@tableName					= @tableNameDestination,
																				@indexName					= NULL,
																				@indexID					= 1,
																				@partitionNumber			= 1,
																				@flgAction					= 1,
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
								IF @flgActions & 2 = 2
									begin
										EXEC [dbo].[usp_logPrintMessage] @customMessage = 'Delete Data from Destination - Start', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
	
										--------------------------------------------------------------------------------------------------------
										SET @nestExecutionLevel=@executionLevel+1
										EXEC @tmpCount = [dbo].[usp_tableGetRowCount]	@sqlServerName			= @destinationServerName,
																						@databaseName			= @destinationDB,
																						@schemaName				= @schemaNameDestination,
																						@tableName				= @tableNameDestination,
																						@executionLevel			= @nestExecutionLevel,
																						@debugMode				= @debugMode

										SET @queryToRun1= 'Total Rows In Destination Table Before Delete: ' + CAST(@tmpCount AS varchar(20))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
								
										--------------------------------------------------------------------------------------------------------
										SET @queryToRun1= 'Deleteting records... '
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										SET @queryToRun='DELETE FROM [' + @destinationDB + '].[' + @schemaNameDestination + '].[' + @tableNameDestination + ']'
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

										EXEC @tmpServerDestination @queryToRun
										SET @ReturnValue=@@ERROR

										SET @queryToRun='Error Returned: ' + CAST(@ReturnValue AS varchar)
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
										
										--------------------------------------------------------------------------------------------------------
										--update usage
										SET @nestExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@SQLServerName			= @destinationServerName,
																					@DBName					= @destinationDB,
																					@TableSchema			= @schemaNameDestination,
																					@TableName				= @tableNameDestination,
																					@flgActions				= 64,
																					@flgOptions				= DEFAULT,
																					@executionLevel			= @nestExecutionLevel,
																					@debugMode				= @debugMode

										--------------------------------------------------------------------------------------------------------
										SET @nestExecutionLevel=@executionLevel+1
										EXEC @tmpCount = [dbo].[usp_tableGetRowCount]	@sqlServerName			= @destinationServerName,
																						@databaseName			= @destinationDB,
																						@schemaName				= @schemaNameDestination,
																						@tableName				= @tableNameDestination,
																						@executionLevel			= @nestExecutionLevel,
																						@debugMode				= @debugMode

										SET @queryToRun1= 'Total Rows In Destination Table After Delete: ' + CAST(@tmpCount AS varchar(20))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										EXEC [dbo].[usp_logPrintMessage] @customMessage = 'Delete Data from Destination - Stop', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
									end
								
								---------------------------------------------------------------------------------------------------------
								IF @flgActions & 1 = 1
									begin
										EXEC [dbo].[usp_logPrintMessage] @customMessage = 'Copy Data from Source to Destination - Start', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										--construiect scriptul de import date			
										SET @queryToRun='INSERT INTO [' + @destinationDB + '].[' + @schemaNameDestination + '].[' + @tableNameDestination + '] ('
										DECLARE crsColumnsMixed CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnSource], [ColumnDestination] 
																								FROM #tmpTableColumnsMixed
										OPEN crsColumnsMixed
										FETCH NEXT FROM crsColumnsMixed INTO @columnSource, @columnDestination
										WHILE @@FETCH_STATUS=0
											begin
												SET @queryToRun=@queryToRun +'[' + @columnDestination + '],' 			
												FETCH NEXT FROM crsColumnsMixed INTO @columnSource, @columnDestination
											end
										CLOSE crsColumnsMixed
										DEALLOCATE crsColumnsMixed
				
										SET @queryToRun=SUBSTRING(@queryToRun,1,LEN(@queryToRun)-1) + ') SELECT '
										--------------------------------------------------------------------------------------------------------
		
										DECLARE crsColumnsMixed CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnSource], [ColumnDestination] 
																								FROM #tmpTableColumnsMixed
										OPEN crsColumnsMixed
										FETCH NEXT FROM crsColumnsMixed INTO @columnSource, @columnDestination
										WHILE @@FETCH_STATUS=0
											begin
												SET @queryToRun=@queryToRun + '[' + @columnSource + '],' 			
												FETCH NEXT FROM crsColumnsMixed INTO @columnSource, @columnDestination
											end
										CLOSE crsColumnsMixed
										DEALLOCATE crsColumnsMixed
										SET @queryToRun=SUBSTRING(@queryToRun,1,LEN(@queryToRun)-1) + ' FROM [' + @sourceServerName + '].[' + @sourceDB + '].[' + @schemaNameSource + '].[' + @tableNameSource + ']'
		
										---------------------------------------------------------------------------------------------------------
										--detectie identity_insert
										SET @queryToRun1='SELECT count(*) 
														FROM 
														(	SELECT [id] FROM [' + @destinationDB + '].[dbo].[syscolumns] 
															WHERE [AutoVal] IS NOT NULL AND [id] IN (	SELECT so.[id] 
																										FROM [' + @destinationDB + '].[dbo].[sysobjects] so
																										INNER JOIN [' + @destinationDB + '].[dbo].[sysusers] su ON so.[uid] = su.[uid]
																										WHERE so.[name]=''' + @tableNameDestination + '''
																												AND su.[name]=''' + @schemaNameDestination + '''
																									)
															UNION ALL
															SELECT [object_id] FROM [' + @destinationDB + '].[sys].[columns] 
															WHERE [is_identity]=1 AND [object_id] IN (	SELECT so.[id] 
																										FROM [' + @destinationDB + '].[dbo].[sysobjects] so
																										INNER JOIN [' + @destinationDB + '].[dbo].[sysusers] su ON so.[uid] = su.[uid]
																										WHERE so.[name]=''' + @tableNameDestination + '''
																												AND su.[name]=''' + @schemaNameDestination + '''
																									)
														)X'
										SET @queryToRun1 = [dbo].[ufn_formatSQLQueryForLinkedServer](@destinationServerName, @queryToRun1)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=01
				
										TRUNCATE TABLE #tmpCount
										INSERT	INTO #tmpCount ([result])
												EXEC (@queryToRun1)
				
										SET @tmpCount=null
										SET @queryToRun1=null
										SELECT @tmpCount=[result] FROM #tmpCount

										IF ISNULL(@tmpCount, 0)>0
											SET @queryToRun1='SET IDENTITY_INSERT [' + @destinationDB + '].[' + @schemaNameDestination + '].[' + @tableNameDestination + '] ON'
										--------------------------------------------------------------------------------------------------------
			
										--in @queryToRun am construit scriptul de insert: INSERT
										--SET IDENTITY_INSERT ON / INSERT / SET IDENTITY_INSERT OFF
										IF ISNULL(@queryToRun1, '')<>''
											SET @queryToRun=@queryToRun1 + char(13) + @queryToRun + char(13) + REPLACE(@queryToRun1, ' ON', ' OFF')

										--------------------------------------------------------------------------------------------------------
										SET @nestExecutionLevel=@executionLevel+1
										EXEC @tmpCount = [dbo].[usp_tableGetRowCount]	@sqlServerName			= @destinationServerName,
																						@databaseName			= @destinationDB,
																						@schemaName				= @schemaNameDestination,
																						@tableName				= @tableNameDestination,
																						@executionLevel			= @nestExecutionLevel,
																						@debugMode				= @debugMode

										SET @queryToRun1= 'Total Rows In Destination Table Before Insert: ' + CAST(@tmpCount AS varchar(20))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
		
										--------------------------------------------------------------------------------------------------------
										SET @nestExecutionLevel=@executionLevel+1
										EXEC @tmpCount = [dbo].[usp_tableGetRowCount]	@sqlServerName			= @sourceServerName,
																						@databaseName			= @sourceDB,
																						@schemaName				= @schemaNameSource,
																						@tableName				= @tableNameSource,
																						@executionLevel			= @nestExecutionLevel,
																						@debugMode				= @debugMode

										SET @queryToRun1= 'Total Rows In Source Table To Be Copied In Destination: ' + CAST(@tmpCount AS varchar(20))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
										--------------------------------------------------------------------------------------------------------
				
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

										SET @queryToRun1= 'Inserting records... '
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun1, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										EXEC @tmpServerDestination @queryToRun
										SET @ReturnValue=@@ERROR
										SET @queryToRun='Error Returned: ' + CAST(@ReturnValue AS varchar)
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

										--------------------------------------------------------------------------------------------------------
										--checkident
										SET @nestExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@SQLServerName			= @destinationServerName,
																					@DBName					= @destinationDB,
																					@TableSchema			= @schemaNameDestination,
																					@TableName				= @tableNameDestination,
																					@flgActions				= 32,
																					@flgOptions				= DEFAULT,
																					@executionLevel			= @nestExecutionLevel,
																					@debugMode				= @debugMode

										--update usage
										EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@SQLServerName			= @destinationServerName,
																					@DBName					= @destinationDB,
																					@TableSchema			= @schemaNameDestination,
																					@TableName				= @tableNameDestination,
																					@flgActions				= 64,
																					@flgOptions				= DEFAULT,
																					@executionLevel			= @nestExecutionLevel,
																					@debugMode				= @debugMode

		
										--------------------------------------------------------------------------------------------------------
										SET @nestExecutionLevel=@executionLevel+1
										EXEC @tmpCount = [dbo].[usp_tableGetRowCount]	@sqlServerName			= @destinationServerName,
																						@databaseName			= @destinationDB,
																						@schemaName				= @schemaNameDestination,
																						@tableName				= @tableNameDestination,
																						@executionLevel			= @nestExecutionLevel,
																						@debugMode				= @debugMode

										SET @queryToRun= 'Total Rows In Destination Table After Insert: ' + CAST(@tmpCount AS varchar(20))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										--------------------------------------------------------------------------------------------------------
										EXEC [dbo].[usp_logPrintMessage] @customMessage = 'Copy Data from Source to Destination - Stop', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
										--------------------------------------------------------------------------------------------------------
									end

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
							end
						---------------------------------------------------------------------------------------------------------
						FETCH NEXT FROM crsDBMixed INTO @schemaNameSource, @tableNameSource, @schemaNameDestination, @tableNameDestination
					end
				CLOSE crsDBMixed
				DEALLOCATE crsDBMixed
			end

		-----------------------------------------------------------------------------------------------------------------------------------------
		--sters tabelele temporare create
		IF object_id('#tmpDBSource') IS NOT NULL DROP TABLE #tmpDBSource
		IF object_id('#tmpDBDestination') IS NOT NULL DROP TABLE #tmpDBDestination
		IF object_id('#tmpDBMixed') IS NOT NULL DROP TABLE #tmpDBMixed
		IF object_id('#tmpTableColumnsSource') IS NOT NULL DROP TABLE #tmpTableColumnsSource
		IF object_id('#tmpTableColumnsDestination') IS NOT NULL DROP TABLE #tmpTableColumnsDestination
		IF object_id('#tmpTableColumnsMixed') IS NOT NULL DROP TABLE #tmpTableColumnsMixed
		IF object_id('#tmpCount') IS NOT NULL DROP TABLE #tmpCount
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

RAISERROR('Create procedure: [dbo].[usp_mpGetIndexCreationScript]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpGetIndexCreationScript]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
		@sqlServerName		[sysname]=@@SERVERNAME,
		@dbName				[sysname],
		@tableSchema		[sysname]='dbo',
		@tableName			[sysname],
		@indexName			[sysname],
		@indexID			[int],
		@flgOptions			[int] = 4099,
		@sqlIndexCreate		[nvarchar](max) OUTPUT,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 07.01.2010
-- Module     : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed
--		@indexName		- name of the index to be analyzed
--		@indexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@flgOptions:	1 - get also indexes that are created by a table constraint (primary or unique key) (default)
--						2 - use drop existing to recreate the index (default)
--					 4096 - use ONLINE=ON, if applicable (default)
--		@debugMode:		1 - print dynamic SQL statements 
--						0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Output Parameters:
--		@sqlIndexCreate	- sql statement that will create the index
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE @queryToRun			[nvarchar](max),
		@sqlIndexInclude	[nvarchar](max),
		@sqlIndexWithClause [nvarchar](max),
		@sqlScriptOnline	[nvarchar](512),
		@crtIndexName		[sysname],
		@IndexType			[tinyint],
		@FillFactor			[tinyint],
		@IsUniqueConstraint	[int],
		@IsPadded			[int],
		@AllowRowLocks		[int],
		@AllowPageLocks		[int],
		@IgnoreDupKey		[int],
		@KeyOrdinal			[int],
		@IndexColumnID		[int],
		@IsIncludedColumn	[bit],
		@IsDescendingKey	[bit],
		@ColumnName			[sysname],
		@FileGroupName		[sysname],
		@ReturnValue		[int],
		@nestExecutionLevel	[tinyint]

DECLARE @IndexDetails TABLE	(
								[IndexName]			[sysname]	NULL,
								[IndexType]			[tinyint]	NULL,
								[FillFactor]		[tinyint]	NULL,
								[FileGroupName]		[sysname]	NULL,
								[IsUniqueConstraint][bit]		NULL,
								[IsPadded]			[bit]		NULL,
								[AllowRowLocks]		[bit]		NULL,
								[AllowPageLocks]	[bit]		NULL,
								[IgnoreDupKey]		[bit]		NULL
							)

DECLARE @IndexColumnDetails TABLE
							(
								[KeyOrdinal]		[int]		NULL,
								[IndexColumnID]		[int]		NULL,
								[IsIncludedColumn]	[bit]		NULL,
								[IsDescendingKey]	[bit]		NULL,
								[ColumnName]		[sysname]	NULL
							)

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 1

		--get current index properties
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT  idx.[name]
										, idx.[type]
										, idx.[fill_factor]
										, dSp.[name] AS [file_group_name]
										, idx.[is_unique]
										, idx.[is_padded]
										, idx.[allow_row_locks]
										, idx.[allow_page_locks]
										, idx.[ignore_dup_key]
									FROM [' + @dbName + '].[sys].[indexes]				idx
									INNER JOIN [' + @dbName + '].[sys].[objects]		obj ON  idx.[object_id] = obj.[object_id]
									INNER JOIN [' + @dbName + '].[sys].[schemas]		sch ON	sch.[schema_id] = obj.[schema_id]
									INNER JOIN [' + @dbName + '].[sys].[data_spaces]	dSp	ON  idx.[data_space_id] = dSp.[data_space_id]
									WHERE	obj.[name] = ''' + @tableName + '''
											AND sch.[name] = ''' + @tableSchema + '''' + 
											CASE	WHEN @indexName IS NOT NULL 
													THEN ' AND idx.[name] = ''' + @indexName + ''''
													ELSE ' AND idx.[index_id] = ' + CAST(@indexID AS [nvarchar])
											END + 
											CASE WHEN @flgOptions & 1 <> 1
												 THEN '	AND NOT EXISTS	(
																			SELECT 1
																			FROM [' + @dbName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																			WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																					AND [CONSTRAINT_CATALOG]=''' + @dbName + '''
																					AND [TABLE_NAME]=''' + @tableName + '''
																					AND [TABLE_SCHEMA] = ''' + @tableSchema + '''
																					AND [CONSTRAINT_NAME]=''' + @indexName + '''
																		)'
												ELSE ''
											END
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexDetails
		INSERT INTO @IndexDetails ([IndexName], [IndexType], [FillFactor], [FileGroupName], [IsUniqueConstraint], [IsPadded], [AllowRowLocks], [AllowPageLocks], [IgnoreDupKey])
			EXEC (@queryToRun)

		--get index fill factor and file group
		SELECT	  @crtIndexName		= ISNULL(@indexName, [IndexName])
				, @IndexType		= [IndexType]
				, @FillFactor		= [FillFactor]
				, @FileGroupName	= [FileGroupName]
				, @IsUniqueConstraint = [IsUniqueConstraint]
				, @IsPadded			= [IsPadded]
				, @AllowRowLocks	= [AllowRowLocks]
				, @AllowPageLocks	= [AllowPageLocks]
				, @IgnoreDupKey		= [IgnoreDupKey]
		FROM @IndexDetails
		
		--get current index key columns and include columns and their properties
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT    
										  idxCol.[key_ordinal]
										, idxCol.[index_column_id]
										, idxCol.[is_included_column]
										, idxCol.[is_descending_key]
										, col.[name] AS [column_name]
								FROM [' + @dbName + '].[sys].[indexes] idx
								INNER JOIN [' + @dbName + '].[sys].[index_columns] idxCol ON	idx.[object_id] = idxCol.[object_id]
																								AND idx.[index_id] = idxCol.[index_id]
								INNER JOIN [' + @dbName + '].[sys].[columns]		 col	ON	idxCol.[object_id] = col.[object_id]
																								AND idxCol.[column_id] = col.[column_id]
								INNER JOIN [' + @dbName + '].[sys].[objects]		 obj	ON  idx.[object_id] = obj.[object_id]
								INNER JOIN [' + @dbName + '].[sys].[schemas]		 sch	ON	sch.[schema_id] = obj.[schema_id]
								WHERE	obj.[name] = ''' + @tableName + '''
										AND sch.[name] = ''' + @tableSchema + '''' + 
										CASE	WHEN @indexName IS NOT NULL 
												THEN ' AND idx.[name] = ''' + @indexName + ''''
												ELSE ' AND idx.[index_id] = ' + CAST(@indexID AS [nvarchar])
										END + 
										CASE WHEN @flgOptions & 1 <> 1
											 THEN '	AND NOT EXISTS	(
																		SELECT 1
																		FROM [' + @dbName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																		WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																				AND [CONSTRAINT_CATALOG]=''' + @dbName + '''
																				AND [TABLE_NAME]=''' + @tableName + '''
																				AND [TABLE_SCHEMA]=''' + @tableSchema + '''
																				AND [CONSTRAINT_NAME]=''' + @indexName + '''
																	)'
											ELSE ''
										END
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexColumnDetails
		INSERT INTO @IndexColumnDetails ([KeyOrdinal], [IndexColumnID], [IsIncludedColumn], [IsDescendingKey], [ColumnName])
			EXEC (@queryToRun)

		SET @sqlIndexCreate=N''
		IF EXISTS (SELECT 1 FROM @IndexColumnDetails)
			begin
				-- check for online operation mode, for reorganize/rebuild
				SET @nestExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @sqlServerName,
																@dbName				= @dbName,
																@tableSchema		= @tableSchema,
																@tableName			= @tableName,
																@indexName			= @indexName,
																@indexID			= @indexID,
																@partitionNumber	= 1,
																@sqlScriptOnline	= @sqlScriptOnline OUT,
																@flgOptions			= @flgOptions,
																@executionLevel		= @nestExecutionLevel,
																@debugMode			= @debugMode

				SET @sqlIndexCreate = @sqlIndexCreate + N'CREATE'
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IsUniqueConstraint=1	
																THEN ' UNIQUE' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IndexType=1	
																THEN ' CLUSTERED' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 ' INDEX [' + @crtIndexName + '] ON [' + @tableSchema + '].[' + @tableName + '] ('
				--index key columns
				/*
				DECLARE crsIndexKey CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName], [IsDescendingKey]
																	FROM @IndexColumnDetails
																	WHERE [IsIncludedColumn] = 0
																	ORDER BY [KeyOrdinal]
				OPEN crsIndexKey
				FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexCreate = @sqlIndexCreate + '[' + @ColumnName + ']' + 
												CASE WHEN @IsDescendingKey=1	THEN ' DESC'
																				ELSE '' END + ', '
						FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
					end
				CLOSE  crsIndexKey
				DEALLOCATE crsIndexKey
				*/

				SELECT @sqlIndexCreate = @sqlIndexCreate + '[' + [ColumnName] + ']' + 
										CASE WHEN [IsDescendingKey]=1	THEN ' DESC'
																		ELSE '' END + ', '
				FROM @IndexColumnDetails
				WHERE [IsIncludedColumn] = 0
				ORDER BY [KeyOrdinal]

				IF LEN(@sqlIndexCreate)<>0
					SET @sqlIndexCreate = SUBSTRING(@sqlIndexCreate, 1, LEN(@sqlIndexCreate)-1) + ')'

				--index include columns
				SET @sqlIndexInclude = N''
				/*
				DECLARE crsIndexInclude CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName]
																		FROM @IndexColumnDetails
																		WHERE [IsIncludedColumn] = 1
																		ORDER BY [IndexColumnID]
				OPEN crsIndexInclude
				FETCH NEXT FROM crsIndexInclude INTO @ColumnName
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexInclude = @sqlIndexInclude + '[' + @ColumnName + '], '
						FETCH NEXT FROM crsIndexInclude INTO @ColumnName
					end
				CLOSE  crsIndexInclude
				DEALLOCATE crsIndexInclude
				*/
				SELECT @sqlIndexInclude = @sqlIndexInclude + '[' + [ColumnName] + '], '
				FROM @IndexColumnDetails
				WHERE [IsIncludedColumn] = 1
				ORDER BY [IndexColumnID]

				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexInclude = SUBSTRING(@sqlIndexInclude, 1, LEN(@sqlIndexInclude)-1)


				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexCreate = @sqlIndexCreate + N' INCLUDE(' + @sqlIndexInclude + ')'

				--index options
				SET @sqlIndexWithClause = N''
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'PAD_INDEX = ' + CASE WHEN @IsPadded=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_ROW_LOCKS = ' + CASE WHEN @AllowRowLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_PAGE_LOCKS = ' + CASE WHEN @AllowPageLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'IGNORE_DUP_KEY = ' + CASE WHEN @IgnoreDupKey=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + @sqlScriptOnline
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'SORT_IN_TEMPDB = ON'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'STATISTICS_NORECOMPUTE = OFF'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'MAXDOP = 1'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE WHEN @FillFactor<>0	
											 THEN CASE	WHEN LEN(@sqlIndexWithClause)>0 
														THEN ', '
														ELSE ''
												  END + N'FILLFACTOR=' + CAST(@FillFactor AS [nvarchar])
											 ELSE ''
										END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + 
										CASE WHEN @flgOptions & 2 = 2 
											 THEN N'DROP_EXISTING = ON'
											 ELSE ''
										END
				--index storage filegroup
				SET @sqlIndexCreate = @sqlIndexCreate + 
										CASE WHEN LEN(@sqlIndexWithClause)>0
											 THEN N' WITH (' + @sqlIndexWithClause + ')'
											 ELSE ''
										END + N' ON [' + @FileGroupName + ']'
			end
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

RAISERROR('Create procedure: [dbo].[usp_mpDatabaseShrink]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseShrink]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseShrink]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseShrink]
		@sqlServerName		[sysname],
		@dbName				[sysname] = NULL,
		@flgActions			[smallint] = 1,	/*	1 - shrink log file
												2 - shrink database
											*/
		@flgOptions			[int] = 1,	/*	1 - use truncate only
											*/		
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 01.03.2010
-- Module			 : Database Maintenance Scripts
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON

DECLARE		@queryToRun    			[nvarchar](4000),
			@databaseName			[sysname],
			@logName				[sysname],
			@errorCode				[int],
			@nestedExecutionLevel	[int]

---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF object_id('tempdb..#DatabaseList') IS NOT NULL 
	DROP TABLE #DatabaseList

CREATE TABLE #DatabaseList(
								[dbname] [sysname]
							)

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseFiles') IS NOT NULL 
	DROP TABLE #databaseFiles

CREATE TABLE #databaseFiles(
								[name] [varchar](4000)
							)

---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6)

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName				[sysname],
		@agInstanceRoleDesc	[sysname],
		@agStopLimit		[int],
		@actionType			[sysname]

SET @agStopLimit = 0
SET @actionType = NULL

IF @flgActions & 1 = 1	SET @actionType = 'shrink log'
IF @flgActions & 2 = 2	SET @actionType = 'shrink database'

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database shrink',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0

---------------------------------------------------------------------------------------------
SET @errorCode	 = 1
---------------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

---------------------------------------------------------------------------------------------
--get database list that will be analyzed
SET @queryToRun = N''

/* exclude databases that are currently being backuped, to avoid errors like:
	Msg 3023, Level 16, State 2, Line 1
	Backup and file manipulation operations (such as ALTER DATABASE ADD FILE) on a database must be serialized. Reissue the statement after the current backup or file manipulation operation is completed.
*/
IF @dbName IS NULL
	SET @queryToRun = @queryToRun + N'SELECT DISTINCT sdb.[name] 
										FROM master..sysdatabases sdb
										WHERE sdb.[name] LIKE ''' + CASE WHEN @dbName IS NULL THEN '%' ELSE @dbName END + '''
											AND NOT EXISTS (
															 SELECT 1
															 FROM  master.dbo.sysprocesses sp
															 WHERE sp.[cmd] LIKE ''BACKUP %''
																	AND sp.[dbid]=sdb.[dbid]
															)'
ELSE
	SET @queryToRun = @queryToRun + N'SELECT ''' + @dbName + ''' AS [name]
										WHERE NOT EXISTS (
															 SELECT 1
															 FROM  master.dbo.sysprocesses sp
															 WHERE sp.[cmd] LIKE ''BACKUP %''
																	AND sp.[dbid]= DB_ID(''' + @dbName + ''')
															)'

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #DatabaseList
INSERT	INTO #DatabaseList([dbname])
		EXEC (@queryToRun)


DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT	[dbname] 
													FROM	#DatabaseList
OPEN crsDatabases
FETCH NEXT FROM crsDatabases INTO @databaseName
WHILE @@FETCH_STATUS=0
	begin
		---------------------------------------------------------------------------------------------
		--shrink database
		IF @flgActions & 2 = 2
			begin
				SET @queryToRun= 'Shrinking database...' + ' [' + @dbName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun = N'DBCC SHRINKDATABASE([' + @databaseName + N']' + CASE WHEN @flgOptions & 1 = 1 THEN N', TRUNCATEONLY' ELSE N'' END + N') WITH NO_INFOMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@module			= 'dbo.usp_mpDatabaseShrink',
																@eventName		= 'database shrink',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode						
			end


		---------------------------------------------------------------------------------------------
		--shrink log file
		IF @flgActions & 1 = 1
			begin
				SET @queryToRun= 'Shrinking database log files...' + ' [' + @dbName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #databaseFiles

				SET @queryToRun = N'SELECT [name] FROM [' + @databaseName + ']..sysfiles WHERE [status] & 0x40 = 0x40'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				INSERT	INTO #databaseFiles
						EXEC (@queryToRun)

				DECLARE crsLogFile CURSOR LOCAL FAST_FORWARD FOR SELECT LTRIM(RTRIM([name])) FROM #databaseFiles
				OPEN crsLogFile
				FETCH NEXT FROM crsLogFile INTO @logName
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun = N'USE [' + @databaseName + ']; DBCC SHRINKFILE([' + @logName + N']' + CASE WHEN @flgOptions & 1 = 1 THEN N', TRUNCATEONLY' ELSE N'' END + N') WITH NO_INFOMSGS'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@module			= 'dbo.usp_mpDatabaseShrink',
																		@eventName		= 'database shrink log',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode						
						
						FETCH NEXT FROM crsLogFile INTO @logName
					END
				CLOSE crsLogFile
				DEALLOCATE crsLogFile
			end
		FETCH NEXT FROM crsDatabases INTO @databaseName
	end
CLOSE crsDatabases
DEALLOCATE crsDatabases

RETURN @errorCode
GO

RAISERROR('Create procedure: [dbo].[usp_mpDatabaseOptimize]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseOptimize]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseOptimize]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseOptimize]
		@sqlServerName				[sysname]=@@SERVERNAME,
		@dbName						[sysname],
		@tableSchema				[sysname]	=   '%',
		@tableName					[sysname]   =   '%',
		@flgActions					[smallint]	=    27,
		@flgOptions					[int]		= 45185,--32768 + 8192 + 4096 + 128 + 1
		@defragIndexThreshold		[smallint]	=     5,
		@rebuildIndexThreshold		[smallint]	=    30,
		@pageThreshold				[int]		=  1000,
		@rebuildIndexPageCountLimit	[int]		= 2147483647,	--16TB/no limit
		@statsSamplePercent			[smallint]	=   100,
		@statsAgeDays				[smallint]	=   365,
		@statsChangePercent			[smallint]	=     1,
		@maxDOP						[smallint]	=	  1,
		@maxRunningTimeInMinutes	[smallint]	=     0,
		@skipObjectsList			[nvarchar](1024) = NULL,
		@executionLevel				[tinyint]	=     0,
		@debugMode					[bit]		=     0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify % for all tables or a table name to be analyzed
--		@flgActions		 1	- Defragmenting database tables indexes (ALTER INDEX REORGANIZE)				(default)
--							  should be performed daily
--						 2	- Rebuild heavy fragmented indexes (ALTER INDEX REBUILD)						(default)
--							  should be performed daily
--					     4  - Rebuild all indexes (ALTER INDEX REBUILD)
--						 8  - Update statistics for table (UPDATE STATISTICS)								(default)
--							  should be performed daily
--						16  - Rebuild heap tables (SQL versions +2K5 only)									(default)
--		@flgOptions		 1  - Compact large objects (LOB) (default)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					    64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					   128  - Create statistics on index columns only (default). Default behaviour is to not create statistics on all eligible columns
--					   256  - Create statistics using default sample scan. Default behaviour is to create statistics using fullscan mode
--					   512  - update auto-created statistics
--					  1024	- get index statistics using DETAILED analysis (default is to use LIMITED)
--							  for heaps, will always use DETAILED in order to get page density and forwarded records information
--					  4096  - rebuild/reorganize indexes/tables using ONLINE=ON, if applicable (default)
--					  8192  - when rebuilding heaps, disable/enable table triggers (default)
--					 16384  - for versions below 2008, do heap rebuild using temporary clustered index
--					 32768  - analyze only tables with at least @pageThreshold pages reserved (+2k5 only)
--					 65536  - cleanup of ghost records (sp_clean_db_free_space)
--							- this may be forced by setting to true property 'Force cleanup of ghost records'

--		@defragIndexThreshold		- min value for fragmentation level when to start reorganize it
--		@@rebuildIndexThreshold		- min value for fragmentation level when to start rebuild it
--		@pageThreshold				- the minimum number of pages for an index to be reorganized/rebuild
--		@rebuildIndexPageCountLimit	- the maximum number of page for an index to be rebuild. if index has more pages than @rebuildIndexPageCountLimit, it will be reorganized
--		@statsSamplePercent			- value for sample percent when update statistics. if 100 is present, then fullscan will be used
--		@statsAgeDays				- when statistics were last updated (stats ages); don't update statistics more recent then @statsAgeDays days
--		@statsChangePercent			- for more recent statistics, if percent of changes is greater of equal, perform update
--		@maxDOP						- when applicable, use this MAXDOP value (ex. index rebuild)
--		@maxRunningTimeInMinutes	- the number of minutes the optimization job will run. after time exceeds, it will exist. 0 or null means no limit
--		@skipObjectsList			- comma separated list of the objects (tables, index name or stats name) to be excluded from maintenance.
--		@debugMode					- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    					[nvarchar](4000),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@childObjectName				[sysname],
			@IndexName						[sysname],
			@IndexTypeDesc					[sysname],
			@IndexType						[tinyint],
			@IndexFillFactor				[tinyint],
			@DatabaseID						[int], 
			@IndexID						[int],
			@ObjectID						[int],
			@CurrentFragmentation			[numeric] (6,2),
			@CurentPageDensityDeviation		[numeric] (6,2),
			@CurrentPageCount				[bigint],
			@CurrentForwardedRecordsPercent	[numeric] (6,2),
			@errorCode						[int],
			@ClusteredRebuildNonClustered	[bit],
			@flgInheritOptions				[int],
			@statsCount						[int], 
			@nestExecutionLevel				[tinyint],
			@analyzeIndexType				[nvarchar](32),
			@eventData						[varchar](8000),
			@affectedDependentObjects		[nvarchar](4000),
			@indexIsRebuilt					[bit],
			@stopTimeLimit					[datetime]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--determine when to stop current optimization task, based on @maxRunningTimeInMinutes value
---------------------------------------------------------------------------------------------
IF ISNULL(@maxRunningTimeInMinutes, 0)=0
	SET @stopTimeLimit = CONVERT([datetime], '9999-12-31 23:23:59', 120)
ELSE
	SET @stopTimeLimit = DATEADD(minute, @maxRunningTimeInMinutes, GETDATE())


---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

-----------------------------------------------------------------------------------------
--get configuration values: Force cleanup of ghost records
---------------------------------------------------------------------------------------------
DECLARE   @forceCleanupGhostRecords [nvarchar](128)
		, @thresholdGhostRecords	[bigint]

SELECT	@forceCleanupGhostRecords=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Force cleanup of ghost records'
		AND [module] = 'maintenance-plan'

SET @forceCleanupGhostRecords = LOWER(ISNULL(@forceCleanupGhostRecords, 'false'))

--run index statistics using DETAILED option
IF LOWER(@forceCleanupGhostRecords)='true' AND @flgOptions & 1024 = 0
	SET @flgOptions = @flgOptions + 1024

--enable local cleanup of ghost records option
IF LOWER(@forceCleanupGhostRecords)='true' AND @flgOptions & 65536 = 0
	SET @flgOptions = @flgOptions + 65536

IF LOWER(@forceCleanupGhostRecords)='true' OR @flgOptions & 65536 = 65536
	begin
		SELECT	@thresholdGhostRecords=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name]='Ghost records cleanup threshold'
				AND [module] = 'maintenance-plan'
	end

SET @thresholdGhostRecords = ISNULL(@thresholdGhostRecords, 0)

---------------------------------------------------------------------------------------------
--get SQL Server running major version and database compatibility level
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
---------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName				[sysname],
		@agInstanceRoleDesc	[sysname],
		@agStopLimit		[int],
		@actionType			[sysname]

SET @agStopLimit = 0

IF @flgActions &  1 =  1	SET @actionType = 'reorganize index'
IF @flgActions &  2 =  2	SET @actionType = 'rebuilding index'
IF @flgActions &  4 =  4	SET @actionType = 'rebuilding index'
IF @flgActions &  8 =  8	SET @actionType = 'update statistics'
IF @flgActions & 16 = 16	SET @actionType = 'rebuilding heap'

IF @serverVersionNum >= 11
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database maintenance',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT,
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

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC (@queryToRun)

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility

IF @serverVersionNum >= 9 AND @compatibilityLevel<=80
	SET @serverVersionNum = 8

---------------------------------------------------------------------------------------------

SET @errorCode				 = 0
SET @CurrentTableSchema		 = @tableSchema

IF ISNULL(@defragIndexThreshold, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF ISNULL(@rebuildIndexThreshold, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Threshold value for rebuilding indexes should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF ISNULL(@statsSamplePercent, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Sample percent value for update statistics sample should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF @defragIndexThreshold > @rebuildIndexThreshold
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be smalller or equal to threshold value for rebuilding indexes.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end


---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
CREATE TABLE #CurrentIndexFragmentationStats 
		(	
			[ObjectName] 					[varchar] (255),
			[ObjectId] 						[int],
			[IndexName] 					[varchar] (255),
			[IndexId] 						[int],
			[Level] 						[int],
			[Pages]		 					[int],
			[Rows] 							[bigint],
			[MinimumRecordSize]				[int],
			[MaximumRecordSize]				[int],
			[AverageRecordSize] 			[int],
			[ForwardedRecords] 				[int],
			[Extents] 						[int],
			[ExtentSwitches] 				[int],
			[AverageFreeBytes] 				[int],
			[AveragePageDensity] 			[decimal](38,2),
			[ScanDensity] 					[decimal](38,2),
			[BestCount] 					[int],
			[ActualCount] 					[int],
			[LogicalFragmentation] 			[decimal](38,2),
			[ExtentFragmentation] 			[decimal](38,2),
			[ghost_record_count]			[bigint]		NULL
		)	
			
CREATE INDEX IX_CurrentIndexFragmentationStats ON #CurrentIndexFragmentationStats([ObjectId], [IndexId])


---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 
	DROP TABLE #databaseObjectsWithIndexList

CREATE TABLE #databaseObjectsWithIndexList(
											[database_id]					[int],
											[object_id]						[int],
											[table_schema]					[sysname],
											[table_name]					[sysname],
											[index_id]						[int],
											[index_name]					[sysname]	NULL,													
											[index_type]					[tinyint],
											[fill_factor]					[tinyint]	NULL,
											[is_rebuilt]					[bit]		NOT NULL DEFAULT (0),
											[page_count]					[bigint]	NULL,
											[avg_fragmentation_in_percent]	[decimal](38,2)	NULL,
											[ghost_record_count]			[bigint]	NULL,
											[forwarded_records_percentage]	[decimal](38,2)	NULL,
											[page_density_deviation]		[decimal](38,2)	NULL
											)
CREATE INDEX IX_databaseObjectsWithIndexList_TableName ON #databaseObjectsWithIndexList([table_schema], [table_name], [index_id], [avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])
CREATE INDEX IX_databaseObjectsWithIndexList_LogicalDefrag ON #databaseObjectsWithIndexList([avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseObjectsWithStatisticsList') IS NOT NULL 
	DROP TABLE #databaseObjectsWithStatisticsList

CREATE TABLE #databaseObjectsWithStatisticsList(
												[database_id]			[int],
												[object_id]				[int],
												[table_schema]			[sysname],
												[table_name]			[sysname],
												[stats_id]				[int],
												[stats_name]			[sysname],													
												[auto_created]			[bit],
												[rows]					[bigint]		NULL,
												[modification_counter]	[bigint]		NULL,
												[last_updated]			[datetime]		NULL,
												[percent_changes]		[decimal](38,2)	NULL
												)


---------------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

--------------------------------------------------------------------------------------------------
--16 - get current heap tables list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'0'

		SET @queryToRun=N'Create list of heap tables to be analyzed...' + @dbName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		SET @queryToRun = @queryToRun + 
							N'SELECT DISTINCT 
										DB_ID(''' + @dbName + ''') AS [database_id]
									, si.[object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[index_id]
									, si.[name] AS [index_name]
									, si.[type] AS [index_type]
									, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
							FROM [' + @dbName + '].[sys].[indexes]				si
							INNER JOIN [' + @dbName + '].[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
							INNER JOIN [' + @dbName + '].[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
							CASE WHEN @flgOptions & 32768 = 32768 
								THEN N'
							INNER JOIN
									(
											SELECT   [object_id]
												, SUM([reserved_page_count]) as [reserved_page_count]
											FROM [' + @dbName + '].sys.dm_db_partition_stats
											GROUP BY [object_id]
											HAVING SUM([reserved_page_count]) >=' + CAST(@pageThreshold AS [nvarchar](32)) + N'
									) ps ON ps.[object_id] = ob.[object_id]'
								ELSE N''
								END + N'
							WHERE	ob.[name] LIKE ''' + @tableName + '''
									AND sc.[name] LIKE ''' + @tableSchema + '''
									AND si.[type] IN (' + @analyzeIndexType + N')
									AND ob.[type] IN (''U'', ''V'')' + 
									CASE WHEN @skipObjectsList IS NOT NULL  THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))
																					AND (si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) OR si.[name] IS NULL)'  
																			ELSE N'' END

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor])
				EXEC (@queryToRun)

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		DELETE dtl
		FROM #databaseObjectsWithIndexList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[table_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]

		DELETE dtl
		FROM #databaseObjectsWithIndexList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[index_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))

			
--------------------------------------------------------------------------------------------------
--1/2	- Analyzing heap tables fragmentation
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Analyzing heap fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun='[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + CASE WHEN @IndexName IS NOT NULL THEN N' - [' + @IndexName + ']' ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun=N'SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
											, ips.[object_id]
											, si.[name] as index_name
											, ips.[index_id]
											, ips.[avg_fragmentation_in_percent]
											, ips.[page_count]
											, ips.[record_count]
											, ips.[forwarded_record_count]
											, ips.[avg_record_size_in_bytes]
											, ips.[avg_page_space_used_in_percent]
											, ips.[ghost_record_count]
									FROM [' + @dbName + '].sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
													'DETAILED'
											+ ''') ips
									INNER JOIN [' + @dbName + '].sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
									WHERE	si.[type] IN (' + @analyzeIndexType + N')'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
				INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count])  
						EXEC (@queryToRun)

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[LogicalFragmentation]
				, doil.[page_count] = cifs.[Pages]
				, doil.[ghost_record_count] = cifs.[ghost_record_count]
				, doil.[forwarded_records_percentage] = CASE WHEN ISNULL(cifs.[Rows], 0) > 0 
															 THEN (CAST(cifs.[ForwardedRecords] AS decimal(29,2)) / CAST(cifs.[Rows]  AS decimal(29,2))) * 100
															 ELSE 0
														END
				, doil.[page_density_deviation] =  ABS(CASE WHEN ISNULL(cifs.[AverageRecordSize], 0) > 0
															THEN ((FLOOR(8060. / ISNULL(cifs.[AverageRecordSize], 0)) * ISNULL(cifs.[AverageRecordSize], 0)) / 8060.) * doil.[fill_factor] - cifs.[AveragePageDensity]
															ELSE 0
													   END)
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #CurrentIndexFragmentationStats cifs ON cifs.[ObjectId] = doil.[object_id] AND cifs.[IndexId] = doil.[index_id]
	end


--------------------------------------------------------------------------------------------------
-- 16	- Rebuild heap tables (SQL versions +2K5 only)
-- implemented an algoritm based on Tibor Karaszi's one: http://sqlblog.com/blogs/tibor_karaszi/archive/2014/03/06/how-often-do-you-rebuild-your-heaps.aspx
-- rebuilding heaps also rebuild its non-clustered indexes. do heap maintenance before index maintenance
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database heap tables...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[forwarded_records_percentage]
		   													FROM	#databaseObjectsWithIndexList doil
															WHERE	(    doil.[avg_fragmentation_in_percent] >= @rebuildIndexThreshold
																	  OR doil.[forwarded_records_percentage] >= @defragIndexThreshold
																	  OR doil.[page_density_deviation] >= @rebuildIndexThreshold
																	)
																	AND doil.[index_type] IN (0)
															ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   		SET @queryToRun=N'Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density deviation = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				--------------------------------------------------------------------------------------------------
				--log heap fragmentation information
				SET @eventData='<heap-fragmentation><detail>' + 
									'<database_name>' + @dbName + '</database_name>' + 
									'<object_name>' + @objectName + '</object_name>'+ 
									'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
									'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
									'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
									'<forwarded_records_percentage>' + CAST(@CurrentForwardedRecordsPercent AS [varchar](32)) + '</forwarded_records_percentage>' + 
								'</detail></heap-fragmentation>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@objectName		= @objectName,
													@module			= 'dbo.usp_mpDatabaseOptimize',
													@eventName		= 'database maintenance - rebuilding heap',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				--------------------------------------------------------------------------------------------------
				SET @nestExecutionLevel = @executionLevel + 3
				EXEC [dbo].[usp_mpAlterTableRebuildHeap]	@sqlServerName		= @sqlServerName,
															@dbName				= @dbName,
															@tableSchema		= @CurrentTableSchema,
															@tableName			= @CurrentTableName,
															@flgActions			= 1,
															@flgOptions			= @flgOptions,
															@maxDOP				= @maxDOP,
															@executionLevel		= @nestExecutionLevel,
															@debugMode			= @debugMode

				--mark heap as being rebuilt
				UPDATE doil
					SET [is_rebuilt]=1
				FROM	#databaseObjectsWithIndexList doil 
	   			WHERE	doil.[table_name] = @CurrentTableName
	   					AND doil.[table_schema] = @CurrentTableSchema
						AND doil.[index_type] = 0
				
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4 - get current index list: clustered, non-clustered, xml, spatial
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'1,2,3,4'		

		SET @queryToRun=N'Create list of indexes to be analyzed...' + @dbName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF @serverVersionNum >= 9
			SET @queryToRun = @queryToRun + 
								N'SELECT DISTINCT 
										  DB_ID(''' + @dbName + ''') AS [database_id]
										, si.[object_id]
										, sc.[name] AS [table_schema]
										, ob.[name] AS [table_name]
										, si.[index_id]
										, si.[name] AS [index_name]
										, si.[type] AS [index_type]
										, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
								FROM [' + @dbName + '].[sys].[indexes]				si
								INNER JOIN [' + @dbName + '].[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
								INNER JOIN [' + @dbName + '].[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
								CASE WHEN @flgOptions & 32768 = 32768 
									THEN N'
								INNER JOIN
										(
											 SELECT   [object_id]
													, SUM([reserved_page_count]) as [reserved_page_count]
											 FROM [' + @dbName + '].sys.dm_db_partition_stats
											 GROUP BY [object_id]
											 HAVING SUM([reserved_page_count]) >=' + CAST(@pageThreshold AS [nvarchar](32)) + N'
										) ps ON ps.[object_id] = ob.[object_id]'
									ELSE N''
									END + N'
								WHERE	ob.[name] LIKE ''' + @tableName + '''
										AND sc.[name] LIKE ''' + @tableSchema + '''
										AND si.[type] IN (' + @analyzeIndexType + N')
										AND si.[is_disabled]=0
										AND ob.[type] IN (''U'', ''V'')' + 
										CASE WHEN @skipObjectsList IS NOT NULL	THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																						AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
																				ELSE N'' END
		ELSE
			SET @queryToRun = @queryToRun + 
								N'SELECT DISTINCT 
									  DB_ID(''' + @dbName + ''') AS [database_id]
									, si.[id] AS [object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[indid] AS [index_id]
									, si.[name] AS [index_name]
									, CASE WHEN si.[indid]=1 THEN 1 ELSE 2 END AS [index_type]
									, CASE WHEN ISNULL(si.[OrigFillFactor], 0) = 0 THEN 100 ELSE si.[OrigFillFactor] END AS [fill_factor]
								FROM [' + @dbName + ']..sysindexes si
								INNER JOIN [' + @dbName + ']..sysobjects ob	ON ob.[id] = si.[id]
								INNER JOIN [' + @dbName + ']..sysusers sc	ON sc.[uid] = ob.[uid]
								WHERE	ob.[name] LIKE ''' + @tableName + '''
										AND sc.[name] LIKE ''' + @tableSchema + '''
										AND si.[status] & 64 = 0 
										AND si.[status] & 8388608 = 0 
										AND si.[status] & 16777216 = 0 
										AND si.[indid] > 0
										AND si.[reserved] <> 0
										AND ob.[xtype] IN (''U'', ''V'')'+
										CASE WHEN @skipObjectsList IS NOT NULL	THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																						AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
																				ELSE N'' END

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor])
				EXEC (@queryToRun)

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		DELETE dtl
		FROM #databaseObjectsWithIndexList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[table_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]

		DELETE dtl
		FROM #databaseObjectsWithIndexList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[index_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))



--------------------------------------------------------------------------------------------------
--8	- get current statistics list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Create list of statistics to be analyzed...' + @dbName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF @serverVersionNum >= 9 
			begin
				IF (@serverVersionNum >= 10.504000 AND @serverVersionNum < 11) OR @serverVersionNum >= 11.03000
					/* starting with SQL Server 2008 R2 SP2 / SQL Server 2012 SP1 */
					SET @queryToRun = @queryToRun + 
										N'USE [' + @dbName + ']; SELECT DISTINCT 
												  DB_ID(''' + @dbName + ''') AS [database_id]
												, ss.[object_id]
												, sc.[name] AS [table_schema]
												, ob.[name] AS [table_name]
												, ss.[stats_id]
												, ss.[name] AS [stats_name]
												, ss.[auto_created]
												, sp.[last_updated]
												, sp.[rows]
												, ABS(sp.[modification_counter]) AS [modification_counter]
												, (ABS(sp.[modification_counter]) * 100. / CAST(sp.[rows] AS [float])) AS [percent_changes]
										FROM [' + @dbName + '].sys.stats ss
										INNER JOIN [' + @dbName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @dbName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]' + N'
										CROSS APPLY [' + @dbName + '].sys.dm_db_stats_properties(ss.object_id, ss.stats_id) AS sp
										WHERE	ob.[name] LIKE ''' + @tableName + '''
												AND sc.[name] LIKE ''' + @tableSchema + '''
												AND ob.[type] <> ''S''
												AND sp.[rows] > 0
												AND (    (    DATEDIFF(dd, sp.[last_updated], GETDATE()) >= ' + CAST(@statsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0
														 )
													 OR  
														 ( 
															  DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0 
														  AND (ABS(sp.[modification_counter]) * 100. / CAST(sp.[rows] AS [float])) >= ' + CAST(@statsChangePercent AS [nvarchar](32)) + N'
														 )
													)'+
												CASE WHEN @skipObjectsList IS NOT NULL	THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																								AND ss.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
																						ELSE N'' END
				ELSE
					/* SQL Server 2005 up to SQL Server 2008 R2 SP 2*/
					SET @queryToRun = @queryToRun + 
										N'USE [' + @dbName + ']; SELECT DISTINCT 
												  DB_ID(''' + @dbName + ''') AS [database_id]
												, ss.[object_id]
												, sc.[name] AS [table_schema]
												, ob.[name] AS [table_name]
												, ss.[stats_id]
												, ss.[name] AS [stats_name]
												, ss.[auto_created]
												, STATS_DATE(si.[id], si.[indid]) AS [last_updated]
												, si.[rowcnt] AS [rows]
												, ABS(si.[rowmodctr]) AS [modification_counter]
												, (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) AS [percent_changes]
										FROM [' + @dbName + '].sys.stats ss
										INNER JOIN [' + @dbName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @dbName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]
										INNER JOIN [' + @dbName + ']..sysindexes si ON si.[id] = ob.[object_id] AND si.[name] = ss.[name]' + N'
										WHERE	ob.[name] LIKE ''' + @tableName + '''
												AND sc.[name] LIKE ''' + @tableSchema + '''
												AND ob.[type] <> ''S''
												AND si.[rowcnt] > 0
												AND (    (    DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
														  AND si.[rowmodctr] <> 0
														 )
													 OR  
														( 
													 		  DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
														  AND si.[rowmodctr] <> 0 
														  AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@statsChangePercent AS [nvarchar](32)) + N'
														)
												)' +
												CASE WHEN @skipObjectsList IS NOT NULL THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																								AND ss.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))
																								AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
																					   ELSE N'' END
			end
		ELSE
			/* SQL Server 2000 */
			SET @queryToRun = @queryToRun + 
								N'USE [' + @dbName + ']; SELECT DISTINCT 
										  DB_ID(''' + @dbName + ''') AS [database_id]
										, si.[id] AS [object_id]
										, sc.[name] AS [table_schema]
										, ob.[name] AS [table_name]
										, si.[indid] AS [stats_id]
										, si.[name] AS [stats_name]
										, CASE WHEN si.[status] & 8388608 <> 0 THEN 1 ELSE 0 END AS [auto_created]
										, STATS_DATE(si.[id], si.[indid]) AS [last_updated]
										, si.[rowcnt] AS [rows]
										, ABS(si.[rowmodctr]) AS [modification_counter]
										, (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) AS [percent_changes]
									FROM [' + @dbName + ']..sysindexes si
									INNER JOIN [' + @dbName + ']..sysobjects ob	ON ob.[id] = si.[id]
									INNER JOIN [' + @dbName + ']..sysusers sc	ON sc.[uid] = ob.[uid]
									WHERE	ob.[name] LIKE ''' + @tableName + '''
											AND sc.[name] LIKE ''' + @tableSchema + '''
											AND si.[indid] > 0 
											AND si.[indid] < 255
											AND ob.[xtype] <> ''S''
											AND si.[rowcnt] > 0
											AND (    (    DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
													  AND si.[rowmodctr] <> 0
													 )
												 OR  
													( 
													 	  DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
													  AND si.[rowmodctr] <> 0 
													  AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@statsChangePercent AS [nvarchar](32)) + N'
													)
											)' + 
											CASE WHEN @skipObjectsList IS NOT NULL THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																							AND si.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
																				   ELSE N'' END

		IF @sqlServerName<>@@SERVERNAME
			SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC [' + @dbName + N']..sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'


		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithStatisticsList([database_id], [object_id], [table_schema], [table_name], [stats_id], [stats_name], [auto_created], [last_updated], [rows], [modification_counter], [percent_changes])
				EXEC (@queryToRun)

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		DELETE dtl
		FROM #databaseObjectsWithStatisticsList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[table_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]

		DELETE dtl
		FROM #databaseObjectsWithStatisticsList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[stats_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]
	end

UPDATE #databaseObjectsWithStatisticsList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [stats_name] = LTRIM(RTRIM([stats_name]))

IF @flgOptions & 32768 = 32768
	SET @flgOptions = @flgOptions - 32768

	
--------------------------------------------------------------------------------------------------
--1/2	- Analyzing tables fragmentation
--		fragmentation information for the data and indexes of the specified table or view
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4))  AND (GETDATE() <= @stopTimeLimit)
	begin

		SET @queryToRun='Analyzing index fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	WHERE [index_type] <> 0 /* exclude heaps */
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun='[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + CASE WHEN @IndexName IS NOT NULL THEN N' - [' + @IndexName + ']' ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				IF @serverVersionNum < 9	/* SQL 2000 */
					begin
						IF @sqlServerName=@@SERVERNAME
							SET @queryToRun='USE [' + @dbName + N']; IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC SHOWCONTIG (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'', ''' + @IndexName + ''' ) WITH ' + CASE WHEN @flgOptions & 1024 = 1024 THEN '' ELSE 'FAST,' END + ' TABLERESULTS, NO_INFOMSGS'
						ELSE
							SET @queryToRun='SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC [' + @dbName + N'].dbo.sp_executesql N''''IF OBJECT_ID(''''''''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'''''''') IS NOT NULL DBCC SHOWCONTIG (''''''''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'''''''', ''''''''' + @IndexName + ''''''''' ) WITH ' + CASE WHEN @flgOptions & 1024 = 1024 THEN '' ELSE 'FAST,' END + ' TABLERESULTS, NO_INFOMSGS'''''')x'

						IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [Level], [Pages], [Rows], [MinimumRecordSize], [MaximumRecordSize], [AverageRecordSize], [ForwardedRecords], [Extents], [ExtentSwitches], [AverageFreeBytes], [AveragePageDensity], [ScanDensity], [BestCount], [ActualCount], [LogicalFragmentation], [ExtentFragmentation])
								EXEC (@queryToRun)
					end
				ELSE
					begin
						SET @queryToRun=N'SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
													, ips.[object_id]
													, si.[name] as index_name
													, ips.[index_id]
													, ips.[avg_fragmentation_in_percent]
													, ips.[page_count]
													, ips.[record_count]
													, ips.[forwarded_record_count]
													, ips.[avg_record_size_in_bytes]
													, ips.[avg_page_space_used_in_percent]
													, ips.[ghost_record_count]
											FROM [' + @dbName + '].sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
															CASE WHEN @flgOptions & 1024 = 1024 THEN 'DETAILED' ELSE 'LIMITED' END 
													+ ''') ips
											INNER JOIN [' + @dbName + '].sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
											WHERE	si.[type] IN (' + @analyzeIndexType + N')
													AND si.[is_disabled]=0'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
						INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count])  
								EXEC (@queryToRun)
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[LogicalFragmentation]
				, doil.[page_count] = cifs.[Pages]
				, doil.[ghost_record_count] = cifs.[ghost_record_count]
				, doil.[forwarded_records_percentage] = CASE WHEN ISNULL(cifs.[Rows], 0) > 0 
															 THEN (CAST(cifs.[ForwardedRecords] AS decimal(29,2)) / CAST(cifs.[Rows]  AS decimal(29,2))) * 100
															 ELSE 0
														END
				, doil.[page_density_deviation] =  ABS(CASE WHEN ISNULL(cifs.[AverageRecordSize], 0) > 0
															THEN ((FLOOR(8060. / ISNULL(cifs.[AverageRecordSize], 0)) * ISNULL(cifs.[AverageRecordSize], 0)) / 8060.) * doil.[fill_factor] - cifs.[AveragePageDensity]
															ELSE 0
													   END)
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #CurrentIndexFragmentationStats cifs ON cifs.[ObjectId] = doil.[object_id] AND cifs.[IndexId] = doil.[index_id]
	end


--------------------------------------------------------------------------------------------------
-- 1	Defragmenting database tables indexes
--		All indexes with a fragmentation level between defrag and rebuild threshold will be reorganized
--------------------------------------------------------------------------------------------------		
IF ((@flgActions & 1 = 1) AND (@flgActions & 4 = 0)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Defragmenting database tables indexes (fragmentation between ' + CAST(@defragIndexThreshold AS [nvarchar]) + ' and ' + CAST(CAST(@rebuildIndexThreshold AS NUMERIC(6,2)) AS [nvarchar]) + ') and more than ' + CAST(@pageThreshold AS [nvarchar](4000)) + ' pages...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
		   													FROM	#databaseObjectsWithIndexList doil
															WHERE	doil.[page_count] >= @pageThreshold
																	AND doil.[index_type] <> 0 /* heap tables will be excluded */
																	AND	( 
																			(
																				 doil.[avg_fragmentation_in_percent] >= @defragIndexThreshold 
																			 AND doil.[avg_fragmentation_in_percent] < @rebuildIndexThreshold
																			)
																		OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																			(	  @flgOptions & 1024 = 1024 
																			 AND doil.[page_density_deviation] >= @defragIndexThreshold 
																			 AND doil.[page_density_deviation] < @rebuildIndexThreshold
																			)
																		OR
																			(	/* for very large tables, will performed reorganize instead of rebuild */
																				doil.[page_count] >= @rebuildIndexPageCountLimit
																				AND	( 
																						(
																							doil.[avg_fragmentation_in_percent] >= @rebuildIndexThreshold
																						)
																					OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																						(	  @flgOptions & 1024 = 1024 
																							AND doil.[page_density_deviation] >= @rebuildIndexThreshold
																						)
																					)
																			)
																		)
															ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun=N'[' + @CurrentTableSchema+ '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DECLARE crsIndexesToDegfragment CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[object_id], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor]
							   													FROM	#databaseObjectsWithIndexList doil
   																				WHERE	doil.[table_name] = @CurrentTableName
																						AND doil.[table_schema] = @CurrentTableSchema
																						AND doil.[page_count] >= @pageThreshold
																						AND doil.[index_type] <> 0 /* heap tables will be excluded */
																						AND	( 
																								(
																									 doil.[avg_fragmentation_in_percent] >= @defragIndexThreshold 
																								 AND doil.[avg_fragmentation_in_percent] < @rebuildIndexThreshold
																								)
																							OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																								(	  @flgOptions & 1024 = 1024 
																								 AND doil.[page_density_deviation] >= @defragIndexThreshold 
																								 AND doil.[page_density_deviation] < @rebuildIndexThreshold
																								)
																							OR
																								(	/* for very large tables, will performed reorganize instead of rebuild */
																									doil.[page_count] >= @rebuildIndexPageCountLimit
																									AND	( 
																											(
																												doil.[avg_fragmentation_in_percent] >= @rebuildIndexThreshold
																											)
																										OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																											(	  @flgOptions & 1024 = 1024 
																												AND doil.[page_density_deviation] >= @rebuildIndexThreshold
																											)
																										)
																								)
																							)																		
																				ORDER BY doil.[index_id]
				OPEN crsIndexesToDegfragment
				FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered' 
															WHEN 2 THEN 'Nonclustered' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
											END
   						SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @eventData='<index-fragmentation><detail>' + 
											'<database_name>' + @dbName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<index_name>' + @childObjectName + '</index_name>' + 
											'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
											'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
											'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
											'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
											'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
										'</detail></index-fragmentation>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - reorganize index',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						IF @serverVersionNum >= 9 
							begin
								SET @nestExecutionLevel = @executionLevel + 3

								EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName			= @sqlServerName
																		, @dbName					= @dbName
																		, @tableSchema				= @CurrentTableSchema
																		, @tableName				= @CurrentTableName
																		, @indexName				= @IndexName
																		, @indexID					= NULL
																		, @partitionNumber			= DEFAULT
																		, @flgAction				= 2		--reorganize
																		, @flgOptions				= @flgOptions
																		, @maxDOP					= @maxDOP
																		, @executionLevel			= @nestExecutionLevel
																		, @affectedDependentObjects = @affectedDependentObjects OUT
																		, @debugMode				= @debugMode
							end
						ELSE
							begin
								SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
								SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC INDEXDEFRAG (0, ' + RTRIM(@ObjectID) + ', ' + RTRIM(@IndexID) + ') WITH NO_INFOMSGS'
								IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 1
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpDatabaseOptimize',
																				@eventName		= 'database maintenance - reorganize index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode

							end
	   					FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
					end		
				CLOSE crsIndexesToDegfragment
				DEALLOCATE crsIndexesToDegfragment

				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
-- 2	- Rebuild heavy fragmented indexes
--		All indexes with a fragmentation level greater than rebuild threshold will be rebuild
--		If a clustered index needs to be rebuild, then all associated non-clustered indexes will be rebuild
--		http://technet.microsoft.com/en-us/library/ms189858.aspx
--------------------------------------------------------------------------------------------------
IF (@flgActions & 2 = 2) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database tables indexes (fragmentation between ' + CAST(@rebuildIndexThreshold AS [nvarchar]) + ' and 100) or small tables (no more than ' + CAST(@pageThreshold AS [nvarchar](4000)) + ' pages)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
																		
		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
		   													FROM	#databaseObjectsWithIndexList doil
															WHERE	    doil.[index_type] <> 0 /* heap tables will be excluded */
																	AND doil.[page_count] >= @pageThreshold
																	AND doil.[page_count] < @rebuildIndexPageCountLimit
																	AND	( 
																			(
																				doil.[avg_fragmentation_in_percent] >= @rebuildIndexThreshold
																			)
																		OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																			(	  @flgOptions & 1024 = 1024 
																			 AND doil.[page_density_deviation] >= @rebuildIndexThreshold
																			)
																		)
															ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @ClusteredRebuildNonClustered = 0

				DECLARE crsIndexesToRebuild CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor] 
				   							   								FROM	#databaseObjectsWithIndexList doil
		   																	WHERE	doil.[table_name] = @CurrentTableName
		   																			AND doil.[table_schema] = @CurrentTableSchema
																					AND doil.[page_count] >= @pageThreshold
																					AND doil.[page_count] < @rebuildIndexPageCountLimit
																					AND doil.[index_type] <> 0 /* heap tables will be excluded */
																					AND doil.[is_rebuilt] = 0
																					AND	( 
																							(
																								doil.[avg_fragmentation_in_percent] >= @rebuildIndexThreshold
																							)
																						OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																							(	  @flgOptions & 1024 = 1024 
																							 AND doil.[page_density_deviation] >= @rebuildIndexThreshold
																							)
																						)
																			ORDER BY doil.[index_id]

				OPEN crsIndexesToRebuild
				FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
				WHILE @@FETCH_STATUS = 0 AND @ClusteredRebuildNonClustered = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SELECT	@indexIsRebuilt = doil.[is_rebuilt]
						FROM	#databaseObjectsWithIndexList doil
						WHERE	doil.[table_schema] = @CurrentTableSchema 
		   						AND doil.[table_name] = @CurrentTableName
								AND doil.[index_id] = @IndexID

						IF @indexIsRebuilt = 0
							begin
								SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
																	WHEN 1 THEN 'Clustered' 
																	WHEN 2 THEN 'Nonclustered' 
																	WHEN 3 THEN 'XML'
																	WHEN 4 THEN 'Spatial' 
													END
		   						SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) +  ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
								SET @childObjectName = QUOTENAME(@IndexName)

								--------------------------------------------------------------------------------------------------
								--log index fragmentation information
								SET @eventData='<index-fragmentation><detail>' + 
													'<database_name>' + @dbName + '</database_name>' + 
													'<object_name>' + @objectName + '</object_name>'+ 
													'<index_name>' + @childObjectName + '</index_name>' + 
													'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
													'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
													'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
													'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
													'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
												'</detail></index-fragmentation>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@objectName		= @objectName,
																	@childObjectName= @childObjectName,
																	@module			= 'dbo.usp_mpDatabaseOptimize',
																	@eventName		= 'database maintenance - rebuilding index',
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */
																						
								--------------------------------------------------------------------------------------------------
								--4  - Rebuild all dependent indexes when rebuild primary indexes
								IF @IndexType=1 AND (@flgOptions & 4 = 4)
									begin
										SET @ClusteredRebuildNonClustered = 1									
									end

								IF @serverVersionNum >= 9
									begin
										SET @nestExecutionLevel = @executionLevel + 3

										EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName			= @sqlServerName
																				, @dbName					= @dbName
																				, @tableSchema				= @CurrentTableSchema
																				, @tableName				= @CurrentTableName
																				, @indexName				= @IndexName
																				, @indexID					= NULL
																				, @partitionNumber			= DEFAULT
																				, @flgAction				= 1		--rebuild
																				, @flgOptions				= @flgOptions
																				, @maxDOP					= @maxDOP
																				, @executionLevel			= @nestExecutionLevel
																				, @affectedDependentObjects = @affectedDependentObjects OUT
																				, @debugMode				= @debugMode

										--enable foreign key
										IF @IndexType=1
											begin
												 EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName	= @sqlServerName
																						  , @dbName			= @dbName
																						  , @tableSchema	= @CurrentTableSchema
																						  , @tableName		= @CurrentTableName
																						  , @constraintName = '%'
																						  , @flgAction		= 1
																						  , @flgOptions		= DEFAULT
																						  , @executionLevel	= @nestExecutionLevel
																						  , @debugMode		= @debugMode
											end
								
										IF @IndexType IN (1,3) AND @flgOptions & 4 = 4
											begin										
												--mark all dependent non-clustered/xml/spatial indexes as being rebuild
												UPDATE doil
													SET doil.[is_rebuilt]=1
												FROM	#databaseObjectsWithIndexList doil
	   											WHERE	doil.[table_name] = @CurrentTableName
	   													AND doil.[table_schema] = @CurrentTableSchema
														AND CHARINDEX(doil.[index_name], @affectedDependentObjects)<>0
											end
										end
								ELSE
									begin
										SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
										SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + ''', ''' + RTRIM(@IndexName) + ''') WITH NO_INFOMSGS'
										IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																						@dbName			= @dbName,
																						@objectName		= @objectName,
																						@childObjectName= @childObjectName,
																						@module			= 'dbo.usp_mpDatabaseOptimize',
																						@eventName		= 'database maintenance - rebuilding index',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @debugMode
									end
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID

	   					FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
					end		
				CLOSE crsIndexesToRebuild
				DEALLOCATE crsIndexesToRebuild

				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
-- 4	- Rebuild all indexes 
--------------------------------------------------------------------------------------------------
IF (@flgActions & 4 = 4) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database tables indexes  (all)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--minimizing the list of indexes to be rebuild:
		--4  - Rebuild all dependent indexes when rebuild primary indexes
		IF (@flgOptions & 4 = 4)
			begin
				SET @queryToRun=N'optimizing index list to be rebuild'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
					

				DECLARE crsClusteredIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT doil.[table_schema], doil.[table_name], doil.[index_name]
																			FROM	#databaseObjectsWithIndexList doil
																			WHERE	doil.[index_type]=1 --clustered index
																					AND doil.[page_count] >= @pageThreshold
																					AND EXISTS (
																								SELECT 1
																								FROM #databaseObjectsWithIndexList b
																								WHERE b.[table_schema] = doil.[table_schema]
																										AND b.[table_name] = doil.[table_name]
																										AND CHARINDEX(CAST(b.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
																										AND b.[index_type] NOT IN (0, 1)
																										AND b.[is_rebuilt] = 0	--not yet rebuilt
																								)
																			ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]
				OPEN crsClusteredIndexes
				FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName
				WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
	
						--mark indexes as rebuilt
						UPDATE doil	
							SET doil.[is_rebuilt]=1
						FROM #databaseObjectsWithIndexList doil
						WHERE   doil.[table_schema] = @CurrentTableSchema
								AND doil.[table_name] = @CurrentTableName
								AND CHARINDEX(CAST(doil.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
								AND doil.[index_type] NOT IN (0, 1)
										
						FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName
					end
				CLOSE crsClusteredIndexes
				DEALLOCATE crsClusteredIndexes						
			end


		--rebuilding indexes
		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[index_name], doil.[index_type], doil.[index_id], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[fill_factor] 
							   										FROM	#databaseObjectsWithIndexList doil
   																	WHERE	doil.[index_type] <> 0 /* heap tables will be excluded */
																			AND doil.[is_rebuilt]=0
																			AND doil.[page_count] >= @pageThreshold
																			AND	( 
																					(
																						doil.[avg_fragmentation_in_percent] >= @defragIndexThreshold
																					)
																				OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																					(	  @flgOptions & 1024 = 1024 
																						AND doil.[page_density_deviation] >= @defragIndexThreshold
																					)
																				)
																	ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]

		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @indexIsRebuilt = 0
				--for XML indexes, check if it was not previously rebuilt by a primary XML index
				IF @IndexType=3
					SELECT	@indexIsRebuilt = doil.[is_rebuilt]
					FROM	#databaseObjectsWithIndexList doil
					WHERE	doil.[table_name] = @CurrentTableName
		   					AND doil.[table_schema] = @CurrentTableSchema 
							AND doil.[index_id] = @IndexID

				IF @indexIsRebuilt = 0
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered' 
															WHEN 2 THEN 'Nonclustered' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
											END

						--analyze curent object
						SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   				SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						SET @eventData='<index-fragmentation><detail>' + 
											'<database_name>' + @dbName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<index_name>' + @childObjectName + '</index_name>' + 
											'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
											'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
											'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
											'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
											'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
										'</detail></index-fragmentation>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - rebuilding index',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						IF @serverVersionNum >= 9
							begin
								SET @nestExecutionLevel = @executionLevel + 3
								EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName			= @sqlServerName
																		, @dbName					= @dbName
																		, @tableSchema				= @CurrentTableSchema
																		, @tableName				= @CurrentTableName
																		, @indexName				= @IndexName
																		, @indexID					= NULL
																		, @partitionNumber			= DEFAULT
																		, @flgAction				= 1		--rebuild
																		, @flgOptions				= @flgOptions
																		, @maxDOP					= @maxDOP
																		, @executionLevel			= @nestExecutionLevel
																		, @affectedDependentObjects = @affectedDependentObjects OUT
																		, @debugMode				= @debugMode
							--enable foreign key
							IF @IndexType=1
								begin
									 EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName	= @sqlServerName
																			  , @dbName			= @dbName
																			  , @tableSchema	= @CurrentTableSchema
																			  , @tableName		= @CurrentTableName
																			  , @constraintName = '%'
																			  , @flgAction		= 1
																			  , @flgOptions		= DEFAULT
																			  , @executionLevel	= @nestExecutionLevel
																			  , @debugMode		= @debugMode
								end

							--mark secondary indexes as being rebuilt, if primary xml was rebuilt
							IF @IndexType = 3 AND @flgOptions & 4 = 4
								begin										
									--mark all dependent xml indexes as being rebuild
									UPDATE doil
										SET doil.[is_rebuilt]=1
									FROM	#databaseObjectsWithIndexList doil
	   								WHERE	doil.[table_name] = @CurrentTableName
	   										AND doil.[table_schema] = @CurrentTableSchema
											AND CHARINDEX(doil.[index_name], @affectedDependentObjects)<>0
											AND doil.[is_rebuilt] = 0
								end
							end
						ELSE
							begin
								SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
								SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + ''', ''' + RTRIM(@IndexName) + ''') WITH NO_INFOMSGS'
								IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
								SET @childObjectName = QUOTENAME(@IndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpDatabaseOptimize',
																				@eventName		= 'database maintenance - rebuilding index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil 
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4	/ 16 
--------------------------------------------------------------------------------------------------
IF @serverVersionNum >= 9 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
		begin
			SET @nestExecutionLevel = @executionLevel + 1
			EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@sqlServerName	= @sqlServerName,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestExecutionLevel, 
																@debugMode		= @debugMode
		end
	end



--------------------------------------------------------------------------------------------------
--cleanup of ghost records (sp_clean_db_free_space) (starting SQL Server 2005 SP3)
--exclude indexes which got rebuilt or reorganized, since ghost records were already cleaned
--------------------------------------------------------------------------------------------------
IF (@serverVersionNum >= 9.04035 AND @flgOptions & 65536 = 65536) AND (GETDATE() <= @stopTimeLimit)
	IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
			IF (
					SELECT SUM(doil.[ghost_record_count]) 
					FROM	#databaseObjectsWithIndexList doil
					WHERE	NOT (
									doil.[page_count] >= @pageThreshold
								AND doil.[index_type] <> 0 
								AND	( 
										(
											doil.[avg_fragmentation_in_percent] >= @defragIndexThreshold 
										)
									OR  
										(	@flgOptions & 1024 = 1024 
										AND doil.[page_density_deviation] >= @defragIndexThreshold 
										)
									)
								)
							AND doil.[is_rebuilt] = 0
				) >= @thresholdGhostRecords
				begin
					SET @queryToRun='sp_clean_db_free_space (ghost records cleanup)...'
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

					EXEC sp_clean_db_free_space @dbName
				end


--------------------------------------------------------------------------------------------------
--8  - Update statistics for all tables in database
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Update statistics for all tables (' + 
					CASE WHEN @statsSamplePercent<100 
							THEN 'sample ' + CAST(@statsSamplePercent AS [nvarchar]) + ' percent'
							ELSE 'fullscan'
					END + ')...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--remove tables with clustered indexes already rebuild
		SET @queryToRun=N'--	optimizing list (1)'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		DELETE dowsl
		FROM #databaseObjectsWithStatisticsList	dowsl
		WHERE EXISTS(
						SELECT 1
						FROM #databaseObjectsWithIndexList doil
						WHERE doil.[table_schema] = dowsl.[table_schema]
							AND doil.[table_name] = dowsl.[table_name]
							AND doil.[index_name] = dowsl.[stats_name]
							AND doil.[is_rebuilt] = 1
					)

		IF @flgOptions & 512 = 0
			begin
				--remove auto-created statistics
				SET @queryToRun=N'optimizing list (2)'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DELETE dowsl
				FROM #databaseObjectsWithStatisticsList	dowsl
				WHERE [auto_created]=1
			end

		DECLARE   @statsAutoCreated			[bit]
				, @tableRows				[bigint]
				, @statsModificationCounter	[bigint]
				, @lastUpdated				[datetime]
				, @percentChanges			[decimal](38,2)
				, @statsAge					[int]

		DECLARE crsTableList2 CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name], COUNT(*) AS [stats_count]
															FROM #databaseObjectsWithStatisticsList	
															GROUP BY [table_schema], [table_name]
															ORDER BY [table_name]
		OPEN crsTableList2
		FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @queryToRun=N'[' + @CurrentTableSchema+ '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @IndexID=1
				DECLARE crsTableStatsList CURSOR LOCAL FAST_FORWARD FOR	SELECT	  [stats_name], [auto_created], [rows], [modification_counter], [last_updated], [percent_changes]
																				, DATEDIFF(dd, [last_updated], GETDATE()) AS [stats_age]
																		FROM	#databaseObjectsWithStatisticsList	
																		WHERE	[table_schema] = @CurrentTableSchema
																				AND [table_name] = @CurrentTableName
																		ORDER BY [stats_name]
				OPEN crsTableStatsList
				FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=CAST(@IndexID AS [nvarchar](64)) + '/' + CAST(@statsCount AS [nvarchar](64)) + ' - [' + @IndexName+ '] / age = ' + CAST(@statsAge AS [varchar](32)) + ' days / rows = ' + CAST(@tableRows AS [varchar](32)) + ' / changes = ' + CAST(@statsModificationCounter AS [varchar](32))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log statistics information
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						SET @eventData='<statistics-health><detail>' + 
											'<database_name>' + @dbName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<stats_name>' + @childObjectName + '</stats_name>' + 
											'<auto_created>' + CAST(@statsAutoCreated AS [varchar](32)) + '</auto_created>' + 
											'<rows>' + CAST(@tableRows AS [varchar](32)) + '</rows>' + 
											'<modification_counter>' + CAST(@statsModificationCounter AS [varchar](32)) + '</modification_counter>' + 
											'<percent_changes>' + CAST(@percentChanges AS [varchar](32)) + '</percent_changes>' + 
											'<last_updated>' + CONVERT([nvarchar](20), @lastUpdated, 120) + '</last_updated>' + 
											'<age_days>' + CAST(@statsAge AS [varchar](32)) + '</age_days>' + 
										'</detail></statistics-health>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - update statistics',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
						SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL UPDATE STATISTICS [' + @CurrentTableSchema + '].[' + @CurrentTableName + '](' + dbo.ufn_mpObjectQuoteName(@IndexName) + ') WITH '
								
						IF @statsSamplePercent<100
							SET @queryToRun=@queryToRun + N'SAMPLE ' + CAST(@statsSamplePercent AS [nvarchar]) + ' PERCENT'
						ELSE
							SET @queryToRun=@queryToRun + N'FULLSCAN'

						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
						SET @nestedExecutionLevel = @executionLevel + 1

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@childObjectName= @childObjectName,
																		@module			= 'dbo.usp_mpDatabaseOptimize',
																		@eventName		= 'database maintenance - update statistics',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode

						SET @IndexID = @IndexID + 1
						FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge
					end
				CLOSE crsTableStatsList
				DEALLOCATE crsTableStatsList

				FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
			end
		CLOSE crsTableList2
		DEALLOCATE crsTableList2

		--128  - Create statistics on index columns only (default). Default behaviour is to not create statistics on all eligible columns
		IF @flgOptions & 128 = 128
			begin
				SET @queryToRun=N'Creating statistics for all tables / index columns only ...'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'sp_createstats @indexonly = ''indexonly'''

				--256  - Create statistics using default sample scan. Default behaviour is to create statistics using fullscan mode
				IF @flgOptions & 256 = 256
					SET @queryToRun = @queryToRun + N', @fullscan = ''NO'''
				ELSE
					SET @queryToRun = @queryToRun + N', @fullscan = ''fullscan'''

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				SET @nestedExecutionLevel = @executionLevel + 1

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@childObjectName= @childObjectName,
																@module			= 'dbo.usp_mpDatabaseOptimize',
																@eventName		= 'database maintenance - create statistics',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
			end
	end
	

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 	DROP TABLE #databaseObjectsWithIndexList

RETURN @errorCode
GO

RAISERROR('Create procedure: [dbo].[usp_mpDatabaseKillConnections]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpDatabaseKillConnections]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseKillConnections]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseKillConnections]
		@sqlServerName		[sysname],
		@dbName				[sysname] = NULL,
		@flgOptions			[int] = 2,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 05.03.2010
-- Module     : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@flgOptions		- 1 - normal connections
--						  2 - orphan connections
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE @queryToRun			[nvarchar](MAX),
		@serverToRun		[varchar](256),
		@databaseName		[sysname],
		@StartTime			[datetime],
		@MaxWaitTime		[int],
		@ConnectionsLeft	[int],
		@LocksLeft			[int],
		@ReturnValue		[int],
		@spid				[int],
		@uow				[uniqueidentifier]

DECLARE @DatabaseList	TABLE (	[dbname] [sysname] )

DECLARE @RowCount		TABLE (	[rowcount] [int] )

DECLARE @SessionDetails	TABLE (
								[spid]	[int]				NULL,
								[uow]	[uniqueidentifier]	NULL
							  )

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 1
 
		SET @queryToRun= 'Checking database active connections...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		SET @MaxWaitTime = 180 --3 minutes 
		SET @StartTime = GETUTCDATE()

		SET @serverToRun = N''
		SET @serverToRun = @serverToRun + N'[' + @sqlServerName + '].[master].[dbo].[sp_executesql]'

		------------------------------------------------------------------------------
		--get database list that will be analyzed
		------------------------------------------------------------------------------
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT [name]
										FROM (
												SELECT DISTINCT DB_NAME(ISNULL(resource_database_id,1)) [name]
												FROM [master].sys.dm_exec_connections	ec 
												LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
												LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																								and resource_type=''DATABASE'' 
												WHERE	ec.session_id <> @@SPID
														AND (   (ec.session_id <> -2 and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 1 = 1)
															 or (ec.session_id = -2  and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 2 = 2)
															)

												UNION		

												SELECT DB_NAME(rsc_dbid) [name]
												FROM [master].dbo.syslockinfo
												WHERE	req_spid=-2
														and req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
											 )x
										WHERE [name] LIKE ''' + CASE WHEN @dbName IS NULL THEN '%' ELSE @dbName END + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM @DatabaseList
		INSERT	INTO @DatabaseList([dbname])
				EXEC (@queryToRun)


		DECLARE crsDatabases CURSOR LOCAL FAST_FORWARD FOR	SELECT [dbname]
															FROM @DatabaseList
		OPEN crsDatabases
		FETCH NEXT FROM crsDatabases INTO @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'
							SELECT COUNT(*) [row_count]
							FROM [master].sys.dm_exec_connections	ec 
							LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
							LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																			and resource_type=''DATABASE''
							WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + @databaseName + '''
									AND ec.session_id <> @@SPID
									AND (   (ec.session_id<>-2 and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 1 = 1)
										 or (ec.session_id=-2  and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 2 = 2)
										)'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM @RowCount
				INSERT	INTO @RowCount([rowcount])
						EXEC (@queryToRun)
				
				SELECT @ConnectionsLeft = [rowcount] FROM @RowCount


				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'				
							SELECT COUNT(*) [row_count]
							FROM (
									SELECT DISTINCT req_transactionuow
									FROM [master].dbo.syslockinfo
									WHERE	rsc_dbid=DB_ID(''' + @databaseName + ''')
											AND req_spid=-2
											AND req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
								 )y'

				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM @RowCount
				INSERT	INTO @RowCount([rowcount])
						EXEC (@queryToRun)
				
				SELECT @LocksLeft = [rowcount] FROM @RowCount

				WHILE	(@ConnectionsLeft + @LocksLeft)>0 AND DATEDIFF(ss, @StartTime, GETUTCDATE())<=@MaxWaitTime
					begin
						IF @ConnectionsLeft>0
							begin
								DELETE FROM @SessionDetails
								
								IF @flgOptions & 1 = 1
									begin
										SET @queryToRun= 'Get connections for database: [' + @databaseName + ']'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
																			
										------------------------------------------------------------------------------
										--get "normal" connections to database
										------------------------------------------------------------------------------
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'
															SELECT ec.session_id
															FROM [master].sys.dm_exec_connections	ec 
															LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
															LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																											and resource_type=''DATABASE''
															WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + @databaseName + '''
																	AND ec.session_id <> @@SPID
																	AND ec.session_id<>-2'

										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										INSERT	INTO @SessionDetails([spid])
												EXEC (@queryToRun)
								end

								IF @flgOptions & 2 = 2
									begin									
										SET @queryToRun= 'Get orphan connections for database: [' + @databaseName + '] (sys.dm_tran_locks)'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

									
										------------------------------------------------------------------------------
										--get orphan connections to database
										------------------------------------------------------------------------------
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'
															SELECT tl.request_owner_guid
															FROM [master].sys.dm_exec_connections	ec 
															LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
															LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																											and resource_type=''DATABASE''
															WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + @databaseName + '''
																	AND ec.session_id=-2'

										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										INSERT	INTO @SessionDetails([uow])
												EXEC (@queryToRun)
									end
							end

						IF @LocksLeft>0
							begin
								IF @flgOptions & 2 = 2
									begin									
										SET @queryToRun= 'Get orphan connections for database: [' + @databaseName + '] (syslockinfo)'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										------------------------------------------------------------------------------
										--get orphan connections to database - locks
										------------------------------------------------------------------------------
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'
															SELECT req_transactionuow
															FROM (
																	SELECT DISTINCT req_transactionuow
																	FROM [master].dbo.syslockinfo
																	WHERE	rsc_dbid=DB_ID(''' + @databaseName + ''')
																			AND req_spid=-2
																			AND req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
																 )x'

										SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

										INSERT	INTO @SessionDetails([uow])
												EXEC (@queryToRun)
									end
							end

						IF @flgOptions & 1 = 1
							begin
								------------------------------------------------------------------------------
								--kill connections to database
								------------------------------------------------------------------------------
								SET @queryToRun= 'Kill connections for database: [' + @databaseName + ']'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								DECLARE crsSPIDList CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [spid] FROM @SessionDetails WHERE [spid] IS NOT NULL
								OPEN crsSPIDList
								FETCH NEXT FROM crsSPIDList INTO @spid
								WHILE @@FETCH_STATUS=0
									begin
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'KILL ' + CAST(@spid AS [nvarchar](max))
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
										
										BEGIN TRY
											EXEC @serverToRun @queryToRun
										END TRY
										BEGIN CATCH
											SET @queryToRun = ERROR_MESSAGE()
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
										END CATCH
										
										FETCH NEXT FROM crsSPIDList INTO @spid
									end
								CLOSE crsSPIDList
								DEALLOCATE crsSPIDList
							end
							
						IF @flgOptions & 2 = 2
							begin
								------------------------------------------------------------------------------
								--kill orphan connections to database
								------------------------------------------------------------------------------
								SET @queryToRun= 'Kill orphan connections for database: [' + @databaseName + ']'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								DECLARE crsUOWList CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [uow] FROM @SessionDetails WHERE [uow] IS NOT NULL
								OPEN crsUOWList
								FETCH NEXT FROM crsUOWList INTO @uow
								WHILE @@FETCH_STATUS=0
									begin
										SET @queryToRun = N''
										SET @queryToRun = @queryToRun + N'KILL ''' + CAST(@uow AS [nvarchar](max)) + ''''
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
										
										BEGIN TRY
											EXEC @serverToRun @queryToRun
										END TRY
										BEGIN CATCH
											SET @queryToRun = ERROR_MESSAGE()
											EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
										END CATCH
										
										FETCH NEXT FROM crsUOWList INTO @uow
									end
								CLOSE crsUOWList
								DEALLOCATE crsUOWList
							end						

						
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'
									SELECT COUNT(*) [row_count]
									FROM [master].sys.dm_exec_connections	ec 
									LEFT JOIN [master].sys.dm_exec_requests	er on	ec.connection_id = er.connection_id 
									LEFT JOIN [master].sys.dm_tran_locks	tl on	tl.request_session_id = ec.session_id 
																					and resource_type=''DATABASE''
									WHERE	DB_NAME(ISNULL(resource_database_id,1)) = ''' + @databaseName + '''
											AND ec.session_id <> @@SPID
											AND (   (ec.session_id<>-2 and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 1 = 1)
												 or (ec.session_id=-2  and ' + CAST(@flgOptions AS [nvarchar](max)) + N' & 2 = 2)
												)'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @RowCount
						INSERT	INTO @RowCount([rowcount])
								EXEC (@queryToRun)
						
						SELECT @ConnectionsLeft = [rowcount] FROM @RowCount


						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'				
									SELECT COUNT(*) [row_count]
									FROM (
											SELECT DISTINCT req_transactionuow
											FROM [master].dbo.syslockinfo
											WHERE	rsc_dbid=DB_ID(''' + @databaseName + ''')
													AND req_spid=-2
													AND req_transactionuow <> ''00000000-0000-0000-0000-000000000000''
										 )y'

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @RowCount
						INSERT	INTO @RowCount([rowcount])
								EXEC (@queryToRun)
						
						SELECT @LocksLeft = [rowcount] FROM @RowCount
					end

				--check if all connections have been killed
				IF @ConnectionsLeft>0 
					begin 
						SET @queryToRun= 'Cannot kill all connections to database [' +  @databaseName + ']. There are ' + CAST(@ConnectionsLeft AS VARCHAR) + ' active connection(s) left. Operation failed.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

					end
				IF @LocksLeft>0 
					begin 
						SET @queryToRun= 'Cannot kill all connections to database [' +  @databaseName + ']. There are ' + CAST(@LocksLeft AS VARCHAR) + ' active lock(s) left. Operation failed.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
					end

				FETCH NEXT FROM crsDatabases INTO @databaseName
			end
		CLOSE crsDatabases
		DEALLOCATE crsDatabases

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'				
					SELECT COUNT(*) [row_count]
					FROM (
							SELECT DISTINCT req_transactionuow
							FROM [master].dbo.syslockinfo
							WHERE	rsc_dbid=DB_ID(''' + @databaseName + ''')
									AND req_spid=-2
									AND req_transactionuow = ''00000000-0000-0000-0000-000000000000''
						 )y'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM @RowCount
		INSERT	INTO @RowCount([rowcount])
				EXEC (@queryToRun)
		
		SELECT @LocksLeft = [rowcount] FROM @RowCount
			
		IF @LocksLeft>0
			EXEC [dbo].[usp_logPrintMessage] @customMessage = 'You need to restart the MSDTC service. There are orphan {00000000-0000-0000-0000-000000000000 transactions} left. Operation failed.', @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
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
		@maxDOP					[smallint]	=	 1,
		@skipObjectsList		[nvarchar](1024) = NULL,
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
--		@skipObjectsList	- comma separated list of the objects (tables) to be excluded from maintenance.
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
DECLARE @agName				[sysname],
		@agInstanceRoleDesc	[sysname],
		@agStopLimit		[int],
		@actionType			[sysname],
		@actionName			[sysname]

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

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL
	begin
		EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																			@dbName				= @dbName,
																			@actionName			= @actionName,
																			@actionType			= @actionType,
																			@flgActions			= @flgActions,
																			@flgOptions			= @flgOptions OUTPUT,
																			@agName				= @agName OUTPUT,
																			@agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT,
																			@executionLevel		= @executionLevel,
																			@debugMode			= @debugMode
	end
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
								[table_name]	[sysname]	NULL,
								[type]			[sysname]	NULL
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
			SET @queryToRun = @queryToRun + N'SELECT DISTINCT ob.[table_schema], ob.[table_name], ob.[type]
FROM (
		SELECT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name], obj.[type]
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

		SELECT DISTINCT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name], obj.[type]
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
			SET @queryToRun = @queryToRun + N'SELECT ob.[table_schema], ob.[table_name], ob.[type]
FROM (
		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name], obj.[type]
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

		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name], obj.[type]
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

		SET @queryToRun = @queryToRun + CASE WHEN @skipObjectsList IS NOT NULL  
											 THEN N'	WHERE ob.[table_name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
											 ELSE N'' 
										END

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #databaseTableList
		INSERT	INTO #databaseTableList([table_schema], [table_name], [type])
				EXEC (@queryToRun)

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		DELETE dtl
		FROM #databaseTableList dtl
		INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																AND dtl.[table_name] = osl.[object_name]
		WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]
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
					SET @queryToRun = N'SELECT MAX([VALUE]) AS [Value]
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

		IF @serverVersionNum > = 12.05000 /* MAXDOP: applies to: SQL Server 2014 SP2 onwards */
			SET @queryToRun = @queryToRun + ', MAXDOP=' + CAST(@maxDOP AS [nvarchar])

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= 'master',
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
				
				IF @compatibilityLevel >= 100 AND @flgOptions & 2 = 0
					SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

				IF @serverVersionNum > = 12.05000 /* MAXDOP: applies to: SQL Server 2014 SP2 onwards */
					SET @queryToRun = @queryToRun + ', MAXDOP=' + CAST(@maxDOP AS [nvarchar])

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
														@dbName			= 'master',
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
														@dbName			= 'master',
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

				SET @queryToRun = @queryToRun + CASE WHEN @skipObjectsList IS NOT NULL  
													 THEN N'	AND obj.[name] NOT IN (SELECT [value] FROM [' + DB_NAME() + N'].[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
													 ELSE N'' 
												END
				
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #databaseTableListIdent
				INSERT	INTO #databaseTableListIdent([table_schema], [table_name])
						EXEC (@queryToRun)

				--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
				DELETE dtl
				FROM #databaseTableListIdent dtl
				INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] 
																		AND dtl.[table_name] = osl.[object_name]
				WHERE @flgActions & osl.[flg_actions] = osl.[flg_actions]

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
															WHERE	(@serverVersionNum >= 9)
																 OR (@serverVersionNum < 9 AND [type] NOT IN ('S'))
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

RAISERROR('Create procedure: [dbo].[usp_mpCheckAvailabilityGroupLimitations]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpCheckAvailabilityGroupLimitations]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpCheckAvailabilityGroupLimitations]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpCheckAvailabilityGroupLimitations]
		@sqlServerName		[sysname] = @@SERVERNAME,
		@dbName				[sysname],
		@actionName			[sysname],
		@actionType			[sysname],
		@flgActions			[smallint]	= 0,
		@flgOptions			[int]	  OUTPUT,
		@agName				[sysname] OUTPUT,
		@agInstanceRoleDesc	[sysname] OUTPUT,
		@executionLevel		[tinyint]	= 0,
		@debugMode			[bit]		= 0
/* WITH ENCRYPTION */
AS

-----------------------------------------------------------------------------------------
SET NOCOUNT ON

DECLARE		@queryToRun  					[nvarchar](2048),
			@queryParameters				[nvarchar](512),
			@nestedExecutionLevel			[tinyint],
			@eventData						[varchar](8000)

-----------------------------------------------------------------------------------------
SET @nestedExecutionLevel = @executionLevel + 1

--------------------------------------------------------------------------------------------------
DECLARE @clusterName				 [sysname],		
		@agSynchronizationState		 [sysname],
		@agPreferredBackupReplica	 [bit],
		@agAutomatedBackupPreference [tinyint],
		@agReadableSecondary		 [sysname]

SET @agName = NULL

/* get cluster name */
SET @queryToRun = N'SELECT [cluster_name] FROM sys.dm_hadr_cluster'
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT @clusterName = [cluster_name]
					FROM (' + @queryToRun + N')inq'

SET @queryParameters = N'@clusterName [sysname] OUTPUT'
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @queryToRun, @queryParameters, @clusterName = @clusterName OUTPUT


/* availability group configuration */
SET @queryToRun = N'
			SELECT    ag.[name]
					, ars.[role_desc]
					, ag.[automated_backup_preference]
					, ar.[secondary_role_allow_connections_desc]
			FROM sys.availability_replicas ar
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
			INNER JOIN sys.availability_groups ag ON ag.[group_id]=ar.[group_id]
			INNER JOIN sys.dm_hadr_availability_replica_cluster_nodes arcn ON arcn.[group_name]=ag.[name] AND arcn.[replica_server_name]=ar.[replica_server_name]
			INNER JOIN sys.dm_hadr_database_replica_states hdrs ON ar.[replica_id]=hdrs.[replica_id]
			INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
			WHERE arcn.[replica_server_name] = ''' + @sqlServerName + N'''
				  AND adc.[database_name] = ''' + @dbName + N''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT    @agName = [name]
							, @agInstanceRoleDesc = [role_desc]
							, @agAutomatedBackupPreference = [automated_backup_preference]
							, @agReadableSecondary = [secondary_role_allow_connections_desc]
					FROM (' + @queryToRun + N')inq'
SET @queryParameters = N'@agName [sysname] OUTPUT, @agInstanceRoleDesc [sysname] OUTPUT, @agAutomatedBackupPreference [tinyint] OUTPUT, @agReadableSecondary [sysname] OUTPUT'
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @queryToRun, @queryParameters, @agName = @agName OUTPUT
												, @agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT
												, @agAutomatedBackupPreference = @agAutomatedBackupPreference OUTPUT
												, @agReadableSecondary = @agReadableSecondary OUTPUT
	
IF @agName IS NOT NULL AND @clusterName IS NOT NULL
	begin
		/* availability group synchronization status */
		SET @queryToRun = N'
				SELECT    hdrs.[synchronization_state_desc]
						, sys.fn_hadr_backup_is_preferred_replica(''' + @dbName + N''') AS [backup_is_preferred_replica]
				FROM sys.dm_hadr_database_replica_states hdrs
				INNER JOIN sys.availability_replicas ar ON ar.[replica_id]=hdrs.[replica_id]
				INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
				INNER JOIN sys.dm_hadr_availability_replica_cluster_states rcs ON rcs.[replica_id]=ar.[replica_id] AND rcs.[group_id]=hdrs.[group_id]
				INNER JOIN sys.databases sd ON sd.name = adc.database_name
				WHERE	ar.[replica_server_name] = ''' + @sqlServerName + N'''
						AND adc.[database_name] = ''' + @dbName + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

		SET @queryToRun = N'SELECT    @agSynchronizationState = [synchronization_state_desc]
									, @agPreferredBackupReplica = [backup_is_preferred_replica]
							FROM (' + @queryToRun + N')inq'

		SET @queryParameters = N'@agSynchronizationState [sysname] OUTPUT, @agPreferredBackupReplica [bit] OUTPUT'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC sp_executesql @queryToRun, @queryParameters, @agSynchronizationState = @agSynchronizationState OUTPUT
														, @agPreferredBackupReplica = @agPreferredBackupReplica OUTPUT

		SET @agSynchronizationState = ISNULL(@agSynchronizationState, '')
		SET @agInstanceRoleDesc = ISNULL(@agInstanceRoleDesc, '')
	
		IF ISNULL(@agSynchronizationState, '')<>''
			begin
				IF UPPER(@agInstanceRoleDesc) NOT IN ('PRIMARY', 'SECONDARY')
					begin
						SET @queryToRun=N'Availability Group: Current role state [ ' + @agInstanceRoleDesc + N'] does not permit the "' + @actionName + '" operation.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						SET @eventData='<alert><detail>' + 
										'<severity>critical</severity>' + 
										'<instance_name>' + @sqlServerName + '</instance_name>' + 
										'<cluster_name>' + @clusterName + '</instance_name>' + 
										'<availability_group_name>' + @agName + '</instance_name>' + 
										'<action_name>' + @actionName + '</action_name>' + 
										'<action_type>' + @actionType + '</action_type>' + 
										'<message>' + @queryToRun + '</message' + 
										'<event_date_utc>' + CONVERT([varchar](24), GETUTCDATE(), 121) + '</event_date_utc>' + 
										'</detail></alert>'

						EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= DEFAULT,
																		@sqlServerName			= @sqlServerName,
																		@dbName					= @dbName,
																		@objectName				= NULL,
																		@childObjectName		= NULL,
																		@module					= 'dbo.usp_mpDatabaseBackup',
																		@eventName				= 'database backup',
																		@parameters				= NULL,	
																		@eventMessage			= @eventData,
																		@dbMailProfileName		= NULL,
																		@recipientsList			= NULL,
																		@eventType				= 6,	/* 6 - alert-custom */
																		@additionalOption		= 0

						RETURN 1
					end

				/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
				/* database backup - allowed actions on a secondary replica */
				IF @actionName = 'database backup' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
					begin	
						/* if automated_backup_preference is 0 (primary), Backups should always occur on the primary replica */
						IF @agAutomatedBackupPreference = 0
							begin
								SET @queryToRun=N'Availability Group: Current setting for Backup Preferences do not permit backups on a seconday replica (0: Primary).'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end

						/* if instance is preferred replica */
						IF @agPreferredBackupReplica = 0
							begin
								SET @queryToRun=N'Availability Group: Current instance [ ' + @sqlServerName + N'] is not a backup preferred replica for the database [' + @dbName + N'].'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end

						/* copy-only full backups are allowed */
						IF @flgActions & 1 = 1 AND @flgOptions & 4 = 0
							begin
								/* on alwayson availability groups, for secondary replicas, force copy-only backups */
								IF @flgOptions & 1024 = 1024
									begin
										SET @queryToRun='Server is part of an Availability Group as a secondary replica. Forcing copy-only full backups.'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
										SET @flgOptions = @flgOptions + 4
									end
								ELSE
									begin
										SET @queryToRun=N'Availability Group: Only copy-only full backups are allowed on a secondary replica.'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @eventData='<skipaction><detail>' + 
															'<name>' + @actionName + '</name>' + 
															'<type>' + @actionType + '</type>' + 
															'<affected_object>' + @dbName + '</affected_object>' + 
															'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
															'<reason>' + @queryToRun + '</reason>' + 
														'</detail></skipaction>'

										EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																			@dbName			= @dbName,
																			@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																			@eventName		= @actionName,
																			@eventMessage	= @eventData,
																			@eventType		= 0 /* info */

										RETURN 1
									end
							end

						/* Differential backups are not supported on secondary replicas. */
						IF @flgActions & 2 = 2
							begin
								SET @queryToRun=N'Availability Group: Differential backups are not supported on secondary replicas.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end
				
						/* BACKUP LOG supports only regular log backups (the COPY_ONLY option is not supported for log backups on secondary replicas).*/
						IF @flgActions & 4 = 4 AND @flgOptions & 4 = 4
							begin
								SET @queryToRun=N'Availability Group: BACKUP LOG supports only regular log backups (the COPY_ONLY option is not supported for log backups on secondary replicas).'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end

						/* To back up a secondary database, a secondary replica must be able to communicate with the primary replica and must be SYNCHRONIZED or SYNCHRONIZING. */
						IF UPPER(@agSynchronizationState) NOT IN ('SYNCHRONIZED', 'SYNCHRONIZING')
							begin
								SET @queryToRun=N'Availability Group: Current secondary replica state [ ' + @agSynchronizationState + N'] does not permit the backup operation.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end
					end

				/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
				/* database backup - allowed actions on a primary replica */
				IF @actionName = 'database backup' AND UPPER(@agInstanceRoleDesc) = 'PRIMARY'
					begin	
						/* if automated_backup_preference is 1 (secondary only), backups logs must be performed on secondary */
						IF @agAutomatedBackupPreference = 1 AND @flgActions & 4 = 4 /* log */
							begin
								SET @queryToRun=N'Availability Group: Current setting for Backup Preferences do not permit LOG backups on a primary replica (1: Secondary only).'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @eventData='<skipaction><detail>' + 
													'<name>' + @actionName + '</name>' + 
													'<type>' + @actionType + '</type>' + 
													'<affected_object>' + @dbName + '</affected_object>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>' + @queryToRun + '</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																	@eventName		= @actionName,
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								RETURN 1
							end

						/* if automated_backup_preference is 2 (prefered secondary): performing backups on the primary replica is acceptable if no secondary replica is available for backup operations */
						/* full and differential backups are allowed only on primary / restrictions apply for a secondary replica */
						IF @agAutomatedBackupPreference = 2 AND @flgActions & 4 = 4 /* log */
							begin
								/* check if there are secondary replicas available to perform the log backup */
								DECLARE @agAvailableSecondaryReplicas [smallint]

								SET @queryToRun = N'SELECT @agAvailableSecondaryReplicas = COUNT(*)
													FROM sys.dm_hadr_database_replica_states hdrs
													INNER JOIN sys.availability_replicas ar ON ar.[replica_id]=hdrs.[replica_id]
													INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
													INNER JOIN sys.dm_hadr_availability_replica_cluster_states rcs ON rcs.[replica_id]=ar.[replica_id] AND rcs.[group_id]=hdrs.[group_id]
													INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
													INNER JOIN sys.databases sd ON sd.name = adc.database_name
													WHERE	adc.[database_name] = ''' + @dbName + N'''
															AND hdrs.[synchronization_state_desc] IN (''SYNCHRONIZED'', ''SYNCHRONIZING'')
															AND ars.[role_desc] = ''SECONDARY'''

								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

								SET @queryParameters = N'@agAvailableSecondaryReplicas [smallint] OUTPUT'
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								EXEC sp_executesql @queryToRun, @queryParameters, @agAvailableSecondaryReplicas = @agAvailableSecondaryReplicas OUTPUT

								IF @agAvailableSecondaryReplicas > 0
									begin
										SET @queryToRun=N'Availability Group: Current setting for Backup Preferences indicate that LOG backups should be perform on a secondary (current available) replica.'
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @eventData='<skipaction><detail>' + 
															'<name>' + @actionName + '</name>' + 
															'<type>' + @actionType + '</type>' + 
															'<affected_object>' + @dbName + '</affected_object>' + 
															'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
															'<reason>' + @queryToRun + '</reason>' + 
														'</detail></skipaction>'

										EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																			@dbName			= @dbName,
																			@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
																			@eventName		= @actionName,
																			@eventMessage	= @eventData,
																			@eventType		= 0 /* info */

										RETURN 1
									end
							end
					end

				/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
				/* database maintenance - allowed actions on a secondary replica */
				IF @actionName = 'database maintenance' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
					begin								
						SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
				/* database consistency check - allowed actions on a secondary replica */
				IF @actionName = 'database consistency check' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY' AND @agReadableSecondary='NO' AND (@flgActions & 2 = 2 OR @flgActions & 16 = 16)
					begin								
						SET @queryToRun=N'Availability Group: Operation is not supported on a non-readable secondary replica.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1
					end

				/*-------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
				/* database skrink - allowed actions on a secondary replica */
				IF @actionName = 'database shrink' AND UPPER(@agInstanceRoleDesc) = 'SECONDARY'
					begin								
						SET @queryToRun=N'Availability Group: Operation is not supported on a secondary replica.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @eventData='<skipaction><detail>' + 
											'<name>' + @actionName + '</name>' + 
											'<type>' + @actionType + '</type>' + 
											'<affected_object>' + @dbName + '</affected_object>' + 
											'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
											'<reason>' + @queryToRun + '</reason>' + 
										'</detail></skipaction>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
															@dbName			= @dbName,
															@module			= 'dbo.usp_mpCheckAvailabilityGroupLimitations',
															@eventName		= @actionName,
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						RETURN 1

					end

				SET @agName = @clusterName + '$' + @agName
			end
		ELSE
			SET @agName=NULL
	end

RETURN 0
GO

RAISERROR('Create procedure: [dbo].[usp_mpCheckAndRevertInternalActions]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpCheckAndRevertInternalActions]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpCheckAndRevertInternalActions]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpCheckAndRevertInternalActions]
		@sqlServerName			[sysname],
		@flgOptions				[int]	= 12941,
		@executionLevel			[tinyint]	=     0,
		@debugMode				[bit]		=     0
/* WITH ENCRYPTION */
AS

DECLARE   @crtDatabaseName			[sysname]
		, @crtSchemaName			[sysname]
		, @crtObjectName			[sysname]
		, @crtChildObjectName		[sysname]
		, @queryToRun				[nvarchar](1024)
		, @nestExecutionLevel		[tinyint]
		, @affectedDependentObjects	[nvarchar](max)

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.02.2015
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@flgOptions		 1  - Compact large objects (LOB) (default)
--						 2  - Rebuild index by create with drop existing on (default)
--						 4  - Rebuild all non-clustered indexes when rebuild clustered indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding clustered indexes
--						32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					    64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--		@debugMode		 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@flgOptions				= DEFAULT,
														@debugMode				= DEFAULT
*/

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

--reset configuration value
UPDATE [dbo].[appConfigurations]
	SET [value]='-1'
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

-----------------------------------------------------------------------------------------
SET @queryToRun=N'Rebuilding previously disabled indexes...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

SET @nestExecutionLevel = @executionLevel + 1
DECLARE crslogInternalAction CURSOR LOCAL FAST_FORWARD FOR	SELECT	[database_name], [schema_name], [object_name], [child_object_name]
															FROM	[maintenance-plan].[logInternalAction]
															WHERE	[name] = 'index-made-disable'
																	AND [server_name] = @sqlServerName
OPEN crslogInternalAction
FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpAlterTableIndexes]		@sqlServerName				= @sqlServerName,
													@dbName						= @crtDatabaseName,
													@tableSchema				= @crtSchemaName,
													@tableName					= @crtObjectName,
													@indexName					= @crtChildObjectName,
													@indexID					= NULL,
													@partitionNumber			= DEFAULT,
													@flgAction					= 1,
													@flgOptions					= @flgOptions,
													@maxDOP						= 1,
													@executionLevel				= @nestExecutionLevel,
													@affectedDependentObjects	= @affectedDependentObjects OUT,
													@debugMode					= @debugMode

		FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
	end
CLOSE crslogInternalAction
DEALLOCATE crslogInternalAction


-----------------------------------------------------------------------------------------
SET @queryToRun=N'Rebuilding previously disabled foreign key constraints...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DECLARE crslogInternalAction CURSOR LOCAL FAST_FORWARD FOR	SELECT	[database_name], [schema_name], [object_name], [child_object_name]
															FROM	[maintenance-plan].[logInternalAction]
															WHERE	[name] = 'foreign-key-made-disable'
																	AND [server_name] = @sqlServerName
OPEN crslogInternalAction
FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName		= @sqlServerName,
													@dbName				= @crtDatabaseName,
													@tableSchema		= @crtSchemaName,
													@tableName			= @crtObjectName,
													@constraintName		= @crtChildObjectName,
													@flgAction			= 1,
													@flgOptions			= @flgOptions,
													@executionLevel		= @nestExecutionLevel,
													@debugMode			= @debugMode
		FETCH NEXT FROM crslogInternalAction INTO @crtDatabaseName, @crtSchemaName, @crtObjectName, @crtChildObjectName
	end
CLOSE crslogInternalAction
DEALLOCATE crslogInternalAction


-----------------------------------------------------------------------------------------
--restore original configuration value
-----------------------------------------------------------------------------------------
UPDATE [dbo].[appConfigurations]
	SET [value]=@queryLockTimeOut
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

GO

RAISERROR('Create procedure: [dbo].[usp_mpAlterTableTriggers]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableTriggers]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableTriggers]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableTriggers]
		@sqlServerName		[sysname],
		@dbName				[sysname],
		@tableSchema		[sysname] = '%', 
		@tableName			[sysname] = '%',
		@triggerName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2048,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2009
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@flgAction:		 1	- Enable Triggers (default)
--						 0	- Disable Triggers
--		@flgOptions:	 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@objectName				[varchar](512),
			@childObjectName		[sysname],
			@crtTableSchema			[sysname],
			@crtTableName 			[sysname],
			@crtTriggerName			[sysname],
			@errorCode				[int],
			@tmpFlgOptions			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema]	[sysname],
					[table_name]	[sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME FROM [' + @dbName + N'].INFORMATION_SCHEMA.TABLES
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @tableName + N''' 
								AND TABLE_SCHEMA LIKE ''' + @tableSchema + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterTriggers') IS NOT NULL DROP TABLE #tmpTableToAlterTriggers
				CREATE TABLE #tmpTableToAlterTriggers 
							(
								[TriggerName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT	[table_schema], [table_name]
																	FROM	#tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName

				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1  THEN 'Enable'
																ELSE 'Disable'
										END + ' triggers for: [' + @crtTableSchema + N'].[' + @crtTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable triggers, will get only enabled triggers
						--if current action is to enable triggers, will get only disabled triggers
						SET @queryToRun=N'SELECT DISTINCT st.[name]
									FROM [' + @dbName + '].[sys].[triggers] st
									INNER JOIN [' + @dbName + '].[sys].[objects] so ON so.[object_id] = st.[parent_id] 
									INNER JOIN [' + @dbName + '].[sys].[schemas] sch ON sch.[schema_id] = so.[schema_id] 
									WHERE	so.[name]=''' + @crtTableName + '''
											AND sch.[name] = ''' + @crtTableSchema + '''
											AND st.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
											AND st.[is_ms_shipped] = 0
											AND st.[name] LIKE ''' + @triggerName + ''''
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						TRUNCATE TABLE #tmpTableToAlterTriggers
						INSERT	INTO #tmpTableToAlterTriggers([TriggerName])
								EXEC (@queryToRun)
								
						DECLARE crsTableToAlterTriggers CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TriggerName]
																						FROM #tmpTableToAlterTriggers
																						ORDER BY [TriggerName]
						OPEN crsTableToAlterTriggers
						FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= @crtTriggerName
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun=N'ALTER TABLE [' + @dbName + N'].[' + @crtTableSchema + N'].[' + @crtTableName + '] ' + 
													CASE WHEN @flgAction=1  THEN N'ENABLE'
																			ELSE N'DISABLE'
													END + N' TRIGGER [' + @crtTriggerName + ']'
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

								--
								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtTriggerName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableTriggers',
																				@eventName		= 'database maintenance - alter triggers',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode

								FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
							end
						CLOSE crsTableToAlterTriggers
						DEALLOCATE crsTableToAlterTriggers
											
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
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

RAISERROR('Create procedure: [dbo].[usp_mpAlterTableRebuildHeap]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableRebuildHeap]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableRebuildHeap]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableRebuildHeap]
		@sqlServerName		[sysname],
		@dbName				[sysname],
		@tableSchema		[sysname],
		@tableName			[sysname],
		@flgActions			[smallint] = 1,
		@flgOptions			[int] = 14360, --8192 + 4096 + 2048 + 16 + 8
		@maxDOP				[smallint] = 1,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2015
-- Module			 : Database Maintenance Scripts
-- ============================================================================
-- Change Date: 2015.03.04 / Andrei STEFAN
-- Description: heap tables with disabled unique indexes won't be rebuild
-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed.
--		@flgActions		- 1 - ALTER TABLE REBUILD (2k8+). If lower version is detected or error catched, will run CREATE CLUSTERED INDEX / DROP INDEX
--						- 2 - Rebuild table: copy records to a temp table, delete records from source, insert back records from source, rebuild non-clustered indexes
--		@flgOptions		 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding indexes (default)
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  4096  - rebuild/reorganize indexes/tables using ONLINE=ON, if applicable (default)
--					  8192  - when rebuilding heaps, disable/enable table triggers (default)
--					 16384  - for versions below 2008, do heap rebuild using temporary clustered index
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON

DECLARE		@queryToRun					[nvarchar](max),
			@objectName					[nvarchar](512),
			@sqlScriptOnline			[nvarchar](512),
			@CopyTableName				[sysname],
			@crtSchemaName				[sysname], 
			@crtTableName				[sysname], 
			@crtRecordCount				[int],
			@flgCopyMade				[bit],
			@flgErrorsOccured			[bit], 
			@nestExecutionLevel			[tinyint],
			@guid						[nvarchar](40),
			@affectedDependentObjects	[nvarchar](max),
			@flgOptionsNested			[int]


DECLARE		@flgRaiseErrorAndStop		[bit]
		  , @errorCode					[int]
		  

-----------------------------------------------------------------------------------------
DECLARE @tableGetRowCount TABLE	
		(
			[record_count]			[bigint]	NULL
		)

IF object_id('tempdb..#heapTableList') IS NOT NULL 
	DROP TABLE #heapTableList

CREATE TABLE #heapTableList		(
									[schema_name]			[sysname]	NULL,
									[table_name]			[sysname]	NULL,
									[record_count]			[bigint]	NULL
								)


SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 1

		---------------------------------------------------------------------------------------------
		--get configuration values
		---------------------------------------------------------------------------------------------
		DECLARE @queryLockTimeOut [int]
		SELECT	@queryLockTimeOut=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name]='Default lock timeout (ms)'
				AND [module] = 'common'
		
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

		---------------------------------------------------------------------------------------------
		--get current index/heap properties, filtering only the ones not empty
		--heap tables with disabled unique indexes will be excluded: rebuild means also index rebuild, and unique indexes may enable unwanted constraints
		SET @tableName = REPLACE(@tableName, '''', '''''')
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'	SELECT    sch.[name] AS [schema_name]
													, so.[name]  AS [table_name]
													, rc.[record_count]
											FROM [' + @dbName + '].[sys].[objects] so WITH (READPAST)
											INNER JOIN [' + @dbName + '].[sys].[schemas] sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id] 
											INNER JOIN [' + @dbName + '].[sys].[indexes] si WITH (READPAST) ON si.[object_id] = so.[object_id] 
											INNER  JOIN 
													(
														SELECT ps.object_id,
																SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [record_count]
														FROM [' + @dbName + '].[sys].[dm_db_partition_stats] ps WITH (READPAST)
														GROUP BY ps.object_id		
													)rc ON rc.[object_id] = so.[object_id] 
											WHERE   so.[name] LIKE ''' + @tableName + '''
												AND sch.[name] LIKE ''' + @tableSchema + '''
												AND so.[is_ms_shipped] = 0
												AND si.[index_id] = 0
												AND rc.[record_count]<>0
												AND NOT EXISTS(
																SELECT *
																FROM [' + @dbName + '].sys.indexes si_unq
																WHERE si_unq.[object_id] = so.[object_id] 
																		AND si_unq.[is_disabled]=1
																		AND si_unq.[is_unique]=1
															  )'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #heapTableList
		INSERT INTO #heapTableList ([schema_name], [table_name], [record_count])
			EXEC (@queryToRun)


		---------------------------------------------------------------------------------------------
		DECLARE crsTableListToRebuild CURSOR LOCAL FAST_FORWARD FOR	SELECT [schema_name], [table_name], [record_count] 
																	FROM #heapTableList
																	ORDER BY [schema_name], [table_name]
 		OPEN crsTableListToRebuild
		FETCH NEXT FROM crsTableListToRebuild INTO @crtSchemaName, @crtTableName, @crtRecordCount
		WHILE @@FETCH_STATUS=0
			begin
				SET @objectName = '[' + @crtSchemaName + '].[' + @crtTableName + ']'
				SET @queryToRun=N'Rebuilding heap ON ' + @objectName
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			
				SET @flgErrorsOccured=0
				
				IF @flgActions=1
					begin
						IF @serverVersionNum >= 10
							begin
								SET @queryToRun= 'Running ALTER TABLE REBUILD...'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @sqlScriptOnline=N''

								-- check for online operation mode	
								IF @flgOptions & 4096 = 4096
									begin
										SET @nestedExecutionLevel = @executionLevel + 3
										EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @sqlServerName,
																						@dbName				= @dbName,
																						@tableSchema		= @crtSchemaName,
																						@tableName			= @crtTableName,
																						@indexName			= NULL,
																						@indexID			= 0,
																						@partitionNumber	= 1,
																						@sqlScriptOnline	= @sqlScriptOnline OUT,
																						@flgOptions			= @flgOptions,
																						@executionLevel		= @nestedExecutionLevel,
																						@debugMode			= @debugMode
									end

								IF (@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')
									begin
										SET @queryToRun=N'performing online table rebuild'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @queryToRun = N'IF OBJECT_ID(''' + @objectName + ''') IS NOT NULL ALTER TABLE ' + @objectName + N' REBUILD'
										SET @queryToRun = @queryToRun + N' WITH (' + @sqlScriptOnline + N'' + CASE WHEN ISNULL(@maxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@maxDOP AS [nvarchar]) ELSE N'' END + N')'
									end
								ELSE
									begin
										SET @queryToRun = N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; ';
										SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + @objectName + ''') IS NOT NULL ALTER TABLE ' + @objectName + N' REBUILD' + CASE WHEN ISNULL(@maxDOP, 0) <> 0 THEN N' WITH (MAXDOP = ' + CAST(@maxDOP AS [nvarchar]) + N')' ELSE N'' END 
									end

								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								
							end

						IF (@flgOptions & 16384 = 16384) AND (@serverVersionNum < 10 OR @flgErrorsOccured=1)
							begin
								------------------------------------------------------------------------------------------------------------------------
								--disable table non-clustered indexes
								IF @flgOptions & 8 = 8
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @sqlServerName,
																				@dbName						= @dbName,
																				@tableSchema				= @crtSchemaName,
																				@tableName					= @crtTableName,
																				@indexName					= '%',
																				@indexID					= NULL,
																				@partitionNumber			= 1,
																				@flgAction					= 4,
																				@flgOptions					= DEFAULT,
																				@maxDOP						= @maxDOP,
																				@executionLevel				= @nestExecutionLevel,
																				@affectedDependentObjects	= @affectedDependentObjects OUT,
																				@debugMode					= @debugMode
									end

								------------------------------------------------------------------------------------------------------------------------
								--disable table constraints
								IF @flgOptions & 16 = 16
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										SET @flgOptionsNested = 3 + (@flgOptions & 2048)
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName		= @sqlServerName ,
																					@dbName				= @dbName,
																					@tableSchema		= @crtSchemaName, 
																					@tableName			= @crtTableName,
																					@constraintName		= '%',
																					@flgAction			= 0,
																					@flgOptions			= @flgOptionsNested,
																					@executionLevel		= @nestExecutionLevel,
																					@debugMode			= @debugMode
									end

								SET @guid = CAST(NEWID() AS [nvarchar](38))

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Add a new temporary column [bigint]'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'ALTER TABLE [' + @dbName + N'].' + @objectName + N' ADD [' + @guid + N'] [bigint] IDENTITY'
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Create a temporary clustered index'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N' CREATE CLUSTERED INDEX [PK_' + @guid + N'] ON [' + @dbName + N'].' + @objectName + N' ([' + @guid + N'])'
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Drop the temporary clustered index'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'DROP INDEX [PK_' + @guid + N'] ON [' + @dbName + N'].' + @objectName 
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Drop the temporary column'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'ALTER TABLE [' + @dbName + N'].' + @objectName + N' DROP COLUMN [' + @guid + N']'
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								---------------------------------------------------------------------------------------------------------
								--rebuild table non-clustered indexes
								IF @flgOptions & 8 = 8
									begin
										SET @nestExecutionLevel = @executionLevel + 1

										EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @sqlServerName,
																				@dbName						= @dbName,
																				@tableSchema				= @crtSchemaName,
																				@tableName					= @crtTableName,
																				@indexName					= '%',
																				@indexID					= NULL,
																				@partitionNumber			= 1,
																				@flgAction					= 1,
																				@flgOptions					= 6165,
																				@maxDOP						= @maxDOP,
																				@executionLevel				= @nestExecutionLevel, 
																				@affectedDependentObjects	= @affectedDependentObjects OUT,
																				@debugMode					= @debugMode
									end

								---------------------------------------------------------------------------------------------------------
								--enable table constraints
								IF @flgOptions & 16 = 16
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										SET @flgOptionsNested = 3 + (@flgOptions & 2048)
	
										--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
										IF @flgOptions & 64 = 64
											SET @flgOptionsNested = @flgOptionsNested + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

										EXEC [dbo].[usp_mpAlterTableForeignKeys]	@sqlServerName		= @sqlServerName ,
																					@dbName				= @dbName,
																					@tableSchema		= @crtSchemaName, 
																					@tableName			= @crtTableName,
																					@constraintName		= '%',
																					@flgAction			= 1,
																					@flgOptions			= @flgOptionsNested,
																					@executionLevel		= @nestExecutionLevel, 
																					@debugMode			= @debugMode
									end
							end
					end

				-- 2 - Rebuild table: copy records to a temp table, delete records from source, insert back records from source, rebuild non-clustered indexes
				IF @flgActions=2
					begin
						SET @CopyTableName=@crtTableName + 'RebuildCopy'

						SET @queryToRun= 'Total Rows In Table To Be Exported To Temporary Storage: ' + CAST(@crtRecordCount AS [varchar](20))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @flgCopyMade=0
						--------------------------------------------------------------------------------------------------------
						--dropping copy table, if exists
						--------------------------------------------------------------------------------------------------------
						SET @queryToRun = 'IF EXISTS (	SELECT * 
														FROM [' + @dbName + '].[sys].[objects] so
														INNER JOIN [' + @dbName + '].[sys].[schemas] sch ON so.[schema_id] = sch.[schema_id]
														WHERE	sch.[name] = ''' + @crtSchemaName + ''' 
																AND so.[name] = ''' + @CopyTableName + '''
													) 
											DROP TABLE [' + @dbName + '].[' + @crtSchemaName + '].[' + @CopyTableName + ']'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																		@eventName		= 'database maintenance - rebuilding heap',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
				
						--------------------------------------------------------------------------------------------------------
						--create a copy of the source table
						--------------------------------------------------------------------------------------------------------
						SET @queryToRun = 'SELECT * INTO [' + @dbName + '].[' + @crtSchemaName + '].[' + @CopyTableName + '] FROM [' + @dbName + '].' + @objectName 
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																		@eventName		= 'database maintenance - rebuilding heap',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode

						IF @errorCode = 0
							SET @flgCopyMade=1
				
						IF @flgCopyMade=1
							begin
								--------------------------------------------------------------------------------------------------------
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'	SELECT    rc.[record_count]
																	FROM [' + @dbName + '].[sys].[objects] so WITH (READPAST)
																	INNER JOIN [' + @dbName + '].[sys].[schemas] sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id] 
																	INNER JOIN [' + @dbName + '].[sys].[indexes] si WITH (READPAST) ON si.[object_id] = so.[object_id] 
																	INNER  JOIN 
																			(
																				SELECT ps.object_id,
																						SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [record_count]
																				FROM [' + @dbName + '].[sys].[dm_db_partition_stats] ps WITH (READPAST)
																				GROUP BY ps.object_id		
																			)rc ON rc.[object_id] = so.[object_id] 
																	WHERE   so.[name] LIKE ''' + @CopyTableName + '''
																		AND sch.[name] LIKE ''' + @crtSchemaName + '''
																		AND si.[index_id] = 0'
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								DELETE FROM @tableGetRowCount
								INSERT INTO @tableGetRowCount([record_count])
									EXEC (@queryToRun)
							
								SELECT TOP 1 @crtRecordCount=[record_count] FROM @tableGetRowCount
								SET @queryToRun= '--	Total Rows In Temporary Storage Table After Export: ' + CAST(@crtRecordCount AS varchar(20))
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0


								--------------------------------------------------------------------------------------------------------
								--rebuild source table
								SET @nestExecutionLevel=@executionLevel + 2
								EXEC @flgErrorsOccured = [dbo].[usp_mpTableDataSynchronizeInsert]	@sourceServerName		= @sqlServerName,
																									@sourceDB				= @dbName,			
																									@sourceTableSchema		= @crtSchemaName,
																									@sourceTableName		= @CopyTableName,
																									@destinationServerName	= @sqlServerName,
																									@destinationDB			= @dbName,			
																									@destinationTableSchema	= @crtSchemaName,		
																									@destinationTableName	= @crtTableName,		
																									@flgActions				= 3,
																									@flgOptions				= @flgOptions,
																									@allowDataLoss			= 0,
																									@executionLevel			= @nestExecutionLevel,
																									@debugMode				= @debugMode
						
								--------------------------------------------------------------------------------------------------------
								--dropping copy table
								--------------------------------------------------------------------------------------------------------
								IF @flgErrorsOccured=0
									begin
										SET @queryToRun = 'IF EXISTS (	SELECT * 
																		FROM [' + @dbName + '].[sys].[objects] so
																		INNER JOIN [' + @dbName + '].[sys].[schemas] sch ON so.[schema_id] = sch.[schema_id]
																		WHERE	sch.[name] = ''' + @crtSchemaName + ''' 
																				AND so.[name] = ''' + @CopyTableName + '''
																	) 
															DROP TABLE [' + @dbName + '].[' + @crtSchemaName + '].[' + @CopyTableName + ']'
										IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																						@dbName			= @dbName,
																						@objectName		= @objectName,
																						@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																						@eventName		= 'database maintenance - rebuilding heap',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @debugMode
									end
							end
					end

				FETCH NEXT FROM crsTableListToRebuild INTO @crtSchemaName, @crtTableName, @crtRecordCount
			end
		CLOSE crsTableListToRebuild
		DEALLOCATE crsTableListToRebuild
	
		----------------------------------------------------------------------------------
		IF object_id('#tmpRebuildTableList') IS NOT NULL DROP TABLE #tmpRebuildTableList
		IF OBJECT_ID('#heapTableIndexList') IS NOT NULL DROP TABLE #heapTableIndexList
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
		@indexID					[int],
		@partitionNumber			[int] = 1,
		@flgAction					[tinyint] = 1,
		@flgOptions					[int] = 6145, --4096 + 2048 + 1	/* 6177 for space optimized index rebuild */
		@maxDOP						[smallint] = 1,
		@fillFactor					[tinyint] = 0,
		@executionLevel				[tinyint] = 0,
		@affectedDependentObjects	[nvarchar](max) OUTPUT,
		@debugMode					[bit] = 0
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
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed.
--		@indexName		- name of the index to be analyzed
--		@indexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@partitionNumber- index partition number. default value = 1 (index with no partitions)
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
			@flgInheritOptions		[int],
			@tmpIndexName			[sysname],
			@tmpIndexIsPrimaryXML	[bit],
			@nestedExecutionLevel	[tinyint]

DECLARE   @flgRaiseErrorAndStop [bit]
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

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @dbName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @tableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @tableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

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
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT  si.[index_id]
														, si.[name]
														, si.[type]
														, si.[allow_page_locks]
														, si.[is_disabled]
														, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
														, CASE WHEN SUM(CASE WHEN fk.[name] IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS [has_dependent_fk]
														, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0) AS [is_replicated]
													FROM [' + @dbName + '].[sys].[indexes]				si
													INNER JOIN [' + @dbName + '].[sys].[objects]		so  ON so.[object_id] = si.[object_id]
													INNER JOIN [' + @dbName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
													LEFT  JOIN [' + @dbName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
													LEFT  JOIN [' + @dbName + '].[sys].[foreign_keys]	fk  ON fk.[referenced_object_id] = so.[object_id] AND fk.[key_index_id] = si.[index_id]
													LEFT  JOIN [' + @dbName + '].[sys].[tables]			st  ON st.[object_id] = so.[object_id]
													WHERE	so.[name] = ''' + @crtTableName + '''
															AND sch.[name] = ''' + @crtTableSchema + '''
															AND so.[is_ms_shipped] = 0' + 
															CASE	WHEN @indexName IS NOT NULL 
																	THEN ' AND si.[name] LIKE ''' + @indexName + ''''
																	ELSE CASE WHEN @indexID  IS NOT NULL 
																			  THEN ' AND si.[index_id] = ' + CAST(@indexID AS [nvarchar])
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

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @tmpTableToAlterIndexes
						INSERT	INTO @tmpTableToAlterIndexes([index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated])
								EXEC (@queryToRun)

						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList



				DECLARE crsTableToAlterIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated]
																				FROM @tmpTableToAlterIndexes
																				ORDER BY [index_id], [index_name]						
				OPEN crsTableToAlterIndexes
				FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage= [dbo].[ufn_mpObjectQuoteName](@crtIndexName)
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
										EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @sqlServerName,
																						@dbName				= @dbName,
																						@tableSchema		= @crtTableSchema,
																						@tableName			= @crtTableName,
																						@indexName			= @crtIndexName,
																						@indexID			= @crtIndexID,
																						@partitionNumber	= @partitionNumber,
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
									AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
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
								IF @crtIndexType = 1
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										IF @flgOptions & 4 = 4
											begin
												--get all enabled non-clustered/xml/spatial indexes for current table
												SET @queryToRun = N''
												SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
																			FROM [' + @dbName + '].[sys].[indexes]				si
																			INNER JOIN [' + @dbName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																			INNER JOIN [' + @dbName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																			LEFT  JOIN [' + @dbName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
																			WHERE	so.[name] = ''' + @crtTableName + '''
																					AND sch.[name] = ''' + @crtTableSchema + ''' 
																					AND si.[type] in (2,3,4)
																					AND si.[is_disabled] = 0'
												SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
												IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

												INSERT INTO @DependentIndexes ([index_name], [is_primary_xml])
													EXEC (@queryToRun)
											end

										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtTableIsReplicated=0
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
									---------------------------------------------------------------------------------------------
									--xml primary key index options
									IF @crtIndexType = 3 AND @crtIndexIsPrimaryXML=1
										begin
											--4  - Rebuild all dependent indexes when rebuild primary indexes
											IF @flgOptions & 4 = 4
												begin
													--get all enabled secondary xml indexes for current table
													SET @queryToRun = N''
													SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				FROM [' + @dbName + '].[sys].[indexes]				si
																				INNER JOIN [' + @dbName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																				INNER JOIN [' + @dbName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																				INNER JOIN [' + @dbName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id]
																				WHERE	so.[name] = ''' + @crtTableName + '''
																						AND sch.[name] = ''' + @crtTableSchema + ''' 
																						AND si.[type] = 3
																						AND xi.[using_xml_index_id] = ''' + CAST(@crtIndexID AS [sysname]) + '''
																						AND si.[is_disabled] = 0'
													SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
													IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

													INSERT INTO @DependentIndexes ([index_name])
														EXEC (@queryToRun)
												end

											--8  - Disable non-clustered index before rebuild (save space)
											--won't disable the index when performing online rebuild
											IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtTableIsReplicated=0
												begin
													DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																					SELECT [index_name]
																					FROM @DependentIndexes
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
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
											begin
												SET @nestedExecutionLevel = @executionLevel + 2
												EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName	= @sqlServerName
																						, @dbName			= @dbName
																						, @tableSchema		= @crtTableSchema
																						, @tableName		= @crtTableName
																						, @indexName		= @crtIndexName
																						, @indexID			= NULL
																						, @partitionNumber	= @partitionNumber
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
										--get destination server running version/edition
										DECLARE		@serverEdition					[sysname],
													@serverVersionStr				[sysname],
													@serverVersionNum				[numeric](9,6)

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
																				@serverEdition			= @serverEdition OUT,
																				@serverVersionStr		= @serverVersionStr OUT,
																				@serverVersionNum		= @serverVersionNum OUT,
																				@executionLevel			= @nestedExecutionLevel,
																				@debugMode				= @debugMode
										
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
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX ' + dbo.ufn_mpObjectQuoteName(@crtIndexName) + ' ON [' + @crtTableSchema + '].[' + @crtTableName + '] REBUILD'
					
								--rebuild options
								SET @queryToRun = @queryToRun + N' WITH (SORT_IN_TEMPDB = ON' + CASE WHEN ISNULL(@maxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@maxDOP AS [nvarchar]) ELSE N'' END + 
																						CASE WHEN ISNULL(@sqlScriptOnline, N'')<>N'' THEN N', ' + @sqlScriptOnline ELSE N'' END + 
																						CASE WHEN ISNULL(@fillFactor, 0) <> 0 THEN N', FILLFACTOR = ' + CAST(@fillFactor AS [nvarchar]) ELSE N'' END +
																N')'

								IF @partitionNumber>1
									SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@partitionNumber AS [nvarchar])

								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline LIKE N'ONLINE = ON%'))
									begin
										SET @strMessage=N'performing index rebuild'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
									end

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = [dbo].[ufn_mpObjectQuoteName](@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

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
								IF (@crtIndexType = 1) OR (@crtIndexType = 3 AND @crtIndexIsPrimaryXML=1)
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										--will rebuild only indexes disabled by this tool
										IF (@flgOptions & 4 = 4)
											begin											
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT DISTINCT di.[index_name], di.[is_primary_xml]
																				FROM @DependentIndexes di
																				LEFT JOIN [maintenance-plan].[logInternalAction] smpi ON	smpi.[name]=N'index-made-disable'
																																					AND smpi.[server_name]=@sqlServerName
																																					AND smpi.[database_name]=@dbName
																																					AND smpi.[schema_name]=@crtTableSchema
																																					AND smpi.[object_name]=@crtTableName
																																					AND smpi.[child_object_name]=di.[index_name]
																				WHERE	(
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
									AND (@sqlScriptOnline LIKE N'ONLINE = ON%')) AND @crtTableIsReplicated=0
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

						---------------------------------------------------------------------------------------------
						-- 2  - Reorganize indexes
						---------------------------------------------------------------------------------------------
						-- avoid messages like:	The index [...] on table [..] cannot be reorganized because page level locking is disabled.		
						IF @flgAction = 2
							IF @crtIndexAllowPageLocks=1
								begin
									SET @queryToRun = N''
									SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
									SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX ' + dbo.ufn_mpObjectQuoteName(@crtIndexName) + ' ON [' + @crtTableSchema + '].[' + @crtTableName + '] REORGANIZE'
				
									--  1  - Compact large objects (LOB) (default)
									IF @flgOptions & 1 = 1
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = ON) '
									ELSE
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = OFF) '
				
									IF @partitionNumber>1
										SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@partitionNumber AS [nvarchar])
									IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


									SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
									SET @childObjectName = [dbo].[ufn_mpObjectQuoteName](@crtIndexName)
									SET @nestedExecutionLevel = @executionLevel + 1

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
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX ' + dbo.ufn_mpObjectQuoteName(@crtIndexName) + ' ON [' + @crtTableSchema + '].[' + @crtTableName + '] DISABLE'
				
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = [dbo].[ufn_mpObjectQuoteName](@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

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

RAISERROR('Create procedure: [dbo].[usp_mpAlterTableForeignKeys]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableForeignKeys]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
		@sqlServerName		[sysname],
		@dbName				[sysname],
		@tableSchema		[sysname] = '%', 
		@tableName			[sysname] = '%',
		@constraintName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2049,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@constraintName	- specify constraint name to be enabled/disabled. default all
--		@flgAction:		 1	- Enable Constraints (default)
--						 0	- Disable Constraints
--		@flgOptions:	 1	- Use tables that have foreign key constraints that reffer current table (default)
--						 2	- Use tables that current table foreign key constraints reffer  
--						 4  - Enable constraints with NOCHECK. Default is to enable constraints using CHECK option
--						 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@debugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    				[nvarchar](max),
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@tmpSchemaName			[sysname],
			@tmpTableName			[sysname],
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@tmpConstraintName		[sysname],
			@errorCode				[int],
			@tmpFlgAction			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @dbName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @tableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @tableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
				CREATE TABLE #tmpTableToAlterConstraints 
							(
								[TableSchema]		[sysname]
							  , [TableName]			[sysname]
							  , [ConstraintName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1	THEN 'Enable'
																ELSE 'Disable'
										END + ' foreign key constraints for: [' + @crtTableSchema + '].[' + @crtTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable foreign key constraint, will get only enabled constraints
						--if current action is to enable foreign key constraint, will get only disabled constraints
						IF (@flgOptions & 1 = 1)
							begin
								--list all tables that have foreign key constraints that reffers current table					
								SET @queryToRun=N'SELECT DISTINCT sch.[name] AS [schema_name], so.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM [' + @dbName + '].[sys].[objects] so
												INNER JOIN [' + @dbName + '].[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN [' + @dbName + '].[sys].[foreign_keys]	sfk  ON so.[object_id] = sfk.[parent_object_id]
												INNER JOIN [' + @dbName + '].[sys].[objects]		so2  ON sfk.[referenced_object_id] = so2.[object_id]
												INNER JOIN [' + @dbName + '].[sys].[schemas]		sch2 ON sch2.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + @crtTableName + '''
														AND sch2.[name] = ''' + @crtTableSchema + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
														AND sfk.[name] LIKE ''' + @constraintName + ''''
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						IF (@flgOptions & 2 = 2)
							begin
								--list all tables that current table foreign key constraints reffers 
								SET @queryToRun='SELECT DISTINCT sch2.[name] AS [schema_name], so2.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM [' + @dbName + '].[sys].[objects] so
												INNER JOIN [' + @dbName + '].[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN [' + @dbName + '].[sys].[foreign_keys]	sfk ON so.[object_id] = sfk.[referenced_object_id]
												INNER JOIN [' + @dbName + '].[sys].[objects]		so2 ON sfk.[parent_object_id] = so2.[object_id]
												INNER JOIN [' + @dbName + '].[sys].[schemas]		sch2 ON sch.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + @crtTableName + '''
														AND sch2.[name] = ''' + @crtTableSchema + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar])+ '
														AND sfk.[name] LIKE ''' + @constraintName + ''''

								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints ([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						DECLARE crsTableToAlterConstraints CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TableSchema], [TableName], [ConstraintName]
																							FROM #tmpTableToAlterConstraints
																							ORDER BY [TableName]						
						OPEN crsTableToAlterConstraints
						FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= '[' + @tmpSchemaName + '].[' + @tmpTableName + ']'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								--enable/disable foreign key constraints
								SET @queryToRun='ALTER TABLE [' + @dbName + '].[' + @tmpSchemaName + '].[' + @tmpTableName + ']' + 
												CASE WHEN @flgAction=1	
													 THEN ' WITH ' + 
															CASE WHEN @flgOptions & 4 = 4	THEN 'NOCHECK'
																							ELSE 'CHECK'
															END + ' CHECK '	
													 ELSE ' NOCHECK '
												END + 'CONSTRAINT [' + @tmpConstraintName + ']'
								IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								--
								SET @objectName = '[' + @tmpSchemaName + '].[' + @tmpTableName + ']'
								SET @childObjectName = QUOTENAME(@tmpConstraintName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																				@dbName			= @dbName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableForeignKeys',
																				@eventName		= 'database maintenance - alter constraints',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @debugMode

								IF @errorCode=0	
									begin
										/* 0 disable FK -> insert action 1 */
										/* 1 enable FK  -> delete action 2 */
										SET @tmpFlgAction = CASE WHEN @flgAction=1 THEN 2 ELSE 1 END
										EXEC [dbo].[usp_mpMarkInternalAction]		@actionName			= N'foreign-key-made-disable',
																					@flgOperation		= @tmpFlgAction,
																					@server_name		= @sqlServerName,
																					@database_name		= @dbName,
																					@schema_name		= @tmpSchemaName,
																					@object_name		= @tmpTableName,
																					@child_object_name	= @tmpConstraintName
									end
						
								FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
							end
						CLOSE crsTableToAlterConstraints
						DEALLOCATE crsTableToAlterConstraints
						
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		IF object_id('#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
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
	    @recreateMode			[bit] = 0,				/*  1 - existings jobs will be dropped an created based on this stored procedure logic
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
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @featureflgActions	[int]
		, @forInstanceID		[int]
		, @forSQLServerName		[sysname]

DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6)

DECLARE @jobExecutionQueue TABLE
		(
			[instance_id]			[smallint]		NOT NULL,
			[project_id]			[smallint]		NOT NULL,
			[module]				[varchar](32)	NOT NULL,
			[descriptor]			[varchar](256)	NOT NULL,
			[for_instance_id]		[smallint]		NOT NULL,
			[job_name]				[sysname]		NOT NULL,
			[job_step_name]			[sysname]		NOT NULL,
			[job_database_name]		[sysname]		NOT NULL,
			[job_command]			[nvarchar](max) NOT NULL
		)

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
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR	SELECT	cin.[instance_id], cin.[instance_name]
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

		DECLARE crsCollectorDescriptor CURSOR LOCAL FAST_FORWARD FOR	SELECT [descriptor]
																		FROM
																			(
																				SELECT 'dbo.usp_mpDatabaseConsistencyCheck' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseOptimize' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseShrink' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseBackup(Data)' AS [descriptor] UNION ALL
																				SELECT 'dbo.usp_mpDatabaseBackup(Log)' AS [descriptor]
																			)X
																		WHERE (    [descriptor] LIKE @jobDescriptor
																				OR ISNULL(CHARINDEX([descriptor], @jobDescriptor), 0) <> 0
																				)			

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
											  WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
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
										  WHERE jeq.[job_name] LIKE (DB_NAME() + ' - ' + @codeDescriptor + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
										) = 0
									)
								)


				DELETE FROM @jobExecutionQueue

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseConsistencyCheck'
					begin
						/*-------------------------------------------------------------------*/
						/* Weekly: Database Consistency Check - only once a week on Saturday */
						IF @flgActions & 1 = 1 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Database Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName	= ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 1, @flgOptions = 3, @maxDOP	= DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Database Consistency Check', GETDATE()) = 1
							SET @featureflgActions = 8
						ELSE
							SET @featureflgActions = 12

						IF @flgActions & 2 = 2 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Allocation Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Allocation Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [nvarchar]) + N', @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 4 = 4 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Tables Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Tables Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName	= ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 2, @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 8 = 8 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Reference Consistency Check', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Reference Consistency Check' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 16 = 16 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseConsistencyCheck', 'Perform Correction to Space Usage', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Perform Correction to Space Usage' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseConsistencyCheck] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 64, @flgOptions = DEFAULT, @maxDOP	= DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 32 = 32 AND @serverVersionNum > 9 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Rebuild Heap Tables', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Rebuild Heap Tables' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 16, @flgOptions = DEFAULT, @defragIndexThreshold = DEFAULT, @rebuildIndexThreshold = DEFAULT, @pageThreshold = DEFAULT, @rebuildIndexPageCountLimit = DEFAULT, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 64 = 64 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Rebuild or Reorganize Indexes', GETDATE()) = 1
							begin
								SET @featureflgActions = 3
								
								IF @flgActions & 128 = 128 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', GETDATE()) = 1 /* Daily: Update Statistics */
									SET @featureflgActions = 11

								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Rebuild or Reorganize Indexes' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = ' + CAST(@featureflgActions AS [varchar]) + ', @flgOptions = DEFAULT, @defragIndexThreshold = DEFAULT, @rebuildIndexThreshold = DEFAULT, @pageThreshold = DEFAULT, @rebuildIndexPageCountLimit = DEFAULT, @statsSamplePercent = DEFAULT, @statsAgeDays = DEFAULT, @statsChangePercent = DEFAULT, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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

						/*-------------------------------------------------------------------*/
						/* Daily: Update Statistics */
						IF @flgActions & 128 = 128 AND NOT (@flgActions & 64 = 64) AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseOptimize', 'Update Statistics', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Update Statistics' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseOptimize] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @tableSchema = ''%'', @tableName = ''%'', @flgActions = 8, @flgOptions = DEFAULT, @statsSamplePercent = DEFAULT, @statsAgeDays = DEFAULT, @statsChangePercent = DEFAULT, @maxDOP = DEFAULT, @maxRunningTimeInMinutes = DEFAULT, @skipObjectsList = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 256 = 256 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseShrink', 'Shrink Database (TRUNCATEONLY)', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Shrink Database (TRUNCATEONLY)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseShrink] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @flgActions = 2, @flgOptions = 1, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 512 = 512 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseShrink', 'Shrink Log File', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Shrink Log File' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseShrink] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @flgActions = 1, @flgOptions = 0, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 1024 = 1024 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (diff)', GETDATE()) = 1
							AND NOT (@flgActions & 2048 = 2048 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)', GETDATE()) = 1)
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup User Databases (diff)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 2, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 2048 = 2048 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases (full)', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup User Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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
						IF @flgActions & 4096 = 4096 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'System Databases (full)', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup System Databases (full)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 1, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
								FROM
									(
										SELECT [name] AS [database_name]
										FROM [dbo].[catalogDatabaseNames]
										WHERE	[project_id] = @projectID
												AND [instance_id] = @forInstanceID
												AND [active] = 1
												AND [name] IN ('master', 'model', 'msdb', 'distribution')														
									)X
					end

				------------------------------------------------------------------------------------------------------------------------------------------
				IF @codeDescriptor = 'dbo.usp_mpDatabaseBackup(Log)'
					begin
						/*-------------------------------------------------------------------*/
						/* Hourly: Backup User Databases Transaction Log */
						IF @flgActions & 8192 = 8192 AND [dbo].[ufn_mpCheckTaskSchedulerForDate](@projectCode, 'dbo.usp_mpDatabaseBackup', 'User Databases Transaction Log', GETDATE()) = 1
								INSERT INTO @jobExecutionQueue (  [instance_id], [project_id], [module], [descriptor]
																, [for_instance_id], [job_name], [job_step_name], [job_database_name]
																, [job_command])
								SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
										@forInstanceID AS [for_instance_id], 
										SUBSTRING(DB_NAME() + ' - ' + @codeDescriptor + ' - Backup User Databases (log)' + CASE WHEN @forSQLServerName <> @@SERVERNAME THEN ' - ' + REPLACE(@forSQLServerName, '\', '$') + ' ' ELSE ' - ' END + '[' + X.[database_name] + ']', 1, 128) AS [job_name],
										'Run'		AS [job_step_name],
										DB_NAME()	AS [job_database_name],
										'EXEC [dbo].[usp_mpDatabaseBackup] @sqlServerName = ''' + @forSQLServerName + N''', @dbName = ''' + X.[database_name] + N''', @backupLocation = DEFAULT, @flgActions = 4, @flgOptions = DEFAULT, @retentionDays = DEFAULT, @debugMode = ' + CAST(@debugMode AS [varchar])
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

				IF @recreateMode = 0
					UPDATE jeq
						SET   jeq.[execution_date] = NULL
							, jeq.[running_time_sec] = NULL
							, jeq.[log_message] = NULL
							, jeq.[status] = -1
							, jeq.[event_date_utc] = GETUTCDATE()
					FROM [dbo].[jobExecutionQueue] jeq
					INNER JOIN @jobExecutionQueue S ON		jeq.[instance_id] = S.[instance_id]
														AND jeq.[project_id] = S.[project_id]
														AND jeq.[module] = S.[module]
														AND jeq.[descriptor] = S.[descriptor]
														AND jeq.[for_instance_id] = S.[for_instance_id]
														AND jeq.[job_name] = S.[job_name]
														AND jeq.[job_step_name] = S.[job_step_name]
														AND jeq.[job_database_name] = S.[job_database_name]
					WHERE (     @skipDatabasesList IS NULL
							OR (    @skipDatabasesList IS NOT NULL	
									AND (
										SELECT COUNT(*)
										FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
										WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
									) = 0
								)
						  )

				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
														, [for_instance_id], [job_name], [job_step_name], [job_database_name]
														, [job_command])
						SELECT	  S.[instance_id], S.[project_id], S.[module], S.[descriptor]
								, S.[for_instance_id], S.[job_name], S.[job_step_name], S.[job_database_name]
								, S.[job_command]
						FROM @jobExecutionQueue S
						LEFT JOIN [dbo].[jobExecutionQueue] jeq ON		jeq.[instance_id] = S.[instance_id]
																	AND jeq.[project_id] = S.[project_id]
																	AND jeq.[module] = S.[module]
																	AND jeq.[descriptor] = S.[descriptor]
																	AND jeq.[for_instance_id] = S.[for_instance_id]
																	AND jeq.[job_name] = S.[job_name]
																	AND jeq.[job_step_name] = S.[job_step_name]
																	AND jeq.[job_database_name] = S.[job_database_name]
						WHERE	jeq.[job_name] IS NULL
								AND (     @skipDatabasesList IS NULL
										OR (    @skipDatabasesList IS NOT NULL	
												AND (
													SELECT COUNT(*)
													FROM [dbo].[ufn_getTableFromStringList](@skipDatabasesList, ',') X
													WHERE S.[job_name] LIKE (DB_NAME() + ' - ' + S.[descriptor] + '%' + CASE WHEN @@SERVERNAME <> @@SERVERNAME THEN ' - ' + REPLACE(@@SERVERNAME, '\', '$') + ' ' ELSE ' - ' END + '%' + X.[value] + ']')
												) = 0
											)
									  )

				FETCH NEXT FROM crsCollectorDescriptor INTO @codeDescriptor
			end
		CLOSE crsCollectorDescriptor
		DEALLOCATE crsCollectorDescriptor
										

		FETCH NEXT FROM crsActiveInstances INTO @forInstanceID, @forSQLServerName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO



/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT

