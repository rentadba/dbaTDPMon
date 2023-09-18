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
		@flgOptions					[int]		= 45189,--32768 + 8192 + 4096 + 128 + 4 + 1
		@defragIndexThreshold		[smallint]	=     5,
		@rebuildIndexThreshold		[smallint]	=    30,
		@pageThreshold				[int]		=  1000,
		@rebuildIndexPageCountLimit	[int]		= 2147483647,	--16TB/no limit
		@statsSamplePercent			[smallint]	=     0,
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
--						 8  - Update/create statistics for table (UPDATE STATISTICS)						(default)
--							  should be performed daily
--						16  - Rebuild heap tables (SQL versions +2K5 only)									(default)
--		@flgOptions		 1  - Compact large objects (LOB) when reorganize  (default); for columnstore indexes will compress all row groups (COMPRESS_ALL_ROW_GROUPS)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space)
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
--				    131072  - Create statistics on all eligible columns
--					262144	- take a log backup at the end of the optimization process
--					262144	- perform a shrink with truncate_only on the log file at the end of the optimization process

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
			@queryParameters				[nvarchar](512),
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
			@CurrentSegmentCount			[bigint],
			@CurrentDeletedSegmentCount		[bigint],
			@CurentDeletedSegmentPercentage	[numeric] (6,2),
			@errorCode						[int],
			@ClusteredRebuildNonClustered	[bit],
			@flgInheritOptions				[int],
			@statsCount						[int], 
			@nestExecutionLevel				[tinyint],
			@analyzeIndexType				[nvarchar](32),
			@eventData						[varchar](8000),
			@affectedDependentObjects		[nvarchar](4000),
			@indexIsRebuilt					[bit],
			@stopTimeLimit					[datetime],
			@partitionNumber				[int],
			@isPartitioned					[bit],
			@executionDBName				[sysname],
			@databaseStateDesc				[sysname],
			@dbIsReadOnly					[bit],
			@sqlServiceUpTimeDays			[smallint],
			@unusedIndexesThresholdDays		[smallint], 
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

-----------------------------------------------------------------------------------------
--get configuration values: do not run maintenance for unused indexes
---------------------------------------------------------------------------------------------
BEGIN TRY
	SELECT @unusedIndexesThresholdDays = [value] 
	FROM	[dbo].[appConfigurations] 
	WHERE	[name]='Do not run maintenance for indexes not used in the last N days (0=disabled)'
			AND [module] = 'maintenance-plan'
END TRY
BEGIN CATCH
	SET @unusedIndexesThresholdDays = 0
END CATCH

SET @unusedIndexesThresholdDays = ISNULL(@unusedIndexesThresholdDays, 0)


---------------------------------------------------------------------------------------------
--get SQL Server running major version and database compatibility level
---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@serverEngine					[int],
			@nestedExecutionLevel			[tinyint]

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
		@actionType			 [sysname]

SET @agStopLimit = 0

IF @flgActions &  1 =  1	SET @actionType = 'reorganize index'
IF @flgActions &  2 =  2	SET @actionType = 'rebuilding index'
IF @flgActions &  4 =  4	SET @actionType = 'rebuilding index'
IF @flgActions &  8 =  8	SET @actionType = 'update statistics'
IF @flgActions & 16 = 16	SET @actionType = 'rebuilding heap'

IF @serverVersionNum >= 11 AND @isAzureSQLDatabase = 0
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database maintenance',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@clusterName		= @clusterName OUTPUT,
																		@agInstanceRoleDesc = @agInstanceRoleDesc OUTPUT,
																		@agReadableSecondary= @agReadableSecondary OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0

SET @executionDBName = @dbName
IF @clusterName IS NOT NULL AND @agInstanceRoleDesc = 'SECONDARY' AND @agReadableSecondary='NO' 
	SET @executionDBName = 'master'

---------------------------------------------------------------------------------------------
DECLARE @compatibilityLevel [tinyint]
IF OBJECT_ID('#databaseProperties') IS NOT NULL DROP TABLE #databaseProperties
CREATE TABLE #databaseProperties
			(
				  [state_desc]			[sysname]	NULL
				, [is_read_only]		[bit]		NULL
				, [compatibility_level]	[tinyint]	NULL
			)


SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'SELECT [state_desc], [is_read_only], [compatibility_level] FROM sys.databases WHERE [name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + '''';
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

INSERT INTO #databaseProperties([state_desc], [is_read_only], [compatibility_level])
		EXEC sp_executesql @queryToRun

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseProperties

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

IF ISNULL(@statsSamplePercent, 0) < 0
	begin
		SET @queryToRun=N'ERROR: Sample percent value for update statistics sample should be greater or equal to 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF @defragIndexThreshold > @rebuildIndexThreshold
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be smalller or equal to threshold value for rebuilding indexes.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end


--------------------------------------------------------------------------------------------------
--get database status and read/write flag
SET @dbIsReadOnly = 0
SELECT	@databaseStateDesc = [state_desc],
		@dbIsReadOnly = [is_read_only]
FROM #databaseProperties
SET @databaseStateDesc = ISNULL(@databaseStateDesc, 'NULL')

IF @dbIsReadOnly = 1
begin
	SET @queryToRun='Current database state (' + @databaseStateDesc + ') does not allow writing / maintenance.'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

	SET @eventData='<skipaction><detail>' + 
						'<name>database maintenance</name>' + 
						'<type>N/A</type>' + 
						'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
						'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
						'<reason>' + @queryToRun + '</reason>' + 
					'</detail></skipaction>'

	EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
										@dbName			= @dbName,
										@module			= 'dbo.usp_mpDatabaseOptimize',
										@eventName		= 'database maintenance',
										@eventMessage	= @eventData,
										@eventType		= 0 /* info */
end

---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#currentRowStoreFragmentationStats') IS NOT NULL DROP TABLE #currentRowStoreFragmentationStats
CREATE TABLE #currentRowStoreFragmentationStats 
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
			[ghost_record_count]			[bigint]		NULL,
			[partition_number]				[int]
		)	
			
CREATE INDEX IX_currentRowStoreFragmentationStats ON #currentRowStoreFragmentationStats([ObjectId], [IndexId])

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#currentColumnStoreFragmentationStats') IS NOT NULL 
	DROP TABLE #currentColumnStoreFragmentationStats

CREATE TABLE #currentColumnStoreFragmentationStats
		(
			[object_id]						[int],
			[index_id]						[int],
			[partition_number]				[int]		NULL,
			[page_count]					[bigint]	NULL,
			[avg_fragmentation_in_percent]	[decimal](38,2)	NULL,
			[segments_count]				[bigint]	NULL,
			[deleted_segments_count]		[bigint]	NULL,
			[deleted_segments_percentage]	[decimal](38,2)	NULL
		)
CREATE INDEX IX_currentColumnStoreFragmentationStats ON #currentColumnStoreFragmentationStats([object_id], [index_id])

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
											[page_density_deviation]		[decimal](38,2)	NULL,	
											[partition_number]				[int] NULL,
											[is_partitioned]				[bit] NULL,
											[segments_count]				[bigint]	NULL,		/* for columnstore indexes */
											[deleted_segments_count]		[bigint]	NULL,		/* for columnstore indexes */
											[deleted_segments_percentage]	[decimal](38,2)	NULL,	/* for columnstore indexes */
											)
CREATE INDEX IX_databaseObjectsWithIndexList_TableName ON #databaseObjectsWithIndexList([table_schema], [table_name], [index_id], [partition_number], [avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])
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
												[percent_changes]		[decimal](38,2)	NULL,
												[is_incremental]		[bit]			NULL DEFAULT (0),
												[partition_number]		[int]			NULL
												)

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#incrementalStatisticsList') IS NOT NULL 
	DROP TABLE #incrementalStatisticsList

CREATE TABLE #incrementalStatisticsList (
										 [object_id]	[int],
										 [stats_id]		[int]
										)

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#objectsInOfflineFileGroups') IS NOT NULL 
	DROP TABLE #objectsInOfflineFileGroups

CREATE TABLE #objectsInOfflineFileGroups (
											[object_id]			[int],
											[index_id]			[int],										
											[filegroup_name]	[sysname]
										 )

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#unusedIndexes') IS NOT NULL 
	DROP TABLE #unusedIndexes

CREATE TABLE #unusedIndexes (
								[object_id]			[int],
								[index_id]			[int]
							)


---------------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


--------------------------------------------------------------------------------------------------
--perform internal cleanup
--------------------------------------------------------------------------------------------------
EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-rebuild',
										@flgOperation		= 2,
										@server_name		= @sqlServerName,
										@database_name		= @dbName,
										@schema_name		= @tableSchema,
										@object_name		= @tableName,
										@child_object_name	= '%'

--------------------------------------------------------------------------------------------------
--get objects for which maintenance cannot be perform as the filegroup is offline
--------------------------------------------------------------------------------------------------
SET @queryToRun=N'Create list of "offline" tables/indexes ...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

SET @queryToRun = N''				
SET @queryToRun = @queryToRun + 
					CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
					SELECT si.[object_id], si.[index_id], df.[name]
					FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]		si
					INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]	ob	ON ob.[object_id] = si.[object_id]
					INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]	sc	ON sc.[schema_id] = ob.[schema_id]
					INNER JOIN  (	/* "offline" filegroups */
									SELECT df.[data_space_id], ds.[name]
									FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[database_files] df
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[data_spaces] ds ON ds.[data_space_id] = df.[data_space_id]
									LEFT JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[database_files] df2 ON df2.[data_space_id] = df.[data_space_id] AND df2.[state_desc] = ''ONLINE''
									WHERE df.[state_desc]<>''ONLINE''
											AND df2.[file_id] IS NULL
								)df ON si.[data_space_id] = df.[data_space_id]
					WHERE	ob.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
							AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + '''
							AND si.[is_disabled] = 0
							AND ob.[type] IN (''U'', ''V'')'

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #objectsInOfflineFileGroups([object_id], [index_id], [filegroup_name])
		EXEC sp_executesql  @queryToRun	

--------------------------------------------------------------------------------------------------
--16 - get current heap tables list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'0'

		SET @queryToRun=N'Create list of heap tables to be analyzed...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				
		SET @queryToRun = @queryToRun + 
							CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
							SELECT DISTINCT 
									  DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') AS [database_id]
									, si.[object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[index_id]
									, si.[name] AS [index_name]
									, si.[type] AS [index_type]
									, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
									, sp.[partition_number]
									, CASE WHEN sp.[partition_count] <> 1 THEN 1 ELSE 0 END AS [is_partitioned]
							FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			si
							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
							CASE WHEN @flgOptions & 32768 = 32768 
								THEN N'
							INNER JOIN
									(
											SELECT   [object_id]
												, SUM([reserved_page_count]) as [reserved_page_count]
											FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_partition_stats
											GROUP BY [object_id]
											HAVING SUM([reserved_page_count]) >=' + CAST(@pageThreshold AS [nvarchar](32)) + N'
									) ps ON ps.[object_id] = ob.[object_id]'
								ELSE N''
								END + N'
							INNER JOIN
									(
										SELECT [object_id], [index_id], [partition_number], COUNT(*) OVER(PARTITION BY [object_id], [index_id]) AS [partition_count]
										FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.partitions
									) sp ON sp.[object_id] = ob.[object_id] AND sp.[index_id] = si.[index_id]
							WHERE	ob.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
									AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + '''
									AND si.[type] IN (' + @analyzeIndexType + N')
									AND ob.[type] IN (''U'', ''V'')' + 
									CASE WHEN @skipObjectsList IS NOT NULL
										 THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))
													AND (si.[name] NOT IN (SELECT [value] FROM ' +  [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) OR si.[name] IS NULL)'  
									ELSE N'' END

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor], [partition_number], [is_partitioned])
				EXEC sp_executesql  @queryToRun

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseObjectsWithIndexList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[table_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE osl.[instance_name] = @sqlServerName
								AND osl.[database_name] = @dbName
								AND osl.[active] = 1
								AND @flgActions & osl.[flg_actions] = osl.[flg_actions]'
		SET @queryParameters = '@sqlServerName [sysname], @dbName [sysname], @flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
														, @dbName = @dbName
														, @flgActions = @flgActions

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseObjectsWithIndexList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[index_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE osl.[instance_name] = @sqlServerName
								AND osl.[database_name] = @dbName
								AND osl.[active] = 1
								AND @flgActions & osl.[flg_actions] = osl.[flg_actions]'
		SET @queryParameters = '@sqlServerName [sysname], @dbName [sysname], @flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
														, @dbName = @dbName
														, @flgActions = @flgActions

		--delete entries which are for objects in "offline" filegroups
		DELETE dtl
		FROM #databaseObjectsWithIndexList dtl
		INNER JOIN #objectsInOfflineFileGroups oofg ON dtl.[object_id] = oofg.[object_id] 
													AND dtl.[index_id] = oofg.[index_id]
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))

			
--------------------------------------------------------------------------------------------------
--1/2	- Analyzing heap tables fragmentation
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Analyzing heap fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted') + CASE WHEN @IndexName IS NOT NULL THEN N' - ' + [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted')  ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun=CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
									SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
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
											, ips.[partition_number]
									FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
													'DETAILED'
											+ ''') ips
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
									WHERE	si.[type] IN (' + @analyzeIndexType + N')'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
				INSERT	INTO #currentRowStoreFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count], [partition_number])  
						EXEC sp_executesql  @queryToRun

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
		INNER JOIN #currentRowStoreFragmentationStats cifs ON	cifs.[ObjectId] = doil.[object_id] 
															AND cifs.[IndexId] = doil.[index_id] 
															AND cifs.[partition_number] = doil.[partition_number]
	end


--------------------------------------------------------------------------------------------------
-- 16	- Rebuild heap tables (SQL versions +2K5 only)
-- implemented an algoritm based on Tibor Karaszi's one: http://sqlblog.com/blogs/tibor_karaszi/archive/2014/03/06/how-often-do-you-rebuild-your-heaps.aspx
-- rebuilding heaps also rebuild its non-clustered indexes. do heap maintenance before index maintenance
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
	begin
		SET @queryToRun='Rebuilding database heap tables...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[avg_fragmentation_in_percent], doil.[page_count]
																			, doil.[page_density_deviation], doil.[forwarded_records_percentage]
																			, doil.[is_partitioned], doil.[partition_number]
		   													FROM	#databaseObjectsWithIndexList doil
															WHERE	(    doil.[avg_fragmentation_in_percent] >= @rebuildIndexThreshold
																	  OR doil.[forwarded_records_percentage] >= @defragIndexThreshold
																	  OR doil.[page_density_deviation] >= @rebuildIndexThreshold
																	)
																	AND doil.[index_type] IN (0)
															ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent, @isPartitioned, @partitionNumber
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName = [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'quoted')

		   		SET @queryToRun=@objectName + ' => ' + CASE WHEN @isPartitioned = 1 THEN 'partition: ' + CAST(@partitionNumber AS [varchar](32)) + N' / ' ELSE N'' END + N'current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density deviation = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				--------------------------------------------------------------------------------------------------
				--log heap fragmentation information
				SET @eventData='<heap-fragmentation><detail>' + 
									'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
									'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
									'<partition_number>' + CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
									'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
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
															@partitionNumber	= @partitionNumber,
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
						AND doil.[partition_number] = @partitionNumber
				
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent, @isPartitioned, @partitionNumber
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4 - get current index list: clustered, non-clustered, xml, spatial
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'1,2,3,4,5,6'

		SET @queryToRun=N'Create list of indexes to be analyzed...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				
		SET @queryToRun = @queryToRun + 
							CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
							SELECT DISTINCT 
										DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') AS [database_id]
									, si.[object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[index_id]
									, si.[name] AS [index_name]
									, si.[type] AS [index_type]
									, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
									, sp.[partition_number]
									, CASE WHEN sp.[partition_count] <> 1 THEN 1 ELSE 0 END AS [is_partitioned]
							FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			si
							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
							CASE WHEN @flgOptions & 32768 = 32768 
								THEN N'
							INNER JOIN
									(
											SELECT   [object_id]
												, SUM([reserved_page_count]) as [reserved_page_count]
											FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_partition_stats
											GROUP BY [object_id]
											HAVING SUM([reserved_page_count]) >=' + CAST(@pageThreshold AS [nvarchar](32)) + N'
									) ps ON ps.[object_id] = ob.[object_id]'
								ELSE N''
								END + N'
							INNER JOIN
									(
										SELECT [object_id], [index_id], [partition_number], COUNT(*) OVER(PARTITION BY [object_id], [index_id]) AS [partition_count]
										FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.partitions
									) sp ON sp.[object_id] = ob.[object_id] AND sp.[index_id] = si.[index_id]

							WHERE	ob.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
									AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + '''
									AND si.[type] IN (' + @analyzeIndexType + N')
									AND si.[is_disabled]=0
									AND ob.[type] IN (''U'', ''V'')' + 
									CASE WHEN @skipObjectsList IS NOT NULL
											THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
													AND si.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
									ELSE N'' END

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor], [partition_number], [is_partitioned])
				EXEC sp_executesql  @queryToRun

		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseObjectsWithIndexList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[table_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE osl.[instance_name] = @sqlServerName
								AND osl.[database_name] = @dbName
								AND osl.[active] = 1
								AND @flgActions & osl.[flg_actions] = osl.[flg_actions]'
		SET @queryParameters = '@sqlServerName [sysname], @dbName [sysname], @flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
														, @dbName = @dbName
														, @flgActions = @flgActions

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseObjectsWithIndexList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[index_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE osl.[instance_name] = @sqlServerName
								AND osl.[database_name] = @dbName
								AND osl.[active] = 1
								AND @flgActions & osl.[flg_actions] = osl.[flg_actions]'
		SET @queryParameters = '@sqlServerName [sysname], @dbName [sysname], @flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
														, @dbName = @dbName
														, @flgActions = @flgActions

		--delete entries which are for objects in "offline" filegroups
		DELETE dtl
		FROM #databaseObjectsWithIndexList dtl
		INNER JOIN #objectsInOfflineFileGroups oofg ON dtl.[object_id] = oofg.[object_id] 
													AND dtl.[index_id] = oofg.[index_id]


		/* do not run maintenance on unused indexes / more than X days */
		IF @unusedIndexesThresholdDays > 0
			begin
				DECLARE @instanceUptimeDays [smallint]
				SET @queryToRun=N'Get instance uptime...' + [dbo].[ufn_getObjectQuoteName](@sqlServerName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
				SET @queryToRun = N'SELECT DATEDIFF(day, [create_date], GETDATE()) AS [uptime_days] FROM sys.databases WHERE [name]=''tempdb'''				
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				SET @queryToRun = N'SELECT @instanceUptimeDays = [uptime_days] FROM (' + @queryToRun + N')x'
				SET @queryParameters = '@instanceUptimeDays [smallint] OUTPUT'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				EXEC sp_executesql @queryToRun, @queryParameters, @instanceUptimeDays = @instanceUptimeDays OUTPUT
				
				IF @instanceUptimeDays >= @unusedIndexesThresholdDays
					begin
						/* get the list of the unused indexes (and not used to support foreing keys) which will be excluded from the maintenance */
						SET @queryToRun = N''				
						SET @queryToRun = @queryToRun + 
											CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
											SELECT   si.[object_id], si.[index_id]
											FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes] si
											INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[tables] st ON si.[object_id]=st.[object_id]
											LEFT JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[dm_db_index_usage_stats] iut ON	iut.[object_id] = si.[object_id] AND iut.[index_id]=si.[index_id] AND iut.[database_id] = DB_ID()
											LEFT JOIN 
												(
													--indexes used by foreign-key constraints
													SELECT	DISTINCT 
															ob.[schema_id],
															ob.[object_id],
															ip.[index_id]
													FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[foreign_key_columns]	fkc
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[foreign_keys]	fk	ON fkc.[constraint_object_id] = fk.[object_id]
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[columns]			cp	ON fkc.[parent_column_id] = cp.[column_id] AND fkc.[parent_object_id] = cp.[object_id]
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[columns]			cr	ON fkc.[referenced_column_id] = cr.[column_id] AND fkc.[referenced_object_id] = cr.[object_id]
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[objects]			ob	ON ob.[object_id] = fkc.[parent_object_id]
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[index_columns]	icp ON icp.[object_id] = fkc.[parent_object_id] AND icp.[column_id] = fkc.[parent_column_id] AND icp.[key_ordinal] = 1
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			ip	ON ip.[object_id] = icp.[object_id] AND ip.[index_id] = icp.[index_id]
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[index_columns]	icr ON icr.[object_id] = fkc.[referenced_object_id] AND icr.[column_id] = fkc.[referenced_column_id] AND icr.[key_ordinal] = 1
													INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'[sys].[indexes]			ir	ON ir.[object_id] = icr.[object_id] AND ir.[index_id] = icr.[index_id] AND (ir.[is_unique] = 1 OR ir.[is_primary_key] = 1)
												)fki ON fki.[object_id] = st.[object_id] AND fki.[schema_id] = st.[schema_id] AND fki.[index_id] = si.[index_id]
											where   (    iut.[object_id] IS NULL 
													OR 
														(iut.[object_id] IS NOT NULL AND (iut.[user_seeks] + iut.[user_scans] + iut.[user_lookups]) = 0)
													)
													AND st.[is_ms_shipped] = 0
													AND si.[index_id] NOT IN (0, 1)
													AND si.[is_primary_key] = 0
													AND si.[is_unique_constraint] = 0
													AND si.[is_disabled] = 0
													AND fki.[object_id] IS NULL'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						INSERT	INTO #unusedIndexes([object_id], [index_id])
								EXEC sp_executesql  @queryToRun

						/* save information about the skipped indexes */
						DECLARE crsUnusedIndexesToSkip CURSOR LOCAL FAST_FORWARD FOR	SELECT    [dbo].[ufn_getObjectQuoteName]([table_schema], 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM([table_name]), 'quoted') AS [object_name]
																								, [index_name]
																						FROM #databaseObjectsWithIndexList doil
																						INNER JOIN #unusedIndexes ui ON ui.[object_id] = doil.[object_id] AND ui.[index_id] = doil.[index_id]
						OPEN crsUnusedIndexesToSkip
						FETCH NEXT FROM crsUnusedIndexesToSkip INTO @objectName, @IndexName
						WHILE @@FETCH_STATUS=0
							begin
								SET @eventData='<skipaction><detail>' + 
													'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
													'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
													'<index_name>' + [dbo].[ufn_getObjectQuoteName](@IndexName, 'xml') + '</index_name>' + 
													'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
													'<reason>unused index in the last ' + CAST(@unusedIndexesThresholdDays AS [sysname]) + ' days</reason>' + 
												'</detail></skipaction>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																	@dbName			= @dbName,
																	@module			= 'dbo.usp_mpDatabaseOptimize',
																	@eventName		= 'database maintenance',
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */

								FETCH NEXT FROM crsUnusedIndexesToSkip INTO @objectName, @IndexName
							end
						CLOSE crsUnusedIndexesToSkip
						DEALLOCATE crsUnusedIndexesToSkip	


						/* delete unused indexe from the driving table*/
						DELETE doil
						FROM #databaseObjectsWithIndexList doil
						INNER JOIN #unusedIndexes ui ON ui.[object_id] = doil.[object_id] 
														AND ui.[index_id] = doil.[index_id]
					end
			end
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
		SET @queryToRun=N'Create list of statistics to be analyzed...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF (@serverVersionNum >= 10.504000 AND @serverVersionNum < 11) OR @serverVersionNum >= 11.03000
			/* starting with SQL Server 2008 R2 SP2 / SQL Server 2012 SP1 */
			SET @queryToRun = @queryToRun + 
								CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
								SELECT DISTINCT
											DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') AS [database_id]
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
								FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.stats ss WITH (NOLOCK)
								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.objects ob WITH (NOLOCK) ON ob.[object_id] = ss.[object_id]
								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.schemas sc WITH (NOLOCK) ON sc.[schema_id] = ob.[schema_id]' + N'
								CROSS APPLY ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_stats_properties(ss.object_id, ss.stats_id) AS sp
								WHERE	ob.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
										AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + '''
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
												OR  
													(
														DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N' 
													AND sp.[modification_counter] <> 0 
													AND SQRT(1000 * CAST(sp.[rows] AS [float])) <= ABS(sp.[modification_counter]) 
													)
											)'+
										CASE WHEN @skipObjectsList IS NOT NULL
												THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
														AND ss.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
										ELSE N'' END
		ELSE
			/* SQL Server 2005 up to SQL Server 2008 R2 SP 2*/
			SET @queryToRun = @queryToRun + 
								CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
								SELECT DISTINCT
											DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''') AS [database_id]
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
								FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.stats ss WITH (NOLOCK)
								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.objects ob WITH (NOLOCK) ON ob.[object_id] = ss.[object_id]
								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.schemas sc WITH (NOLOCK) ON sc.[schema_id] = ob.[schema_id]
								INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '..' ELSE N'' END + N'sysindexes si WITH (NOLOCK) ON si.[id] = ob.[object_id] AND si.[name] = ss.[name]' + N'
								WHERE	ob.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
										AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + '''
										AND ob.[type] <> ''S''
										AND si.[rowcnt] > 0
										AND (    (      DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
													AND si.[rowmodctr] <> 0
													)
												OR  
												( 
													 	DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
													AND si.[rowmodctr] <> 0 
													AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@statsChangePercent AS [nvarchar](32)) + N'
												)
												OR  
													(
														DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N'
													AND si.[rowmodctr] <> 0
													AND SQRT(1000 * CAST(si.[rowcnt] AS [float])) <= ABS(si.[rowmodctr]) 
													)
										)' +
										CASE WHEN @skipObjectsList IS NOT NULL
												THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
														AND ss.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))
														AND si.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'
										ELSE N'' END

		IF @sqlServerName<>@@SERVERNAME
			SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'..sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithStatisticsList([database_id], [object_id], [table_schema], [table_name], [stats_id], [stats_name], [auto_created], [last_updated], [rows], [modification_counter], [percent_changes])
				EXEC sp_executesql  @queryToRun


		/* starting with SQL Server 2014, incremental statistics are available */
		IF @serverVersionNum >= 12
			begin
				SET @queryToRun=N'Create list of incremental statistics to be analyzed...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + 
									CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
									SELECT ss.[object_id], ss.[stats_id] 
									FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.stats ss WITH (NOLOCK)
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.objects ob WITH (NOLOCK) ON ob.[object_id] = ss.[object_id]
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.schemas sc WITH (NOLOCK) ON sc.[schema_id] = ob.[schema_id]
									WHERE	ob.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + '''
											AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + '''
											AND ob.[type] <> ''S''
											AND ss.[is_incremental] = 1';

				IF @sqlServerName<>@@SERVERNAME
					SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'..sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #incrementalStatisticsList([object_id], [stats_id])
						EXEC sp_executesql  @queryToRun

				DECLARE crsIncrementalStatisticsList CURSOR LOCAL FAST_FORWARD FOR	SELECT [object_id], [stats_id]
																					FROM #incrementalStatisticsList																	
				OPEN crsIncrementalStatisticsList
				FETCH NEXT FROM crsIncrementalStatisticsList INTO @ObjectID, @IndexID
				WHILE @@FETCH_STATUS = 0
					begin
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + 
											CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
											SELECT DB_ID() AS [database_id]
													, sp.[object_id]
													, sc.[name] AS [table_schema]
													, ob.[name] AS [table_name]
													, sp.[stats_id]
													, ss.[name] AS [stats_name]
													, ss.[auto_created]
													, sp.[last_updated]
													, sp.[rows]
													, ABS(sp.[modification_counter]) AS [modification_counter]
													, (ABS(sp.[modification_counter]) * 100. / CAST(sp.[rows] AS [float])) AS [percent_changes]
													, ss.[is_incremental]
													, sp.[partition_number]
											FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.stats ss WITH (NOLOCK)
											INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.objects ob WITH (NOLOCK) ON ob.[object_id] = ss.[object_id]
											INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.schemas sc WITH (NOLOCK) ON sc.[schema_id] = ob.[schema_id]
											INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_incremental_stats_properties(' + CAST(@ObjectID AS [varchar](32)) + N', ' + CAST(@IndexID AS [varchar](32)) + N') AS sp ON sp.[object_id] = ss.[object_id] AND sp.[stats_id] = ss.[stats_id]
											WHERE	sp.[rows] > 0
												AND (    (    DATEDIFF(dd, sp.[last_updated], GETDATE()) >= ' + CAST(@statsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0
														 )
													 OR  
														 ( 
															  DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0 
														  AND (ABS(sp.[modification_counter]) * 100. / CAST(sp.[rows] AS [float])) >= ' + CAST(@statsChangePercent AS [nvarchar](32)) + N'
														 )
													 OR  
													 	 (
															  DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@statsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0 
														  AND SQRT(1000 * CAST(sp.[rows] AS [float])) <= ABS(sp.[modification_counter]) 
														 )
													)'+
												CASE WHEN @skipObjectsList IS NOT NULL
													 THEN N'	AND ob.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '','')) 
																AND ss.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))' 
												ELSE N'' END


						IF @sqlServerName<>@@SERVERNAME
							SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'..sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'

						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						INSERT	INTO #databaseObjectsWithStatisticsList([database_id], [object_id], [table_schema], [table_name], [stats_id], [stats_name], [auto_created], [last_updated], [rows], [modification_counter], [percent_changes], [is_incremental], [partition_number])
								EXEC sp_executesql  @queryToRun

						FETCH NEXT FROM crsIncrementalStatisticsList INTO @ObjectID, @IndexID
					end
				CLOSE crsIncrementalStatisticsList
				DEALLOCATE crsIncrementalStatisticsList
			end


		--delete entries which should be excluded from current maintenance actions, as they are part of [maintenance-plan].[vw_objectSkipList]
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseObjectsWithStatisticsList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[table_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE osl.[instance_name] = @sqlServerName
								AND osl.[database_name] = @dbName
								AND osl.[active] = 1
								AND @flgActions & osl.[flg_actions] = osl.[flg_actions]'
		SET @queryParameters = '@sqlServerName [sysname], @dbName [sysname], @flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
														, @dbName = @dbName
														, @flgActions = @flgActions

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
						DELETE dtl
						FROM #databaseObjectsWithStatisticsList dtl
						INNER JOIN [maintenance-plan].[vw_objectSkipList] osl ON dtl.[table_schema] = osl.[schema_name] COLLATE DATABASE_DEFAULT
																				AND dtl.[stats_name] = osl.[object_name] COLLATE DATABASE_DEFAULT
						WHERE osl.[instance_name] = @sqlServerName
								AND osl.[database_name] = @dbName
								AND osl.[active] = 1
								AND @flgActions & osl.[flg_actions] = osl.[flg_actions]'
		SET @queryParameters = '@sqlServerName [sysname], @dbName [sysname], @flgActions [int]'
		EXEC sp_executesql @queryToRun, @queryParameters, @sqlServerName = @sqlServerName
														, @dbName = @dbName
														, @flgActions = @flgActions

		--delete entries which are for objects in "offline" filegroups
		DELETE dtl
		FROM #databaseObjectsWithStatisticsList dtl
		INNER JOIN #objectsInOfflineFileGroups oofg ON dtl.[object_id] = oofg.[object_id] 
													AND dtl.[stats_id] = oofg.[index_id]
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
		SET @queryToRun='Analyzing row-store indexes fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	WHERE [index_type] IN (1, 2, 3, 4)
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted') + CASE WHEN @IndexName IS NOT NULL THEN N' - ' + [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted') ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun=CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
									SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
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
											, ips.[partition_number]
									FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
													CASE WHEN @flgOptions & 1024 = 1024 THEN 'DETAILED' ELSE 'LIMITED' END 
											+ ''') ips
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
									WHERE	si.[type] IN (' + @analyzeIndexType + N')
											AND si.[is_disabled]=0'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
				INSERT	INTO #currentRowStoreFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count], [partition_number])  
						EXEC sp_executesql  @queryToRun

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
		INNER JOIN #currentRowStoreFragmentationStats cifs ON	cifs.[ObjectId] = doil.[object_id] 
															AND cifs.[IndexId] = doil.[index_id] 
															AND cifs.[partition_number] = doil.[partition_number]
	end

IF (@serverVersionNum >= 13) AND ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4))  AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Analyzing columnstore indexes fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT DISTINCT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	WHERE [index_type] IN (5, 6)
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted') + CASE WHEN @IndexName IS NOT NULL THEN N' - ' + [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted') ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun=CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
					SELECT    cc.object_id, cc.index_id, cc.partition_number, ps.page_count
							, cc.avg_fragmentation_in_percent, cc.deleted_segments_count, cc.segments_count
					FROM (
							SELECT    i.object_id
									, i.index_id
									, p.partition_number
									, 100.0 * (ISNULL(SUM(rgs.deleted_rows), 0)) / NULLIF(SUM(rgs.total_rows), 0) AS avg_fragmentation_in_percent
									, SUM(CASE rgs.deleted_rows WHEN rgs.total_rows THEN 1 ELSE 0 END ) AS deleted_segments_count
									, COUNT(*) AS segments_count
							FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.indexes AS i
							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.partitions AS p ON i.object_id = p.object_id
							INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_column_store_row_group_physical_stats AS rgs ON	i.object_id = rgs.object_id
																								AND i.index_id = rgs.index_id					
							WHERE rgs.state_desc = ''COMPRESSED''
								AND i.object_id = ' + CAST(@ObjectID AS [nvarchar]) + N'
								AND i.index_id = ' + CAST(@IndexID AS [nvarchar]) + N'
								AND i.[is_disabled]=0
							GROUP BY i.object_id, i.index_id, i.name, i.type_desc, p.partition_number
					) cc
					INNER JOIN
						(
							SELECT   object_id, index_id
									, COUNT(*) AS partitions_count
									, SUM(reserved_page_count) AS page_count
									, SUM(row_count) AS row_count
							FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.dm_db_partition_stats
							GROUP BY object_id, index_id
						) ps ON cc.object_id = ps.object_id and cc.index_id = ps.index_id'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				INSERT	INTO #currentColumnStoreFragmentationStats([object_id], [index_id], [partition_number], [page_count], [avg_fragmentation_in_percent], [deleted_segments_count], [segments_count])
						EXEC sp_executesql  @queryToRun

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[avg_fragmentation_in_percent]
				, doil.[page_count] = cifs.[page_count]
				, doil.[segments_count] = cifs.[segments_count]
				, doil.[deleted_segments_count] = cifs.[deleted_segments_count]
				, doil.[deleted_segments_percentage] = CASE WHEN cifs.[segments_count] > 0 THEN 100.0 * cifs.[deleted_segments_count] / cifs.[segments_count] ELSE 0 END
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #currentColumnStoreFragmentationStats cifs ON	cifs.[object_id] = doil.[object_id] 
																	AND cifs.[index_id] = doil.[index_id] 
																	AND cifs.[partition_number] = doil.[partition_number]
	end


--------------------------------------------------------------------------------------------------
-- 1	Defragmenting database tables indexes
--		All indexes with a fragmentation level between defrag and rebuild threshold will be reorganized
--------------------------------------------------------------------------------------------------		
IF ((@flgActions & 1 = 1) AND (@flgActions & 4 = 0)) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
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
																		OR (
																			/* for columnstore indexes */
																				doil.[index_type] IN (5, 6)
																			AND	doil.[deleted_segments_percentage] >= @defragIndexThreshold
																			AND doil.[deleted_segments_percentage] < @rebuildIndexThreshold
																		   )
																		)
															ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DECLARE crsIndexesToDegfragment CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[object_id], doil.[index_id]
																								, doil.[page_density_deviation], doil.[fill_factor]
																								, doil.[is_partitioned], doil.[partition_number]
																								, doil.[segments_count], doil.[deleted_segments_count], doil.[deleted_segments_percentage]																								
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
																							OR (
																								/* for columnstore indexes */
																									doil.[index_type] IN (5, 6)
																								AND	doil.[deleted_segments_percentage] >= @defragIndexThreshold
																								AND doil.[deleted_segments_percentage] < @rebuildIndexThreshold
																							   )
																							)																		
																				ORDER BY doil.[index_id]
				OPEN crsIndexesToDegfragment
				FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor, @isPartitioned, @partitionNumber, @CurrentSegmentCount, @CurrentDeletedSegmentCount, @CurentDeletedSegmentPercentage
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered rowstore' 
															WHEN 2 THEN 'Nonclustered rowstore' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
															WHEN 5 THEN 'Clustered columnstore'
															WHEN 6 THEN 'Nonclustered columnstore'
															WHEN 7 THEN 'Nonclustered hash'
											END
		   				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted') + ' => ' + CASE WHEN @isPartitioned = 1 THEN 'partition ' + CAST(@partitionNumber AS [varchar](32)) + N' / ' ELSE N'' END +
																										'current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + 
																										CASE WHEN @IndexType IN (1, 2, 3, 4) THEN ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) 
																											 WHEN @IndexType IN (5, 6) THEN ' / segments count = ' + CAST(@CurrentSegmentCount AS [varchar](32)) 
																										END + 																							+ 
																										' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						SET @objectName =  [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'quoted')
						SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted')

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						IF @IndexType IN (1, 2, 3, 4)
							SET @eventData='<index-fragmentation><detail>' + 
												'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
												'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
												'<index_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</index_name>' + 
												'<partition_number>' + CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
												'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
												'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
												'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
												'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
												'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
												'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
											'</detail></index-fragmentation>'
						IF @IndexType IN (5, 6)
							SET @eventData='<index-fragmentation><detail>' + 
												'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
												'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
												'<index_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</index_name>' + 
												'<partition_number>' + CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
												'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
												'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
												'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
												'<segment_count>' + CAST(@CurrentSegmentCount AS [varchar](32)) + '</segment_count>' + 
												'<deleted_segment_count>' + CAST(@CurrentDeletedSegmentCount AS [varchar](32)) + '</deleted_segment_count>' + 
												'<deleted_segment_percentage>' + CAST(@CurentDeletedSegmentPercentage AS [varchar](32)) + '</deleted_segment_percentage>' + 
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
						SET @nestExecutionLevel = @executionLevel + 3

						EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName			= @sqlServerName
																, @dbName					= @dbName
																, @tableSchema				= @CurrentTableSchema
																, @tableName				= @CurrentTableName
																, @indexName				= @IndexName
																, @indexID					= NULL
																, @partitionNumber			= @partitionNumber
																, @flgAction				= 2		--reorganize
																, @flgOptions				= @flgOptions
																, @maxDOP					= @maxDOP
																, @executionLevel			= @nestExecutionLevel
																, @affectedDependentObjects = @affectedDependentObjects OUT
																, @debugMode				= @debugMode

	   					FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor, @isPartitioned, @partitionNumber, @CurrentSegmentCount, @CurrentDeletedSegmentCount, @CurentDeletedSegmentPercentage
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
IF (@flgActions & 2 = 2) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
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
																		OR (
																			/* for columnstore indexes */
																				doil.[index_type] IN (5,6)
																			AND doil.[deleted_segments_percentage] >= @rebuildIndexThreshold
																		   )
																		)
															ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @ClusteredRebuildNonClustered = 0
				DECLARE crsIndexesToRebuild CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[index_id]
																							, doil.[page_density_deviation], doil.[fill_factor] 
																							, doil.[is_partitioned], doil.[partition_number]
																							, doil.[segments_count], doil.[deleted_segments_count], doil.[deleted_segments_percentage]
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
																						OR (
																							/* for columnstore indexes */
																								doil.[index_type] IN (5,6)
																							AND doil.[deleted_segments_percentage] >= @rebuildIndexThreshold
																						   )
																						)
																			ORDER BY doil.[index_id]

				OPEN crsIndexesToRebuild
				FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor, @isPartitioned, @partitionNumber, @CurrentSegmentCount, @CurrentDeletedSegmentCount, @CurentDeletedSegmentPercentage
				WHILE @@FETCH_STATUS = 0 AND @ClusteredRebuildNonClustered = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SELECT	@indexIsRebuilt = doil.[is_rebuilt]
						FROM	#databaseObjectsWithIndexList doil
						WHERE	doil.[table_schema] = @CurrentTableSchema 
		   						AND doil.[table_name] = @CurrentTableName
								AND doil.[index_id] = @IndexID
								AND doil.[partition_number] = @partitionNumber

						IF @indexIsRebuilt = 0
							begin
								SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
																	WHEN 1 THEN 'Clustered rowstore' 
																	WHEN 2 THEN 'Nonclustered rowstore' 
																	WHEN 3 THEN 'XML'
																	WHEN 4 THEN 'Spatial' 
																	WHEN 5 THEN 'Clustered columnstore'
																	WHEN 6 THEN 'Nonclustered columnstore'
																	WHEN 7 THEN 'Nonclustered hash'
													END
		   						SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted') + ' => ' + CASE WHEN @isPartitioned = 1 THEN 'partition ' + CAST(@partitionNumber AS [varchar](32)) + N' / ' ELSE N'' END +
																												'current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + 
																												CASE WHEN @IndexType IN (1, 2, 3, 4) THEN ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) 
																													 WHEN @IndexType IN (5, 6) THEN ' / segments count = ' + CAST(@CurrentSegmentCount AS [varchar](32)) 
																												END + 																							+ 
																												' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @objectName = [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'quoted')
								SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted')

								--------------------------------------------------------------------------------------------------
								--log index fragmentation information
								IF @IndexType IN (1, 2, 3, 4)
									SET @eventData='<index-fragmentation><detail>' + 
														'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
														'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
														'<index_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</index_name>' + 
														'<partition_number>' + CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
														'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
														'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
														'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
														'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
														'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
														'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
													'</detail></index-fragmentation>'
								IF @IndexType IN (5, 6)
									SET @eventData='<index-fragmentation><detail>' + 
														'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
														'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
														'<index_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</index_name>' + 
														'<partition_number>' + CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
														'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
														'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
														'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
														'<segment_count>' + CAST(@CurrentSegmentCount AS [varchar](32)) + '</segment_count>' + 
														'<deleted_segment_count>' + CAST(@CurrentDeletedSegmentCount AS [varchar](32)) + '</deleted_segment_count>' + 
														'<deleted_segment_percentage>' + CAST(@CurentDeletedSegmentPercentage AS [varchar](32)) + '</deleted_segment_percentage>' + 
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

								SET @nestExecutionLevel = @executionLevel + 3

								EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName			= @sqlServerName
																		, @dbName					= @dbName
																		, @tableSchema				= @CurrentTableSchema
																		, @tableName				= @CurrentTableName
																		, @indexName				= @IndexName
																		, @indexID					= NULL
																		, @partitionNumber			= @partitionNumber
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
												AND (   (@isPartitioned = 1 AND doil.[partition_number] = @partitionNumber)
														OR @isPartitioned = 0
													)
									end
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID
									AND doil.[partition_number] = @partitionNumber

	   					FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor, @isPartitioned, @partitionNumber, @CurrentSegmentCount, @CurrentDeletedSegmentCount, @CurentDeletedSegmentPercentage
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
IF (@flgActions & 4 = 4) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
	begin
		SET @queryToRun='Rebuilding database tables indexes  (all)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--minimizing the list of indexes to be rebuild:
		--4  - Rebuild all dependent indexes when rebuild primary indexes
		IF (@flgOptions & 4 = 4)
			begin
				SET @queryToRun=N'optimizing index list to be rebuild'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
					

				DECLARE crsClusteredIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT doil.[table_schema], doil.[table_name]
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
																					AND doil.[is_partitioned] = 0
																			ORDER BY doil.[table_schema], doil.[table_name]
				OPEN crsClusteredIndexes
				FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
	
						--mark indexes as rebuilt
						UPDATE doil	
							SET doil.[is_rebuilt]=1
						FROM #databaseObjectsWithIndexList doil
						WHERE   doil.[table_schema] = @CurrentTableSchema
								AND doil.[table_name] = @CurrentTableName
								AND CHARINDEX(CAST(doil.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
								AND doil.[index_type] NOT IN (0, 1)
										
						FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsClusteredIndexes
				DEALLOCATE crsClusteredIndexes						
			end

		--rebuilding indexes
		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[index_name], doil.[index_type], doil.[index_id]
																					, doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[fill_factor] 
																					, doil.[is_partitioned], doil.[partition_number]
																					, doil.[segments_count], doil.[deleted_segments_count], doil.[deleted_segments_percentage]
							   										FROM	#databaseObjectsWithIndexList doil
   																	WHERE	doil.[index_type] <> 0 /* heap tables will be excluded */
																			AND doil.[is_rebuilt]=0
																			AND doil.[page_count] >= @pageThreshold
																	ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]

		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor, @isPartitioned, @partitionNumber, @CurrentSegmentCount, @CurrentDeletedSegmentCount, @CurentDeletedSegmentPercentage
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
							AND doil.[partition_number] = @partitionNumber

				IF @indexIsRebuilt = 0
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered rowstore' 
															WHEN 2 THEN 'Nonclustered rowstore' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
															WHEN 5 THEN 'Clustered columnstore'
															WHEN 6 THEN 'Nonclustered columnstore'
															WHEN 7 THEN 'Nonclustered hash'
											END

						--analyze curent object
						SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted') + ' => ' + CASE WHEN @isPartitioned = 1 THEN 'partition ' + CAST(@partitionNumber AS [varchar](32)) + N' / ' ELSE N'' END +
																										'current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + 
																										CASE WHEN @IndexType IN (1, 2, 3, 4) THEN ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) 
																											 WHEN @IndexType IN (5, 6) THEN ' / segments count = ' + CAST(@CurrentSegmentCount AS [varchar](32)) 
																										END + 																							+ 
																										' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @objectName = [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'quoted')
						SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted')

						IF @IndexType IN (1, 2, 3, 4)
							SET @eventData='<index-fragmentation><detail>' + 
												'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
												'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
												'<index_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</index_name>' + 
												'<partition_number>' +  CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
												'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
												'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
												'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
												'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
												'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
												'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
											'</detail></index-fragmentation>'
						IF @IndexType IN (5, 6)
							SET @eventData='<index-fragmentation><detail>' + 
												'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
												'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
												'<index_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</index_name>' + 
												'<partition_number>' + CAST(@partitionNumber AS [varchar](32)) + '</partition_number>' + 
												'<is_partitioned>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_partitioned>' + 
												'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
												'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
												'<segment_count>' + CAST(@CurrentSegmentCount AS [varchar](32)) + '</segment_count>' + 
												'<deleted_segment_count>' + CAST(@CurrentDeletedSegmentCount AS [varchar](32)) + '</deleted_segment_count>' + 
												'<deleted_segment_percentage>' + CAST(@CurentDeletedSegmentPercentage AS [varchar](32)) + '</deleted_segment_percentage>' + 
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
						SET @nestExecutionLevel = @executionLevel + 3
						EXEC [dbo].[usp_mpAlterTableIndexes]	  @sqlServerName			= @sqlServerName
																, @dbName					= @dbName
																, @tableSchema				= @CurrentTableSchema
																, @tableName				= @CurrentTableName
																, @indexName				= @IndexName
																, @indexID					= NULL
																, @partitionNumber			= @partitionNumber
																, @flgAction				= 1		--rebuild
																, @flgOptions				= @flgOptions
																, @maxDOP					= @maxDOP
																, @executionLevel			= @nestExecutionLevel
																, @affectedDependentObjects = @affectedDependentObjects OUT
																, @debugMode				= @debugMode
						--enable foreign key
						IF @IndexType=1
							begin
									EXEC [dbo].[usp_mpAlterTableForeignKeys]  @sqlServerName	= @sqlServerName
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
										AND (   (@isPartitioned = 1 AND doil.[partition_number] = @partitionNumber)
												OR @isPartitioned = 0
											)
							end

						--mark index as being rebuilt
						UPDATE doil
							SET [is_rebuilt]=1
						FROM	#databaseObjectsWithIndexList doil 
	   					WHERE	doil.[table_name] = @CurrentTableName
	   							AND doil.[table_schema] = @CurrentTableSchema
								AND doil.[index_id] = @IndexID
								AND doil.[partition_number] = @partitionNumber
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor, @isPartitioned, @partitionNumber, @CurrentSegmentCount, @CurrentDeletedSegmentCount, @CurentDeletedSegmentPercentage
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4	/ 16 
--------------------------------------------------------------------------------------------------
IF (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
	begin
		IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
		begin
			SET @nestExecutionLevel = @executionLevel + 1
			EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestExecutionLevel, 
																@debugMode		= @debugMode
		end

		--perform internal cleanup
		EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-rebuild',
												@flgOperation		= 2,
												@server_name		= @sqlServerName,
												@database_name		= @dbName,
												@schema_name		= @tableSchema,
												@object_name		= @tableName,
												@child_object_name	= '%'
	end

--------------------------------------------------------------------------------------------------
--cleanup of ghost records (sp_clean_db_free_space) (starting SQL Server 2005 SP3)
--exclude indexes which got rebuilt or reorganized, since ghost records were already cleaned
--------------------------------------------------------------------------------------------------
IF (@serverVersionNum >= 9.04035 AND @flgOptions & 65536 = 65536) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
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

					SET @objectName = [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
					EXEC sp_clean_db_free_space @objectName
				end


--------------------------------------------------------------------------------------------------
--8  - Update statistics for all tables in database
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0
	begin
		SET @queryToRun=N'Update statistics for all tables (' + 
					CASE WHEN @statsSamplePercent =   0 THEN 'default sample'
						 WHEN @statsSamplePercent < 100 THEN 'sample ' + CAST(@statsSamplePercent AS [nvarchar]) + ' percent'
						 ELSE 'fullscan'
					END + ')...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--remove tables with clustered indexes already rebuild
		SET @queryToRun=N'optimizing list (1)'
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

		/* for incremental statistics, keep only partition related changes */
		IF @serverVersionNum >= 12
			begin
				SET @queryToRun=N'optimizing list (3)'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DELETE dowsl
				FROM #databaseObjectsWithStatisticsList	dowsl
				WHERE [is_incremental] = 0
						AND EXISTS(
									SELECT 1
									FROM #databaseObjectsWithStatisticsList x
									WHERE	x.[database_id] = dowsl.[database_id]
											AND x.[object_id] = dowsl.[object_id]
											AND x.[stats_id] = dowsl.[stats_id]
											AND x.[is_incremental] = 1
								  )
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
				SET @queryToRun= [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @IndexID=1
				DECLARE crsTableStatsList CURSOR LOCAL FAST_FORWARD FOR	SELECT	  [stats_name], [auto_created], [rows], [modification_counter], [last_updated], [percent_changes]
																				, DATEDIFF(dd, [last_updated], GETDATE()) AS [stats_age]
																				, [is_incremental], [partition_number]
																		FROM	#databaseObjectsWithStatisticsList	
																		WHERE	[table_schema] = @CurrentTableSchema
																				AND [table_name] = @CurrentTableName
																		ORDER BY [stats_name]
				OPEN crsTableStatsList
				FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge, @isPartitioned, @partitionNumber
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=CAST(@IndexID AS [nvarchar](64)) + '/' + CAST(@statsCount AS [nvarchar](64)) + ' - ' + [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted') + ' : ' + CASE WHEN @isPartitioned = 1 THEN ' partition ' + CAST(@partitionNumber AS [varchar](32)) + N' / ' ELSE N'' END + 'age = ' + CAST(@statsAge AS [varchar](32)) + ' days / rows = ' + CAST(@tableRows AS [varchar](32)) + ' / changes = ' + CAST(@statsModificationCounter AS [varchar](32))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log statistics information
						SET @objectName = [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](RTRIM(@CurrentTableName), 'quoted')
						SET @childObjectName = [dbo].[ufn_getObjectQuoteName](@IndexName, 'quoted')

						SET @eventData='<statistics-health><detail>' + 
											'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
											'<object_name>' + [dbo].[ufn_getObjectQuoteName](@objectName, 'xml') + '</object_name>'+ 
											'<stats_name>' + [dbo].[ufn_getObjectQuoteName](@childObjectName, 'xml') + '</stats_name>' + 
											'<auto_created>' + CAST(@statsAutoCreated AS [varchar](32)) + '</auto_created>' + 
											'<is_incremental>' + CASE WHEN @isPartitioned = 1 THEN 'Yes' ELSE 'No' END + '</is_incremental>' + 
											'<partition_number>' +  CASE WHEN @partitionNumber IS NOT NULL THEN CAST(@partitionNumber AS [varchar](32)) ELSE '' END + '</partition_number>' + 
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
						SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted') + ''') IS NOT NULL UPDATE STATISTICS ' + [dbo].[ufn_getObjectQuoteName](@CurrentTableSchema, 'quoted') + '.' + [dbo].[ufn_getObjectQuoteName](@CurrentTableName, 'quoted') + '(' + dbo.ufn_getObjectQuoteName(@IndexName, 'quoted') + ')'
						
						IF @isPartitioned = 1  
							SET @queryToRun=@queryToRun + N' WITH RESAMPLE ON PARTITIONS (' + CAST(@partitionNumber AS [varchar](32)) + N')'
						ELSE
							IF @statsSamplePercent > 0 AND @statsSamplePercent < 100
								SET @queryToRun=@queryToRun + N' WITH SAMPLE ' + CAST(@statsSamplePercent AS [nvarchar]) + N' PERCENT'
							ELSE
								IF @statsSamplePercent = 100
									SET @queryToRun=@queryToRun + N' WITH FULLSCAN'

						/* starting with SQL Server 2017 CU3 and SQL Server 2016 SP2
							MAXDOP option is available for UPDATE STATISTICS: https://docs.microsoft.com/en-us/sql/t-sql/statements/update-statistics-transact-sql?view=sql-server-2017 
						*/
						IF @serverVersionNum >= 14.03015 OR (@serverVersionNum >= 13.05026 AND @serverVersionNum < 14)
							begin
								IF CHARINDEX(' WITH ', @queryToRun) = 0
									SET @queryToRun = @queryToRun + ' WITH '
								ELSE
									SET @queryToRun = @queryToRun + ','
								SET @queryToRun = @queryToRun + ' MAXDOP = ' + CAST(@maxDOP AS [nvarchar])
							end
					
						SET @nestedExecutionLevel = @executionLevel + 2
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

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
						FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge, @isPartitioned, @partitionNumber
					end
				CLOSE crsTableStatsList
				DEALLOCATE crsTableStatsList

				FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
			end
		CLOSE crsTableList2
		DEALLOCATE crsTableList2
	end

--------------------------------------------------------------------------------------------------
--8  - create statistics for all tables in database
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit) AND @dbIsReadOnly = 0 AND (@flgOptions & 128 = 128 OR @flgOptions & 131072 = 131072)
	AND @isAzureSQLDatabase = 0
	begin
		SET @queryToRun=N'Creating statistics for all tables / ' + CASE WHEN @flgOptions & 128 = 128 THEN 'index' ELSE 'all' END + ' columns ...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE @missingColumnStatsCounter [int]

		/* detect if sp_createstats should be executed: check for columns without histograms */
		IF object_id('tempdb..#checkMissingColumnStatistics') IS NOT NULL 
		DROP TABLE #checkMissingColumnStatistics

		CREATE TABLE #checkMissingColumnStatistics
			(
				[counter]		[int]
			)

		SET @queryToRun = N''				
		SET @queryToRun = @queryToRun + 
							CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
							SELECT COUNT(*) AS MissingColumnStats
							FROM (
									SELECT DISTINCT so.[name] AS [table_name], sc.[name] AS [schema_name]
									FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.objects so WITH (READPAST)
									INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.columns sc WITH (READPAST) ON sc.[object_id] = so.[object_id] 
									' + CASE WHEN @flgOptions & 128 = 128 THEN 'INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.indexes si WITH (READPAST) ON si.[object_id] = so.[object_id] ' ELSE N'' END + N'
									' + CASE WHEN @flgOptions & 128 = 128 THEN 'INNER JOIN ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.index_columns sic WITH (READPAST) ON sic.[object_id] = so.[object_id] AND sc.[column_id] = sic.[column_id] AND sic.[index_id] = si.[index_id]' ELSE N'' END + N'
									LEFT JOIN 
										(
											SELECT [object_id], [column_id]
											FROM	' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '.' ELSE N'' END + N'sys.stats_columns WITH (READPAST)
											WHERE	[stats_column_id] = 1
										)ssc ON ssc.[object_id] = so.[object_id] AND sc.[column_id] = ssc.[column_id]
									WHERE  so.[type] IN (''U'', ''IT'')
										' + CASE WHEN @flgOptions & 128 = 128 THEN 'AND not (si.[is_disabled] = 1 OR si.[type] IN (5,6)) AND sic.[key_ordinal] <> 1 AND sic.[is_included_column] = 0' ELSE '' END + N'
										AND (TYPE_NAME(sc.[system_type_id]) NOT IN (''xml'')) ' + 
									CASE WHEN @serverVersionNum >= 10 
										THEN '
										AND (OBJECTPROPERTY(so.[object_id], ''tablehascolumnset'') = 0 or sc.[is_sparse]=0)  
										AND (sc.[is_filestream] = 0)  
										AND (	sc.[is_computed] = 0  
												OR (	 sc.[is_computed] = 1   
													AND COLUMNPROPERTY(so.[object_id], sc.[name], ''isdeterministic'') = 1 
													AND COLUMNPROPERTY(so.[object_id], sc.[name], ''isprecise'') = 1
												)
											) '
										ELSE ''
									END + N'
										AND ssc.[object_id] IS NULL
										AND so.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + N'''
										AND sc.[name] LIKE ''' + [dbo].[ufn_getObjectQuoteName](@tableSchema, 'sql') + N'''' + 
										CASE WHEN @skipObjectsList IS NOT NULL
												THEN N'	AND so.[name] NOT IN (SELECT [value] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.[dbo].[ufn_getTableFromStringList](''' + @skipObjectsList + N''', '',''))'  
												ELSE N'' 
										END + N'
									)X'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				
		INSERT	INTO #checkMissingColumnStatistics([counter])
				EXEC sp_executesql  @queryToRun
				
		SELECT @missingColumnStatsCounter = [counter] 
		FROM #checkMissingColumnStatistics


		/* detect if sp_createstats should be executed: check for objects in "offline" filegroups */
		IF EXISTS(SELECT * FROM #objectsInOfflineFileGroups)
				SET @missingColumnStatsCounter = 0
				
		SET @queryToRun = N''
		IF @missingColumnStatsCounter > 0
			begin
				--128 - Create statistics on index columns only (default). Default behaviour is to not create statistics on all eligible columns
				IF @flgOptions & 128 = 128 
					SET @queryToRun = @queryToRun + N'sp_createstats @indexonly = ''indexonly'''
				ELSE
					--131072  - Create statistics on all eligible column
					IF @flgOptions & 131072 = 131072 
						SET @queryToRun = @queryToRun + N'sp_createstats @indexonly = ''NO'''

				--256  - Create statistics using default sample scan. Default behaviour is to create statistics using fullscan mode
				IF @flgOptions & 256 = 256
					SET @queryToRun = @queryToRun + N', @fullscan = ''NO'''
				ELSE
					SET @queryToRun = @queryToRun + N', @fullscan = ''fullscan'''

				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

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
IF object_id('tempdb..#currentRowStoreFragmentationStats') IS NOT NULL DROP TABLE #currentRowStoreFragmentationStats
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 	DROP TABLE #databaseObjectsWithIndexList

--------------------------------------------------------------------------------------------------
--additional maintenance, if selected: backup transaction log and shrink log file to reclaim disk space
--------------------------------------------------------------------------------------------------
-- 262144	- take a log backup at the end of the optimization process
IF @flgOptions & 262144 = 262144
	begin
		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_mpDatabaseBackup]	@sqlServerName			= @sqlServerName,
											@dbName					= @dbName,
											@backupLocation			= DEFAULT,
											@flgActions				= 4, /* perform transaction log backup */
											@flgOptions				= DEFAULT,
											@retentionDays			= DEFAULT,
											@dataChangesThreshold	= DEFAULT,
											@executionLevel			= @nestedExecutionLevel,
											@debugMode				= DEFAULT
	end

-- 524288	- perform a shrink with truncate_only on the log file at the end of the optimization process
IF @flgOptions & 524288 = 524288
	begin
		SET @nestedExecutionLevel = @executionLevel
		EXEC [dbo].[usp_mpDatabaseShrink]	@sqlServerName		= @sqlServerName,
											@dbName				= @dbName,
											@flgActions			= 1, /* shrink log file */
											@flgOptions			= 3, /*	use truncate only and wait when in AG for recovery_time=0 */		
											@executionLevel		= @nestedExecutionLevel,
											@debugMode			= DEFAULT
	end

RETURN @errorCode
GO
