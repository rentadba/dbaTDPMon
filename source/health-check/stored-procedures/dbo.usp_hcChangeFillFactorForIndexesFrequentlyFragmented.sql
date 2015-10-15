RAISERROR('Create procedure: [dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]
GO

CREATE PROCEDURE [dbo].[usp_hcChangeFillFactorForIndexesFrequentlyFragmented]
		@projectCode				[varchar](32)=NULL,
		@dropFillFactorByPercent	[tinyint] = 5,
		@minFillFactorAcceptedLevel	[tinyint] = 50,
		@executionLevel				[tinyint] = 0,
		@debugMode					[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.08.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE	@minimumIndexMaintenanceFrequencyDays	[tinyint] = 2,
		@analyzeOnlyMessagesFromTheLastHours	[tinyint] = 24 ,
		@analyzeIndexMaintenanceOperation		[nvarchar](128) = 'REBUILD',
		@affectedDependentObjects				[nvarchar](max),
		@instanceName							[sysname],
		@databaseName							[sysname],
		@tableSchema							[sysname],
		@tableName								[sysname],
		@indexName								[sysname],
		@fillFactor								[tinyint],
		@newFillFactor							[tinyint],
		@indexType								[sysname],
		@queryToRun								[nvarchar](max),
		@nestExecutionLevel						[tinyint],
		@errorCode								[int],
		@objectName								[nvarchar](512),
		@childObjectName						[sysname]
		
				
-----------------------------------------------------------------------------------------------------
--reading report options
SELECT	@minimumIndexMaintenanceFrequencyDays = [value]
FROM	[report].[htmlOptions]
WHERE	[name] = N'Minimum Index Maintenance Frequency (days)'
		AND [module] = 'health-check'

SET @minimumIndexMaintenanceFrequencyDays = ISNULL(@minimumIndexMaintenanceFrequencyDays, 2)

-----------------------------------------------------------------------------------------------------
SELECT	@analyzeOnlyMessagesFromTheLastHours = [value]
FROM	[report].[htmlOptions]
WHERE	[name] = N'Analyze Only Messages from the last hours'
		AND [module] = 'health-check'

SET @analyzeOnlyMessagesFromTheLastHours = ISNULL(@analyzeOnlyMessagesFromTheLastHours, 24)
	
-----------------------------------------------------------------------------------------------------
SELECT	@analyzeIndexMaintenanceOperation = [value]
FROM	[report].[htmlOptions]
WHERE	[name] = N'Analyze Index Maintenance Operation'
		AND [module] = 'health-check'


-----------------------------------------------------------------------------------------------------
SET @queryToRun=N'Analyzing event messages for frequently fragmented indexes...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DECLARE crsFrequentlyFragmentedIndexes CURSOR READ_ONLY FAST_FORWARD FOR	SELECT    [instance_name], [database_name]
																					, REPLACE(REPLACE(SUBSTRING([object_name], 1, CHARINDEX('].[', [object_name])), ']', ''), '[', '')						AS [schema_name]
																					, REPLACE(REPLACE(SUBSTRING([object_name], CHARINDEX('].[', [object_name])+2, LEN([object_name])), ']', ''), '[', '')	AS [table_name]
																					, REPLACE(REPLACE([index_name], ']', ''), '[', '') AS [index_name]
																					, CASE WHEN [fill_factor]=0 THEN 100 ELSE [fill_factor] END AS [fill_factor]
																					, [index_type]
																			FROM	[dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectCode, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, @analyzeIndexMaintenanceOperation)
																			ORDER BY [instance_name], [database_name], [schema_name], [table_name], [index_name]
OPEN crsFrequentlyFragmentedIndexes
FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @tableSchema, @tableName, @indexName, @fillFactor, @indexType
WHILE @@FETCH_STATUS=0
	begin
		--analyze curent object
		SET @queryToRun=N'instance=[' + @instanceName + '], database=[' + @databaseName + '], table-name=[' + @tableSchema + '].[' + @tableName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		SET @queryToRun=N'index-name=[' + @indexName + '], type=[' + @indexType + '], current fill-factor= ' + CAST(@fillFactor AS [nvarchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
			
		SET @newFillFactor = @fillFactor-@dropFillFactorByPercent
		IF  @newFillFactor >= @minFillFactorAcceptedLevel
			begin
				SET @queryToRun=N'lowering fill-factor by ' + CAST(@dropFillFactorByPercent AS [nvarchar]) + ', new fill-factor value=' + CAST(@newFillFactor AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				---------------------------------------------------------------------------------------------
				--get destination server running version/edition
				DECLARE		@serverEdition					[sysname],
							@serverVersionStr				[sysname],
							@serverVersionNum				[numeric](9,6),
							@nestedExecutionLevel			[tinyint]

				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @instanceName,
														@serverEdition			= @serverEdition OUT,
														@serverVersionStr		= @serverVersionStr OUT,
														@serverVersionNum		= @serverVersionNum OUT,
														@executionLevel			= @nestedExecutionLevel,
														@debugMode				= @debugMode

				SET @nestExecutionLevel = @executionLevel + 3

				---------------------------------------------------------------------------------------------
				IF @serverVersionNum>=9
					begin
						EXEC [dbo].[usp_mpAlterTableIndexes]	@SQLServerName				= @instanceName,
																@DBName						= @databaseName,
																@TableSchema				= @tableSchema,
																@TableName					= @tableName,
																@IndexName					= @indexName,
																@IndexID					= NULL,
																@PartitionNumber			= 1,
																@flgAction					= 1,
																@flgOptions					= DEFAULT,
																@MaxDOP						= DEFAULT,
																@FillFactor					= @newFillFactor,
																@executionLevel				= 0,
																@affectedDependentObjects	= @affectedDependentObjects OUTPUT,
																@DebugMode					= @debugMode
					end
				ELSE
					begin
						SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
						SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @tableSchema + '].[' + @tableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @tableSchema + '].[' + @tableName + ']' + ''', ''' + RTRIM(@indexName) + ''', ' + CAST(@newFillFactor AS [nvarchar]) + N') WITH NO_INFOMSGS'
						IF @debugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @objectName = '[' + @tableSchema + '].[' + RTRIM(@tableName) + ']'
						SET @childObjectName = QUOTENAME(@indexName)

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @instanceName,
																		@dbName			= @databaseName,
																		@objectName		= @objectName,
																		@childObjectName= @childObjectName,
																		@module			= 'dbo.usp_hcChangeFillFactorForIndexesFrequentlyFragmented',
																		@eventName		= 'database maintenance - rebuilding index',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= 0,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode						
					end
					
			end
		ELSE
			begin
				SET @queryToRun=N'fill factor will not be lowered, since it will be under acceptable limit = ' + CAST(@minFillFactorAcceptedLevel AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
			end
		FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @tableSchema, @tableName, @indexName, @fillFactor, @indexType
	end
CLOSE crsFrequentlyFragmentedIndexes
DEALLOCATE crsFrequentlyFragmentedIndexes
GO
