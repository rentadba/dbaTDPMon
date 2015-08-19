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
		@nestExecutionLevel						[tinyint]
		
				
-----------------------------------------------------------------------------------------------------
--reading report options
SELECT	@minimumIndexMaintenanceFrequencyDays = [value]
FROM	[dbo].[reportHTMLOptions]
WHERE	[name] = N'Minimum Index Maintenance Frequency (days)'
		AND [report_type_id]=0

SET @minimumIndexMaintenanceFrequencyDays = ISNULL(@minimumIndexMaintenanceFrequencyDays, 2)

-----------------------------------------------------------------------------------------------------
SELECT	@analyzeOnlyMessagesFromTheLastHours = [value]
FROM	[dbo].[reportHTMLOptions]
WHERE	[name] = N'Analyze Only Messages from the last hours'
		AND [report_type_id]=0

SET @analyzeOnlyMessagesFromTheLastHours = ISNULL(@analyzeOnlyMessagesFromTheLastHours, 24)
	
-----------------------------------------------------------------------------------------------------
SELECT	@analyzeIndexMaintenanceOperation = [value]
FROM	[dbo].[reportHTMLOptions]
WHERE	[name] = N'Analyze Index Maintenance Operation'
		AND [report_type_id]=0


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

				SET @nestExecutionLevel = @executionLevel + 3
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
				SET @queryToRun=N'fill factor will not be lowered, since it will be under acceptable limit = ' + CAST(@minFillFactorAcceptedLevel AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
			end
		FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @tableSchema, @tableName, @indexName, @fillFactor, @indexType
	end
CLOSE crsFrequentlyFragmentedIndexes
DEALLOCATE crsFrequentlyFragmentedIndexes
GO
