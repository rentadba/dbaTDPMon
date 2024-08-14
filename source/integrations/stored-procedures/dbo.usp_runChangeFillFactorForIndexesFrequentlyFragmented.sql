RAISERROR('Create procedure: [dbo].[usp_runChangeFillFactorForIndexesFrequentlyFragmented]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_runChangeFillFactorForIndexesFrequentlyFragmented]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_runChangeFillFactorForIndexesFrequentlyFragmented]
GO

CREATE PROCEDURE [dbo].[usp_runChangeFillFactorForIndexesFrequentlyFragmented]
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

DECLARE	@minimumIndexMaintenanceFrequencyDays	[tinyint],
		@analyzeOnlyMessagesFromTheLastHours	[tinyint],
		@analyzeIndexMaintenanceOperation		[nvarchar](128),
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

DECLARE crsFrequentlyFragmentedIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT    [instance_name], [database_name], [object_name]
																				, [index_name]
																				, CASE WHEN [fill_factor]=0 THEN 100 ELSE [fill_factor] END AS [fill_factor]
																				, [index_type]
																		FROM	[dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectCode, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, @analyzeIndexMaintenanceOperation)
																		ORDER BY [instance_name], [database_name], [object_name], [index_name]
OPEN crsFrequentlyFragmentedIndexes
FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @objectName, @indexName, @fillFactor, @indexType
WHILE @@FETCH_STATUS=0
	begin
		--analyze curent object
		SET @queryToRun=N'instance=[' + @instanceName + '], database=' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + ', table-name=' + @objectName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		SET @queryToRun=N'index-name=' + @indexName + ', type=' + [dbo].[ufn_getObjectQuoteName](@indexType, 'quoted') + ', current/saved fill-factor=' + CAST(@fillFactor AS [nvarchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

		SET @tableSchema = REPLACE(REPLACE(SUBSTRING(@objectName, 1, CHARINDEX('].[', @objectName)), ']', ''), '[', '')
		SET @tableName = REPLACE(REPLACE(SUBSTRING(@objectName, CHARINDEX('].[', @objectName)+2, LEN(@objectName)), ']', ''), '[', '')
		SET @indexName = REPLACE(REPLACE(@indexName, ']', ''), '[', '') 
		
		SET @newFillFactor = @fillFactor-@dropFillFactorByPercent
		IF  @newFillFactor >= @minFillFactorAcceptedLevel
			begin
				SET @queryToRun=N'lowering fill-factor by ' + CAST(@dropFillFactorByPercent AS [nvarchar]) + ', new fill-factor value=' + CAST(@newFillFactor AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				---------------------------------------------------------------------------------------------
				SET @nestExecutionLevel = @executionLevel + 3

				---------------------------------------------------------------------------------------------
				EXEC [dbo].[usp_mpAlterTableIndexes]	@sqlServerName				= @instanceName,
														@dbName						= @databaseName,
														@tableSchema				= @tableSchema,
														@tableName					= @tableName,
														@indexName					= @indexName,
														@indexID					= NULL,
														@partitionNumber			= 1,
														@flgAction					= 1,
														@flgOptions					= DEFAULT,
														@maxDOP						= DEFAULT,
														@fillFactor					= @newFillFactor,
														@executionLevel				= 0,
														@affectedDependentObjects	= @affectedDependentObjects OUTPUT,
														@debugMode					= @debugMode
			end
		ELSE
			begin
				SET @queryToRun=N'fill factor will not be lowered, since it will be under acceptable limit = ' + CAST(@minFillFactorAcceptedLevel AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
			end
		FETCH NEXT FROM crsFrequentlyFragmentedIndexes INTO @instanceName, @databaseName, @objectName, @indexName, @fillFactor, @indexType
	end
CLOSE crsFrequentlyFragmentedIndexes
DEALLOCATE crsFrequentlyFragmentedIndexes
GO
