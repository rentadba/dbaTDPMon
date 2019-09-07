RAISERROR('Create procedure: [dbo].[usp_getSQLServerVersion]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_getSQLServerVersion]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_getSQLServerVersion]
GO

CREATE PROCEDURE [dbo].[usp_getSQLServerVersion]
		@sqlServerName			[sysname],
		@serverEdition			[sysname]		OUT,
		@serverVersionStr		[sysname]		OUT,
		@serverVersionNum		[numeric](9,6)	OUT,
		@serverEngine			[int]			OUT,
		@executionLevel			[tinyint] = 0,
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 04.03.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON
-----------------------------------------------------------------------------------------
DECLARE @queryToRun [nvarchar](2000)

-----------------------------------------------------------------------------------------
/* get SQL Server Edition and Product Version */
-----------------------------------------------------------------------------------------
BEGIN TRY
	SELECT    @serverEdition    = [edition]
			, @serverVersionStr = [version]
			, @serverVersionNum = SUBSTRING([version], 1, CHARINDEX('.', [version])-1) + '.' + REPLACE(SUBSTRING([version], CHARINDEX('.', [version])+1, LEN([version])), '.', '')
			, @serverEngine		= [engine]
	FROM [dbo].[vw_catalogInstanceNames]
	WHERE [instance_name] = @sqlServerName
END TRY
BEGIN CATCH
	SELECT    @serverEdition    = NULL
			, @serverVersionStr = NULL
			, @serverVersionNum = NULL
END CATCH

IF @serverEdition IS NULL
	begin
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + 'SELECT CAST(SERVERPROPERTY(''Edition'') AS [sysname]) AS [edition],
												CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]) AS [product_version],
												CAST(SERVERPROPERTY(''EngineEdition'') AS [int]) AS [engine]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		IF object_id('#serverProperty') IS NOT NULL DROP TABLE #serverProperty
		CREATE TABLE #serverProperty 
			(
				[edition]			[sysname]
			  , [product_version]	[sysname]
			  , [engine]			[int]
			)

		INSERT	INTO #serverProperty([edition], [product_version], [engine])
				EXEC sp_executesql  @queryToRun

		SELECT    @serverEdition = [edition] 
				, @serverVersionStr = [product_version]
				, @serverEngine = [engine]
		FROM #serverProperty

		SET @serverVersionNum=SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1) + '.' + REPLACE(SUBSTRING(@serverVersionStr, CHARINDEX('.', @serverVersionStr)+1, LEN(@serverVersionStr)), '.', '')
	end
-----------------------------------------------------------------------------------------
GO
