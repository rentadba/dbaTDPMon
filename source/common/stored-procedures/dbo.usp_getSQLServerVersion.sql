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

IF object_id('#serverProperty') IS NOT NULL DROP TABLE #serverProperty
CREATE TABLE #serverProperty
			(
				[edition]			[sysname]	NULL,
				[product_version]	[sysname]	NULL
			)

/* cache results for maximum 60 minutes */
IF OBJECT_ID('tempdb..##tdp_sql_version_requests') IS NULL
	BEGIN TRY
		CREATE TABLE ##tdp_sql_version_requests
			(
				  [instance_name]				[sysname]	NOT NULL
				, [edition]						[sysname]	NULL
				, [product_version]				[sysname]	NULL
				, [product_version_num]			[numeric](9,6) NULL
				, [event_date_utc]				[datetime]	NULL
			)
	END TRY
	BEGIN CATCH
		/* in a high concurency environment, the above table creation may fail. ignore the error */
		SET @queryToRun = ERROR_MESSAGE()
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
	END CATCH

-----------------------------------------------------------------------------------------
/* get SQL Server Edition and Product Version */
-----------------------------------------------------------------------------------------
SELECT    @serverEdition    = [edition]
		, @serverVersionStr = [product_version]
		, @serverVersionNum = [product_version_num]
FROM ##tdp_sql_version_requests
WHERE [instance_name] = @sqlServerName

--if data was not found in cache
IF @serverEdition IS NULL
	begin

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + 'SELECT CAST(SERVERPROPERTY(''Edition'') AS [sysname]) AS [edition],
												CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]) AS [product_version]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #serverProperty([edition], [product_version])
				EXEC sp_executesql  @queryToRun

		SELECT    @serverEdition = [edition] 
				, @serverVersionStr = [product_version] 
		FROM #serverProperty

		SET @serverVersionNum=SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1) + '.' + REPLACE(SUBSTRING(@serverVersionStr, CHARINDEX('.', @serverVersionStr)+1, LEN(@serverVersionStr)), '.', '')

		INSERT	INTO ##tdp_sql_version_requests([instance_name], [edition], [product_version], [product_version_num], [event_date_utc])
				SELECT @sqlServerName, @serverEdition, @serverVersionStr, @serverVersionNum, GETUTCDATE()
	end

--purge old cache values
DELETE FROM ##tdp_sql_version_requests
WHERE DATEDIFF(minute, [event_date_utc], GETUTCDATE()) > 60
-----------------------------------------------------------------------------------------
GO
