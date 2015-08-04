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
				[value]			[sysname]	NULL
			)

-----------------------------------------------------------------------------------------
/* get SQL Server Edition */
-----------------------------------------------------------------------------------------
SET @queryToRun = N''
SET @queryToRun = @queryToRun + 'SELECT CAST(SERVERPROPERTY(''Edition'') AS [sysname])'
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

INSERT	INTO #serverProperty([value])
		EXEC (@queryToRun)

SELECT @serverEdition = [value] 
FROM #serverProperty

-----------------------------------------------------------------------------------------
/* get SQL Server Version */
-----------------------------------------------------------------------------------------
SET @queryToRun = N''
SET @queryToRun = @queryToRun + 'SELECT CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname])'
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #serverProperty
INSERT	INTO #serverProperty([value])
		EXEC (@queryToRun)

SELECT @serverVersionStr = [value] 
FROM #serverProperty

SET @serverVersionNum=SUBSTRING(@serverVersionStr, 1, CHARINDEX('.', @serverVersionStr)-1) + '.' + REPLACE(SUBSTRING(@serverVersionStr, CHARINDEX('.', @serverVersionStr)+1, LEN(@serverVersionStr)), '.', '')
GO
