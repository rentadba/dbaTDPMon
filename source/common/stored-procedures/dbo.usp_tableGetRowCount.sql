RAISERROR('Create procedure: [dbo].[usp_tableGetRowCount]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_tableGetRowCount]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_tableGetRowCount]
GO

CREATE PROCEDURE [dbo].[usp_tableGetRowCount]
		@sqlServerName			[sysname]=NULL,
		@databaseName			[sysname]=NULL,
		@schemaName				[sysname]=NULL,
		@tableName				[sysname]=NULL,
		@executionLevel			[tinyint]=0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun	[nvarchar](max)
		, @recordCount	[int]


DECLARE @tableRowCount TABLE ([row_count] [int])


SET @queryToRun=N''
SET @queryToRun=@queryToRun + N'USE ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + '; 
								SELECT rc.[row_count]
								FROM [sys].[objects] so
								INNER JOIN [sys].[schemas] sch ON sch.[schema_id] = so.[schema_id]
								LEFT JOIN
										(
											SELECT   ps.[object_id]
													, SUM (CASE WHEN (ps.[index_id] < 2) THEN [row_count] ELSE 0 END) AS [row_count]
											FROM sys.dm_db_partition_stats ps with (readpast)
											GROUP BY ps.[object_id]
										) AS rc ON rc.[object_id] = so.[object_id]
								WHERE so.[name]=''' + [dbo].[ufn_getObjectQuoteName](@tableName, 'sql') + N'''
									AND sch.[name]=''' + [dbo].[ufn_getObjectQuoteName](@schemaName, 'sql') + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT INTO @tableRowCount([row_count])
		EXEC (@queryToRun)

SELECT TOP 1 @recordCount = [row_count]
FROM @tableRowCount

RETURN @recordCount
GO
