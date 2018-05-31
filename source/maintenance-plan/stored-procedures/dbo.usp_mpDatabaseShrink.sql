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
DECLARE @clusterName		[sysname],
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
																		@clusterName		= @clusterName OUTPUT,
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
										WHERE sdb.[name] LIKE ''' + CASE WHEN @dbName IS NULL THEN '%' ELSE [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') END + '''
											AND NOT EXISTS (
															 SELECT 1
															 FROM  master.dbo.sysprocesses sp
															 WHERE sp.[cmd] LIKE ''BACKUP %''
																	AND sp.[dbid]=sdb.[dbid]
															)'
ELSE
	SET @queryToRun = @queryToRun + N'SELECT ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''' AS [name]
										WHERE NOT EXISTS (
															 SELECT 1
															 FROM  master.dbo.sysprocesses sp
															 WHERE sp.[cmd] LIKE ''BACKUP %''
																	AND sp.[dbid]= DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''')
															)'

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #DatabaseList
INSERT	INTO #DatabaseList([dbname])
		EXEC sp_executesql @queryToRun


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
				SET @queryToRun= 'Shrinking database...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun = N'DBCC SHRINKDATABASE(' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'' + CASE WHEN @flgOptions & 1 = 1 THEN N', TRUNCATEONLY' ELSE N'' END + N') WITH NO_INFOMSGS'
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
				SET @queryToRun= 'Shrinking database log files...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #databaseFiles

				SET @queryToRun = CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
								 SELECT [name] FROM ' + CASE WHEN @sqlServerName<>@@SERVERNAME THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '..' ELSE N'' END + N'sysfiles WHERE [status] & 0x40 = 0x40'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				INSERT	INTO #databaseFiles
						EXEC sp_executesql @queryToRun

				DECLARE crsLogFile CURSOR LOCAL FAST_FORWARD FOR SELECT LTRIM(RTRIM([name])) FROM #databaseFiles
				OPEN crsLogFile
				FETCH NEXT FROM crsLogFile INTO @logName
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun = N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; DBCC SHRINKFILE(' + [dbo].[ufn_getObjectQuoteName](@logName, 'quoted') + CASE WHEN @flgOptions & 1 = 1 THEN N', TRUNCATEONLY' ELSE N'' END + N') WITH NO_INFOMSGS'
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
