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
											2 - if database is in AG, in async, wait for estimated_recovery_time = 0
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

DECLARE		@queryToRun    			[nvarchar](max),
			@queryParam				[nvarchar](512),
			@databaseName			[sysname],
			@logName				[sysname],
			@errorCode				[int],
			@nestedExecutionLevel	[int],
			@executionDBName		[sysname], 
			@isAzureSQLDatabase		[bit]

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
			@serverVersionNum				[numeric](9,6),
			@serverEngine					[int]

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
SET @actionType = NULL

IF @flgActions & 1 = 1	SET @actionType = 'shrink log'
IF @flgActions & 2 = 2	SET @actionType = 'shrink database'

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL AND @isAzureSQLDatabase = 0
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database shrink',
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
										FROM sys.databases sdb
										WHERE sdb.[name] LIKE ''' + CASE WHEN @dbName IS NULL THEN '%' ELSE [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') END + '''
											AND NOT EXISTS (
															 SELECT 1
															 FROM  sys.dm_exec_requests sp
															 WHERE sp.[command] LIKE ''BACKUP %''
																	AND sp.[database_id]=sdb.[database_id]
															)'
ELSE
	SET @queryToRun = @queryToRun + N'SELECT ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''' AS [name]
										WHERE NOT EXISTS (
															 SELECT 1
															 FROM  sys.dm_exec_requests sp
															 WHERE sp.[command] LIKE ''BACKUP %''
																	AND sp.[database_id]= DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + ''')
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
		--if database is part of an AlwaysOn Availability Group, wait for estimated_recovery_time to be 0
		IF @flgOptions & 2 = 2 AND @clusterName IS NOT NULL
			begin
				DECLARE @estimatedRecoveryTime [int] 
				SET @queryToRun=N'SELECT MAX(CASE WHEN s.[redo_rate] = 0 THEN 0 ELSE CAST(s.[redo_queue_size] / s.[redo_rate] AS BIGINT) END) AS [estimated_recovery_time]
								FROM (
										SELECT    adc.[database_name]
												, hdrs.[redo_queue_size]
												, hdrs.[redo_rate]
										FROM sys.dm_hadr_database_replica_states hdrs
										INNER JOIN sys.availability_replicas ar on ar.replica_id=hdrs.replica_id
										INNER JOIN sys.dm_hadr_availability_replica_states ars on ars.replica_id=ar.replica_id and ars.group_id=ar.group_id
										INNER JOIN sys.availability_databases_cluster adc on adc.group_id=hdrs.group_id and adc.group_database_id=hdrs.group_database_id
										WHERE adc.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
											AND [role_desc] = ''PRIMARY''
									) p
								LEFT JOIN 
									(
										SELECT    adc.[database_name]
												, hdrs.[redo_queue_size]
												, hdrs.[redo_rate]
										FROM sys.dm_hadr_database_replica_states hdrs
										INNER JOIN sys.availability_replicas ar on ar.replica_id=hdrs.replica_id
										INNER JOIN sys.dm_hadr_availability_replica_states ars on ars.replica_id=ar.replica_id and ars.group_id=ar.group_id
										INNER JOIN sys.availability_databases_cluster adc on adc.group_id=hdrs.group_id and adc.group_database_id=hdrs.group_database_id
										WHERE adc.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
												AND	[role_desc] = ''SECONDARY''
									) s ON [s].[database_name] = [p].[database_name]'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				SET @queryToRun = N'SELECT @estimatedRecoveryTime = [estimated_recovery_time]
									FROM (' + @queryToRun + N')y'
				SET @queryParam = '@estimatedRecoveryTime [int] OUTPUT'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @estimatedRecoveryTime = 1
				WHILE @estimatedRecoveryTime > 0
					begin
						EXEC sp_executesql @queryToRun, @queryParam, @estimatedRecoveryTime = @estimatedRecoveryTime OUTPUT
						SET @estimatedRecoveryTime = ISNULL(@estimatedRecoveryTime, 0)

						IF @estimatedRecoveryTime > 0
							begin
								SET @logName = 'Run in AlwaysON AG mode: Waiting for databases to be in sync...'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @logName, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								WAITFOR DELAY '00:00:05'
							end
					end
			end
		
		---------------------------------------------------------------------------------------------
		--shrink database
		IF @flgActions & 2 = 2
			begin
				SET @queryToRun= 'Shrinking database...' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun = N'DBCC SHRINKDATABASE(' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'' + CASE WHEN @flgOptions & 1 = 1 THEN N', TRUNCATEONLY' ELSE N'' END + N') WITH NO_INFOMSGS'
				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
				
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
						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

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
