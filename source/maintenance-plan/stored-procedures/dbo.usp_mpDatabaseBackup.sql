RAISERROR('Create procedure: [dbo].[usp_mpDatabaseBackup]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseBackup]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseBackup]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseBackup]
		@sqlServerName			[sysname] = @@SERVERNAME,
		@dbName					[sysname],
		@backupLocation			[nvarchar](1024)=NULL,	/*  disk: local / UNC or URL https:// */
		@flgActions				[smallint] = 1,			/*  1 - perform full database backup
															2 - perform differential database backup
															4 - perform transaction log backup
														*/
		@flgOptions				[int] = 5083,		/*  1 - use CHECKSUM (default)
														2 - use COMPRESSION, if available (default)
														4 - use COPY_ONLY
														8 - force change backup type (default): if log is set, and no database backup is found, a database backup will be first triggered
													  										    if diff is set, and no full database backup is found, a full database backup will be first triggered
																								if diff is set, and extent changes >=@dataChangesThreshold, a full database backup will be triggered instead (+2016 SP2)
													   16 - verify backup file (default)
												       32 - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
													   64 - create folders for each database (default)
													  128 - when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
													  256 - for +2k5 versions, use xp_delete_file option (default)
													  512 - skip databases involved in log shipping (primary or secondary or logs, secondary for full/diff backups) (default)
													 1024 - on alwayson availability groups, for secondary replicas, force copy-only backups
													 2048 - change retention policy from RetentionDays to RetentionBackupsCount (number of full database backups to be kept)
														  - this may be forced by setting to true property 'Change retention policy from RetentionDays to RetentionBackupsCount'
													 4096 - use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default)
													 8192 - use tail log backup - NORECOVERY
													*/
		@retentionDays			[smallint]	= NULL,
		@dataChangesThreshold	[smallint]	=  0,	/* default to value set in dbo.appConfiguration table : 50 */
		@credential				[sysname]	= NULL,
		@executionLevel			[tinyint]	=  0,
		@debugMode				[bit]		=  0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006 / review on 2015.03.04
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================

------------------------------------------------------------------------------------------------------------------------------------------
--returns: 0 = success, >0 = failure

DECLARE		@queryToRun  					[nvarchar](2048),
			@queryParameters				[nvarchar](512),
			@nestedExecutionLevel			[tinyint]

DECLARE		@backupFileName					[nvarchar](1024),
			@backupFilePath					[nvarchar](1024),
			@backupType						[nvarchar](8),
			@backupOptions					[nvarchar](256),
			@optionBackupWithChecksum		[bit],
			@optionBackupWithCompression	[bit],
			@optionBackupWithCopyOnly		[bit],
			@optionForceChangeBackupType	[bit],
			@optionTailLogBackup		    [bit],
			@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@serverEngine					[int],
			@hostPlatform					[sysname],
			@errorCode						[int],
			@currentDate					[datetime],
			@databaseStatus					[int],
			@databaseStateDesc				[sysname], 
			@isAzureSQLDatabase				[bit],
			@backupToURL					[bit]

DECLARE		@backupStartDate				[datetime],
			@backupDurationSec				[int],
			@backupSizeBytes				[bigint],
			@eventData						[varchar](8000),
			@maxPATHLength					[smallint],
			@executionDBName				[sysname]

-----------------------------------------------------------------------------------------
SET NOCOUNT ON

-----------------------------------------------------------------------------------------
IF OBJECT_ID('#runtimeProperty') IS NOT NULL DROP TABLE #runtimeProperty
CREATE TABLE #runtimeProperty
			(
				[value]			[sysname]	NULL
			)

IF OBJECT_ID('#databaseProperties') IS NOT NULL DROP TABLE #databaseProperties
CREATE TABLE #databaseProperties
			(
				  [state_desc]			[sysname]	NULL
				, [is_in_standby]		[bit]		NULL
				, [is_read_only]		[bit]		NULL
				, [recovery_model_desc]	[sysname]	NULL
				, [source_database_id]	[int]		NULL
			)

-----------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
SET @queryToRun= 'Backup database: ' + @dbName
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

-----------------------------------------------------------------------------------------
--get default backup location
IF @backupLocation IS NULL
	begin
		SELECT	@backupLocation = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Default backup location'
				AND [module] = 'maintenance-plan'

		IF @backupLocation IS NULL
			begin
				SET @queryToRun= 'ERROR: @backupLocation parameter value not set'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=1
			end
	end

-----------------------------------------------------------------------------------------
--get default backup retention
IF @retentionDays IS NULL
	begin
		SELECT	@retentionDays = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Default backup retention (days)'
				AND [module] = 'maintenance-plan'

		IF @retentionDays IS NULL
			begin
				SET @queryToRun= 'WARNING: @retentionDays parameter value not set'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
			end
	end

-----------------------------------------------------------------------------------------
--get default data changes threshold
IF ISNULL(@dataChangesThreshold, 0) = 0
	begin
		BEGIN TRY
			SELECT	@dataChangesThreshold = [value]
			FROM	[dbo].[appConfigurations]
			WHERE	[name] = N'SMART default changes threshold'
					AND [module] = 'maintenance-plan'
		END TRY
		BEGIN CATCH
			SET @dataChangesThreshold = NULL
		END CATCH
		
		SET @dataChangesThreshold = ISNULL(@dataChangesThreshold, 0)
	end

-----------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
										@serverEngine		= @serverEngine OUT,
										@executionLevel		= @executionLevel,
										@debugMode			= @debugMode

---------------------------------------------------------------------------------------------
SET @isAzureSQLDatabase = CASE WHEN @serverEngine IS NOT NULL
								THEN  CASE	WHEN (@serverEngine IN (5, 6)) OR (@serverEngine IN (8) AND @flgActions = 1 AND @flgOptions & 4 = 4)
											THEN 1 
											ELSE 0
									  END
								ELSE 0
						 END

-----------------------------------------------------------------------------------------
IF @isAzureSQLDatabase = 1
	begin
		SELECT @sqlServerName = CASE WHEN ss.[name] IS NOT NULL THEN ss.[name] ELSE NULL END 
		FROM	[dbo].[vw_catalogDatabaseNames] cdn
		LEFT JOIN [sys].[servers] ss ON ss.[catalog] = cdn.[database_name] COLLATE SQL_Latin1_General_CP1_CI_AS
		WHERE 	cdn.[instance_name] = @sqlServerName
				AND cdn.[active]=1
				AND cdn.[database_name] = @dbName

		IF @sqlServerName IS NULL and @serverEngine NOT IN (5, 6, 8)
			begin
				SET @queryToRun=N'Could not find a linked server defined for Azure SQL database: [' + @dbName + ']' 
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
			end

		SET @queryToRun=N'Azure provides automated backups for a SQL Database (https://docs.microsoft.com/en-us/azure/sql-database/sql-database-automated-backups).' 
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		RETURN 0
	end

--get OS platform
SELECT	@hostPlatform = [host_platform]
FROM	[dbo].[vw_catalogInstanceNames]
WHERE	[instance_name] = @sqlServerName

SET @nestedExecutionLevel = @executionLevel + 1

-----------------------------------------------------------------------------------------
--backup to URL pre-requisites
SET @backupToURL = 0
IF @backupLocation LIKE 'https://%'
	begin
		SET @backupToURL = 1
		IF @serverVersionNum < 11.03339
			begin
				SET @queryToRun=N'Backup to URL functionaly is only available starting with SQL Server 2012 SP1 onwards.' 
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				RETURN 0
			end
		IF RIGHT(@backupLocation, 1) IN ('\', '/')
			SET @backupLocation = SUBSTRING(@backupLocation, 1, LEN(@backupLocation)-1)
		
		IF @credential IS NOT NULL 
			begin
				/* get credentials defined on destination server name */
				SET @queryToRun = N'SELECT [name]
									FROM sys.credentials
									WHERE [name] = ''' + @credential + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #runtimeProperty
				INSERT	INTO #runtimeProperty([value])
						EXEC sp_executesql @queryToRun

				IF (SELECT COUNT(*) FROM #runtimeProperty) = 0
					begin
						SET @queryToRun=N'Credential specified as parameter does not exist.' 
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
						RETURN 1
					end
			end
		ELSE
			IF @serverVersionNum < 13 AND @serverEngine NOT IN (5, 6, 8)
				begin
					SET @queryToRun=N'Credential parameter value was not set.' 
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
					RETURN 1
				end
			ELSE
				begin
					/* check shared access signature defined credentials */
					SET @queryToRun = N'SELECT [name]
										FROM sys.credentials 
										WHERE UPPER([credential_identity]) = ''SHARED ACCESS SIGNATURE''
												AND [name] = ''' + @backupLocation + ''''
					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

					DELETE FROM #runtimeProperty
					INSERT	INTO #runtimeProperty([value])
							EXEC sp_executesql @queryToRun

					IF (SELECT COUNT(*) FROM #runtimeProperty) = 0
						begin
							SET @queryToRun=N'SHARED ACCESS SIGNATURE credential is not defined for the specified URL/container: '  + @backupLocation
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
							RETURN 1
						end	
				end
		IF RIGHT(@backupLocation, 1)<>'/' SET @backupLocation = @backupLocation + N'/'
	end
	
--------------------------------------------------------------------------------------------------
--treat exceptions
IF @dbName='master'
	begin
		SET @optionForceChangeBackupType=0
		SET @flgActions=1 /* only full backup is allowed for master database */
	end

--------------------------------------------------------------------------------------------------
--selected backup type
SELECT @backupType = CASE WHEN @flgActions & 1 = 1 THEN N'full'
						  WHEN @flgActions & 2 = 2 THEN N'diff'
						  WHEN @flgActions & 4 = 4 THEN N'log'
					 END

--------------------------------------------------------------------------------------------------
--get database status
DELETE FROM #databaseProperties

SET @queryToRun = N'SELECT [state_desc], [is_in_standby], [is_read_only], [recovery_model_desc], [source_database_id] FROM sys.databases WHERE [name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + '''';
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

INSERT INTO #databaseProperties([state_desc], [is_in_standby], [is_read_only], [recovery_model_desc], [source_database_id])
		EXEC sp_executesql @queryToRun

SELECT @databaseStateDesc = [state_desc] FROM #databaseProperties
SET @databaseStateDesc = ISNULL(@databaseStateDesc, 'NULL')

/* check for the standby property */
IF  @databaseStateDesc IN ('ONLINE') AND (SELECT [is_in_standby] FROM #databaseProperties) = 1
	SET @databaseStateDesc = 'STANDBY'
			
/* check if the database is a snapshot */
IF  @databaseStateDesc IN ('ONLINE') AND (SELECT [source_database_id] FROM #databaseProperties) IS NOT NULL
	SET @databaseStateDesc = 'DATABASE SNAPSHOT'			

IF  @databaseStateDesc NOT IN ('ONLINE')
begin
	SET @queryToRun='Current database state (' + @databaseStateDesc + ') does not allow backup.'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

	SET @eventData='<skipaction><detail>' + 
						'<name>database backup</name>' + 
						'<type>' + @backupType + '</type>' + 
						'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
						'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
						'<reason>' + @queryToRun + '</reason>' + 
					'</detail></skipaction>'

	EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
										@dbName			= @dbName,
										@module			= 'dbo.usp_mpDatabaseBackup',
										@eventName		= 'database backup',
										@eventMessage	= @eventData,
										@eventType		= 0 /* info */

	RETURN 0
end

--------------------------------------------------------------------------------------------------
/*	Operations that cannot run during a database backup or transaction log backup include the following:
		Shrink database or shrink file operations. This includes auto-shrink operations.
*/
SET @queryToRun = N'SELECT CAST(COUNT(*) AS [sysname]) AS [session_count] FROM sys.dm_exec_requests
					WHERE	DB_NAME([database_id]) = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
							AND [command] LIKE ''Dbcc%'''
		
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #runtimeProperty
INSERT	INTO #runtimeProperty([value])
		EXEC sp_executesql @queryToRun

IF (SELECT CAST([value] AS [int]) FROM #runtimeProperty) > 0
	begin
		SET @queryToRun='A shrink operation is in progress for the current database. Backup cannot run.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @eventData='<skipaction><detail>' + 
							'<name>database backup</name>' + 
							'<type>' + @backupType + '</type>' + 
							'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
							'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
							'<reason>' + @queryToRun + '</reason>' + 
						'</detail></skipaction>'

		EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
											@dbName			= @dbName,
											@module			= 'dbo.usp_mpDatabaseBackup',
											@eventName		= 'database backup',
											@eventMessage	= @eventData,
											@eventType		= 0 /* info */
		RETURN 0
	end

--------------------------------------------------------------------------------------------------
--skip databases involved in log shipping (primary or secondary or logs, secondary for full/diff backups)
IF @flgOptions & 512 = 512
	begin
		--for full and diff backups
		IF @flgActions IN (1, 2)
			SET @queryToRun = N'SELECT	[secondary_database]
								FROM	msdb.dbo.log_shipping_monitor_secondary
								WHERE	[secondary_server]=@@SERVERNAME
										AND [secondary_database] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''''

		--for log backups
		IF @flgActions=4
			SET @queryToRun = N'SELECT	[primary_database]
								FROM	msdb.dbo.log_shipping_monitor_primary
								WHERE	[primary_server]=@@SERVERNAME
										AND [primary_database] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
								UNION ALL
								SELECT	[secondary_database]
								FROM	msdb.dbo.log_shipping_monitor_secondary
								WHERE	[secondary_server]=@@SERVERNAME
										AND [secondary_database] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''''

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #runtimeProperty
		INSERT	INTO #runtimeProperty([value])
				EXEC sp_executesql @queryToRun

		IF (SELECT COUNT(*) FROM #runtimeProperty)>0
			begin
				SET @queryToRun='Log Shipping: '
				IF @flgActions IN (1, 2)
					SET @queryToRun = @queryToRun + 'Cannot perform a full or differential backup on a secondary database.'
				IF @flgActions IN (4)
					SET @queryToRun = @queryToRun + 'Cannot perform a transaction log backup since it may break the log shipping chain.'

				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpDatabaseBackup',
													@eventName		= 'database backup',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				RETURN 0
			end
	end

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @clusterName		 [sysname],
		@agInstanceRoleDesc	 [sysname],
		@agReadableSecondary [sysname],
		@agStopLimit		 [int]

SET @agStopLimit = 0
IF @serverVersionNum >= 11
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database backup',
																		@actionType			= @backupType,
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

--------------------------------------------------------------------------------------------------
--check recovery model for database. transaction log backup is allowed only for FULL
--if force option is selected, for SIMPLE recovery model, backup type will be changed to diff
--------------------------------------------------------------------------------------------------
IF @flgActions & 4 = 4
	begin
		IF (SELECT UPPER([recovery_model_desc]) FROM #databaseProperties) = 'SIMPLE'
			begin
				SET @queryToRun = 'Database recovery model is SIMPLE. Transaction log backup cannot be performed.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpDatabaseBackup',
													@eventName		= 'database backup',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				RETURN 0
			end
	end
	
--------------------------------------------------------------------------------------------------
SET @maxPATHLength = 259
IF NOT (@backupToURL=1)
	begin
		--create destination path: <@backupLocation>\@sqlServerName\@dbName
		IF RIGHT(@backupLocation, 1)<>'\' SET @backupLocation = @backupLocation + N'\'
		IF @clusterName IS NULL
			SET @backupLocation = @backupLocation + REPLACE(@sqlServerName, '\', '$') + '\' + CASE WHEN @flgOptions & 64 = 64 THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'filename') + '\' ELSE '' END
		ELSE
			SET @backupLocation = @backupLocation + REPLACE(@clusterName, '\', '$') + '\' + CASE WHEN @flgOptions & 64 = 64 THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'filename') + '\' ELSE '' END
		SET @backupLocation = SUBSTRING(@backupLocation, 1, 2) + REPLACE(REPLACE(REPLACE(REPLACE(SUBSTRING(@backupLocation, 3, LEN(@backupLocation)), '<', '_'), '>', '_'), ':', '_'), '"', '_')

		SET @backupLocation = [dbo].[ufn_formatPlatformSpecificPath](@sqlServerName, @backupLocation)

		--check for maximum length of the file path
		--https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
		IF LEN(@backupLocation) >= @maxPATHLength
			begin
				SET @eventData='<alert><detail>' + 
									'<severity>critical</severity>' + 
									'<instance_name>' + @sqlServerName + '</instance_name>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<reason>Msg 3057, Level 16: The length of the device name provided exceeds supported limit (maximum length is:' + CAST(@maxPATHLength AS [nvarchar]) + ')</reason>' + 
									'<path>' + [dbo].[ufn_getObjectQuoteName](@backupLocation, 'xml') + '</path>' + 
									'<event_date_utc>' + CONVERT([varchar](24), GETDATE(), 121) + '</event_date_utc>' + 
								'</detail></alert>'

				EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																@dbName					= @dbName,
																@objectName				= 'critical',
																@childObjectName		= 'dbo.usp_mpDatabaseBackup',
																@module					= 'maintenance-plan',
																@eventName				= 'database backup',
																@parameters				= NULL,	
																@eventMessage			= @eventData,
																@dbMailProfileName		= NULL,
																@recipientsList			= NULL,
																@eventType				= 6,	/* 6 - alert-custom */
																@additionalOption		= 0

				SET @errorCode = -1
			end
		ELSE
			begin
				SET @queryToRun = N'EXEC ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + '.[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @sqlServerName + N''',
																							@folderName		= ''' + [dbo].[ufn_getObjectQuoteName](@backupLocation, 'sql') + N''',
																							@executionLevel	= ' + CAST(@nestedExecutionLevel AS [nvarchar]) + N',
																							@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
																@dbName			= NULL,
																@module			= 'dbo.usp_mpDatabaseBackup',
																@eventName		= 'create folder on disk',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
			end

		IF @errorCode<>0 
			begin
				RETURN @errorCode
			end
	end

--------------------------------------------------------------------------------------------------
--smart differential backup: apply to SQL Server 2016 SP2 onwards
IF @flgActions & 2 = 2 AND @serverVersionNum>=13.05026
	begin
		DECLARE @dataChangesPercent [numeric](9,2)

		SET @queryToRun = CASE WHEN @sqlServerName=@@SERVERNAME THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + '; ' ELSE N'' END + N'
								SELECT CAST(CAST((SUM(modified_extent_page_count)*100.0)/SUM(allocated_extent_page_count) AS DECIMAL(9,2)) AS [sysname]) AS [changes_percent]
							FROM ' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N'.sys.dm_db_file_space_usage'
		IF @sqlServerName<>@@SERVERNAME
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #runtimeProperty
		INSERT	INTO #runtimeProperty([value])
				EXEC sp_executesql @queryToRun

		BEGIN TRY
			SELECT @dataChangesPercent = [value]
			FROM #runtimeProperty
		END TRY
		BEGIN CATCH
			SET @dataChangesPercent = 0
		END CATCH

		--log information
		SET @eventData='<backup><info>' + 
							'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
							'<requested_backup_type>' + @backupType + '</requested_backup_type>' + 
							'<data_changes_percent>' + CAST(@dataChangesPercent AS [varchar](32)) + '</data_changes_percent>' + 
							'<data_changes_threshold>' + CAST(@dataChangesThreshold AS [varchar](32)) + '</data_changes_threshold>' +
							'<change_backup_type_allowed>' + CASE WHEN @flgOptions & 8 = 8 THEN 'yes' ELSE 'no' END + '</change_backup_type_allowed>' + 
						'</info></backup>'

		EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
											@dbName			= @dbName,
											@module			= 'dbo.usp_mpDatabaseBackup',
											@eventName		= 'database backup',
											@eventMessage	= @eventData,
											@eventType		= 0 /* info */

		IF @flgOptions & 8 = 8 AND @dataChangesPercent >= @dataChangesThreshold
			begin
				/* convert to a full database backup */
				SET @flgActions = 1 
				SET @backupType = N'full'

				SET @queryToRun = 'INFO: The number of extents changed (' + CAST(@dataChangesPercent AS [sysname]) + '%) exceed the specified threshold ('+ CAST(@dataChangesThreshold AS [sysname]) + N'%). A full database backup will be taken instead.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			end
	end


--------------------------------------------------------------------------------------------------
--check if CHECKSUM backup option may apply
SET @optionBackupWithChecksum=0
IF @flgOptions & 1 = 1
	SET @optionBackupWithChecksum=1

--check COMPRESSION backup option may apply
SET @optionBackupWithCompression=0
IF @flgOptions & 2 = 2 AND @serverVersionNum >= 10
	begin
		IF @serverVersionNum>=10 AND @serverVersionNum<10.5 AND (CHARINDEX('Enterprise', @serverEdition)>0 OR CHARINDEX('Developer', @serverEdition)>0)
			SET @optionBackupWithCompression=1
		
		IF @serverVersionNum>=10.5 AND (CHARINDEX('Enterprise', @serverEdition)>0 OR CHARINDEX('Developer', @serverEdition)>0 OR CHARINDEX('Standard', @serverEdition)>0)
			SET @optionBackupWithCompression=1
	end

--check COPY_ONLY backup option may apply
SET @optionBackupWithCopyOnly=0
IF @flgOptions & 4 = 4
	SET @optionBackupWithCopyOnly=1

-- check TAIL_LOG_BACKUP option
SET @optionTailLogBackup = 0
IF @flgActions & 4 = 4 AND @flgOptions & 8192 = 8192
	SET @optionTailLogBackup=1

--check if another backup is needed (full) / partially applicable to AlwaysOn Availability Groups
SET @optionForceChangeBackupType=0
IF @flgOptions & 8 = 8 AND (@clusterName IS NULL OR (@clusterName IS NOT NULL AND @agInstanceRoleDesc = 'PRIMARY')) AND @serverEngine NOT IN (5, 6, 8)
	begin
		--check for any full database backup (when differential should be made) or any full/incremental database backup (when transaction log should be made)
		IF @flgActions & 2 = 2 OR @flgActions & 4 = 4
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + 'SELECT	[differential_base_lsn] FROM sys.master_files WHERE [database_id] = DB_ID(''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''') AND [type] = 0 AND [file_id] = 1'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #runtimeProperty
				INSERT	INTO #runtimeProperty([value])
						EXEC sp_executesql @queryToRun

				DECLARE @differentialBaseLSN	[numeric](25,0)

				SELECT @differentialBaseLSN = [value] FROM #runtimeProperty
				
				IF @differentialBaseLSN IS NULL
					begin
						SET @optionForceChangeBackupType=1 
						SET @queryToRun = 'WARNING: Specified backup type cannot be performed since no full database backup exists. A full database backup will be taken before the requested backup type.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
					end
				ELSE	
					/* only for databases in single instance mode */
					IF @clusterName IS NULL
						begin
							SET @queryToRun = N''
							SET @queryToRun = @queryToRun + 'SELECT COUNT(*) 
															FROM msdb.dbo.backupset bs
															INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.[media_set_id] = bs.[media_set_id]
															WHERE bs.[server_name] = N''' + @sqlServerName + ''' 
																AND bs.[database_name]=''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''' 
																AND bs.[type] IN (''D''' + CASE WHEN @flgActions & 4 = 4 THEN N', ''I''' ELSE N'' END + N')
																AND ' + CAST(@differentialBaseLSN AS [nvarchar]) + N' BETWEEN bs.[first_lsn] AND bs.[last_lsn]
																AND bmf.[device_type] <> 7 /* virtual device */
																AND [is_copy_only]=0'
							SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
							IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

							DELETE FROM #runtimeProperty
							INSERT	INTO #runtimeProperty([value])
									EXEC sp_executesql @queryToRun

							IF (SELECT [value] FROM #runtimeProperty) = 0
								begin
									SET @queryToRun = 'WARNING: Specified backup type cannot be performed since no full database backup exists. A full database backup will be taken before the requested backup type.'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

									SET @optionForceChangeBackupType=1 
								end
						end

				/* check for database header: dbi_dbbackupLSN */
				IF @differentialBaseLSN IS NOT NULL AND @optionForceChangeBackupType = 0
					begin
						DECLARE @dbi_dbbackupLSN [sysname]

						IF object_id('tempdb..#dbi_dbbackupLSN') IS NOT NULL DROP TABLE #dbi_dbbackupLSN
						CREATE TABLE #dbi_dbbackupLSN
						(
							[Value]					[sysname]			NULL
						)

						IF object_id('tempdb..#dbccDBINFO') IS NOT NULL DROP TABLE #dbccDBINFO
						CREATE TABLE #dbccDBINFO
							(
								[id]				[int] IDENTITY(1,1),
								[ParentObject]		[varchar](255) NULL,
								[Object]			[varchar](255) NULL,
								[Field]				[varchar](255) NULL,
								[Value]				[varchar](255) NULL
							)
	
						IF @sqlServerName <> @@SERVERNAME
							begin
								IF @serverVersionNum < 11
									SET @queryToRun = N'SELECT MAX([VALUE]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC (''''DBCC DBINFO (' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N') WITH TABLERESULTS, NO_INFOMSGS'''')'')x
														WHERE [Field]=''dbi_dbbackupLSN'''
								ELSE
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC (''''DBCC DBINFO (' + [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N') WITH TABLERESULTS, NO_INFOMSGS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
														WHERE [Field]=''dbi_differentialBaseLSN'''
							end
						ELSE
							begin							
								SET @queryToRun='DBCC DBINFO (''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N''') WITH TABLERESULTS, NO_INFOMSGS'
								IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

								INSERT	INTO #dbccDBINFO([ParentObject], [Object], [Field], [Value])
										EXEC sp_executesql @queryToRun
								
								IF @serverVersionNum < 11 
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbbackupLSN'''											
								ELSE
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_differentialBaseLSN'''											
							end

						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				
						INSERT	INTO #dbi_dbbackupLSN([Value])
								EXEC sp_executesql @queryToRun

						SELECT @dbi_dbbackupLSN = ISNULL([Value], 0)
						FROM #dbi_dbbackupLSN
		
						SET @dbi_dbbackupLSN = ISNULL(@dbi_dbbackupLSN, 0)
						IF CHARINDEX('0:0:0', @dbi_dbbackupLSN) <> 0
								begin
									SET @queryToRun = 'WARNING: The database header does not contain any backup information. A full database backup will be taken before the requested backup type.'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

									SET @optionForceChangeBackupType=1 
								end
					end
			end			
	end

--------------------------------------------------------------------------------------------------
--compiling backup options
SET @backupOptions=N''

IF @optionBackupWithChecksum=1
	SET @backupOptions = @backupOptions + N', CHECKSUM'
IF @optionBackupWithCompression=1
	SET @backupOptions = @backupOptions + N', COMPRESSION'
IF @optionBackupWithCopyOnly=1
	SET @backupOptions = @backupOptions + N', COPY_ONLY'
IF @optionTailLogBackup=1
	SET @backupOptions = @backupOptions + N', NORECOVERY'
IF ISNULL(@retentionDays, 0) <> 0 AND @backupToURL=0
	SET @backupOptions = @backupOptions + N', RETAINDAYS=' + CAST(@retentionDays AS [nvarchar](32))
IF @backupToURL=1 AND @credential IS NOT NULL
	SET @backupOptions = @backupOptions + N', CREDENTIAL=''' + @credential + ''''
	
--------------------------------------------------------------------------------------------------
--run a full database backup, in order to perform an additional diff or log backup
IF @optionForceChangeBackupType=1
	begin
		SET @queryToRun='SELECT GETDATE() AS [server_date]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		SET @queryToRun = 'SELECT @currentDate = [server_date] FROM (' + @queryToRun + ')Y'
		SET @queryParameters = '@currentDate [datetime] OUTPUT'
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 1, @messageTreelevel = 0, @stopExecution=0
		EXEC sp_executesql @queryToRun, @queryParameters, @currentDate = @currentDate OUT
		
		IF @clusterName IS NULL
			SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@sqlServerName, @dbName, 'full', @currentDate)
		ELSE
			SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@clusterName, @dbName, 'full', @currentDate)

		--check for maximum length of the file path
		--https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
		IF LEN(@backupLocation + @backupFileName) > @maxPATHLength
			begin
				SET @eventData='<alert><detail>' + 
									'<severity>critical</severity>' + 
									'<instance_name>' + @sqlServerName + '</instance_name>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
									'<reason>Msg 3057, Level 16: The length of the device name provided exceeds supported limit (maximum length is:' + CAST(@maxPATHLength AS [nvarchar]) + ')</reason>' + 
									'<path>' + [dbo].[ufn_getObjectQuoteName]((@backupLocation + @backupFileName), 'xml') + '</path>' + 
									'<event_date_utc>' + CONVERT([varchar](24), GETDATE(), 121) + '</event_date_utc>' + 
								'</detail></alert>'

				EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																@dbName					= @dbName,
																@objectName				= 'critical',
																@childObjectName		= 'dbo.usp_mpDatabaseBackup',
																@module					= 'maintenance-plan',
																@eventName				= 'database backup',
																@parameters				= NULL,	
																@eventMessage			= @eventData,
																@dbMailProfileName		= NULL,
																@recipientsList			= NULL,
																@eventType				= 6,	/* 6 - alert-custom */
																@additionalOption		= 0

				SET @errorCode = -1
			end
		ELSE
			begin
				SET @queryToRun	= N'BACKUP DATABASE '+ [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + 
									CASE WHEN @backupToURL = 0 THEN N' TO DISK = ''' ELSE N' TO URL = ''' END + 
									[dbo].[ufn_getObjectQuoteName](@backupLocation, 'sql') + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N''' WITH STATS = 10, NAME = ''' + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N'''' + @backupOptions
				SET @nestedExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @executionDBName,
																@module			= 'dbo.usp_mpDatabaseBackup',
																@eventName		= 'database backup',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
			end
	end

--------------------------------------------------------------------------------------------------
SET @queryToRun='SELECT GETDATE() AS [server_date]'
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
SET @queryToRun = 'SELECT @currentDate = [server_date] FROM (' + @queryToRun + ')Y'
SET @queryParameters = '@currentDate [datetime] OUTPUT'
IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 1, @messageTreelevel = 0, @stopExecution=0
EXEC sp_executesql @queryToRun, @queryParameters, @currentDate = @currentDate OUT

IF @clusterName IS NULL
	SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@sqlServerName, @dbName, @backupType, @currentDate)
ELSE
	SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@clusterName, @dbName, @backupType, @currentDate)

IF @flgActions & 1 = 1 
	begin
		SET @queryToRun	= N'BACKUP DATABASE '+ [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') +  + 
									CASE WHEN @backupToURL = 0 THEN N' TO DISK = ''' ELSE N' TO URL = ''' END + 
									[dbo].[ufn_getObjectQuoteName](@backupLocation, 'sql') + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N''' WITH STATS = 10, NAME = ''' + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N'''' + @backupOptions
	end

IF @flgActions & 2 = 2
	begin
		SET @queryToRun	= N'BACKUP DATABASE '+ [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + 
									CASE WHEN @backupToURL = 0 THEN N' TO DISK = ''' ELSE N' TO URL = ''' END + 
									[dbo].[ufn_getObjectQuoteName](@backupLocation, 'sql') + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N''' WITH DIFFERENTIAL, STATS = 10, NAME=''' + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N'''' + @backupOptions
	end

IF @flgActions & 4 = 4
	begin
		SET @queryToRun	= N'BACKUP LOG '+ [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + 
									CASE WHEN @backupToURL = 0 THEN N' TO DISK = ''' ELSE N' TO URL = ''' END + 
									[dbo].[ufn_getObjectQuoteName](@backupLocation, 'sql') + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N''' WITH STATS = 10, NAME=''' + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N'''' + @backupOptions
	end

--check for maximum length of the file path
--https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
IF LEN(@backupLocation + @backupFileName) > @maxPATHLength
	begin
		SET @eventData='<alert><detail>' + 
							'<severity>critical</severity>' + 
							'<instance_name>' + @sqlServerName + '</instance_name>' + 
							'<name>database backup</name>' + 
							'<type>' + @backupType + '</type>' + 
							'<affected_object>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</affected_object>' + 
							'<reason>Msg 3057, Level 16: The length of the device name provided exceeds supported limit (maximum length is:' + CAST(@maxPATHLength AS [nvarchar]) + ')</reason>' + 
							'<path>' + [dbo].[ufn_getObjectQuoteName]((@backupLocation + @backupFileName), 'xml') + '</path>' + 
							'<event_date_utc>' + CONVERT([varchar](24), GETDATE(), 121) + '</event_date_utc>' + 
						'</detail></alert>'

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
														@dbName					= @dbName,
														@objectName				= 'critical',
														@childObjectName		= 'dbo.usp_mpDatabaseBackup',
														@module					= 'maintenance-plan',
														@eventName				= 'database backup',
														@parameters				= NULL,	
														@eventMessage			= @eventData,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0
		
		SET @errorCode = -1
	end
ELSE
	begin
		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= 'master',
														@module			= 'dbo.usp_mpDatabaseBackup',
														@eventName		= 'database backup',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

IF @errorCode=0
	begin
		SET @queryToRun = '	SELECT TOP 1  bs.[backup_start_date]
										, DATEDIFF(ss, bs.[backup_start_date], bs.[backup_finish_date]) AS [backup_duration_sec]
										, ' + CASE WHEN @optionBackupWithCompression=1 THEN 'bs.[compressed_backup_size]' ELSE 'bs.[backup_size]' END + ' AS [backup_size]
							FROM msdb.dbo.backupset bs
							INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.[media_set_id] = bs.[media_set_id]
							WHERE bmf.[physical_device_name] = (''' + [dbo].[ufn_getObjectQuoteName](@backupLocation + @backupFileName, 'sql') + N''')
							ORDER BY bs.[backup_set_id] DESC'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		SET @queryToRun = N' SELECT   @backupStartDate = [backup_start_date]
									, @backupDurationSec = [backup_duration_sec]
									, @backupSizeBytes = [backup_size]
							FROM (' + @queryToRun + N')X'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryParameters = N'@backupStartDate [datetime] OUTPUT, @backupDurationSec [int] OUTPUT, @backupSizeBytes [bigint] OUTPUT'

		EXEC sp_executesql @queryToRun, @queryParameters, @backupStartDate = @backupStartDate OUT
														, @backupDurationSec = @backupDurationSec OUT
														, @backupSizeBytes = @backupSizeBytes OUT
	end

--------------------------------------------------------------------------------------------------
--verify backup, if option is selected
IF @flgOptions & 16 = 16 AND @errorCode = 0 
	begin
		SET @queryToRun	= N'RESTORE VERIFYONLY FROM ' + CASE WHEN @backupToURL = 0 THEN N' DISK = ''' ELSE N' URL = ''' END + [dbo].[ufn_getObjectQuoteName](@backupLocation + @backupFileName, 'sql') + N''''
		IF @optionBackupWithChecksum=1
			SET @queryToRun = @queryToRun + N' WITH CHECKSUM'

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @executionDBName,
														@module			= 'dbo.usp_mpDatabaseBackup',
														@eventName		= 'database backup verify',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

--------------------------------------------------------------------------------------------------
IF @errorCode = 0 
	begin
		--log backup database information
		SET @eventData='<backupset><detail>' + 
							'<database_name>' + [dbo].[ufn_getObjectQuoteName](@dbName, 'xml') + '</database_name>' + 
							'<type>' + @backupType + '</type>' + 
							'<start_date>' + CONVERT([varchar](24), ISNULL(@backupStartDate, GETDATE()), 121) + '</start_date>' + 
							'<duration>' + REPLICATE('0', 2-LEN(CAST(@backupDurationSec / 3600 AS [varchar]))) + CAST(@backupDurationSec / 3600 AS [varchar]) + 'h'
												+ ' ' + REPLICATE('0', 2-LEN(CAST((@backupDurationSec / 60) % 60 AS [varchar]))) + CAST((@backupDurationSec / 60) % 60 AS [varchar]) + 'm'
												+ ' ' + REPLICATE('0', 2-LEN(CAST(@backupDurationSec % 60 AS [varchar]))) + CAST(@backupDurationSec % 60 AS [varchar]) + 's' + '</duration>' + 
							'<size>' + CONVERT([varchar](32), CAST(@backupSizeBytes/(1024*1024*1.0) AS [money]), 1) + ' mb</size>' + 
							'<size_bytes>' + CAST(@backupSizeBytes AS [varchar](32)) + '</size_bytes>' + 
							'<verified>' + CASE WHEN @flgOptions & 16 = 16 AND @errorCode = 0  THEN 'Yes' ELSE 'No' END + '</verified>' + 
							'<file_name>' + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'xml') + '</file_name>' + 
							'<error_code>' + CAST(@errorCode AS [varchar](32)) + '</error_code>' + 
						'</detail></backupset>'

		EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
											@dbName			= @dbName,
											@module			= 'dbo.usp_mpDatabaseBackup',
											@eventName		= 'database backup',
											@eventMessage	= @eventData,
											@eventType		= 0 /* info */
	end

--------------------------------------------------------------------------------------------------
--as XP is not yet available on Linux, custom file deletion is not possible
IF NOT (@serverVersionNum >= 14 AND @hostPlatform='linux' )
	begin
		--performing backup cleanup
		IF @errorCode = 0 AND ISNULL(@retentionDays,0) <> 0
			begin

				SELECT	@backupType = SUBSTRING(@backupFileName, LEN(@backupFileName)-CHARINDEX('.', REVERSE(@backupFileName))+2, CHARINDEX('.', REVERSE(@backupFileName)))

				SET @nestedExecutionLevel = @executionLevel + 1
					
				EXEC [dbo].[usp_mpDatabaseBackupCleanup]	@sqlServerName			= @sqlServerName,
															@dbName					= @dbName,
															@backupLocation			= @backupLocation,
															@backupFileExtension	= @backupType,
															@flgOptions				= @flgOptions,
															@retentionDays			= @retentionDays,
															@executionLevel			= @nestedExecutionLevel,
															@debugMode				= @debugMode
			end
	end

RETURN @errorCode
GO
