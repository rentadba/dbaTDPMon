RAISERROR('Create procedure: [dbo].[usp_mpDatabaseBackupCleanup]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseBackupCleanup]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseBackupCleanup]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseBackupCleanup]
		@sqlServerName			[sysname],
		@dbName					[sysname],
		@backupLocation			[nvarchar](1024)=NULL,	/*  disk only: local or UNC */
		@backupFileExtension	[nvarchar](8),			/*  BAK - cleanup full/incremental database backup
															TRN - cleanup transaction log backup
														*/
		@flgOptions				[int]	= 4544,			/* 32 - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
														   64 - create folders for each database (default)
														  128 - when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
														  256 - for +2k5 versions, use xp_delete_file option (default)
														 2048 - change retention policy from RetentionDays to RetentionBackupsCount (number of full database backups to be kept)
															  - this may be forced by setting to true property 'Change retention policy from RetentionDays to RetentionFullBackupsCount'
														 4096 - use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default)
														*/
		@retentionDays			[smallint]	= 14,
		@executionLevel			[tinyint]	=  0,
		@debugMode				[bit]		=  0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006 / review on 2015.03.10
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
--					   - if @retentionDays is set to Days, this number represent the number of days on which database can be restored
--						 depending on the backup strategy, a full backup will always be included
--					   - if @retentionDays is set to BackupCount, this number represent the number of full and differential backups to be kept
--						 an older full backup may exists to ensure that a newer differential backuup can be restored
-- ============================================================================

------------------------------------------------------------------------------------------------------------------------------------------
--returns: 0 = success, >0 = failure

DECLARE		@queryToRun  					[nvarchar](2048),
			@nestedExecutionLevel			[tinyint]

DECLARE		@backupFileName					[nvarchar](1024),
			@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@serverEngine					[int],
			@errorCode						[int],
			@maxAllowedDate					[datetime]

DECLARE		@lastFullRemainingBackupSetID	[int],
			@lastFullRemainingFirstLSN		[numeric](25),
			@lastDiffRemainingBackupSetID	[int],
			@lastBackupType					[char](1),
			@optionXPValue					[int]

IF OBJECT_ID('tempdb..#backupSET') IS NOT NULL
	DROP TABLE #backupSET

CREATE TABLE #backupSET 
		(
			  [backup_set_id]		[int]
			, [backup_start_date]	[datetime]		NULL
			, [type]				[char](1)		NULL
			, [first_lsn]			[numeric](25)	NULL
		)

IF OBJECT_ID('tempdb..#backupDevice') IS NOT NULL
	DROP TABLE #backupDevice
CREATE TABLE #backupDevice 
	(
		  [backup_set_id]			[int]
		, [physical_device_name]	[nvarchar](260)
	)


-----------------------------------------------------------------------------------------
SET NOCOUNT ON

-----------------------------------------------------------------------------------------
IF @executionLevel=0
	EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

SET @queryToRun= 'Cleanup backup files for database: ' + @dbName 
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

-----------------------------------------------------------------------------------------
--get destination server running version/edition
SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
										@serverEngine		= @serverEngine OUT,
										@executionLevel		= @nestedExecutionLevel,
										@debugMode			= @debugMode

-----------------------------------------------------------------------------------------
--get configuration values: force retention policy
---------------------------------------------------------------------------------------------
DECLARE @forceChangeRetentionPolicy [nvarchar](128)
SELECT	@forceChangeRetentionPolicy=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Change retention policy from RetentionDays to RetentionBackupsCount'
		AND [module] = 'maintenance-plan'

SET @forceChangeRetentionPolicy = LOWER(ISNULL(@forceChangeRetentionPolicy, 'false'))

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

		IF RIGHT(@backupLocation, 1)<>'\' SET @backupLocation = @backupLocation + N'\'
		SET @backupLocation = @backupLocation + @sqlServerName + '\' + CASE WHEN @flgOptions & 64 = 64 THEN @dbName + '\' ELSE '' END
	end

SET @backupLocation = [dbo].[ufn_getObjectQuoteName](@backupLocation, 'filepath')
SET @backupLocation = [dbo].[ufn_formatPlatformSpecificPath](@sqlServerName, @backupLocation)

-----------------------------------------------------------------------------------------
--changing backup expiration date from RetentionDays to full/diff database backup count
IF @flgOptions & 2048 = 2048 OR @forceChangeRetentionPolicy='true'
	begin
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SET ROWCOUNT ' + CAST(@retentionDays AS [nvarchar]) + N'		
										SELECT bs.[backup_set_id], bs.[backup_start_date], bs.[type], bs.[first_lsn]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type] IN (''D'', ''I'')
												AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
												AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type], [first_lsn])
				EXEC sp_executesql @queryToRun

		--check for remote server msdb information
		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type], [first_lsn])
						EXEC sp_executesql @queryToRun
			end
		

		SELECT TOP 1  @maxAllowedDate = DATEADD(ss, -1, [backup_start_date])
					, @lastFullRemainingBackupSetID = [backup_set_id]
					, @lastBackupType = [type]
					, @lastFullRemainingFirstLSN = [first_lsn]
		FROM #backupSET
		ORDER BY [backup_start_date]

		--if oldest backup is a differential one, go deep and find the full database backup that it will need/use
		IF @lastBackupType='I'
			begin
				SET @queryToRun=N''
				SET @queryToRun = @queryToRun + N'SELECT TOP 1  bs.[backup_set_id]
															, bs.[backup_start_date]
															, bs.[type]
												FROM msdb.dbo.backupset bs
												INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
												WHERE	bs.[type] IN (''D'')
														AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
														AND bs.[backup_set_id] < ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
														AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
												ORDER BY bs.[backup_start_date] DESC'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #backupSET
				INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
						EXEC sp_executesql @queryToRun

				IF @sqlServerName<>@@SERVERNAME
					begin
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
								EXEC sp_executesql @queryToRun
					end

				SELECT TOP 1  @maxAllowedDate  = DATEADD(ss, -1, [backup_start_date])
							, @lastFullRemainingBackupSetID = [backup_set_id]
							, @lastBackupType = [type]
				FROM #backupSET
				ORDER BY [backup_start_date] DESC
			end

		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT TOP 1 bs.[backup_set_id]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''I''
												AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
												AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
										ORDER BY bs.[backup_set_id] DESC'

		DELETE FROM #backupSET
		INSERT	INTO #backupSET([backup_set_id])
				EXEC sp_executesql @queryToRun

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC sp_executesql @queryToRun
			end

		SELECT TOP 1  @lastDiffRemainingBackupSetID  = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC
	end
ELSE
	begin
		/* SET @maxAllowedDate = DATEADD(dd, -@retentionDays, GETDATE()) */
		--find first full database backup to allow @retentionDays database restore
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SET ROWCOUNT 1		
										SELECT bs.[backup_set_id], bs.[backup_start_date]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''D''
												AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date])
				EXEC sp_executesql @queryToRun

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id], [backup_start_date])
						EXEC sp_executesql @queryToRun
			end

		SELECT TOP 1  @maxAllowedDate = DATEADD(ss, -1, [backup_start_date])
					, @lastFullRemainingBackupSetID = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC

		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT TOP 1 bs.[backup_set_id]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''I''
												AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
												AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
										ORDER BY bs.[backup_set_id] DESC'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #backupSET
		INSERT	INTO #backupSET([backup_set_id])
				EXEC sp_executesql @queryToRun

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC sp_executesql @queryToRun
			end

		SELECT TOP 1  @lastDiffRemainingBackupSetID = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC
	end

-----------------------------------------------------------------------------------------
--for +2k5 versions, will use xp_delete_file
SET @errorCode=0
IF @flgOptions & 256 = 256
	begin
		SET @queryToRun = N'EXEC master.dbo.xp_delete_file 0, N''' + @backupLocation + ''', N''' + @backupFileExtension + ''', N''' + CONVERT([varchar](20), @maxAllowedDate, 120) + ''', 0'
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= NULL,
														@module			= 'dbo.usp_mpDatabaseBackupCleanup',
														@eventName		= 'database backup cleanup',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

IF @debugMode=1
	SELECT	@maxAllowedDate AS maxAllowedDate, 
			@lastFullRemainingBackupSetID AS lastFullBackupSetIDRemaining, 
			@lastDiffRemainingBackupSetID AS lastDiffBackupSetIDRemaining, 
			@forceChangeRetentionPolicy AS forceChangeRetentionPolicy,
			@flgOptions & 256,
			@errorCode,
			@serverVersionNum,
			@flgOptions & 128

-----------------------------------------------------------------------------------------
--in case of previous errors or 2k version, will use "standard" delete file
IF (@flgOptions & 256 = 0) OR (@errorCode<>0 AND @flgOptions & 256 = 256) OR (@flgOptions & 128 = 128 AND @lastFullRemainingBackupSetID IS NOT NULL)
	begin
		SET @optionXPValue = 0

		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
														, @flgAction	 = 1			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode

		IF @optionXPValue = 0
			begin
				RETURN 1
			end		
		
		/* identify backup files to be deleted, based on msdb information */
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT bs.[backup_set_id], bmf.[physical_device_name]
										FROM [msdb].[dbo].[backupset] bs
										INNER JOIN [msdb].[dbo].[backupmediafamily] bmf ON bmf.[media_set_id]=bs.[media_set_id]
										WHERE	(   (    bs.[backup_start_date] <= CONVERT([datetime], ''' + CONVERT([nvarchar](20), @maxAllowedDate, 120) + N''', 120)
														AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
														AND bmf.[physical_device_name] LIKE (''%.' + @backupFileExtension + N''')
														AND (	 (' + CAST(@flgOptions AS [nvarchar]) + N' & 256 = 0) 
															OR (' + CAST(@errorCode AS [nvarchar]) + N'<>0 AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 256 = 256) 
															)
													)
													OR (
															-- when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
															bs.[backup_set_id] < ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
														AND bs.[database_backup_lsn] <> ' + CAST(ISNULL(@lastFullRemainingFirstLSN, 0) AS [nvarchar]) + N'
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
													OR (
															-- delete incremental and transaction log backups to keep the retention/restore period fixed
															' + CAST(ISNULL(@lastDiffRemainingBackupSetID, 0)  AS [nvarchar]) + N' <> 0
														AND bs.[backup_set_id] < ' + CAST(ISNULL(@lastDiffRemainingBackupSetID, 0) AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + [dbo].[ufn_getObjectQuoteName](@dbName, 'sql') + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND CHARINDEX(''' + @backupLocation + ''', bmf.[physical_device_name]) <> 0
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
												)														
												AND bmf.[device_type] = 2'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupDevice([backup_set_id], [physical_device_name])
				EXEC sp_executesql @queryToRun

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupDevice([backup_set_id], [physical_device_name])
						EXEC sp_executesql @queryToRun
			end

		/* identify backup files to be deleted, based on file existence on disk */
		/* use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default) */
		IF @flgOptions & 128 = 128 AND @flgOptions & 4096 = 4096
			begin
				IF OBJECT_ID('tempdb..#backupFilesOnDisk') IS NOT NULL DROP TABLE #backupFilesOnDisk
				CREATE TABLE #backupFilesOnDisk
				(
					  [id]			[int] IDENTITY(1, 1)
					, [file_name]	[nvarchar](260)
					, [depth]		[int]
					, [is_file]		[int]
					, [create_time]	[varchar](20)
				)

				SET @queryToRun = N'EXEC xp_dirtree ''' + @backupLocation + ''', 8, 1';
				IF @sqlServerName<>@@SERVERNAME
					begin
						IF @serverVersionNum < 11
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
						ELSE
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([subdirectory] [nvarchar](260), [depth] [int], [file] [int]))'')'
					end

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #backupFilesOnDisk
				INSERT INTO #backupFilesOnDisk([file_name], [depth], [is_file])
						EXEC sp_executesql @queryToRun

				/* remove files which are no longer on disk */
				IF EXISTS(SELECT * FROM #backupFilesOnDisk)
					DELETE bd
					FROM #backupDevice bd
					LEFT JOIN #backupFilesOnDisk bf ON (@backupLocation + bf.[file_name]) = bd.[physical_device_name] 
					WHERE bf.[file_name] IS NULL
			end


		/* remove the backup files, one by one */
		DECLARE crsCleanupBackupFiles CURSOR LOCAL FAST_FORWARD FOR	SELECT [physical_device_name]
																	FROM #backupDevice														
																	ORDER BY [backup_set_id] ASC
		OPEN crsCleanupBackupFiles
		FETCH NEXT FROM crsCleanupBackupFiles INTO @backupFileName
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N'EXEC ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted')+ '.[dbo].[usp_mpDeleteFileOnDisk]	@sqlServerName	= ''' + @sqlServerName + N''',
																							@fileName		= ''' + [dbo].[ufn_getObjectQuoteName](@backupFileName, 'sql') + N''',
																							@executionLevel	= ' + CAST(@nestedExecutionLevel AS [nvarchar]) + N',
																							@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
																@dbName			= NULL,
																@module			= 'dbo.usp_mpDatabaseBackupCleanup',
																@eventName		= 'database backup cleanup',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode

				FETCH NEXT FROM crsCleanupBackupFiles INTO @backupFileName
			end
		CLOSE crsCleanupBackupFiles
		DEALLOCATE crsCleanupBackupFiles

		/* disable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
														, @flgAction	 = 0			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode
	end

RETURN @errorCode
GO
