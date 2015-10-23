USE [dbaTDPMon]
GO

UPDATE [dbo].[appConfigurations] SET [value] = N'2015.10.23' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('Rebuild table: [dbo].[appConfigurations]', 10, 1) WITH NOWAIT
GO
IF EXISTS(SELECT * FROM sys.objects WHERE [name] = 'appConfigurations_Backup' AND type='U')
	DROP TABLE dbo.appConfigurations_Backup
GO	

SELECT * 
INTO dbo.appConfigurations_Backup
FROM dbo.appConfigurations
GO

TRUNCATE TABLE [dbo].[appConfigurations]
Go

INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'common'			AS [module], 'Application Version'															AS [name], N'2015.10.16'		AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Default project code'															AS [name], N'$(projectCode)'	AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Database Mail profile name to use for sending emails'							AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Default recipients list - Reports (semicolon separated)'						AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Default recipients list - Job Status (semicolon separated)'					AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Default recipients list - Alerts (semicolon separated)'						AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Local storage path for HTML reports'											AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'HTTP address for report files'												AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Alert repeat interval (minutes)'												AS [name], '60'			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Notify job status only for Failed jobs'										AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Ignore alerts for: Error 1222 - Lock request time out period exceeded'		AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Log action events'															AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Log events retention (days)'													AS [name], '15'			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Default lock timeout (ms)'													AS [name], '5000'		AS [value]		UNION ALL

		  SELECT 'maintenance-plan' AS [module], 'Default backup location'														AS [name], NULL			AS [value]		UNION ALL
		  SELECT 'maintenance-plan' AS [module], 'Default backup retention (days)'												AS [name], '7'			AS [value]		UNION ALL
		  SELECT 'maintenance-plan' AS [module], 'Change retention policy from RetentionDays to RetentionBackupsCount'			AS [name], 'true'		AS [value]		UNION ALL
		  SELECT 'maintenance-plan' AS [module], 'Force cleanup of ghost records'												AS [name], 'false'		AS [value]		UNION ALL
		  SELECT 'maintenance-plan' AS [module], 'Ghost records cleanup threshold'												AS [name], '131072'		AS [value]		UNION ALL

		  SELECT 'health-check'		AS [module], 'Collect SQL Agent jobs step details'											AS [name], 'false'		AS [value]		UNION ALL
		  SELECT 'health-check'		AS [module], 'Collect SQL Errorlog last files'												AS [name], '1'			AS [value]		UNION ALL
		  SELECT 'health-check'		AS [module], 'Collect Warning OS Events'													AS [name], 'false'		AS [value]		UNION ALL
		  SELECT 'health-check'		AS [module], 'Collect Information OS Events'												AS [name], 'false'		AS [value]		UNION ALL
		  SELECT 'health-check'		AS [module], 'Collect OS Events timeout (seconds)'											AS [name], '600'		AS [value]		UNION ALL
		  SELECT 'health-check'		AS [module], 'Collect OS Events from last hours'											AS [name], '24'			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Parallel Data Collecting Jobs'												AS [name], '16'			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Maximum number of retries at failed job'										AS [name], '3'			AS [value]		UNION ALL
		  SELECT 'common'			AS [module], 'Fail master job if any queued job fails'										AS [name], 'false'		AS [value]
GO

UPDATE new
	SET new.[value] = old.[value]
FROM [dbo].[appConfigurations] new
INNER JOIN [dbo].[appConfigurations_Backup] old ON new.[module] = old.[module] AND new.[name] = old.[name]
GO

SELECT * FROM [dbo].[appConfigurations]
GO

IF EXISTS(SELECT * FROM sys.objects WHERE [name] = 'appConfigurations_Backup' AND type='U')
	DROP TABLE dbo.appConfigurations_Backup
GO	


RAISERROR('Create procedure: [dbo].[usp_createFolderOnDisk]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_createFolderOnDisk]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_createFolderOnDisk]
GO

CREATE PROCEDURE [dbo].[usp_createFolderOnDisk]
		@sqlServerName			[sysname],
		@folderName				[nvarchar](1024),
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

DECLARE   @queryToRun				[nvarchar](1024)
		, @serverToRun				[nvarchar](512)
		, @errorCode				[int]

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @nestedExecutionLevel		[tinyint]
		, @warningMessage			[nvarchar](1024)
		, @runWithxpCreateSubdir	[bit]
		, @retryAttempts			[tinyint]

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

IF object_id('#fileExists') IS NOT NULL DROP TABLE #fileExists
CREATE TABLE #fileExists
			(
				[file_exists]				[bit]	NULL,
				[file_is_directory]			[bit]	NULL,
				[parent_directory_exists]	[bit]	NULL
			)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF RIGHT(@folderName, 1)<>'\' SET @folderName = @folderName + N'\'

SET @queryToRun= 'Creating destination folder: "' + @folderName + '"'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

-----------------------------------------------------------------------------------------
--get destination server running version/edition
SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* check if folderName exists																									 */
IF @sqlServerName=@@SERVERNAME
		SET @queryToRun = N'master.dbo.xp_fileexist ''' + @folderName + ''''
else
		SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @folderName + ''''';'')x'

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
INSERT	INTO #fileExists([file_exists], [file_is_directory], [parent_directory_exists])
		EXEC (@queryToRun)

SET @warningMessage = N''
IF (SELECT [parent_directory_exists] FROM #fileExists)=0
	begin
		SET @warningMessage = N'WARNING: Root folder does not exists or it is not available.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @warningMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
	end

SET @retryAttempts=3

IF (SELECT [file_is_directory] FROM #fileExists)=1
	begin
		SET @queryToRun = N'Folder already exists.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
	end
ELSE
	WHILE (SELECT [file_is_directory] FROM #fileExists)=0 AND @retryAttempts > 0
		begin
			SET @runWithxpCreateSubdir=0

			SELECT  @optionXPIsAvailable		= 0,
					@optionXPValue				= 0,
					@optionXPHasChanged			= 0,
					@optionAdvancedIsAvailable	= 0,
					@optionAdvancedValue		= 0,
					@optionAdvancedHasChanged	= 0

			IF @serverVersionNum>=9
				begin
					/*-------------------------------------------------------------------------------------------------------------------------------*/
					SET @queryToRun = N'[' + @sqlServerName + '].master.sys.xp_create_subdir N''' + @folderName + ''''
					IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
					EXEC (@queryToRun)
				
					IF @@ERROR=0
						SET @runWithxpCreateSubdir=1
					ELSE
						begin
							/* enable xp_cmdshell configuration option */
							EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																				@configOptionName	= 'xp_cmdshell',
																				@configOptionValue	= 1,
																				@optionIsAvailable	= @optionXPIsAvailable OUT,
																				@optionCurrentValue	= @optionXPValue OUT,
																				@optionHasChanged	= @optionXPHasChanged OUT,
																				@executionLevel		= 0,
																				@debugMode			= @debugMode

							IF @optionXPIsAvailable = 0
								begin
									/* enable show advanced options configuration option */
									EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																						@configOptionName	= 'show advanced options',
																						@configOptionValue	= 1,
																						@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																						@optionCurrentValue	= @optionAdvancedValue OUT,
																						@optionHasChanged	= @optionAdvancedHasChanged OUT,
																						@executionLevel		= 0,
																						@debugMode			= @debugMode

									IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
										EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																							@configOptionName	= 'xp_cmdshell',
																							@configOptionValue	= 1,
																							@optionIsAvailable	= @optionXPIsAvailable OUT,
																							@optionCurrentValue	= @optionXPValue OUT,
																							@optionHasChanged	= @optionXPHasChanged OUT,
																							@executionLevel		= 0,
																							@debugMode			= @debugMode
								end

							IF @optionXPIsAvailable=0 OR @optionXPValue=0
								begin
									set @queryToRun='xp_cmdshell component is turned off. Cannot continue'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
									RETURN 1
								end		
						end
				end

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			/* creating folder   																											 */
			IF @runWithxpCreateSubdir=0
				begin
					SET @queryToRun = N'MKDIR -P "' + @folderName + '"'
					SET @serverToRun = N'[' + @sqlServerName + '].master.dbo.xp_cmdshell'
					IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
					EXEC @serverToRun @queryToRun , NO_OUTPUT
				end

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			IF @serverVersionNum>=9 AND @runWithxpCreateSubdir=0 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
				begin
					/* disable xp_cmdshell configuration option */
					IF @optionXPHasChanged = 1
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																			@configOptionName	= 'xp_cmdshell',
																			@configOptionValue	= 0,
																			@optionIsAvailable	= @optionXPIsAvailable OUT,
																			@optionCurrentValue	= @optionXPValue OUT,
																			@optionHasChanged	= @optionXPHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode

					/* disable show advanced options configuration option */
					IF @optionAdvancedHasChanged = 1
							EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																				@configOptionName	= 'show advanced options',
																				@configOptionValue	= 0,
																				@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																				@optionCurrentValue	= @optionAdvancedValue OUT,
																				@optionHasChanged	= @optionAdvancedHasChanged OUT,
																				@executionLevel		= 0,
																				@debugMode			= @debugMode
				end


			---------------------------------------------------------------------------------------------
			/* get configuration values - wait/lock timeout */
			DECLARE @queryLockTimeOut [int]
			SELECT	@queryLockTimeOut=[value] 
			FROM	[dbo].[appConfigurations] 
			WHERE	[name] = 'Default lock timeout (ms)'
					AND [module] = 'common'


			SET @queryLockTimeOut = @queryLockTimeOut / 1000
			DECLARE @waitDelay [varchar](16)

			SET @waitDelay = REPLICATE('0', 2-LEN(CAST(@queryLockTimeOut/3600 AS [varchar]))) + CAST(@queryLockTimeOut/3600 AS [varchar]) + ':' + 
							 REPLICATE('0', 2-LEN(CAST((@queryLockTimeOut%3600)/60 AS [varchar]))) + CAST((@queryLockTimeOut%3600)/60 AS [varchar]) + ':' +
							 REPLICATE('0', 2-LEN(CAST(@queryLockTimeOut%60 AS [varchar]))) + CAST(@queryLockTimeOut%60 AS [varchar])

			--wait 5 seconds before
			WAITFOR DELAY @waitDelay

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			/* check if folderName exists																									 */
			IF @sqlServerName=@@SERVERNAME
					SET @queryToRun = N'master.dbo.xp_fileexist ''' + @folderName + ''''
			else
					SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @folderName + ''''';'')x'

			IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			DELETE FROM #fileExists
			INSERT	INTO #fileExists([file_exists], [file_is_directory], [parent_directory_exists])
					EXEC (@queryToRun)

			IF (SELECT [file_is_directory] FROM #fileExists)=0
				SET @retryAttempts=@retryAttempts - 1
			ELSE
				SET @retryAttempts=0
		end

IF (SELECT [file_is_directory] FROM #fileExists)=0
	begin
		SET @queryToRun = CASE WHEN @warningMessage <> N'' THEN @warningMessage + N' ' ELSE N'' END + N'ERROR: Destination folder cannot be created.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end
ELSE
	begin
		SET @queryToRun = N'Folder was successfully created.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
	end

RETURN 0
GO

