USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.6 to 2017.10 (2017.10.02)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.10.02' WHERE [module] = 'common' AND [name] = 'Application Version'
GO


/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: common																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMON', 10, 1) WITH NOWAIT

RAISERROR('Create procedure: [dbo].[usp_changeServerOption_xp_cmdshell]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_changeServerOption_xp_cmdshell]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_changeServerOption_xp_cmdshell]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_changeServerOption_xp_cmdshell]
		  @serverToRun			[sysname]
		, @flgAction			[tinyint] = 1 -- 1=enable | 0=disable
		, @optionXPValue		[bit] = 0 OUTPUT
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2017 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 02.10.2017
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE @queryToRun					[nvarchar](1024), 
		@optionXPIsAvailable		[bit],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit],
		@currentSPIDCounterValue	[int],
		@currentAllCounterValue		[int]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
SELECT  @optionXPIsAvailable		= 0,
		@optionXPValue				= 0,
		@optionXPHasChanged			= 0,
		@optionAdvancedIsAvailable	= 0,
		@optionAdvancedValue		= 0,
		@optionAdvancedHasChanged	= 0,
		@currentSPIDCounterValue	= 0,
		@currentAllCounterValue		= 0

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @flgAction = 1
	begin
		IF OBJECT_ID('tempdb..##tdp_xp_cmdshell_requests') IS NULL
			CREATE TABLE ##tdp_xp_cmdshell_requests
				(
					  [spid]						[smallint]	NOT NULL
					, [option_xp_changed]			[bit]		NOT NULL DEFAULT (0)
					, [option_advanced_changed]		[bit]		NOT NULL DEFAULT (0)
					, [counter]						[int]		NOT NULL DEFAULT (0)
				)

		/* try to update counter value */
		UPDATE ##tdp_xp_cmdshell_requests 
			SET [counter] = [counter] + 1
		WHERE [spid] = @@SPID

		/* current session did not requested xp_cmdshell enable, yet */
		IF @@ROWCOUNT = 0 
			begin
				/* if no other session turned the option on, will enable it */
				IF NOT EXISTS (
								SELECT *
								FROM ##tdp_xp_cmdshell_requests
								WHERE [counter] > 0
							  )
					begin
						/* enable xp_cmdshell configuration option */
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
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
								EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																					@configOptionName	= 'show advanced options',
																					@configOptionValue	= 1,
																					@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																					@optionCurrentValue	= @optionAdvancedValue OUT,
																					@optionHasChanged	= @optionAdvancedHasChanged OUT,
																					@executionLevel		= 0,
																					@debugMode			= @debugMode

								IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
									EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
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
								set @queryToRun='xp_cmdshell component is turned off. Cannot continue.'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

								RETURN 1
							end		
						ELSE
							begin
								--mark the xp_cmdshell enable request
								INSERT	INTO ##tdp_xp_cmdshell_requests([spid], [option_xp_changed], [option_advanced_changed], [counter])
										SELECT @@SPID, @optionXPHasChanged, @optionAdvancedHasChanged, 1
							end
					end
				ELSE
					begin
						/* preserve old flags, for current session */
						SELECT   @optionXPHasChanged = MAX(CAST([option_xp_changed] AS [tinyint]))
							   , @optionAdvancedHasChanged = MAX(CAST([option_advanced_changed] AS [tinyint]))
						FROM ##tdp_xp_cmdshell_requests

						--mark the xp_cmdshell enable request
						INSERT	INTO ##tdp_xp_cmdshell_requests([spid], [option_xp_changed], [option_advanced_changed], [counter])
								SELECT @@SPID, @optionXPHasChanged, @optionAdvancedHasChanged, 1
					end
			end
		ELSE
			SET @optionXPValue = 1
	end


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @flgAction = 0 AND OBJECT_ID('tempdb..##tdp_xp_cmdshell_requests') IS NOT NULL
	begin
		/* get current session counter value */
		SELECT  @currentSPIDCounterValue  = [counter]
			  , @optionXPHasChanged		  = [option_xp_changed]
			  , @optionAdvancedHasChanged = [option_advanced_changed]
		FROM ##tdp_xp_cmdshell_requests 
		WHERE [spid] = @@SPID

		/* get all sessions counter value */
		SELECT @currentAllCounterValue  = SUM([counter])
		FROM ##tdp_xp_cmdshell_requests 

		IF @currentAllCounterValue = 1
			begin
				/* disable xp_cmdshell configuration option */
				IF @optionXPHasChanged = 1
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 0,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode

				/* disable show advanced options configuration option */
				IF @optionAdvancedHasChanged = 1
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 0,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode
			end
		ELSE
			SET @optionXPValue = 1

		/* decrement counter value. when 0, remove the entry */
		SET @currentSPIDCounterValue = @currentSPIDCounterValue - 1
		
		IF @currentSPIDCounterValue = 0
			DELETE FROM ##tdp_xp_cmdshell_requests 
			WHERE [spid] = @@SPID
		ELSE
			UPDATE ##tdp_xp_cmdshell_requests
				SET [counter] = @currentSPIDCounterValue
			WHERE [spid] = @@SPID

		IF @currentAllCounterValue = 1 AND OBJECT_ID('tempdb..##tdp_xp_cmdshell_requests') IS NOT NULL
			DROP TABLE ##tdp_xp_cmdshell_requests
	end

	RETURN 0
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
		, @optionXPValue			[int]

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
	begin
		IF @serverVersionNum < 11	
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @folderName + ''''';'')x'
		ELSE
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''master.dbo.xp_fileexist ''''''''' + @folderName + ''''''''' '''') WITH RESULT SETS(([File Exists] [int], [File is a Directory] [int], [Parent Directory Exists] [int])) '')x'
	end

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
			SET @runWithxpCreateSubdir = 0
			SET @optionXPValue = 0

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
							EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																			, @flgAction	 = 1			-- 1=enable | 0=disable
																			, @optionXPValue = @optionXPValue OUTPUT
																			, @debugMode	 = @debugMode

							IF @optionXPValue = 0
								begin
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
			IF @serverVersionNum>=9 AND @runWithxpCreateSubdir=0
				begin
					/* disable xp_cmdshell configuration option */
					EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																	, @flgAction	 = 0			-- 1=enable | 0=disable
																	, @optionXPValue = @optionXPValue OUTPUT
																	, @debugMode	 = @debugMode

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
				begin
					IF @serverVersionNum < 11	
						SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @folderName + ''''';'')x'
					ELSE
						SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''master.dbo.xp_fileexist ''''''''' + @folderName + ''''''''' '''') WITH RESULT SETS(([File Exists] [int], [File is a Directory] [int], [Parent Directory Exists] [int])) '')x'
				end

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

RAISERROR('Create procedure: [dbo].[usp_refreshMachineCatalogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_refreshMachineCatalogs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_refreshMachineCatalogs]
GO

CREATE PROCEDURE [dbo].[usp_refreshMachineCatalogs]
		@projectCode		[varchar](32)=NULL,
		@sqlServerName		[sysname],
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date: 2015.04.03 / Andrei STEFAN
-- Description: add domain name to machine information
-----------------------------------------------------------------------------------------


SET NOCOUNT ON

DECLARE   @returnValue			[smallint]
		, @errMessage			[nvarchar](4000)
		, @errDescriptor		[nvarchar](256)
		, @errNumber			[int]

DECLARE   @queryToRun			[nvarchar](max)	-- used for dynamic statements
		, @projectID			[smallint]
		, @isClustered			[bit]
		, @isActive				[bit]
		, @instanceID			[smallint]
		, @domainName			[sysname]
		, @optionXPValue		[int]

-- { sql_statement | statement_block }
BEGIN TRY
	SET @returnValue=1

	-----------------------------------------------------------------------------------------------------
	SET @errMessage=N'--Getting Instance information: [' + @sqlServerName + '] / project: [' + @projectCode + ']'
	RAISERROR(@errMessage, 10, 1) WITH NOWAIT
	SET @errMessage=N''
	-----------------------------------------------------------------------------------------------------

	-----------------------------------------------------------------------------------------------------
	--check that SQLServerName is defined as local or as a linked server to current sql server instance
	-----------------------------------------------------------------------------------------------------
	IF (SELECT count(*) FROM sys.sysservers WHERE srvname=@sqlServerName)=0
		begin
			PRINT N'Specified instance name is not defined as local or linked server: ' + @sqlServerName
			PRINT N'Create a new linked server.'

			/* create a linked server for the instance found */
			EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
		end


	-----------------------------------------------------------------------------------------------------
	IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
	CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

	-----------------------------------------------------------------------------------------------------
	IF object_id('tempdb..#xpCMDShellOutput') IS NOT NULL 
	DROP TABLE #xpCMDShellOutput

	CREATE TABLE #xpCMDShellOutput
	(
		[output]	[nvarchar](max)			NULL
	)
			
	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogMachineNames') IS NOT NULL 
	DROP TABLE #catalogMachineNames

	CREATE TABLE #catalogMachineNames
	(
		[name]					[sysname]		NULL,
		[domain]				[sysname]		NULL
	)

	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogInstanceNames') IS NOT NULL 
	DROP TABLE #catalogInstanceNames

	CREATE TABLE #catalogInstanceNames
	(
		[name]					[sysname]		NULL,
		[version]				[sysname]		NULL,
		[edition]				[varchar](256)	NULL,
		[machine_name]			[sysname]		NULL
	)

	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogDatabaseNames') IS NOT NULL 
	DROP TABLE #catalogDatabaseNames

	CREATE TABLE #catalogDatabaseNames
	(
		[database_id]			[int]			NULL,
		[name]					[sysname]		NULL,
		[state]					[int]			NULL,
		[state_desc]			[nvarchar](64)	NULL
	)

	------------------------------------------------------------------------------------------------------------------------------------------
	--get default project code
	IF @projectCode IS NULL
		SELECT	@projectCode = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Default project code'
				AND [module] = 'common'

	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'ERROR: The value specifief for Project Code is not valid.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		end

	-----------------------------------------------------------------------------------------------------
	--check if the connection to machine can be made & discover instance name
	-----------------------------------------------------------------------------------------------------
	SET @queryToRun = N'SELECT    @@SERVERNAME
								, [product_version]
								, [edition]
								, [machine_name]
						FROM (
								SELECT CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]) AS [product_version]
									 , SUBSTRING(@@VERSION, 1, CHARINDEX(CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]), @@VERSION)-1) + CAST(SERVERPROPERTY(''Edition'') AS [sysname]) AS [edition]
									 , CAST(SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [sysname]) AS [machine_name]
							 )X'
	SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
	IF @debugMode = 1 PRINT @queryToRun

	BEGIN TRY
		INSERT	INTO #catalogInstanceNames([name], [version], [edition], [machine_name])
				EXEC (@queryToRun)
		SET @isActive=1
	END TRY
	BEGIN CATCH
		SET @errMessage=ERROR_MESSAGE()
		SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs - Offline'
		RAISERROR(@errMessage, 10, 1) WITH NOWAIT

		SET @isActive=0
	END CATCH
	

	IF @isActive=0
		begin
			INSERT	INTO #catalogMachineNames([name])
					SELECT cmn.[name]
					FROM [dbo].[catalogMachineNames] cmn
					INNER JOIN [dbo].[catalogInstanceNames] cin ON cmn.[id] = cin.[machine_id] AND cmn.[project_id] = cin.[project_id]
					WHERE cin.[project_id] = @projectID
							AND cin.[name] = @sqlServerName
			
			IF @@ROWCOUNT=0				
				INSERT	INTO #catalogMachineNames([name])					
						SELECT SUBSTRING(@sqlServerName, 1, CASE WHEN CHARINDEX('\', @sqlServerName) > 0 THEN CHARINDEX('\', @sqlServerName)-1 ELSE LEN(@sqlServerName) END)
			
			INSERT	INTO #catalogInstanceNames([name], [version])
					SELECT @sqlServerName, NULL
			
			SET @isClustered = 0
		end
	ELSE
		begin
			DECLARE @SQLMajorVersion [int]

			BEGIN TRY
				SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL([version], ''), 2), '.', '') 
				FROM #catalogInstanceNames
			END TRY
			BEGIN CATCH
				SET @SQLMajorVersion = 8
			END CATCH

			-----------------------------------------------------------------------------------------------------
			--discover machine names (if clustered instance is present, get all cluster nodes)
			-----------------------------------------------------------------------------------------------------
			SET @isClustered=0

			IF @SQLMajorVersion<=8
				SET @queryToRun = N'SELECT [NodeName] FROM ::fn_virtualservernodes()'
			ELSE
				SET @queryToRun = N'SELECT [NodeName] FROM sys.dm_os_cluster_nodes'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun
			
			BEGIN TRY
				INSERT	INTO #catalogMachineNames([name])
						EXEC (@queryToRun)		
			END TRY
			BEGIN CATCH
				IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()
			END CATCH
	
			IF (SELECT COUNT(*) FROM #catalogMachineNames)=0
				begin
					SET @queryToRun = N'SELECT CASE WHEN [computer_name] IS NOT NULL 
													THEN [computer_name]
													ELSE [machine_name]
											  END
										FROM (
												SELECT CAST(SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [sysname]) AS [computer_name]
											)X,
											(
												SELECT CAST(SERVERPROPERTY(''MachineName'') AS [sysname]) AS [machine_name]
											)Y'
					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 PRINT @queryToRun

					BEGIN TRY
						INSERT	INTO #catalogMachineNames([name])
								EXEC (@queryToRun)
					END TRY
					BEGIN CATCH
						SET @errMessage=ERROR_MESSAGE()
						SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
						RAISERROR(@errMessage, 16, 1) WITH NOWAIT
					END CATCH
				end
			ELSE
				begin
					SET @isClustered = 1
				end
				
			
			-----------------------------------------------------------------------------------------------------
			--discover database names
			-----------------------------------------------------------------------------------------------------
			IF @SQLMajorVersion<=8
				SET @queryToRun = N'SELECT sdb.[dbid], sdb.[name], sdb.[status] AS [state]
											, CASE  WHEN sdb.[status] & 4194584 = 4194584 THEN ''SUSPECT''
													WHEN sdb.[status] & 2097152 = 2097152 THEN ''STANDBY''
													WHEN sdb.[status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													WHEN sdb.[status] & 4096 = 4096 THEN ''SINGLE USER''
													WHEN sdb.[status] & 2048 = 2048 THEN ''DBO USE ONLY''
													WHEN sdb.[status] & 1024 = 1024 THEN ''READ ONLY''
													WHEN sdb.[status] & 512 = 512 THEN ''OFFLINE''
													WHEN sdb.[status] & 256 = 256 THEN ''NOT RECOVERED''
													WHEN sdb.[status] & 128 = 128 THEN ''RECOVERING''
													WHEN sdb.[status] & 64 = 64 THEN ''PRE RECOVERY''
													WHEN sdb.[status] & 32 = 32 THEN ''LOADING''
													WHEN sdb.[status] = 0 THEN ''UNKNOWN''
													ELSE ''ONLINE''
												END AS [state_desc]
									FROM master.dbo.sysdatabases sdb'
			ELSE
				SET @queryToRun = N'SELECT sdb.[database_id], sdb.[name], sdb.[state], sdb.[state_desc]
									FROM sys.databases sdb
									WHERE [is_read_only] = 0 AND [is_in_standby] = 0
									UNION ALL
									SELECT sdb.[database_id], sdb.[name], sdb.[state], ''READ ONLY''
									FROM sys.databases sdb
									WHERE [is_read_only] = 1 AND [is_in_standby] = 0
									UNION ALL
									SELECT sdb.[database_id], sdb.[name], sdb.[state], ''STANDBY''
									FROM sys.databases sdb
									WHERE [is_in_standby] = 1'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun

			BEGIN TRY
				INSERT	INTO #catalogDatabaseNames([database_id], [name], [state], [state_desc])
						EXEC (@queryToRun)		
			END TRY
			BEGIN CATCH
				SET @errMessage=ERROR_MESSAGE()
				SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
				RAISERROR(@errMessage, 16, 1) WITH NOWAIT
			END CATCH

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			/* check if xp_cmdshell is enabled or should be enabled																			 */
			BEGIN TRY
				IF @SQLMajorVersion>8
					begin
						SET @optionXPValue = 0

						/* enable xp_cmdshell configuration option */
						EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																		, @flgAction	 = 1			-- 1=enable | 0=disable
																		, @optionXPValue = @optionXPValue OUTPUT
																		, @debugMode	 = @debugMode
					end

				IF @optionXPValue=1 OR @SQLMajorVersion=8
					begin
						BEGIN TRY
							--run wmi to get the domain name
							SET @queryToRun = N''
							SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic computersystem get Domain''; EXEC xp_cmdshell @cmdQuery;'
			
							IF @sqlServerName<>@@SERVERNAME
								SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
							IF @debugMode = 1 PRINT @queryToRun

							INSERT	INTO #xpCMDShellOutput([output])
									EXEC (@queryToRun)
									
							UPDATE #xpCMDShellOutput SET [output]=REPLACE(REPLACE(REPLACE(LTRIM(RTRIM([output])), ' ', ''), CHAR(10), ''), CHAR(13), '')
			
							DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3 OR [output] IS NULL
							DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%not recognized as an internal or external command%'
							DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%operable program or batch file%'
							DELETE TOP (1) FROM #xpCMDShellOutput WHERE SUBSTRING([output], 1, 8)='Domain'
			
							SELECT TOP 1 @domainName = LOWER([output])
							FROM #xpCMDShellOutput
						END TRY
						BEGIN CATCH
							SET @queryToRun = N''
							SET @queryToRun = @queryToRun + N'SELECT DEFAULT_DOMAIN()';
							IF @sqlServerName<>@@SERVERNAME
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
							IF @debugMode = 1 PRINT @queryToRun
							INSERT	INTO #xpCMDShellOutput([output])
									EXEC (@queryToRun)
							SELECT TOP 1 @domainName = LOWER([output])
								FROM #xpCMDShellOutput
						END CATCH

						UPDATE #catalogMachineNames SET [domain] = @domainName
					end

				IF @SQLMajorVersion>8 
					begin
						/* disable xp_cmdshell configuration option */
						EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																		, @flgAction	 = 0			-- 1=enable | 0=disable
																		, @optionXPValue = @optionXPValue OUTPUT
																		, @debugMode	 = @debugMode
					end
			END TRY
			BEGIN CATCH
				PRINT ERROR_MESSAGE()
			END CATCH
		end


	-----------------------------------------------------------------------------------------------------
	--upsert catalog tables
	-----------------------------------------------------------------------------------------------------
/*
	MERGE INTO [dbo].[catalogMachineNames] AS dest
	USING (	
			SELECT [name], [domain]
			FROM #catalogMachineNames
		  ) AS src([name], [domain])
		ON dest.[name] = src.[name] AND dest.[project_id] = @projectID
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([project_id], [name], [domain]) 
		VALUES (@projectID, src.[name], src.[domain]) 
	WHEN MATCHED THEN
		UPDATE SET dest.[domain]=src.[domain];
*/
	UPDATE dest
		SET dest.[domain]=src.[domain]
	FROM [dbo].[catalogMachineNames] AS dest
	INNER JOIN 
			(	
			 SELECT [name], [domain]
			 FROM #catalogMachineNames
			) src ON dest.[name] = src.[name] AND dest.[project_id] = @projectID;

	INSERT	INTO [dbo].[catalogMachineNames] ([project_id], [name], [domain]) 
			SELECT @projectID, src.[name], src.[domain]
			FROM 
				(	
					 SELECT [name], [domain]
					 FROM #catalogMachineNames
				) src 
			LEFT JOIN [dbo].[catalogMachineNames] AS dest ON dest.[name] = src.[name] AND dest.[project_id] = @projectID			
			WHERE dest.[name] IS NULL;

	UPDATE dest
		SET   dest.[is_clustered] = src.[is_clustered]
			, dest.[version] = src.[version]
			, dest.[active] = CASE WHEN src.[is_clustered]=1
									THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
												THEN 1 
												ELSE 0
										 END
									ELSE src.[active]
								END
			, dest.[edition] = src.[edition]
			, dest.[cluster_node_machine_id] = src.[cluster_node_machine_id]
			, dest.[last_refresh_date_utc] = GETUTCDATE()
	FROM [dbo].[catalogInstanceNames] AS dest
	INNER JOIN
		 (	
			SELECT  cmn.[id]	  AS [machine_id]
				  , cin.[name]	  AS [name]
				  , cin.[version]
				  , cin.[edition]
				  , @isClustered  AS [is_clustered]
				  , @isActive	  AS [active]
				  , cmnA.[id]	  AS [cluster_node_machine_id]
			FROM #catalogInstanceNames cin
			INNER JOIN #catalogMachineNames src ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON		cmn.[name] = src.[name] 
															AND cmn.[project_id]=@projectID
			LEFT  JOIN [dbo].[catalogMachineNames] cmnA ON		cmnA.[name] = cin.[machine_name] 
															AND cmnA.[project_id]=@projectID 
															AND @isClustered=1
		  ) AS src	ON dest.[machine_id] = src.[machine_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID;

	INSERT INTO [dbo].[catalogInstanceNames]([machine_id], [project_id], [name], [version], [edition], [is_clustered], [active], [cluster_node_machine_id], [last_refresh_date_utc]) 
			SELECT   src.[machine_id], @projectID, src.[name], src.[version], src.[edition], src.[is_clustered]
					, CASE WHEN src.[is_clustered]=1
							THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
										THEN 1 
										ELSE 0
								 END
							ELSE src.[active]
					 END
					, src.[cluster_node_machine_id]
					, GETUTCDATE()
			FROM (	
					SELECT  cmn.[id]	  AS [machine_id]
						  , cin.[name]	  AS [name]
						  , cin.[version]
						  , cin.[edition]
						  , @isClustered  AS [is_clustered]
						  , @isActive	  AS [active]
						  , cmnA.[id]	  AS [cluster_node_machine_id]
					FROM #catalogInstanceNames cin
					INNER JOIN #catalogMachineNames src ON 1=1
					INNER JOIN [dbo].[catalogMachineNames] cmn ON		cmn.[name] = src.[name] 
																	AND cmn.[project_id]=@projectID
					LEFT  JOIN [dbo].[catalogMachineNames] cmnA ON		cmnA.[name] = cin.[machine_name] 
																	AND cmnA.[project_id]=@projectID 
																	AND @isClustered=1
				  ) AS src
			LEFT JOIN [dbo].[catalogInstanceNames] AS dest ON dest.[machine_id] = src.[machine_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID
			WHERE dest.[machine_id] IS NULL;


	UPDATE cdn
		SET cdn.[active] = 0
	FROM [dbo].[catalogDatabaseNames] cdn
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
	INNER JOIN #catalogInstanceNames	srcIN ON cin.[name] = srcIN.[name]
	WHERE cin.[project_id] = @projectID

	/*
	MERGE INTO [dbo].[catalogDatabaseNames] AS dest
	USING (	
			SELECT  cin.[id] AS [instance_id]
				  , src.[name]
				  , src.[database_id]
				  , src.[state]
				  , src.[state_desc]
			FROM  #catalogDatabaseNames src
			INNER JOIN #catalogMachineNames srcMn ON 1=1
			INNER JOIN #catalogInstanceNames srcIN ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
			INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]
		  ) AS src([instance_id], [name], [database_id], [state], [state_desc])
		ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([instance_id], [project_id], [database_id], [name], [state], [state_desc], [active])
		VALUES (src.[instance_id], @projectID, src.[database_id], src.[name], src.[state], src.[state_desc], 1)
	WHEN MATCHED THEN
		UPDATE SET	dest.[database_id] = src.[database_id]
				  , dest.[state] = src.[state]
				  , dest.[state_desc] = src.[state_desc]
				  , dest.[active] = 1;
	*/

	UPDATE dest
		SET	dest.[database_id] = src.[database_id]
					  , dest.[state] = src.[state]
					  , dest.[state_desc] = src.[state_desc]
					  , dest.[active] = 1
	FROM [dbo].[catalogDatabaseNames] AS dest
	INNER JOIN
		 (	
			SELECT  cin.[id] AS [instance_id]
				  , src.[name]
				  , src.[database_id]
				  , src.[state]
				  , src.[state_desc]
			FROM  #catalogDatabaseNames src
			INNER JOIN #catalogMachineNames srcMn ON 1=1
			INNER JOIN #catalogInstanceNames srcIN ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
			INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]
		  ) AS src ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name];


	INSERT INTO [dbo].[catalogDatabaseNames]([instance_id], [project_id], [database_id], [name], [state], [state_desc], [active])
			SELECT src.[instance_id], @projectID, src.[database_id], src.[name], src.[state], src.[state_desc], 1
			FROM (	
					SELECT  cin.[id] AS [instance_id]
						  , src.[name]
						  , src.[database_id]
						  , src.[state]
						  , src.[state_desc]
					FROM  #catalogDatabaseNames src
					INNER JOIN #catalogMachineNames srcMn ON 1=1
					INNER JOIN #catalogInstanceNames srcIN ON 1=1
					INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
					INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]
				  ) AS src
			LEFT JOIN [dbo].[catalogDatabaseNames] AS dest ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name]
			WHERE dest.[instance_id] IS NULL;

	SELECT TOP 1 @instanceID = cin.[id]
	FROM  #catalogMachineNames srcMn
	INNER JOIN #catalogInstanceNames srcIN ON 1=1
	INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]

	IF @errMessage IS NOT NULL AND @errMessage<>''
		INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
				SELECT  @instanceID
					  , @projectID
					  , GETUTCDATE()
					  , @errDescriptor
					  , @errMessage

	RETURN @instanceID
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @returnValue
GO


RAISERROR('Create procedure: [dbo].[usp_refreshProjectCatalogsAndDiscovery]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_refreshProjectCatalogsAndDiscovery]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_refreshProjectCatalogsAndDiscovery]
GO

CREATE PROCEDURE [dbo].[usp_refreshProjectCatalogsAndDiscovery]
		@projectCode		[varchar](32),
		@runDiscovery		[bit]=0,	/* using sqlcmd -L*/
		@enableXPCMDSHELL	[bit]=1,
		@debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 09.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](1024)
		, @sqlServerName			[sysname]
		, @existingServerID			[int]
		, @projectID				[smallint]
		, @instanceID				[smallint]
		, @errMessage				[nvarchar](4000)
		, @errorCode				[int]
		, @optionXPValue			[int]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#xp_cmdshell') IS NOT NULL DROP TABLE #xp_cmdshell

CREATE TABLE #xp_cmdshell
(
	[output]		[nvarchar](max)		NULL,
	[instance_name]	[sysname]			NULL,
	[machine_name]	[sysname]			NULL
)


-----------------------------------------------------------------------------------------------------
SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @errMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@errMessage, 16, 1) WITH NOWAIT
	end

-----------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE lam 
FROM [dbo].[logAnalysisMessages] lam 
WHERE lam.[project_id] = @projectID

IF @runDiscovery=1
	begin		
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* check if xp_cmdshell is enabled or should be enabled																			 */
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

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* perform discovery																											 */
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		RAISERROR('Performing SQL Server instance discovery...', 10, 1) WITH NOWAIT

		SET @queryToRun='sqlcmd -L'
		INSERT	INTO #xp_cmdshell([output])
				EXEC xp_cmdshell @queryToRun

		UPDATE #xp_cmdshell SET [output]=LTRIM(RTRIM([output]))
		DELETE FROM #xp_cmdshell where [output] LIKE 'NULL%' OR [output] LIKE 'Servers:%' OR [output] IS NULL
		DELETE FROM #xp_cmdshell WHERE LEN([output])<=1

		UPDATE #xp_cmdshell 
			SET   [instance_name] = [output]
				, [machine_name] = CASE WHEN CHARINDEX('\', [output])>0 THEN SUBSTRING([output], 1, CHARINDEX('\', [output])-1) ELSE [output] END

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* disable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
														, @flgAction	 = 0			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode


		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* catalog discovered servers																									 */
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		DECLARE crsDiscoveredServer CURSOR LOCAL FAST_FORWARD FOR	SELECT xp.[instance_name], ss.[server_id]
																	FROM #xp_cmdshell xp
																	LEFT  JOIN
																		(
																			SELECT    cin.[name] AS [instance_name]
																					, cmn.[name] AS [machine_name]
																			FROM [dbo].[catalogInstanceNames]		cin 	
																			INNER JOIN [dbo].[catalogMachineNames]  cmn ON	cmn.[id] = cin.[machine_id]
																														AND cmn.[project_id] = cin.[project_id]
																			INNER JOIN [dbo].[catalogProjects]		cp	ON	cp.[id] = cin.[project_id] 
																			WHERE cp.[code] = @projectCode
																		)cat ON	cat.[instance_name] = xp.[instance_name] 
																				OR cat.[machine_name] = xp.[instance_name]
																				OR cat.[machine_name] = xp.[machine_name]
																	LEFT  JOIN sys.servers					ss	ON	ss.[name] = xp.[instance_name]
																	WHERE cat.[instance_name] IS NULL AND cat.[machine_name] IS NULL
		OPEN crsDiscoveredServer
		FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
		WHILE @@FETCH_STATUS=0
			begin
				SET @errMessage = 'New SQL Server Instance found: [' + @sqlServerName + ']'
				RAISERROR(@errMessage, 10, 1) WITH NOWAIT
		
				IF @existingServerID IS NULL
					begin
						/* create a linked server for the instance found */
						EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
					end
					
				/* catalog the instance */
				EXEC @instanceID = [dbo].[usp_refreshMachineCatalogs] 	@projectCode	= @projectCode,
																		@sqlServerName	= @sqlServerName,
																		@debugMode		= @debugMode


				INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
						SELECT  @instanceID
							  , @projectID
							  , GETUTCDATE()
							  , 'dbo.usp_refreshProjectCatalogsAndDiscovery'
							  , @errMessage
					  												
				FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
			end
		CLOSE crsDiscoveredServer
		DEALLOCATE crsDiscoveredServer
	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* check status / update catalog for previous discovered serverd																 */
/*-------------------------------------------------------------------------------------------------------------------------------*/
DECLARE crsDiscoveredServer CURSOR LOCAL FAST_FORWARD FOR	SELECT cin.[name], ss.[server_id]
															FROM [dbo].[catalogInstanceNames] cin 
															INNER JOIN [dbo].[catalogProjects]		cp	ON	cp.[id] = cin.[project_id] 
															INNER JOIN [dbo].[catalogMachineNames]  cmn ON	cmn.[id] = cin.[machine_id] 
																											AND cmn.[project_id] = cin.[project_id]
															LEFT  JOIN #xp_cmdshell					xp  ON	cin.[name] = xp.[output] 
																											OR cmn.[name] = xp.[output] 
															LEFT  JOIN sys.servers					ss	ON	ss.[name] = cin.[name]
															WHERE	cp.[code] = @projectCode
																	AND xp.[output] IS NULL
															ORDER BY cin.[name]
OPEN crsDiscoveredServer
FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
WHILE @@FETCH_STATUS=0
	begin
		IF @existingServerID IS NULL
			begin
				/* create a linked server for the instance found */
				EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
			end
					
		/* update instance information */
		EXEC [dbo].[usp_refreshMachineCatalogs] 	@projectCode	= @projectCode,
													@sqlServerName	= @sqlServerName,
													@debugMode		= @debugMode
												
		FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
	end
CLOSE crsDiscoveredServer
DEALLOCATE crsDiscoveredServer
/*-------------------------------------------------------------------------------------------------------------------------------*/
GO



/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: maintenance-plan																					   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: MAINTENANCE-PLAN', 10, 1) WITH NOWAIT

RAISERROR('Create procedure: [dbo].[usp_mpDeleteFileOnDisk]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDeleteFileOnDisk]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDeleteFileOnDisk]
GO

CREATE PROCEDURE [dbo].[usp_mpDeleteFileOnDisk]
		@sqlServerName			[sysname],
		@fileName				[nvarchar](1024),
		@executionLevel			[tinyint] = 0,
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 10.03.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE   @queryToRun				[nvarchar](1024)
		, @serverToRun				[nvarchar](512)
		, @errorCode				[int]

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @nestedExecutionLevel		[tinyint]
		, @optionXPValue			[int]

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
		SET @queryToRun = N'master.dbo.xp_fileexist ''' + @fileName + ''''
else
	IF @serverVersionNum < 11
		SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @fileName + ''''';'')x'
	ELSE
		SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''master.dbo.xp_fileexist ''''''''' + @fileName + ''''''''' '''') WITH RESULT SETS(([File Exists] [int], [File is a Directory] [int], [Parent Directory Exists] [int])) '')x'

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
INSERT	INTO #fileExists([file_exists], [file_is_directory], [parent_directory_exists])
		EXEC (@queryToRun)

IF (SELECT [file_exists] FROM #fileExists)=1
	begin
		SET @queryToRun= 'Deleting file: "' + @fileName + '"'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		SET @optionXPValue = 0

		IF @serverVersionNum>=9
			begin
				/* enable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																, @flgAction	 = 1			-- 1=enable | 0=disable
																, @optionXPValue = @optionXPValue OUTPUT
																, @debugMode	 = @debugMode

				IF @optionXPValue = 0
					begin
						RETURN 1
					end		
			end

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* deleting file     																											 */
		SET @queryToRun = N'DEL "' + @fileName + '"'
		SET @serverToRun = N'[' + @sqlServerName + '].master.dbo.xp_cmdshell'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		EXEC @serverToRun @queryToRun , NO_OUTPUT


		/*-------------------------------------------------------------------------------------------------------------------------------*/
		IF @serverVersionNum>=9
			begin
				/* disable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																, @flgAction	 = 0			-- 1=enable | 0=disable
																, @optionXPValue = @optionXPValue OUTPUT
																, @debugMode	 = @debugMode

			end

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* check if file still exists																									 */
		IF @sqlServerName=@@SERVERNAME
				SET @queryToRun = N'master.dbo.xp_fileexist ''' + @fileName + ''''
		else
				SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @fileName + ''''';'')x'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		DELETE FROM #fileExists
		INSERT	INTO #fileExists([file_exists], [file_is_directory], [parent_directory_exists])
				EXEC (@queryToRun)

		IF (SELECT [file_exists] FROM #fileExists)=1
			begin
				SET @queryToRun = N'ERROR: File could not be deleted.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				RETURN 1
			end
		ELSE
			begin
				SET @queryToRun = N'File successfully deleted.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			end
	end
RETURN 0
GO


RAISERROR('Create procedure: [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]
		  @serverToRun			[sysname]
		, @forSQLServerName		[sysname] = '%'
		, @forDatabaseName		[sysname]
		, @backupLocation		[nvarchar](512)
		, @backupType			[nvarchar](32) = 'full' /* options available: FULL, DIFF, LOG */
		, @nameConvention		[nvarchar](32) = 'dbaTDPMon' /*options available: dbaTDPMon, Ola */
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
WITH RECOMPILE
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.05.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](1024)

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)
		, @optionXPValue			[int]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

-----------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @serverToRun,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= 0,
										@debugMode				= @debugMode

/*-------------------------------------------------------------------------------------------------------------------------------*/
SET @optionXPValue = 0

IF @serverVersionNum>=9
	begin
		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @serverToRun
														, @flgAction	 = 1			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode
	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
	IF OBJECT_ID('tempdb..#backupFiles') IS NOT NULL DROP TABLE #backupFiles;
	CREATE TABLE #backupFiles
	(
		  [id]			[int] IDENTITY(1, 1)
		, [file_name]	[nvarchar](260)
		, [depth]		[int]
		, [is_file]		[int]
		, [create_time]	[varchar](20)
	)

IF RIGHT(@backupLocation, 1)<>'\' 
	SET @backupLocation = @backupLocation + '\'
IF @forSQLServerName <> '%' 
	SET @backupLocation = @backupLocation + 
							CASE	WHEN @nameConvention='dbaTDPMon' THEN @forSQLServerName
									WHEN @nameConvention='Ola' THEN REPLACE(@forSQLServerName, '\', '$')
							END + '\' + 
							@forDatabaseName + '\' + 
							CASE	WHEN @nameConvention='dbaTDPMon' THEN ''
									WHEN @nameConvention='Ola' THEN @backupType + '\'
							END
	
SET @queryToRun = N'EXEC xp_dirtree ''' + @backupLocation + ''', 8, 1';
IF @serverToRun<>@@SERVERNAME
	begin
		IF @serverVersionNum < 11
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @serverToRun + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
		ELSE
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @serverToRun + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([subdirectory] [nvarchar](260), [depth] [int], [file] [int]))'')'
	end

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #backupFiles
INSERT INTO #backupFiles([file_name], [depth], [is_file])
		EXEC (@queryToRun)


IF @nameConvention = 'dbaTDPMon'
	begin
		SET @backupType = CASE	WHEN LOWER(@backupType) = 'full' THEN '_full.BAK'
								WHEN LOWER(@backupType) = 'diff' THEN '_diff.BAK'
								WHEN LOWER(@backupType) = 'log' THEN '_log.TRN'
						  END
		UPDATE #backupFiles
			SET [create_time] = SUBSTRING([file_name], CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) + LEN(@forDatabaseName) + 2, 15)
		WHERE	CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) <> 0
				AND CHARINDEX(@backupType, [file_name], 1) <> 0
	end

IF @nameConvention = 'Ola'
	begin
		SET @backupType = CASE	WHEN LOWER(@backupType) = 'full' THEN '_full_'
								WHEN LOWER(@backupType) = 'diff' THEN '_diff_'
								WHEN LOWER(@backupType) = 'log' THEN '_log_'
						  END
		UPDATE #backupFiles
			SET [create_time] = SUBSTRING([file_name], CHARINDEX( @backupType, [file_name], 1) + LEN(@backupType) , 15)
		WHERE	CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) <> 0
				AND CHARINDEX(@backupType, [file_name], 1) <> 0

	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* get last backup type with full path */
DECLARE   @fileName		[nvarchar](512)
		, @depth		[int]
		, @id			[int]
		, @timeStamp	[varchar](20)

SELECT TOP 1 
	   @id = [id]
	 , @fileName = [file_name]
	 , @depth = [depth]
	 , @timeStamp = [create_time]
FROM #backupFiles 
WHERE [create_time] IS NOT NULL
ORDER BY [create_time] DESC

WHILE @depth>0
	begin
		SELECT TOP 1
			@fileName = [file_name] + '\' + @fileName
		FROM #backupFiles
		WHERE [id] < @id
			AND [depth] = @depth -1
		ORDER BY [id] DESC

		SET @depth = @depth -1
	end

SET @fileName = @backupLocation + @fileName
SELECT @fileName AS [file_name], @timeStamp AS [time_stamp]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @serverVersionNum>=9
	begin
		/* disable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @serverToRun
														, @flgAction	 = 0			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode
	end
GO


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

SET @queryToRun= 'Cleanup backup files for database: ' + ' [' + @dbName + ']'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

-----------------------------------------------------------------------------------------
--get destination server running version/edition
SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
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
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type], [first_lsn])
				EXEC (@queryToRun)

		--check for remote server msdb information
		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
						EXEC (@queryToRun)
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
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[backup_set_id] < ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
												ORDER BY bs.[backup_start_date] DESC'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #backupSET
				INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
						EXEC (@queryToRun)

				IF @sqlServerName<>@@SERVERNAME
					begin
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
								EXEC (@queryToRun)
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
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_set_id] DESC'

		DELETE FROM #backupSET
		INSERT	INTO #backupSET([backup_set_id])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC (@queryToRun)
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
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id], [backup_start_date])
						EXEC (@queryToRun)
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
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_set_id] DESC'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #backupSET
		INSERT	INTO #backupSET([backup_set_id])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC (@queryToRun)
			end

		SELECT TOP 1  @lastDiffRemainingBackupSetID = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC
	end

-----------------------------------------------------------------------------------------
--for +2k5 versions, will use xp_delete_file
SET @errorCode=0
IF @serverVersionNum>=9 AND @flgOptions & 256 = 256
	begin
		SET @queryToRun = N'EXEC master.dbo.xp_delete_file 0, N''' + @backupLocation + ''', N''' + @backupFileExtension + ''', N''' + CONVERT([varchar](20), @maxAllowedDate, 120) + ''', 0'
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
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
IF (@flgOptions & 256 = 0) OR (@errorCode<>0 AND @flgOptions & 256 = 256) OR (@serverVersionNum < 9) OR (@flgOptions & 128 = 128 AND @lastFullRemainingBackupSetID IS NOT NULL)
	begin
		SET @optionXPValue = 0

		IF @serverVersionNum>=9
			begin
				/* enable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																, @flgAction	 = 1			-- 1=enable | 0=disable
																, @optionXPValue = @optionXPValue OUTPUT
																, @debugMode	 = @debugMode

				IF @optionXPValue = 0
					begin
						RETURN 1
					end		
			end											
		
		/* identify backup files to be deleted, based on msdb information */
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT bs.[backup_set_id], bmf.[physical_device_name]
										FROM [msdb].[dbo].[backupset] bs
										INNER JOIN [msdb].[dbo].[backupmediafamily] bmf ON bmf.[media_set_id]=bs.[media_set_id]
										WHERE	(   (    bs.[backup_start_date] <= CONVERT([datetime], ''' + CONVERT([nvarchar](20), @maxAllowedDate, 120) + N''', 120)
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.' + @backupFileExtension + N''')
														AND (	 (' + CAST(@flgOptions AS [nvarchar]) + N' & 256 = 0) 
															OR (' + CAST(@errorCode AS [nvarchar]) + N'<>0 AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 256 = 256) 
															OR (' + CAST(@serverVersionNum AS [nvarchar]) + N'< 9)
															)
													)
													OR (
															-- when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
															bs.[backup_set_id] < ' + CAST(@lastFullRemainingBackupSetID AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + N'%'')
														AND bs.[database_backup_lsn] <> ' + CAST(ISNULL(@lastFullRemainingFirstLSN, 0) AS [nvarchar]) + N'
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
													OR (
															-- delete incremental and transaction log backups to keep the retention/restore period fixed
															' + CAST(ISNULL(@lastDiffRemainingBackupSetID, 0)  AS [nvarchar]) + N' <> 0
														AND bs.[backup_set_id] < ' + CAST(ISNULL(@lastDiffRemainingBackupSetID, 0) AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + N'%'')
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
												)														
												AND bmf.[device_type] = 2'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupDevice([backup_set_id], [physical_device_name])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupDevice([backup_set_id], [physical_device_name])
						EXEC (@queryToRun)
			end


		/* identify backup files to be deleted, based on file existence on disk */
		/* use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default) */
		IF @flgOptions & 128 = 128 AND @flgOptions & 4096 = 4096 AND @serverVersionNum>=9
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
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
						ELSE
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([subdirectory] [nvarchar](260), [depth] [int], [file] [int]))'')'
					end

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #backupFilesOnDisk
				INSERT INTO #backupFilesOnDisk([file_name], [depth], [is_file])
						EXEC (@queryToRun)

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
				SET @queryToRun = N'EXEC [' + DB_NAME() + '].[dbo].[usp_mpDeleteFileOnDisk]	@sqlServerName	= ''' + @sqlServerName + N''',
																							@fileName		= ''' + @backupFileName + N''',
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

		IF @serverVersionNum>=9
			begin
				/* disable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																, @flgAction	 = 0			-- 1=enable | 0=disable
																, @optionXPValue = @optionXPValue OUTPUT
																, @debugMode	 = @debugMode
			end
	end

RETURN @errorCode
GO


/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT

