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
	IF @serverVersionNum<11
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

		SELECT  @optionXPIsAvailable		= 0,
				@optionXPValue				= 0,
				@optionXPHasChanged			= 0,
				@optionAdvancedIsAvailable	= 0,
				@optionAdvancedValue		= 0,
				@optionAdvancedHasChanged	= 0

		IF @serverVersionNum>=9
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

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* deleting file     																											 */
		SET @queryToRun = N'DEL "' + @fileName + '"'
		SET @serverToRun = N'[' + @sqlServerName + '].master.dbo.xp_cmdshell'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		EXEC @serverToRun @queryToRun , NO_OUTPUT


		/*-------------------------------------------------------------------------------------------------------------------------------*/
		IF @serverVersionNum>=9 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
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

