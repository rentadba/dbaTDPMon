RAISERROR('Create procedure: [dbo].[usp_changeServerConfigurationOption]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_changeServerConfigurationOption]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_changeServerConfigurationOption]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_changeServerConfigurationOption]
		@sqlServerName		[sysname],
		@configOptionName	[sysname],
		@configOptionValue	[int],
		@optionIsAvailable	[bit] OUTPUT,
		@optionCurrentValue	[int] OUTPUT,
		@optionHasChanged	[bit] OUTPUT,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
WITH RECOMPILE
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.04.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](512)	-- used for dynamic statements
		, @queryParameters			[nvarchar](512)
		, @nestedExecutionLevel		[tinyint]
	
DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)


-----------------------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
		(
			[config_name]	[sysname]		NULL,
			[minimum]		[sql_variant]	NULL,
			[maximum]		[sql_variant]	NULL,
			[config_value]	[sql_variant]	NULL,
			[run_value]		[sql_variant]	NULL
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

-----------------------------------------------------------------------------------------
SET @optionCurrentValue=0
SET @optionIsAvailable=0
SET @optionHasChanged=0

SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'EXEC master.dbo.sp_configure'
	
IF @sqlServerName<>@@SERVERNAME
	begin
		IF @serverVersionNum < 11
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
		ELSE
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([name] [nvarchar](70), [minimum] [sql_variant], [maximum] [sql_variant], [config_value] [sql_variant], [run_value] [sql_variant]))'')'
	end

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

INSERT	INTO #serverPropertyConfig--([config_name], [minimum], [maximum], [config_value], [run_value])
		EXEC (@queryToRun)

SET @queryToRun = N'SELECT   @optionIsAvailable = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
						   , @optionCurrentValue = MAX(CAST(config_value AS [int]))
					FROM #serverPropertyConfig
					WHERE [config_name] = @configOptionName'
SET @queryParameters = N'@optionIsAvailable [bit] OUTPUT, @optionCurrentValue [int] OUTPUT, @configOptionName [sysname]'

EXEC sp_executesql @queryToRun, @queryParameters, @configOptionName = @configOptionName
												, @optionIsAvailable = @optionIsAvailable OUT
												, @optionCurrentValue = @optionCurrentValue OUT

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @optionIsAvailable=1 AND ISNULL(@optionCurrentValue, 0) <> @configOptionValue
	begin
		--changing option value and run reconfigure
		SET @queryToRun  = N'sp_executesql N''sp_configure ''''' + @configOptionName + N''''', ' + CAST(@configOptionValue AS [nvarchar](32)) + N'''; RECONFIGURE WITH OVERRIDE;'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
											@module			= 'dbo.usp_changeServerConfigurationOption',
											@eventName		= 'configuration option change',
											@queryToRun  	= @queryToRun,
											@flgOptions		= 0,
											@executionLevel	= @nestedExecutionLevel,
											@debugMode		= @debugMode

		--check the new value
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'EXEC master.dbo.sp_configure'

		IF @sqlServerName<>@@SERVERNAME
			begin
				IF @serverVersionNum < 11
					SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
				ELSE
					SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([name] [nvarchar](70), [minimum] [sql_variant], [maximum] [sql_variant], [config_value] [sql_variant], [run_value] [sql_variant]))'')'
			end

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig--([name], [minimum], [maximum], [config_value], [run_value])
				EXEC (@queryToRun)

		SET @queryToRun = N'SELECT @optionCurrentValue = config_value
							FROM #serverPropertyConfig
							WHERE [config_name] = @configOptionName'
		SET @queryParameters = N' @optionCurrentValue [int] OUTPUT, @configOptionName [sysname]'

		EXEC sp_executesql @queryToRun, @queryParameters, @configOptionName = @configOptionName
														, @optionCurrentValue = @optionCurrentValue OUT


		IF ISNULL(@optionCurrentValue, 0) = @configOptionValue
			SET	@optionHasChanged = 1
	end
GO
