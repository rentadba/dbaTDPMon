USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2016.06.02' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

/* common module */
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




RAISERROR('Create procedure: [dbo].[usp_sqlAgentJobEmailStatusReport]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_sqlAgentJobEmailStatusReport]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_sqlAgentJobEmailStatusReport]
GO

CREATE PROCEDURE [dbo].[usp_sqlAgentJobEmailStatusReport]
		@sqlServerName			[sysname] = @@SERVERNAME,
		@jobName				[sysname],
		@logFileLocation		[nvarchar](512),
		@module					[varchar](32),
		@sendLogAsAttachment	[bit]=1,
		@eventType				[smallint]=2,
		@currentlyRunning		[bit] = 1,
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS

SET NOCOUNT ON

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.03.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE @eventMessageData			[varchar](max),
		@jobID						[uniqueidentifier],
		@strMessage					[nvarchar](512),
		@lastCompletionInstanceID	[int],
		@queryToRun					[nvarchar](max),
		@queryParams				[nvarchar](1024)

-----------------------------------------------------------------------------------------------------
--get job id
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'SELECT [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' + @jobName + ''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT @jobID = [job_id] FROM (' + @queryToRun + N')inq'
SET @queryParams = '@jobID [uniqueidentifier] OUTPUT'

IF @debugMode=1	PRINT @queryToRun
EXEC sp_executesql @queryToRun, @queryParams, @jobID = @jobID OUTPUT

-----------------------------------------------------------------------------------------------------
--get last instance_id when job completed
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'SELECT MAX(h.[instance_id]) AS [instance_id]
					FROM [msdb].[dbo].[sysjobs] j 
					RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
					WHERE	j.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
							AND h.[step_name] =''(Job outcome)'''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT @lastCompletionInstanceID = [instance_id] FROM (' + @queryToRun + N')inq'
SET @queryParams = '@lastCompletionInstanceID [int] OUTPUT'

IF @debugMode=1	PRINT @queryToRun
EXEC sp_executesql @queryToRun, @queryParams, @lastCompletionInstanceID = @lastCompletionInstanceID OUTPUT

SET @lastCompletionInstanceID = ISNULL(@lastCompletionInstanceID, 0)
-----------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#jobHistory') IS NOT NULL
	DROP TABLE #jobHistory

CREATE TABLE #jobHistory
	(
		  [step_id]			[int]
		, [step_name]		[sysname]
		, [run_status]		[nvarchar](32)
		, [run_date]		[nvarchar](32)
		, [run_time]		[nvarchar](32)
		, [duration]		[nvarchar](32)
		, [message]			[nvarchar](max)
		, [log_filename]	[nvarchar](512)
	)

SET @queryToRun = N''
IF @currentlyRunning = 0
	SET @queryToRun = @queryToRun + N'
			SELECT	[step_id]
					, [step_name]
					, [run_status]
					, SUBSTRING([run_date], 1, 4) + ''-'' + SUBSTRING([run_date], 5 ,2) + ''-'' + SUBSTRING([run_date], 7 ,2) AS [run_date]
					, SUBSTRING([run_time], 1, 2) + '':'' + SUBSTRING([run_time], 3, 2) + '':'' + SUBSTRING([run_time], 5, 2) AS [run_time]
					, SUBSTRING([run_duration], 1, 2) + ''h '' + SUBSTRING([run_duration], 3, 2) + ''m '' + SUBSTRING([run_duration], 5, 2) + ''s'' AS [duration]
					, [message]
					, [output_file_name]
			FROM (		
					SELECT    h.[step_id]
							, h.[step_name]
							, CASE h.[run_status]	WHEN ''0'' THEN ''Failed''
													WHEN ''1'' THEN ''Succeded''	
													WHEN ''2'' THEN ''Retry''
													WHEN ''3'' THEN ''Canceled''
													WHEN ''4'' THEN ''In progress''
													ELSE ''Unknown''
								END [run_status]
							, CAST(h.[run_date] AS varchar) AS [run_date]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_time] AS varchar))) + CAST(h.[run_time] AS varchar) AS [run_time]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_duration] AS varchar))) + CAST(h.[run_duration] AS varchar) AS [run_duration]
							, CASE WHEN [run_status] IN (0, 2) THEN LEFT(h.[message], 256) ELSE '''' END AS [message]
							, sjs.[output_file_name]
					FROM [msdb].[dbo].[sysjobhistory] h
					LEFT JOIN [msdb].[dbo].[sysjobsteps] sjs ON sjs.[job_id] = h.[job_id] AND sjs.[step_id] = h.[step_id]
					WHERE	 h.[instance_id] < (
												SELECT TOP 1 [instance_id] 
												FROM (	
														SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
														FROM [msdb].[dbo].[sysjobhistory] h
														WHERE	h.[job_id]= ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																AND h.[step_name] =''(Job outcome)''
														ORDER BY h.[instance_id] DESC
													)A
												) 
							AND	h.[instance_id] > ISNULL(
												( SELECT [instance_id] 
												FROM (	
														SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
														FROM [msdb].[dbo].[sysjobhistory] h
														WHERE	h.[job_id]= ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																AND h.[step_name] =''(Job outcome)''
														ORDER BY h.[instance_id] DESC
													)A
												WHERE [instance_id] NOT IN 
													(
													SELECT TOP 1 [instance_id] 
													FROM (	SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
															FROM [msdb].[dbo].[sysjobhistory] h
															WHERE	h.[job_id]= ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																	AND h.[step_name] =''(Job outcome)''
															ORDER BY h.[instance_id] DESC
														)A
													)),0)
							AND h.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
				)A'
ELSE
	SET @queryToRun = @queryToRun + N'
			SELECT	[step_id]
					, [step_name]
					, [run_status]
					, SUBSTRING([run_date], 1, 4) + ''-'' + SUBSTRING([run_date], 5 ,2) + ''-'' + SUBSTRING([run_date], 7 ,2) AS [run_date]
					, SUBSTRING([run_time], 1, 2) + '':'' + SUBSTRING([run_time], 3, 2) + '':'' + SUBSTRING([run_time], 5, 2) AS [run_time]
					, SUBSTRING([run_duration], 1, 2) + ''h '' + SUBSTRING([run_duration], 3, 2) + ''m '' + SUBSTRING([run_duration], 5, 2) + ''s'' AS [duration]
					, [message]
					, [output_file_name]
			FROM (		
					SELECT    h.[step_id]
							, h.[step_name]
							, CASE h.[run_status]	WHEN ''0'' THEN ''Failed''
													WHEN ''1'' THEN ''Succeded''	
													WHEN ''2'' THEN ''Retry''
													WHEN ''3'' THEN ''Canceled''
													WHEN ''4'' THEN ''In progress''
													ELSE ''Unknown''
								END [run_status]
							, CAST(h.[run_date] AS varchar) AS [run_date]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_time] AS varchar))) + CAST(h.[run_time] AS varchar) AS [run_time]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_duration] AS varchar))) + CAST(h.[run_duration] AS varchar) AS [run_duration]
							, CASE WHEN [run_status] IN (0, 2) THEN LEFT(h.[message], 256) ELSE '''' END AS [message]
							, sjs.[output_file_name]
					FROM [msdb].[dbo].[sysjobhistory] h
					LEFT JOIN [msdb].[dbo].[sysjobsteps] sjs ON sjs.[job_id] = h.[job_id] AND sjs.[step_id] = h.[step_id]
					WHERE	 h.[instance_id] > ISNULL((
														SELECT TOP 1 [instance_id] 
														FROM (	
																SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																FROM [msdb].[dbo].[sysjobhistory] h
																WHERE	h.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																		AND h.[step_name] =''(Job outcome)''
																ORDER BY h.[instance_id] DESC
															)A
														), 0)
							AND h.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
				)A'

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

IF @debugMode=1	PRINT @queryToRun
INSERT	INTO #jobHistory([step_id], [step_name], [run_status], [run_date], [run_time], [duration], [message], [log_filename])
		EXEC (@queryToRun)

-----------------------------------------------------------------------------------------------------
SET @eventMessageData = ''
SELECT @eventMessageData = @eventMessageData + 
							'<job-step>' + 
							'<step_id>' + CAST(ISNULL([step_id], 0) AS [varchar](32)) + '</step_id>' + 
							'<step_name>' + REPLACE(ISNULL([step_name], ''), '&', '&amp;') + '</step_name>' + 
							'<run_status>' + ISNULL([run_status], '') + '</run_status>' + 
							'<run_date>' + ISNULL([run_date], '') + '</run_date>' + 
							'<run_time>' + ISNULL([run_time], '') + '</run_time>' + 
							'<duration>' + ISNULL([duration], '') + '</duration>' +
							'<message>' + REPLACE(ISNULL([message], ''), '&', '&amp;') + '</message>' +
							'</job-step>'
FROM #jobHistory

SET @eventMessageData = '<job-history>' + @eventMessageData + '</job-history>'


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @sendLogAsAttachment=0
	SET @logFileLocation = NULL
ELSE
	IF @logFileLocation IS NULL
		SELECT TOP 1 @logFileLocation = [log_filename]
		FROM #jobHistory
		WHERE [run_status] = 'Failed'
		ORDER BY [step_id]

/* check if @logFileLocation exists	*/
IF @logFileLocation IS NOT NULL
	begin
		IF object_id('#fileExists') IS NOT NULL DROP TABLE #fileExists
		CREATE TABLE #fileExists
					(
						[file_exists]				[bit]	NULL,
						[file_is_directory]			[bit]	NULL,
						[parent_directory_exists]	[bit]	NULL
					)

		DECLARE	  @serverEdition			[sysname]
				, @serverVersionStr			[sysname]
				, @serverVersionNum			[numeric](9,6)
				, @nestedExecutionLevel		[tinyint]

		EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
												@serverEdition			= @serverEdition OUT,
												@serverVersionStr		= @serverVersionStr OUT,
												@serverVersionNum		= @serverVersionNum OUT,
												@executionLevel			= @nestedExecutionLevel,
												@debugMode				= @debugMode

		IF @sqlServerName=@@SERVERNAME
				SET @queryToRun = N'master.dbo.xp_fileexist ''' + @logFileLocation + ''''
		else
			IF @serverVersionNum<11
				SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @logFileLocation + ''''';'')x'
			ELSE
				SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''master.dbo.xp_fileexist ''''''''' + @logFileLocation + ''''''''' '''') WITH RESULT SETS(([File Exists] [int], [File is a Directory] [int], [Parent Directory Exists] [int])) '')x'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		INSERT	INTO #fileExists([file_exists], [file_is_directory], [parent_directory_exists])
				EXEC (@queryToRun)

		IF (SELECT [file_exists] FROM #fileExists)=0
			SET @logFileLocation = NULL
	end
/*-------------------------------------------------------------------------------------------------------------------------------*/

--if one of the job steps failed, will fail the job
DECLARE @failedSteps [int]

SELECT @failedSteps = COUNT(*)
FROM #jobHistory
WHERE [run_status] = 'Failed'

EXEC [dbo].[usp_logEventMessageAndSendEmail] @projectCode		= NULL,
											 @sqlServerName		= @sqlServerName,
											 @objectName		= @jobName,
											 @module			= @module,
											 @eventName			= 'sql agent job status',
											 @parameters		= @logFileLocation,
											 @eventMessage		= @eventMessageData,
											 @recipientsList	= NULL,
											 @eventType			= @eventType,
											 @additionalOption	= @failedSteps

IF @failedSteps <> 0
	begin
		SET @strMessage = 'Job execution failed. See individual steps status.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=1
	end
GO




/* maintenance-plan module */
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
		@sqlServerName		[sysname] = @@SERVERNAME,
		@dbName				[sysname],
		@backupLocation		[nvarchar](1024)=NULL,	/*  disk only: local or UNC */
		@flgActions			[smallint] = 1,			/*  1 - perform full database backup
														2 - perform differential database backup
														4 - perform transaction log backup
													*/
		@flgOptions			[int] = 1883,		/*  1 - use CHECKSUM (default)
													2 - use COMPRESSION, if available (default)
													4 - use COPY_ONLY
													8 - force change backup type (default): if log is set, and no database backup is found, a database backup will be first triggered
												  										    if diff is set, and no full database backup is found, a full database backup will be first triggered
												   16 - verify backup file (default)
											       32 - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
												   64 - create folders for each database (default)
												  128 - when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
												  256 - for +2k5 versions, use xp_delete_file option
												  512 - skip databases involved in log shipping (primary or secondary or logs, secondary for full/diff backups) (default)
												 1024 - on alwayson availability groups, for secondary replicas, force copy-only backups (default)
												 2048 - change retention policy from RetentionDays to RetentionBackupsCount (number of full database backups to be kept)
													  - this may be forced by setting to true property 'Change retention policy from RetentionDays to RetentionBackupsCount'
												*/
		@retentionDays		[smallint]	= NULL,
		@executionLevel		[tinyint]	=  0,
		@debugMode			[bit]		=  0
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
			@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@errorCode						[int],
			@currentDate					[datetime],
			@databaseStatus					[int],
			@databaseStateDesc				[sysname]

DECLARE		@backupStartDate				[datetime],
			@backupDurationSec				[int],
			@backupSizeBytes				[bigint],
			@eventData						[varchar](8000)

-----------------------------------------------------------------------------------------
SET NOCOUNT ON

-----------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

-----------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
SET @queryToRun= 'Backup database: ' + ' [' + @dbName + ']'
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
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
										@executionLevel		= @executionLevel,
										@debugMode			= @debugMode

SET @nestedExecutionLevel = @executionLevel + 1

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
IF @serverVersionNum >= 9
	begin
		SET @queryToRun = N'SELECT CONVERT([sysname], DATABASEPROPERTYEX(''' + @dbName + N''', ''Status'')) AS [state]' 
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		SELECT @databaseStateDesc = [value]
		FROM #serverPropertyConfig

		SET @databaseStateDesc = ISNULL(@databaseStateDesc, 'NULL')
	end
ELSE
	begin
		SET @queryToRun = N'SELECT [status] FROM master.dbo.sysdatabases WHERE [name]=''' + @dbName + N'''' 
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		SELECT @databaseStatus = [value]
		FROM #serverPropertyConfig

		SET @databaseStateDesc =   CASE	WHEN @databaseStatus & 32 = 32			 THEN 'LOADING'
										WHEN @databaseStatus & 64 = 64			 THEN 'PRE RECOVERY'
										WHEN @databaseStatus & 128 = 128		 THEN 'RECOVERING'
										WHEN @databaseStatus & 256 = 256		 THEN 'NOT RECOVERED'
										WHEN @databaseStatus & 512 = 512		 THEN 'OFFLINE'
										WHEN @databaseStatus & 2048 = 2048		 THEN 'DBO USE ONLY'
										WHEN @databaseStatus & 4096 = 4096		 THEN 'SINGLE USER'
										WHEN @databaseStatus & 32768 = 32768	 THEN 'EMERGENCY MODE'
										WHEN @databaseStatus & 2097152 = 2097152 THEN 'STANDBY'
										WHEN @databaseStatus & 4194584 = 4194584 THEN 'SUSPECT'
										WHEN @databaseStatus = 0				 THEN 'UNKNOWN'
										ELSE 'ONLINE'
									END
	end

IF  @databaseStateDesc NOT IN ('ONLINE')
begin
	SET @queryToRun='Current database state (' + @databaseStateDesc + ') does not allow backup.'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

	SET @eventData='<skipaction><detail>' + 
						'<name>database backup</name>' + 
						'<type>' + @backupType + '</type>' + 
						'<affected_object>' + @dbName + '</affected_object>' + 
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
			begin
				IF @serverVersionNum >= 9			
					SET @queryToRun = N'SELECT	[secondary_database]
										FROM	msdb.dbo.log_shipping_monitor_secondary
										WHERE	[secondary_server]=@@SERVERNAME
												AND [secondary_database] = ''' + @dbName + N''''
				ELSE 
					SET @queryToRun = N'SELECT	[secondary_database_name]
										FROM	msdb.dbo.log_shipping_secondaries
										WHERE	[secondary_server_name]=@@SERVERNAME
												AND [secondary_database_name] = ''' + @dbName + N''''
			end

		--for log backups
		IF @flgActions=4
			begin
				IF @serverVersionNum >= 9			
					SET @queryToRun = N'SELECT	[primary_database]
										FROM	msdb.dbo.log_shipping_monitor_primary
										WHERE	[primary_server]=@@SERVERNAME
												AND [primary_database] = ''' + @dbName + N'''
										UNION ALL
										SELECT	[secondary_database]
										FROM	msdb.dbo.log_shipping_monitor_secondary
										WHERE	[secondary_server]=@@SERVERNAME
												AND [secondary_database] = ''' + @dbName + N''''
				ELSE 
					SET @queryToRun = N'SELECT	[primary_database_name]
										FROM	msdb.dbo.log_shipping_primaries
										WHERE	[primary_server_name]=@@SERVERNAME
												AND [primary_database_name] = ''' + @dbName + N'''
										UNION ALL
										SELECT	[secondary_database_name]
										FROM	msdb.dbo.log_shipping_secondaries
										WHERE	[secondary_server_name]=@@SERVERNAME
												AND [secondary_database_name] = ''' + @dbName + N''''
			end

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		IF (SELECT COUNT(*) FROM #serverPropertyConfig)>0
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
									'<affected_object>' + @dbName + '</affected_object>' + 
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
DECLARE @agName			[sysname],
		@agStopLimit	[int]

SET @agStopLimit = 0
IF @serverVersionNum >= 11
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database backup',
																		@actionType			= @backupType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0
																				
--------------------------------------------------------------------------------------------------
--check recovery model for database. transaction log backup is allowed only for FULL
--if force option is selected, for SIMPLE recovery model, backup type will be changed to diff
--------------------------------------------------------------------------------------------------
IF @flgActions & 4 = 4
	begin
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + 'SELECT CAST(DATABASEPROPERTYEX(''' + @dbName + N''', ''Recovery'') AS [sysname])'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		IF (SELECT UPPER([value]) FROM #serverPropertyConfig) = 'SIMPLE'
			begin
				SET @queryToRun = 'Database recovery model is SIMPLE. Transaction log backup cannot be performed.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + @dbName + '</affected_object>' + 
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
--create destination path: <@backupLocation>\@sqlServerName\@dbName
IF RIGHT(@backupLocation, 1)<>'\' SET @backupLocation = @backupLocation + N'\'
SET @backupLocation = @backupLocation + @sqlServerName + '\' + CASE WHEN @flgOptions & 64 = 64 THEN @dbName + '\' ELSE '' END

SET @queryToRun = N'EXEC [' + DB_NAME() + '].[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @sqlServerName + N''',
																			@folderName		= ''' + @backupLocation + N''',
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

IF @errorCode<>0 
	begin
		RETURN @errorCode
	end

--------------------------------------------------------------------------------------------------
--check if CHECKSUM backup option may apply
SET @optionBackupWithChecksum=0
IF @flgOptions & 1 = 1 AND @serverVersionNum >= 9
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
IF @flgOptions & 4 = 4 AND @serverVersionNum >= 9
	SET @optionBackupWithCopyOnly=1

--check if another backup is needed (full)
SET @optionForceChangeBackupType=0
IF @flgOptions & 8 = 8
	begin
		--check for any full database backup (when differential should be made) or any full/incremental database backup (when transaction log should be made)
		IF @flgActions & 2 = 2 OR @flgActions & 4 = 4
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + 'SELECT COUNT(*) FROM msdb.dbo.backupset WHERE [database_name]=''' + @dbName + N''' AND [type] IN (''D''' + CASE WHEN @flgActions & 4 = 4 THEN N', ''I''' ELSE N'' END + N')'
				IF @serverVersionNum >= 9
					SET @queryToRun = @queryToRun + N' AND [is_copy_only]=0'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #serverPropertyConfig
				INSERT	INTO #serverPropertyConfig([value])
						EXEC (@queryToRun)

				IF (SELECT [value] FROM #serverPropertyConfig) = 0
					begin
						SET @queryToRun = 'WARNING: Specified backup type cannot be performed since no full database backup exists. A full database backup will be taken before the requested backup type.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @optionForceChangeBackupType=1 
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
IF ISNULL(@retentionDays, 0) <> 0
	SET @backupOptions = @backupOptions + N', RETAINDAYS=' + CAST(@retentionDays AS [nvarchar](32))

--------------------------------------------------------------------------------------------------
--run a full database backup, in order to perform an additional diff or log backup
IF @optionForceChangeBackupType=1
	begin
		SET @currentDate = GETDATE()
		
		IF @agName IS NULL
			SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@sqlServerName, @dbName, 'full', @currentDate)
		ELSE
			SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@agName, @dbName, 'full', @currentDate)

		SET @queryToRun	= N'BACKUP DATABASE ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH STATS = 10, NAME = ''' + @backupFileName + N'''' + @backupOptions
		--IF @debugMode=1	
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseBackup',
														@eventName		= 'database backup',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

--------------------------------------------------------------------------------------------------
SET @currentDate = GETDATE()
IF @agName IS NULL
	SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@sqlServerName, @dbName, @backupType, @currentDate)
ELSE
	SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@agName, @dbName, @backupType, @currentDate)

IF @flgActions & 1 = 1 
	begin
		SET @queryToRun	= N'BACKUP DATABASE ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH STATS = 10, NAME = ''' + @backupFileName + N'''' + @backupOptions
	end

IF @flgActions & 2 = 2
	begin
		SET @queryToRun	= N'BACKUP DATABASE ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH DIFFERENTIAL, STATS = 10, NAME=''' + @backupFileName + N'''' + @backupOptions
	end

IF @flgActions & 4 = 4
	begin
		SET @queryToRun	= N'BACKUP LOG ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH STATS = 10, NAME=''' + @backupFileName + N'''' + @backupOptions
	end

--IF @debugMode=1	
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0	
EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
												@dbName			= @dbName,
												@module			= 'dbo.usp_mpDatabaseBackup',
												@eventName		= 'database backup',
												@queryToRun  	= @queryToRun,
												@flgOptions		= @flgOptions,
												@executionLevel	= @nestedExecutionLevel,
												@debugMode		= @debugMode

IF @errorCode=0
	begin
		SET @queryToRun = '	SELECT TOP 1  bs.[backup_start_date]
										, DATEDIFF(ss, bs.[backup_start_date], bs.[backup_finish_date]) AS [backup_duration_sec]
										, ' + CASE WHEN @optionBackupWithCompression=1 THEN 'bs.[compressed_backup_size]' ELSE 'bs.[backup_size]' END + ' AS [backup_size]
							FROM msdb.dbo.backupset bs
							INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.[media_set_id] = bs.[media_set_id]
							WHERE bmf.[physical_device_name] = (''' + @backupLocation + @backupFileName + N''')
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
		SET @queryToRun	= N'RESTORE VERIFYONLY FROM DISK=''' + @backupLocation + @backupFileName + N''''
		IF @optionBackupWithChecksum=1
			SET @queryToRun = @queryToRun + N' WITH CHECKSUM'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseBackup',
														@eventName		= 'database backup verify',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

--------------------------------------------------------------------------------------------------
--log backup database information
SET @eventData='<backupset><detail>' + 
					'<database_name>' + @dbName + '</database_name>' + 
					'<type>' + @backupType + '</type>' + 
					'<start_date>' + CONVERT([varchar](24), ISNULL(@backupStartDate, GETDATE()), 121) + '</start_date>' + 
					'<duration>' + REPLICATE('0', 2-LEN(CAST(@backupDurationSec / 3600 AS [varchar]))) + CAST(@backupDurationSec / 3600 AS [varchar]) + 'h'
										+ ' ' + REPLICATE('0', 2-LEN(CAST((@backupDurationSec / 60) % 60 AS [varchar]))) + CAST((@backupDurationSec / 60) % 60 AS [varchar]) + 'm'
										+ ' ' + REPLICATE('0', 2-LEN(CAST(@backupDurationSec % 60 AS [varchar]))) + CAST(@backupDurationSec % 60 AS [varchar]) + 's' + '</duration>' + 
					'<size>' + CONVERT([varchar](32), CAST(@backupSizeBytes/(1024*1024*1.0) AS [money]), 1) + ' mb</size>' + 
					'<size_bytes>' + CAST(@backupSizeBytes AS [varchar](32)) + '</size_bytes>' + 
					'<verified>' + CASE WHEN @flgOptions & 16 = 16 AND @errorCode = 0  THEN 'Yes' ELSE 'No' END + '</verified>' + 
					'<file_name>' + @backupFileName + '</file_name>' + 
					'<error_code>' + CAST(@errorCode AS [varchar](32)) + '</error_code>' + 
				'</detail></backupset>'

EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
									@dbName			= @dbName,
									@module			= 'dbo.usp_mpDatabaseBackup',
									@eventName		= 'database backup',
									@eventMessage	= @eventData,
									@eventType		= 0 /* info */

--------------------------------------------------------------------------------------------------
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

RETURN @errorCode
GO


SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
