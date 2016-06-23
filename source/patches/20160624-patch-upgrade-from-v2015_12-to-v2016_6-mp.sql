USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2016.06.24' WHERE [module] = 'common' AND [name] = 'Application Version'
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



RAISERROR('Create procedure: [dbo].[usp_logEventMessageAndSendEmail]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_logEventMessageAndSendEmail]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_logEventMessageAndSendEmail]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_logEventMessageAndSendEmail]
		@projectCode			[sysname]=NULL,
		@sqlServerName			[sysname]=NULL,
		@dbName					[sysname] = NULL,
		@objectName				[nvarchar](512) = NULL,
		@childObjectName		[sysname] = NULL,
		@module					[sysname],
		@eventName				[nvarchar](256) = NULL,
		@parameters				[nvarchar](512) = NULL,			/* may contain the attach file name */
		@eventMessage			[varchar](8000) = NULL,
		@dbMailProfileName		[sysname] = NULL,
		@recipientsList			[nvarchar](1024) = NULL,
		@eventType				[smallint]=1,	/*	0 - info
													1 - alert 
													2 - job-history
													3 - report-html
													4 - action
													5 - backup-job-history
													6 - alert-custom
												*/
		@additionalOption		[smallint]=0
/* WITH ENCRYPTION */
WITH RECOMPILE
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.11.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE @projectID					[smallint],
		@instanceID					[smallint],		
		@alertFrequency				[int],
		@alertSent					[int],
		@isEmailSent				[bit],
		@isFloodControl				[bit],
		@HTMLBody					[nvarchar](max),
		@emailSubject				[nvarchar](256),
		@queryToRun					[nvarchar](max),
		@ReturnValue				[int],
		@ErrMessage					[nvarchar](1024),
		@clientName					[nvarchar](260),
		@eventData					[varchar](8000),
		@ignoreAlertsForError1222	[bit],
		@errorCode					[int],
		@eventMessageXML			[xml]
		

DECLARE   @handle				[int]
		, @PrepareXmlStatus		[int]

SET @ReturnValue=1

-----------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

-----------------------------------------------------------------------------------------------------
SELECT  @instanceID = [id] 
FROM	[dbo].[catalogInstanceNames]  
WHERE	[name] = @sqlServerName
		AND [project_id] = @projectID

		
-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
--get default database mail profile name from configuration table
IF UPPER(@dbMailProfileName)='NULL'
	SET @dbMailProfileName = NULL
		
IF @dbMailProfileName IS NULL
	SELECT	@dbMailProfileName=[value] 
	FROM	[dbo].[appConfigurations] 
	WHERE	[name]='Database Mail profile name to use for sending emails'
			AND [module] = 'common'

IF @recipientsList = ''		SET @recipientsList = NULL
IF @dbMailProfileName = ''	SET @dbMailProfileName = NULL


IF @recipientsList IS NULL
	SELECT	@recipientsList=[value] 
	FROM	[dbo].[appConfigurations] 
	WHERE  (@eventType IN (1, 6) AND [name]='Default recipients list - Alerts (semicolon separated)' AND [module] = 'common')
		OR (@eventType IN (2, 5) AND [name]='Default recipients list - Job Status (semicolon separated)' AND [module] = 'common')
		OR (@eventType=3 AND [name]='Default recipients list - Reports (semicolon separated)' AND [module] = 'common')

-----------------------------------------------------------------------------------------------------
--get alert repeat frequency, default every 60 minutes
-----------------------------------------------------------------------------------------------------
SELECT	@alertFrequency = [value]
FROM	[dbo].[appConfigurations]
WHERE	[name]='Alert repeat interval (minutes)'
		AND [module] = 'common'

SELECT @alertFrequency = ISNULL(@alertFrequency, 60)


-----------------------------------------------------------------------------------------------------
--check what alerts can be ignored
-----------------------------------------------------------------------------------------------------
SELECT	@ignoreAlertsForError1222 = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Ignore alerts for: Error 1222 - Lock request time out period exceeded'
		AND [module] = 'common'

SET @ignoreAlertsForError1222 = ISNULL(@ignoreAlertsForError1222, 0)


-----------------------------------------------------------------------------------------------------
--check if alert should be sent
-----------------------------------------------------------------------------------------------------
SET @alertSent=0
IF @projectID IS NOT NULL AND @instanceID IS NOT NULL
	SELECT @alertSent=COUNT(*)
	FROM [dbo].[logEventMessages]
	WHERE	[instance_id] = @instanceID
			AND [project_id] = @projectID
			AND [module] = @module
			AND [event_name] = @eventName
			AND [event_type] = @eventType
			AND ISNULL([database_name], '') = ISNULL(@dbName, '')
			AND ISNULL([object_name], '') = ISNULL(@objectName, '')
			AND ISNULL([child_object_name], '') = ISNULL(@childObjectName, '')
			AND ISNULL([parameters], '') = ISNULL(@parameters, '')
			AND DATEDIFF(mi, [event_date_utc], GETUTCDATE()) BETWEEN 0 AND @alertFrequency
			AND [is_email_sent] = 1
			AND @eventType IN (1, 6)


-----------------------------------------------------------------------------------------------------
--processing the xml message
-----------------------------------------------------------------------------------------------------
SET @eventMessage = REPLACE(@eventMessage, '&', '&amp;')

SET @eventMessageXML = CAST(@eventMessage AS [xml])
SET @HTMLBody = N''

-----------------------------------------------------------------------------------------------------
--alert details
IF @eventType=1	AND @eventMessageXML IS NOT NULL
	begin
		EXEC @PrepareXmlStatus= sp_xml_preparedocument @handle OUTPUT, @eventMessageXML  

		SET @HTMLBody =@HTMLBody + COALESCE(
								CAST ( ( 
										SELECT	li = 'error number: ' + CAST([error_code] AS [nvarchar](32)), '',
												li = [error_string], '',
												li = [query_executed], '',
												li = 'duration: ' + CAST([duration_seconds] AS [nvarchar](32)) + ' seconds', '',
												li = 'event-date (utc): ' + CAST([event_date_utc] AS [varchar](32)), ''
										FROM (
												SELECT  *
												FROM    OPENXML(@handle, '/alert/detail', 2)  
														WITH (
																[error_code]		[int],
																[error_string]		[nvarchar](max),
																[query_executed]	[nvarchar](max),
																[duration_seconds]	[bigint],
																[event_date_utc]	[sysname]
															)  
											)x
										FOR XML PATH('ul'), TYPE 
							) AS NVARCHAR(MAX) )
							, '') ;
			
		SELECT	@errorCode = [error_code]
		FROM (
				SELECT  *
				FROM    OPENXML(@handle, '/alert/detail', 2)  
						WITH (
								[error_code]		[int],
								[error_string]		[nvarchar](max),
								[query_executed]	[nvarchar](max),
								[duration_seconds]	[bigint]
							)  
			)x
		EXEC sp_xml_removedocument @handle 
	end

-----------------------------------------------------------------------------------------------------
--alert details
IF @eventType=6	AND @eventMessageXML IS NOT NULL
	begin
		SET @HTMLBody = @HTMLBody + REPLACE(REPLACE(REPLACE(REPLACE(CAST(@eventMessageXML AS [nvarchar](max)), '<alert><detail>', '<ul>'), '</detail></alert>', '</ul>'), '><', '><li><'), '<li></ul>','</ul>')

		DECLARE @strPos		[int],
				@strPos2	[int]

		SET @strPos = CHARINDEX('</', @HTMLBody)
		WHILE @strPos <> 0
			BEGIN
				SET @strPos2 = CHARINDEX('><', @HTMLBody, @strPos)
				SET @HTMLBody = SUBSTRING(@HTMLBody, 1, @strPos+1) + SUBSTRING(@HTMLBody, @strPos2, LEN(@HTMLBody))
	
				SET @strPos = CHARINDEX('</', @HTMLBody, @strPos2)
			END
		SET @HTMLBody = REPLACE(@HTMLBody, '</>', '')
		SET @HTMLBody = REPLACE(@HTMLBody, '><', '>')
		SET @HTMLBody = REPLACE(@HTMLBody, '>', ': ')
		SET @HTMLBody = REPLACE(@HTMLBody, '<li: ', '<li>')
		SET @HTMLBody = REPLACE(@HTMLBody, '<ul: li: ', '<ul><li>')
		SET @HTMLBody = REPLACE(@HTMLBody, '</ul: ', '</ul>')
	end

-----------------------------------------------------------------------------------------------------
--job-status details
IF @eventType IN (2, 5)	AND @eventMessageXML IS NOT NULL
	begin
		EXEC @PrepareXmlStatus= sp_xml_preparedocument @handle OUTPUT, @eventMessageXML  

		SET @HTMLBody =@HTMLBody + COALESCE(
							N'<TABLE BORDER="1">' +
							N'<TR>' +
								N'<TH>Step ID</TH>
									<TH>Step Name</TH>
									<TH>Run Status</TH>
									<TH>Run Date</TH>
									<TH>Run Time</TH>
									<TH>Run Duration</TH>
									<TH >Message</TH>' +
								CAST ( ( 
										SELECT	TD = [step_id], '',
												TD = [step_name], '',
												TD = [run_status], '',
												TD = [run_date], '',
												TD = [run_time], '',
												TD = [duration], '',
												TD = [message], ''
										FROM (
												SELECT  *
												FROM    OPENXML(@handle, '/job-history/job-step', 2)  
														WITH (
																[step_id]		[int],
																[step_name]		[sysname],
																[run_status]	[nvarchar](32),
																[run_date]		[nvarchar](32),
																[run_time]		[nvarchar](32),
																[duration]		[nvarchar](32),
																[message]		[nvarchar](max)
															)  
											)x
										FOR XML PATH('TR'), TYPE 
							) AS NVARCHAR(MAX) ) +
							N'</TABLE>', '') ;

		EXEC sp_xml_removedocument @handle 

		-- go out in style
		SET @HTMLBody = N'
						<style>
							body {
								/*background-color: #F0F8FF;*/
								font-family: Arial, Tahoma;
							}
							h1 {
								font-size: 20px;
								font-weight: bold;
							}
							table {
								border-color: #ccc;
								border-collapse: collapse;
							}
							th {
								font-size: 12px;
								font-weight: bold;
								font-color: #000000;
								border-spacing: 2px;
								border-style: solid;
								border-width: 1px;
								border-color: #ccc;
								background-color: #00AEEF;
								padding: 4px;
							}
							td {
								font-size: 12px;
								border-spacing: 2px;
								border-style: solid;
								border-width: 1px;
								border-color: #ccc;
								background-color: #EDF8FE;
								padding: 4px;
								white-space: nowrap;
							}
						</style>' + @HTMLBody
	end

-----------------------------------------------------------------------------------------------------
--report details
IF @eventType=3	AND @eventMessageXML IS NOT NULL
	begin
		EXEC @PrepareXmlStatus= sp_xml_preparedocument @handle OUTPUT, @eventMessageXML  

		DECLARE @xmlMessage			[nvarchar](max),
				@xmlFileName		[nvarchar](max),
				@xmlHTTPAddress		[nvarchar](max),
				@xmlRelativePath	[nvarchar](max)

		SELECT TOP 1 @xmlMessage = [message],
						@xmlFileName = [file_name],
						@xmlHTTPAddress = [http_address],
						@xmlRelativePath = [relative_path]
		FROM    OPENXML(@handle, '/report-html/detail', 2)  
				WITH (
						[message]		[nvarchar](max),
						[file_name]		[nvarchar](max),
						[http_address]	[nvarchar](max),
						[relative_path]	[nvarchar](max)
					)  

		EXEC sp_xml_removedocument @handle 

		SET @HTMLBody =@HTMLBody + @xmlMessage + N'<br>File name: <b>' + @xmlFileName + N'</b><br>'
	
		IF @xmlHTTPAddress IS NOT NULL				
			begin
				SET @HTMLBody = @HTMLBody + N'Full report file is available for download <A HREF="' + @xmlHTTPAddress + @xmlRelativePath + @xmlFileName + '">here</A><br>'
				SET @HTMLBody = @HTMLBody + N'Browser support: IE 8, Firefox 3.5 and Google Chrome 7 (on lower versions, some features may be missing).<br>'
			end
	end


-----------------------------------------------------------------------------------------------------
--backup-job-status details
IF @eventType IN (5) AND @eventMessageXML IS NOT NULL
	begin
		DECLARE   @jobStartTime [datetime]

		EXEC @PrepareXmlStatus= sp_xml_preparedocument @handle OUTPUT, @eventMessageXML  
			
		SELECT    @jobStartTime = MIN([job_step_start_time])
		FROM	(
					SELECT CASE WHEN [run_date] IS NOT NULL AND [run_time] IS NOT NULL
								THEN CONVERT([datetime], ([run_date] + ' ' + [run_time]), 120) 
								ELSE GETDATE()
							END AS [job_step_start_time]
					FROM (
							SELECT  *
							FROM    OPENXML(@handle, '/job-history/job-step', 2)  
									WITH (
											[step_id]		[int],
											[step_name]		[sysname],
											[run_status]	[nvarchar](32),
											[run_date]		[nvarchar](32),
											[run_time]		[nvarchar](32),
											[duration]		[nvarchar](32)
										)  
						)x
				)y

		EXEC sp_xml_removedocument @handle 

		DECLARE @xmlBackupSet TABLE
			(
				  [database_name]	[sysname]
				, [type]			[nvarchar](32)
				, [start_date]		[nvarchar](32)
				, [duration]		[nvarchar](32)
				, [size]			[nvarchar](32)
				, [size_bytes]		[bigint]
				, [verified]		[nvarchar](8)
				, [file_name]		[nvarchar](512)
				, [error_code]		[int]
			)


		DECLARE @xmlSkippedActions TABLE
			(
				  [database_name]	[sysname]
				, [backup_type]		[nvarchar](32)
				, [date]			[nvarchar](32)
				, [reason]			[nvarchar](512)
			)

		INSERT	INTO @xmlBackupSet([database_name], [type], [start_date], [duration], [size], [size_bytes], [verified], [file_name], [error_code])
				SELECT [database_name], [type], [start_date], [duration], [size], [size_bytes], [verified], [file_name], [error_code]
				FROM (
						SELECT	  ref.value ('database_name[1]', 'sysname') as [database_name]
								, ref.value ('type[1]', 'nvarchar(32)') as [type]
								, ref.value ('start_date[1]', '[datetime]') as [start_date]
								, ref.value ('duration[1]', 'nvarchar(32)') as [duration]
								, ref.value ('size[1]', 'nvarchar(32)') as [size]
								, ref.value ('size_bytes[1]', 'bigint') as [size_bytes]
								, ref.value ('verified[1]', 'nvarchar(8)') as [verified]
								, ref.value ('file_name[1]', 'nvarchar(512)') as [file_name]
								, ref.value ('error_code[1]', 'int') as [error_code]
						FROM (
								SELECT	CAST([message] AS [xml]) AS [message_xml]
								FROM	[dbo].[logEventMessages]
								WHERE	[message] LIKE '<backupset>%'
										AND ISNULL([project_id], 0) = ISNULL(@projectID, 0)
										AND ISNULL([instance_id], 0) = ISNULL(@instanceID, 0)
										AND [event_type]=0
							)x CROSS APPLY [message_xml].nodes ('//backupset/detail') R(ref)								
					)bs
				WHERE [start_date] BETWEEN @jobStartTime AND GETDATE()


		INSERT	INTO @xmlSkippedActions ([database_name], [backup_type], [date], [reason])
				SELECT [database_name], [backup_type], [date], [reason]
				FROM (
						SELECT	  ref.value ('affected_object[1]', 'sysname') as [database_name]
								, ref.value ('type[1]', 'nvarchar(32)') as [backup_type]
								, ref.value ('date[1]', '[datetime]') as [date]
								, ref.value ('reason[1]', 'nvarchar(512)') as [reason]
						FROM (
								SELECT	CAST([message] AS [xml]) AS [message_xml]
								FROM	[dbo].[logEventMessages]
								WHERE	[message] LIKE '<skipaction>%'
										AND ISNULL([project_id], 0) = ISNULL(@projectID, 0)
										AND ISNULL([instance_id], 0) = ISNULL(@instanceID, 0)
										AND [event_type]=0
										AND [event_name] = 'database backup'
							)x CROSS APPLY [message_xml].nodes ('//skipaction/detail') R(ref)	
					) sa
				WHERE [date] BETWEEN @jobStartTime AND GETDATE()


		SET @HTMLBody =@HTMLBody + N'<br><br>'
		SET @HTMLBody =@HTMLBody + COALESCE(
							N'<TABLE BORDER="1">' +
							N'<TR>' +
								N'	<TH>Database Name</TH>
									<TH>Backup Type</TH>
									<TH>Start Time</TH>
									<TH>Run Duration</TH>
									<TH>Size</TH>
									<TH>Verified</TH>
									<TH>File Name</TH>
									<TH>Error Code</TH>' +
								CAST ( ( 
										SELECT	TD = [database_name], '',
												TD = [type], '',
												TD = [start_date], '',
												TD = [duration], '',
												TD = [size], '',
												TD = [verified], '',
												TD = [file_name], '',
												TD = [error_code], ''
										FROM (
												SELECT	TOP (100) PERCENT *
												FROM @xmlBackupSet							
												ORDER BY [database_name]
											)x
										FOR XML PATH('TR'), TYPE 
							) AS NVARCHAR(MAX) ) +
							N'</TABLE>', '') ;

		IF (SELECT COUNT(*) FROM @xmlSkippedActions)>0
			begin
				SET @HTMLBody =@HTMLBody + N'<br><br>'
				SET @HTMLBody =@HTMLBody + COALESCE(
									N'<TABLE BORDER="1">' +
									N'<TR>' +
										N'	<TH>Database Name</TH>
											<TH>Backup Type</TH>
											<TH>Date</TH>
											<TH>Reason</TH>' +
										CAST ( ( 
												SELECT	TD = [database_name], '',
														TD = [backup_type], '',
														TD = [date], '',
														TD = [reason], ''
												FROM (
														SELECT	TOP (100) PERCENT *
														FROM @xmlSkippedActions							
														ORDER BY [database_name]
													)x
												FOR XML PATH('TR'), TYPE 
									) AS NVARCHAR(MAX) ) +
									N'</TABLE>', '') ;
			end

		--if any of the backups had failed, send notification
		IF @additionalOption=0
			SELECT @additionalOption = COUNT(*)
			FROM @xmlBackupSet
			WHERE [error_code]<>0
	end


SET @HTMLBody = REPLACE(@HTMLBody, N'&amp;', N'&')
SET @HTMLBody = REPLACE(@HTMLBody, N'&amp;lt;', N'<')
SET @HTMLBody = REPLACE(@HTMLBody, N'&amp;gt;', N'>')


-----------------------------------------------------------------------------------------------------
--get notification status
-----------------------------------------------------------------------------------------------------
IF @eventType IN (2, 5)
	begin
		DECLARE @notifyOnlyFailedJobs [nvarchar](32)

		SELECT	@notifyOnlyFailedJobs = LOWER([value])
		FROM	[dbo].[appConfigurations]
		WHERE	[name]='Notify job status only for Failed jobs'
				AND [module] = 'common'


		IF @notifyOnlyFailedJobs = 'true' AND @additionalOption=0
			SET @recipientsList=NULL
	end
	
IF @eventType IN (1)
	begin
		IF @ignoreAlertsForError1222=1 AND @errorCode=1222
			begin
				SET @alertSent=1
				SET @isFloodControl=1
				SET @recipientsList=NULL
			end
	end

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
SET @isEmailSent	= 0 
SET @isFloodControl	= 0

IF @alertSent=0
	begin
		SET @projectCode = ISNULL(@projectCode, 'N/A')
		
		SET @emailSubject = CASE WHEN @projectID IS NOT NULL THEN N'[' + @projectCode + '] ' ELSE N'' END 
							+ CASE	WHEN @eventType=0 THEN N'info'
									WHEN @eventType=1 THEN N'alert'
									WHEN @eventType IN (2, 5) THEN N'job status'
									WHEN @eventType=3 THEN N'report'
									WHEN @eventType=4 THEN N'action'
									WHEN @eventType=6 THEN N'alert'
								END	 
							+ N' on ' + N'[' +  @sqlServerName + ']: ' 
							+ CASE WHEN @dbName IS NOT NULL THEN QUOTENAME(@dbName) + N' - ' ELSE N'' END 
							+ CASE	WHEN @eventType=1 THEN N'[error] - '
									WHEN @eventType IN (2, 5) THEN 
											CASE	WHEN @additionalOption=0 
													THEN N'[completed] - '
													ELSE N'[failed] - '
											END	
									ELSE N''
								END
							+ @eventName
							+ CASE WHEN @objectName IS NOT NULL THEN N' - ' + @objectName ELSE N'' END
			
		SET @HTMLBody = @HTMLBody + N'<HR><P STYLE="font-family: Arial, Tahoma; font-size:10px;">This email is sent by [' + @@SERVERNAME + N'].	Generated by dbaTDPMon.<br><P>'
				
		-----------------------------------------------------------------------------------------------------		
		IF @recipientsList IS NOT NULL AND @dbMailProfileName IS NOT NULL
			begin
				-----------------------------------------------------------------------------------------------------
				--sending email using dbmail
				-----------------------------------------------------------------------------------------------------
				IF @eventType in (2, 3, 5) AND @parameters IS NOT NULL
					EXEC msdb.dbo.sp_send_dbmail  @profile_name		= @dbMailProfileName
												, @recipients		= @recipientsList
												, @subject			= @emailSubject
												, @body				= @HTMLBody
												, @file_attachments = @parameters
												, @body_format		= 'HTML'
				ELSE
					EXEC msdb.dbo.sp_send_dbmail  @profile_name		= @dbMailProfileName
												, @recipients		= @recipientsList
												, @subject			= @emailSubject
												, @body				= @HTMLBody
												, @file_attachments = NULL
												, @body_format		= 'HTML'			
					
				SET @isEmailSent=1

				EXEC [dbo].[usp_logPrintMessage] @customMessage='email sent', @raiseErrorAsPrint=1, @messagRootLevel=0, @messageTreelevel=1, @stopExecution=0
			end
	end
ELSE
	begin
		SET @isFloodControl=1
	end

SET @eventData = SUBSTRING(CAST(@eventMessageXML AS [varchar](8000)), 1, 8000)
EXEC [dbo].[usp_logEventMessage]	@projectCode			= @projectCode,
									@sqlServerName			= @sqlServerName,
									@dbName					= @dbName,
									@objectName				= @objectName,
									@childObjectName		= @childObjectName,
									@module					= @module,
									@eventName				= @eventName,
									@parameters				= @parameters,
									@eventMessage			= @eventData,
									@eventType				= @eventType,
									@recipientsList			= @recipientsList,
									@isEmailSent			= @isEmailSent,
									@isFloodControl			= @isFloodControl


RETURN @ReturnValue
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
		@flgOptions			[int] = 6107,		/*  1 - use CHECKSUM (default)
													2 - use COMPRESSION, if available (default)
													4 - use COPY_ONLY
													8 - force change backup type (default): if log is set, and no database backup is found, a database backup will be first triggered
												  										    if diff is set, and no full database backup is found, a full database backup will be first triggered
												   16 - verify backup file (default)
											       32 - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
												   64 - create folders for each database (default)
												  128 - when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
												  256 - for +2k5 versions, use xp_delete_file option (default)
												  512 - skip databases involved in log shipping (primary or secondary or logs, secondary for full/diff backups) (default)
												 1024 - on alwayson availability groups, for secondary replicas, force copy-only backups (default)
												 2048 - change retention policy from RetentionDays to RetentionBackupsCount (number of full database backups to be kept)
													  - this may be forced by setting to true property 'Change retention policy from RetentionDays to RetentionBackupsCount'
												 4096 - use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default)
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

		/* check for the standby property */
		IF  @databaseStateDesc IN ('ONLINE')
			begin
				SET @queryToRun = N'SELECT CONVERT([sysname], DATABASEPROPERTYEX(''' + @dbName + N''', ''IsInStandBy'')) AS [state]' 
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #serverPropertyConfig
				INSERT	INTO #serverPropertyConfig([value])
						EXEC (@queryToRun)

				IF (SELECT [value] FROM #serverPropertyConfig) = '1'
					SET @databaseStateDesc = 'STANDBY'
			end

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

DECLARE		@lastFullBackupSetIDRemaining	[int],
			@lastDiffBackupSetIDRemaining	[int],
			@lastBackupType					[char](1)

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

IF OBJECT_ID('tempdb..#backupSET') IS NOT NULL
	DROP TABLE #backupSET

CREATE TABLE #backupSET 
		(
			  [backup_set_id]		[int]
			, [backup_start_date]	[datetime]	NULL
			, [type]				[char](1)	NULL
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
										SELECT bs.[backup_set_id], bs.[backup_start_date], bs.[type]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type] IN (''D'', ''I'')
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
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
					, @lastFullBackupSetIDRemaining = [backup_set_id]
					, @lastBackupType = [type]
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
														AND bs.[backup_set_id] < ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
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
							, @lastFullBackupSetIDRemaining = [backup_set_id]
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
												AND bs.[backup_set_id] > ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
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

		SELECT TOP 1  @lastDiffBackupSetIDRemaining  = [backup_set_id]
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

				INSERT	INTO #backupSET([backup_set_id])
						EXEC (@queryToRun)
			end

		SELECT TOP 1  @maxAllowedDate = DATEADD(ss, -1, [backup_start_date])
					, @lastFullBackupSetIDRemaining = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC

		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT TOP 1 bs.[backup_set_id]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''I''
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
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

		SELECT TOP 1  @lastDiffBackupSetIDRemaining = [backup_set_id]
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
			@lastFullBackupSetIDRemaining AS lastFullBackupSetIDRemaining, 
			@lastDiffBackupSetIDRemaining AS lastDiffBackupSetIDRemaining, 
			@forceChangeRetentionPolicy AS forceChangeRetentionPolicy,
			@flgOptions & 256,
			@errorCode,
			@serverVersionNum,
			@flgOptions & 128

-----------------------------------------------------------------------------------------
--in case of previous errors or 2k version, will use "standard" delete file
IF (@flgOptions & 256 = 0) OR (@errorCode<>0 AND @flgOptions & 256 = 256) OR (@serverVersionNum < 9) OR (@flgOptions & 128 = 128 AND @lastFullBackupSetIDRemaining IS NOT NULL)
	begin
		SELECT  @optionXPIsAvailable		= 0,
				@optionXPValue				= 0,
				@optionXPHasChanged			= 0,
				@optionAdvancedIsAvailable	= 0,
				@optionAdvancedValue		= 0,
				@optionAdvancedHasChanged	= 0

		IF @serverVersionNum>=9
			begin
				/* enable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
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
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 1,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode

						IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
							EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
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
															bs.[backup_set_id] < ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + N'%'')
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
													OR (
															-- delete incremental and transaction log backups to keep the retention/restore period fixed
															' + CAST(ISNULL(@lastDiffBackupSetIDRemaining, 0)  AS [nvarchar]) + N' <> 0
														AND bs.[backup_set_id] < ' + CAST(ISNULL(@lastDiffBackupSetIDRemaining, 0) AS [nvarchar]) + N'
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
		DECLARE crsCleanupBackupFiles CURSOR FOR	SELECT [physical_device_name]
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

		IF @serverVersionNum>=9 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
			begin
				/* disable xp_cmdshell configuration option */
				IF @optionXPHasChanged = 1
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 0,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode

				/* disable show advanced options configuration option */
				IF @optionAdvancedHasChanged = 1
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 0,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode
			end
	end

RETURN @errorCode
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

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

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
SELECT  @optionXPIsAvailable		= 0,
		@optionXPValue				= 0,
		@optionXPHasChanged			= 0,
		@optionAdvancedIsAvailable	= 0,
		@optionAdvancedValue		= 0,
		@optionAdvancedHasChanged	= 0

IF @serverVersionNum>=9
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
				set @queryToRun='xp_cmdshell component is turned off. Cannot continue'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				--RETURN 1
			end		
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
IF @serverVersionNum>=9 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
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
GO



RAISERROR('Create procedure: [dbo].[usp_mpRestoreDatabaseAs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpRestoreDatabaseAs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpRestoreDatabaseAs]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_mpRestoreDatabaseAs]
		  @forDatabaseName				[sysname]
		, @toDatabaseName				[sysname]
		, @restorePathForData			[nvarchar](260)
		, @restorePathForLog			[nvarchar](260)
		, @addTimeStampSuffixToFiles	[bit] = 0
AS

SET NOCOUNT ON
DECLARE   @queryToRun		[nvarchar](max)
		, @BackupFileName	[nvarchar](260)
		, @LogicalName		[sysname]
		, @logicalFileType	[char](1)
		, @PhysicalName		[nvarchar](260)
		, @timeStampSuffix	[nvarchar](20)

IF OBJECT_ID('tempdb..#mostRecentBackupFile') IS NOT NULL DROP TABLE #mostRecentBackupFile
CREATE TABLE #mostRecentBackupFile
	(
		  [file_name]	[nvarchar](260)
		, [time_stamp]	[nvarchar](20)
		, [type]		[sysname]	NULL
	)
		
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'
DECLARE	  @serverToRun			[sysname]
		, @forSQLServerName		[sysname]
		, @backupLocation		[nvarchar](512)
		, @backupType			[nvarchar](32)
		, @nameConvention		[nvarchar](32)


SELECT    @serverToRun = [RunSQLFrom]
		, @forSQLServerName = [ServerName]
		, @backupLocation = [BackupLocation]
		, @backupType = ''full''
		, @nameConvention = [BackupMode]
FROM DbaAdmin.refresh.ClientSourceData
WHERE [DatabaseName] = ''' + @forDatabaseName + '''

EXEC dbaTDPMon.[dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]	  @serverToRun		= @serverToRun
																		, @forSQLServerName = @forSQLServerName
																		, @forDatabaseName	= ''' + @forDatabaseName + '''
																		, @backupLocation	= @backupLocation
																		, @backupType		= @backupType
																		, @nameConvention	= @nameConvention
																		, @debugMode		= 0
'

SET @queryToRun = N'SELECT * FROM OPENQUERY([AWISMONDB1], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([file_name] [nvarchar](260), [time_stamp] [nvarchar](20)))'')'

/* get latest full database backup */
--PRINT @queryToRun
INSERT	INTO #mostRecentBackupFile([file_name], [time_stamp])
		EXEC (@queryToRun)

UPDATE #mostRecentBackupFile SET [type]='full' WHERE [type] IS NULL

/* get latest differential database backup */
SET @queryToRun = REPLACE(@queryToRun, '@backupType = ''''''''full''''''''', '@backupType = ''''''''diff''''''''')

--PRINT @queryToRun
INSERT	INTO #mostRecentBackupFile([file_name], [time_stamp])
		EXEC (@queryToRun)

UPDATE #mostRecentBackupFile SET [type]='diff' WHERE [type] IS NULL

DELETE FROM #mostRecentBackupFile
WHERE	[type]='diff' 
		AND CAST(REPLACE([time_stamp], '_', '') AS [bigint]) < (
																SELECT CAST(REPLACE([time_stamp], '_', '') AS [bigint]) 
																FROM #mostRecentBackupFile 
																WHERE [type]='full'
																)

IF @addTimeStampSuffixToFiles=1
	SELECT @timeStampSuffix = MAX([time_stamp])
	FROM #mostRecentBackupFile

		
IF OBJECT_ID('tempdb..#dbFileList') IS NOT NULL DROP TABLE #dbFileList;
CREATE TABLE #dbFileList
(
	  [LogicalName]				nvarchar(128)
	, [PhysicalName]			nvarchar(260)
	, [Type]					char(1)
	, [FileGroupName]			nvarchar(128)
	, [Size]					numeric(20,0)
	, [MaxSize]					numeric(20,0)
	, [FileID]					bigint
	, [CreateLSN]				numeric(25,0)
	, [DropLSN]					numeric(25,0) NULL
	, [UniqueID]				uniqueidentifier
	, [ReadOnlyLSN]				numeric(25,0) NULL
	, [ReadWriteLSN]			numeric(25,0) NULL
	, [BackupSizeInBytes]		bigint
	, [SourceBlockSize]			int
	, [FileGroupID]				int
	, [LogGroupGUID]			uniqueidentifier NULL
	, [DifferentialBaseLSN]		numeric(25,0) NULL
	, [DifferentialBaseGUID]	uniqueidentifier
	, [IsReadOnly]				bit
	, [IsPresent]				bit
	, [TDEThumbprint]			varbinary(32)
)

/* restore last full database backup */
SELECT @BackupFileName = [file_name] FROM #mostRecentBackupFile WHERE [type]='full'

SET @queryToRun = N'RESTORE FILELISTONLY FROM DISK = ''' + @BackupFileName + ''';';
INSERT	INTO #dbFileList 
		EXEC (@queryToRun)

SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] FROM DISK = ''' + @BackupFileName + N''' WITH STATS=1, REPLACE, NORECOVERY';
			
DECLARE crsdbFileList CURSOR FOR	SELECT [LogicalName], [PhysicalName], [Type] 
									FROM #dbFileList;
OPEN crsdbFileList;
FETCH NEXT FROM crsdbFileList INTO @LogicalName, @PhysicalName, @logicalFileType;
WHILE @@FETCH_STATUS = 0
	begin
		SET @PhysicalName = RIGHT(@PhysicalName, CHARINDEX('\', REVERSE(@PhysicalName)) - 1)
		IF @addTimeStampSuffixToFiles=1
			SET @PhysicalName = SUBSTRING(@PhysicalName, 1, LEN(@PhysicalName)-4) + 
								'_' + @timeStampSuffix + 
								'_' + REPLACE(REPLACE(REPLACE(CONVERT([varchar](19), GETDATE(), 121), '-', ''), ':', ''), ' ', '_') + 
								SUBSTRING(@PhysicalName, LEN(@PhysicalName)-3, 4)

		SET @queryToRun = @queryToRun + N', MOVE ''' + @LogicalName + ''' TO ''' + 
							CASE	WHEN @logicalFileType='D' THEN @restorePathForData
									WHEN @logicalFileType='L' THEN @restorePathForLog
							END  + @PhysicalName + '''';
				
		FETCH NEXT FROM crsdbFileList INTO @LogicalName, @PhysicalName, @logicalFileType;
	end
		
CLOSE crsdbFileList;
DEALLOCATE crsdbFileList;			

/* kill existing database connections*/
EXEC [dbaTDPMon].[dbo].[usp_mpDatabaseKillConnections]	@SQLServerName	= @@SERVERNAME,
														@DBName			= @toDatabaseName,
														@flgOptions		= 3

PRINT '--' + @queryToRun
EXEC sp_executesql @queryToRun


/* restore last differential backup */
SET @BackupFileName=NULL
SELECT @BackupFileName = [file_name] FROM #mostRecentBackupFile WHERE [type]='diff'

IF @BackupFileName IS NOT NULL
	begin
		SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] FROM DISK = ''' + @BackupFileName + N''' WITH STATS=1, REPLACE, NORECOVERY';

		PRINT '--' + @queryToRun
		EXEC sp_executesql @queryToRun
	end


SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] WITH RECOVERY';
PRINT '--' + @queryToRun
EXEC sp_executesql @queryToRun
GO


RAISERROR('update jobs description..', 10, 1) WITH NOWAIT
GO
UPDATE msdb.dbo.sysjobs SET [description]=REPLACE([description], 'codeple.com', 'codeplex.com') WHERE [description] LIKE '%codeple.com%'
GO


USE [dbaTDPMon]
GO
RAISERROR('Create procedure: [dbo].[usp_mpDatabaseConsistencyCheck]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseConsistencyCheck]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseConsistencyCheck]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseConsistencyCheck]
		@sqlServerName			[sysname]=@@SERVERNAME,
		@dbName					[sysname],
		@tableSchema			[sysname]	=  '%',
		@tableName				[sysname]   =  '%',
		@flgActions				[smallint]	=   12,
		@flgOptions				[int]		=    3,
		@executionLevel			[tinyint]	=    0,
		@debugMode				[bit]		=    0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan 
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : Consistency Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify % for all tables or a table name to be analyzed
--		@flgActions		1	- perform database consistency check (DBCC CHECKDB)
--							  should be performed weekly
--						2	- perform table consistency check (DBCC CHECKTABLE)
--							  should be performed weekly
--					    4   - perform consistency check of disk space allocation structures (DBCC CHECKALLOC) (default)
--							  should be performed daily
--					    8   - perform consistency check of catalogs (DBCC CHECKCATALOG) (default)
--							  should be performed daily
--					   16   - perform consistency check of table constraints (DBCC CHECKCONSTRAINTS)
--							  should be performed weekly
--					   32   - perform consistency check of table identity value (DBCC CHECKIDENT)
--							  should be performed weekly
--					   64   - perform correction to space usage (DBCC UPDATEUSAGE)
--							  should be performed once at 2 weeks
--					  128 	- Cleaning wasted space in Database (variable-length column) (DBCC CLEANTABLE)
--							  should be performed once a year
--		@flgOptions	    1	- run DBCC CHECKDB/DBCC CHECKTABLE using PHYSICAL_ONLY (default). 
--							  by default DBCC CHECKDB is doing all consistency checks and for a VLDB it may take a very long time
--					    2  - use NOINDEX when running DBCC CHECKTABLE. Index consistency errors are not critical (default)
--					   32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution

--		@debugMode			- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= 'dbSQLTools',
												@tableSchema			= 'dbo',
												@tableName				= '%',
												@flgActions				= DEFAULT,
												@flgOptions				= DEFAULT,
												@debugMode				= DEFAULT
*/

DECLARE		@queryToRun  					[nvarchar](2048),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@DBCCCheckTableBatchSize 		[int],
			@errorCode						[int],
			@databaseStatus					[int],
			@dbi_dbccFlags					[int]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName			[sysname],
		@agStopLimit	[int],
		@actionType		[sysname]

SET @agStopLimit = 0
SET @actionType = NULL

IF @flgActions &  64 = 64	SET @actionType = 'update space usage'
IF @flgActions & 128 = 128	SET @actionType = 'clean wasted space - table'

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database maintenance',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0
	
---------------------------------------------------------------------------------------------
DECLARE @compatibilityLevel [tinyint]
IF object_id('tempdb..#databaseCompatibility') IS NOT NULL 
	DROP TABLE #databaseCompatibility

CREATE TABLE #databaseCompatibility
	(
		[compatibility_level]		[tinyint]
	)


SET @queryToRun = N''
IF @serverVersionNum >= 9
	SET @queryToRun = @queryToRun + N'SELECT [compatibility_level] FROM sys.databases WHERE [name] = ''' + @dbName + N''''
ELSE
	SET @queryToRun = @queryToRun + N'SELECT [cmptlevel] FROM master.dbo.sysdatabases WHERE [name] = ''' + @dbName + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@compatibilityLevel, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC (@queryToRun)

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility


---------------------------------------------------------------------------------------------
SET @DBCCCheckTableBatchSize = 65536
SET @CurrentTableSchema		 = @tableSchema
SET @tableName				 = REPLACE(@tableName, '''', '''''')
SET @errorCode				 = 0

---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseTableList') IS NOT NULL 
	DROP TABLE #databaseTableList

CREATE TABLE #databaseTableList(
								[table_schema]	[sysname]	NULL,
								[table_name]	[sysname]	NULL
								)
CREATE INDEX IX_databaseTableList_TableName ON #databaseTableList([table_name])



--------------------------------------------------------------------------------------------------
--get database status
-----------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)
			
SET @queryToRun = N'SELECT [status] FROM master.dbo.sysdatabases WHERE [name]=''' + @dbName + N'''' 
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #serverPropertyConfig
INSERT	INTO #serverPropertyConfig([value])
		EXEC (@queryToRun)

SELECT @databaseStatus = [value]
FROM #serverPropertyConfig

---------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2 OR @flgActions & 16 = 16 OR @flgActions & 64 = 64 OR @flgActions & 128 = 128
	begin
		--get table list that will be analyzed including materialized views; will pick only tables with reserved pages
		SET @queryToRun = N''
		IF @serverVersionNum >= 9
			SET @queryToRun = @queryToRun + N'SELECT DISTINCT ob.[table_schema], ob.[table_name]
FROM (
		SELECT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N'].sys.objects obj WITH (READPAST)
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''' +

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL

		SELECT DISTINCT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N'].sys.indexes idx WITH (READPAST)
		INNER JOIN [' + @dbName + N'].sys.objects obj WITH (READPAST) ON obj.[object_id] = idx.[object_id]
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N''''
		END + N'
	)ob
INNER JOIN
	(
		SELECT	ps.[object_id],
				sch.[name]	AS [schema_name],
				so.[name]	AS [table_name],
				ps.[reserved_page_count]
		FROM (
				SELECT	ps.[object_id]
						, SUM (ps.[reserved_page_count]) AS [reserved_page_count]
				FROM [' + @dbName + N'].sys.dm_db_partition_stats ps WITH (READPAST)
				GROUP BY ps.[object_id]
			) AS ps
		INNER JOIN [' + @dbName + N'].sys.objects so  WITH (READPAST) ON so.[object_id] = ps.[object_id] 
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id]
		WHERE	so.[type] in (''S'', ''U'', ''V'')
			AND ps.[reserved_page_count] > 0
	)ps ON ob.[object_id] = ps.[object_id]'
		ELSE
			SET @queryToRun = @queryToRun + N'SELECT ob.[table_schema], ob.[table_name]
FROM (
		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N']..sysobjects obj
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = obj.[uid]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''' + 

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL			

		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N']..sysindexes idx
		INNER JOIN [' + @dbName + N']..sysobjects obj ON obj.[id] = idx.[id]
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = obj.[uid]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N''''
		END + N'
	)ob
INNER JOIN
	(
		SELECT si.[id] AS [object_id], sch.[name] AS [table_schema], so.[name] AS [table_name]
		FROM [' + @dbName + N']..sysobjects so
		INNER JOIN [' + @dbName + N']..sysindexes si on so.[id] = si.[id]
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = so.[uid]
		WHERE si.[reserved]<>0
	)ps ON ob.[object_id] = ps.[object_id]'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #databaseTableList
		INSERT	INTO #databaseTableList([table_schema], [table_name])
				EXEC (@queryToRun)
	end

--------------------------------------------------------------------------------------------------
--when running DBCC CHECKDB, check if DATA_PURITY option should be used or not (run only when dbi_dbccFlags=0)
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1 AND @serverVersionNum >= 9 AND @flgOptions & 1 = 0
	begin
		IF object_id('tempdb..#dbi_dbccFlags') IS NOT NULL DROP TABLE #dbccLastKnownGood
		CREATE TABLE #dbi_dbccFlags
		(
			[Value]					[sysname]			NULL
		)

		IF object_id('tempdb..#dbccDBINFO') IS NOT NULL DROP TABLE #dbccDBINFO
		CREATE TABLE #dbccDBINFO
			(
				[id]				[int] IDENTITY(1,1),
				[ParentObject]		[varchar](255),
				[Object]			[varchar](255),
				[Field]				[varchar](255),
				[Value]				[varchar](255)
			)
	
		IF @sqlServerName <> @@SERVERNAME
			begin
				IF @serverVersionNum < 11
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @dbName + N']) WITH TABLERESULTS'''')'')x
										WHERE [Field]=''dbi_dbccFlags'''
				ELSE
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @dbName + N']) WITH TABLERESULTS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
										WHERE [Field]=''dbi_dbccFlags'''
			end
		ELSE
			begin							
				INSERT	INTO #dbccDBINFO
						EXEC ('DBCC DBINFO (''' + @dbName + N''') WITH TABLERESULTS')

				SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccFlags'''											
			end

		IF @debugMode = 1 PRINT @queryToRun
				
		TRUNCATE TABLE #dbi_dbccFlags
		INSERT	INTO #dbi_dbccFlags([Value])
				EXEC (@queryToRun)

		SELECT @dbi_dbccFlags = ISNULL([Value], 0)
		FROM #dbi_dbccFlags
		
		SET @dbi_dbccFlags = ISNULL(@dbi_dbccFlags, 0)
	end


--------------------------------------------------------------------------------------------------
--database consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Database consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + '...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKDB(''' + @dbName + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS' + CASE WHEN @flgOptions & 1 = 1 THEN ', PHYSICAL_ONLY' ELSE '' END

		IF @serverVersionNum >= 9 AND @flgOptions & 1 = 0 AND @dbi_dbccFlags <> 2
			SET @queryToRun = @queryToRun + ', DATA_PURITY'

		IF @compatibilityLevel >= 100 AND @flgOptions & 1 = 0
			SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end	


--------------------------------------------------------------------------------------------------
--tables and views consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Tables/views consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + CASE WHEN @flgOptions & 2 = 2 THEN '(NOINDEX)' ELSE '' END + '...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
										FROM #databaseTableList	
										ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKTABLE(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']''' + CASE WHEN @flgOptions & 2 = 2 THEN ', NOINDEX' ELSE '' END + ') WITH ALL_ERRORMSGS, NO_INFOMSGS'
				
				IF @serverVersionNum >= 9 AND @dbi_dbccFlags <> 2
					SET @queryToRun = @queryToRun + ', DATA_PURITY'
				
				IF @compatibilityLevel >= 10 AND @flgOptions & 2 = 0
					SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				
				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database consistency check - tables/views',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end			


--------------------------------------------------------------------------------------------------
--allocation structures consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 4 = 4
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Allocation structures consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKALLOC(''' + @dbName + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check - allocation structures',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end			


--------------------------------------------------------------------------------------------------
--catalogs consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 8 = 8
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Catalogs consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKCATALOG(''' + @dbName + ''') WITH NO_INFOMSGS'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check - catalogs',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end			


--------------------------------------------------------------------------------------------------
--table constraints consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 16 = 16
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Table constraints consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
										FROM #databaseTableList	
										ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKCONSTRAINTS(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'') WITH ALL_ERRORMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database consistency check - table constraints',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end			


--------------------------------------------------------------------------------------------------
--table identity value consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 32 = 32
	begin
		IF	@databaseStatus & 32 = 32				/* LOADING */
			OR @databaseStatus & 64 = 64			/* PRE RECOVERY */
			OR @databaseStatus & 128 = 128			/* RECOVERING */
			OR @databaseStatus & 256 = 256			/* NOT RECOVERED */
			OR @databaseStatus & 512 = 512			/* OFFLINE */
			OR @databaseStatus & 1024 = 1024		/* READ ONLY */
			OR @databaseStatus & 2048 = 2048		/* DBO USE ONLY */
			OR @databaseStatus & 4096 = 4096		/* SINGLE USER */
			OR @databaseStatus & 32768 = 32768		/* EMERGENCY MODE */
			OR @databaseStatus & 2097152 = 2097152	/* STANDBY */
			OR @databaseStatus & 4194584 = 4194584	/* SUSPECT */
			OR @databaseStatus = 0
			begin
				SET @queryToRun='Current database state does not allow running DBCC CHECKIDENT. It will be skipped.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			end
		ELSE
			begin
				IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun=N'Table identity value consistency check ...' + ' [' + @dbName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
				---------------------------------------------------------------------------------------------
				--create temporary tables that will be used 
				---------------------------------------------------------------------------------------------
				IF object_id('tempdb..#databaseTableListIdent') IS NOT NULL 
					DROP TABLE #databaseTableListIdent

				CREATE TABLE #databaseTableListIdent(
														[table_schema]	[sysname],
														[table_name]	[sysname]
													)
				CREATE INDEX IX_databaseTableListIdent_TableName ON #databaseTableListIdent([table_name])


				--get table list that will be analyzed. only tables with identity columns
				SET @queryToRun = N''
				IF @serverVersionNum >= 9
					SET @queryToRun = @queryToRun + N'	SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM [' + @dbName + '].sys.objects obj
												INNER JOIN [' + @dbName + '].sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
												WHERE obj.[type] IN (''U'')
														AND obj.[object_id] IN (
																			SELECT [object_id]
																			FROM [' + @dbName + '].sys.columns
																			WHERE [is_identity] = 1
																			)
														AND obj.[name] LIKE ''' + @tableName + '''
														AND sch.[name] LIKE ''' + @tableSchema + ''''
				ELSE
					SET @queryToRun = @queryToRun + N'SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM  [' + @dbName + ']..sysobjects obj
												INNER JOIN  [' + @dbName + ']..sysusers sch ON sch.[uid] = obj.[uid]
												WHERE obj.[type] IN (''U'')
														AND obj.[id] IN (
																		SELECT [id]
																		FROM  [' + @dbName + ']..syscolumns
																		WHERE [autoval] is not null
																		)
														AND obj.[name] LIKE ''' + @tableName + '''
														AND sch.[name] LIKE ''' + @tableSchema + ''''			
				
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #databaseTableListIdent
				INSERT	INTO #databaseTableListIdent([table_schema], [table_name])
						EXEC (@queryToRun)

				DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
												FROM #databaseTableListIdent	
												ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0
					begin
						SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
						SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC CHECKIDENT(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'', RESEED)'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																		@eventName		= 'database consistency check - table identity value',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
																					
						FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList

				IF object_id('tempdb..#databaseTableListIdent') IS NOT NULL 
					DROP TABLE #databaseTableListIdent
			end			
	end

--------------------------------------------------------------------------------------------------
--correct space usage
--------------------------------------------------------------------------------------------------
IF @flgActions & 64 = 64
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Update space usage...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		IF @tableName='%' 
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + @dbName + ''') WITH NO_INFOMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= NULL,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database maintenance - update space usage',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
			end
		ELSE
			begin
				DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
												FROM #databaseTableList	
												ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0
					begin
						SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
						SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + @dbName + ''', ''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'')'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																		@eventName		= 'database maintenance - update space usage',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
																		
						FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end
	end			


--------------------------------------------------------------------------------------------------
--		Cleaning wasted space in Database
--		DBCC CLEANTABLE reclaims space after a variable-length column is dropped. 
--		A variable-length column can be one of the following data types:  varchar, nvarchar, varchar(max),
--		nvarchar(max), varbinary, varbinary(max), text, ntext, image, sql_variant, and xml. 
--		The command does not reclaim space after a fixed-length column is dropped.

--		Best Practices
--		DBCC CLEANTABLE should not be executed as a routine maintenance task. 
--		Instead, use DBCC CLEANTABLE after you make significant changes to variable-length columns in 
--		a table or indexed view and you need to immediately reclaim the unused space. 
--		Alternatively, you can rebuild the indexes on the table or view; however, doing so is a more 
--		resource-intensive operation.
--------------------------------------------------------------------------------------------------
IF @flgActions & 128 = 128
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Cleaning wasted space in variable length columns...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
										FROM #databaseTableList	
										ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CLEANTABLE(''' + @dbName + ''', ''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'', ' + CAST(@DBCCCheckTableBatchSize AS [nvarchar]) + ')'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database maintenance - clean wasted space - table',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end

RETURN @errorCode
GO


SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
