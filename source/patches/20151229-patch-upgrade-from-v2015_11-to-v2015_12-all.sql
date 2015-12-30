USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2015.12.29' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='logEventMessages' AND COLUMN_NAME='message' AND CHARACTER_MAXIMUM_LENGTH<>-1)
	ALTER TABLE [dbo].[logEventMessages] ALTER COLUMN [message] [varchar](max)
GO

RAISERROR('Create procedure: [dbo].[usp_sqlExecuteAndLog]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_sqlExecuteAndLog]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_sqlExecuteAndLog]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_sqlExecuteAndLog]
		@sqlServerName			[sysname],
		@dbName					[sysname] = NULL,
		@objectName				[nvarchar](512) = NULL,
		@childObjectName		[sysname] = NULL,
		@module					[sysname] = NULL,
		@eventName				[nvarchar](256) = NULL,
		@queryToRun  			[nvarchar](4000) = NULL,
		@flgOptions				[int]=32,
		@executionLevel			[tinyint]= 0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 23.03.2015
-- Module			 : Database Maintenance Plan 
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : run SQL command and log action
-- ============================================================================

DECLARE		@queryParameters				[nvarchar](512),
			@tmpSQL		  					[nvarchar](2048),
			@tmpServer						[varchar](256),
			@ReturnValue					[int]

DECLARE		@projectID						[smallint],
			@instanceID						[smallint],
			@errorCode						[int],
			@durationSeconds				[bigint],
			@eventData						[varchar](8000)
			
SET NOCOUNT ON


---------------------------------------------------------------------------------------------
--get default project id / instance id
SELECT	@projectID = [id]
FROM	[dbo].[catalogProjects]
WHERE	[code] IN ( 
					SELECT	[value]
					FROM	[dbo].[appConfigurations]
					WHERE	[name] = 'Default project code'
							AND [module] = 'common'
				  )

SELECT  @instanceID = [id] 
FROM	[dbo].[catalogInstanceNames]  
WHERE	[name] = @sqlServerName
		AND [project_id] = @projectID

---------------------------------------------------------------------------------------------
DECLARE @logEventActions	[nvarchar](32)

SELECT	@logEventActions = LOWER([value])
FROM	[dbo].[appConfigurations]
WHERE	[name]='Log action events'
		AND [module] = 'common'


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
SET @tmpServer='[' + @sqlServerName + '].[' + ISNULL(@dbName, 'master') + '].[dbo].[sp_executesql]'

IF @serverVersionNum >= 9
	SET @tmpSQL = N'DECLARE @startTime [datetime]

					BEGIN TRY
						SET @startTime = GETDATE()
						
						EXEC @tmpServer @queryToRun

						SET @errorCode = 0
						SET @durationSeconds=DATEDIFF(ss, @startTime, GETDATE())
					END TRY

					BEGIN CATCH
						DECLARE   @flgRaiseErrorAndStop [bit]
								, @errorString			[nvarchar](max)
								, @eventMessageData		[varchar](8000)

						SET @errorString = ERROR_MESSAGE()
						SET @errorCode = ERROR_NUMBER()
						SET @durationSeconds=DATEDIFF(ss, @startTime, GETDATE())

						IF LEFT(@errorString, 2)=''--'' 
							SET @errorString = LTRIM(SUBSTRING(@errorString, 3, LEN(@errorString)))

						SET @flgRaiseErrorAndStop = CASE WHEN @flgOptions & 32 = 32 THEN 1 ELSE 0 END
						
						SET @eventMessageData = ''<alert><detail>'' + 
												''<error_code>'' + CAST(@errorCode AS [varchar](32)) + ''</error_code>'' + 
												''<error_string>'' + @errorString + ''</error_string>'' + 
												''<query_executed>'' + @queryToRun + ''</query_executed>'' + 
												''<duration_seconds>'' + CAST(@durationSeconds AS [varchar](32)) + ''</duration_seconds>'' + 
												''<event_date_utc>'' + CONVERT([varchar](20), GETUTCDATE(), 120) + ''</event_date_utc>'' + 
												''</detail></alert>''

						EXEC [dbo].[usp_logEventMessageAndSendEmail]	@sqlServerName			= @sqlServerName,
																		@dbName					= @dbName,
																		@objectName				= @objectName,
																		@childObjectName		= @childObjectName,
																		@module					= @module,
																		@eventName				= @eventName,
																		@eventMessage			= @eventMessageData,
																		@eventType				= 1

						EXEC [dbo].[usp_logPrintMessage] @customMessage = @errorString, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=@flgRaiseErrorAndStop
					END CATCH'
ELSE
	SET @tmpSQL = N'DECLARE   @startTime			[datetime]
					
					SET @startTime = GETDATE()
					
					EXEC @tmpServer @queryToRun
					
					SET @errorCode=@@ERROR
					SET @durationSeconds=DATEDIFF(ss, @startTime, GETDATE())

					IF @errorCode<>0
						begin
							DECLARE   @flgRaiseErrorAndStop [bit]
									, @errorString			[nvarchar](255)
									, @eventData			[varchar](8000)

							SELECT @errorString = [description]
							FROM master.dbo.sysmessages 
							WHERE [error] = @errorCode

							SET @flgRaiseErrorAndStop = CASE WHEN @flgOptions & 32 = 32 THEN 1 ELSE 0 END
							
							SET @eventData = ''<alert><detail>'' + 
												''<error_code>'' + CAST(@errorCode AS [varchar](32)) + ''</error_code>'' + 
												''<error_string>'' + ISNULL(@errorString, '''') + ''</error_string>'' + 
												''<query_executed>'' + @queryToRun + ''</query_executed>'' + 
												''<duration_seconds>'' + CAST(@durationSeconds AS [varchar](32)) + ''</duration_seconds>'' + 
											''</detail></alert>''

							EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@childObjectName= @childObjectName,
																@module			= @module,
																@eventName		= @eventName,
																@eventMessage	= @eventData,
																@eventType		= 1

							EXEC [dbo].[usp_logPrintMessage] @customMessage = @errorString, @stopExecution=0
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @stopExecution=@flgRaiseErrorAndStop
						end'

SET @queryParameters=N'@tmpServer [nvarchar](512), @queryToRun [nvarchar](2048), @flgOptions [int], @module [sysname], @eventName [nvarchar](512), @sqlServerName [sysname], @dbName [sysname], @objectName [nvarchar](512), @childObjectName [sysname], @errorCode [int] OUTPUT, @durationSeconds [bigint] OUTPUT'


--------------------------------------------------------------------------------------------------
--running action
SET @errorCode=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpServer, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @tmpSQL, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

EXEC sp_executesql @tmpSQL, @queryParameters, @tmpServer		= @tmpServer
											, @queryToRun		= @queryToRun
											, @flgOptions		= @flgOptions
											, @eventName		= @eventName
											, @module			= @module
											, @sqlServerName	= @sqlServerName
											, @dbName			= @dbName
											, @objectName		= @objectName
											, @childObjectName	= @childObjectName
											, @errorCode		= @errorCode OUT
											, @durationSeconds	= @durationSeconds OUT

--------------------------------------------------------------------------------------------------
--logging action
IF @logEventActions = 'true'
	begin
		SET @eventData = '<action><detail>' + 
							CASE WHEN @dbName IS NOT NULL THEN '<database_name>' + @dbName + '</database_name>' ELSE N'' END + 
							CASE WHEN @eventName IS NOT NULL THEN '<event_name>' + @eventName + '</event_name>' ELSE N'' END + 
							CASE WHEN @objectName IS NOT NULL THEN '<object_name>' + @objectName + '</object_name>' ELSE N'' END + 
							CASE WHEN @childObjectName IS NOT NULL THEN '<child_object_name>' + @childObjectName + '</child_object_name>' ELSE N'' END + 
							'<query_executed>' + @queryToRun + '</query_executed>' + 
							'<duration>' + REPLICATE('0', 2-LEN(CAST(@durationSeconds / 3600 AS [varchar]))) + CAST(@durationSeconds / 3600 AS [varchar]) + 'h'
												+ ' ' + REPLICATE('0', 2-LEN(CAST((@durationSeconds / 60) % 60 AS [varchar]))) + CAST((@durationSeconds / 60) % 60 AS [varchar]) + 'm'
												+ ' ' + REPLICATE('0', 2-LEN(CAST(@durationSeconds % 60 AS [varchar]))) + CAST(@durationSeconds % 60 AS [varchar]) + 's' + '</duration>' + 
							'<error_code>' + CAST(@errorCode AS [varchar](32) )+ '</error_code>' + 
							'</detail></action>'

		EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
											@dbName			= @dbName,
											@objectName		= @objectName,
											@childObjectName= @childObjectName,
											@module			= @module,
											@eventName		= @eventName,
											@eventMessage	= @eventData,
											@eventType		= 4 /* action */
	end

RETURN @errorCode
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
		@ErrMessage					[nvarchar](256),
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
		EXEC @PrepareXmlStatus= sp_xml_preparedocument @handle OUTPUT, @eventMessageXML  

		SET @HTMLBody =@HTMLBody + COALESCE(
								CAST ( ( 
										SELECT	
												li = 'target-name: ' + [target_name], '',
												li = 'machine-name: ' + [machine_name], '',
												li = 'measure-unit: ' + [measure_unit], '',
												li = 'current-value: ' + CAST([current_value] AS [varchar](32)), '',
												li = 'current-percentage: ' + CAST([current_percentage] AS [varchar](32)), '',
												li = 'reference-value: ' + CAST([refference_value] AS [varchar](32)), '',
												li = 'reference-percentage: ' + CAST([refference_percentage] AS [varchar](32)), '',
												li = 'threshold-value: ' + CAST([threshold_value] AS [varchar](32)), '',
												li = 'threshold-percentage: ' + CAST([threshold_percentage] AS [varchar](32)), '',
												li = 'severity: ' + [severity], '',
												li = 'event-date (utc): ' + CAST([event_date_utc] AS [varchar](32)), ''
										FROM (
												SELECT  *
												FROM    OPENXML(@handle, '/alert/detail', 2)  
														WITH (											
																[severity]					[sysname],
																[instance_name]				[sysname],
																[machine_name]				[sysname],
																[counter_name]				[sysname],
																[target_name]				[nvarchar](512),
																[measure_unit]				[sysname],
																[current_value]				[numeric](18,3),
																[current_percentage]		[numeric](18,3),
																[refference_value]			[numeric](18,3),
																[refference_percentage]		[numeric](18,3),
																[threshold_value]			[numeric](18,3),
																[threshold_percentage]		[numeric](18,3),
																[event_date_utc]			[sysname]															)  
											)x
										FOR XML PATH('ul'), TYPE 
							) AS NVARCHAR(MAX) )
							, '') ;
		
		EXEC sp_xml_removedocument @handle 
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
									<TH>Message</TH>' +
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
					FROM [msdb].[dbo].[sysjobhistory] h
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
					FROM [msdb].[dbo].[sysjobhistory] h
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
INSERT	INTO #jobHistory([step_id], [step_name], [run_status], [run_date], [run_time], [duration], [message])
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

IF @sendLogAsAttachment=0
	SET @logFileLocation = NULL


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

RAISERROR('Create procedure: [dbo].[usp_monReplicationPublicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monReplicationPublicationLatency]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
GO

CREATE PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
		  @projectCode			[varchar](32)=NULL
		, @sqlServerNameFilter	[sysname]='%'
		, @iterations			[int] = 2
		, @iterationDelay		[varchar](10) = N'00:00:01'
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 13.10.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date	: 
-- Description	: 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON 
		
DECLARE   @sqlServerName		[sysname]
		, @projectID			[smallint]
		, @strMessage			[nvarchar](512)
		, @queryToRun			[nvarchar](max)
		, @serverToRun			[nvarchar](512)
		, @eventMessageData		[nvarchar](max)
		, @runStartTime			[datetime]
		, @replicationDelay		[varchar](10)

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
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get value for critical alert threshold
DECLARE   @alertThresholdCriticalReplicationLatencySec [int]
		, @alertThresholdWarningReplicationLatencySec [int] 
		

SELECT	@alertThresholdCriticalReplicationLatencySec = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Replication Latency'
		AND [category] = 'replication'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalReplicationLatencySec = ISNULL(@alertThresholdCriticalReplicationLatencySec, 20)


SELECT	@alertThresholdWarningReplicationLatencySec = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Replication Latency'
		AND [category] = 'replication'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningReplicationLatencySec = ISNULL(@alertThresholdWarningReplicationLatencySec, 15)

SET @replicationDelay = REPLICATE('0', 2 - LEN(CAST(@alertThresholdCriticalReplicationLatencySec / 3600 AS [varchar]))) + CAST(@alertThresholdCriticalReplicationLatencySec / 3600 AS [varchar]) + ':' + 
						REPLICATE('0', 2 - LEN(CAST((@alertThresholdCriticalReplicationLatencySec % 3600) / 60 AS [varchar]))) + CAST((@alertThresholdCriticalReplicationLatencySec % 3600) / 60 AS [varchar]) + ':' + 
						REPLICATE('0', 2 - LEN(CAST((@alertThresholdCriticalReplicationLatencySec % 60) AS [varchar]))) + CAST((@alertThresholdCriticalReplicationLatencySec % 60) AS [varchar])

------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Delete existing information....', 10, 1) WITH NOWAIT

DELETE srl
FROM [monitoring].[statsReplicationLatency]		srl
WHERE srl.[project_id] = @projectID
	AND [publisher_server] LIKE @sqlServerNameFilter


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Get Publications & Subscriptions Information....', 10, 1) WITH NOWAIT
SET @runStartTime = GETUTCDATE()

--replication distribution servers
DECLARE crsReplicationDistributorServers CURSOR FAST_FORWARD READ_ONLY FOR	SELECT [instance_name]
																			FROM [dbo].[vw_catalogDatabaseNames] 
																			WHERE [project_id] = @projectID
																					AND [active] = 1
																					AND [database_name] = 'distribution'
																					AND [instance_name] LIKE @sqlServerNameFilter
OPEN crsReplicationDistributorServers
FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		--publications and subscriptions
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
							SELECT   ' + CAST(@projectID AS [nvarchar]) + N' AS [project_id]
									, @@SERVERNAME		AS [distributor_server]
									, p.[publication]	AS [publication_name]
									, p.[publication_type]
									, srv.[srvname]		AS [publisher_server]
									, p.[publisher_db]
									, ss.[srvname]		AS [subscriber_server]
									, s.[subscriber_db] 
									, s.[status]		AS [subscription_status]
									, s.[subscription_type]
									, COUNT(DISTINCT s.[article_id]) AS [subscription_articles]
							FROM distribution..MSpublications p 
							JOIN distribution..MSsubscriptions s ON p.[publication_id] = s.[publication_id] 
							JOIN master..sysservers ss ON s.[subscriber_id] = ss.[srvid]
							JOIN master..sysservers srv ON srv.[srvid] = p.[publisher_id]
							JOIN distribution..MSdistribution_agents da ON da.[publisher_id] = p.[publisher_id] AND da.[subscriber_id] = s.[subscriber_id] 
							GROUP BY p.[publication]
									, srv.[srvname]
									, p.[publisher_db]
									, ss.[srvname]
									, s.[subscriber_db] 
									, s.[status]
									, p.[publication_type]
									, s.[subscription_type]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO [monitoring].[statsReplicationLatency]([project_id], [distributor_server], [publication_name], [publication_type], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_status], [subscription_type], [subscription_articles])
				EXEC (@queryToRun)

		FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
	end
CLOSE crsReplicationDistributorServers
DEALLOCATE crsReplicationDistributorServers

		
------------------------------------------------------------------------------------------------------------------------------------------
--generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.....', 10, 1) WITH NOWAIT

DECLARE   @publicationName		[sysname]
		, @publicationServer	[sysname]
		, @publisherDB			[sysname]
		, @subcriptionServer	[sysname]
		, @subscriptionDB		[sysname]
		, @distributorServer	[sysname]
		, @subscriptionArticles	[int]

DECLARE crsInactiveSubscriptions CURSOR FAST_FORWARD READ_ONLY FOR	SELECT [publication_name], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_articles], [distributor_server]
																	FROM [monitoring].[statsReplicationLatency]
																	WHERE [subscription_status] = 0
OPEN crsInactiveSubscriptions
FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
WHILE @@FETCH_STATUS=0
	begin
		SET @queryToRun = 'Publication: [' + @publicationName + '] / Subscriber [' + @subcriptionServer + '].[' + @subscriptionDB + '] / Publisher: [' + @publicationServer + '].[' + @publisherDB + '] / Distributor: [' + @distributorServer + '] / Articles: ' + CAST(@subscriptionArticles as [nvarchar])
		RAISERROR(@queryToRun, 10, 1) WITH NOWAIT

		SET @eventMessageData = '<alert><detail>' + 
								'<error_code>21074</error_code>' + 
								'<error_string>The subscription(s) have been marked inactive and must be reinitialized.</error_string>' + 
								'<query_executed>' + @queryToRun + '</query_executed>' + 
								'<duration_seconds>' + CAST(ISNULL(DATEDIFF(ss, @runStartTime, GETUTCDATE()), 0) AS [nvarchar]) + '</duration_seconds>' + 
								'<event_date_utc>' + CONVERT([varchar](20), GETUTCDATE(), 120) + '</event_date_utc>' + 
								'</detail></alert>'

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @publicationServer,
														@dbName					= @publicationName,
														@objectName				= @subcriptionServer,
														@childObjectName		= @subscriptionDB,
														@module					= 'monitoring',
														@eventName				= 'subscription marked inactive',
														@parameters				= NULL,			/* may contain the attach file name */
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 1,	
														@additionalOption		= 0

		FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
	end
CLOSE crsInactiveSubscriptions
DEALLOCATE crsInactiveSubscriptions


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Deploy temporary objects for Replication Latency analysis...', 10, 1) WITH NOWAIT

DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publisher_server]
																FROM [monitoring].[statsReplicationLatency]
																WHERE [subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	running on server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_ReplicationGetPublicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_ReplicationGetPublicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'			
			CREATE PROCEDURE dbo.usp_ReplicationGetPublicationLatency
				  @publisherDB			[sysname]
				, @publicationName		[sysname]
				, @replicationDelay		[varchar](10) = N''00:00:15''
				, @iterations			[int] = 1
				, @iterationDelay		[varchar](10) = N''00:00:05''
			AS
			/*
				original code source:
				Name:       dba_replicationLatencyGet_sp
				Author:     Michelle F. Ufford
			*/
			SET NOCOUNT ON
			DECLARE   @currentIteration [int]
					, @tokenID			[bigint]
					, @currentDateTime	[smalldatetime]
					, @queryToRun		[nvarchar](4000)
					, @queryParam		[nvarchar](512)

			IF NOT EXISTS(SELECT * FROM sysobjects WHERE [name]=''replicationTokenResults'' AND [type]=''U'')
				CREATE TABLE [dbo].[replicationTokenResults]
					(
						  [publisher_db]		[sysname] NULL
						, [publication]			[sysname] NULL
						, [iteration]			[int] NULL
						, [tracer_id]			[int] NULL
						, [distributor_latency]	[int] NULL
						, [subscriber]			[sysname] NULL
						, [subscriber_db]		[sysname] NULL
						, [subscriber_latency]	[int] NULL
						, [overall_latency]		[int] NULL
					)
			ELSE
				DELETE FROM [dbo].[replicationTokenResults] WHERE [publication] = @publicationName AND [publisher_db] = @publisherDB

			DECLARE @temptokenresult TABLE 
				(
					  [tracer_id]			[int] NULL
					, [distributor_latency] [int] NULL
					, [subscriber]			[sysname] NULL
					, [subscriber_db]		[sysname] NULL
					, [subscriber_latency]	[int] NULL
					, [overall_latency]		[int] NULL
				);

			SET @currentIteration = 0
			SET @currentDateTime  = GETDATE()

			WHILE @currentIteration < @iterations
				begin
					/* Insert a new tracer token in the publication database */
					SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_postTracerToken @publication = @publicationName, @tracer_token_id = @tokenID OUTPUT''
					SET @queryParam = N''@publicationName [sysname], @tokenID [bigint] OUTPUT''
					
					PRINT @queryToRun
					EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																, @tokenID = @tokenID OUTPUT

					/* Give a few seconds to allow the record to reach the subscriber */
					WAITFOR DELAY @replicationDelay

					SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_helpTracerTokenHistory @publicationName, @tokenID'' 
					PRINT @queryToRun

					/* Store our results in a temp table for retrieval later */
					INSERT	INTO @temptokenResult ([distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
							EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																		, @tokenID = @tokenID

					INSERT	[dbo].[replicationTokenResults] ([publisher_db], [publication], [distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
							SELECT    @publisherDB
									, @publicationName
									, distributor_latency
									, subscriber
									, subscriber_db
									, subscriber_latency
									, overall_latency
							FROM @temptokenResult

					/* Assign the iteration and token id to the results for easier investigation */
					UPDATE [dbo].[replicationTokenResults]
					SET   [iteration] = @currentIteration + 1
						, [tracer_id] = @tokenID
					WHERE [iteration] IS NULL;

					DELETE FROM @temptokenresult

					/* Wait for the specified time period before creating another token */
					WAITFOR DELAY @iterationDelay

					SET @currentIteration = @currentIteration + 1;
				end;

			/* perform cleanup */
			SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_deleteTracerTokenHistory @publication = @publicationName, @cutoff_date = @currentDateTime''
			SET @queryParam = N''@publicationName [sysname], @currentDateTime [datetime]''
			PRINT @queryToRun

			EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
														, @currentDateTime = @currentDateTime

			/* SELECT * FROM [dbo].[replicationTokenResults] */'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		FETCH NEXT FROM crsActivePublications INTO @publicationServer
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate Replication Latency check internal jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE   @publisherInstanceID	[int]
		, @currentInstanceID	[int]

SELECT	@currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[project_id] = @projectID
		AND cin.[name] = @@SERVERNAME

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'usp_monReplicationPublicationLatency'

DECLARE crsActivePublishers	CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT cin.[id], [publisher_server]
																FROM	[monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[catalogInstanceNames] cin ON srl.[publisher_server] = cin.[name]
																WHERE	[subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
																		AND cin.[active] = 1
																		AND cin.[project_id] = @projectID
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publication_name], [publisher_db]
																		FROM	[monitoring].[statsReplicationLatency] srl																		
																		WHERE	[subscription_status] <> 0
																				AND srl.[publisher_server] = @publicationServer
																				AND [publication_type] = 0 /* only transactional publications */
																			
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'EXEC [' + @publicationServer + '].tempdb.dbo.usp_ReplicationGetPublicationLatency @publisherDB = ''' + @publisherDB + N''', @publicationName = ''' + @publicationName + N''', @replicationDelay = ''' + @replicationDelay + N''', @iterations = ' + CAST(@iterations  AS [nvarchar]) + N', @iterationDelay = ''' + @iterationDelay + N''';'

				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monReplicationPublicationLatency', @publicationName + ' - ' + @publisherDB, @publisherInstanceID,
								'dbaTDPMon - usp_monReplicationPublicationLatency(1) - ' + REPLACE(@publicationServer, '\', '_') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Run Analysis', 'tempdb', @queryToRun
				
				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Running jobs to compute replication latency..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monReplicationPublicationLatency',
								@waitForDelay		= '00:00:10',
								@debugMode			= @debugMode

UPDATE srl
	SET srl.[state] = 1	/* analysis job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monReplicationPublicationLatency(1) - ' + REPLACE(srl.[publisher_server], '\', '_') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Run Analysis'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'usp_monReplicationPublicationLatency'
		AND jeq.[status] = 1 /* succedded */

------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate Replication Latency getdata internal jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE crsActivePublishers	CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT cin.[id], [publisher_server]
																FROM	[monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[catalogInstanceNames] cin ON srl.[publisher_server] = cin.[name]
																WHERE	[subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
																		AND cin.[active] = 1
																		AND cin.[project_id] = @projectID
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		
		DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publication_name], [publisher_db]
																		FROM	[monitoring].[statsReplicationLatency] srl																		
																		WHERE	[subscription_status] <> 0
																				AND srl.[publisher_server] = @publicationServer
																				AND [publication_type] = 0 /* only transactional publications */
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT * FROM tempdb.[dbo].[replicationTokenResults] WHERE [publication]=''' + @publicationName + N''' AND [publisher_db] = ''' + @publisherDB + N''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@publicationServer, @queryToRun)

				SET @queryToRun = 'UPDATE srl
										SET   srl.[distributor_latency] = x.[distributor_latency]
											, srl.[subscriber_latency] = x.[subscriber_latency]
											, srl.[overall_latency] = x.[overall_latency]
											, srl.[event_date_utc] = GETUTCDATE()
									FROM [monitoring].[statsReplicationLatency] srl
									INNER JOIN (
													SELECT    [publisher_db], [publication], [subscriber], [subscriber_db]
															, MAX(ISNULL([distributor_latency],  2147483647))	AS [distributor_latency]
															, MAX(ISNULL([subscriber_latency],  2147483647))	AS [subscriber_latency]
															, MAX(ISNULL([overall_latency],  2147483647))		AS [overall_latency]
													FROM (' + @queryToRun + ')y
													GROUP BY [publisher_db], [publication], [subscriber], [subscriber_db]
												)x ON	srl.[publisher_db] = x.[publisher_db] 
													and srl.[publication_name] = x.[publication] 
													AND srl.[subscriber_server] = x.[subscriber] 
													AND srl.[subscriber_db] = x.[subscriber_db]
													AND srl.[publisher_server] = ''' + @publicationServer + N''''
		
				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monReplicationPublicationLatency', @publicationName + ' - ' + @publisherDB , @publisherInstanceID,
								'dbaTDPMon - usp_monReplicationPublicationLatency(2) - ' + REPLACE(@publicationServer, '\', '_') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Get Latency', DB_NAME(), @queryToRun

				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Running GetData jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monReplicationPublicationLatency',
								@waitForDelay		= '00:00:10',
								@debugMode			= @debugMode

UPDATE [monitoring].[statsReplicationLatency] SET [distributor_latency] = NULL	WHERE [distributor_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [subscriber_latency] = NULL	WHERE [subscriber_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [overall_latency] = NULL		WHERE [overall_latency] = 2147483647

UPDATE srl
	SET srl.[state] = 2	/* getdate job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monReplicationPublicationLatency(2) - ' + REPLACE(srl.[publisher_server], '\', '_') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Get Latency'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'usp_monReplicationPublicationLatency'
		AND jeq.[status] = 1 /* succedded */


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Perform cleanup...', 10, 1) WITH NOWAIT

DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publisher_server]
																FROM	[monitoring].[statsReplicationLatency]
																WHERE	[subscription_status] <> 0
																		AND [publication_type] = 0 /* only transactional publications */
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	running on server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_ReplicationGetPublicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_ReplicationGetPublicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''replicationTokenResults'' AND [type]=''U'')
				DROP TABLE dbo.replicationTokenResults'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		FETCH NEXT FROM crsActivePublications INTO @publicationServer
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
--generate alerts: Replication latency exceeds thresold
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--generate alerts: Replication latency exceeds thresold...', 10, 1) WITH NOWAIT

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsReplicationAlarms CURSOR FOR	SELECT  DISTINCT
												  srl.[publisher_server] AS [instance_name]
												, 'Publication: ' + srl.[publication_name] + ' - Subscriber:' + srl.[subscriber_server] + '.' + srl.[subscriber_db] AS [object_name]
												, 'critical'			AS [severity]
												, 'replication latency'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>critical</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>replication latency</counter_name>
													<target_name>Publication: ' + srl.[publication_name] + ' / Subscriber: [' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + '] / Publisher: [' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '] / Distributor: [' + srl.[distributor_server] + ']</target_name>' + 
													'<measure_unit>sec</measure_unit>' + 
													'<current_value>' + ISNULL(CAST(srl.[overall_latency] AS [nvarchar]), '-1') +'</current_value>' + 
													'<threshold_value>' + CAST(@alertThresholdCriticalReplicationLatencySec AS [varchar]) + '</threshold_value>' + 
													'<event_date_utc>' + CONVERT([varchar](20), srl.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [monitoring].[statsReplicationLatency] srl ON srl.[project_id] = cin.[project_id] AND srl.[publisher_server] = cin.[instance_name]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication latency'
																						AND asr.[alert_name] IN ('')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																						AND (   asr.[skip_value2] IS NULL 
																							 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = (srl.[subscriber_server] + '.' + srl.[subscriber_db]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerNameFilter
												AND (srl.[overall_latency] IS NULL OR srl.[overall_latency]>=@alertThresholdCriticalReplicationLatencySec)									
												AND srl.[subscription_status] <> 0
												AND srl.[state] = 2 /* run analysis and get data jobs completed successfully */
										ORDER BY [instance_name], [object_name]
OPEN crsReplicationAlarms
FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsReplicationAlarms
DEALLOCATE crsReplicationAlarms
GO

USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
