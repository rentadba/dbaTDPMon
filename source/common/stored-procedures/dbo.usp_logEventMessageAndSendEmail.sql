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

DECLARE @projectID						[smallint],
		@instanceID						[smallint],		
		@alertFrequency					[int],
		@alertSent						[int],
		@maxAlertCountPer5Min			[int],
		@isEmailSent					[bit],
		@isFloodControl					[bit],
		@HTMLBody						[nvarchar](max),
		@emailSubject					[nvarchar](256),
		@ReturnValue					[int],
		@clientName						[nvarchar](260),
		@eventData						[varchar](8000),
		@ignoreAlertsForError1222		[bit],
		@ignoreAlertsForError15281		[bit],
		@ignoreAlertsForError1927		[bit],
		@ignoreAlertsForAgentsJobLimit	[bit],
		@errorCode						[int],
		@eventMessageXML				[xml]
		

DECLARE   @handle				[int]
		, @PrepareXmlStatus		[int]

SET @ReturnValue=1

-----------------------------------------------------------------------------------------------------
-- try to get project code by database name / or get the default project value
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](@sqlServerName, @dbName)

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
--flood control: get the maximum allowed number of alerts to be sent in a 5 minutes interval, default 50
-----------------------------------------------------------------------------------------------------
SELECT	@maxAlertCountPer5Min = [value]
FROM	[dbo].[appConfigurations]
WHERE	[name]='Flood control: maximum alerts in 5 minutes'
		AND [module] = 'common'

SELECT @maxAlertCountPer5Min = ISNULL(@maxAlertCountPer5Min, 50)

-----------------------------------------------------------------------------------------------------
--check what alerts can be ignored
-----------------------------------------------------------------------------------------------------
SELECT	@ignoreAlertsForError1222 = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Ignore alerts for: Error 1222 - Lock request time out period exceeded'
		AND [module] = 'common'

SET @ignoreAlertsForError1222 = ISNULL(@ignoreAlertsForError1222, 0)

-----------------------------------------------------------------------------------------------------
SELECT	@ignoreAlertsForError15281 = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Ignore alerts for: Error 15281 - SQL Server blocked access to procedure'
		AND [module] = 'maintenance-plan'

SET @ignoreAlertsForError15281 = ISNULL(@ignoreAlertsForError15281, 0)

-----------------------------------------------------------------------------------------------------
SELECT	@ignoreAlertsForError1927 = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Ignore alerts for: Error 1927 - There are already statistics on table'
		AND [module] = 'maintenance-plan'

SET @ignoreAlertsForError1927 = ISNULL(@ignoreAlertsForError1927, 0)

-----------------------------------------------------------------------------------------------------
SELECT	@ignoreAlertsForAgentsJobLimit = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Ignore alerts for: Maximum SQL Agent jobs running limit reached'
		AND [module] = 'common'

SET @ignoreAlertsForAgentsJobLimit = ISNULL(@ignoreAlertsForAgentsJobLimit, 0)


-----------------------------------------------------------------------------------------------------
--check if alert should be sent
-----------------------------------------------------------------------------------------------------
--flood control: alerts sent in the last 5 minutes
SELECT @alertSent=COUNT(*)
FROM [dbo].[logEventMessages]
WHERE	[instance_id] = @instanceID
		AND [project_id] = @projectID
		AND DATEDIFF(mi, [event_date_utc], GETUTCDATE()) BETWEEN 0 AND 5
		AND [is_email_sent] = 1
		AND @eventType IN (1, 6)

IF @alertSent < @maxAlertCountPer5Min
	begin
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
	end

-----------------------------------------------------------------------------------------------------
--get notification status
-----------------------------------------------------------------------------------------------------
SET @isFloodControl	= 0
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
	
IF     (@eventType IN (1) AND ((@ignoreAlertsForError1222=1 AND @errorCode=1222) or (@ignoreAlertsForError15281=1 and @errorCode=15281) or (@ignoreAlertsForError1927=1 and @errorCode=1927)))
	OR (@eventType IN (6) AND ((@ignoreAlertsForAgentsJobLimit=1 AND @eventName = 'job queue execute' AND @childObjectName='dbo.usp_jobQueueExecute')))
	begin
		SET @alertSent=1
		SET @isFloodControl=1
		SET @recipientsList=NULL
	end

-----------------------------------------------------------------------------------------------------
--processing the xml message
-----------------------------------------------------------------------------------------------------
SET @eventMessageXML = CAST(@eventMessage AS [xml])
SET @HTMLBody = N''

-----------------------------------------------------------------------------------------------------
--alert details
IF @eventMessageXML IS NOT NULL AND (@alertSent=0 AND @recipientsList IS NOT NULL AND @dbMailProfileName IS NOT NULL)
	begin
		IF @eventType=1
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
		IF @eventType=6
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
		IF @eventType IN (2, 5)
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
		IF @eventType=3
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
		IF @eventType IN (5)
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
	end
SET @HTMLBody = [dbo].[ufn_getObjectQuoteName](@HTMLBody, 'undo-xml')

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
SET @isEmailSent	= 0 

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
							+ CASE WHEN @dbName IS NOT NULL THEN [dbo].[ufn_getObjectQuoteName](@dbName, 'quoted') + N' - ' ELSE N'' END 
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

SET @eventData = SUBSTRING(@eventMessage, 1, 8000)
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
