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
												li = 'duration: ' + CAST([duration_seconds] AS [nvarchar](32)) + ' seconds', ''
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
																[target_name]				[sysname],
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
									<TH>Run Duration</TH>' +
								CAST ( ( 
										SELECT	TD = [step_id], '',
												TD = [step_name], '',
												TD = [run_status], '',
												TD = [run_date], '',
												TD = [run_time], '',
												TD = [duration], ''
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
					SELECT CONVERT([datetime], ([run_date] + ' ' + [run_time]), 120) AS [job_step_start_time]
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

		INSERT	INTO @xmlBackupSet([database_name], [type], [start_date], [duration], [size], [size_bytes], [verified], [file_name], [error_code])
				SELECT [database_name], [type], [start_date], [duration], [size], [size_bytes], [verified], [file_name], [error_code]
				FROM (
						SELECT	  ref.value ('database_name[1]', 'sysname') as [database_name]
								, ref.value ('type[1]', 'nvarchar(32)') as [type]
								, ref.value ('start_date[1]', 'datetime') as [start_date]
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

		--if any of the backups had failed, send notification
		IF @additionalOption=0
			SELECT @additionalOption = COUNT(*)
			FROM @xmlBackupSet
			WHERE [error_code]<>0
	end

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
