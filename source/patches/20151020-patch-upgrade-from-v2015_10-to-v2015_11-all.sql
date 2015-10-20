USE [dbaTDPMon]
GO

UPDATE [dbo].[appConfigurations] SET [value] = N'2015.10.20' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Fail master job if any queued job fails' AND [module] = 'common')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
			SELECT 'common'		AS [module], 'Fail master job if any queued job fails'	AS [name], 'false' AS [value]
GO

UPDATE [dbo].[appConfigurations] SET [module]='common'
WHERE	[name] IN ('Parallel Data Collecting Jobs', 'Maximum number of retries at failed job', 'Fail master job if any queued job fails')
		AND [module] = 'health-check'
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
												li = 'machine-name:' + [machine_name], '',
												li = 'measure-unit:' + [measure_unit], '',
												li = 'current-value:' + CAST([current_value] AS [varchar](32)), '',
												li = 'current-percentage:' + CAST([current_percentage] AS [varchar](32)), '',
												li = 'refference-value:' + CAST([refference_value] AS [varchar](32)), '',
												li = 'refference-percentage:' + CAST([refference_percentage] AS [varchar](32)), '',
												li = 'threshold-value: ' + CAST([threshold_value] AS [varchar](32)), '',
												li = 'threshold-percentage: ' + CAST([threshold_percentage] AS [varchar](32)), '',
												li = 'severity:' + [severity], '',
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

RAISERROR('Create procedure: [dbo].[usp_jobQueueExecute]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_jobQueueExecute]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_jobQueueExecute]
GO

CREATE PROCEDURE dbo.usp_jobQueueExecute
		@projectCode			[varchar](32) = NULL,
		@moduleFilter			[varchar](32) = '%',
		@descriptorFilter		[varchar](256)= '%',
		@waitForDelay			[varchar](8) = '00:00:30',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @projectID				[smallint]
		, @jobName					[sysname]
		, @jobStepName				[sysname]
		, @jobDBName				[sysname]
		, @sqlServerName			[sysname]
		, @jobCommand				[nvarchar](max)
		, @logFileLocation			[nvarchar](512)
		, @jobQueueID				[int]

		, @configParallelJobs		[smallint]
		, @configMaxNumberOfRetries	[smallint]
		, @configFailMasterJob		[bit]
		, @runningJobs				[smallint]
		, @executedJobs				[smallint]
		, @jobQueueCount			[smallint]

		, @strMessage				[varchar](8000)	
		, @currentRunning			[int]
		, @lastExecutionStatus		[int]
		, @lastExecutionDate		[varchar](10)
		, @lastExecutionTime 		[varchar](8)
		, @runningTimeSec			[bigint]


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
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end
	
------------------------------------------------------------------------------------------------------------------------------------------
--check if parallel collector is enabled
BEGIN TRY
	SELECT	@configParallelJobs = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Parallel Data Collecting Jobs'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configParallelJobs = 1
END CATCH

SET @configParallelJobs = ISNULL(@configParallelJobs, 1)


------------------------------------------------------------------------------------------------------------------------------------------
--get the number of retries in case of a failure
BEGIN TRY
	SELECT	@configMaxNumberOfRetries = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Maximum number of retries at failed job'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configMaxNumberOfRetries = 3
END CATCH

SET @configMaxNumberOfRetries = ISNULL(@configMaxNumberOfRetries, 3)


------------------------------------------------------------------------------------------------------------------------------------------
--get the number of retries in case of a failure
BEGIN TRY
	SELECT	@configFailMasterJob = CASE WHEN lower([value])='true' THEN 1 ELSE 0 END
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Fail master job if any queued job fails'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configFailMasterJob = 0
END CATCH

SET @configFailMasterJob = ISNULL(@configFailMasterJob, 0)

------------------------------------------------------------------------------------------------------------------------------------------
SELECT @jobQueueCount = COUNT(*)
FROM [dbo].[vw_jobExecutionQueue]
WHERE  [project_id] = @projectID 
		AND [module] LIKE @moduleFilter
		AND [descriptor] LIKE @descriptorFilter
		AND [status]=-1


SET @strMessage='Number of jobs in the queue to be executed : ' + CAST(@jobQueueCount AS [varchar]) 
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

SET @runningJobs  = 0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsJobQueue CURSOR FOR	SELECT  [id], [instance_name]
										, [job_name], [job_step_name], [job_database_name], [job_command]
								FROM [dbo].[vw_jobExecutionQueue]
								WHERE  [project_id] = @projectID 
										AND [module] LIKE @moduleFilter
										AND [descriptor] LIKE @descriptorFilter
										AND [status]=-1
								ORDER BY [id]
OPEN crsJobQueue
FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand
SET @executedJobs = 1
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Executing job# : ' + CAST(@executedJobs AS [varchar]) + ' / ' + CAST(@jobQueueCount AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		SET @strMessage='Create SQL Agent job : "' + @jobName + '"'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

		---------------------------------------------------------------------------------------------------
		/* setting the job name & job log location */
		SELECT @logFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
		FROM (
				SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
			)er

		IF @logFileLocation IS NULL SET @logFileLocation =N'C:\'
		SET @logFileLocation = @logFileLocation + N'job-' + @jobName + N'.log'

		SET @jobCommand = REPLACE(@jobCommand, '''', '''''')
		---------------------------------------------------------------------------------------------------
		/* defining job and start it */
		EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
										@jobName		= @jobName,
										@operation		= 'Clean',
										@dbName			= @jobDBName, 
										@jobStepName 	= @jobStepName,
										@debugMode		= @debugMode

		EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
										@jobName		= @jobName,
										@operation		= 'Add',
										@dbName			= @jobDBName, 
										@jobStepName 	= @jobStepName,
										@jobStepCommand	= @jobCommand,
										@jobLogFileName	= @logFileLocation,
										@jobStepRetries = @configMaxNumberOfRetries,
										@debugMode		= @debugMode

		---------------------------------------------------------------------------------------------------
		/* starting job */
		EXEC dbo.usp_sqlAgentJobStartAndWatch	@sqlServerName						= @sqlServerName,
												@jobName							= @jobName,
												@stepToStart						= 1,
												@stepToStop							= 1,
												@waitForDelay						= @waitForDelay,
												@dontRunIfLastExecutionSuccededLast	= 0,
												@startJobIfPrevisiousErrorOcured	= 1,
												@watchJob							= 0,
												@debugMode							= @debugMode
		
		/* mark job as running */
		UPDATE [dbo].[jobExecutionQueue] SET [status]=4 WHERE [id] = @jobQueueID	
		SET @runningJobs = @runningJobs + 1

		SET @runningJobs = @executedJobs
		EXEC @runningJobs = dbo.usp_jobQueueGetStatus	@projectCode			= @projectCode,
														@moduleFilter			= @moduleFilter,
														@descriptorFilter		= @descriptorFilter,
														@waitForDelay			= @waitForDelay,
														@minJobToRunBeforeExit	= @configParallelJobs,
														@executionLevel			= 1,
														@debugMode				= @debugMode
		---------------------------------------------------------------------------------------------------
		IF @runningJobs < @jobQueueCount
			begin
				FETCH NEXT FROM crsJobQueue INTO @jobQueueID, @sqlServerName, @jobName, @jobStepName, @jobDBName, @jobCommand
				SET @executedJobs = @executedJobs + 1
			end
	end
CLOSE crsJobQueue
DEALLOCATE crsJobQueue

EXEC dbo.usp_jobQueueGetStatus	@projectCode			= @projectCode,
								@moduleFilter			= @moduleFilter,
								@descriptorFilter		= @descriptorFilter,
								@waitForDelay			= @waitForDelay,
								@minJobToRunBeforeExit	= 0,
								@executionLevel			= 1,
								@debugMode				= @debugMode

IF @configFailMasterJob=1 
	AND EXISTS(	SELECT *
			FROM [dbo].[vw_jobExecutionQueue]
			WHERE  [project_id] = @projectID 
					AND [module] LIKE @moduleFilter
					AND [descriptor] LIKE @descriptorFilter
					AND [status]=0 /* failed */
			)
		EXEC [dbo].[usp_logPrintMessage]	@customMessage		= 'Execution failed. Check log for internal job failures (dbo.vw_jobExecutionQueue).',
											@raiseErrorAsPrint	= 1,
											@messagRootLevel	= 0,
											@messageTreelevel	= 1,
											@stopExecution		= 1
GO

RAISERROR('Create procedure: [dbo].[usp_hcJobQueueCreate]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcJobQueueCreate]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcJobQueueCreate]
GO

CREATE PROCEDURE [dbo].[usp_hcJobQueueCreate]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@collectorDescriptor	[varchar](256)='%',
		@enableXPCMDSHELL		[bit]=1,
		@debugMode				[bit]=0

/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 21.09.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
SET NOCOUNT ON

DECLARE   @codeDescriptor		[varchar](260)
		, @strMessage			[varchar](1024)
		, @projectID			[smallint]
		, @instanceID			[smallint]
		, @configParallelJobs	[int]

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
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end

------------------------------------------------------------------------------------------------------------------------------------------
--check if parallel collector is enabled
BEGIN TRY
	SELECT	@configParallelJobs = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Parallel Data Collecting Jobs'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @configParallelJobs = 1
END CATCH

SET @configParallelJobs = ISNULL(@configParallelJobs, 1)

------------------------------------------------------------------------------------------------------------------------------------------
SELECT @instanceID = [id]
FROM [dbo].[catalogInstanceNames]
WHERE [project_id] = @projectID
		AND [name] = @@SERVERNAME

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE crsCollectorDescriptior CURSOR READ_ONLY FAST_FORWARD FOR	SELECT [descriptor]
																	FROM
																		(
																			SELECT 'dbo.usp_hcCollectDatabaseDetails' AS [descriptor] UNION ALL
																			SELECT 'dbo.usp_hcCollectSQLServerAgentJobsStatus' AS [descriptor] UNION ALL
																			SELECT 'dbo.usp_hcCollectDiskSpaceUsage' AS [descriptor] UNION ALL
																			SELECT 'dbo.usp_hcCollectErrorlogMessages' AS [descriptor] UNION ALL
																			SELECT 'dbo.usp_hcCollectOSEventLogs' AS [descriptor] UNION ALL
																			SELECT 'dbo.usp_hcCollectEventMessages' AS [descriptor]
																		)X
																	WHERE [descriptor] LIKE @collectorDescriptor
OPEN crsCollectorDescriptior
FETCH NEXT FROM crsCollectorDescriptior INTO @codeDescriptor
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Generating queue for : ' + @codeDescriptor
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

		DELETE FROM [dbo].[jobExecutionQueue]
		WHERE [project_id] = @projectID
				AND [instance_id] = @instanceID
				AND [descriptor] = @codeDescriptor
				AND [module] = 'health-check'

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDatabaseDetails'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], 'health-check' AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectDatabaseDetails' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectDatabaseDetails] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @databaseNameFilter = ''%'', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end
			
		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectSQLServerAgentJobsStatus'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], 'health-check' AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectSQLServerAgentJobsStatus' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectSQLServerAgentJobsStatus] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @jobNameFilter = ''%'', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDiskSpaceUsage'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], 'health-check' AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectDiskSpaceUsage' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectDiskSpaceUsage] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @enableXPCMDSHELL = ' + CAST(@enableXPCMDSHELL AS [varchar]) + ', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end



		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectErrorlogMessages'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], 'health-check' AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectErrorlogMessages' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectErrorlogMessages] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectEventMessages'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], 'health-check' AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectEventMessages' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectEventMessages] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										AND cin.[instance_name] <> @@SERVERNAME
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X
			end
		
		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectOSEventLogs'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor], [filter]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], 'health-check' AS [module], @codeDescriptor AS [descriptor], L.[log_type_name],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'hcCollectOSEventLogs' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END  + ' (' + L.[log_type_name] + ')' AS [job_name],
								'Run Collect'	AS [job_step_name],
								DB_NAME()		AS [job_database_name],
								'EXEC [dbo].[usp_hcCollectOSEventLogs] @projectCode = ''' + @projectCode + ''', @sqlServerNameFilter = ''' + X.[instance_name] + ''', @logNameFilter = ''' + L.[log_type_name] + ''', @enableXPCMDSHELL = ' + CAST(@enableXPCMDSHELL AS [varchar]) + ', @debugMode = ' + CAST(@debugMode AS [varchar])
						FROM
							(
								SELECT	DISTINCT cin.[instance_id], cin.[instance_name]
								FROM	[dbo].[vw_catalogInstanceNames] cin
								WHERE 	cin.[project_id] = @projectID
										AND cin.[instance_active]=1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
										--AND cin.[instance_name] <> @@SERVERNAME
										AND @configParallelJobs <> 1
								
								UNION ALL

								SELECT @instanceID AS [instance_id], '%' AS [instance_name]
								WHERE @configParallelJobs = 1
							)X,
							(
								SELECT 'Application' AS [log_type_name], 1 AS [log_type_id] UNION ALL
								SELECT 'System'		 AS [log_type_name], 2 AS [log_type_id] UNION ALL
								SELECT 'Setup'		 AS [log_type_name], 3 AS [log_type_id] 
							)L

				--cleaning machine names with multi-instance; keep only one instance, since machine logs will be fetched
				DELETE jeq1
				FROM [dbo].[jobExecutionQueue] jeq1
				INNER JOIN 
					(
						SELECT jeq.[id], ROW_NUMBER() OVER(PARTITION BY cin.[machine_id], jeq.[filter] ORDER BY cin.[instance_id]) AS row_no
						FROM [dbo].[jobExecutionQueue] jeq
						INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = jeq.[project_id] 
																		AND cin.[instance_id] = jeq.[for_instance_id]
						INNER JOIN
							(
								SELECT cin.[machine_id], cin.[machine_name], jeq.[filter], COUNT(*) AS cnt
								FROM [dbo].[jobExecutionQueue] jeq
								INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = jeq.[project_id] 
																				AND cin.[instance_id] = jeq.[for_instance_id]
								WHERE	jeq.[descriptor]=@codeDescriptor
										AND jeq.[instance_id] = @instanceID
										AND jeq.[status]=-1
										AND cin.[project_id] = @projectID
										AND cin.[instance_active] = 1
										AND cin.[instance_name] LIKE @sqlServerNameFilter
								GROUP BY cin.[machine_id], cin.[machine_name], jeq.[filter]		
								HAVING COUNT(*)>1
							)x ON x.[machine_id] = cin.[machine_id] AND x.[machine_name] = cin.[machine_name] AND x.[filter] = jeq.[filter]
						WHERE	jeq.[descriptor]=@codeDescriptor
								AND jeq.[instance_id] = @instanceID
								AND jeq.[status]=-1
								AND cin.[project_id] = @projectID
								AND cin.[instance_active] = 1
								AND cin.[instance_name] LIKE @sqlServerNameFilter
					) y  on jeq1.[id] = y.[id]
				WHERE y.[row_no] <> 1
			end
			
		FETCH NEXT FROM crsCollectorDescriptior INTO @codeDescriptor
	end
CLOSE crsCollectorDescriptior
DEALLOCATE crsCollectorDescriptior
GO

RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomFreeDiskSpace]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomFreeDiskSpace]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomFreeDiskSpace]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomFreeDiskSpace]
		@projectCode		[varchar](32)=NULL,
		@sqlServerName		[sysname]='%'
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

DECLARE	  @projectID						[int]
		, @warningFreeDiskMinPercent		[numeric](6,2)
		, @warningFreeDiskMinSpaceMB		[numeric](18,3)
		, @criticalFreeDiskMinPercent		[numeric](6,2)
		, @criticalFreeDiskMinSpaceMB		[numeric](18,3)
		, @ErrMessage						[nvarchar](256)
		
-----------------------------------------------------------------------------------------------------
--get default project code
IF @projectCode IS NULL
	SELECT	@projectCode = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Default project code'
			AND [module] = 'common'

SELECT    @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @ErrMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@ErrMessage, 16, 1) WITH NOWAIT
	end

-----------------------------------------------------------------------------------------------------
SELECT	@warningFreeDiskMinPercent = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (%)'
		AND [category] = 'disk-space'
		AND [is_warning_limit_enabled]=1

SELECT	@criticalFreeDiskMinPercent = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (%)'
		AND [category] = 'disk-space'
		AND [is_critical_limit_enabled]=1

SELECT	@warningFreeDiskMinSpaceMB = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (MB)'
		AND [category] = 'disk-space'
		AND [is_warning_limit_enabled]=1

SELECT	@criticalFreeDiskMinSpaceMB = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Logical Disk: Free Disk Space (MB)'
		AND [category] = 'disk-space'
		AND [is_critical_limit_enabled]=1

-----------------------------------------------------------------------------------------------------

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsDiskSpaceAlarms CURSOR FOR	SELECT  DISTINCT
												  cin.[instance_name]
												, ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) AS [object_name]
												, 'warning'			AS [severity]
												, 'low disk space'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>warning</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>low disk space</counter_name><target_name>' + ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) + '</target_name>' + 
													'<measure_unit>MB</measure_unit>' + 
													'<current_value>' + CAST(dsi.[available_space_mb] AS [varchar]) +'</current_value>' + 
													CASE WHEN dsi.[percent_available] IS NOT NULL THEN '<current_percentage>' + CAST(dsi.[percent_available] AS [varchar]) + '</current_percentage>' ELSE '' END + 
													CASE WHEN dsi.[total_size_mb] IS NOT NULL	  THEN '<refference_value>' + CAST(dsi.[total_size_mb] AS [varchar]) + '</refference_value>' ELSE '' END + 
													'<threshold_value>' + CAST(@warningFreeDiskMinSpaceMB AS [varchar]) + '</threshold_value>' + 
													'<threshold_percentage>' + CAST(@warningFreeDiskMinPercent AS [varchar]) + '</threshold_percentage>' + 
													'<event_date_utc>' + CONVERT([varchar](20), dsi.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
										LEFT  JOIN 
													(
														SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
														FROM [health-check].[vw_statsDatabaseDetails]
													)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'disk-space'
																						AND asr.[alert_name] IN ('Logical Disk: Free Disk Space (%)', 'Logical Disk: Free Disk Space (MB)')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																						AND (   asr.[skip_value2] IS NULL 
																							 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerName
												AND (    (	  dsi.[percent_available] IS NOT NULL 
															AND dsi.[percent_available] <= @warningFreeDiskMinPercent
															AND dsi.[percent_available] > @criticalFreeDiskMinPercent
															)
														OR 
														(	   dsi.[percent_available] IS NULL 
															AND dsi.[available_space_mb] IS NOT NULL 
															AND dsi.[available_space_mb] <= @warningFreeDiskMinSpaceMB
															AND dsi.[percent_available] > @criticalFreeDiskMinSpaceMB
														)
													)
												AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
												AND asr.[id] IS NULL
												AND (@warningFreeDiskMinSpaceMB IS NOT NULL AND @warningFreeDiskMinPercent IS NOT NULL)

										UNION ALL

										SELECT  DISTINCT
												  cin.[instance_name]
												, ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) AS [object_name]
												, 'critical'			AS [severity]
												, 'low disk space'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>critical</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>low disk space</counter_name><target_name>' + ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]) + '</target_name>' + 
													'<measure_unit>MB</measure_unit>' + 
													'<current_value>' + CAST(dsi.[available_space_mb] AS [varchar]) +'</current_value>' + 
													CASE WHEN dsi.[percent_available] IS NOT NULL THEN '<current_percentage>' + CAST(dsi.[percent_available] AS [varchar]) + '</current_percentage>' ELSE '' END + 
													CASE WHEN dsi.[total_size_mb] IS NOT NULL	  THEN '<refference_value>' + CAST(dsi.[total_size_mb] AS [varchar]) + '</refference_value>' ELSE '' END + 
													'<threshold_value>' + CAST(@criticalFreeDiskMinSpaceMB AS [varchar]) + '</threshold_value>' + 
													'<threshold_percentage>' + CAST(@criticalFreeDiskMinPercent AS [varchar]) + '</threshold_percentage>' + 
													'<event_date_utc>' + CONVERT([varchar](20), dsi.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
										LEFT  JOIN 
													(
														SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
														FROM [health-check].[vw_statsDatabaseDetails]
													)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'disk-space'
																						AND asr.[alert_name] IN ('Logical Disk: Free Disk Space (%)', 'Logical Disk: Free Disk Space (MB)')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																						AND (   asr.[skip_value2] IS NULL 
																							 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ISNULL(dsi.[volume_mount_point], dsi.[logical_drive]))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerName
												AND (    (	  dsi.[percent_available] IS NOT NULL 
															AND dsi.[percent_available] < @criticalFreeDiskMinPercent
															)
														OR 
														(	   dsi.[percent_available] IS NULL 
															AND dsi.[available_space_mb] IS NOT NULL 
															AND dsi.[available_space_mb] < @criticalFreeDiskMinSpaceMB
														)
													)
												AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
												AND asr.[id] IS NULL
												AND (@criticalFreeDiskMinSpaceMB IS NOT NULL AND @criticalFreeDiskMinPercent IS NOT NULL)
										
										ORDER BY [instance_name], [object_name]
OPEN crsDiskSpaceAlarms
FETCH NEXT FROM crsDiskSpaceAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'health-check',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsDiskSpaceAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsDiskSpaceAlarms
DEALLOCATE crsDiskSpaceAlarms
GO
