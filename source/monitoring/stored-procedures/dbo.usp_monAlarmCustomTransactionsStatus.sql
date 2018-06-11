SET QUOTED_IDENTIFIER ON
GO
RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomTransactionsStatus]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomTransactionsStatus]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomTransactionsStatus]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomTransactionsStatus]
		  @projectCode			[varchar](32)=NULL
		, @sqlServerNameFilter	[sysname]='%'
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 12.01.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date	: 
-- Description	: 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON 
		
DECLARE   @sqlServerName		[sysname]
		, @projectID			[smallint]
		, @strMessage			[nvarchar](512)
		, @eventMessageData		[nvarchar](max)
		, @executionLevel		[tinyint]
		, @additionalRecipients	[nvarchar](1024)

SET @executionLevel = 0
------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get value for critical alert threshold
DECLARE   @alertThresholdCriticalUncommitted [int]
		, @alertThresholdWarningUncommitted [int] 
		
SELECT	@alertThresholdCriticalUncommitted = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Uncommitted Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalUncommitted = ISNULL(@alertThresholdCriticalUncommitted, 1800)


SELECT	@alertThresholdWarningUncommitted = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Uncommitted Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningUncommitted = ISNULL(@alertThresholdWarningUncommitted, 900)

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @alertThresholdCriticalRunningTransactions [int]
		, @alertThresholdWarningRunningTransactions [int] 
		
SELECT	@alertThresholdCriticalRunningTransactions = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Running Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalRunningTransactions = ISNULL(@alertThresholdCriticalRunningTransactions, 1800)


SELECT	@alertThresholdWarningRunningTransactions = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Running Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningRunningTransactions = ISNULL(@alertThresholdWarningRunningTransactions, 900)

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @alertThresholdCriticalBlocking [int]
		, @alertThresholdWarningBlocking [int] 
		
SELECT	@alertThresholdCriticalBlocking = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Blocking Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalBlocking = ISNULL(@alertThresholdCriticalBlocking, 900)


SELECT	@alertThresholdWarningBlocking = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Blocking Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningBlocking = ISNULL(@alertThresholdWarningBlocking, 600)

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @alertThresholdCriticalTempdb[int]
		, @alertThresholdWarningTempdb [int] 
		
SELECT	@alertThresholdCriticalTempdb = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'tempdb: space used by a single session'
		AND [category] = 'performance'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalTempdb = ISNULL(@alertThresholdCriticalTempdb, 16384)


SELECT	@alertThresholdWarningTempdb = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'tempdb: space used by a single session'
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningTempdb = ISNULL(@alertThresholdWarningTempdb, 8192)

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @alertThresholdCriticalActiveRequest [int]
		, @alertThresholdWarningActiveRequest [int] 
		
SELECT	@alertThresholdCriticalActiveRequest = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Active Request/Session Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalActiveRequest = ISNULL(@alertThresholdCriticalActiveRequest, 1800)


SELECT	@alertThresholdWarningActiveRequest = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Active Request/Session Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningActiveRequest = ISNULL(@alertThresholdWarningActiveRequest, 900)

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Generate internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @currentInstanceID	[int]

SELECT	TOP 1 @currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[name] = @@SERVERNAME
		--AND cin.[project_id] = @projectID

/* save the previous executions statistics */
EXEC [dbo].[usp_jobExecutionSaveStatistics]	@projectCode		= @projectCode,
											@moduleFilter		= 'monitoring',
											@descriptorFilter	= 'dbo.usp_monAlarmCustomTransactionsStatus'

/* save the execution history */
INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
										 [job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
										 [running_time_sec], [log_message], [status], [event_date_utc], [task_id])
		SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
				[job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
				[running_time_sec], [log_message], [status], [event_date_utc], [task_id]
		FROM [dbo].[jobExecutionQueue]
		WHERE [project_id] = @projectID
				AND [instance_id] = @currentInstanceID
				AND [module] = 'monitoring'
				AND [descriptor] = 'dbo.usp_monAlarmCustomTransactionsStatus'
				AND [status] <> -1
				
DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'dbo.usp_monAlarmCustomTransactionsStatus'


INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [task_id],
										[filter], [for_instance_id],
										[job_name], [job_step_name], [job_database_name], [job_command])
		SELECT	@currentInstanceID, @projectID, 'monitoring', 'dbo.usp_monAlarmCustomTransactionsStatus', 
				(SELECT it.[id] FROM [dbo].[appInternalTasks] it WHERE it.[descriptor] = 'dbo.usp_monAlarmCustomTransactionsStatus'),
				NULL, cin.[id],
				'dbaTDPMon - usp_monAlarmCustomTransactionsStatus - ' + REPLACE(cin.[name], '\', '$'), 'Run Analysis', DB_NAME()
				, N'EXEC dbo.usp_monGetTransactionsStatus @projectCode = ''' + @projectCode + N''', @sqlServerNameFilter = ''' + cin.[name] + N''''
		FROM	[dbo].[catalogInstanceNames] cin
		WHERE	cin.[active] = 1
				AND cin.[project_id] = @projectID
				AND cin.[name] LIKE @sqlServerNameFilter

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Running internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'dbo.usp_monAlarmCustomTransactionsStatus',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

------------------------------------------------------------------------------------------------------------------------------------------
--generate alerts: Replication latency exceeds thresold
------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Generate alerts..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

DECLARE   @instanceName		[sysname]
		, @databaseName		[sysname]
		, @childObjectName	[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)

DECLARE crsTransactionStatusAlarms CURSOR LOCAL FAST_FORWARD FOR	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'warning'			AS [severity]
																			, 'running transaction'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>warning</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>running transaction</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'N/A' END + '</transaction_begin_time>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdWarningRunningTransactions*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Running Transaction Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)

																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[transaction_elapsed_time_sec] >= @alertThresholdWarningRunningTransactions 
																			AND sts.[transaction_elapsed_time_sec] < @alertThresholdCriticalRunningTransactions 
																			AND sts.[request_completed] = 0 /* running transaction */
																			AND asr.[id] IS NULL
												
																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'critical'			AS [severity]
																			, 'running transaction'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>critical</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>running transaction</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'N/A' END + '</transaction_begin_time>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdCriticalRunningTransactions*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Running Transaction Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[transaction_elapsed_time_sec] >= @alertThresholdCriticalRunningTransactions
																			AND sts.[request_completed] = 0 /* running transaction */
																			AND asr.[id] IS NULL
												
																	UNION ALL
												
																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'warning'			AS [severity]
																			, 'uncommitted transaction'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>warning</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>uncommitted transaction</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'' END + '</transaction_begin_time>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdWarningUncommitted*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Uncommitted Transaction Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[transaction_elapsed_time_sec] >= @alertThresholdWarningUncommitted 
																			AND sts.[transaction_elapsed_time_sec] < @alertThresholdCriticalUncommitted 
																			AND sts.[request_completed] = 1 /* uncommitted transaction / request has completed */
																			AND asr.[id] IS NULL
												
																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'critical'			AS [severity]
																			, 'uncommitted transaction'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>critical</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>uncommitted transaction</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'' END + '</transaction_begin_time>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdCriticalUncommitted*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Uncommitted Transaction Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[transaction_elapsed_time_sec] >= @alertThresholdCriticalUncommitted
																			AND sts.[request_completed] = 1 /* uncommitted transaction / request has completed */
																			AND asr.[id] IS NULL

																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'warning'				AS [severity]
																			, 'blocked transaction'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>warning</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>blocked transaction</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'' END + '</transaction_begin_time>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdWarningBlocking*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Blocking Transaction Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[wait_duration_sec] >= @alertThresholdWarningBlocking
																			AND sts.[wait_duration_sec] < @alertThresholdCriticalBlocking
																			AND sts.[is_session_blocked] = 1
																			AND asr.[id] IS NULL
												
																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'critical'			AS [severity]
																			, 'blocked transaction'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>critical</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>blocked transaction</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'' END + '</transaction_begin_time>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdCriticalBlocking*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Blocking Transaction Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[wait_duration_sec] >= @alertThresholdCriticalBlocking
																			AND sts.[is_session_blocked] = 1
																			AND asr.[id] IS NULL

																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'warning'				AS [severity]
																			, 'tempdb space'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>warning</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>tempdb space</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'' END + '</transaction_begin_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<tempdb_usage>' + CAST(sts.[tempdb_space_used_mb] AS [nvarchar]) + '</tempdb_usage>' + 
																				'<threshold_value>' + CAST(@alertThresholdWarningTempdb AS [nvarchar]) + '</threshold_value>' + 
																				'<measure_unit>mb</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('tempdb: space used by a single session')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[tempdb_space_used_mb] >= @alertThresholdWarningTempdb
																			AND sts.[tempdb_space_used_mb] < @alertThresholdCriticalTempdb
																			AND asr.[id] IS NULL
												
																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'critical'			AS [severity]
																			, 'tempdb space'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>critical</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>tempdb space</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + [dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml') + '</databases>' + 
																				'<host_name>' + [dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml') + '</host_name>' + 
																				'<program_name>' + [dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml') + '</program_name>' + 
																				'<login_name>' + [dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml') + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<transaction_begin_time>' + CASE WHEN  sts.[transaction_begin_time] IS NOT NULL THEN CONVERT([varchar](20), sts.[transaction_begin_time], 120) ELSE N'' END + '</transaction_begin_time>' + 
																				'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
																				'<tempdb_usage>' + CAST(sts.[tempdb_space_used_mb] AS [nvarchar]) + '</tempdb_usage>' + 
																				'<threshold_value>' + CAST(@alertThresholdCriticalTempdb AS [nvarchar]) + '</threshold_value>' + 
																				'<measure_unit>mb</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('tempdb: space used by a single session')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[tempdb_space_used_mb] >= @alertThresholdCriticalTempdb
																			AND asr.[id] IS NULL

																	UNION ALL
												
																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'warning'			AS [severity]
																			, 'long session request'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>warning</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>long session request</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + ISNULL([dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml'), N'') + '</databases>' + 
																				'<host_name>' + ISNULL([dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml'), N'')  + '</host_name>' + 
																				'<program_name>' + ISNULL([dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml'), N'')  + '</program_name>' + 
																				'<login_name>' + ISNULL([dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml'), N'')  + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdWarningActiveRequest*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Active Request/Session Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[last_request_elapsed_time_sec] >= @alertThresholdWarningActiveRequest 
																			AND sts.[last_request_elapsed_time_sec] < @alertThresholdCriticalActiveRequest 
																			AND sts.[request_completed] = 0
																			AND sts.[is_session_blocked] = 0
																			AND asr.[id] IS NULL
												
																	UNION ALL

																	SELECT  DISTINCT
																			  cin.[instance_name] AS [instance_name]
																			, db.[database_name] AS [object_name]
																			, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
																			, 'critical'			AS [severity]
																			, 'long session request'	AS [event_name]
																			, '<alert><detail>' + 
																				'<severity>critical</severity>' + 
																				'<instance_name>' + [dbo].[ufn_getObjectQuoteName](cin.[instance_name], 'xml') + '</instance_name>' + 
																				'<counter_name>long session request</counter_name>' + 
																				'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
																				'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
																				'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
																				'<databases>' + ISNULL([dbo].[ufn_getObjectQuoteName](db.[database_name], 'xml'), N'')  + '</databases>' + 
																				'<host_name>' + ISNULL([dbo].[ufn_getObjectQuoteName](sts.[host_name], 'xml'), N'')  + '</host_name>' + 
																				'<program_name>' + ISNULL([dbo].[ufn_getObjectQuoteName](sts.[program_name], 'xml'), N'')  + '</program_name>' + 
																				'<login_name>' + ISNULL([dbo].[ufn_getObjectQuoteName](sts.[login_name], 'xml'), N'')  + '</login_name>' + 
																				'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN ISNULL(sh.[sql_handle], '') ELSE N'' END + '</sql_handle>' + 
																				'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
																				'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdCriticalActiveRequest*1000) + '</threshold_value>' + 
																				'<measure_unit>sec</measure_unit>' + 
																				'<event_date_utc>' + CONVERT([varchar](20), sts.[event_date_utc], 120) + '</event_date_utc>' + 
																				'</detail></alert>' AS [event_message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [monitoring].[statsTransactionsStatus] sts ON sts.[project_id] = cin.[project_id] AND sts.[instance_id] = cin.[instance_id]
																	INNER JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + [database_name]
																								FROM (
																										SELECT DISTINCT [database_name]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																									)db
																								ORDER BY [database_name]
																								FOR XML PATH ('')
																							) ,1,2,'') [database_name]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)db ON db.[session_id] = sts.[session_id]
																	LEFT JOIN
																			(
																				SELECT [session_id],
																						STUFF((
																								SELECT ', ' + '0x' + CAST('' AS XML).value('xs:hexBinary(sql:column("[sql_handle]") )', 'VARCHAR(64)')
																								FROM (
																										SELECT DISTINCT [sql_handle]
																										FROM [monitoring].[statsTransactionsStatus]
																										WHERE [session_id] = sts.[session_id]
																											 AND [sql_handle] IS NOT NULL
																											 AND [sql_handle] <> 0x0000000000000000000000000000000000000000
																									)db
																								ORDER BY [sql_handle]
																								FOR XML PATH ('')
																							) ,1,2,'') [sql_handle]
																				FROM [monitoring].[statsTransactionsStatus] sts
																				GROUP BY [session_id]
																			)sh ON sh.[session_id] = sts.[session_id]
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'performance'
																													AND asr.[alert_name] IN ('Active Request/Session Elapsed Time (sec)')
																													AND asr.[active] = 1
																													AND (asr.[skip_value] = cin.[machine_name] OR asr.[skip_value]=cin.[instance_name])
																													AND (   asr.[skip_value2] IS NULL 
																														 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = sh.[sql_handle])			/* SQL Handle skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[program_name] LIKE asr.[skip_value2])	/* program name/application skip */
																														 OR (asr.[skip_value2] IS NOT NULL AND sts.[login_name] LIKE asr.[skip_value2])		/* login_name skip */
																														)
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND cin.[instance_name] LIKE @sqlServerNameFilter
																			AND sts.[last_request_elapsed_time_sec] >= @alertThresholdCriticalActiveRequest
																			AND sts.[request_completed] = 0
																			AND sts.[is_session_blocked] = 0
																			AND asr.[id] IS NULL
																	ORDER BY [instance_name], [object_name]
OPEN crsTransactionStatusAlarms
FETCH NEXT FROM crsTransactionStatusAlarms INTO @instanceName, @databaseName, @childObjectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		/* check for additional receipients for the alert */		
		SET @additionalRecipients = [dbo].[ufn_monGetAdditionalAlertRecipients](@projectID, @instanceName, @eventName, @databaseName)

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @databaseName,
														@objectName				= @severity,
														@childObjectName		= @childObjectName,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= @additionalRecipients,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsTransactionStatusAlarms INTO @instanceName, @databaseName, @childObjectName, @severity, @eventName, @eventMessage
	end
CLOSE crsTransactionStatusAlarms
DEALLOCATE crsTransactionStatusAlarms
GO
