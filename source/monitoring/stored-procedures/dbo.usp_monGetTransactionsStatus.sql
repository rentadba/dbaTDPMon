SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_monGetTransactionsStatus]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monGetTransactionsStatus]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monGetTransactionsStatus]
GO

CREATE PROCEDURE [dbo].[usp_monGetTransactionsStatus]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 12.01.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@sqlServerVersion		[sysname],
		@SQLMajorVersion		[tinyint],
		@executionLevel			[tinyint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000)


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#blockedSessionInfo') IS NOT NULL  DROP TABLE #blockedSessionInfo

CREATE TABLE #blockedSessionInfo
(
	[id]					[int] IDENTITY(1,1),
	[session_id]			[smallint],
	[blocking_session_id]	[smallint],
	[wait_duration_sec]		[int],
	[wait_type]				[nvarchar](60)
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#sessionTempdbUsage') IS NOT NULL  DROP TABLE #sessionTempdbUsage

CREATE TABLE #sessionTempdbUsage
(
	[id]					[int] IDENTITY(1,1),
	[session_id]			[smallint],
	[request_id]			[smallint],
	[space_used_mb]			[int]
)
 
/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#transactionInfo') IS NOT NULL  DROP TABLE #transactionInfo

CREATE TABLE #transactionInfo
(
	[id]						[int] IDENTITY(1,1),
	[transaction_begin_time]	[datetime],
	[elapsed_time_seconds]		[bigint],
	[session_id]				[smallint],
	[database_name]				[sysname]
)


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#monTransactionsStatus') IS NOT NULL  DROP TABLE #monTransactionsStatus

CREATE TABLE #monTransactionsStatus
(
	[id]								[int] IDENTITY(1,1),
	[server_name]						[sysname],
	[database_name]						[sysname] NULL,
	[session_id]						[smallint],
	[transaction_begin_time]			[datetime],
	[host_name]							[sysname] NULL,
	[program_name]						[sysname] NULL,
	[login_name]						[sysname] NULL,
	[last_request_elapsed_time_seconds]	[bigint],
	[transaction_elapsed_time_seconds]	[bigint],
	[sessions_blocked]					[smallint],
	[sql_handle]						[varbinary](64),
	[request_completed]					[bit],
	[is_session_blocked]				[bit],
	[wait_duration_sec]					[int],
	[wait_type]							[nvarchar](60),
	[tempdb_space_used_mb]				[int]
)

SET @executionLevel = 0

------------------------------------------------------------------------------------------------------------------------------------------
--get value for critical alert threshold
DECLARE   @alertThresholdWarning [int] 

SELECT	@alertThresholdWarning = MIN([warning_limit])
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] IN ('Uncommitted Transaction Elapsed Time (sec)', 'Running Transaction Elapsed Time (sec)')
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarning = ISNULL(@alertThresholdWarning, 900)

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
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 1: Delete existing information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

DELETE sut
FROM [monitoring].[statsTransactionsStatus]	sut
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = sut.[instance_id] AND cin.[project_id] = sut.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_monGetTransactionsStatus'


-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 2: Get Instance Details Information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		TRUNCATE TABLE #blockedSessionInfo
		TRUNCATE TABLE #transactionInfo
		TRUNCATE TABLE #monTransactionsStatus

		IF @SQLMajorVersion > 8
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  owt.[session_id]
														, owt.[blocking_session_id]
														, owt.[wait_duration_ms] / 1000
														, owt.[wait_type]
												FROM sys.dm_os_waiting_tasks owt WITH (READPAST)
												INNER JOIN sys.dm_exec_sessions es WITH (READPAST) ON es.[session_id] = owt.[session_id]
												WHERE ISNULL(owt.[session_id], 0) <> ISNULL(owt.[blocking_session_id], 0)'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				BEGIN TRY
						INSERT	INTO #blockedSessionInfo([session_id], [blocking_session_id], [wait_duration_sec], [wait_type])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[blocking-session-info]:' + @strMessage
				END CATCH


				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  tat.[transaction_begin_time]
														, ISNULL(tasdt.[elapsed_time_seconds], ABS(DATEDIFF(ss, tat.[transaction_begin_time], GETDATE()))) [elapsed_time_seconds]
														, ISNULL(tst.[session_id], tasdt.[session_id]) AS [session_id]
														, DB_NAME(tdt.[database_id]) AS [database_name]
												FROM sys.dm_tran_active_transactions						tat WITH (READPAST)
												LEFT JOIN sys.dm_tran_session_transactions					tst WITH (READPAST)		ON	tst.[transaction_id] = tat.[transaction_id]
												LEFT JOIN sys.dm_tran_database_transactions					tdt WITH (READPAST)		ON	tdt.[transaction_id] = tat.[transaction_id]
												LEFT JOIN sys.dm_tran_active_snapshot_database_transactions tasdt WITH (READPAST)	ON	tasdt.[transaction_id] = tat.[transaction_id] 
												WHERE ISNULL(tasdt.[elapsed_time_seconds], 0) >= ' + CAST(@alertThresholdWarning AS [nvarchar])
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				BEGIN TRY
						INSERT	INTO #transactionInfo([transaction_begin_time], [elapsed_time_seconds], [session_id], [database_name])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[active-transaction-info]:' + @strMessage
				END CATCH


				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT [session_id], [request_id], SUM([space_used_mb]) AS [space_used_mb]
												FROM (
														SELECT	[session_id], [request_id],
																SUM(([internal_objects_alloc_page_count] - [internal_objects_dealloc_page_count])*8)/1024 AS [space_used_mb]
														FROM sys.dm_db_task_space_usage
														GROUP BY [session_id], [request_id]
														)x
												WHERE x.[space_used_mb] > 0
												GROUP BY [session_id], [request_id]'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				BEGIN TRY
						INSERT	INTO #sessionTempdbUsage([session_id], [request_id], [space_used_mb])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[tempdb-usage-info]:' + @strMessage
				END CATCH

			
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  @@SERVERNAME AS [server_name]
														, es.[session_id]
														, er.[request_id]
														, es.[host_name]
														, es.[program_name]
														, CASE WHEN ISNULL(es.[login_name], '''') <> '''' THEN es.[login_name] ELSE sp.[loginame] END [login_name]
														, DATEDIFF(ss, es.[last_request_start_time], GETDATE()) AS [last_request_elapsed_time_seconds]
														, sp.[sql_handle]
														, CASE WHEN er.[session_id] IS NULL THEN 1 ELSE 0 END AS [request_completed]
														, DB_NAME(ISNULL(er.[database_id], es.[database_id])) AS [database_name]
												FROM sys.dm_exec_sessions es WITH (READPAST)
												INNER JOIN master.dbo.sysprocesses sp WITH (READPAST) ON sp.[spid] = es.[session_id]
												LEFT  JOIN sys.dm_exec_requests er WITH (READPAST) ON er.[session_id] = es.[session_id]
												WHERE es.[is_user_process] = 1
														AND sp.[ecid] = 0'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

				SET @queryToRun = N'SELECT DISTINCT
										   x. [server_name]
										 , x.[session_id]
										 , ISNULL(x.[database_name], ti.[database_name]) AS [database_name]
										 , x.[host_name]
										 , x.[program_name]
										 , x.[login_name]
										 , ti.[transaction_begin_time]
										 , CASE WHEN x.[last_request_elapsed_time_seconds] < 0 THEN 0 ELSE x.[last_request_elapsed_time_seconds] END AS [last_request_elapsed_time_seconds]
										 , ti.[elapsed_time_seconds] AS [transaction_elapsed_time_seconds]
										 , bk.[sessions_blocked]
										 , x.[sql_handle]
										 , x.[request_completed]
										 , CASE WHEN si.[blocking_session_id] IS NOT NULL THEN 1 ELSE 0 END AS [is_session_blocked]
										 , si.[wait_duration_sec]
										 , si.[wait_type]
										 , stu.[space_used_mb] AS [tempdb_space_used_mb]
									FROM (' + @queryToRun + N') x
									LEFT JOIN #transactionInfo ti ON ti.[session_id] = x.[session_id]
									LEFT JOIN #sessionTempdbUsage stu ON stu.[session_id] = x.[session_id] AND stu.[request_id] = x.[request_id]
									LEFT JOIN 
										(
											SELECT si.[session_id], ISNULL(bk.[sessions_blocked], 0) AS [sessions_blocked]
											FROM #blockedSessionInfo si
											LEFT JOIN
													(
														SELECT [blocking_session_id], COUNT(*) AS [sessions_blocked]
														FROM #blockedSessionInfo 
														GROUP BY [blocking_session_id]
													)bk ON bk.[blocking_session_id] = si.[session_id]
											UNION
											SELECT [blocking_session_id] AS [session_id], COUNT(*) AS [sessions_blocked]
											FROM #blockedSessionInfo 
											WHERE [blocking_session_id] IS NOT NULL
											GROUP BY [blocking_session_id]
										)bk ON bk.[session_id] = x.[session_id]
									LEFT JOIN #blockedSessionInfo si ON si.[session_id] = x.[session_id]
									WHERE	   (    ISNULL(x.[last_request_elapsed_time_seconds], 0) >= ' + CAST(@alertThresholdWarning AS [nvarchar]) + N'
											    AND x.[request_completed] = 0
											   )
											OR ISNULL(ti.[elapsed_time_seconds], 0) >= ' + CAST(@alertThresholdWarning AS [nvarchar]) + N'
									'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				BEGIN TRY
						INSERT	INTO #monTransactionsStatus([server_name], [session_id], [database_name], [host_name], [program_name], [login_name], [transaction_begin_time], [last_request_elapsed_time_seconds], [transaction_elapsed_time_seconds], [sessions_blocked], [sql_handle], [request_completed], [is_session_blocked], [wait_duration_sec], [wait_type], [tempdb_space_used_mb])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[session-info]:' + @strMessage
				END CATCH
			end								
				
		/* save results to stats table */
		INSERT INTO [monitoring].[statsTransactionsStatus]([instance_id], [project_id], [event_date_utc]
																, [database_name], [session_id], [transaction_begin_time], [host_name], [program_name], [login_name]
																, [last_request_elapsed_time_sec], [transaction_elapsed_time_sec], [sessions_blocked], [sql_handle]
																, [request_completed], [is_session_blocked], [wait_duration_sec], [wait_type], [tempdb_space_used_mb])
				SELECT    @instanceID, @projectID, GETUTCDATE()
						, [database_name], [session_id], [transaction_begin_time], [host_name], [program_name], [login_name]
						, [last_request_elapsed_time_seconds], [transaction_elapsed_time_seconds], [sessions_blocked], [sql_handle]
						, [request_completed], [is_session_blocked], [wait_duration_sec], [wait_type], [tempdb_space_used_mb]
				FROM #monTransactionsStatus
								
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
