SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectEventMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectEventMessages]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectEventMessages]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectEventMessages]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 30.03.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000),
		@maxRemoteEventID		[bigint]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#checkIfObjectExists') IS NOT NULL 
DROP TABLE #checkIfObjectExists

CREATE TABLE #checkIfObjectExists
(
	[object_id]	[int]		NULL
)


/*-------------------------------------------------------------------------------------------------------------------------------*/
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end


------------------------------------------------------------------------------------------------------------------------------------------
--A. get databases informations
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectEventMessages'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 2: Copy Event Messages Information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
																	AND cin.[instance_name] <> @@SERVERNAME
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		--check if destination server has event messages feature
		SET @queryToRun=N''
		SET @queryToRun=@queryToRun + N'SELECT OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.logEventMessages'', ''U'') AS [object_id]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		BEGIN TRY
			TRUNCATE TABLE #checkIfObjectExists
			INSERT	INTO #checkIfObjectExists([object_id])
					EXEC sp_executesql  @queryToRun
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectEventMessages'
							, @strMessage
		END CATCH
		
		IF ISNULL((SELECT [object_id] FROM #checkIfObjectExists), 0) <> 0
			begin
				--get last copied event
				SELECT	@maxRemoteEventID = MAX([remote_event_id])
				FROM	[dbo].[logEventMessages]
				WHERE	[project_id] = @projectID
						AND [instance_id] = @instanceID

				SET @maxRemoteEventID = ISNULL(@maxRemoteEventID, 0)

				SET @queryToRun=N''
				SET @queryToRun=@queryToRun + N'SELECT    lem.[id], lem.[event_date_utc], lem.[module], lem.[parameters], lem.[event_name]
														, lem.[database_name], lem.[object_name], lem.[child_object_name], lem.[message]
														, lem.[send_email_to], lem.[event_type], lem.[is_email_sent], lem.[flood_control]
									FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.logEventMessages lem
									WHERE lem.[id] > ' + CAST(ISNULL(@maxRemoteEventID, 0) AS [nvarchar](32))
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

				SET @queryToRun= N'SELECT x.[id]
										, ' + CAST(@projectID AS [nvarchar]) + N' AS [project_id]
										, ' + CAST(@instanceID AS [nvarchar]) + N' AS [instance_id]
										, x.[event_date_utc], x.[module], x.[parameters], x.[event_name]
										, x.[database_name], x.[object_name], x.[child_object_name], x.[message]
										, x.[send_email_to], x.[event_type], x.[is_email_sent], x.[flood_control]
									FROM (' + @queryToRun + N')x'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				BEGIN TRY
					INSERT	INTO [dbo].[logEventMessages]([remote_event_id], [project_id], [instance_id], [event_date_utc], [module], [parameters], [event_name], [database_name], [object_name], [child_object_name], [message], [send_email_to], [event_type], [is_email_sent], [flood_control])
							EXEC sp_executesql  @queryToRun
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectEventMessages'
									, @strMessage
				END CATCH
			end


		--xxx
		--check if destination server has job execution feature
		SET @queryToRun=N''
		SET @queryToRun=@queryToRun + N'SELECT OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.jobExecutionHistory'', ''U'') AS [object_id]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		BEGIN TRY
			TRUNCATE TABLE #checkIfObjectExists
			INSERT	INTO #checkIfObjectExists([object_id])
					EXEC sp_executesql  @queryToRun
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectEventMessages'
							, @strMessage
		END CATCH
		
		IF ISNULL((SELECT [object_id] FROM #checkIfObjectExists), 0) <> 0
			begin
				--get last copied job event
				SELECT	@maxRemoteEventID = MAX([remote_id])
				FROM	[dbo].[jobExecutionHistory]
				WHERE	[project_id] = @projectID
						AND [instance_id] = @instanceID

				SET @maxRemoteEventID = ISNULL(@maxRemoteEventID, 0)

				SET @queryToRun=N''
				SET @queryToRun=@queryToRun + N'SELECT    jeh.[id], jeh.[instance_id], jeh.[project_id], jeh.[module], jeh.[descriptor], jeh.[filter]
														, jeh.[task_id], jeh.[database_name]
														, cin.[name] AS [for_instance_name]
														, jeh.[job_name], jeh.[job_id], jeh.[job_step_name]
														, jeh.[job_database_name], jeh.[job_command], jeh.[execution_date], jeh.[running_time_sec], jeh.[log_message]
														, jeh.[status], jeh.[event_date_utc]
									FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.jobExecutionHistory jeh
									INNER JOIN ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.catalogInstanceNames cin ON jeh.[instance_id] = cin.[id] AND jeh.[project_id] = cin.[project_id]
									WHERE jeh.[id] > ' + CAST(ISNULL(@maxRemoteEventID, 0) AS [nvarchar](32))
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

				SET @queryToRun= N'SELECT   x.[id]
											, ' + CAST(@projectID AS [nvarchar]) + N' AS [project_id]
											, ' + CAST(@instanceID AS [nvarchar]) + N' AS [instance_id]
											, x.[module], x.[descriptor], x.[filter]
											, x.[task_id], x.[database_name]
											, cin.[id] AS [for_instance_id]
											, x.[job_name], x.[job_id], x.[job_step_name]
											, x.[job_database_name], x.[job_command], x.[execution_date], x.[running_time_sec], x.[log_message]
											, x.[status], x.[event_date_utc]
									FROM (' + @queryToRun + N')x
									LEFT JOIN ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.catalogInstanceNames cin ON cin.[project_id] = ' + CAST(@projectID AS [nvarchar])  + N' AND cin.[name] = x.[for_instance_name]'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				BEGIN TRY
					INSERT INTO [dbo].[jobExecutionHistory]([remote_id], [project_id], [instance_id], [module], [descriptor], [filter], [task_id], [database_name], 
															[for_instance_id], [job_name], [job_id], [job_step_name], [job_database_name], [job_command], [execution_date], 
															[running_time_sec], [log_message], [status], [event_date_utc])
							EXEC sp_executesql  @queryToRun
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectEventMessages'
									, @strMessage
				END CATCH
			end

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
