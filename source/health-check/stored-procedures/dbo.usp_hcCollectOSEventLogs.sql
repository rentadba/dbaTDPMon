SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectOSEventLogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectOSEventLogs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectOSEventLogs]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectOSEventLogs]
		@projectCode				[varchar](32)=NULL,
		@sqlServerNameFilter		[sysname]='%',
		@logNameFilter				[sysname]='%',
		@enableXPCMDSHELL			[bit]=1,
		@configEventsInLastHours	[smallint] = NULL,
		@debugMode					[bit]=0

/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 20.11.2014
-- Module			 : Database Analysis & Performance health-check
-- Description		 : read OS event logs: Application, System, Setup
-- ============================================================================
SET NOCOUNT ON

DECLARE   @eventDescriptor				[varchar](256)
		, @logEntryType					[varchar](64)
		, @psLogTypeName				[sysname]
		, @psLogTypeID					[tinyint]
		, @queryToRun					[nvarchar](max)
		, @eventLog						[varchar](max)
		, @eventLogXML					[XML]
		, @projectID					[smallint]
		, @instanceID					[smallint]
		, @strMessage					[nvarchar](4000)
		, @machineID					[smallint]
		, @machineName					[nvarchar](512)
		, @instanceName					[sysname]
		, @psFileLocation				[nvarchar](260)
		, @psFileName					[nvarchar](260)
		, @configEventsTimeOutSeconds	[int]
		, @startTime					[datetime]
		, @endTime						[datetime]
		, @getInformationEvent			[bit]
		, @getWarningsEvent				[bit]
		, @optionXPValue				[int]
		, @hoursOffsetToUTC				[smallint]
		, @queryParam					[nvarchar](max)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#psOutput') IS NOT NULL DROP TABLE #psOutput
CREATE TABLE #psOutput
	(
		  [id]	[int] identity(1,1) primary key
		, [xml] [varchar](max)
	)

------------------------------------------------------------------------------------------------------------------------------------------
--get default folder
BEGIN TRY
	SELECT	@psFileLocation = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Default folder for logs'
			AND [module] = 'common'
END TRY
BEGIN CATCH
	SET @psFileLocation = NULL
END CATCH

IF @psFileLocation IS NULL
		SELECT @psFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
		FROM (
				SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
			)er

SET @psFileLocation = ISNULL(@psFileLocation, N'C:\')
IF RIGHT(@psFileLocation, 1)<>'\' SET @psFileLocation = @psFileLocation + '\'
SET @psFileLocation = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, @psFileLocation)

------------------------------------------------------------------------------------------------------------------------------------------
--create folder on disk
SET @queryToRun = N'EXEC ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + '.[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @@SERVERNAME + N''',
																			@folderName		= ''' + [dbo].[ufn_getObjectQuoteName](@psFileLocation, 'sql') + N''',
																			@executionLevel	= 1,
																			@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

EXEC  [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
									@dbName			= NULL,
									@module			= 'dbo.usp_hcCollectOSEventLogs',
									@eventName		= 'create folder on disk',
									@queryToRun  	= @queryToRun,
									@flgOptions		= 32,
									@executionLevel	= 1,
									@debugMode		= @debugMode

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'ERROR: The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end

------------------------------------------------------------------------------------------------------------------------------------------
--get event messages time delta
IF @configEventsInLastHours IS NULL
	BEGIN TRY
		SELECT	@configEventsInLastHours = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Collect OS Events from last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configEventsInLastHours = 24
	END CATCH

SET @configEventsInLastHours = ISNULL(@configEventsInLastHours, 24)

------------------------------------------------------------------------------------------------------------------------------------------
--option to fetch also information OS events
SET @getInformationEvent = 0

BEGIN TRY
	SELECT	@getInformationEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Collect Information OS Events'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @getInformationEvent = 0
END CATCH

SET @getInformationEvent = ISNULL(@getInformationEvent, 0)

------------------------------------------------------------------------------------------------------------------------------------------
--option to fetch also warnings OS events
SET @getWarningsEvent = 0

BEGIN TRY
	SELECT	@getWarningsEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Collect Warning OS Events'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @getWarningsEvent = 0
END CATCH

SET @getWarningsEvent = ISNULL(@getWarningsEvent, 0)


------------------------------------------------------------------------------------------------------------------------------------------
--option for timeout when fetching OS events
BEGIN TRY
	SELECT	@configEventsTimeOutSeconds = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = 'Collect OS Events timeout (seconds)'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @configEventsTimeOutSeconds = 600
END CATCH

SET @configEventsTimeOutSeconds = ISNULL(@configEventsTimeOutSeconds, 600)



-------------------------------------------------------------------------------------------------------------------------
IF @enableXPCMDSHELL=1
	begin
		SET @optionXPValue = 0

		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @@SERVERNAME
														, @flgAction	 = 1			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode

		IF @optionXPValue = 0
			begin
				RETURN 1
			end		
	end

------------------------------------------------------------------------------------------------------------------------------------------
--A. get servers OS events details
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage=N'Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE soel
FROM [health-check].[statsOSEventLogs]			soel
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = soel.[instance_id] AND cin.[project_id] = soel.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND soel.[log_type_id] IN	(	
									 SELECT [log_type_id]
									 FROM (
											SELECT 'Application' AS [log_type_name], 1 AS [log_type_id] UNION ALL
											SELECT 'System'		 AS [log_type_name], 2 AS [log_type_id] UNION ALL
											SELECT 'Setup'		 AS [log_type_name], 3 AS [log_type_id] 
									      )l
									 WHERE [log_type_name] LIKE @logNameFilter
									)
		AND [time_created_utc] >= DATEADD(hour, -@configEventsInLastHours, GETUTCDATE())

		
DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectOSEventLogs'


-------------------------------------------------------------------------------------------------------------------------
SET @strMessage=N'Step 2: Generate PowerShell script ...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0


/*-------------------------------------------------------------------------------------------------------------------------------*/
SET @logEntryType='1,2' /*Critical, Error*/
IF @getWarningsEvent=1
	SET @logEntryType=@logEntryType + ',3'
IF @getInformationEvent=1
	SET @logEntryType=@logEntryType + ',4'

SET @eventDescriptor = 'dbo.usp_hcCollectOSEventLogs-Powershell'

DECLARE crsMachineList CURSOR LOCAL FAST_FORWARD FOR	SELECT cin.[id] AS [instance_id], cin.[name] AS [instance_name], cmn.[id] AS [machine_id], cmn.[name] AS [machine_name]
														FROM	[dbo].[catalogInstanceNames] cin
														INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[project_id]=cin.[project_id] AND cmn.[id]=cin.[machine_id]
														WHERE 	cin.[project_id] = @projectID
																AND cin.[name] LIKE @sqlServerNameFilter
																AND (   cin.[active] = 1
																		OR 
																		(
																			cin.[active] = 0
																			AND cin.[is_clustered] = 1
																			AND EXISTS (
																						SELECT 1
																						FROM	[dbo].[catalogInstanceNames] cin2
																						INNER JOIN [dbo].[catalogMachineNames] cmn2 ON cmn2.[project_id]=cin2.[project_id] AND cmn2.[id]=cin2.[machine_id]
																						WHERE cin2.[project_id] = @projectID
																								AND cin2.[active] = 1	
																								AND cin2.[name] = cin.[name]
																								AND cmn2.[id] <> cmn.[id]
																					)
																		)
																	)
																AND cin.[engine] NOT IN (5, 6, 8) /* feature not available on Azure managed database*/
														ORDER BY cin.[name], cmn.[name]
OPEN crsMachineList
FETCH NEXT FROM crsMachineList INTO @instanceID, @instanceName, @machineID, @machineName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @machineName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

		-------------------------------------------------------------------------------------------------------------------------
		/* get local time to UTC offset */
		SET @queryToRun='SELECT DATEDIFF(hh, GETDATE(), GETUTCDATE()) AS [offset_to_utc]' 
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@instanceName, @queryToRun)
		SET @queryToRun = 'SELECT @hoursOffsetToUTC = [offset_to_utc] FROM (' + @queryToRun + ')y'
		SET @queryParam = '@hoursOffsetToUTC [smallint] OUTPUT'
		
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		EXEC sp_executesql @queryToRun, @queryParam, @hoursOffsetToUTC = @hoursOffsetToUTC OUT

		-------------------------------------------------------------------------------------------------------------------------
		DECLARE crsLogName CURSOR LOCAL FAST_FORWARD FOR	SELECT [log_type_name], [log_type_id]
															FROM (
																	SELECT 'Application' AS [log_type_name], 1 AS [log_type_id] UNION ALL
																	SELECT 'System'		 AS [log_type_name], 2 AS [log_type_id] UNION ALL
																	SELECT 'Setup'		 AS [log_type_name], 3 AS [log_type_id] 
																)l
															WHERE [log_type_name] LIKE @logNameFilter

		OPEN crsLogName
		FETCH NEXT FROM crsLogName INTO @psLogTypeName, @psLogTypeID
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage=N'Analyze type: ' + @psLogTypeName
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 3, @stopExecution=0

				SET @strMessage=N'generate powershell script'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				DELETE lsam
				FROM [dbo].[logAnalysisMessages]	lsam
				INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
				WHERE cin.[project_id] = @projectID
						AND cin.[id]= @instanceID
						AND lsam.[descriptor]=@eventDescriptor
						
				SET @queryToRun='SELECT CONVERT([varchar](20), GETDATE(), 120) AS [current_date]'
				SET @queryToRun = dbo.ufn_formatSQLQueryForLinkedServer(@instanceName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				DELETE FROM #psOutput
				BEGIN TRY
					INSERT	INTO #psOutput([xml])
							EXEC sp_executesql  @queryToRun

					SELECT TOP 1 @endTime = CONVERT([datetime], [xml], 120)
					FROM #psOutput
				END TRY
				BEGIN CATCH
					SET @endTime = GETDATE()
				END CATCH

				SET @endTime = ISNULL(@endTime, GETDATE())
				SET @startTime = DATEADD(hh, -@configEventsInLastHours, @endTime)

				-------------------------------------------------------------------------------------------------------------------------
				SET @queryToRun = N'
						#-- ============================================================================
						#-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
						#-- ============================================================================
						#-- Author			 : Dan Andrei STEFAN
						#-- Create date		 : 20.11.2014
						#-- Module			 : Database Analysis & Performance health-check
						#-- Description		 : read OS event logs: Application, System, Setup
						#-- ============================================================================					
						$timeoutSeconds = ' + CAST(@configEventsTimeOutSeconds AS [nvarchar]) + N'
						$code = {
									$ErrorActionPreference = "SilentlyContinue"

									#setup OS event filters
									$machineName = ''' + @machineName + N'''
									$eventName = ''' + @psLogTypeName + '''
									$startTime = ''' + CONVERT([varchar](20), @startTime, 120) + N'''
									$endTime = ''' + CONVERT([varchar](20), @endTime, 120) + N'''
									$level = ' + @logEntryType + N'

									#get OS events
									$Error.Clear()
									Get-WinEvent -Computername $machineName -FilterHashTable @{logname=$eventName; Level=$level; StartTime=$startTime; EndTime=$endTime}|Select-Object Id, Level, RecordId, Task, TaskDisplayName, ProviderName, LogName, ProcessId, ThreadId, MachineName, UserId, TimeCreated, LevelDisplayName, Message|ConvertTo-XML -As string|Out-String -Width 32768

									if ($Error) 
									{
										$Error[0].ToString()
									}
								}
						$j = Start-Job -ScriptBlock $code
						if (Wait-Job $j -Timeout $timeoutSeconds) { Receive-Job $j }
						Remove-Job -force $j'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
						SELECT  @instanceID
								, @projectID
								, GETUTCDATE()
								, @eventDescriptor
								, @queryToRun


			
				-------------------------------------------------------------------------------------------------------------------------
				IF @optionXPValue = 1
					begin
						-- save powershell script
						SET @psFileName = 'GetOSSystemEvents_' + REPLACE(@machineName, '\', '$') + '_' + @psLogTypeName + '.ps1'
						SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [message] FROM ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + '.[dbo].[logAnalysisMessages] WHERE [descriptor]=''''' + @eventDescriptor + ''''' AND [instance_id]=' + CAST(@instanceID AS [varchar]) + ' AND [project_id]=' + CAST(@projectID AS [varchar]) + '" queryout "' + @psFileLocation + @psFileName + '" -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'', no_output'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						EXEC sp_executesql  @queryToRun 
					end

				-------------------------------------------------------------------------------------------------------------------------
				--executing script to get the OS events
				IF @optionXPValue = 1
					begin
						SET @strMessage=N'running powershell script - get OS events...'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						SET @queryToRun='master.dbo.xp_cmdshell N''@PowerShell -ExecutionPolicy Bypass -File "' + [dbo].[ufn_getObjectQuoteName](@psFileLocation + @psFileName, 'sql') + '"'''
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						DELETE FROM #psOutput
						BEGIN TRY
							INSERT	INTO #psOutput([xml])
									EXEC sp_executesql  @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, @strMessage
						END CATCH

						BEGIN TRY
							SET @queryToRun=N'master.dbo.xp_cmdshell ''del "' + [dbo].[ufn_getObjectQuoteName](@psFileLocation + @psFileName, 'sql') + '"'', no_output'
							IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0
							EXEC sp_executesql  @queryToRun 
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, @strMessage
						END CATCH
					end

				-------------------------------------------------------------------------------------------------------------------------
				--executing script to get the OS events
				SET @strMessage=N'analyzing data...'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				IF @debugMode=1 
					SELECT * FROM #psOutput 

				IF	EXISTS (SELECT * FROM #psOutput WHERE [xml] LIKE '%Objects%')
					AND NOT EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%No events were found that match the specified selection criteria%')
					AND NOT EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%There are no more endpoints available from the endpoint mapper%')
					AND NOT EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%The RPC server is unavailable%')
					begin
						SET @eventLog=''
						SELECT @eventLog = ((
												SELECT [xml]
												FROM #psOutput
												ORDER BY [id]
												FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))
						/*
						SELECT @eventLog=@eventLog + [xml] 
						FROM #psOutput 
						WHERE [xml] IS NOT NULL 
						ORDER BY [id] 
				  		*/
						SET @eventLogXML = @eventLog

						IF @debugMode=1 
							SELECT    @instanceID, @projectID, @machineID, GETUTCDATE(), @psLogTypeID
									, [Id] AS [EventID], [Level], [RecordId], [Task] AS [Category], [TaskDisplayName] AS [CategoryName]
									, [ProviderName] AS [Source]
									, [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [Message]
							FROM (
									SELECT [value], [attribute], [unique_object] AS [idX]
									FROM (
											SELECT	[property].value('(./text())[1]', 'Varchar(1024)') AS [value],
													[property].value('@Name', 'Varchar(1024)') AS [attribute],
													DENSE_RANK() OVER (ORDER BY [object]) AS unique_object
											FROM @eventLogXML.nodes('Objects/Object') AS b ([object])
											CROSS APPLY b.object.nodes('./Property') AS c (property)
										)X
									WHERE [attribute] IN ('Id', 'Level', 'RecordId', 'Task', 'TaskDisplayName', 'ProviderName', 'LogName', 'ProcessId', 'ThreadId', 'MachineName', 'UserId', 'TimeCreated', 'LevelDisplayName', 'Message')
								)P
							PIVOT
								(
									MAX([value])
									FOR [attribute] IN ([Id], [Level], [RecordId], [Task], [TaskDisplayName], [ProviderName], [LogName], [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [LevelDisplayName], [Message])
								)pvt

						/* save results to stats table */
						INSERT	INTO [health-check].[statsOSEventLogs](   [instance_id], [project_id], [machine_id], [event_date_utc], [log_type_id]
																		, [event_id], [level_id], [record_id], [category_id], [category_name]
																		, [source], [process_id], [thread_id], [machine_name], [user_id], [time_created], [message]
																		, [time_created_utc])
								SELECT    @instanceID, @projectID, @machineID, GETUTCDATE(), @psLogTypeID
										, [Id] AS [EventID], [Level], [RecordId], [Task] AS [Category], [TaskDisplayName] AS [CategoryName]
										, [ProviderName] AS [Source]
										, [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [Message]
										, DATEADD(hh, @hoursOffsetToUTC, CAST([TimeCreated] AS [datetime])) AS [time_created_utc]
								FROM (
										SELECT [value], [attribute], [unique_object] AS [idX]
										FROM (
												SELECT	[property].value('(./text())[1]', 'Varchar(1024)') AS [value],
														[property].value('@Name', 'Varchar(1024)') AS [attribute],
														DENSE_RANK() OVER (ORDER BY [object]) AS unique_object
												FROM @eventLogXML.nodes('Objects/Object') AS b ([object])
												CROSS APPLY b.object.nodes('./Property') AS c (property)
											)X
										WHERE [attribute] IN ('Id', 'Level', 'RecordId', 'Task', 'TaskDisplayName', 'ProviderName', 'LogName', 'ProcessId', 'ThreadId', 'MachineName', 'UserId', 'TimeCreated', 'LevelDisplayName', 'Message')
									)P
								PIVOT
									(
										MAX([value])
										FOR [attribute] IN ([Id], [Level], [RecordId], [Task], [TaskDisplayName], [ProviderName], [LogName], [ProcessId], [ThreadId], [MachineName], [UserId], [TimeCreated], [LevelDisplayName], [Message])
									)pvt

					end
				ELSE
					begin
						IF (SELECT COUNT(*) FROM #psOutput WHERE [xml] IS NOT NULL)=0
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'Timeout occured while running powershell script. (LogName = ' + @psLogTypeName + ')'

						IF EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%There are no more endpoints available from the endpoint mapper%')
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'There are no more endpoints available from the endpoint mapper.'

						IF EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%The RPC server is unavailable%')
							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'The RPC server is unavailable.'
					end
					
				FETCH NEXT FROM crsLogName INTO @psLogTypeName, @psLogTypeID
			end
		CLOSE crsLogName
		DEALLOCATE crsLogName

		FETCH NEXT FROM crsMachineList INTO @instanceID, @instanceName, @machineID, @machineName
	end
CLOSE crsMachineList
DEALLOCATE crsMachineList

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]=@eventDescriptor

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* disable xp_cmdshell configuration option */
IF @enableXPCMDSHELL=1
	begin
		EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @@SERVERNAME
														, @flgAction	 = 0			-- 1=enable | 0=disable
														, @optionXPValue = @optionXPValue OUTPUT
														, @debugMode	 = @debugMode
	end
/*-------------------------------------------------------------------------------------------------------------------------------*/
GO
