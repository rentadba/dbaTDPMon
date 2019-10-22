SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectErrorlogMessages]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectErrorlogMessages]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectErrorlogMessages]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectErrorlogMessages]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 29.04.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](max),
		@errorCode				[int],
		@lineID					[int],
		@hoursOffsetToUTC		[smallint],
		@queryParam				[nvarchar](max)


DECLARE @SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@configErrorlogFileNo	[int],
		@errorlogFileNo			[int], 
		@isAzureSQLDatabase		[bit],
		@lastCollectedEventTime [datetime]


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#xpReadErrorLog') IS NOT NULL 
DROP TABLE #xpReadErrorLog

CREATE TABLE #xpReadErrorLog
(
	[id]					[int] IDENTITY (1, 1)NOT NULL PRIMARY KEY CLUSTERED ,
	[log_date]				[datetime]		NULL,
	[process_info]			[sysname]		NULL,
	[text]					[varchar](max)	NULL,
	[continuation_row]		[bit]			NULL,
)

------------------------------------------------------------------------------------------------------------------------------------------
--get default projectCode
IF @projectCode IS NULL
	SET @projectCode = [dbo].[ufn_getProjectCode](NULL, NULL)

SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @strMessage=N'The value specified for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=1
	end


------------------------------------------------------------------------------------------------------------------------------------------
--check the option for number of errorlog files to be analyzed
BEGIN TRY
	SELECT	@configErrorlogFileNo = [value]
	FROM	[dbo].[appConfigurations]
	WHERE	[name] = N'Collect SQL Errorlog last files'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @configErrorlogFileNo = 1
END CATCH

SET @configErrorlogFileNo = ISNULL(@configErrorlogFileNo, 1)

------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage= 'Step 1: Delete existing information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectErrorlogMessages'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage= 'Step 2: Get Errorlog messages...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version],
																	CASE WHEN cin.[engine] IN (5, 6) THEN 1 ELSE 0 END AS [isAzureSQLDatabase]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion, @isAzureSQLDatabase
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage= 'Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		-------------------------------------------------------------------------------------------------------------------------
		/* get last event already saved */
		SELECT @lastCollectedEventTime = MAX(eld.[log_date])
		FROM [health-check].[statsErrorlogDetails]	eld
		WHERE	eld.[project_id] = @projectID
				AND eld.[instance_id] = @instanceID

		-------------------------------------------------------------------------------------------------------------------------
		/* get local time to UTC offset */
		SET @queryToRun='SELECT DATEDIFF(hh, GETDATE(), GETUTCDATE()) AS [offset_to_utc]' 
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		SET @queryToRun = 'SELECT @hoursOffsetToUTC = [offset_to_utc] FROM (' + @queryToRun + ')y'
		SET @queryParam = '@hoursOffsetToUTC [smallint] OUTPUT'
		
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		EXEC sp_executesql @queryToRun, @queryParam, @hoursOffsetToUTC = @hoursOffsetToUTC OUT

		-------------------------------------------------------------------------------------------------------------------------
		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 9
		END CATCH

		TRUNCATE TABLE #xpReadErrorLog
		
		IF @isAzureSQLDatabase = 0
			begin
				/* get errorlog messages */
				SET @errorlogFileNo = @configErrorlogFileNo
				WHILE @errorlogFileNo > 0
					begin
						IF @sqlServerName <> @@SERVERNAME
							begin
								IF @SQLMajorVersion < 11
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; DECLARE @startTime [datetime]; SET @startTime = ''''' + CONVERT([varchar](20), @lastCollectedEventTime, 120) + '''''; EXEC xp_readerrorlog ' + CAST((@errorlogFileNo-1) AS [nvarchar]) + ', 1, NULL, NULL, @startTime, NULL'')x'
								ELSE
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; DECLARE @startTime [datetime]; SET @startTime = ''''' + CONVERT([varchar](20), @lastCollectedEventTime, 120) + '''''; EXEC xp_readerrorlog ' + CAST((@errorlogFileNo-1) AS [nvarchar]) + ', 1, NULL, NULL, @startTime, NULL WITH RESULT SETS(([log_date] [datetime] NULL, [process_info] [sysname] NULL, [text] [varchar](max) NULL))'')x'
							end
						ELSE
							SET @queryToRun = N'xp_readerrorlog ' + CAST((@errorlogFileNo-1) AS [nvarchar]) + N', 1, NULL, NULL, ''' + CONVERT([varchar](20), @lastCollectedEventTime, 120) + N''', NULL'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						BEGIN TRY
							INSERT	INTO #xpReadErrorLog([log_date], [process_info], [text])
									EXEC sp_executesql  @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectErrorlogMessages'
											, @strMessage
						END CATCH

						SET @errorlogFileNo = @errorlogFileNo - 1
					end
			end
		ELSE
			begin
				DECLARE @databaseName		[sysname]
				
				/* get the linked server database name, if specified */
				SELECT @databaseName = ISNULL([catalog], 'master')
				FROM sys.servers
				WHERE UPPER([name]) = UPPER(@sqlServerName)

				/* Azure System Events are only in master database; get logs only for monitored databases */
				IF @databaseName = 'master'
					begin
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT @@SERVERNAME AS [machine_name], [start_time], [event_category], [database_name],
																''{"database_name":"'' + [database_name] + ''","event_type":"'' + [event_type] + ''","event_subtype":"'' + 
																	[event_subtype_desc] + ''","description":"'' + [description] + ''","occurrences":'' + CAST([event_count] AS [nvarchar]) + ''}'' AS [text]
															FROM sys.event_log
															WHERE [start_time] > ''' + CONVERT([varchar](20), @lastCollectedEventTime, 120) + N''''
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						SET @queryToRun = N'SELECT [start_time], [event_category], [text]
										FROM (' + @queryToRun + N')x
										INNER JOIN 
												(
												SELECT DISTINCT cin.[machine_name], sdd.[database_name]
												FROM [health-check].[vw_statsDatabaseDetails] sdd
												INNER JOIN [dbo].[vw_catalogInstanceNames] cin ON cin.[project_id] = sdd.[project_id] AND cin.[instance_id] = sdd.[instance_id]
												)y ON x.[machine_name] = y.[machine_name] COLLATE SQL_Latin1_General_CP1_CI_AS
														AND x.[database_name] = y.[database_name] COLLATE SQL_Latin1_General_CP1_CI_AS'

						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						BEGIN TRY
							INSERT	INTO #xpReadErrorLog([log_date], [process_info], [text])
									EXEC sp_executesql  @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectErrorlogMessages'
											, @strMessage
						END CATCH
				end
		end

		/* save results to stats table */
		INSERT	INTO [health-check].[statsErrorlogDetails]([instance_id], [project_id], [event_date_utc], [log_date], [process_info], [text], [log_date_utc])
				SELECT   @instanceID, @projectID, GETUTCDATE(), [log_date], [process_info], [text]
						, DATEADD(hh, @hoursOffsetToUTC, CAST([log_date] AS [datetime])) AS [log_date_utc]
				FROM #xpReadErrorLog
				WHERE [log_date] IS NOT NULL
				ORDER BY [log_date], [id]

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion, @isAzureSQLDatabase
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
