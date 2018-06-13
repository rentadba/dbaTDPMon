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
		@lineID					[int]

DECLARE @SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@configErrorlogFileNo	[int],
		@errorlogFileNo			[int]


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

DELETE eld
FROM [health-check].[statsErrorlogDetails]	eld
INNER JOIN [dbo].[catalogInstanceNames]		cin ON cin.[id] = eld.[instance_id] AND cin.[project_id] = eld.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectErrorlogMessages'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage= 'Step 2: Get Errorlog messages...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		
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
		SET @strMessage= 'Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		TRUNCATE TABLE #xpReadErrorLog

		/* get errorlog messages */
		SET @errorlogFileNo = @configErrorlogFileNo
		WHILE @errorlogFileNo > 0
			begin
				IF @sqlServerName <> @@SERVERNAME
					begin
						IF @SQLMajorVersion < 11
							IF @SQLMajorVersion > 8
								SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog ' + CAST((@errorlogFileNo-1) AS [nvarchar]) + ''')x'
							ELSE 
								SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog'')x'
						ELSE
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog ' + CAST((@errorlogFileNo-1) AS [nvarchar]) + ' WITH RESULT SETS(([log_date] [datetime] NULL, [process_info] [sysname] NULL, [text] [varchar](max) NULL))'')x'
					end
				ELSE
					SET @queryToRun = N'xp_readerrorlog ' + CAST((@errorlogFileNo-1) AS [nvarchar])
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				BEGIN TRY
					IF @SQLMajorVersion > 8 
						INSERT	INTO #xpReadErrorLog([log_date], [process_info], [text])
								EXEC sp_executesql  @queryToRun
					ELSE
						INSERT	INTO #xpReadErrorLog([text], [continuation_row])
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

		/* re-parse messages for 2k version */
		IF @SQLMajorVersion = 8 
			begin
				SET @strMessage= 'rebuild messages for ContinuationRows'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				/*
				DECLARE crsErrorlogContinuation CURSOR LOCAL FAST_FORWARD FOR	SELECT [id], [text]
																				FROM #xpReadErrorLog
																				WHERE [continuation_row]=1
				OPEN crsErrorlogContinuation
				FETCH NEXT FROM crsErrorlogContinuation INTO @lineID, @strMessage
				WHILE @@FETCH_STATUS=0
					begin
						UPDATE #xpReadErrorLog
							SET [text] = [text] + @strMessage
						WHERE [id] = @lineID-1

						FETCH NEXT FROM crsErrorlogContinuation INTO @lineID, @strMessage
					end
				CLOSE crsErrorlogContinuation
				DEALLOCATE crsErrorlogContinuation
				*/

				UPDATE S
					SET S.[text] = S.[text] + D.[text]
				FROM #xpReadErrorLog S
				INNER JOIN 
					(
						SELECT [id], [text]
						FROM #xpReadErrorLog
						WHERE [continuation_row]=1
					) D ON S.[id] = D.[id]-1


				DELETE 
				FROM #xpReadErrorLog
				WHERE [continuation_row]=1

				SET @strMessage= 'split messages / SQL Server 2000'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				UPDATE eld
					SET   eld.[log_date] = X.[log_date]
						, eld.[process_info] = X.[process_info]
						, eld.[text] = X.[text]
				FROM #xpReadErrorLog eld
				INNER JOIN 
					(
						SELECT    [id]
								, SUBSTRING([text], 1, 22) AS [log_date]
								, LTRIM(RTRIM(SUBSTRING([text], 24, CHARINDEX(' ', [text], 24) -23))) AS [process_info]
								, LTRIM(RTRIM(SUBSTRING([text], CHARINDEX(' ', [text], 24), LEN([text])))) AS [text]
						FROM #xpReadErrorLog
						WHERE LEFT([text], 4) = CAST(YEAR(GETDATE()) AS [varchar])
							OR LEFT([text], 4) =CAST(YEAR(GETDATE())-1 AS [varchar])
					)X ON X.[id] = eld.[id]
			end

		/* save results to stats table */
		INSERT	INTO [health-check].[statsErrorlogDetails]([instance_id], [project_id], [event_date_utc], [log_date], [process_info], [text])
				SELECT @instanceID, @projectID, GETUTCDATE(), [log_date], [process_info], [text]
				FROM #xpReadErrorLog
				WHERE [log_date] IS NOT NULL
				ORDER BY [log_date], [id]

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
