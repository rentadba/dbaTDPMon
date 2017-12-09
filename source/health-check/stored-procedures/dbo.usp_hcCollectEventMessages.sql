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
		@sqlServerVersion		[varchar](32),
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
--A. get databases informations
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectEventMessages'

-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Copy Event Messages Information....', 10, 1) WITH NOWAIT
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
																	AND cin.[instance_name] <> @@SERVERNAME
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		--check if destination server has event messages feature
		SET @queryToRun=N''
		SET @queryToRun=@queryToRun + N'SELECT OBJECT_ID(''' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + N'.dbo.logEventMessages'', ''U'') AS [object_id]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	PRINT @queryToRun

		BEGIN TRY
			TRUNCATE TABLE #checkIfObjectExists
			INSERT	INTO #checkIfObjectExists([object_id])
					EXEC (@queryToRun)
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			PRINT @strMessage
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
				IF @debugMode=1	PRINT @queryToRun

				BEGIN TRY
					INSERT	INTO [dbo].[logEventMessages]([remote_event_id], [project_id], [instance_id], [event_date_utc], [module], [parameters], [event_name], [database_name], [object_name], [child_object_name], [message], [send_email_to], [event_type], [is_email_sent], [flood_control])
							EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectEventMessages'
									, @strMessage
				END CATCH
			end

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
