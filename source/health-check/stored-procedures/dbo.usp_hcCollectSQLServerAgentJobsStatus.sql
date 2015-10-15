SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectSQLServerAgentJobsStatus]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectSQLServerAgentJobsStatus]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectSQLServerAgentJobsStatus]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectSQLServerAgentJobsStatus]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@jobNameFilter			[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 19.10.2010
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @sqlServerName			[sysname],
		@jobName				[sysname],
		@queryToRun				[nvarchar](4000),
		@currentRunning			[int],
		@lastExecutionStatus	[int],
		@lastExecutionDate		[varchar](10),
		@lastExecutionTime		[varchar](10),
		@runningTimeSec			[bigint],
		@projectID				[smallint],
		@instanceID				[smallint],
		@collectStepDetails		[bit],
		@strMessage				[nvarchar](max)


-----------------------------------------------------------------------------------------------------
--appConfigurations - check if step details should be collected
-----------------------------------------------------------------------------------------------------
SELECT	@collectStepDetails = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
FROM	[dbo].[appConfigurations]
WHERE	[name]='Collect SQL Agent jobs step details'
		AND [module] = 'health-check'

SET @collectStepDetails = ISNULL(@collectStepDetails, 0)


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#msdbSysJobs') IS NOT NULL DROP TABLE #msdbSysJobs

CREATE TABLE #msdbSysJobs
(
	[name]		[sysname]			NULL
)

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
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
--A. get servers jobs status informations
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE ssajh
FROM [health-check].[statsSQLServerAgentJobsHistory]		ssajh
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = ssajh.[instance_id] AND cin.[project_id] = ssajh.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectSQLServerAgentJobsStatus'


-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Get Jobs Status Information....', 10, 1) WITH NOWAIT

		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name]
												FROM	[dbo].[vw_catalogInstanceNames] cin
												WHERE 	cin.[project_id] = @projectID
														AND cin.[instance_active]=1
														AND cin.[instance_name] LIKE @sqlServerNameFilter
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		
		TRUNCATE TABLE #msdbSysJobs
		BEGIN TRY
			SET @queryToRun='SELECT [name] FROM msdb.dbo.sysjobs WHERE [name] LIKE ''' + @jobNameFilter + ''' ORDER BY [name]'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun		

			INSERT INTO #msdbSysJobs EXEC (@queryToRun)
		END TRY
		BEGIN CATCH
			INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectSQLServerAgentJobsStatus'
							, ERROR_MESSAGE()		
		END CATCH				


		DECLARE crsJobs CURSOR FOR	SELECT REPLACE([name] , '''', '''''')
									FROM #msdbSysJobs
		OPEN crsJobs
		FETCH NEXT FROM crsJobs INTO @jobName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage				= NULL
				SET @currentRunning			= NULL
				SET @lastExecutionStatus	= NULL
				SET @lastExecutionDate		= NULL
				SET @lastExecutionTime 		= NULL

				BEGIN TRY
					EXEC dbo.usp_sqlAgentJobCheckStatus		@sqlServerName			= @sqlServerName,
															@jobName				= @jobName,
															@strMessage				= @strMessage OUT,
															@currentRunning			= @currentRunning OUT,
															@lastExecutionStatus	= @lastExecutionStatus OUT,
															@lastExecutionDate		= @lastExecutionDate OUT,
															@lastExecutionTime 		= @lastExecutionTime OUT,
															@runningTimeSec			= @runningTimeSec OUT,
															@selectResult			= 0,
															@extentedStepDetails	= @collectStepDetails,		
															@debugMode				= @debugMode

					INSERT	INTO [health-check].[statsSQLServerAgentJobsHistory]([instance_id], [project_id], [event_date_utc], [job_name], [message], [last_execution_status], [last_execution_date], [last_execution_time], [running_time_sec])
							SELECT	  @instanceID, @projectID, GETUTCDATE(), @jobName, @strMessage
									, @lastExecutionStatus, @lastExecutionDate, @lastExecutionTime
									, @runningTimeSec
				END TRY
				BEGIN CATCH
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
								  , @projectID
								  , GETUTCDATE()
								  , 'dbo.usp_hcCollectSQLServerAgentJobsStatus'
								  , ERROR_MESSAGE()
				END CATCH
				FETCH NEXT FROM crsJobs INTO @jobName
			end
		CLOSE crsJobs
		DEALLOCATE crsJobs
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO


