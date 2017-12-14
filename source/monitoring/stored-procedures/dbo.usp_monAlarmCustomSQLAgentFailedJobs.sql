RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomSQLAgentFailedJobs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomSQLAgentFailedJobs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomSQLAgentFailedJobs]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomSQLAgentFailedJobs]
		  @projectCode			[varchar](32)=NULL
		, @sqlServerNameFilter	[sysname]='%'
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.02.2016
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

SET @executionLevel = 0
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
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
	end

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Generate internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @currentInstanceID	[int]

SELECT	@currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[project_id] = @projectID
		AND cin.[name] = @@SERVERNAME

/* save the execution history */
INSERT	INTO [dbo].[jobExecutionHistory]([instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
										 [job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
										 [running_time_sec], [log_message], [status], [event_date_utc])
		SELECT	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id], 
				[job_name], [job_step_name], [job_database_name], [job_command], [execution_date], 
				[running_time_sec], [log_message], [status], [event_date_utc]
		FROM [dbo].[jobExecutionQueue]
		WHERE [project_id] = @projectID
				AND [instance_id] = @currentInstanceID
				AND [module] = 'monitoring'
				AND [descriptor] = 'usp_monAlarmCustomSQLAgentFailedJobs'
				AND [status] <> -1

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'usp_monAlarmCustomSQLAgentFailedJobs'


INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
										[job_name], [job_step_name], [job_database_name], [job_command])
		SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monAlarmCustomSQLAgentFailedJobs', NULL, cin.[id],
				'dbaTDPMon - usp_monAlarmCustomSQLAgentFailedJobs - ' + REPLACE(cin.[name], '\', '$'), 'Run Analysis', DB_NAME()
				, N'EXEC dbo.usp_monGetSQLAgentFailedJobs @projectCode = ''' + @projectCode + N''', @sqlServerNameFilter = ''' + cin.[name] + N''''
		FROM	[dbo].[catalogInstanceNames] cin
		WHERE	cin.[active] = 1
						AND cin.[project_id] = @projectID

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Running internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monAlarmCustomSQLAgentFailedJobs',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

GO
