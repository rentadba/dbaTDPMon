USE [dbaTDPMon]
GO

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2016.06.24' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

/* common module */
RAISERROR('Create procedure: [dbo].[usp_changeServerConfigurationOption]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_changeServerConfigurationOption]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_changeServerConfigurationOption]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_changeServerConfigurationOption]
		@sqlServerName		[sysname],
		@configOptionName	[sysname],
		@configOptionValue	[int],
		@optionIsAvailable	[bit] OUTPUT,
		@optionCurrentValue	[int] OUTPUT,
		@optionHasChanged	[bit] OUTPUT,
		@executionLevel		[tinyint] = 0,
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
WITH RECOMPILE
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.04.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](512)	-- used for dynamic statements
		, @queryParameters			[nvarchar](512)
		, @nestedExecutionLevel		[tinyint]
	
DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)


-----------------------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
		(
			[config_name]	[sysname]		NULL,
			[minimum]		[sql_variant]	NULL,
			[maximum]		[sql_variant]	NULL,
			[config_value]	[sql_variant]	NULL,
			[run_value]		[sql_variant]	NULL
		)

-----------------------------------------------------------------------------------------
--get destination server running version/edition
SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

-----------------------------------------------------------------------------------------
SET @optionCurrentValue=0
SET @optionIsAvailable=0
SET @optionHasChanged=0

SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'EXEC master.dbo.sp_configure'
	
IF @sqlServerName<>@@SERVERNAME
	begin
		IF @serverVersionNum < 11
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
		ELSE
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([name] [nvarchar](70), [minimum] [sql_variant], [maximum] [sql_variant], [config_value] [sql_variant], [run_value] [sql_variant]))'')'
	end

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

INSERT	INTO #serverPropertyConfig--([config_name], [minimum], [maximum], [config_value], [run_value])
		EXEC (@queryToRun)

SET @queryToRun = N'SELECT   @optionIsAvailable = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
						   , @optionCurrentValue = MAX(CAST(config_value AS [int]))
					FROM #serverPropertyConfig
					WHERE [config_name] = @configOptionName'
SET @queryParameters = N'@optionIsAvailable [bit] OUTPUT, @optionCurrentValue [int] OUTPUT, @configOptionName [sysname]'

EXEC sp_executesql @queryToRun, @queryParameters, @configOptionName = @configOptionName
												, @optionIsAvailable = @optionIsAvailable OUT
												, @optionCurrentValue = @optionCurrentValue OUT

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @optionIsAvailable=1 AND ISNULL(@optionCurrentValue, 0) <> @configOptionValue
	begin
		--changing option value and run reconfigure
		SET @queryToRun  = N'sp_executesql N''sp_configure ''''' + @configOptionName + N''''', ' + CAST(@configOptionValue AS [nvarchar](32)) + N'''; RECONFIGURE WITH OVERRIDE;'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
											@module			= 'dbo.usp_changeServerConfigurationOption',
											@eventName		= 'configuration option change',
											@queryToRun  	= @queryToRun,
											@flgOptions		= 0,
											@executionLevel	= @nestedExecutionLevel,
											@debugMode		= @debugMode

		--check the new value
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'EXEC master.dbo.sp_configure'

		IF @sqlServerName<>@@SERVERNAME
			begin
				IF @serverVersionNum < 11
					SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
				ELSE
					SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([name] [nvarchar](70), [minimum] [sql_variant], [maximum] [sql_variant], [config_value] [sql_variant], [run_value] [sql_variant]))'')'
			end

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig--([name], [minimum], [maximum], [config_value], [run_value])
				EXEC (@queryToRun)

		SET @queryToRun = N'SELECT @optionCurrentValue = config_value
							FROM #serverPropertyConfig
							WHERE [config_name] = @configOptionName'
		SET @queryParameters = N' @optionCurrentValue [int] OUTPUT, @configOptionName [sysname]'

		EXEC sp_executesql @queryToRun, @queryParameters, @configOptionName = @configOptionName
														, @optionCurrentValue = @optionCurrentValue OUT


		IF ISNULL(@optionCurrentValue, 0) = @configOptionValue
			SET	@optionHasChanged = 1
	end
GO


RAISERROR('Create procedure: [dbo].[usp_jobQueueGetStatus]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_jobQueueGetStatus]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_jobQueueGetStatus]
GO

CREATE PROCEDURE dbo.usp_jobQueueGetStatus
		@projectCode			[varchar](32) = NULL,
		@moduleFilter			[varchar](32) = '%',
		@descriptorFilter		[varchar](256)= '%',
		@waitForDelay			[varchar](8) = '00:00:30',
		@minJobToRunBeforeExit	[smallint] = 0,
		@executionLevel			[tinyint] = 0,
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
		, @sqlServerName			[sysname]
		, @jobDBName				[sysname]
		, @jobQueueID				[int]
		, @runningJobs				[smallint]

		, @strMessage				[varchar](8000)	
		, @currentRunning			[int]
		, @lastExecutionStatus		[int]
		, @lastExecutionDate		[varchar](10)
		, @lastExecutionTime 		[varchar](8)
		, @runningTimeSec			[bigint]
		, @queryToRun				[nvarchar](max)


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
SELECT @runningJobs = COUNT(*)
FROM [dbo].[vw_jobExecutionQueue]
WHERE  [project_id] = @projectID 
		AND [module] LIKE @moduleFilter
		AND [descriptor] LIKE @descriptorFilter
		AND [status]=4

WHILE (@runningJobs >= @minJobToRunBeforeExit AND @minJobToRunBeforeExit <> 0) OR (@runningJobs > @minJobToRunBeforeExit AND @minJobToRunBeforeExit = 0)
	begin
		---------------------------------------------------------------------------------------------------
		/* check running job status and make updates */
		SET @runningJobs = 0

		DECLARE crsRunningJobs CURSOR FOR	SELECT  [id], [instance_name], [job_name]
											FROM [dbo].[vw_jobExecutionQueue]
											WHERE  [project_id] = @projectID 
													AND [module] LIKE @moduleFilter
													AND [descriptor] LIKE @descriptorFilter
													AND [status]=4
											ORDER BY [id]
		OPEN crsRunningJobs
		FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage			= NULL
				SET @currentRunning		= NULL
				SET @lastExecutionStatus= NULL
				SET @lastExecutionDate	= NULL
				SET @lastExecutionTime 	= NULL
				SET @runningTimeSec		= NULL

				EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
													@jobName				= @jobName,
													@strMessage				= @strMessage OUTPUT,
													@currentRunning			= @currentRunning OUTPUT,
													@lastExecutionStatus	= @lastExecutionStatus OUTPUT,
													@lastExecutionDate		= @lastExecutionDate OUTPUT,
													@lastExecutionTime 		= @lastExecutionTime OUTPUT,
													@runningTimeSec			= @runningTimeSec OUTPUT,
													@selectResult			= 0,
													@extentedStepDetails	= 0,		
													@debugMode				= @debugMode

				IF @currentRunning = 0 AND @lastExecutionStatus<>5 /* Unknown */
					begin
						--double check
						WAITFOR DELAY '00:00:01'						
						EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
															@jobName				= @jobName,
															@strMessage				= @strMessage OUTPUT,
															@currentRunning			= @currentRunning OUTPUT,
															@lastExecutionStatus	= @lastExecutionStatus OUTPUT,
															@lastExecutionDate		= @lastExecutionDate OUTPUT,
															@lastExecutionTime 		= @lastExecutionTime OUTPUT,
															@runningTimeSec			= @runningTimeSec OUTPUT,
															@selectResult			= 0,
															@extentedStepDetails	= 0,		
															@debugMode				= @debugMode
						IF @currentRunning = 0 AND @lastExecutionStatus<>5 /* Unknown */
							begin
								
								IF @lastExecutionStatus = 0 /* failed */
									SET @strMessage = CASE	WHEN CHARINDEX('--Job execution return this message: ', @strMessage) > 0
															THEN SUBSTRING(@strMessage, CHARINDEX('--Job execution return this message: ', @strMessage) + 37, LEN(@strMessage))
															ELSE @strMessage
													  END
								ELSE
									SET @strMessage=NULL

								UPDATE [dbo].[jobExecutionQueue]
									SET [status] = @lastExecutionStatus,
										[execution_date] = CONVERT([datetime], @lastExecutionDate + ' ' + @lastExecutionTime, 120),
										[running_time_sec] = @runningTimeSec,
										[log_message] = @strMessage
								WHERE [id] = @jobQueueID

								/* removing job */
								EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
																@jobName		= @jobName,
																@operation		= 'Clean',
																@dbName			= @jobDBName, 
																@jobStepName 	= '',
																@debugMode		= @debugMode
							end
						ELSE
							begin
								IF @currentRunning <> 0
									SET @runningJobs = @runningJobs + 1
							end
					end
				ELSE
					IF @currentRunning <> 0
						SET @runningJobs = @runningJobs + 1

				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
			end
		CLOSE crsRunningJobs
		DEALLOCATE crsRunningJobs

		SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
						
		IF @runningJobs > @minJobToRunBeforeExit
			WAITFOR DELAY @waitForDelay
	end

IF @minJobToRunBeforeExit=0
	begin
		SET @strMessage='Performing cleanup...'
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		SET @queryToRun = N''
		SET @queryToRun = 'SELECT [name] FROM [msdb].[dbo].[sysjobs]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		IF OBJECT_ID('tempdb..#existingSQLAgentJobs') IS NOT NULL DROP TABLE #existingSQLAgentJobs
		CREATE TABLE #existingSQLAgentJobs
			(
				[job_name] [sysname]
			)

		INSERT	INTO #existingSQLAgentJobs([job_name])
				EXEC (@queryToRun)

		SET @runningJobs = 0
		DECLARE crsRunningJobs CURSOR FOR	SELECT  jeq.[id], jeq.[instance_name], jeq.[job_name]
											FROM [dbo].[vw_jobExecutionQueue] jeq
											INNER JOIN #existingSQLAgentJobs esaj ON esaj.[job_name] = jeq.[job_name]
											WHERE  jeq.[project_id] = @projectID 
													AND jeq.[module] LIKE @moduleFilter
													AND jeq.[descriptor] LIKE @descriptorFilter
													AND jeq.[status]<>-1
											ORDER BY jeq.[id]
		OPEN crsRunningJobs
		FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage			= NULL
				SET @currentRunning		= NULL
				SET @lastExecutionStatus= NULL
				SET @lastExecutionDate	= NULL
				SET @lastExecutionTime 	= NULL
				SET @runningTimeSec		= NULL

				EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @sqlServerName,
													@jobName				= @jobName,
													@strMessage				= @strMessage OUTPUT,
													@currentRunning			= @currentRunning OUTPUT,
													@lastExecutionStatus	= @lastExecutionStatus OUTPUT,
													@lastExecutionDate		= @lastExecutionDate OUTPUT,
													@lastExecutionTime 		= @lastExecutionTime OUTPUT,
													@runningTimeSec			= @runningTimeSec OUTPUT,
													@selectResult			= 0,
													@extentedStepDetails	= 0,		
													@debugMode				= @debugMode

				IF @currentRunning = 0
					begin
						/* removing job */
						EXEC [dbo].[usp_sqlAgentJob]	@sqlServerName	= @sqlServerName,
														@jobName		= @jobName,
														@operation		= 'Clean',
														@dbName			= @jobDBName, 
														@jobStepName 	= '',
														@debugMode		= @debugMode
					end
				ELSE
					SET @runningJobs = @runningJobs + 1

				FETCH NEXT FROM crsRunningJobs INTO @jobQueueID, @sqlServerName, @jobName
			end
		CLOSE crsRunningJobs
		DEALLOCATE crsRunningJobs

		SET @strMessage='Currently running jobs : ' + CAST(@runningJobs AS [varchar])
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
	end

RETURN @runningJobs
GO


RAISERROR('Create procedure: [dbo].[usp_removeFromCatalog]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_removeFromCatalog]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_removeFromCatalog]
GO

CREATE PROCEDURE [dbo].[usp_removeFromCatalog]
		@projectCode		[varchar](32),
		@sqlServerName		[sysname],
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 20.02.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @returnValue			[smallint]
		, @errMessage			[nvarchar](4000)

DECLARE   @projectID			[smallint]
		, @instanceID			[smallint]
		, @machineID			[smallint]
		
-- { sql_statement | statement_block }
BEGIN TRY
	SET @returnValue=1

	-----------------------------------------------------------------------------------------------------
	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'The value specifief for Project Code is not valid.'
			RAISERROR(@errMessage, 16, 1) WITH NOWAIT
		end

	SELECT   @instanceID = [id]
		   , @machineID = [machine_id]
	FROM [dbo].[catalogInstanceNames]
	WHERE [project_id] = @projectID
		AND [name] = @sqlServerName

	IF @instanceID IS NULL
		begin
			SET @errMessage=N'The value specifief for SQL Server Instance Name is not valid.'
			RAISERROR(@errMessage, 16, 1) WITH NOWAIT
		end

	BEGIN TRANSACTION
		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionQueue jeq
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = jeq.[project_id] AND cin.[id] = jeq.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionQueue jeq
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = jeq.[project_id] AND cin.[id] = jeq.[for_instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID
	
		-----------------------------------------------------------------------------------------------------
		DELETE lem
		FROM dbo.logEventMessages lem
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = lem.[project_id] AND cin.[id] = lem.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE lsam
		FROM dbo.logAnalysisMessages lsam
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = lsam.[project_id] AND cin.[id] = lsam.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE sosel
		FROM [health-check].statsOSEventLogs sosel
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sosel.[project_id] AND cin.[id] = sosel.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE sseld
		FROM [health-check].statsSQLServerErrorlogDetails sseld
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sseld.[project_id] AND cin.[id] = sseld.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE ssajh
		FROM [health-check].statsSQLServerAgentJobsHistory ssajh
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = ssajh.[project_id] AND cin.[id] = ssajh.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE shcdsi
		FROM [health-check].statsDiskSpaceInfo shcdsi
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = shcdsi.[project_id] AND cin.[id] = shcdsi.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE shcdd
		FROM [health-check].statsDatabaseDetails shcdd
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = shcdd.[instance_id] AND cdb.[id] = shcdd.[catalog_database_id]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE ssaj
		FROM [monitoring].statsSQLAgentJobs ssaj
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = ssaj.[project_id] AND cin.[id] = ssaj.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID				
				
		-----------------------------------------------------------------------------------------------------
		DELETE FROM  dbo.catalogDatabaseNames 
		WHERE [project_id] = @projectID
				AND [instance_id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE FROM  dbo.catalogInstanceNames
		WHERE [project_id] = @projectID
				AND [id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		IF NOT EXISTS(	SELECT * FROM dbo.catalogInstanceNames
						WHERE [project_id] = @projectID
							AND [machine_id] = @machineID
					)
			DELETE FROM  dbo.catalogMachineNames
			WHERE [project_id] = @projectID
					AND [id] = @machineID

	COMMIT
END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    
    
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
       IF @@TRANCOUNT >0 ROLLBACK TRANSACTION 
END CATCH

RETURN @returnValue
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
		@ErrMessage					[nvarchar](1024),
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
												li = 'duration: ' + CAST([duration_seconds] AS [nvarchar](32)) + ' seconds', '',
												li = 'event-date (utc): ' + CAST([event_date_utc] AS [varchar](32)), ''
										FROM (
												SELECT  *
												FROM    OPENXML(@handle, '/alert/detail', 2)  
														WITH (
																[error_code]		[int],
																[error_string]		[nvarchar](max),
																[query_executed]	[nvarchar](max),
																[duration_seconds]	[bigint],
																[event_date_utc]	[sysname]
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
		SET @HTMLBody = @HTMLBody + REPLACE(REPLACE(REPLACE(REPLACE(CAST(@eventMessageXML AS [nvarchar](max)), '<alert><detail>', '<ul>'), '</detail></alert>', '</ul>'), '><', '><li><'), '<li></ul>','</ul>')

		DECLARE @strPos		[int],
				@strPos2	[int]

		SET @strPos = CHARINDEX('</', @HTMLBody)
		WHILE @strPos <> 0
			BEGIN
				SET @strPos2 = CHARINDEX('><', @HTMLBody, @strPos)
				SET @HTMLBody = SUBSTRING(@HTMLBody, 1, @strPos+1) + SUBSTRING(@HTMLBody, @strPos2, LEN(@HTMLBody))
	
				SET @strPos = CHARINDEX('</', @HTMLBody, @strPos2)
			END
		SET @HTMLBody = REPLACE(@HTMLBody, '</>', '')
		SET @HTMLBody = REPLACE(@HTMLBody, '><', '>')
		SET @HTMLBody = REPLACE(@HTMLBody, '>', ': ')
		SET @HTMLBody = REPLACE(@HTMLBody, '<li: ', '<li>')
		SET @HTMLBody = REPLACE(@HTMLBody, '<ul: li: ', '<ul><li>')
		SET @HTMLBody = REPLACE(@HTMLBody, '</ul: ', '</ul>')
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
									<TH>Run Duration</TH>
									<TH >Message</TH>' +
								CAST ( ( 
										SELECT	TD = [step_id], '',
												TD = [step_name], '',
												TD = [run_status], '',
												TD = [run_date], '',
												TD = [run_time], '',
												TD = [duration], '',
												TD = [message], ''
										FROM (
												SELECT  *
												FROM    OPENXML(@handle, '/job-history/job-step', 2)  
														WITH (
																[step_id]		[int],
																[step_name]		[sysname],
																[run_status]	[nvarchar](32),
																[run_date]		[nvarchar](32),
																[run_time]		[nvarchar](32),
																[duration]		[nvarchar](32),
																[message]		[nvarchar](max)
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
					SELECT CASE WHEN [run_date] IS NOT NULL AND [run_time] IS NOT NULL
								THEN CONVERT([datetime], ([run_date] + ' ' + [run_time]), 120) 
								ELSE GETDATE()
							END AS [job_step_start_time]
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


		DECLARE @xmlSkippedActions TABLE
			(
				  [database_name]	[sysname]
				, [backup_type]		[nvarchar](32)
				, [date]			[nvarchar](32)
				, [reason]			[nvarchar](512)
			)

		INSERT	INTO @xmlBackupSet([database_name], [type], [start_date], [duration], [size], [size_bytes], [verified], [file_name], [error_code])
				SELECT [database_name], [type], [start_date], [duration], [size], [size_bytes], [verified], [file_name], [error_code]
				FROM (
						SELECT	  ref.value ('database_name[1]', 'sysname') as [database_name]
								, ref.value ('type[1]', 'nvarchar(32)') as [type]
								, ref.value ('start_date[1]', '[datetime]') as [start_date]
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


		INSERT	INTO @xmlSkippedActions ([database_name], [backup_type], [date], [reason])
				SELECT [database_name], [backup_type], [date], [reason]
				FROM (
						SELECT	  ref.value ('affected_object[1]', 'sysname') as [database_name]
								, ref.value ('type[1]', 'nvarchar(32)') as [backup_type]
								, ref.value ('date[1]', '[datetime]') as [date]
								, ref.value ('reason[1]', 'nvarchar(512)') as [reason]
						FROM (
								SELECT	CAST([message] AS [xml]) AS [message_xml]
								FROM	[dbo].[logEventMessages]
								WHERE	[message] LIKE '<skipaction>%'
										AND ISNULL([project_id], 0) = ISNULL(@projectID, 0)
										AND ISNULL([instance_id], 0) = ISNULL(@instanceID, 0)
										AND [event_type]=0
										AND [event_name] = 'database backup'
							)x CROSS APPLY [message_xml].nodes ('//skipaction/detail') R(ref)	
					) sa
				WHERE [date] BETWEEN @jobStartTime AND GETDATE()


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

		IF (SELECT COUNT(*) FROM @xmlSkippedActions)>0
			begin
				SET @HTMLBody =@HTMLBody + N'<br><br>'
				SET @HTMLBody =@HTMLBody + COALESCE(
									N'<TABLE BORDER="1">' +
									N'<TR>' +
										N'	<TH>Database Name</TH>
											<TH>Backup Type</TH>
											<TH>Date</TH>
											<TH>Reason</TH>' +
										CAST ( ( 
												SELECT	TD = [database_name], '',
														TD = [backup_type], '',
														TD = [date], '',
														TD = [reason], ''
												FROM (
														SELECT	TOP (100) PERCENT *
														FROM @xmlSkippedActions							
														ORDER BY [database_name]
													)x
												FOR XML PATH('TR'), TYPE 
									) AS NVARCHAR(MAX) ) +
									N'</TABLE>', '') ;
			end

		--if any of the backups had failed, send notification
		IF @additionalOption=0
			SELECT @additionalOption = COUNT(*)
			FROM @xmlBackupSet
			WHERE [error_code]<>0
	end


SET @HTMLBody = REPLACE(@HTMLBody, N'&amp;', N'&')
SET @HTMLBody = REPLACE(@HTMLBody, N'&amp;lt;', N'<')
SET @HTMLBody = REPLACE(@HTMLBody, N'&amp;gt;', N'>')


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



RAISERROR('Create procedure: [dbo].[usp_sqlAgentJobEmailStatusReport]', 10, 1) WITH NOWAIT
GO---
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_sqlAgentJobEmailStatusReport]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_sqlAgentJobEmailStatusReport]
GO

CREATE PROCEDURE [dbo].[usp_sqlAgentJobEmailStatusReport]
		@sqlServerName			[sysname] = @@SERVERNAME,
		@jobName				[sysname],
		@logFileLocation		[nvarchar](512),
		@module					[varchar](32),
		@sendLogAsAttachment	[bit]=1,
		@eventType				[smallint]=2,
		@currentlyRunning		[bit] = 1,
		@debugMode				[bit] = 0
/* WITH ENCRYPTION */
AS

SET NOCOUNT ON

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 17.03.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE @eventMessageData			[varchar](max),
		@jobID						[uniqueidentifier],
		@strMessage					[nvarchar](512),
		@lastCompletionInstanceID	[int],
		@queryToRun					[nvarchar](max),
		@queryParams				[nvarchar](1024)

-----------------------------------------------------------------------------------------------------
--get job id
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'SELECT [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' + @jobName + ''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT @jobID = [job_id] FROM (' + @queryToRun + N')inq'
SET @queryParams = '@jobID [uniqueidentifier] OUTPUT'

IF @debugMode=1	PRINT @queryToRun
EXEC sp_executesql @queryToRun, @queryParams, @jobID = @jobID OUTPUT

-----------------------------------------------------------------------------------------------------
--get last instance_id when job completed
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'SELECT MAX(h.[instance_id]) AS [instance_id]
					FROM [msdb].[dbo].[sysjobs] j 
					RIGHT JOIN [msdb].[dbo].[sysjobhistory] h ON j.[job_id] = h.[job_id] 
					WHERE	j.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
							AND h.[step_name] =''(Job outcome)'''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

SET @queryToRun = N'SELECT @lastCompletionInstanceID = [instance_id] FROM (' + @queryToRun + N')inq'
SET @queryParams = '@lastCompletionInstanceID [int] OUTPUT'

IF @debugMode=1	PRINT @queryToRun
EXEC sp_executesql @queryToRun, @queryParams, @lastCompletionInstanceID = @lastCompletionInstanceID OUTPUT

SET @lastCompletionInstanceID = ISNULL(@lastCompletionInstanceID, 0)
-----------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#jobHistory') IS NOT NULL
	DROP TABLE #jobHistory

CREATE TABLE #jobHistory
	(
		  [step_id]			[int]
		, [step_name]		[sysname]
		, [run_status]		[nvarchar](32)
		, [run_date]		[nvarchar](32)
		, [run_time]		[nvarchar](32)
		, [duration]		[nvarchar](32)
		, [message]			[nvarchar](max)
		, [log_filename]	[nvarchar](512)
	)

SET @queryToRun = N''
IF @currentlyRunning = 0
	SET @queryToRun = @queryToRun + N'
			SELECT	[step_id]
					, [step_name]
					, [run_status]
					, SUBSTRING([run_date], 1, 4) + ''-'' + SUBSTRING([run_date], 5 ,2) + ''-'' + SUBSTRING([run_date], 7 ,2) AS [run_date]
					, SUBSTRING([run_time], 1, 2) + '':'' + SUBSTRING([run_time], 3, 2) + '':'' + SUBSTRING([run_time], 5, 2) AS [run_time]
					, SUBSTRING([run_duration], 1, 2) + ''h '' + SUBSTRING([run_duration], 3, 2) + ''m '' + SUBSTRING([run_duration], 5, 2) + ''s'' AS [duration]
					, [message]
					, [output_file_name]
			FROM (		
					SELECT    h.[step_id]
							, h.[step_name]
							, CASE h.[run_status]	WHEN ''0'' THEN ''Failed''
													WHEN ''1'' THEN ''Succeded''	
													WHEN ''2'' THEN ''Retry''
													WHEN ''3'' THEN ''Canceled''
													WHEN ''4'' THEN ''In progress''
													ELSE ''Unknown''
								END [run_status]
							, CAST(h.[run_date] AS varchar) AS [run_date]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_time] AS varchar))) + CAST(h.[run_time] AS varchar) AS [run_time]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_duration] AS varchar))) + CAST(h.[run_duration] AS varchar) AS [run_duration]
							, CASE WHEN [run_status] IN (0, 2) THEN LEFT(h.[message], 256) ELSE '''' END AS [message]
							, sjs.[output_file_name]
					FROM [msdb].[dbo].[sysjobhistory] h
					LEFT JOIN [msdb].[dbo].[sysjobsteps] sjs ON sjs.[job_id] = h.[job_id] AND sjs.[step_id] = h.[step_id]
					WHERE	 h.[instance_id] < (
												SELECT TOP 1 [instance_id] 
												FROM (	
														SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
														FROM [msdb].[dbo].[sysjobhistory] h
														WHERE	h.[job_id]= ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																AND h.[step_name] =''(Job outcome)''
														ORDER BY h.[instance_id] DESC
													)A
												) 
							AND	h.[instance_id] > ISNULL(
												( SELECT [instance_id] 
												FROM (	
														SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
														FROM [msdb].[dbo].[sysjobhistory] h
														WHERE	h.[job_id]= ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																AND h.[step_name] =''(Job outcome)''
														ORDER BY h.[instance_id] DESC
													)A
												WHERE [instance_id] NOT IN 
													(
													SELECT TOP 1 [instance_id] 
													FROM (	SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
															FROM [msdb].[dbo].[sysjobhistory] h
															WHERE	h.[job_id]= ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																	AND h.[step_name] =''(Job outcome)''
															ORDER BY h.[instance_id] DESC
														)A
													)),0)
							AND h.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
				)A'
ELSE
	SET @queryToRun = @queryToRun + N'
			SELECT	[step_id]
					, [step_name]
					, [run_status]
					, SUBSTRING([run_date], 1, 4) + ''-'' + SUBSTRING([run_date], 5 ,2) + ''-'' + SUBSTRING([run_date], 7 ,2) AS [run_date]
					, SUBSTRING([run_time], 1, 2) + '':'' + SUBSTRING([run_time], 3, 2) + '':'' + SUBSTRING([run_time], 5, 2) AS [run_time]
					, SUBSTRING([run_duration], 1, 2) + ''h '' + SUBSTRING([run_duration], 3, 2) + ''m '' + SUBSTRING([run_duration], 5, 2) + ''s'' AS [duration]
					, [message]
					, [output_file_name]
			FROM (		
					SELECT    h.[step_id]
							, h.[step_name]
							, CASE h.[run_status]	WHEN ''0'' THEN ''Failed''
													WHEN ''1'' THEN ''Succeded''	
													WHEN ''2'' THEN ''Retry''
													WHEN ''3'' THEN ''Canceled''
													WHEN ''4'' THEN ''In progress''
													ELSE ''Unknown''
								END [run_status]
							, CAST(h.[run_date] AS varchar) AS [run_date]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_time] AS varchar))) + CAST(h.[run_time] AS varchar) AS [run_time]
							, REPLICATE(''0'', 6 - LEN(CAST(h.[run_duration] AS varchar))) + CAST(h.[run_duration] AS varchar) AS [run_duration]
							, CASE WHEN [run_status] IN (0, 2) THEN LEFT(h.[message], 256) ELSE '''' END AS [message]
							, sjs.[output_file_name]
					FROM [msdb].[dbo].[sysjobhistory] h
					LEFT JOIN [msdb].[dbo].[sysjobsteps] sjs ON sjs.[job_id] = h.[job_id] AND sjs.[step_id] = h.[step_id]
					WHERE	 h.[instance_id] > ISNULL((
														SELECT TOP 1 [instance_id] 
														FROM (	
																SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																FROM [msdb].[dbo].[sysjobhistory] h
																WHERE	h.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
																		AND h.[step_name] =''(Job outcome)''
																ORDER BY h.[instance_id] DESC
															)A
														), 0)
							AND h.[job_id] = ''' + CAST(@jobID AS [nvarchar](36)) + N'''
				)A'

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

IF @debugMode=1	PRINT @queryToRun
INSERT	INTO #jobHistory([step_id], [step_name], [run_status], [run_date], [run_time], [duration], [message], [log_filename])
		EXEC (@queryToRun)

-----------------------------------------------------------------------------------------------------
SET @eventMessageData = ''
SELECT @eventMessageData = @eventMessageData + 
							'<job-step>' + 
							'<step_id>' + CAST(ISNULL([step_id], 0) AS [varchar](32)) + '</step_id>' + 
							'<step_name>' + REPLACE(ISNULL([step_name], ''), '&', '&amp;') + '</step_name>' + 
							'<run_status>' + ISNULL([run_status], '') + '</run_status>' + 
							'<run_date>' + ISNULL([run_date], '') + '</run_date>' + 
							'<run_time>' + ISNULL([run_time], '') + '</run_time>' + 
							'<duration>' + ISNULL([duration], '') + '</duration>' +
							'<message>' + REPLACE(ISNULL([message], ''), '&', '&amp;') + '</message>' +
							'</job-step>'
FROM #jobHistory

SET @eventMessageData = '<job-history>' + @eventMessageData + '</job-history>'


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @sendLogAsAttachment=0
	SET @logFileLocation = NULL
ELSE
	IF @logFileLocation IS NULL
		SELECT TOP 1 @logFileLocation = [log_filename]
		FROM #jobHistory
		WHERE [run_status] = 'Failed'
		ORDER BY [step_id]

/* check if @logFileLocation exists	*/
IF @logFileLocation IS NOT NULL
	begin
		IF object_id('#fileExists') IS NOT NULL DROP TABLE #fileExists
		CREATE TABLE #fileExists
					(
						[file_exists]				[bit]	NULL,
						[file_is_directory]			[bit]	NULL,
						[parent_directory_exists]	[bit]	NULL
					)

		DECLARE	  @serverEdition			[sysname]
				, @serverVersionStr			[sysname]
				, @serverVersionNum			[numeric](9,6)
				, @nestedExecutionLevel		[tinyint]

		EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
												@serverEdition			= @serverEdition OUT,
												@serverVersionStr		= @serverVersionStr OUT,
												@serverVersionNum		= @serverVersionNum OUT,
												@executionLevel			= @nestedExecutionLevel,
												@debugMode				= @debugMode

		IF @sqlServerName=@@SERVERNAME
				SET @queryToRun = N'master.dbo.xp_fileexist ''' + @logFileLocation + ''''
		else
			IF @serverVersionNum<11
				SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC master.dbo.xp_fileexist ''''' + @logFileLocation + ''''';'')x'
			ELSE
				SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''master.dbo.xp_fileexist ''''''''' + @logFileLocation + ''''''''' '''') WITH RESULT SETS(([File Exists] [int], [File is a Directory] [int], [Parent Directory Exists] [int])) '')x'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		INSERT	INTO #fileExists([file_exists], [file_is_directory], [parent_directory_exists])
				EXEC (@queryToRun)

		IF (SELECT [file_exists] FROM #fileExists)=0
			SET @logFileLocation = NULL
	end
/*-------------------------------------------------------------------------------------------------------------------------------*/

--if one of the job steps failed, will fail the job
DECLARE @failedSteps [int]

SELECT @failedSteps = COUNT(*)
FROM #jobHistory
WHERE [run_status] = 'Failed'

EXEC [dbo].[usp_logEventMessageAndSendEmail] @projectCode		= NULL,
											 @sqlServerName		= @sqlServerName,
											 @objectName		= @jobName,
											 @module			= @module,
											 @eventName			= 'sql agent job status',
											 @parameters		= @logFileLocation,
											 @eventMessage		= @eventMessageData,
											 @recipientsList	= NULL,
											 @eventType			= @eventType,
											 @additionalOption	= @failedSteps

IF @failedSteps <> 0
	begin
		SET @strMessage = 'Job execution failed. See individual steps status.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=1
	end
GO



/* health-check module */
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
		@module					[varchar](32)='health-check',
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
				AND [module] = @module

		------------------------------------------------------------------------------------------------------------------------------------------
		IF @codeDescriptor = 'dbo.usp_hcCollectDatabaseDetails'
			begin
				INSERT	INTO [dbo].[jobExecutionQueue](  [instance_id], [project_id], [module], [descriptor]
													   , [for_instance_id], [job_name], [job_step_name], [job_database_name]
													   , [job_command])
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectDatabaseDetails' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectSQLServerAgentJobsStatus' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectDiskSpaceUsage' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectErrorlogMessages' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'usp_hcCollectEventMessages' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END + ' - ' + @projectCode AS [job_name],
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
						SELECT	@instanceID AS [instance_id], @projectID AS [project_id], @module AS [module], @codeDescriptor AS [descriptor], L.[log_type_name],
								X.[instance_id] AS [for_instance_id], 
								DB_NAME() + ' - ' + 'hcCollectOSEventLogs' + CASE WHEN X.[instance_name] <> '%' THEN ' - ' + X.[instance_name] ELSE '' END  + ' (' + L.[log_type_name] + ')' + ' - ' + @projectCode AS [job_name],
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



RAISERROR('Create procedure: [dbo].[usp_reportHTMLBuildHealthCheck]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_reportHTMLBuildHealthCheck]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_reportHTMLBuildHealthCheck]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_reportHTMLBuildHealthCheck]
		@projectCode			[varchar](32)=NULL,
		@flgActions				[int]			= 63,		/*	1 - Instance Availability 
																2 - Databases status
																4 - SQL Server Agent Job status
																8 - Disk Space information
															   16 - Errorlog messages
															   32 - OS Event messages
															*/
		@flgOptions				[int]			= 266338303,/*	 1 - Instances - Offline
																 2 - Instances - Online
																 4 - Databases Status - Issues Detected
																 8 - Databases Status - Complete Details
																16 - SQL Server Agent Jobs - Job Failures
																32 - SQL Server Agent Jobs - Permissions errors
																64 - SQL Server Agent Jobs - Complete Details
															   128 - Big Size for System Databases - Issues Detected
															   256 - Databases Status - Permissions errors
															   512 - Databases with Auto Close / Shrink - Issues Detected
															  1024 - Big Size for Database Log files - Issues Detected
															  2048 - Low Usage of Data Space - Issues Detected
															  4096 - Log vs. Data - Allocated Size - Issues Detected
															  8192 - Outdated Backup for Databases - Issues Detected
															 16384 - Outdated DBCC CHECKDB Databases - Issues Detected
															 32768 - High Usage of Log Space - Issues Detected
															 65536 - Disk Space Information - Complete Detais
														    131072 - Disk Space Information - Permission errors
														    262144 - Low Free Disk Space - Issues Detected
															524288 - Errorlog messages - Permission errors
														   1048576 - Errorlog messages - Issues Detected
														   2097152 - Errorlog messages - Complete Details
														   4194304 - Databases with Fixed File(s) Size - Issues Detected													
														   8388608 - Databases with (Page Verify not CHECKSUM) or (Page Verify is NONE)
														  16777216 - Frequently Fragmented Indexes (consider lowering the fill-factor)
														  33554432 - SQL Server Agent Jobs - Long Running SQL Agent Jobs
														  67108864 - OS Event messages - Permission errors
														 134217728 - OS Event messages - Complete Details
															*/
		@reportDescription		[nvarchar](256) = NULL,
		@reportFileName			[nvarchar](max) = NULL,	/* if file name is null, than the name will be generated */
		@localStoragePath		[nvarchar](260) = NULL,
		@dbMailProfileName		[sysname]		= NULL,		
		@recipientsList			[nvarchar](1024)= NULL,
		@sendReportAsAttachment	[bit]			= 0		/* if set to 1, the report file will always be attached */
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @HTMLReport							[nvarchar](max)
		, @HTMLReportArea						[nvarchar](max)
		, @CSSClass								[nvarchar](max)
		, @tmpHTMLReport						[nvarchar](max)
		, @file_attachments						[nvarchar](1024)
		
		, @ReturnValue							[int]
		, @ErrMessage							[nvarchar](256)
		, @idx									[int]

DECLARE   @queryToRun							[nvarchar](max)

DECLARE   @reportID								[int]
		, @HTMLReportFileName					[nvarchar](260)
		, @reportFilePath						[nvarchar](260)
		, @relativeStoragePath					[nvarchar](260)
		, @projectID							[int]
		, @projectName							[nvarchar](128)
		, @reportBuildStartTime					[datetime]
	
DECLARE   @databaseName							[sysname]
		, @configAdmittedState					[sysname]
		, @configDBMaxSizeMaster				[int]
		, @configDBMaxSizeMSDB					[int]
		, @configLogMaxSize						[int]
		, @configLogVsDataPercent				[numeric](6,2)
		, @configDataSpaceMinPercent			[numeric](6,2)
		, @configLogSpaceMaxPercent				[numeric](6,2)
		, @configDBMinSizeForAnalysis			[int]
		, @configFailuresInLastHours			[int]
		, @configUserDBCCCHECKDBAgeDays			[int]
		, @configSystemDBCCCHECKDBAgeDays		[int]
		, @configUserDatabaseBACKUPAgeDays		[int]
		, @configSystemDatabaseBACKUPAgeDays	[int]
		, @configFreeDiskMinPercent				[numeric](6,2)
		, @configFreeDiskMinSpace				[int]
		, @configErrorlogMessageLastHours		[int]
		, @configErrorlogMessageLimit			[int]
		, @configMaxJobRunningTimeInHours		[int]
		, @configOSEventMessageLastHours		[int]
		, @configOSEventMessageLimit			[int]
		, @configOSEventGetInformationEvent		[bit]
		, @configOSEventGetWarningsEvent		[bit]
		, @configOSEventsTimeOutSeconds			[int]

		, @logSizeMB							[numeric](20,3)
		, @dataSizeMB							[numeric](18,3)
		, @stateDesc							[nvarchar](64)
		, @dataSpaceUsedPercent					[numeric](6,2)
		, @logSpaceUsedPercent					[numeric](6,2)
		, @reclaimableSpaceMB					[numeric](18,3)
		, @logVSDataPercent						[numeric](20,2)
		, @lastBackupDate						[datetime]
		, @lastCheckDBDate						[datetime]
		, @lastDatabaseEventAgeDays				[int]
		, @logicalDrive							[char](1)
		, @volumeMountPoint						[nvarchar](512)
		, @diskTotalSizeMB						[numeric](18,3)
		, @diskAvailableSpaceMB					[numeric](18,3)
		, @diskPercentAvailable					[numeric](6,2)
		, @dateTimeLowerLimit					[datetime]

		, @messageCount							[int]
		, @issuesDetectedCount					[int]

DECLARE @eventMessageData						[varchar](8000)

/*-------------------------------------------------------------------------------------------------------------------------------*/
-- { sql_statement | statement_block }
BEGIN TRY
	SET @reportBuildStartTime = GETUTCDATE()
	SET @ReturnValue=1
	
	-----------------------------------------------------------------------------------------------------
	--get default project code
	IF @projectCode IS NULL
		SELECT	@projectCode = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Default project code'
				AND [module] = 'common'

	SELECT    @projectID = [id]
			, @projectName = [name]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @ErrMessage=N'The value specifief for Project Code is not valid.'
			RAISERROR(@ErrMessage, 16, 1) WITH NOWAIT
		end
			
	-----------------------------------------------------------------------------------------------------
	SET @ErrMessage='Building Daily Health Check Report for: [' + @projectCode + ']'
	RAISERROR(@ErrMessage, 10, 1) WITH NOWAIT


	-----------------------------------------------------------------------------------------------------
	--generating file name
	-----------------------------------------------------------------------------------------------------
	IF @reportFileName IS NOT NULL AND LEFT(@reportFileName, 1) <> '+'
		SET @HTMLReportFileName = @reportFileName
	ELSE
		SET @HTMLReportFileName = 'Daily_HealthCheck_Report_for_' + REPLACE(@projectName, '\', '_') + '_from_' +
						CONVERT([varchar](8), @reportBuildStartTime, 112)
							+ '_' + LEFT(REPLACE(CONVERT([varchar](8),@reportBuildStartTime, 108), ':', ''), 4)
	
	SET @HTMLReportFileName = REPLACE(@HTMLReportFileName, ' ', '_')

	IF @localStoragePath IS NULL
		EXEC [dbo].[usp_reportHTMLGetStorageFolder]	@projectID					= @projectID,
													@instanceID					= NULL,
													@StartDate					= @reportBuildStartTime,
													@StopDate					= @reportBuildStartTime,
													@flgCreateOutputFolder		= DEFAULT,
													@localStoragePath			= @localStoragePath OUTPUT,
													@relativeStoragePath		= @relativeStoragePath OUTPUT,
													@debugMode					= 0

	-----------------------------------------------------------------------------------------------------
	--reading report options
	-----------------------------------------------------------------------------------------------------
	SELECT	@configAdmittedState = [value]
	FROM	[report].[htmlOptions]
	WHERE	[name] = N'Database online admitted state'
			AND [module] = 'health-check'

	SET @configAdmittedState = ISNULL(@configAdmittedState, 'ONLINE, READ ONLY')
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMaxSizeMaster = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database max size (mb) - master'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDBMaxSizeMaster = 0
	END CATCH
	SET @configDBMaxSizeMaster = ISNULL(@configDBMaxSizeMaster, 0)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMaxSizeMSDB = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database max size (mb) - msdb'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDBMaxSizeMSDB = 0
	END CATCH
	SET @configDBMaxSizeMSDB = ISNULL(@configDBMaxSizeMSDB, 0)
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configLogMaxSize = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Max Log Size (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configLogMaxSize = 32768
	END CATCH
	SET @configLogMaxSize = ISNULL(@configLogMaxSize, 32768)
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDataSpaceMinPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Min Data Usage (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDataSpaceMinPercent = 50
	END CATCH
	SET @configDataSpaceMinPercent = ISNULL(@configDataSpaceMinPercent, 50)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configLogSpaceMaxPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Max Log Usage (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configLogSpaceMaxPercent = 90
	END CATCH
	SET @configLogSpaceMaxPercent = ISNULL(@configLogSpaceMaxPercent, 90)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMinSizeForAnalysis = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Min Size for Analysis (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configDBMinSizeForAnalysis = 512
	END CATCH
	SET @configDBMinSizeForAnalysis = ISNULL(@configDBMinSizeForAnalysis, 512)

	-----------------------------------------------------------------------------------------------------			
	BEGIN TRY
		SELECT	@configLogVsDataPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Database Log vs. Data Size (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configLogVsDataPercent = 50
	END CATCH
	SET @configLogVsDataPercent = ISNULL(@configLogVsDataPercent, 50)
									
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configFailuresInLastHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'SQL Agent Job - Failures in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configFailuresInLastHours = 24
	END CATCH
	SET @configFailuresInLastHours = ISNULL(@configFailuresInLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configUserDatabaseBACKUPAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'User Database BACKUP Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configUserDatabaseBACKUPAgeDays = 2
	END CATCH
	SET @configUserDatabaseBACKUPAgeDays = ISNULL(@configUserDatabaseBACKUPAgeDays, 2)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configSystemDatabaseBACKUPAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'System Database BACKUP Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configSystemDatabaseBACKUPAgeDays = 14
	END CATCH
	SET @configSystemDatabaseBACKUPAgeDays = ISNULL(@configSystemDatabaseBACKUPAgeDays, 14)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configUserDBCCCHECKDBAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'User Database DBCC CHECKDB Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configUserDBCCCHECKDBAgeDays = 30
	END CATCH
	SET @configUserDBCCCHECKDBAgeDays = ISNULL(@configUserDBCCCHECKDBAgeDays, 30)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configSystemDBCCCHECKDBAgeDays = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'System Database DBCC CHECKDB Age (days)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configSystemDBCCCHECKDBAgeDays = 90
	END CATCH
	SET @configSystemDBCCCHECKDBAgeDays = ISNULL(@configSystemDBCCCHECKDBAgeDays, 90)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configFreeDiskMinPercent = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Free Disk Space Min Percent (percent)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configFreeDiskMinPercent = 10
	END CATCH
	SET @configFreeDiskMinPercent = ISNULL(@configFreeDiskMinPercent, 10)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configFreeDiskMinSpace = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Free Disk Space Min Space (mb)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configFreeDiskMinSpace = 3000
	END CATCH
	SET @configFreeDiskMinSpace = ISNULL(@configFreeDiskMinSpace, 3000)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configErrorlogMessageLastHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Errorlog Messages in last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configErrorlogMessageLastHours = 24
	END CATCH
	SET @configErrorlogMessageLastHours = ISNULL(@configErrorlogMessageLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configErrorlogMessageLimit = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'Errorlog Messages Limit to Max'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configErrorlogMessageLimit = 1000
	END CATCH
	SET @configErrorlogMessageLimit = ISNULL(@configErrorlogMessageLimit, 1000)

	IF @configErrorlogMessageLimit= 0 SET @configErrorlogMessageLimit=2147483647

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configMaxJobRunningTimeInHours = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'SQL Agent Job - Maximum Running Time (hours)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configMaxJobRunningTimeInHours = 3
	END CATCH
	SET @configMaxJobRunningTimeInHours = ISNULL(@configMaxJobRunningTimeInHours, 3)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configOSEventMessageLastHours = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Collect OS Events from last hours'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventMessageLastHours = 24
	END CATCH
	SET @configOSEventMessageLastHours = ISNULL(@configOSEventMessageLastHours, 24)

	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configOSEventMessageLimit = [value]
		FROM	[report].[htmlOptions]
		WHERE	[name] = N'OS Event Messages Limit to Max'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventMessageLimit = 1000
	END CATCH
	SET @configOSEventMessageLimit = ISNULL(@configOSEventMessageLimit, 1000)

	IF @configOSEventMessageLimit= 0 SET @configOSEventMessageLimit=2147483647
		
	------------------------------------------------------------------------------------------------------------------------------------------
	--option for timeout when fetching OS events
	BEGIN TRY
		SELECT	@configOSEventsTimeOutSeconds = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Collect OS Events timeout (seconds)'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventsTimeOutSeconds = 600
	END CATCH

	SET @configOSEventsTimeOutSeconds = ISNULL(@configOSEventsTimeOutSeconds, 600)
	
	------------------------------------------------------------------------------------------------------------------------------------------
	--option to fetch also information OS events
	BEGIN TRY
		SELECT	@configOSEventGetInformationEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Collect Information OS Events'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventGetInformationEvent = 0
	END CATCH

	SET @configOSEventGetInformationEvent = ISNULL(@configOSEventGetInformationEvent, 0)

	------------------------------------------------------------------------------------------------------------------------------------------
	--option to fetch also warnings OS events
	BEGIN TRY
		SELECT	@configOSEventGetWarningsEvent = CASE WHEN LOWER([value])='true' THEN 1 ELSE 0 END
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = 'Collect Warning OS Events'
				AND [module] = 'health-check'
	END TRY
	BEGIN CATCH
		SET @configOSEventGetWarningsEvent = 0
	END CATCH

	SET @configOSEventGetWarningsEvent = ISNULL(@configOSEventGetWarningsEvent, 0)

		
	
	-----------------------------------------------------------------------------------------------------
	--setting styles used in html report
	-----------------------------------------------------------------------------------------------------
	SET @CSSClass=N''
	SET @CSSClass = @CSSClass + N'
<style type="text/css">
	dummmy
		{
		font-family: Arial, Tahoma; 
		}
	body.normal
		{
		font-family: Arial, Tahoma; 
		margin-top: 0px;
		}
	p.title-style
		{
		font-size:24px; 
		font-weight:bold;
		}
	p.title2-style
		{
		font-size:18px; 
		font-weight:bold;
		}
	p.title3-style
		{
		font-size:14px; 
		}
	p.title4-style
		{
		font-size:12px; 
		font-style:italic;
		}
	p.title5-style
		{
		font-size:12px; 
		}
	p.disclaimer
		{
		font-size:9px; 
		}
	a.category-style
		{
		font-size:20px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.summary-style-title
		{
		font-size:12px; 
		font-weight:bold;
		text-decoration: none;
		color: #000000;
		}
	a.summary-style
		{
		font-size:12px; 
		text-decoration: none;
		}
	a.graphs-style
		{
		font-size:16px; 
		font-weight:bold;
		text-decoration: none;
		}
	a.graphs-summary
		{
		font-size:12px; 
		text-decoration: none;
		}	
	td.small-size
		{
		font-size:10px; 
		}
	td.category-style
		{
		font-size:20px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.summary-style
		{
		font-size:12px; 
		text-decoration: none;
		}
	table.no-border
		{
		border-style: solid; 
		border-width: 0 0 0 0; 
		border-color: #ccc;
		}
	table.with-border
		{
		border-style: solid; 
		border-width: 0 0 1px 1px; 
		border-color: #ccc;
		}
	td.color-1
		{
		background-color: #EDF8FE;
		}
	td.color-2
		{
		background-color: #FFFFFF;
		}
	td.color-3
		{
		background-color: #00AEEF;
		}
	td.color-alert-warning
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		background-color: #FDD017;
		}
	td.color-alert-out-of-range
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		background-color: #E42217;
		color: #FFFFFF;
		}
	tr.color-1
		{
		background-color: #EDF8FE;
		}
	tr.color-2
		{
		background-color: #FFFFFF;
		}
	tr.color-3
		{
		background-color: #00AEEF;
		}
	tr.color-alert-out-of-range
		{
		background-color: #E42217;
		color: #FFFFFF;
		}
	td.graphs-style
		{
		font-size:16px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.graphs-style-title
		{
		font-size:12px; 
		font-weight:bold;
		text-decoration: none;
		}
	td.graphs-summary
		{
		font-size:12px; 
		text-decoration: none;
		}
	td.add-border
		{
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details-very-small
		{
		font-size:9px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details-small-blank-line
		{
		font-size:4px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		}
	td.details-very-very-small
		{
		font-size:6px; 
		border-style: solid; 
		border-width: 0 0 0 0; 
		}
	th.details-bold
		{
		font-size:12px; 
		font-weight:bold; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		color: #000000
		}
	td.wrap
		{
		font-size:12px; 
		border-style: solid; 
		border-width: 1px 1px 0 0; 
		border-color: #ccc;
		white-space: pre-wrap; 
		white-space: -moz-pre-wrap; 
		white-space: -pre-wrap; 
		white-space: -o-pre-wrap; 
		word-wrap: break-word;
		max-width: 150px;
		}
	p.normal
		{
		font-size:12px;
		}
	a.normal
		{
		font-size:12px; 
		text-decoration: none;
		}
	input.summary-checkbox
		{
		font-size: 6px
		width: 10px;
		height: 10px;
		}
	indent-from-margin
		{
		text-indent:10px;
		}		
		
	a.tooltip
		{
		font-size:11px; 
		text-decoration: none;
		}
	a.tooltip span 
		{
		display:none; 
		padding:2px 3px; 
		margin-left:8px; 
		width:250px;
		font-size:12px; 
		text-decoration: none;
		}
	a.tooltip:hover span
		{
		display:inline; 
		position:absolute; 
		border:1px solid #cccccc; 
		background:	#FFF8C6;
		color:#000000;
		font-size:12px; 
		text-decoration: none;
		}	
</style>'
	
	
	-----------------------------------------------------------------------------------------------------
	--report header
	-----------------------------------------------------------------------------------------------------
	RAISERROR('	...Build Report: Header', 10, 1) WITH NOWAIT

	SET @HTMLReport = N''	
	SET @HTMLReport = @HTMLReport + N'<html><head>
											<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
											<title>dbaTDPMon: Daily Health Check Report for ' + @projectName + N'</title>
											<meta name="Author" content="Dan Andrei STEFAN">' + @CSSClass + N'</head><body class="normal">'

	SET @HTMLReport = @HTMLReport + N'
	<A NAME="Home" class="normal">&nbsp;</A>
	<HR WIDTH="1130px" ALIGN=LEFT><br>
	<TABLE BORDER=0 CELLSPACING=0 CELLPADDING="3px" WIDTH="1130px">
	<TR VALIGN=TOP>
		<TD WIDTH="410px" ALIGN=LEFT>
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
				<TR VALIGN=TOP>
					<TD WIDTH="200px">' + [dbo].[ufn_reportHTMLGetImage]('Logo') + N'</TD>	
					<TD WIDTH="210px" ALIGN=CENTER><P class="title2-style" ALIGN=CENTER>dbaTDPMon<br>Health Check Report</P></TD>
				</TR>
			</TABLE>
			<HR WIDTH="400px" ALIGN=LEFT>
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
				<TR>
					<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">Project:</P></TD>
					<TD ALIGN=LEFT  WIDTH="340px"><P class="title-style">' +  @projectName + N'</P></TD>
				</TR>
				<TR>
					<TD ALIGN=RIGHT WIDTH="60px"><P class="title3-style">@</P></TD>
					<TD ALIGN=LEFT  WIDTH="340px"><P class="title2-style">' + CONVERT([varchar](20), ISNULL(@reportBuildStartTime, CONVERT([datetime], N'1900-01-01', 120)), 120) + N' (UTC)</P></TD>							
				</TR>
			</TABLE>' + 
			CASE WHEN @reportDescription IS NOT NULL
				 THEN N'
						<HR WIDTH="400px" ALIGN=LEFT>
						<DIV ALIGN=CENTER>
						<TABLE CELLSPACING=0 CELLPADDING="3px" border=0> 
							<TR>
								<TD ALIGN=CENTER><P class="title4-style">' + @reportDescription + N'</P></TD>							
							</TR>
						</TABLE>
						</DIV>'
				 ELSE N''
			END + 
			N'
		</TD>
		<TD ALIGN=RIGHT>'


	SET @HTMLReport = @HTMLReport + N'				
			<TABLE CELLSPACING=0 CELLPADDING="3px" border=0 width="360px">
			<TR VALIGN="TOP">
				<TD WIDTH="360px">
					<TABLE CELLSPACING=0 CELLPADDING="1px" border=0 width="360px" class="with-border">
						<TR VALIGN="TOP" class="color-1">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   1 =   1 THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END + N'&nbsp;&nbsp;Instance Availability</TD>
						</TR>
						<TR VALIGN="TOP" class="color-2">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   2 =   2 THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END + N'&nbsp;&nbsp;Databases status</TD>
						</TR>
						<TR VALIGN="TOP" class="color-1">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   4 =   4  THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END  + N'&nbsp;&nbsp;SQL Server Agent Jobs status</TD>
						</TR>
						<TR VALIGN="TOP" class="color-2">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &   8 =   8 THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END + N'&nbsp;&nbsp;Disk Space information</TD>
						</TR>
						<TR VALIGN="TOP" class="color-1">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &  16 =  16  THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END  + N'&nbsp;&nbsp;Errorlog messages</TD>
						</TR>
						<TR VALIGN="TOP" class="color-2">
							<TD WIDTH="180px" class="details-very-small" ALIGN="LEFT">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;' + CASE WHEN @flgActions &  32 =  32  THEN [dbo].[ufn_reportHTMLGetImage]('check-checked') ELSE [dbo].[ufn_reportHTMLGetImage]('check-unchecked') END  + N'&nbsp;&nbsp;OS Event messages</TD>
						</TR>
					</TABLE>
				</TD>
			</TR>
			</TABLE>
			'

	SET @HTMLReportArea=N''
	SET @HTMLReportArea = @HTMLReportArea + N'				
			<P class="disclaimer">Browser support: IE 8, Firefox 3.5 and Google Chrome 7 (on lower versions, some features may be missing).</P>
		</TD>
	</TR>
	</TABLE>
	<HR WIDTH="1130px" ALIGN=LEFT><br>'
	
	SET @HTMLReport = @HTMLReport + @HTMLReportArea

	SET @HTMLReport = @HTMLReport + N'
	<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px">
	<TR VALIGN=TOP>	
		<TD COLSPAN="2">
			<A NAME="TableOfContents" class="category-style">Table of Contents</A>
		</TD>
	<TR VALIGN=TOP>	
		<TD class="graphs-style-title" width="452px">
			<table CELLSPACING=0 CELLPADDING="3px" border=0 width="452px" class="with-border">' + 
			CASE WHEN (@flgActions & 1 = 1)
				 THEN N'
				<TR VALIGN="TOP" class="color-3">
					<TD ALIGN=LEFT class="summary-style-title add-border color-3" colspan="3">Modules</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						Instance Availability
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 1 = 1)
						  THEN N'<A HREF="#InstancesOnline" class="summary-style color-1">Online {InstancesOnlineCount}</A>'
						  ELSE N'Online'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 2 = 2)
						  THEN N'<A HREF="#InstancesOffline" class="summary-style color-1">Offline {InstancesOfflineCount}</A>'
						  ELSE N'Offline'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 2 = 2) 
				 THEN N'
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">
						Databases Status
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 8 = 8)
						  THEN N'<A HREF="#DatabasesStatusCompleteDetails" class="summary-style color-2">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 256 = 256)
						  THEN N'<A HREF="#DatabasesStatusPermissionErrors" class="summary-style color-2">Permission Errors {DatabasesStatusPermissionErrorsCount}</A>'
						  ELSE N'&Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 4 = 4) 
				 THEN N'
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						SQL Server Agent Jobs Status
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 64 = 64)
						  THEN N'<A HREF="#SQLServerAgentJobsStatusCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 32 = 32)
						  THEN N'<A HREF="#SQLServerAgentJobsStatusPermissionErrors" class="summary-style color-1">Permission Errors {SQLServerAgentJobsStatusPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 8 = 8) 
				 THEN N'
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">
						Disk Space Information
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 65536 = 65536)
						  THEN N'<A HREF="#DiskSpaceInformationCompleteDetails" class="summary-style color-2">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 131072 = 131072)
						  THEN N'<A HREF="#DiskSpaceInformationPermissionErrors" class="summary-style color-2">Permission Errors {DiskSpaceInformationPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 16 = 16) 
				 THEN N'
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						Errorlog Messages
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 2097152 = 2097152)
						  THEN N'<A HREF="#ErrorlogMessagesCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 524288 = 524288)
						  THEN N'<A HREF="#ErrorlogMessagesPermissionErrors" class="summary-style color-1">Permission Errors {ErrorlogMessagesPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors;'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 32 = 32) 
				 THEN N'
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">
						OS Event Messages
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 134217728 = 134217728)
						  THEN N'<A HREF="#OSEventMessagesCompleteDetails" class="summary-style color-2">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 67108864 = 67108864)
						  THEN N'<A HREF="#OSEventMessagesPermissionErrors" class="summary-style color-2">Permission Errors {OSEventMessagesPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors;'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + N'
			</table>
		</TD>
		<TD class="graphs-style-title" width="126px">
			&nbsp;
		</TD>
		<TD class="graphs-style-title" width="552px">
			<table CELLSPACING=0 CELLPADDING="3px" border=0 width="552px" class="with-border">
				<TR VALIGN="TOP" class="color-3">
					<TD ALIGN=LEFT class="summary-style-title add-border color-3" colspan="2">Potential Issues</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4 = 4)
						  THEN N'<A HREF="#DatabasesStatusIssuesDetected" class="summary-style color-1">Offline Databases {DatabasesStatusIssuesDetectedCount}</A>'
						  ELSE N'Offline Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2)  AND (@flgOptions & 128 = 128)
						  THEN N'<A HREF="#SystemDatabasesSizeIssuesDetected" class="summary-style color-1">Big Size for System Databases {SystemDatabasesSizeIssuesDetectedCount}</A>'
						  ELSE N'Big Size for System Databases (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 4 = 4) AND (@flgOptions & 16 = 16)
						  THEN N'<A HREF="#SQLServerAgentJobsStatusIssuesDetected" class="summary-style color-2">SQL Server Agent Job Failures {SQLServerAgentJobsStatusIssuesDetectedCount}</A>'
						  ELSE N'SQL Server Agent Job Failures'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 1024 = 1024)
						  THEN N'<A HREF="#DatabaseMaxLogSizeIssuesDetected" class="summary-style color-2">Big Size for Database Log files {DatabaseMaxLogSizeIssuesDetectedCount}</A>'
						  ELSE N'Big Size for Database Log files (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 4 = 4) AND (@flgOptions & 33554432 = 33554432)
						  THEN N'<A HREF="#LongRunningSQLAgentJobsIssuesDetected" class="summary-style color-1">Long Running SQL Agent Jobs {LongRunningSQLAgentJobsIssuesDetectedCount}</A>'
						  ELSE N'Long Running SQL Agent Jobs'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 512 = 512)
						  THEN N'<A HREF="#DatabasesWithAutoCloseShrinkIssuesDetected" class="summary-style color-1">Databases with Auto Close / Shrink {DatabasesWithAutoCloseShrinkIssuesDetectedCount}</A>'
						  ELSE N'Auto Close / Shrink Databases (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 8 = 8) AND (@flgOptions & 262144 = 262144)
						  THEN N'<A HREF="#DiskSpaceInformationIssuesDetected" class="summary-style color-2">Low Free Disk Space {DiskSpaceInformationIssuesDetectedCount}</A>'
						  ELSE N'Low Free Disk Space (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 2048 = 2048)
						  THEN N'<A HREF="#DatabaseMinDataSpaceIssuesDetected" class="summary-style color-2">Low Usage of Data Space {DatabaseMinDataSpaceIssuesDetectedCount}</A>'
						  ELSE N'Low Usage of Data Space (N/A)'
					END + N'
					</TD>
				</TR>
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 8192 = 8192)
						  THEN N'<A HREF="#DatabaseBACKUPAgeIssuesDetected" class="summary-style color-1">Outdated Backup for Databases {DatabaseBACKUPAgeIssuesDetectedCount}</A>'
						  ELSE N'Outdated Backup for Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 32768 = 32768)
						  THEN N'<A HREF="#DatabaseMaxLogSpaceIssuesDetected" class="summary-style color-1">High Usage of Log Space {DatabaseMaxLogSpaceIssuesDetectedCount}</A>'
						  ELSE N'High Usage of Log Spacee (N/A)'
					END + N'
					</TD>
				</TR> 
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 16384 = 16384)
						  THEN N'<A HREF="#DatabaseDBCCCHECKDBAgeIssuesDetected" class="summary-style color-2">Outdated DBCC CHECKDB Databases {DatabaseDBCCCHECKDBAgeIssuesDetectedCount}</A>'
						  ELSE N'Outdated DBCC CHECKDB Databases (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4096 = 4096)
						  THEN N'<A HREF="#DatabaseLogVsDataSizeIssuesDetected" class="summary-style color-2">Log vs. Data - Allocated Size {DatabaseLogVsDataSizeIssuesDetectedCount}</A>'
						  ELSE N'Log vs. Data - Allocated Size (N/A)'
					END + N'
					</TD>
				</TR> 
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 16 = 16) AND (@flgOptions & 1048576 = 1048576)
						  THEN N'<A HREF="#ErrorlogMessagesIssuesDetected" class="summary-style color-1">Errorlog Messages {ErrorlogMessagesIssuesDetectedCount}</A>'
						  ELSE N'ErrorlogMessagesIssuesDetected (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-1">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 4194304 = 4194304)
						  THEN N'<A HREF="#DatabaseFixedFileSizeIssuesDetected" class="summary-style color-1">Databases with Fixed File(s) Size {DatabaseFixedFileSizeIssuesDetectedCount}</A>'
						  ELSE N'>Databases with Fixed File(s) Size (N/A)'
					END + N'
					</TD>
				</TR> 
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 16777216 = 16777216)
						  THEN N'<A HREF="#FrequentlyFragmentedIndexesIssuesDetected" class="summary-style color-2">Frequently Fragmented Indexes {FrequentlyFragmentedIndexesIssuesDetectedCount}</A>'
						  ELSE N'>Frequently Fragmented Indexes (N/A)'
					END + N'
					</TD>
					<TD ALIGN=LEFT class="summary-style add-border color-2">' +
					CASE WHEN (@flgActions & 2 = 2) AND (@flgOptions & 8388608 = 8388608)
						  THEN N'<A HREF="#DatabasePageVerifyIssuesDetected" class="summary-style color-2">Databases with Improper Page Verify Option {DatabasePageVerifyIssuesDetectedCount}</A>'
						  ELSE N'>Databases with Improper Page Verify Option (N/A)'
					END + N'
					</TD>
				</TR>
			</table>
		</TD>
	</TR>
	</TABLE>			
	<HR WIDTH="1130px" ALIGN=LEFT><br>'



	-----------------------------------------------------------------------------------------------------
	--Offline Instances
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 1 = 1) AND (@flgOptions & 1 = 1)
		begin
			RAISERROR('	...Build Report: Instance Availability - Offline', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="InstancesOffline" class="category-style">Instance Availability - Offline</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			DECLARE   @machineName		[sysname]
					, @instanceName		[sysname]
					, @isClustered		[bit]
					, @clusterNodeName	[sysname]
					, @eventDate		[datetime]
					, @message			[nvarchar](max)

			SET @idx=1		

			DECLARE crsInstancesOffline CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																			, cin.[is_clustered], cin.[cluster_node_machine_name]
																			, MAX(lsam.[event_date_utc]) [event_date_utc]
																			, lsam.[message]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																	LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																												AND rsr.[rule_id] = 1
																												AND rsr.[active] = 1
																												AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																	WHERE	cin.[instance_active]=0
																			AND cin.[project_id] = @projectID
																			AND lsam.[descriptor] IN (N'dbo.usp_refreshMachineCatalogs - Offline')
																			AND rsr.[id] IS NULL

																	GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																	ORDER BY cin.[instance_name], cin.[machine_name], [event_date_utc]
			OPEN crsInstancesOffline
			FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsInstancesOffline
			DEALLOCATE crsInstancesOffline

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOfflineCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Online Instances
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 1 = 1) AND (@flgOptions & 2 = 2)
		begin
			RAISERROR('	...Build Report: Instance Availability - Online', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="InstancesOnline" class="category-style">Instance Availability - Online</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="100px" class="details-bold" nowrap>Details</TH>
											<TH WIDTH="150px" class="details-bold">Machine Name</TH>
											<TH WIDTH="200px" class="details-bold">Instance Name</TH>
											<TH WIDTH="100px" class="details-bold">Clustered</TH>
											<TH WIDTH= "90px" class="details-bold" nowrap >Version</TH>
											<TH WIDTH="260px" class="details-bold">Edition</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Refresh Date (UTC)</TH>'

			DECLARE   @version				[sysname]
					, @edition				[varchar](256)
					, @hasDatabaseDetails	[int]
					, @hasSQLagentJob		[int]
					, @hasDiskSpaceInfo		[int]
					, @hasErrorlogMessages	[int]
					, @hasOSEventMessages	[int]
					, @lastRefreshDate		[datetime]
					, @dbSize				[numeric](20,3)

			SET @idx=1		

			DECLARE crsInstancesOffline CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																			, cin.[is_clustered], cin.[cluster_node_machine_name]
																			, cin.[version], cin.[edition], cin.[last_refresh_date_utc]	
																			, shcdd.[size_mb]
																	FROM [dbo].[vw_catalogInstanceNames]  cin
																	LEFT JOIN 
																		(
																			SELECT    [project_id], [instance_id]
																					, SUM(ISNULL([size_mb], 0)) [size_mb]
																			FROM [health-check].[vw_statsDatabaseDetails]
																			WHERE [project_id] = @projectID
																			GROUP BY [project_id], [instance_id]
																		) shcdd ON shcdd.[instance_id] = cin.[instance_id] AND shcdd.[project_id] = cin.[project_id]
																	LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																												AND rsr.[rule_id] = 2
																												AND rsr.[active] = 1
																												AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																	WHERE cin.[instance_active]=1
																			AND cin.[project_id] = @projectID
																			AND rsr.[id] IS NULL
																	ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsInstancesOffline
			FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @version, @edition, @lastRefreshDate, @dbSize
			WHILE @@FETCH_STATUS=0
				begin
					SELECT	@hasDatabaseDetails = COUNT(*)
					FROM	[dbo].[vw_catalogDatabaseNames]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasSQLagentJob = COUNT(*)
					FROM	[health-check].[vw_statsSQLServerAgentJobsHistory]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasDiskSpaceInfo = COUNT(*)
					FROM	[health-check].[vw_statsDiskSpaceInfo]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName
					
					SELECT	@hasErrorlogMessages = COUNT(*)
					FROM	[health-check].[vw_statsSQLServerErrorlogDetails]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasOSEventMessages = COUNT(*)
					FROM	[health-check].[vw_statsOSEventLogs] 
					WHERE	[project_id]=@projectID
							AND [instance_name] = @machineName
																				  

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="CENTER" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + 
										CASE	WHEN @hasDatabaseDetails<>0 AND @flgOptions & 8 = 8
												THEN N'<BR><A HREF="#DatabasesStatusCompleteDetails' + @instanceName + N'">Databases</A>'
												ELSE N''
										END +
										CASE WHEN @hasSQLagentJob<>0 AND @flgOptions & 64 = 64
												THEN N'<BR><A HREF="#SQLServerAgentJobsStatusCompleteDetails' + @instanceName + N'">SQL Agent Jobs</A>'
												ELSE N''
										END +
										CASE WHEN @hasDiskSpaceInfo<>0 AND @flgOptions & 65536 = 65536
												THEN N'<BR><A HREF="#DiskSpaceInformationCompleteDetails' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'">Disk Space</A>'
												ELSE N''
										END +  
										CASE WHEN @hasErrorlogMessages<>0 AND @flgOptions & 2097152 = 2097152
												THEN N'<BR><A HREF="#ErrorlogMessagesCompleteDetails' + @instanceName + N'">Errorlog</A>'
												ELSE N''
										END +  
											N'<BR><BR>
										</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH= "90px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@version, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="260px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@edition, 0), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastRefreshDate, 121), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsInstancesOffline INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @version, @edition, @lastRefreshDate, @dbSize
				end
			CLOSE crsInstancesOffline
			DEALLOCATE crsInstancesOffline

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{InstancesOnlineCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		

	-----------------------------------------------------------------------------------------------------
	--Databases Status - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 256 = 256)
		begin
			RAISERROR('	...Build Report: Databases Status - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @messageCount=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusPermissionErrors" class="category-style">Databases Status - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		
			
			DECLARE crsDatabasesStatusPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, COUNT(DISTINCT lsam.[message]) AS [message_count]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 256
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE	cin.[instance_active]=1
																							AND cin.[project_id] = @projectID
																							AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																					ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDatabasesStatusPermissionErrors
			FETCH NEXT FROM crsDatabasesStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'"><A NAME="DatabasesStatusPermissionErrors' + @instanceName + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</A></TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [event_date_utc], 121), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([message], 0), N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT [message], [event_date_utc]
															, ROW_NUMBER() OVER(ORDER BY [event_date_utc]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM (
															SELECT    lsam.[message]
																	, MAX(lsam.[event_date_utc]) [event_date_utc]
															FROM [dbo].[vw_catalogInstanceNames]  cin
															INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
															WHERE	cin.[instance_active]=1
																	AND cin.[project_id] = @projectID	
																	AND cin.[instance_name] = @instanceName
																	AND cin.[machine_name] = @machineName
																	AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')
															GROUP BY lsam.[message]
														)Z
												)X
											ORDER BY [event_date_utc]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsDatabasesStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																	<TD class="details" COLSPAN=5>&nbsp;</TD>
															</TR>'
				end
			CLOSE crsDatabasesStatusPermissionErrors
			DEALLOCATE crsDatabasesStatusPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SELECT     @idx = COUNT(DISTINCT lsam.[message])
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
			LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
														AND rsr.[rule_id] = 256
														AND rsr.[active] = 1
														AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
			WHERE	cin.[instance_active]=1
					AND cin.[project_id] = @projectID
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')
					AND rsr.[id] IS NULL

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusPermissionErrorsCount}', '(' + CAST((@idx) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Databases Status - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4 = 4)
		begin
			RAISERROR('	...Build Report: Databases Status - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusIssuesDetected" class="category-style">Databases Status - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">	
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="5">database status not in (' + @configAdmittedState + N')</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="490px" class="details-bold">Database Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>State</TH>'


			SET @idx=1		

			DECLARE crsDatabasesStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																						, cin.[is_clustered], cin.[cluster_node_machine_name]
																						, cdn.[database_name]
																						, cdn.[state_desc]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 4
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID	
																						AND CHARINDEX(cdn.[state_desc], @configAdmittedState)=0
																						AND rsr.[id] IS NULL
																				ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @stateDesc
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(@stateDesc, N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @stateDesc
				end
			CLOSE crsDatabasesStatusIssuesDetected
			DEALLOCATE crsDatabasesStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesStatusIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 64 = 64)
		begin
			RAISERROR('	...Build Report: SQL Server Agent Jobs Status - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusPermissionErrors" class="category-style">SQL Server Agent Jobs Status - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		

			DECLARE crsSQLServerAgentJobsStatusPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																									, cin.[is_clustered], cin.[cluster_node_machine_name]
																									, MAX(lsam.[event_date_utc]) [event_date_utc]
																									, lsam.[message]
																							FROM [dbo].[vw_catalogInstanceNames]  cin
																							INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 64
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																							WHERE	cin.[instance_active]=1
																									AND cin.[project_id] = @projectID
																									AND lsam.descriptor IN (N'dbo.usp_hcCollectSQLServerAgentJobsStatus')
																									AND rsr.[id] IS NULL
																							GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																							ORDER BY cin.[instance_name], cin.[machine_name], [event_date_utc]
			OPEN crsSQLServerAgentJobsStatusPermissionErrors
			FETCH NEXT FROM crsSQLServerAgentJobsStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes<BR>' + ISNULL(N'[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsSQLServerAgentJobsStatusPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsSQLServerAgentJobsStatusPermissionErrors
			DEALLOCATE crsSQLServerAgentJobsStatusPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{SQLServerAgentJobsStatusPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 16 = 16)
		begin
			RAISERROR('	...Build Report: SQL Server Agent Jobs Status - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusIssuesDetected" class="category-style">SQL Server Agent Jobs Status - Issues Detected (last ' + CAST(@configFailuresInLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">job status in (Failed, Retry, Canceled)</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Job Name</TH>
											<TH WIDTH="110px" class="details-bold" nowrap>Execution Status</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Execution Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Execution Time</TH>
											<TH WIDTH="460px" class="details-bold">Message</TH>'


			SET @idx=1		

			DECLARE   @jobName			[sysname]
					, @lastExecStatus	[int]
					, @lastExecDate		[varchar](10)
					, @lastExecTime		[varchar](8)
			
			SET @dateTimeLowerLimit = DATEADD(hh, -@configFailuresInLastHours, GETDATE())
			DECLARE crsSQLServerAgentJobsStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT	ssajh.[instance_name], ssajh.[job_name], ssajh.[last_execution_status], ssajh.[last_execution_date], ssajh.[last_execution_time], ssajh.[message]
																							FROM	[health-check].[vw_statsSQLServerAgentJobsHistory] ssajh
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 16
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value]=ssajh.[instance_name])
																							WHERE	ssajh.[project_id]=@projectID
																									AND ssajh.[last_execution_status] IN (0, 2, 3) /* 0 = Failed; 2 = Retry; 3 = Canceled */
																									AND CONVERT([datetime], ssajh.[last_execution_date] + ' ' + ssajh.[last_execution_time], 120) >= @dateTimeLowerLimit
																									AND rsr.[id] IS NULL
																							ORDER BY ssajh.[instance_name], ssajh.[job_name], ssajh.[last_execution_date], ssajh.[last_execution_time]
			OPEN crsSQLServerAgentJobsStatusIssuesDetected
			FETCH NEXT FROM crsSQLServerAgentJobsStatusIssuesDetected INTO @instanceName, @jobName, @lastExecStatus, @lastExecDate, @lastExecTime, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @message = CASE WHEN LEFT(@message, 2) = '--' THEN SUBSTRING(@message, 3, LEN(@message)) ELSE @message END
					SET @message = ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') 
					SET @message = REPLACE(@message, CHAR(13), N'<BR>')
					SET @message = REPLACE(@message, '--', N'<BR>')
					SET @message = REPLACE(@message, N'<BR><BR>', N'<BR>')

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @jobName + N'</TD>' + 
										N'<TD WIDTH="110px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @lastExecStatus = 0 THEN N'Failed'
																											WHEN @lastExecStatus = 1 THEN N'Succeded'
																											WHEN @lastExecStatus = 2 THEN N'Retry'
																											WHEN @lastExecStatus = 3 THEN N'Canceled'
																											WHEN @lastExecStatus = 4 THEN N'In progress'
																											ELSE N'Unknown'
																										END
										 + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecDate + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecTime + N'</TD>' + 
										N'<TD WIDTH="460px" class="details" ALIGN="LEFT">' + @message + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsSQLServerAgentJobsStatusIssuesDetected INTO @instanceName, @jobName, @lastExecStatus, @lastExecDate, @lastExecTime, @message
				end
			CLOSE crsSQLServerAgentJobsStatusIssuesDetected
			DEALLOCATE crsSQLServerAgentJobsStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{SQLServerAgentJobsStatusIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	-- Long Running SQL Agent Jobs
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 33554432 = 33554432)
		begin
			RAISERROR('	...Build Report: Long Running SQL Agent Jobs - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="LongRunningSQLAgentJobsIssuesDetected" class="category-style">Long Running SQL Agent Jobs - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">jobs currently running for more than ' + CAST(@configMaxJobRunningTimeInHours AS [nvarchar]) + N'hours</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Job Name</TH>
											<TH WIDTH="110px" class="details-bold" nowrap>Running Time</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Start Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Start Time</TH>
											<TH WIDTH="460px" class="details-bold">Message</TH>'


			SET @idx=1		

			DECLARE   @runningTime		[varchar](32)
			
			SET @dateTimeLowerLimit = DATEADD(hh, -@configFailuresInLastHours, GETDATE())
			DECLARE crsLongRunningSQLAgentJobsIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT	  ssajh.[instance_name], ssajh.[job_name]
																								, ssajh.[last_execution_date] AS [start_date], ssajh.[last_execution_time] AS [start_time]
																								, [dbo].[ufn_reportHTMLFormatTimeValue](CAST(ssajh.[running_time_sec]*1000 AS [bigint])) AS [running_time]
																								, ssajh.[message]
																						FROM [health-check].[vw_statsSQLServerAgentJobsHistory] ssajh
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 33554432
																																	AND rsr.[active] = 1
																																	AND (    rsr.[skip_value]=ssajh.[instance_name]
																																		 AND ISNULL(rsr.[skip_value2], '') = ISNULL(ssajh.[job_name], '') 
																																		)
																						WHERE	ssajh.[project_id]=@projectID
																								AND ssajh.[last_execution_status] = 4
																								AND ssajh.[last_execution_date] IS NOT NULL
																								AND ssajh.[last_execution_time] IS NOT NULL
																								AND (ssajh.[running_time_sec]/3600) >= @configMaxJobRunningTimeInHours
																								AND rsr.[id] IS NULL
																						ORDER BY [start_date], [start_time]

			OPEN crsLongRunningSQLAgentJobsIssuesDetected
			FETCH NEXT FROM crsLongRunningSQLAgentJobsIssuesDetected INTO @instanceName, @jobName, @lastExecDate, @lastExecTime, @runningTime, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @message = CASE WHEN LEFT(@message, 2) = '--' THEN SUBSTRING(@message, 3, LEN(@message)) ELSE @message END
					SET @message = ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') 
					SET @message = REPLACE(@message, CHAR(13), N'<BR>')
					SET @message = REPLACE(@message, '--', N'<BR>')
					SET @message = REPLACE(@message, N'<BR><BR>', N'<BR>')

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @jobName + N'</TD>' + 
										N'<TD WIDTH="110px" class="details" ALIGN="CENTER" nowrap>' + @runningTime + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecDate + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + @lastExecTime + N'</TD>' + 
										N'<TD WIDTH="460px" class="details" ALIGN="LEFT">' + @message + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsLongRunningSQLAgentJobsIssuesDetected INTO @instanceName, @jobName, @lastExecDate, @lastExecTime, @runningTime, @message
				end
			CLOSE crsLongRunningSQLAgentJobsIssuesDetected
			DEALLOCATE crsLongRunningSQLAgentJobsIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{LongRunningSQLAgentJobsIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
		
	-----------------------------------------------------------------------------------------------------
	--Low Free Disk Space - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 131072 = 131072)
		begin
			RAISERROR('	...Build Report: Low Free Disk Space - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @messageCount=0

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationPermissionErrors" class="category-style">Low Free Disk Space - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		
			
			DECLARE crsDiskSpaceInformationPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, COUNT(DISTINCT lsam.[message]) AS [message_count]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 131072
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE	cin.[instance_active]=1
																								AND cin.[project_id] = @projectID
																								AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')
																								AND rsr.[id] IS NULL
																						GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																						ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDiskSpaceInformationPermissionErrors
			FETCH NEXT FROM crsDiskSpaceInformationPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'"><A NAME="DiskSpaceInformationPermissionErrors' + @instanceName + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</A></TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + N'">' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' 


					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [event_date_utc], 121), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([message], 0), N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT	[message], [event_date_utc]
															, ROW_NUMBER() OVER(ORDER BY [event_date_utc]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM (
															SELECT    lsam.[message]
																	, MAX(lsam.[event_date_utc]) [event_date_utc]
															FROM [dbo].[vw_catalogInstanceNames]  cin
															INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																WHERE	cin.[instance_active]=1
																	AND cin.[project_id] = @projectID	
																	AND cin.[instance_name] = @instanceName
																	AND cin.[machine_name] = @machineName
																	AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')
															GROUP BY lsam.[message]
														)Z
												)X
											ORDER BY [event_date_utc]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsDiskSpaceInformationPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @messageCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																<TD class="details" COLSPAN=5>&nbsp;</TD>
														</TR>'
				end
			CLOSE crsDiskSpaceInformationPermissionErrors
			DEALLOCATE crsDiskSpaceInformationPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SELECT    @idx = COUNT(*) + 1
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
			WHERE	cin.[instance_active]=1
					AND cin.[project_id] = @projectID
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDiskSpaceUsage')

			SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
	

	-----------------------------------------------------------------------------------------------------
	--Low Free Disk Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 262144 = 262144)
		begin
			RAISERROR('	...Build Report: Low Free Disk Space - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationIssuesDetected" class="category-style">Low Free Disk Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">free disk space (%) &lt; ' + CAST(@configFreeDiskMinPercent AS [nvarchar](32)) + N' OR free disk space (MB) &lt; ' + CAST(@configFreeDiskMinSpace AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Logical Drive</TH>
											<TH WIDTH="230px" class="details-bold" nowrap>Volume Mount Point</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Total Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Available Space (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Percent Available (%)</TH>'

			SET @idx=1		

			DECLARE crsDiskSpaceInformationIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT  DISTINCT
																								  cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, dsi.[logical_drive], dsi.[volume_mount_point]
																								, dsi.[total_size_mb], dsi.[available_space_mb], dsi.[percent_available]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
																						LEFT  JOIN 
																									(
																										SELECT DISTINCT [project_id], [instance_id], [physical_drives] 
																										FROM [health-check].[vw_statsDatabaseDetails]
																									)   cdd ON cdd.[project_id] = cin.[project_id] AND cdd.[instance_id] = cin.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 262144
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cin.[project_id] = @projectID
																								AND (    (	  dsi.[percent_available] IS NOT NULL 
																											AND dsi.[percent_available] < @configFreeDiskMinPercent
																											)
																										OR 
																										(	   dsi.[percent_available] IS NULL 
																											AND dsi.[available_space_mb] IS NOT NULL 
																											AND dsi.[available_space_mb] < @configFreeDiskMinSpace
																										)
																									)
																								AND (dsi.[logical_drive] IN ('C') OR CHARINDEX(dsi.[logical_drive], cdd.[physical_drives])>0)
																								AND rsr.[id] IS NULL
																						ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDiskSpaceInformationIssuesDetected
			FETCH NEXT FROM crsDiskSpaceInformationIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @logicalDrive, @volumeMountPoint, @diskTotalSizeMB, @diskAvailableSpaceMB, @diskPercentAvailable
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@logicalDrive, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="230px" class="details" ALIGN="LEFT" nowrap>' + ISNULL(@volumeMountPoint, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskTotalSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskAvailableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@diskPercentAvailable AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDiskSpaceInformationIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @logicalDrive, @volumeMountPoint, @diskTotalSizeMB, @diskAvailableSpaceMB, @diskPercentAvailable
				end
			CLOSE crsDiskSpaceInformationIssuesDetected
			DEALLOCATE crsDiskSpaceInformationIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DiskSpaceInformationIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
	
	
	-----------------------------------------------------------------------------------------------------
	--System Databases Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 128 = 128)
		begin
			RAISERROR('	...Build Report: System Databases Size - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SystemDatabasesSizeIssuesDetected" class="category-style">System Databases Size - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="5">size master (MB) &ge; ' + CAST(@configDBMaxSizeMaster AS [nvarchar](32)) + N' OR size msdb (MB) &ge; ' + CAST(@configDBMaxSizeMSDB AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="490px" class="details-bold">Database Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Size (MB)</TH>'

			SET @idx=1		

			DECLARE crsDatabasesStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																						, cin.[is_clustered], cin.[cluster_node_machine_name]
																						, cdn.[database_name]
																						, shcdd.[size_mb]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				LEFT  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																				LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 128
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID	
																						AND (   (cdn.[database_name]='master' AND shcdd.[size_mb] >= @configDBMaxSizeMaster AND @configDBMaxSizeMaster<>0)
																							 OR (cdn.[database_name]='msdb'   AND shcdd.[size_mb] >= @configDBMaxSizeMSDB   AND @configDBMaxSizeMSDB<>0)
																							)
																						AND rsr.[id] IS NULL
																				ORDER BY shcdd.[size_mb] DESC, cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize
				end
			CLOSE crsDatabasesStatusIssuesDetected
			DEALLOCATE crsDatabasesStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{SystemDatabasesSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
		
	-----------------------------------------------------------------------------------------------------
	--Databases with Auto Close / Shrink - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 512 = 512)
		begin
			RAISERROR('	...Build Report: Databases with Auto Close / Shrink - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesWithAutoCloseShrinkIssuesDetected" class="category-style">Databases with Auto Close / Shrink - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="490px" class="details-bold">Database Name</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Auto Close</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Auto Shrink</TH>'

			SET @idx=1		

			DECLARE   @isAutoClose		[bit]
					, @isAutoShrink		[bit]

			DECLARE crsDatabasesStatusIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																						, cin.[is_clustered], cin.[cluster_node_machine_name]
																						, cdn.[database_name]
																						, shcdd.[is_auto_close]
																						, shcdd.[is_auto_shrink]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																				LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 512
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID
																						AND (shcdd.[is_auto_close]=1 OR shcdd.[is_auto_shrink]=1)
																						AND rsr.[id] IS NULL
																				ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasesStatusIssuesDetected
			FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @isAutoClose, @isAutoShrink
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isAutoClose=0 THEN N'No' ELSE N'Yes' END + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isAutoShrink=0 THEN N'No' ELSE N'Yes' END + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @isAutoClose, @isAutoShrink
				end
			CLOSE crsDatabasesStatusIssuesDetected
			DEALLOCATE crsDatabasesStatusIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasesWithAutoCloseShrinkIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--Big Size for Database Log files - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 1024 = 1024)
		begin
			RAISERROR('	...Build Report: Big Size for Database Log files - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMaxLogSizeIssuesDetected" class="category-style">Big Size for Database Log files - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="6">log size (MB) &ge; ' + CAST(@configLogMaxSize AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="450px" class="details-bold">Database Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Log Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Log Used (%)</TH>'

			SET @idx=1		

			DECLARE crsDatabaseMaxLogSizeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[log_size_mb]
																							, shcdd.[log_space_used_percent]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 1024
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[log_size_mb] >= @configLogMaxSize 
																							AND rsr.[id] IS NULL
																					ORDER BY shcdd.[log_size_mb] DESC, cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabaseMaxLogSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseMaxLogSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @logSizeMB, @logSpaceUsedPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="450px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMaxLogSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @logSizeMB, @logSpaceUsedPercent
				end
			CLOSE crsDatabaseMaxLogSizeIssuesDetected
			DEALLOCATE crsDatabaseMaxLogSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMaxLogSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Low Usage of Data Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 2048 = 2048)
		begin
			RAISERROR('	...Build Report: Low Usage of Data Space - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMinDataSpaceIssuesDetected" class="category-style">Low Usage of Data Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@configDBMinSizeForAnalysis AS [nvarchar](32)) + N' AND data size used (%) &le; ' + CAST(@configDataSpaceMinPercent AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="370px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Space Used (%)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Reclaimable Space (MB)</TH>
											'

			SET @idx=1		
					
			DECLARE crsDatabaseMinDataSpaceIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[size_mb]
																							, shcdd.[data_size_mb]
																							, shcdd.[data_space_used_percent]
																							, ((100.0 - shcdd.[data_space_used_percent]) * shcdd.[data_size_mb]) / 100 AS [reclaimable_space_mb]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 2048
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																							AND shcdd.[data_space_used_percent] <= @configDataSpaceMinPercent 
																							AND @configDataSpaceMinPercent<>0
																							AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb', 'distribution')
																							AND rsr.[id] IS NULL
																					ORDER BY --[reclaimable_space_mb] DESC, 
																							 cin.[instance_name], cin.[machine_name], shcdd.[data_space_used_percent] DESC, cdn.[database_name]
			OPEN crsDatabaseMinDataSpaceIssuesDetected
			FETCH NEXT FROM crsDatabaseMinDataSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@reclaimableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMinDataSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @reclaimableSpaceMB
				end
			CLOSE crsDatabaseMinDataSpaceIssuesDetected
			DEALLOCATE crsDatabaseMinDataSpaceIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMinDataSpaceIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--High Usage of Log Space - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 32768 = 32768)
		begin
			RAISERROR('	...Build Report: High Usage of Log Space - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseMaxLogSpaceIssuesDetected" class="category-style">High Usage of Log Space - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@configDBMinSizeForAnalysis AS [nvarchar](32)) + N' AND log size used (%) &ge; ' + CAST(@configLogSpaceMaxPercent AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="370px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log Space Used (%)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Available space (MB)</TH>
											'

			SET @idx=1		
					
			DECLARE crsDatabaseMaxLogSpaceIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[size_mb]
																							, shcdd.[log_size_mb]
																							, shcdd.[log_space_used_percent]
																							, ((100.0 - shcdd.[log_space_used_percent]) * shcdd.[log_size_mb]) / 100 AS [available_space_mb]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 32768
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																							AND shcdd.[log_space_used_percent] >= @configLogSpaceMaxPercent 
																							AND @configLogSpaceMaxPercent<>0
																							AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb', 'distribution')
																							AND rsr.[id] IS NULL
																					ORDER BY --[available_space_mb] DESC, 
																							 cin.[instance_name], cin.[machine_name], shcdd.[data_space_used_percent] DESC, cdn.[database_name]
			OPEN crsDatabaseMaxLogSpaceIssuesDetected
			FETCH NEXT FROM crsDatabaseMaxLogSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@reclaimableSpaceMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseMaxLogSpaceIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @logSizeMB, @logSpaceUsedPercent, @reclaimableSpaceMB
				end
			CLOSE crsDatabaseMaxLogSpaceIssuesDetected
			DEALLOCATE crsDatabaseMaxLogSpaceIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseMaxLogSpaceIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--Log vs. Data - Allocated Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4096 = 4096)
		begin
			RAISERROR('	...Build Report: Log vs. Data - Allocated Size - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseLogVsDataSizeIssuesDetected" class="category-style">Log vs. Data - Allocated Size - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="8">size (MB) &ge; ' + CAST(@configDBMinSizeForAnalysis AS [nvarchar](32)) + N' AND log/data size (%) &gt; ' + CAST(@configLogVsDataPercent AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="370px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Data Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log Size (MB)</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Log vs. Data (%)</TH>'

			SET @idx=1		

			DECLARE crsDatabaseLogVsDataSizeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    [machine_name], [instance_name], [is_clustered], [cluster_node_machine_name], [database_name]
																								, [size_mb], [data_size_mb], [log_size_mb]
																								, [log_vs_data]
																						FROM (
																								SELECT  cin.[machine_name], cin.[instance_name]
																										, cin.[is_clustered], cin.[cluster_node_machine_name]
																										, cdn.[database_name]
																										, shcdd.[size_mb]
																										, shcdd.[data_size_mb]
																										, shcdd.[log_size_mb]
																										, (shcdd.[log_size_mb] / shcdd.[data_size_mb] * 100.) AS [log_vs_data]
																								FROM [dbo].[vw_catalogInstanceNames]  cin
																								INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																								INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																								LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																			AND rsr.[rule_id] = 4096
																																			AND rsr.[active] = 1
																																			AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																								WHERE cin.[instance_active]=1
																										AND cdn.[active]=1
																										AND cin.[project_id] = @projectID	
																										AND shcdd.[data_size_mb] <> 0
																										AND (shcdd.[log_size_mb] / shcdd.[data_size_mb] * 100.) > @configLogVsDataPercent
																										AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																										AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb', 'distribution')
																										AND rsr.[id] IS NULL
																							)X
																						WHERE [log_vs_data] >= @configLogVsDataPercent
																						ORDER BY [instance_name], [machine_name], [log_vs_data] DESC, [database_name]
			OPEN crsDatabaseLogVsDataSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @logSizeMB, @logVSDataPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="370px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logVSDataPercent AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseLogVsDataSizeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @dataSizeMB, @logSizeMB, @logVSDataPercent
				end
			CLOSE crsDatabaseLogVsDataSizeIssuesDetected
			DEALLOCATE crsDatabaseLogVsDataSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseLogVsDataSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		

	-----------------------------------------------------------------------------------------------------
	--Databases with Fixed File(s) Size - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 4194304 = 4194304)
		begin
			RAISERROR('	...Databases with Fixed File(s) Size - Issues Detected', 10, 1) WITH NOWAIT
		
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseFixedFileSizeIssuesDetected" class="category-style">Databases with Fixed File(s) Size</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold">Instance Name</TH>
											<TH WIDTH="220px" class="details-bold">Database Name</TH>
											<TH WIDTH="120px" class="details-bold">Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold">Data Size (MB)</TH>
											<TH WIDTH="100px" class="details-bold">Data Space Used (%)</TH>
											<TH WIDTH="120px" class="details-bold">Log Size (MB)</TH>
											<TH WIDTH="100px" class="details-bold">Log Space Used (%)</TH>
											<TH WIDTH="150px" class="details-bold">State</TH>'

			SET @idx=1		
			
			DECLARE crsDatabaseFixedFileSizeIssuesDetected CURSOR READ_ONLY LOCAL FOR	
																				SELECT    cin.[instance_name]
																						, cdn.[database_name], cdn.[state_desc]
																						, shcdd.[size_mb]
																						, shcdd.[data_size_mb], shcdd.[data_space_used_percent]
																						, shcdd.[log_size_mb], shcdd.[log_space_used_percent] 
																				FROM [dbo].[vw_catalogInstanceNames] cin
																				INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																				LEFT  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																				LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 4194304
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE	cin.[instance_active]=1
																						AND cdn.[active]=1
																						AND cin.[project_id] = @projectID	
																						AND shcdd.[is_growth_limited]=1
																						AND rsr.[id] IS NULL
																				ORDER BY cdn.[database_name]
			OPEN crsDatabaseFixedFileSizeIssuesDetected
			FETCH NEXT FROM crsDatabaseFixedFileSizeIssuesDetected INTO  @instanceName, @databaseName, @stateDesc, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @logSizeMB, @logSpaceUsedPercent
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="220px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSizeMB AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dataSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSizeMB AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@logSpaceUsedPercent AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + ISNULL(@stateDesc, N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseFixedFileSizeIssuesDetected INTO @instanceName, @databaseName, @stateDesc, @dbSize, @dataSizeMB, @dataSpaceUsedPercent, @logSizeMB, @logSpaceUsedPercent
				end
			CLOSE crsDatabaseFixedFileSizeIssuesDetected
			DEALLOCATE crsDatabaseFixedFileSizeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseFixedFileSizeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Databases with Improper Page Verify Option
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8388608 = 8388608)
		begin
			RAISERROR('	...Databases with Improper Page Verify Option - Issues Detected', 10, 1) WITH NOWAIT
		
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasePageVerifyIssuesDetected" class="category-style">Databases with Improper Page Verify Option</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="340px" class="details-bold">Database Name</TH>
											<TH WIDTH= "90px" class="details-bold" nowrap>SQL Version</TH>
											<TH WIDTH="100px" class="details-bold" nowrap>Compatibility</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Page Verify</TH>'

			SET @idx=1		

			DECLARE @pageVerify			[sysname],
					@compatibilityLevel	[tinyint]

			DECLARE crsDatabasePageVerifyIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, cin.[version]
																							, shcdd.[page_verify_option_desc]
																							, shcdd.[compatibility_level]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 8388608
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID
																							AND cdn.[database_name] NOT IN ('tempdb')
																							AND (   
																									(     shcdd.[page_verify_option_desc] <> 'CHECKSUM'
																									  AND cin.[version] NOT LIKE '8.%'
																									)
																								 OR (     shcdd.[page_verify_option_desc] = 'NONE'
																									  AND cin.[version] LIKE '8.%'
																									)
																								)
																							AND CHARINDEX(cdn.[state_desc], @configAdmittedState)<>0
																							AND rsr.[id] IS NULL
																					ORDER BY cin.[instance_name], cin.[machine_name], cdn.[database_name]
			OPEN crsDatabasePageVerifyIssuesDetected
			FETCH NEXT FROM crsDatabasePageVerifyIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @version, @pageVerify, @compatibilityLevel
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="340px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH= "90px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@version, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="100px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CAST(@compatibilityLevel AS [sysname]), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="160px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(@pageVerify, N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasePageVerifyIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @version, @pageVerify, @compatibilityLevel
				end
			CLOSE crsDatabasePageVerifyIssuesDetected
			DEALLOCATE crsDatabasePageVerifyIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabasePageVerifyIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')			
		end


	-----------------------------------------------------------------------------------------------------
	--Frequently Fragmented Indexes
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 16777216 = 16777216)
		begin
			RAISERROR('	...Frequently Fragmented Indexes - Issues Detected', 10, 1) WITH NOWAIT

			DECLARE @indexAnalyzedCount						[int],
					@indexesPerInstance						[int],
					@minimumIndexMaintenanceFrequencyDays	[tinyint] = 2,
					@analyzeOnlyMessagesFromTheLastHours	[tinyint] = 24 ,
					@analyzeIndexMaintenanceOperation		[nvarchar](128) = 'REBUILD'

		
			-----------------------------------------------------------------------------------------------------
			--reading report options
			SELECT	@minimumIndexMaintenanceFrequencyDays = [value]
			FROM	[report].[htmlOptions]
			WHERE	[name] = N'Minimum Index Maintenance Frequency (days)'
					AND [module] = 'health-check'

			SET @minimumIndexMaintenanceFrequencyDays = ISNULL(@minimumIndexMaintenanceFrequencyDays, 2)

			-----------------------------------------------------------------------------------------------------
			SELECT	@analyzeOnlyMessagesFromTheLastHours = [value]
			FROM	[report].[htmlOptions]
			WHERE	[name] = N'Analyze Only Messages from the last hours'
					AND [module] = 'health-check'

			SET @analyzeOnlyMessagesFromTheLastHours = ISNULL(@analyzeOnlyMessagesFromTheLastHours, 24)
	
			-----------------------------------------------------------------------------------------------------
			SELECT	@analyzeIndexMaintenanceOperation = [value]
			FROM	[report].[htmlOptions]
			WHERE	[name] = N'Analyze Index Maintenance Operation'
					AND [module] = 'health-check'

			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="FrequentlyFragmentedIndexesIssuesDetected" class="category-style">Frequently Fragmented Indexes</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="11">indexes which got fragmented in the last ' + CAST(@minimumIndexMaintenanceFrequencyDays AS [nvarchar](32)) + N' day(s), were analyzed in the last ' + CAST(@analyzeOnlyMessagesFromTheLastHours AS [nvarchar](32)) + N' hours and last action was in (' + @analyzeIndexMaintenanceOperation + N')</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="11">consider lowering the fill-factor with at least 5 percent</TD>
							</TR>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold">Instance Name</TH>
											<TH WIDTH="120px" class="details-bold">Database Name</TH>
											<TH WIDTH="120px" class="details-bold">Table Name</TH>
											<TH WIDTH="120px" class="details-bold">Index Name</TH>
											<TH WIDTH="100px" class="details-bold">Type</TH>
											<TH WIDTH=" 80px" class="details-bold">Frequency (days)</TH>
											<TH WIDTH=" 80px" class="details-bold">Page Count</TH>
											<TH WIDTH=" 90px" class="details-bold">Fragmentation</TH>
											<TH WIDTH="100px" class="details-bold">Page Density Deviation</TH>
											<TH WIDTH=" 80px" class="details-bold">Fill-Factor</TH>
											<TH WIDTH="120px" class="details-bold">Last Action</TH>
											'
			SET @idx=1		

			-----------------------------------------------------------------------------------------------------
			RAISERROR('		...analyzing fragmentation logs', 10, 1) WITH NOWAIT

			IF OBJECT_ID('tempdb..#filteredStatsIndexesFrequentlyFragmented]') IS NOT NULL
				DROP TABLE #filteredStatsIndexesFrequentlyFragmented

			SELECT iff.*
			INTO #filteredStatsIndexesFrequentlyFragmented
			FROM [dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectCode, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, @analyzeIndexMaintenanceOperation) iff
			INNER JOIN [dbo].[vw_catalogInstanceNames]  cin ON iff.[instance_name] = cin.[instance_name]
			LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
														AND rsr.[rule_id] = 16777216
														AND rsr.[active] = 1
														AND (rsr.[skip_value]=iff.[instance_name])
			WHERE cin.[instance_active] = 1
				 AND cin.[project_id] = @projectID

			CREATE INDEX IX_filteredStatsIndexesFrequentlyFragmented_InstanceName ON #filteredStatsIndexesFrequentlyFragmented([instance_name])

			RAISERROR('		...done', 10, 1) WITH NOWAIT
			-----------------------------------------------------------------------------------------------------
			SET @indexAnalyzedCount=0

			DECLARE crsFrequentlyFragmentedIndexesMachineNames CURSOR READ_ONLY LOCAL FOR		SELECT    iff.[instance_name]
																										, COUNT(*) AS [index_count]
																								FROM #filteredStatsIndexesFrequentlyFragmented iff
																								GROUP BY iff.[instance_name]
																								ORDER BY iff.[instance_name]
			OPEN crsFrequentlyFragmentedIndexesMachineNames
			FETCH NEXT FROM crsFrequentlyFragmentedIndexesMachineNames INTO  @instanceName, @indexesPerInstance
			WHILE @@FETCH_STATUS=0
				begin
					SET @indexAnalyzedCount = @indexAnalyzedCount + @indexesPerInstance
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@indexesPerInstance AS [nvarchar](64)) + N'"><A NAME="FrequentlyFragmentedIndexesCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([database_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([object_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT" >' + ISNULL([index_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="100px" class="details" ALIGN="LEFT" >' + ISNULL([index_type], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" >' + ISNULL(CAST([interval_days] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([page_count] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "90px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([fragmentation] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="100px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([page_density_deviation] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" >' + ISNULL(CAST([fill_factor] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([last_action_made], N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT    [event_date_utc], [database_name], [object_name], [index_name]
															, [interval_days], [index_type], [fragmentation], [page_count], [fill_factor], [page_density_deviation], [last_action_made]
															, ROW_NUMBER() OVER(ORDER BY [database_name], [object_name], [index_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	#filteredStatsIndexesFrequentlyFragmented
													WHERE	[instance_name] =  @instanceName
												)X
											ORDER BY [database_name], [object_name], [index_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsFrequentlyFragmentedIndexesMachineNames INTO  @instanceName, @indexesPerInstance

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
												<TD class="details" COLSPAN=11>&nbsp;</TD>
										</TR>'
				end
			CLOSE crsFrequentlyFragmentedIndexesMachineNames
			DEALLOCATE crsFrequentlyFragmentedIndexesMachineNames

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea						

			SET @HTMLReport = REPLACE(@HTMLReport, '{FrequentlyFragmentedIndexesIssuesDetectedCount}', '(' + CAST((@indexAnalyzedCount) AS [nvarchar]) + ')')			
		end
		
	
	-----------------------------------------------------------------------------------------------------
	--Outdated Backup for Databases - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8192 = 8192)
		begin
			RAISERROR('	...Build Report: Outdated Backup for Databases - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseBACKUPAgeIssuesDetected" class="category-style">Outdated Backup for Databases - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">backup age (system db) &gt; ' + CAST(@configSystemDatabaseBACKUPAgeDays AS [nvarchar](32)) + N' OR backup age (user db) &gt; ' + CAST(@configUserDatabaseBACKUPAgeDays AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="360px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Last Backup Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>Backup Age (Days)</TH>'
			SET @idx=1		

			DECLARE crsDatabaseBACKUPAgeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, cdn.[database_name]
																							, shcdd.[size_mb]
																							, shcdd.[last_backup_time]
																							, DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) AS [backup_age_days]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 8192
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND (
																									(    cdn.[database_name] NOT IN ('master', 'model', 'msdb', 'distribution') 
																										AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configUserDatabaseBACKUPAgeDays
																									)
																								    OR (    cdn.[database_name] IN ('master', 'model', 'msdb', 'distribution') 
																										AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configSystemDatabaseBACKUPAgeDays
																									)
																									OR (
																											cdn.[database_name] NOT IN ('tempdb')
																										AND shcdd.[last_backup_time] IS NULL
																									)
																								)
																							AND CHARINDEX(cdn.[state_desc], @configAdmittedState)<>0
																							AND rsr.[id] IS NULL
																					ORDER BY [instance_name], [machine_name], [backup_age_days] DESC, [database_name]
			OPEN crsDatabaseBACKUPAgeIssuesDetected
			FETCH NEXT FROM crsDatabaseBACKUPAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastBackupDate, @lastDatabaseEventAgeDays
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="360px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastBackupDate, 121), N'N/A') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@lastDatabaseEventAgeDays AS [nvarchar](64)), N'N/A')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseBACKUPAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastBackupDate, @lastDatabaseEventAgeDays
				end
			CLOSE crsDatabaseBACKUPAgeIssuesDetected
			DEALLOCATE crsDatabaseBACKUPAgeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseBACKUPAgeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end

		
	-----------------------------------------------------------------------------------------------------
	--Outdated DBCC CHECKDB Databases - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 16384 = 16384)
		begin
			RAISERROR('	...Build Report: Outdated DBCC CHECKDB Databases - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabaseDBCCCHECKDBAgeIssuesDetected" class="category-style">Outdated DBCC CHECKDB Databases - Issues Detected</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">dbcc checkdb age (system db) &gt; ' + CAST(@configSystemDBCCCHECKDBAgeDays AS [nvarchar](32)) + N' OR dbcc checkdb age (user db) &gt; ' + CAST(@configUserDBCCCHECKDBAgeDays AS [nvarchar](32)) + N'</TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="360px" class="details-bold">Database Name</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>DB Size (MB)</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Last CHECKDB Date</TH>
											<TH WIDTH="80px" class="details-bold" nowrap>CHECKDB Age (Days)</TH>'
			SET @idx=1		

			DECLARE crsDatabaseDBCCCHECKDBAgeIssuesDetected CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, cdn.[database_name]
																								, shcdd.[size_mb]
																								, shcdd.[last_dbcc checkdb_time]
																								, CASE	 WHEN shcdd.[last_dbcc checkdb_time] IS NOT NULL 
																										THEN DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) 
																										ELSE NULL
																									END AS [dbcc_checkdb_age_days]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																						INNER JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 16384
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND cin.[project_id] = @projectID	
																								AND (
																										(    cdn.[database_name] NOT IN ('master', 'model', 'msdb', 'distribution') 
																											AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @configUserDBCCCHECKDBAgeDays
																										)
																										OR (    cdn.[database_name] IN ('master', 'model', 'msdb', 'distribution') 
																											AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @configSystemDBCCCHECKDBAgeDays
																										)
																										OR (
																												cdn.[database_name] NOT IN ('tempdb')
																											AND shcdd.[last_dbcc checkdb_time] IS NULL
																										)
																									)
																								AND CHARINDEX(cdn.[state_desc], 'ONLINE')<>0
																								AND cin.[version] NOT LIKE '8.%'
																								AND rsr.[id] IS NULL
																						ORDER BY [instance_name], [machine_name], [dbcc_checkdb_age_days] DESC, [database_name]
			OPEN crsDatabaseDBCCCHECKDBAgeIssuesDetected
			FETCH NEXT FROM crsDatabaseDBCCCHECKDBAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastCheckDBDate, @lastDatabaseEventAgeDays
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes' + ISNULL(N'<BR>[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="360px" class="details" ALIGN="LEFT">' + ISNULL(@databaseName, N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@dbSize AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @lastCheckDBDate, 121), N'N/A') + N'</TD>' + 
										N'<TD WIDTH="80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST(@lastDatabaseEventAgeDays AS [nvarchar](64)), N'N/A')+ N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabaseDBCCCHECKDBAgeIssuesDetected INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @databaseName, @dbSize, @lastCheckDBDate, @lastDatabaseEventAgeDays
				end
			CLOSE crsDatabaseDBCCCHECKDBAgeIssuesDetected
			DEALLOCATE crsDatabaseDBCCCHECKDBAgeIssuesDetected

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{DatabaseDBCCCHECKDBAgeIssuesDetectedCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end
		

	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 524288 = 524288)
		begin
			RAISERROR('	...Build Report: Errorlog Messages - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="ErrorlogMessagesPermissionErrors" class="category-style">Errorlog Messages - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="540px" class="details-bold">Message</TH>'

			SET @idx=1		

			DECLARE crsErrorlogMessagesPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																							, MAX(lsam.[event_date_utc]) [event_date_utc]
																							, lsam.[message]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 524288
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE	cin.[instance_active]=1
																							AND cin.[project_id] = @projectID
																							AND lsam.descriptor IN (N'dbo.usp_hcCollectErrorlogMessages')
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																					ORDER BY cin.[instance_name], cin.[machine_name], [event_date_utc]
			OPEN crsErrorlogMessagesPermissionErrors
			FETCH NEXT FROM crsErrorlogMessagesPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + CASE WHEN @isClustered=0 THEN @machineName ELSE dbo.ufn_reportHTMLGetClusterNodeNames(@projectID, @instanceName) END + N'</TD>' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap>' + @instanceName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes<BR>' + ISNULL(N'[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="540px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsErrorlogMessagesPermissionErrors INTO @machineName, @instanceName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsErrorlogMessagesPermissionErrors
			DEALLOCATE crsErrorlogMessagesPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{ErrorlogMessagesPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Issues Detected
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 1048576 = 1048576)
		begin
			RAISERROR('	...Build Report: Errorlog Messages - Issues Detected', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="ErrorlogMessagesIssuesDetected" class="category-style">Errorlog Messages - Issues Detected (last ' + CAST(@configErrorlogMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit messages per instance to maximum ' + CAST(@configErrorlogMessageLimit AS [nvarchar](32)) + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Log Date</TH>
											<TH WIDTH= "60px" class="details-bold" nowrap>Process Info</TH>
											<TH WIDTH="710px" class="details-bold">Message</TH>'

			SET @idx=1		

			-----------------------------------------------------------------------------------------------------
			RAISERROR('		...analyzing errorlog messages', 10, 1) WITH NOWAIT

			IF OBJECT_ID('tempdb..#filteredStatsSQLServerErrorlogDetail') IS NOT NULL
				DROP TABLE #filteredStatsSQLServerErrorlogDetail

			SET @dateTimeLowerLimit = DATEADD(hh, -@configErrorlogMessageLastHours, GETDATE())

			SELECT DISTINCT 
					cin.[instance_name], 
					eld.[log_date], eld.[id], 
					eld.[process_info], eld.[text]
			INTO #filteredStatsSQLServerErrorlogDetail
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [health-check].[vw_statsSQLServerErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
			LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
														AND rsr.[rule_id] = 1048576
														AND rsr.[active] = 1
														AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
			WHERE cin.[instance_active]=1
					AND cin.[project_id] = @projectID																							
					AND eld.[log_date] >= @dateTimeLowerLimit
					AND NOT EXISTS	( 
										SELECT 1
										FROM	[report].[hardcodedFilters] chf 
										WHERE	chf.[module] = 'health-check'
												AND chf.[object_name] = 'statsSQLServerErrorlogDetails'
												AND chf.[active] = 1
												AND PATINDEX(chf.[filter_pattern], eld.[text]) > 0
									)
					AND rsr.[id] IS NULL
			
			CREATE INDEX IX_filteredStatsSQLServerErrorlogDetail_InstanceName ON #filteredStatsSQLServerErrorlogDetail([instance_name])

			RAISERROR('		...done', 10, 1) WITH NOWAIT

			-----------------------------------------------------------------------------------------------------
			SET @issuesDetectedCount = 0 
			DECLARE crsErrorlogMessagesInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT DISTINCT
																						  [instance_name]
																						, COUNT(*) AS [messages_count]
																				FROM #filteredStatsSQLServerErrorlogDetail
																				GROUP BY [instance_name]
																				ORDER BY [instance_name]
			OPEN crsErrorlogMessagesInstanceName
			FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					IF @messageCount > @configErrorlogMessageLimit SET @messageCount = @configErrorlogMessageLimit
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="ErrorlogMessagesCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT N'<TD WIDTH="160px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [log_date], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="60px" class="details" ALIGN="LEFT">' + ISNULL([process_info], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="710px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([text], 0), N'&nbsp;')  + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	TOP (@messageCount)
															[log_date], [id], 
															[process_info], [text],
															ROW_NUMBER() OVER(ORDER BY [log_date], [id]) [row_no],
															SUM(1) OVER() AS [row_count]
													FROM	#filteredStatsSQLServerErrorlogDetail													
													WHERE	[instance_name] = @instanceName
												)X
											ORDER BY [log_date], [id]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1

					FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
				end
			CLOSE crsErrorlogMessagesInstanceName
			DEALLOCATE crsErrorlogMessagesInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{ErrorlogMessagesIssuesDetectedCount}', '(' + CAST((@issuesDetectedCount) AS [nvarchar]) + ')')
		end

	
	-----------------------------------------------------------------------------------------------------
	--Databases Status - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 2 = 2) AND (@flgOptions & 8 = 8)
		begin
			RAISERROR('	...Build Report: Databases Status - Complete Details', 10, 1) WITH NOWAIT

			DECLARE   @dbCount		[int]
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DatabasesStatusCompleteDetails" class="category-style">Databases Status - Complete Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold">Instance Name</TH>
											<TH WIDTH="200px" class="details-bold">Database Name</TH>
											<TH WIDTH=" 80px" class="details-bold">Size (MB)</TH>
											<TH WIDTH=" 80px" class="details-bold">Data Size (MB)</TH>
											<TH WIDTH=" 60px" class="details-bold">Data Space Used (%)</TH>
											<TH WIDTH=" 80px" class="details-bold">Log Size (MB)</TH>
											<TH WIDTH=" 60px" class="details-bold">Log Space Used (%)</TH>
											<TH WIDTH="150px" class="details-bold">BACKUP Date</TH>
											<TH WIDTH="150px" class="details-bold">CHECKDB Date</TH>
											<TH WIDTH="150px" class="details-bold">State</TH>
											'

			SET @idx=1		
			
			DECLARE crsDatabasesStatusMachineNames CURSOR READ_ONLY LOCAL FOR		SELECT    cin.[machine_name], cin.[instance_name]
																							, COUNT(*) AS [database_count]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_catalogDatabaseNames] cdn ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 8
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[instance_name]
																							, cin.[is_clustered], cin.[cluster_node_machine_name]
																					ORDER BY cin.[instance_name], cin.[machine_name]
			OPEN crsDatabasesStatusMachineNames
			FETCH NEXT FROM crsDatabasesStatusMachineNames INTO  @machineName, @instanceName, @dbCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@dbCount AS [nvarchar](64)) + N'"><A NAME="DatabasesStatusCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + ISNULL([database_name], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([size_mb] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([data_size_mb] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "60px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([data_space_used_percent] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([log_size_mb] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "60px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([data_space_used_percent] AS [nvarchar](64)), N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [last_backup_time], 121), N'N/A') + N'</TD>' + 
													N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [last_dbcc checkdb_time], 121), N'N/A') + N'</TD>' + 
													N'<TD WIDTH="150px" class="details" ALIGN="LEFT">' + ISNULL([state_desc], N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END
											FROM (
													SELECT    cdn.[database_name], cdn.[state_desc]
															, shcdd.[size_mb]
															, shcdd.[data_size_mb], shcdd.[data_space_used_percent]
															, shcdd.[log_size_mb], shcdd.[log_space_used_percent] 
															, shcdd.[last_backup_time], shcdd.[last_dbcc checkdb_time]
															, ROW_NUMBER() OVER(ORDER BY cdn.[database_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM [dbo].[vw_catalogInstanceNames] cin
													INNER JOIN [dbo].[vw_catalogDatabaseNames]			  cdn	ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
													LEFT  JOIN [health-check].[vw_statsDatabaseDetails] shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id]
													WHERE	cin.[instance_active]=1
															AND cdn.[active]=1
															AND cin.[project_id] = @projectID	
															AND cin.[instance_name] =  @instanceName
															AND cin.[machine_name] = @machineName
												)X
											ORDER BY [database_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx+1

					FETCH NEXT FROM crsDatabasesStatusMachineNames INTO @machineName, @instanceName, @dbCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
												<TD class="details" COLSPAN=10>&nbsp;</TD>
										</TR>'
				end
			CLOSE crsDatabasesStatusMachineNames
			DEALLOCATE crsDatabasesStatusMachineNames

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					
		end


	-----------------------------------------------------------------------------------------------------
	--SQL Server Agent Jobs Status - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 4 = 4) AND (@flgOptions & 32 = 32)
		begin
			RAISERROR('	...Build Report: SQL Server Agent Jobs Status - Complete Details', 10, 1) WITH NOWAIT
			
			DECLARE @jobCount [int]

			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="SQLServerAgentJobsStatusCompleteDetails" class="category-style">SQL Server Agent Jobs Status - Complete Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="200px" class="details-bold">Job Name</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>Execution Status</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>Execution Date</TH>
											<TH WIDTH= "80px" class="details-bold" nowrap>Execution Time</TH>
											<TH WIDTH="490px" class="details-bold">Message</TH>'

			SET @idx=1		
			
			DECLARE crsSQLServerAgentJobsInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT	ssajh.[instance_name], COUNT(*) AS [job_count]
																					FROM	[health-check].[vw_statsSQLServerAgentJobsHistory] ssajh
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 32
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value]=ssajh.[instance_name])
																					WHERE	ssajh.[project_id]=@projectID
																							AND rsr.[id] IS NULL
																					GROUP BY ssajh.[instance_name]
																					ORDER BY ssajh.[instance_name]
			OPEN crsSQLServerAgentJobsInstanceName
			FETCH NEXT FROM crsSQLServerAgentJobsInstanceName INTO @instanceName, @jobCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@jobCount AS [nvarchar](64)) + '"><A NAME="SQLServerAgentJobsStatusCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="200px" class="details" ALIGN="LEFT">' + [job_name] + N'</TD>' + 
													N'<TD WIDTH="80px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN [last_execution_status] = 0 THEN N'Failed'
																														WHEN [last_execution_status] = 1 THEN N'Succeded'
																														WHEN [last_execution_status] = 2 THEN N'Retry'
																														WHEN [last_execution_status] = 3 THEN N'Canceled'
																														WHEN [last_execution_status] = 4 THEN N'In progress'
																														ELSE N'&nbsp;'
																													END
																+ N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" nowrap>' + isnull([last_execution_date], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH= "80px" class="details" ALIGN="CENTER" nowrap>' + isnull([last_execution_time], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="490px" class="details" ALIGN="LEFT">' + REPLACE(REPLACE(REPLACE(ISNULL([dbo].[ufn_reportHTMLPrepareText](CASE WHEN LEFT([message], 2) = '--' THEN SUBSTRING([message], 3, LEN([message])) ELSE [message] END, 0), N'&nbsp;') , CHAR(13), N'<BR>'), '--', N'<BR>'), N'<BR><BR>', N'<BR>') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	[job_name], [last_execution_status], [last_execution_date], [last_execution_time], [message]
															, ROW_NUMBER() OVER(ORDER BY [job_name]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	[health-check].[vw_statsSQLServerAgentJobsHistory]
													WHERE	[project_id]=@projectID
															AND [instance_name] = @instanceName
												)X
											ORDER BY [job_name]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @idx=@idx+1
					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')

					FETCH NEXT FROM crsSQLServerAgentJobsInstanceName INTO @instanceName, @jobCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																	<TD class="details" COLSPAN=6>&nbsp;</TD>
															</TR>'
				end
			CLOSE crsSQLServerAgentJobsInstanceName
			DEALLOCATE crsSQLServerAgentJobsInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					
		end


	-----------------------------------------------------------------------------------------------------
	--Disk Space Information - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 8 = 8) AND (@flgOptions & 65536 = 65536)
		begin
			RAISERROR('	...Build Report: Disk Space Information - Complete Details', 10, 1) WITH NOWAIT

			DECLARE   @volumeCount		[int]
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="DiskSpaceInformationCompleteDetails" class="category-style">Disk Space Information - Complete Details</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="300px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="100px" class="details-bold">Logical Drive</TH>
											<TH WIDTH="370px" class="details-bold">Volume Mount Point</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Total Size (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Available Space (MB)</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Percent Available (%)</TH>'

			SET @idx=1		

			DECLARE crsDiskSpaceInformationMachineNames CURSOR READ_ONLY LOCAL FOR		SELECT DISTINCT
																								  cin.[machine_name]/*, cin.[instance_name]*/
																								, cin.[is_clustered], cin.[cluster_node_machine_name]
																								, COUNT(*) AS [volume_count]
																						FROM [dbo].[vw_catalogInstanceNames]  cin
																						INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
																						LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 65536
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cin.[project_id] = @projectID
																								AND rsr.[id] IS NULL	
																						GROUP BY cin.[machine_name], cin.[instance_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																						ORDER BY cin.[machine_name]/*, cin.[instance_name]*/
			OPEN crsDiskSpaceInformationMachineNames
			FETCH NEXT FROM crsDiskSpaceInformationMachineNames INTO  @machineName, /*@instanceName, */@isClustered, @clusterNodeName, @volumeCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" ROWSPAN="' + CAST(@volumeCount AS [nvarchar](64)) + N'"><A NAME="DiskSpaceInformationCompleteDetails' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'">' + CASE WHEN @isClustered=0 THEN @machineName ELSE @clusterNodeName END + N'</A></TD>'

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="100px" class="details" ALIGN="CENTER">' + ISNULL([logical_drive], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="270px" class="details" ALIGN="LEFT">' + ISNULL([volume_mount_point], N'&nbsp;') + N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([total_size_mb] AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([available_space_mb] AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'<TD WIDTH="120px" class="details" ALIGN="RIGHT" nowrap>' + ISNULL(CAST([percent_available] AS [nvarchar](64)), N'&nbsp;')+ N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT  DISTINCT
																  dsi.[logical_drive]
																, dsi.[volume_mount_point]
																, MAX(dsi.[total_size_mb])		AS [total_size_mb]
																, MIN(dsi.[available_space_mb]) AS [available_space_mb]
																, MIN(dsi.[percent_available])	AS [percent_available]
																, ROW_NUMBER() OVER(ORDER BY dsi.[logical_drive], dsi.[volume_mount_point]) [row_no]
																, SUM(1) OVER() AS [row_count]
													FROM [dbo].[vw_catalogInstanceNames] cin
													INNER JOIN [health-check].[vw_statsDiskSpaceInfo]		dsi	ON dsi.[project_id] = cin.[project_id] AND dsi.[instance_id] = cin.[instance_id]
													WHERE	cin.[instance_active]=1
															AND cin.[project_id] = @projectID	
															/*AND cin.[instance_name] =  @instanceName*/
															AND cin.[machine_name] = @machineName
													GROUP BY dsi.[logical_drive], dsi.[volume_mount_point]
												)X
											ORDER BY [logical_drive], [volume_mount_point]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1

					FETCH NEXT FROM crsDiskSpaceInformationMachineNames INTO @machineName, /*@instanceName, */@isClustered, @clusterNodeName, @volumeCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
																	<TD class="details" COLSPAN=6>&nbsp;</TD>
															</TR>'
				end
			CLOSE crsDiskSpaceInformationMachineNames
			DEALLOCATE crsDiskSpaceInformationMachineNames

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					
		end


	-----------------------------------------------------------------------------------------------------
	--Errorlog Messages - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 16 = 16) AND (@flgOptions & 2097152 = 2097152)
		begin
			RAISERROR('	...Build Report: Errorlog Messages - Complete Details', 10, 1) WITH NOWAIT

			SET @idx=1		
			
			SET @HTMLReportArea = N''
			SET @HTMLReportArea = @HTMLReportArea + 
					N'<A NAME="ErrorlogMessagesCompleteDetails" class="category-style">Errorlog Messages - Complete Details (last ' + CAST(@configErrorlogMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Instance Name</TH>
											<TH WIDTH="160px" class="details-bold" nowrap>Log Date</TH>
											<TH WIDTH= "60px" class="details-bold" nowrap>Process Info</TH>
											<TH WIDTH="710px" class="details-bold">Message</TH>'

			SET @dateTimeLowerLimit = DATEADD(hh, -@configErrorlogMessageLastHours, GETDATE())
			
			DECLARE crsErrorlogMessagesInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT DISTINCT
																						  cin.[instance_name]
																						, COUNT(*) AS [messages_count]
																				FROM [dbo].[vw_catalogInstanceNames]  cin
																				INNER JOIN [health-check].[vw_statsSQLServerErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
																				LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 2097152
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cin.[project_id] = @projectID	
																						AND eld.[log_date] >= @dateTimeLowerLimit
																						AND rsr.[id] IS NULL
																				GROUP BY cin.[instance_name]
																				ORDER BY cin.[instance_name]
			OPEN crsErrorlogMessagesInstanceName
			FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="ErrorlogMessagesCompleteDetails' + @instanceName + N'">' + @instanceName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="160px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [log_date], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="60px" class="details" ALIGN="LEFT">' + ISNULL([process_info], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="710px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([text], 0), N'&nbsp;') + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT	eld.[log_date], eld.[id], eld.[process_info], eld.[text]
															, ROW_NUMBER() OVER(ORDER BY eld.[log_date], eld.[id]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM	[health-check].[vw_statsSQLServerErrorlogDetails] eld
													WHERE	eld.[project_id]=@projectID
															AND eld.[instance_name] = @instanceName
															AND eld.[log_date] >= @dateTimeLowerLimit
												)X
											ORDER BY [log_date], [id]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1

					FETCH NEXT FROM crsErrorlogMessagesInstanceName INTO @instanceName, @messageCount

					IF @@FETCH_STATUS=0
						SET @HTMLReportArea = @HTMLReportArea + N'<TR VALIGN="TOP" class="color-2" HEIGHT="5px">
											<TD class="details" COLSPAN=4>&nbsp;</TD>
									</TR>'
				end
			CLOSE crsErrorlogMessagesInstanceName
			DEALLOCATE crsErrorlogMessagesInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea						
		end


	-----------------------------------------------------------------------------------------------------
	--OS Event Messages - Permission Errors
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 32 = 32) AND (@flgOptions & 67108864 = 67108864)
		begin
			RAISERROR('	...Build Report: OS Event Messages - Permission Errors', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="OSEventMessagesPermissionErrors" class="category-style">OS Event Messages - Permission Errors</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">powershell script timeout value = ' + CAST(@configOSEventsTimeOutSeconds AS [nvarchar](32)) + N' seconds </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="120px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Clustered</TH>
											<TH WIDTH="150px" class="details-bold" nowrap>Event Date (UTC)</TH>
											<TH WIDTH="740px" class="details-bold">Message</TH>'

			SET @idx=1		

			DECLARE crsOSEventMessagesPermissionErrors CURSOR READ_ONLY LOCAL FOR	SELECT    cin.[machine_name], cin.[is_clustered], cin.[cluster_node_machine_name]
																							, MAX(lsam.[event_date_utc]) [event_date_utc]
																							, lsam.[message]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [dbo].[vw_logAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																AND rsr.[rule_id] = 67108864
																																AND rsr.[active] = 1
																																AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE	cin.[instance_active]=1
																							AND cin.[project_id] = @projectID
																							AND lsam.descriptor IN (N'dbo.usp_hcCollectOSEventLogs')
																							AND rsr.[id] IS NULL
																					GROUP BY cin.[machine_name], cin.[is_clustered], cin.[cluster_node_machine_name], lsam.[message]
																					ORDER BY cin.[machine_name], [event_date_utc]
			OPEN crsOSEventMessagesPermissionErrors
			FETCH NEXT FROM crsOSEventMessagesPermissionErrors INTO @machineName, @isClustered, @clusterNodeName, @eventDate, @message
			WHILE @@FETCH_STATUS=0
				begin
					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="120px" class="details" ALIGN="LEFT" nowrap>' + @machineName + N'</TD>' + 
										N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + CASE WHEN @isClustered=0 THEN N'No' ELSE N'Yes<BR>' + ISNULL(N'[' + @clusterNodeName + ']', N'&nbsp;') END + N'</TD>' + 
										N'<TD WIDTH="150px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), @eventDate, 121), N'&nbsp;') + N'</TD>' + 
										N'<TD WIDTH="740px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText](@message, 0), N'&nbsp;') + N'</TD>' + 
									N'</TR>'
					SET @idx=@idx+1

					FETCH NEXT FROM crsOSEventMessagesPermissionErrors INTO @machineName, @isClustered, @clusterNodeName, @eventDate, @message
				end
			CLOSE crsOSEventMessagesPermissionErrors
			DEALLOCATE crsOSEventMessagesPermissionErrors

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea					

			SET @HTMLReport = REPLACE(@HTMLReport, '{OSEventMessagesPermissionErrorsCount}', '(' + CAST((@idx-1) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	--OS Event messages - Complete Details
	-----------------------------------------------------------------------------------------------------
	IF (@flgActions & 32 = 32) AND (@flgOptions & 134217728 = 134217728)
		begin
			RAISERROR('	...Build Report: OS Event messages - Complete Details', 10, 1) WITH NOWAIT
			
			SET @HTMLReportArea=N''
			SET @HTMLReportArea = @HTMLReportArea + 
							N'<A NAME="OSEventMessagesCompleteDetails" class="category-style">OS Event messages - Complete Details (last ' + CAST(@configOSEventMessageLastHours AS [nvarchar]) + N'h)</A><br>
							<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="0px" class="no-border">
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">limit messages per machine to maximum ' + CAST(@configOSEventMessageLimit AS [nvarchar](32)) + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD class="small-size" COLLSPAN="7">Severity: Critical, Error' + CASE WHEN @configOSEventGetWarningsEvent=1 THEN N', Warning' ELSE N'' END + CASE WHEN @configOSEventGetInformationEvent=1 THEN N', Information' ELSE N'' END + N' </TD>
							</TR>
							<TR VALIGN=TOP>
								<TD WIDTH="1130px">
									<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px" class="with-border">' +
										N'<TR class="color-3">
											<TH WIDTH="200px" class="details-bold" nowrap>Machine Name</TH>
											<TH WIDTH="120px" class="details-bold" nowrap>Event Time</TH>
											<TH WIDTH=" 80px" class="details-bold" nowrap>Log Name</TH>
											<TH WIDTH=" 60px" class="details-bold" nowrap>Level</TH>
											<TH WIDTH=" 60px" class="details-bold" nowrap>Event ID</TH>
											<TH WIDTH="120px" class="details-bold">Source</TH>
											<TH WIDTH="480px" class="details-bold">Message</TH>'
			SET @idx=1		

			SET @dateTimeLowerLimit = DATEADD(hh, -@configOSEventMessageLastHours, GETDATE())
			SET @issuesDetectedCount = 0 
			
			DECLARE crsOSEventMessagesInstanceName CURSOR READ_ONLY LOCAL FOR	SELECT DISTINCT
																						  oel.[machine_name]
																						, COUNT(*) AS [messages_count]
																				FROM [dbo].[vw_catalogInstanceNames]	cin
																				INNER JOIN [health-check].[vw_statsOSEventLogs]	oel	ON oel.[project_id] = cin.[project_id] AND oel.[instance_id] = cin.[instance_id]
																				LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																															AND rsr.[rule_id] = 134217728
																															AND rsr.[active] = 1
																															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																				WHERE cin.[instance_active]=1
																						AND cin.[project_id] = @projectID
																						AND rsr.[id] IS NULL
																				GROUP BY oel.[machine_name]
																				ORDER BY oel.[machine_name]
			OPEN crsOSEventMessagesInstanceName
			FETCH NEXT FROM crsOSEventMessagesInstanceName INTO @machineName, @messageCount
			WHILE @@FETCH_STATUS=0
				begin
					IF @messageCount > @configOSEventMessageLimit SET @messageCount = @configOSEventMessageLimit
					SET @issuesDetectedCount = @issuesDetectedCount + @messageCount

					SET @HTMLReportArea = @HTMLReportArea + 
								N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">' + 
										N'<TD WIDTH="200px" class="details" ALIGN="LEFT" nowrap ROWSPAN="' + CAST(@messageCount AS [nvarchar](64)) + '"><A NAME="OSEventMessagesCompleteDetails' + @machineName + N'">' + @machineName + N'</A></TD>' 

					SET @tmpHTMLReport=N''
					SELECT @tmpHTMLReport=((
											SELECT	N'<TD WIDTH="120px" class="details" ALIGN="CENTER" nowrap>' + ISNULL(CONVERT([nvarchar](24), [time_created], 121), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH=" 80px" class="details" ALIGN="LEFT" >' + ISNULL([log_type_desc], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH=" 60px" class="details" ALIGN="LEFT">' + ISNULL([level_desc], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH=" 60px" class="details" ALIGN="LEFT">' + ISNULL(CAST([event_id] AS [nvarchar]), N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="120px" class="details" ALIGN="LEFT">' + ISNULL([source], N'&nbsp;') + N'</TD>' + 
														N'<TD WIDTH="480px" class="details" ALIGN="LEFT">' + ISNULL([dbo].[ufn_reportHTMLPrepareText]([message], 0), N'&nbsp;')  + N'</TD>' + 
													N'</TR>' + 
													CASE WHEN [row_count] > [row_no]
														 THEN N'<TR VALIGN="TOP" class="' + CASE WHEN @idx & 1 = 1 THEN 'color-2' ELSE 'color-1' END + '">'
														 ELSE N''
													END

											FROM (
													SELECT  TOP (@configOSEventMessageLimit)
															oel.[time_created], oel.[log_type_desc], oel.[level_desc], 
															oel.[event_id], oel.[record_id], oel.[source], oel.[message]
															, ROW_NUMBER() OVER(ORDER BY oel.[time_created], oel.[record_id]) [row_no]
															, SUM(1) OVER() AS [row_count]
													FROM [health-check].[vw_statsOSEventLogs]	oel
													WHERE	oel.[project_id]=@projectID
															AND oel.[machine_name] = @machineName
												)X
											ORDER BY [time_created], [record_id]
											FOR XML PATH(''), TYPE
											).value('.', 'nvarchar(max)'))

					SET @HTMLReportArea = @HTMLReportArea + COALESCE(@tmpHTMLReport, '')
					SET @idx=@idx + 1
				
					FETCH NEXT FROM crsOSEventMessagesInstanceName INTO @machineName, @messageCount
				end
			CLOSE crsOSEventMessagesInstanceName
			DEALLOCATE crsOSEventMessagesInstanceName

			SET @HTMLReportArea = @HTMLReportArea + N'</TABLE>
								</TD>
							</TR>
						</TABLE>'

			SET @HTMLReportArea = @HTMLReportArea + N'<TABLE WIDTH="1130px" CELLSPACING=0 CELLPADDING="3px"><TR><TD WIDTH="1130px" ALIGN=RIGHT><A HREF="#Home" class="normal">Go Up</A></TD></TR></TABLE>'	
			SET @HTMLReport = @HTMLReport + @HTMLReportArea						

			SET @HTMLReport = REPLACE(@HTMLReport, '{OSEventMessagesIssuesDetectedCount}', '(' + CAST((@issuesDetectedCount) AS [nvarchar]) + ')')
		end


	-----------------------------------------------------------------------------------------------------
	SET @HTMLReport = @HTMLReport + N'</body></html>'	
	
	-----------------------------------------------------------------------------------------------------
	--save report entry
	-----------------------------------------------------------------------------------------------------
	INSERT INTO [report].[htmlContent](   [project_id], [module], [start_date], [flg_actions], [flg_options]
										, [file_name], [file_path]
										, [build_at], [build_duration], [html_content], [build_in_progress], [report_uid])												

			SELECT    @projectID, 'health-check', @reportBuildStartTime, @flgActions, @flgOptions
					, @HTMLReportFileName, @localStoragePath
					, @reportBuildStartTime, DATEDIFF(ms, @reportBuildStartTime, GETUTCDATE()), @HTMLReport
					, 0, NEWID()

		
	-----------------------------------------------------------------------------------------------------
	--save HTML report to external file
	-----------------------------------------------------------------------------------------------------
	SET @reportID=SCOPE_IDENTITY()

	IF @reportFileName IS NOT NULL AND LEFT(@reportFileName, 1) = '+'
		SET @HTMLReportFileName = REPLACE(REPLACE(@HTMLReportFileName, '.html', ''), '.htm', '') + '_' + CAST(@reportID AS [nvarchar]) + SUBSTRING(@reportFileName, 2, LEN(@reportFileName)-1) + '.html'
	ELSE
		SET @HTMLReportFileName = REPLACE(REPLACE(@HTMLReportFileName, '.html', ''), '.htm', '') + '_' + CAST(@reportID AS [nvarchar]) + '.html'

			
	SET @reportFilePath='"' + @localStoragePath + @HTMLReportFileName + '"'
	

	-----------------------------------------------------------------------------------------------------
	DECLARE @optionXPIsAvailable		[bit],
			@optionXPValue				[int],
			@optionXPHasChanged			[bit],
			@optionAdvancedIsAvailable	[bit],
			@optionAdvancedValue		[int],
			@optionAdvancedHasChanged	[bit]

	SELECT  @optionXPIsAvailable		= 0,
			@optionXPValue				= 0,
			@optionXPHasChanged			= 0,
			@optionAdvancedIsAvailable	= 0,
			@optionAdvancedValue		= 0,
			@optionAdvancedHasChanged	= 0

	/* enable xp_cmdshell configuration option */
	EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
														@configOptionName	= 'xp_cmdshell',
														@configOptionValue	= 1,
														@optionIsAvailable	= @optionXPIsAvailable OUT,
														@optionCurrentValue	= @optionXPValue OUT,
														@optionHasChanged	= @optionXPHasChanged OUT,
														@executionLevel		= 0,
														@debugMode			= 0

	IF @optionXPIsAvailable = 0
		begin
			/* enable show advanced options configuration option */
			EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																@configOptionName	= 'show advanced options',
																@configOptionValue	= 1,
																@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																@optionCurrentValue	= @optionAdvancedValue OUT,
																@optionHasChanged	= @optionAdvancedHasChanged OUT,
																@executionLevel		= 0,
																@debugMode			= 0

			IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																	@configOptionName	= 'xp_cmdshell',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionXPIsAvailable OUT,
																	@optionCurrentValue	= @optionXPValue OUT,
																	@optionHasChanged	= @optionXPHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= 0
		end

	/* save report using bcp */	
	SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [html_content] FROM [' + DB_NAME() + '].[report].[htmlContent] WHERE [id]=' + CAST(@reportID AS [varchar]) + '" queryout ' + @reportFilePath + ' -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'''
	EXEC (@queryToRun)
	
	/* disable xp_cmdshell configuration option */
	IF @optionXPHasChanged = 1
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'xp_cmdshell',
															@configOptionValue	= 0,
															@optionIsAvailable	= @optionXPIsAvailable OUT,
															@optionCurrentValue	= @optionXPValue OUT,
															@optionHasChanged	= @optionXPHasChanged OUT,
															@executionLevel		= 0,
															@debugMode			= 0

	/* disable show advanced options configuration option */
	IF @optionAdvancedHasChanged = 1
			EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																@configOptionName	= 'show advanced options',
																@configOptionValue	= 0,
																@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																@optionCurrentValue	= @optionAdvancedValue OUT,
																@optionHasChanged	= @optionAdvancedHasChanged OUT,
																@executionLevel		= 0,
																@debugMode			= 0


	IF @@ERROR=0
		UPDATE [report].[htmlContent]
			SET   [html_content] = NULL
				, [file_name]	 = @HTMLReportFileName
		WHERE [id] = @reportID
		
	-----------------------------------------------------------------------------------------------------
	--
	-----------------------------------------------------------------------------------------------------
	IF @recipientsList = ''		SET @recipientsList = NULL
	IF @dbMailProfileName = ''	SET @dbMailProfileName = NULL

	DECLARE	@HTTPAddress [nvarchar](128)
	
	--get configuration values
	SELECT	@HTTPAddress=[value] 
	FROM	[dbo].[appConfigurations] 
	WHERE	[name]='HTTP address for report files'
			AND [module] = 'common'

	
	-----------------------------------------------------------------------------------------------------
	--
	-----------------------------------------------------------------------------------------------------
	IF @HTTPAddress IS NOT NULL				
		begin		
			UPDATE [report].[htmlContent]
				SET   [http_address] = @HTTPAddress + @relativeStoragePath + @HTMLReportFileName
			WHERE [id] = @reportID
		end

	SELECT @eventMessageData='<report-html><detail>' + 
								'<message>Health Check report is attached.</message>' + 
								'<file_name>' + ISNULL(@HTMLReportFileName,'') + '</file_name>' + 
								CASE WHEN @HTTPAddress IS NOT NULL THEN '<http_address>' + @HTTPAddress + '</http_address>' ELSE '' END + 
								'<relative_path>' + ISNULL(@relativeStoragePath,'') + '</relative_path>' + 
								'</detail></report-html>'

	IF (@sendReportAsAttachment=1) OR (@HTTPAddress IS NULL)
		begin
			SET @file_attachments	= REPLACE(@reportFilePath, '"', '')
			PRINT @reportFilePath
			
			EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
															@sqlServerName			= @@SERVERNAME,
															@module					= 'dbo.usp_reportHTMLBuildHealthCheck',
															@eventName				= 'daily health check',
															@parameters				= @file_attachments,
															@eventMessage			= @eventMessageData,
															@dbMailProfileName		= @dbMailProfileName,
															@recipientsList			= @recipientsList,
															@eventType				= 3 /* Report */
		end
	ELSE
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @@SERVERNAME,
														@module					= 'dbo.usp_reportHTMLBuildHealthCheck',
														@eventName				= 'daily health check',
														@parameters				= NULL,
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= @dbMailProfileName,
														@recipientsList			= @recipientsList,
														@eventType				= 3 /* Report */

	-----------------------------------------------------------------------------------------------------

END TRY

BEGIN CATCH
DECLARE 
        @ErrorMessage    NVARCHAR(4000),
        @ErrorNumber     INT,
        @ErrorSeverity   INT,
        @ErrorState      INT,
        @ErrorLine       INT,
        @ErrorProcedure  NVARCHAR(200);
    -- Assign variables to error-handling functions that 
    -- capture information for RAISERROR.
    SELECT 
        @ErrorNumber = ERROR_NUMBER(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = CASE WHEN ERROR_STATE() BETWEEN 1 AND 127 THEN ERROR_STATE() ELSE 1 END ,
        @ErrorLine = ERROR_LINE(),
        @ErrorProcedure = ISNULL(ERROR_PROCEDURE(), '-');
	-- Building the message string that will contain original
    -- error information.
    SELECT @ErrorMessage = 
        N'Error %d, Level %d, State %d, Procedure %s, Line %d, ' + 
            'Message: '+ ERROR_MESSAGE();
    -- Raise an error: msg_str parameter of RAISERROR will contain
    -- the original error information.
    RAISERROR 
        (
        @ErrorMessage, 
        @ErrorSeverity, 
        @ErrorState,               
        @ErrorNumber,    -- parameter: original error number.
        @ErrorSeverity,  -- parameter: original error severity.
        @ErrorState,     -- parameter: original error state.
        @ErrorProcedure, -- parameter: original error procedure name.
        @ErrorLine       -- parameter: original error line number.
        );

        -- Test XACT_STATE:
        -- If 1, the transaction is committable.
        -- If -1, the transaction is uncommittable and should 
        --     be rolled back.
        -- XACT_STATE = 0 means that there is no transaction and
        --     a COMMIT or ROLLBACK would generate an error.

    -- Test if the transaction is uncommittable.
    IF (XACT_STATE()) = -1
    BEGIN
        PRINT
            N'The transaction is in an uncommittable state.' +
            'Rolling back transaction.'
        ROLLBACK TRANSACTION 
   END;

END CATCH

RETURN @ReturnValue
GO





--update job: Discovery & Health Check
RAISERROR('update job: Discovery & Health Check', 10, 1) WITH NOWAIT
GO
DECLARE   @queryToRun	[nvarchar](max)
		, @projectCode	[sysname]
		, @job_id		[uniqueidentifier]

SELECT @projectCode = [value] FROM dbo.appConfigurations WHERE [name] = 'Default project code'
SET @queryToRun = N'
' + CASE WHEN @projectCode IS NULL 
			THEN N'TRUNCATE TABLE [dbo].[logAnalysisMessages]' 
			ELSE N'DELETE lam FROM [dbo].[logAnalysisMessages] lam INNER JOIN [dbo].[catalogProjects] cp ON cp.[id] = lam.[project_id] WHERE cp.[code] = ''' + @projectCode + N''''
	END + N'
EXEC [dbo].[usp_refreshProjectCatalogsAndDiscovery] @projectCode	 = ''' + @projectCode + N''',
													@runDiscovery	 = 0,
													@enableXPCMDSHELL= 1,
													@debugMode		 = 0'

IF (SELECT COUNT(*) FROM msdb.dbo.sysjobs WHERE [name] LIKE '% - Discovery & Health Check%')=1
	SELECT @job_id = [job_id]
	FROM msdb.dbo.sysjobs 
	WHERE [name] LIKE '% - Discovery & Health Check%'
ELSE
	SET @job_id = NULL

RAISERROR('--	update job step 1', 10, 1) WITH NOWAIT
IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 1
									, @command = @queryToRun

RAISERROR('--	update job step 3', 10, 1) WITH NOWAIT
SET @queryToRun = N'exec dbo.usp_jobQueueExecute	@projectCode			= ''' + @projectCode + N''',
													@moduleFilter			= ''health-check'',
													@descriptorFilter		= ''%'',
													@waitForDelay			= DEFAULT,
													@debugMode				= 0'

IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 3
									, @command = @queryToRun
GO


/* maintenance-plan module */
RAISERROR('Create procedure: [dbo].[usp_mpDatabaseBackup]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseBackup]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseBackup]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseBackup]
		@sqlServerName		[sysname] = @@SERVERNAME,
		@dbName				[sysname],
		@backupLocation		[nvarchar](1024)=NULL,	/*  disk only: local or UNC */
		@flgActions			[smallint] = 1,			/*  1 - perform full database backup
														2 - perform differential database backup
														4 - perform transaction log backup
													*/
		@flgOptions			[int] = 6107,		/*  1 - use CHECKSUM (default)
													2 - use COMPRESSION, if available (default)
													4 - use COPY_ONLY
													8 - force change backup type (default): if log is set, and no database backup is found, a database backup will be first triggered
												  										    if diff is set, and no full database backup is found, a full database backup will be first triggered
												   16 - verify backup file (default)
											       32 - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
												   64 - create folders for each database (default)
												  128 - when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
												  256 - for +2k5 versions, use xp_delete_file option (default)
												  512 - skip databases involved in log shipping (primary or secondary or logs, secondary for full/diff backups) (default)
												 1024 - on alwayson availability groups, for secondary replicas, force copy-only backups (default)
												 2048 - change retention policy from RetentionDays to RetentionBackupsCount (number of full database backups to be kept)
													  - this may be forced by setting to true property 'Change retention policy from RetentionDays to RetentionBackupsCount'
												 4096 - use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default)
												*/
		@retentionDays		[smallint]	= NULL,
		@executionLevel		[tinyint]	=  0,
		@debugMode			[bit]		=  0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006 / review on 2015.03.04
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================

------------------------------------------------------------------------------------------------------------------------------------------
--returns: 0 = success, >0 = failure

DECLARE		@queryToRun  					[nvarchar](2048),
			@queryParameters				[nvarchar](512),
			@nestedExecutionLevel			[tinyint]

DECLARE		@backupFileName					[nvarchar](1024),
			@backupFilePath					[nvarchar](1024),
			@backupType						[nvarchar](8),
			@backupOptions					[nvarchar](256),
			@optionBackupWithChecksum		[bit],
			@optionBackupWithCompression	[bit],
			@optionBackupWithCopyOnly		[bit],
			@optionForceChangeBackupType	[bit],
			@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@errorCode						[int],
			@currentDate					[datetime],
			@databaseStatus					[int],
			@databaseStateDesc				[sysname]

DECLARE		@backupStartDate				[datetime],
			@backupDurationSec				[int],
			@backupSizeBytes				[bigint],
			@eventData						[varchar](8000)

-----------------------------------------------------------------------------------------
SET NOCOUNT ON

-----------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

-----------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
SET @queryToRun= 'Backup database: ' + ' [' + @dbName + ']'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

-----------------------------------------------------------------------------------------
--get default backup location
IF @backupLocation IS NULL
	begin
		SELECT	@backupLocation = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Default backup location'
				AND [module] = 'maintenance-plan'

		IF @backupLocation IS NULL
			begin
				SET @queryToRun= 'ERROR: @backupLocation parameter value not set'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=1
			end
	end

-----------------------------------------------------------------------------------------
--get default backup retention
IF @retentionDays IS NULL
	begin
		SELECT	@retentionDays = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Default backup retention (days)'
				AND [module] = 'maintenance-plan'

		IF @retentionDays IS NULL
			begin
				SET @queryToRun= 'WARNING: @retentionDays parameter value not set'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
			end
	end

-----------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
										@executionLevel		= @executionLevel,
										@debugMode			= @debugMode

SET @nestedExecutionLevel = @executionLevel + 1

--------------------------------------------------------------------------------------------------
--treat exceptions
IF @dbName='master'
	begin
		SET @optionForceChangeBackupType=0
		SET @flgActions=1 /* only full backup is allowed for master database */
	end

--------------------------------------------------------------------------------------------------
--selected backup type
SELECT @backupType = CASE WHEN @flgActions & 1 = 1 THEN N'full'
						  WHEN @flgActions & 2 = 2 THEN N'diff'
						  WHEN @flgActions & 4 = 4 THEN N'log'
					 END

--------------------------------------------------------------------------------------------------
--get database status
IF @serverVersionNum >= 9
	begin
		SET @queryToRun = N'SELECT CONVERT([sysname], DATABASEPROPERTYEX(''' + @dbName + N''', ''Status'')) AS [state]' 
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		SELECT @databaseStateDesc = [value]
		FROM #serverPropertyConfig

		SET @databaseStateDesc = ISNULL(@databaseStateDesc, 'NULL')

		/* check for the standby property */
		IF  @databaseStateDesc IN ('ONLINE')
			begin
				SET @queryToRun = N'SELECT CONVERT([sysname], DATABASEPROPERTYEX(''' + @dbName + N''', ''IsInStandBy'')) AS [state]' 
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #serverPropertyConfig
				INSERT	INTO #serverPropertyConfig([value])
						EXEC (@queryToRun)

				IF (SELECT [value] FROM #serverPropertyConfig) = '1'
					SET @databaseStateDesc = 'STANDBY'
			end

	end
ELSE
	begin
		SET @queryToRun = N'SELECT [status] FROM master.dbo.sysdatabases WHERE [name]=''' + @dbName + N'''' 
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		SELECT @databaseStatus = [value]
		FROM #serverPropertyConfig

		SET @databaseStateDesc =   CASE	WHEN @databaseStatus & 32 = 32			 THEN 'LOADING'
										WHEN @databaseStatus & 64 = 64			 THEN 'PRE RECOVERY'
										WHEN @databaseStatus & 128 = 128		 THEN 'RECOVERING'
										WHEN @databaseStatus & 256 = 256		 THEN 'NOT RECOVERED'
										WHEN @databaseStatus & 512 = 512		 THEN 'OFFLINE'
										WHEN @databaseStatus & 2048 = 2048		 THEN 'DBO USE ONLY'
										WHEN @databaseStatus & 4096 = 4096		 THEN 'SINGLE USER'
										WHEN @databaseStatus & 32768 = 32768	 THEN 'EMERGENCY MODE'
										WHEN @databaseStatus & 2097152 = 2097152 THEN 'STANDBY'
										WHEN @databaseStatus & 4194584 = 4194584 THEN 'SUSPECT'
										WHEN @databaseStatus = 0				 THEN 'UNKNOWN'
										ELSE 'ONLINE'
									END
	end

IF  @databaseStateDesc NOT IN ('ONLINE')
begin
	SET @queryToRun='Current database state (' + @databaseStateDesc + ') does not allow backup.'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

	SET @eventData='<skipaction><detail>' + 
						'<name>database backup</name>' + 
						'<type>' + @backupType + '</type>' + 
						'<affected_object>' + @dbName + '</affected_object>' + 
						'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
						'<reason>' + @queryToRun + '</reason>' + 
					'</detail></skipaction>'

	EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
										@dbName			= @dbName,
										@module			= 'dbo.usp_mpDatabaseBackup',
										@eventName		= 'database backup',
										@eventMessage	= @eventData,
										@eventType		= 0 /* info */

	RETURN 0
end


--------------------------------------------------------------------------------------------------
--skip databases involved in log shipping (primary or secondary or logs, secondary for full/diff backups)
IF @flgOptions & 512 = 512
	begin
		--for full and diff backups
		IF @flgActions IN (1, 2)
			begin
				IF @serverVersionNum >= 9			
					SET @queryToRun = N'SELECT	[secondary_database]
										FROM	msdb.dbo.log_shipping_monitor_secondary
										WHERE	[secondary_server]=@@SERVERNAME
												AND [secondary_database] = ''' + @dbName + N''''
				ELSE 
					SET @queryToRun = N'SELECT	[secondary_database_name]
										FROM	msdb.dbo.log_shipping_secondaries
										WHERE	[secondary_server_name]=@@SERVERNAME
												AND [secondary_database_name] = ''' + @dbName + N''''
			end

		--for log backups
		IF @flgActions=4
			begin
				IF @serverVersionNum >= 9			
					SET @queryToRun = N'SELECT	[primary_database]
										FROM	msdb.dbo.log_shipping_monitor_primary
										WHERE	[primary_server]=@@SERVERNAME
												AND [primary_database] = ''' + @dbName + N'''
										UNION ALL
										SELECT	[secondary_database]
										FROM	msdb.dbo.log_shipping_monitor_secondary
										WHERE	[secondary_server]=@@SERVERNAME
												AND [secondary_database] = ''' + @dbName + N''''
				ELSE 
					SET @queryToRun = N'SELECT	[primary_database_name]
										FROM	msdb.dbo.log_shipping_primaries
										WHERE	[primary_server_name]=@@SERVERNAME
												AND [primary_database_name] = ''' + @dbName + N'''
										UNION ALL
										SELECT	[secondary_database_name]
										FROM	msdb.dbo.log_shipping_secondaries
										WHERE	[secondary_server_name]=@@SERVERNAME
												AND [secondary_database_name] = ''' + @dbName + N''''
			end

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		IF (SELECT COUNT(*) FROM #serverPropertyConfig)>0
			begin
				SET @queryToRun='Log Shipping: '
				IF @flgActions IN (1, 2)
					SET @queryToRun = @queryToRun + 'Cannot perform a full or differential backup on a secondary database.'
				IF @flgActions IN (4)
					SET @queryToRun = @queryToRun + 'Cannot perform a transaction log backup since it may break the log shipping chain.'

				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + @dbName + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpDatabaseBackup',
													@eventName		= 'database backup',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				RETURN 0
			end
	end

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName			[sysname],
		@agStopLimit	[int]

SET @agStopLimit = 0
IF @serverVersionNum >= 11
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database backup',
																		@actionType			= @backupType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0
																				
--------------------------------------------------------------------------------------------------
--check recovery model for database. transaction log backup is allowed only for FULL
--if force option is selected, for SIMPLE recovery model, backup type will be changed to diff
--------------------------------------------------------------------------------------------------
IF @flgActions & 4 = 4
	begin
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + 'SELECT CAST(DATABASEPROPERTYEX(''' + @dbName + N''', ''Recovery'') AS [sysname])'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #serverPropertyConfig
		INSERT	INTO #serverPropertyConfig([value])
				EXEC (@queryToRun)

		IF (SELECT UPPER([value]) FROM #serverPropertyConfig) = 'SIMPLE'
			begin
				SET @queryToRun = 'Database recovery model is SIMPLE. Transaction log backup cannot be performed.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @eventData='<skipaction><detail>' + 
									'<name>database backup</name>' + 
									'<type>' + @backupType + '</type>' + 
									'<affected_object>' + @dbName + '</affected_object>' + 
									'<date>' + CONVERT([varchar](24), GETDATE(), 121) + '</date>' + 
									'<reason>' + @queryToRun + '</reason>' + 
								'</detail></skipaction>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
													@dbName			= @dbName,
													@module			= 'dbo.usp_mpDatabaseBackup',
													@eventName		= 'database backup',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				RETURN 0
			end
	end
	
--------------------------------------------------------------------------------------------------
--create destination path: <@backupLocation>\@sqlServerName\@dbName
IF RIGHT(@backupLocation, 1)<>'\' SET @backupLocation = @backupLocation + N'\'
SET @backupLocation = @backupLocation + @sqlServerName + '\' + CASE WHEN @flgOptions & 64 = 64 THEN @dbName + '\' ELSE '' END

SET @queryToRun = N'EXEC [' + DB_NAME() + '].[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @sqlServerName + N''',
																			@folderName		= ''' + @backupLocation + N''',
																			@executionLevel	= ' + CAST(@nestedExecutionLevel AS [nvarchar]) + N',
																			@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
												@dbName			= NULL,
												@module			= 'dbo.usp_mpDatabaseBackup',
												@eventName		= 'create folder on disk',
												@queryToRun  	= @queryToRun,
												@flgOptions		= @flgOptions,
												@executionLevel	= @nestedExecutionLevel,
												@debugMode		= @debugMode

IF @errorCode<>0 
	begin
		RETURN @errorCode
	end

--------------------------------------------------------------------------------------------------
--check if CHECKSUM backup option may apply
SET @optionBackupWithChecksum=0
IF @flgOptions & 1 = 1 AND @serverVersionNum >= 9
	SET @optionBackupWithChecksum=1

--check COMPRESSION backup option may apply
SET @optionBackupWithCompression=0
IF @flgOptions & 2 = 2 AND @serverVersionNum >= 10
	begin
		IF @serverVersionNum>=10 AND @serverVersionNum<10.5 AND (CHARINDEX('Enterprise', @serverEdition)>0 OR CHARINDEX('Developer', @serverEdition)>0)
			SET @optionBackupWithCompression=1
		
		IF @serverVersionNum>=10.5 AND (CHARINDEX('Enterprise', @serverEdition)>0 OR CHARINDEX('Developer', @serverEdition)>0 OR CHARINDEX('Standard', @serverEdition)>0)
			SET @optionBackupWithCompression=1
	end

--check COPY_ONLY backup option may apply
SET @optionBackupWithCopyOnly=0
IF @flgOptions & 4 = 4 AND @serverVersionNum >= 9
	SET @optionBackupWithCopyOnly=1

--check if another backup is needed (full)
SET @optionForceChangeBackupType=0
IF @flgOptions & 8 = 8
	begin
		--check for any full database backup (when differential should be made) or any full/incremental database backup (when transaction log should be made)
		IF @flgActions & 2 = 2 OR @flgActions & 4 = 4
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + 'SELECT COUNT(*) FROM msdb.dbo.backupset WHERE [database_name]=''' + @dbName + N''' AND [type] IN (''D''' + CASE WHEN @flgActions & 4 = 4 THEN N', ''I''' ELSE N'' END + N')'
				IF @serverVersionNum >= 9
					SET @queryToRun = @queryToRun + N' AND [is_copy_only]=0'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				DELETE FROM #serverPropertyConfig
				INSERT	INTO #serverPropertyConfig([value])
						EXEC (@queryToRun)

				IF (SELECT [value] FROM #serverPropertyConfig) = 0
					begin
						SET @queryToRun = 'WARNING: Specified backup type cannot be performed since no full database backup exists. A full database backup will be taken before the requested backup type.'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @optionForceChangeBackupType=1 
					end
			end			
	end

--------------------------------------------------------------------------------------------------
--compiling backup options
SET @backupOptions=N''

IF @optionBackupWithChecksum=1
	SET @backupOptions = @backupOptions + N', CHECKSUM'
IF @optionBackupWithCompression=1
	SET @backupOptions = @backupOptions + N', COMPRESSION'
IF @optionBackupWithCopyOnly=1
	SET @backupOptions = @backupOptions + N', COPY_ONLY'
IF ISNULL(@retentionDays, 0) <> 0
	SET @backupOptions = @backupOptions + N', RETAINDAYS=' + CAST(@retentionDays AS [nvarchar](32))

--------------------------------------------------------------------------------------------------
--run a full database backup, in order to perform an additional diff or log backup
IF @optionForceChangeBackupType=1
	begin
		SET @currentDate = GETDATE()
		
		IF @agName IS NULL
			SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@sqlServerName, @dbName, 'full', @currentDate)
		ELSE
			SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@agName, @dbName, 'full', @currentDate)

		SET @queryToRun	= N'BACKUP DATABASE ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH STATS = 10, NAME = ''' + @backupFileName + N'''' + @backupOptions
		--IF @debugMode=1	
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseBackup',
														@eventName		= 'database backup',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

--------------------------------------------------------------------------------------------------
SET @currentDate = GETDATE()
IF @agName IS NULL
	SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@sqlServerName, @dbName, @backupType, @currentDate)
ELSE
	SET @backupFileName = dbo.[ufn_mpBackupBuildFileName](@agName, @dbName, @backupType, @currentDate)

IF @flgActions & 1 = 1 
	begin
		SET @queryToRun	= N'BACKUP DATABASE ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH STATS = 10, NAME = ''' + @backupFileName + N'''' + @backupOptions
	end

IF @flgActions & 2 = 2
	begin
		SET @queryToRun	= N'BACKUP DATABASE ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH DIFFERENTIAL, STATS = 10, NAME=''' + @backupFileName + N'''' + @backupOptions
	end

IF @flgActions & 4 = 4
	begin
		SET @queryToRun	= N'BACKUP LOG ['+ @dbName + N'] TO DISK = ''' + @backupLocation + @backupFileName + N''' WITH STATS = 10, NAME=''' + @backupFileName + N'''' + @backupOptions
	end

--IF @debugMode=1	
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0	
EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
												@dbName			= @dbName,
												@module			= 'dbo.usp_mpDatabaseBackup',
												@eventName		= 'database backup',
												@queryToRun  	= @queryToRun,
												@flgOptions		= @flgOptions,
												@executionLevel	= @nestedExecutionLevel,
												@debugMode		= @debugMode

IF @errorCode=0
	begin
		SET @queryToRun = '	SELECT TOP 1  bs.[backup_start_date]
										, DATEDIFF(ss, bs.[backup_start_date], bs.[backup_finish_date]) AS [backup_duration_sec]
										, ' + CASE WHEN @optionBackupWithCompression=1 THEN 'bs.[compressed_backup_size]' ELSE 'bs.[backup_size]' END + ' AS [backup_size]
							FROM msdb.dbo.backupset bs
							INNER JOIN msdb.dbo.backupmediafamily bmf ON bmf.[media_set_id] = bs.[media_set_id]
							WHERE bmf.[physical_device_name] = (''' + @backupLocation + @backupFileName + N''')
							ORDER BY bs.[backup_set_id] DESC'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		SET @queryToRun = N' SELECT   @backupStartDate = [backup_start_date]
									, @backupDurationSec = [backup_duration_sec]
									, @backupSizeBytes = [backup_size]
							FROM (' + @queryToRun + N')X'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryParameters = N'@backupStartDate [datetime] OUTPUT, @backupDurationSec [int] OUTPUT, @backupSizeBytes [bigint] OUTPUT'

		EXEC sp_executesql @queryToRun, @queryParameters, @backupStartDate = @backupStartDate OUT
														, @backupDurationSec = @backupDurationSec OUT
														, @backupSizeBytes = @backupSizeBytes OUT
	end

--------------------------------------------------------------------------------------------------
--verify backup, if option is selected
IF @flgOptions & 16 = 16 AND @errorCode = 0 
	begin
		SET @queryToRun	= N'RESTORE VERIFYONLY FROM DISK=''' + @backupLocation + @backupFileName + N''''
		IF @optionBackupWithChecksum=1
			SET @queryToRun = @queryToRun + N' WITH CHECKSUM'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseBackup',
														@eventName		= 'database backup verify',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

--------------------------------------------------------------------------------------------------
--log backup database information
SET @eventData='<backupset><detail>' + 
					'<database_name>' + @dbName + '</database_name>' + 
					'<type>' + @backupType + '</type>' + 
					'<start_date>' + CONVERT([varchar](24), ISNULL(@backupStartDate, GETDATE()), 121) + '</start_date>' + 
					'<duration>' + REPLICATE('0', 2-LEN(CAST(@backupDurationSec / 3600 AS [varchar]))) + CAST(@backupDurationSec / 3600 AS [varchar]) + 'h'
										+ ' ' + REPLICATE('0', 2-LEN(CAST((@backupDurationSec / 60) % 60 AS [varchar]))) + CAST((@backupDurationSec / 60) % 60 AS [varchar]) + 'm'
										+ ' ' + REPLICATE('0', 2-LEN(CAST(@backupDurationSec % 60 AS [varchar]))) + CAST(@backupDurationSec % 60 AS [varchar]) + 's' + '</duration>' + 
					'<size>' + CONVERT([varchar](32), CAST(@backupSizeBytes/(1024*1024*1.0) AS [money]), 1) + ' mb</size>' + 
					'<size_bytes>' + CAST(@backupSizeBytes AS [varchar](32)) + '</size_bytes>' + 
					'<verified>' + CASE WHEN @flgOptions & 16 = 16 AND @errorCode = 0  THEN 'Yes' ELSE 'No' END + '</verified>' + 
					'<file_name>' + @backupFileName + '</file_name>' + 
					'<error_code>' + CAST(@errorCode AS [varchar](32)) + '</error_code>' + 
				'</detail></backupset>'

EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @sqlServerName,
									@dbName			= @dbName,
									@module			= 'dbo.usp_mpDatabaseBackup',
									@eventName		= 'database backup',
									@eventMessage	= @eventData,
									@eventType		= 0 /* info */

--------------------------------------------------------------------------------------------------
--performing backup cleanup
IF @errorCode = 0 AND ISNULL(@retentionDays,0) <> 0
	begin
		SELECT	@backupType = SUBSTRING(@backupFileName, LEN(@backupFileName)-CHARINDEX('.', REVERSE(@backupFileName))+2, CHARINDEX('.', REVERSE(@backupFileName)))

		SET @nestedExecutionLevel = @executionLevel + 1

		EXEC [dbo].[usp_mpDatabaseBackupCleanup]	@sqlServerName			= @sqlServerName,
													@dbName					= @dbName,
													@backupLocation			= @backupLocation,
													@backupFileExtension	= @backupType,
													@flgOptions				= @flgOptions,
													@retentionDays			= @retentionDays,
													@executionLevel			= @nestedExecutionLevel,
													@debugMode				= @debugMode
	end

RETURN @errorCode
GO



RAISERROR('Create procedure: [dbo].[usp_mpDatabaseBackupCleanup]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseBackupCleanup]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseBackupCleanup]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseBackupCleanup]
		@sqlServerName			[sysname],
		@dbName					[sysname],
		@backupLocation			[nvarchar](1024)=NULL,	/*  disk only: local or UNC */
		@backupFileExtension	[nvarchar](8),			/*  BAK - cleanup full/incremental database backup
															TRN - cleanup transaction log backup
														*/
		@flgOptions				[int]	= 4544,			/* 32 - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
														   64 - create folders for each database (default)
														  128 - when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
														  256 - for +2k5 versions, use xp_delete_file option (default)
														 2048 - change retention policy from RetentionDays to RetentionBackupsCount (number of full database backups to be kept)
															  - this may be forced by setting to true property 'Change retention policy from RetentionDays to RetentionFullBackupsCount'
														 4096 - use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default)
														*/
		@retentionDays			[smallint]	= 14,
		@executionLevel			[tinyint]	=  0,
		@debugMode				[bit]		=  0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2006 / review on 2015.03.10
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
--					   - if @retentionDays is set to Days, this number represent the number of days on which database can be restored
--						 depending on the backup strategy, a full backup will always be included
--					   - if @retentionDays is set to BackupCount, this number represent the number of full and differential backups to be kept
--						 an older full backup may exists to ensure that a newer differential backuup can be restored
-- ============================================================================

------------------------------------------------------------------------------------------------------------------------------------------
--returns: 0 = success, >0 = failure

DECLARE		@queryToRun  					[nvarchar](2048),
			@nestedExecutionLevel			[tinyint]

DECLARE		@backupFileName					[nvarchar](1024),
			@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@errorCode						[int],
			@maxAllowedDate					[datetime]

DECLARE		@lastFullBackupSetIDRemaining	[int],
			@lastDiffBackupSetIDRemaining	[int],
			@lastBackupType					[char](1)

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

IF OBJECT_ID('tempdb..#backupSET') IS NOT NULL
	DROP TABLE #backupSET

CREATE TABLE #backupSET 
		(
			  [backup_set_id]		[int]
			, [backup_start_date]	[datetime]	NULL
			, [type]				[char](1)	NULL
		)

IF OBJECT_ID('tempdb..#backupDevice') IS NOT NULL
	DROP TABLE #backupDevice
CREATE TABLE #backupDevice 
	(
		  [backup_set_id]			[int]
		, [physical_device_name]	[nvarchar](260)
	)


-----------------------------------------------------------------------------------------
SET NOCOUNT ON

-----------------------------------------------------------------------------------------
IF @executionLevel=0
	EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

SET @queryToRun= 'Cleanup backup files for database: ' + ' [' + @dbName + ']'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

-----------------------------------------------------------------------------------------
--get destination server running version/edition
SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName		= @sqlServerName,
										@serverEdition		= @serverEdition OUT,
										@serverVersionStr	= @serverVersionStr OUT,
										@serverVersionNum	= @serverVersionNum OUT,
										@executionLevel		= @nestedExecutionLevel,
										@debugMode			= @debugMode

-----------------------------------------------------------------------------------------
--get configuration values: force retention policy
---------------------------------------------------------------------------------------------
DECLARE @forceChangeRetentionPolicy [nvarchar](128)
SELECT	@forceChangeRetentionPolicy=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Change retention policy from RetentionDays to RetentionBackupsCount'
		AND [module] = 'maintenance-plan'

SET @forceChangeRetentionPolicy = LOWER(ISNULL(@forceChangeRetentionPolicy, 'false'))

-----------------------------------------------------------------------------------------
--get default backup location
IF @backupLocation IS NULL
	begin
		SELECT	@backupLocation = [value]
		FROM	[dbo].[appConfigurations]
		WHERE	[name] = N'Default backup location'
				AND [module] = 'maintenance-plan'

		IF @backupLocation IS NULL
			begin
				SET @queryToRun= 'ERROR: @backupLocation parameter value not set'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=1
			end

		IF RIGHT(@backupLocation, 1)<>'\' SET @backupLocation = @backupLocation + N'\'
		SET @backupLocation = @backupLocation + @sqlServerName + '\' + CASE WHEN @flgOptions & 64 = 64 THEN @dbName + '\' ELSE '' END
	end
	
-----------------------------------------------------------------------------------------
--changing backup expiration date from RetentionDays to full/diff database backup count
IF @flgOptions & 2048 = 2048 OR @forceChangeRetentionPolicy='true'
	begin
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SET ROWCOUNT ' + CAST(@retentionDays AS [nvarchar]) + N'		
										SELECT bs.[backup_set_id], bs.[backup_start_date], bs.[type]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type] IN (''D'', ''I'')
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
				EXEC (@queryToRun)

		--check for remote server msdb information
		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
						EXEC (@queryToRun)
			end
		

		SELECT TOP 1  @maxAllowedDate = DATEADD(ss, -1, [backup_start_date])
					, @lastFullBackupSetIDRemaining = [backup_set_id]
					, @lastBackupType = [type]
		FROM #backupSET
		ORDER BY [backup_start_date]

		--if oldest backup is a differential one, go deep and find the full database backup that it will need/use
		IF @lastBackupType='I'
			begin
				SET @queryToRun=N''
				SET @queryToRun = @queryToRun + N'SELECT TOP 1  bs.[backup_set_id]
															, bs.[backup_start_date]
															, bs.[type]
												FROM msdb.dbo.backupset bs
												INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
												WHERE	bs.[type] IN (''D'')
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[backup_set_id] < ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
												ORDER BY bs.[backup_start_date] DESC'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #backupSET
				INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
						EXEC (@queryToRun)

				IF @sqlServerName<>@@SERVERNAME
					begin
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						INSERT	INTO #backupSET([backup_set_id], [backup_start_date], [type])
								EXEC (@queryToRun)
					end

				SELECT TOP 1  @maxAllowedDate  = DATEADD(ss, -1, [backup_start_date])
							, @lastFullBackupSetIDRemaining = [backup_set_id]
							, @lastBackupType = [type]
				FROM #backupSET
				ORDER BY [backup_start_date] DESC
			end

		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT TOP 1 bs.[backup_set_id]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''I''
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_set_id] DESC'

		DELETE FROM #backupSET
		INSERT	INTO #backupSET([backup_set_id])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC (@queryToRun)
			end

		SELECT TOP 1  @lastDiffBackupSetIDRemaining  = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC
	end
ELSE
	begin
		/* SET @maxAllowedDate = DATEADD(dd, -@retentionDays, GETDATE()) */
		--find first full database backup to allow @retentionDays database restore
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SET ROWCOUNT 1		
										SELECT bs.[backup_set_id], bs.[backup_start_date]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''D''
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_start_date] DESC
										SET ROWCOUNT 0'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupSET([backup_set_id], [backup_start_date])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC (@queryToRun)
			end

		SELECT TOP 1  @maxAllowedDate = DATEADD(ss, -1, [backup_start_date])
					, @lastFullBackupSetIDRemaining = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC

		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT TOP 1 bs.[backup_set_id]
										FROM msdb.dbo.backupset bs
										INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.[media_set_id] = bmf.[media_set_id]
										WHERE	bs.[type]=''I''
												AND bs.[database_name] = ''' + @dbName + N'''
												AND bs.[backup_start_date] <= DATEADD(dd, -' + CAST(@retentionDays AS [nvarchar]) + N', GETDATE())
												AND bs.[backup_set_id] > ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
												AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.%' + N''')
										ORDER BY bs.[backup_set_id] DESC'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #backupSET
		INSERT	INTO #backupSET([backup_set_id])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupSET([backup_set_id])
						EXEC (@queryToRun)
			end

		SELECT TOP 1  @lastDiffBackupSetIDRemaining = [backup_set_id]
		FROM #backupSET
		ORDER BY [backup_start_date] DESC
	end

-----------------------------------------------------------------------------------------
--for +2k5 versions, will use xp_delete_file
SET @errorCode=0
IF @serverVersionNum>=9 AND @flgOptions & 256 = 256
	begin
		SET @queryToRun = N'EXEC master.dbo.xp_delete_file 0, N''' + @backupLocation + ''', N''' + @backupFileExtension + ''', N''' + CONVERT([varchar](20), @maxAllowedDate, 120) + ''', 0'
		IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseBackupCleanup',
														@eventName		= 'database backup cleanup',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end

IF @debugMode=1
	SELECT	@maxAllowedDate AS maxAllowedDate, 
			@lastFullBackupSetIDRemaining AS lastFullBackupSetIDRemaining, 
			@lastDiffBackupSetIDRemaining AS lastDiffBackupSetIDRemaining, 
			@forceChangeRetentionPolicy AS forceChangeRetentionPolicy,
			@flgOptions & 256,
			@errorCode,
			@serverVersionNum,
			@flgOptions & 128

-----------------------------------------------------------------------------------------
--in case of previous errors or 2k version, will use "standard" delete file
IF (@flgOptions & 256 = 0) OR (@errorCode<>0 AND @flgOptions & 256 = 256) OR (@serverVersionNum < 9) OR (@flgOptions & 128 = 128 AND @lastFullBackupSetIDRemaining IS NOT NULL)
	begin
		SELECT  @optionXPIsAvailable		= 0,
				@optionXPValue				= 0,
				@optionXPHasChanged			= 0,
				@optionAdvancedIsAvailable	= 0,
				@optionAdvancedValue		= 0,
				@optionAdvancedHasChanged	= 0

		IF @serverVersionNum>=9
			begin
				/* enable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																	@configOptionName	= 'xp_cmdshell',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionXPIsAvailable OUT,
																	@optionCurrentValue	= @optionXPValue OUT,
																	@optionHasChanged	= @optionXPHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= @debugMode

				IF @optionXPIsAvailable = 0
					begin
						/* enable show advanced options configuration option */
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 1,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode

						IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
							EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																				@configOptionName	= 'xp_cmdshell',
																				@configOptionValue	= 1,
																				@optionIsAvailable	= @optionXPIsAvailable OUT,
																				@optionCurrentValue	= @optionXPValue OUT,
																				@optionHasChanged	= @optionXPHasChanged OUT,
																				@executionLevel		= 0,
																				@debugMode			= @debugMode
					end

				IF @optionXPIsAvailable=0 OR @optionXPValue=0
					begin
						set @queryToRun='xp_cmdshell component is turned off. Cannot continue'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
						RETURN 1
					end		
			end											
		
		/* identify backup files to be deleted, based on msdb information */
		SET @queryToRun=N''
		SET @queryToRun = @queryToRun + N'SELECT bs.[backup_set_id], bmf.[physical_device_name]
										FROM [msdb].[dbo].[backupset] bs
										INNER JOIN [msdb].[dbo].[backupmediafamily] bmf ON bmf.[media_set_id]=bs.[media_set_id]
										WHERE	(   (    bs.[backup_start_date] <= CONVERT([datetime], ''' + CONVERT([nvarchar](20), @maxAllowedDate, 120) + N''', 120)
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + '%.' + @backupFileExtension + N''')
														AND (	 (' + CAST(@flgOptions AS [nvarchar]) + N' & 256 = 0) 
															OR (' + CAST(@errorCode AS [nvarchar]) + N'<>0 AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 256 = 256) 
															OR (' + CAST(@serverVersionNum AS [nvarchar]) + N'< 9)
															)
													)
													OR (
															-- when performing cleanup, delete also orphans diff and log backups, when cleanup full database backups(default)
															bs.[backup_set_id] < ' + CAST(@lastFullBackupSetIDRemaining AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + N'%'')
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
													OR (
															-- delete incremental and transaction log backups to keep the retention/restore period fixed
															' + CAST(ISNULL(@lastDiffBackupSetIDRemaining, 0)  AS [nvarchar]) + N' <> 0
														AND bs.[backup_set_id] < ' + CAST(ISNULL(@lastDiffBackupSetIDRemaining, 0) AS [nvarchar]) + N'
														AND bs.[database_name] = ''' + @dbName + N'''
														AND bs.[type] IN (''I'', ''L'')
														AND bmf.[physical_device_name] LIKE (''' + @backupLocation + N'%'')
														AND ' + CAST(@flgOptions AS [nvarchar]) + N' & 128 = 128
													)
												)														
												AND bmf.[device_type] = 2'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #backupDevice([backup_set_id], [physical_device_name])
				EXEC (@queryToRun)

		IF @sqlServerName<>@@SERVERNAME
			begin
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				INSERT	INTO #backupDevice([backup_set_id], [physical_device_name])
						EXEC (@queryToRun)
			end


		/* identify backup files to be deleted, based on file existence on disk */
		/* use xp_dirtree to identify orphan backup files to be deleted, when using option 128 (default) */
		IF @flgOptions & 128 = 128 AND @flgOptions & 4096 = 4096 
			begin
				IF OBJECT_ID('tempdb..#backupFilesOnDisk') IS NOT NULL DROP TABLE #backupFilesOnDisk
				CREATE TABLE #backupFilesOnDisk
				(
					  [id]			[int] IDENTITY(1, 1)
					, [file_name]	[nvarchar](260)
					, [depth]		[int]
					, [is_file]		[int]
					, [create_time]	[varchar](20)
				)

				SET @queryToRun = N'EXEC xp_dirtree ''' + @backupLocation + ''', 8, 1';
				IF @sqlServerName<>@@SERVERNAME
					begin
						IF @serverVersionNum < 11
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
						ELSE
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([subdirectory] [nvarchar](260), [depth] [int], [file] [int]))'')'
					end

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #backupFilesOnDisk
				INSERT INTO #backupFilesOnDisk([file_name], [depth], [is_file])
						EXEC (@queryToRun)

				/* remove files which are no longer on disk */
				IF EXISTS(SELECT * FROM #backupFilesOnDisk)
					DELETE bd
					FROM #backupDevice bd
					LEFT JOIN #backupFilesOnDisk bf ON (@backupLocation + bf.[file_name]) = bd.[physical_device_name] 
					WHERE bf.[file_name] IS NULL
			end


		/* remove the backup files, one by one */
		DECLARE crsCleanupBackupFiles CURSOR FOR	SELECT [physical_device_name]
													FROM #backupDevice														
													ORDER BY [backup_set_id] ASC
		OPEN crsCleanupBackupFiles
		FETCH NEXT FROM crsCleanupBackupFiles INTO @backupFileName
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N'EXEC [' + DB_NAME() + '].[dbo].[usp_mpDeleteFileOnDisk]	@sqlServerName	= ''' + @sqlServerName + N''',
																							@fileName		= ''' + @backupFileName + N''',
																							@executionLevel	= ' + CAST(@nestedExecutionLevel AS [nvarchar]) + N',
																							@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
																@dbName			= NULL,
																@module			= 'dbo.usp_mpDatabaseBackupCleanup',
																@eventName		= 'database backup cleanup',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode

				FETCH NEXT FROM crsCleanupBackupFiles INTO @backupFileName
			end
		CLOSE crsCleanupBackupFiles
		DEALLOCATE crsCleanupBackupFiles

		IF @serverVersionNum>=9 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
			begin
				/* disable xp_cmdshell configuration option */
				IF @optionXPHasChanged = 1
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 0,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode

				/* disable show advanced options configuration option */
				IF @optionAdvancedHasChanged = 1
						EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @sqlServerName,
																			@configOptionName	= 'show advanced options',
																			@configOptionValue	= 0,
																			@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																			@optionCurrentValue	= @optionAdvancedValue OUT,
																			@optionHasChanged	= @optionAdvancedHasChanged OUT,
																			@executionLevel		= 0,
																			@debugMode			= @debugMode
			end
	end

RETURN @errorCode
GO



RAISERROR('Create procedure: [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]
		  @serverToRun			[sysname]
		, @forSQLServerName		[sysname] = '%'
		, @forDatabaseName		[sysname]
		, @backupLocation		[nvarchar](512)
		, @backupType			[nvarchar](32) = 'full' /* options available: FULL, DIFF, LOG */
		, @nameConvention		[nvarchar](32) = 'dbaTDPMon' /*options available: dbaTDPMon, Ola */
		, @debugMode			[bit]=0
/* WITH ENCRYPTION */
WITH RECOMPILE
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.05.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](1024)

DECLARE	  @serverEdition			[sysname]
		, @serverVersionStr			[sysname]
		, @serverVersionNum			[numeric](9,6)

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

SET NOCOUNT ON

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

-----------------------------------------------------------------------------------------
--get destination server running version/edition
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @serverToRun,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= 0,
										@debugMode				= @debugMode

/*-------------------------------------------------------------------------------------------------------------------------------*/
SELECT  @optionXPIsAvailable		= 0,
		@optionXPValue				= 0,
		@optionXPHasChanged			= 0,
		@optionAdvancedIsAvailable	= 0,
		@optionAdvancedValue		= 0,
		@optionAdvancedHasChanged	= 0

IF @serverVersionNum>=9
	begin
		/* enable xp_cmdshell configuration option */
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
															@configOptionName	= 'xp_cmdshell',
															@configOptionValue	= 1,
															@optionIsAvailable	= @optionXPIsAvailable OUT,
															@optionCurrentValue	= @optionXPValue OUT,
															@optionHasChanged	= @optionXPHasChanged OUT,
															@executionLevel		= 0,
															@debugMode			= @debugMode

		IF @optionXPIsAvailable = 0
			begin
				/* enable show advanced options configuration option */
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																	@configOptionName	= 'show advanced options',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																	@optionCurrentValue	= @optionAdvancedValue OUT,
																	@optionHasChanged	= @optionAdvancedHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= @debugMode

				IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 1,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 0,
																		@debugMode			= @debugMode
			end

		IF @optionXPIsAvailable=0 OR @optionXPValue=0
			begin
				set @queryToRun='xp_cmdshell component is turned off. Cannot continue'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				--RETURN 1
			end		
	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
	IF OBJECT_ID('tempdb..#backupFiles') IS NOT NULL DROP TABLE #backupFiles;
	CREATE TABLE #backupFiles
	(
		  [id]			[int] IDENTITY(1, 1)
		, [file_name]	[nvarchar](260)
		, [depth]		[int]
		, [is_file]		[int]
		, [create_time]	[varchar](20)
	)

IF RIGHT(@backupLocation, 1)<>'\' 
	SET @backupLocation = @backupLocation + '\'
IF @forSQLServerName <> '%' 
	SET @backupLocation = @backupLocation + 
							CASE	WHEN @nameConvention='dbaTDPMon' THEN @forSQLServerName
									WHEN @nameConvention='Ola' THEN REPLACE(@forSQLServerName, '\', '$')
							END + '\' + 
							@forDatabaseName + '\' + 
							CASE	WHEN @nameConvention='dbaTDPMon' THEN ''
									WHEN @nameConvention='Ola' THEN @backupType + '\'
							END
	
SET @queryToRun = N'EXEC xp_dirtree ''' + @backupLocation + ''', 8, 1';
IF @serverToRun<>@@SERVERNAME
	begin
		IF @serverVersionNum < 11
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @serverToRun + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
		ELSE
			SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @serverToRun + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([subdirectory] [nvarchar](260), [depth] [int], [file] [int]))'')'
	end

IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #backupFiles
INSERT INTO #backupFiles([file_name], [depth], [is_file])
		EXEC (@queryToRun)


IF @nameConvention = 'dbaTDPMon'
	begin
		SET @backupType = CASE	WHEN LOWER(@backupType) = 'full' THEN '_full.BAK'
								WHEN LOWER(@backupType) = 'diff' THEN '_diff.BAK'
								WHEN LOWER(@backupType) = 'log' THEN '_log.TRN'
						  END
		UPDATE #backupFiles
			SET [create_time] = SUBSTRING([file_name], CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) + LEN(@forDatabaseName) + 2, 15)
		WHERE	CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) <> 0
				AND CHARINDEX(@backupType, [file_name], 1) <> 0
	end

IF @nameConvention = 'Ola'
	begin
		SET @backupType = CASE	WHEN LOWER(@backupType) = 'full' THEN '_full_'
								WHEN LOWER(@backupType) = 'diff' THEN '_diff_'
								WHEN LOWER(@backupType) = 'log' THEN '_log_'
						  END
		UPDATE #backupFiles
			SET [create_time] = SUBSTRING([file_name], CHARINDEX( @backupType, [file_name], 1) + LEN(@backupType) , 15)
		WHERE	CHARINDEX('_' + @forDatabaseName + '_', [file_name], 1) <> 0
				AND CHARINDEX(@backupType, [file_name], 1) <> 0

	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* get last backup type with full path */
DECLARE   @fileName		[nvarchar](512)
		, @depth		[int]
		, @id			[int]
		, @timeStamp	[varchar](20)

SELECT TOP 1 
	   @id = [id]
	 , @fileName = [file_name]
	 , @depth = [depth]
	 , @timeStamp = [create_time]
FROM #backupFiles 
WHERE [create_time] IS NOT NULL
ORDER BY [create_time] DESC

WHILE @depth>0
	begin
		SELECT TOP 1
			@fileName = [file_name] + '\' + @fileName
		FROM #backupFiles
		WHERE [id] < @id
			AND [depth] = @depth -1
		ORDER BY [id] DESC

		SET @depth = @depth -1
	end

SET @fileName = @backupLocation + @fileName
SELECT @fileName AS [file_name], @timeStamp AS [time_stamp]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF @serverVersionNum>=9 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
	begin
		/* disable xp_cmdshell configuration option */
		IF @optionXPHasChanged = 1
			EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																@configOptionName	= 'xp_cmdshell',
																@configOptionValue	= 0,
																@optionIsAvailable	= @optionXPIsAvailable OUT,
																@optionCurrentValue	= @optionXPValue OUT,
																@optionHasChanged	= @optionXPHasChanged OUT,
																@executionLevel		= 0,
																@debugMode			= @debugMode

		/* disable show advanced options configuration option */
		IF @optionAdvancedHasChanged = 1
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @serverToRun,
																	@configOptionName	= 'show advanced options',
																	@configOptionValue	= 0,
																	@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																	@optionCurrentValue	= @optionAdvancedValue OUT,
																	@optionHasChanged	= @optionAdvancedHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= @debugMode
	end
GO



RAISERROR('Create procedure: [dbo].[usp_mpRestoreDatabaseAs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpRestoreDatabaseAs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpRestoreDatabaseAs]
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_mpRestoreDatabaseAs]
		  @forDatabaseName				[sysname]
		, @toDatabaseName				[sysname]
		, @restorePathForData			[nvarchar](260)
		, @restorePathForLog			[nvarchar](260)
		, @addTimeStampSuffixToFiles	[bit] = 0
AS

SET NOCOUNT ON
DECLARE   @queryToRun		[nvarchar](max)
		, @BackupFileName	[nvarchar](260)
		, @LogicalName		[sysname]
		, @logicalFileType	[char](1)
		, @PhysicalName		[nvarchar](260)
		, @timeStampSuffix	[nvarchar](20)

IF OBJECT_ID('tempdb..#mostRecentBackupFile') IS NOT NULL DROP TABLE #mostRecentBackupFile
CREATE TABLE #mostRecentBackupFile
	(
		  [file_name]	[nvarchar](260)
		, [time_stamp]	[nvarchar](20)
		, [type]		[sysname]	NULL
	)
		
SET @queryToRun = N''
SET @queryToRun = @queryToRun + N'
DECLARE	  @serverToRun			[sysname]
		, @forSQLServerName		[sysname]
		, @backupLocation		[nvarchar](512)
		, @backupType			[nvarchar](32)
		, @nameConvention		[nvarchar](32)


SELECT    @serverToRun = [RunSQLFrom]
		, @forSQLServerName = [ServerName]
		, @backupLocation = [BackupLocation]
		, @backupType = ''full''
		, @nameConvention = [BackupMode]
FROM DbaAdmin.refresh.ClientSourceData
WHERE [DatabaseName] = ''' + @forDatabaseName + '''

EXEC dbaTDPMon.[dbo].[usp_mpDatabaseGetMostRecentBackupFromLocation]	  @serverToRun		= @serverToRun
																		, @forSQLServerName = @forSQLServerName
																		, @forDatabaseName	= ''' + @forDatabaseName + '''
																		, @backupLocation	= @backupLocation
																		, @backupType		= @backupType
																		, @nameConvention	= @nameConvention
																		, @debugMode		= 0
'

SET @queryToRun = N'SELECT * FROM OPENQUERY([AWISMONDB1], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([file_name] [nvarchar](260), [time_stamp] [nvarchar](20)))'')'

/* get latest full database backup */
--PRINT @queryToRun
INSERT	INTO #mostRecentBackupFile([file_name], [time_stamp])
		EXEC (@queryToRun)

UPDATE #mostRecentBackupFile SET [type]='full' WHERE [type] IS NULL

/* get latest differential database backup */
SET @queryToRun = REPLACE(@queryToRun, '@backupType = ''''''''full''''''''', '@backupType = ''''''''diff''''''''')

--PRINT @queryToRun
INSERT	INTO #mostRecentBackupFile([file_name], [time_stamp])
		EXEC (@queryToRun)

UPDATE #mostRecentBackupFile SET [type]='diff' WHERE [type] IS NULL

DELETE FROM #mostRecentBackupFile
WHERE	[type]='diff' 
		AND CAST(REPLACE([time_stamp], '_', '') AS [bigint]) < (
																SELECT CAST(REPLACE([time_stamp], '_', '') AS [bigint]) 
																FROM #mostRecentBackupFile 
																WHERE [type]='full'
																)

IF @addTimeStampSuffixToFiles=1
	SELECT @timeStampSuffix = MAX([time_stamp])
	FROM #mostRecentBackupFile

		
IF OBJECT_ID('tempdb..#dbFileList') IS NOT NULL DROP TABLE #dbFileList;
CREATE TABLE #dbFileList
(
	  [LogicalName]				nvarchar(128)
	, [PhysicalName]			nvarchar(260)
	, [Type]					char(1)
	, [FileGroupName]			nvarchar(128)
	, [Size]					numeric(20,0)
	, [MaxSize]					numeric(20,0)
	, [FileID]					bigint
	, [CreateLSN]				numeric(25,0)
	, [DropLSN]					numeric(25,0) NULL
	, [UniqueID]				uniqueidentifier
	, [ReadOnlyLSN]				numeric(25,0) NULL
	, [ReadWriteLSN]			numeric(25,0) NULL
	, [BackupSizeInBytes]		bigint
	, [SourceBlockSize]			int
	, [FileGroupID]				int
	, [LogGroupGUID]			uniqueidentifier NULL
	, [DifferentialBaseLSN]		numeric(25,0) NULL
	, [DifferentialBaseGUID]	uniqueidentifier
	, [IsReadOnly]				bit
	, [IsPresent]				bit
	, [TDEThumbprint]			varbinary(32)
)

/* restore last full database backup */
SELECT @BackupFileName = [file_name] FROM #mostRecentBackupFile WHERE [type]='full'

SET @queryToRun = N'RESTORE FILELISTONLY FROM DISK = ''' + @BackupFileName + ''';';
INSERT	INTO #dbFileList 
		EXEC (@queryToRun)

SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] FROM DISK = ''' + @BackupFileName + N''' WITH STATS=1, REPLACE, NORECOVERY';
			
DECLARE crsdbFileList CURSOR FOR	SELECT [LogicalName], [PhysicalName], [Type] 
									FROM #dbFileList;
OPEN crsdbFileList;
FETCH NEXT FROM crsdbFileList INTO @LogicalName, @PhysicalName, @logicalFileType;
WHILE @@FETCH_STATUS = 0
	begin
		SET @PhysicalName = RIGHT(@PhysicalName, CHARINDEX('\', REVERSE(@PhysicalName)) - 1)
		IF @addTimeStampSuffixToFiles=1
			SET @PhysicalName = SUBSTRING(@PhysicalName, 1, LEN(@PhysicalName)-4) + 
								'_' + @timeStampSuffix + 
								'_' + REPLACE(REPLACE(REPLACE(CONVERT([varchar](19), GETDATE(), 121), '-', ''), ':', ''), ' ', '_') + 
								SUBSTRING(@PhysicalName, LEN(@PhysicalName)-3, 4)

		SET @queryToRun = @queryToRun + N', MOVE ''' + @LogicalName + ''' TO ''' + 
							CASE	WHEN @logicalFileType='D' THEN @restorePathForData
									WHEN @logicalFileType='L' THEN @restorePathForLog
							END  + @PhysicalName + '''';
				
		FETCH NEXT FROM crsdbFileList INTO @LogicalName, @PhysicalName, @logicalFileType;
	end
		
CLOSE crsdbFileList;
DEALLOCATE crsdbFileList;			

/* kill existing database connections*/
EXEC [dbaTDPMon].[dbo].[usp_mpDatabaseKillConnections]	@SQLServerName	= @@SERVERNAME,
														@DBName			= @toDatabaseName,
														@flgOptions		= 3

PRINT '--' + @queryToRun
EXEC sp_executesql @queryToRun


/* restore last differential backup */
SET @BackupFileName=NULL
SELECT @BackupFileName = [file_name] FROM #mostRecentBackupFile WHERE [type]='diff'

IF @BackupFileName IS NOT NULL
	begin
		SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] FROM DISK = ''' + @BackupFileName + N''' WITH STATS=1, REPLACE, NORECOVERY';

		PRINT '--' + @queryToRun
		EXEC sp_executesql @queryToRun
	end


SET @queryToRun = N'RESTORE DATABASE [' + @toDatabaseName + N'] WITH RECOVERY';
PRINT '--' + @queryToRun
EXEC sp_executesql @queryToRun



/* monitoring module */
RAISERROR('Update table: [monitoring].[alertThresholds]', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Running Transaction Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'Running Transaction Elapsed Time (sec)', '>', 1800, 3600
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Uncommitted Transaction Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'Uncommitted Transaction Elapsed Time (sec)', '>', 900, 1800
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'Blocking Transaction Elapsed Time (sec)')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'Blocking Transaction Elapsed Time (sec)', '>', 600, 900
GO

IF NOT EXISTS(SELECT * FROM [monitoring].[alertThresholds] WHERE [category]='performance' AND [alert_name] = 'tempdb: space used by a single session')
	INSERT	INTO [monitoring].[alertThresholds] ([category], [alert_name], [operator], [warning_limit], [critical_limit])
		SELECT 'performance', 'tempdb: space used by a single session', '>', 8192, 16384
GO


RAISERROR('Update table: [monitoring].[alertSkipRules]', 10, 1) WITH NOWAIT
GO
SET NOCOUNT ON
-----------------------------------------------------------------------------------------------------
RAISERROR('		...insert default data', 10, 1) WITH NOWAIT
GO
IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='disk-space' AND [alert_name]='Logical Disk: Free Disk Space (%)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (%)', NULL, NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='disk-space' AND [alert_name]='Logical Disk: Free Disk Space (MB)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'disk-space', 'Logical Disk: Free Disk Space (MB)', NULL, NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='replication' AND [alert_name]='subscription marked inactive')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'replication', 'subscription marked inactive', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='replication' AND [alert_name]='subscription not active')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'replication', 'subscription not active', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='replication' AND [alert_name]='replication latency')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'replication', 'replication latency', '[PublisherServer].[PublishedDB](PublicationName)', '[SubscriberServer].[SubscriberDB]', 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='Running Transaction Elapsed Time (sec)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'Running Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='Uncommitted Transaction Elapsed Time (sec)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'Uncommitted Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='Blocking Transaction Elapsed Time (sec)')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'Blocking Transaction Elapsed Time (sec)', 'InstanceName', NULL, 0

IF NOT EXISTS(SELECT * FROM [monitoring].[alertSkipRules] WHERE [category]='performance' AND [alert_name]='tempdb: space used by a single session')
INSERT	INTO [monitoring].[alertSkipRules] ([category], [alert_name], [skip_value], [skip_value2], [active])
		SELECT 'performance', 'tempdb: space used by a single session', 'InstanceName', NULL, 0
GO



-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.02.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[statsSQLAgentJobs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsSQLAgentJobs]') AND type in (N'U'))
DROP TABLE [monitoring].[statsSQLAgentJobs]
GO
CREATE TABLE [monitoring].[statsSQLAgentJobs]
(
	[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]						[smallint]		NOT NULL,
	[project_id]						[smallint]		NOT NULL,
	[event_date_utc]					[datetime]		NOT NULL,
	[job_name]							[sysname]		NOT NULL,
	[job_completion_status]				[tinyint],
	[last_completion_time]				[datetime],
	[last_completion_time_utc]			[datetime],
	[local_server_date_utc]				[datetime],
	CONSTRAINT [PK_statsSQLAgentJobs] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsSQLAgentJobs_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsSQLAgentJobs_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsSQLAgentJobs_InstanceID] ON [monitoring].[statsSQLAgentJobs]([instance_id], [project_id]) INCLUDE ([last_completion_time]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsSQLAgentJobs_ProjecteID] ON [monitoring].[statsSQLAgentJobs]([project_id]) ON [FG_Statistics_Index]
GO



-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 11.01.2016
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: disk space information
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [monitoring].[statsTransactionsStatus]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[statsTransactionsStatus]') AND type in (N'U'))
DROP TABLE [monitoring].[statsTransactionsStatus]
GO
CREATE TABLE [monitoring].[statsTransactionsStatus]
(
	[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
	[instance_id]						[smallint]		NOT NULL,
	[project_id]						[smallint]		NOT NULL,
	[event_date_utc]					[datetime]		NOT NULL,
	[database_name]						[sysname],
	[session_id]						[smallint],
	[transaction_begin_time]			[datetime],
	[host_name]							[sysname],
	[program_name]						[sysname],
	[login_name]						[sysname],
	[last_request_elapsed_time_sec]		[int],
	[transaction_elapsed_time_sec]		[int],
	[sessions_blocked]					[smallint],
	[is_session_blocked]				[bit],
	[sql_handle]						[varbinary](64),
	[request_completed]					[bit],
	[wait_duration_sec]					[int],
	[wait_type]							[nvarchar](60),
	[tempdb_space_used_mb]				[int],
	CONSTRAINT [PK_statsTransactionsStatus] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[instance_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsTransactionsStatus_catalogProjects] FOREIGN KEY 
	(
		[project_id]
	) 
	REFERENCES [dbo].[catalogProjects] 
	(
		[id]
	),
	CONSTRAINT [FK_statsTransactionsStatus_catalogInstanceNames] FOREIGN KEY 
	(
		[instance_id],
		[project_id]
	) 
	REFERENCES [dbo].[catalogInstanceNames] 
	(
		[id],
		[project_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsTransactionsStatus_InstanceID] ON [monitoring].[statsTransactionsStatus]([instance_id], [project_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsTransactionsStatus_ProjecteID] ON [monitoring].[statsTransactionsStatus]([project_id]) ON [FG_Statistics_Index]
GO



RAISERROR('Drop procedure: [dbo].[usp_monReplicationPublicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monReplicationPublicationLatency]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monReplicationPublicationLatency]
GO



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
	[database_name]						[sysname],
	[session_id]						[smallint],
	[transaction_begin_time]			[datetime],
	[host_name]							[sysname],
	[program_name]						[sysname],
	[login_name]						[sysname],
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
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Step 1: Delete existing information...'
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
SET @strMessage='--Step 2: Get Instance Details Information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
												FROM	[dbo].[vw_catalogInstanceNames] cin
												WHERE 	cin.[project_id] = @projectID
														AND cin.[instance_active]=1
														AND cin.[instance_name] LIKE @sqlServerNameFilter
												ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
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
												INNER JOIN sys.dm_exec_sessions es WITH (READPAST) ON es.[session_id] = owt.[session_id]'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				BEGIN TRY
						INSERT	INTO #blockedSessionInfo([session_id], [blocking_session_id], [wait_duration_sec], [wait_type])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[session-info]:' + @strMessage
				END CATCH


				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  tat.[transaction_begin_time]
														, tasdt.[elapsed_time_seconds]
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
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[transaction-info]:' + @strMessage
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
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[session-tempdb-info]:' + @strMessage
				END CATCH

			
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  @@SERVERNAME AS [server_name]
														, es.[session_id]
														, er.[request_id]
														, es.[host_name]
														, es.[program_name]
														, CASE WHEN ISNULL(es.[login_name], '''') <> '''' THEN es.[login_name] ELSE sp.[loginame] END [login_name]
														, DATEDIFF(ss, es.[last_request_start_time], es.[last_request_end_time]) AS [last_request_elapsed_time_seconds]
														, sp.[sql_handle]
														, CASE WHEN er.[session_id] IS NULL THEN 1 ELSE 0 END AS [request_completed]
												FROM sys.dm_exec_sessions es WITH (READPAST)
												INNER JOIN master.dbo.sysprocesses sp WITH (READPAST) ON sp.[spid] = es.[session_id]
												LEFT JOIN sys.dm_exec_requests er WITH (READPAST) ON er.[session_id] = es.[session_id]'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)

				SET @queryToRun = N'SELECT x. [server_name]
										 , x.[session_id]
										 , ti.[database_name]
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
									INNER JOIN #transactionInfo ti ON ti.[session_id] = x.[session_id]
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
									'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				BEGIN TRY
						INSERT	INTO #monTransactionsStatus([server_name], [session_id], [database_name], [host_name], [program_name], [login_name], [transaction_begin_time], [last_request_elapsed_time_seconds], [transaction_elapsed_time_seconds], [sessions_blocked], [sql_handle], [request_completed], [is_session_blocked], [wait_duration_sec], [wait_type], [tempdb_space_used_mb])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetTransactionsStatus'
									, '[uncommitted-info]:' + @strMessage
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




SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_monGetSQLAgentFailedJobs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monGetSQLAgentFailedJobs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monGetSQLAgentFailedJobs]
GO

CREATE PROCEDURE [dbo].[usp_monGetSQLAgentFailedJobs]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.02.2016
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
		@strMessage				[nvarchar](4000),
		@minJobCompletionTime	[datetime],
		@jobName				[sysname]
		
/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#statsSQLAgentJobs') IS NOT NULL  DROP TABLE #statsSQLAgentJobs

CREATE TABLE #statsSQLAgentJobs
(
	[id]								[int]	 IDENTITY (1, 1)	NOT NULL,
	[job_name]							[sysname]		NOT NULL,
	[job_completion_status]				[tinyint],
	[last_completion_time]				[datetime],
	[last_completion_time_utc]			[datetime],
	[local_server_date_utc]				[datetime]
)

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
		RAISERROR(@strMessage, 16, 1) WITH NOWAIT
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Step 1: Delete existing information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_monGetSQLAgentFailedJobs'


-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Step 2: Get Instance Details Information...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
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
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		------------------------------------------------------------------------------------------------------------------------------------------
		SELECT	@minJobCompletionTime = MIN([last_completion_time])
		FROM	[monitoring].[statsSQLAgentJobs]
		WHERE	[instance_id] = @instanceID
				AND [project_id] = @projectID

		SET @minJobCompletionTime = ISNULL(@minJobCompletionTime, CAST(GETDATE() AS [date]))

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		TRUNCATE TABLE #statsSQLAgentJobs
		
		IF @SQLMajorVersion > 8
			begin
				/* get failed jobs */
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT  [job_name]
														, [job_completion_status]
														, [last_completion_time]
														, DATEADD(hh, DATEDIFF(hh, GETDATE(), GETUTCDATE()), [last_completion_time]) AS [last_completion_time_utc]
														, GETUTCDATE() AS [local_server_date_utc]
												FROM (
														SELECT	  @@SERVERNAME AS [instance_name]
																, sj.[name] AS [job_name]
																, sjh.[run_status] AS [job_completion_status]
																, CONVERT([datetime], SUBSTRING(CAST(sjh.[run_date] AS [varchar](8)), 1, 4) + ''-'' + 
																					  SUBSTRING(CAST(sjh.[run_date] AS [varchar](8)), 5, 2) + ''-'' + 
																					  SUBSTRING(CAST(sjh.[run_date] AS [varchar](8)), 7, 2) + '' '' + 
																					  SUBSTRING((REPLICATE(''0'', 6 - LEN(CAST(sjh.[run_time] AS [varchar](6)))) + CAST(sjh.[run_time] AS [varchar](6))), 1, 2) + '':'' + 
																					  SUBSTRING((REPLICATE(''0'', 6 - LEN(CAST(sjh.[run_time] AS [varchar](6)))) + CAST(sjh.[run_time] AS [varchar](6))), 3, 2) + '':'' + 
																					  SUBSTRING((REPLICATE(''0'', 6 - LEN(CAST(sjh.[run_time] AS [varchar](6)))) + CAST(sjh.[run_time] AS [varchar](6))), 5, 2)
																			, 120) AS [last_completion_time]

														FROM msdb.dbo.sysjobs sj
														INNER JOIN 
															(
																/* last job execution failed */
																SELECT [instance_id], [job_id], [run_date], [run_time], [run_status]
																FROM (
																		SELECT    [instance_id], [job_id], [run_status], [run_date], [run_time]
																				, ROW_NUMBER() OVER(PARTITION BY [job_id] ORDER BY [instance_id] DESC) [row_no]
																		FROM [msdb].[dbo].[sysjobhistory]
																		WHERE [step_name] =''(Job outcome)''
																	)X
																WHERE	[run_status] = 0
																		AND [row_no] = 1
															)sjh ON sj.[job_id]=sjh.[job_id]
													)jobs
												WHERE [last_completion_time] >''' + CONVERT([varchar](24), @minJobCompletionTime, 121)  + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
				
				BEGIN TRY
						INSERT	INTO #statsSQLAgentJobs([job_name], [job_completion_status], [last_completion_time], [last_completion_time_utc], [local_server_date_utc])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_monGetSQLAgentFailedJobs'
									, @strMessage
				END CATCH


				DECLARE crsFailedJobs CURSOR READ_ONLY FOR	SELECT j.[job_name]
															FROM #statsSQLAgentJobs j
															LEFT JOIN [monitoring].[statsSQLAgentJobs] saj ON	saj.[project_id] = @projectID
																												AND saj.[instance_id] = @instanceID
																												AND saj.[job_name] = j.[job_name]
																												AND saj.[last_completion_time] = j.[last_completion_time]
															WHERE saj.[job_name] IS NULL
				OPEN crsFailedJobs
				FETCH NEXT FROM crsFailedJobs INTO @jobName
				WHILE @@FETCH_STATUS=0
					begin
						/* generating alarm/email event */
						EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@sqlServerName			= @sqlServerName,
																		@jobName				= @jobName,
																		@logFileLocation		= NULL,
																		@module					= 'monitoring',
																		@sendLogAsAttachment	= 1,
																		@eventType				= 2,
																		@currentlyRunning		= 0,
																		@debugMode				= @debugMode

						FETCH NEXT FROM crsFailedJobs INTO @jobName
					end
				CLOSE crsFailedJobs
				DEALLOCATE crsFailedJobs
			end								
				
		/* save results to stats table */
		INSERT INTO [monitoring].[statsSQLAgentJobs]([instance_id], [project_id], [event_date_utc], [job_name], [job_completion_status], [last_completion_time], [last_completion_time_utc], [local_server_date_utc])
				SELECT    @instanceID, @projectID, GETUTCDATE()
						, j.[job_name], j.[job_completion_status], j.[last_completion_time], j.[last_completion_time_utc], j.[local_server_date_utc]
				FROM #statsSQLAgentJobs j
				LEFT JOIN [monitoring].[statsSQLAgentJobs] saj ON	saj.[project_id] = @projectID
																	AND saj.[instance_id] = @instanceID
																	AND saj.[job_name] = j.[job_name]
																	AND saj.[last_completion_time] = j.[last_completion_time]
				WHERE saj.[job_name] IS NULL
								
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO




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
		, @queryToRun			[nvarchar](max)
		, @serverToRun			[nvarchar](512)
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
SET @strMessage='--Generate internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @currentInstanceID	[int]

SELECT	@currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[project_id] = @projectID
		AND cin.[name] = @@SERVERNAME

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'usp_monAlarmCustomSQLAgentFailedJobs'


INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
										[job_name], [job_step_name], [job_database_name], [job_command])
		SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monAlarmCustomSQLAgentFailedJobs', NULL, cin.[id],
				'dbaTDPMon - usp_monAlarmCustomSQLAgentFailedJobs - ' + REPLACE(cin.[name], '\', '_'), 'Run Analysis', DB_NAME()
				, N'EXEC dbo.usp_monGetSQLAgentFailedJobs @projectCode = ''' + @projectCode + N''', @sqlServerNameFilter = ''' + cin.[name] + N''''
		FROM	[dbo].[catalogInstanceNames] cin
		WHERE	cin.[active] = 1
						AND cin.[project_id] = @projectID

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Running internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monAlarmCustomSQLAgentFailedJobs',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

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
		, @queryToRun			[nvarchar](max)
		, @serverToRun			[nvarchar](512)
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
DECLARE   @alertThresholdCriticalRunning [int]
		, @alertThresholdWarningRunning [int] 
		
SELECT	@alertThresholdCriticalRunning = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Running Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalRunning = ISNULL(@alertThresholdCriticalRunning, 1800)


SELECT	@alertThresholdWarningRunning = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Running Transaction Elapsed Time (sec)'
		AND [category] = 'performance'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningRunning = ISNULL(@alertThresholdWarningRunning, 900)

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
SET @strMessage='--Generate internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE   @currentInstanceID	[int]

SELECT	@currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[project_id] = @projectID
		AND cin.[name] = @@SERVERNAME

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'usp_monAlarmCustomTransactionsStatus'


INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
										[job_name], [job_step_name], [job_database_name], [job_command])
		SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monAlarmCustomTransactionsStatus', NULL, cin.[id],
				'dbaTDPMon - usp_monAlarmCustomTransactionsStatus - ' + REPLACE(cin.[name], '\', '_'), 'Run Analysis', DB_NAME()
				, N'EXEC dbo.usp_monGetTransactionsStatus @projectCode = ''' + @projectCode + N''', @sqlServerNameFilter = ''' + cin.[name] + N''''
		FROM	[dbo].[catalogInstanceNames] cin
		WHERE	cin.[active] = 1
						AND cin.[project_id] = @projectID

------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Running internal jobs..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monAlarmCustomTransactionsStatus',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

------------------------------------------------------------------------------------------------------------------------------------------
--generate alerts: Replication latency exceeds thresold
------------------------------------------------------------------------------------------------------------------------------------------
SET @strMessage='--Generate alerts..'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

DECLARE   @instanceName		[sysname]
		, @databaseName		[sysname]
		, @childObjectName	[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)

DECLARE crsTransactionStatusAlarms CURSOR FOR	SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, db.[database_name] AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'warning'			AS [severity]
														, 'running transaction'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>warning</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>running transaction</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
															'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
															'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
															'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdWarningRunning*1000) + '</threshold_value>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[transaction_elapsed_time_sec] >= @alertThresholdWarningRunning 
														AND sts.[transaction_elapsed_time_sec] < @alertThresholdCriticalRunning 
														AND sts.[request_completed] = 0 /* running transaction */
												
												UNION ALL

												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, db.[database_name] AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'critical'			AS [severity]
														, 'running transaction'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>critical</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>running transaction</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
															'<last_request_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[last_request_elapsed_time_sec]*1000, 0)) +'</last_request_elapsed_time>' + 
															'<transaction_elapsed_time>' + [dbo].[ufn_reportHTMLFormatTimeValue](ISNULL(sts.[transaction_elapsed_time_sec]*1000, 0)) +'</transaction_elapsed_time>' + 
															'<threshold_value>' + [dbo].[ufn_reportHTMLFormatTimeValue](@alertThresholdCriticalRunning*1000) + '</threshold_value>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[transaction_elapsed_time_sec] >= @alertThresholdCriticalRunning
														AND sts.[request_completed] = 0 /* running transaction */
												
												UNION ALL
												
												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, db.[database_name] AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'warning'			AS [severity]
														, 'uncommitted transaction'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>warning</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>uncommitted transaction</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[transaction_elapsed_time_sec] >= @alertThresholdWarningUncommitted 
														AND sts.[transaction_elapsed_time_sec] < @alertThresholdCriticalUncommitted 
														AND sts.[request_completed] = 1 /* uncommitted transaction / request has completed */
												
												UNION ALL

												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, db.[database_name] AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'critical'			AS [severity]
														, 'uncommitted transaction'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>critical</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>uncommitted transaction</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[transaction_elapsed_time_sec] >= @alertThresholdCriticalUncommitted
														AND sts.[request_completed] = 1 /* uncommitted transaction / request has completed */

												UNION ALL

												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, db.[database_name] AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'warning'				AS [severity]
														, 'blocked transaction'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>warning</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>blocked transaction</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[wait_duration_sec] >= @alertThresholdWarningBlocking
														AND sts.[wait_duration_sec] < @alertThresholdCriticalBlocking
														AND sts.[is_session_blocked] = 1
												
												UNION ALL

												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, db.[database_name] AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'critical'			AS [severity]
														, 'blocked transaction'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>critical</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>blocked transaction</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[wait_duration_sec] >= @alertThresholdCriticalBlocking
														AND sts.[is_session_blocked] = 1

												UNION ALL

												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'warning'				AS [severity]
														, 'tempdb space'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>warning</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>tempdb space</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[tempdb_space_used_mb] >= @alertThresholdWarningTempdb
														AND sts.[tempdb_space_used_mb] < @alertThresholdCriticalTempdb
												
												UNION ALL

												SELECT  DISTINCT
														  cin.[instance_name] AS [instance_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') AS [object_name]
														, 'session_id=' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') + CASE WHEN sh.[sql_handle] IS NOT NULL THEN '; sqlhandle=' + sh.[sql_handle] ELSE N'' END AS [child_object_name]
														, 'critical'			AS [severity]
														, 'tempdb space'	AS [event_name]
														, '<alert><detail>' + 
															'<severity>critical</severity>' + 
															'<instance_name>' + cin.[instance_name] + '</instance_name>' + 
															'<counter_name>tempdb space</counter_name>' + 
															'<session_id>' + ISNULL(CAST(sts.[session_id] AS [nvarchar]), '0') +'</session_id>' + 
															'<is_session_blocked>' + CASE WHEN ISNULL(sts.[is_session_blocked], 0)=1 THEN N'Yes' ELSE N'No' END +'</is_session_blocked>' + 
															'<sessions_blocked>' + ISNULL(CAST(sts.[sessions_blocked] AS [nvarchar]), '0') +'</sessions_blocked>' + 
															'<databases>' + db.[database_name] + '</databases>' + 
															'<host_name>' + sts.[host_name] + '</host_name>' + 
															'<program_name>' + sts.[program_name] + '</program_name>' + 
															'<login_name>' + sts.[login_name] + '</login_name>' + 
															'<sql_handle>' + CASE WHEN sts.[sql_handle] IS NOT NULL THEN sh.[sql_handle] ELSE N'' END + '</sql_handle>' + 
															'<transaction_begin_time>' + CONVERT([varchar](20), sts.[transaction_begin_time], 120) + '</transaction_begin_time>' + 
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
												WHERE cin.[instance_active]=1
														AND cin.[project_id] = @projectID
														AND cin.[instance_name] LIKE @sqlServerNameFilter
														AND sts.[tempdb_space_used_mb] >= @alertThresholdCriticalTempdb
												ORDER BY [instance_name], [object_name]
OPEN crsTransactionStatusAlarms
FETCH NEXT FROM crsTransactionStatusAlarms INTO @instanceName, @databaseName, @childObjectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
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
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsTransactionStatusAlarms INTO @instanceName, @databaseName, @childObjectName, @severity, @eventName, @eventMessage
	end
CLOSE crsTransactionStatusAlarms
DEALLOCATE crsTransactionStatusAlarms
GO




RAISERROR('Create procedure: [dbo].[usp_monAlarmCustomReplicationLatency]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_monAlarmCustomReplicationLatency]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monAlarmCustomReplicationLatency]
GO

CREATE PROCEDURE [dbo].[usp_monAlarmCustomReplicationLatency]
		  @projectCode			[varchar](32)=NULL
		, @sqlServerNameFilter	[sysname]='%'
		, @operationDelay		[varchar](10) = N'00:00:05'
		, @debugMode			[bit]=0
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
		
DECLARE   @sqlServerName		[sysname]
		, @projectID			[smallint]
		, @strMessage			[nvarchar](512)
		, @queryToRun			[nvarchar](max)
		, @serverToRun			[nvarchar](512)
		, @eventMessageData		[nvarchar](max)
		, @runStartTime			[datetime]

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
--get value for critical alert threshold
DECLARE   @alertThresholdCriticalReplicationLatencySec [int]
		, @alertThresholdWarningReplicationLatencySec [int] 
		

SELECT	@alertThresholdCriticalReplicationLatencySec = [critical_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Replication Latency'
		AND [category] = 'replication'
		AND [is_critical_limit_enabled]=1
SET @alertThresholdCriticalReplicationLatencySec = ISNULL(@alertThresholdCriticalReplicationLatencySec, 20)


SELECT	@alertThresholdWarningReplicationLatencySec = [warning_limit]
FROM	[monitoring].[alertThresholds]
WHERE	[alert_name] = 'Replication Latency'
		AND [category] = 'replication'
		AND [is_warning_limit_enabled]=1
SET @alertThresholdWarningReplicationLatencySec = ISNULL(@alertThresholdWarningReplicationLatencySec, 15)

---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name] = 'Default lock timeout (ms)'
		AND [module] = 'common'
				
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Delete existing information....', 10, 1) WITH NOWAIT

DELETE srl
FROM [monitoring].[statsReplicationLatency]		srl
WHERE srl.[project_id] = @projectID
	AND [publisher_server] LIKE @sqlServerNameFilter


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Get Publications & Subscriptions Information....', 10, 1) WITH NOWAIT
SET @runStartTime = GETUTCDATE()

--replication distribution servers
DECLARE crsReplicationDistributorServers CURSOR FAST_FORWARD READ_ONLY FOR	SELECT [instance_name]
																			FROM [dbo].[vw_catalogDatabaseNames] 
																			WHERE [project_id] = @projectID
																					AND [active] = 1
																					AND [database_name] = 'distribution'
																					AND [instance_name] LIKE @sqlServerNameFilter
OPEN crsReplicationDistributorServers
FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		--publications and subscriptions
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
							SELECT   ' + CAST(@projectID AS [nvarchar]) + N' AS [project_id]
									, @@SERVERNAME		AS [distributor_server]
									, p.[publication]	AS [publication_name]
									, p.[publication_type]
									, srv.[srvname]		AS [publisher_server]
									, p.[publisher_db]
									, ss.[srvname]		AS [subscriber_server]
									, s.[subscriber_db] 
									, s.[status]		AS [subscription_status]
									, s.[subscription_type]
									, COUNT(DISTINCT s.[article_id]) AS [subscription_articles]
							FROM distribution..MSpublications p 
							JOIN distribution..MSsubscriptions s ON p.[publication_id] = s.[publication_id] 
							JOIN master..sysservers ss ON s.[subscriber_id] = ss.[srvid]
							JOIN master..sysservers srv ON srv.[srvid] = p.[publisher_id]
							JOIN distribution..MSdistribution_agents da ON da.[publisher_id] = p.[publisher_id] AND da.[subscriber_id] = s.[subscriber_id] 
							GROUP BY p.[publication]
									, srv.[srvname]
									, p.[publisher_db]
									, ss.[srvname]
									, s.[subscriber_db] 
									, s.[status]
									, p.[publication_type]
									, s.[subscription_type]'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO [monitoring].[statsReplicationLatency]([project_id], [distributor_server], [publication_name], [publication_type], [publisher_server], [publisher_db], [subscriber_server], [subscriber_db], [subscription_status], [subscription_type], [subscription_articles])
				EXEC (@queryToRun)

		FETCH NEXT FROM crsReplicationDistributorServers INTO @sqlServerName
	end
CLOSE crsReplicationDistributorServers
DEALLOCATE crsReplicationDistributorServers

		
------------------------------------------------------------------------------------------------------------------------------------------
--generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate 21074 errors: The subscription(s) have been marked inactive and must be reinitialized.....', 10, 1) WITH NOWAIT

DECLARE   @publicationName		[sysname]
		, @publicationServer	[sysname]
		, @publisherDB			[sysname]
		, @subcriptionServer	[sysname]
		, @subscriptionDB		[sysname]
		, @distributorServer	[sysname]
		, @subscriptionArticles	[int]

DECLARE crsInactiveSubscriptions CURSOR FAST_FORWARD READ_ONLY FOR	SELECT    srl.[publication_name], srl.[publisher_server], srl.[publisher_db]
																			, srl.[subscriber_server], srl.[subscriber_db], srl.[subscription_articles], srl.[distributor_server]
																	FROM [monitoring].[statsReplicationLatency] srl
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																												AND asr.[alert_name] IN ('subscription marked inactive')
																												AND asr.[active] = 1
																												AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																												AND (    asr.[skip_value2] IS NULL 
																													 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																													)
																	WHERE	srl.[subscription_status] = 0 /* inactive subscriptions */
																			AND asr.[id] IS NULL
																			AND srl.[publisher_server] LIKE @sqlServerNameFilter
OPEN crsInactiveSubscriptions
FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
WHILE @@FETCH_STATUS=0
	begin
		SET @queryToRun = 'Publication: [' + @publicationName + '] / Subscriber [' + @subcriptionServer + '].[' + @subscriptionDB + '] / Publisher: [' + @publicationServer + '].[' + @publisherDB + '] / Distributor: [' + @distributorServer + '] / Articles: ' + CAST(@subscriptionArticles as [nvarchar])
		RAISERROR(@queryToRun, 10, 1) WITH NOWAIT

		SET @eventMessageData = '<alert><detail>' + 
								'<error_code>21074</error_code>' + 
								'<error_string>The subscription(s) have been marked inactive and must be reinitialized.</error_string>' + 
								'<query_executed>' + @queryToRun + '</query_executed>' + 
								'<duration_seconds>' + CAST(ISNULL(DATEDIFF(ss, @runStartTime, GETUTCDATE()), 0) AS [nvarchar]) + '</duration_seconds>' + 
								'<event_date_utc>' + CONVERT([varchar](20), GETUTCDATE(), 120) + '</event_date_utc>' + 
								'</detail></alert>'

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @publicationServer,
														@dbName					= @publicationName,
														@objectName				= @subcriptionServer,
														@childObjectName		= @subscriptionDB,
														@module					= 'monitoring',
														@eventName				= 'subscription marked inactive',
														@parameters				= NULL,			/* may contain the attach file name */
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 1,	
														@additionalOption		= 0

		FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
	end
CLOSE crsInactiveSubscriptions
DEALLOCATE crsInactiveSubscriptions


------------------------------------------------------------------------------------------------------------------------------------------
-- Subscribed but not active subscriptions
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Check for subscribed but not active subscriptions....', 10, 1) WITH NOWAIT


DECLARE crsInactiveSubscriptions CURSOR FAST_FORWARD READ_ONLY FOR	SELECT    srl.[publication_name], srl.[publisher_server], srl.[publisher_db], srl.[subscriber_server]
																			, srl.[subscriber_db], srl.[subscription_articles], srl.[distributor_server]
																	FROM [monitoring].[statsReplicationLatency] srl
																	LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																												AND asr.[alert_name] IN ('subscription not active')
																												AND asr.[active] = 1
																												AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																												AND (    asr.[skip_value2] IS NULL 
																													 OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																													)
																	WHERE	srl.[subscription_status] = 1 /* subscribed subscriptions */
																			AND asr.[id] IS NULL
																			AND srl.[publisher_server] LIKE @sqlServerNameFilter
OPEN crsInactiveSubscriptions
FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
WHILE @@FETCH_STATUS=0
	begin
		SET @queryToRun = 'Publication: [' + @publicationName + '] / Subscriber [' + @subcriptionServer + '].[' + @subscriptionDB + '] / Publisher: [' + @publicationServer + '].[' + @publisherDB + '] / Distributor: [' + @distributorServer + '] / Articles: ' + CAST(@subscriptionArticles as [nvarchar])
		RAISERROR(@queryToRun, 10, 1) WITH NOWAIT

		SET @eventMessageData = '<alert><detail>' + 
								'<error_code>21488</error_code>' + 
								'<error_string>The subscription is not active. Subscription must have active in order to post a tracer token.</error_string>' + 
								'<query_executed>' + @queryToRun + '</query_executed>' + 
								'<duration_seconds>' + CAST(ISNULL(DATEDIFF(ss, @runStartTime, GETUTCDATE()), 0) AS [nvarchar]) + '</duration_seconds>' + 
								'<event_date_utc>' + CONVERT([varchar](20), GETUTCDATE(), 120) + '</event_date_utc>' + 
								'</detail></alert>'

		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @publicationServer,
														@dbName					= @publicationName,
														@objectName				= @subcriptionServer,
														@childObjectName		= @subscriptionDB,
														@module					= 'monitoring',
														@eventName				= 'subscription not active',
														@parameters				= NULL,			/* may contain the attach file name */
														@eventMessage			= @eventMessageData,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 1,	
														@additionalOption		= 0

		FETCH NEXT FROM crsInactiveSubscriptions INTO @publicationName, @publicationServer, @publisherDB, @subcriptionServer, @subscriptionDB, @subscriptionArticles, @distributorServer
	end
CLOSE crsInactiveSubscriptions
DEALLOCATE crsInactiveSubscriptions

------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Deploy temporary objects for Replication Latency analysis...', 10, 1) WITH NOWAIT

DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT srl.[publisher_server]
																FROM [monitoring].[statsReplicationLatency] srl
																LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																											AND asr.[alert_name] IN ('subscription marked inactive', 'subscription not active')
																											AND asr.[active] = 1
																											AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																											AND (    asr.[skip_value2] IS NULL 
																													OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																												)

																WHERE srl.[subscription_status] = 2 /* active subscriptions */
																		AND srl.[publication_type] = 0 /* only transactional publications */
																		AND asr.[id] IS NULL
																		AND srl.[publisher_server] LIKE @sqlServerNameFilter

OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	running on server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_monGetReplicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_monGetReplicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			CREATE PROCEDURE dbo.usp_monGetReplicationLatency
				  @publisherDB			[sysname]
				, @publicationName		[sysname]
				, @replicationDelay		[int] = 15
				, @operationDelay		[varchar](10) = N''00:00:05''
			AS
			/*
				original code source:
				Name:       dba_replicationLatencyGet_sp
				Author:     Michelle F. Ufford
			*/
			SET NOCOUNT ON
			DECLARE   @currentIteration [int]
					, @tokenID			[bigint]
					, @currentDateTime	[smalldatetime]
					, @tokenStartTime	[datetime]
					, @queryToRun		[nvarchar](4000)
					, @queryParam		[nvarchar](512)

			IF NOT EXISTS(SELECT * FROM sysobjects WHERE [name]=''replicationTokenResults'' AND [type]=''U'')
				CREATE TABLE [dbo].[replicationTokenResults]
					(
						  [publisher_db]		[sysname] NULL
						, [publication]			[sysname] NULL
						, [iteration]			[int] NULL
						, [tracer_id]			[int] NULL
						, [distributor_latency]	[int] NULL
						, [subscriber]			[sysname] NULL
						, [subscriber_db]		[sysname] NULL
						, [subscriber_latency]	[int] NULL
						, [overall_latency]		[int] NULL
					)
			ELSE
				DELETE FROM [dbo].[replicationTokenResults] WHERE [publication] = @publicationName AND [publisher_db] = @publisherDB

			DECLARE @temptokenresult TABLE 
				(
					  [tracer_id]			[int] NULL
					, [distributor_latency] [int] NULL
					, [subscriber]			[sysname] NULL
					, [subscriber_db]		[sysname] NULL
					, [subscriber_latency]	[int] NULL
					, [overall_latency]		[int] NULL
				);

			SET @currentIteration = 1
			SET @currentDateTime  = GETDATE()

			WHILE @currentIteration <= 2
				begin
					/* Insert a new tracer token in the publication database */
					SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_postTracerToken @publication = @publicationName, @tracer_token_id = @tokenID OUTPUT''
					SET @queryParam = N''@publicationName [sysname], @tokenID [bigint] OUTPUT''
					
					PRINT @queryToRun
					SET @tokenStartTime = GETDATE()
					EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																, @tokenID = @tokenID OUTPUT

					/* Give a few seconds to allow the record to reach the subscriber */
					WHILE GETDATE() <= DATEADD(ss, @replicationDelay, @tokenStartTime)
						begin
							/* Give a few seconds to allow the record to reach the subscriber */
							WAITFOR DELAY @operationDelay

							SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_helpTracerTokenHistory @publicationName, @tokenID'' 
							PRINT @queryToRun

							/* Store our results in a temp table for retrieval later */
							INSERT	INTO @temptokenResult ([distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
									EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
																				, @tokenID = @tokenID

							IF NOT EXISTS(	SELECT * FROM @temptokenResult 
											WHERE [subscriber_latency] IS NULL OR [overall_latency] IS NULL OR [distributor_latency] IS NULL
										 )													
								BREAK
							ELSE
								DELETE FROM @temptokenResult							
						end										

					INSERT	[dbo].[replicationTokenResults] ([publisher_db], [publication], [distributor_latency], [subscriber], [subscriber_db], [subscriber_latency], [overall_latency])
							SELECT    @publisherDB
									, @publicationName
									, distributor_latency
									, subscriber
									, subscriber_db
									, subscriber_latency
									, overall_latency
							FROM @temptokenResult

					/* Assign the iteration and token id to the results for easier investigation */
					UPDATE [dbo].[replicationTokenResults]
					SET   [iteration] = @currentIteration
						, [tracer_id] = @tokenID
					WHERE [iteration] IS NULL;

					DELETE FROM @temptokenresult		
					
					/* add retry mechanism for 1st iteration */
					IF	@currentIteration=1
						AND EXISTS(	SELECT * FROM [dbo].[replicationTokenResults] 
									WHERE	[publication] = @publicationName AND [publisher_db] = @publisherDB 
											AND ([overall_latency] IS NULL OR [distributor_latency] IS NULL OR [subscriber_latency] IS NULL)
									)
						begin
							DELETE FROM [dbo].[replicationTokenResults] 
							WHERE [publication] = @publicationName AND [publisher_db] = @publisherDB
							
							SET @currentIteration = @currentIteration + 1;					
						end
					ELSE
						SET @currentIteration = 3;	
				end;

			/* perform cleanup */
			SET @queryToRun = N''EXECUTE ['' + @publisherDB + N''].sys.sp_deleteTracerTokenHistory @publication = @publicationName, @cutoff_date = @currentDateTime''
			SET @queryParam = N''@publicationName [sysname], @currentDateTime [datetime]''
			PRINT @queryToRun

			EXEC sp_executesql @queryToRun, @queryParam , @publicationName = @publicationName
														, @currentDateTime = @currentDateTime

			/* SELECT * FROM [dbo].[replicationTokenResults]  */'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		FETCH NEXT FROM crsActivePublications INTO @publicationServer
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate Replication Latency check internal jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE   @publisherInstanceID	[int]
		, @currentInstanceID	[int]

SELECT	@currentInstanceID = [id]
FROM	[dbo].[catalogInstanceNames] cin
WHERE	cin.[active] = 1
		AND cin.[project_id] = @projectID
		AND cin.[name] = @@SERVERNAME

DELETE FROM [dbo].[jobExecutionQueue]
WHERE [project_id] = @projectID
		AND [instance_id] = @currentInstanceID
		AND [module] = 'monitoring'
		AND [descriptor] = 'usp_monAlarmCustomReplicationLatency'

DECLARE crsActivePublishers	CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT cin.[id], [publisher_server]
																FROM	[monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[catalogInstanceNames] cin ON srl.[publisher_server] = cin.[name]
																WHERE	[subscription_status] = 2 /* active subscriptions */
																		AND [publication_type] = 0 /* only transactional publications */
																		AND cin.[active] = 1
																		AND cin.[project_id] = @projectID
																		AND srl.[publisher_server] LIKE @sqlServerNameFilter
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publication_name], [publisher_db]
																		FROM	[monitoring].[statsReplicationLatency] srl																		
																		WHERE	[subscription_status] = 2 /* active subscriptions */
																				AND srl.[publisher_server] = @publicationServer
																				AND [publication_type] = 0 /* only transactional publications */
																				AND srl.[publisher_server] LIKE @sqlServerNameFilter
																			
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'EXEC [' + @publicationServer + '].tempdb.dbo.usp_monGetReplicationLatency @publisherDB = ''' + @publisherDB + N''', @publicationName = ''' + @publicationName + N''', @replicationDelay = ' + CAST(@alertThresholdCriticalReplicationLatencySec AS [nvarchar]) + N', @operationDelay = ''' + @operationDelay + N''';'

				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monAlarmCustomReplicationLatency', @publicationName + ' - ' + @publisherDB, @publisherInstanceID,
								'dbaTDPMon - usp_monAlarmCustomReplicationLatency(1) - ' + REPLACE(@publicationServer, '\', '_') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Run Analysis', 'tempdb', @queryToRun
				
				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Running jobs to compute replication latency..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monAlarmCustomReplicationLatency',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

UPDATE srl
	SET srl.[state] = 1	/* analysis job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monAlarmCustomReplicationLatency(1) - ' + REPLACE(srl.[publisher_server], '\', '_') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Run Analysis'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'usp_monAlarmCustomReplicationLatency'
		AND jeq.[status] = 1 /* succedded */

------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Generate Replication Latency getdata internal jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE crsActivePublishers	CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT cin.[id], [publisher_server]
																FROM	[monitoring].[statsReplicationLatency] srl
																INNER JOIN [dbo].[catalogInstanceNames] cin ON srl.[publisher_server] = cin.[name]
																WHERE	[subscription_status] = 2 /* active subscriptions */
																		AND [publication_type] = 0 /* only transactional publications */
																		AND cin.[active] = 1
																		AND cin.[project_id] = @projectID
																		AND srl.[publisher_server] LIKE @sqlServerNameFilter
OPEN crsActivePublishers
FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	Analyzing server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		
		DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT [publication_name], [publisher_db]
																		FROM	[monitoring].[statsReplicationLatency] srl																		
																		WHERE	[subscription_status] = 2 /* active subscriptions */
																				AND srl.[publisher_server] = @publicationServer
																				AND [publication_type] = 0 /* only transactional publications */
																				AND srl.[publisher_server] LIKE @sqlServerNameFilter
		OPEN crsActivePublications
		FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
		WHILE @@FETCH_STATUS=0
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT * FROM tempdb.[dbo].[replicationTokenResults] WHERE [publication]=''' + @publicationName + N''' AND [publisher_db] = ''' + @publisherDB + N''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@publicationServer, @queryToRun)

				SET @queryToRun = N'UPDATE srl
										SET   srl.[distributor_latency] = x.[distributor_latency]
											, srl.[subscriber_latency] = x.[subscriber_latency]
											, srl.[overall_latency] = x.[overall_latency]
											, srl.[event_date_utc] = GETUTCDATE()
									FROM [monitoring].[statsReplicationLatency] srl
									INNER JOIN (
													SELECT    [publisher_db], [publication], [subscriber], [subscriber_db]
															, MAX(ISNULL([distributor_latency],  2147483647))	AS [distributor_latency]
															, MAX(ISNULL([subscriber_latency],  2147483647))	AS [subscriber_latency]
															, MAX(ISNULL([overall_latency],  2147483647))		AS [overall_latency]
													FROM (' + @queryToRun + ')y
													GROUP BY [publisher_db], [publication], [subscriber], [subscriber_db]
												)x ON	srl.[publisher_db] = x.[publisher_db] 
													and srl.[publication_name] = x.[publication] 
													AND srl.[subscriber_server] = x.[subscriber] 
													AND srl.[subscriber_db] = x.[subscriber_db]
													AND srl.[publisher_server] = ''' + @publicationServer + N'''
									WHERE srl.[publisher_db]=''' + @publisherDB + N'''
										AND srl.[publication_name]=''' + @publicationName + N''''

				SET @queryToRun = N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; ' + @queryToRun
		
				INSERT	INTO [dbo].[jobExecutionQueue](	[instance_id], [project_id], [module], [descriptor], [filter], [for_instance_id],
														[job_name], [job_step_name], [job_database_name], [job_command])
						SELECT	@currentInstanceID, @projectID, 'monitoring', 'usp_monAlarmCustomReplicationLatency', @publicationName + ' - ' + @publisherDB , @publisherInstanceID,
								'dbaTDPMon - usp_monAlarmCustomReplicationLatency(2) - ' + REPLACE(@publicationServer, '\', '_') + ' - ' + @publicationName + ' - ' + @publisherDB, 'Get Latency', DB_NAME(), @queryToRun

				FETCH NEXT FROM crsActivePublications INTO @publicationName, @publisherDB
			end
		CLOSE crsActivePublications
		DEALLOCATE crsActivePublications

		FETCH NEXT FROM crsActivePublishers INTO @publisherInstanceID, @publicationServer
	end
CLOSE crsActivePublishers
DEALLOCATE crsActivePublishers


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Running GetData jobs..', 10, 1) WITH NOWAIT
------------------------------------------------------------------------------------------------------------------------------------------
EXEC dbo.usp_jobQueueExecute	@projectCode		= @projectCode,
								@moduleFilter		= 'monitoring',
								@descriptorFilter	= 'usp_monAlarmCustomReplicationLatency',
								@waitForDelay		= DEFAULT,
								@debugMode			= @debugMode

UPDATE [monitoring].[statsReplicationLatency] SET [distributor_latency] = NULL	WHERE [distributor_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [subscriber_latency] = NULL	WHERE [subscriber_latency] = 2147483647
UPDATE [monitoring].[statsReplicationLatency] SET [overall_latency] = NULL		WHERE [overall_latency] = 2147483647

UPDATE srl
	SET srl.[state] = 2	/* getdate job executed successfully */
FROM [monitoring].[statsReplicationLatency] srl
INNER JOIN [dbo].[jobExecutionQueue] jeq ON jeq.[filter] = srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_name] = 'dbaTDPMon - usp_monAlarmCustomReplicationLatency(2) - ' + REPLACE(srl.[publisher_server], '\', '_') + ' - ' + srl.[publication_name] + ' - ' + srl.[publisher_db]
											AND jeq.[job_step_name] = 'Get Latency'
WHERE	jeq.[module] = 'monitoring'
		AND jeq.[descriptor] = 'usp_monAlarmCustomReplicationLatency'
		AND jeq.[status] = 1 /* succedded */


------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Perform cleanup...', 10, 1) WITH NOWAIT

DECLARE crsActivePublications CURSOR FAST_FORWARD READ_ONLY FOR	SELECT DISTINCT srl.[publisher_server], srl.[publication_name], srl.[publisher_db]
																FROM [monitoring].[statsReplicationLatency] srl
																LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																											AND asr.[alert_name] IN ('subscription marked inactive', 'subscription not active')
																											AND asr.[active] = 1
																											AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																											AND (    asr.[skip_value2] IS NULL 
																													OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																												)

																WHERE srl.[subscription_status] = 2 /* active subscriptions */
																		AND srl.[publication_type] = 0 /* only transactional publications */
																		AND asr.[id] IS NULL
																		AND srl.[publisher_server] LIKE @sqlServerNameFilter
OPEN crsActivePublications
FETCH NEXT FROM crsActivePublications INTO @publicationServer, @publicationName, @publisherDB
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='--	running on server: ' + @publicationServer
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		IF @publicationServer<>@@SERVERNAME
			SET @serverToRun = '[' + @publicationServer + '].tempdb.dbo.sp_executesql'
		ELSE
			SET @serverToRun = 'tempdb.dbo.sp_executesql'

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''usp_monGetReplicationLatency'' AND [type]=''P'')
				DROP PROCEDURE dbo.usp_monGetReplicationLatency'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun

		/*
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'
			IF EXISTS(SELECT * FROM sysobjects WHERE [name] = ''replicationTokenResults'' AND [type]=''U'')
				DELETE FROM dbo.replicationTokenResults WHERE [publisher_db]=''' + @publisherDB + N''' AND [publication] = ''' + @publicationName + N''''

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @serverToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		EXEC @serverToRun @queryToRun
		*/
		FETCH NEXT FROM crsActivePublications INTO @publicationServer, @publicationName, @publisherDB
	end
CLOSE crsActivePublications
DEALLOCATE crsActivePublications


------------------------------------------------------------------------------------------------------------------------------------------
--generate alerts: Replication latency exceeds thresold
------------------------------------------------------------------------------------------------------------------------------------------
RAISERROR('--generate alerts: Replication latency exceeds thresold...', 10, 1) WITH NOWAIT

DECLARE   @instanceName		[sysname]
		, @objectName		[nvarchar](512)
		, @eventName		[sysname]
		, @severity			[sysname]
		, @eventMessage		[nvarchar](max)


DECLARE crsReplicationAlarms CURSOR FOR	SELECT  DISTINCT
												  srl.[publisher_server] AS [instance_name]
												, 'Publication: ' + srl.[publication_name] + ' - Subscriber:' + srl.[subscriber_server] + '.' + srl.[subscriber_db] AS [object_name]
												, 'critical'			AS [severity]
												, 'replication latency'	AS [event_name]
												, '<alert><detail>' + 
													'<severity>critical</severity>' + 
													'<machine_name>' + cin.[machine_name] + '</machine_name>' + 
													'<counter_name>replication latency</counter_name>
													<target_name>Publication: ' + srl.[publication_name] + ' / Subscriber: [' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + '] / Publisher: [' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '] / Distributor: [' + srl.[distributor_server] + ']</target_name>' + 
													'<measure_unit>sec</measure_unit>' + 
													'<current_value>' + ISNULL(CAST(srl.[overall_latency] AS [nvarchar]), '-1') +'</current_value>' + 
													'<threshold_value>' + CAST(@alertThresholdCriticalReplicationLatencySec AS [varchar]) + '</threshold_value>' + 
													'<event_date_utc>' + CONVERT([varchar](20), srl.[event_date_utc], 120) + '</event_date_utc>' + 
													'</detail></alert>' AS [event_message]
										FROM [dbo].[vw_catalogInstanceNames]  cin
										INNER JOIN [monitoring].[statsReplicationLatency] srl ON srl.[project_id] = cin.[project_id] AND srl.[publisher_server] = cin.[instance_name]
										LEFT JOIN [monitoring].[alertSkipRules] asr ON	asr.[category] = 'replication'
																						AND asr.[alert_name] IN ('replication latency')
																						AND asr.[active] = 1
																						AND (asr.[skip_value] = ('[' + srl.[publisher_server] + '].[' + srl.[publisher_db] + '](' + srl.[publication_name] + ')'))
																						AND (    asr.[skip_value2] IS NULL 
																								OR (asr.[skip_value2] IS NOT NULL AND asr.[skip_value2] = ('[' + srl.[subscriber_server] + '].[' + srl.[subscriber_db] + ']'))
																							)
										WHERE cin.[instance_active]=1
												AND cin.[project_id] = @projectID
												AND cin.[instance_name] LIKE @sqlServerNameFilter
												AND (srl.[overall_latency] IS NULL OR srl.[overall_latency]>=@alertThresholdCriticalReplicationLatencySec)									
												AND srl.[subscription_status] = 2 /* active subscriptions */
												AND srl.[state] = 2 /* run analysis and get data jobs completed successfully */
												AND asr.[id] IS NULL
										ORDER BY [instance_name], [object_name]
OPEN crsReplicationAlarms
FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
WHILE @@FETCH_STATUS=0
	begin
		EXEC [dbo].[usp_logEventMessageAndSendEmail]	@projectCode			= @projectCode,
														@sqlServerName			= @instanceName,
														@dbName					= @severity,
														@objectName				= @objectName,
														@childObjectName		= NULL,
														@module					= 'monitoring',
														@eventName				= @eventName,
														@parameters				= NULL,	
														@eventMessage			= @eventMessage,
														@dbMailProfileName		= NULL,
														@recipientsList			= NULL,
														@eventType				= 6,	/* 6 - alert-custom */
														@additionalOption		= 0

		FETCH NEXT FROM crsReplicationAlarms INTO @instanceName, @objectName, @severity, @eventName, @eventMessage
	end
CLOSE crsReplicationAlarms
DEALLOCATE crsReplicationAlarms
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




--update job: Monitoring - Disk Space
RAISERROR('update job: Monitoring - Disk Space', 10, 1) WITH NOWAIT
GO
DECLARE   @queryToRun	[nvarchar](max)
		, @projectCode	[sysname]
		, @job_id		[uniqueidentifier]

SELECT @projectCode = [value] FROM dbo.appConfigurations WHERE [name] = 'Default project code'
SET @queryToRun = N'DECLARE		@strMessage				[varchar](8000)='''',
			@currentRunning			[int]=0,
			@lastExecutionStatus	[int]=0,
			@lastExecutionDate		[varchar](10)='''',
			@lastExecutionTime 		[varchar](8)='''',
			@runningTimeSec			[bigint]=0,
			@jobName				[sysname]

/* check execution overlapping with Health Check job */
SELECT TOP 1 @jobName = sj.[name]
FROM [msdb].dbo.sysjobs sj
INNER JOIN [msdb].dbo.sysjobsteps sjs ON sj.[job_id] = sjs.[job_id] 
WHERE sj.[name] LIKE ''%Discovery & Health Check%''
	AND sjs.[database_name] = DB_NAME()

SET @lastExecutionStatus = 4
WHILE @lastExecutionStatus = 4
	begin
		EXEC dbo.usp_sqlAgentJobCheckStatus	@sqlServerName			= @@SERVERNAME,
											@jobName				= @jobName,
											@strMessage				= @strMessage OUTPUT,	
											@currentRunning			= @currentRunning OUTPUT,			
											@lastExecutionStatus	= @lastExecutionStatus OUTPUT,			
											@lastExecutionDate		= @lastExecutionDate OUTPUT,		
											@lastExecutionTime 		= @lastExecutionTime OUTPUT,	
											@runningTimeSec			= @runningTimeSec OUTPUT,
											@selectResult			= 0,
											@extentedStepDetails	= 0,		
											@debugMode				= 0
		IF @lastExecutionStatus = 4
			begin
				SET @strMessage = ''Job "'' + @jobName + ''" is currently running. Waiting for its completion...''
				RAISERROR(@strMessage, 10, 1) WITH NOWAIT

				WAITFOR DELAY ''00:00:30''
			end
	end
'

IF (SELECT COUNT(*) FROM msdb.dbo.sysjobs WHERE [name] LIKE '% - Monitoring - Disk Space%')=1
	SELECT @job_id = [job_id]
	FROM msdb.dbo.sysjobs 
	WHERE [name] LIKE '% - Monitoring - Disk Space%'
ELSE
	SET @job_id = NULL

RAISERROR('--	update job step 1', 10, 1) WITH NOWAIT
IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 1
									, @command = @queryToRun

SET @queryToRun = N'EXEC [dbo].[usp_jobQueueExecute]	@projectCode			= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N',
														@moduleFilter			= ''monitoring'',
														@descriptorFilter		= ''dbo.usp_hcCollectDiskSpaceUsage'',
														@waitForDelay			= DEFAULT,
														@debugMode				= 0'

RAISERROR('--	update job step 3', 10, 1) WITH NOWAIT
IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 3
									, @command = @queryToRun

GO



--update job: Monitoring - Replication
RAISERROR('update job: Monitoring - Replication', 10, 1) WITH NOWAIT
GO
DECLARE   @queryToRun	[nvarchar](max)
		, @projectCode	[sysname]
		, @job_id		[uniqueidentifier]

SELECT @projectCode = [value] FROM dbo.appConfigurations WHERE [name] = 'Default project code'
SET @queryToRun = N'EXEC [dbo].[usp_monAlarmCustomReplicationLatency]	  @projectCode			= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N'
																		, @sqlServerNameFilter	= ''%''
																		, @operationDelay		= DEFAULT
																		, @debugMode			= 0'

IF (SELECT COUNT(*) FROM msdb.dbo.sysjobs WHERE [name] LIKE '% - Replication%')=1
	SELECT @job_id = [job_id]
	FROM msdb.dbo.sysjobs 
	WHERE [name] LIKE '% - Monitoring - Replication%'
ELSE
	SET @job_id = NULL

RAISERROR('--	update job step 1', 10, 1) WITH NOWAIT
IF @job_id IS NOT NULL
	EXEC msdb.dbo.sp_update_jobstep   @job_id = @job_id
									, @step_id = 1
									, @command = @queryToRun
GO



-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 12.01.2016
-- Module			 : SQL Server 2005/2008/2008R2/2012/2014+
-- Description		 : monitor transaction status: uncommitted or long running
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create job: Monitoring - TransactionStatus', 10, 1) WITH NOWAIT
GO
USE [msdb]
GO

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @databaseName		[sysname]
		, @projectCode		[nvarchar](32)

SELECT @logFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
FROM (
		SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
	)er


IF @logFileLocation IS NULL SET @logFileLocation = N'C:\'

---------------------------------------------------------------------------------------------------
/* setting the job name & job log location */
---------------------------------------------------------------------------------------------------
SET @projectCode  = N'$(projectCode)'	/* add local project code here */

SET @databaseName = DB_NAME()
SET @job_name = @databaseName + N' - Monitoring - TransactionStatus'
SET @logFileLocation = @logFileLocation + N'job-' + @job_name + N'.log'


---------------------------------------------------------------------------------------------------
/* dropping job if exists */
---------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @job_name)
		EXEC msdb.dbo.sp_delete_job @job_name=@job_name, @delete_unused_schedule=1		


---------------------------------------------------------------------------------------------------
/* creating the job */
BEGIN TRANSACTION
	DECLARE @ReturnCode INT
	SELECT @ReturnCode = 0
	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
	BEGIN
		EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB',
													@type=N'LOCAL', 
													@name=N'Database Maintenance'
		IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	END

	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=1, 
											@notify_level_eventlog=0, 
											@notify_level_email=2, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'Transaction Status custom monitoring and alarms
http://dbaTDPMon.codeplex.com', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'EXEC [dbo].[usp_monAlarmCustomTransactionsStatus] @projectCode			= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N'
																		, @sqlServerNameFilter	= ''%''
																		, @debugMode			= 0
'
	
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Check Alarm',
												@step_id=1, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=4, 
												@on_fail_step_id=2, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @job_name + ''',
												@logFileLocation=''' + @logFileLocation + ''',
												@module			=''monitoring'',
												@sendLogAsAttachment = 1,
												@eventType		= 2'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Send email', 
												@step_id=2, 
												@cmdexec_success_code=0, 
												@on_success_action=1, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@flags=0

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job	@job_id = @jobId, 
												@start_step_id = 1

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Every 15 minutes', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=4, 
													@freq_subday_interval=15, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=20151012, 
													@active_end_date=99991231, 
													@active_start_time=0, 
													@active_end_time=235959

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

---------------------------------------------------------------------------------------------------
COMMIT TRANSACTION
GOTO EndSave

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

---------------------------------------------------------------------------------------------------
GO



-- ============================================================================
-- Copyright (c) 2004-2016 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 03.02.2016
-- Module			 : SQL Server 2005/2008/2008R2/2012/2014+
-- Description		 : monitor failed SQL Agent jobs
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create job: Monitoring - SQLAgentFailedJobs', 10, 1) WITH NOWAIT
GO
USE [msdb]
GO

DECLARE   @job_name			[sysname]
		, @logFileLocation	[nvarchar](512)
		, @queryToRun		[nvarchar](4000)
		, @databaseName		[sysname]
		, @projectCode		[nvarchar](32)

SELECT @logFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
FROM (
		SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
	)er


IF @logFileLocation IS NULL SET @logFileLocation = N'C:\'

---------------------------------------------------------------------------------------------------
/* setting the job name & job log location */
---------------------------------------------------------------------------------------------------
SET @projectCode  = N'$(projectCode)'	/* add local project code here */

SET @databaseName = DB_NAME()
SET @job_name = @databaseName + N' - Monitoring - SQLAgentFailedJobs'
SET @logFileLocation = @logFileLocation + N'job-' + @job_name + N'.log'


---------------------------------------------------------------------------------------------------
/* dropping job if exists */
---------------------------------------------------------------------------------------------------
IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = @job_name)
		EXEC msdb.dbo.sp_delete_job @job_name=@job_name, @delete_unused_schedule=1		


---------------------------------------------------------------------------------------------------
/* creating the job */
BEGIN TRANSACTION
	DECLARE @ReturnCode INT
	SELECT @ReturnCode = 0
	IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
	BEGIN
		EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB',
													@type=N'LOCAL', 
													@name=N'Database Maintenance'
		IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	END

	DECLARE @jobId BINARY(16)
	EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=@job_name, 
											@enabled=1, 
											@notify_level_eventlog=0, 
											@notify_level_email=2, 
											@notify_level_netsend=0, 
											@notify_level_page=0, 
											@delete_level=0, 
											@description=N'SQL Server Agent failed jobs custom monitoring and alarms
http://dbaTDPMon.codeplex.com', 
											@category_name=N'Database Maintenance', 
											@owner_login_name=N'sa', 
											@job_id = @jobId OUTPUT
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'EXEC [dbo].[usp_monAlarmCustomSQLAgentFailedJobs] @projectCode			= ' + CASE WHEN @projectCode IS NOT NULL THEN N'''' + @projectCode + '''' ELSE 'NULL' END + N'
																		, @sqlServerNameFilter	= ''%''
																		, @debugMode			= 0
'
	
	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Check Alarm',
												@step_id=1, 
												@cmdexec_success_code=0, 
												@on_success_action=3, 
												@on_success_step_id=0, 
												@on_fail_action=4, 
												@on_fail_step_id=2, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@output_file_name=@logFileLocation, 
												@flags=2
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
	
	---------------------------------------------------------------------------------------------------
	SET @queryToRun = N'
EXEC [dbo].[usp_sqlAgentJobEmailStatusReport]	@jobName		=''' + @job_name + ''',
												@logFileLocation=''' + @logFileLocation + ''',
												@module			=''monitoring'',
												@sendLogAsAttachment = 1,
												@eventType		= 2'

	EXEC @ReturnCode = msdb.dbo.sp_add_jobstep	@job_id=@jobId, 
												@step_name=N'Send email', 
												@step_id=2, 
												@cmdexec_success_code=0, 
												@on_success_action=1, 
												@on_success_step_id=0, 
												@on_fail_action=2, 
												@on_fail_step_id=0, 
												@retry_attempts=0, 
												@retry_interval=0, 
												@os_run_priority=0, @subsystem=N'TSQL', 
												@command=@queryToRun, 
												@database_name=@databaseName, 
												@flags=0

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_update_job	@job_id = @jobId, 
												@start_step_id = 1

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule	@job_id=@jobId, 
													@name=N'Every 10 minutes', 
													@enabled=1, 
													@freq_type=4, 
													@freq_interval=1, 
													@freq_subday_type=4, 
													@freq_subday_interval=10, 
													@freq_relative_interval=0, 
													@freq_recurrence_factor=0, 
													@active_start_date=20151012, 
													@active_end_date=99991231, 
													@active_start_time=0, 
													@active_end_time=235959

	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

	---------------------------------------------------------------------------------------------------
	EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
	
	IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

---------------------------------------------------------------------------------------------------
COMMIT TRANSACTION
GOTO EndSave

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

---------------------------------------------------------------------------------------------------
GO

RAISERROR('update jobs description..', 10, 1) WITH NOWAIT
GO
UPDATE msdb.dbo.sysjobs SET [description]=REPLACE([description], 'codeple.com', 'codeplex.com') WHERE [description] LIKE '%codeple.com%'
GO


USE [dbaTDPMon]
GO
RAISERROR('Create procedure: [dbo].[usp_mpDatabaseConsistencyCheck]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE id = OBJECT_ID(N'[dbo].[usp_mpDatabaseConsistencyCheck]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseConsistencyCheck]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseConsistencyCheck]
		@sqlServerName			[sysname]=@@SERVERNAME,
		@dbName					[sysname],
		@tableSchema			[sysname]	=  '%',
		@tableName				[sysname]   =  '%',
		@flgActions				[smallint]	=   12,
		@flgOptions				[int]		=    3,
		@executionLevel			[tinyint]	=    0,
		@debugMode				[bit]		=    0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan 
--					 : SQL Server 2000/2005/2008/2008R2/2012+
-- Description		 : Consistency Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@sqlServerName	- name of SQL Server instance to analyze
--		@dbName			- database to be analyzed
--		@tableSchema	- schema that current table belongs to
--		@tableName		- specify % for all tables or a table name to be analyzed
--		@flgActions		1	- perform database consistency check (DBCC CHECKDB)
--							  should be performed weekly
--						2	- perform table consistency check (DBCC CHECKTABLE)
--							  should be performed weekly
--					    4   - perform consistency check of disk space allocation structures (DBCC CHECKALLOC) (default)
--							  should be performed daily
--					    8   - perform consistency check of catalogs (DBCC CHECKCATALOG) (default)
--							  should be performed daily
--					   16   - perform consistency check of table constraints (DBCC CHECKCONSTRAINTS)
--							  should be performed weekly
--					   32   - perform consistency check of table identity value (DBCC CHECKIDENT)
--							  should be performed weekly
--					   64   - perform correction to space usage (DBCC UPDATEUSAGE)
--							  should be performed once at 2 weeks
--					  128 	- Cleaning wasted space in Database (variable-length column) (DBCC CLEANTABLE)
--							  should be performed once a year
--		@flgOptions	    1	- run DBCC CHECKDB/DBCC CHECKTABLE using PHYSICAL_ONLY (default). 
--							  by default DBCC CHECKDB is doing all consistency checks and for a VLDB it may take a very long time
--					    2  - use NOINDEX when running DBCC CHECKTABLE. Index consistency errors are not critical (default)
--					   32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution

--		@debugMode			- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------
/*
	--usage sample
	EXEC [dbo].[usp_mpDatabaseConsistencyCheck]	@sqlServerName			= @@SERVERNAME,
												@dbName					= 'dbSQLTools',
												@tableSchema			= 'dbo',
												@tableName				= '%',
												@flgActions				= DEFAULT,
												@flgOptions				= DEFAULT,
												@debugMode				= DEFAULT
*/

DECLARE		@queryToRun  					[nvarchar](2048),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@DBCCCheckTableBatchSize 		[int],
			@errorCode						[int],
			@databaseStatus					[int],
			@dbi_dbccFlags					[int]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @sqlServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @debugMode

--------------------------------------------------------------------------------------------------
/* AlwaysOn Availability Groups */
DECLARE @agName			[sysname],
		@agStopLimit	[int],
		@actionType		[sysname]

SET @agStopLimit = 0
SET @actionType = NULL

IF @flgActions &  64 = 64	SET @actionType = 'update space usage'
IF @flgActions & 128 = 128	SET @actionType = 'clean wasted space - table'

IF @serverVersionNum >= 11 AND @flgActions IS NOT NULL
	EXEC @agStopLimit = [dbo].[usp_mpCheckAvailabilityGroupLimitations]	@sqlServerName		= @sqlServerName,
																		@dbName				= @dbName,
																		@actionName			= 'database maintenance',
																		@actionType			= @actionType,
																		@flgActions			= @flgActions,
																		@flgOptions			= @flgOptions OUTPUT,
																		@agName				= @agName OUTPUT,
																		@executionLevel		= @executionLevel,
																		@debugMode			= @debugMode

IF @agStopLimit <> 0
	RETURN 0
	
---------------------------------------------------------------------------------------------
DECLARE @compatibilityLevel [tinyint]
IF object_id('tempdb..#databaseCompatibility') IS NOT NULL 
	DROP TABLE #databaseCompatibility

CREATE TABLE #databaseCompatibility
	(
		[compatibility_level]		[tinyint]
	)


SET @queryToRun = N''
IF @serverVersionNum >= 9
	SET @queryToRun = @queryToRun + N'SELECT [compatibility_level] FROM sys.databases WHERE [name] = ''' + @dbName + N''''
ELSE
	SET @queryToRun = @queryToRun + N'SELECT [cmptlevel] FROM master.dbo.sysdatabases WHERE [name] = ''' + @dbName + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@compatibilityLevel, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC (@queryToRun)

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility


---------------------------------------------------------------------------------------------
SET @DBCCCheckTableBatchSize = 65536
SET @CurrentTableSchema		 = @tableSchema
SET @tableName				 = REPLACE(@tableName, '''', '''''')
SET @errorCode				 = 0

---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseTableList') IS NOT NULL 
	DROP TABLE #databaseTableList

CREATE TABLE #databaseTableList(
								[table_schema]	[sysname]	NULL,
								[table_name]	[sysname]	NULL
								)
CREATE INDEX IX_databaseTableList_TableName ON #databaseTableList([table_name])



--------------------------------------------------------------------------------------------------
--get database status
-----------------------------------------------------------------------------------------
IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)
			
SET @queryToRun = N'SELECT [status] FROM master.dbo.sysdatabases WHERE [name]=''' + @dbName + N'''' 
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

DELETE FROM #serverPropertyConfig
INSERT	INTO #serverPropertyConfig([value])
		EXEC (@queryToRun)

SELECT @databaseStatus = [value]
FROM #serverPropertyConfig

---------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2 OR @flgActions & 16 = 16 OR @flgActions & 64 = 64 OR @flgActions & 128 = 128
	begin
		--get table list that will be analyzed including materialized views; will pick only tables with reserved pages
		SET @queryToRun = N''
		IF @serverVersionNum >= 9
			SET @queryToRun = @queryToRun + N'SELECT DISTINCT ob.[table_schema], ob.[table_name]
FROM (
		SELECT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N'].sys.objects obj WITH (READPAST)
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''' +

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL

		SELECT DISTINCT obj.[object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N'].sys.indexes idx WITH (READPAST)
		INNER JOIN [' + @dbName + N'].sys.objects obj WITH (READPAST) ON obj.[object_id] = idx.[object_id]
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = obj.[schema_id]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N''''
		END + N'
	)ob
INNER JOIN
	(
		SELECT	ps.[object_id],
				sch.[name]	AS [schema_name],
				so.[name]	AS [table_name],
				ps.[reserved_page_count]
		FROM (
				SELECT	ps.[object_id]
						, SUM (ps.[reserved_page_count]) AS [reserved_page_count]
				FROM [' + @dbName + N'].sys.dm_db_partition_stats ps WITH (READPAST)
				GROUP BY ps.[object_id]
			) AS ps
		INNER JOIN [' + @dbName + N'].sys.objects so  WITH (READPAST) ON so.[object_id] = ps.[object_id] 
		INNER JOIN [' + @dbName + N'].sys.schemas sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id]
		WHERE	so.[type] in (''S'', ''U'', ''V'')
			AND ps.[reserved_page_count] > 0
	)ps ON ob.[object_id] = ps.[object_id]'
		ELSE
			SET @queryToRun = @queryToRun + N'SELECT ob.[table_schema], ob.[table_name]
FROM (
		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N']..sysobjects obj
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = obj.[uid]
		WHERE obj.[type] IN (''S'', ''U'')
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N'''' + 

		CASE WHEN @flgActions & 16 = 16 
				THEN N'' 
				ELSE		
		N'
		UNION ALL			

		SELECT DISTINCT obj.[id] AS [object_id], sch.[name] AS [table_schema], obj.[name] AS [table_name]
		FROM [' + @dbName + N']..sysindexes idx
		INNER JOIN [' + @dbName + N']..sysobjects obj ON obj.[id] = idx.[id]
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = obj.[uid]
		WHERE obj.[type]= ''V''
				AND obj.[name] LIKE ''' + @tableName + N'''
				AND sch.[name] LIKE ''' + @tableSchema + N''''
		END + N'
	)ob
INNER JOIN
	(
		SELECT si.[id] AS [object_id], sch.[name] AS [table_schema], so.[name] AS [table_name]
		FROM [' + @dbName + N']..sysobjects so
		INNER JOIN [' + @dbName + N']..sysindexes si on so.[id] = si.[id]
		INNER JOIN [' + @dbName + N']..sysusers sch ON sch.[uid] = so.[uid]
		WHERE si.[reserved]<>0
	)ps ON ob.[object_id] = ps.[object_id]'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM #databaseTableList
		INSERT	INTO #databaseTableList([table_schema], [table_name])
				EXEC (@queryToRun)
	end

--------------------------------------------------------------------------------------------------
--when running DBCC CHECKDB, check if DATA_PURITY option should be used or not (run only when dbi_dbccFlags=0)
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1 AND @serverVersionNum >= 9 AND @flgOptions & 1 = 0
	begin
		IF object_id('tempdb..#dbi_dbccFlags') IS NOT NULL DROP TABLE #dbccLastKnownGood
		CREATE TABLE #dbi_dbccFlags
		(
			[Value]					[sysname]			NULL
		)

		IF object_id('tempdb..#dbccDBINFO') IS NOT NULL DROP TABLE #dbccDBINFO
		CREATE TABLE #dbccDBINFO
			(
				[id]				[int] IDENTITY(1,1),
				[ParentObject]		[varchar](255),
				[Object]			[varchar](255),
				[Field]				[varchar](255),
				[Value]				[varchar](255)
			)
	
		IF @sqlServerName <> @@SERVERNAME
			begin
				IF @serverVersionNum < 11
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @dbName + N']) WITH TABLERESULTS'''')'')x
										WHERE [Field]=''dbi_dbccFlags'''
				ELSE
					SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
										FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @dbName + N']) WITH TABLERESULTS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
										WHERE [Field]=''dbi_dbccFlags'''
			end
		ELSE
			begin							
				INSERT	INTO #dbccDBINFO
						EXEC ('DBCC DBINFO (''' + @dbName + N''') WITH TABLERESULTS')

				SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccFlags'''											
			end

		IF @debugMode = 1 PRINT @queryToRun
				
		TRUNCATE TABLE #dbi_dbccFlags
		INSERT	INTO #dbi_dbccFlags([Value])
				EXEC (@queryToRun)

		SELECT @dbi_dbccFlags = ISNULL([Value], 0)
		FROM #dbi_dbccFlags
		
		SET @dbi_dbccFlags = ISNULL(@dbi_dbccFlags, 0)
	end


--------------------------------------------------------------------------------------------------
--database consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 1 = 1
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Database consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + '...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKDB(''' + @dbName + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS' + CASE WHEN @flgOptions & 1 = 1 THEN ', PHYSICAL_ONLY' ELSE '' END

		IF @serverVersionNum >= 9 AND @flgOptions & 1 = 0 AND @dbi_dbccFlags <> 2
			SET @queryToRun = @queryToRun + ', DATA_PURITY'

		IF @compatibilityLevel >= 100 AND @flgOptions & 1 = 0
			SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
		
		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end	


--------------------------------------------------------------------------------------------------
--tables and views consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 2 = 2
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Tables/views consistency check ' + CASE WHEN @flgOptions & 1 = 1 THEN '(PHYSICAL_ONLY)' ELSE '' END + CASE WHEN @flgOptions & 2 = 2 THEN '(NOINDEX)' ELSE '' END + '...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
										FROM #databaseTableList	
										ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKTABLE(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']''' + CASE WHEN @flgOptions & 2 = 2 THEN ', NOINDEX' ELSE '' END + ') WITH ALL_ERRORMSGS, NO_INFOMSGS'
				
				IF @serverVersionNum >= 9 AND @dbi_dbccFlags <> 2
					SET @queryToRun = @queryToRun + ', DATA_PURITY'
				
				IF @compatibilityLevel >= 10 AND @flgOptions & 2 = 0
					SET @queryToRun = @queryToRun + ', EXTENDED_LOGICAL_CHECKS'

				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
				
				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database consistency check - tables/views',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end			


--------------------------------------------------------------------------------------------------
--allocation structures consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 4 = 4
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Allocation structures consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKALLOC(''' + @dbName + ''') WITH ALL_ERRORMSGS, NO_INFOMSGS'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check - allocation structures',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end			


--------------------------------------------------------------------------------------------------
--catalogs consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 8 = 8
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Catalogs consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'DBCC CHECKCATALOG(''' + @dbName + ''') WITH NO_INFOMSGS'
		IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
														@dbName			= @dbName,
														@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
														@eventName		= 'database consistency check - catalogs',
														@queryToRun  	= @queryToRun,
														@flgOptions		= @flgOptions,
														@executionLevel	= @nestedExecutionLevel,
														@debugMode		= @debugMode
	end			


--------------------------------------------------------------------------------------------------
--table constraints consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 16 = 16
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Table constraints consistency check ...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
										FROM #databaseTableList	
										ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CHECKCONSTRAINTS(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'') WITH ALL_ERRORMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database consistency check - table constraints',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end			


--------------------------------------------------------------------------------------------------
--table identity value consistency check
--------------------------------------------------------------------------------------------------
IF @flgActions & 32 = 32
	begin
		IF	@databaseStatus & 32 = 32				/* LOADING */
			OR @databaseStatus & 64 = 64			/* PRE RECOVERY */
			OR @databaseStatus & 128 = 128			/* RECOVERING */
			OR @databaseStatus & 256 = 256			/* NOT RECOVERED */
			OR @databaseStatus & 512 = 512			/* OFFLINE */
			OR @databaseStatus & 1024 = 1024		/* READ ONLY */
			OR @databaseStatus & 2048 = 2048		/* DBO USE ONLY */
			OR @databaseStatus & 4096 = 4096		/* SINGLE USER */
			OR @databaseStatus & 32768 = 32768		/* EMERGENCY MODE */
			OR @databaseStatus & 2097152 = 2097152	/* STANDBY */
			OR @databaseStatus & 4194584 = 4194584	/* SUSPECT */
			OR @databaseStatus = 0
			begin
				SET @queryToRun='Current database state does not allow running DBCC CHECKIDENT. It will be skipped.'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			end
		ELSE
			begin
				IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

				SET @queryToRun=N'Table identity value consistency check ...' + ' [' + @dbName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
				---------------------------------------------------------------------------------------------
				--create temporary tables that will be used 
				---------------------------------------------------------------------------------------------
				IF object_id('tempdb..#databaseTableListIdent') IS NOT NULL 
					DROP TABLE #databaseTableListIdent

				CREATE TABLE #databaseTableListIdent(
														[table_schema]	[sysname],
														[table_name]	[sysname]
													)
				CREATE INDEX IX_databaseTableListIdent_TableName ON #databaseTableListIdent([table_name])


				--get table list that will be analyzed. only tables with identity columns
				SET @queryToRun = N''
				IF @serverVersionNum >= 9
					SET @queryToRun = @queryToRun + N'	SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM [' + @dbName + '].sys.objects obj
												INNER JOIN [' + @dbName + '].sys.schemas sch ON sch.[schema_id] = obj.[schema_id]
												WHERE obj.[type] IN (''U'')
														AND obj.[object_id] IN (
																			SELECT [object_id]
																			FROM [' + @dbName + '].sys.columns
																			WHERE [is_identity] = 1
																			)
														AND obj.[name] LIKE ''' + @tableName + '''
														AND sch.[name] LIKE ''' + @tableSchema + ''''
				ELSE
					SET @queryToRun = @queryToRun + N'SELECT DISTINCT sch.[name] AS [table_schema], obj.[name] AS [table_name]
												FROM  [' + @dbName + ']..sysobjects obj
												INNER JOIN  [' + @dbName + ']..sysusers sch ON sch.[uid] = obj.[uid]
												WHERE obj.[type] IN (''U'')
														AND obj.[id] IN (
																		SELECT [id]
																		FROM  [' + @dbName + ']..syscolumns
																		WHERE [autoval] is not null
																		)
														AND obj.[name] LIKE ''' + @tableName + '''
														AND sch.[name] LIKE ''' + @tableSchema + ''''			
				
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				DELETE FROM #databaseTableListIdent
				INSERT	INTO #databaseTableListIdent([table_schema], [table_name])
						EXEC (@queryToRun)

				DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
												FROM #databaseTableListIdent	
												ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0
					begin
						SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
						SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC CHECKIDENT(''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'', RESEED)'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																		@eventName		= 'database consistency check - table identity value',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
																					
						FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList

				IF object_id('tempdb..#databaseTableListIdent') IS NOT NULL 
					DROP TABLE #databaseTableListIdent
			end			
	end

--------------------------------------------------------------------------------------------------
--correct space usage
--------------------------------------------------------------------------------------------------
IF @flgActions & 64 = 64
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Update space usage...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		IF @tableName='%' 
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + @dbName + ''') WITH NO_INFOMSGS'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= NULL,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database maintenance - update space usage',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
			end
		ELSE
			begin
				DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
												FROM #databaseTableList	
												ORDER BY [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
				WHILE @@FETCH_STATUS = 0
					begin
						SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
						SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DBCC UPDATEUSAGE(''' + @dbName + ''', ''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'')'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																		@dbName			= @dbName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																		@eventName		= 'database maintenance - update space usage',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @debugMode
																		
						FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end
	end			


--------------------------------------------------------------------------------------------------
--		Cleaning wasted space in Database
--		DBCC CLEANTABLE reclaims space after a variable-length column is dropped. 
--		A variable-length column can be one of the following data types:  varchar, nvarchar, varchar(max),
--		nvarchar(max), varbinary, varbinary(max), text, ntext, image, sql_variant, and xml. 
--		The command does not reclaim space after a fixed-length column is dropped.

--		Best Practices
--		DBCC CLEANTABLE should not be executed as a routine maintenance task. 
--		Instead, use DBCC CLEANTABLE after you make significant changes to variable-length columns in 
--		a table or indexed view and you need to immediately reclaim the unused space. 
--		Alternatively, you can rebuild the indexes on the table or view; however, doing so is a more 
--		resource-intensive operation.
--------------------------------------------------------------------------------------------------
IF @flgActions & 128 = 128
	begin
		IF @executionLevel=0 EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun=N'Cleaning wasted space in variable length columns...' + ' [' + @dbName + ']'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR FOR	SELECT DISTINCT [table_schema], [table_name] 
										FROM #databaseTableList	
										ORDER BY [table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @objectName=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DBCC CLEANTABLE(''' + @dbName + ''', ''[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'', ' + CAST(@DBCCCheckTableBatchSize AS [nvarchar]) + ')'
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

				EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @sqlServerName,
																@dbName			= @dbName,
																@objectName		= @objectName,
																@module			= 'dbo.usp_mpDatabaseConsistencyCheck',
																@eventName		= 'database maintenance - clean wasted space - table',
																@queryToRun  	= @queryToRun,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestedExecutionLevel,
																@debugMode		= @debugMode
					
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end

RETURN @errorCode
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
										, [job_name], [job_step_name], [job_database_name], REPLACE([job_command], '''', '''''') AS [job_command]
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


RAISERROR('Create procedure: [dbo].[usp_sqlAgentJob]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_sqlAgentJob]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_sqlAgentJob]
GO

CREATE PROCEDURE [dbo].[usp_sqlAgentJob]
		@sqlServerName			[sysname],
		@jobName				[sysname],
		@operation				[varchar](10), 
		@dbName					[sysname], 
		@jobStepName 			[sysname]='',
		@jobStepCommand			[varchar](8000)='',
		@jobLogFileName			[varchar](512)='',
		@jobStepRetries			[smallint]=0,
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

------------------------------------------------------------------------------------------------------------------------------------------
--		@jobName		- numele job-ului... toate operatiunile se vor face functie de acest nume!
--		@operation		'Add'   - se adauga un nou step definit de @jobStepName si @jobStepCommand
--						'Clean' - curata job-ul de pasi si sterge job-ul
--		@dbName			- baza de date pentru care este asociat job-ul
--		@jobStepName	- numele pasului ce se adauga
--		@jobStepCommand	- script sql ce se va executa pentru pasul definit
------------------------------------------------------------------------------------------------------------------------------------------

DECLARE @Error				[int],
		@jobID 				[varchar](200),
		@jobStepID			[int],
		@jobStepIDNew		[int],
		@jobCategoryID		[int],
		@jobStepStatus		[int], 
		@queryToRun			[nvarchar](4000),
		@tmpServer			[varchar](8000)

---------------------------------------------------------------------------------------------
SET NOCOUNT ON
---------------------------------------------------------------------------------------------

IF object_id('#tmpCheckParameters') IS NOT NULL DROP TABLE #tmpCheckParameters
CREATE TABLE #tmpCheckParameters (Result varchar(1024))

IF ISNULL(@sqlServerName, '')=''
	begin
		SET @queryToRun='--	ERROR: The specified value for SOURCE server is not valid.'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

IF LEN(@jobName)=0 OR ISNULL(@jobName, '')=''
	begin
		RAISERROR('--ERROR: Must specify a job name.', 10, 1) WITH NOWAIT
		RETURN 1
	end

SET @queryToRun='SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
TRUNCATE TABLE #tmpCheckParameters
INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
IF (SELECT count(*) FROM #tmpCheckParameters)=0
	begin
		SET @queryToRun='--	ERROR: SOURCE server [' + @sqlServerName + '] is not defined as linked server on THIS server [' + @sqlServerName + '].'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

SET @tmpServer = '[' + @sqlServerName + '].master.dbo.sp_executesql'
------------------------------------------------------------------------------------------------------------------------------------------
--adding a new job or step to the existing job
IF @operation='Add'
	begin
		SET @queryToRun='SELECT category_id FROM msdb.dbo.syscategories WHERE name LIKE ''%Database Maintenance%'''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
		SELECT TOP 1 @jobCategoryID=Result FROM #tmpCheckParameters

		SET @jobStepID=1

		SET @queryToRun='SELECT count(*) FROM msdb.dbo.sysjobs WHERE name = ''' + @jobName + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
		
		--defining job and job properties
		IF (SELECT ISNULL(Result,0) FROM #tmpCheckParameters) =0
			begin
				--adding job
				set @queryToRun='EXEC msdb.dbo.sp_add_job 	@enabled 	 = 1, 
															@job_name	 = ''' + @jobName + ''', 
															@description = ''' + @jobName + ''', 
															@category_id = ' + CAST(@jobCategoryID as varchar) + ', 
															@owner_login_name = ''sa'''
				IF @debugMode=1	PRINT @queryToRun
				EXEC @Error=@tmpServer @queryToRun

				IF @Error<>0
					begin
						SET @queryToRun='--Cannot add job [' + @jobName + '] to SQL Server Agent.'
						RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
						RETURN 1
					end

				--adding job to server
				SET @queryToRun='EXEC msdb.dbo.sp_add_jobserver @job_name = ''' + @jobName + ''', @server_name = ''(local)'''
				IF @debugMode=1	PRINT @queryToRun
				EXEC @Error=@tmpServer @queryToRun

				IF @Error<>0
					begin
						SET @queryToRun='--Cannot add job [' + @jobName + '] to SQL Server Agent.'
						RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
						RETURN 1
					end
				ELSE
					begin
						SET @queryToRun='--Successfully add job [' + @jobName + '] to SQL Server Agent.'
						RAISERROR(@queryToRun, 10, 1) WITH NOWAIT
					end
		
			end
		SET @queryToRun='SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ''' + @jobName + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
		SELECT TOP 1 @jobID = ISNULL(Result,'') FROM #tmpCheckParameters

		SET @queryToRun='SELECT TOP 1 (step_id+1) FROM msdb.dbo.sysjobsteps WHERE job_id=''' + @jobID + ''' ORDER BY step_id DESC'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
		SELECT TOP 1 @jobStepID = ISNULL(Result,0) FROM #tmpCheckParameters

		IF @jobStepID-1>0
			begin
				SET @queryToRun='UPDATE msdb.dbo.sysjobsteps SET on_success_action=4, on_success_step_id=' + CAST(@jobStepID as varchar) + ', on_fail_action=4, on_fail_step_id=' + CAST(@jobStepID as varchar) + ' WHERE job_id=''' + @jobID + ''' AND step_id=' + CAST((@jobStepID-1) as varchar) 
				IF @debugMode=1	PRINT @queryToRun
				EXEC @tmpServer @queryToRun				
			end

		--defining job step and step properties
		SET @queryToRun='EXEC msdb.dbo.sp_add_jobstep	@job_id = ''' + @jobID + ''',
														@step_id = ' + CAST(@jobStepID as varchar) + ',
														@step_name = ''' + @jobStepName + ''',
														@on_success_action = 1,
														@on_fail_action = 2, 
														@retry_interval = 1,							
														@command = ''' + @jobStepCommand + ''',
														@database_name = ''' + @dbName + ''','
		IF @jobLogFileName<>'' 
			SET @queryToRun=@queryToRun + '
								@output_file_name=''' + @jobLogFileName + ''','
		SET @queryToRun=@queryToRun + '				
								@retry_attempts=' + CAST(@jobStepRetries AS [varchar]) + ',
								@flags=6'
		
		IF @debugMode=1 PRINT @queryToRun
		EXEC @tmpServer @queryToRun

		IF @Error<>0
			begin
				SET @queryToRun= '--Cannot add job step: [' + @jobStepName + '] to server job [' + @jobName + ']'
				RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
				RETURN 1
			end
		ELSE
			begin
				SET @queryToRun= '--Successfully add job step: [' + @jobStepName + '] to server job [' + @jobName + ']'
				RAISERROR(@queryToRun, 10, 1) WITH NOWAIT
			end
	end
------------------------------------------------------------------------------------------------------------------------------------------
--erase all job steps
IF @operation='Clean'
	begin
		EXEC [dbo].[usp_sqlAgentJobCheckStatus] @sqlServerName, @jobName, '', @Error OUT, '', '', '', 0, 0, 0, 0
		IF @Error=1
			begin
				RAISERROR('--Cannot delete a job while it is running.', 10, 1) WITH NOWAIT
				RETURN 1
			end

		SET @queryToRun='SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ''' + @jobName + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
		SELECT TOP 1 @jobID = ISNULL(Result,'') FROM #tmpCheckParameters

		SET @queryToRun='SELECT count(*) FROM msdb.dbo.sysjobsteps WHERE job_id=''' + @jobID + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
		
		WHILE (SELECT Result FROM #tmpCheckParameters)<>0
			begin
				SET @queryToRun='SELECT step_id FROM msdb.dbo.sysjobsteps WHERE job_id=''' + @jobID + ''' ORDER BY step_id ASC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC (@queryToRun)

				DECLARE JobSteps CURSOR FOR SELECT Result FROM #tmpCheckParameters
				OPEN JobSteps
				FETCH NEXT FROM JobSteps INTO @jobStepID
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun='EXEC msdb.dbo.sp_delete_jobstep @job_id=''' + @jobID + ''', @step_id=1'
						IF @debugMode=1 PRINT @queryToRun

						EXEC @Error=@tmpServer @queryToRun
						IF @Error<>0
							begin
								SET @queryToRun= '--Cannot delete job step [' + @jobName + '], StepID [' + CAST(@jobStepID AS varchar) + ']'
								RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
								CLOSE JobSteps
								DEALLOCATE JobSteps
								RETURN 1
							end							
						FETCH NEXT FROM JobSteps INTO @jobStepID
					end
				CLOSE JobSteps
				DEALLOCATE JobSteps
				SET @queryToRun='SELECT count(*) FROM msdb.dbo.sysjobsteps WHERE job_id=''' + @jobID + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC (@queryToRun)
			end

		SET @queryToRun='SELECT count(*) FROM msdb.dbo.sysjobsteps WHERE job_id=''' + @jobID + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun

		TRUNCATE TABLE #tmpCheckParameters
		INSERT INTO #tmpCheckParameters EXEC (@queryToRun)

		IF (SELECT Result FROM #tmpCheckParameters)=0
			begin
				SET @queryToRun='SELECT count(*) FROM msdb.dbo.sysjobs WHERE job_id=''' + @jobID + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #tmpCheckParameters
				INSERT INTO #tmpCheckParameters EXEC (@queryToRun)

				IF (SELECT Result FROM #tmpCheckParameters)<>0
					begin
						SET @queryToRun='EXEC msdb.dbo.sp_delete_job @job_id=''' + @jobID + ''''
						IF @debugMode=1 PRINT @queryToRun

						EXEC @Error=@tmpServer @queryToRun
						IF @Error<>0
							begin
								SET @queryToRun= '--Cannot delete job [' + @jobName + ']'
								RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
								RETURN 1
							end		
						SET @queryToRun= '--Successfully deleted job : [' + @jobName + ']'
						RAISERROR(@queryToRun, 10, 1) WITH NOWAIT
					end
			end
		ELSE
			begin
				SET @queryToRun= '--The specified job: [' + @jobName + '] does not exist on the server.'
				RAISERROR(@queryToRun, 10, 1) WITH NOWAIT
			end
	end

RETURN 0

GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO


USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
