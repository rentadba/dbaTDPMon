RAISERROR('Create procedure: [dbo].[usp_reportHTMLGetStorageFolder]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_reportHTMLGetStorageFolder]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_reportHTMLGetStorageFolder]
GO

CREATE PROCEDURE [dbo].[usp_reportHTMLGetStorageFolder]
		@projectID					[smallint],
		@instanceID					[smallint]  = NULL,
		@StartDate					[datetime]	= NULL,
		@StopDate					[datetime]	= NULL,
		@flgCreateOutputFolder		[bit]		= 1,
		@localStoragePath			[nvarchar](260) OUTPUT,
		@relativeStoragePath		[nvarchar](260) OUTPUT,
		@debugMode					[bit]		= 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 18.11.2010
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

DECLARE @ReturnValue		[int],			-- will contain 1 : Succes  -1 : Fail
		@errMessage			[nvarchar](4000),
		@ErrNumber			[int],
		@projectName		[nvarchar](128),
		@instanceName		[sysname],
		@queryToRun			[nvarchar](4000)

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
	SET @ReturnValue=1

	-----------------------------------------------------------------------------------------------------
	SELECT    @projectName = [name]
	FROM [dbo].[catalogProjects]
	WHERE [id] = @projectID 

	IF @projectName IS NULL
		begin
			SET @errMessage=N'The value specified for Project ID is not valid.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		end

	-----------------------------------------------------------------------------------------------------
	SELECT  @instanceName = [name]
	FROM	[dbo].[catalogInstanceNames]
	WHERE	[project_id] = @projectID 
			AND [id] = @instanceID

	-----------------------------------------------------------------------------------------------------
	SET @errMessage='Create HTML report file storage folder: [' + @projectName + '][' + CAST(@projectID AS VARCHAR) + ']' + CASE WHEN @instanceName IS NOT NULL 
																																 THEN ' - [' + @instanceName + '][' + CAST(@instanceID AS VARCHAR) + ']'
																																 ELSE ''
																															END
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

	-----------------------------------------------------------------------------------------------------
	SELECT	@localStoragePath=[value] 
	FROM	[dbo].[appConfigurations] 
	WHERE	[name] = 'Local storage path for HTML reports'
			AND [module] = 'common'

	IF @localStoragePath IS NULL
		begin
			SET @errMessage=N'"Local storage path for HTML reports" configuration is not defined in dbo.appConfigurations table.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		end

	SET @relativeStoragePath = N''
	
	-----------------------------------------------------------------------------------------------------
	--default path\ProjectName\InstanceName\Year - MonthNo. MonthName\
	-----------------------------------------------------------------------------------------------------
	SET @projectName = ISNULL(@projectName, 'DEFAULT')
	SET @projectName = REPLACE(@projectName, '.', '')

	--SET @relativeStoragePath = @relativeStoragePath + CASE WHEN @projectName IS NOT NULL THEN @projectName + '\' ELSE '' END

	IF @instanceName IS NOT NULL
		begin
			SET @instanceName = REPLACE(@instanceName, '\', '$')
			SET @relativeStoragePath = @relativeStoragePath + @instanceName + '\'
		end

	SET @relativeStoragePath = @relativeStoragePath + CAST(DATEPART(YEAR, ISNULL(@StopDate, GETUTCDATE())) AS [nvarchar]) + ' - ' + 
														CAST(DATEPART(M, ISNULL(@StopDate, GETUTCDATE())) AS [nvarchar]) + '. ' + 
														DATENAME(M, ISNULL(@StopDate, GETUTCDATE())) + '\'

	SET @localStoragePath = REPLACE(@localStoragePath, ' ', '_')		
	SET @localStoragePath = @localStoragePath + CASE WHEN RIGHT(@localStoragePath, 1) <> '\' THEN N'\' ELSE N'' END
	SET @localStoragePath = @localStoragePath + @relativeStoragePath
	
	SET @localStoragePath = [dbo].[ufn_formatPlatformSpecificPath](@@SERVERNAME, @localStoragePath)

	IF @flgCreateOutputFolder=1	
		begin
			SET @queryToRun = N'EXEC ' + [dbo].[ufn_getObjectQuoteName](DB_NAME(), 'quoted') + '.[dbo].[usp_createFolderOnDisk]	@sqlServerName	= ''' + @@SERVERNAME + N''',
																						@folderName		= ''' + @localStoragePath + N''',
																						@executionLevel	= 1,
																						@debugMode		= ' + CAST(@debugMode AS [nvarchar]) 

			EXEC  [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @@SERVERNAME,
												@dbName			= NULL,
												@module			= 'dbo.usp_reportHTMLGetStorageFolder',
												@eventName		= 'create folder on disk',
												@queryToRun  	= @queryToRun,
												@flgOptions		= 32,
												@executionLevel	= 1,
												@debugMode		= @debugMode
		end
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
	SET @ReturnValue = -1

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

