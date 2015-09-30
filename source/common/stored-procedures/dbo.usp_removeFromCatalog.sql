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
		, @errMessage			[nvarchar](max)

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
		FROM dbo.logServerAnalysisMessages lsam
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = lsam.[project_id] AND cin.[id] = lsam.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE sosel
		FROM dbo.statsOSEventLogs sosel
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sosel.[project_id] AND cin.[id] = sosel.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE sseld
		FROM dbo.statsSQLServerErrorlogDetails sseld
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sseld.[project_id] AND cin.[id] = sseld.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE ssajh
		FROM dbo.statsSQLServerAgentJobsHistory ssajh
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = ssajh.[project_id] AND cin.[id] = ssajh.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE shcdsi
		FROM dbo.statsHealthCheckDiskSpaceInfo shcdsi
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = shcdsi.[project_id] AND cin.[id] = shcdsi.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[id] = @instanceID

		-----------------------------------------------------------------------------------------------------
		DELETE shcdd
		FROM dbo.statsHealthCheckDatabaseDetails shcdd
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = shcdd.[instance_id] AND cdb.[id] = shcdd.[catalog_database_id]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
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

