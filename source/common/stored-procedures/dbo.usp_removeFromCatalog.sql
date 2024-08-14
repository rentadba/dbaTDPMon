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
		@projectCode		[varchar](32) = NULL,
		@sqlServerName		[sysname] = NULL,
		@databaseNameFilter	[sysname] = '%',
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
	--get default projectCode
	IF @projectCode IS NULL
		SET @projectCode = [dbo].[ufn_getProjectCode](@sqlServerName, NULL)

	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'ERROR: The value specifief for Project Code is not valid.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		end

	IF @sqlServerName IS NOT NULL
		begin
			SELECT   @instanceID = [id]
				   , @machineID = [machine_id]
			FROM [dbo].[catalogInstanceNames]
			WHERE [project_id] = @projectID
				AND [name] = @sqlServerName

			IF @instanceID IS NULL
				begin
					SET @errMessage=N'The value specifief for SQL Server Instance Name is not valid.'
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
				end
		end

	BEGIN TRANSACTION
		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionHistory jeq
		WHERE jeq.[project_id] = @projectID
				AND jeq.[instance_id] = @instanceID
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND jeq.[database_name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionHistory jeq
		WHERE jeq.[project_id] = @projectID
				AND jeq.[for_instance_id] = @instanceID
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND jeq.[database_name] LIKE @databaseNameFilter)
					)
	
		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionQueue jeq
		WHERE jeq.[project_id] = @projectID
				AND jeq.[instance_id] = @instanceID
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND jeq.[database_name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE jeq
		FROM dbo.jobExecutionQueue jeq
		WHERE jeq.[project_id] = @projectID
				AND jeq.[for_instance_id] = @instanceID
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND jeq.[database_name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE jesh
		FROM dbo.jobExecutionStatisticsHistory jesh
		WHERE jesh.[project_id] = @projectID
				AND jesh.[instance_id] = @instanceID
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE lem
		FROM dbo.logEventMessages lem
		WHERE lem.[project_id] = @projectID
				AND lem.[instance_id] = @instanceID
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND lem.[database_name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE lsam
		FROM dbo.logAnalysisMessages lsam
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = lsam.[project_id] AND cin.[id] = lsam.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE sosel
		FROM [health-check].statsOSEventLogs sosel
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sosel.[project_id] AND cin.[id] = sosel.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE sseld
		FROM [health-check].statsErrorlogDetails sseld
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sseld.[project_id] AND cin.[id] = sseld.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE ssajh
		FROM [health-check].statsSQLAgentJobsHistory ssajh
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = ssajh.[project_id] AND cin.[id] = ssajh.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE shcdsi
		FROM [health-check].statsDiskSpaceInfo shcdsi
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = shcdsi.[project_id] AND cin.[id] = shcdsi.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE shcdd
		FROM [health-check].statsDatabaseDetails shcdd
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = shcdd.[instance_id] AND cdb.[id] = shcdd.[catalog_database_id]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND cdb.[name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE sduh
		FROM [health-check].statsDatabaseUsageHistory sduh
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = sduh.[instance_id] AND cdb.[id] = sduh.[catalog_database_id]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND cdb.[name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE sdaod
		FROM [health-check].[statsDatabaseAlwaysOnDetails] sdaod
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = sdaod.[instance_id] AND cdb.[id] = sdaod.[catalog_database_id]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND cdb.[name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE sdg
		FROM [health-check].[statsDatabaseGrowth] sdg
		INNER JOIN dbo.catalogDatabaseNames cdb ON cdb.[instance_id] = sdg.[instance_id] AND cdb.[name] = sdg.[database_name]
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND cdb.[name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE aar
		FROM [monitoring].[alertAdditionalRecipients] aar
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = aar.[project_id] AND cin.[id] = aar.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName	
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		DELETE srl
		FROM [monitoring].[statsReplicationLatency] srl
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = srl.[project_id] AND cin.[name] = srl.[publisher_server]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName	
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND srl.[publisher_db] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE ssaj
		FROM [monitoring].statsSQLAgentJobs ssaj
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = ssaj.[project_id] AND cin.[id] = ssaj.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName	
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		DELETE sts
		FROM [monitoring].statsTransactionsStatus sts
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = sts.[project_id] AND cin.[id] = sts.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName	
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND sts.[database_name] LIKE @databaseNameFilter)
					)				
			
		-----------------------------------------------------------------------------------------------------
		DELETE hc
		FROM [report].htmlContent hc
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = hc.[project_id] AND cin.[id] = hc.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName

		-----------------------------------------------------------------------------------------------------
		DELETE cdn
		FROM  dbo.catalogDatabaseNames cdn
		INNER JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = cdn.[project_id] AND cin.[id] = cdn.[instance_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] = @sqlServerName
				AND (	@databaseNameFilter IS NULL
					 OR @databaseNameFilter = '%'
					 OR (@databaseNameFilter IS NOT NULL AND @databaseNameFilter <> '%' AND cdn.[name] LIKE @databaseNameFilter)
					)

		-----------------------------------------------------------------------------------------------------
		DELETE FROM  dbo.catalogInstanceNames
		WHERE [project_id] = @projectID
				AND [name] = @sqlServerName
				AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		IF NOT EXISTS(	SELECT * FROM dbo.catalogInstanceNames
						WHERE [project_id] = @projectID
							AND [machine_id] = @machineID
							AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)
					)
			DELETE cmn 
			FROM  dbo.catalogMachineNames cmn
			WHERE cmn.[project_id] = @projectID
					AND cmn.[id] = @machineID
					AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)

		-----------------------------------------------------------------------------------------------------
		IF @sqlServerName IS NULL
				DELETE cmn 
				FROM  dbo.catalogMachineNames cmn
				LEFT JOIN dbo.catalogInstanceNames cin ON cin.[project_id] = cmn.[project_id] AND cin.[machine_id] = cmn.[id]
				WHERE cmn.[project_id] = @projectID	
						AND cin.[id] IS NULL

		-----------------------------------------------------------------------------------------------------
		IF NOT EXISTS(	SELECT [id] FROM dbo.catalogMachineNames
						WHERE [project_id] = @projectID
						UNION ALL
						SELECT [id] FROM dbo.catalogInstanceNames
						WHERE [project_id] = @projectID
					)
			AND (@databaseNameFilter = '%' OR @databaseNameFilter IS NULL)
			begin
				DELETE hc
				FROM [report].[htmlContent] hc
				WHERE [project_id] = @projectID

				DELETE isch
				FROM [maintenance-plan].[internalScheduler] isch
				WHERE [project_id] = @projectID
						
				DELETE cp
				FROM [dbo].[catalogProjects] cp
				WHERE [id] = @projectID
			end
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
