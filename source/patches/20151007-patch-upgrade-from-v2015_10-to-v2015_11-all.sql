USE [dbaTDPMon]
GO

IF NOT EXISTS(SELECT * FROM [dbo].[appConfigurations] WHERE [name] = 'Collect SQL Errorlog last files')
	INSERT	INTO [dbo].[appConfigurations] ([module], [name], [value])
		  SELECT 'health-check'		AS [module], 'Collect SQL Errorlog last files'	AS [name], '1'			AS [value]
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

RAISERROR('Create procedure: [dbo].[usp_refreshMachineCatalogs]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_refreshMachineCatalogs]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_refreshMachineCatalogs]
GO

CREATE PROCEDURE [dbo].[usp_refreshMachineCatalogs]
		@projectCode		[varchar](32),
		@sqlServerName		[sysname],
		@debugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================
-- Change Date: 2015.04.03 / Andrei STEFAN
-- Description: add domain name to machine information
-----------------------------------------------------------------------------------------


SET NOCOUNT ON

DECLARE   @returnValue			[smallint]
		, @errMessage			[nvarchar](4000)
		, @errDescriptor		[nvarchar](256)
		, @errNumber			[int]

DECLARE   @queryToRun			[nvarchar](max)	-- used for dynamic statements
		, @projectID			[smallint]
		, @isClustered			[bit]
		, @isActive				[bit]
		, @instanceID			[smallint]
		, @domainName			[sysname]

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]


-- { sql_statement | statement_block }
BEGIN TRY
	SET @returnValue=1

	-----------------------------------------------------------------------------------------------------
	SET @errMessage=N'--Getting Instance information: [' + @sqlServerName + '] / project: [' + @projectCode + ']'
	RAISERROR(@errMessage, 10, 1) WITH NOWAIT
	SET @errMessage=N''
	-----------------------------------------------------------------------------------------------------

	-----------------------------------------------------------------------------------------------------
	--check that SQLServerName is defined as local or as a linked server to current sql server instance
	-----------------------------------------------------------------------------------------------------
	IF (SELECT count(*) FROM sys.sysservers WHERE srvname=@sqlServerName)=0
		begin
			PRINT N'Specified instance name is not defined as local or linked server: ' + @sqlServerName
			PRINT N'Create a new linked server.'

			/* create a linked server for the instance found */
			EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
		end


	-----------------------------------------------------------------------------------------------------
	IF object_id('#serverPropertyConfig') IS NOT NULL DROP TABLE #serverPropertyConfig
	CREATE TABLE #serverPropertyConfig
			(
				[value]			[sysname]	NULL
			)

	-----------------------------------------------------------------------------------------------------
	IF object_id('tempdb..#xpCMDShellOutput') IS NOT NULL 
	DROP TABLE #xpCMDShellOutput

	CREATE TABLE #xpCMDShellOutput
	(
		[output]	[nvarchar](max)			NULL
	)
			
	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogMachineNames') IS NOT NULL 
	DROP TABLE #catalogMachineNames

	CREATE TABLE #catalogMachineNames
	(
		[name]					[sysname]		NULL,
		[domain]				[sysname]		NULL
	)

	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogInstanceNames') IS NOT NULL 
	DROP TABLE #catalogInstanceNames

	CREATE TABLE #catalogInstanceNames
	(
		[name]					[sysname]		NULL,
		[version]				[sysname]		NULL,
		[edition]				[varchar](256)	NULL,
		[machine_name]			[sysname]		NULL
	)

	-----------------------------------------------------------------------------------------------------
	IF object_id('#catalogDatabaseNames') IS NOT NULL 
	DROP TABLE #catalogDatabaseNames

	CREATE TABLE #catalogDatabaseNames
	(
		[database_id]			[int]			NULL,
		[name]					[sysname]		NULL,
		[state]					[int]			NULL,
		[state_desc]			[nvarchar](64)	NULL
	)

	-----------------------------------------------------------------------------------------------------
	SELECT @projectID = [id]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'The value specifief for Project Code is not valid.'
			RAISERROR(@errMessage, 16, 1) WITH NOWAIT
		end

	-----------------------------------------------------------------------------------------------------
	--check if the connection to machine can be made & discover instance name
	-----------------------------------------------------------------------------------------------------
	SET @queryToRun = N'SELECT    @@SERVERNAME
								, [product_version]
								, [edition]
								, [machine_name]
						FROM (
								SELECT CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]) AS [product_version]
									 , SUBSTRING(@@VERSION, 1, CHARINDEX(CAST(SERVERPROPERTY(''ProductVersion'') AS [sysname]), @@VERSION)-1) + CAST(SERVERPROPERTY(''Edition'') AS [sysname]) AS [edition]
									 , CAST(SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [sysname]) AS [machine_name]
							 )X'
	SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
	IF @debugMode = 1 PRINT @queryToRun

	BEGIN TRY
		INSERT	INTO #catalogInstanceNames([name], [version], [edition], [machine_name])
				EXEC (@queryToRun)
		SET @isActive=1
	END TRY
	BEGIN CATCH
		SET @errMessage=ERROR_MESSAGE()
		SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs - Offline'
		RAISERROR(@errMessage, 10, 1) WITH NOWAIT

		SET @isActive=0
	END CATCH
	

	IF @isActive=0
		begin
			INSERT	INTO #catalogMachineNames([name])
					SELECT cmn.[name]
					FROM [dbo].[catalogMachineNames] cmn
					INNER JOIN [dbo].[catalogInstanceNames] cin ON cmn.[id] = cin.[machine_id] AND cmn.[project_id] = cin.[project_id]
					WHERE cin.[project_id] = @projectID
							AND cin.[name] = @sqlServerName
			
			IF @@ROWCOUNT=0				
				INSERT	INTO #catalogMachineNames([name])					
						SELECT SUBSTRING(@sqlServerName, 1, CASE WHEN CHARINDEX('\', @sqlServerName) > 0 THEN CHARINDEX('\', @sqlServerName)-1 ELSE LEN(@sqlServerName) END)
			
			INSERT	INTO #catalogInstanceNames([name], [version])
					SELECT @sqlServerName, NULL
			
			SET @isClustered = 0
		end
	ELSE
		begin
			DECLARE @SQLMajorVersion [int]

			BEGIN TRY
				SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL([version], ''), 2), '.', '') 
				FROM #catalogInstanceNames
			END TRY
			BEGIN CATCH
				SET @SQLMajorVersion = 8
			END CATCH

			-----------------------------------------------------------------------------------------------------
			--discover machine names (if clustered instance is present, get all cluster nodes)
			-----------------------------------------------------------------------------------------------------
			SET @isClustered=0

			IF @SQLMajorVersion<=8
				SET @queryToRun = N'SELECT [NodeName] FROM ::fn_virtualservernodes()'
			ELSE
				SET @queryToRun = N'SELECT [NodeName] FROM sys.dm_os_cluster_nodes'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun
			
			BEGIN TRY
				INSERT	INTO #catalogMachineNames([name])
						EXEC (@queryToRun)		
			END TRY
			BEGIN CATCH
				IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()
			END CATCH
	
			IF (SELECT COUNT(*) FROM #catalogMachineNames)=0
				begin
					SET @queryToRun = N'SELECT CASE WHEN [computer_name] IS NOT NULL 
													THEN [computer_name]
													ELSE [machine_name]
											  END
										FROM (
												SELECT CAST(SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [sysname]) AS [computer_name]
											)X,
											(
												SELECT CAST(SERVERPROPERTY(''MachineName'') AS [sysname]) AS [machine_name]
											)Y'
					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 PRINT @queryToRun

					BEGIN TRY
						INSERT	INTO #catalogMachineNames([name])
								EXEC (@queryToRun)
					END TRY
					BEGIN CATCH
						SET @errMessage=ERROR_MESSAGE()
						SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
						RAISERROR(@errMessage, 16, 1) WITH NOWAIT
					END CATCH
				end
			ELSE
				begin
					SET @isClustered = 1
				end
				
			
			-----------------------------------------------------------------------------------------------------
			--discover database names
			-----------------------------------------------------------------------------------------------------
			IF @SQLMajorVersion<=8
				SET @queryToRun = N'SELECT sdb.[dbid], sdb.[name], sdb.[status] AS [state]
											, CASE  WHEN sdb.[status] & 4194584 = 4194584 THEN ''SUSPECT''
													WHEN sdb.[status] & 2097152 = 2097152 THEN ''STANDBY''
													WHEN sdb.[status] & 32768 = 32768 THEN ''EMERGENCY MODE''
													WHEN sdb.[status] & 4096 = 4096 THEN ''SINGLE USER''
													WHEN sdb.[status] & 2048 = 2048 THEN ''DBO USE ONLY''
													WHEN sdb.[status] & 1024 = 1024 THEN ''READ ONLY''
													WHEN sdb.[status] & 512 = 512 THEN ''OFFLINE''
													WHEN sdb.[status] & 256 = 256 THEN ''NOT RECOVERED''
													WHEN sdb.[status] & 128 = 128 THEN ''RECOVERING''
													WHEN sdb.[status] & 64 = 64 THEN ''PRE RECOVERY''
													WHEN sdb.[status] & 32 = 32 THEN ''LOADING''
													WHEN sdb.[status] = 0 THEN ''UNKNOWN''
													ELSE ''ONLINE''
												END AS [state_desc]
									FROM master.dbo.sysdatabases sdb'
			ELSE
				SET @queryToRun = N'SELECT sdb.[database_id], sdb.[name], sdb.[state], sdb.[state_desc]
									FROM sys.databases sdb
									WHERE [is_read_only] = 0 AND [is_in_standby] = 0
									UNION ALL
									SELECT sdb.[database_id], sdb.[name], sdb.[state], ''READ ONLY''
									FROM sys.databases sdb
									WHERE [is_read_only] = 1 AND [is_in_standby] = 0
									UNION ALL
									SELECT sdb.[database_id], sdb.[name], sdb.[state], ''STANDBY''
									FROM sys.databases sdb
									WHERE [is_in_standby] = 1'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 PRINT @queryToRun

			BEGIN TRY
				INSERT	INTO #catalogDatabaseNames([database_id], [name], [state], [state_desc])
						EXEC (@queryToRun)		
			END TRY
			BEGIN CATCH
				SET @errMessage=ERROR_MESSAGE()
				SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
				RAISERROR(@errMessage, 16, 1) WITH NOWAIT
			END CATCH

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			/* check if xp_cmdshell is enabled or should be enabled																			 */
			BEGIN TRY
				IF @SQLMajorVersion>8
					begin
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
																			@debugMode			= @debugMode

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
																					@debugMode			= @debugMode

								IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
									EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																						@configOptionName	= 'xp_cmdshell',
																						@configOptionValue	= 1,
																						@optionIsAvailable	= @optionXPIsAvailable OUT,
																						@optionCurrentValue	= @optionXPValue OUT,
																						@optionHasChanged	= @optionXPHasChanged OUT,
																						@executionLevel		= 0,
																						@debugMode			= @debugMode
							end
					end

				IF @optionXPValue=1 OR @SQLMajorVersion=8
					begin
						--run wmi to get the domain name
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic computersystem get Domain''; EXEC xp_cmdshell @cmdQuery;'
			
						IF @sqlServerName<>@@SERVERNAME
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
						IF @debugMode = 1 PRINT @queryToRun

						INSERT	INTO #xpCMDShellOutput([output])
								EXEC (@queryToRun)
									
						UPDATE #xpCMDShellOutput SET [output]=REPLACE(REPLACE(REPLACE(LTRIM(RTRIM([output])), ' ', ''), CHAR(10), ''), CHAR(13), '')
			
						DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3 OR [output] IS NULL
						DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%not recognized as an internal or external command%'
						DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%operable program or batch file%'
						DELETE TOP (1) FROM #xpCMDShellOutput WHERE SUBSTRING([output], 1, 8)='Domain'
			
						SELECT TOP 1 @domainName = LOWER([output])
						FROM #xpCMDShellOutput

						UPDATE #catalogMachineNames SET [domain] = @domainName
					end

				IF @SQLMajorVersion>8 AND (@optionXPHasChanged=1 OR @optionAdvancedHasChanged=1)
					begin
						/* disable xp_cmdshell configuration option */
						IF @optionXPHasChanged = 1
							EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																				@configOptionName	= 'xp_cmdshell',
																				@configOptionValue	= 0,
																				@optionIsAvailable	= @optionXPIsAvailable OUT,
																				@optionCurrentValue	= @optionXPValue OUT,
																				@optionHasChanged	= @optionXPHasChanged OUT,
																				@executionLevel		= 0,
																				@debugMode			= @debugMode

						/* disable show advanced options configuration option */
						IF @optionAdvancedHasChanged = 1
								EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																					@configOptionName	= 'show advanced options',
																					@configOptionValue	= 0,
																					@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																					@optionCurrentValue	= @optionAdvancedValue OUT,
																					@optionHasChanged	= @optionAdvancedHasChanged OUT,
																					@executionLevel		= 0,
																					@debugMode			= @debugMode
					end
			END TRY
			BEGIN CATCH
				PRINT ERROR_MESSAGE()
			END CATCH
		end


	-----------------------------------------------------------------------------------------------------
	--upsert catalog tables
	-----------------------------------------------------------------------------------------------------														
	MERGE INTO [dbo].[catalogMachineNames] AS dest
	USING (	
			SELECT [name], [domain]
			FROM #catalogMachineNames
		  ) AS src([name], [domain])
		ON dest.[name] = src.[name] AND dest.[project_id] = @projectID
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([project_id], [name], [domain]) 
		VALUES (@projectID, src.[name], src.[domain]) 
	WHEN MATCHED THEN
		UPDATE SET dest.[domain]=src.[domain];


	MERGE INTO [dbo].[catalogInstanceNames] AS dest
	USING (	
			SELECT  cmn.[id]	  AS [machine_id]
				  , cin.[name]	  AS [name]
				  , cin.[version]
				  , cin.[edition]
				  , @isClustered  AS [is_clustered]
				  , @isActive	  AS [active]
				  , cmnA.[id]	  AS [cluster_node_machine_id]
			FROM #catalogInstanceNames cin
			INNER JOIN #catalogMachineNames src ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON		cmn.[name] = src.[name] 
															AND cmn.[project_id]=@projectID
			LEFT  JOIN [dbo].[catalogMachineNames] cmnA ON		cmnA.[name] = cin.[machine_name] 
															AND cmnA.[project_id]=@projectID 
															AND @isClustered=1
		  ) AS src([machine_id], [name], [version], [edition], [is_clustered], [active], [cluster_node_machine_id])
		ON dest.[machine_id] = src.[machine_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([machine_id], [project_id], [name], [version], [edition], [is_clustered], [active], [cluster_node_machine_id], [last_refresh_date_utc]) 
		VALUES (src.[machine_id], @projectID, src.[name], src.[version], src.[edition], src.[is_clustered]
				, CASE WHEN src.[is_clustered]=1
						THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
									THEN 1 
									ELSE 0
							 END
						ELSE src.[active]
				 END
				, src.[cluster_node_machine_id]
				, GETUTCDATE())
	WHEN MATCHED THEN
		UPDATE SET    dest.[is_clustered] = src.[is_clustered]
					, dest.[version] = src.[version]
					, dest.[active] = CASE WHEN src.[is_clustered]=1
											THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
														THEN 1 
														ELSE 0
												 END
											ELSE src.[active]
										END
					, dest.[edition] = src.[edition]
					, dest.[cluster_node_machine_id] = src.[cluster_node_machine_id]
					, dest.[last_refresh_date_utc] = GETUTCDATE();

	UPDATE cdn
		SET cdn.[active] = 0
	FROM [dbo].[catalogDatabaseNames] cdn
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
	INNER JOIN #catalogInstanceNames	srcIN ON cin.[name] = srcIN.[name]
	WHERE cin.[project_id] = @projectID

	MERGE INTO [dbo].[catalogDatabaseNames] AS dest
	USING (	
			SELECT  cin.[id] AS [instance_id]
				  , src.[name]
				  , src.[database_id]
				  , src.[state]
				  , src.[state_desc]
			FROM  #catalogDatabaseNames src
			INNER JOIN #catalogMachineNames srcMn ON 1=1
			INNER JOIN #catalogInstanceNames srcIN ON 1=1
			INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
			INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]
		  ) AS src([instance_id], [name], [database_id], [state], [state_desc])
		ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([instance_id], [project_id], [database_id], [name], [state], [state_desc], [active])
		VALUES (src.[instance_id], @projectID, src.[database_id], src.[name], src.[state], src.[state_desc], 1)
	WHEN MATCHED THEN
		UPDATE SET	dest.[database_id] = src.[database_id]
				  , dest.[state] = src.[state]
				  , dest.[state_desc] = src.[state_desc]
				  , dest.[active] = 1;

	SELECT TOP 1 @instanceID = cin.[id]
	FROM  #catalogMachineNames srcMn
	INNER JOIN #catalogInstanceNames srcIN ON 1=1
	INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]

	IF @errMessage IS NOT NULL AND @errMessage<>''
		INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
				SELECT  @instanceID
					  , @projectID
					  , GETUTCDATE()
					  , @errDescriptor
					  , @errMessage

	RETURN @instanceID
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

RETURN @returnValue



RAISERROR('Create procedure: [dbo].[usp_refreshProjectCatalogsAndDiscovery]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_refreshProjectCatalogsAndDiscovery]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_refreshProjectCatalogsAndDiscovery]
GO

CREATE PROCEDURE [dbo].[usp_refreshProjectCatalogsAndDiscovery]
		@projectCode		[varchar](32),
		@runDiscovery		[bit]=0,	/* using sqlcmd -L*/
		@enableXPCMDSHELL	[bit]=1,
		@debugMode			[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 09.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

DECLARE   @queryToRun				[nvarchar](1024)
		, @sqlServerName			[sysname]
		, @existingServerID			[int]
		, @projectID				[smallint]
		, @instanceID				[smallint]
		, @errMessage				[nvarchar](4000)
		, @errorCode				[int]

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#xp_cmdshell') IS NOT NULL DROP TABLE #xp_cmdshell

CREATE TABLE #xp_cmdshell
(
	[output]		[nvarchar](max)		NULL,
	[instance_name]	[sysname]			NULL,
	[machine_name]	[sysname]			NULL
)


-----------------------------------------------------------------------------------------------------
SELECT @projectID = [id]
FROM [dbo].[catalogProjects]
WHERE [code] = @projectCode 

IF @projectID IS NULL
	begin
		SET @errMessage=N'The value specifief for Project Code is not valid.'
		RAISERROR(@errMessage, 16, 1) WITH NOWAIT
	end

IF @runDiscovery=1
	begin		
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* check if xp_cmdshell is enabled or should be enabled																			 */
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
															@debugMode			= @debugMode

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
																	@debugMode			= @debugMode

				IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
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
				RAISERROR('xp_cmdshell component is turned off. Cannot continue', 16, 1) WITH NOWAIT
				return
			end		


		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* perform discovery																											 */
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		RAISERROR('Performing SQL Server instance discovery...', 10, 1) WITH NOWAIT

		SET @queryToRun='sqlcmd -L'
		INSERT	INTO #xp_cmdshell([output])
				EXEC xp_cmdshell @queryToRun

		UPDATE #xp_cmdshell SET [output]=LTRIM(RTRIM([output]))
		DELETE FROM #xp_cmdshell where [output] LIKE 'NULL%' OR [output] LIKE 'Servers:%' OR [output] IS NULL
		DELETE FROM #xp_cmdshell WHERE LEN([output])<=1

		UPDATE #xp_cmdshell 
			SET   [instance_name] = [output]
				, [machine_name] = CASE WHEN CHARINDEX('\', [output])>0 THEN SUBSTRING([output], 1, CHARINDEX('\', [output])-1) ELSE [output] END

		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* disable xp_cmdshell configuration option */
		IF @optionXPHasChanged = 1
			EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																@configOptionName	= 'xp_cmdshell',
																@configOptionValue	= 0,
																@optionIsAvailable	= @optionXPIsAvailable OUT,
																@optionCurrentValue	= @optionXPValue OUT,
																@optionHasChanged	= @optionXPHasChanged OUT,
																@executionLevel		= 0,
																@debugMode			= @debugMode

		/* disable show advanced options configuration option */
		IF @optionAdvancedHasChanged = 1
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																	@configOptionName	= 'show advanced options',
																	@configOptionValue	= 0,
																	@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																	@optionCurrentValue	= @optionAdvancedValue OUT,
																	@optionHasChanged	= @optionAdvancedHasChanged OUT,
																	@executionLevel		= 0,
																	@debugMode			= @debugMode


		/*-------------------------------------------------------------------------------------------------------------------------------*/
		/* catalog discovered servers																									 */
		/*-------------------------------------------------------------------------------------------------------------------------------*/
		DECLARE crsDiscoveredServer CURSOR READ_ONLY FOR	SELECT xp.[instance_name], ss.[server_id]
															FROM #xp_cmdshell xp
															LEFT  JOIN
																(
																	SELECT    cin.[name] AS [instance_name]
																			, cmn.[name] AS [machine_name]
																	FROM [dbo].[catalogInstanceNames]		cin 	
																	INNER JOIN [dbo].[catalogMachineNames]  cmn ON	cmn.[id] = cin.[machine_id]
																												AND cmn.[project_id] = cin.[project_id]
																	INNER JOIN [dbo].[catalogProjects]		cp	ON	cp.[id] = cin.[project_id] 
																	WHERE cp.[code] = @projectCode
																)cat ON	cat.[instance_name] = xp.[instance_name] 
																		OR cat.[machine_name] = xp.[instance_name]
																		OR cat.[machine_name] = xp.[machine_name]
															LEFT  JOIN sys.servers					ss	ON	ss.[name] = xp.[instance_name]
															WHERE cat.[instance_name] IS NULL AND cat.[machine_name] IS NULL
		OPEN crsDiscoveredServer
		FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
		WHILE @@FETCH_STATUS=0
			begin
				SET @errMessage = 'New SQL Server Instance found: [' + @sqlServerName + ']'
				RAISERROR(@errMessage, 10, 1) WITH NOWAIT
		
				IF @existingServerID IS NULL
					begin
						/* create a linked server for the instance found */
						EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
					end
					
				/* catalog the instance */
				EXEC @instanceID = [dbo].[usp_refreshMachineCatalogs] 	@projectCode	= @projectCode,
																		@sqlServerName	= @sqlServerName,
																		@debugMode		= @debugMode


				INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
						SELECT  @instanceID
							  , @projectID
							  , GETUTCDATE()
							  , 'dbo.usp_refreshProjectCatalogsAndDiscovery'
							  , @errMessage
					  												
				FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
			end
		CLOSE crsDiscoveredServer
		DEALLOCATE crsDiscoveredServer
	end

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* check status / update catalog for previous discovered serverd																 */
/*-------------------------------------------------------------------------------------------------------------------------------*/
DECLARE crsDiscoveredServer CURSOR READ_ONLY FOR	SELECT cin.[name], ss.[server_id]
													FROM [dbo].[catalogInstanceNames] cin 
													INNER JOIN [dbo].[catalogProjects]		cp	ON	cp.[id] = cin.[project_id] 
													INNER JOIN [dbo].[catalogMachineNames]  cmn ON	cmn.[id] = cin.[machine_id] 
																									AND cmn.[project_id] = cin.[project_id]
													LEFT  JOIN #xp_cmdshell					xp  ON	cin.[name] = xp.[output] 
																									OR cmn.[name] = xp.[output] 
													LEFT  JOIN sys.servers					ss	ON	ss.[name] = cin.[name]
													WHERE	cp.[code] = @projectCode
															AND xp.[output] IS NULL
													ORDER BY cin.[name]
OPEN crsDiscoveredServer
FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
WHILE @@FETCH_STATUS=0
	begin
		IF @existingServerID IS NULL
			begin
				/* create a linked server for the instance found */
				EXEC [dbo].[usp_addLinkedSQLServer] @ServerName = @sqlServerName
			end
					
		/* update instance information */
		EXEC [dbo].[usp_refreshMachineCatalogs] 	@projectCode	= @projectCode,
													@sqlServerName	= @sqlServerName,
													@debugMode		= @debugMode
												
		FETCH NEXT FROM crsDiscoveredServer INTO @sqlServerName, @existingServerID
	end
CLOSE crsDiscoveredServer
DEALLOCATE crsDiscoveredServer
/*-------------------------------------------------------------------------------------------------------------------------------*/
GO


RAISERROR('Create procedure: [dbo].[usp_sqlAgentJobCheckStatus]', 10, 1) WITH NOWAIT
GO
SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_sqlAgentJobCheckStatus]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_sqlAgentJobCheckStatus]
GO

CREATE PROCEDURE dbo.usp_sqlAgentJobCheckStatus
		@sqlServerName			[sysname],
		@jobName				[varchar](255),
		@strMessage				[varchar](8000)=''	OUTPUT,	
		@currentRunning			[int]=0 			OUTPUT,			
		@lastExecutionStatus	[int]=0 			OUTPUT,			
		@lastExecutionDate		[varchar](10)=''	OUTPUT,		
		@lastExecutionTime 		[varchar](8)=''		OUTPUT,	
		@runningTimeSec			[bigint]=0			OUTPUT,
		@selectResult			[bit]=0,
		@extentedStepDetails	[bit]=0,		
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
DECLARE 	@Message 			[varchar](8000), 
			@StepName			[varchar](255),
			@JobID				[varchar](255),
			@StepID				[int],
			@JobSessionID		[int],
			@RunDate			[varchar](10),
			@RunDateDetail		[varchar](10),
			@RunTime			[varchar](8),
			@RunTimeDetail		[varchar](8),
			@RunDuration		[varchar](8),
			@RunDurationDetail	[varchar](8),
			@RunStatus			[varchar](32),
			@RunStatusDetail	[varchar](32),
			@RunDurationLast	[varchar](8),
			@EventTime			[datetime],		
			@ReturnValue		[int],
			@queryToRun			[nvarchar](4000)

SET NOCOUNT ON

IF OBJECT_ID('tempdb..#tmpCheck') IS NOT NULL DROP TABLE #tmpCheck
CREATE TABLE #tmpCheck (Result varchar(1024))

IF ISNULL(@sqlServerName, '')=''
	begin
		SET @queryToRun=N'--	ERROR: The specified value for SOURCE server is not valid.'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

IF LEN(@jobName)=0 OR ISNULL(@jobName, '')=''
	begin
		RAISERROR('--ERROR: Must specify a job name.', 10, 1) WITH NOWAIT
		RETURN 1
	end

SET @queryToRun=N'SELECT [srvid] FROM master.dbo.sysservers WHERE [srvname]=''' + @sqlServerName + ''''
TRUNCATE TABLE #tmpCheck
INSERT INTO #tmpCheck EXEC (@queryToRun)
IF (SELECT count(*) FROM #tmpCheck)=0
	begin
		SET @queryToRun=N'--	ERROR: SOURCE server [' + @sqlServerName + '] is not defined as linked server on THIS server [' + @sqlServerName + '].'
		RAISERROR(@queryToRun, 16, 1) WITH NOWAIT
		RETURN 1
	end

---------------------------------------------------------------------------------------------
SET	@strMessage			= NULL
SET	@currentRunning		= NULL
SET	@lastExecutionStatus= NULL
SET	@lastExecutionDate	= NULL
SET	@lastExecutionTime 	= NULL
SET	@runningTimeSec		= NULL


---------------------------------------------------------------------------------------------
SET @ReturnValue	= 5 --Unknown

SET @queryToRun=N'SELECT  CAST([job_id] AS [varchar](255)) AS [job_id] FROM [msdb].[dbo].[sysjobs] WHERE [name]=''' + @jobName + ''''
SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
IF @debugMode = 1 PRINT @queryToRun

TRUNCATE TABLE #tmpCheck
INSERT INTO #tmpCheck EXEC (@queryToRun)
------------------------------------------------------------------------------------------------------------------------------------------
IF (SELECT COUNT(*) FROM #tmpCheck)=0
	begin
		SET @strMessage='--SQL Server Agent: The specified job name [' + @jobName + '] does not exists on this server [' + @sqlServerName + ']'
		IF @debugMode=1
			RAISERROR(@strMessage, 10, 1) WITH NOWAIT
		SET @currentRunning = 0
		SET @ReturnValue = -5 --Unknown
	end
ELSE
	begin
		SELECT TOP 1 @JobID = [Result] FROM #tmpCheck
			
		IF OBJECT_ID('tempdb..#runningSQLAgentJobsProcess') IS NOT NULL DROP TABLE #runningSQLAgentJobsProcess
		CREATE TABLE #runningSQLAgentJobsProcess
			(
				  [step_id]		[int], 
				  [job_id]		[uniqueidentifier],
				  [session_id]	[int]
			)
		
		--check for active processes started by SQL Agent job
		SET @currentRunning=0
		SET @queryToRun=N'SELECT DISTINCT sp.[step_id], sp.[job_id], sp.[spid]
						FROM (
							  SELECT  [step_id]
									, SUBSTRING([job_id], 7, 2) + SUBSTRING([job_id], 5, 2) + SUBSTRING([job_id], 3, 2) + LEFT([job_id], 2) + ''-'' + SUBSTRING([job_id], 11, 2) + SUBSTRING([job_id], 9, 2) + ''-'' + SUBSTRING([job_id], 15, 2) + SUBSTRING([job_id], 13, 2) + ''-'' + SUBSTRING([job_id], 17, 4) + ''-'' + RIGHT([job_id], 12) AS [job_id] 
									, [spid]
 							  FROM (
									SELECT SUBSTRING([program_name], CHARINDEX('': Step'', [program_name]) + 7, LEN([program_name]) - CHARINDEX('': Step'', [program_name]) - 7) [step_id]
										 , SUBSTRING([program_name], CHARINDEX(''(Job 0x'', [program_name]) + 7, CHARINDEX('' : Step '', [program_name]) - CHARINDEX(''(Job 0x'', [program_name]) - 7) [job_id]
										 , [spid]
			 						FROM [master].[dbo].[sysprocesses] 
									WHERE [program_name] LIKE ''SQLAgent - %JobStep%''
								   ) sp
							) sp
						INNER JOIN [msdb].[dbo].[sysjobs] sj ON sj.[job_id] = sp.[job_id]
						WHERE sj.[name]= ''' + @jobName + N'''
						UNION
						SELECT DISTINCT sjs.[step_id], sj.[job_id], sp.[spid]
						FROM [master].[dbo].[sysprocesses] sp
						INNER JOIN [msdb].[dbo].[sysjobs]		sj  ON sj.[name] = sp.[program_name]
						INNER JOIN [msdb].[dbo].[sysjobsteps]	sjs ON sjs.[job_id] = sj.[job_id]
						INNER JOIN [msdb].[dbo].[sysjobhistory] sjh ON sjh.[job_id] = sj.[job_id] AND sjh.[step_id] = sjs.[step_id] AND sjh.[run_status] = 4
						WHERE sj.[name]= ''' + @jobName + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun
		INSERT	INTO #runningSQLAgentJobsProcess([step_id], [job_id], [session_id])
				EXEC (@queryToRun)

		SET @StepID = NULL
		SET @JobSessionID = NULL

		SELECT @currentRunning = COUNT(*) FROM #runningSQLAgentJobsProcess
		SELECT TOP 1  @StepID = [step_id]
					, @JobID  = CAST([job_id] AS [varchar](255))
					, @JobSessionID = [session_id]
		FROM #runningSQLAgentJobsProcess	

		IF OBJECT_ID('tempdb..#runningSQLAgentJobsProcess') IS NOT NULL DROP TABLE #runningSQLAgentJobsProcess
	
		IF @currentRunning > 0 
			begin
				SET @queryToRun=N'SELECT [step_name] FROM [msdb].[dbo].[sysjobsteps] WHERE [step_id]=' + CAST(@StepID AS [nvarchar]) + ' AND [job_id]=''' + @JobID + ''''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #tmpCheck
				INSERT INTO #tmpCheck EXEC (@queryToRun)
				SELECT TOP 1 @StepName=Result FROM #tmpCheck

				SET @lastExecutionStatus=4 -- in progress
				IF @debugMode=1
					RAISERROR(@strMessage, 10, 1) WITH NOWAIT
				SET @ReturnValue=4

				--get job start date/time
				IF OBJECT_ID('tempdb..#jobStartInfo') IS NOT NULL DROP TABLE #jobStartInfo
				CREATE TABLE #jobStartInfo
					(
						[start_date]	[varchar](16), 
						[start_time]	[varchar](16), 
						[run_status]	[int], 
						[event_time]	[datetime]
					)

				SET @queryToRun=N'SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [start_date]
											, CAST(h.[run_time] AS varchar) AS [start_time]
											, NULL AS [run_status]
											, GETDATE() AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory] h
								WHERE h.[job_id]=''' + @JobID + N''' 
										AND h.[instance_id] > (
																/* last job completion id */
																SELECT TOP 1 h1.[instance_id]
																FROM [msdb].[dbo].[sysjobhistory] h1
																WHERE h1.[job_id]=''' + @JobID + N''' 
																		AND [step_name] =''(Job outcome)''
																ORDER BY h1.[instance_id] DESC
																)
								ORDER BY h.[instance_id] ASC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				INSERT	INTO #jobStartInfo([start_date], [start_time], [run_status], [event_time])
						EXEC (@queryToRun)

				
				IF (SELECT COUNT(*) FROM #jobStartInfo)=0
					begin
						IF @StepID <> 1
							begin
								/* job was cancelled, but process is still running, probably performing a rollback */
								SET @queryToRun=N'SELECT TOP 1 CAST(h.[run_date] AS varchar) AS [start_date]
															, CAST(h.[run_time] AS varchar) AS [start_time]
															, h.[run_status]
															, GETDATE() AS [event_time]
												FROM [msdb].[dbo].[sysjobhistory] h
												WHERE h.[job_id]=''' + @JobID + N''' 
														AND h.[instance_id] = (
																				/* last job completion id */
																				SELECT TOP 1 h1.[instance_id]
																				FROM [msdb].[dbo].[sysjobhistory] h1
																				WHERE h1.[job_id]=''' + @JobID + N''' 
																						AND [step_name] =''(Job outcome)''
																				ORDER BY h1.[instance_id] DESC
																				)
												ORDER BY h.[instance_id] ASC'
							end
						ELSE
							begin
								SET @queryToRun=N'SELECT  REPLACE(SUBSTRING(CONVERT([varchar](19), [login_time], 120), 1, 10), ''-'', '''')  AS [start_date]
														, REPLACE(SUBSTRING(CONVERT([varchar](19), [login_time], 120), 12, 19), '':'', '''') AS [start_time]
														, 4 AS [run_status]
														, GETDATE() AS [event_time]
												FROM [master].[dbo].[sysprocesses]
												WHERE [spid] = ' + CAST(@JobSessionID AS [nvarchar])
							end
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						INSERT	INTO #jobStartInfo([start_date], [start_time], [run_status], [event_time])
								EXEC (@queryToRun)
					end
									
				SET @RunDate	= NULL
				SET @RunTime	= NULL
				SET @EventTime	= NULL
				SELECT TOP 1  @RunDate	 = [start_date]
							, @RunTime	 = [start_time]
							, @RunStatus = CAST(ISNULL([run_status], @lastExecutionStatus) AS [varchar]) 
							, @EventTime = [event_time]
				FROM #jobStartInfo
	

				SET @RunTime = REPLICATE('0', 6 - LEN(@RunTime)) + @RunTime
				SET @RunTime = SUBSTRING(@RunTime, 1, 2) + ':' + SUBSTRING(@RunTime, 3, 2) + ':' + SUBSTRING(@RunTime, 5, 2)
				SET @RunDate = SUBSTRING(@RunDate, 1, 4) + '-' + SUBSTRING(@RunDate, 5, 2) + '-' + SUBSTRING(@RunDate, 7, 2)

				SET @lastExecutionDate = @RunDate
				SET @lastExecutionTime = @RunTime
				SET @runningTimeSec = [dbo].[ufn_getMilisecondsBetweenDates](CONVERT([datetime], @lastExecutionDate + ' ' + @lastExecutionTime, 120), @EventTime) / 1000

				SET @RunStatus = CASE @RunStatus WHEN '0' THEN 'Failed'
												 WHEN '1' THEN 'Succeded'				
												 WHEN '2' THEN 'Retry'
												 WHEN '3' THEN 'Canceled'
												 WHEN '4' THEN 'In progress'
								 END
				
				SET @strMessage=                         '--Job currently running step: [' + CAST(@StepID AS varchar) + '] - [' + @StepName + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--Job started at            : [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
				SET @strMessage=@strMessage + CHAR(13) + '--Execution status          : [' + ISNULL(@RunStatus, '') + ']'	
			end
		ELSE
			begin
				IF OBJECT_ID('tempdb..#jobLastRunDetails') IS NOT NULL DROP TABLE #jobLastRunDetails
				CREATE TABLE #jobLastRunDetails
					(
						[message]		[varchar](4000), 
						[step_id]		[int], 
						[step_name]		[varchar](255), 
						[run_status]	[int], 
						[run_date]		[varchar](16), 
						[run_time]		[varchar](16), 
						[run_duration]	[varchar](16), 
						[event_time]	[datetime])

				SET @queryToRun=N'SELECT TOP 1 h.[message], h.[step_id], h.[step_name], h.[run_status]
											, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
											, GETDATE() AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory] h
								WHERE	h.[job_id]=''' + @JobID + N''' 
										AND h.[step_name] <> ''(Job outcome)''
								ORDER BY h.[instance_id] DESC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
						EXEC (@queryToRun)
				
				SET @Message	=null
				SET @StepID		=null
				SET @StepName	=null
				SET @lastExecutionStatus=null
				SET @RunStatus	=null
				SET @RunDate	=null
				SET @RunTime	=null
				SET @RunDuration=null
				SET @EventTime	=null
				SELECT TOP 1  @Message		= [message]
							, @StepID		= [step_id]
							, @StepName		= [step_name]
							, @RunDate		= [run_date]
							, @RunTime		= [run_time]
							, @RunDuration	= [run_duration] 
							, @EventTime	= [event_time]
				FROM #jobLastRunDetails
				
				SET @queryToRun=N'SELECT TOP 1 NULL AS [message], NULL AS [step_id], NULL AS [step_name], [run_status], NULL AS [run_date], NULL AS [run_time], CAST([run_duration] AS varchar) AS [RunDuration], NULL AS [event_time]
								FROM [msdb].[dbo].[sysjobhistory]
								WHERE	[job_id] = ''' + @JobID + N'''
										AND [step_name] =''(Job outcome)''
								ORDER BY [instance_id] DESC'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun

				TRUNCATE TABLE #jobLastRunDetails
				INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
						EXEC (@queryToRun)
				
				SET @RunDurationLast=null
				SET @RunStatus=null
				SELECT TOP 1  @RunDurationLast	   = [run_duration]
							, @RunStatus		   = CAST([run_status] AS varchar)
							, @lastExecutionStatus = [run_status] 
				FROM #jobLastRunDetails
			
				--for failed jobs, get last step message
				IF @RunStatus=0
					begin
						SET @queryToRun='SELECT TOP 1 h.[message], NULL AS [step_id], NULL AS [step_name], NULL AS [run_status], NULL AS [run_date], NULL AS [run_time], NULL AS [run_duration], NULL AS [event_time]
									FROM [msdb].[dbo].[sysjobhistory] h
									WHERE h.[job_id]=''' + @JobID + ''' 
											AND h.[step_name] <> ''(Job outcome)'' 
											AND h.[run_status]=0
									ORDER BY h.[instance_id] DESC'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 PRINT @queryToRun

						TRUNCATE TABLE #jobLastRunDetails
						INSERT	INTO #jobLastRunDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
								EXEC (@queryToRun)

						SELECT TOP 1 @Message=[message] 
						FROM #jobLastRunDetails
						
						SET @lastExecutionStatus=0
					end

				SET @RunDurationLast=REPLICATE('0', 6 - LEN(@RunDurationLast)) + @RunDurationLast
				SET @runningTimeSec = CAST(SUBSTRING(@RunDurationLast, 1, LEN(@RunDurationLast) - 4) AS [bigint])*3600 + CAST(SUBSTRING(RIGHT(@RunDurationLast, 4), 1, 2) AS [bigint])*60 + CAST(SUBSTRING(RIGHT(@RunDurationLast, 4), 3, 2) AS [bigint])
				SET @RunDurationLast=SUBSTRING(@RunDurationLast, 1, LEN(@RunDurationLast) - 4) + ':' + SUBSTRING(RIGHT(@RunDurationLast, 4), 1, 2) + ':' + SUBSTRING(RIGHT(@RunDurationLast, 4), 3, 2)
				
				IF @lastExecutionStatus IS NULL
					begin
						SET @RunStatus='Unknown'
						SET @lastExecutionStatus='5' 
					end

				SET @RunStatus = CASE @RunStatus WHEN '0' THEN 'Failed'
												 WHEN '1' THEN 'Succeded'				
												 WHEN '2' THEN 'Retry'
												 WHEN '3' THEN 'Canceled'
												 WHEN '4' THEN 'In progress'
								 END

				SET @RunTime=REPLICATE('0', 6 - LEN(@RunTime)) + @RunTime
				SET @RunTime=SUBSTRING(@RunTime, 1, 2) + ':' + SUBSTRING(@RunTime, 3, 2) + ':' + SUBSTRING(@RunTime, 5, 2)
				SET @RunDate=SUBSTRING(@RunDate, 1, 4) + '-' + SUBSTRING(@RunDate, 5, 2) + '-' + SUBSTRING(@RunDate, 7, 2)
				SET @RunDuration=REPLICATE('0', 6 - LEN(@RunDuration)) + @RunDuration
				--SET @RunDuration=SUBSTRING(@RunDuration, 1,2) + ':' + SUBSTRING(@RunDuration, 3,2) + ':' + SUBSTRING(@RunDuration, 5,2)
				SET @RunDuration=SUBSTRING(@RunDuration, 1, LEN(@RunDuration) - 4) + ':' + SUBSTRING(RIGHT(@RunDuration, 4), 1, 2) + ':' + SUBSTRING(RIGHT(@RunDuration, 4), 3, 2)
				
				SET @strMessage='--The specified job [' + @sqlServerName + '].[' + @jobName + '] is not currently running.'
				IF @RunStatus<>'Unknown'
					begin
						SET @strMessage=@strMessage + CHAR(13) + '--Last execution step			: [' + ISNULL(CAST(@StepID AS varchar), '') + '] - [' + ISNULL(@StepName, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Last step finished at      	: [' + ISNULL(@RunDate, '') + ' ' + ISNULL(@RunTime, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Last step running time		: [' + ISNULL(@RunDuration, '') + ']'
						SET @strMessage=@strMessage + CHAR(13) + '--Job execution time (total)	: [' + ISNULL(@RunDurationLast, '') + ']'	
					end
				SET @strMessage=@strMessage + CHAR(13) + '--Last job execution status  	: [' + ISNULL(@RunStatus, 'Unknown') + ']'	

				SET @lastExecutionDate=@RunDate
				SET @lastExecutionTime=@RunTime

				SET @ReturnValue=@lastExecutionStatus
			end

			IF @extentedStepDetails=1
				begin
					IF OBJECT_ID('tempdb..#jobRunStepDetails') IS NOT NULL DROP TABLE #jobRunStepDetails
					CREATE TABLE #jobRunStepDetails
						(
							[message]		[varchar](4000), 
							[step_id]		[int], 
							[step_name]		[varchar](255), 
							[run_status]	[int], 
							[run_date]		[varchar](16), 
							[run_time]		[varchar](16), 
							[run_duration]	[varchar](16), 
							[event_time]	[datetime])

					--get job execution details: steps execution status
					IF @currentRunning = 0 
						SET @queryToRun=N'SELECT   h.[message], h.[step_id], h.[step_name], h.[run_status]
												, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
												, GETDATE() AS [event_time]
										FROM [msdb].[dbo].[sysjobhistory] h
										WHERE	 h.[instance_id] < (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h
																			WHERE	h.[job_id]=''' + @JobID + N''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND	h.[instance_id] > ISNULL(
																	( SELECT [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h
																			WHERE	h.[job_id]=''' + @JobID + N''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	WHERE [instance_id] NOT IN 
																		(
																		SELECT TOP 1 [instance_id] 
																		FROM (	SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																				FROM [msdb].[dbo].[sysjobhistory] h
																				WHERE	h.[job_id]=''' + @JobID + N''' 
																						AND h.[step_name] =''(Job outcome)''
																				ORDER BY h.[instance_id] DESC
																			)A
																		)),0)
												AND h.[job_id] = ''' + @JobID + N'''
											ORDER BY h.[instance_id]'
					ELSE
						SET @queryToRun=N'SELECT   h.[message], h.[step_id], h.[step_name], h.[run_status]
												, CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
												, GETDATE() AS [event_time]
										FROM [msdb].[dbo].[sysjobhistory] h
										WHERE	 h.[instance_id] > (
																	SELECT TOP 1 [instance_id] 
																	FROM (	
																			SELECT TOP 2 h.[instance_id], h.[message], h.[step_id], h.[step_name], h.[run_status], CAST(h.[run_date] AS varchar) AS [run_date], CAST(h.[run_time] AS varchar) AS [run_time], CAST(h.[run_duration] AS varchar) AS [run_duration]
																			FROM [msdb].[dbo].[sysjobhistory] h
																			WHERE	h.[job_id]=''' + @JobID + N''' 
																					AND h.[step_name] =''(Job outcome)''
																			ORDER BY h.[instance_id] DESC
																		)A
																	) 
												AND j.[job_id] = ''' + @JobID + N'''
											ORDER BY h.[instance_id]'

					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 PRINT @queryToRun

					TRUNCATE TABLE #jobRunStepDetails
					INSERT	INTO #jobRunStepDetails ([message], [step_id], [step_name], [run_status], [run_date], [run_time], [run_duration], [event_time])
							EXEC (@queryToRun)
						
					DECLARE @maxLengthStepName [int]
					SELECT @maxLengthStepName = MAX(LEN([step_name]))
					FROM #jobRunStepDetails
					
					SET @maxLengthStepName = ISNULL(@maxLengthStepName, 16)

					DECLARE crsJobDetails CURSOR FOR	SELECT DISTINCT   [step_id]
																		, [step_name]
																		, [run_status]
																		, [run_date]
																		, [run_time]
																		, [run_duration]
																		, [message]
														FROM #jobRunStepDetails
														ORDER BY [run_date], [run_time]
					OPEN crsJobDetails
					FETCH NEXT FROM crsJobDetails INTO @StepID, @StepName, @RunStatusDetail, @RunDateDetail, @RunTimeDetail, @RunDurationDetail, @queryToRun

					IF @@FETCH_STATUS=0
						begin
							SET @queryToRun='[' + LEFT('Run Date' + SPACE(10), 10) + '] [' + LEFT('RunTime' + SPACE(8), 8) +'] [' + LEFT('Status' + SPACE(12), 12) + '] [' + LEFT('Duration' + SPACE(20), 20) + '] [' + LEFT('ID' + SPACE(3), 3) + '] [' + LEFT('Step Name' + SPACE(@maxLengthStepName), @maxLengthStepName) + ']'
							SET @strMessage=@strMessage + CHAR(13) + @queryToRun
						end
						
					WHILE @@FETCH_STATUS=0
						begin								
							SET @RunStatusDetail = CASE @RunStatusDetail WHEN '0' THEN 'Failed'
																			WHEN '1' THEN 'Succeded'				
																			WHEN '2' THEN 'Retry'
																			WHEN '3' THEN 'Canceled'
																			WHEN '4' THEN 'In progress'
														END
	
							SET @RunTimeDetail=REPLICATE('0', 6 - LEN(@RunTimeDetail)) + @RunTimeDetail
							SET @RunTimeDetail=SUBSTRING(@RunTimeDetail, 1, 2) + ':' + SUBSTRING(@RunTimeDetail, 3, 2) + ':' + SUBSTRING(@RunTimeDetail, 5, 2)
							SET @RunDateDetail=SUBSTRING(@RunDateDetail, 1, 4) + '-' + SUBSTRING(@RunDateDetail, 5, 2) + '-' + SUBSTRING(@RunDateDetail, 7, 2)

							SET @RunDurationDetail=REPLICATE('0', 6 - LEN(@RunDurationDetail)) + @RunDurationDetail
								
							SET @strMessage=@strMessage + CHAR(13) + ISNULL(
									'[' + LEFT(@RunDateDetail + SPACE(10), 10) + '] ' + 
									'[' + LEFT(@RunTimeDetail + SPACE(8), 8) + '] ' + 
									'[' + LEFT(@RunStatusDetail + SPACE(12), 12) + '] ' + 
									'[' + LEFT(dbo.ufn_reportHTMLFormatTimeValue((CAST(SUBSTRING(@RunDurationDetail, 1, LEN(@RunDurationDetail) - 4) AS [bigint])*3600 + CAST(SUBSTRING(RIGHT(@RunDurationDetail, 4), 1, 2) AS [bigint])*60 + CAST(SUBSTRING(RIGHT(@RunDurationDetail, 4), 3, 2) AS [bigint]))*1000) + SPACE(20), 20) + '] ' + 
									'[' + LEFT(CAST(@StepID AS varchar) + SPACE(3), 3) + '] ' + 
									'[' + LEFT(@StepName + SPACE(@maxLengthStepName), @maxLengthStepName) + ']', '')

							FETCH NEXT FROM crsJobDetails INTO @StepID, @StepName, @RunStatusDetail, @RunDateDetail, @RunTimeDetail, @RunDurationDetail, @queryToRun
						end
					CLOSE crsJobDetails
					DEALLOCATE crsJobDetails					
				end

			--final error message
			IF @currentRunning = 0  AND @RunStatus='Failed'
				begin
					SET @strMessage=@strMessage + CHAR(13) + '--Job execution return this message: ' + ISNULL(@Message, '')
					IF @debugMode=1
						print '--Job execution return this message: ' + ISNULL(@Message, '')
				end
	end

IF @debugMode=1
	print @strMessage
SET @ReturnValue=ISNULL(@ReturnValue, 0)
IF @selectResult=1
	SELECT @strMessage AS StrMessage, @currentRunning AS CurrentRunning, @lastExecutionStatus AS LastExecutionStatus, @lastExecutionDate AS LastExecutionDate, @lastExecutionTime AS LastExecutionTime, @runningTimeSec AS RunningTimeSec
RETURN @ReturnValue



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectDatabaseDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectDatabaseDetails]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectDatabaseDetails]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectDatabaseDetails]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@databaseNameFilter		[sysname]='%',
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 30.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@catalogDatabaseID		[smallint],
		@databaseID				[int],
		@databaseName			[sysname],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000)

DECLARE @SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@dbccLastKnownGood		[datetime]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#databaseSpaceInfo') IS NOT NULL DROP TABLE #databaseSpaceInfo
CREATE TABLE #databaseSpaceInfo
(
	[drive]					[varchar](2)		NULL,
	[is_log_file]			[bit]				NULL,
	[size_kb]				[int]				NULL,
	[space_used_kb]			[int]				NULL,
	[is_growth_limited]		[bit]				NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#dbccDBINFO') IS NOT NULL DROP TABLE #dbccDBINFO
CREATE TABLE #dbccDBINFO
	(
		[id]				[int] IDENTITY(1,1),
		[ParentObject]		[varchar](255),
		[Object]			[varchar](255),
		[Field]				[varchar](255),
		[Value]				[varchar](255)
	)
	
/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#dbccLastKnownGood') IS NOT NULL DROP TABLE #dbccLastKnownGood
CREATE TABLE #dbccLastKnownGood
(
	[Value]					[sysname]			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#statsHealthCheckDatabaseDetails') IS NOT NULL DROP TABLE #statsHealthCheckDatabaseDetails
CREATE TABLE #statsHealthCheckDatabaseDetails
(
	[database_id]				[int]			NOT NULL,
	[query_type]				[tinyint]		NOT NULL,
	[data_size_mb]				[numeric](20,3)	NULL,
	[data_space_used_percent]	[numeric](6,2)	NULL,
	[log_size_mb]				[numeric](20,3)	NULL,
	[log_space_used_percent]	[numeric](6,2)	NULL,
	[is_auto_close]				[bit]			NULL,
	[is_auto_shrink]			[bit]			NULL,
	[physical_drives]			[sysname]		NULL,
	[last_backup_time]			[datetime]		NULL,
	[last_dbcc checkdb_time]	[datetime]		NULL,
	[recovery_model]			[tinyint]		NULL,
	[page_verify_option]		[tinyint]		NULL,
	[compatibility_level]		[tinyint]		NULL,
	[is_growth_limited]			[bit]			NULL
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
--A. get databases informations
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE shcdd
FROM [dbo].[statsHealthCheckDatabaseDetails]	shcdd
INNER JOIN [dbo].[catalogDatabaseNames]			cdb ON cdb.[id] = shcdd.[catalog_database_id] AND cdb.[instance_id] = shcdd.[instance_id]
INNER JOIN [dbo].[catalogInstanceNames]			cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND cdb.[name] LIKE @databaseNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectDatabaseDetails'


-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Get Database Details Information....', 10, 1) WITH NOWAIT
		
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
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		TRUNCATE TABLE #statsHealthCheckDatabaseDetails

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		DECLARE crsActiveDatabases CURSOR LOCAL FOR 	SELECT	cdn.[catalog_database_id], cdn.[database_id], cdn.[database_name]
														FROM	[dbo].[vw_catalogDatabaseNames] cdn
														WHERE 	cdn.[project_id] = @projectID
																AND cdn.[instance_id] = @instanceID
																AND cdn.[active]=1
																AND cdn.[database_name] LIKE @databaseNameFilter
																AND CHARINDEX(cdn.[state_desc], 'ONLINE, READ ONLY')<>0
														ORDER BY cdn.[database_name]
		OPEN crsActiveDatabases	
		FETCH NEXT FROM crsActiveDatabases INTO @catalogDatabaseID, @databaseID, @databaseName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage='--		database: ' + @databaseName
				RAISERROR(@strMessage, 10, 1) WITH NOWAIT

				/* get space allocated / used details */
				IF @sqlServerName <> @@SERVERNAME
					SET @queryToRun = N'SELECT *
										FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC(''''USE [' + @databaseName + N']; 
												SELECT    [drive]
														, CAST([is_logfile]		AS [bit]) AS [is_logfile]
														, SUM([size_kb])		AS [size_mb]
														, SUM([space_used_kb])	AS [space_used_mb]
														, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
												FROM (		
														SELECT    [name], [size] * 8 as [size_kb]
																, CAST(FILEPROPERTY([name], ''''''''SpaceUsed'''''''') AS [int]) * 8	AS [space_used_kb]
																, CAST(FILEPROPERTY([name], ''''''''IsLogFile'''''''') AS [bit])		AS [is_logfile]
																, REPLACE(LEFT([' + CASE WHEN @SQLMajorVersion <=8 THEN N'filename' ELSE N'physical_name' END + N'], 2), '''''''':'''''''', '''''''''''''''') AS [drive]
																, ' + CASE	WHEN @SQLMajorVersion <= 8 
																			THEN N'CASE WHEN ([maxsize]=-1 AND [groupid]<>0) OR ([maxsize]=-1 AND [groupid]=0) OR ([maxsize]=268435456 AND [groupid]=0) THEN 0 ELSE 1 END ' 
																			ELSE N'CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END '
																	 END + N' AS [is_growth_limited]
														FROM [' + @databaseName + N'].' + CASE WHEN @SQLMajorVersion <=8 THEN N'dbo.sysfiles' ELSE N'sys.database_files' END + N'
													)sf
												GROUP BY [drive], [is_logfile]
										'''')'')x'
				ELSE
					SET @queryToRun = N'USE [' + @databaseName + N']; 
										SELECT    [drive]
												, CAST([is_logfile]		AS [bit]) AS [is_logfile]
												, SUM([size_kb])		AS [size_mb]
												, SUM([space_used_kb])	AS [space_used_mb]
												, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
										FROM (		
												SELECT    [name], [size] * 8 as [size_kb]
														, CAST(FILEPROPERTY([name], ''SpaceUsed'') AS [int]) * 8	AS [space_used_kb]
														, CAST(FILEPROPERTY([name], ''IsLogFile'') AS [bit])		AS [is_logfile]
														, REPLACE(LEFT([' + CASE WHEN @SQLMajorVersion <=8 THEN N'filename' ELSE N'physical_name' END + N'], 2), '':'', '''') AS [drive]	
														, ' + CASE	WHEN @SQLMajorVersion <= 8 
																	THEN N'CASE WHEN ([maxsize]=-1 AND [groupid]<>0) OR ([maxsize]=-1 AND [groupid]=0) OR ([maxsize]=268435456 AND [groupid]=0) THEN 0 ELSE 1 END ' 
																	ELSE N'CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END '
																END + N' AS [is_growth_limited]
												FROM [' + @databaseName + N'].' + CASE WHEN @SQLMajorVersion <=8 THEN N'dbo.sysfiles' ELSE N'sys.database_files' END + N'
											)sf
										GROUP BY [drive], [is_logfile]'			
				IF @debugMode = 1 PRINT @queryToRun
				
				TRUNCATE TABLE #databaseSpaceInfo
				BEGIN TRY
						INSERT	INTO #databaseSpaceInfo([drive], [is_log_file], [size_kb], [space_used_kb], [is_growth_limited])
							EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage
					INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
								  , @projectID
								  , GETUTCDATE()
								  , 'dbo.usp_hcCollectDatabaseDetails'
								  , '[' + @databaseName + ']:' + @strMessage
				END CATCH

				/* get last date for dbcc checkdb, only for 2k5+ */
				IF @SQLMajorVersion > 8 
					begin
						IF @sqlServerName <> @@SERVERNAME
							begin
								IF @SQLMajorVersion < 11
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @databaseName + N']) WITH TABLERESULTS'''')'')x
														WHERE [Field]=''dbi_dbccLastKnownGood'''
								ELSE
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC(''''DBCC DBINFO ([' + @databaseName + N']) WITH TABLERESULTS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
														WHERE [Field]=''dbi_dbccLastKnownGood'''
							end
						ELSE
							begin							
								BEGIN TRY
									INSERT INTO #dbccDBINFO
											EXEC ('DBCC DBINFO (''' + @databaseName + N''') WITH TABLERESULTS')
								END TRY
								BEGIN CATCH
									SET @strMessage = ERROR_MESSAGE()
									PRINT @strMessage

									INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
											SELECT  @instanceID
												  , @projectID
												  , GETUTCDATE()
												  , 'dbo.usp_hcCollectDatabaseDetails'
												  , '[' + @databaseName + ']:' + @strMessage
								END CATCH

								SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccLastKnownGood'''											
							end

						IF @debugMode = 1 PRINT @queryToRun
				
						TRUNCATE TABLE #dbccLastKnownGood
						BEGIN TRY
							INSERT	INTO #dbccLastKnownGood([Value])
									EXEC (@queryToRun)
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage

							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
										  , @projectID
										  , GETUTCDATE()
										  , 'dbo.usp_hcCollectDatabaseDetails'
										  , '[' + @databaseName + ']:' + @strMessage
						END CATCH

						BEGIN TRY
							SELECT @dbccLastKnownGood = CASE WHEN [Value] = '1900-01-01 00:00:00.000' THEN NULL ELSE [Value] END 
							FROM #dbccLastKnownGood
						END TRY
						BEGIN CATCH
							SET @dbccLastKnownGood=NULL
						END CATCH
					end

				/* compute database statistics */
				INSERT	INTO #statsHealthCheckDatabaseDetails([query_type], [database_id], [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], [physical_drives], [last_dbcc checkdb_time], [is_growth_limited])
						SELECT    1, @databaseID
								, CAST([data_size_kb] / 1024. AS [numeric](20,3)) AS [data_size_mb]
								, CAST(CASE WHEN [data_size_kb] <>0 THEN [data_space_used_kb] * 100. / [data_size_kb] ELSE 0 END AS [numeric](6,2)) AS [data_used_percent]
								, CAST([log_size_kb] / 1024. AS [numeric](20,3)) AS [log_size_mb]
								, CAST(CASE WHEN [log_size_kb] <>0 THEN [log_space_used_kb] * 100. / [log_size_kb] ELSE 0 END AS [numeric](6,2)) AS [log_used_percent]
								, [drives]
								, @dbccLastKnownGood
								, [is_growth_limited]
						FROM (
								SELECT    SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[size_kb] ELSE 0 END)		AS [data_size_kb]
										, SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[space_used_kb] ELSE 0 END) 	AS [data_space_used_kb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[size_kb] ELSE 0 END) 		AS [log_size_kb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[space_used_kb] ELSE 0 END) 	AS [log_space_used_kb]
										, MAX(x.[drives]) [drives]
										, MAX(CAST([is_growth_limited] AS [tinyint])) [is_growth_limited]
								FROM #databaseSpaceInfo dsi
								CROSS APPLY(
											SELECT STUFF(
															(	SELECT ', ' + [drive]
																FROM (	
																		SELECT DISTINCT UPPER([drive]) [drive]
																		FROM #databaseSpaceInfo
																	) AS x
																ORDER BY [drive]
																FOR XML PATH('')
															),1,1,''
														) AS [drives]
											)x
							)db
				FETCH NEXT FROM crsActiveDatabases INTO @catalogDatabaseID, @databaseID, @databaseName
			end
		CLOSE crsActiveDatabases
		DEALLOCATE crsActiveDatabases

		/* get last date for backup and other database flags / options */
		SET @queryToRun = N'SELECT	  2 AS [query_type]
									, bkp.[database_id]
									, CASE WHEN bkp.[last_backup_time] = CONVERT([datetime], ''1900-01-01'', 120) THEN NULL ELSE bkp.[last_backup_time] END AS [last_backup_time]
									, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoClose'')  AS [bit])	AS [is_auto_close]
									, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoShrink'')  AS [bit])	AS [is_auto_shrink]
									, bkp.[recovery_model]
									, bkp.[page_verify_option]
									, bkp.[compatibility_level]
							FROM 	
								(' + 
							CASE	WHEN @SQLMajorVersion <= 8 
									THEN N'	SELECT	  sdb.[dbid]	AS [database_id]
													, sdb.[name]	AS [database_name]
													, CASE CAST(DATABASEPROPERTYEX(sdb.[name], ''Recovery'') AS [sysname]) 
															WHEN ''FULL'' THEN 1 
															WHEN ''BULK_LOGGED'' THEN 2
															WHEN ''SIMPLE'' THEN 3
															ELSE NULL
													  END AS [recovery_model]
													, CASE WHEN sdb.[status] & 16 = 16 THEN 1 ELSE 0 END AS [page_verify_option]
													, sdb.[cmptlevel] AS [compatibility_level]
													, MAX(bs.[backup_finish_date]) AS [last_backup_time]
											FROM dbo.sysdatabases sdb
											LEFT OUTER JOIN msdb.dbo.backupset bs ON bs.[database_name] = sdb.[name] AND bs.type IN (''D'', ''I'')
											WHERE sdb.[name] LIKE ''' + @databaseNameFilter + N'''
											GROUP BY sdb.[name], sdb.[dbid], sdb.[status], sdb.[cmptlevel]'
									ELSE N'SELECT	  sdb.[name]	AS [database_name]
													, sdb.[database_id]
													, sdb.[recovery_model]
													, sdb.[page_verify_option]
													, sdb.[compatibility_level]
													, MAX(bs.[backup_finish_date]) AS [last_backup_time]
											FROM sys.databases sdb
											LEFT OUTER JOIN msdb.dbo.backupset bs ON bs.[database_name] = sdb.[name] AND bs.type IN (''D'', ''I'')
											WHERE sdb.[name] LIKE ''' + @databaseNameFilter + N'''
											GROUP BY sdb.[name], sdb.[database_id], sdb.[recovery_model], sdb.[page_verify_option], sdb.[compatibility_level]'
							END + N'
								)bkp'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
		IF @debugMode = 1 PRINT @queryToRun
		
		BEGIN TRY
			INSERT	INTO #statsHealthCheckDatabaseDetails([query_type], [database_id], [last_backup_time], [is_auto_close], [is_auto_shrink], [recovery_model], [page_verify_option], [compatibility_level])
					EXEC (@queryToRun)
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			PRINT @strMessage

			INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectDatabaseDetails'
							, @strMessage
		END CATCH

		/* save results to stats table */
		INSERT	INTO [dbo].[statsHealthCheckDatabaseDetails]([catalog_database_id], [instance_id], 
				 											 [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], 
															 [is_auto_close], [is_auto_shrink], [physical_drives], 
															 [last_backup_time], [last_dbcc checkdb_time],  [recovery_model], [page_verify_option], [compatibility_level], [is_growth_limited], [event_date_utc])
				SELECT cdn.[id], @instanceID, 
				 		qt.[data_size_mb], qt.[data_space_used_percent], qt.[log_size_mb], qt.[log_space_used_percent], 
						qt.[is_auto_close], qt.[is_auto_shrink], qt.[physical_drives], 
						qt.[last_backup_time], qt.[last_dbcc checkdb_time],  qt.[recovery_model], qt.[page_verify_option], qt.[compatibility_level], qt.[is_growth_limited], GETUTCDATE()
				FROM (
						SELECT    ISNULL(qt1.[database_id], qt2.[database_id]) [database_id]
								, qt2.[recovery_model]
								, qt2.[page_verify_option]
								, qt2.[compatibility_level]
								, qt1.[data_size_mb]
								, qt1.[data_space_used_percent]
								, qt1.[log_size_mb]
								, qt1.[log_space_used_percent]
								, qt1.[physical_drives]
								, qt2.[is_auto_close]
								, qt2.[is_auto_shrink]
								, qt2.[last_backup_time]
								, qt1.[last_dbcc checkdb_time]
								, qt1.[is_growth_limited]
						FROM (
								SELECT    [database_id]
										, [data_size_mb]
										, [data_space_used_percent]
										, [log_size_mb]
										, [log_space_used_percent]
										, [physical_drives]
										, [last_dbcc checkdb_time]
										, [is_growth_limited]
								FROM #statsHealthCheckDatabaseDetails
								WHERE [query_type]=1
							) qt1
						FULL OUTER JOIN
							(
								SELECT    [database_id]
										, [is_auto_close]
										, [is_auto_shrink]
										, [last_backup_time]
										, [recovery_model]
										, [page_verify_option]
										, [compatibility_level]
								FROM #statsHealthCheckDatabaseDetails
								WHERE [query_type]=2
							) qt2 ON qt1.[database_id] = qt2.[database_id]
					)qt
				INNER JOIN [dbo].[catalogDatabaseNames] cdn ON	cdn.[database_id] = qt.[database_id] 
															AND cdn.[instance_id] = @instanceID 
															AND cdn.[project_id] = @projectID
	
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO


SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

RAISERROR('Create procedure: [dbo].[usp_hcCollectDiskSpaceUsage]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_hcCollectDiskSpaceUsage]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_hcCollectDiskSpaceUsage]
GO

CREATE PROCEDURE [dbo].[usp_hcCollectDiskSpaceUsage]
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@enableXPCMDSHELL		[bit]=0,
		@debugMode				[bit]=0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 28.01.2015
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SET NOCOUNT ON

------------------------------------------------------------------------------------------------------------------------------------------
DECLARE @projectID				[smallint],
		@sqlServerName			[sysname],
		@instanceID				[smallint],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000),
		@SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@runxpFixedDrives		[bit],
		@runwmicLogicalDisk		[bit],
		@errorCode				[int]

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]


/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#xpCMDShellOutput') IS NOT NULL 
DROP TABLE #xpCMDShellOutput

CREATE TABLE #xpCMDShellOutput
(
	[output]	[nvarchar](max)			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#xp_cmdshell') IS NOT NULL DROP TABLE #xp_cmdshell

CREATE TABLE #xp_cmdshell
(
	[output]		[nvarchar](max)		NULL,
	[instance_name]	[sysname]			NULL,
	[machine_name]	[sysname]			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#diskSpaceInfo') IS NOT NULL DROP TABLE #diskSpaceInfo
CREATE TABLE #diskSpaceInfo
(
	[logical_drive]			[char](1)			NULL,
	[volume_mount_point]	[nvarchar](512)		NULL,
	[total_size_mb]			[numeric](18,3)		NULL,
	[available_space_mb]	[numeric](18,3)		NULL,
	[percent_available]		[numeric](6,2)		NULL
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
IF @enableXPCMDSHELL=1
	begin
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
															@debugMode			= @debugMode

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
																	@debugMode			= @debugMode

				IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
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
				RETURN 1
			end		
	end


------------------------------------------------------------------------------------------------------------------------------------------
--
-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 1: Delete existing information....', 10, 1) WITH NOWAIT

DELETE dsi
FROM [dbo].[statsHealthCheckDiskSpaceInfo]		dsi
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = dsi.[instance_id] AND cin.[project_id] = dsi.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectDiskSpaceUsage'

-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Get Instance Details Information....', 10, 1) WITH NOWAIT
		
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
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		TRUNCATE TABLE #diskSpaceInfo
		TRUNCATE TABLE #xp_cmdshell
		TRUNCATE TABLE #xpCMDShellOutput

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
		END CATCH

		/* get volume space / free disk space details */
		SET @runwmicLogicalDisk=1
		SET @runxpFixedDrives=1
		IF @SQLMajorVersion >= 10
			begin				
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'SELECT DISTINCT
													  UPPER(SUBSTRING([physical_name], 1, 1)) [logical_drive]
													, CASE WHEN LEN([volume_mount_point])=3 THEN UPPER([volume_mount_point]) ELSE [volume_mount_point] END [volume_mount_point]
													, [total_bytes] / 1024 / 1024 AS [total_size_mb]
													, [available_bytes] / 1024 / 1024 AS [available_space_mb]
													, CAST(ISNULL(ROUND([available_bytes] / CAST(NULLIF([total_bytes], 0) AS [numeric](20,3)) * 100, 2), 0) AS [numeric](10,2)) AS [percent_available]
												FROM sys.master_files AS f
												CROSS APPLY sys.dm_os_volume_stats(f.[database_id], f.[file_id])'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode=1	PRINT @queryToRun

				TRUNCATE TABLE #diskSpaceInfo
				BEGIN TRY
						INSERT	INTO #diskSpaceInfo([logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
							EXEC (@queryToRun)
						SET @runwmicLogicalDisk=0
						SET @runxpFixedDrives=0
				END TRY
				BEGIN CATCH
					IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()					
				END CATCH
			end

		IF @runwmicLogicalDisk=1
			begin
				/* try to run wmic */
				IF @enableXPCMDSHELL=1 AND @optionXPIsAvailable=1
					begin
						BEGIN TRY
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic logicaldisk get Caption, FreeSpace, Size''; EXEC xp_cmdshell @cmdQuery;'
			
								IF @sqlServerName<>@@SERVERNAME
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC(''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
								IF @debugMode = 1 PRINT @queryToRun

								INSERT	INTO #xpCMDShellOutput([output])
										EXEC (@queryToRun)

								DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3

								INSERT	INTO #diskSpaceInfo([logical_drive], [available_space_mb], [total_size_mb])
										SELECT	[drive]
												, CAST(LTRIM(RTRIM(CASE WHEN CHARINDEX(' ', [total_size])>0 THEN SUBSTRING([total_size], 1, CHARINDEX(' ', [total_size])) END)) AS [bigint]) / (1024 * 1024.) AS [free_bytes]
												, CAST(LTRIM(RTRIM(CASE WHEN CHARINDEX(' ', [total_size])>0 THEN SUBSTRING([total_size], CHARINDEX(' ', [total_size]), 100) END))  AS [bigint]) / (1024 * 1024.) AS [size_bytes]
										FROM (
												SELECT    SUBSTRING([output], 1, 1) AS [drive]
														, LTRIM(RTRIM(SUBSTRING([output], 3, LEN([output])-3))) AS [total_size]
												FROM	#xpCMDShellOutput 
												WHERE	SUBSTRING([output], 2, 1)=':'
														AND LEN(LTRIM(RTRIM(SUBSTRING([output], 3, LEN([output])-3))))<>0					
											)X

								DELETE FROM #diskSpaceInfo WHERE [total_size_mb]=0

								UPDATE #diskSpaceInfo
										SET [percent_available] =  CAST(ISNULL(ROUND([available_space_mb] / CAST(NULLIF([total_size_mb], 0) AS [numeric](20,3)) * 100, 2), 0) AS [numeric](10,2)) 

								SET @runxpFixedDrives=0
						END TRY
						BEGIN CATCH
							IF @debugMode=1 PRINT 'An error occured. It will be ignored: ' + ERROR_MESSAGE()					
						END CATCH
					end
			end

		IF @runxpFixedDrives=1
			begin
				IF @sqlServerName <> @@SERVERNAME
					begin
						SET @queryToRun = N''
						IF @SQLMajorVersion < 11
							SET @queryToRun = @queryToRun + N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_fixeddrives'')x'
						ELSE
							SET @queryToRun = @queryToRun + N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_fixeddrives WITH RESULT SETS(([drive] [sysname], [MB free] [bigint]))'')x'

						IF @debugMode=1	PRINT @queryToRun

						TRUNCATE TABLE #diskSpaceInfo
						BEGIN TRY
								INSERT	INTO #diskSpaceInfo([logical_drive], [available_space_mb])
									EXEC (@queryToRun)
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDiskSpaceUsage'
											, @strMessage
						END CATCH

					end
				ELSE
					begin							
						BEGIN TRY
							INSERT	INTO #diskSpaceInfo([logical_drive], [available_space_mb])
									EXEC xp_fixeddrives
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage

							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDiskSpaceUsage'
											, @strMessage
						END CATCH
					end

			end
				
		/* save results to stats table */
		INSERT	INTO [dbo].[statsHealthCheckDiskSpaceInfo]([instance_id], [project_id], [event_date_utc], [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available])
				SELECT    @instanceID, @projectID, GETUTCDATE()
						, [logical_drive], [volume_mount_point], [total_size_mb], [available_space_mb], [percent_available]
				FROM #diskSpaceInfo
							
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances


/*-------------------------------------------------------------------------------------------------------------------------------*/
/* disable xp_cmdshell configuration option */
IF @optionXPHasChanged = 1
	EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
														@configOptionName	= 'xp_cmdshell',
														@configOptionValue	= 0,
														@optionIsAvailable	= @optionXPIsAvailable OUT,
														@optionCurrentValue	= @optionXPValue OUT,
														@optionHasChanged	= @optionXPHasChanged OUT,
														@executionLevel		= 0,
														@debugMode			= @debugMode

/* disable show advanced options configuration option */
IF @optionAdvancedHasChanged = 1
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'show advanced options',
															@configOptionValue	= 0,
															@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
															@optionCurrentValue	= @optionAdvancedValue OUT,
															@optionHasChanged	= @optionAdvancedHasChanged OUT,
															@executionLevel		= 0,
															@debugMode			= @debugMode

GO


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
		@configErrorlogFileNo	[int]


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
FROM [dbo].[statsSQLServerErrorlogDetails]	eld
INNER JOIN [dbo].[catalogInstanceNames]		cin ON cin.[id] = eld.[instance_id] AND cin.[project_id] = eld.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectErrorlogMessages'

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage= 'Step 2: Get Errorlog messages...'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 0, @stopExecution=0
		
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
		WHILE @configErrorlogFileNo > 0
			begin
				IF @sqlServerName <> @@SERVERNAME
					begin
						IF @SQLMajorVersion < 11
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog ' + CAST((@configErrorlogFileNo-1) AS [nvarchar]) + ''')x'
						ELSE
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC xp_readerrorlog ' + CAST((@configErrorlogFileNo-1) AS [nvarchar]) + ' WITH RESULT SETS(([log_date] [datetime] NULL, [process_info] [sysname] NULL, [text] [varchar](max) NULL))'')x'
					end
				ELSE
					SET @queryToRun = N'xp_readerrorlog ' + CAST((@configErrorlogFileNo-1) AS [nvarchar])
				IF @debugMode=1	PRINT @queryToRun

				BEGIN TRY
					IF @SQLMajorVersion > 8 
						INSERT	INTO #xpReadErrorLog([log_date], [process_info], [text])
								EXEC (@queryToRun)
					ELSE
						INSERT	INTO #xpReadErrorLog([text], [continuation_row])
								EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage

					INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectErrorlogMessages'
									, @strMessage
				END CATCH

				SET @configErrorlogFileNo = @configErrorlogFileNo - 1
			end

		/* re-parse messages for 2k version */
		IF @SQLMajorVersion = 8 
			begin
				SET @strMessage= 'rebuild messages for ContinuationRows'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0

				DECLARE crsErrorlogContinuation CURSOR FAST_FORWARD FOR SELECT [id], [text]
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
		INSERT	INTO [dbo].[statsSQLServerErrorlogDetails]([instance_id], [project_id], [event_date_utc], [log_date], [process_info], [text])
				SELECT @instanceID, @projectID, GETUTCDATE(), [log_date], [process_info], [text]
				FROM #xpReadErrorLog
				WHERE [log_date] IS NOT NULL
				ORDER BY [log_date], [id]

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO


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
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectEventMessages'

-------------------------------------------------------------------------------------------------------------------------
RAISERROR('--Step 2: Copy Event Messages Information....', 10, 1) WITH NOWAIT
		
DECLARE crsActiveInstances CURSOR LOCAL FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
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
		SET @queryToRun=@queryToRun + N'SELECT OBJECT_ID(''' + DB_NAME() + N'.dbo.logEventMessages'', ''U'') AS [object_id]'
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
			INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
									FROM [' + DB_NAME() + N'].dbo.logEventMessages lem
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
					INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
		@projectCode			[varchar](32)=NULL,
		@sqlServerNameFilter	[sysname]='%',
		@logNameFilter			[sysname]='%',
		@enableXPCMDSHELL		[bit]=1,
		@debugMode				[bit]=0

/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 20.11.2014
-- Module			 : Database Analysis & Performance Monitoring
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
		, @configEventsInLastHours		[smallint]
		, @configEventsTimeOutSeconds	[int]
		, @startTime					[datetime]
		, @endTime						[datetime]
		, @getInformationEvent			[bit]=0
		, @getWarningsEvent				[bit]=0
		

DECLARE @optionXPIsAvailable		[bit],
		@optionXPValue				[int],
		@optionXPHasChanged			[bit],
		@optionAdvancedIsAvailable	[bit],
		@optionAdvancedValue		[int],
		@optionAdvancedHasChanged	[bit]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('tempdb..#psOutput') IS NOT NULL DROP TABLE #psOutput
CREATE TABLE #psOutput
	(
		  [id]	[int] identity(1,1) primary key
		, [xml] [varchar](max)
	)

------------------------------------------------------------------------------------------------------------------------------------------
SELECT @psFileLocation = REVERSE(SUBSTRING(REVERSE([value]), CHARINDEX('\', REVERSE([value])), LEN(REVERSE([value]))))
FROM (
		SELECT CAST(SERVERPROPERTY('ErrorLogFileName') AS [nvarchar](1024)) AS [value]
	)er
	
IF @psFileLocation IS NULL SET @psFileLocation =N'C:\'

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
--get event messages time delta
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
															@executionLevel		= 3,
															@debugMode			= @debugMode

		IF @optionXPIsAvailable = 0
			begin
				/* enable show advanced options configuration option */
				EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																	@configOptionName	= 'show advanced options',
																	@configOptionValue	= 1,
																	@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
																	@optionCurrentValue	= @optionAdvancedValue OUT,
																	@optionHasChanged	= @optionAdvancedHasChanged OUT,
																	@executionLevel		= 3,
																	@debugMode			= @debugMode

				IF @optionAdvancedIsAvailable = 1 AND (@optionAdvancedValue=1 OR @optionAdvancedHasChanged=1)
					EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
																		@configOptionName	= 'xp_cmdshell',
																		@configOptionValue	= 1,
																		@optionIsAvailable	= @optionXPIsAvailable OUT,
																		@optionCurrentValue	= @optionXPValue OUT,
																		@optionHasChanged	= @optionXPHasChanged OUT,
																		@executionLevel		= 3,
																		@debugMode			= @debugMode

			end

		IF @optionXPIsAvailable=0 OR @optionXPValue=0
			begin
				set @strMessage='xp_cmdshell component is turned off. Cannot continue'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				RETURN 1
			end		
	end

------------------------------------------------------------------------------------------------------------------------------------------
--A. get servers OS events details
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage=N'Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

DELETE soel
FROM [dbo].[statsOSEventLogs]			soel
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = soel.[instance_id] AND cin.[project_id] = soel.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter

DELETE lsam
FROM [dbo].[logServerAnalysisMessages]	lsam
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

DECLARE crsMachineList CURSOR READ_ONLY FAST_FORWARD FOR SELECT cin.[id] AS [instance_id], cin.[name] AS [instance_name], cmn.[id] AS [machine_id], cmn.[name] AS [machine_name]
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
														ORDER BY cin.[name], cmn.[name]
OPEN crsMachineList
FETCH NEXT FROM crsMachineList INTO @instanceID, @instanceName, @machineID, @machineName
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @machineName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 2, @stopExecution=0


		-------------------------------------------------------------------------------------------------------------------------
		DECLARE crsLogName CURSOR READ_ONLY FOR SELECT [log_type_name], [log_type_id]
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
				FROM [dbo].[logServerAnalysisMessages]	lsam
				INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
				WHERE cin.[project_id] = @projectID
						AND cin.[id]= @instanceID
						AND lsam.[descriptor]=@eventDescriptor
						
				SET @queryToRun='SELECT CONVERT([varchar](20), GETDATE(), 120) AS [current_date]'
				SET @queryToRun = dbo.ufn_formatSQLQueryForLinkedServer(@instanceName, @queryToRun)
				IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

				TRUNCATE TABLE #psOutput
				BEGIN TRY
					INSERT	INTO #psOutput([xml])
							EXEC (@queryToRun)

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
						#-- Module			 : Database Analysis & Performance Monitoring
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

				INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
						SELECT  @instanceID
								, @projectID
								, GETUTCDATE()
								, @eventDescriptor
								, @queryToRun


			
				-------------------------------------------------------------------------------------------------------------------------
				IF NOT (@optionXPIsAvailable=0 OR @optionXPValue=0)
					begin
						-- save powershell script
						SET @psFileName = 'GetOSSystemEvents_' + REPLACE(@machineName, '\', '_') + '_' + @psLogTypeName + '.ps1'
						SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [message] FROM [' + DB_NAME() + '].[dbo].[logServerAnalysisMessages] WHERE [descriptor]=''''' + @eventDescriptor + ''''' AND [instance_id]=' + CAST(@instanceID AS [varchar]) + ' AND [project_id]=' + CAST(@projectID AS [varchar]) + '" queryout "' + @psFileLocation + @psFileName + '" -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'', no_output'
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						EXEC (@queryToRun) 
					end

				-------------------------------------------------------------------------------------------------------------------------
				--executing script to get the OS events
				IF NOT (@optionXPIsAvailable=0 OR @optionXPValue=0)
					begin
						SET @strMessage=N'running powershell script - get OS events...'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						SET @queryToRun='master.dbo.xp_cmdshell N''@PowerShell -File "' + @psFileLocation + @psFileName + '"'''
						IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0

						TRUNCATE TABLE #psOutput
						BEGIN TRY
							INSERT	INTO #psOutput([xml])
									EXEC (@queryToRun)
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage
			
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, @strMessage
						END CATCH

						BEGIN TRY
							SET @queryToRun=N'master.dbo.xp_cmdshell ''del "' + @psFileLocation + @psFileName + '"'', no_output'
							IF @debugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 4, @stopExecution=0
							EXEC (@queryToRun) 
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							PRINT @strMessage
			
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
						INSERT	INTO [dbo].[statsOSEventLogs](  [instance_id], [project_id], [machine_id], [event_date_utc], [log_type_id]
															  , [event_id], [level_id], [record_id], [category_id], [category_name]
															  , [source], [process_id], [thread_id], [machine_name], [user_id], [time_created], [message])
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

					end
				ELSE
					begin
						IF (SELECT COUNT(*) FROM #psOutput WHERE [xml] IS NOT NULL)=0
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'Timeout occured while running powershell script. (LogName = ' + @psLogTypeName + ')'

						IF EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%There are no more endpoints available from the endpoint mapper%')
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectOSEventLogs'
											, 'There are no more endpoints available from the endpoint mapper.'

						IF EXISTS(SELECT * FROM #psOutput WHERE [xml] LIKE '%The RPC server is unavailable%')
							INSERT	INTO [dbo].[logServerAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
FROM [dbo].[logServerAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]=@eventDescriptor

/*-------------------------------------------------------------------------------------------------------------------------------*/
/* disable xp_cmdshell configuration option */
IF @optionXPHasChanged = 1
	EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
														@configOptionName	= 'xp_cmdshell',
														@configOptionValue	= 0,
														@optionIsAvailable	= @optionXPIsAvailable OUT,
														@optionCurrentValue	= @optionXPValue OUT,
														@optionHasChanged	= @optionXPHasChanged OUT,
														@executionLevel		= 3,
														@debugMode			= @debugMode

/* disable show advanced options configuration option */
IF @optionAdvancedHasChanged = 1
		EXEC [dbo].[usp_changeServerConfigurationOption]	@sqlServerName		= @@SERVERNAME,
															@configOptionName	= 'show advanced options',
															@configOptionValue	= 0,
															@optionIsAvailable	= @optionAdvancedIsAvailable OUT,
															@optionCurrentValue	= @optionAdvancedValue OUT,
															@optionHasChanged	= @optionAdvancedHasChanged OUT,
															@executionLevel		= 3,
															@debugMode			= @debugMode

/*-------------------------------------------------------------------------------------------------------------------------------*/
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
	FROM	[dbo].[reportHTMLOptions]
	WHERE	[name] = N'Database online admitted state'
			AND [module] = 'health-check'

	SET @configAdmittedState = ISNULL(@configAdmittedState, 'ONLINE, READ ONLY')
			
	-----------------------------------------------------------------------------------------------------
	BEGIN TRY
		SELECT	@configDBMaxSizeMaster = [value]
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
		FROM	[dbo].[reportHTMLOptions]
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
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						Disk Space Information
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 65536 = 65536)
						  THEN N'<A HREF="#DiskSpaceInformationCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 131072 = 131072)
						  THEN N'<A HREF="#DiskSpaceInformationPermissionErrors" class="summary-style color-1">Permission Errors {DiskSpaceInformationPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 16 = 16) 
				 THEN N'
				<TR VALIGN="TOP" class="color-2">
					<TD ALIGN=LEFT class="summary-style add-border color-2">
						Errorlog Messages
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 2097152 = 2097152)
						  THEN N'<A HREF="#ErrorlogMessagesCompleteDetails" class="summary-style color-2">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-2">' +
					CASE WHEN (@flgOptions & 524288 = 524288)
						  THEN N'<A HREF="#ErrorlogMessagesPermissionErrors" class="summary-style color-2">Permission Errors {ErrorlogMessagesPermissionErrorsCount}</A>'
						  ELSE N'Permission Errors;'
					END + N'
					</TD>
				</TR>'
				ELSE N''
			END + 
 			CASE WHEN (@flgActions & 32 = 32) 
				 THEN N'
				<TR VALIGN="TOP" class="color-1">
					<TD ALIGN=LEFT class="summary-style add-border color-1">
						OS Event Messages
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 134217728 = 134217728)
						  THEN N'<A HREF="#OSEventMessagesCompleteDetails" class="summary-style color-1">Complete Details</A>'
						  ELSE N'Complete Details'
					END + N'
					</TD>
					<TD ALIGN=CENTER class="summary-style add-border color-1">' +
					CASE WHEN (@flgOptions & 67108864 = 67108864)
						  THEN N'<A HREF="#OSEventMessagesPermissionErrors" class="summary-style color-1">Permission Errors {OSEventMessagesPermissionErrorsCount}</A>'
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
																	INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																	LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																	LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																					INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
															INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
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

			SELECT    @idx = COUNT(*)
			FROM [dbo].[vw_catalogInstanceNames]  cin
			INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
			WHERE	cin.[instance_active]=1
					AND cin.[project_id] = @projectID
					AND lsam.descriptor IN (N'dbo.usp_hcCollectDatabaseDetails')

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
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																							INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																							LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
								<TD class="small-size" COLLSPAN="6">job status not in (Succeded, In progress)</TD>
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
																							LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																			AND rsr.[rule_id] = 16
																																			AND rsr.[active] = 1
																																			AND (rsr.[skip_value]=ssajh.[instance_name])

																							WHERE	ssajh.[project_id]=@projectID
																									AND ssajh.[last_execution_status] NOT IN (1, 4) /* 1 = Succeded; 4 = In progress */
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
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 33554432
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value]=ssajh.[instance_name])
																						WHERE ssajh.[last_execution_status] = 4
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
																						INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
															INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
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
			INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
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
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 2048
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																							AND shcdd.[data_space_used_percent] <= @configDataSpaceMinPercent 
																							AND @configDataSpaceMinPercent<>0
																							AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb')
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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 32768
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																							AND shcdd.[log_space_used_percent] >= @configLogSpaceMaxPercent 
																							AND @configLogSpaceMaxPercent<>0
																							AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb')
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
																								LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																				AND rsr.[rule_id] = 4096
																																				AND rsr.[active] = 1
																																				AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																								WHERE cin.[instance_active]=1
																										AND cdn.[active]=1
																										AND cin.[project_id] = @projectID	
																										AND shcdd.[data_size_mb] <> 0
																										AND (shcdd.[log_size_mb] / shcdd.[data_size_mb] * 100.) > @configLogVsDataPercent
																										AND shcdd.[size_mb]>=@configDBMinSizeForAnalysis
																										AND cdn.[database_name] NOT IN ('master', 'msdb', 'model', 'tempdb')
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
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
			FROM	[dbo].[reportHTMLOptions]
			WHERE	[name] = N'Minimum Index Maintenance Frequency (days)'
					AND [module] = 'health-check'

			SET @minimumIndexMaintenanceFrequencyDays = ISNULL(@minimumIndexMaintenanceFrequencyDays, 2)

			-----------------------------------------------------------------------------------------------------
			SELECT	@analyzeOnlyMessagesFromTheLastHours = [value]
			FROM	[dbo].[reportHTMLOptions]
			WHERE	[name] = N'Analyze Only Messages from the last hours'
					AND [module] = 'health-check'

			SET @analyzeOnlyMessagesFromTheLastHours = ISNULL(@analyzeOnlyMessagesFromTheLastHours, 24)
	
			-----------------------------------------------------------------------------------------------------
			SELECT	@analyzeIndexMaintenanceOperation = [value]
			FROM	[dbo].[reportHTMLOptions]
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

			SELECT *
			INTO #filteredStatsIndexesFrequentlyFragmented
			FROM [dbo].[ufn_hcGetIndexesFrequentlyFragmented](@projectCode, @minimumIndexMaintenanceFrequencyDays, @analyzeOnlyMessagesFromTheLastHours, @analyzeIndexMaintenanceOperation) iff
			LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
															AND rsr.[rule_id] = 16777216
															AND rsr.[active] = 1
															AND (rsr.[skip_value]=iff.[instance_name])

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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																	AND rsr.[rule_id] = 8192
																																	AND rsr.[active] = 1
																																	AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																					WHERE cin.[instance_active]=1
																							AND cdn.[active]=1
																							AND cin.[project_id] = @projectID	
																							AND (
																									(    cdn.[database_name] NOT IN ('master', 'model', 'msdb') 
																										AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configUserDatabaseBACKUPAgeDays
																									)
																								    OR (    cdn.[database_name] IN ('master', 'model', 'msdb') 
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
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 16384
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																						WHERE cin.[instance_active]=1
																								AND cdn.[active]=1
																								AND cin.[project_id] = @projectID	
																								AND (
																										(    cdn.[database_name] NOT IN ('master', 'model', 'msdb') 
																											AND DATEDIFF(dd, shcdd.[last_dbcc checkdb_time], GETDATE()) > @configUserDBCCCHECKDBAgeDays
																										)
																										OR (    cdn.[database_name] IN ('master', 'model', 'msdb') 
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
																					INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
			LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
															AND rsr.[rule_id] = 1048576
															AND rsr.[active] = 1
															AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
			WHERE cin.[instance_active]=1
					AND cin.[project_id] = @projectID																							
					AND eld.[log_date] >= @dateTimeLowerLimit
					AND NOT EXISTS	( 
										SELECT 1
										FROM	[dbo].[catalogHardcodedFilters] chf 
										WHERE	chf.[module] = 'health-check'
												AND chf.[object_name] = 'dbo.statsSQLServerErrorlogDetails'
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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																						LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																					INNER JOIN [dbo].[vw_logServerAnalysisMessages] lsam ON lsam.[project_id] = cin.[project_id] AND lsam.[instance_id] = cin.[instance_id]
																					LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
																				LEFT JOIN [dbo].[reportHTMLSkipRules] rsr ON	rsr.[module] = 'health-check'
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
	INSERT INTO [dbo].[reportHTML](   [project_id], [module], [start_date], [flg_actions], [flg_options]
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
	SET @queryToRun=N'master.dbo.xp_cmdshell ''bcp "SELECT [html_content] FROM [' + DB_NAME() + '].[dbo].[reportHTML] WHERE [id]=' + CAST(@reportID AS [varchar]) + '" queryout ' + @reportFilePath + ' -c ' + CASE WHEN SERVERPROPERTY('InstanceName') IS NOT NULL THEN N'-S ' + @@SERVERNAME ELSE N'' END + N' -T'''
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
		UPDATE [dbo].[reportHTML]
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
			UPDATE [dbo].[reportHTML]
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



RAISERROR('Create procedure: [dbo].[usp_mpAlterTableForeignKeys]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableForeignKeys]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableForeignKeys]
		@SQLServerName		[sysname],
		@DBName				[sysname],
		@TableSchema		[sysname] = '%', 
		@TableName			[sysname] = '%',
		@ConstraintName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2049,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 06.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@ConstraintName	- specify constraint name to be enabled/disabled. default all
--		@flgAction:		 1	- Enable Constraints (default)
--						 0	- Disable Constraints
--		@flgOptions:	 1	- Use tables that have foreign key constraints that reffer current table (default)
--						 2	- Use tables that current table foreign key constraints reffer  
--						 4  - Enable constraints with NOCHECK. Default is to enable constraints using CHECK option
--						 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    				[nvarchar](max),
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@tmpSchemaName			[sysname],
			@tmpTableName			[sysname],
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@tmpConstraintName		[sysname],
			@errorCode				[int],
			@tmpFlgAction			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
				CREATE TABLE #tmpTableToAlterConstraints 
							(
								[TableSchema]		[sysname]
							  , [TableName]			[sysname]
							  , [ConstraintName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1	THEN 'Enable'
																ELSE 'Disable'
										END + ' foreign key constraints for: [' + @crtTableSchema + '].[' + @crtTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable foreign key constraint, will get only enabled constraints
						--if current action is to enable foreign key constraint, will get only disabled constraints
						IF (@flgOptions & 1 = 1)
							begin
								--list all tables that have foreign key constraints that reffers current table					
								SET @queryToRun=N'SELECT DISTINCT sch.[name] AS [schema_name], so.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM [' + @DBName + '].[sys].[objects] so
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN [' + @DBName + '].[sys].[foreign_keys]	sfk  ON so.[object_id] = sfk.[parent_object_id]
												INNER JOIN [' + @DBName + '].[sys].[objects]		so2  ON sfk.[referenced_object_id] = so2.[object_id]
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch2 ON sch2.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + @crtTableName + '''
														AND sch2.[name] = ''' + @crtTableSchema + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
														AND sfk.[name] LIKE ''' + @ConstraintName + ''''
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						IF (@flgOptions & 2 = 2)
							begin
								--list all tables that current table foreign key constraints reffers 
								SET @queryToRun='SELECT DISTINCT sch2.[name] AS [schema_name], so2.[name] AS [table_name], sfk.[name] AS [constraint_name]
												FROM [' + @DBName + '].[sys].[objects] so
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch  ON sch.[schema_id] = so.[schema_id]
												INNER JOIN [' + @DBName + '].[sys].[foreign_keys]	sfk ON so.[object_id] = sfk.[referenced_object_id]
												INNER JOIN [' + @DBName + '].[sys].[objects]		so2 ON sfk.[parent_object_id] = so2.[object_id]
												INNER JOIN [' + @DBName + '].[sys].[schemas]		sch2 ON sch.[schema_id] = so2.[schema_id]
												WHERE	so2.[name]=''' + @crtTableName + '''
														AND sch2.[name] = ''' + @crtTableSchema + '''
														AND sfk.[is_disabled]=' + CAST(@flgAction AS [varchar])+ '
														AND sfk.[name] LIKE ''' + @ConstraintName + ''''

								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								INSERT	INTO #tmpTableToAlterConstraints ([TableSchema], [TableName], [ConstraintName])
										EXEC (@queryToRun)
							end

						DECLARE crsTableToAlterConstraints CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TableSchema], [TableName], [ConstraintName]
																							FROM #tmpTableToAlterConstraints
																							ORDER BY [TableName]						
						OPEN crsTableToAlterConstraints
						FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= '[' + @tmpSchemaName + '].[' + @tmpTableName + ']'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								--enable/disable foreign key constraints
								SET @queryToRun='ALTER TABLE [' + @DBName + '].[' + @tmpSchemaName + '].[' + @tmpTableName + ']' + 
												CASE WHEN @flgAction=1	
													 THEN ' WITH ' + 
															CASE WHEN @flgOptions & 4 = 4	THEN 'NOCHECK'
																							ELSE 'CHECK'
															END + ' CHECK '	
													 ELSE ' NOCHECK '
												END + 'CONSTRAINT [' + @tmpConstraintName + ']'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								--
								SET @objectName = '[' + @tmpSchemaName + '].[' + @tmpTableName + ']'
								SET @childObjectName = QUOTENAME(@tmpConstraintName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableForeignKeys',
																				@eventName		= 'database maintenance - alter constraints',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								IF @errorCode=0	
									begin
										/* 0 disable FK -> insert action 1 */
										/* 1 enable FK  -> delete action 2 */
										SET @tmpFlgAction = CASE WHEN @flgAction=1 THEN 2 ELSE 1 END
										EXEC [dbo].[usp_mpMarkInternalAction]		@actionName			= N'foreign-key-made-disable',
																					@flgOperation		= @tmpFlgAction,
																					@server_name		= @SQLServerName,
																					@database_name		= @DBName,
																					@schema_name		= @tmpSchemaName,
																					@object_name		= @tmpTableName,
																					@child_object_name	= @tmpConstraintName
									end
						
								FETCH NEXT FROM crsTableToAlterConstraints INTO @tmpSchemaName, @tmpTableName, @tmpConstraintName
							end
						CLOSE crsTableToAlterConstraints
						DEALLOCATE crsTableToAlterConstraints
						
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		IF object_id('#tmpTableToAlterConstraints') IS NOT NULL DROP TABLE #tmpTableToAlterConstraints
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
	SET @errorCode = -1

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

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableIndexes]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableIndexes]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableIndexes]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableIndexes]
		@SQLServerName				[sysname],
		@DBName						[sysname],
		@TableSchema				[sysname] = '%',
		@TableName					[sysname] = '%',
		@IndexName					[sysname] = '%',
		@IndexID					[int],
		@PartitionNumber			[int] = 1,
		@flgAction					[tinyint] = 1,
		@flgOptions					[int] = 6145, --4096 + 2048 + 1	/* 6177 for space optimized index rebuild */
		@MaxDOP						[smallint] = 1,
		@FillFactor					[tinyint] = 0,
		@executionLevel				[tinyint] = 0,
		@affectedDependentObjects	[nvarchar](max) OUTPUT,
		@DebugMode					[bit] = 0
/* WITH ENCRYPTION */
AS


-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed.
--		@IndexName		- name of the index to be analyzed
--		@IndexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@PartitionNumber- index partition number. default value = 1 (index with no partitions)
--		@flgAction:		 1	- Rebuild index (default)
--						 2  - Reorganize indexes
--						 4	- Disable index
--		@flgOptions		 1  - Compact large objects (LOB) when reorganize  (default)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes
--						 8  - Disable non-clustered index before rebuild (save space) (won't apply when 4096 is applicable)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@strMessage				[nvarchar](4000),
			@sqlIndexCreate			[nvarchar](max),
			@sqlScriptOnline		[nvarchar](512),
			@objectName				[nvarchar](512),
			@childObjectName		[sysname],
			@crtTableSchema 		[sysname],
			@crtTableName 			[sysname],
			@crtIndexID				[int],
			@crtIndexName			[sysname],			
			@crtIndexType			[tinyint],
			@crtIndexAllowPageLocks	[bit],
			@crtIndexIsDisabled		[bit],
			@crtIndexIsPrimaryXML	[bit],
			@crtIndexHasDependentFK	[bit],
			@crtTableIsReplicated	[bit],
			@flgInheritOptions		[int],
			@tmpIndexName			[sysname],
			@tmpIndexIsPrimaryXML	[bit],
			@nestedExecutionLevel	[tinyint]

DECLARE   @flgRaiseErrorAndStop [bit]
		, @errorCode			[int]

DECLARE @DependentIndexes TABLE	(
									[index_name]		[sysname]	NULL
								  , [is_primary_xml]	[bit]		DEFAULT(0)
								)

SET NOCOUNT ON

DECLARE @tmpTableToAlterIndexes TABLE
			(
				[index_id]			[int]		NULL
			  , [index_name]		[sysname]	NULL
			  , [index_type]		[tinyint]	NULL
			  , [allow_page_locks]	[bit]		NULL
			  , [is_disabled]		[bit]		NULL
			  , [is_primary_xml]	[bit]		NULL
			  , [has_dependent_fk]	[bit]		NULL
			  , [is_replicated]		[bit]		NULL
			)


-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get configuration values
		---------------------------------------------------------------------------------------------
		DECLARE @queryLockTimeOut [int]
		SELECT	@queryLockTimeOut=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name] = 'Default lock timeout (ms)'
				AND [module] = 'common'

		---------------------------------------------------------------------------------------------		
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema] [sysname],
					[table_name] [sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME 
						FROM [' + @DBName + '].INFORMATION_SCHEMA.TABLES 
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + ''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + ''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT [table_schema], [table_name]
																	FROM #tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage=N'Alter indexes ON [' + @crtTableSchema + '].[' + @crtTableName + '] : ' + 
											CASE @flgAction WHEN 1 THEN 'REBUILD'
															WHEN 2 THEN 'REORGANIZE'
															WHEN 4 THEN 'DISABLE'
															ELSE 'N/A'
											END
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable/reorganize indexes, will get only enabled indexes
						--if current action is to rebuild, will get both enabled/disabled indexes
						SET @queryToRun = N''
						SET @queryToRun = @queryToRun + N'SELECT  si.[index_id]
														, si.[name]
														, si.[type]
														, si.[allow_page_locks]
														, si.[is_disabled]
														, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
														, CASE WHEN SUM(CASE WHEN fk.[name] IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS [has_dependent_fk]
														, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0) AS [is_replicated]
													FROM [' + @DBName + '].[sys].[indexes]				si
													INNER JOIN [' + @DBName + '].[sys].[objects]		so  ON so.[object_id] = si.[object_id]
													INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
													LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
													LEFT  JOIN [' + @DBName + '].[sys].[foreign_keys]	fk  ON fk.[referenced_object_id] = so.[object_id] AND fk.[key_index_id] = si.[index_id]
													LEFT  JOIN [' + @DBName + '].[sys].[tables]			st  ON st.[object_id] = so.[object_id]
													WHERE	so.[name] = ''' + @crtTableName + '''
															AND sch.[name] = ''' + @crtTableSchema + '''
															AND so.[is_ms_shipped] = 0' + 
															CASE	WHEN @IndexName IS NOT NULL 
																	THEN ' AND si.[name] LIKE ''' + @IndexName + ''''
																	ELSE CASE WHEN @IndexID  IS NOT NULL 
																			  THEN ' AND si.[index_id] = ' + CAST(@IndexID AS [nvarchar])
																			  ELSE ''
																		 END
															END + '
															AND si.[is_disabled] IN ( ' + CASE WHEN @flgAction IN (2, 4) THEN '0' ELSE '0,1' END + ')
													GROUP BY si.[index_id]
															, si.[name]
															, si.[type]
															, si.[allow_page_locks]
															, si.[is_disabled]
															, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END
															, ISNULL(st.[is_replicated], 0) | ISNULL(st.[is_merge_published], 0) | ISNULL(st.[is_published], 0)'

						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM @tmpTableToAlterIndexes
						INSERT	INTO @tmpTableToAlterIndexes([index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated])
								EXEC (@queryToRun)

						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList



				DECLARE crsTableToAlterIndexes CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [index_id], [index_name], [index_type], [allow_page_locks], [is_disabled], [is_primary_xml], [has_dependent_fk], [is_replicated]
																				FROM @tmpTableToAlterIndexes
																				ORDER BY [index_id], [index_name]						
				OPEN crsTableToAlterIndexes
				FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
				WHILE @@FETCH_STATUS=0
					begin
						SET @strMessage= '[' + @crtIndexName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @sqlScriptOnline=N''
						---------------------------------------------------------------------------------------------
						-- 1  - Rebuild indexes
						---------------------------------------------------------------------------------------------
						IF @flgAction = 1
							begin
								-- check for online operation mode	
								IF @flgOptions & 4096 = 4096
									begin
										SET @nestedExecutionLevel = @executionLevel + 3
										EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																						@dbName				= @DBName,
																						@tableSchema		= @crtTableSchema,
																						@tableName			= @crtTableName,
																						@indexName			= @crtIndexName,
																						@indexID			= @crtIndexID,
																						@partitionNumber	= @PartitionNumber,
																						@sqlScriptOnline	= @sqlScriptOnline OUT,
																						@flgOptions			= @flgOptions,
																						@executionLevel		= @nestedExecutionLevel,
																						@debugMode			= @DebugMode
									end

								---------------------------------------------------------------------------------------------
								--primary / unique index options
								-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
								IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
									AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) 
									AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
									begin
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @SQLServerName	= @SQLServerName
																					, @DBName			= @DBName
																					, @TableSchema		= @crtTableSchema
																					, @TableName		= @crtTableName
																					, @ConstraintName	= '%'
																					, @flgAction		= 0		-- Disable Constraints
																					, @flgOptions		= 1		-- Use tables that have foreign key constraints that reffers current table (default)
																					, @executionLevel	= @nestedExecutionLevel
																					, @DebugMode		= @DebugMode
									end

								---------------------------------------------------------------------------------------------
								--clustered/primary key index options
								IF @crtIndexType = 1
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										IF @flgOptions & 4 = 4
											begin
												--get all enabled non-clustered/xml/spatial indexes for current table
												SET @queryToRun = N''
												SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				, CASE WHEN xi.[type]=3 AND xi.[using_xml_index_id] IS NULL THEN 1 ELSE 0 END AS [is_primary_xml]
																			FROM [' + @DBName + '].[sys].[indexes]				si
																			INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																			INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																			LEFT  JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id] AND si.[type]=3
																			WHERE	so.[name] = ''' + @crtTableName + '''
																					AND sch.[name] = ''' + @crtTableSchema + ''' 
																					AND si.[type] in (2,3,4)
																					AND si.[is_disabled] = 0'
												SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
												IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

												INSERT INTO @DependentIndexes ([index_name], [is_primary_xml])
													EXEC (@queryToRun)
											end

										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
											begin
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT [index_name]
																				FROM @DependentIndexes
																				ORDER BY [is_primary_xml]
												OPEN crsNonClusteredIndexes
												FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
												WHILE @@FETCH_STATUS=0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																								, @DBName			= @DBName
																								, @TableSchema		= @crtTableSchema
																								, @TableName		= @crtTableName
																								, @IndexName		= @tmpIndexName
																								, @IndexID			= NULL
																								, @PartitionNumber	= DEFAULT
																								, @flgAction		= 4				--disable
																								, @flgOptions		= @flgOptions
																								, @executionLevel	= @nestedExecutionLevel
																								, @affectedDependentObjects = @affectedDependentObjects OUT
																								, @DebugMode		= @DebugMode										

														FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
													end
												CLOSE crsNonClusteredIndexes
												DEALLOCATE crsNonClusteredIndexes
											end
									end
								ELSE
									---------------------------------------------------------------------------------------------
									--xml primary key index options
									IF @crtIndexType = 3 AND @crtIndexIsPrimaryXML=1
										begin
											--4  - Rebuild all dependent indexes when rebuild primary indexes
											IF @flgOptions & 4 = 4
												begin
													--get all enabled secondary xml indexes for current table
													SET @queryToRun = N''
													SET @queryToRun = @queryToRun + N'SELECT  si.[name]
																				FROM [' + @DBName + '].[sys].[indexes]				si
																				INNER JOIN [' + @DBName + '].[sys].[objects]		so ON  si.[object_id] = so.[object_id]
																				INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON sch.[schema_id] = so.[schema_id]
																				INNER JOIN [' + @DBName + '].[sys].[xml_indexes]	xi  ON xi.[object_id] = si.[object_id] AND xi.[index_id] = si.[index_id]
																				WHERE	so.[name] = ''' + @crtTableName + '''
																						AND sch.[name] = ''' + @crtTableSchema + ''' 
																						AND si.[type] = 3
																						AND xi.[using_xml_index_id] = ''' + CAST(@crtIndexID AS [sysname]) + '''
																						AND si.[is_disabled] = 0'
													SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
													IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

													INSERT INTO @DependentIndexes ([index_name])
														EXEC (@queryToRun)
												end

											--8  - Disable non-clustered index before rebuild (save space)
											--won't disable the index when performing online rebuild
											IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
												begin
													DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																					SELECT [index_name]
																					FROM @DependentIndexes
													OPEN crsNonClusteredIndexes
													FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
													WHILE @@FETCH_STATUS=0
														begin
															SET @nestedExecutionLevel = @executionLevel + 2
															EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																									, @DBName			= @DBName
																									, @TableSchema		= @crtTableSchema
																									, @TableName		= @crtTableName
																									, @IndexName		= @tmpIndexName
																									, @IndexID			= NULL
																									, @PartitionNumber	= DEFAULT
																									, @flgAction		= 4				--disable
																									, @flgOptions		= @flgOptions
																									, @executionLevel	= @nestedExecutionLevel
																									, @affectedDependentObjects = @affectedDependentObjects OUT
																									, @DebugMode		= @DebugMode										

															FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName
														end
													CLOSE crsNonClusteredIndexes
													DEALLOCATE crsNonClusteredIndexes
												end
										end
									ELSE
										--8  - Disable non-clustered index before rebuild (save space)
										--won't disable the index when performing online rebuild										
										IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0
											begin
												SET @nestedExecutionLevel = @executionLevel + 2
												EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																						, @DBName			= @DBName
																						, @TableSchema		= @crtTableSchema
																						, @TableName		= @crtTableName
																						, @IndexName		= @crtIndexName
																						, @IndexID			= NULL
																						, @PartitionNumber	= @PartitionNumber
																						, @flgAction		= 4				--disable
																						, @flgOptions		= @flgOptions
																						, @executionLevel	= @nestedExecutionLevel
																						, @affectedDependentObjects = @affectedDependentObjects OUT
																						, @DebugMode		= @DebugMode										
										end

								---------------------------------------------------------------------------------------------
								/* FIX: Data corruption occurs in clustered index when you run online index rebuild in SQL Server 2012 or SQL Server 2014 https://support.microsoft.com/en-us/kb/2969896 */
								IF (@sqlScriptOnline = N'ONLINE = ON')
									begin
										--get destination server running version/edition
										DECLARE		@serverEdition					[sysname],
													@serverVersionStr				[sysname],
													@serverVersionNum				[numeric](9,6)

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
																				@serverEdition			= @serverEdition OUT,
																				@serverVersionStr		= @serverVersionStr OUT,
																				@serverVersionNum		= @serverVersionNum OUT,
																				@executionLevel			= @nestedExecutionLevel,
																				@debugMode				= @DebugMode
										
										IF     (@serverVersionNum >= 11.02100 AND @serverVersionNum < 11.03449) /* SQL Server 2012 RTM till SQL Server 2012 SP1 CU 11*/
											OR (@serverVersionNum >= 11.05058 AND @serverVersionNum < 11.05532) /* SQL Server 2012 SP2 till SQL Server 2012 SP2 CU 1*/
											OR (@serverVersionNum >= 12.02000 AND @serverVersionNum < 12.02370) /* SQL Server 2014 RTM CU 2*/
											begin
												SET @MaxDOP=1
											end
									end

								---------------------------------------------------------------------------------------------
								--generate rebuild index script
								SET @queryToRun = N''

								SET @queryToRun = @queryToRun + N'SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REBUILD'
					
								--rebuild options
								SET @queryToRun = @queryToRun + N' WITH (SORT_IN_TEMPDB = ON' + CASE WHEN ISNULL(@MaxDOP, 0) <> 0 THEN N', MAXDOP = ' + CAST(@MaxDOP AS [nvarchar]) ELSE N'' END + 
																						CASE WHEN ISNULL(@sqlScriptOnline, N'')<>N'' THEN N', ' + @sqlScriptOnline ELSE N'' END + 
																						CASE WHEN ISNULL(@FillFactor, 0) <> 0 THEN N', FILLFACTOR = ' + CAST(@FillFactor AS [nvarchar]) ELSE N'' END +
																N')'

								IF @PartitionNumber>1
									SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])

								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								IF @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON'))
									begin
										SET @strMessage=N'performing index rebuild'
										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @nestedExecutionLevel, @messageTreelevel = 1, @stopExecution=0
									end

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - rebuilding index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								
								IF @errorCode=0
									EXEC [dbo].[usp_mpMarkInternalAction]	@actionName			= N'index-made-disable',
																			@flgOperation		= 2,
																			@server_name		= @SQLServerName,
																			@database_name		= @DBName,
																			@schema_name		= @crtTableSchema,
																			@object_name		= @crtTableName,
																			@child_object_name	= @crtIndexName

								---------------------------------------------------------------------------------------------
								--rebuild dependent indexes
								--clustered / xml primary key index options
								IF (@crtIndexType = 1) OR (@crtIndexType = 3 AND @crtIndexIsPrimaryXML=1)
									begin
										--4  - Rebuild all dependent indexes when rebuild primary indexes
										--will rebuild only indexes disabled by this tool
										IF (@flgOptions & 4 = 4)
											begin											
												DECLARE crsNonClusteredIndexes	CURSOR LOCAL FAST_FORWARD FOR
																				SELECT DISTINCT di.[index_name], di.[is_primary_xml]
																				FROM @DependentIndexes di
																				LEFT JOIN [dbo].[statsMaintenancePlanInternals] smpi ON	smpi.[name]=N'index-made-disable'
																																		AND smpi.[server_name]=@SQLServerName
																																		AND smpi.[database_name]=@DBName
																																		AND smpi.[schema_name]=@crtTableSchema
																																		AND smpi.[object_name]=@crtTableName
																																		AND smpi.[child_object_name]=di.[index_name]
																				WHERE	(
																							/* index was disabled (option selected) and marked as disabled */
																							(@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name]=N'index-made-disable'
																						)
																						OR
																						(
																							/* index was not disabled (option selected) */
																							NOT (@flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtIndexIsDisabled=0 AND @crtTableIsReplicated=0) 
																							AND smpi.[name] IS NULL
																						)
																				ORDER BY di.[is_primary_xml] DESC
												OPEN crsNonClusteredIndexes
												FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
												WHILE @@FETCH_STATUS=0
													begin
														SET @nestedExecutionLevel = @executionLevel + 2
														EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName	= @SQLServerName
																								, @DBName			= @DBName
																								, @TableSchema		= @crtTableSchema
																								, @TableName		= @crtTableName
																								, @IndexName		= @tmpIndexName
																								, @IndexID			= NULL
																								, @PartitionNumber	= DEFAULT
																								, @flgAction		= 1		--rebuild
																								, @flgOptions		= @flgOptions
																								, @executionLevel	= @nestedExecutionLevel
																								, @affectedDependentObjects = @affectedDependentObjects OUT
																								, @DebugMode		= @DebugMode										

														FETCH NEXT FROM crsNonClusteredIndexes INTO @tmpIndexName, @tmpIndexIsPrimaryXML
													end
												CLOSE crsNonClusteredIndexes
												DEALLOCATE crsNonClusteredIndexes
											end
									end		

								---------------------------------------------------------------------------------------------
								-- must enable previous disabled constraints
								-- 16  - Disable foreign key constraints that reffer current table before rebuilding clustered/unique indexes
								IF @flgOptions & 16 = 16 AND @crtIndexHasDependentFK=1 
									AND @flgOptions & 8 = 8 AND NOT ((@flgOptions & 4096 = 4096) 
									AND (@sqlScriptOnline = N'ONLINE = ON')) AND @crtTableIsReplicated=0
									begin
										SET @flgInheritOptions = 1								-- Use tables that have foreign key constraints that reffers current table (default)

										--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
										IF @flgOptions & 64 = 64
											SET @flgInheritOptions = @flgInheritOptions + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

										SET @nestedExecutionLevel = @executionLevel + 2
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	  @SQLServerName	= @SQLServerName
																					, @DBName			= @DBName
																					, @TableSchema		= @crtTableSchema
																					, @TableName		= @crtTableName
																					, @ConstraintName	= '%'
																					, @flgAction		= 1		-- Enable Constraints
																					, @flgOptions		= @flgInheritOptions
																					, @executionLevel	= @nestedExecutionLevel
																					, @DebugMode		= @DebugMode
									end
							end

						---------------------------------------------------------------------------------------------
						-- 2  - Reorganize indexes
						---------------------------------------------------------------------------------------------
						-- avoid messages like:	The index [...] on table [..] cannot be reorganized because page level locking is disabled.		
						IF @flgAction = 2
							IF @crtIndexAllowPageLocks=1
								begin
									SET @queryToRun = N''
									SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
									SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] REORGANIZE'
				
									--  1  - Compact large objects (LOB) (default)
									IF @flgOptions & 1 = 1
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = ON) '
									ELSE
										SET @queryToRun = @queryToRun + N' WITH (LOB_COMPACTION = OFF) '
				
									IF @PartitionNumber>1
										SET @queryToRun = @queryToRun + N' PARTITION ' + CAST(@PartitionNumber AS [nvarchar])
									IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0


									SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
									SET @childObjectName = QUOTENAME(@crtIndexName)
									SET @nestedExecutionLevel = @executionLevel + 1

									EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																					@dbName			= @DBName,
																					@objectName		= @objectName,
																					@childObjectName= @childObjectName,
																					@module			= 'dbo.usp_mpAlterTableIndexes',
																					@eventName		= 'database maintenance - reorganize index',
																					@queryToRun  	= @queryToRun,
																					@flgOptions		= @flgOptions,
																					@executionLevel	= @nestedExecutionLevel,
																					@debugMode		= @DebugMode
								end
							ELSE
								begin
									SET @strMessage=N'--	index cannot be REORGANIZE because ALLOW_PAGE_LOCKS is set to OFF. Skipping...'
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
								end

						---------------------------------------------------------------------------------------------
						-- 4  - Disable indexes 
						---------------------------------------------------------------------------------------------
						IF @flgAction = 4
							begin
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @crtTableSchema + '].[' + @crtTableName + ']'') IS NOT NULL ALTER INDEX [' + @crtIndexName + '] ON [' + @crtTableSchema + '].[' + @crtTableName + '] DISABLE'
				
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtIndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableIndexes',
																				@eventName		= 'database maintenance - disable index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								/* 4 disable index -> insert action 1 */
								IF @errorCode=0
									EXEC [dbo].[usp_mpMarkInternalAction]	@actionName		= N'index-made-disable',
																			@flgOperation	= 1,
																			@server_name		= @SQLServerName,
																			@database_name		= @DBName,
																			@schema_name		= @crtTableSchema,
																			@object_name		= @crtTableName,
																			@child_object_name	= @crtIndexName
							end

						FETCH NEXT FROM crsTableToAlterIndexes INTO @crtIndexID, @crtIndexName, @crtIndexType, @crtIndexAllowPageLocks, @crtIndexIsDisabled, @crtIndexIsPrimaryXML, @crtIndexHasDependentFK, @crtTableIsReplicated
					end
				CLOSE crsTableToAlterIndexes
				DEALLOCATE crsTableToAlterIndexes
			end

		SET @affectedDependentObjects=N''
		SELECT @affectedDependentObjects = @affectedDependentObjects + N'[' + [index_name] + N'];'
		FROM @DependentIndexes
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
		SET @errorCode = -1

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

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableRebuildHeap]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableRebuildHeap]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableRebuildHeap]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableRebuildHeap]
		@SQLServerName		[sysname],
		@DBName				[sysname],
		@TableSchema		[sysname],
		@TableName			[sysname],
		@flgActions			[smallint] = 1,
		@flgOptions			[int] = 10264, --8192 + 2048 + 16 + 8
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2015
-- Module			 : Database Maintenance Scripts
-- ============================================================================
-- Change Date: 2015.03.04 / Andrei STEFAN
-- Description: heap tables with disabled unique indexes won't be rebuild
-----------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed.
--		@flgActions		- 1 - ALTER TABLE REBUILD (2k8+). If lower version is detected or error catched, will run CREATE CLUSTERED INDEX / DROP INDEX
--						- 2 - Rebuild table: copy records to a temp table, delete records from source, insert back records from source, rebuild non-clustered indexes
--		@flgOptions		 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable all foreign key constraints that reffered current table before rebuilding indexes (default)
--						64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					  2048  - send email when a error occurs (default)
--					  8192  - when rebuilding heaps, disable/enable table triggers (default)
--					 16384  - for versions below 2008, do heap rebuild using temporary clustered index
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

SET NOCOUNT ON

DECLARE		@queryToRun					[nvarchar](max),
			@objectName					[nvarchar](512),
			@CopyTableName				[sysname],
			@crtSchemaName				[sysname], 
			@crtTableName				[sysname], 
			@crtRecordCount				[int],
			@flgCopyMade				[bit],
			@flgErrorsOccured			[bit], 
			@nestExecutionLevel			[tinyint],
			@guid						[nvarchar](40),
			@affectedDependentObjects	[nvarchar](max),
			@flgOptionsNested			[int]


DECLARE		@flgRaiseErrorAndStop		[bit]
		  , @errorCode					[int]
		  

-----------------------------------------------------------------------------------------
DECLARE @tableGetRowCount TABLE	
		(
			[record_count]			[bigint]	NULL
		)

IF object_id('tempdb..#heapTableList') IS NOT NULL 
	DROP TABLE #heapTableList

CREATE TABLE #heapTableList		(
									[schema_name]			[sysname]	NULL,
									[table_name]			[sysname]	NULL,
									[record_count]			[bigint]	NULL
								)


SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 1

		---------------------------------------------------------------------------------------------
		--get configuration values
		---------------------------------------------------------------------------------------------
		DECLARE @queryLockTimeOut [int]
		SELECT	@queryLockTimeOut=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name]='Default lock timeout (ms)'
				AND [module] = 'common'
		
		---------------------------------------------------------------------------------------------
		--get destination server running version/edition
		DECLARE		@serverEdition					[sysname],
					@serverVersionStr				[sysname],
					@serverVersionNum				[numeric](9,6),
					@nestedExecutionLevel			[tinyint]

		SET @nestedExecutionLevel = @executionLevel + 1
		EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
												@serverEdition			= @serverEdition OUT,
												@serverVersionStr		= @serverVersionStr OUT,
												@serverVersionNum		= @serverVersionNum OUT,
												@executionLevel			= @nestedExecutionLevel,
												@debugMode				= @DebugMode

		---------------------------------------------------------------------------------------------
		--get current index/heap properties, filtering only the ones not empty
		--heap tables with disabled unique indexes will be excluded: rebuild means also index rebuild, and unique indexes may enable unwanted constraints
		SET @TableName = REPLACE(@TableName, '''', '''''')
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'	SELECT    sch.[name] AS [schema_name]
													, so.[name]  AS [table_name]
													, rc.[record_count]
											FROM [' + @DBName + '].[sys].[objects] so WITH (READPAST)
											INNER JOIN [' + @DBName + '].[sys].[schemas] sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id] 
											INNER JOIN [' + @DBName + '].[sys].[indexes] si WITH (READPAST) ON si.[object_id] = so.[object_id] 
											INNER  JOIN 
													(
														SELECT ps.object_id,
																SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [record_count]
														FROM [' + @DBName + '].[sys].[dm_db_partition_stats] ps WITH (READPAST)
														GROUP BY ps.object_id		
													)rc ON rc.[object_id] = so.[object_id] 
											WHERE   so.[name] LIKE ''' + @TableName + '''
												AND sch.[name] LIKE ''' + @TableSchema + '''
												AND so.[is_ms_shipped] = 0
												AND si.[index_id] = 0
												AND rc.[record_count]<>0
												AND NOT EXISTS(
																SELECT *
																FROM [' + @DBName + '].sys.indexes si_unq
																WHERE si_unq.[object_id] = so.[object_id] 
																		AND si_unq.[is_disabled]=1
																		AND si_unq.[is_unique]=1
															  )'
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #heapTableList
		INSERT INTO #heapTableList ([schema_name], [table_name], [record_count])
			EXEC (@queryToRun)


		---------------------------------------------------------------------------------------------
		DECLARE crsTableListToRebuild CURSOR LOCAL READ_ONLY FOR	SELECT [schema_name], [table_name], [record_count] 
																	FROM #heapTableList
																	ORDER BY [schema_name], [table_name]
 		OPEN crsTableListToRebuild
		FETCH NEXT FROM crsTableListToRebuild INTO @crtSchemaName, @crtTableName, @crtRecordCount
		WHILE @@FETCH_STATUS=0
			begin
				SET @objectName = '[' + @crtSchemaName + '].[' + @crtTableName + ']'
				SET @queryToRun=N'Rebuilding heap ON ' + @objectName
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
			
				SET @flgErrorsOccured=0
				
				IF @flgActions=1
					begin
						IF @serverVersionNum >= 10
							begin
								SET @queryToRun= 'Running ALTER TABLE REBUILD...'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; ';
								SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''' + @objectName + ''') IS NOT NULL ALTER TABLE ' + @objectName + N' REBUILD'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								
							end

						IF (@flgOptions & 16384 = 16384) AND (@serverVersionNum < 10 OR @flgErrorsOccured=1)
							begin
								------------------------------------------------------------------------------------------------------------------------
								--disable table non-clustered indexes
								IF @flgOptions & 8 = 8
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										EXEC [dbo].[usp_mpAlterTableIndexes]	@SQLServerName				= @SQLServerName,
																				@DBName						= @DBName,
																				@TableSchema				= @crtSchemaName,
																				@TableName					= @crtTableName,
																				@IndexName					= '%',
																				@IndexID					= NULL,
																				@PartitionNumber			= 1,
																				@flgAction					= 4,
																				@flgOptions					= DEFAULT,
																				@MaxDOP						= 1,
																				@executionLevel				= @nestExecutionLevel,
																				@affectedDependentObjects	= @affectedDependentObjects OUT,
																				@debugMode					= @DebugMode
									end

								------------------------------------------------------------------------------------------------------------------------
								--disable table constraints
								IF @flgOptions & 16 = 16
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										SET @flgOptionsNested = 3 + (@flgOptions & 2048)
										EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName		= @SQLServerName ,
																					@DBName				= @DBName,
																					@TableSchema		= @crtSchemaName, 
																					@TableName			= @crtTableName,
																					@ConstraintName		= '%',
																					@flgAction			= 0,
																					@flgOptions			= @flgOptionsNested,
																					@executionLevel		= @nestExecutionLevel,
																					@debugMode			= @DebugMode
									end

								SET @guid = CAST(NEWID() AS [nvarchar](38))

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Add a new temporary column [bigint]'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'ALTER TABLE [' + @DBName + N'].' + @objectName + N' ADD [' + @guid + N'] [bigint] IDENTITY'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Create a temporary clustered index'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N' CREATE CLUSTERED INDEX [PK_' + @guid + N'] ON [' + @DBName + N'].' + @objectName + N' ([' + @guid + N'])'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Drop the temporary clustered index'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'DROP INDEX [PK_' + @guid + N'] ON [' + @DBName + N'].' + @objectName 
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1

								--------------------------------------------------------------------------------------------------------
								SET @queryToRun= 'Drop the temporary column'
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun = N'ALTER TABLE [' + @DBName + N'].' + @objectName + N' DROP COLUMN [' + @guid + N']'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 3
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																				@eventName		= 'database maintenance - rebuilding heap',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
								IF @errorCode<>0 SET @flgErrorsOccured=1								

								---------------------------------------------------------------------------------------------------------
								--rebuild table non-clustered indexes
								IF @flgOptions & 8 = 8
									begin
										SET @nestExecutionLevel = @executionLevel + 1

										EXEC [dbo].[usp_mpAlterTableIndexes]	@SQLServerName				= @SQLServerName,
																				@DBName						= @DBName,
																				@TableSchema				= @crtSchemaName,
																				@TableName					= @crtTableName,
																				@IndexName					= '%',
																				@IndexID					= NULL,
																				@PartitionNumber			= 1,
																				@flgAction					= 1,
																				@flgOptions					= 6165,
																				@MaxDOP						= 1,
																				@executionLevel				= @nestExecutionLevel, 
																				@affectedDependentObjects	= @affectedDependentObjects OUT,
																				@debugMode					= @DebugMode
									end

								---------------------------------------------------------------------------------------------------------
								--enable table constraints
								IF @flgOptions & 16 = 16
									begin
										SET @nestExecutionLevel = @executionLevel + 1
										SET @flgOptionsNested = 3 + (@flgOptions & 2048)
	
										--64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
										IF @flgOptions & 64 = 64
											SET @flgOptionsNested = @flgOptionsNested + 4		-- Enable constraints with NOCHECK. Default is to enable constraints using CHECK option

										EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName		= @SQLServerName ,
																					@DBName				= @DBName,
																					@TableSchema		= @crtSchemaName, 
																					@TableName			= @crtTableName,
																					@ConstraintName		= '%',
																					@flgAction			= 1,
																					@flgOptions			= @flgOptionsNested,
																					@executionLevel		= @nestExecutionLevel, 
																					@debugMode			= @DebugMode
									end
							end
					end

				-- 2 - Rebuild table: copy records to a temp table, delete records from source, insert back records from source, rebuild non-clustered indexes
				IF @flgActions=2
					begin
						SET @CopyTableName=@crtTableName + 'RebuildCopy'

						SET @queryToRun= 'Total Rows In Table To Be Exported To Temporary Storage: ' + CAST(@crtRecordCount AS [varchar](20))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

						SET @flgCopyMade=0
						--------------------------------------------------------------------------------------------------------
						--dropping copy table, if exists
						--------------------------------------------------------------------------------------------------------
						SET @queryToRun = 'IF EXISTS (	SELECT * 
														FROM [' + @DBName + '].[sys].[objects] so
														INNER JOIN [' + @DBName + '].[sys].[schemas] sch ON so.[schema_id] = sch.[schema_id]
														WHERE	sch.[name] = ''' + @crtSchemaName + ''' 
																AND so.[name] = ''' + @CopyTableName + '''
													) 
											DROP TABLE [' + @DBName + '].[' + @crtSchemaName + '].[' + @CopyTableName + ']'
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																		@dbName			= @DBName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																		@eventName		= 'database maintenance - rebuilding heap',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @DebugMode
				
						--------------------------------------------------------------------------------------------------------
						--create a copy of the source table
						--------------------------------------------------------------------------------------------------------
						SET @queryToRun = 'SELECT * INTO [' + @DBName + '].[' + @crtSchemaName + '].[' + @CopyTableName + '] FROM [' + @DBName + '].' + @objectName 
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						SET @nestedExecutionLevel = @executionLevel + 1
						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																		@dbName			= @DBName,
																		@objectName		= @objectName,
																		@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																		@eventName		= 'database maintenance - rebuilding heap',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @DebugMode

						IF @errorCode = 0
							SET @flgCopyMade=1
				
						IF @flgCopyMade=1
							begin
								--------------------------------------------------------------------------------------------------------
								SET @queryToRun = N''
								SET @queryToRun = @queryToRun + N'	SELECT    rc.[record_count]
																	FROM [' + @DBName + '].[sys].[objects] so WITH (READPAST)
																	INNER JOIN [' + @DBName + '].[sys].[schemas] sch WITH (READPAST) ON sch.[schema_id] = so.[schema_id] 
																	INNER JOIN [' + @DBName + '].[sys].[indexes] si WITH (READPAST) ON si.[object_id] = so.[object_id] 
																	INNER  JOIN 
																			(
																				SELECT ps.object_id,
																						SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [record_count]
																				FROM [' + @DBName + '].[sys].[dm_db_partition_stats] ps WITH (READPAST)
																				GROUP BY ps.object_id		
																			)rc ON rc.[object_id] = so.[object_id] 
																	WHERE   so.[name] LIKE ''' + @CopyTableName + '''
																		AND sch.[name] LIKE ''' + @crtSchemaName + '''
																		AND si.[index_id] = 0'
								SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								DELETE FROM @tableGetRowCount
								INSERT INTO @tableGetRowCount([record_count])
									EXEC (@queryToRun)
							
								SELECT TOP 1 @crtRecordCount=[record_count] FROM @tableGetRowCount
								SET @queryToRun= '--	Total Rows In Temporary Storage Table After Export: ' + CAST(@crtRecordCount AS varchar(20))
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0


								--------------------------------------------------------------------------------------------------------
								--rebuild source table
								SET @nestExecutionLevel=@executionLevel + 2
								EXEC @flgErrorsOccured = [dbo].[usp_mpTableDataSynchronizeInsert]	@sourceServerName		= @SQLServerName,
																									@sourceDB				= @DBName,			
																									@sourceTableSchema		= @crtSchemaName,
																									@sourceTableName		= @CopyTableName,
																									@destinationServerName	= @SQLServerName,
																									@destinationDB			= @DBName,			
																									@destinationTableSchema	= @crtSchemaName,		
																									@destinationTableName	= @crtTableName,		
																									@flgActions				= 3,
																									@flgOptions				= @flgOptions,
																									@allowDataLoss			= 0,
																									@executionLevel			= @nestExecutionLevel,
																									@DebugMode				= @DebugMode
						
								--------------------------------------------------------------------------------------------------------
								--dropping copy table
								--------------------------------------------------------------------------------------------------------
								IF @flgErrorsOccured=0
									begin
										SET @queryToRun = 'IF EXISTS (	SELECT * 
																		FROM [' + @DBName + '].[sys].[objects] so
																		INNER JOIN [' + @DBName + '].[sys].[schemas] sch ON so.[schema_id] = sch.[schema_id]
																		WHERE	sch.[name] = ''' + @crtSchemaName + ''' 
																				AND so.[name] = ''' + @CopyTableName + '''
																	) 
															DROP TABLE [' + @DBName + '].[' + @crtSchemaName + '].[' + @CopyTableName + ']'
										IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																						@dbName			= @DBName,
																						@objectName		= @objectName,
																						@module			= 'dbo.usp_mpAlterTableRebuildHeap',
																						@eventName		= 'database maintenance - rebuilding heap',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @DebugMode
									end
							end
					end

				FETCH NEXT FROM crsTableListToRebuild INTO @crtSchemaName, @crtTableName, @crtRecordCount
			end
		CLOSE crsTableListToRebuild
		DEALLOCATE crsTableListToRebuild
	
		----------------------------------------------------------------------------------
		IF object_id('#tmpRebuildTableList') IS NOT NULL DROP TABLE #tmpRebuildTableList
		IF OBJECT_ID('#heapTableIndexList') IS NOT NULL DROP TABLE #heapTableIndexList
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
	SET @errorCode = -1

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

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpAlterTableTriggers]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpAlterTableTriggers]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpAlterTableTriggers]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpAlterTableTriggers]
		@SQLServerName		[sysname],
		@DBName				[sysname],
		@TableSchema		[sysname] = '%', 
		@TableName			[sysname] = '%',
		@TriggerName		[sysname] = '%',
		@flgAction			[bit] = 1,
		@flgOptions			[int] = 2048,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 2004-2009
-- Module			 : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed. default = %, all tables will be analyzed
--		@flgAction:		 1	- Enable Triggers (default)
--						 0	- Disable Triggers
--		@flgOptions:	 8  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					  2048  - send email when a error occurs (default)
--		@DebugMode:		 1 - print dynamic SQL statements 
--						 0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun   				[nvarchar](max),
			@objectName				[varchar](512),
			@childObjectName		[sysname],
			@crtTableSchema			[sysname],
			@crtTableName 			[sysname],
			@crtTriggerName			[sysname],
			@errorCode				[int],
			@tmpFlgOptions			[smallint],
			@nestedExecutionLevel	[tinyint]

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @errorCode	 = 0

		---------------------------------------------------------------------------------------------
		--get tables list	
		IF object_id('tempdb..#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
		CREATE TABLE #tmpTableList 
				(
					[table_schema]	[sysname],
					[table_name]	[sysname]
				)

		SET @queryToRun = N'SELECT TABLE_SCHEMA, TABLE_NAME FROM [' + @DBName + N'].INFORMATION_SCHEMA.TABLES
						WHERE	TABLE_TYPE = ''BASE TABLE'' 
								AND TABLE_NAME LIKE ''' + @TableName + N''' 
								AND TABLE_SCHEMA LIKE ''' + @TableSchema + N''''
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #tmpTableList ([table_schema], [table_name])
				EXEC (@queryToRun)

		---------------------------------------------------------------------------------------------
		IF EXISTS(SELECT 1 FROM #tmpTableList)
			begin
				IF object_id('tempdb..#tmpTableToAlterTriggers') IS NOT NULL DROP TABLE #tmpTableToAlterTriggers
				CREATE TABLE #tmpTableToAlterTriggers 
							(
								[TriggerName]	[sysname]
							)

				DECLARE crsTableList CURSOR LOCAL FAST_FORWARD FOR	SELECT	[table_schema], [table_name]
																	FROM	#tmpTableList
																	ORDER BY [table_schema], [table_name]
				OPEN crsTableList
				FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName

				WHILE @@FETCH_STATUS=0
					begin
						SET @queryToRun= CASE WHEN @flgAction=1  THEN 'Enable'
																ELSE 'Disable'
										END + ' triggers for: [' + @crtTableSchema + N'].[' + @crtTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

						--if current action is to disable triggers, will get only enabled triggers
						--if current action is to enable triggers, will get only disabled triggers
						SET @queryToRun=N'SELECT DISTINCT st.[name]
									FROM [' + @DBName + '].[sys].[triggers] st
									INNER JOIN [' + @DBName + '].[sys].[objects] so ON so.[object_id] = st.[parent_id] 
									INNER JOIN [' + @DBName + '].[sys].[schemas] sch ON sch.[schema_id] = so.[schema_id] 
									WHERE	so.[name]=''' + @crtTableName + '''
											AND sch.[name] = ''' + @crtTableSchema + '''
											AND st.[is_disabled]=' + CAST(@flgAction AS [varchar]) + '
											AND st.[is_ms_shipped] = 0
											AND st.[name] LIKE ''' + @TriggerName + ''''
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

						TRUNCATE TABLE #tmpTableToAlterTriggers
						INSERT	INTO #tmpTableToAlterTriggers([TriggerName])
								EXEC (@queryToRun)
								
						DECLARE crsTableToAlterTriggers CURSOR	LOCAL FAST_FORWARD FOR	SELECT DISTINCT [TriggerName]
																						FROM #tmpTableToAlterTriggers
																						ORDER BY [TriggerName]
						OPEN crsTableToAlterTriggers
						FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
						WHILE @@FETCH_STATUS=0
							begin
								SET @queryToRun= @crtTriggerName
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

								SET @queryToRun=N'ALTER TABLE [' + @DBName + N'].[' + @crtTableSchema + N'].[' + @crtTableName + '] ' + 
													CASE WHEN @flgAction=1  THEN N'ENABLE'
																			ELSE N'DISABLE'
													END + N' TRIGGER [' + @crtTriggerName + ']'
								IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

								--
								SET @objectName = '[' + @crtTableSchema + '].[' + @crtTableName + ']'
								SET @childObjectName = QUOTENAME(@crtTriggerName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpAlterTableTriggers',
																				@eventName		= 'database maintenance - alter triggers',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

								FETCH NEXT FROM crsTableToAlterTriggers INTO @crtTriggerName
							end
						CLOSE crsTableToAlterTriggers
						DEALLOCATE crsTableToAlterTriggers
											
						FETCH NEXT FROM crsTableList INTO @crtTableSchema, @crtTableName
					end
				CLOSE crsTableList
				DEALLOCATE crsTableList
			end

		---------------------------------------------------------------------------------------------
		--delete all temporary tables
		IF object_id('#tmpTableList') IS NOT NULL DROP TABLE #tmpTableList
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
	SET @errorCode = -1

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

RETURN @errorCode
GO


RAISERROR('Create procedure: [dbo].[usp_mpGetIndexCreationScript]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sys.objects 
	     WHERE object_id = OBJECT_ID(N'[dbo].[usp_mpGetIndexCreationScript]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpGetIndexCreationScript]
		@SQLServerName		[sysname]=@@SERVERNAME,
		@DBName				[sysname],
		@TableSchema		[sysname]='dbo',
		@TableName			[sysname],
		@IndexName			[sysname],
		@IndexID			[int],
		@flgOptions			[int] = 4099,
		@sqlIndexCreate		[nvarchar](max) OUTPUT,
		@executionLevel		[tinyint] = 0,
		@DebugMode			[bit] = 0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date: 07.01.2010
-- Module     : Database Maintenance Scripts
-- ============================================================================

-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify table name to be analyzed
--		@IndexName		- name of the index to be analyzed
--		@IndexID		- id of the index to be analyzed. to may specify either index name or id. 
--						  if you specify both, index name will be taken into consideration
--		@flgOptions:	1 - get also indexes that are created by a table constraint (primary or unique key) (default)
--						2 - use drop existing to recreate the index (default)
--					 4096 - use ONLINE=ON, if applicable (default)
--		@DebugMode:		1 - print dynamic SQL statements 
--						0 - no statements will be displayed (default)
-----------------------------------------------------------------------------------------
-- Output Parameters:
--		@sqlIndexCreate	- sql statement that will create the index
-----------------------------------------------------------------------------------------
-- Return : 
-- 1 : Succes  -1 : Fail 
-----------------------------------------------------------------------------------------

DECLARE @queryToRun			[nvarchar](max),
		@sqlIndexInclude	[nvarchar](max),
		@sqlIndexWithClause [nvarchar](max),
		@sqlScriptOnline	[nvarchar](512),
		@crtIndexName		[sysname],
		@IndexType			[tinyint],
		@FillFactor			[tinyint],
		@IsUniqueConstraint	[int],
		@IsPadded			[int],
		@AllowRowLocks		[int],
		@AllowPageLocks		[int],
		@IgnoreDupKey		[int],
		@KeyOrdinal			[int],
		@IndexColumnID		[int],
		@IsIncludedColumn	[bit],
		@IsDescendingKey	[bit],
		@ColumnName			[sysname],
		@FileGroupName		[sysname],
		@ReturnValue		[int],
		@nestExecutionLevel	[tinyint]

DECLARE @IndexDetails TABLE	(
								[IndexName]			[sysname]	NULL,
								[IndexType]			[tinyint]	NULL,
								[FillFactor]		[tinyint]	NULL,
								[FileGroupName]		[sysname]	NULL,
								[IsUniqueConstraint][bit]		NULL,
								[IsPadded]			[bit]		NULL,
								[AllowRowLocks]		[bit]		NULL,
								[AllowPageLocks]	[bit]		NULL,
								[IgnoreDupKey]		[bit]		NULL
							)

DECLARE @IndexColumnDetails TABLE
							(
								[KeyOrdinal]		[int]		NULL,
								[IndexColumnID]		[int]		NULL,
								[IsIncludedColumn]	[bit]		NULL,
								[IsDescendingKey]	[bit]		NULL,
								[ColumnName]		[sysname]	NULL
							)

SET NOCOUNT ON

-- { sql_statement | statement_block }
BEGIN TRY
		SET @ReturnValue	 = 1

		--get current index properties
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT  idx.[name]
										, idx.[type]
										, idx.[fill_factor]
										, dSp.[name] AS [file_group_name]
										, idx.[is_unique]
										, idx.[is_padded]
										, idx.[allow_row_locks]
										, idx.[allow_page_locks]
										, idx.[ignore_dup_key]
									FROM [' + @DBName + '].[sys].[indexes]				idx
									INNER JOIN [' + @DBName + '].[sys].[objects]		obj ON  idx.[object_id] = obj.[object_id]
									INNER JOIN [' + @DBName + '].[sys].[schemas]		sch ON	sch.[schema_id] = obj.[schema_id]
									INNER JOIN [' + @DBName + '].[sys].[data_spaces]	dSp	ON  idx.[data_space_id] = dSp.[data_space_id]
									WHERE	obj.[name] = ''' + @TableName + '''
											AND sch.[name] = ''' + @TableSchema + '''' + 
											CASE	WHEN @IndexName IS NOT NULL 
													THEN ' AND idx.[name] = ''' + @IndexName + ''''
													ELSE ' AND idx.[index_id] = ' + CAST(@IndexID AS [nvarchar])
											END + 
											CASE WHEN @flgOptions & 1 <> 1
												 THEN '	AND NOT EXISTS	(
																			SELECT 1
																			FROM [' + @DBName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																			WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																					AND [CONSTRAINT_CATALOG]=''' + @DBName + '''
																					AND [TABLE_NAME]=''' + @TableName + '''
																					AND [TABLE_SCHEMA] = ''' + @TableSchema + '''
																					AND [CONSTRAINT_NAME]=''' + @IndexName + '''
																		)'
												ELSE ''
											END
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexDetails
		INSERT INTO @IndexDetails ([IndexName], [IndexType], [FillFactor], [FileGroupName], [IsUniqueConstraint], [IsPadded], [AllowRowLocks], [AllowPageLocks], [IgnoreDupKey])
			EXEC (@queryToRun)

		--get index fill factor and file group
		SELECT	  @crtIndexName		= ISNULL(@IndexName, [IndexName])
				, @IndexType		= [IndexType]
				, @FillFactor		= [FillFactor]
				, @FileGroupName	= [FileGroupName]
				, @IsUniqueConstraint = [IsUniqueConstraint]
				, @IsPadded			= [IsPadded]
				, @AllowRowLocks	= [AllowRowLocks]
				, @AllowPageLocks	= [AllowPageLocks]
				, @IgnoreDupKey		= [IgnoreDupKey]
		FROM @IndexDetails
		
		--get current index key columns and include columns and their properties
		SET @queryToRun = N''
		SET @queryToRun = @queryToRun + N'SELECT    
										  idxCol.[key_ordinal]
										, idxCol.[index_column_id]
										, idxCol.[is_included_column]
										, idxCol.[is_descending_key]
										, col.[name] AS [column_name]
								FROM [' + @DBName + '].[sys].[indexes] idx
								INNER JOIN [' + @DBName + '].[sys].[index_columns] idxCol ON	idx.[object_id] = idxCol.[object_id]
																								AND idx.[index_id] = idxCol.[index_id]
								INNER JOIN [' + @DBName + '].[sys].[columns]		 col	ON	idxCol.[object_id] = col.[object_id]
																								AND idxCol.[column_id] = col.[column_id]
								INNER JOIN [' + @DBName + '].[sys].[objects]		 obj	ON  idx.[object_id] = obj.[object_id]
								INNER JOIN [' + @DBName + '].[sys].[schemas]		 sch	ON	sch.[schema_id] = obj.[schema_id]
								WHERE	obj.[name] = ''' + @TableName + '''
										AND sch.[name] = ''' + @TableSchema + '''' + 
										CASE	WHEN @IndexName IS NOT NULL 
												THEN ' AND idx.[name] = ''' + @IndexName + ''''
												ELSE ' AND idx.[index_id] = ' + CAST(@IndexID AS [nvarchar])
										END + 
										CASE WHEN @flgOptions & 1 <> 1
											 THEN '	AND NOT EXISTS	(
																		SELECT 1
																		FROM [' + @DBName + '].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS]
																		WHERE [CONSTRAINT_TYPE]=''PRIMARY KEY''
																				AND [CONSTRAINT_CATALOG]=''' + @DBName + '''
																				AND [TABLE_NAME]=''' + @TableName + '''
																				AND [TABLE_SCHEMA]=''' + @TableSchema + '''
																				AND [CONSTRAINT_NAME]=''' + @IndexName + '''
																	)'
											ELSE ''
										END
		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		DELETE FROM @IndexColumnDetails
		INSERT INTO @IndexColumnDetails ([KeyOrdinal], [IndexColumnID], [IsIncludedColumn], [IsDescendingKey], [ColumnName])
			EXEC (@queryToRun)

		SET @sqlIndexCreate=N''
		IF EXISTS (SELECT 1 FROM @IndexColumnDetails)
			begin
				-- check for online operation mode, for reorganize/rebuild
				SET @nestExecutionLevel = @executionLevel + 1
				EXEC [dbo].[usp_mpCheckIndexOnlineOperation]	@sqlServerName		= @SQLServerName,
																@dbName				= @DBName,
																@tableSchema		= @TableSchema,
																@tableName			= @TableName,
																@indexName			= @IndexName,
																@indexID			= @IndexID,
																@partitionNumber	= 1,
																@sqlScriptOnline	= @sqlScriptOnline OUT,
																@flgOptions			= @flgOptions,
																@executionLevel		= @nestExecutionLevel,
																@debugMode			= @DebugMode

				SET @sqlIndexCreate = @sqlIndexCreate + N'CREATE'
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IsUniqueConstraint=1	
																THEN ' UNIQUE' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 CASE	WHEN @IndexType=1	
																THEN ' CLUSTERED' 
																ELSE ''
														 END 
				SET @sqlIndexCreate = @sqlIndexCreate +	 ' INDEX [' + @crtIndexName + '] ON [' + @TableSchema + '].[' + @TableName + '] ('
				--index key columns
				DECLARE crsIndexKey CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName], [IsDescendingKey]
																	FROM @IndexColumnDetails
																	WHERE [IsIncludedColumn] = 0
																	ORDER BY [KeyOrdinal]
				OPEN crsIndexKey
				FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexCreate = @sqlIndexCreate + '[' + @ColumnName + ']' + 
												CASE WHEN @IsDescendingKey=1	THEN ' DESC'
																				ELSE '' END + ', '
						FETCH NEXT FROM crsIndexKey INTO @ColumnName, @IsDescendingKey
					end
				CLOSE  crsIndexKey
				DEALLOCATE crsIndexKey
				IF LEN(@sqlIndexCreate)<>0
					SET @sqlIndexCreate = SUBSTRING(@sqlIndexCreate, 1, LEN(@sqlIndexCreate)-1) + ')'

				--index include columns
				SET @sqlIndexInclude = N''
				DECLARE crsIndexInclude CURSOR LOCAL FAST_FORWARD FOR	SELECT [ColumnName]
																		FROM @IndexColumnDetails
																		WHERE [IsIncludedColumn] = 1
																		ORDER BY [IndexColumnID]
				OPEN crsIndexInclude
				FETCH NEXT FROM crsIndexInclude INTO @ColumnName
				WHILE @@FETCH_STATUS=0
					begin
						SET @sqlIndexInclude = @sqlIndexInclude + '[' + @ColumnName + '], '
						FETCH NEXT FROM crsIndexInclude INTO @ColumnName
					end
				CLOSE  crsIndexInclude
				DEALLOCATE crsIndexInclude
				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexInclude = SUBSTRING(@sqlIndexInclude, 1, LEN(@sqlIndexInclude)-1)


				IF LEN(@sqlIndexInclude)<>0
					SET @sqlIndexCreate = @sqlIndexCreate + N' INCLUDE(' + @sqlIndexInclude + ')'

				--index options
				SET @sqlIndexWithClause = N''
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'PAD_INDEX = ' + CASE WHEN @IsPadded=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_ROW_LOCKS = ' + CASE WHEN @AllowRowLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'ALLOW_PAGE_LOCKS = ' + CASE WHEN @AllowPageLocks=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'IGNORE_DUP_KEY = ' + CASE WHEN @IgnoreDupKey=1 THEN 'ON' ELSE 'OFF' END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + @sqlScriptOnline
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'SORT_IN_TEMPDB = ON'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'STATISTICS_NORECOMPUTE = OFF'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + N'MAXDOP = 1'
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE WHEN @FillFactor<>0	
											 THEN CASE	WHEN LEN(@sqlIndexWithClause)>0 
														THEN ', '
														ELSE ''
												  END + N'FILLFACTOR=' + CAST(@FillFactor AS [nvarchar])
											 ELSE ''
										END
				SET @sqlIndexWithClause = @sqlIndexWithClause + 
										CASE	WHEN LEN(@sqlIndexWithClause)>0 
												THEN ', '
												ELSE ''
									    END + 
										CASE WHEN @flgOptions & 2 = 2 
											 THEN N'DROP_EXISTING = ON'
											 ELSE ''
										END
				--index storage filegroup
				SET @sqlIndexCreate = @sqlIndexCreate + 
										CASE WHEN LEN(@sqlIndexWithClause)>0
											 THEN N' WITH (' + @sqlIndexWithClause + ')'
											 ELSE ''
										END + N' ON [' + @FileGroupName + ']'
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


RAISERROR('Create procedure: [dbo].[usp_mpDatabaseOptimize]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (
	    SELECT * 
	      FROM sysobjects 
	     WHERE [id] = OBJECT_ID(N'[dbo].[usp_mpDatabaseOptimize]') 
	       AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_mpDatabaseOptimize]
GO

-----------------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[usp_mpDatabaseOptimize]
		@SQLServerName				[sysname]=@@SERVERNAME,
		@DBName						[sysname],
		@TableSchema				[sysname]	=   '%',
		@TableName					[sysname]   =   '%',
		@flgActions					[smallint]	=    27,
		@flgOptions					[int]		= 45697,--32768 + 8192 + 4096 + 512 + 128 + 1
		@DefragIndexThreshold		[smallint]	=     5,
		@RebuildIndexThreshold		[smallint]	=    30,
		@PageThreshold				[int]		=  1000,
		@RebuildIndexPageCountLimit	[int]		= 2147483647,	--16TB/no limit
		@StatsSamplePercent			[smallint]	=   100,
		@StatsAgeDays				[smallint]	=     7,
		@StatsChangePercent			[smallint]	=     1,
		@MaxDOP						[smallint]	=	  1,
		@MaxRunningTimeInMinutes	[smallint]	=     0,
		@executionLevel				[tinyint]	=     0,
		@DebugMode					[bit]		=     0
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 08.01.2010
-- Module			 : Database Maintenance Plan 
-- Description		 : Optimization and Maintenance Checks
-- ============================================================================
-----------------------------------------------------------------------------------------
-- Input Parameters:
--		@SQLServerName	- name of SQL Server instance to analyze
--		@DBName			- database to be analyzed
--		@TableSchema	- schema that current table belongs to
--		@TableName		- specify % for all tables or a table name to be analyzed
--		@flgActions		 1	- Defragmenting database tables indexes (ALTER INDEX REORGANIZE)				(default)
--							  should be performed daily
--						 2	- Rebuild heavy fragmented indexes (ALTER INDEX REBUILD)						(default)
--							  should be performed daily
--					     4  - Rebuild all indexes (ALTER INDEX REBUILD)
--						 8  - Update statistics for table (UPDATE STATISTICS)								(default)
--							  should be performed daily
--						16  - Rebuild heap tables (SQL versions +2K5 only)									(default)
--		@flgOptions		 1  - Compact large objects (LOB) (default)
--						 2  - 
--						 4  - Rebuild all dependent indexes when rebuild primary indexes (default)
--						 8  - Disable non-clustered index before rebuild (save space) (default)
--						16  - Disable foreign key constraints that reffer current table before rebuilding with disable clustered/unique indexes
--						32  - Stop execution if an error occurs. Default behaviour is to print error messages and continue execution
--					    64  - When enabling foreign key constraints, do no check values. Default behaviour is to enabled foreign key constraint with check option
--					   128  - Create statistics on index columns only (default). Default behaviour is to not create statistics on all eligible columns
--					   256  - Create statistics using default sample scan. Default behaviour is to create statistics using fullscan mode
--					   512  - update auto-created statistics (default)
--					  1024	- get index statistics using DETAILED analysis (default is to use LIMITED)
--							  for heaps, will always use DETAILED in order to get page density and forwarded records information
--					  4096  - rebuild/reorganize indexes using ONLINE=ON, if applicable (default)
--					  8192  - when rebuilding heaps, disable/enable table triggers (default)
--					 16384  - for versions below 2008, do heap rebuild using temporary clustered index
--					 32768  - analyze only tables with at least @PageThreshold pages reserved (+2k5 only)
--					 65536  - cleanup of ghost records (sp_clean_db_free_space)
--							- this may be forced by setting to true property 'Force cleanup of ghost records'

--		@DefragIndexThreshold		- min value for fragmentation level when to start reorganize it
--		@@RebuildIndexThreshold		- min value for fragmentation level when to start rebuild it
--		@PageThreshold				- the minimum number of pages for an index to be reorganized/rebuild
--		@RebuildIndexPageCountLimit	- the maximum number of page for an index to be rebuild. if index has more pages than @RebuildIndexPageCountLimit, it will be reorganized
--		@StatsSamplePercent			- value for sample percent when update statistics. if 100 is present, then fullscan will be used
--		@StatsAgeDays				- when statistics were last updated (stats ages); don't update statistics more recent then @StatsAgeDays days
--		@StatsChangePercent			- for more recent statistics, if percent of changes is greater of equal, perform update
--		@MaxDOP						- when applicable, use this MAXDOP value (ex. index rebuild)
--		@MaxRunningTimeInMinutes	- the number of minutes the optimization job will run. after time exceeds, it will exist. 0 or null means no limit
--		@DebugMode					- 1 - print dynamic SQL statements / 0 - no statements will be displayed
-----------------------------------------------------------------------------------------

DECLARE		@queryToRun    					[nvarchar](4000),
			@CurrentTableSchema				[sysname],
			@CurrentTableName 				[sysname],
			@objectName						[nvarchar](512),
			@childObjectName				[sysname],
			@IndexName						[sysname],
			@IndexTypeDesc					[sysname],
			@IndexType						[tinyint],
			@IndexFillFactor				[tinyint],
			@DatabaseID						[int], 
			@IndexID						[int],
			@ObjectID						[int],
			@CurrentFragmentation			[numeric] (6,2),
			@CurentPageDensityDeviation		[numeric] (6,2),
			@CurrentPageCount				[bigint],
			@CurrentForwardedRecordsPercent	[numeric] (6,2),
			@errorCode						[int],
			@ClusteredRebuildNonClustered	[bit],
			@flgInheritOptions				[int],
			@statsCount						[int], 
			@nestExecutionLevel				[tinyint],
			@analyzeIndexType				[nvarchar](32),
			@eventData						[varchar](8000),
			@affectedDependentObjects		[nvarchar](4000),
			@indexIsRebuilt					[bit],
			@stopTimeLimit					[datetime]

SET NOCOUNT ON

---------------------------------------------------------------------------------------------
--determine when to stop current optimization task, based on @MaxRunningTimeInMinutes value
---------------------------------------------------------------------------------------------
IF ISNULL(@MaxRunningTimeInMinutes, 0)=0
	SET @stopTimeLimit = CONVERT([datetime], '9999-12-31 23:23:59', 120)
ELSE
	SET @stopTimeLimit = DATEADD(minute, @MaxRunningTimeInMinutes, GETDATE())


---------------------------------------------------------------------------------------------
--get configuration values
---------------------------------------------------------------------------------------------
DECLARE @queryLockTimeOut [int]
SELECT	@queryLockTimeOut=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Default lock timeout (ms)'
		AND [module] = 'common'

-----------------------------------------------------------------------------------------
--get configuration values: Force cleanup of ghost records
---------------------------------------------------------------------------------------------
DECLARE   @forceCleanupGhostRecords [nvarchar](128)
		, @thresholdGhostRecords	[bigint]

SELECT	@forceCleanupGhostRecords=[value] 
FROM	[dbo].[appConfigurations] 
WHERE	[name]='Force cleanup of ghost records'
		AND [module] = 'maintenance-plan'

SET @forceCleanupGhostRecords = LOWER(ISNULL(@forceCleanupGhostRecords, 'false'))

--run index statistics using DETAILED option
IF LOWER(@forceCleanupGhostRecords)='true' AND @flgOptions & 1024 = 0
	SET @flgOptions = @flgOptions + 1024

--enable local cleanup of ghost records option
IF LOWER(@forceCleanupGhostRecords)='true' AND @flgOptions & 65536 = 0
	SET @flgOptions = @flgOptions + 65536

IF LOWER(@forceCleanupGhostRecords)='true' OR @flgOptions & 65536 = 65536
	begin
		SELECT	@thresholdGhostRecords=[value] 
		FROM	[dbo].[appConfigurations] 
		WHERE	[name]='Ghost records cleanup threshold'
				AND [module] = 'maintenance-plan'
	end

SET @thresholdGhostRecords = ISNULL(@thresholdGhostRecords, 0)

---------------------------------------------------------------------------------------------
--get SQL Server running major version and database compatibility level
---------------------------------------------------------------------------------------------
--get destination server running version/edition
DECLARE		@serverEdition					[sysname],
			@serverVersionStr				[sysname],
			@serverVersionNum				[numeric](9,6),
			@nestedExecutionLevel			[tinyint]

SET @nestedExecutionLevel = @executionLevel + 1
EXEC [dbo].[usp_getSQLServerVersion]	@sqlServerName			= @SQLServerName,
										@serverEdition			= @serverEdition OUT,
										@serverVersionStr		= @serverVersionStr OUT,
										@serverVersionNum		= @serverVersionNum OUT,
										@executionLevel			= @nestedExecutionLevel,
										@debugMode				= @DebugMode
---------------------------------------------------------------------------------------------

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
	SET @queryToRun = @queryToRun + N'SELECT [compatibility_level] FROM sys.databases WHERE [name] = ''' + @DBName + N''''
ELSE
	SET @queryToRun = @queryToRun + N'SELECT [cmptlevel] FROM master.dbo.sysdatabases WHERE [name] = ''' + @DBName + N''''

SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

INSERT	INTO #databaseCompatibility([compatibility_level])
		EXEC (@queryToRun)

SELECT TOP 1 @compatibilityLevel = [compatibility_level] FROM #databaseCompatibility

IF @serverVersionNum >= 9 AND @compatibilityLevel<=80
	SET @serverVersionNum = 8

---------------------------------------------------------------------------------------------

SET @errorCode				 = 0
SET @CurrentTableSchema		 = @TableSchema

IF ISNULL(@DefragIndexThreshold, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF ISNULL(@RebuildIndexThreshold, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Threshold value for rebuilding indexes should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF ISNULL(@StatsSamplePercent, 0)=0 
	begin
		SET @queryToRun=N'ERROR: Sample percent value for update statistics sample should be greater than 0.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end

IF @DefragIndexThreshold > @RebuildIndexThreshold
	begin
		SET @queryToRun=N'ERROR: Threshold value for defragmenting indexes should be smalller or equal to threshold value for rebuilding indexes.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=1
		RETURN 1
	end


---------------------------------------------------------------------------------------------
--create temporary tables that will be used 
---------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
CREATE TABLE #CurrentIndexFragmentationStats 
		(	
			[ObjectName] 					[varchar] (255),
			[ObjectId] 						[int],
			[IndexName] 					[varchar] (255),
			[IndexId] 						[int],
			[Level] 						[int],
			[Pages]		 					[int],
			[Rows] 							[bigint],
			[MinimumRecordSize]				[int],
			[MaximumRecordSize]				[int],
			[AverageRecordSize] 			[int],
			[ForwardedRecords] 				[int],
			[Extents] 						[int],
			[ExtentSwitches] 				[int],
			[AverageFreeBytes] 				[int],
			[AveragePageDensity] 			[decimal](38,2),
			[ScanDensity] 					[decimal](38,2),
			[BestCount] 					[int],
			[ActualCount] 					[int],
			[LogicalFragmentation] 			[decimal](38,2),
			[ExtentFragmentation] 			[decimal](38,2),
			[ghost_record_count]			[bigint]		NULL
		)	
			
CREATE INDEX IX_CurrentIndexFragmentationStats ON #CurrentIndexFragmentationStats([ObjectId], [IndexId])


---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 
	DROP TABLE #databaseObjectsWithIndexList

CREATE TABLE #databaseObjectsWithIndexList(
											[database_id]					[int],
											[object_id]						[int],
											[table_schema]					[sysname],
											[table_name]					[sysname],
											[index_id]						[int],
											[index_name]					[sysname]	NULL,													
											[index_type]					[tinyint],
											[fill_factor]					[tinyint]	NULL,
											[is_rebuilt]					[bit]		NOT NULL DEFAULT (0),
											[page_count]					[bigint]	NULL,
											[avg_fragmentation_in_percent]	[decimal](38,2)	NULL,
											[ghost_record_count]			[bigint]	NULL,
											[forwarded_records_percentage]	[decimal](38,2)	NULL,
											[page_density_deviation]		[decimal](38,2)	NULL
											)
CREATE INDEX IX_databaseObjectsWithIndexList_TableName ON #databaseObjectsWithIndexList([table_schema], [table_name], [index_id], [avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])
CREATE INDEX IX_databaseObjectsWithIndexList_LogicalDefrag ON #databaseObjectsWithIndexList([avg_fragmentation_in_percent], [page_count], [index_type], [is_rebuilt])

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#databaseObjectsWithStatisticsList') IS NOT NULL 
	DROP TABLE #databaseObjectsWithStatisticsList

CREATE TABLE #databaseObjectsWithStatisticsList(
												[database_id]			[int],
												[object_id]				[int],
												[table_schema]			[sysname],
												[table_name]			[sysname],
												[stats_id]				[int],
												[stats_name]			[sysname],													
												[auto_created]			[bit],
												[rows]					[bigint]		NULL,
												[modification_counter]	[bigint]		NULL,
												[last_updated]			[datetime]		NULL,
												[percent_changes]		[decimal](38,2)	NULL
												)


---------------------------------------------------------------------------------------------
EXEC [dbo].[usp_logPrintMessage] @customMessage = '<separator-line>', @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

--------------------------------------------------------------------------------------------------
--16 - get current heap tables list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'0'

		SET @queryToRun=N'Create list of heap tables to be analyzed...' + @DBName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		SET @queryToRun = @queryToRun + 
							N'SELECT DISTINCT 
										DB_ID(''' + @DBName + ''') AS [database_id]
									, si.[object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[index_id]
									, si.[name] AS [index_name]
									, si.[type] AS [index_type]
									, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
							FROM [' + @DBName + '].[sys].[indexes]				si
							INNER JOIN [' + @DBName + '].[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
							INNER JOIN [' + @DBName + '].[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
							CASE WHEN @flgOptions & 32768 = 32768 
								THEN N'
							INNER JOIN
									(
											SELECT   [object_id]
												, SUM([reserved_page_count]) as [reserved_page_count]
											FROM [' + @DBName + '].sys.dm_db_partition_stats
											GROUP BY [object_id]
											HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
									) ps ON ps.[object_id] = ob.[object_id]'
								ELSE N''
								END + N'
							WHERE	ob.[name] LIKE ''' + @TableName + '''
									AND sc.[name] LIKE ''' + @TableSchema + '''
									AND si.[type] IN (' + @analyzeIndexType + N')
									AND ob.[type] IN (''U'', ''V'')'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor])
				EXEC (@queryToRun)
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))

			
--------------------------------------------------------------------------------------------------
--1/2	- Analyzing heap tables fragmentation
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Analyzing heap fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun='[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + CASE WHEN @IndexName IS NOT NULL THEN N' - [' + @IndexName + ']' ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @queryToRun=N'SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
											, ips.[object_id]
											, si.[name] as index_name
											, ips.[index_id]
											, ips.[avg_fragmentation_in_percent]
											, ips.[page_count]
											, ips.[record_count]
											, ips.[forwarded_record_count]
											, ips.[avg_record_size_in_bytes]
											, ips.[avg_page_space_used_in_percent]
											, ips.[ghost_record_count]
									FROM [' + @DBName + '].sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
													'DETAILED'
											+ ''') ips
									INNER JOIN [' + @DBName + '].sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
									WHERE	si.[type] IN (' + @analyzeIndexType + N')'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
				IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
				INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count])  
						EXEC (@queryToRun)

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[LogicalFragmentation]
				, doil.[page_count] = cifs.[Pages]
				, doil.[ghost_record_count] = cifs.[ghost_record_count]
				, doil.[forwarded_records_percentage] = CASE WHEN ISNULL(cifs.[Rows], 0) > 0 
															 THEN (CAST(cifs.[ForwardedRecords] AS decimal(29,2)) / CAST(cifs.[Rows]  AS decimal(29,2))) * 100
															 ELSE 0
														END
				, doil.[page_density_deviation] =  ABS(CASE WHEN ISNULL(cifs.[AverageRecordSize], 0) > 0
															THEN ((FLOOR(8060. / ISNULL(cifs.[AverageRecordSize], 0)) * ISNULL(cifs.[AverageRecordSize], 0)) / 8060.) * doil.[fill_factor] - cifs.[AveragePageDensity]
															ELSE 0
													   END)
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #CurrentIndexFragmentationStats cifs ON cifs.[ObjectId] = doil.[object_id] AND cifs.[IndexId] = doil.[index_id]
	end


--------------------------------------------------------------------------------------------------
-- 16	- Rebuild heap tables (SQL versions +2K5 only)
-- implemented an algoritm based on Tibor Karaszi's one: http://sqlblog.com/blogs/tibor_karaszi/archive/2014/03/06/how-often-do-you-rebuild-your-heaps.aspx
-- rebuilding heaps also rebuild its non-clustered indexes. do heap maintenance before index maintenance
--------------------------------------------------------------------------------------------------
IF (@flgActions & 16 = 16) AND (@serverVersionNum >= 9) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database heap tables...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsTableList CURSOR FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[forwarded_records_percentage]
		   									FROM	#databaseObjectsWithIndexList doil
											WHERE	(    doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
													  OR doil.[forwarded_records_percentage] >= @DefragIndexThreshold
													  OR doil.[page_density_deviation] >= @RebuildIndexThreshold
													)
													AND doil.[index_type] IN (0)
											ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @objectName, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   		SET @queryToRun=N'Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density deviation = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar])
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

				--------------------------------------------------------------------------------------------------
				--log heap fragmentation information
				SET @eventData='<heap-fragmentation><detail>' + 
									'<database_name>' + @DBName + '</database_name>' + 
									'<object_name>' + @objectName + '</object_name>'+ 
									'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
									'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
									'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
									'<forwarded_records_percentage>' + CAST(@CurrentForwardedRecordsPercent AS [varchar](32)) + '</forwarded_records_percentage>' + 
								'</detail></heap-fragmentation>'

				EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
													@dbName			= @DBName,
													@objectName		= @objectName,
													@module			= 'dbo.usp_mpDatabaseOptimize',
													@eventName		= 'database maintenance - rebuilding heap',
													@eventMessage	= @eventData,
													@eventType		= 0 /* info */

				--------------------------------------------------------------------------------------------------
				SET @nestExecutionLevel = @executionLevel + 3
				EXEC [dbo].[usp_mpAlterTableRebuildHeap]	@SQLServerName		= @SQLServerName,
															@DBName				= @DBName,
															@TableSchema		= @CurrentTableSchema,
															@TableName			= @CurrentTableName,
															@flgActions			= 1,
															@flgOptions			= @flgOptions,
															@executionLevel		= @nestExecutionLevel,
															@DebugMode			= @DebugMode

				--mark heap as being rebuilt
				UPDATE doil
					SET [is_rebuilt]=1
				FROM	#databaseObjectsWithIndexList doil 
	   			WHERE	doil.[table_name] = @CurrentTableName
	   					AND doil.[table_schema] = @CurrentTableSchema
						AND doil.[index_type] = 0
				
				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @CurrentForwardedRecordsPercent
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4 - get current index list: clustered, non-clustered, xml, spatial
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @analyzeIndexType=N'1,2,3,4'		

		SET @queryToRun=N'Create list of indexes to be analyzed...' + @DBName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF @serverVersionNum >= 9
			SET @queryToRun = @queryToRun + 
								N'SELECT DISTINCT 
										  DB_ID(''' + @DBName + ''') AS [database_id]
										, si.[object_id]
										, sc.[name] AS [table_schema]
										, ob.[name] AS [table_name]
										, si.[index_id]
										, si.[name] AS [index_name]
										, si.[type] AS [index_type]
										, CASE WHEN si.[fill_factor] = 0 THEN 100 ELSE si.[fill_factor] END AS [fill_factor]
								FROM [' + @DBName + '].[sys].[indexes]				si
								INNER JOIN [' + @DBName + '].[sys].[objects]		ob	ON ob.[object_id] = si.[object_id]
								INNER JOIN [' + @DBName + '].[sys].[schemas]		sc	ON sc.[schema_id] = ob.[schema_id]' +
								CASE WHEN @flgOptions & 32768 = 32768 
									THEN N'
								INNER JOIN
										(
											 SELECT   [object_id]
													, SUM([reserved_page_count]) as [reserved_page_count]
											 FROM [' + @DBName + '].sys.dm_db_partition_stats
											 GROUP BY [object_id]
											 HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
										) ps ON ps.[object_id] = ob.[object_id]'
									ELSE N''
									END + N'
								WHERE	ob.[name] LIKE ''' + @TableName + '''
										AND sc.[name] LIKE ''' + @TableSchema + '''
										AND si.[type] IN (' + @analyzeIndexType + N')
										AND si.[is_disabled]=0
										AND ob.[type] IN (''U'', ''V'')'
		ELSE
			SET @queryToRun = @queryToRun + 
								N'SELECT DISTINCT 
									  DB_ID(''' + @DBName + ''') AS [database_id]
									, si.[id] AS [object_id]
									, sc.[name] AS [table_schema]
									, ob.[name] AS [table_name]
									, si.[indid] AS [index_id]
									, si.[name] AS [index_name]
									, CASE WHEN si.[indid]=1 THEN 1 ELSE 2 END AS [index_type]
									, CASE WHEN ISNULL(si.[OrigFillFactor], 0) = 0 THEN 100 ELSE si.[OrigFillFactor] END AS [fill_factor]
								FROM [' + @DBName + ']..sysindexes si
								INNER JOIN [' + @DBName + ']..sysobjects ob	ON ob.[id] = si.[id]
								INNER JOIN [' + @DBName + ']..sysusers sc	ON sc.[uid] = ob.[uid]
								WHERE	ob.[name] LIKE ''' + @TableName + '''
										AND sc.[name] LIKE ''' + @TableSchema + '''
										AND si.[status] & 64 = 0 
										AND si.[status] & 8388608 = 0 
										AND si.[status] & 16777216 = 0 
										AND si.[indid] > 0
										AND ob.[xtype] IN (''U'', ''V'')'

		SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithIndexList([database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor])
				EXEC (@queryToRun)
	end


UPDATE #databaseObjectsWithIndexList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [index_name] = LTRIM(RTRIM([index_name]))



--------------------------------------------------------------------------------------------------
--8	- get current statistics list
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Create list of statistics to be analyzed...' + @DBName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		SET @queryToRun = N''				

		IF @serverVersionNum >= 9 
			begin
				IF (@serverVersionNum >= 10.504000 AND @serverVersionNum < 11) OR @serverVersionNum >= 11.03000
					/* starting with SQL Server 2008 R2 SP2 / SQL Server 2012 SP1 */
					SET @queryToRun = @queryToRun + 
										N'USE [' + @DBName + ']; SELECT DISTINCT 
												  DB_ID(''' + @DBName + ''') AS [database_id]
												, ss.[object_id]
												, sc.[name] AS [table_schema]
												, ob.[name] AS [table_name]
												, ss.[stats_id]
												, ss.[name] AS [stats_name]
												, ss.[auto_created]
												, sp.[last_updated]
												, sp.[rows]
												, ABS(sp.[modification_counter]) AS [modification_counter]
												, (ABS(sp.[modification_counter]) * 100. / sp.[rows]) AS [percent_changes]
										FROM [' + @DBName + '].sys.stats ss
										INNER JOIN [' + @DBName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @DBName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]' +
										CASE WHEN @flgOptions & 32768 = 32768 
											THEN N'
										INNER JOIN
												(
													 SELECT   [object_id]
															, SUM([reserved_page_count]) as [reserved_page_count]
													 FROM [' + @DBName + '].sys.dm_db_partition_stats
													 GROUP BY [object_id]
													 HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
												) ps ON ps.[object_id] = ob.[object_id]'
											ELSE N''
											END + N'
										CROSS APPLY [' + @DBName + '].sys.dm_db_stats_properties(ss.object_id, ss.stats_id) AS sp
										WHERE	ob.[name] LIKE ''' + @TableName + '''
												AND sc.[name] LIKE ''' + @TableSchema + '''
												AND sp.[rows] > 0
												AND (    (    DATEDIFF(dd, sp.[last_updated], GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0
														 )
													 OR  
														 ( 
															  DATEDIFF(dd, sp.[last_updated], GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N' 
														  AND sp.[modification_counter] <> 0 
														  AND (ABS(sp.[modification_counter]) * 100. / sp.[rows]) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
														 )
													)'
				ELSE
					/* SQL Server 2005 up to SQL Server 2008 R2 SP 2*/
					SET @queryToRun = @queryToRun + 
										N'USE [' + @DBName + ']; SELECT DISTINCT 
												  DB_ID(''' + @DBName + ''') AS [database_id]
												, ss.[object_id]
												, sc.[name] AS [table_schema]
												, ob.[name] AS [table_name]
												, ss.[stats_id]
												, ss.[name] AS [stats_name]
												, ss.[auto_created]
												, STATS_DATE(si.[id], si.[indid]) AS [last_updated]
												, si.[rowcnt] AS [rows]
												, ABS(si.[rowmodctr]) AS [modification_counter]
												, (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) AS [percent_changes]
										FROM [' + @DBName + '].sys.stats ss
										INNER JOIN [' + @DBName + '].sys.objects ob	ON ob.[object_id] = ss.[object_id]
										INNER JOIN [' + @DBName + '].sys.schemas sc	ON sc.[schema_id] = ob.[schema_id]
										INNER JOIN [' + @DBName + ']..sysindexes si ON si.[id] = ob.[object_id] AND si.[name] = ss.[name]' +
										CASE WHEN @flgOptions & 32768 = 32768 
											THEN N'
										INNER JOIN
												(
													 SELECT   [object_id]
															, SUM([reserved_page_count]) as [reserved_page_count]
													 FROM [' + @DBName + '].sys.dm_db_partition_stats
													 GROUP BY [object_id]
													 HAVING SUM([reserved_page_count]) >=' + CAST(@PageThreshold AS [nvarchar](32)) + N'
												) ps ON ps.[object_id] = ob.[object_id]'
											ELSE N''
											END + N'
										WHERE	ob.[name] LIKE ''' + @TableName + '''
												AND sc.[name] LIKE ''' + @TableSchema + '''
												AND si.[rowcnt] > 0
												AND (    (    DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
														  AND si.[rowmodctr] <> 0
														 )
													 OR  
														( 
													 		  DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
														  AND si.[rowmodctr] <> 0 
														  AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
														)
												)'
			end
		ELSE
			/* SQL Server 2000 */
			SET @queryToRun = @queryToRun + 
								N'USE [' + @DBName + ']; SELECT DISTINCT 
										  DB_ID(''' + @DBName + ''') AS [database_id]
										, si.[id] AS [object_id]
										, sc.[name] AS [table_schema]
										, ob.[name] AS [table_name]
										, si.[indid] AS [stats_id]
										, si.[name] AS [stats_name]
										, CASE WHEN si.[status] & 8388608 <> 0 THEN 1 ELSE 0 END AS [auto_created]
										, STATS_DATE(si.[id], si.[indid]) AS [last_updated]
										, si.[rowcnt] AS [rows]
										, ABS(si.[rowmodctr]) AS [modification_counter]
										, (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) AS [percent_changes]
									FROM [' + @DBName + ']..sysindexes si
									INNER JOIN [' + @DBName + ']..sysobjects ob	ON ob.[id] = si.[id]
									INNER JOIN [' + @DBName + ']..sysusers sc	ON sc.[uid] = ob.[uid]
									WHERE	ob.[name] LIKE ''' + @TableName + '''
											AND sc.[name] LIKE ''' + @TableSchema + '''
											AND si.[indid] > 0 
											AND si.[indid] < 255
											AND ob.[xtype] <> ''S''
											AND si.[rowcnt] > 0
											AND (    (    DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) >= ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
													  AND si.[rowmodctr] <> 0
													 )
												 OR  
													( 
													 	  DATEDIFF(dd, STATS_DATE(si.[id], si.[indid]), GETDATE()) < ' + CAST(@StatsAgeDays AS [nvarchar](32)) + N'
													  AND si.[rowmodctr] <> 0 
													  AND (ABS(si.[rowmodctr]) * 100. / si.[rowcnt]) >= ' + CAST(@StatsChangePercent AS [nvarchar](32)) + N'
													)
											)'

		IF @SQLServerName<>@@SERVERNAME
			SET @queryToRun = N'SELECT x.* FROM OPENQUERY([' + @SQLServerName + N'], ''EXEC [' + @DBName + N'].sys.sp_executesql N''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''''')x'


		IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0

		INSERT	INTO #databaseObjectsWithStatisticsList([database_id], [object_id], [table_schema], [table_name], [stats_id], [stats_name], [auto_created], [last_updated], [rows], [modification_counter], [percent_changes])
				EXEC (@queryToRun)
	end

UPDATE #databaseObjectsWithStatisticsList 
		SET   [table_schema] = LTRIM(RTRIM([table_schema]))
			, [table_name] = LTRIM(RTRIM([table_name]))
			, [stats_name] = LTRIM(RTRIM([stats_name]))

IF @flgOptions & 32768 = 32768
	SET @flgOptions = @flgOptions - 32768


--------------------------------------------------------------------------------------------------
--1/2	- Analyzing tables fragmentation
--		fragmentation information for the data and indexes of the specified table or view
--------------------------------------------------------------------------------------------------
IF ((@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4))  AND (GETDATE() <= @stopTimeLimit)
	begin

		SET @queryToRun='Analyzing index fragmentation...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT [database_id], [object_id], [table_schema], [table_name], [index_id], [index_name], [index_type], [fill_factor]
																	FROM #databaseObjectsWithIndexList																	
																	WHERE [index_type] <> 0 /* exclude heaps */
																	ORDER BY [table_schema], [table_name], [index_id]
		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun='[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + CASE WHEN @IndexName IS NOT NULL THEN N' - [' + @IndexName + ']' ELSE N' (heap)' END
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				IF @serverVersionNum < 9	/* SQL 2000 */
					begin
						IF @SQLServerName=@@SERVERNAME
							SET @queryToRun='USE [' + @DBName + N']; IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC SHOWCONTIG (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'', ''' + @IndexName + ''' ) WITH ' + CASE WHEN @flgOptions & 1024 = 1024 THEN '' ELSE 'FAST,' END + ' TABLERESULTS, NO_INFOMSGS'
						ELSE
							SET @queryToRun='SELECT * FROM OPENQUERY([' + @SQLServerName + N'], ''SET FMTONLY OFF; EXEC [' + @DBName + N'].dbo.sp_executesql N''''IF OBJECT_ID(''''''''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'''''''') IS NOT NULL DBCC SHOWCONTIG (''''''''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'''''''', ''''''''' + @IndexName + ''''''''' ) WITH ' + CASE WHEN @flgOptions & 1024 = 1024 THEN '' ELSE 'FAST,' END + ' TABLERESULTS, NO_INFOMSGS'''''')x'

						IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [Level], [Pages], [Rows], [MinimumRecordSize], [MaximumRecordSize], [AverageRecordSize], [ForwardedRecords], [Extents], [ExtentSwitches], [AverageFreeBytes], [AveragePageDensity], [ScanDensity], [BestCount], [ActualCount], [LogicalFragmentation], [ExtentFragmentation])
								EXEC (@queryToRun)
					end
				ELSE
					begin
						SET @queryToRun=N'SELECT	 OBJECT_NAME(ips.[object_id])	AS [table_name]
													, ips.[object_id]
													, si.[name] as index_name
													, ips.[index_id]
													, ips.[avg_fragmentation_in_percent]
													, ips.[page_count]
													, ips.[record_count]
													, ips.[forwarded_record_count]
													, ips.[avg_record_size_in_bytes]
													, ips.[avg_page_space_used_in_percent]
													, ips.[ghost_record_count]
											FROM [' + @DBName + '].sys.dm_db_index_physical_stats (' + CAST(@DatabaseID AS [nvarchar](4000)) + N', ' + CAST(@ObjectID AS [nvarchar](4000)) + N', ' + CAST(@IndexID AS [nvarchar](4000)) + N' , NULL, ''' + 
															CASE WHEN @flgOptions & 1024 = 1024 THEN 'DETAILED' ELSE 'LIMITED' END 
													+ ''') ips
											INNER JOIN [' + @DBName + '].sys.indexes si ON ips.[object_id]=si.[object_id] AND ips.[index_id]=si.[index_id]
											WHERE	si.[type] IN (' + @analyzeIndexType + N')
													AND si.[is_disabled]=0'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@SQLServerName, @queryToRun)
						IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
						
						INSERT	INTO #CurrentIndexFragmentationStats([ObjectName], [ObjectId], [IndexName], [IndexId], [LogicalFragmentation], [Pages], [Rows], [ForwardedRecords], [AverageRecordSize], [AveragePageDensity], [ghost_record_count])  
								EXEC (@queryToRun)
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @DatabaseID, @ObjectID, @CurrentTableSchema, @CurrentTableName, @IndexID, @IndexName, @IndexType, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes

		UPDATE doil
			SET   doil.[avg_fragmentation_in_percent] = cifs.[LogicalFragmentation]
				, doil.[page_count] = cifs.[Pages]
				, doil.[ghost_record_count] = cifs.[ghost_record_count]
				, doil.[forwarded_records_percentage] = CASE WHEN ISNULL(cifs.[Rows], 0) > 0 
															 THEN (CAST(cifs.[ForwardedRecords] AS decimal(29,2)) / CAST(cifs.[Rows]  AS decimal(29,2))) * 100
															 ELSE 0
														END
				, doil.[page_density_deviation] =  ABS(CASE WHEN ISNULL(cifs.[AverageRecordSize], 0) > 0
															THEN ((FLOOR(8060. / ISNULL(cifs.[AverageRecordSize], 0)) * ISNULL(cifs.[AverageRecordSize], 0)) / 8060.) * doil.[fill_factor] - cifs.[AveragePageDensity]
															ELSE 0
													   END)
		FROM	#databaseObjectsWithIndexList doil
		INNER JOIN #CurrentIndexFragmentationStats cifs ON cifs.[ObjectId] = doil.[object_id] AND cifs.[IndexId] = doil.[index_id]
	end


--------------------------------------------------------------------------------------------------
-- 1	Defragmenting database tables indexes
--		All indexes with a fragmentation level between defrag and rebuild threshold will be reorganized
--------------------------------------------------------------------------------------------------		
IF ((@flgActions & 1 = 1) AND (@flgActions & 4 = 0)) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Defragmenting database tables indexes (fragmentation between ' + CAST(@DefragIndexThreshold AS [nvarchar]) + ' and ' + CAST(CAST(@RebuildIndexThreshold AS NUMERIC(6,2)) AS [nvarchar]) + ') and more than ' + CAST(@PageThreshold AS [nvarchar](4000)) + ' pages...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
		
		DECLARE crsTableList CURSOR FOR	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
		   								FROM	#databaseObjectsWithIndexList doil
										WHERE	doil.[page_count] >= @PageThreshold
												AND doil.[index_type] <> 0 /* heap tables will be excluded */
												AND	( 
														(
															 doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold 
														 AND doil.[avg_fragmentation_in_percent] < @RebuildIndexThreshold
														)
													OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
														(	  @flgOptions & 1024 = 1024 
														 AND doil.[page_density_deviation] >= @DefragIndexThreshold 
														 AND doil.[page_density_deviation] < @RebuildIndexThreshold
														)
													OR
														(	/* for very large tables, will performed reorganize instead of rebuild */
															doil.[page_count] >= @RebuildIndexPageCountLimit
															AND	( 
																	(
																		doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
																	)
																OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																	(	  @flgOptions & 1024 = 1024 
																		AND doil.[page_density_deviation] >= @RebuildIndexThreshold
																	)
																)
														)
													)
										ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun=N'[' + @CurrentTableSchema+ '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DECLARE crsIndexesToDegfragment CURSOR FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[object_id], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor]
							   								FROM	#databaseObjectsWithIndexList doil
   															WHERE	doil.[table_name] = @CurrentTableName
																	AND doil.[table_schema] = @CurrentTableSchema
																	AND doil.[page_count] >= @PageThreshold
																	AND doil.[index_type] <> 0 /* heap tables will be excluded */
																	AND	( 
																			(
																				 doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold 
																			 AND doil.[avg_fragmentation_in_percent] < @RebuildIndexThreshold
																			)
																		OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																			(	  @flgOptions & 1024 = 1024 
																			 AND doil.[page_density_deviation] >= @DefragIndexThreshold 
																			 AND doil.[page_density_deviation] < @RebuildIndexThreshold
																			)
																		OR
																			(	/* for very large tables, will performed reorganize instead of rebuild */
																				doil.[page_count] >= @RebuildIndexPageCountLimit
																				AND	( 
																						(
																							doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
																						)
																					OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																						(	  @flgOptions & 1024 = 1024 
																							AND doil.[page_density_deviation] >= @RebuildIndexThreshold
																						)
																					)
																			)
																		)																		
															ORDER BY doil.[index_id]
				OPEN crsIndexesToDegfragment
				FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered' 
															WHEN 2 THEN 'Nonclustered' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
											END
   						SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @eventData='<index-fragmentation><detail>' + 
											'<database_name>' + @DBName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<index_name>' + @childObjectName + '</index_name>' + 
											'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
											'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
											'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
											'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
											'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
										'</detail></index-fragmentation>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
															@dbName			= @DBName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - reorganize index',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						IF @serverVersionNum >= 9 
							begin
								SET @nestExecutionLevel = @executionLevel + 3

								EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName			= @SQLServerName
																		, @DBName					= @DBName
																		, @TableSchema				= @CurrentTableSchema
																		, @TableName				= @CurrentTableName
																		, @IndexName				= @IndexName
																		, @IndexID					= NULL
																		, @PartitionNumber			= DEFAULT
																		, @flgAction				= 2		--reorganize
																		, @flgOptions				= @flgOptions
																		, @MaxDOP					= @MaxDOP
																		, @executionLevel			= @nestExecutionLevel
																		, @affectedDependentObjects = @affectedDependentObjects OUT
																		, @DebugMode				= @DebugMode
							end
						ELSE
							begin
								SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
								SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC INDEXDEFRAG (0, ' + RTRIM(@ObjectID) + ', ' + RTRIM(@IndexID) + ') WITH NO_INFOMSGS'
								IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

								SET @nestedExecutionLevel = @executionLevel + 1
								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpDatabaseOptimize',
																				@eventName		= 'database maintenance - reorganize index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode

							end
	   					FETCH NEXT FROM crsIndexesToDegfragment INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @ObjectID, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
					end		
				CLOSE crsIndexesToDegfragment
				DEALLOCATE crsIndexesToDegfragment

				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
-- 2	- Rebuild heavy fragmented indexes
--		All indexes with a fragmentation level greater than rebuild threshold will be rebuild
--		If a clustered index needs to be rebuild, then all associated non-clustered indexes will be rebuild
--		http://technet.microsoft.com/en-us/library/ms189858.aspx
--------------------------------------------------------------------------------------------------
IF (@flgActions & 2 = 2) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database tables indexes (fragmentation between ' + CAST(@RebuildIndexThreshold AS [nvarchar]) + ' and 100) or small tables (no more than ' + CAST(@PageThreshold AS [nvarchar](4000)) + ' pages)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
																		
		DECLARE crsTableList CURSOR FOR 	SELECT	DISTINCT doil.[table_schema], doil.[table_name]
		   									FROM	#databaseObjectsWithIndexList doil
											WHERE	    doil.[index_type] <> 0 /* heap tables will be excluded */
													AND doil.[page_count] >= @PageThreshold
													AND doil.[page_count] < @RebuildIndexPageCountLimit
													AND	( 
															(
																doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
															)
														OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
															(	  @flgOptions & 1024 = 1024 
															 AND doil.[page_density_deviation] >= @RebuildIndexThreshold
															)
														)
											ORDER BY doil.[table_schema], doil.[table_name]
		OPEN crsTableList
		FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @ClusteredRebuildNonClustered = 0

				DECLARE crsIndexesToRebuild CURSOR LOCAL FAST_FORWARD FOR 	SELECT	DISTINCT doil.[index_name], doil.[index_type], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[index_id], doil.[page_density_deviation], doil.[fill_factor] 
				   							   								FROM	#databaseObjectsWithIndexList doil
		   																	WHERE	doil.[table_name] = @CurrentTableName
		   																			AND doil.[table_schema] = @CurrentTableSchema
																					AND doil.[page_count] >= @PageThreshold
																					AND doil.[page_count] < @RebuildIndexPageCountLimit
																					AND doil.[index_type] <> 0 /* heap tables will be excluded */
																					AND doil.[is_rebuilt] = 0
																					AND	( 
																							(
																								doil.[avg_fragmentation_in_percent] >= @RebuildIndexThreshold
																							)
																						OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																							(	  @flgOptions & 1024 = 1024 
																							 AND doil.[page_density_deviation] >= @RebuildIndexThreshold
																							)
																						)
																			ORDER BY doil.[index_id]

				OPEN crsIndexesToRebuild
				FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
				WHILE @@FETCH_STATUS = 0 AND @ClusteredRebuildNonClustered = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SELECT	@indexIsRebuilt = doil.[is_rebuilt]
						FROM	#databaseObjectsWithIndexList doil
						WHERE	doil.[table_schema] = @CurrentTableSchema 
		   						AND doil.[table_name] = @CurrentTableName
								AND doil.[index_id] = @IndexID

						IF @indexIsRebuilt = 0
							begin
								SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
																	WHEN 1 THEN 'Clustered' 
																	WHEN 2 THEN 'Nonclustered' 
																	WHEN 3 THEN 'XML'
																	WHEN 4 THEN 'Spatial' 
													END
		   						SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) +  ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

								SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
								SET @childObjectName = QUOTENAME(@IndexName)

								--------------------------------------------------------------------------------------------------
								--log index fragmentation information
								SET @eventData='<index-fragmentation><detail>' + 
													'<database_name>' + @DBName + '</database_name>' + 
													'<object_name>' + @objectName + '</object_name>'+ 
													'<index_name>' + @childObjectName + '</index_name>' + 
													'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
													'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
													'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
													'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
													'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
												'</detail></index-fragmentation>'

								EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
																	@dbName			= @DBName,
																	@objectName		= @objectName,
																	@childObjectName= @childObjectName,
																	@module			= 'dbo.usp_mpDatabaseOptimize',
																	@eventName		= 'database maintenance - rebuilding index',
																	@eventMessage	= @eventData,
																	@eventType		= 0 /* info */
																						
								--------------------------------------------------------------------------------------------------
								--4  - Rebuild all dependent indexes when rebuild primary indexes
								IF @IndexType=1 AND (@flgOptions & 4 = 4)
									begin
										SET @ClusteredRebuildNonClustered = 1									
									end

								IF @serverVersionNum >= 9
									begin
										SET @nestExecutionLevel = @executionLevel + 3

										EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName			= @SQLServerName
																				, @DBName					= @DBName
																				, @TableSchema				= @CurrentTableSchema
																				, @TableName				= @CurrentTableName
																				, @IndexName				= @IndexName
																				, @IndexID					= NULL
																				, @PartitionNumber			= DEFAULT
																				, @flgAction				= 1		--rebuild
																				, @flgOptions				= @flgOptions
																				, @MaxDOP					= @MaxDOP
																				, @executionLevel			= @nestExecutionLevel
																				, @affectedDependentObjects = @affectedDependentObjects OUT
																				, @DebugMode				= @DebugMode

										--enable foreign key
										IF @IndexType=1
											begin
												 EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName	= @SQLServerName
																						  , @DBName			= @DBName
																						  , @TableSchema	= @CurrentTableSchema
																						  , @TableName		= @CurrentTableName
																						  , @ConstraintName = '%'
																						  , @flgAction		= 1
																						  , @flgOptions		= DEFAULT
																						  , @executionLevel	= @nestExecutionLevel
																						  , @DebugMode		= @DebugMode
											end
								
										IF @IndexType IN (1,3) AND @flgOptions & 4 = 4
											begin										
												--mark all dependent non-clustered/xml/spatial indexes as being rebuild
												UPDATE doil
													SET doil.[is_rebuilt]=1
												FROM	#databaseObjectsWithIndexList doil
	   											WHERE	doil.[table_name] = @CurrentTableName
	   													AND doil.[table_schema] = @CurrentTableSchema
														AND CHARINDEX(doil.[index_name], @affectedDependentObjects)<>0
											end
										end
								ELSE
									begin
										SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
										SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + ''', ''' + RTRIM(@IndexName) + ''') WITH NO_INFOMSGS'
										IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

										SET @nestedExecutionLevel = @executionLevel + 1
										EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																						@dbName			= @DBName,
																						@objectName		= @objectName,
																						@childObjectName= @childObjectName,
																						@module			= 'dbo.usp_mpDatabaseOptimize',
																						@eventName		= 'database maintenance - rebuilding index',
																						@queryToRun  	= @queryToRun,
																						@flgOptions		= @flgOptions,
																						@executionLevel	= @nestedExecutionLevel,
																						@debugMode		= @DebugMode
									end
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID

	   					FETCH NEXT FROM crsIndexesToRebuild INTO @IndexName, @IndexType, @CurrentFragmentation, @CurrentPageCount, @IndexID, @CurentPageDensityDeviation, @IndexFillFactor
					end		
				CLOSE crsIndexesToRebuild
				DEALLOCATE crsIndexesToRebuild

				FETCH NEXT FROM crsTableList INTO @CurrentTableSchema, @CurrentTableName
			end
		CLOSE crsTableList
		DEALLOCATE crsTableList
	end


--------------------------------------------------------------------------------------------------
-- 4	- Rebuild all indexes 
--------------------------------------------------------------------------------------------------
IF (@flgActions & 4 = 4) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun='Rebuilding database tables indexes  (all)...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--minimizing the list of indexes to be rebuild:
		--4  - Rebuild all dependent indexes when rebuild primary indexes
		IF (@flgOptions & 4 = 4)
			begin
				SET @queryToRun=N'optimizing index list to be rebuild'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0
					

				DECLARE crsClusteredIndexes CURSOR LOCAL FAST_FORWARD FOR	SELECT doil.[table_schema], doil.[table_name], doil.[index_name]
																			FROM	#databaseObjectsWithIndexList doil
																			WHERE	doil.[index_type]=1 --clustered index
																					AND doil.[page_count] >= @PageThreshold
																					AND EXISTS (
																								SELECT 1
																								FROM #databaseObjectsWithIndexList b
																								WHERE b.[table_schema] = doil.[table_schema]
																										AND b.[table_name] = doil.[table_name]
																										AND CHARINDEX(CAST(b.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
																										AND b.[index_type] NOT IN (0, 1)
																										AND b.[is_rebuilt] = 0	--not yet rebuilt
																								)
																			ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]
				OPEN crsClusteredIndexes
				FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName
				WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0
	
						--mark indexes as rebuilt
						UPDATE doil	
							SET doil.[is_rebuilt]=1
						FROM #databaseObjectsWithIndexList doil
						WHERE   doil.[table_schema] = @CurrentTableSchema
								AND doil.[table_name] = @CurrentTableName
								AND CHARINDEX(CAST(doil.[index_type] AS [nvarchar](8)), @analyzeIndexType) <> 0
								AND doil.[index_type] NOT IN (0, 1)
										
						FETCH NEXT FROM crsClusteredIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName
					end
				CLOSE crsClusteredIndexes
				DEALLOCATE crsClusteredIndexes						
			end


		--rebuilding indexes
		DECLARE crsObjectsWithIndexes CURSOR LOCAL FAST_FORWARD FOR SELECT	DISTINCT doil.[table_schema], doil.[table_name], doil.[index_name], doil.[index_type], doil.[index_id], doil.[avg_fragmentation_in_percent], doil.[page_count], doil.[page_density_deviation], doil.[fill_factor] 
							   										FROM	#databaseObjectsWithIndexList doil
   																	WHERE	doil.[index_type] <> 0 /* heap tables will be excluded */
																			AND doil.[is_rebuilt]=0
																			AND doil.[page_count] >= @PageThreshold
																			AND	( 
																					(
																						doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold
																					)
																				OR  /* when DETAILED analysis is selected, page density information will be used to reorganize / rebuild an index */
																					(	  @flgOptions & 1024 = 1024 
																						AND doil.[page_density_deviation] >= @DefragIndexThreshold
																					)
																				)
																	ORDER BY doil.[table_schema], doil.[table_name], doil.[index_id]

		OPEN crsObjectsWithIndexes
		FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor
		WHILE @@FETCH_STATUS=0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @indexIsRebuilt = 0
				--for XML indexes, check if it was not previously rebuilt by a primary XML index
				IF @IndexType=3
					SELECT	@indexIsRebuilt = doil.[is_rebuilt]
					FROM	#databaseObjectsWithIndexList doil
					WHERE	doil.[table_name] = @CurrentTableName
		   					AND doil.[table_schema] = @CurrentTableSchema 
							AND doil.[index_id] = @IndexID

				IF @indexIsRebuilt = 0
					begin
						SET @IndexTypeDesc=CASE @IndexType	WHEN 0 THEN 'Heap' 
															WHEN 1 THEN 'Clustered' 
															WHEN 2 THEN 'Nonclustered' 
															WHEN 3 THEN 'XML'
															WHEN 4 THEN 'Spatial' 
											END

						--analyze curent object
						SET @queryToRun=N'[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		   				SET @queryToRun=N'[' + @IndexName + ']: Current fragmentation level: ' + CAST(CAST(@CurrentFragmentation AS NUMERIC(6,2)) AS [nvarchar]) + ' / page density = ' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + ' / pages = ' + CAST(@CurrentPageCount AS [nvarchar]) + ' / type = ' + @IndexTypeDesc
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log index fragmentation information
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						SET @eventData='<index-fragmentation><detail>' + 
											'<database_name>' + @DBName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<index_name>' + @childObjectName + '</index_name>' + 
											'<index_type>' +  @IndexTypeDesc + '</index_type>' + 
											'<fragmentation>' + CAST(@CurrentFragmentation AS [varchar](32)) + '</fragmentation>' + 
											'<page_count>' + CAST(@CurrentPageCount AS [varchar](32)) + '</page_count>' + 
											'<fill_factor>' + CAST(@IndexFillFactor AS [varchar](32)) + '</fill_factor>' + 
											'<page_density_deviation>' + CAST(@CurentPageDensityDeviation AS [varchar](32)) + '</page_density_deviation>' + 
										'</detail></index-fragmentation>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
															@dbName			= @DBName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - rebuilding index',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						IF @serverVersionNum >= 9
							begin
								SET @nestExecutionLevel = @executionLevel + 3
								EXEC [dbo].[usp_mpAlterTableIndexes]	  @SQLServerName			= @SQLServerName
																		, @DBName					= @DBName
																		, @TableSchema				= @CurrentTableSchema
																		, @TableName				= @CurrentTableName
																		, @IndexName				= @IndexName
																		, @IndexID					= NULL
																		, @PartitionNumber			= DEFAULT
																		, @flgAction				= 1		--rebuild
																		, @flgOptions				= @flgOptions
																		, @MaxDOP					= @MaxDOP
																		, @executionLevel			= @nestExecutionLevel
																		, @affectedDependentObjects = @affectedDependentObjects OUT
																		, @DebugMode				= @DebugMode
							--enable foreign key
							IF @IndexType=1
								begin
									 EXEC [dbo].[usp_mpAlterTableForeignKeys]	@SQLServerName	= @SQLServerName
																			  , @DBName			= @DBName
																			  , @TableSchema	= @CurrentTableSchema
																			  , @TableName		= @CurrentTableName
																			  , @ConstraintName = '%'
																			  , @flgAction		= 1
																			  , @flgOptions		= DEFAULT
																			  , @executionLevel	= @nestExecutionLevel
																			  , @DebugMode		= @DebugMode
								end

							--mark secondary indexes as being rebuilt, if primary xml was rebuilt
							IF @IndexType = 3 AND @flgOptions & 4 = 4
								begin										
									--mark all dependent xml indexes as being rebuild
									UPDATE doil
										SET doil.[is_rebuilt]=1
									FROM	#databaseObjectsWithIndexList doil
	   								WHERE	doil.[table_name] = @CurrentTableName
	   										AND doil.[table_schema] = @CurrentTableSchema
											AND CHARINDEX(doil.[index_name], @affectedDependentObjects)<>0
											AND doil.[is_rebuilt] = 0
								end
							end
						ELSE
							begin
								SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; '
								SET @queryToRun = @queryToRun +	N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL DBCC DBREINDEX (''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']' + ''', ''' + RTRIM(@IndexName) + ''') WITH NO_INFOMSGS'
								IF @DebugMode=1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0
								
								SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
								SET @childObjectName = QUOTENAME(@IndexName)
								SET @nestedExecutionLevel = @executionLevel + 1

								EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																				@dbName			= @DBName,
																				@objectName		= @objectName,
																				@childObjectName= @childObjectName,
																				@module			= 'dbo.usp_mpDatabaseOptimize',
																				@eventName		= 'database maintenance - rebuilding index',
																				@queryToRun  	= @queryToRun,
																				@flgOptions		= @flgOptions,
																				@executionLevel	= @nestedExecutionLevel,
																				@debugMode		= @DebugMode
							end

							--mark index as being rebuilt
							UPDATE doil
								SET [is_rebuilt]=1
							FROM	#databaseObjectsWithIndexList doil 
	   						WHERE	doil.[table_name] = @CurrentTableName
	   								AND doil.[table_schema] = @CurrentTableSchema
									AND doil.[index_id] = @IndexID
					end

				FETCH NEXT FROM crsObjectsWithIndexes INTO @CurrentTableSchema, @CurrentTableName, @IndexName, @IndexType, @IndexID, @CurrentFragmentation, @CurrentPageCount, @CurentPageDensityDeviation, @IndexFillFactor
			end
		CLOSE crsObjectsWithIndexes
		DEALLOCATE crsObjectsWithIndexes
	end


--------------------------------------------------------------------------------------------------
--1 / 2 / 4	/ 16 
--------------------------------------------------------------------------------------------------
IF @serverVersionNum >= 9 AND (GETDATE() <= @stopTimeLimit)
	begin
		IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
		begin
			SET @nestExecutionLevel = @executionLevel + 1
			EXEC [dbo].[usp_mpCheckAndRevertInternalActions]	@sqlServerName	= @SQLServerName,
																@flgOptions		= @flgOptions,
																@executionLevel	= @nestExecutionLevel, 
																@debugMode		= @DebugMode
		end
	end



--------------------------------------------------------------------------------------------------
--cleanup of ghost records (sp_clean_db_free_space) (starting SQL Server 2005 SP3)
--exclude indexes which got rebuilt or reorganized, since ghost records were already cleaned
--------------------------------------------------------------------------------------------------
IF (@serverVersionNum >= 9.04035 AND @flgOptions & 65536 = 65536) AND (GETDATE() <= @stopTimeLimit)
	IF (@flgActions & 1 = 1) OR (@flgActions & 2 = 2) OR (@flgActions & 4 = 4) OR (@flgActions & 16 = 16)
			IF (
					SELECT SUM(doil.[ghost_record_count]) 
					FROM	#databaseObjectsWithIndexList doil
					WHERE	NOT (
									doil.[page_count] >= @PageThreshold
								AND doil.[index_type] <> 0 
								AND	( 
										(
											doil.[avg_fragmentation_in_percent] >= @DefragIndexThreshold 
										)
									OR  
										(	@flgOptions & 1024 = 1024 
										AND doil.[page_density_deviation] >= @DefragIndexThreshold 
										)
									)
								)
							AND doil.[is_rebuilt] = 0
				) >= @thresholdGhostRecords
				begin
					SET @queryToRun='sp_clean_db_free_space (ghost records cleanup)...'
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

					EXEC sp_clean_db_free_space @DBName
				end


--------------------------------------------------------------------------------------------------
--8  - Update statistics for all tables in database
--------------------------------------------------------------------------------------------------
IF (@flgActions & 8 = 8) AND (GETDATE() <= @stopTimeLimit)
	begin
		SET @queryToRun=N'Update statistics for all tables (' + 
					CASE WHEN @StatsSamplePercent<100 
							THEN 'sample ' + CAST(@StatsSamplePercent AS [nvarchar]) + ' percent'
							ELSE 'fullscan'
					END + ')...'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 1, @stopExecution=0

		--remove tables with clustered indexes already rebuild
		SET @queryToRun=N'--	optimizing list (1)'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

		DELETE dowsl
		FROM #databaseObjectsWithStatisticsList	dowsl
		WHERE EXISTS(
						SELECT 1
						FROM #databaseObjectsWithIndexList doil
						WHERE doil.[table_schema] = dowsl.[table_schema]
							AND doil.[table_name] = dowsl.[table_name]
							AND doil.[index_name] = dowsl.[stats_name]
							AND doil.[is_rebuilt] = 1
					)

		IF @flgOptions & 512 = 0
			begin
				--remove auto-created statistics
				SET @queryToRun=N'optimizing list (2)'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				DELETE dowsl
				FROM #databaseObjectsWithStatisticsList	dowsl
				WHERE [auto_created]=1
			end

		DECLARE   @statsAutoCreated			[bit]
				, @tableRows				[bigint]
				, @statsModificationCounter	[bigint]
				, @lastUpdated				[datetime]
				, @percentChanges			[decimal](38,2)
				, @statsAge					[int]

		DECLARE crsTableList2 CURSOR FOR	SELECT [table_schema], [table_name], COUNT(*) AS [stats_count]
											FROM #databaseObjectsWithStatisticsList	
											GROUP BY [table_schema], [table_name]
											ORDER BY [table_name]
		OPEN crsTableList2
		FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
		WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
			begin
				SET @CurrentTableName = REPLACE(@CurrentTableName, '''', '''''')
				SET @queryToRun=N'[' + @CurrentTableSchema+ '].[' + @CurrentTableName + ']'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 2, @stopExecution=0

				SET @IndexID=1
				DECLARE crsTableStatsList CURSOR FOR	SELECT	  [stats_name], [auto_created], [rows], [modification_counter], [last_updated], [percent_changes]
																, DATEDIFF(dd, [last_updated], GETDATE()) AS [stats_age]
														FROM	#databaseObjectsWithStatisticsList	
														WHERE	[table_schema] = @CurrentTableSchema
																AND [table_name] = @CurrentTableName
														ORDER BY [stats_name]
				OPEN crsTableStatsList
				FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge
				WHILE @@FETCH_STATUS = 0 AND (GETDATE() <= @stopTimeLimit)
					begin
						SET @queryToRun=CAST(@IndexID AS [nvarchar](64)) + '/' + CAST(@statsCount AS [nvarchar](64)) + ' - [' + @IndexName+ '] / age = ' + CAST(@statsAge AS [varchar](32)) + ' days / rows = ' + CAST(@tableRows AS [varchar](32)) + ' / changes = ' + CAST(@statsModificationCounter AS [varchar](32))
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 1, @messagRootLevel = @executionLevel, @messageTreelevel = 3, @stopExecution=0

						--------------------------------------------------------------------------------------------------
						--log statistics information
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)

						SET @eventData='<statistics-health><detail>' + 
											'<database_name>' + @DBName + '</database_name>' + 
											'<object_name>' + @objectName + '</object_name>'+ 
											'<stats_name>' + @childObjectName + '</stats_name>' + 
											'<auto_created>' + CAST(@statsAutoCreated AS [varchar](32)) + '</auto_created>' + 
											'<rows>' + CAST(@tableRows AS [varchar](32)) + '</rows>' + 
											'<modification_counter>' + CAST(@statsModificationCounter AS [varchar](32)) + '</modification_counter>' + 
											'<percent_changes>' + CAST(@percentChanges AS [varchar](32)) + '</percent_changes>' + 
											'<last_updated>' + CONVERT([nvarchar](20), @lastUpdated, 120) + '</last_updated>' + 
											'<age_days>' + CAST(@statsAge AS [varchar](32)) + '</age_days>' + 
										'</detail></statistics-health>'

						EXEC [dbo].[usp_logEventMessage]	@sqlServerName	= @SQLServerName,
															@dbName			= @DBName,
															@objectName		= @objectName,
															@childObjectName= @childObjectName,
															@module			= 'dbo.usp_mpDatabaseOptimize',
															@eventName		= 'database maintenance - update statistics',
															@eventMessage	= @eventData,
															@eventType		= 0 /* info */

						--------------------------------------------------------------------------------------------------
						SET @queryToRun = N'SET ARITHABORT ON; SET QUOTED_IDENTIFIER ON; SET LOCK_TIMEOUT ' + CAST(@queryLockTimeOut AS [nvarchar]) + N'; '
						SET @queryToRun = @queryToRun + N'IF OBJECT_ID(''[' + @CurrentTableSchema + '].[' + @CurrentTableName + ']'') IS NOT NULL UPDATE STATISTICS [' + @CurrentTableSchema + '].[' + @CurrentTableName + ']([' +  @IndexName + ']) WITH '
								
						IF @StatsSamplePercent<100
							SET @queryToRun=@queryToRun + N'SAMPLE ' + CAST(@StatsSamplePercent AS [nvarchar]) + ' PERCENT'
						ELSE
							SET @queryToRun=@queryToRun + N'FULLSCAN'

						IF @DebugMode=1	EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = @executionLevel, @messageTreelevel = 0, @stopExecution=0
						
						SET @objectName = '[' + @CurrentTableSchema + '].[' + RTRIM(@CurrentTableName) + ']'
						SET @childObjectName = QUOTENAME(@IndexName)
						SET @nestedExecutionLevel = @executionLevel + 1

						EXEC @errorCode = [dbo].[usp_sqlExecuteAndLog]	@sqlServerName	= @SQLServerName,
																		@dbName			= @DBName,
																		@objectName		= @objectName,
																		@childObjectName= @childObjectName,
																		@module			= 'dbo.usp_mpDatabaseOptimize',
																		@eventName		= 'database maintenance - update statistics',
																		@queryToRun  	= @queryToRun,
																		@flgOptions		= @flgOptions,
																		@executionLevel	= @nestedExecutionLevel,
																		@debugMode		= @DebugMode

						SET @IndexID = @IndexID + 1
						FETCH NEXT FROM crsTableStatsList INTO @IndexName, @statsAutoCreated, @tableRows, @statsModificationCounter, @lastUpdated, @percentChanges, @statsAge
					end
				CLOSE crsTableStatsList
				DEALLOCATE crsTableStatsList

				FETCH NEXT FROM crsTableList2 INTO @CurrentTableSchema, @CurrentTableName, @statsCount
			end
		CLOSE crsTableList2
		DEALLOCATE crsTableList2
	end
	

---------------------------------------------------------------------------------------------
IF object_id('tempdb..#CurrentIndexFragmentationStats') IS NOT NULL DROP TABLE #CurrentIndexFragmentationStats
IF object_id('tempdb..#databaseObjectsWithIndexList') IS NOT NULL 	DROP TABLE #databaseObjectsWithIndexList

RETURN @errorCode
GO

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 01.10.2015
-- Module			 : SQL Server 2000/2005/2008/2008R2/2012/2014+
-- Description		 : add indexes to msdb database in order to improve system maintenance execution times
-------------------------------------------------------------------------------
-- Change date		 : 
-- Description		 : 
-------------------------------------------------------------------------------
RAISERROR('Create additional indexes on msdb database, if required...', 10, 1) WITH NOWAIT
GO
USE [msdb]
GO
--  backupset
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_set_uuid'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_set_uuid' AND [id]=OBJECT_ID('dbo.backupset'))
begin   
	RAISERROR('--Creating index => [IX_backupset_backup_set_uuid] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_set_uuid] ON [dbo].[backupset]([backup_set_uuid])
end 

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'media_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_media_set_id' AND [id]=OBJECT_ID('dbo.backupset'))
begin   
	RAISERROR('--Creating index => [IX_backupset_media_set_id] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_media_set_id] ON [dbo].[backupset]([media_set_id])
end

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_finish_date'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_finish_date' AND [id]=OBJECT_ID('dbo.backupset'))
begin  
	RAISERROR('--Creating index => [IX_backupset_backup_finish_date] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_finish_date] ON [dbo].[backupset]([backup_finish_date])
end 

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupset')
						AND sc.[name] = 'backup_start_date'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupset_backup_start_date' AND [id]=OBJECT_ID('dbo.backupset'))
begin
	RAISERROR('--Creating index => [IX_backupset_backup_start_date] ON [dbo].[backupset]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupset_backup_start_date] ON [dbo].[backupset]([backup_start_date])
end


--  backupfile
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupfile')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupfile_backup_set_id' AND [id]=OBJECT_ID('dbo.backupfile'))
begin
	RAISERROR('--Creating index => [IX_backupfile_backup_set_id] ON [dbo].[backupfile]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupfile_backup_set_id] ON [dbo].[backupfile]([backup_set_id])
end

--  backupmediafamily
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupmediafamily')
						AND sc.[name] = 'media_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupmediafamily_media_set_id' AND [id]=OBJECT_ID('dbo.backupmediafamily'))
begin
	RAISERROR('--Creating index => [IX_backupmediafamily_media_set_id] ON [dbo].[backupmediafamily]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_backupmediafamily_media_set_id] ON [dbo].[backupmediafamily]([media_set_id])
end

--  backupfilegroup
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.backupfilegroup')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_backupfilegroup_backup_set_id' AND [id]=OBJECT_ID('dbo.backupfilegroup'))
begin
    RAISERROR('--Creating index => [IX_backupfilegroup_backup_set_id] ON [dbo].[backupfilegroup]', 10, 1) WITH NOWAIT
	CREATE INDEX [IX_backupfilegroup_backup_set_id] ON [dbo].[backupfilegroup]([backup_set_id])
end

--  restorehistory
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorehistory')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorehistory_restore_history_id' AND [id]=OBJECT_ID('dbo.restorehistory'))
begin
	RAISERROR('--Creating index => [IX_restorehistory_restore_history_id] ON [dbo].[restorehistory]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorehistory_restore_history_id] ON [dbo].[restorehistory]([restore_history_id])
end

IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorehistory')
						AND sc.[name] = 'backup_set_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorehistory_backup_set_id' AND [id]=OBJECT_ID('dbo.restorehistory'))
begin
	RAISERROR('--Creating index => [IX_restorehistory_backup_set_id] ON [dbo].[restorehistory]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorehistory_backup_set_id] ON [dbo].[restorehistory]([backup_set_id])
end

--  restorefile
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorefile')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorefile_restore_history_id' AND [id]=OBJECT_ID('dbo.restorefile'))
begin
	RAISERROR('--Creating index => [IX_restorefile_restore_history_id] ON [dbo].[restorefile]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorefile_restore_history_id] ON [dbo].[restorefile]([restore_history_id])
end

--  restorefilegroup
IF NOT EXISTS(	SELECT *
				FROM [msdb]..sysindexes si
				INNER JOIN [msdb]..sysindexkeys sik ON si.[id] = sik.[id] AND si.[indid] = sik.[indid] 
				INNER JOIN [msdb]..syscolumns sc ON sik.[id] = sc.[id] AND sik.[colid] = sc.[colid]
				WHERE si.[id] = OBJECT_ID('dbo.restorefilegroup')
						AND sc.[name] = 'restore_history_id'
						and sik.[keyno] = 1
				)
	AND NOT EXISTS(SELECT * FROM [msdb]..sysindexes si WHERE [name]='IX_restorefilegroup_restore_history_id' AND [id]=OBJECT_ID('dbo.restorefilegroup'))
begin
	RAISERROR('--Creating index => [IX_restorefilegroup_restore_history_id] ON [dbo].[restorefilegroup]', 10, 1) WITH NOWAIT
    CREATE INDEX [IX_restorefilegroup_restore_history_id] ON [dbo].[restorefilegroup]([restore_history_id])
end
