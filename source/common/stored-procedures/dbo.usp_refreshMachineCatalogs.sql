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
		@projectCode				[varchar](32)=NULL,
		@sqlServerName				[sysname],
		@addNewDatabasesToProject	[bit] = 1,
		@debugMode					[bit] = 0
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
		, @optionXPValue		[int]
		, @hostPlatform			[sysname]
		, @dbFilter				[sysname]
		, @isAzureSQLDatabase	[bit]

-- { sql_statement | statement_block }
BEGIN TRY
	SET @returnValue=1

	-----------------------------------------------------------------------------------------------------
	SET @errMessage=N'Getting Instance information: [' + @sqlServerName + '] / project: [' + @projectCode + ']'
	EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

	SET @errMessage=N''
	-----------------------------------------------------------------------------------------------------

	-----------------------------------------------------------------------------------------------------
	--check that SQLServerName is defined as local or as a linked server to current sql server instance
	-----------------------------------------------------------------------------------------------------
	IF (SELECT count(*) FROM sys.sysservers WHERE srvname=@sqlServerName)=0
		begin
			SET @errMessage= N'Specified instance name is not defined as local or linked server: ' + @sqlServerName
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			SET @errMessage= N'Create a new linked server.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

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
		[domain]				[sysname]		NULL,
		[host_platform]			[sysname]		NULL
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

	------------------------------------------------------------------------------------------------------------------------------------------
	--get default projectCode
	IF @projectCode IS NULL
		SET @projectCode = [dbo].[ufn_getProjectCode](@sqlServerName, NULL)

	SELECT    @projectID = [id]
			, @dbFilter = [db_filter]
	FROM [dbo].[catalogProjects]
	WHERE [code] = @projectCode 

	IF @projectID IS NULL
		begin
			SET @errMessage=N'ERROR: The value specifief for Project Code is not valid.'
			EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
		end

	IF ISNULL(@dbFilter, '')='' SET @dbFilter = '%'
		
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
	IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

	BEGIN TRY
		INSERT	INTO #catalogInstanceNames([name], [version], [edition], [machine_name])
				EXEC sp_executesql @queryToRun
		SET @isActive=1
	END TRY
	BEGIN CATCH
		SET @errMessage=ERROR_MESSAGE()
		SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs - Offline'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

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
				SELECT    @SQLMajorVersion = REPLACE(LEFT(ISNULL([version], ''), 2), '.', '') 
						, @isAzureSQLDatabase = CASE WHEN [edition] LIKE '%SQL Azure' THEN 1 ELSE 0 END
				FROM #catalogInstanceNames
			END TRY
			BEGIN CATCH
				SET @SQLMajorVersion = 9
			END CATCH

			-----------------------------------------------------------------------------------------------------
			--discover machine names (if clustered instance is present, get all cluster nodes)
			-----------------------------------------------------------------------------------------------------
			SET @isClustered=0
			IF @isAzureSQLDatabase = 0 
				begin
					SET @queryToRun = N'SELECT [NodeName] FROM sys.dm_os_cluster_nodes WITH (NOLOCK)'
					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			
					BEGIN TRY
						INSERT	INTO #catalogMachineNames([name])
								EXEC sp_executesql @queryToRun
					END TRY
					BEGIN CATCH
						IF @debugMode=1 
							begin
								SET @errMessage = 'An error occured. It will be ignored: ' + ERROR_MESSAGE()
								EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
							end
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
							IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

							BEGIN TRY
								INSERT	INTO #catalogMachineNames([name])
										EXEC sp_executesql @queryToRun
							END TRY
							BEGIN CATCH
								SET @errMessage=ERROR_MESSAGE()
								SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'

								EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
							END CATCH
						end
					ELSE
						begin
							SET @isClustered = 1
						end
				end
			ELSE
				begin
					/* On SQL Azure, assume the machine name is the same as the server-name */
					INSERT	INTO #catalogMachineNames([name])
					SELECT [name]
					FROM #catalogInstanceNames
				end
			
			-----------------------------------------------------------------------------------------------------
			--discover database names
			-----------------------------------------------------------------------------------------------------
			SET @queryToRun = N'SELECT sdb.[database_id], sdb.[name], sdb.[state], sdb.[state_desc]
								FROM sys.databases sdb WITH (NOLOCK)
								WHERE	[is_read_only] = 0 
										AND [is_in_standby] = 0
										/* AND sdb.[name] LIKE ''' + @dbFilter + ''' */
								UNION ALL
								SELECT sdb.[database_id], sdb.[name], sdb.[state], ''READ ONLY''
								FROM sys.databases sdb WITH (NOLOCK)
								WHERE	[is_read_only] = 1
										AND [is_in_standby] = 0
										/* AND sdb.[name] LIKE ''' + @dbFilter + ''' */
								UNION ALL
								SELECT sdb.[database_id], sdb.[name], sdb.[state], ''STANDBY''
								FROM sys.databases sdb WITH (NOLOCK)
								WHERE	[is_in_standby] = 1
										/* AND sdb.[name] LIKE ''' + @dbFilter + ''' */'
			SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
			IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

			BEGIN TRY
				INSERT	INTO #catalogDatabaseNames([database_id], [name], [state], [state_desc])
						EXEC sp_executesql @queryToRun
			END TRY
			BEGIN CATCH
				SET @errMessage=ERROR_MESSAGE()
				SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'

				EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
			END CATCH

			/*-------------------------------------------------------------------------------------------------------------------------------*/
			/* check if xp_cmdshell is enabled or should be enabled																			 */
			IF @isAzureSQLDatabase = 0
			BEGIN TRY
				SET @optionXPValue = 0

				/* enable xp_cmdshell configuration option */
				EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																, @flgAction	 = 1			-- 1=enable | 0=disable
																, @optionXPValue = @optionXPValue OUTPUT
																, @debugMode	 = @debugMode

				IF @optionXPValue=1
					begin
						BEGIN TRY
							--run wmi to get the domain name
							SET @queryToRun = N''
							SET @queryToRun = @queryToRun + N'DECLARE @cmdQuery [varchar](102); SET @cmdQuery=''wmic computersystem get Domain''; EXEC xp_cmdshell @cmdQuery;'
			
							IF @sqlServerName<>@@SERVERNAME
								IF @SQLMajorVersion < 11
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
								ELSE 
									SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''') WITH RESULT SETS(([Output] [nvarchar](max)))'')x'
							
							IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

							INSERT	INTO #xpCMDShellOutput([output])
									EXEC sp_executesql @queryToRun
									
							UPDATE #xpCMDShellOutput SET [output]=REPLACE(REPLACE(REPLACE(LTRIM(RTRIM([output])), ' ', ''), CHAR(10), ''), CHAR(13), '')
			
							DELETE FROM #xpCMDShellOutput WHERE LEN([output])<=3 OR [output] IS NULL
							DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%not recognized as an internal or external command%'
							DELETE FROM #xpCMDShellOutput WHERE [output] LIKE '%operable program or batch file%'
							DELETE TOP (1) FROM #xpCMDShellOutput WHERE SUBSTRING([output], 1, 8)='Domain'
			
							SELECT TOP 1 @domainName = LOWER([output])
							FROM #xpCMDShellOutput
						END TRY
						BEGIN CATCH
							SET @queryToRun = N''
							SET @queryToRun = @queryToRun + N'SELECT DEFAULT_DOMAIN()';
							IF @sqlServerName<>@@SERVERNAME
							SET @queryToRun = N'SELECT * FROM OPENQUERY([' + @sqlServerName + '], ''SET FMTONLY OFF; EXEC (''''' + REPLACE(@queryToRun, '''', '''''''''') + ''''')'')'
							IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
							INSERT	INTO #xpCMDShellOutput([output])
									EXEC sp_executesql @queryToRun

							SELECT TOP 1 @domainName = LOWER([output])
							FROM #xpCMDShellOutput
						END CATCH

						UPDATE #catalogMachineNames SET [domain] = @domainName
					end

					/* disable xp_cmdshell configuration option */
					EXEC [dbo].[usp_changeServerOption_xp_cmdshell]   @serverToRun	 = @sqlServerName
																	, @flgAction	 = 0			-- 1=enable | 0=disable
																	, @optionXPValue = @optionXPValue OUTPUT
																	, @debugMode	 = @debugMode
			END TRY
			BEGIN CATCH
				SET @errMessage = ERROR_MESSAGE()
				SET @errDescriptor = 'dbo.usp_refreshMachineCatalogs'
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
			END CATCH

			-----------------------------------------------------------------------------------------------------
			--discover platform type: windows/linux/azure
			-----------------------------------------------------------------------------------------------------
			IF @SQLMajorVersion>=14 AND @isAzureSQLDatabase = 0
				begin
					SET @queryToRun = N'SELECT [host_platform] FROM sys.dm_os_host_info WITH (NOLOCK)'
					SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
					IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					BEGIN TRY
						TRUNCATE TABLE #xpCMDShellOutput
						INSERT	INTO #xpCMDShellOutput([output])
								EXEC sp_executesql @queryToRun

						SELECT @hostPlatform = LOWER([output])
						FROM #xpCMDShellOutput

						UPDATE #catalogMachineNames SET [host_platform] = @hostPlatform
					END TRY
					BEGIN CATCH
						SET @errMessage = ERROR_MESSAGE()
						EXEC [dbo].[usp_logPrintMessage] @customMessage = @errMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
					END CATCH
				end
			ELSE
				IF @isAzureSQLDatabase = 1
					UPDATE #catalogMachineNames SET [host_platform] = 'azure' 
		end

		/* for Azure SQL database, consider the @@servername = UPPER(db_name()0 */
		IF @isAzureSQLDatabase = 1
			begin
				/* Azure SQL Database - linked server defined to a single database */
				IF (SELECT COUNT(*) FROM #catalogDatabaseNames) = 2
					begin
						DELETE FROM #catalogDatabaseNames 
						WHERE [name] IN ('master')

						UPDATE ci	
							SET ci.[name] = UPPER(cdn.[name])
						FROM #catalogInstanceNames ci
						CROSS JOIN 
							(
								SELECT TOP 1 [name]
								FROM #catalogDatabaseNames
							) cdn
					end			
				ELSE
					/* Azure SQL Server or Pool - linked server defined to master */
					begin
						UPDATE ci	
								SET ci.[name] = UPPER(ci.[name])
						FROM #catalogInstanceNames ci
					end
			end
	-----------------------------------------------------------------------------------------------------
	--upsert catalog tables
	-----------------------------------------------------------------------------------------------------
	UPDATE dest
		SET   dest.[domain]=src.[domain]
			, dest.[host_platform] = src.[host_platform]			
	FROM [dbo].[catalogMachineNames] AS dest
	INNER JOIN 
			(	
			 SELECT [name], [domain], [host_platform]
			 FROM #catalogMachineNames
			) src ON dest.[name] = src.[name] AND dest.[project_id] = @projectID;

	INSERT	INTO [dbo].[catalogMachineNames] ([project_id], [name], [domain], [host_platform]) 
			SELECT @projectID, src.[name], src.[domain], src.[host_platform]
			FROM 
				(	
					 SELECT [name], [domain], [host_platform]
					 FROM #catalogMachineNames
				) src 
			LEFT JOIN [dbo].[catalogMachineNames] AS dest ON dest.[name] = src.[name] AND dest.[project_id] = @projectID			
			WHERE dest.[name] IS NULL;

	UPDATE dest
		SET   dest.[is_clustered] = src.[is_clustered]
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
			, dest.[last_refresh_date_utc] = GETUTCDATE()
	FROM [dbo].[catalogInstanceNames] AS dest
	INNER JOIN
		 (	
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
		  ) AS src	ON dest.[machine_id] = src.[machine_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID;

	INSERT INTO [dbo].[catalogInstanceNames]([machine_id], [project_id], [name], [version], [edition], [is_clustered], [active], [cluster_node_machine_id], [last_refresh_date_utc]) 
			SELECT   src.[machine_id], @projectID, src.[name], src.[version], src.[edition], src.[is_clustered]
					, CASE WHEN src.[is_clustered]=1
							THEN CASE	WHEN src.[active]=1 AND src.[machine_id]=src.[cluster_node_machine_id] 
										THEN 1 
										ELSE 0
								 END
							ELSE src.[active]
					 END
					, src.[cluster_node_machine_id]
					, GETUTCDATE()
			FROM (	
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
				  ) AS src
			LEFT JOIN [dbo].[catalogInstanceNames] AS dest ON dest.[machine_id] = src.[machine_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID
			WHERE dest.[machine_id] IS NULL;


	UPDATE cdn
		SET cdn.[active] = 0
	FROM [dbo].[catalogDatabaseNames] cdn
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
	INNER JOIN #catalogInstanceNames	srcIN ON cin.[name] = srcIN.[name]
	WHERE cin.[project_id] = @projectID

	UPDATE dest
		SET	  dest.[database_id] = src.[database_id]
			, dest.[state] = src.[state]
			, dest.[state_desc] = src.[state_desc]
			, dest.[active] = 1
	FROM [dbo].[catalogDatabaseNames] AS dest
	INNER JOIN
		 (	
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
			WHERE cin.[project_id] = @projectID
		  ) AS src ON dest.[instance_id] = src.[instance_id] AND dest.[name] = src.[name] AND dest.[project_id] = @projectID;;


	IF @addNewDatabasesToProject = 1
		/* add only databases not allocated to other projects */
		INSERT INTO [dbo].[catalogDatabaseNames]([instance_id], [project_id], [database_id], [name], [state], [state_desc], [active])
				SELECT src.[instance_id], @projectID, src.[database_id], src.[database_name], src.[state], src.[state_desc], 1
				FROM (	
						SELECT  cin.[id] AS [instance_id]
							  , cin.[name] AS [instance_name]
							  , src.[name] AS [database_name]
							  , src.[database_id]
							  , src.[state]
							  , src.[state_desc]
						FROM  #catalogDatabaseNames src
						INNER JOIN #catalogMachineNames srcMn ON 1=1
						INNER JOIN #catalogInstanceNames srcIN ON 1=1
						INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
						INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]
						WHERE src.[name] LIKE @dbFilter
								AND cin.[project_id] = @projectID
					  ) AS src
				LEFT JOIN 
						(
							SELECT cin.[project_id], cin.[name] AS [instance_name], cdn.[name] AS [database_name]
							FROM [dbo].[catalogDatabaseNames] cdn
							INNER JOIN [dbo].[catalogInstanceNames] cin ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[id]
						) AS dest ON dest.[instance_name] = src.[instance_name] AND dest.[database_name] = src.[database_name]
				WHERE dest.[project_id] IS NULL;

	SELECT TOP 1 @instanceID = cin.[id]
	FROM  #catalogMachineNames srcMn
	INNER JOIN #catalogInstanceNames srcIN ON 1=1
	INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[name] = srcMn.[name] AND cmn.[project_id]=@projectID
	INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[name] = srcIN.[name] AND cin.[machine_id] = cmn.[id]

	IF @errMessage IS NOT NULL AND @errMessage<>''
		INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
GO
