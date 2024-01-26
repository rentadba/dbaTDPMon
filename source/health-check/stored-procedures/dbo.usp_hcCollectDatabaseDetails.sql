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
		@catalogDatabaseID		[int],
		@databaseID				[int],
		@databaseName			[sysname],
		@queryToRun				[nvarchar](4000),
		@strMessage				[nvarchar](4000)

DECLARE @serverVersionNum		[numeric](9,6),
		@sqlServerVersion		[sysname],
		@dbccLastKnownGood		[datetime], 
		@isAzureSQLDatabase		[bit]

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#databaseSpaceInfo') IS NOT NULL DROP TABLE #databaseSpaceInfo
CREATE TABLE #databaseSpaceInfo
(
	[volume_mount_point]	[nvarchar](512)		NULL,
	[is_log_file]			[bit]				NULL,
	[size_mb]				[numeric](20,3)		NULL,
	[space_used_mb]			[numeric](20,3)		NULL,
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
IF object_id('#statsDatabaseDetails') IS NOT NULL DROP TABLE #statsDatabaseDetails
CREATE TABLE #statsDatabaseDetails
(
	[database_id]				[int]			NOT NULL,
	[query_type]				[tinyint]		NOT NULL,
	[data_size_mb]				[numeric](20,3)	NULL,
	[data_space_used_percent]	[numeric](6,2)	NULL,
	[log_size_mb]				[numeric](20,3)	NULL,
	[log_space_used_percent]	[numeric](6,2)	NULL,
	[is_auto_close]				[bit]			NULL,
	[is_auto_shrink]			[bit]			NULL,
	[volume_mount_point]		[nvarchar](512)	NULL,
	[last_backup_time]			[datetime]		NULL,
	[last_dbcc checkdb_time]	[datetime]		NULL,
	[recovery_model]			[tinyint]		NULL,
	[page_verify_option]		[tinyint]		NULL,
	[compatibility_level]		[tinyint]		NULL,
	[is_growth_limited]			[bit]			NULL,
	[is_snapshot]				[bit]			NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#statsDatabaseAlwaysOnDetails') IS NOT NULL DROP TABLE #statsDatabaseAlwaysOnDetails
CREATE TABLE #statsDatabaseAlwaysOnDetails
(
	[cluster_name]					[sysname]		NOT NULL,
	[ag_name]						[sysname]		NOT NULL,
	[host_name]						[sysname]		NOT NULL,
	[instance_name]					[sysname]		NOT NULL,
	[database_name]					[sysname]		NOT NULL,
	[role_desc]						[nvarchar](60)	NULL,
	[replica_join_state_desc]		[nvarchar](60)	NULL,
	[replica_connected_state_desc]	[nvarchar](60)	NULL,
	[failover_mode_desc]			[nvarchar](60)	NULL,
	[availability_mode_desc]		[nvarchar](60)	NULL,
	[synchronization_health_desc]	[nvarchar](60)	NULL,
	[synchronization_state_desc]	[nvarchar](60)	NULL,
	[suspend_reason_desc]			[nvarchar](60)	NULL,
	[readable_secondary_replica]	[nvarchar](60)	NULL
)

/*-------------------------------------------------------------------------------------------------------------------------------*/
IF object_id('#msdbBackupHistory') IS NOT NULL DROP TABLE #msdbBackupHistory
CREATE TABLE #msdbBackupHistory
(
	[database_name]			[sysname]		NOT NULL,
	[backup_type]			[nvarchar](60)	NULL,
	[backup_start_date]		[datetime]		NULL,
	[duration_sec]			[int]			NULL,
	[size_bytes]			[bigint]		NULL,
	[file_name]				[nvarchar](256)	NULL
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
		SET @strMessage=N'The value specifief for Project Code is not valid.'
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=1
	end

-----------------------------------------------------------------------------------------------------
DECLARE @reportOptionGetBackupSizeLastDays [int]
BEGIN TRY
	SELECT	@reportOptionGetBackupSizeLastDays = [value]
	FROM	[report].[htmlOptions]
	WHERE	[name] = N'Analyze backup size (GB) in the last days'
			AND [module] = 'health-check'
END TRY
BEGIN CATCH
	SET @reportOptionGetBackupSizeLastDays = 7
END CATCH
SET @reportOptionGetBackupSizeLastDays = ISNULL(@reportOptionGetBackupSizeLastDays, 7)

		
DECLARE   @startEvent		[datetime]

SET @startEvent = DATEADD(day, -@reportOptionGetBackupSizeLastDays, GETUTCDATE())
SET @startEvent = CONVERT([datetime], CONVERT([varchar](10), @startEvent, 120), 120);

------------------------------------------------------------------------------------------------------------------------------------------
--A. get databases informations
-------------------------------------------------------------------------------------------------------------------------
SET @strMessage = 'Step 1: Delete existing information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

INSERT	INTO [health-check].[statsDatabaseUsageHistory]([catalog_database_id], [instance_id], 
									  				 [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], 
													 [volume_mount_point], [event_date_utc])
		SELECT	shcdd.[catalog_database_id], shcdd.[instance_id], 
				shcdd.[data_size_mb], shcdd.[data_space_used_percent], shcdd.[log_size_mb], shcdd.[log_space_used_percent], 
				shcdd.[volume_mount_point], shcdd.[event_date_utc]
		FROM [health-check].[statsDatabaseDetails]		shcdd
		INNER JOIN [dbo].[catalogDatabaseNames]			cdb ON cdb.[id] = shcdd.[catalog_database_id] AND cdb.[instance_id] = shcdd.[instance_id]
		INNER JOIN [dbo].[catalogInstanceNames]			cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE cin.[project_id] = @projectID
				AND cin.[name] LIKE @sqlServerNameFilter
				AND cdb.[name] LIKE @databaseNameFilter

DELETE shcdd
FROM [health-check].[statsDatabaseDetails]		shcdd
INNER JOIN [dbo].[catalogDatabaseNames]			cdb ON cdb.[id] = shcdd.[catalog_database_id] AND cdb.[instance_id] = shcdd.[instance_id]
INNER JOIN [dbo].[catalogInstanceNames]			cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND cdb.[name] LIKE @databaseNameFilter

DELETE lsam
FROM [dbo].[logAnalysisMessages]	lsam
INNER JOIN [dbo].[catalogInstanceNames] cin ON cin.[id] = lsam.[instance_id] AND cin.[project_id] = lsam.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND lsam.[descriptor]='dbo.usp_hcCollectDatabaseDetails'

DELETE sdaodM
FROM [health-check].[statsDatabaseAlwaysOnDetails] sdaodM
INNER JOIN 
	(
		SELECT sdaod.[cluster_name], sdaod.[ag_name]
		FROM [health-check].[statsDatabaseAlwaysOnDetails] sdaod
		INNER JOIN [dbo].[catalogDatabaseNames]			cdb ON cdb.[id] = sdaod.[catalog_database_id] AND cdb.[instance_id] = sdaod.[instance_id]
		INNER JOIN [dbo].[catalogInstanceNames]			cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
		WHERE	cin.[project_id] = @projectID
				AND cin.[name] LIKE @sqlServerNameFilter
				AND cdb.[name] LIKE @databaseNameFilter
				AND sdaod.[role_desc] = 'PRIMARY'
	)X ON sdaodM.[cluster_name] = X.[cluster_name] AND sdaodM.[ag_name] = X.[ag_name]

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 2: Get Database Details Information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version],
																	CASE WHEN cin.[engine] IN (5, 6) THEN 1 ELSE 0 END AS [isAzureSQLDatabase]
															FROM	[dbo].[vw_catalogInstanceNames] cin
															WHERE 	cin.[project_id] = @projectID
																	AND cin.[instance_active]=1
																	AND cin.[instance_name] LIKE @sqlServerNameFilter
															ORDER BY cin.[instance_name]
OPEN crsActiveInstances
FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion, @isAzureSQLDatabase
WHILE @@FETCH_STATUS=0
	begin
		SET @strMessage='Analyzing server: ' + @sqlServerName
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		DELETE FROM #statsDatabaseDetails

		BEGIN TRY
			SET @serverVersionNum=SUBSTRING(@sqlServerVersion, 1, CHARINDEX('.', @sqlServerVersion)-1) + '.' + REPLACE(SUBSTRING(@sqlServerVersion, CHARINDEX('.', @sqlServerVersion)+1, LEN(@sqlServerVersion)), '.', '')
		END TRY
		BEGIN CATCH
			SET @serverVersionNum = 9.0
		END CATCH

		/* check for AlwaysOn Availability Groups configuration */
		IF @serverVersionNum >= 12 AND @isAzureSQLDatabase = 0 
			begin
				SET @queryToRun = N'SELECT    hc.[cluster_name]
											, ag.[name] AS [ag_name]
											, hinm.[node_name] as [host_name]
											, arcn.[replica_server_name] AS [instance_name]
											, adc.[database_name]
											, ars.[role_desc]
											, rcs.[join_state_desc] AS [replica_join_state_desc]
											, ars.[connected_state_desc] AS [replica_connected_state_desc]
											, ar.[failover_mode_desc]
											, ar.[availability_mode_desc]
											, hdrs.[suspend_reason_desc]
											, ars.[synchronization_health_desc]
											, hdrs.[synchronization_state_desc]
											, ar.[secondary_role_allow_connections_desc]
									FROM sys.availability_replicas ar
									INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
									INNER JOIN sys.availability_groups ag ON ag.[group_id]=ar.[group_id]
									INNER JOIN sys.dm_hadr_availability_replica_cluster_nodes arcn ON arcn.[group_name]=ag.[name] AND arcn.[replica_server_name]=ar.[replica_server_name]
									INNER JOIN sys.dm_hadr_database_replica_states hdrs ON ar.[replica_id]=hdrs.[replica_id]
									INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
									INNER JOIN sys.dm_hadr_instance_node_map hinm ON hinm.[ag_resource_id] = ag.[resource_id] AND hinm.[instance_name] = arcn.[replica_server_name]
									INNER JOIN sys.dm_hadr_cluster hc ON 1=1
									INNER JOIN sys.dm_hadr_availability_replica_cluster_states rcs on rcs.replica_id=ar.replica_id and rcs.group_id=hdrs.group_id'

				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
				BEGIN TRY
					INSERT	INTO #statsDatabaseAlwaysOnDetails(	[cluster_name], [ag_name], [host_name], [instance_name], [database_name], [role_desc], 
																[replica_join_state_desc], [replica_connected_state_desc], [failover_mode_desc], [availability_mode_desc], [suspend_reason_desc],
																[synchronization_health_desc], [synchronization_state_desc], [readable_secondary_replica])
							EXEC sp_executesql @queryToRun
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectDatabaseDetails'
									, @strMessage
				END CATCH
			end

		/* get list of databases for which will collect more details */
		IF OBJECT_ID('tempdb..#activeDatabases') IS NOT NULL DROP TABLE #activeDatabases;

		SELECT [catalog_database_id], [database_id], [database_name], [linked_server_name]
		INTO #activeDatabases
		FROM (
				SELECT	cdn.[catalog_database_id], cdn.[database_id], cdn.[database_name],
						CASE WHEN @isAzureSQLDatabase = 0
								THEN @sqlServerName
								ELSE CASE WHEN ss.[name] IS NOT NULL THEN ss.[name] ELSE NULL END
						END [linked_server_name]
				FROM	[dbo].[vw_catalogDatabaseNames] cdn
				LEFT JOIN [sys].[servers] ss ON ss.[catalog] = cdn.[database_name] COLLATE SQL_Latin1_General_CP1_CI_AS
				WHERE 	cdn.[project_id] = @projectID
						AND cdn.[instance_id] = @instanceID
						AND cdn.[active]=1
						AND cdn.[database_name] LIKE @databaseNameFilter
						AND CHARINDEX(cdn.[state_desc], 'ONLINE, READ ONLY')<>0
			)x
		WHERE [linked_server_name] IS NOT NULL

		IF @serverVersionNum >= 12 AND @isAzureSQLDatabase = 0 
			begin
				SET @queryToRun = N''
				SET @queryToRun = @queryToRun + N'DELETE FROM #activeDatabases
				WHERE [database_name] IN (
											SELECT [database_name]
											FROM [health-check].[vw_statsDatabaseAlwaysOnDetails]
											WHERE [instance_id]=' + CAST(@instanceID AS [nvarchar]) + N'
											AND [role_desc] = ''SECONDARY''
											AND [readable_secondary_replica] = ''NO''
										)'
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				EXEC sp_executesql @queryToRun

				DELETE FROM #activeDatabases
				WHERE [database_name] IN (
											SELECT [database_name]
											FROM #statsDatabaseAlwaysOnDetails
											WHERE [instance_name] = @sqlServerName
											AND [role_desc] = 'SECONDARY'
											AND [readable_secondary_replica] = 'NO'
										)
			end

		DECLARE crsActiveDatabases CURSOR LOCAL FAST_FORWARD FOR 	SELECT [catalog_database_id], [database_id], [database_name], [linked_server_name]
																	FROM #activeDatabases
																	ORDER BY [database_name]
		OPEN crsActiveDatabases	
		FETCH NEXT FROM crsActiveDatabases INTO @catalogDatabaseID, @databaseID, @databaseName, @sqlServerName
		WHILE @@FETCH_STATUS=0
			begin
				SET @strMessage='database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 2, @stopExecution=0

				/* get space allocated / used details */
				IF (SELECT COUNT(*) FROM [health-check].[statsDiskSpaceInfo] 
					WHERE	[instance_id] = @instanceID 
							AND DATEDIFF(day, [event_date_utc], GETUTCDATE()) <= 1
					) = 0 OR @isAzureSQLDatabase = 1
				begin
					IF @sqlServerName <> @@SERVERNAME
						SET @queryToRun = N'SELECT *
											FROM OPENQUERY([' + @sqlServerName + N'], ''EXEC (''''USE ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + N'; 
													SELECT    [volume_mount_point]
															, CAST([is_logfile]		AS [bit]) AS [is_logfile]
															, SUM([size_mb])		AS [size_mb]
															, SUM([space_used_mb])	AS [space_used_mb]
															, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
													FROM (		
															SELECT    [name]
																	, CAST([size] AS [numeric](20,3)) * 8 / 1024. AS [size_mb]
																	, CAST(FILEPROPERTY([name], ''''''''SpaceUsed'''''''') AS [numeric](20,3)) * 8 / 1024. AS [space_used_mb]
																	, CAST(FILEPROPERTY([name], ''''''''IsLogFile'''''''') AS [bit])		AS [is_logfile] ' + 
																	CASE WHEN @isAzureSQLDatabase = 0 
																		 THEN	CASE WHEN @serverVersionNum >= 10.5
																					 THEN N', CASE WHEN LEN([volume_mount_point])=3 THEN UPPER([volume_mount_point]) ELSE [volume_mount_point] END [volume_mount_point] '
																					 ELSE N', REPLACE(LEFT([physical_name], 2), '''''''':'''''''', '''''''''''''''') AS [volume_mount_point] '
																				END
																		 ELSE N', ( SELECT ISNULL([elastic_pool_name], sd.[name]) 
																					FROM sys.database_service_objectives dso
																					INNER JOIN sys.databases sd ON dso.[database_id] = sd.[database_id]
																					WHERE sd.[name]=''''''''' + @databaseName + '''''''''
  																				  ) AS [volume_mount_point] '
																	END + N'
																	, CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END AS [is_growth_limited]
															FROM sys.database_files' + 
															CASE WHEN @serverVersionNum >= 10.5 AND @isAzureSQLDatabase = 0 
																 THEN N' CROSS APPLY sys.dm_os_volume_stats(DB_ID(), [file_id])'
																 ELSE N''
															END + 
															N'
														)sf
													GROUP BY [volume_mount_point], [is_logfile]
											'''')'')x'
					ELSE
						SET @queryToRun = N'USE ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + N'; 
											SELECT    [volume_mount_point]
													, CAST([is_logfile]		AS [bit]) AS [is_logfile]
													, SUM([size_mb])		AS [size_mb]
													, SUM([space_used_mb])	AS [space_used_mb]
													, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
											FROM (		
													SELECT    [name]
															, CAST([size] AS [numeric](20,3)) * 8 / 1024. AS [size_mb]
															, CAST(FILEPROPERTY([name], ''SpaceUsed'') AS [numeric](20,3)) * 8 / 1024. AS [space_used_mb]
															, CAST(FILEPROPERTY([name], ''IsLogFile'') AS [bit])		AS [is_logfile] ' + 
															CASE WHEN @serverVersionNum >= 10.5
																	THEN N', CASE WHEN LEN([volume_mount_point])=3 THEN UPPER([volume_mount_point]) ELSE [volume_mount_point] END [volume_mount_point] '
																	ELSE N', REPLACE(LEFT([physical_name], 2), '''''''':'''''''', '''''''''''''''') AS [volume_mount_point] '
																END + N'
															, CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END AS [is_growth_limited]
													FROM sys.database_files' + 
													CASE WHEN @serverVersionNum >= 10.5
															THEN N' CROSS APPLY sys.dm_os_volume_stats(DB_ID(), [file_id])'
															ELSE N''
													END + N'
												)sf
											GROUP BY [volume_mount_point], [is_logfile]'			
				end
				ELSE
					begin
						IF @sqlServerName <> @@SERVERNAME
								SET @queryToRun = N'OPENQUERY([' + @sqlServerName + N'], ''EXEC (''''USE ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + N'; 
																									SELECT    [name]
																											, [physical_name]
																											, CAST([size] AS [numeric](20,3)) * 8 / 1024. AS [size_mb]
																											, CAST(FILEPROPERTY([name], ''''''''SpaceUsed'''''''') AS [numeric](20,3)) * 8 / 1024. AS [space_used_mb]
																											, CAST(FILEPROPERTY([name], ''''''''IsLogFile'''''''') AS [bit])		AS [is_logfile]
																											, CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END AS [is_growth_limited]
																									FROM sys.database_files
																								'''')'')'
						ELSE
							SET @queryToRun = N'(SELECT   [name]
														, [physical_name]
														, CAST([size] AS [numeric](20,3)) * 8 / 1024. AS [size_mb]
														, CAST(FILEPROPERTY([name], ''SpaceUsed'') AS [numeric](20,3)) * 8 / 1024. AS [space_used_mb]
														, CAST(FILEPROPERTY([name], ''IsLogFile'') AS [bit])		AS [is_logfile]
														, CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END AS [is_growth_limited]
												FROM sys.database_files
												)'

						SET @queryToRun = CASE WHEN  @sqlServerName = @@SERVERNAME 
											   THEN N'USE ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + N';' 
											   ELSE N'' 
										  END + N'
												SELECT    CASE WHEN [volume_mount_point] = '''' THEN NULL ELSE [volume_mount_point] END [volume_mount_point]
														, CAST([is_logfile]		AS [bit]) AS [is_logfile]
														, SUM([size_mb])		AS [size_mb]
														, SUM([space_used_mb])	AS [space_used_mb]
														, MAX(CAST([is_growth_limited] AS [tinyint])) AS [is_growth_limited]
												FROM (		
														SELECT    [name]
																, [is_logfile]
																, MAX(ISNULL([size_mb], 0))					AS [size_mb]
																, MAX(ISNULL([space_used_mb], 0))			AS [space_used_mb]
																, MAX(ISNULL(dsi.[volume_mount_point], ''''))	AS [volume_mount_point]
																, MAX([is_growth_limited])		AS [is_growth_limited]
														FROM ' + @queryToRun + N' df
														LEFT JOIN [' + DB_NAME() + N'].[health-check].[vw_statsDiskSpaceInfo] dsi ON CHARINDEX(dsi.[volume_mount_point] COLLATE SQL_Latin1_General_CP1_CI_AS, df.[physical_name] COLLATE SQL_Latin1_General_CP1_CI_AS) > 0 
																															AND dsi.[instance_name] = ''' + @sqlServerName + N'''
														GROUP BY [name], [is_logfile]
													)sf
												GROUP BY [volume_mount_point], [is_logfile]'
						end

				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				DELETE FROM #databaseSpaceInfo
				BEGIN TRY				
						INSERT	INTO #databaseSpaceInfo([volume_mount_point], [is_log_file], [size_mb], [space_used_mb], [is_growth_limited])
							EXEC sp_executesql @queryToRun
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
								  , @projectID
								  , GETUTCDATE()
								  , 'dbo.usp_hcCollectDatabaseDetails'
								  , [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + ':' + @strMessage
				END CATCH

				/* get last date for dbcc checkdb, only for 2k5+ */
				IF @isAzureSQLDatabase = 0 
					begin
						IF @sqlServerName <> @@SERVERNAME
							begin
								IF @serverVersionNum < 11
									SET @queryToRun = N'SELECT MAX([VALUE]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC (''''DBCC DBINFO (' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + N') WITH TABLERESULTS, NO_INFOMSGS'''')'')x
														WHERE [Field]=''dbi_dbccLastKnownGood'''
								ELSE
									SET @queryToRun = N'SELECT MAX([Value]) AS [Value]
														FROM OPENQUERY([' + @sqlServerName + N'], ''SET FMTONLY OFF; EXEC (''''DBCC DBINFO (' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + N') WITH TABLERESULTS, NO_INFOMSGS'''') WITH RESULT SETS(([ParentObject] [nvarchar](max), [Object] [nvarchar](max), [Field] [nvarchar](max), [Value] [nvarchar](max))) '')x
														WHERE [Field]=''dbi_dbccLastKnownGood'''
							end
						ELSE
							begin							
								BEGIN TRY
									SET @queryToRun = N'DBCC DBINFO (''' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'sql') + N''') WITH TABLERESULTS, NO_INFOMSGS'
									INSERT INTO #dbccDBINFO
											EXEC sp_executesql @queryToRun
								END TRY
								BEGIN CATCH
									SET @strMessage = ERROR_MESSAGE()
									EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

									INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
											SELECT  @instanceID
													, @projectID
													, GETUTCDATE()
													, 'dbo.usp_hcCollectDatabaseDetails'
													, [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + ':' + @strMessage
								END CATCH

								SET @queryToRun = N'SELECT MAX([Value]) AS [Value] FROM #dbccDBINFO WHERE [Field]=''dbi_dbccLastKnownGood'''											
							end

						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

						DELETE FROM #dbccLastKnownGood
						BEGIN TRY
							INSERT	INTO #dbccLastKnownGood([Value])
									EXEC sp_executesql @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDatabaseDetails'
											, [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted') + ':' + @strMessage
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
				INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], [volume_mount_point], [last_dbcc checkdb_time], [is_growth_limited])
						SELECT    1, @databaseID
								, CAST([data_size_mb] AS [numeric](20,3)) AS [data_size_mb]
								, CAST(CASE WHEN [data_size_mb] <>0 THEN [data_space_used_mb] * 100. / [data_size_mb] ELSE 0 END AS [numeric](6,2)) AS [data_used_percent]
								, CAST([log_size_mb] AS [numeric](20,3)) AS [log_size_mb]
								, CAST(CASE WHEN [log_size_mb] <>0 THEN [log_space_used_mb] * 100. / [log_size_mb] ELSE 0 END AS [numeric](6,2)) AS [log_used_percent]
								, LTRIM(RTRIM([volume_mount_point])) AS [volume_mount_point]
								, @dbccLastKnownGood
								, [is_growth_limited]
						FROM (
								SELECT    SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[size_mb] ELSE 0 END)		AS [data_size_mb]
										, SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[space_used_mb] ELSE 0 END) 	AS [data_space_used_mb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[size_mb] ELSE 0 END) 		AS [log_size_mb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[space_used_mb] ELSE 0 END) 	AS [log_space_used_mb]
										, MAX(ISNULL(x.[volume_mount_points], '')) [volume_mount_point]
										, MAX(CAST([is_growth_limited] AS [tinyint])) [is_growth_limited]
								FROM #databaseSpaceInfo dsi
								CROSS APPLY(
											SELECT STUFF(
															(	SELECT ', ' + [volume_mount_point]
																FROM (	
																		SELECT DISTINCT [volume_mount_point]
																		FROM #databaseSpaceInfo
																	) AS x
																ORDER BY [volume_mount_point]
																FOR XML PATH('')
															),1,1,''
														) AS [volume_mount_points]
											)x
							)db
				
				IF @isAzureSQLDatabase = 1
					begin
						/* get last date for backup and other database flags / options */
						SET @queryToRun = N'SELECT	  2 AS [query_type]
													, bkp.[database_id]
													, NULL AS [last_backup_time]
													, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoClose'')  AS [bit])	AS [is_auto_close]
													, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoShrink'')  AS [bit])	AS [is_auto_shrink]
													, bkp.[recovery_model]
													, bkp.[page_verify_option]
													, bkp.[compatibility_level]
													, bkp.[is_snapshot]
											FROM (
													SELECT	  sdb.[name]	AS [database_name]
															, sdb.[database_id]
															, sdb.[recovery_model]
															, sdb.[page_verify_option]
															, sdb.[compatibility_level] 
															, CASE WHEN sdb.[source_database_id] IS NULL THEN 0 ELSE 1 END AS [is_snapshot]
													FROM sys.databases sdb
													WHERE sdb.[name]=''' + @databaseName + N'''
													GROUP BY sdb.[name], sdb.[database_id], sdb.[recovery_model], sdb.[page_verify_option], sdb.[compatibility_level], sdb.[source_database_id]
												)bkp'
						SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
						IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
						BEGIN TRY
							INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [last_backup_time], [is_auto_close], [is_auto_shrink], [recovery_model], [page_verify_option], [compatibility_level], [is_snapshot])
									EXEC sp_executesql @queryToRun
						END TRY
						BEGIN CATCH
							SET @strMessage = ERROR_MESSAGE()
							EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
									SELECT  @instanceID
											, @projectID
											, GETUTCDATE()
											, 'dbo.usp_hcCollectDatabaseDetails'
											, @strMessage
						END CATCH
					end

				FETCH NEXT FROM crsActiveDatabases INTO @catalogDatabaseID, @databaseID, @databaseName, @sqlServerName
			end
		CLOSE crsActiveDatabases
		DEALLOCATE crsActiveDatabases

		IF OBJECT_ID('tempdb..#activeDatabases') IS NOT NULL DROP TABLE #activeDatabases;

		/* get last date for backup and other database flags / options */
		IF @isAzureSQLDatabase = 0
			begin
				SET @queryToRun = N'SELECT	  2 AS [query_type]
											, bkp.[database_id]
											, CASE WHEN bkp.[last_backup_time] = CONVERT([datetime], ''1900-01-01'', 120) THEN NULL ELSE bkp.[last_backup_time] END AS [last_backup_time]
											, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoClose'')  AS [bit])	AS [is_auto_close]
											, CAST(DATABASEPROPERTY(bkp.[database_name], ''IsAutoShrink'')  AS [bit])	AS [is_auto_shrink]
											, bkp.[recovery_model]
											, bkp.[page_verify_option]
											, bkp.[compatibility_level]
											, bkp.[is_snapshot]
									FROM (
											SELECT	  sdb.[name]	AS [database_name]
													, sdb.[database_id]
													, sdb.[recovery_model]
													, sdb.[page_verify_option]
													, sdb.[compatibility_level] ' +
													CASE WHEN @isAzureSQLDatabase = 0 
														 THEN N', MAX(ISNULL(bs.[backup_finish_date], CONVERT([datetime], ''1900-01-01'', 120)))'
														 ELSE N', NULL'
													END + N' AS [last_backup_time]
													, CASE WHEN sdb.[source_database_id] IS NULL THEN 0 ELSE 1 END AS [is_snapshot]
											FROM sys.databases sdb ' +
											CASE WHEN @isAzureSQLDatabase = 0
												 THEN N'LEFT OUTER JOIN msdb.dbo.backupset bs ON bs.[database_name] = sdb.[name] AND bs.type IN (''D'', ''I'')'
												 ELSE N''
											END + N'
											WHERE sdb.[name] LIKE ''' + @databaseNameFilter + N'''
											GROUP BY sdb.[name], sdb.[database_id], sdb.[recovery_model], sdb.[page_verify_option], sdb.[compatibility_level], sdb.[source_database_id]
										)bkp'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
				BEGIN TRY
					INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [last_backup_time], [is_auto_close], [is_auto_shrink], [recovery_model], [page_verify_option], [compatibility_level], [is_snapshot])
							EXEC sp_executesql @queryToRun
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectDatabaseDetails'
									, @strMessage
				END CATCH

				/* get msdb backup files history */
				TRUNCATE TABLE #msdbBackupHistory;
				SET @queryToRun = N'SELECT	  bs.[database_name]
											, CASE bs.[type] WHEN ''D'' THEN ''full'' WHEN ''I'' THEN ''diff'' WHEN ''L'' THEN ''log'' END AS [backup_type]
											, bs.[backup_start_date]
											, DATEDIFF(ss, bs.[backup_start_date], bs.[backup_finish_date]) AS [duration_sec]
											, ' + CASE WHEN @serverVersionNum > 10 
														THEN N'bs.[compressed_backup_size]'
														ELSE N'bs.[backup_size]'
												  END + N' AS [size_bytes]
											, REPLACE(bmf.[physical_device_name], REVERSE(SUBSTRING(REVERSE(bmf.[physical_device_name]), CHARINDEX(''\'', REVERSE(bmf.[physical_device_name])), LEN(REVERSE(bmf.[physical_device_name])))), '''') AS [file_name]
									FROM msdb.dbo.backupset bs
									INNER JOIN msdb.dbo.backupmediafamily bmf on bs.[media_set_id] = bmf.[media_set_id]
									WHERE	bs.[backup_start_date] >= DATEADD(hour, DATEDIFF(hour, GETUTCDATE(), GETDATE()), CONVERT([datetime], CONVERT([varchar](10), DATEADD(day, -' + CAST(@reportOptionGetBackupSizeLastDays AS [nvarchar]) + N', GETUTCDATE()), 120), 120))'
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0

				BEGIN TRY
					INSERT	INTO #msdbBackupHistory([database_name], [backup_type], [backup_start_date], [duration_sec], [size_bytes], [file_name])
							EXEC sp_executesql @queryToRun
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
					
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
							SELECT  @instanceID
									, @projectID
									, GETUTCDATE()
									, 'dbo.usp_hcCollectDatabaseDetails'
									, @strMessage
				END CATCH
			end

		/* save results to stats table */
		INSERT	INTO [health-check].[statsDatabaseDetails]([catalog_database_id], [instance_id], 
				 											 [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], 
															 [is_auto_close], [is_auto_shrink], [volume_mount_point], 
															 [last_backup_time], [last_dbcc checkdb_time],  [recovery_model], [page_verify_option], [compatibility_level], [is_growth_limited], [is_snapshot], [event_date_utc])
				SELECT cdn.[id], @instanceID, 
				 		qt.[data_size_mb], qt.[data_space_used_percent], qt.[log_size_mb], qt.[log_space_used_percent], 
						qt.[is_auto_close], qt.[is_auto_shrink], qt.[volume_mount_point], 
						qt.[last_backup_time], qt.[last_dbcc checkdb_time],  qt.[recovery_model], qt.[page_verify_option], qt.[compatibility_level], qt.[is_growth_limited], qt.[is_snapshot], GETUTCDATE()
				FROM (
						SELECT    ISNULL(qt1.[database_id], qt2.[database_id]) [database_id]
								, qt2.[recovery_model]
								, qt2.[page_verify_option]
								, qt2.[compatibility_level]
								, qt1.[data_size_mb]
								, qt1.[data_space_used_percent]
								, qt1.[log_size_mb]
								, qt1.[log_space_used_percent]
								, qt1.[volume_mount_point]
								, qt2.[is_auto_close]
								, qt2.[is_auto_shrink]
								, qt2.[last_backup_time]
								, qt1.[last_dbcc checkdb_time]
								, qt1.[is_growth_limited]
								, ISNULL(qt2.[is_snapshot], 0) AS [is_snapshot]
						FROM (
								SELECT    [database_id]
										, [data_size_mb]
										, [data_space_used_percent]
										, [log_size_mb]
										, [log_space_used_percent]
										, [volume_mount_point]
										, [last_dbcc checkdb_time]
										, [is_growth_limited]										
								FROM #statsDatabaseDetails
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
										, [is_snapshot]
								FROM #statsDatabaseDetails
								WHERE [query_type]=2
							) qt2 ON qt1.[database_id] = qt2.[database_id]
					)qt
				INNER JOIN [dbo].[catalogDatabaseNames] cdn ON	cdn.[database_id] = qt.[database_id] 
															AND cdn.[instance_id] = @instanceID 
															AND cdn.[project_id] = @projectID

		INSERT	INTO [health-check].[statsDatabaseAlwaysOnDetails](	[catalog_database_id], [instance_id], [cluster_name], [ag_name], [role_desc], 
																	[replica_join_state_desc], [replica_connected_state_desc], [failover_mode_desc], [availability_mode_desc], [suspend_reason_desc],
																	[synchronization_health_desc], [synchronization_state_desc], [readable_secondary_replica], [event_date_utc])
				SELECT  DISTINCT
						  cdn.[id] AS [catalog_database_id]
						, cin.[id] AS [instance_id]
						, X.[cluster_name]
						, X.[ag_name]
						, X.[role_desc]
						, X.[replica_join_state_desc]
						, X.[replica_connected_state_desc]
						, X.[failover_mode_desc]
						, X.[availability_mode_desc]
						, X.[suspend_reason_desc]
						, X.[synchronization_health_desc]
						, X.[synchronization_state_desc]
						, X.[readable_secondary_replica]
						, GETUTCDATE()
				FROM #statsDatabaseAlwaysOnDetails X
				INNER JOIN dbo.catalogMachineNames cmn  ON	cmn.[name] = X.[host_name] COLLATE DATABASE_DEFAULT
				INNER JOIN dbo.catalogInstanceNames cin ON	cin.[name] = X.[instance_name] COLLATE DATABASE_DEFAULT 
															AND cin.[project_id] = cmn.[project_id] 
															AND cin.[machine_id] = cmn.[id]
				INNER JOIN dbo.catalogDatabaseNames cdn ON	cdn.[name] = X.[database_name] COLLATE DATABASE_DEFAULT
															AND cdn.[project_id] = cmn.[project_id] 
															AND cdn.[instance_id] = cin.[id]
				LEFT JOIN [health-check].[statsDatabaseAlwaysOnDetails] sdaod ON sdaod.[catalog_database_id] = cdn.[id] 
																				AND sdaod.[instance_id] = cin.[id] 
																				AND sdaod.[cluster_name] = X.[cluster_name] COLLATE DATABASE_DEFAULT
																				AND sdaod.[ag_name] = X.[ag_name] COLLATE DATABASE_DEFAULT
				WHERE cin.[project_id] = @projectID
						AND sdaod.[id] IS NULL
		
		/* extract backupset information - include backup details not made using this utility */
		;WITH tdpBackups AS
		(
			SELECT	  lem.[project_id], lem.[instance_id], lem.[instance_name], lem.[database_name]
					, info.value ('size_bytes[1]', 'bigint') as [size_bytes]
					, info.value ('file_name[1]', 'sysname') as [file_name]
					, info.value ('type[1]', 'nvarchar(16)') as [backup_type]
					, lem.[event_date_utc]
					, lem.[message_xml]
			FROM (
					SELECT l.[project_id], l.[instance_id], l.[instance_name], l.[database_name], l.[message_xml], l.[event_date_utc]
					FROM [dbo].[vw_logEventMessages] l
					INNER JOIN [dbo].[vw_catalogProjects] p ON l.[project_id] = p.[project_id]
					WHERE l.[module]='dbo.usp_mpDatabaseBackup'
						AND l.[event_name]='database backup'
						AND l.[event_type]=0
						AND l.[message] LIKE '<backupset>%'
						AND l.[event_date_utc] >= @startEvent
						AND p.[project_code] LIKE @projectCode
						AND l.[instance_name] LIKE @sqlServerNameFilter
				) lem
			CROSS APPLY [message_xml].nodes('//backupset/detail') M(info)
		)
		INSERT	INTO [dbo].[logEventMessages]([project_id], [instance_id], [event_date_utc], [module], [event_name], [database_name], [message], [event_type])
		SELECT	@projectID, @instanceID, GETUTCDATE()
				, 'dbo.usp_mpDatabaseBackup' AS [module]
				, 'database backup' AS [event_name]
				, bs.[database_name]
				, '<backupset><detail>' + 
						'<database_name>' + [dbo].[ufn_getObjectQuoteName](bs.[database_name], 'xml') + '</database_name>' + 
						'<type>' + bs.[backup_type] + '</type>' + 
						'<start_date>' + CONVERT([varchar](24), ISNULL([backup_start_date], GETDATE()), 121) + '</start_date>' + 
						'<duration>' + REPLICATE('0', 2-LEN(CAST([duration_sec] / 3600 AS [varchar]))) + CAST([duration_sec] / 3600 AS [varchar]) + 'h'
											+ ' ' + REPLICATE('0', 2-LEN(CAST(([duration_sec] / 60) % 60 AS [varchar]))) + CAST(([duration_sec] / 60) % 60 AS [varchar]) + 'm'
											+ ' ' + REPLICATE('0', 2-LEN(CAST([duration_sec] % 60 AS [varchar]))) + CAST([duration_sec] % 60 AS [varchar]) + 's' + '</duration>' + 
						'<size>' + CONVERT([varchar](32), CAST(bs.[size_bytes]/(1024*1024*1.0) AS [money]), 1) + ' mb</size>' + 
						'<size_bytes>' + CAST(bs.[size_bytes] AS [varchar](32)) + '</size_bytes>' + 
						'<verified>N/A</verified>' + 
						'<file_name>' + [dbo].[ufn_getObjectQuoteName](bs.[file_name], 'xml') + '</file_name>' + 
						'<error_code>0</error_code>' + 
					'</detail></backupset>'
				, 0 AS [event_type]
		FROM #msdbBackupHistory bs
		LEFT JOIN tdpBackups tdp ON		tdp.[instance_name] = @sqlServerName
									AND	tdp.[database_name] = bs.[database_name]
									AND tdp.[backup_type] = bs.[backup_type]
									AND tdp.[file_name] = bs.[file_name]
		WHERE tdp.[instance_name] IS NULL

		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion, @isAzureSQLDatabase
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
