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
		@dbccLastKnownGood		[datetime]

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
	[synchronization_health_desc]	[nvarchar](60)	NULL,
	[synchronization_state_desc]	[nvarchar](60)	NULL,
	[readable_secondary_replica]	[nvarchar](60)	NULL
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


DELETE sdaod
FROM [health-check].[statsDatabaseAlwaysOnDetails] sdaod
INNER JOIN [dbo].[catalogDatabaseNames]			cdb ON cdb.[id] = sdaod.[catalog_database_id] AND cdb.[instance_id] = sdaod.[instance_id]
INNER JOIN [dbo].[catalogInstanceNames]			cin ON cin.[id] = cdb.[instance_id] AND cin.[project_id] = cdb.[project_id]
WHERE cin.[project_id] = @projectID
		AND cin.[name] LIKE @sqlServerNameFilter
		AND cdb.[name] LIKE @databaseNameFilter

-------------------------------------------------------------------------------------------------------------------------
SET @strMessage='Step 2: Get Database Details Information....'
EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
DECLARE crsActiveInstances CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cin.[instance_id], cin.[instance_name], cin.[version]
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
		EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 1, @stopExecution=0

		TRUNCATE TABLE #statsDatabaseDetails

		BEGIN TRY
			SET @serverVersionNum=SUBSTRING(@sqlServerVersion, 1, CHARINDEX('.', @sqlServerVersion)-1) + '.' + REPLACE(SUBSTRING(@sqlServerVersion, CHARINDEX('.', @sqlServerVersion)+1, LEN(@sqlServerVersion)), '.', '')
		END TRY
		BEGIN CATCH
			SET @serverVersionNum = 9.0
		END CATCH

		DECLARE crsActiveDatabases CURSOR LOCAL FAST_FORWARD FOR 	SELECT	cdn.[catalog_database_id], cdn.[database_id], cdn.[database_name]
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
				SET @strMessage='database: ' + [dbo].[ufn_getObjectQuoteName](@databaseName, 'quoted')
				EXEC [dbo].[usp_logPrintMessage] @customMessage = @strMessage, @raiseErrorAsPrint = 1, @messagRootLevel = 1, @messageTreelevel = 2, @stopExecution=0

				/* get space allocated / used details */
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
																CASE WHEN @serverVersionNum >= 10.5
																	 THEN N', CASE WHEN LEN([volume_mount_point])=3 THEN UPPER([volume_mount_point]) ELSE [volume_mount_point] END [volume_mount_point] '
																	 ELSE N', REPLACE(LEFT([physical_name], 2), '''''''':'''''''', '''''''''''''''') AS [volume_mount_point] '
																END + '
																, CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END AS [is_growth_limited]
														FROM sys.database_files' + 
														CASE WHEN @serverVersionNum >= 10.5
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
														END + '
														, CASE WHEN ([max_size]=-1 AND [type]=0) OR ([max_size]=-1 AND [type]=1) OR ([max_size]=268435456 AND [type]=1) THEN 0 ELSE 1 END AS [is_growth_limited]
												FROM sys.database_files' + 
												CASE WHEN @serverVersionNum >= 10.5
														THEN N' CROSS APPLY sys.dm_os_volume_stats(DB_ID(), [file_id])'
														ELSE N''
												END + N'
											)sf
										GROUP BY [volume_mount_point], [is_logfile]'			
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
				
				TRUNCATE TABLE #databaseSpaceInfo
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
				
				TRUNCATE TABLE #dbccLastKnownGood
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

				/* compute database statistics */
				INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], [volume_mount_point], [last_dbcc checkdb_time], [is_growth_limited])
						SELECT    1, @databaseID
								, CAST([data_size_mb] AS [numeric](20,3)) AS [data_size_mb]
								, CAST(CASE WHEN [data_size_mb] <>0 THEN [data_space_used_mb] * 100. / [data_size_mb] ELSE 0 END AS [numeric](6,2)) AS [data_used_percent]
								, CAST([log_size_mb] AS [numeric](20,3)) AS [log_size_mb]
								, CAST(CASE WHEN [log_size_mb] <>0 THEN [log_space_used_mb] * 100. / [log_size_mb] ELSE 0 END AS [numeric](6,2)) AS [log_used_percent]
								, [volume_mount_point]
								, @dbccLastKnownGood
								, [is_growth_limited]
						FROM (
								SELECT    SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[size_mb] ELSE 0 END)		AS [data_size_mb]
										, SUM(CASE WHEN [is_log_file] = 0 THEN dsi.[space_used_mb] ELSE 0 END) 	AS [data_space_used_mb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[size_mb] ELSE 0 END) 		AS [log_size_mb]
										, SUM(CASE WHEN [is_log_file] = 1 THEN dsi.[space_used_mb] ELSE 0 END) 	AS [log_space_used_mb]
										, MAX(x.[volume_mount_points]) [volume_mount_point]
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
									, bkp.[is_snapshot]
							FROM (
								  SELECT	  sdb.[name]	AS [database_name]
											, sdb.[database_id]
											, sdb.[recovery_model]
											, sdb.[page_verify_option]
											, sdb.[compatibility_level]
											, MAX(bs.[backup_finish_date]) AS [last_backup_time]
											, CASE WHEN sdb.[source_database_id] IS NULL THEN 0 ELSE 1 END AS [is_snapshot]
								  FROM sys.databases sdb
								  LEFT OUTER JOIN msdb.dbo.backupset bs ON bs.[database_name] = sdb.[name] AND bs.type IN (''D'', ''I'')
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

		/* check for AlwaysOn Availability Groups configuration */
		IF @serverVersionNum >= 12
			begin
				SET @queryToRun = N'SELECT    hc.[cluster_name]
											, ag.[name] AS [ag_name]
											, hinm.[node_name] as [host_name]
											, arcn.[replica_server_name] AS [instance_name]
											, adc.[database_name]
											, ars.[role_desc]
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
									WHERE arcn.[replica_server_name] = ''' + @sqlServerName + ''''

				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 EXEC [dbo].[usp_logPrintMessage] @customMessage = @queryToRun, @raiseErrorAsPrint = 0, @messagRootLevel = 0, @messageTreelevel = 1, @stopExecution=0
		
				BEGIN TRY
					INSERT	INTO #statsDatabaseAlwaysOnDetails([cluster_name], [ag_name], [host_name], [instance_name], [database_name], [role_desc], [synchronization_health_desc], [synchronization_state_desc], [readable_secondary_replica])
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
								, qt2.[is_snapshot]
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

		INSERT	INTO [health-check].[statsDatabaseAlwaysOnDetails]([catalog_database_id], [instance_id], [cluster_name], [ag_name]
																	, [role_desc], [synchronization_health_desc], [synchronization_state_desc], [readable_secondary_replica], [event_date_utc])
				SELECT    cdn.[id] AS [catalog_database_id]
						, cin.[id] AS [instance_id]
						, X.[cluster_name]
						, X.[ag_name]
						, X.[role_desc]
						, X.[synchronization_health_desc]
						, X.[synchronization_state_desc]
						, X.[readable_secondary_replica]
						, GETUTCDATE()
				FROM #statsDatabaseAlwaysOnDetails X
				INNER JOIN dbo.catalogMachineNames cmn ON cmn.[name] = X.[host_name]
				INNER JOIN dbo.catalogInstanceNames cin ON cin.[name] = X.[instance_name] AND cin.[project_id] = cmn.[project_id]  AND cin.[machine_id] = cmn.[id]
				INNER JOIN dbo.catalogDatabaseNames cdn ON cdn.[name] = X.[database_name] AND cdn.[project_id] = cmn.[project_id]  AND cdn.[instance_id] = cin.[id]
				LEFT JOIN [health-check].[statsDatabaseAlwaysOnDetails] sdaod ON sdaod.[catalog_database_id] = cdn.[id] AND sdaod.[instance_id] = cin.[id] 
																				AND sdaod.[cluster_name] = X.[cluster_name] AND sdaod.[ag_name] = X.[ag_name]
				WHERE cin.[project_id] = @projectID
						AND sdaod.[id] IS NULL
		
		FETCH NEXT FROM crsActiveInstances INTO @instanceID, @sqlServerName, @sqlServerVersion
	end
CLOSE crsActiveInstances
DEALLOCATE crsActiveInstances
GO
