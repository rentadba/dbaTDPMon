USE [dbaTDPMon]
GO

RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* dbaTDPMon (Troubleshoot Database Performance / Monitoring)                  *', 10, 1) WITH NOWAIT
RAISERROR('* https://github.com/rentadba/dbaTDPMon, under GNU (GPLv3) licence model      *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT
RAISERROR('* Patch script: from version 2017.4 to 2017.6 (2017.05.26)				  *', 10, 1) WITH NOWAIT
RAISERROR('*-----------------------------------------------------------------------------*', 10, 1) WITH NOWAIT

SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO
UPDATE [dbo].[appConfigurations] SET [value] = N'2017.05.26' WHERE [module] = 'common' AND [name] = 'Application Version'
GO

/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: commons																							   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: COMMONS', 10, 1) WITH NOWAIT



/*---------------------------------------------------------------------------------------------------------------------*/
/* patch module: health-check																						   */
/*---------------------------------------------------------------------------------------------------------------------*/
RAISERROR('Patching module: HEALTH-CHECK', 10, 1) WITH NOWAIT

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 26.05.2017
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

-----------------------------------------------------------------------------------------------------
--Health Check: database statistics & details
-----------------------------------------------------------------------------------------------------
RAISERROR('Create table: [health-check].[statsDatabaseAlwaysOnDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[health-check].[statsDatabaseAlwaysOnDetails]') AND type in (N'U'))
DROP TABLE [health-check].[statsDatabaseAlwaysOnDetails]
GO
CREATE TABLE [health-check].[statsDatabaseAlwaysOnDetails]
(
	[id]							[int]	 IDENTITY (1, 1)	NOT NULL,
	[catalog_database_id]			[smallint]		NOT NULL,
	[instance_id]					[smallint]		NOT NULL,
	[cluster_name]					[sysname]		NOT NULL,
	[ag_name]						[sysname]		NOT NULL,
	[role_desc]						[nvarchar](60)	NULL,
	[synchronization_health_desc]	[nvarchar](60)	NULL,
	[synchronization_state_desc]	[nvarchar](60)	NULL,
	[data_loss_sec]					[int]			NULL,
	[event_date_utc]				[datetime]		NOT NULL,
	CONSTRAINT [PK_statsDatabaseAlwaysOnDetails] PRIMARY KEY  CLUSTERED 
	(
		[id],
		[catalog_database_id]
	) ON [FG_Statistics_Data],
	CONSTRAINT [FK_statsDatabaseAlwaysOnDetails_catalogDatabaseNames] FOREIGN KEY 
	(
		  [catalog_database_id]
		, [instance_id]
	) 
	REFERENCES [dbo].[catalogDatabaseNames] 
	(
		  [id]
		, [instance_id]
	)
)ON [FG_Statistics_Data]
GO

CREATE INDEX [IX_statsDatabaseAlwaysOnDetails_CatalogDatabaseID] ON [health-check].[statsDatabaseAlwaysOnDetails] ([catalog_database_id], [instance_id]) ON [FG_Statistics_Index]
GO
CREATE INDEX [IX_statsDatabaseAlwaysOnDetails_InstanceID] ON [health-check].[statsDatabaseAlwaysOnDetails] ([instance_id]) ON [FG_Statistics_Index]
GO


RAISERROR('Create view : [health-check].[vw_statsDatabaseAlwaysOnDetails]', 10, 1) WITH NOWAIT
GO
IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[health-check].[vw_statsDatabaseAlwaysOnDetails]'))
DROP VIEW [health-check].[vw_statsDatabaseAlwaysOnDetails]
GO

CREATE VIEW [health-check].[vw_statsDatabaseAlwaysOnDetails]
/* WITH ENCRYPTION */
AS

-- ============================================================================
-- Copyright (c) 2004-2015 Dan Andrei STEFAN (danandrei.stefan@gmail.com)
-- ============================================================================
-- Author			 : Dan Andrei STEFAN
-- Create date		 : 05.12.2014
-- Module			 : Database Analysis & Performance Monitoring
-- ============================================================================

SELECT 	  cin.[project_id]		AS [project_id]
		, cmn.[id]				AS [machine_id]
		, cmn.[name]			AS [machine_name]
		, cin.[id]				AS [instance_id]
		, cin.[name]			AS [instance_name]
		, sdaod.[catalog_database_id]
		, cdn.[database_id]
		, cdn.[name]			AS [database_name]
		, cdn.[active]
		, cdn.[state]
		, cdn.[state_desc] 
		, sdaod.[cluster_name]
		, sdaod.[ag_name]
		, sdaod.[role_desc]
		, sdaod.[synchronization_health_desc]
		, sdaod.[synchronization_state_desc]
		, sdaod.[data_loss_sec]
		, sdaod.[event_date_utc]
FROM [dbo].[catalogInstanceNames]	cin	
INNER JOIN [dbo].[catalogMachineNames] cmn ON cmn.[id] = cin.[machine_id] AND cmn.[project_id] = cin.[project_id]
INNER JOIN [dbo].[catalogDatabaseNames] cdn ON cin.[id] = cdn.[instance_id] AND cin.[project_id] = cdn.[project_id]
INNER JOIN [health-check].[statsDatabaseAlwaysOnDetails] sdaod ON sdaod.[catalog_database_id] = cdn.[id] AND sdaod.[instance_id] = cdn.[instance_id]
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
	[physical_drives]			[sysname]		NULL,
	[last_backup_time]			[datetime]		NULL,
	[last_dbcc checkdb_time]	[datetime]		NULL,
	[recovery_model]			[tinyint]		NULL,
	[page_verify_option]		[tinyint]		NULL,
	[compatibility_level]		[tinyint]		NULL,
	[is_growth_limited]			[bit]			NULL
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
	[data_loss_sec]					[int]			NULL
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

INSERT	INTO [health-check].[statsDatabaseUsageHistory]([catalog_database_id], [instance_id], 
									  				 [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], 
													 [physical_drives], [event_date_utc])
		SELECT	shcdd.[catalog_database_id], shcdd.[instance_id], 
				shcdd.[data_size_mb], shcdd.[data_space_used_percent], shcdd.[log_size_mb], shcdd.[log_space_used_percent], 
				shcdd.[physical_drives], shcdd.[event_date_utc]
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
RAISERROR('--Step 2: Get Database Details Information....', 10, 1) WITH NOWAIT
		
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
		SET @strMessage='--	Analyzing server: ' + @sqlServerName
		RAISERROR(@strMessage, 10, 1) WITH NOWAIT

		TRUNCATE TABLE #statsDatabaseDetails

		BEGIN TRY
			SELECT @SQLMajorVersion = REPLACE(LEFT(ISNULL(@sqlServerVersion, ''), 2), '.', '') 
		END TRY
		BEGIN CATCH
			SET @SQLMajorVersion = 8
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
														SELECT    [name], CAST([size] AS [int]) * 8 AS [size_kb]
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
					INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
									SET @queryToRun = N'SELECT MAX([VALUE]) AS [Value]
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

									INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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

							INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
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
				INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [data_size_mb], [data_space_used_percent], [log_size_mb], [log_space_used_percent], [physical_drives], [last_dbcc checkdb_time], [is_growth_limited])
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
			INSERT	INTO #statsDatabaseDetails([query_type], [database_id], [last_backup_time], [is_auto_close], [is_auto_shrink], [recovery_model], [page_verify_option], [compatibility_level])
					EXEC (@queryToRun)
		END TRY
		BEGIN CATCH
			SET @strMessage = ERROR_MESSAGE()
			PRINT @strMessage

			INSERT	INTO [dbo].[logAnalysisMessages]([instance_id], [project_id], [event_date_utc], [descriptor], [message])
					SELECT  @instanceID
							, @projectID
							, GETUTCDATE()
							, 'dbo.usp_hcCollectDatabaseDetails'
							, @strMessage
		END CATCH

		/* check for AlwaysOn Availability Groups configuration */
		IF @SQLMajorVersion >= 12
			begin
				SET @queryToRun = N'WITH agDatabaseDetails AS
									(
										SELECT    hc.[cluster_name]
												, ag.[name] AS [ag_name]
												, hinm.[node_name] as [host_name]
												, arcn.[replica_server_name] AS [instance_name]
												, adc.[database_name]
												, ars.[role_desc]
												, ars.[synchronization_health_desc]
												, hdrs.[synchronization_state_desc]
												, hdrs.[last_commit_time]
										FROM sys.availability_replicas ar
										INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.[replica_id]=ar.[replica_id] AND ars.[group_id]=ar.[group_id]
										INNER JOIN sys.availability_groups ag ON ag.[group_id]=ar.[group_id]
										INNER JOIN sys.dm_hadr_availability_replica_cluster_nodes arcn ON arcn.[group_name]=ag.[name] AND arcn.[replica_server_name]=ar.[replica_server_name]
										INNER JOIN sys.dm_hadr_database_replica_states hdrs ON ar.[replica_id]=hdrs.[replica_id]
										INNER JOIN sys.availability_databases_cluster adc ON adc.[group_id]=hdrs.[group_id] AND adc.[group_database_id]=hdrs.[group_database_id]
										INNER JOIN sys.dm_hadr_instance_node_map hinm ON hinm.[ag_resource_id] = ag.[resource_id] AND hinm.[instance_name] = arcn.[replica_server_name]
										INNER JOIN sys.dm_hadr_cluster_members hcm ON hcm.[member_name] = hinm.[node_name]
										INNER JOIN sys.dm_hadr_cluster hc ON 1=1
									)
									SELECT    a.[cluster_name], a.[ag_name], a.[host_name], a.[instance_name], a.[database_name]
											, a.[role_desc], a.[synchronization_health_desc], a.[synchronization_state_desc]
											, DATEDIFF(ss, a.[last_commit_time], b.[last_commit_time]) AS [data_loss_sec]
									FROM agDatabaseDetails a
									INNER JOIN agDatabaseDetails b ON	a.[cluster_name]=b.[cluster_name] AND a.[ag_name]=b.[ag_name] 
																		AND a.[database_name]=b.[database_name] AND b.[role_desc]=''PRIMARY'''
				SET @queryToRun = [dbo].[ufn_formatSQLQueryForLinkedServer](@sqlServerName, @queryToRun)
				IF @debugMode = 1 PRINT @queryToRun
		
				BEGIN TRY
					INSERT	INTO #statsDatabaseAlwaysOnDetails([cluster_name], [ag_name], [host_name], [instance_name], [database_name], [role_desc], [synchronization_health_desc], [synchronization_state_desc], [data_loss_sec])
							EXEC (@queryToRun)
				END TRY
				BEGIN CATCH
					SET @strMessage = ERROR_MESSAGE()
					PRINT @strMessage

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
								FROM #statsDatabaseDetails
								WHERE [query_type]=2
							) qt2 ON qt1.[database_id] = qt2.[database_id]
					)qt
				INNER JOIN [dbo].[catalogDatabaseNames] cdn ON	cdn.[database_id] = qt.[database_id] 
															AND cdn.[instance_id] = @instanceID 
															AND cdn.[project_id] = @projectID

		INSERT	INTO [health-check].[statsDatabaseAlwaysOnDetails]([catalog_database_id], [instance_id], [cluster_name], [ag_name]
																	, [role_desc], [synchronization_health_desc], [synchronization_state_desc], [data_loss_sec], [event_date_utc])
				SELECT    cdn.[id] AS [catalog_database_id]
						, cin.[id] AS [instance_id]
						, X.[cluster_name]
						, X.[ag_name]
						, X.[role_desc]
						, X.[synchronization_health_desc]
						, X.[synchronization_state_desc]
						, X.[data_loss_sec]
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
		SET @HTMLReportFileName = 'Daily_HealthCheck_Report_for_' + REPLACE(@projectName, '\', '$') + '_from_' +
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

			DECLARE crsInstancesOffline CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsInstancesOffline CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
					FROM	[health-check].[vw_statsSQLAgentJobsHistory]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName

					SELECT	@hasDiskSpaceInfo = COUNT(*)
					FROM	[health-check].[vw_statsDiskSpaceInfo]
					WHERE	[project_id]=@projectID
							AND [instance_name] = @instanceName
					
					SELECT	@hasErrorlogMessages = COUNT(*)
					FROM	[health-check].[vw_statsErrorlogDetails]
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
			
			DECLARE crsDatabasesStatusPermissionErrors CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsDatabasesStatusIssuesDetected CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsSQLServerAgentJobsStatusPermissionErrors CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
			DECLARE crsSQLServerAgentJobsStatusIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT	ssajh.[instance_name], ssajh.[job_name], ssajh.[last_execution_status], ssajh.[last_execution_date], ssajh.[last_execution_time], ssajh.[message]
																								FROM	[health-check].[vw_statsSQLAgentJobsHistory] ssajh
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
			DECLARE crsLongRunningSQLAgentJobsIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT	  ssajh.[instance_name], ssajh.[job_name]
																									, ssajh.[last_execution_date] AS [start_date], ssajh.[last_execution_time] AS [start_time]
																									, [dbo].[ufn_reportHTMLFormatTimeValue](CAST(ssajh.[running_time_sec]*1000 AS [bigint])) AS [running_time]
																									, ssajh.[message]
																							FROM [health-check].[vw_statsSQLAgentJobsHistory] ssajh
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
			
			DECLARE crsDiskSpaceInformationPermissionErrors CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsDiskSpaceInformationIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT  DISTINCT
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

			DECLARE crsDatabasesStatusIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsDatabasesStatusIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsDatabaseMaxLogSizeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
					
			DECLARE crsDatabaseMinDataSpaceIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
					
			DECLARE crsDatabaseMaxLogSpaceIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsDatabaseLogVsDataSizeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    [machine_name], [instance_name], [is_clustered], [cluster_node_machine_name], [database_name]
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
			
			DECLARE crsDatabaseFixedFileSizeIssuesDetected CURSOR LOCAL FAST_FORWARD FOR	
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

			DECLARE crsDatabasePageVerifyIssuesDetected CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
					@minimumIndexMaintenanceFrequencyDays	[tinyint],
					@analyzeOnlyMessagesFromTheLastHours	[tinyint],
					@analyzeIndexMaintenanceOperation		[nvarchar](128)

			SET @minimumIndexMaintenanceFrequencyDays = 2
			SET @analyzeOnlyMessagesFromTheLastHours = 24
			SET @analyzeIndexMaintenanceOperation = 'REBUILD'

		
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

			DECLARE crsFrequentlyFragmentedIndexesMachineNames CURSOR LOCAL FAST_FORWARD FOR	SELECT    iff.[instance_name]
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

			DECLARE crsDatabaseBACKUPAgeIssuesDetected CURSOR LOCAL FAST_FORWARD FOR	WITH databaseBackupAgeDetails AS
																						(
																							SELECT    cin.[machine_name], cin.[instance_name]
																									, cin.[is_clustered], cin.[cluster_node_machine_name]
																									, cdn.[database_name]
																									, shcdd.[size_mb]
																									, shcdd.[last_backup_time]
																									, DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) AS [backup_age_days]
																									, CASE WHEN (    cdn.[database_name] NOT IN ('master', 'model', 'msdb', 'distribution') 
																												AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configUserDatabaseBACKUPAgeDays
																												)
																												OR (    cdn.[database_name] IN ('master', 'model', 'msdb', 'distribution') 
																													AND DATEDIFF(dd, shcdd.[last_backup_time], GETDATE()) > @configSystemDatabaseBACKUPAgeDays
																												)
																												OR (
																														cdn.[database_name] NOT IN ('tempdb')
																													AND shcdd.[last_backup_time] IS NULL
																												) THEN 1 ELSE 0 
																										END AS [outdated_backup]
																									, cdn.[catalog_database_id] 
																									, cdn.[instance_id] 
																									, sdaod.[cluster_name]
																									, sdaod.[ag_name]
																							FROM [dbo].[vw_catalogInstanceNames]  cin
																							INNER JOIN [dbo].[vw_catalogDatabaseNames]					cdn   ON cdn.[project_id] = cin.[project_id] AND cdn.[instance_id] = cin.[instance_id]
																							INNER JOIN [health-check].[vw_statsDatabaseDetails]			shcdd ON shcdd.[catalog_database_id] = cdn.[catalog_database_id] AND shcdd.[instance_id] = cdn.[instance_id] 
																							LEFT JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails]	sdaod ON sdaod.[catalog_database_id] = cdn.[catalog_database_id] AND sdaod.[instance_id] = cdn.[instance_id] 
																							LEFT JOIN [report].[htmlSkipRules] rsr ON	rsr.[module] = 'health-check'
																																		AND rsr.[rule_id] = 8192
																																		AND rsr.[active] = 1
																																		AND (rsr.[skip_value] = cin.[machine_name] OR rsr.[skip_value]=cin.[instance_name])
																							WHERE cin.[instance_active]=1
																									AND cdn.[active]=1
																									AND cin.[project_id] = @projectID
																									AND CHARINDEX(cdn.[state_desc], @configAdmittedState) <> 0
																									AND rsr.[id] IS NULL
																						)
																						SELECT   dbad.[machine_name], dbad.[instance_name], dbad.[is_clustered], dbad.[cluster_node_machine_name]
																							   , dbad.[database_name], dbad.[size_mb], dbad.[last_backup_time], dbad.[backup_age_days]
																						FROM databaseBackupAgeDetails dbad
																						WHERE [outdated_backup]=1
																							 AND NOT EXISTS (	
																												SELECT *
																												FROM databaseBackupAgeDetails dbad2
																												INNER JOIN [health-check].[vw_statsDatabaseAlwaysOnDetails] sdaod ON sdaod.[catalog_database_id] = dbad2.[catalog_database_id] AND sdaod.[instance_id] = dbad2.[instance_id] 
																												WHERE sdaod.[synchronization_health_desc] = 'HEALTHY'
																													  AND dbad2.[outdated_backup] = 0
																													  AND dbad2.[cluster_name] = dbad.[cluster_name]
																													  AND dbad2.[ag_name] = dbad.[ag_name]
																													  AND dbad2.[database_name] = dbad.[database_name]
																											)

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

			DECLARE crsDatabaseDBCCCHECKDBAgeIssuesDetected CURSOR LOCAL FAST_FORWARD  FOR	SELECT    cin.[machine_name], cin.[instance_name]
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

			DECLARE crsErrorlogMessagesPermissionErrors CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
			INNER JOIN [health-check].[vw_statsErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
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
												AND chf.[object_name] = 'statsErrorlogDetails'
												AND chf.[active] = 1
												AND PATINDEX(chf.[filter_pattern], eld.[text]) > 0
									)
					AND rsr.[id] IS NULL
			
			CREATE INDEX IX_filteredStatsSQLServerErrorlogDetail_InstanceName ON #filteredStatsSQLServerErrorlogDetail([instance_name])

			RAISERROR('		...done', 10, 1) WITH NOWAIT

			-----------------------------------------------------------------------------------------------------
			SET @issuesDetectedCount = 0 
			DECLARE crsErrorlogMessagesInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
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
			
			DECLARE crsDatabasesStatusMachineNames CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[instance_name]
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
			
			DECLARE crsSQLServerAgentJobsInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT	ssajh.[instance_name], COUNT(*) AS [job_count]
																					FROM	[health-check].[vw_statsSQLAgentJobsHistory] ssajh
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
													FROM	[health-check].[vw_statsSQLAgentJobsHistory]
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

			DECLARE crsDiskSpaceInformationMachineNames CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
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
			
			DECLARE crsErrorlogMessagesInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
																							  cin.[instance_name]
																							, COUNT(*) AS [messages_count]
																					FROM [dbo].[vw_catalogInstanceNames]  cin
																					INNER JOIN [health-check].[vw_statsErrorlogDetails]	eld	ON eld.[project_id] = cin.[project_id] AND eld.[instance_id] = cin.[instance_id]
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
													FROM	[health-check].[vw_statsErrorlogDetails] eld
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

			DECLARE crsOSEventMessagesPermissionErrors CURSOR LOCAL FAST_FORWARD FOR	SELECT    cin.[machine_name], cin.[is_clustered], cin.[cluster_node_machine_name]
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
			
			DECLARE crsOSEventMessagesInstanceName CURSOR LOCAL FAST_FORWARD FOR	SELECT DISTINCT
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



/*---------------------------------------------------------------------------------------------------------------------*/
USE [dbaTDPMon]
GO
SELECT * FROM [dbo].[appConfigurations] WHERE [module] = 'common' AND [name] = 'Application Version'
GO

RAISERROR('* Done *', 10, 1) WITH NOWAIT
