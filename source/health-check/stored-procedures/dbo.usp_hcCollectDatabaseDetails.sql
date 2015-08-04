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
		@strMessage				[nvarchar](max)

DECLARE @SQLMajorVersion		[int],
		@sqlServerVersion		[sysname],
		@dbccLastKnownGood		[datetime],
		@optionAdmittedState	[sysname]

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
	SELECT @projectCode = [value]
	FROM [dbo].[appConfigurations]
	WHERE [name] = 'Default project code'

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


/*-------------------------------------------------------------------------------------------------------------------------------*/
SELECT	@optionAdmittedState = [value]
FROM	[dbo].[reportHTMLOptions]
WHERE	[name] = N'Database online admitted state'
		AND [report_type_id]=0

SET @optionAdmittedState = ISNULL(@optionAdmittedState, 'ONLINE, READ ONLY')

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
																AND CHARINDEX(cdn.[state_desc], @optionAdmittedState)<>0
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
